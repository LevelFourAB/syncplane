package connect

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/levelfourab/syncplane/engine/server"
)

// Ingress runs one source→engine pass: incremental when the source
// implements Incremental and its stored cursor is still live, a full
// Reconcile otherwise. It is the entry point a runner uses, and the one to
// call by hand for a source that reports its own changes; Reconcile remains
// available for forcing the full enumeration.
//
// For a source that is not Incremental this is exactly Reconcile.
func (c *Connector) Ingress(ctx context.Context, workspaceID string) error {
	return c.converge(ctx, workspaceID, func(p *pass) error {
		return c.ingress(ctx, workspaceID, p)
	})
}

func (c *Connector) ingress(ctx context.Context, workspaceID string, p *pass) error {
	inc, ok := p.src.(Incremental)
	if !ok {
		// No change cursor to keep: every pass is the full enumeration, and
		// the connector stores no ingress bookkeeping at all.
		return c.reconcile(ctx, workspaceID, p)
	}
	sync, err := c.state.SourceSync(ctx, workspaceID)
	if err != nil {
		return err
	}
	if c.needsFullSync(sync) {
		return c.fullSync(ctx, workspaceID, p, inc)
	}
	err = c.incrementalSync(ctx, workspaceID, p, inc, sync)
	if errors.Is(err, ErrCursorInvalid) {
		// The source disowned the cursor mid-pass (UIDVALIDITY bumped, the
		// history window expired). Recover inside this same call rather than
		// leaving the workspace stale until the next tick.
		c.logger.LogAttrs(ctx, slog.LevelInfo, "source cursor invalid; falling back to full sync",
			slog.String("workspace", workspaceID), slog.Any("err", err))
		return c.fullSync(ctx, workspaceID, p, inc)
	}
	return err
}

// needsFullSync reports whether the safety net is due: no cursor to resume
// from, or the full-enumeration interval has elapsed. A negative
// FullReconcileInterval opts out of the periodic net entirely, leaving
// ErrCursorInvalid and a missing cursor as the only triggers.
func (c *Connector) needsFullSync(sync SourceSync) bool {
	if sync.Cursor == "" {
		return true
	}
	if c.fullInterval < 0 {
		return false
	}
	return time.Since(sync.LastFullSync) >= c.fullInterval
}

// fullSync checkpoints the source, enumerates it, and records both.
//
// The checkpoint is taken BEFORE the list, never after: a change landing
// while the list runs may or may not appear in it, and resuming from a cursor
// taken beforehand redelivers that change (a fingerprint no-op if the list
// already carried it) instead of stepping over it.
func (c *Connector) fullSync(ctx context.Context, workspaceID string, p *pass, inc Incremental) error {
	cursor, err := inc.Checkpoint(ctx, workspaceID)
	if err != nil {
		return fmt.Errorf("connect: checkpoint %s: %w", workspaceID, err)
	}
	if err := c.reconcile(ctx, workspaceID, p); err != nil {
		return err
	}
	return c.state.SetSourceSync(ctx, workspaceID, SourceSync{Cursor: cursor, LastFullSync: time.Now()})
}

// incrementalSync pages Changes until the source says it is caught up.
//
// Each page is applied and only then is its cursor persisted, which makes
// delivery at-least-once: a crash in between replays the page. Both halves
// tolerate the replay — an item whose fingerprint matches the shadow is a
// no-op, and applyDeletion reads the row first so a redelivered delete does
// not burn a second tombstone.
func (c *Connector) incrementalSync(ctx context.Context, workspaceID string, p *pass, inc Incremental, sync SourceSync) error {
	for {
		delta, err := inc.Changes(ctx, workspaceID, sync.Cursor)
		if err != nil {
			return fmt.Errorf("connect: changes %s: %w", workspaceID, err)
		}
		for _, item := range delta.Items {
			if _, _, err := c.ingestItem(ctx, workspaceID, p, item); err != nil {
				return err
			}
		}
		for _, del := range delta.Deletions {
			if err := c.applyDeletion(ctx, workspaceID, del); err != nil {
				return err
			}
		}
		if n := len(delta.Items) + len(delta.Deletions); n > 0 {
			c.logger.LogAttrs(ctx, slog.LevelDebug, "ingress applied delta",
				slog.String("workspace", workspaceID),
				slog.Int("items", len(delta.Items)), slog.Int("deletions", len(delta.Deletions)))
		}
		sync.Cursor = delta.NextCursor
		if err := c.state.SetSourceSync(ctx, workspaceID, sync); err != nil {
			return err
		}
		if !delta.More {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

// ingestItem resolves one item's binding and converges it, returning the row
// it addressed. Shared by the full enumeration and the incremental path, so
// both treat an unbound entity and an undecodable text field identically —
// and so the full pass can mark the row seen even when the item was skipped.
func (c *Connector) ingestItem(ctx context.Context, workspaceID string, p *pass, item Item) (entity, key string, err error) {
	entity, key, err = c.srv.EntityKey(item.Msg)
	if err != nil {
		return "", "", fmt.Errorf("connect: ingress: %w", err)
	}
	b, ok := c.bound[entity]
	if !ok {
		return "", "", fmt.Errorf("connect: ingress: source reported unbound entity %q", entity)
	}
	// Text fields arrive as plain strings the source read from wherever it
	// keeps them, and a mis-encoded one (a latin-1 file, a truncated
	// multi-byte read) is not storable: the text package refuses it rather
	// than substituting U+FFFD, because a substituted document would never
	// compare equal to the source again and would be rewritten on every pass.
	// Skip this item and keep going — one broken record must not stall
	// ingress for the rest of the source. Nothing is written, so the shadow
	// keeps its old base and the item converges on the first pass after its
	// encoding is fixed.
	if field, off := invalidTextField(item.TextFields); field != "" {
		c.logger.LogAttrs(ctx, slog.LevelError, "ingress skipped item with undecodable text",
			slog.String("workspace", workspaceID), slog.String("entity", entity),
			slog.String("key", key), slog.String("field", field), slog.Int("byte", off))
		return entity, key, nil
	}
	return entity, key, c.ingest(ctx, workspaceID, p, b, entity, key, item)
}

// applyDeletion tombstones a row the source reported gone. Unlike the full
// pass's sweep this needs no enumeration — the source named the row — but it
// must tolerate redelivery, so it writes nothing when the engine no longer
// holds the row.
func (c *Connector) applyDeletion(ctx context.Context, workspaceID string, del Deletion) error {
	if del.Prototype == nil {
		return fmt.Errorf("connect: ingress: deletion of %q carries no prototype", del.Key)
	}
	entity, _, err := c.srv.EntityKey(del.Prototype)
	if err != nil {
		return fmt.Errorf("connect: ingress: %w", err)
	}
	b, ok := c.bound[entity]
	if !ok {
		return fmt.Errorf("connect: ingress: source reported unbound entity %q", entity)
	}
	// Arrival-order LWW, like every write: the delete reaches the
	// linearization point now; a concurrent client edit that lands later
	// recreates the row.
	if err := c.srv.Mutate(server.TrustedAs(ctx, c.label), workspaceID, func(tx *server.EntityTx) error {
		cur := b.prototype.ProtoReflect().New().Interface()
		found, err := tx.Get(cur, del.Key)
		if err != nil || !found {
			return err
		}
		return tx.Delete(b.prototype, del.Key)
	}); err != nil {
		return err
	}
	return c.state.DeleteShadow(ctx, workspaceID, entity, del.Key)
}
