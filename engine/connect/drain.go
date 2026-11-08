package connect

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"google.golang.org/protobuf/proto"

	"github.com/levelfourab/syncplane/engine/server"
)

// Drain egresses committed engine changes to the source, resuming from the
// durable cursor and advancing it page by page until the feed is dry.
// Delivery is at-least-once: a crash between writing the source and
// persisting bookkeeping redelivers the change, and the shadow makes the
// redelivery a no-op.
//
// Entities bound IngressOnly are skipped: their changes advance the cursor
// like any other, they just never reach the source.
func (c *Connector) Drain(ctx context.Context, workspaceID string) error {
	return c.converge(ctx, workspaceID, func(p *pass) error {
		return c.drain(ctx, workspaceID, p)
	})
}

func (c *Connector) drain(ctx context.Context, workspaceID string, p *pass) error {
	logger := c.logger.With("workspace", workspaceID)
	for {
		cur, err := c.state.Cursor(ctx, workspaceID)
		if err != nil {
			return err
		}
		if cur.Seq == 0 {
			// A State that lost its cursor resumes from the copy in the
			// engine: the changes below it reached the source already.
			if mirrored, ok, err := c.egressMirror(ctx, workspaceID, p); err != nil {
				return err
			} else if ok {
				cur = mirrored
			}
		}
		// Collect the page inside the read snapshot, act on it outside: the
		// source's I/O must never run inside (and pin) an engine read tx.
		var page []server.Change
		next, more, err := c.srv.Changes(ctx, workspaceID, cur, c.batch, func(ch server.Change) error {
			if b, ok := c.bound[ch.Entity]; ok && b.direction == Bidirectional {
				page = append(page, ch)
			}
			return nil
		})
		if errors.Is(err, server.ErrCursorTooOld) {
			// The cursor cannot be resumed: changes below it were collected,
			// or the store answering now is not the one that issued it. Either
			// way the feed can no longer say what the source is missing, so
			// heal both directions outside it — source→engine via reconcile,
			// engine→source via the shadow/state sweep — then resume from the
			// position the feed handed back.
			foreign := errors.Is(err, server.ErrCursorForeign)
			logger.LogAttrs(ctx, slog.LevelWarn, "feed cursor cannot be resumed; healing via full reconcile + resync",
				slog.Uint64("resume_seq", next.Seq), slog.Bool("foreign_history", foreign))
			// After a store swap, a restore or a workspace delete and restore,
			// the shadows describe rows of a history the engine no longer
			// holds. Kept, they would delete source rows the engine lacks as
			// lost deletes, and suppress new changes at or below their old
			// sequences as the connector's own writes. Without them the heal
			// is the initial import. Below the horizon the history is still
			// the engine's, and the shadows are what finds the lost deletes.
			if foreign {
				if err := c.forgetShadows(ctx, workspaceID); err != nil {
					return err
				}
			}
			if err := c.reconcile(ctx, workspaceID, p); err != nil {
				return err
			}
			if err := c.resyncSource(ctx, workspaceID, p, next.Seq); err != nil {
				return err
			}
			if err := c.setCursor(ctx, workspaceID, p, next); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		for _, ch := range page {
			if err := c.egress(ctx, workspaceID, p, ch); err != nil {
				return err
			}
		}
		if len(page) > 0 {
			logger.LogAttrs(ctx, slog.LevelDebug, "drain page egressed", slog.Int("rows", len(page)))
		}
		if err := c.setCursor(ctx, workspaceID, p, next); err != nil {
			return err
		}
		if !more {
			return nil
		}
	}
}

// setCursor records the egress position in the State, and its copy in the
// engine (see egressMirror).
func (c *Connector) setCursor(ctx context.Context, workspaceID string, p *pass, next server.Cursor) error {
	if err := c.state.SetCursor(ctx, workspaceID, next); err != nil {
		return err
	}
	if c.consumer == "" {
		return nil
	}
	err := c.srv.AdvanceConsumer(ctx, workspaceID, c.consumer, server.ConsumerUpdate{Cursor: next})
	if errors.Is(err, server.ErrCursorForeign) {
		// The workspace's history changed since Changes returned next, and the
		// next pass meets the foreign cursor and heals from there.
		return nil
	}
	if err != nil {
		return err
	}
	p.egressed, p.egressedRead = &server.Cursor{Seq: next.Seq, Entity: next.Entity, Key: next.Key}, true
	return nil
}

// egressMirror returns the egress position the connector copied into the
// engine's store, as the consumer position of its label, and whether there is
// one. The copy lives with the rows, so it survives a lost State: the changes
// at or below it reached the source, and a row the engine changed after it
// holds an edit the source has not received. There is none without
// Config.WriterID, whose label is the only name stable across restarts.
func (c *Connector) egressMirror(ctx context.Context, workspaceID string, p *pass) (server.Cursor, bool, error) {
	if c.consumer == "" {
		return server.Cursor{}, false, nil
	}
	if !p.egressedRead {
		cur, ok, err := c.srv.ConsumerPosition(ctx, workspaceID, c.consumer)
		if err != nil {
			return server.Cursor{}, false, err
		}
		p.egressedRead = true
		if ok {
			p.egressed = &cur
		}
	}
	if p.egressed == nil {
		return server.Cursor{}, false, nil
	}
	return *p.egressed, true, nil
}

// forgetShadows deletes every shadow of the workspace.
func (c *Connector) forgetShadows(ctx context.Context, workspaceID string) error {
	// Collect first: State implementations need not support reentrant calls
	// under an open scan.
	type rowID struct{ entity, key string }
	var ids []rowID
	if err := c.state.ScanShadows(ctx, workspaceID, func(entity, key string, _ Shadow) error {
		ids = append(ids, rowID{entity, key})
		return nil
	}); err != nil {
		return err
	}
	for _, id := range ids {
		if err := c.state.DeleteShadow(ctx, workspaceID, id.entity, id.key); err != nil {
			return err
		}
	}
	return nil
}

// egress projects one engine change onto the source, consulting the shadow
// to suppress the connector's own ingress writes.
func (c *Connector) egress(ctx context.Context, workspaceID string, p *pass, ch server.Change) error {
	prototype := c.bound[ch.Entity].prototype
	w, err := p.writer()
	if err != nil {
		return err
	}
	sh, err := c.state.Shadow(ctx, workspaceID, ch.Entity, ch.Key)
	if err != nil {
		return err
	}
	// The shadow's engine version is the seq of the connector's own ingress
	// write (or of a change already egressed); anything at or below it is
	// already reflected in the source.
	if sh != nil && ch.Seq <= sh.EngineVersion {
		return nil
	}
	if ch.Deleted {
		if err := w.Delete(ctx, workspaceID, prototype, ch.Key); err != nil {
			return fmt.Errorf("connect: egress delete %s/%s: %w", ch.Entity, ch.Key, err)
		}
		return c.state.DeleteShadow(ctx, workspaceID, ch.Entity, ch.Key)
	}
	if ch.Message == nil {
		return fmt.Errorf("connect: egress %s/%s: change carries no message", ch.Entity, ch.Key)
	}
	// Crash repair: the content already reached the source (we crashed before
	// stamping the shadow's engine version); just repair the bookkeeping
	// instead of re-writing identical content.
	if sh != nil && len(sh.Base) > 0 {
		base, err := c.decodeBase(prototype, sh.Base)
		if err != nil {
			return err
		}
		if proto.Equal(base, ch.Message) {
			return c.state.PutShadow(ctx, workspaceID, ch.Entity, ch.Key, Shadow{
				SourceFP:      sh.SourceFP,
				EngineVersion: ch.Seq,
				Base:          sh.Base,
				BaseText:      sh.BaseText,
			})
		}
	}
	fp, err := w.Write(ctx, workspaceID, ch.Message)
	if err != nil {
		return fmt.Errorf("connect: egress write %s/%s: %w", ch.Entity, ch.Key, err)
	}
	return c.putSyncedShadow(ctx, workspaceID, ch.Entity, ch.Key, ch.Message, fp, ch.Seq)
}

// putSyncedShadow records a row the source has just been written: the engine
// message is now both sides' agreed state, so it is the base — and its text
// fields render to what the source holds.
func (c *Connector) putSyncedShadow(ctx context.Context, workspaceID, entity, key string, msg proto.Message, fp string, seq uint64) error {
	base, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	baseText, err := plainTextFields(msg)
	if err != nil {
		return err
	}
	return c.state.PutShadow(ctx, workspaceID, entity, key, Shadow{
		SourceFP:      fp,
		EngineVersion: seq,
		Base:          base,
		BaseText:      baseText,
	})
}
