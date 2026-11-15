package connect

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"time"

	"github.com/levelfourab/syncplane/engine/server"
)

// RowRef names one row by its entity's wire name and its key.
type RowRef struct {
	Entity string
	Key    string
}

// Refused reports rows whose write the host could not keep and reverted,
// such as writes of this connector that an origin server refused. On a
// replica, call it from upstream.Config.OnRejected, as the example shows.
//
// The connector marks the record of each row, durably, and schedules a pass.
// The next egress of a marked record gives the store content it replaces or
// deletes to Records.Overwrite with OverwriteRefused first, so that a store
// edit the engine did not keep is not lost without a trace. When egress
// wrote the record before the call, the content it replaced is given
// instead, if Records.KeepReplaced kept it; else nothing is given. One
// refusal can give two contents: the one egress replaced, when it was kept,
// and the one the store holds. A mark is cleared after use, and after a pass
// that finds its record written and no kept content.
//
// Rows of entities outside record mode, of types that are not
// Bidirectional, and rows no record holds are ignored. Calling it more than
// once for one refusal is safe. Refused is safe for concurrent use. It
// returns an error in row mode and when the engine's store fails; nothing is
// marked then.
func (c *Connector) Refused(ctx context.Context, workspaceID string, rows []RowRef) error {
	if c.rec == nil {
		return errors.New("connect: Refused works in record mode only")
	}
	if workspaceID == "" {
		return errors.New("connect: workspace id is required")
	}
	// The rows were reverted before the call, so a drain to head sees the
	// revert before a pass decides that the record was written already.
	head, err := c.srv.CurrentSeq(ctx, workspaceID)
	if err != nil {
		return err
	}
	type recID struct {
		rt  *recordType
		key string
	}
	marks := make(map[recID]bool)
	var unmapped []RowRef
	err = c.srv.View(server.Trusted(ctx), workspaceID, func(tx *server.ViewTx) error {
		clear(marks)
		unmapped = unmapped[:0]
		for _, row := range rows {
			rt, ok := c.rec.byEntity[row.Entity]
			if !ok || rt.direction != Bidirectional {
				continue
			}
			if row.Entity == rt.rootName {
				marks[recID{rt, row.Key}] = true
				continue
			}
			mapped := false
			owner, err := tx.ConsumerShadow(c.consumer, idxEntity(row.Entity), row.Key)
			if err != nil {
				return err
			}
			if owner != nil {
				marks[recID{rt, string(owner)}] = true
				mapped = true
			}
			m := c.rec.memberOf[row.Entity]
			msg := m.prototype.ProtoReflect().New().Interface()
			found, _, err := tx.Get(msg, row.Key)
			if err != nil {
				return err
			}
			if key := m.recordKeyOf(msg); found && key != "" {
				marks[recID{rt, key}] = true
				mapped = true
			}
			if !mapped {
				unmapped = append(unmapped, row)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	// A member row the revert deleted, of a record egress wrote since, is in
	// no index: only the kept content of the record names it.
	for _, row := range unmapped {
		rt := c.rec.byEntity[row.Entity]
		if err := c.srv.ScanConsumerShadows(ctx, workspaceID, c.consumer, keptEntity(rt), func(key string, raw []byte) error {
			k, err := decodeKept(raw)
			if err != nil {
				return err
			}
			if slices.Contains(k.Members[row.Entity], row.Key) {
				marks[recID{rt, key}] = true
			}
			return nil
		}); err != nil {
			return err
		}
	}
	if len(marks) == 0 {
		return nil
	}
	mark := encodeJSON(refusedMark{Head: head})
	if err := c.shadowMutate(ctx, workspaceID, func(tx *server.EntityTx) error {
		for id := range marks {
			if err := tx.PutConsumerShadow(c.consumer, refusedEntity(id.rt), id.key, mark); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started && !c.stopped {
		if w, fresh := c.admitLocked(workspaceID); !fresh {
			c.scheduleLocked(w, triggerDrain)
		}
	}
	return nil
}

// refusedMark is a record whose write the host reported refused.
type refusedMark struct {
	// Head is the workspace sequence when Refused ran. The revert is at or
	// below it.
	Head uint64 `json:"s"`
}

// keptContent is store content an egress write or delete replaced, kept for
// Records.KeepReplaced in case Refused reports the record later.
type keptContent struct {
	Locator string `json:"l"`
	Content []byte `json:"c"`
	// At is when egress replaced it, and Ingested the Ingested of the agreed
	// state it replaced, both in Unix ns.
	At       int64 `json:"a"`
	Ingested int64 `json:"i,omitempty"`
	// Members are the member row keys of the agreed state it replaced.
	Members map[string][]string `json:"m,omitempty"`
}

func decodeKept(raw []byte) (*keptContent, error) {
	if raw == nil {
		return nil, nil
	}
	var k keptContent
	if err := json.Unmarshal(raw, &k); err != nil {
		return nil, fmt.Errorf("connect: decode kept content: %w", err)
	}
	return &k, nil
}

// refusal is the refused mark and the kept content of one record.
type refusal struct {
	marked bool
	kept   *keptContent
}

// loadRefusal reads the refusal state of a record; nil when it has no mark.
func (c *Connector) loadRefusal(ctx context.Context, ws string, rt *recordType, key string) (*refusal, error) {
	if rt.direction != Bidirectional {
		return nil, nil
	}
	raw, err := c.readShadow(ctx, ws, refusedEntity(rt), key)
	if err != nil || raw == nil {
		return nil, err
	}
	rf := &refusal{marked: true}
	raw, err = c.readShadow(ctx, ws, keptEntity(rt), key)
	if err != nil {
		return nil, err
	}
	rf.kept, err = decodeKept(raw)
	return rf, err
}

// handOverRefused gives Records.Overwrite the content of a refused record that
// egress is about to replace with next (nil for a delete): the content the
// connector kept when it was replaced after the record's last ingest, and the
// content cur the store holds.
func (c *Connector) handOverRefused(ctx context.Context, ws string, rt *recordType, key string, sh *recShadow, cur *Record, next []byte, rf *refusal) error {
	if rf == nil || !rf.marked {
		return nil
	}
	if k := rf.kept; k != nil && (sh == nil || k.Ingested >= sh.Ingested) && (cur == nil || !bytes.Equal(k.Content, cur.Content)) {
		var now []byte
		if cur != nil {
			now = cur.Content
		}
		if err := c.overwrite(ctx, ws, rt, key, k.Locator, OverwriteRefused, k.Content, now); err != nil {
			return err
		}
	}
	if cur != nil && sh != nil && !bytes.Equal(cur.Content, next) {
		return c.overwrite(ctx, ws, rt, key, sh.Locator, OverwriteRefused, cur.Content, next)
	}
	return nil
}

// afterReplace updates the refusal state of a record in the transaction that
// records an egress write or delete over cur: a mark was used and goes with
// the kept content, and else cur is kept when the record had an ingest within
// Records.KeepReplaced.
func (c *Connector) afterReplace(tx *server.EntityTx, rt *recordType, key string, sh *recShadow, cur *Record, rf *refusal) error {
	if rt.direction != Bidirectional {
		return nil
	}
	if rf != nil && rf.marked {
		if err := tx.DeleteConsumerShadow(c.consumer, refusedEntity(rt), key); err != nil {
			return err
		}
		return tx.DeleteConsumerShadow(c.consumer, keptEntity(rt), key)
	}
	if c.rec.keep <= 0 || sh == nil || cur == nil || sh.Ingested == 0 {
		return nil
	}
	now := c.now()
	if now.Sub(time.Unix(0, sh.Ingested)) >= c.rec.keep {
		return nil
	}
	return tx.PutConsumerShadow(c.consumer, keptEntity(rt), key, encodeJSON(&keptContent{
		Locator: sh.Locator, Content: cur.Content, At: now.UnixNano(), Ingested: sh.Ingested, Members: sh.Members,
	}))
}

// settleRefused handles the refused marks egress did not use: a marked record
// with no dirty mark was written before the mark came, or its render held
// nothing new. Its kept content, if any, goes to Records.Overwrite, and the
// mark goes. A mark newer than the drained position pos waits, because the
// drain has not seen its revert yet. It also drops kept content older than
// Records.KeepReplaced.
func (c *Connector) settleRefused(ctx context.Context, ws string, p *pass, pos uint64) error {
	now := c.now()
	for _, rt := range c.rec.types {
		if rt.direction != Bidirectional {
			continue
		}
		var stale []string
		if err := c.srv.ScanConsumerShadows(ctx, ws, c.consumer, keptEntity(rt), func(key string, raw []byte) error {
			k, err := decodeKept(raw)
			if err != nil {
				return err
			}
			if now.Sub(time.Unix(0, k.At)) >= max(c.rec.keep, 0) {
				stale = append(stale, key)
			}
			return nil
		}); err != nil {
			return err
		}
		marks := make(map[string]refusedMark)
		if err := c.srv.ScanConsumerShadows(ctx, ws, c.consumer, refusedEntity(rt), func(key string, raw []byte) error {
			var m refusedMark
			if err := json.Unmarshal(raw, &m); err != nil {
				return fmt.Errorf("connect: decode refused mark: %w", err)
			}
			marks[key] = m
			return nil
		}); err != nil {
			return err
		}
		for _, key := range slices.Sorted(maps.Keys(marks)) {
			if marks[key].Head > pos {
				continue
			}
			if err := c.settleRefusedOne(ctx, ws, p, rt, key); err != nil {
				return err
			}
			stale = slices.DeleteFunc(stale, func(k string) bool { return k == key })
		}
		if len(stale) > 0 {
			if err := c.shadowMutate(ctx, ws, func(tx *server.EntityTx) error {
				for _, key := range stale {
					if err := tx.DeleteConsumerShadow(c.consumer, keptEntity(rt), key); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Connector) settleRefusedOne(ctx context.Context, ws string, p *pass, rt *recordType, key string) error {
	dirty, err := c.readShadow(ctx, ws, dirtyEntity(rt), key)
	if err != nil || dirty != nil {
		// Egress uses the mark.
		return err
	}
	raw, err := c.readShadow(ctx, ws, keptEntity(rt), key)
	if err != nil {
		return err
	}
	k, err := decodeKept(raw)
	if err != nil {
		return err
	}
	if k != nil {
		var now []byte
		rec, err := p.store(rt).Read(ctx, ws, k.Locator)
		switch {
		case err == nil:
			now = rec.Content
		case !errors.Is(err, ErrRecordNotFound):
			return fmt.Errorf("connect: read %s at %q: %w", rt.rootName, k.Locator, err)
		}
		if err := c.overwrite(ctx, ws, rt, key, k.Locator, OverwriteRefused, k.Content, now); err != nil {
			return err
		}
	} else {
		c.logger.LogAttrs(ctx, slog.LevelWarn, "refused record was written before the refusal came, and no content was kept",
			slog.String("workspace", ws), slog.String("entity", rt.rootName), slog.String("key", key))
	}
	return c.shadowMutate(ctx, ws, func(tx *server.EntityTx) error {
		if err := tx.DeleteConsumerShadow(c.consumer, refusedEntity(rt), key); err != nil {
			return err
		}
		return tx.DeleteConsumerShadow(c.consumer, keptEntity(rt), key)
	})
}
