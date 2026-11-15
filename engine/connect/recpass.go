package connect

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/levelfourab/syncplane/engine/server"
)

// passSteps names the steps of one record-mode pass. Every pass also runs
// the first pass when the workspace has none, and deletes the rows of the
// records whose delete grace is over.
type passSteps struct {
	// list lists every record of every type (Reconcile).
	list bool
	// touched ingests the locators Touch reported (Ingress).
	touched bool
	// drain reads the feed and writes the records whose quiet period is over
	// (Drain); flushAll also writes those whose quiet period is not over
	// (Flush).
	drain    bool
	flushAll bool
}

// recordsPass runs one record-mode pass of a workspace.
//
// Ingest runs before the drain, so a record that changed on both sides is
// merged before it is rendered.
func (c *Connector) recordsPass(ctx context.Context, ws string, p *pass, steps passSteps) error {
	if err := c.settlePending(ctx, ws); err != nil {
		return err
	}
	pos, ok, err := c.srv.ConsumerPosition(ctx, ws, c.consumer)
	if err != nil {
		return err
	}
	if !ok {
		if steps.flushAll {
			// Nothing waits for egress before the first pass, and a flush
			// on Close or Forget must not import or rebuild a workspace.
			return nil
		}
		if err := c.firstPass(ctx, ws, p); err != nil {
			return err
		}
		steps.list = false
		if pos, _, err = c.srv.ConsumerPosition(ctx, ws, c.consumer); err != nil {
			return err
		}
	}
	if steps.list {
		for _, rt := range c.rec.types {
			if err := c.reconcileType(ctx, ws, p, rt); err != nil {
				return err
			}
		}
	}
	if steps.touched {
		if err := c.ingestTouched(ctx, ws, p); err != nil {
			return err
		}
	}
	if steps.drain {
		if pos, err = c.drainRecords(ctx, ws, pos); err != nil {
			return err
		}
		if err := c.flushRecords(ctx, ws, p, pos.Seq, steps.flushAll); err != nil {
			return err
		}
		if err := c.settleRefused(ctx, ws, pos.Seq); err != nil {
			return err
		}
	}
	if err := c.applyDeletes(ctx, ws, p); err != nil {
		return err
	}
	return c.planWake(ctx, ws, p)
}

// afterRecordPass hands the wake a pass asked for to the scheduler, and
// remembers whether the workspace has records waiting for egress. A pass that
// failed stopped before it planned, so the plan is made here; when that fails
// too, the workspace counts as unflushed and keeps the wake it had.
func (c *Connector) afterRecordPass(ctx context.Context, w *workspace, p *pass, err error) {
	keepWake := false
	if err != nil {
		p.wake, p.unflushed = time.Time{}, false
		pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bookkeepTimeout)
		if perr := c.planWake(pctx, w.id, p); perr != nil {
			p.unflushed, keepWake = true, true
		}
		cancel()
		// A wake that is due now would retry the failed pass at once when
		// backoff is disabled.
		if floor := c.now().Add(max(c.retryMin, time.Second)); !p.wake.IsZero() && p.wake.Before(floor) {
			p.wake = floor
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !keepWake {
		c.setWakeLocked(w, p.wake)
	}
	if p.unflushed {
		c.unflushed[w.id] = struct{}{}
	} else {
		delete(c.unflushed, w.id)
	}
}

// planWake finds when the workspace next has work: the end of the earliest
// quiet period or delete grace. A dirty record with a missing mark waits for
// the grace, not for its quiet period.
func (c *Connector) planWake(ctx context.Context, ws string, p *pass) error {
	var next time.Time
	earlier := func(t time.Time) {
		if next.IsZero() || t.Before(next) {
			next = t
		}
	}
	for _, rt := range c.rec.types {
		gone, err := c.loadGone(ctx, ws, rt)
		if err != nil {
			return err
		}
		for _, g := range gone {
			earlier(time.Unix(0, g.Since).Add(max(c.rec.grace, 0)))
		}
		if err := c.srv.ScanConsumerShadows(ctx, ws, c.consumer, dirtyEntity(rt), func(key string, raw []byte) error {
			if gone[key] != nil {
				return nil
			}
			m, err := decodeDirty(raw)
			if err != nil {
				return err
			}
			p.unflushed = true
			earlier(c.rec.due(m))
			return nil
		}); err != nil {
			return err
		}
	}
	p.wake = next
	return nil
}

// drainRecords reads the feed from pos and marks each record a change
// reaches, unless the record's agreed content already reflects the change.
// The marks of a page commit before the position that passes it.
//
// A change maps to its record by the root key, by the RecordField of a
// member row, and by the member index for the row's earlier record, which
// is the only way for a deleted row, whose change has no message.
func (c *Connector) drainRecords(ctx context.Context, ws string, pos server.Cursor) (server.Cursor, error) {
	type recID struct {
		rt  *recordType
		key string
	}
	for {
		var page []server.Change
		next, more, err := c.srv.Changes(ctx, ws, pos, c.batch, func(ch server.Change) error {
			if rt, ok := c.rec.byEntity[ch.Entity]; ok && rt.direction != IngressOnly {
				page = append(page, ch)
			}
			return nil
		})
		if errors.Is(err, server.ErrCursorTooOld) {
			// The feed cannot say what changed: every record is a candidate.
			// Egress renders each and writes only those that differ.
			c.logger.LogAttrs(ctx, slog.LevelWarn, "feed cursor cannot be resumed; marking every record for egress",
				slog.String("workspace", ws), slog.Uint64("resume_seq", next.Seq))
			marks, err := c.markAll(ctx, ws)
			if err != nil {
				return pos, err
			}
			if done, err := c.advance(ctx, ws, pos, next, marks); err != nil || done {
				return pos, err
			}
			pos = next
			continue
		}
		if err != nil {
			return pos, err
		}
		// The lookups of a page share one read snapshot, and its marks
		// commit with the position that passes it, so a crash before the
		// position commits marks the page again.
		var marks []server.ConsumerShadowWrite
		if len(page) > 0 {
			if err := c.srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
				marks = marks[:0]
				shadows := make(map[recID]*recShadow)
				marked := make(map[recID]bool)
				for _, ch := range page {
					rt := c.rec.byEntity[ch.Entity]
					keys, err := c.changeRecords(tx, rt, ch)
					if err != nil {
						return err
					}
					for _, key := range keys {
						id := recID{rt, key}
						if marked[id] {
							continue
						}
						sh, ok := shadows[id]
						if !ok {
							if sh, err = c.txRecShadow(tx, rt, key); err != nil {
								return err
							}
							shadows[id] = sh
						}
						if sh != nil && ch.Seq <= sh.Version {
							continue
						}
						if sh == nil && ch.Deleted && ch.Entity == rt.rootName {
							// No store record to delete: the store never had it, or
							// the connector removed it with its shadow.
							continue
						}
						marked[id] = true
						mark, err := c.nextDirty(tx, rt, key)
						if err != nil {
							return err
						}
						marks = append(marks, server.ConsumerShadowWrite{Entity: dirtyEntity(rt), Key: key, Snapshot: mark})
					}
				}
				return nil
			}); err != nil {
				return pos, err
			}
		}
		if done, err := c.advance(ctx, ws, pos, next, marks); err != nil || done {
			return pos, err
		}
		pos = next
		if !more {
			return pos, nil
		}
	}
}

// advance commits the feed position next with the dirty marks of the page
// read from pos. done reports that the workspace's history changed since the
// page was read; the next pass meets that and heals.
func (c *Connector) advance(ctx context.Context, ws string, pos, next server.Cursor, marks []server.ConsumerShadowWrite) (done bool, err error) {
	from := server.Cursor{Seq: pos.Seq, Entity: pos.Entity, Key: pos.Key}
	err = c.srv.AdvanceConsumer(ctx, ws, c.consumer, server.ConsumerUpdate{Cursor: next, Shadows: marks, From: &from, Retain: true})
	if errors.Is(err, server.ErrCursorForeign) {
		return true, nil
	}
	return false, err
}

// changeRecords returns the keys of the records one change reaches.
func (c *Connector) changeRecords(tx shadowReader, rt *recordType, ch server.Change) ([]string, error) {
	if ch.Entity == rt.rootName {
		return []string{ch.Key}, nil
	}
	var keys []string
	raw, err := tx.ConsumerShadow(c.consumer, idxEntity(ch.Entity), ch.Key)
	if err != nil {
		return nil, err
	}
	if raw != nil {
		keys = append(keys, string(raw))
	}
	if !ch.Deleted && ch.Message != nil {
		if key := c.rec.memberOf[ch.Entity].recordKeyOf(ch.Message); key != "" && (len(keys) == 0 || keys[0] != key) {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

// markAll returns a dirty mark for every record of every type egress writes:
// each record with a shadow and each root row.
func (c *Connector) markAll(ctx context.Context, ws string) ([]server.ConsumerShadowWrite, error) {
	t := c.now()
	now := encodeDirty(dirtyMark{last: t, first: t})
	var marks []server.ConsumerShadowWrite
	for _, rt := range c.rec.types {
		if rt.direction == IngressOnly {
			continue
		}
		keys := make(map[string]bool)
		if err := c.srv.ScanConsumerShadows(ctx, ws, c.consumer, recEntity(rt), func(key string, _ []byte) error {
			keys[key] = true
			return nil
		}); err != nil {
			return nil, err
		}
		roots, err := c.rootKeys(ctx, ws, rt)
		if err != nil {
			return nil, err
		}
		for _, k := range roots {
			keys[k] = true
		}
		for _, key := range slices.Sorted(maps.Keys(keys)) {
			marks = append(marks, server.ConsumerShadowWrite{Entity: dirtyEntity(rt), Key: key, Snapshot: now})
		}
	}
	return marks, nil
}

// rootKeys returns the key of every root row of a type, in key order.
func (c *Connector) rootKeys(ctx context.Context, ws string, rt *recordType) ([]string, error) {
	var keys []string
	err := c.srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
		after := ""
		for {
			last, more, err := tx.ScanPage(rt.root, after, c.rec.batchRows, func(msg proto.Message, _ uint64) error {
				_, key, err := c.srv.EntityKey(msg)
				keys = append(keys, key)
				return err
			})
			if err != nil || !more {
				return err
			}
			after = last
		}
	})
	return keys, err
}

// flushRecords writes the dirty records whose egress is due (see
// records.due), or every dirty record with all. A record with a missing mark waits: the store
// deleted it, and that delete wins when the grace is over.
func (c *Connector) flushRecords(ctx context.Context, ws string, p *pass, pos uint64, all bool) error {
	now := c.now()
	for _, rt := range c.rec.types {
		if rt.direction == IngressOnly {
			continue
		}
		var due []string
		if err := c.srv.ScanConsumerShadows(ctx, ws, c.consumer, dirtyEntity(rt), func(key string, raw []byte) error {
			m, err := decodeDirty(raw)
			if err != nil {
				return err
			}
			if all || c.rec.quiet < 0 || !now.Before(c.rec.due(m)) {
				due = append(due, key)
			}
			return nil
		}); err != nil {
			return err
		}
		if len(due) == 0 {
			continue
		}
		gone, err := c.loadGone(ctx, ws, rt)
		if err != nil {
			return err
		}
		var taken []string
		for _, key := range due {
			if gone[key] != nil {
				continue
			}
			err := c.egressRecord(ctx, ws, p, rt, key, pos, egressOpts{})
			var lt *locatorTaken
			if errors.As(err, &lt) {
				taken = append(taken, key)
				continue
			}
			if err != nil {
				return err
			}
		}
		// A record can take the locator that a later record of this pass
		// leaves.
		for _, key := range taken {
			if err := c.egressOrDefer(ctx, ws, p, rt, key, pos, egressOpts{}); err != nil {
				return err
			}
		}
	}
	return nil
}

// locatorTaken is the error of an egress whose locator belongs to another
// record, or holds a store record the connector does not know (owner empty).
type locatorTaken struct{ locator, owner string }

func (e *locatorTaken) Error() string {
	if e.owner == "" {
		return fmt.Sprintf("connect: locator %q holds a store record the connector does not know", e.locator)
	}
	return fmt.Sprintf("connect: locator %q belongs to record %q", e.locator, e.owner)
}

// egressOrDefer runs egressRecord, and moves a record whose locator belongs
// to another record out of the next passes, as deferDirty does: its dirty
// mark stays, so the edit is written when the locator is free.
func (c *Connector) egressOrDefer(ctx context.Context, ws string, p *pass, rt *recordType, key string, pos uint64, o egressOpts) error {
	err := c.egressRecord(ctx, ws, p, rt, key, pos, o)
	var lt *locatorTaken
	if !errors.As(err, &lt) {
		return err
	}
	c.noteFailure(ws, RecordFailure{Entity: rt.rootName, Key: key, Err: err})
	c.logger.LogAttrs(ctx, slog.LevelError, "record skipped: its locator is taken",
		slog.String("workspace", ws), slog.String("entity", rt.rootName), slog.String("key", key),
		slog.String("locator", lt.locator), slog.String("owner", lt.owner))
	return c.deferDirty(ctx, ws, rt, key)
}

// egressOpts are the options of one egressRecord call.
type egressOpts struct {
	// rebuild replaces store content that no shadow describes, after a call
	// to Records.Overwrite: the first pass with EngineWins. Without it, such
	// content is ingested as a store record first.
	rebuild bool
	// noIngest leaves a record whose store content moved since the agreed
	// state for a later pass, instead of ingesting it first. A write-back
	// sets it, so that a store that keeps changing cannot recurse.
	noIngest bool
}

// egressRecord writes one record from its rows to the store. pos is the feed
// position the drain reached: the render reflects at least the changes up to
// it.
//
// Before the write it reads the store record. When its fingerprint moved
// since the agreed state, the store has an edit the engine has not seen, and
// the record is ingested instead: the merge keeps both edits, and its
// write-back takes the engine's edit to the store. An EgressOnly record is
// written over the edit instead.
func (c *Connector) egressRecord(ctx context.Context, ws string, p *pass, rt *recordType, key string, pos uint64, o egressOpts) error {
	sh, err := c.loadRecShadow(ctx, ws, rt, key)
	if err != nil {
		return err
	}
	rf, err := c.loadRefusal(ctx, ws, rt, key)
	if err != nil {
		return err
	}
	// cur is the store record at the agreed locator; nil when there is none.
	var cur *Record
	if sh != nil {
		rec, err := rt.store.Read(ctx, ws, sh.Locator)
		switch {
		case errors.Is(err, ErrRecordNotFound) && rt.direction == EgressOnly:
			// Written again below: the engine is the only source of the record.
		case errors.Is(err, ErrRecordNotFound):
			// The dirty mark stays: a record that comes back in the grace
			// gets the engine's edit.
			return c.markGone(ctx, ws, rt, key, nil)
		case err != nil:
			return fmt.Errorf("connect: read %s at %q: %w", rt.rootName, sh.Locator, err)
		default:
			cur = &rec
		}
		if cur != nil && cur.Fingerprint != sh.Fingerprint && rt.direction != EgressOnly {
			rec := *cur
			if o.noIngest {
				return nil
			}
			if rec.Locator == "" {
				rec.Locator = sh.Locator
			}
			j := &ingestJob{rt: rt, key: key, ref: rec.RecordRef, rec: rec, read: true}
			if err := c.ingestJobs(ctx, ws, p, []*ingestJob{j}); err != nil {
				return err
			}
			if j.failed {
				return c.deferDirty(ctx, ws, rt, key)
			}
			return nil
		}
	}

	var (
		del, agreed bool
		out         Rendered
		next        *recShadow
	)
	err = c.srv.Mutate(server.TrustedAs(ctx, c.label), ws, func(tx *server.EntityTx) error {
		del, agreed, next = false, false, nil
		root, members, err := c.loadRows(tx, rt, key)
		if err != nil {
			return err
		}
		if root == nil {
			del = true
			return nil
		}
		writes := tx.Writes()
		r, ok, err := rt.codec.Render(tx, RenderInput{Key: key, Base: sh.agreed(), Root: root, Members: members})
		if err != nil {
			return &codecError{err: fmt.Errorf("connect: render %s %q: %w", rt.rootName, key, err)}
		}
		if !ok {
			del = true
			return nil
		}
		if r.Locator == "" {
			if sh == nil {
				return &codecError{err: fmt.Errorf("connect: render %s %q: a new record needs a locator", rt.rootName, key)}
			}
			r.Locator = sh.Locator
		}
		if tx.Writes() != writes {
			if _, members, err = c.loadRows(tx, rt, key); err != nil {
				return err
			}
		}
		mk, err := c.memberKeys(members)
		if err != nil {
			return err
		}
		out = r
		next = &recShadow{Locator: r.Locator, Hash: contentHash(r.Content), Version: max(pos, tx.Seq()), Payload: r.Payload, Members: mk, Ingested: sh.ingested()}
		if cur != nil && r.Locator == sh.Locator && bytes.Equal(cur.Content, r.Content) {
			agreed = true
			next.Fingerprint = cur.Fingerprint
			return c.putAgreed(tx, rt, key, sh, next, true)
		}
		return nil
	})
	var ce *codecError
	if errors.As(err, &ce) {
		p.refused++
		f := RecordFailure{Entity: rt.rootName, Key: key, Err: err}
		if sh != nil {
			f.Locator = sh.Locator
		}
		c.noteFailure(ws, f)
		c.logger.LogAttrs(ctx, slog.LevelError, "record skipped: codec failed",
			slog.String("workspace", ws), slog.String("entity", rt.rootName),
			slog.String("key", key), slog.Any("err", err))
		return c.deferDirty(ctx, ws, rt, key)
	}
	if err != nil {
		return err
	}
	switch {
	case del:
		return c.deleteRecord(ctx, ws, rt, key, sh, cur, rf)
	case agreed:
		c.clearFailure(ws, rt.rootName, key, "")
		return nil
	}
	return c.writeRecord(ctx, ws, p, rt, key, pos, sh, cur, rf, out, next, o)
}

// overwrite calls Records.Overwrite, when set, before egress replaces old at
// locator with next, nil for a delete.
func (c *Connector) overwrite(ctx context.Context, ws string, rt *recordType, key, locator string, why OverwriteReason, old, next []byte) error {
	if c.rec.overwrite == nil {
		return nil
	}
	if err := c.rec.overwrite(ctx, Overwrite{
		WorkspaceID: ws, Entity: rt.rootName, Key: key, Locator: locator, Reason: why, Old: old, New: next,
	}); err != nil {
		return fmt.Errorf("connect: overwrite %s %q at %q: %w", rt.rootName, key, locator, err)
	}
	return nil
}

// changedInStore reports whether the store record cur holds content other
// than the agreed state sh: an edit the engine does not have.
func changedInStore(sh *recShadow, cur *Record) bool {
	return sh != nil && cur != nil && !bytes.Equal(contentHash(cur.Content), sh.Hash)
}

// writeRecord writes the render out of the record with key to the store, and
// agrees on it as next. sh is the agreed state, nil for a record the store
// does not hold yet, cur the store record at its locator, nil when there is
// none, and rf its refusal state.
//
// A pending write commits before the store write and names what the store
// holds when the write is done, so that a pass that stops between the store
// write and the agreed state is completed by the next one (see
// settlePending) instead of taking the written record for a new one.
func (c *Connector) writeRecord(ctx context.Context, ws string, p *pass, rt *recordType, key string, pos uint64, sh *recShadow, cur *Record, rf *refusal, out Rendered, next *recShadow, o egressOpts) error {
	if err := c.handOverRefused(ctx, ws, rt, key, sh, cur, out.Content, rf); err != nil {
		return err
	}
	pend := &pendingWrite{Next: next}
	write := true
	if sh != nil && out.Locator == sh.Locator {
		if cur != nil {
			pend.Replaces = contentHash(cur.Content)
		}
		// Only an EgressOnly record gets here with a store edit; another is
		// ingested first.
		if changedInStore(sh, cur) {
			if err := c.overwrite(ctx, ws, rt, key, out.Locator, OverwriteEgressOnly, cur.Content, out.Content); err != nil {
				return err
			}
		}
	} else {
		if sh != nil {
			pend.From = sh.Locator
			if changedInStore(sh, cur) {
				if err := c.overwrite(ctx, ws, rt, key, sh.Locator, OverwriteEgressOnly, cur.Content, nil); err != nil {
					return err
				}
			}
		}
		// A locator new to this record: no shadow describes what the store
		// holds there.
		owner, err := c.readShadow(ctx, ws, locEntity(rt), out.Locator)
		if err != nil {
			return err
		}
		if owner != nil && string(owner) != key {
			return &locatorTaken{locator: out.Locator, owner: string(owner)}
		}
		existing, err := rt.store.Read(ctx, ws, out.Locator)
		switch {
		case errors.Is(err, ErrRecordNotFound):
		case err != nil:
			return fmt.Errorf("connect: read %s at %q: %w", rt.rootName, out.Locator, err)
		case bytes.Equal(existing.Content, out.Content):
			write = false
			next.Fingerprint = existing.Fingerprint
		case !o.rebuild && rt.direction != EgressOnly:
			// A store record made since the last list. It is no part of this
			// record, so it is ingested as its own; this record then waits
			// for a free locator. Once is enough: when the ingest did not
			// take the locator, the next list tries again.
			if o.noIngest {
				return &locatorTaken{locator: out.Locator}
			}
			if existing.Locator == "" {
				existing.Locator = out.Locator
			}
			if err := c.ingestUnknown(ctx, ws, p, rt, existing); err != nil {
				return err
			}
			return c.egressRecord(ctx, ws, p, rt, key, pos, egressOpts{noIngest: true})
		default:
			why := OverwriteFirstPass
			if rt.direction == EgressOnly {
				why = OverwriteEgressOnly
			}
			if err := c.overwrite(ctx, ws, rt, key, out.Locator, why, existing.Content, out.Content); err != nil {
				return err
			}
			pend.Replaces = contentHash(existing.Content)
		}
	}
	if err := c.shadowMutate(ctx, ws, func(tx *server.EntityTx) error {
		return tx.PutConsumerShadow(c.consumer, pendEntity(rt), key, encodeJSON(pend))
	}); err != nil {
		return err
	}
	if write {
		fp, err := rt.store.Write(ctx, ws, Record{RecordRef: RecordRef{Key: key, Locator: out.Locator}, Content: out.Content}, pend.From)
		if err != nil {
			// The pending write stays: the store can have written part of
			// it, and the next pass reads what it holds.
			return fmt.Errorf("connect: write %s %q to %q: %w", rt.rootName, key, out.Locator, err)
		}
		next.Fingerprint = fp
	} else if pend.From != "" {
		if err := rt.store.Delete(ctx, ws, pend.From); err != nil {
			return fmt.Errorf("connect: delete %s at %q: %w", rt.rootName, pend.From, err)
		}
	}
	if err := c.bookkeep(ctx, ws, func(tx *server.EntityTx) error {
		if err := c.commitPending(tx, rt, key, sh, next, true); err != nil {
			return err
		}
		return c.afterReplace(tx, rt, key, sh, cur, rf)
	}); err != nil {
		return err
	}
	c.clearFailure(ws, rt.rootName, key, "")
	return nil
}

// deleteRecord removes the record with key from the store and forgets it. sh
// is its agreed state, nil for a record the store never held, and cur the
// store record at its locator, nil when there is none, and rf its refusal
// state.
func (c *Connector) deleteRecord(ctx context.Context, ws string, rt *recordType, key string, sh *recShadow, cur *Record, rf *refusal) error {
	if sh != nil && cur == nil && rt.direction == EgressOnly {
		// The store deleted it too.
		return c.shadowMutate(ctx, ws, func(tx *server.EntityTx) error {
			return c.forgetRecord(tx, rt, key, sh)
		})
	}
	if changedInStore(sh, cur) {
		if err := c.overwrite(ctx, ws, rt, key, sh.Locator, OverwriteEgressOnly, cur.Content, nil); err != nil {
			return err
		}
	}
	if err := c.handOverRefused(ctx, ws, rt, key, sh, cur, nil, rf); err != nil {
		return err
	}
	if sh == nil {
		c.clearFailure(ws, rt.rootName, key, "")
		return c.shadowMutate(ctx, ws, func(tx *server.EntityTx) error {
			return c.forgetRecord(tx, rt, key, nil)
		})
	}
	if err := c.shadowMutate(ctx, ws, func(tx *server.EntityTx) error {
		return tx.PutConsumerShadow(c.consumer, pendEntity(rt), key, encodeJSON(&pendingWrite{Delete: sh.Locator}))
	}); err != nil {
		return err
	}
	if err := rt.store.Delete(ctx, ws, sh.Locator); err != nil {
		return fmt.Errorf("connect: delete %s at %q: %w", rt.rootName, sh.Locator, err)
	}
	if err := c.bookkeep(ctx, ws, func(tx *server.EntityTx) error {
		if err := c.forgetRecord(tx, rt, key, sh); err != nil {
			return err
		}
		return c.afterReplace(tx, rt, key, sh, cur, rf)
	}); err != nil {
		return err
	}
	c.clearFailure(ws, rt.rootName, key, "")
	return nil
}

// bookkeepTimeout bounds the shadow transaction that follows a store write.
const bookkeepTimeout = 10 * time.Second

// bookkeep runs a shadow transaction that records a store write that is
// done. It does not stop when ctx is cancelled: Close and Forget cancel a
// pass, and the write it records cannot be taken back. bookkeepTimeout
// bounds it.
func (c *Connector) bookkeep(ctx context.Context, ws string, fn func(tx *server.EntityTx) error) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bookkeepTimeout)
	defer cancel()
	return c.shadowMutate(ctx, ws, fn)
}

// settlePending completes the pending writes and deletes a pass left behind
// when it stopped between the bookkeeping and the store call, from what the
// store holds now:
//
//   - The written content: the write is done, and is agreed on.
//   - The content the write replaced, or nothing: the write was not done. The
//     record's dirty mark makes a later pass write it.
//   - Other content: the store changed it after the write. It is agreed on
//     with no fingerprint, so that the next ingest merges the store's edit
//     with the written content as base.
//
// For a delete, a missing record completes it and a present one drops it.
// It runs first in each pass, so that no other step meets a written record
// whose agreed state is not saved.
func (c *Connector) settlePending(ctx context.Context, ws string) error {
	for _, rt := range c.rec.types {
		pends := make(map[string]*pendingWrite)
		if err := c.srv.ScanConsumerShadows(ctx, ws, c.consumer, pendEntity(rt), func(key string, raw []byte) error {
			pw, err := decodePending(raw)
			pends[key] = pw
			return err
		}); err != nil {
			return err
		}
		for _, key := range slices.Sorted(maps.Keys(pends)) {
			if err := c.settleOne(ctx, ws, rt, key, pends[key]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Connector) settleOne(ctx context.Context, ws string, rt *recordType, key string, pw *pendingWrite) error {
	sh, err := c.loadRecShadow(ctx, ws, rt, key)
	if err != nil {
		return err
	}
	drop := func(tx *server.EntityTx) error { return tx.DeleteConsumerShadow(c.consumer, pendEntity(rt), key) }
	if pw.Next == nil {
		_, err := rt.store.Read(ctx, ws, pw.Delete)
		switch {
		case errors.Is(err, ErrRecordNotFound):
			return c.shadowMutate(ctx, ws, func(tx *server.EntityTx) error { return c.forgetRecord(tx, rt, key, sh) })
		case err != nil:
			return fmt.Errorf("connect: read %s at %q: %w", rt.rootName, pw.Delete, err)
		}
		return c.shadowMutate(ctx, ws, drop)
	}
	rec, err := rt.store.Read(ctx, ws, pw.Next.Locator)
	if errors.Is(err, ErrRecordNotFound) {
		return c.shadowMutate(ctx, ws, drop)
	}
	if err != nil {
		return fmt.Errorf("connect: read %s at %q: %w", rt.rootName, pw.Next.Locator, err)
	}
	next := *pw.Next
	switch hash := contentHash(rec.Content); {
	case bytes.Equal(hash, next.Hash):
		next.Fingerprint = rec.Fingerprint
	case pw.Replaces != nil && bytes.Equal(hash, pw.Replaces):
		return c.shadowMutate(ctx, ws, drop)
	default:
		next.Fingerprint = ""
	}
	if pw.From != "" && sh != nil {
		// A store that moves a record in two steps can hold the old copy.
		old, err := rt.store.Read(ctx, ws, pw.From)
		switch {
		case err == nil && old.Fingerprint == sh.Fingerprint:
			if err := rt.store.Delete(ctx, ws, pw.From); err != nil {
				return fmt.Errorf("connect: delete %s at %q: %w", rt.rootName, pw.From, err)
			}
		case err != nil && !errors.Is(err, ErrRecordNotFound):
			return fmt.Errorf("connect: read %s at %q: %w", rt.rootName, pw.From, err)
		}
	}
	// The dirty mark stays: a change drained after the render is in it too,
	// and the next egress finds the render agreed when there is none.
	return c.shadowMutate(ctx, ws, func(tx *server.EntityTx) error {
		return c.commitPending(tx, rt, key, sh, &next, false)
	})
}

// deferDirty moves a record the codec refused out of the next passes: its
// dirty mark is due again after the retry delay, not at once, so a codec
// that fails on every attempt does not spin the workspace.
func (c *Connector) deferDirty(ctx context.Context, ws string, rt *recordType, key string) error {
	at := c.now().Add(max(c.retryMin, 0))
	return c.shadowMutate(ctx, ws, func(tx *server.EntityTx) error {
		return tx.PutConsumerShadow(c.consumer, dirtyEntity(rt), key, encodeDirty(dirtyMark{last: at, first: at}))
	})
}

// firstPass runs the first pass of a workspace that has no record-mode
// position, under the policy of firstPolicy, and then records the position
// the feed had before it started. The changes the pass itself wrote are skipped by
// the shadows' versions; client changes made during the pass are drained.
//
// A pass that fails leaves no position, so the next pass runs the first pass
// again. The records it already agreed on have shadows and are skipped. A
// record the codec refuses fails the pass where skipping it would lose data:
// SourceWins would delete the rows it holds, and EngineWins would ingest the
// store record of a root it could not render as a new record.
func (c *Connector) firstPass(ctx context.Context, ws string, p *pass) error {
	policy, err := c.firstPolicy(ctx, ws)
	if err != nil {
		return err
	}
	head, err := c.srv.CurrentSeq(ctx, ws)
	if err != nil {
		return err
	}
	for _, rt := range c.rec.types {
		if rt.direction == EgressOnly {
			if err := c.rebuildRoots(ctx, ws, p, rt, head); err != nil {
				return err
			}
			continue
		}
		var refs []RecordRef
		if err := rt.store.List(ctx, ws, func(ref RecordRef) error {
			refs = append(refs, ref)
			return nil
		}); err != nil {
			return fmt.Errorf("connect: list %s: %w", rt.rootName, err)
		}
		refused := p.refused
		if rt.direction == IngressOnly || policy == SourceWins {
			if err := c.ingestRefs(ctx, ws, p, rt, refs, true); err != nil {
				return err
			}
			if policy == SourceWins {
				// A refused record holds rows the sweep cannot tell from
				// stale ones: the codec could have matched it to any root.
				if n := p.refused - refused; n > 0 {
					return fmt.Errorf("connect: first pass of %s: the codec refused %d records, so no row was deleted", rt.rootName, n)
				}
				if err := c.deleteUnrecorded(ctx, ws, p, rt); err != nil {
					return err
				}
			}
			continue
		}
		if err := c.rebuildRoots(ctx, ws, p, rt, head); err != nil {
			return err
		}
		// The locator of a root the codec could not render is unknown, and
		// ingest would take the store record there for a new one.
		if n := p.refused - refused; n > 0 {
			return fmt.Errorf("connect: first pass of %s: the codec refused %d records, so no store record was ingested", rt.rootName, n)
		}
		// What no root rendered to is new to the engine. The rest is agreed
		// now, and ingest skips it by its fingerprint.
		if err := c.ingestRefs(ctx, ws, p, rt, refs, false); err != nil {
			return err
		}
	}
	// Retained, because the shadows stay valid below the tombstone horizon
	// and drainRecords heals a cursor that is too old. A garbage collection
	// that removed them would run the first pass again, and SourceWins would
	// delete the rows clients created while the connector did not run.
	return c.srv.AdvanceConsumer(ctx, ws, c.consumer, server.ConsumerUpdate{Cursor: server.Cursor{Seq: head}, Retain: true})
}

// rebuildRoots writes each root row of a type that has no record shadow to
// the store, and replaces what the store holds at its locator after a call to
// Records.Overwrite.
func (c *Connector) rebuildRoots(ctx context.Context, ws string, p *pass, rt *recordType, head uint64) error {
	shadows, err := c.loadRecShadows(ctx, ws, rt)
	if err != nil {
		return err
	}
	roots, err := c.rootKeys(ctx, ws, rt)
	if err != nil {
		return err
	}
	for _, key := range roots {
		if shadows[key] != nil {
			continue
		}
		if err := c.egressOrDefer(ctx, ws, p, rt, key, head, egressOpts{rebuild: true}); err != nil {
			return err
		}
	}
	return nil
}

// firstPolicy returns the first-pass policy of the workspace: the one
// Records.ChooseFirstPass gives, else Records.FirstPass.
func (c *Connector) firstPolicy(ctx context.Context, ws string) (FirstPass, error) {
	if c.rec.choose == nil {
		return c.rec.firstPass, nil
	}
	policy, err := c.rec.choose(ctx, ws)
	if err != nil {
		return 0, fmt.Errorf("connect: choose first pass of workspace %q: %w", ws, err)
	}
	if policy != SourceWins && policy != EngineWins {
		return 0, fmt.Errorf("connect: ChooseFirstPass returned unknown FirstPass %d for workspace %q", policy, ws)
	}
	return policy, nil
}

// deleteUnrecorded deletes the root rows with no record shadow, and the
// member rows whose record has none: the rows a SourceWins first pass found
// in no store record.
func (c *Connector) deleteUnrecorded(ctx context.Context, ws string, p *pass, rt *recordType) error {
	shadows, err := c.loadRecShadows(ctx, ws, rt)
	if err != nil {
		return err
	}
	sweep := func(prototype proto.Message, recordOf func(msg proto.Message, key string) string) error {
		after := ""
		for {
			var (
				last string
				more bool
			)
			wrote := false
			if err := c.srv.Mutate(server.TrustedAs(ctx, c.label), ws, func(tx *server.EntityTx) error {
				type row struct {
					msg proto.Message
					key string
				}
				var doomed []row
				var err error
				last, more, err = tx.ScanPage(prototype, after, c.rec.batchRows, func(msg proto.Message) error {
					_, key, err := c.srv.EntityKey(msg)
					if err != nil {
						return err
					}
					if shadows[recordOf(msg, key)] == nil {
						doomed = append(doomed, row{msg, key})
					}
					return nil
				})
				if err != nil {
					return err
				}
				for _, r := range doomed {
					if err := tx.Delete(r.msg, r.key); err != nil {
						return err
					}
				}
				wrote = len(doomed) > 0
				return nil
			}); err != nil {
				return err
			}
			if wrote {
				if err := c.pace(ctx, p); err != nil {
					return err
				}
			}
			if !more {
				return nil
			}
			after = last
		}
	}
	if err := sweep(rt.root, func(_ proto.Message, key string) string { return key }); err != nil {
		return err
	}
	for _, m := range rt.members {
		if err := sweep(m.prototype, func(msg proto.Message, _ string) string { return m.recordKeyOf(msg) }); err != nil {
			return err
		}
	}
	return nil
}
