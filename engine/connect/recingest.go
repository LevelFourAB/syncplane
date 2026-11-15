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

	"github.com/levelfourab/syncplane/engine/id"
	"github.com/levelfourab/syncplane/engine/server"
)

// codecError is a Merge or Render failure. It is about one record, so the
// connector skips that record instead of failing the pass. index is the
// record's place in the batch that failed.
type codecError struct {
	index int
	err   error
}

func (e *codecError) Error() string { return e.err.Error() }
func (e *codecError) Unwrap() error { return e.err }

// ingestJob is one store record on its way into the engine.
type ingestJob struct {
	rt  *recordType
	key string
	ref RecordRef
	// rec is the content, valid when read is set.
	rec  Record
	read bool

	// Results of the merge, reset on each attempt of the transaction.
	outKey string
	agreed *recShadow
	back   *Rendered
	wrote  bool
	// failed is set when the codec refused the record.
	failed bool
}

// knownRecords is a snapshot of the record shadows and the locator index of
// one type, which resolveRef reads instead of the store during a full list.
type knownRecords struct {
	shadows map[string]*recShadow
	byLoc   map[string]string
}

// loadKnown reads the record shadows and the locator index of a type.
func (c *Connector) loadKnown(ctx context.Context, ws string, rt *recordType) (*knownRecords, error) {
	shadows, err := c.loadRecShadows(ctx, ws, rt)
	if err != nil {
		return nil, err
	}
	byLoc := make(map[string]string, len(shadows))
	err = c.srv.ScanConsumerShadows(ctx, ws, c.consumer, locEntity(rt), func(loc string, raw []byte) error {
		byLoc[loc] = string(raw)
		return nil
	})
	return &knownRecords{shadows: shadows, byLoc: byLoc}, err
}

// resolveRef finds the record key of a store record, and returns nil when
// the shadow already agrees with it. gone holds the type's missing-record
// marks: a new locator with the fingerprint of a missing record is a rename,
// and takes that record's key and mark. known, when not nil, is read instead
// of the store.
func (c *Connector) resolveRef(ctx context.Context, ws string, rt *recordType, ref RecordRef, gone map[string]*goneMark, known *knownRecords) (*ingestJob, error) {
	var key string
	if known != nil {
		key = known.byLoc[ref.Locator]
	} else {
		raw, err := c.readShadow(ctx, ws, locEntity(rt), ref.Locator)
		if err != nil {
			return nil, err
		}
		key = string(raw)
	}
	if key == "" {
		key = ref.Key
	}
	if key != "" {
		var sh *recShadow
		if known != nil {
			sh = known.shadows[key]
		} else {
			var err error
			if sh, err = c.loadRecShadow(ctx, ws, rt, key); err != nil {
				return nil, err
			}
		}
		if sh != nil && sh.Locator == ref.Locator && sh.Fingerprint == ref.Fingerprint {
			// Unchanged, or the echo of the connector's own write. A missing
			// mark from an earlier list goes: the record is back.
			if _, ok := gone[key]; ok {
				delete(gone, key)
				if err := c.shadowMutate(ctx, ws, func(tx *server.EntityTx) error {
					return tx.DeleteConsumerShadow(c.consumer, goneEntity(rt), key)
				}); err != nil {
					return nil, err
				}
			}
			return nil, nil
		}
	}
	if key == "" && ref.Fingerprint != "" {
		for _, k := range slices.Sorted(maps.Keys(gone)) {
			if gone[k].Fingerprint == ref.Fingerprint {
				key = k
				delete(gone, k)
				break
			}
		}
	}
	if key == "" && ref.Fingerprint != "" && known == nil {
		var err error
		if key, err = c.renamedFrom(ctx, ws, rt, ref); err != nil {
			return nil, err
		}
	}
	if key == "" {
		key = id.New()
	}
	return &ingestJob{rt: rt, key: key, ref: ref}, nil
}

// renamedFrom returns the key of the record a new locator was renamed from:
// the first record whose agreed fingerprint is the ref's and whose locator
// the store no longer holds. It returns "" when there is none, as for a copy.
// A full list does not need it: it marks the missing records before it
// resolves a new locator.
func (c *Connector) renamedFrom(ctx context.Context, ws string, rt *recordType, ref RecordRef) (string, error) {
	raw, err := c.readShadow(ctx, ws, fpEntity(rt), ref.Fingerprint)
	if err != nil {
		return "", err
	}
	keys, err := decodeKeys(raw)
	if err != nil {
		return "", err
	}
	for _, key := range keys {
		sh, err := c.loadRecShadow(ctx, ws, rt, key)
		if err != nil {
			return "", err
		}
		if sh == nil || sh.Locator == ref.Locator || sh.Fingerprint != ref.Fingerprint {
			continue
		}
		_, err = rt.store.Read(ctx, ws, sh.Locator)
		if errors.Is(err, ErrRecordNotFound) {
			return key, nil
		}
		if err != nil {
			return "", fmt.Errorf("connect: read %s at %q: %w", rt.rootName, sh.Locator, err)
		}
	}
	return "", nil
}

// readJob reads the content of a job, and reports false when there is
// nothing to ingest: the record is gone, or its shadow agrees with it.
func (c *Connector) readJob(ctx context.Context, ws string, j *ingestJob) (bool, error) {
	if !j.read {
		rec, err := j.rt.store.Read(ctx, ws, j.ref.Locator)
		if errors.Is(err, ErrRecordNotFound) {
			// Gone since it was listed; the next list marks it missing.
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("connect: read %s at %q: %w", j.rt.rootName, j.ref.Locator, err)
		}
		if rec.Locator == "" {
			rec.Locator = j.ref.Locator
		}
		j.rec, j.read = rec, true
	}
	return true, nil
}

// ingestJobs merges store records into the engine, several small records in
// one transaction (Records.BatchRows). When the codec refuses a record of a
// batch, the records before it and the refused one are merged one by one, so
// the refused one is skipped alone.
func (c *Connector) ingestJobs(ctx context.Context, ws string, p *pass, jobs []*ingestJob) error {
	window := min(c.rec.batchRows, 256)
	for len(jobs) > 0 {
		n := min(window, len(jobs))
		batch := make([]*ingestJob, 0, n)
		for _, j := range jobs[:n] {
			ok, err := c.readJob(ctx, ws, j)
			if err != nil {
				return err
			}
			if ok {
				batch = append(batch, j)
			}
		}
		jobs = jobs[n:]
		for len(batch) > 0 {
			done, err := c.mergeBatch(ctx, ws, batch)
			var ce *codecError
			if errors.As(err, &ce) {
				upto := ce.index + 1
				if len(batch) == 1 {
					c.skipRecord(ctx, ws, p, batch[0], err)
				} else if err := c.ingestEach(ctx, ws, p, batch[:upto]); err != nil {
					return err
				}
				batch = batch[upto:]
				continue
			}
			if err != nil {
				return err
			}
			if err := c.afterMerge(ctx, ws, p, batch[:done]); err != nil {
				return err
			}
			batch = batch[done:]
		}
	}
	return nil
}

// ingestEach merges jobs one transaction each, skipping those the codec
// refuses.
func (c *Connector) ingestEach(ctx context.Context, ws string, p *pass, jobs []*ingestJob) error {
	for _, j := range jobs {
		one := []*ingestJob{j}
		_, err := c.mergeBatch(ctx, ws, one)
		var ce *codecError
		if errors.As(err, &ce) {
			c.skipRecord(ctx, ws, p, j, err)
			continue
		}
		if err != nil {
			return err
		}
		if err := c.afterMerge(ctx, ws, p, one); err != nil {
			return err
		}
	}
	return nil
}

func (c *Connector) skipRecord(ctx context.Context, ws string, p *pass, j *ingestJob, err error) {
	j.failed = true
	p.refused++
	c.logger.LogAttrs(ctx, slog.LevelError, "record skipped: codec failed",
		slog.String("workspace", ws), slog.String("entity", j.rt.rootName),
		slog.String("key", j.key), slog.String("locator", j.ref.Locator), slog.Any("err", err))
}

// mergeBatch merges a prefix of batch in one transaction, and returns its
// length: it stops after the record that brings the rows past BatchRows.
func (c *Connector) mergeBatch(ctx context.Context, ws string, batch []*ingestJob) (int, error) {
	var done int
	err := c.srv.Mutate(server.TrustedAs(ctx, c.label), ws, func(tx *server.EntityTx) error {
		done = 0
		rows := 0
		for i, j := range batch {
			n, err := c.mergeOne(tx, j)
			if err != nil {
				var ce *codecError
				if errors.As(err, &ce) {
					ce.index = i
				}
				return err
			}
			done = i + 1
			rows += n
			if rows >= c.rec.batchRows {
				break
			}
		}
		return nil
	})
	return done, err
}

// mergeOne merges one record in tx and records its new agreed state with the
// rows, so a crash cannot separate the two. It returns the record's row
// count.
//
// A bidirectional record is rendered right after the merge. When the render
// differs from the store's content, the merge kept engine edits, and the
// record must be written back: the shadow then still describes the store's
// content, and a dirty mark in the same transaction makes sure a crash before
// the write-back does not lose those edits.
func (c *Connector) mergeOne(tx *server.EntityTx, j *ingestJob) (int, error) {
	rt := j.rt
	j.outKey, j.agreed, j.back, j.wrote = "", nil, nil, false
	key := j.key
	sh, err := c.txRecShadow(tx, rt, key)
	if err != nil {
		return 0, err
	}
	if sh != nil && sh.Locator == j.rec.Locator && sh.Fingerprint == j.rec.Fingerprint {
		return 0, nil
	}
	root, members, err := c.loadRows(tx, rt, key)
	if err != nil {
		return 0, err
	}
	merged, err := rt.codec.Merge(tx, MergeInput{
		Key: key, Record: j.rec, Base: sh.agreed(), Root: root, Members: members, TextPeer: c.peer,
	})
	if err != nil {
		return 0, &codecError{err: fmt.Errorf("connect: merge %s %q: %w", rt.rootName, key, err)}
	}
	if merged.Key != "" && merged.Key != key {
		if sh != nil {
			return 0, &codecError{err: fmt.Errorf("connect: merge %s %q: a known record cannot change its key to %q", rt.rootName, key, merged.Key)}
		}
		other, err := c.txRecShadow(tx, rt, merged.Key)
		if err != nil {
			return 0, err
		}
		if other != nil && other.Locator != j.rec.Locator {
			return 0, &codecError{err: fmt.Errorf("connect: merge %s at %q: record %q is at %q", rt.rootName, j.rec.Locator, merged.Key, other.Locator)}
		}
		key, sh = merged.Key, other
	}
	if root, members, err = c.loadRows(tx, rt, key); err != nil {
		return 0, err
	}
	next := &recShadow{
		Locator:     j.rec.Locator,
		Fingerprint: j.rec.Fingerprint,
		Hash:        contentHash(j.rec.Content),
		Payload:     merged.Payload,
	}
	clearDirty, dirty := false, false
	if rt.direction == Bidirectional {
		if root == nil {
			// No root after the merge: the record has no native form, and
			// egress removes it from the store.
			dirty = true
		} else {
			out, ok, err := rt.codec.Render(tx, RenderInput{Key: key, Base: next.agreed(), Root: root, Members: members})
			if err != nil {
				return 0, &codecError{err: fmt.Errorf("connect: render %s %q: %w", rt.rootName, key, err)}
			}
			switch {
			case !ok:
				dirty = true
			case (out.Locator != "" && out.Locator != j.rec.Locator) || !bytes.Equal(out.Content, j.rec.Content):
				if out.Locator == "" {
					out.Locator = j.rec.Locator
				}
				j.back = &out
				dirty = true
			default:
				next.Payload = out.Payload
				clearDirty = true
			}
			// Render can write rows.
			if _, members, err = c.loadRows(tx, rt, key); err != nil {
				return 0, err
			}
		}
	}
	if next.Members, err = c.memberKeys(members); err != nil {
		return 0, err
	}
	next.Version = tx.Seq()
	if next.Version == 0 && sh != nil {
		next.Version = sh.Version
	}
	if err := c.putAgreed(tx, rt, key, sh, next, clearDirty); err != nil {
		return 0, err
	}
	if dirty {
		if err := c.markDirty(tx, rt, key); err != nil {
			return 0, err
		}
	}
	j.outKey, j.agreed, j.wrote = key, next, tx.Seq() != 0
	return 1 + len(members), nil
}

// afterMerge paces the import and writes back the records whose merge kept
// engine edits.
func (c *Connector) afterMerge(ctx context.Context, ws string, p *pass, jobs []*ingestJob) error {
	for _, j := range jobs {
		if j.wrote {
			if err := c.pace(ctx, p); err != nil {
				return err
			}
			break
		}
	}
	for _, j := range jobs {
		if j.back == nil || j.agreed == nil {
			continue
		}
		if err := c.egressOrDefer(ctx, ws, p, j.rt, j.outKey, j.agreed.Version, egressOpts{noIngest: true}); err != nil {
			return fmt.Errorf("connect: write back: %w", err)
		}
	}
	return nil
}

// ingestRefs ingests the listed records of one type. With markMissing, refs
// is the whole store, and each record with a shadow that refs does not hold
// gets a missing mark first, so that a new locator in refs can be found as
// its rename.
func (c *Connector) ingestRefs(ctx context.Context, ws string, p *pass, rt *recordType, refs []RecordRef, markMissing bool) error {
	gone, err := c.loadGone(ctx, ws, rt)
	if err != nil {
		return err
	}
	known, err := c.loadKnown(ctx, ws, rt)
	if err != nil {
		return err
	}
	if markMissing {
		locs := make(map[string]bool, len(refs))
		keys := make(map[string]bool)
		for _, ref := range refs {
			locs[ref.Locator] = true
			if ref.Key != "" {
				keys[ref.Key] = true
			}
		}
		now := c.now()
		var marks []string
		for _, key := range slices.Sorted(maps.Keys(known.shadows)) {
			sh := known.shadows[key]
			if locs[sh.Locator] || keys[key] || gone[key] != nil {
				continue
			}
			gone[key] = &goneMark{Locator: sh.Locator, Fingerprint: sh.Fingerprint, Since: now.UnixNano()}
			marks = append(marks, key)
		}
		if len(marks) > 0 {
			if err := c.shadowMutate(ctx, ws, func(tx *server.EntityTx) error {
				for _, key := range marks {
					if err := tx.PutConsumerShadow(c.consumer, goneEntity(rt), key, encodeJSON(gone[key])); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				return err
			}
		}
	}
	var jobs []*ingestJob
	// A new record listed twice would get two keys.
	seen := make(map[string]bool, len(refs))
	for _, ref := range refs {
		if seen[ref.Locator] {
			continue
		}
		seen[ref.Locator] = true
		j, err := c.resolveRef(ctx, ws, rt, ref, gone, known)
		if err != nil {
			return err
		}
		if j != nil {
			jobs = append(jobs, j)
		}
	}
	return c.ingestJobs(ctx, ws, p, jobs)
}

// reconcileType lists every record of one type and ingests what changed.
func (c *Connector) reconcileType(ctx context.Context, ws string, p *pass, rt *recordType) error {
	var refs []RecordRef
	if err := rt.store.List(ctx, ws, func(ref RecordRef) error {
		refs = append(refs, ref)
		return nil
	}); err != nil {
		return fmt.Errorf("connect: list %s: %w", rt.rootName, err)
	}
	return c.ingestRefs(ctx, ws, p, rt, refs, true)
}

// ingestTouched ingests the locators Touch reported. They go back to the
// workspace when the pass fails, so the retry ingests them.
func (c *Connector) ingestTouched(ctx context.Context, ws string, p *pass) error {
	c.mu.Lock()
	var locs []string
	if w, ok := c.tracked[ws]; ok && len(w.touched) > 0 {
		locs = slices.Sorted(maps.Keys(w.touched))
		w.touched = nil
	}
	c.mu.Unlock()
	if len(locs) == 0 {
		return nil
	}
	err := c.ingestLocators(ctx, ws, p, locs)
	if err != nil {
		c.mu.Lock()
		// A workspace that Forget dropped meanwhile stays dropped.
		if w, ok := c.tracked[ws]; ok {
			if w.touched == nil {
				w.touched = make(map[string]struct{}, len(locs))
			}
			for _, l := range locs {
				w.touched[l] = struct{}{}
			}
		}
		c.mu.Unlock()
	}
	return err
}

// ingestLocators ingests the records at locators. A locator the connector
// knows belongs to its record's type; another goes to the first type whose
// store holds it. A known locator whose record is missing gets a missing
// mark first, so that a touched rename finds its record.
func (c *Connector) ingestLocators(ctx context.Context, ws string, p *pass, locs []string) error {
	type found struct {
		rt  *recordType
		rec Record
	}
	var present []found
	gone := make(map[*recordType]map[string]*goneMark)
	goneOf := func(rt *recordType) (map[string]*goneMark, error) {
		if g, ok := gone[rt]; ok {
			return g, nil
		}
		g, err := c.loadGone(ctx, ws, rt)
		gone[rt] = g
		return g, err
	}
	for _, loc := range locs {
		var (
			owner *recordType
			key   string
		)
		for _, rt := range c.rec.types {
			raw, err := c.readShadow(ctx, ws, locEntity(rt), loc)
			if err != nil {
				return err
			}
			if raw != nil {
				owner, key = rt, string(raw)
				break
			}
		}
		candidates := c.rec.types
		if owner != nil {
			candidates = []*recordType{owner}
		}
		var hit bool
		for _, rt := range candidates {
			rec, err := rt.store.Read(ctx, ws, loc)
			if errors.Is(err, ErrRecordNotFound) {
				continue
			}
			if err != nil {
				return fmt.Errorf("connect: read %s at %q: %w", rt.rootName, loc, err)
			}
			if rec.Locator == "" {
				rec.Locator = loc
			}
			present = append(present, found{rt, rec})
			hit = true
			break
		}
		if hit || owner == nil {
			continue
		}
		g, err := goneOf(owner)
		if err != nil {
			return err
		}
		if err := c.markGone(ctx, ws, owner, key, g); err != nil {
			return err
		}
	}
	jobs := make([]*ingestJob, 0, len(present))
	for _, f := range present {
		g, err := goneOf(f.rt)
		if err != nil {
			return err
		}
		j, err := c.resolveRef(ctx, ws, f.rt, f.rec.RecordRef, g, nil)
		if err != nil {
			return err
		}
		if j != nil {
			j.rec, j.read = f.rec, true
			jobs = append(jobs, j)
		}
	}
	return c.ingestJobs(ctx, ws, p, jobs)
}

// ingestUnknown ingests a store record that no locator index entry names.
func (c *Connector) ingestUnknown(ctx context.Context, ws string, p *pass, rt *recordType, rec Record) error {
	gone, err := c.loadGone(ctx, ws, rt)
	if err != nil {
		return err
	}
	j, err := c.resolveRef(ctx, ws, rt, rec.RecordRef, gone, nil)
	if err != nil || j == nil {
		return err
	}
	j.rec, j.read = rec, true
	return c.ingestJobs(ctx, ws, p, []*ingestJob{j})
}

// markGone gives a record that is missing from the store a missing mark,
// unless it has one. gone, when not nil, is the type's marks, kept current.
func (c *Connector) markGone(ctx context.Context, ws string, rt *recordType, key string, gone map[string]*goneMark) error {
	if gone != nil && gone[key] != nil {
		return nil
	}
	var mark *goneMark
	err := c.shadowMutate(ctx, ws, func(tx *server.EntityTx) error {
		mark = nil
		raw, err := tx.ConsumerShadow(c.consumer, goneEntity(rt), key)
		if err != nil || raw != nil {
			return err
		}
		sh, err := c.txRecShadow(tx, rt, key)
		if err != nil || sh == nil {
			return err
		}
		mark = &goneMark{Locator: sh.Locator, Fingerprint: sh.Fingerprint, Since: c.now().UnixNano()}
		return tx.PutConsumerShadow(c.consumer, goneEntity(rt), key, encodeJSON(mark))
	})
	if err == nil && mark != nil && gone != nil {
		gone[key] = mark
	}
	return err
}

// applyDeletes deletes the rows of the records whose delete grace is over
// and that are still missing from the store. A record that is back is
// ingested instead.
func (c *Connector) applyDeletes(ctx context.Context, ws string, p *pass) error {
	now := c.now()
	for _, rt := range c.rec.types {
		gone, err := c.loadGone(ctx, ws, rt)
		if err != nil {
			return err
		}
		for _, key := range slices.Sorted(maps.Keys(gone)) {
			g := gone[key]
			if c.rec.grace >= 0 && now.Before(time.Unix(0, g.Since).Add(c.rec.grace)) {
				continue
			}
			rec, err := rt.store.Read(ctx, ws, g.Locator)
			if err == nil {
				if err := c.shadowMutate(ctx, ws, func(tx *server.EntityTx) error {
					return tx.DeleteConsumerShadow(c.consumer, goneEntity(rt), key)
				}); err != nil {
					return err
				}
				if rec.Locator == "" {
					rec.Locator = g.Locator
				}
				j, err := c.resolveRef(ctx, ws, rt, rec.RecordRef, nil, nil)
				if err != nil {
					return err
				}
				if j != nil {
					j.rec, j.read = rec, true
					if err := c.ingestJobs(ctx, ws, p, []*ingestJob{j}); err != nil {
						return err
					}
				}
				continue
			}
			if !errors.Is(err, ErrRecordNotFound) {
				return fmt.Errorf("connect: read %s at %q: %w", rt.rootName, g.Locator, err)
			}
			if err := c.deleteRecordRows(ctx, ws, rt, key); err != nil {
				return err
			}
			if err := c.pace(ctx, p); err != nil {
				return err
			}
		}
	}
	return nil
}

// deleteRecordRows deletes the root row and the member rows of a record that
// left the store, with its shadows, in one transaction.
func (c *Connector) deleteRecordRows(ctx context.Context, ws string, rt *recordType, key string) error {
	return c.srv.Mutate(server.TrustedAs(ctx, c.label), ws, func(tx *server.EntityTx) error {
		sh, err := c.txRecShadow(tx, rt, key)
		if err != nil {
			return err
		}
		root, members, err := c.loadRows(tx, rt, key)
		if err != nil {
			return err
		}
		for _, msg := range members {
			_, k, err := c.srv.EntityKey(msg)
			if err != nil {
				return err
			}
			if err := tx.Delete(msg, k); err != nil {
				return err
			}
		}
		if root != nil {
			if err := tx.Delete(root, key); err != nil {
				return err
			}
		}
		return c.forgetRecord(tx, rt, key, sh)
	})
}
