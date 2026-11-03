package upstream

import (
	"context"
	"fmt"
	"iter"

	"google.golang.org/protobuf/proto"

	"github.com/levelfourab/syncplane/engine/client"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
)

// RejectedError reports a local write the origin rejected. Await and
// Receipt.Wait return it.
type RejectedError struct {
	Rejection
}

func (e *RejectedError) Error() string {
	return fmt.Sprintf("upstream: workspace %q: write rejected by the origin: %s: %s", e.Workspace, e.Code, e.Message)
}

// Workspace is the Go app's handle on one workspace of the local engine. Its
// writes go through the local engine's write path, so the link captures and
// forwards them like any other local write; its reads see the local rows,
// pending writes included. Handles are cheap and safe for concurrent use.
//
// Writes and reads are authorized against ctx as server.Apply and
// server.View authorize them; mark ctx with server.Trusted for system work.
// A write's mode follows Config.WriteMode unless ctx was made with
// WriteThrough or WriteBehind.
type Workspace struct {
	l  *Link
	id string
}

// Workspace returns a handle on a workspace of the local engine. The id is
// not checked until a call uses it.
func (l *Link) Workspace(id string) *Workspace { return &Workspace{l: l, id: id} }

// ID returns the workspace id.
func (w *Workspace) ID() string { return w.id }

// Receipt identifies one local write for awaiting the origin's verdict.
type Receipt struct {
	Workspace string
	// Seq is the local sequence the write took, 0 when it wrote no row.
	Seq uint64
	// Forwarded reports that the write was queued for the origin. A write
	// to local-only entities is not.
	Forwarded bool

	l   *Link
	ref uint64
}

// Wait blocks until the origin has decided the write. It returns nil when the
// origin applied it or when nothing was forwarded, and a *RejectedError when
// the origin rejected it. Before a verdict it returns ErrRevoked once the
// origin revoked the workspace, ErrNotLinked when the link does not sync the
// workspace (any more), ErrClosed when the link closed, ErrUnknownWrite when
// the link holds no record of the write (one decided before the process
// started, or long enough ago to be forgotten), and ctx's error when ctx ends.
func (r Receipt) Wait(ctx context.Context) error {
	if !r.Forwarded || r.l == nil {
		return nil
	}
	w := r.l.workspace(r.Workspace)
	if w == nil {
		return ErrNotLinked
	}
	return w.await(ctx, func() (uint64, bool) { return r.ref, true })
}

// Apply runs fn as server.Apply does and returns the receipt of the writes
// it recorded.
func (w *Workspace) Apply(ctx context.Context, fn func(tx *server.ApplyTx) error) (Receipt, error) {
	b, err := w.l.boundServer()
	if err != nil {
		return Receipt{}, err
	}
	rc := &receipt{}
	if err := b.srv.Apply(context.WithValue(ctx, receiptKey{}, rc), w.id, fn); err != nil {
		return Receipt{}, err
	}
	return w.receipt(rc), nil
}

// Put writes msg as a whole row, as server.ApplyTx.Put does.
func (w *Workspace) Put(ctx context.Context, msg proto.Message, opts ...server.WriteOption) (Receipt, error) {
	return w.Apply(ctx, func(tx *server.ApplyTx) error { return tx.Put(msg, opts...) })
}

// Patch merges the fields mask names from msg onto the stored row, as
// server.ApplyTx.Patch does. A non-nil expected makes it a compare-and-swap
// against the row's local version.
func (w *Workspace) Patch(ctx context.Context, msg proto.Message, mask []string, expected *uint64, opts ...server.WriteOption) (Receipt, error) {
	return w.Apply(ctx, func(tx *server.ApplyTx) error { return tx.Patch(msg, mask, expected, opts...) })
}

// Delete deletes the row of prototype's entity with the given key.
func (w *Workspace) Delete(ctx context.Context, prototype proto.Message, key string) (Receipt, error) {
	return w.Apply(ctx, func(tx *server.ApplyTx) error { return tx.Delete(prototype, key) })
}

// Invoke runs a custom mutation, as server.Invoke does. A mutator the local
// engine declares runs locally as a prediction that the origin's result
// replaces; one it does not declare is forwarded and writes nothing locally.
func (w *Workspace) Invoke(ctx context.Context, name string, args proto.Message) (Receipt, error) {
	b, err := w.l.boundServer()
	if err != nil {
		return Receipt{}, err
	}
	rc := &receipt{}
	if err := b.srv.Invoke(context.WithValue(ctx, receiptKey{}, rc), w.id, name, args); err != nil {
		return Receipt{}, err
	}
	return w.receipt(rc), nil
}

func (w *Workspace) receipt(rc *receipt) Receipt {
	return Receipt{Workspace: w.id, Seq: rc.localSeq, Forwarded: rc.queued, l: w.l, ref: rc.ref}
}

// Flush is Link.Flush for the workspace.
func (w *Workspace) Flush(ctx context.Context) error { return w.l.Flush(ctx, w.id) }

// Row is one row of the local engine, decoded.
type Row[T proto.Message] struct {
	Key string
	// Version is the local sequence that last wrote the row.
	Version uint64
	Value   T
}

// Get reads the row of T's entity that key names from the local engine. It
// reports false when the row is absent, deleted, or hidden from ctx's caller.
// T must be a pointer to a generated message type.
func Get[T proto.Message](ctx context.Context, w *Workspace, key string) (Row[T], bool, error) {
	b, err := w.l.boundServer()
	if err != nil {
		return Row[T]{}, false, err
	}
	var zero T
	msg := zero.ProtoReflect().New().Interface().(T)
	var (
		found   bool
		version uint64
	)
	err = b.srv.View(ctx, w.id, func(tx *server.ViewTx) error {
		var err error
		found, version, err = tx.Get(msg, key)
		return err
	})
	if err != nil || !found {
		return Row[T]{}, false, err
	}
	return Row[T]{Key: key, Version: version, Value: msg}, true, nil
}

// Query returns the rows of T's entity in the local engine that spec
// matches, in spec's order. Build spec with the engine/query constructors.
// T must be a pointer to a generated message type.
//
// The whole result is decoded into the returned slice in one read; bound it
// with spec.Limit, and page with spec.Offset. Without a secondary index that
// covers the predicate and order, the read scans the whole entity whatever
// the limit.
func Query[T proto.Message](ctx context.Context, w *Workspace, spec query.Spec) ([]Row[T], error) {
	b, err := w.l.boundServer()
	if err != nil {
		return nil, err
	}
	var zero T
	prototype := zero.ProtoReflect().New().Interface()
	var out []Row[T]
	err = b.srv.View(ctx, w.id, func(tx *server.ViewTx) error {
		return tx.Query(prototype, spec, func(msg proto.Message, version uint64) error {
			_, key, err := client.EntityKey(msg)
			if err != nil {
				return err
			}
			out = append(out, Row[T]{Key: key, Version: version, Value: msg.(T)})
			return nil
		})
	})
	return out, err
}

// ChangePage is one page of Workspace.Watch.
type ChangePage struct {
	Changes []server.Change
	// Cursor is the feed position after Changes, with the store lineage the
	// feed stamped on it. Pass it to Watch to resume after them.
	Cursor server.Cursor
}

// Watch yields the workspace's committed changes after from, page by page
// as they commit, origin changes and local writes alike. A nil from starts
// at the current head. Iteration ends when ctx ends, without an error, or
// after the first error; server.ErrCursorTooOld reports a from the change
// feed can no longer serve, from another store lineage included. Each change
// carries the latest state of its row, so changes to one row between two
// pages arrive as one.
func (w *Workspace) Watch(ctx context.Context, from *server.Cursor) iter.Seq2[ChangePage, error] {
	return func(yield func(ChangePage, error) bool) {
		b, err := w.l.boundServer()
		if err != nil {
			yield(ChangePage{}, err)
			return
		}
		signal, cancel := b.srv.SubscribeChanges(w.id)
		defer cancel()
		var cur server.Cursor
		if from != nil {
			cur = *from
		} else {
			seq, err := b.srv.CurrentSeq(ctx, w.id)
			if err != nil {
				yield(ChangePage{}, err)
				return
			}
			cur = server.Cursor{Seq: seq}
		}
		for {
			var page []server.Change
			next, more, err := b.srv.Changes(ctx, w.id, cur, 0, func(c server.Change) error {
				page = append(page, c)
				return nil
			})
			if err != nil {
				if ctx.Err() == nil {
					yield(ChangePage{}, err)
				}
				return
			}
			cur = next
			if len(page) > 0 && !yield(ChangePage{Changes: page, Cursor: next}, nil) {
				return
			}
			if more {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case <-signal:
			}
		}
	}
}

// Flush blocks until a linked workspace has no write the origin has not
// confirmed, every rejection was handed to Config.OnRejected, and the
// workspace is caught up with the origin. It may be called from a callback.
// It returns ErrNotLinked for a workspace the link does not sync, the
// status's LastError for a StateFailed one (ErrNotLinked when its link state
// was deleted), ErrRevoked once the origin revoked it, and ctx's error when
// ctx ends first. While SkewBlock holds local writes, Flush waits for the
// block to lift.
func (l *Link) Flush(ctx context.Context, workspaceID string) error {
	w := l.workspace(workspaceID)
	if w == nil {
		return ErrNotLinked
	}
	if err := w.refresh(ctx); err != nil {
		return err
	}
	// A round of the worker compares the schema revisions again, which is
	// how a block lifts after the local engine registered a new schema.
	w.kick()
	return w.waitView(ctx, func() (bool, error) {
		switch w.st.state() {
		case StateFailed:
			return false, w.st.failErr
		case StateRevoked:
			return false, ErrRevoked
		case StateLive:
			return len(w.pend) == 0 && w.allReported(), nil
		}
		return false, nil
	})
}

// Await blocks until the origin has decided the local write that took local
// sequence seq. It returns nil when the origin applied it, and a
// *RejectedError when the origin rejected it. Before a verdict it returns
// ErrUnknownWrite when the link holds no record of a forwarded write at seq,
// ErrRevoked once the origin revoked the workspace, ErrNotLinked when the
// link does not sync the workspace (any more), ErrClosed when the link
// closed, and ctx's error when ctx ends.
func (l *Link) Await(ctx context.Context, workspaceID string, seq uint64) error {
	w := l.workspace(workspaceID)
	if w == nil {
		return ErrNotLinked
	}
	if seq == 0 {
		return ErrUnknownWrite
	}
	return w.await(ctx, func() (uint64, bool) {
		ref, ok := w.bySeq[seq]
		return ref, ok
	})
}

// await waits for the verdict on the entry lookup names. lookup runs under
// vmu.
func (w *workspace) await(ctx context.Context, lookup func() (uint64, bool)) error {
	refreshed := false
	for {
		var verdict error
		known, decided := false, false
		w.vmu.Lock()
		if ref, ok := lookup(); ok {
			if v, ok := w.verdicts[ref]; ok {
				verdict, decided = v, true
			}
			_, known = w.pend[ref]
			known = known || decided
		}
		revoked, failed := w.st.revoked, w.st.failed
		ch := w.changed
		w.vmu.Unlock()
		switch {
		case decided:
			return verdict
		case revoked:
			return ErrRevoked
		case failed:
			return ErrNotLinked
		case !known && !refreshed:
			if err := w.refresh(ctx); err != nil {
				return err
			}
			refreshed = true
			continue
		case !known:
			return ErrUnknownWrite
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		case <-w.done:
			w.vmu.Lock()
			revoked := w.st.revoked
			w.vmu.Unlock()
			if revoked {
				return ErrRevoked
			}
			return ErrClosed
		}
	}
}

// refresh brings the pending view up to date with the stored queue.
func (w *workspace) refresh(ctx context.Context) error {
	_, err := w.txn(ctx, func(*txn) error { return nil })
	return err
}

func (l *Link) boundServer() (*bound, error) {
	b := l.bind.Load()
	if b == nil {
		return nil, ErrNotStarted
	}
	return b, nil
}
