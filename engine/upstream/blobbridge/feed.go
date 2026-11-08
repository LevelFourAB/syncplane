package blobbridge

import (
	"context"
	"errors"
	"time"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/levelfourab/syncplane/engine/blob"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/lww"
	"github.com/levelfourab/syncplane/engine/server"
)

// feedPage is how many feed rows one visit to a workspace reads.
const feedPage = 256

// feedVisits is how many workspaces the feed visits before it reads new
// activity again, and how many pending workspaces one sweep step lists.
const feedVisits = 64

// feed follows the local change feed of each workspace that is written, and
// queues a check of each digest that a changed row of a counted entity names.
// Its position in each workspace lives in the local engine, under the
// bridge's consumer name, and moves only after the checks are queued.
//
// Work reaches it two ways: workspace activity, and a sweep that pages
// through the workspaces whose position is behind (which finds what activity
// did not report, such as writes while no Run ran). Each visit reads one page
// of one workspace, so a step of the loop costs at most feedVisits pages.
type feed struct {
	b  *Bridge
	st *runState

	activity <-chan string
	cancel   func()

	// queue holds the workspaces to visit, in order, each once.
	queue  []string
	queued map[string]bool
	// sweepAfter is where the next sweep step continues the listing of
	// pending workspaces; "" starts it over.
	sweepAfter string
	// sweeping is set while a sweep has more pages to list.
	sweeping bool
}

func newFeed(b *Bridge, st *runState) *feed {
	return &feed{b: b, st: st, queued: map[string]bool{}}
}

// start subscribes to workspace activity.
func (f *feed) start() {
	if ids, cancel, ok := f.st.srv.SubscribeActivity(); ok {
		f.activity, f.cancel = ids, cancel
	}
}

func (f *feed) stop() {
	if f.cancel != nil {
		f.cancel()
	}
}

// enqueue queues a visit to ws.
func (f *feed) enqueue(ws string) {
	if !f.queued[ws] {
		f.queued[ws] = true
		f.queue = append(f.queue, ws)
	}
}

// loop visits workspaces until ctx ends. A sweep starts at once and then at
// each sweep interval; while it has more pages, the next page is listed as
// soon as the queue is empty.
func (f *feed) loop(ctx context.Context) {
	sweep := time.NewTicker(f.b.cfg.sweep)
	defer sweep.Stop()
	f.sweeping = true
	for ctx.Err() == nil {
		f.absorb()
		switch {
		case len(f.queue) > 0:
			f.visitSome(ctx)
			continue
		case f.sweeping:
			f.sweepStep(ctx)
			continue
		}
		select {
		case <-ctx.Done():
		case ws, ok := <-f.activity:
			if !ok {
				f.activity = nil
				continue
			}
			f.enqueue(ws)
		case <-sweep.C:
			f.sweeping = true
		}
	}
}

// absorb queues the workspaces of the activity received so far, without
// waiting.
func (f *feed) absorb() {
	for {
		select {
		case ws, ok := <-f.activity:
			if !ok {
				f.activity = nil
				return
			}
			f.enqueue(ws)
		default:
			return
		}
	}
}

// visitSome visits up to feedVisits workspaces from the front of the queue.
// A workspace with more to read goes to the back.
func (f *feed) visitSome(ctx context.Context) {
	n := min(len(f.queue), feedVisits)
	batch := f.queue[:n:n]
	f.queue = f.queue[n:]
	for _, ws := range batch {
		delete(f.queued, ws)
	}
	for _, ws := range batch {
		if ctx.Err() != nil {
			return
		}
		if f.visit(ctx, ws) {
			f.enqueue(ws)
		}
	}
}

// sweepStep lists one page of the workspaces whose position is behind and
// queues them.
func (f *feed) sweepStep(ctx context.Context) {
	ids, err := f.st.srv.ConsumerPendingWorkspaces(ctx, f.b.cfg.consumer, f.sweepAfter, feedVisits)
	if err != nil {
		f.b.logFailure(ctx, "list workspaces with unread changes", "", "", err)
		f.sweeping, f.sweepAfter = false, ""
		return
	}
	for _, ws := range ids {
		f.enqueue(ws)
	}
	if len(ids) < feedVisits {
		f.sweeping, f.sweepAfter = false, ""
		return
	}
	f.sweepAfter = ids[len(ids)-1]
}

// visit reads one page of the feed of ws from the bridge's position, queues
// checks for the digests it names, and moves the position past it. It
// reports whether the feed has more to read.
func (f *feed) visit(ctx context.Context, ws string) bool {
	cur, _, err := f.st.srv.ConsumerPosition(ctx, ws, f.b.cfg.consumer)
	if err != nil {
		f.b.logFailure(ctx, "read change feed position", ws, "", err)
		return false
	}
	var found []blob.Digest
	next, more, err := f.st.srv.Changes(ctx, ws, cur, feedPage, func(ch server.Change) error {
		if ch.Message != nil && f.b.counts(ch.Entity) {
			blobDigests(ch.Message.ProtoReflect(), func(d blob.Digest) { found = append(found, d) })
		}
		return nil
	})
	switch {
	case errors.Is(err, server.ErrCursorTooOld):
		// Rows the feed can no longer report may name blobs: check every
		// local blob of the workspace instead, and go on from the head.
		f.st.requestRescan(ws)
	case err != nil:
		// The sweep finds the workspace again: its position did not move.
		f.b.logFailure(ctx, "read change feed", ws, "", err)
		return false
	}
	// The rows are committed: the uploads they call for stand even when a
	// row leaves the local engine before its check runs, as a row outside
	// the link's scope does once the origin confirms it.
	f.st.sched.check(ws, found, true)
	if err := f.st.srv.AdvanceConsumer(ctx, ws, f.b.cfg.consumer, server.ConsumerUpdate{Cursor: next}); err != nil {
		f.b.logFailure(ctx, "save change feed position", ws, "", err)
		return false
	}
	return more
}

// blobRefName is the full name of the message type that marks a blob
// reference.
var blobRefName = (&syncplanev1.BlobRef{}).ProtoReflect().Descriptor().FullName()

// blobDigests calls fn with the digest of every BlobRef in msg, at any depth,
// except below a reference field, whose rows hold only the target's key. An
// empty digest names nothing.
func blobDigests(msg protoreflect.Message, fn func(blob.Digest)) {
	if msg.Descriptor().FullName() == blobRefName {
		if d := msg.Get(msg.Descriptor().Fields().ByName("digest")).String(); d != "" {
			if pd, err := blob.ParseDigest(d); err == nil {
				fn(pd)
			}
		}
		return
	}
	msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if lww.IsRef(fd) {
			return true
		}
		switch {
		case fd.IsMap():
			if fd.MapValue().Kind() == protoreflect.MessageKind {
				v.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
					blobDigests(mv.Message(), fn)
					return true
				})
			}
		case fd.IsList():
			if fd.Kind() == protoreflect.MessageKind {
				l := v.List()
				for i := 0; i < l.Len(); i++ {
					blobDigests(l.Get(i).Message(), fn)
				}
			}
		case fd.Kind() == protoreflect.MessageKind:
			blobDigests(v.Message(), fn)
		}
		return true
	})
}
