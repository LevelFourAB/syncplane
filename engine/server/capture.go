package server

import (
	"context"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/storage"
)

// WriteCapture observes every mutation the server applies, inside the write
// transaction, after the mutation's writes and before it commits. It is how a
// replication link records local writes atomically with the rows they wrote.
//
// It is called once for each mutation a client push applies, and once for each
// Apply, ApplyIdempotent, Mutate, MutateIdempotent, Invoke and InvokeIdempotent
// call that writes a row or invokes a mutator. It is not called for a mutation
// that is rejected or fails before it, for a replayed idempotent call, or for
// writes made through Replicate.
//
// A Rejection it returns (see Reject) rejects the mutation: nothing the
// mutation wrote commits, a push reports the mutation REJECTED with the
// Rejection's code, and every other entry point returns the error. Any other
// error fails the write as a storage error does: a push fails the workspace's
// batch with CodeInternal.
//
// It runs while the workspace's write lock is held, and it can run more than
// once for one mutation when the transaction is run again. Only what it writes
// through CapturedWrite.Tx is kept from a run that does not commit, so any
// other effect must tolerate a repeat. ctx is the context of the call that
// wrote; a write call made with it fails rather than deadlocks.
type WriteCapture func(ctx context.Context, w *CapturedWrite) error

// CapturedWrite is one mutation as a WriteCapture sees it. Its fields, and
// the slices they hold, are valid only for the duration of the call.
type CapturedWrite struct {
	// Workspace is the workspace the mutation wrote to.
	Workspace string
	// Seq is the sequence the mutation's rows were written at, or 0 when it
	// wrote none and was assigned none.
	Seq uint64
	// Actor is who the mutation's rows are attributed to (see storage.Actor).
	Actor storage.Actor
	// Custom is the custom mutation that was invoked, nil for a CRUD write.
	// Ops then holds the rows its mutator wrote, and is empty for a mutation
	// that WithUnknownMutatorForwarding accepted.
	Custom *syncplanev1.CustomMutation
	// Ops are the row writes the mutation made, in the order they were made.
	Ops []CapturedOp
	// Origin is the client mutation this write applied, nil for a write that
	// did not come from a client push (Apply, Mutate, Invoke and their
	// idempotent forms).
	Origin *MutationOrigin
	// Tx is the mutation's write transaction. What the capture writes through
	// it commits or rolls back with the mutation. A row written through it
	// skips every derived structure the engine keeps, so use it for consumer
	// shadows (see storage.WriteTx.PutConsumerShadow), not for rows.
	Tx storage.WriteTx
}

// CapturedOp is one row write of a CapturedWrite.
type CapturedOp struct {
	Entity string
	Key    string
	Op     syncplanev1.CrudOp
	// Value is the value the write submitted, encoded, nil for a DELETE. For a
	// PUT it holds the stored value of each field the caller may not write,
	// and each plain-string text value resolved to a document. A
	// collaborative-text field holds the submitted document or fragment, not
	// the union stored in After.
	Value []byte
	// Mask is the PATCH field mask the write merged, nil for PUT and DELETE.
	Mask []string
	// ExpectedVersion is the version the write required the row to be at, nil
	// for an unguarded write.
	ExpectedVersion *uint64
	// PrevVersion is the row's version before the write, 0 when the row was
	// absent or deleted.
	PrevVersion uint64
	// Before is the row's stored value before the write, nil when the row was
	// absent or deleted. A later write of the same mutation to the same row
	// sees what the earlier one stored.
	Before []byte
	// After is the row's stored value after the write, nil when the write
	// deleted it.
	After []byte
}

// WithWriteCapture installs a WriteCapture. A server has at most one; a later
// WithWriteCapture replaces an earlier one.
func WithWriteCapture(c WriteCapture) Option {
	return func(s *Server) error {
		s.capture = c
		return nil
	}
}

// writeLog collects the row writes of one mutation for the WriteCapture. A nil
// *writeLog records nothing, which is what every write path holds when no
// capture is installed.
type writeLog struct {
	custom *syncplanev1.CustomMutation
	ops    []CapturedOp
}

// newWriteLog returns a log for one mutation, nil when no capture is installed.
func (s *Server) newWriteLog() *writeLog {
	if s.capture == nil {
		return nil
	}
	return &writeLog{}
}

func (l *writeLog) add(op CapturedOp) {
	if l != nil {
		l.ops = append(l.ops, op)
	}
}

// mark returns how many writes the log holds, for rewind.
func (l *writeLog) mark() int {
	if l == nil {
		return 0
	}
	return len(l.ops)
}

// rewind drops the writes recorded after mark, for a write whose savepoint
// rolled them back.
func (l *writeLog) rewind(mark int) {
	if l != nil {
		l.ops = l.ops[:mark]
	}
}

// invoked records the custom mutation a log's writes were made for. args are
// the args as the caller passed them, nil for none.
func (l *writeLog) invoked(name string, args proto.Message) error {
	if l == nil {
		return nil
	}
	custom := &syncplanev1.CustomMutation{Name: name}
	if args != nil {
		packed, err := anypb.New(args)
		if err != nil {
			return err
		}
		custom.Args = packed
	}
	l.custom = custom
	return nil
}

// runCapture hands one mutation's log to the WriteCapture. A log with no
// writes and no custom mutation is a call that did nothing, and is not
// captured.
func (s *Server) runCapture(ctx context.Context, ws string, tx storage.WriteTx, l *writeLog, seq uint64, origin *MutationOrigin) error {
	if l == nil || (len(l.ops) == 0 && l.custom == nil) {
		return nil
	}
	actor, err := actorOf(ctx)
	if err != nil {
		return err
	}
	return s.capture(markWriteTx(ctx), &CapturedWrite{
		Workspace: ws,
		Seq:       seq,
		Actor:     actor,
		Custom:    l.custom,
		Ops:       l.ops,
		Origin:    origin,
		Tx:        tx,
	})
}

// marshalIfLogged encodes msg for a CapturedOp, and skips the work when no
// capture is installed.
func marshalIfLogged(l *writeLog, msg proto.Message) ([]byte, error) {
	if l == nil {
		return nil, nil
	}
	return proto.Marshal(msg)
}
