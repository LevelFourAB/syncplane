package client

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/naming"
)

// wsState is the write state of one workspace: the mutation-id counter and
// the mutation whose outcome is unknown.
type wsState struct {
	// mu is held for a whole write, from id assignment until the outcome is
	// known. The server accepts only mutation id watermark+1, so a second
	// write may not reach it before the first has an outcome.
	mu sync.Mutex
	// known reports that next is in step with the server's watermark.
	known bool
	next  uint64
	// sent is the highest id this Client put in a push request, or the
	// watermark it learned. Each push sends it as sent_through, so the server
	// refuses a batch with CodeClientIDInUse once another writer has used an
	// id after it, rather than answer this Client with that writer's verdict.
	sent uint64
	// pending is a mutation sent without a known outcome. It is sent again,
	// under its own id, before any newer one.
	pending *syncplanev1.Mutation
	// maxSeq is the highest sequence a push of this Client was applied at in
	// the workspace; pulls and streams send it as their freshness floor.
	maxSeq atomic.Uint64
}

// lowerFloor takes back the freshness floor sent when the server answered a
// read from it with a head below it. A read meets its floor unless the
// workspace's history no longer holds that sequence (a deleted and restored
// workspace, a restore below it), and such a floor sends every read to the
// primary until the new history passes it. A push applied since the read
// changed the floor, and then it stays.
func (st *wsState) lowerFloor(sent, head uint64) {
	if head != 0 && head < sent {
		st.maxSeq.CompareAndSwap(sent, head)
	}
}

// Result is the outcome of a write the server accepted.
type Result struct {
	// MutationID is the id the write was pushed under.
	MutationID uint64
	// Seq is the workspace sequence the write took: the version of every row
	// it wrote. It is 0 when AlreadyApplied is true.
	Seq uint64
	// AlreadyApplied reports that the server had applied the mutation before:
	// an earlier attempt landed although its response was lost.
	AlreadyApplied bool
}

// WriteOption configures one write operation.
type WriteOption func(*writeOpts)

type writeOpts struct {
	expected *uint64
	texts    map[string]string
}

// WithExpectedVersion makes the operation a compare-and-swap: the server
// applies it only when the stored row's version equals version, and rejects it
// with CodeConflict otherwise. An absent or deleted row has version 0, so
// WithExpectedVersion(0) means "only when the row does not exist". Read the
// version from Row.Version or Result.Seq.
func WithExpectedVersion(version uint64) WriteOption {
	return func(o *writeOpts) { o.expected = &version }
}

// WithText sets the collaborative-text field at path from a plain string.
// path is the dotted proto path; a document inside a keyed list element or a
// map entry is named by its instance path ("notes.<key>.body"), and the
// written message must carry that element or entry. On a Patch the path joins
// the field mask. Repeat the option for more fields.
//
// The write reads the stored row first, makes the document read as value
// with the fewest edits, and sends the whole document. The server merges it
// with the stored document, so edits that land between the read and the
// write are kept. An empty value deletes all text the read saw. Each such
// write adds one writer identity to the document's peer table. Not allowed
// on a Delete.
func WithText(path, value string) WriteOption {
	return func(o *writeOpts) {
		if o.texts == nil {
			o.texts = make(map[string]string)
		}
		o.texts[path] = value
	}
}

// Op is one CRUD operation for Workspace.Apply. Build it with Put, Patch or
// Delete; the zero Op is invalid. Errors in building it, such as a message
// without entity options, are reported by Apply.
type Op struct {
	kind     syncplanev1.CrudOp
	msg      proto.Message
	entity   string
	key      string
	paths    []string
	expected *uint64
	texts    map[string]string
	err      error
}

// Put replaces the whole row msg addresses, or creates it. Collaborative-text
// fields are merged with the stored documents, and fields the caller may not
// write keep their stored values. Apply marshals msg; msg is not changed.
func Put(msg proto.Message, opts ...WriteOption) Op {
	return newOp(syncplanev1.CrudOp_CRUD_OP_PUT, msg, nil, opts)
}

// Patch writes the fields that paths name from msg to the row msg addresses,
// field by field, last writer wins. paths are dotted proto field paths, the
// form a google.protobuf.FieldMask carries. A patch does not create a row:
// the server rejects a patch of an absent row with CodeConflict.
func Patch(msg proto.Message, paths []string, opts ...WriteOption) Op {
	return newOp(syncplanev1.CrudOp_CRUD_OP_PATCH, msg, paths, opts)
}

// Delete deletes the row of prototype's entity that key names. Only the type
// of prototype is read. Deleting an absent row is not an error.
func Delete(prototype proto.Message, key string, opts ...WriteOption) Op {
	o := collect(opts)
	op := Op{kind: syncplanev1.CrudOp_CRUD_OP_DELETE, key: key, expected: o.expected}
	if len(o.texts) > 0 {
		op.err = errors.New("client: WithText on a Delete")
		return op
	}
	op.entity, op.err = EntityName(prototype)
	if op.err == nil {
		if err := naming.ValidateRowKey(key); err != nil {
			op.err = fmt.Errorf("client: entity %q: %w", op.entity, err)
		}
	}
	return op
}

func newOp(kind syncplanev1.CrudOp, msg proto.Message, paths []string, opts []WriteOption) Op {
	o := collect(opts)
	op := Op{kind: kind, msg: msg, paths: slices.Clone(paths), expected: o.expected, texts: o.texts}
	op.entity, op.key, op.err = EntityKey(msg)
	return op
}

func collect(opts []WriteOption) writeOpts {
	var o writeOpts
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// Put replaces the whole row msg addresses, or creates it; see the Put
// function. It is Apply with one operation.
func (w *Workspace) Put(ctx context.Context, msg proto.Message, opts ...WriteOption) (Result, error) {
	return w.Apply(ctx, Put(msg, opts...))
}

// Patch writes the fields paths name to the row msg addresses; see the Patch
// function. It is Apply with one operation.
func (w *Workspace) Patch(ctx context.Context, msg proto.Message, paths []string, opts ...WriteOption) (Result, error) {
	return w.Apply(ctx, Patch(msg, paths, opts...))
}

// Delete deletes the row of prototype's entity that key names; see the
// Delete function. It is Apply with one operation.
func (w *Workspace) Delete(ctx context.Context, prototype proto.Message, key string, opts ...WriteOption) (Result, error) {
	return w.Apply(ctx, Delete(prototype, key, opts...))
}

// Apply pushes ops as one mutation and returns when its outcome is known. More
// than one op form a batch: the server applies them in order and atomically,
// at one sequence, or rejects them all. Apply with no ops returns an error.
//
// Apply blocks until the server answers, retrying a push that fails in
// transit (see WithMaxAttempts), and until every earlier write of the Client
// to the workspace has its outcome. An unconfirmed earlier write is sent
// again first; when its outcome still cannot be learned, Apply returns that
// error and sends nothing. Errors:
//   - *RejectedError: the server rejected the mutation, for example with
//     CodeConflict for a failed compare-and-swap.
//   - *WorkspaceError: the server refused the push for the whole workspace.
//   - ErrUnconfirmed: the outcome is unknown; see its documentation.
//   - ErrClientIDInUse: another writer uses the client id.
//   - A *connect.Error the server returned for the request, which then did
//     not apply.
//   - An error for an invalid op, before anything is sent.
func (w *Workspace) Apply(ctx context.Context, ops ...Op) (Result, error) {
	if len(ops) == 0 {
		return Result{}, errors.New("client: Apply with no operations")
	}
	for _, op := range ops {
		if op.err != nil {
			return Result{}, op.err
		}
		if op.kind == syncplanev1.CrudOp_CRUD_OP_UNSPECIFIED {
			return Result{}, errors.New("client: zero Op")
		}
	}
	return w.write(ctx, func(ctx context.Context) (*syncplanev1.Mutation, error) {
		cruds := make([]*syncplanev1.CrudMutation, len(ops))
		for i, op := range ops {
			crud, err := w.crudOf(ctx, op)
			if err != nil {
				return nil, err
			}
			cruds[i] = crud
		}
		if len(cruds) == 1 {
			return &syncplanev1.Mutation{Payload: &syncplanev1.Mutation_Crud{Crud: cruds[0]}}, nil
		}
		return &syncplanev1.Mutation{Payload: &syncplanev1.Mutation_Batch{
			Batch: &syncplanev1.BatchMutation{Mutations: cruds},
		}}, nil
	})
}

// Invoke runs the custom mutator registered on the server under name, with
// args packed as a google.protobuf.Any; nil args sends none. Its outcome and
// errors are those of Apply: a mutator that rejects reports a
// *RejectedError, and one that fails reports a *WorkspaceError with
// CodeMutationFailed.
func (w *Workspace) Invoke(ctx context.Context, name string, args proto.Message) (Result, error) {
	custom := &syncplanev1.CustomMutation{Name: name}
	if args != nil {
		a, err := anypb.New(args)
		if err != nil {
			return Result{}, fmt.Errorf("client: mutator %q args: %w", name, err)
		}
		custom.Args = a
	}
	return w.write(ctx, func(context.Context) (*syncplanev1.Mutation, error) {
		return &syncplanev1.Mutation{Payload: &syncplanev1.Mutation_Custom{Custom: custom}}, nil
	})
}

// Confirm sends the write whose outcome is unknown again, under its mutation
// id, and returns its outcome with the errors of Apply. It returns a zero
// Result and nil when no write of the workspace is unconfirmed.
func (w *Workspace) Confirm(ctx context.Context) (Result, error) {
	st := w.st
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.pending == nil {
		return Result{}, nil
	}
	return w.send(ctx, st.pending, true)
}

// crudOf builds the wire form of op, resolving text strings against the
// stored row.
func (w *Workspace) crudOf(ctx context.Context, op Op) (*syncplanev1.CrudMutation, error) {
	crud := &syncplanev1.CrudMutation{Entity: op.entity, Key: op.key, Op: op.kind, ExpectedVersion: op.expected}
	if op.kind == syncplanev1.CrudOp_CRUD_OP_DELETE {
		return crud, nil
	}
	msg := op.msg
	paths := op.paths
	if len(op.texts) > 0 {
		cur, err := w.getRaw(ctx, op.entity, op.key)
		if err != nil {
			return nil, err
		}
		msg = proto.Clone(op.msg)
		var curMsg protoreflect.Message
		if cur != nil {
			curMsg = msg.ProtoReflect().New()
			if err := proto.Unmarshal(cur.GetValue(), curMsg.Interface()); err != nil {
				return nil, fmt.Errorf("client: decode %s %q: %w", op.entity, op.key, err)
			}
		}
		if err := applyTexts(msg.ProtoReflect(), curMsg, op.texts, w.c.clientID); err != nil {
			return nil, err
		}
		if op.kind == syncplanev1.CrudOp_CRUD_OP_PATCH {
			paths = slices.Clone(op.paths)
			for p := range op.texts {
				if !slices.Contains(paths, p) {
					paths = append(paths, p)
				}
			}
			slices.Sort(paths[len(op.paths):])
		}
	}
	value, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("client: marshal %s %q: %w", op.entity, op.key, err)
	}
	crud.Value = value
	if op.kind == syncplanev1.CrudOp_CRUD_OP_PATCH {
		crud.FieldMask = &fieldmaskpb.FieldMask{Paths: paths}
	}
	return crud, nil
}

// write runs one write under the workspace lock: it brings the id counter in
// step, settles an unconfirmed earlier mutation, builds the new one and sends
// it.
func (w *Workspace) write(ctx context.Context, build func(context.Context) (*syncplanev1.Mutation, error)) (Result, error) {
	st := w.st
	st.mu.Lock()
	defer st.mu.Unlock()
	if err := w.syncCounter(ctx); err != nil {
		return Result{}, err
	}
	if st.pending != nil {
		// The earlier write's caller already has ErrUnconfirmed; only a failure
		// to settle it concerns this one, because it blocks the id sequence.
		if _, err := w.send(ctx, st.pending, true); err != nil && st.pending != nil {
			return Result{}, fmt.Errorf("client: workspace %q: settle unconfirmed mutation %d: %w",
				w.id, st.pending.GetMutationId(), err)
		}
		if err := w.syncCounter(ctx); err != nil {
			return Result{}, err
		}
	}
	m, err := build(ctx)
	if err != nil {
		return Result{}, err
	}
	m.ClientId = w.c.clientID
	m.MutationId = st.next
	st.next++
	return w.send(ctx, m, false)
}

// syncCounter brings the id counter in step with the server. A random client id is
// new to every workspace; a set one may have a watermark from an earlier
// process.
func (w *Workspace) syncCounter(ctx context.Context) error {
	st := w.st
	if st.known {
		return nil
	}
	if !w.c.pinned {
		st.next, st.known = 1, true
		return nil
	}
	acked, err := w.watermark(ctx)
	if err != nil {
		return err
	}
	st.next, st.known = acked+1, true
	// The ids up to the watermark count as this Client's own: a pinned id
	// takes the server's watermark over.
	st.sent = max(st.sent, acked)
	return nil
}

// watermark reads the server's last acked mutation id for this client. The
// pull carries no cursor, which the server reads on its primary store, so a
// read replica that lags cannot give a watermark below the ids already used.
func (w *Workspace) watermark(ctx context.Context) (uint64, error) {
	resp, err := w.c.sync.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
		WorkspaceId: w.id,
		ClientId:    w.c.clientID,
		Limit:       1,
	}))
	if err != nil {
		return 0, fmt.Errorf("client: workspace %q: read mutation watermark: %w", w.id, err)
	}
	return resp.Msg.GetLastAckedMutationId(), nil
}

// send pushes m until its outcome is known or the attempts run out. resend
// reports that m may have reached the server before. The caller holds st.mu.
func (w *Workspace) send(ctx context.Context, m *syncplanev1.Mutation, resend bool) (Result, error) {
	st := w.st
	gapRetried := false
	var lastErr error
	for attempt := 0; attempt < w.c.maxAttempts; attempt++ {
		if attempt > 0 {
			if err := sleep(ctx, backoff(attempt)); err != nil {
				lastErr = err
				break
			}
		}
		sentThrough := st.sent
		st.sent = max(st.sent, m.GetMutationId())
		resp, err := w.c.sync.Push(ctx, connect.NewRequest(&syncplanev1.PushRequest{
			Batches: []*syncplanev1.WorkspaceBatch{{
				WorkspaceId: w.id,
				Mutations:   []*syncplanev1.Mutation{m},
				SentThrough: &sentThrough,
			}},
		}))
		if err != nil {
			if ctx.Err() == nil && !ambiguous(err) {
				// The request was refused before anything applied.
				return Result{}, w.refused(m, resend, err)
			}
			lastErr, resend = err, true
			if ctx.Err() != nil {
				break
			}
			continue
		}
		results := resp.Msg.GetResults()
		if len(results) != 1 {
			lastErr, resend = fmt.Errorf("client: push answered %d workspace results for 1", len(results)), true
			continue
		}
		wr := results[0]
		switch code := wr.GetErrorCode(); code {
		case "":
		case CodeInternal:
			// The server can fail after its commit landed, so a later
			// ALREADY_APPLIED answer can be this mutation's own.
			lastErr, resend = &WorkspaceError{Workspace: w.id, Code: code, Message: wr.GetErrorMessage()}, true
			continue
		case CodeClientIDInUse:
			// Nothing in this batch applied. Another writer used an id after
			// the ones this Client sent, so a resend of m can never be
			// answered for m again: an earlier attempt may have applied, or
			// the other writer took its id. Learn the watermark again before
			// the next write.
			w.release(m)
			st.known = false
			if resend {
				return Result{}, fmt.Errorf("client: workspace %q: mutation %d: %w: %w: %s",
					w.id, m.GetMutationId(), ErrUnconfirmed, ErrClientIDInUse, wr.GetErrorMessage())
			}
			return Result{}, fmt.Errorf("client: workspace %q: mutation %d: %w: %s",
				w.id, m.GetMutationId(), ErrClientIDInUse, wr.GetErrorMessage())
		case CodeMutationIDGap:
			// Nothing in the batch applied, so the id is free to move. The
			// server's watermark is not where the counter thought: another
			// writer under this id, or a watermark the server collected.
			if gapRetried {
				w.release(m)
				return Result{}, &WorkspaceError{Workspace: w.id, Code: code, Message: wr.GetErrorMessage()}
			}
			gapRetried = true
			acked, err := w.watermark(ctx)
			if err != nil {
				lastErr = err
				continue
			}
			m.MutationId = acked + 1
			st.next = acked + 2
			attempt--
			continue
		default:
			return Result{}, w.refused(m, resend, &WorkspaceError{Workspace: w.id, Code: code, Message: wr.GetErrorMessage()})
		}
		if len(wr.GetResults()) != 1 {
			lastErr, resend = fmt.Errorf("client: push answered %d mutation results for 1", len(wr.GetResults())), true
			continue
		}
		r := wr.GetResults()[0]
		st.pending = nil
		switch r.GetStatus() {
		case syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED:
			for {
				cur := st.maxSeq.Load()
				if r.GetSeq() <= cur || st.maxSeq.CompareAndSwap(cur, r.GetSeq()) {
					break
				}
			}
			return Result{MutationID: m.GetMutationId(), Seq: r.GetSeq()}, nil
		case syncplanev1.MutationStatus_MUTATION_STATUS_ALREADY_APPLIED:
			if !resend {
				// No earlier attempt of this mutation can have landed, so
				// someone else holds the id. Learn the watermark again before
				// the next write.
				st.known = false
				return Result{}, fmt.Errorf("client: workspace %q: mutation %d: %w", w.id, m.GetMutationId(), ErrClientIDInUse)
			}
			return Result{MutationID: m.GetMutationId(), AlreadyApplied: true}, nil
		case syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED:
			return Result{}, &RejectedError{
				Workspace:  w.id,
				MutationID: m.GetMutationId(),
				Code:       r.GetErrorCode(),
				Message:    r.GetErrorMessage(),
			}
		default:
			st.known = false
			return Result{}, fmt.Errorf("client: workspace %q: mutation %d: unknown status %v", w.id, m.GetMutationId(), r.GetStatus())
		}
	}
	return Result{}, w.unconfirmed(m, lastErr)
}

// release gives back the id of a mutation the server did not apply, so the
// next one takes it and the sequence stays contiguous. m holds the newest id
// because writes are serialized.
func (w *Workspace) release(m *syncplanev1.Mutation) {
	w.st.pending = nil
	w.st.next = m.GetMutationId()
}

// refused handles a push the server refused before anything in it applied,
// and returns the error to report. Most refusals say nothing about an earlier
// attempt of m, so when one may have applied (resend), m stays unconfirmed and
// keeps its id: giving the id back would let the next write take an id the
// server may already hold, and get m's verdict in place of its own. The
// exception is CodeMutationFailed: the mutator runs after the watermark check,
// where an earlier attempt that applied gets ALREADY_APPLIED, so the id is
// free.
func (w *Workspace) refused(m *syncplanev1.Mutation, resend bool, err error) error {
	var we *WorkspaceError
	if !resend || errors.As(err, &we) && we.Code == CodeMutationFailed {
		w.release(m)
		return err
	}
	return w.unconfirmed(m, err)
}

// unconfirmed keeps m as the mutation whose outcome is unknown, and returns
// the ErrUnconfirmed error that reports it, wrapping err.
func (w *Workspace) unconfirmed(m *syncplanev1.Mutation, err error) error {
	w.st.pending = m
	return fmt.Errorf("client: workspace %q: mutation %d: %w: %w", w.id, m.GetMutationId(), ErrUnconfirmed, err)
}

// ambiguous reports whether a push that failed with err may have been applied.
// Codes a server or proxy returns before it handles a request are definite;
// a lost connection or a timeout is not.
func ambiguous(err error) bool {
	switch connect.CodeOf(err) {
	case connect.CodeUnknown, connect.CodeUnavailable, connect.CodeDeadlineExceeded,
		connect.CodeAborted, connect.CodeInternal, connect.CodeCanceled, connect.CodeDataLoss:
		return true
	}
	return false
}

func backoff(attempt int) time.Duration {
	d := min(100*time.Millisecond<<min(attempt-1, 5), 2*time.Second)
	return d/2 + rand.N(d/2+1)
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
