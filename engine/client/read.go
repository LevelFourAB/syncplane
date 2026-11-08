package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
)

// Row is one row as the server sent it, decoded into its entity message.
type Row[T proto.Message] struct {
	Key string
	// Version is the workspace sequence that last wrote the row; pass it to
	// WithExpectedVersion for a compare-and-swap.
	Version uint64
	// Value is the row. It is the zero T (nil) when Deleted is true. A field
	// named in Hidden, or in the RowChange's text_deltas, reads as its zero
	// value.
	Value T
	// Hidden names the fields the caller may not read.
	Hidden []string
	// Deleted reports a tombstone. Only rows from a pull or a stream can be
	// tombstones.
	Deleted bool
}

// DecodeRow decodes one RowChange of T's entity. It returns an error when rc
// belongs to another entity or its value does not decode as T. T must be a
// pointer to a generated message type.
func DecodeRow[T proto.Message](rc *syncplanev1.RowChange) (Row[T], error) {
	var zero T
	name, err := EntityName(zero)
	if err != nil {
		return Row[T]{}, err
	}
	if rc.GetEntity() != name {
		return Row[T]{}, fmt.Errorf("client: row of entity %q is not a %q row", rc.GetEntity(), name)
	}
	row := Row[T]{Key: rc.GetKey(), Version: rc.GetVersion(), Hidden: rc.GetHidden(), Deleted: rc.GetTombstone()}
	if row.Deleted {
		return row, nil
	}
	row.Value = zero.ProtoReflect().New().Interface().(T)
	if err := proto.Unmarshal(rc.GetValue(), row.Value); err != nil {
		return Row[T]{}, fmt.Errorf("client: decode %s %q: %w", name, rc.GetKey(), err)
	}
	return row, nil
}

// Get reads the row of T's entity that key names. It reports false when the
// row is absent, deleted, or hidden from the caller by a read rule. T must be
// a pointer to a generated message type.
func Get[T proto.Message](ctx context.Context, w *Workspace, key string) (Row[T], bool, error) {
	var zero T
	name, err := EntityName(zero)
	if err != nil {
		return Row[T]{}, false, err
	}
	rc, err := w.getRaw(ctx, name, key)
	if err != nil || rc == nil {
		return Row[T]{}, false, err
	}
	row, err := DecodeRow[T](rc)
	if err != nil {
		return Row[T]{}, false, err
	}
	return row, true, nil
}

// getRaw reads one row through a snapshot scoped to its key, which the server
// serves by key lookup rather than by scan. It returns nil when the row is
// absent or unreadable.
func (w *Workspace) getRaw(ctx context.Context, entity, key string) (*syncplanev1.RowChange, error) {
	snap, err := w.Bootstrap(ctx, WithSubscriptions(&syncplanev1.Subscription{
		Entity: entity,
		KeySet: &syncplanev1.KeySet{Keys: []string{key}},
	}))
	if err != nil {
		return nil, err
	}
	var found *syncplanev1.RowChange
	for rc, err := range snap.Rows() {
		if err != nil {
			return nil, err
		}
		if rc.GetEntity() == entity && rc.GetKey() == key && !rc.GetTombstone() {
			found = rc
		}
	}
	return found, nil
}

// Page is one page of a query.
type Page[T proto.Message] struct {
	// Rows are ordered by the query's OrderBy, ties broken by row key.
	Rows []Row[T]
	// StartCursor and EndCursor are the positions of the first and last row,
	// for Spec.Before and Spec.After. Both are empty for an empty page.
	StartCursor, EndCursor []byte
	// HasMore reports rows past the end the limit counted from.
	HasMore bool
}

// Query runs spec against T's entity on the server, with the caller's read
// rules applied, and returns one page. Build spec.Where and spec.OrderBy with
// the engine/query constructors.
//
// spec.Limit of 0 or less asks for the server's default page size, and the
// server clamps every limit to its maximum. Without a secondary index that
// covers the predicate and order, the server scans the whole entity per call.
// The server's errors arrive as *connect.Error, for example InvalidArgument
// for an unknown field path or a cursor of another query.
func Query[T proto.Message](ctx context.Context, w *Workspace, spec query.Spec) (Page[T], error) {
	var zero T
	name, err := EntityName(zero)
	if err != nil {
		return Page[T]{}, err
	}
	resp, err := w.c.sync.Query(ctx, connect.NewRequest(&syncplanev1.QueryRequest{
		WorkspaceId: w.id,
		Entity:      name,
		Predicate:   spec.Where,
		OrderBy:     spec.OrderBy,
		Limit:       uint32(max(spec.Limit, 0)),
		Offset:      uint32(max(spec.Offset, 0)),
		After:       spec.After,
		Before:      spec.Before,
	}))
	if err != nil {
		return Page[T]{}, err
	}
	page := Page[T]{
		Rows:        make([]Row[T], 0, len(resp.Msg.GetRows())),
		StartCursor: resp.Msg.GetStartCursor(),
		EndCursor:   resp.Msg.GetEndCursor(),
		HasMore:     resp.Msg.GetHasMore(),
	}
	for _, rc := range resp.Msg.GetRows() {
		row, err := DecodeRow[T](rc)
		if err != nil {
			return Page[T]{}, err
		}
		page.Rows = append(page.Rows, row)
	}
	return page, nil
}

// QueryAll runs spec like Query and yields every matching row, fetching the
// next page after the last one when the server reports more. spec.Limit is
// the page size. spec.Offset applies to the first page only. Rows written
// between two pages may be missed or seen twice. A spec with Before but no
// After yields an error, because it pages backward.
//
// Iteration stops after the first error.
func QueryAll[T proto.Message](ctx context.Context, w *Workspace, spec query.Spec) iter.Seq2[Row[T], error] {
	return func(yield func(Row[T], error) bool) {
		if len(spec.Before) > 0 && len(spec.After) == 0 {
			yield(Row[T]{}, errors.New("client: QueryAll pages forward; Before needs After"))
			return
		}
		for {
			page, err := Query[T](ctx, w, spec)
			if err != nil {
				yield(Row[T]{}, err)
				return
			}
			for _, row := range page.Rows {
				if !yield(row, nil) {
					return
				}
			}
			if !page.HasMore || len(page.EndCursor) == 0 {
				return
			}
			spec.After, spec.Offset = page.EndCursor, 0
		}
	}
}

// ReadOption configures Bootstrap, Pull and Subscribe.
type ReadOption func(*readOpts)

type readOpts struct {
	subs    []*syncplanev1.Subscription
	limit   uint32
	noDelta []string
}

// WithSubscriptions scopes Bootstrap and Pull to subs. Without it they cover
// every entity. Subscribe ignores it; set Scope.Subscriptions instead.
func WithSubscriptions(subs ...*syncplanev1.Subscription) ReadOption {
	return func(o *readOpts) { o.subs = append(o.subs, subs...) }
}

// WithPageLimit sets the most changes per Pull page or Subscribe catch-up
// page. 0, the default, asks for the server's default; the server clamps to
// its maximum.
func WithPageLimit(n uint32) ReadOption {
	return func(o *readOpts) { o.limit = n }
}

// WithNoDeltaEntities makes Pull and Subscribe send every collaborative-text
// field of the named entities in full. Without it, a page may send such a
// field as a fragment relative to the cursor (RowChange.text_deltas), which
// only a caller that keeps the rows can apply.
func WithNoDeltaEntities(entities ...string) ReadOption {
	return func(o *readOpts) { o.noDelta = append(o.noDelta, entities...) }
}

func readOptions(opts []ReadOption) readOpts {
	var o readOpts
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// Snapshot is a Bootstrap in progress: the position it captures, and its rows.
type Snapshot struct {
	// Cursor is the position the snapshot captures. Pass it to Pull or
	// Subscribe to receive every change written after it.
	Cursor []byte
	// Seq is the workspace sequence Cursor captures.
	Seq uint64
	// LastAckedMutationID is the server's mutation watermark for the Client's
	// client id.
	LastAckedMutationID uint64
	// SchemaRevision is the server's schema revision, or "" when it advertises
	// none.
	SchemaRevision string
	// Entities lists every entity the snapshot ships rows of, in delivery
	// order.
	Entities []string

	stream *connect.ServerStreamForClient[syncplanev1.BootstrapResponse]
	used   bool
}

// Bootstrap opens a snapshot of the workspace: every row the caller can read,
// within the WithSubscriptions scope. It returns when the server has sent the
// snapshot's position; read the rows with Snapshot.Rows. The caller must
// range over Rows or call Close to release the stream.
func (w *Workspace) Bootstrap(ctx context.Context, opts ...ReadOption) (*Snapshot, error) {
	o := readOptions(opts)
	stream, err := w.c.sync.Bootstrap(ctx, connect.NewRequest(&syncplanev1.BootstrapRequest{
		WorkspaceId:   w.id,
		ClientId:      w.c.clientID,
		Subscriptions: o.subs,
	}))
	if err != nil {
		return nil, err
	}
	if !stream.Receive() {
		err := stream.Err()
		_ = stream.Close()
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		return nil, fmt.Errorf("client: bootstrap %q: %w", w.id, err)
	}
	begin := stream.Msg().GetBegin()
	if begin == nil {
		_ = stream.Close()
		return nil, fmt.Errorf("client: bootstrap %q: stream did not start with Begin", w.id)
	}
	return &Snapshot{
		Cursor:              begin.GetCursor(),
		Seq:                 begin.GetSeq(),
		LastAckedMutationID: begin.GetLastAckedMutationId(),
		SchemaRevision:      begin.GetSchemaRevision(),
		Entities:            begin.GetEntities(),
		stream:              stream,
	}, nil
}

// Rows yields the snapshot's rows in delivery order, and closes the stream
// when the iteration ends. A snapshot holds no tombstones. Rows can be ranged
// over once; a second range yields an error. Iteration stops after the first
// error.
func (s *Snapshot) Rows() iter.Seq2[*syncplanev1.RowChange, error] {
	return func(yield func(*syncplanev1.RowChange, error) bool) {
		if s.used {
			yield(nil, errors.New("client: snapshot rows already read"))
			return
		}
		s.used = true
		defer s.Close()
		for s.stream.Receive() {
			msg := s.stream.Msg()
			switch {
			case msg.GetChunk() != nil:
				for _, rc := range msg.GetChunk().GetRows() {
					if !yield(rc, nil) {
						return
					}
				}
			case msg.GetEnd() != nil:
				return
			}
		}
		err := s.stream.Err()
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		yield(nil, fmt.Errorf("client: bootstrap ended before its End: %w", err))
	}
}

// Close releases the snapshot's stream. It is safe to call more than once.
func (s *Snapshot) Close() error {
	return s.stream.Close()
}

// Pull yields the pages of changes after cursor, one Pull call per page,
// until the server reports no more. Store each page's NextCursor once its
// changes are applied; the last one resumes from where the iteration ended. A
// nil cursor starts at the beginning of the workspace, which only a workspace
// that has collected no tombstones can serve.
//
// When the cursor cannot continue, Pull yields ErrReset; bootstrap again.
// Iteration stops after the first error. Each request carries the highest
// sequence this Client's writes were applied at, so a server reading from a
// replica never answers with state older than them. A response whose head is
// below that sequence tells that the workspace's history no longer holds it,
// and later requests carry that head instead.
func (w *Workspace) Pull(ctx context.Context, cursor []byte, opts ...ReadOption) iter.Seq2[*syncplanev1.PullResponse, error] {
	o := readOptions(opts)
	return func(yield func(*syncplanev1.PullResponse, error) bool) {
		for {
			floor := w.st.maxSeq.Load()
			resp, err := w.c.sync.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
				WorkspaceId:     w.id,
				ClientId:        w.c.clientID,
				Cursor:          cursor,
				Subscriptions:   o.subs,
				Limit:           o.limit,
				NoDeltaEntities: o.noDelta,
				MinSeq:          floor,
			}))
			if err != nil {
				yield(nil, err)
				return
			}
			w.st.lowerFloor(floor, resp.Msg.GetHeadSeq())
			if resp.Msg.GetReset_() {
				yield(nil, fmt.Errorf("client: pull %q: %w", w.id, ErrReset))
				return
			}
			if !yield(resp.Msg, nil) || !resp.Msg.GetHasMore() {
				return
			}
			// An empty NextCursor keeps the previous one; a nil cursor would
			// start the feed over from the beginning.
			if c := resp.Msg.GetNextCursor(); len(c) > 0 {
				cursor = c
			}
		}
	}
}

// Scope is one workspace of a Subscribe stream.
type Scope struct {
	Workspace string
	// Cursor is where the stream resumes, from a Snapshot, a Pull page or a
	// stream page. Nil starts at the beginning, as for Pull.
	Cursor []byte
	// Subscriptions scope the workspace; empty means every entity.
	Subscriptions []*syncplanev1.Subscription
}

// Event is one frame of a Subscribe stream. Exactly one of Page, CaughtUp and
// Dropped is set.
type Event struct {
	Workspace      string
	SchemaRevision string
	// Page holds changes for the workspace. Store its NextCursor once the
	// changes are applied; an empty NextCursor keeps the previous one.
	Page *syncplanev1.PullResponse
	// CaughtUp reports that the workspace's backlog is drained; live changes
	// follow.
	CaughtUp bool
	// Dropped reports that the workspace left the stream: ErrReset when its
	// cursor cannot continue, or a *WorkspaceError with
	// CodeWorkspaceAccessRevoked, CodeWorkspaceNoAccess or CodeInternal.
	Dropped error
}

// Subscribe opens one stream over scopes and yields its events: catch-up
// pages, then live changes. Heartbeats are not yielded. WithPageLimit and
// WithNoDeltaEntities apply to every scope.
//
// The stream does not reconnect. Iteration ends when ctx is done (without an
// error), when every scope has been dropped, or when the stream fails, which
// it yields as an error. Each scope carries the highest sequence this
// Client's writes to the workspace were applied at, as Pull does.
func (c *Client) Subscribe(ctx context.Context, scopes []Scope, opts ...ReadOption) iter.Seq2[Event, error] {
	o := readOptions(opts)
	return func(yield func(Event, error) bool) {
		req := &syncplanev1.SubscribeRequest{
			ClientId:        c.clientID,
			Limit:           o.limit,
			NoDeltaEntities: o.noDelta,
		}
		live := make(map[string]bool, len(scopes))
		floors := make(map[string]uint64, len(scopes))
		for _, s := range scopes {
			floors[s.Workspace] = c.state(s.Workspace).maxSeq.Load()
			req.Scopes = append(req.Scopes, &syncplanev1.WorkspaceScope{
				WorkspaceId:   s.Workspace,
				Cursor:        s.Cursor,
				Subscriptions: s.Subscriptions,
				MinSeq:        floors[s.Workspace],
			})
			live[s.Workspace] = true
		}
		stream, err := c.sync.Subscribe(ctx, connect.NewRequest(req))
		if err != nil {
			if ctx.Err() == nil {
				yield(Event{}, err)
			}
			return
		}
		defer stream.Close()
		for stream.Receive() {
			msg := stream.Msg()
			ev := Event{Workspace: msg.GetWorkspaceId(), SchemaRevision: msg.GetSchemaRevision()}
			switch {
			case msg.GetHeartbeat():
				continue
			case msg.GetPage() != nil:
				ev.Page = msg.GetPage()
				if f, ok := floors[ev.Workspace]; ok {
					c.state(ev.Workspace).lowerFloor(f, ev.Page.GetHeadSeq())
				}
			case msg.GetCaughtUp():
				ev.CaughtUp = true
			case msg.GetDropped() != nil:
				ev.Dropped = droppedErr(ev.Workspace, msg.GetDropped())
				delete(live, ev.Workspace)
			default:
				continue
			}
			if !yield(ev, nil) || len(live) == 0 {
				return
			}
		}
		if err := stream.Err(); err != nil && ctx.Err() == nil {
			yield(Event{}, err)
		}
	}
}

func droppedErr(ws string, d *syncplanev1.WorkspaceDropped) error {
	code := CodeInternal
	switch d.GetReason() {
	case syncplanev1.WorkspaceDropped_REASON_RESET:
		return fmt.Errorf("client: workspace %q: %s: %w", ws, d.GetMessage(), ErrReset)
	case syncplanev1.WorkspaceDropped_REASON_ACCESS_REVOKED:
		code = CodeWorkspaceAccessRevoked
	case syncplanev1.WorkspaceDropped_REASON_NO_ACCESS:
		code = CodeWorkspaceNoAccess
	}
	return &WorkspaceError{Workspace: ws, Code: code, Message: d.GetMessage()}
}
