package connect_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	engineconnect "github.com/levelfourab/syncplane/engine/connect"
	"github.com/levelfourab/syncplane/engine/connect/sqlitestate"
	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

// init quiets the engine's default-logger output for the whole test binary
// (init runs before any test) so specs don't spam stderr.
func init() { slog.SetDefault(slog.New(slog.DiscardHandler)) }

func TestConnect(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Connect Suite")
}

// fpOf is the fake source's fingerprint: a deterministic content hash, so
// identical content always stamps identically (the Source contract).
func fpOf(msg proto.Message) string {
	GinkgoHelper()
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
	Expect(err).NotTo(HaveOccurred())
	return fmt.Sprintf("%x", sha256.Sum256(raw))
}

// todoSource is an in-memory Source over Todo entities, keyed by Id. Tests
// mutate items/fps directly to simulate edits made behind the engine's back.
type todoSource struct {
	mu      sync.Mutex
	items   map[string]map[string]*todov1.Todo // ws → id → todo
	fps     map[string]map[string]string       // ws → id → fingerprint
	writes  int
	deletes int
}

func newTodoSource() *todoSource {
	return &todoSource{
		items: make(map[string]map[string]*todov1.Todo),
		fps:   make(map[string]map[string]string),
	}
}

func (s *todoSource) set(ws string, todo *todov1.Todo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.items[ws] == nil {
		s.items[ws] = make(map[string]*todov1.Todo)
		s.fps[ws] = make(map[string]string)
	}
	s.items[ws][todo.GetId()] = proto.Clone(todo).(*todov1.Todo)
	s.fps[ws][todo.GetId()] = fpOf(todo)
}

func (s *todoSource) remove(ws, id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items[ws], id)
	delete(s.fps[ws], id)
}

func (s *todoSource) get(ws, id string) *todov1.Todo {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.items[ws][id]
	if !ok {
		return nil
	}
	return proto.Clone(t).(*todov1.Todo)
}

// touch rewrites the stored fingerprint without changing content — the
// moral equivalent of an mtime bump.
func (s *todoSource) touch(ws, id, fp string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fps[ws][id] = fp
}

func (s *todoSource) fp(ws, id string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fps[ws][id]
}

// base satisfies todoBacked: a todoSource is its own store.
func (s *todoSource) base() *todoSource { return s }

func (s *todoSource) writeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writes
}

func (s *todoSource) deleteCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deletes
}

func (s *todoSource) List(ctx context.Context, ws string, fn func(engineconnect.Item) error) error {
	s.mu.Lock()
	type entry struct {
		msg *todov1.Todo
		fp  string
	}
	var entries []entry
	for id, t := range s.items[ws] {
		entries = append(entries, entry{proto.Clone(t).(*todov1.Todo), s.fps[ws][id]})
	}
	s.mu.Unlock()
	for _, e := range entries {
		if err := fn(engineconnect.Item{Msg: e.msg, Fingerprint: e.fp}); err != nil {
			return err
		}
	}
	return nil
}

func (s *todoSource) Write(ctx context.Context, ws string, msg proto.Message) (string, error) {
	todo, ok := msg.(*todov1.Todo)
	if !ok {
		return "", fmt.Errorf("todoSource: unexpected type %T", msg)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.items[ws] == nil {
		s.items[ws] = make(map[string]*todov1.Todo)
		s.fps[ws] = make(map[string]string)
	}
	s.writes++
	clone := proto.Clone(todo).(*todov1.Todo)
	s.items[ws][todo.GetId()] = clone
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(clone)
	if err != nil {
		return "", err
	}
	fp := fmt.Sprintf("%x", sha256.Sum256(raw))
	s.fps[ws][todo.GetId()] = fp
	return fp, nil
}

func (s *todoSource) Delete(ctx context.Context, ws string, prototype proto.Message, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletes++
	delete(s.items[ws], key)
	delete(s.fps[ws], key)
	return nil
}

// todoBacked is a Source the fixtures can wire while still handing the spec
// the in-memory todo store underneath it — the seam the incremental,
// read-only and failing wrappers below hang off.
type todoBacked interface {
	engineconnect.Source
	base() *todoSource
}

// incTodoSource gives a todoSource a native change cursor: every edit made
// through it appends to a per-workspace log, and a cursor is an offset into
// that log — the shape a Gmail historyId or a CalDAV sync-token has. Edits
// made through the embedded todoSource deliberately bypass the log, standing
// in for a change the source's feed never reported.
type incTodoSource struct {
	*todoSource

	mu       sync.Mutex
	log      map[string][]string // ws → keys, in edit order
	pageSize int                 // 0 = one page per call
	invalid  bool                // the next Changes rejects its cursor
	lists    int
	deltas   int
	points   int
}

func newIncTodoSource() *incTodoSource {
	return &incTodoSource{todoSource: newTodoSource(), log: make(map[string][]string)}
}

func (s *incTodoSource) record(ws, id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log[ws] = append(s.log[ws], id)
}

// set and remove shadow the embedded store's so an edit lands in the log too.
func (s *incTodoSource) set(ws string, todo *todov1.Todo) {
	s.todoSource.set(ws, todo)
	s.record(ws, todo.GetId())
}

func (s *incTodoSource) remove(ws, id string) {
	s.todoSource.remove(ws, id)
	s.record(ws, id)
}

// invalidateCursor makes the next Changes call reject whatever cursor it is
// handed, as UIDVALIDITY bumping or a history window expiring would.
func (s *incTodoSource) invalidateCursor() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.invalid = true
}

func (s *incTodoSource) counts() (lists, deltas, points int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lists, s.deltas, s.points
}

func (s *incTodoSource) List(ctx context.Context, ws string, fn func(engineconnect.Item) error) error {
	s.mu.Lock()
	s.lists++
	s.mu.Unlock()
	return s.todoSource.List(ctx, ws, fn)
}

func (s *incTodoSource) Checkpoint(_ context.Context, ws string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.points++
	return strconv.Itoa(len(s.log[ws])), nil
}

func (s *incTodoSource) Changes(_ context.Context, ws, cursor string) (engineconnect.Delta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deltas++
	if s.invalid {
		s.invalid = false
		return engineconnect.Delta{}, fmt.Errorf("uidvalidity bumped: %w", engineconnect.ErrCursorInvalid)
	}
	off, err := strconv.Atoi(cursor)
	if err != nil || off > len(s.log[ws]) {
		return engineconnect.Delta{}, fmt.Errorf("cursor %q: %w", cursor, engineconnect.ErrCursorInvalid)
	}
	end := len(s.log[ws])
	if s.pageSize > 0 && end-off > s.pageSize {
		end = off + s.pageSize
	}
	delta := engineconnect.Delta{NextCursor: strconv.Itoa(end), More: end < len(s.log[ws])}
	for _, key := range s.log[ws][off:end] {
		// The log says "this row changed"; what it changed to is read now, so
		// a key whose row is gone reports as a deletion.
		if todo := s.todoSource.get(ws, key); todo != nil {
			delta.Items = append(delta.Items, engineconnect.Item{Msg: todo, Fingerprint: s.todoSource.fp(ws, key)})
		} else {
			delta.Deletions = append(delta.Deletions, engineconnect.Deletion{Prototype: &todov1.Todo{}, Key: key})
		}
	}
	return delta, nil
}

// readOnlyTodoSource exposes only the List half of a todoSource: a store the
// app never writes back to, which an IngressOnly binding must accept and a
// Bidirectional one must reject.
type readOnlyTodoSource struct{ inner *todoSource }

func (s *readOnlyTodoSource) List(ctx context.Context, ws string, fn func(engineconnect.Item) error) error {
	return s.inner.List(ctx, ws, fn)
}

func (s *readOnlyTodoSource) base() *todoSource { return s.inner }

// flakyTodoSource fails every method for the workspaces a spec names, with
// whatever error it names — the seam for the failure-classification, health
// and backoff specs.
type flakyTodoSource struct {
	*todoSource

	mu    sync.Mutex
	fail  map[string]error
	lists int
}

func newFlakyTodoSource() *flakyTodoSource {
	return &flakyTodoSource{todoSource: newTodoSource(), fail: make(map[string]error)}
}

// failFor makes every call for ws fail with err; a nil err heals it.
func (s *flakyTodoSource) failFor(ws string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err == nil {
		delete(s.fail, ws)
		return
	}
	s.fail[ws] = err
}

func (s *flakyTodoSource) listCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lists
}

func (s *flakyTodoSource) errFor(ws string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fail[ws]
}

func (s *flakyTodoSource) List(ctx context.Context, ws string, fn func(engineconnect.Item) error) error {
	s.mu.Lock()
	s.lists++
	s.mu.Unlock()
	if err := s.errFor(ws); err != nil {
		return err
	}
	return s.todoSource.List(ctx, ws, fn)
}

func (s *flakyTodoSource) Write(ctx context.Context, ws string, msg proto.Message) (string, error) {
	if err := s.errFor(ws); err != nil {
		return "", err
	}
	return s.todoSource.Write(ctx, ws, msg)
}

func (s *flakyTodoSource) Delete(ctx context.Context, ws string, prototype proto.Message, key string) error {
	if err := s.errFor(ws); err != nil {
		return err
	}
	return s.todoSource.Delete(ctx, ws, prototype, key)
}

// sourceOpener mints one source per workspace over a shared store, counting
// opens and closes — the Config.OpenSource half of the port.
type sourceOpener struct {
	store *todoSource

	mu      sync.Mutex
	opens   map[string]int
	closes  map[string]int
	fail    error
	listErr error
}

func newSourceOpener() *sourceOpener {
	return &sourceOpener{store: newTodoSource(), opens: make(map[string]int), closes: make(map[string]int)}
}

func (o *sourceOpener) open(_ context.Context, ws string) (engineconnect.Source, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.fail != nil {
		return nil, o.fail
	}
	o.opens[ws]++
	return &openedSource{todoSource: o.store, opener: o, ws: ws}, nil
}

// failWith makes opening fail; failList makes the opened source's List fail.
func (o *sourceOpener) failWith(err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.fail = err
}

func (o *sourceOpener) failList(err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.listErr = err
}

func (o *sourceOpener) counts(ws string) (opens, closes int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.opens[ws], o.closes[ws]
}

// openedSource is one workspace's handle; closing it is what a real
// implementation would use to drop a connection or a token.
type openedSource struct {
	*todoSource
	opener *sourceOpener
	ws     string
}

func (s *openedSource) List(ctx context.Context, ws string, fn func(engineconnect.Item) error) error {
	s.opener.mu.Lock()
	err := s.opener.listErr
	s.opener.mu.Unlock()
	if err != nil {
		return err
	}
	return s.todoSource.List(ctx, ws, fn)
}

func (s *openedSource) Close() error {
	s.opener.mu.Lock()
	defer s.opener.mu.Unlock()
	s.opener.closes[s.ws]++
	return nil
}

// fixture bundles a server, source, state and connector over a fresh store.
type fixture struct {
	srv  *server.Server
	src  *todoSource
	st   engineconnect.State
	conn *engineconnect.Connector
}

func newFixture(opts ...func(*engineconnect.Config)) *fixture {
	GinkgoHelper()
	return newFixtureFull(nil, opts...)
}

func newFixtureFull(srvOpts []server.Option, opts ...func(*engineconnect.Config)) *fixture {
	GinkgoHelper()
	return newFixtureOver(srvOpts, newTodoSource(), opts...)
}

// newFixtureOver builds a fixture over a specific source; fixture.src is the
// todo store underneath it, whatever wrapper the spec chose.
func newFixtureOver(srvOpts []server.Option, src todoBacked, opts ...func(*engineconnect.Config)) *fixture {
	GinkgoHelper()
	store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "engine.db"))
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

	srv, err := server.New(store, append(srvOpts,
		server.WithEntities(&todov1.Todo{}, &testdatav1.Widget{}))...)
	Expect(err).NotTo(HaveOccurred())

	st, err := sqlitestate.Open(filepath.Join(GinkgoT().TempDir(), "state.db"))
	Expect(err).NotTo(HaveOccurred())

	cfg := engineconnect.Config{
		Server:   srv,
		Source:   src,
		State:    st,
		Entities: []proto.Message{&todov1.Todo{}},
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	conn, err := engineconnect.New(cfg)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { Expect(conn.Close()).To(Succeed()) })
	return &fixture{srv: srv, src: src.base(), st: cfg.State, conn: conn}
}

// pushOf builds a single-workspace push; a PushRequest carries one batch per
// workspace.
func pushOf(ws string, muts []*syncplanev1.Mutation) *syncplanev1.PushRequest {
	return &syncplanev1.PushRequest{Batches: []*syncplanev1.WorkspaceBatch{{WorkspaceId: ws, Mutations: muts}}}
}

func (f *fixture) push(ctx context.Context, ws string, m *syncplanev1.Mutation) {
	GinkgoHelper()
	resp, err := f.srv.Push(ctx, connect.NewRequest(pushOf(
		ws,
		[]*syncplanev1.Mutation{m})))
	Expect(err).NotTo(HaveOccurred())
	Expect(resp.Msg.GetResults()).To(HaveLen(1))
	Expect(resp.Msg.GetResults()[0].GetErrorCode()).To(BeEmpty())
	for _, r := range resp.Msg.GetResults()[0].GetResults() {
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
	}
}

// engineTodo reads a todo from the engine store; nil when absent/deleted.
func (f *fixture) engineTodo(ctx context.Context, ws, id string) *todov1.Todo {
	GinkgoHelper()
	var got *todov1.Todo
	Expect(f.srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
		var todo todov1.Todo
		found, err := tx.Get(&todo, id)
		if err != nil {
			return err
		}
		if found {
			got = &todo
		}
		return nil
	})).To(Succeed())
	return got
}

// currentSeq reads the workspace's high-water sequence via the change feed.
// The probe cursor deliberately overshoots; the feed reports that as
// ErrCursorTooOld (a real consumer cursor ahead of current means a recreated
// or restored store) but still returns current as the resume position, which
// is exactly the high-water mark this helper wants.
func (f *fixture) currentSeq(ctx context.Context, ws string) uint64 {
	GinkgoHelper()
	next, _, err := f.srv.Changes(ctx, ws, server.Cursor{Seq: ^uint64(0) >> 1}, 1, func(server.Change) error { return nil })
	if err != nil {
		Expect(err).To(MatchError(server.ErrCursorTooOld))
	}
	return next.Seq
}

func mustMarshal(m proto.Message) []byte {
	GinkgoHelper()
	raw, err := proto.Marshal(m)
	Expect(err).NotTo(HaveOccurred())
	return raw
}

func putMutation(clientID string, mutationID uint64, entity, key string, value proto.Message) *syncplanev1.Mutation {
	return &syncplanev1.Mutation{
		ClientId:   clientID,
		MutationId: mutationID,
		Payload: &syncplanev1.Mutation_Crud{Crud: &syncplanev1.CrudMutation{
			Entity: entity,
			Key:    key,
			Op:     syncplanev1.CrudOp_CRUD_OP_PUT,
			Value:  mustMarshal(value),
		}},
	}
}

func deleteMutation(clientID string, mutationID uint64, entity, key string) *syncplanev1.Mutation {
	return &syncplanev1.Mutation{
		ClientId:   clientID,
		MutationId: mutationID,
		Payload: &syncplanev1.Mutation_Crud{Crud: &syncplanev1.CrudMutation{
			Entity: entity,
			Key:    key,
			Op:     syncplanev1.CrudOp_CRUD_OP_DELETE,
		}},
	}
}
