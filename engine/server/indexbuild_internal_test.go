package server

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

// The build mark's whole job: a second pass over a workspace whose indexes are
// already built must not read their state, take its lease or open a
// transaction. At a few thousand workspaces and a handful of indexes apiece,
// what it replaces is six figures of point reads per interval per node.
func TestBuildAllIndexesVisitsOnlyStaleWorkspaces(t *testing.T) {
	ctx := context.Background()
	base, err := sqlitestore.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	defer func() { _ = base.Close() }()
	store := &countingStore{Adapter: base}
	locker := &fakeLocker{}
	s, err := New(store, WithLeases(locker))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	if err := store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
		return tx.PutRow("widget", "k", []byte("v"), 1, storage.Actor{})
	}); err != nil {
		t.Fatalf("seeding: %v", err)
	}

	store.writes, locker.tried = 0, nil
	if err := s.buildAllIndexes(ctx); err != nil {
		t.Fatalf("build: %v", err)
	}
	if want := []string{lockerKey(leaseRoleIndexes, "ws1")}; !equal(locker.tried, want) {
		t.Fatalf("leases probed = %v, want %v", locker.tried, want)
	}
	if store.writes != 1 {
		t.Fatalf("write transactions = %d, want 1 — the mark this pass recorded", store.writes)
	}

	store.writes, locker.tried = 0, nil
	if err := s.buildAllIndexes(ctx); err != nil {
		t.Fatalf("build: %v", err)
	}
	if store.writes != 0 || len(locker.tried) != 0 {
		t.Fatalf("a marked workspace must cost the next pass nothing: writes = %d, leases = %v",
			store.writes, locker.tried)
	}

	// What the pass recorded is the running catalog's mark, so a declaration
	// change — which moves it, see TestIndexBuildMarkTracksDeclarations — brings
	// the workspace back.
	ids, err := store.ListStaleIndexWorkspaceIDs(ctx, s.catalogPtr().indexBuildMark()+"x", "", 10)
	if err != nil {
		t.Fatalf("listing stale: %v", err)
	}
	if want := []string{"ws1"}; !equal(ids, want) {
		t.Fatalf("a moved mark must re-select the workspace: %v, want %v", ids, want)
	}
}

// A row's index entries are written as one set, so visiting a row builds every
// index of its entity at once. Walking per index would read the entity and
// rewrite that set once per index for no extra coverage — the difference
// between a minute and five on a corpus with a few indexes per entity.
func TestBuildIndexesWalksTheEntityOncePerPass(t *testing.T) {
	ctx := context.Background()
	store := &rowCountingStore{Adapter: newIndexBuildStore(t)}
	srv := newIndexedNoteServer(t, store)
	seedNotes(t, store, 12)

	store.rows = 0
	if err := srv.BuildIndexes(ctx, "ws1"); err != nil {
		t.Fatalf("build: %v", err)
	}
	if store.rows != 12 {
		t.Fatalf("rows visited = %d, want 12 — one pass over the entity, not one per index", store.rows)
	}
	for _, name := range noteIndexNames {
		if !readyState(t, store, name) {
			t.Fatalf("index %q not ready after the pass", name)
		}
		if got := entryRowKeys(t, store, name); len(got) != 12 {
			t.Fatalf("index %q holds %d entries, want one per row", name, len(got))
		}
	}
}

// A declaration that changes mid-walk invalidates the entries the walk is
// writing, and only for the index it changed. The pass must leave that index
// unready — and, because BuildIndexes records the workspace's build mark only
// when everything ended ready, must leave the workspace selected for the next
// pass.
func TestBuildIndexesLeavesReRegisteredIndexUnready(t *testing.T) {
	ctx := context.Background()
	base := newIndexBuildStore(t)
	// Restamping one index's state row with a fingerprint the running catalog
	// does not hold is exactly what a re-registration does to storage.
	store := &restampingStore{Adapter: base, index: noteIndexNames[0], specHash: "some-other-spec"}
	srv := newIndexedNoteServer(t, store)
	seedNotes(t, store, 6)

	if err := srv.BuildIndexes(ctx, "ws1"); err != nil {
		t.Fatalf("build: %v", err)
	}
	if readyState(t, store, noteIndexNames[0]) {
		t.Fatalf("index %q was re-registered mid-walk and must not be marked ready", noteIndexNames[0])
	}
	for _, name := range noteIndexNames[1:] {
		if !readyState(t, store, name) {
			t.Fatalf("index %q was untouched by the re-registration and should be ready", name)
		}
	}
	ids, err := store.ListStaleIndexWorkspaceIDs(ctx, srv.catalogPtr().indexBuildMark(), "", 10)
	if err != nil {
		t.Fatalf("listing stale: %v", err)
	}
	if want := []string{"ws1"}; !equal(ids, want) {
		t.Fatalf("stale workspaces = %v, want %v — a pass that left an index unready records no mark", ids, want)
	}
}

// The time budget is what bounds the stall a build imposes on writers, so it
// has to be able to cut a batch to a single row without the walk losing its
// place or its termination condition.
func TestBuildIndexesFinishesUnderATinyTimeBudget(t *testing.T) {
	ctx := context.Background()
	store := &rowCountingStore{Adapter: newIndexBuildStore(t)}
	// A budget below the cost of one row cuts every batch after the first row,
	// which is the extreme the row cap can never reach.
	srv := newIndexedNoteServer(t, store)
	srv.indexBuild.txBudget = time.Nanosecond
	seedNotes(t, store, 12)

	store.rows = 0
	if err := srv.BuildIndexes(ctx, "ws1"); err != nil {
		t.Fatalf("build: %v", err)
	}
	if store.rows != 12 {
		t.Fatalf("rows visited = %d, want 12 — every row exactly once across the cut batches", store.rows)
	}
	for _, name := range noteIndexNames {
		if !readyState(t, store, name) {
			t.Fatalf("index %q not ready: a cut batch must resume, not abandon the walk", name)
		}
		if got := entryRowKeys(t, store, name); len(got) != 12 {
			t.Fatalf("index %q holds %d entries, want one per row", name, len(got))
		}
	}
}

// noteIndexNames are the indexes newIndexedNoteServer's entity declares, in no
// particular order: two desugared field marks and one composite.
var noteIndexNames = []string{"__field_state", "__field_rank", "by_state_rank"}

var noteSpecForBuild = DynamicEntity{
	Name:      "note",
	KeyFields: []string{"id"},
	Fields: []DynamicField{
		{Name: "id", Number: 1, Kind: protoreflect.StringKind},
		{Name: "state", Number: 2, Kind: protoreflect.StringKind, Indexed: true},
		{Name: "rank", Number: 3, Kind: protoreflect.Int64Kind, Indexed: true},
	},
	Indexes: []DynamicIndex{{
		Name: "by_state_rank",
		Key:  []DynamicIndexKey{{Path: "state"}, {Path: "rank"}},
	}},
}

func newIndexBuildStore(t *testing.T) storage.Adapter {
	t.Helper()
	store, err := sqlitestore.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// newIndexedNoteServer registers an entity carrying several indexes, so that a
// pass has more than one target to build for one walk.
func newIndexedNoteServer(t *testing.T, store storage.Adapter) *Server {
	t.Helper()
	srv, err := New(store)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	if err := srv.RegisterDynamicEntity(noteSpecForBuild); err != nil {
		t.Fatalf("registering note: %v", err)
	}
	return srv
}

// seedNotes writes rows straight to the store so they predate every index, the
// state a build exists to correct. Going through the write path instead would
// have maintenance write the entries first, and then the build would have
// nothing left to do.
func seedNotes(t *testing.T, store storage.Adapter, n int) {
	t.Helper()
	mt, err := BuildDynamicMessageType(noteSpecForBuild)
	if err != nil {
		t.Fatalf("building note type: %v", err)
	}
	fds := mt.Descriptor().Fields()
	if err := store.WithWriteTx(context.Background(), "ws1", func(tx storage.WriteTx) error {
		for i := range n {
			key := fmt.Sprintf("n%02d", i)
			m := mt.New()
			m.Set(fds.ByName("id"), protoreflect.ValueOfString(key))
			m.Set(fds.ByName("state"), protoreflect.ValueOfString([]string{"open", "closed"}[i%2]))
			m.Set(fds.ByName("rank"), protoreflect.ValueOfInt64(int64(i)))
			raw, err := proto.Marshal(m.Interface())
			if err != nil {
				return err
			}
			if err := tx.PutRow("note", key, raw, uint64(i+1), storage.Actor{}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seeding notes: %v", err)
	}
}

func readyState(t *testing.T, store storage.Adapter, index string) bool {
	t.Helper()
	var ready bool
	if err := store.WithReadTx(context.Background(), "ws1", func(tx storage.ReadTx) error {
		var err error
		_, ready, err = tx.IndexState("note", index)
		return err
	}); err != nil {
		t.Fatalf("reading index state: %v", err)
	}
	return ready
}

func entryRowKeys(t *testing.T, store storage.Adapter, index string) []string {
	t.Helper()
	var out []string
	if err := store.WithReadTx(context.Background(), "ws1", func(tx storage.ReadTx) error {
		return tx.ScanIndex("note", index, nil, nil, 0, storage.ScanAsc, func(_ []byte, rowKey string) error {
			out = append(out, rowKey)
			return nil
		})
	}); err != nil {
		t.Fatalf("scanning index %q: %v", index, err)
	}
	return out
}

// rowCountingStore counts the entity rows a walk visits, which is what
// separates one pass over an entity from one pass per index of it.
type rowCountingStore struct {
	storage.Adapter
	rows int
}

func (c *rowCountingStore) WithWriteTx(ctx context.Context, ws string, fn func(storage.WriteTx) error) error {
	return c.Adapter.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
		return fn(&rowCountingTx{WriteTx: tx, store: c})
	})
}

type rowCountingTx struct {
	storage.WriteTx
	store *rowCountingStore
}

func (t *rowCountingTx) ScanEntityAfter(entity, afterKey string, limit int, fn func(storage.Row) error) error {
	return t.WriteTx.ScanEntityAfter(entity, afterKey, limit, func(row storage.Row) error {
		t.store.rows++
		return fn(row)
	})
}

// restampingStore moves one index's spec fingerprint out from under the walk,
// the way a schema re-registration does, on the first row the build visits.
type restampingStore struct {
	storage.Adapter
	index    string
	specHash string
	done     bool
}

func (c *restampingStore) WithWriteTx(ctx context.Context, ws string, fn func(storage.WriteTx) error) error {
	return c.Adapter.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
		return fn(&restampingTx{WriteTx: tx, store: c})
	})
}

type restampingTx struct {
	storage.WriteTx
	store *restampingStore
}

func (t *restampingTx) ScanEntityAfter(entity, afterKey string, limit int, fn func(storage.Row) error) error {
	return t.WriteTx.ScanEntityAfter(entity, afterKey, limit, func(row storage.Row) error {
		if err := fn(row); err != nil {
			return err
		}
		if t.store.done {
			return nil
		}
		t.store.done = true
		return t.WriteTx.SetIndexState("note", t.store.index, t.store.specHash, false)
	})
}

func TestIndexBuildMarkTracksDeclarations(t *testing.T) {
	mark := func(indexes ...compiledIndex) string {
		return (&catalog{entities: map[string]*entityInfo{
			"widget": {name: "widget", indexes: indexes},
		}}).indexBuildMark()
	}
	a := compiledIndex{name: "by_title", specHash: "v1|title"}
	b := compiledIndex{name: "by_owner", specHash: "v1|owner"}

	if mark(a) == mark() {
		t.Fatal("declaring an index must move the mark")
	}
	if mark(a) == mark(compiledIndex{name: a.name, specHash: "v1|title:desc"}) {
		t.Fatal("a changed spec fingerprint must move the mark")
	}
	if mark(a, b) != mark(b, a) {
		t.Fatal("the mark must not depend on declaration order within a catalog")
	}
}
