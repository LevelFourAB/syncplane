package server

import (
	"context"
	"path/filepath"
	"testing"

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
