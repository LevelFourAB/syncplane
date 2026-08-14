package server

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/levelfourab/syncplane/engine/lease"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

// fakeGCStore is a minimal Adapter that records which workspaces a GC pass
// reached and can be told to fail (or cancel the pass) for specific ones. It
// resolves CollectGarbage at the WithWriteTx seam, so it never needs a real
// WriteTx.
type fakeGCStore struct {
	ids       []string
	failOn    map[string]bool
	onWrite   func(ws string) // optional side effect (e.g. cancel the pass)
	collected []string
}

func (f *fakeGCStore) ListWorkspaceIDs(context.Context) ([]string, error) { return f.ids, nil }
func (f *fakeGCStore) Close() error                                       { return nil }

// The background passes reach every workspace this fake holds, so both work
// queries answer the whole list. Paging it for real is what lets a test assert
// that a pass covers more workspaces than one page carries.
func (f *fakeGCStore) ListCollectableWorkspaceIDs(_ context.Context, _ storage.GCCutoffs, after string, limit int) ([]string, error) {
	return pageIDs(f.ids, after, limit), nil
}

func (f *fakeGCStore) ListStaleIndexWorkspaceIDs(_ context.Context, _, after string, limit int) ([]string, error) {
	return pageIDs(f.ids, after, limit), nil
}

func (f *fakeGCStore) ListCompactableWorkspaceIDs(_ context.Context, _ int, after string, limit int) ([]string, error) {
	return pageIDs(f.ids, after, limit), nil
}

// pageIDs is the paging every work query owes its caller: ids strictly after
// `after`, in order, at most limit of them. ids is assumed sorted, as an
// adapter's ORDER BY makes it.
func pageIDs(ids []string, after string, limit int) []string {
	var out []string
	for _, id := range ids {
		if id <= after {
			continue
		}
		if len(out) == limit {
			break
		}
		out = append(out, id)
	}
	return out
}

// GC sweeps every workspace, not a consumer's pending ones, so this fake never
// answers it.
func (f *fakeGCStore) ListPendingWorkspaceIDs(context.Context, string, string, int) ([]string, error) {
	return nil, errors.New("unused")
}

func (f *fakeGCStore) Namespace(string) storage.Adapter { return f }
func (f *fakeGCStore) PutAppSchema(context.Context, string, []byte, string) error {
	return errors.New("unused")
}
func (f *fakeGCStore) ListAppSchemas(context.Context) ([]storage.AppSchema, error) { return nil, nil }
func (f *fakeGCStore) DeleteAppSchema(context.Context, string) error               { return nil }

func (f *fakeGCStore) WithReadTx(context.Context, string, func(storage.ReadTx) error) error {
	return errors.New("unused")
}

func (f *fakeGCStore) WithWriteTx(_ context.Context, ws string, _ func(storage.WriteTx) error) error {
	if f.failOn[ws] {
		return fmt.Errorf("boom %s", ws)
	}
	f.collected = append(f.collected, ws)
	if f.onWrite != nil {
		f.onWrite(ws)
	}
	return nil
}

func TestCollectAllGarbageSkipsFailingWorkspaces(t *testing.T) {
	store := &fakeGCStore{
		ids:    []string{"a", "b", "c"},
		failOn: map[string]bool{"b": true},
	}
	s := &Server{store: store, leases: lease.NewLocal(), gc: &gcLoop{retention: time.Hour, configured: true}}

	err := s.collectAllGarbage(context.Background())
	if err == nil {
		t.Fatal("expected the failing workspace to surface an error")
	}
	if got := err.Error(); !contains(got, "workspace b") {
		t.Fatalf("error should name the failing workspace, got %q", got)
	}
	// The failure must not starve the workspaces after it in listing order.
	if want := []string{"a", "c"}; !equal(store.collected, want) {
		t.Fatalf("collected = %v, want %v", store.collected, want)
	}
}

func TestCollectAllGarbageStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &fakeGCStore{ids: []string{"a", "b", "c"}}
	// Cancel right after the first workspace is collected; the loop's
	// top-of-iteration check must then abort before touching the rest.
	store.onWrite = func(ws string) {
		if ws == "a" {
			cancel()
		}
	}
	s := &Server{store: store, leases: lease.NewLocal(), gc: &gcLoop{retention: time.Hour, configured: true}}

	err := s.collectAllGarbage(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if want := []string{"a"}; !equal(store.collected, want) {
		t.Fatalf("pass should have aborted early: collected = %v, want %v", store.collected, want)
	}
}

// countingStore counts the write transactions a pass opens. Embedding the real
// adapter keeps everything else — including the entity purge under test — exactly
// as it ships.
type countingStore struct {
	storage.Adapter
	writes int
}

func (c *countingStore) WithWriteTx(ctx context.Context, ws string, fn func(storage.WriteTx) error) error {
	c.writes++
	return c.Adapter.WithWriteTx(ctx, ws, fn)
}

// plantExpiredHistory writes n history rows straight to the store, bypassing the
// write path: what the sweep selects is the entity and the age, and neither
// depends on a record decoding.
func plantExpiredHistory(t *testing.T, store storage.Adapter, ws string, n int) {
	t.Helper()
	if err := store.WithWriteTx(context.Background(), ws, func(tx storage.WriteTx) error {
		for i := range n {
			key := fmt.Sprintf("historied/h1/%03d", i)
			if err := tx.PutRow(historyEntityName, key, []byte("v"), uint64(i+1), storage.Actor{}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("planting history: %v", err)
	}
}

func historyRowCount(t *testing.T, store storage.Adapter, ws string) int {
	t.Helper()
	n := 0
	if err := store.WithReadTx(context.Background(), ws, func(tx storage.ReadTx) error {
		return tx.ScanEntity(historyEntityName, func(storage.Row) error {
			n++
			return nil
		})
	}); err != nil {
		t.Fatalf("counting history: %v", err)
	}
	return n
}

// The expiry sweep is the one collector whose volume scales with writes rather
// than rows, so it must not delete a whole backlog inside the pass's own
// transaction: it takes a chunk there and drains the rest in transactions of its
// own, which is what keeps any single delete short.
func TestCollectGarbageDrainsHistoryAcrossTransactions(t *testing.T) {
	defer func(prev int) { historyPurgeChunk = prev }(historyPurgeChunk)
	historyPurgeChunk = 2

	base, err := sqlitestore.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	defer func() { _ = base.Close() }()
	store := &countingStore{Adapter: base}

	s, err := New(store, WithGC(time.Hour, 0), WithHistoryRetention(time.Millisecond))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	plantExpiredHistory(t, store, "ws1", 5)
	time.Sleep(5 * time.Millisecond)

	store.writes = 0
	if err := s.CollectGarbage(context.Background(), "ws1"); err != nil {
		t.Fatalf("collect: %v", err)
	}
	if n := historyRowCount(t, store, "ws1"); n != 0 {
		t.Fatalf("the drain must reach the whole backlog: %d records left", n)
	}
	// Five records at two per chunk: the pass takes the first two, then two
	// transactions of its own take two and the last one — which, being under the
	// chunk, is also what tells the loop it is done.
	if store.writes != 3 {
		t.Fatalf("write transactions = %d, want 3 — the backlog should drain a chunk per transaction", store.writes)
	}
}

// A history that fits in one chunk must not cost the pass a second transaction:
// the drain exists for the backlog, and the ordinary pass is still one write.
func TestCollectGarbageKeepsOneTransactionWithoutABacklog(t *testing.T) {
	base, err := sqlitestore.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	defer func() { _ = base.Close() }()
	store := &countingStore{Adapter: base}

	s, err := New(store, WithGC(time.Hour, 0), WithHistoryRetention(time.Millisecond))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	plantExpiredHistory(t, store, "ws1", 3)
	time.Sleep(5 * time.Millisecond)

	store.writes = 0
	if err := s.CollectGarbage(context.Background(), "ws1"); err != nil {
		t.Fatalf("collect: %v", err)
	}
	if n := historyRowCount(t, store, "ws1"); n != 0 {
		t.Fatalf("%d records left", n)
	}
	if store.writes != 1 {
		t.Fatalf("write transactions = %d, want 1", store.writes)
	}
}

// The whole point of the work query: a workspace with nothing collectable must
// cost the pass nothing — not the write transaction that would find nothing,
// and not even the lease probe that precedes it. Multiplied by workspace count
// and node count, that difference is the maintenance floor of an idle
// deployment.
func TestCollectAllGarbageVisitsOnlyCollectableWorkspaces(t *testing.T) {
	ctx := context.Background()
	base, err := sqlitestore.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	defer func() { _ = base.Close() }()
	store := &countingStore{Adapter: base}
	locker := &fakeLocker{}
	s, err := New(store, WithGC(time.Hour, 0), WithLeases(locker))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	// idle holds one live row and nothing else; collectable also holds a
	// tombstone old enough for this pass's retention.
	if err := store.WithWriteTx(ctx, "idle", func(tx storage.WriteTx) error {
		return tx.PutRow("widget", "k", []byte("v"), 1, storage.Actor{})
	}); err != nil {
		t.Fatalf("seeding idle: %v", err)
	}
	if err := store.WithWriteTx(ctx, "collectable", func(tx storage.WriteTx) error {
		if err := tx.PutRow("widget", "k", []byte("v"), 1, storage.Actor{}); err != nil {
			return err
		}
		return tx.DeleteRow("widget", "k", 2, storage.Actor{})
	}); err != nil {
		t.Fatalf("seeding collectable: %v", err)
	}

	store.writes, locker.tried = 0, nil
	// A retention of an hour keeps the tombstone just written, so the first
	// pass has nothing to do in either workspace.
	if err := s.collectAllGarbage(ctx); err != nil {
		t.Fatalf("collect: %v", err)
	}
	if store.writes != 0 || len(locker.tried) != 0 {
		t.Fatalf("a pass with nothing due must not touch a workspace: writes = %d, leases = %v",
			store.writes, locker.tried)
	}

	// Widen the retention past the tombstone and only that workspace is due.
	// A negative retention puts the cutoff ahead of now, which is what makes
	// "old enough" independent of how much wall clock passed since the seed
	// wrote it — at a retention of zero the two land in the same millisecond
	// and the strict comparison against updated_at goes either way.
	s.gc.retention = -time.Second
	store.writes, locker.tried = 0, nil
	if err := s.collectAllGarbage(ctx); err != nil {
		t.Fatalf("collect: %v", err)
	}
	if want := []string{lockerKey(leaseRoleGC, "collectable")}; !equal(locker.tried, want) {
		t.Fatalf("leases probed = %v, want %v", locker.tried, want)
	}
	if store.writes != 1 {
		t.Fatalf("write transactions = %d, want 1 — only the collectable workspace", store.writes)
	}
}

// A deployment larger than one page must still be covered in full, which is
// what makes the page a memory bound rather than a cap on the pass.
func TestCollectAllGarbagePagesPastOnePage(t *testing.T) {
	ids := make([]string, maintenancePage+3)
	for i := range ids {
		ids[i] = fmt.Sprintf("ws%04d", i)
	}
	store := &fakeGCStore{ids: ids}
	s := &Server{store: store, leases: lease.NewLocal(), gc: &gcLoop{retention: time.Hour, configured: true}}

	if err := s.collectAllGarbage(context.Background()); err != nil {
		t.Fatalf("collect: %v", err)
	}
	if !equal(store.collected, ids) {
		t.Fatalf("collected %d workspaces, want all %d", len(store.collected), len(ids))
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
