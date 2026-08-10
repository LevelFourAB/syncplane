package server

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeLocker grants every lease except the (role, workspace) pairs in deny,
// recording what was tried and released.
type fakeLocker struct {
	mu       sync.Mutex
	deny     map[string]bool
	errOn    map[string]bool
	tried    []string
	released []string
}

func lockerKey(role, ws string) string { return role + "\x00" + ws }

func (f *fakeLocker) TryAcquire(_ context.Context, role, ws string) (func(), bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := lockerKey(role, ws)
	f.tried = append(f.tried, k)
	if f.errOn[k] {
		return nil, false, errors.New("locker down")
	}
	if f.deny[k] {
		return nil, false, nil
	}
	return func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.released = append(f.released, k)
	}, true, nil
}

func TestTryLease(t *testing.T) {
	ctx := context.Background()
	locker := &fakeLocker{
		deny:  map[string]bool{lockerKey("gc", "held"): true},
		errOn: map[string]bool{lockerKey("gc", "broken"): true},
	}
	s := &Server{leases: locker}

	release, run := s.tryLease(ctx, "gc", "free")
	if !run {
		t.Fatal("a free lease must let the pass run")
	}
	release()
	if want := []string{lockerKey("gc", "free")}; !equal(locker.released, want) {
		t.Fatalf("released = %v, want %v", locker.released, want)
	}

	if _, run := s.tryLease(ctx, "gc", "held"); run {
		t.Fatal("a held lease must skip the pass")
	}

	// A broken locker must not stop idempotent maintenance.
	release, run = s.tryLease(ctx, "gc", "broken")
	if !run {
		t.Fatal("a locker error must run the pass unleased")
	}
	release() // the no-op release must be callable
	if len(locker.released) != 1 {
		t.Fatalf("an unleased pass must not release anything: released = %v", locker.released)
	}
}

func TestCollectAllGarbageSkipsLeasedWorkspaces(t *testing.T) {
	store := &fakeGCStore{ids: []string{"a", "b", "c"}}
	locker := &fakeLocker{deny: map[string]bool{lockerKey(leaseRoleGC, "b"): true}}
	s := &Server{store: store, leases: locker, gc: &gcLoop{retention: time.Hour, configured: true}}

	if err := s.collectAllGarbage(context.Background()); err != nil {
		t.Fatalf("a skipped workspace is not a failure: %v", err)
	}
	// b's lease is held elsewhere: whoever holds it collects b.
	if want := []string{"a", "c"}; !equal(store.collected, want) {
		t.Fatalf("collected = %v, want %v", store.collected, want)
	}
	if want := []string{lockerKey(leaseRoleGC, "a"), lockerKey(leaseRoleGC, "c")}; !equal(locker.released, want) {
		t.Fatalf("every granted lease must be released: released = %v, want %v", locker.released, want)
	}
}

func TestBuildAllIndexesSkipsLeasedWorkspaces(t *testing.T) {
	// The fake fails every write transaction, so a workspace whose pass runs
	// surfaces as an error while a leased-away workspace stays silent — which
	// is exactly the observable difference the lease should make.
	store := &fakeGCStore{ids: []string{"a", "b"}, failOn: map[string]bool{"a": true, "b": true}}
	locker := &fakeLocker{deny: map[string]bool{lockerKey(leaseRoleIndexes, "b"): true}}
	s := &Server{store: store, leases: locker}
	s.cat.Store(&catalog{entities: map[string]*entityInfo{
		"widget": {name: "widget", indexes: make([]compiledIndex, 1)},
	}})

	err := s.buildAllIndexes(context.Background())
	if err == nil || !contains(err.Error(), "workspace a") {
		t.Fatalf("workspace a's pass should have run and failed, got %v", err)
	}
	if contains(err.Error(), "workspace b") {
		t.Fatalf("workspace b is leased away and must not have run: %v", err)
	}
}
