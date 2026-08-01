package server

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/levelfourab/syncplane/engine/storage"
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
	s := &Server{store: store, gc: &gcLoop{retention: time.Hour, configured: true}}

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
	s := &Server{store: store, gc: &gcLoop{retention: time.Hour, configured: true}}

	err := s.collectAllGarbage(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if want := []string{"a"}; !equal(store.collected, want) {
		t.Fatalf("pass should have aborted early: collected = %v, want %v", store.collected, want)
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
