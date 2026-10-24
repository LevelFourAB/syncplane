package server

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/levelfourab/syncplane/engine/storage"
)

// fakePendingStore answers the pending-workspace query from a fixed, sorted
// list, paged as an adapter pages it, and fails the page after failAfter when
// failAfter is set.
type fakePendingStore struct {
	fakeGCStore
	pending   []string
	failAfter string
	calls     int
}

func (f *fakePendingStore) ListPendingWorkspaceIDs(_ context.Context, _ string, rng storage.Range, limit int) ([]string, error) {
	f.calls++
	if f.failAfter != "" && rng.After == f.failAfter {
		return nil, errors.New("boom")
	}
	return pageIDs(f.pending, rng, limit), nil
}

func pendingIDs(n int) []string {
	ids := make([]string, n)
	for i := range n {
		ids[i] = fmt.Sprintf("ws%05d", i)
	}
	return ids
}

func TestPendingWorkspacesYieldsEveryPage(t *testing.T) {
	for _, n := range []int{0, 1, pendingPage, 2*pendingPage + 7} {
		store := &fakePendingStore{pending: pendingIDs(n)}
		s := &Server{store: store}

		var got []string
		for id, err := range s.PendingWorkspaces(context.Background(), "indexer") {
			if err != nil {
				t.Fatalf("n=%d: %v", n, err)
			}
			got = append(got, id)
		}
		if !slices.Equal(got, store.pending) {
			t.Fatalf("n=%d: got %d ids, want %d", n, len(got), len(store.pending))
		}
	}
}

func TestPendingWorkspacesStopsWhenTheLoopBreaks(t *testing.T) {
	store := &fakePendingStore{pending: pendingIDs(2 * pendingPage)}
	s := &Server{store: store}

	for range s.PendingWorkspaces(context.Background(), "indexer") {
		break
	}
	if store.calls != 1 {
		t.Fatalf("store queried %d times after a break on the first id, want 1", store.calls)
	}
}

func TestPendingWorkspacesYieldsAPageErrorAndStops(t *testing.T) {
	ids := pendingIDs(2 * pendingPage)
	store := &fakePendingStore{pending: ids, failAfter: ids[pendingPage-1]}
	s := &Server{store: store}

	var (
		got  []string
		errs []error
	)
	for id, err := range s.PendingWorkspaces(context.Background(), "indexer") {
		if err != nil {
			errs = append(errs, err)
			continue
		}
		got = append(got, id)
	}
	if len(got) != pendingPage || len(errs) != 1 {
		t.Fatalf("got %d ids and %d errors, want %d ids and 1 error", len(got), len(errs), pendingPage)
	}
}

func TestPendingWorkspacesRefusesAnInvalidConsumerName(t *testing.T) {
	s := &Server{store: &fakePendingStore{}}

	for _, err := range s.PendingWorkspaces(context.Background(), "") {
		if err == nil {
			t.Fatal("yielded an id for an empty consumer name")
		}
		return
	}
	t.Fatal("yielded nothing for an empty consumer name")
}
