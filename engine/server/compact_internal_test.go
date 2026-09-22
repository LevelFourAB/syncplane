package server

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

// quieten writes one delta at seq and prunes the log through it, which is how a
// workspace comes to hold rows the sweep may compact. It leaves the field-delta
// horizon at seq.
func quieten(t *testing.T, store storage.Adapter, ws string, seq uint64) {
	t.Helper()
	ctx := context.Background()
	if err := store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
		return tx.PutFieldDelta("widget", "k", "body", seq, []byte("frag"))
	}); err != nil {
		t.Fatalf("logging delta: %v", err)
	}
	if err := store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
		_, err := tx.PruneFieldDeltas(time.Now().Add(time.Hour))
		return err
	}); err != nil {
		t.Fatalf("pruning deltas: %v", err)
	}
}

// The mark's whole job on the discovery side: a workspace the sweep has caught
// up with must not be selected again until the delta prune moves its horizon.
// Without it every workspace holding any quiet row is due on every pass, which
// is what kept compaction out of the GC work query in the first place.
func TestCompactAllTextVisitsOnlyWorkspacesBehindTheHorizon(t *testing.T) {
	ctx := context.Background()
	base, err := sqlitestore.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	defer func() { _ = base.Close() }()
	store := &countingStore{Adapter: base}
	locker := &fakeLocker{}
	s, err := New(store, WithLeases(locker), WithTextCompaction(1))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	if err := store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
		return tx.PutRow("widget", "k", []byte("v"), 1, storage.Actor{})
	}); err != nil {
		t.Fatalf("seeding: %v", err)
	}

	store.writes, locker.tried = 0, nil
	if err := s.compactAllText(ctx); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if store.writes != 0 || len(locker.tried) != 0 {
		t.Fatalf("a workspace whose log has never been pruned holds nothing quiet: writes = %d, leases = %v",
			store.writes, locker.tried)
	}

	quieten(t, base, "ws1", 1)
	store.writes, locker.tried = 0, nil
	if err := s.compactAllText(ctx); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if want := []string{lockerKey(leaseRoleCompact, "ws1")}; !equal(locker.tried, want) {
		t.Fatalf("leases probed = %v, want %v", locker.tried, want)
	}
	if store.writes != 1 {
		t.Fatalf("write transactions = %d, want 1 — the batch that swept to the horizon", store.writes)
	}

	store.writes, locker.tried = 0, nil
	if err := s.compactAllText(ctx); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if store.writes != 0 || len(locker.tried) != 0 {
		t.Fatalf("a swept workspace must cost the next pass nothing: writes = %d, leases = %v",
			store.writes, locker.tried)
	}
}

// Lowering the threshold re-sweeps, since rows the earlier pass skipped may
// clear the lower bar; raising it does not, because a row skipped under the old
// threshold is skipped under the higher one too. The asymmetry is what keeps a
// rolling deploy that raises the threshold from re-sweeping every workspace on
// every node while the two versions disagree.
func TestCompactTextResweepsOnlyWhenTheThresholdDrops(t *testing.T) {
	ctx := context.Background()
	store, err := sqlitestore.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	defer func() { _ = store.Close() }()
	s, err := New(store, WithTextCompaction(10))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	if err := store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
		return tx.PutRow("widget", "k", []byte("v"), 1, storage.Actor{})
	}); err != nil {
		t.Fatalf("seeding: %v", err)
	}
	quieten(t, store, "ws1", 1)
	if _, err := s.CompactText(ctx, "ws1"); err != nil {
		t.Fatalf("compact: %v", err)
	}

	due := func(minPurged int) bool {
		t.Helper()
		ids, err := store.ListCompactableWorkspaceIDs(ctx, minPurged, storage.Range{}, 10)
		if err != nil {
			t.Fatalf("listing compactable: %v", err)
		}
		return len(ids) == 1
	}
	if due(10) {
		t.Fatal("a workspace swept at the running threshold must not be due again")
	}
	if due(50) {
		t.Fatal("raising the threshold selects nothing: a row skipped at 10 is skipped at 50")
	}
	if !due(5) {
		t.Fatal("lowering the threshold must re-select: a row skipped at 10 may clear 5")
	}
}

// The row count alone does not bound what a batch takes on, because a text row
// is as large as the document in it — which is the case WithTextCompaction is
// recommended for. Rows well under the batch count but over the byte cap have to
// end the batch anyway, or the transaction holding the workspace's write lock is
// bounded by nothing.
func TestCompactTextEndsABatchOnTheByteCap(t *testing.T) {
	ctx := context.Background()
	base, err := sqlitestore.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	defer func() { _ = base.Close() }()
	store := &countingStore{Adapter: base}
	// widget carries a text field, so its rows are the ones a batch collects.
	s, err := New(store, WithEntities(&testdatav1.Widget{}), WithTextCompaction(1))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	// Two rows fill the cap exactly, so four of them are two full batches and a
	// third that finds the window empty. Uncapped they are one batch, since four
	// rows is far inside the default count.
	const rows = 4
	value, err := proto.Marshal(&testdatav1.Widget{
		Id:    "k",
		Label: strings.Repeat("a", compactBatchBytes/2),
	})
	if err != nil {
		t.Fatalf("marshalling row: %v", err)
	}
	for i := range rows {
		if err := store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			return tx.PutRow("widget", fmt.Sprintf("k%d", i), value, uint64(i+1), storage.Actor{})
		}); err != nil {
			t.Fatalf("seeding: %v", err)
		}
	}
	quieten(t, base, "ws1", rows)

	store.writes = 0
	if _, err := s.CompactText(ctx, "ws1"); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if want := rows/2 + 1; store.writes != want {
		t.Fatalf("batches = %d, want %d — two rows per batch plus the one that finds the window empty",
			store.writes, want)
	}
	ids, err := base.ListCompactableWorkspaceIDs(ctx, 1, storage.Range{}, 10)
	if err != nil {
		t.Fatalf("listing compactable: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("workspaces still due = %v, want none — a capped batch must still finish the sweep", ids)
	}
}

// The reason the mark carries a full (seq, entity, key) sub-position rather
// than a bare sequence. One mutation writes every row it touches at ONE
// sequence, so a batch smaller than such a group splits it — and a bare
// sequence cannot name where the split fell. Recording the group's sequence
// resumes PAST it and the rest of the group is never considered; recording the
// one below it resumes before the rows already swept and never gets past the
// group at all. Neither is visible from outside: the sweep reports success and
// the work query reports the workspace caught up.
//
// What pins it is how many batches the sweep takes. Ten rows at one sequence
// against a batch of one is ten batches plus the one that finds the window
// empty; a mark that skipped the group's tail would finish in two.
func TestCompactTextResumesInsideAVersionGroupLargerThanTheBatch(t *testing.T) {
	// Bounded so a mark that fails to advance fails as a deadline rather than
	// hanging the suite until the whole run times out.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	base, err := sqlitestore.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	defer func() { _ = base.Close() }()
	store := &countingStore{Adapter: base}
	s, err := New(store, WithTextCompaction(1), WithTextCompactionPacing(time.Minute, 1))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	const rows = 10
	if err := store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
		for i := range rows {
			if err := tx.PutRow("widget", fmt.Sprintf("k%02d", i), []byte("v"), 4, storage.Actor{}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seeding: %v", err)
	}
	quieten(t, base, "ws1", 4)

	store.writes = 0
	if _, err := s.CompactText(ctx, "ws1"); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if want := rows + 1; store.writes != want {
		t.Fatalf("batches = %d, want %d — one per row of the split group, plus the one that finds the window empty",
			store.writes, want)
	}
	if err := base.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
		mark, err := tx.TextCompactMark()
		if err != nil {
			return err
		}
		if want := (storage.TextCompactMark{Seq: 4, MinPurged: 1}); mark != want {
			t.Fatalf("mark = %+v, want %+v — the sweep must finish at a clean boundary on the horizon", mark, want)
		}
		return nil
	}); err != nil {
		t.Fatalf("reading mark: %v", err)
	}
	ids, err := base.ListCompactableWorkspaceIDs(ctx, 1, storage.Range{}, 10)
	if err != nil {
		t.Fatalf("listing compactable: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("workspaces still due = %v, want none", ids)
	}
}
