package prefixstore_test

import (
	"context"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/prefixstore"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

// A multi-app host gives each app the prefix "<appID>/". App ids and workspace
// ids share one alphabet (letters, digits, `_`, `-`, `:`), so the specs below
// pick app ids whose prefixes sit next to each other in byte order, or that
// could be confused with an app id plus part of a workspace id. Every work
// query of one partition must answer for that partition alone, and a write
// through one partition must never reach another's rows.
var _ = Describe("Neighbouring app partitions over one store", func() {
	// Byte order of the separators involved: '-' 0x2d < '/' 0x2f < '0' 0x30 <
	// ':' 0x3a < 'A' 0x41 < '_' 0x5f < 'a' 0x61. "a" is the partition the
	// others crowd around.
	apps := []string{"a", "a-", "a0", "a:b", "a_", "aa", "A", "b"}

	var (
		ctx   context.Context
		root  storage.Adapter
		parts map[string]storage.Adapter
	)

	BeforeEach(func() {
		ctx = context.Background()
		store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "test.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		root = store
		parts = map[string]storage.Adapter{}
		for _, app := range apps {
			parts[app] = prefixstore.Wrap(store, app+"/")
		}
	})

	// seed writes, in every partition, one workspace per id in wss holding a
	// tombstone (collectable), a client watermark (collectable when idle) and
	// a pruned text-delta log (compactable).
	seed := func(wss ...string) {
		GinkgoHelper()
		for _, app := range apps {
			for _, ws := range wss {
				Expect(parts[app].WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
					seq, err := tx.NextSeq()
					if err != nil {
						return err
					}
					if err := tx.PutRow("widget", "live", []byte(app+"|"+ws), seq, storage.Actor{}); err != nil {
						return err
					}
					if err := tx.DeleteRow("widget", "gone", seq, storage.Actor{}); err != nil {
						return err
					}
					if err := tx.PutRow("hist", "todo/t1/1", []byte("h"), seq, storage.Actor{}); err != nil {
						return err
					}
					return tx.SetClientLastMutationID("c1", 1)
				})).To(Succeed())
			}
		}
	}

	// Given app "a" with workspace "b:c" and app "a:b" with workspace "c",
	// When each writes its row, Then the two rows are separate and each reads
	// back its own. Must NOT: share one stored workspace.
	It("keeps app a / workspace b:c apart from app a:b / workspace c", func() {
		put := func(s storage.Adapter, ws, value string) {
			GinkgoHelper()
			Expect(s.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
				seq, err := tx.NextSeq()
				if err != nil {
					return err
				}
				return tx.PutRow("widget", "k", []byte(value), seq, storage.Actor{})
			})).To(Succeed())
		}
		put(parts["a"], "b:c", "app a")
		put(parts["a:b"], "c", "app a:b")

		for app, ws := range map[string]string{"a": "b:c", "a:b": "c"} {
			Expect(parts[app].WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
				row, err := tx.GetRow("widget", "k")
				Expect(err).NotTo(HaveOccurred())
				Expect(string(row.Value)).To(Equal("app " + app))
				Expect(tx.CurrentSeq()).To(BeEquivalentTo(1))
				return nil
			})).To(Succeed())
		}
		Expect(root.ListWorkspaceIDs(ctx, storage.Range{})).To(ConsistOf("a/b:c", "a:b/c"))
	})

	// Given every neighbouring partition holds the same workspace ids with
	// work owed, When each partition runs every work query unbounded, Then it
	// sees exactly its own workspaces. Must NOT: list a neighbour's workspace,
	// which a background pass would then open a transaction against.
	It("bounds every work query to its own partition", func() {
		seed("w1", "w2:x", "z")
		cutoffs := storage.GCCutoffs{
			Tombstones: time.Now().Add(time.Hour),
			Clients:    time.Now().Add(time.Hour),
		}
		for _, app := range apps {
			p := parts[app]
			want := []string{"w1", "w2:x", "z"}
			Expect(p.ListWorkspaceIDs(ctx, storage.Range{})).To(ConsistOf(want), app)
			Expect(p.ListCollectableWorkspaceIDs(ctx, cutoffs, storage.Range{}, 100)).To(ConsistOf(want), app)
			Expect(p.ListStaleIndexWorkspaceIDs(ctx, "mark", storage.Range{}, 100)).To(ConsistOf(want), app)
			Expect(p.ListPendingWorkspaceIDs(ctx, "indexer", storage.Range{}, 100)).To(ConsistOf(want), app)
		}
	})

	// Given the partitions above, When app "a" pages its workspaces one at
	// a time from an exclusive cursor, Then the pages cover exactly its own
	// three workspaces in order and then stop. Must NOT: end early on a page
	// a neighbour filled, or continue into one.
	It("pages through its own partition and nothing else", func() {
		seed("w1", "w2:x", "z")
		var seen []string
		rng := storage.Range{}
		for range 10 {
			page, err := parts["a"].ListPendingWorkspaceIDs(ctx, "indexer", rng, 1)
			Expect(err).NotTo(HaveOccurred())
			if len(page) == 0 {
				break
			}
			seen = append(seen, page...)
			rng.After = page[len(page)-1]
		}
		Expect(seen).To(Equal([]string{"w1", "w2:x", "z"}))
	})

	// Given the partitions above, When app "a" collects every tombstone,
	// idle client and history row of its workspace "w1", Then no other
	// partition loses anything. Must NOT: delete a neighbour's rows.
	It("collects and purges inside its own partition only", func() {
		seed("w1")
		Expect(parts["a"].WithWriteTx(ctx, "w1", func(tx storage.WriteTx) error {
			if _, err := tx.CollectTombstones(time.Now().Add(time.Hour)); err != nil {
				return err
			}
			if _, err := tx.CollectIdleClients(time.Now().Add(time.Hour)); err != nil {
				return err
			}
			_, _, err := tx.PurgeEntityRows("hist", "", time.Time{}, 0)
			return err
		})).To(Succeed())

		for _, app := range apps {
			Expect(parts[app].WithReadTx(ctx, "w1", func(tx storage.ReadTx) error {
				_, tombErr := tx.GetRow("widget", "gone")
				_, histErr := tx.GetRow("hist", "todo/t1/1")
				mid, err := tx.ClientLastMutationID("c1")
				Expect(err).NotTo(HaveOccurred())
				live, err := tx.GetRow("widget", "live")
				Expect(err).NotTo(HaveOccurred())
				Expect(string(live.Value)).To(Equal(app+"|w1"), "every partition keeps its own live row")
				if app == "a" {
					Expect(tombErr).To(MatchError(storage.ErrNotFound))
					Expect(histErr).To(MatchError(storage.ErrNotFound))
					Expect(mid).To(BeZero())
				} else {
					Expect(tombErr).NotTo(HaveOccurred(), app)
					Expect(histErr).NotTo(HaveOccurred(), app)
					Expect(mid).To(BeEquivalentTo(1), app)
				}
				return nil
			})).To(Succeed())
		}
	})
})
