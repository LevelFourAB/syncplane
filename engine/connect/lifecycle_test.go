package connect_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	engineconnect "github.com/levelfourab/syncplane/engine/connect"
	"github.com/levelfourab/syncplane/engine/connect/sqlitestate"
	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

// keptState is a State that outlives the connectors built over it, as a
// connector state database does when only the engine store is replaced.
type keptState struct{ engineconnect.State }

func (keptState) Close() error { return nil }

// copyStore copies a SQLite database with its WAL files, which is what a file
// backup or restore of the engine store does.
func copyStore(from, to string) {
	GinkgoHelper()
	for _, suffix := range []string{"", "-wal", "-shm"} {
		data, err := os.ReadFile(from + suffix)
		if os.IsNotExist(err) {
			_ = os.Remove(to + suffix)
			continue
		}
		Expect(err).NotTo(HaveOccurred())
		Expect(os.WriteFile(to+suffix, data, 0o600)).To(Succeed())
	}
}

var _ = Describe("Connector lifecycle", func() {
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	It("converges both directions while running", func() {
		const ws = "ws-live"
		f := newFixture(func(cfg *engineconnect.Config) {
			cfg.ReconcileInterval = 20 * time.Millisecond
		})
		Expect(f.conn.Start(ctx)).To(Succeed())

		// Engine → source, driven by the commit signal.
		f.push(ctx, ws, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "from client"}))
		Eventually(func() *todov1.Todo {
			return f.src.get(ws, "t1")
		}).Should(Not(BeNil()))
		Expect(f.src.get(ws, "t1").GetTitle()).To(Equal("from client"))

		// Source → engine, driven by Kick.
		f.src.set(ws, &todov1.Todo{Id: "t2", Title: "from source"})
		f.conn.Kick(ws)
		Eventually(func() *todov1.Todo {
			return f.engineTodo(ctx, ws, "t2")
		}).Should(Not(BeNil()))
	})

	It("exports rows that existed before Start (engine-first initial sync)", func() {
		const ws = "ws-preexisting"
		f := newFixture()
		// Rows committed long before the connector ever runs — the
		// "layer sync over my existing database" case.
		f.push(ctx, ws, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "old data"}))
		f.push(ctx, ws, putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "older data"}))

		Expect(f.conn.Start(ctx)).To(Succeed())
		Eventually(func() *todov1.Todo {
			return f.src.get(ws, "t1")
		}).Should(Not(BeNil()), "initial discovery must drain pre-existing rows")
		Eventually(func() *todov1.Todo {
			return f.src.get(ws, "t2")
		}).Should(Not(BeNil()))
		Expect(f.src.get(ws, "t1").GetTitle()).To(Equal("old data"))
	})

	It("starts a runner for a source-first workspace via Kick", func() {
		const ws = "ws-source-first"
		f := newFixture(func(cfg *engineconnect.Config) {
			// Periodic reconcile off: only the Kick may trigger the import.
			cfg.ReconcileInterval = -1
		})
		f.src.set(ws, &todov1.Todo{Id: "t1", Title: "pre-existing"})

		Expect(f.conn.Start(ctx)).To(Succeed())
		f.conn.Kick(ws)
		Eventually(func() *todov1.Todo {
			return f.engineTodo(ctx, ws, "t1")
		}).Should(Not(BeNil()))
	})

	It("rejects a second Start and stops cleanly on Close", func() {
		f := newFixture()
		Expect(f.conn.Start(ctx)).To(Succeed())
		Expect(f.conn.Start(ctx)).To(MatchError(ContainSubstring("twice")))
		// Close is exercised by the fixture's DeferCleanup; Kick after Close
		// must not panic.
		Expect(f.conn.Close()).To(Succeed())
		f.conn.Kick("ws-any")
	})
})

// useOpener rewires a fixture to mint one source per workspace.
func useOpener(o *sourceOpener) func(*engineconnect.Config) {
	return func(cfg *engineconnect.Config) {
		cfg.Source = nil
		cfg.OpenSource = o.open
	}
}

var _ = Describe("Per-workspace sources", func() {
	var ctx context.Context

	BeforeEach(func() { ctx = context.Background() })

	It("opens once per workspace and closes on Close", func() {
		opener := newSourceOpener()
		f := newFixtureOver(nil, opener.store, useOpener(opener))

		Expect(f.conn.Reconcile(ctx, "ws-a")).To(Succeed())
		Expect(f.conn.Ingress(ctx, "ws-a")).To(Succeed())
		Expect(f.conn.Drain(ctx, "ws-a")).To(Succeed())
		opens, closes := opener.counts("ws-a")
		Expect(opens).To(Equal(1), "a source is built once and reused across passes")
		Expect(closes).To(BeZero())

		Expect(f.conn.Reconcile(ctx, "ws-b")).To(Succeed())
		opens, _ = opener.counts("ws-b")
		Expect(opens).To(Equal(1), "and every workspace gets its own")

		Expect(f.conn.Close()).To(Succeed())
		_, closes = opener.counts("ws-a")
		Expect(closes).To(Equal(1))
		_, closes = opener.counts("ws-b")
		Expect(closes).To(Equal(1))
	})

	It("closes the workspace's source on Forget and reopens it on demand", func() {
		const ws = "ws-forget"
		opener := newSourceOpener()
		f := newFixtureOver(nil, opener.store, useOpener(opener))
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())

		f.conn.Forget(ws)
		opens, closes := opener.counts(ws)
		Expect(opens).To(Equal(1))
		Expect(closes).To(Equal(1))
		_, ok := f.conn.Health(ws)
		Expect(ok).To(BeFalse(), "a forgotten workspace leaves no stale status behind")

		// Shadows survive, so coming back resumes rather than re-imports.
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		opens, _ = opener.counts(ws)
		Expect(opens).To(Equal(2))
	})

	// The re-consent seam: a credential that will not heal by retrying drops
	// the cached source, so whatever a reconnect flow stored is picked up on
	// the next pass without restarting the process.
	It("rebuilds the source after a pass needs user action", func() {
		const ws = "ws-reauth"
		opener := newSourceOpener()
		f := newFixtureOver(nil, opener.store, useOpener(opener))
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())

		opener.failList(errors.New("connection reset"))
		Expect(f.conn.Reconcile(ctx, ws)).NotTo(Succeed())
		opens, closes := opener.counts(ws)
		Expect(opens).To(Equal(1), "a transient failure keeps the source")
		Expect(closes).To(BeZero())

		opener.failList(fmt.Errorf("refresh token revoked: %w", engineconnect.ErrNeedsUserAction))
		Expect(f.conn.Reconcile(ctx, ws)).NotTo(Succeed())
		opens, closes = opener.counts(ws)
		Expect(opens).To(Equal(1))
		Expect(closes).To(Equal(1), "a credential failure drops it")

		opener.failList(nil)
		opener.store.set(ws, &todov1.Todo{Id: "t1"})
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		opens, _ = opener.counts(ws)
		Expect(opens).To(Equal(2))
		Expect(f.engineTodo(ctx, ws, "t1")).NotTo(BeNil())
	})

	It("reports a source that cannot be opened at all", func() {
		const ws = "ws-noauth"
		opener := newSourceOpener()
		f := newFixtureOver(nil, opener.store, useOpener(opener))
		opener.failWith(fmt.Errorf("no stored credential: %w", engineconnect.ErrNeedsUserAction))

		Expect(f.conn.Ingress(ctx, ws)).To(MatchError(engineconnect.ErrNeedsUserAction))
		h, ok := f.conn.Health(ws)
		Expect(ok).To(BeTrue())
		Expect(h.Kind).To(Equal(engineconnect.FailureUserAction))
		opens, _ := opener.counts(ws)
		Expect(opens).To(BeZero())
	})
})

var _ = Describe("Cursor-too-old recovery", func() {
	const ws = "ws-gc"
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	It("heals engine-side deletes and creates the feed lost to GC", func() {
		f := newFixtureFull([]server.Option{server.WithGC(-time.Second, 0)})

		// Converged baseline: t1 exists on both sides, cursor past seq 1.
		f.push(ctx, ws, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one"}))
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.get(ws, "t1")).NotTo(BeNil())

		// While the connector is "offline": t2 is created, t1 is deleted,
		// and GC collects the tombstone — the feed below the horizon is gone.
		f.push(ctx, ws, putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "two"}))
		f.push(ctx, ws, deleteMutation("c1", 3, "todo", "t1"))
		Expect(f.srv.CollectGarbage(ctx, ws)).To(Succeed())

		// The next drain hits the horizon and falls back to a full repair.
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.get(ws, "t1")).To(BeNil(), "the GC-collected delete reached the source")
		Expect(f.src.get(ws, "t2")).NotTo(BeNil(), "the skipped create reached the source")

		// Converged and quiescent afterwards.
		writes := f.src.writeCount()
		seq := f.currentSeq(ctx, ws)
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.writeCount()).To(Equal(writes))
		Expect(f.currentSeq(ctx, ws)).To(Equal(seq))
	})

	It("heals a cursor left behind by another store lineage", func() {
		const ws = "ws-swap"
		f := newFixture()

		// Converged baseline: t1 exists on both sides, cursor past seq 1.
		f.push(ctx, ws, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one"}))
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.get(ws, "t1")).NotTo(BeNil())

		cur, err := f.st.Cursor(ctx, ws)
		Expect(err).NotTo(HaveOccurred())
		Expect(cur.Epoch).NotTo(BeZero(), "the drained cursor names the store that issued it")

		// The engine store is swapped for another lineage that already holds
		// t2 above the cursor's position — the connector's own state DB
		// survives the swap, so it presents a cursor whose seq still fits.
		f.push(ctx, ws, putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "two"}))
		Expect(f.st.SetCursor(ctx, ws, server.Cursor{Seq: cur.Seq, Epoch: cur.Epoch + 1})).To(Succeed())

		// t2 must reach the source through the full repair, not by being read
		// off the feed as a change this cursor was entitled to.
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.get(ws, "t2")).NotTo(BeNil())

		healed, err := f.st.Cursor(ctx, ws)
		Expect(err).NotTo(HaveOccurred())
		Expect(healed.Epoch).To(Equal(cur.Epoch), "the cursor now names the store it was served by")

		// Converged and quiescent afterwards.
		writes := f.src.writeCount()
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.writeCount()).To(Equal(writes))
	})
	Describe("after a workspace delete and restore", func() {
		const ws = "ws-restored"

		// reseeded leaves the connector healed onto a restored workspace whose
		// t1 holds the same content as the deleted one's, where t1 was the
		// deleted workspace's write at seq 10.
		reseeded := func() *fixture {
			GinkgoHelper()
			f := newFixture()
			for i := 1; i <= 9; i++ {
				id := fmt.Sprintf("f%d", i)
				f.push(ctx, ws, putMutation("c1", uint64(i), "todo", id, &todov1.Todo{Id: id}))
			}
			f.push(ctx, ws, putMutation("c1", 10, "todo", "t1", &todov1.Todo{Id: "t1", Title: "seed"}))
			Expect(f.conn.Drain(ctx, ws)).To(Succeed())

			Expect(f.srv.DeleteWorkspace(ctx, ws)).To(Succeed())
			Expect(f.srv.RestoreWorkspace(ctx, ws)).To(Succeed())
			f.push(ctx, ws, putMutation("c2", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "seed"}))
			Expect(f.conn.Drain(ctx, ws)).To(Succeed())
			return f
		}

		It("egresses an edit the restored workspace makes below the deleted one's sequence", func() {
			// Given a healed connector over the re-seeded t1
			f := reseeded()

			// When a client edits t1 at seq 2
			f.push(ctx, ws, putMutation("c2", 2, "todo", "t1", &todov1.Todo{Id: "t1", Title: "edited"}))
			Expect(f.conn.Drain(ctx, ws)).To(Succeed())

			// Then the edit reaches the source
			Expect(f.src.get(ws, "t1").GetTitle()).To(Equal("edited"),
				"a shadow from the deleted workspace took the edit for the connector's own write")
		})

		It("egresses an edit the restored workspace makes above the deleted one's sequence", func() {
			f := reseeded()
			for i := 2; i <= 10; i++ {
				f.push(ctx, ws, putMutation("c2", uint64(i), "todo", "w", &todov1.Todo{Id: "w", Title: fmt.Sprint(i)}))
			}
			Expect(f.currentSeq(ctx, ws)).To(BeNumerically(">=", 10))

			f.push(ctx, ws, putMutation("c2", 11, "todo", "t1", &todov1.Todo{Id: "t1", Title: "edited"}))
			Expect(f.conn.Drain(ctx, ws)).To(Succeed())

			Expect(f.src.get(ws, "t1").GetTitle()).To(Equal("edited"))
		})
	})

	It("keeps the shadows of unchanged rows across a heal from the tombstone horizon", func() {
		const ws = "ws-horizon"
		f := newFixtureFull([]server.Option{server.WithGC(-time.Second, 0)})
		f.push(ctx, ws, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one"}))
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		before, err := f.st.Shadow(ctx, ws, "todo", "t1")
		Expect(err).NotTo(HaveOccurred())

		f.push(ctx, ws, putMutation("c1", 2, "todo", "gone", &todov1.Todo{Id: "gone"}))
		f.push(ctx, ws, deleteMutation("c1", 3, "todo", "gone"))
		Expect(f.srv.CollectGarbage(ctx, ws)).To(Succeed())
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		writes := f.src.writeCount()

		after, err := f.st.Shadow(ctx, ws, "todo", "t1")
		Expect(err).NotTo(HaveOccurred())
		Expect(after.EngineVersion).To(Equal(before.EngineVersion))
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.writeCount()).To(Equal(writes))
	})
	It("keeps a row made at the source after the engine backup it is restored from", func() {
		const ws = "ws-backup"
		dir := GinkgoT().TempDir()
		live, backup := filepath.Join(dir, "engine.db"), filepath.Join(dir, "backup.db")
		src := newTodoSource()
		st, err := sqlitestate.Open(filepath.Join(dir, "state.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(st.Close()).To(Succeed()) })

		// open starts an engine over the live store file and a connector over
		// the one connector state, and returns how to stop both.
		open := func() (*fixture, func()) {
			GinkgoHelper()
			store, err := sqlitestore.Open(live)
			Expect(err).NotTo(HaveOccurred())
			srv, err := server.New(store, server.WithEntities(&todov1.Todo{}, &testdatav1.Widget{}))
			Expect(err).NotTo(HaveOccurred())
			conn, err := engineconnect.New(engineconnect.Config{
				Server: srv, Source: src, State: keptState{st}, Entities: []proto.Message{&todov1.Todo{}},
			})
			Expect(err).NotTo(HaveOccurred())
			return &fixture{srv: srv, src: src, st: st, conn: conn}, func() {
				GinkgoHelper()
				Expect(conn.Close()).To(Succeed())
				Expect(srv.Close()).To(Succeed())
				Expect(store.Close()).To(Succeed())
			}
		}

		// Given an engine backup at seq 1, then s1 made at the source and
		// imported at seq 2
		f, stop := open()
		f.push(ctx, ws, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one"}))
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		stop()
		copyStore(live, backup)
		f, stop = open()
		src.set(ws, &todov1.Todo{Id: "s1", Title: "made at the source"})
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		f.push(ctx, ws, putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "two"}))
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.engineTodo(ctx, ws, "s1")).NotTo(BeNil())
		stop()

		// When the engine store is restored from the backup, with the
		// connector state kept, and the connector heals
		copyStore(backup, live)
		f, stop = open()
		DeferCleanup(stop)
		f.push(ctx, ws, putMutation("c9", 1, "todo", "r1", &todov1.Todo{Id: "r1", Title: "after restore"}))
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())

		// Then s1 is still at the source, and the engine holds it again
		Expect(src.get(ws, "s1")).NotTo(BeNil(), "a row the user made in the external system was deleted there")
		Expect(f.engineTodo(ctx, ws, "s1")).NotTo(BeNil())
	})

	It("does not delete the source rows of a workspace that was deleted and restored", func() {
		const ws = "ws-restored"
		// Given a workspace synced with f1..f9 at the source
		f := newFixture()
		for i := 1; i <= 9; i++ {
			id := fmt.Sprintf("f%d", i)
			f.push(ctx, ws, putMutation("c1", uint64(i), "todo", id, &todov1.Todo{Id: id}))
		}
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())

		// When the workspace is deleted, restored and written once, and the
		// connector heals
		Expect(f.srv.DeleteWorkspace(ctx, ws)).To(Succeed())
		Expect(f.srv.RestoreWorkspace(ctx, ws)).To(Succeed())
		f.push(ctx, ws, putMutation("c2", 1, "todo", "t1", &todov1.Todo{Id: "t1"}))
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())

		// Then the heal treats the new workspace as an initial sync, and
		// keeps the source's rows
		for i := 1; i <= 9; i++ {
			Expect(f.src.get(ws, fmt.Sprintf("f%d", i))).NotTo(BeNil(),
				"shadows from the deleted workspace decided to delete f%d at the source", i)
		}
	})
})

var _ = Describe("Multiple workspaces", func() {
	It("drain one workspace without touching another", func() {
		ctx := context.Background()
		f := newFixture()
		f.push(ctx, "ws-a", putMutation("c1", 1, "todo", "a1", &todov1.Todo{Id: "a1"}))
		f.push(ctx, "ws-b", putMutation("c1", 1, "todo", "b1", &todov1.Todo{Id: "b1"}))

		Expect(f.conn.Drain(ctx, "ws-a")).To(Succeed())
		Expect(f.src.get("ws-a", "a1")).NotTo(BeNil())
		Expect(f.src.get("ws-b", "b1")).To(BeNil(), "ws-b has not drained")

		Expect(f.conn.Drain(ctx, "ws-b")).To(Succeed())
		Expect(f.src.get("ws-b", "b1")).NotTo(BeNil())
	})
})
