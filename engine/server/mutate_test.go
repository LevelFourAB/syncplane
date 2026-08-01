package server_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	syncplanecursorv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/cursor/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/internal/pubsub"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

// countingNotifier counts pokes so specs can assert exactly-once delivery.
type countingNotifier struct {
	*pubsub.Local
	notifies atomic.Int64
}

func (n *countingNotifier) Notify(workspaceID string) {
	n.notifies.Add(1)
	n.Local.Notify(workspaceID)
}

// retryingAdapter exercises the storage contract's "fn may run multiple
// times" clause: every write closure runs once inside a savepoint that is
// rolled back, then again for real. Engine closures that fail to reset
// captured state duplicate their effects under it.
type retryingAdapter struct {
	storage.Adapter
}

var errDiscardAttempt = errors.New("retryingAdapter: discard first attempt")

func (r *retryingAdapter) WithWriteTx(ctx context.Context, workspaceID string, fn func(storage.WriteTx) error) error {
	return r.Adapter.WithWriteTx(ctx, workspaceID, func(tx storage.WriteTx) error {
		err := tx.InSavepoint(func() error {
			if err := fn(tx); err != nil {
				return err
			}
			return errDiscardAttempt
		})
		if err != nil && !errors.Is(err, errDiscardAttempt) {
			return err
		}
		return fn(tx)
	})
}

var _ = Describe("Server.Mutate", func() {
	var (
		ctx      context.Context
		srv      *server.Server
		store    storage.Adapter
		notifier *countingNotifier
		client   syncplanev1connect.SyncServiceClient
	)

	newServer := func(s storage.Adapter, opts ...server.Option) *server.Server {
		notifier = &countingNotifier{Local: pubsub.NewLocal()}
		srv := mustNewServer(s, append(opts,
			server.WithEntities(&todov1.Todo{}),
			server.WithNotifier(notifier))...)
		DeferCleanup(func() { Expect(srv.Close()).To(Succeed()) })

		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
		return srv
	}

	pull := func() *syncplanev1.PullResponse {
		GinkgoHelper()
		resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws}))
		Expect(err).NotTo(HaveOccurred())
		return resp.Msg
	}

	currentSeq := func() uint64 {
		GinkgoHelper()
		var seq uint64
		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			var err error
			seq, err = tx.CurrentSeq()
			return err
		})).To(Succeed())
		return seq
	}

	BeforeEach(func() {
		ctx = context.Background()
		sqlite, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "mutate.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(sqlite.Close()).To(Succeed()) })
		store = sqlite
		srv = newServer(store)
	})

	It("applies writes at one shared sequence, visible via Pull", func() {
		Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			if err := tx.Put(&todov1.Todo{Id: "t1", Title: "one"}); err != nil {
				return err
			}
			return tx.Put(&todov1.Todo{Id: "t2", Title: "two"})
		})).To(Succeed())

		changes := pull().GetPatch().GetChanges()
		Expect(changes).To(HaveLen(2))
		Expect(changes[0].GetVersion()).To(Equal(changes[1].GetVersion()), "both rows share the call's sequence")
		Expect(currentSeq()).To(BeEquivalentTo(1))
		Expect(notifier.notifies.Load()).To(BeEquivalentTo(1), "one poke per writing call")
	})

	It("coalesces a burst of writes into a leading and one trailing poke", func() {
		// A wide window so the whole burst lands inside one cooldown.
		srv = newServer(store, server.WithNotifyCoalescing(500*time.Millisecond))
		for i := 0; i < 5; i++ {
			id := fmt.Sprintf("t%d", i)
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Put(&todov1.Todo{Id: id})
			})).To(Succeed())
		}
		Expect(notifier.notifies.Load()).To(BeEquivalentTo(1), "only the burst's leading edge passes through")
		Eventually(notifier.notifies.Load, "2s").Should(BeEquivalentTo(2), "one trailing poke covers the rest of the burst")
		Consistently(notifier.notifies.Load, "300ms").Should(BeEquivalentTo(2))
	})

	It("sends no poke after Close", func() {
		srv = newServer(store, server.WithNotifyCoalescing(500*time.Millisecond))
		for i := 0; i < 2; i++ {
			id := fmt.Sprintf("t%d", i)
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Put(&todov1.Todo{Id: id})
			})).To(Succeed())
		}
		Expect(notifier.notifies.Load()).To(BeEquivalentTo(1), "a trailing poke is now owed")
		Expect(srv.Close()).To(Succeed())
		Consistently(notifier.notifies.Load, "1s").Should(BeEquivalentTo(1),
			"a shared notifier must not receive a poke from a server that has shut down")
	})

	It("consumes no sequence and sends no poke when fn does not write", func() {
		Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			var todo todov1.Todo
			_, err := tx.Get(&todo, "missing")
			return err
		})).To(Succeed())
		Expect(currentSeq()).To(BeZero())
		Expect(notifier.notifies.Load()).To(BeZero())
		// Nothing collects workspaces, so a caller that polls workspace ids
		// looking for work (a connector) must not create one by looking.
		Expect(srv.WorkspaceIDs(ctx)).To(BeEmpty())
	})

	It("rolls back all writes when fn errors", func() {
		boom := errors.New("boom")
		Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			Expect(tx.Put(&todov1.Todo{Id: "t1"})).To(Succeed())
			return boom
		})).To(MatchError(boom))
		Expect(pull().GetPatch().GetChanges()).To(BeEmpty())
		Expect(currentSeq()).To(BeZero(), "rolled-back call consumes no sequence")
		Expect(notifier.notifies.Load()).To(BeZero())
	})

	It("surfaces Reject as a typed rejection", func() {
		err := srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			return server.Reject(server.CodeConflict, "taken")
		})
		r, ok := server.AsRejection(err)
		Expect(ok).To(BeTrue())
		Expect(r.Code).To(Equal(server.CodeConflict))
	})

	It("requires a workspace id", func() {
		Expect(srv.Mutate(ctx, "", func(tx *server.EntityTx) error { return nil })).
			To(MatchError(ContainSubstring("workspace id")))
	})

	Describe("Invoke", func() {
		// seen records what the mutator body was handed, so a spec can assert
		// the args contract rather than infer it from the rows written.
		var seen proto.Message
		BeforeEach(func() {
			seen = nil
			srv = newServer(store,
				server.WithMutator("complete", &todov1.Todo{},
					func(ctx context.Context, tx *server.EntityTx, args proto.Message) error {
						todo := args.(*todov1.Todo)
						todo.Completed = true
						return tx.Put(todo)
					}),
				// Asserts unconditionally, exactly as a body written against the
				// push path may: applyCustom never hands it a nil interface.
				server.WithMutator("record", &todov1.Todo{},
					func(ctx context.Context, tx *server.EntityTx, args proto.Message) error {
						seen = args.(*todov1.Todo)
						return nil
					}),
				server.WithMutator("no_args", nil,
					func(ctx context.Context, tx *server.EntityTx, args proto.Message) error {
						seen = args
						return nil
					}))
		})

		It("runs a registered mutator", func() {
			Expect(srv.Invoke(ctx, ws, "complete", &todov1.Todo{Id: "t1", Title: "x"})).To(Succeed())
			changes := pull().GetPatch().GetChanges()
			Expect(changes).To(HaveLen(1))
			var todo todov1.Todo
			Expect(proto.Unmarshal(changes[0].GetValue(), &todo)).To(Succeed())
			Expect(todo.GetCompleted()).To(BeTrue())
		})

		It("rejects unknown mutator names", func() {
			Expect(srv.Invoke(ctx, ws, "nope", nil)).To(MatchError(ContainSubstring(`unknown mutator "nope"`)))
		})

		It("rejects args of the wrong type", func() {
			Expect(srv.Invoke(ctx, ws, "complete", &syncplanecursorv1.Cursor{})).
				To(MatchError(ContainSubstring("expects args of type")))
		})

		It("hands a prototype-registered mutator a zeroed message when args are nil", func() {
			// The push path clones and resets the prototype for an args-less
			// custom mutation; Invoke must hand the same body the same thing
			// rather than a nil interface it would panic on.
			Expect(srv.Invoke(ctx, ws, "record", nil)).To(Succeed())
			Expect(seen).To(BeAssignableToTypeOf(&todov1.Todo{}))
			Expect(proto.Equal(seen, &todov1.Todo{})).To(BeTrue())
		})

		It("hands a prototype-less mutator nil args", func() {
			Expect(srv.Invoke(ctx, ws, "no_args", nil)).To(Succeed())
			Expect(seen).To(BeNil())
		})

		It("rejects args for a mutator registered without a prototype", func() {
			Expect(srv.Invoke(ctx, ws, "no_args", &todov1.Todo{Id: "t1"})).
				To(MatchError(ContainSubstring("takes no args")))
		})
	})

	Describe("re-entrancy guard", func() {
		// A nested write transaction would block forever on the single SQLite
		// writer connection; the guard must turn that into an immediate error.
		// Bound the spec context so a regression fails the suite instead of
		// hanging it.
		// opts carries what a spec needs beyond the mutator itself (a GC
		// configuration, say); the server is rebuilt because a mutator is a
		// construction-time declaration.
		guarded := func(body func(reCtx context.Context) error, opts ...server.Option) error {
			GinkgoHelper()
			reCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			DeferCleanup(cancel)
			var inner error
			srv = newServer(store, append(opts, server.WithMutator("reenter", nil,
				func(mctx context.Context, _ *server.EntityTx, _ proto.Message) error {
					inner = body(mctx)
					return inner
				}))...)
			Expect(srv.Invoke(reCtx, ws, "reenter", nil)).To(HaveOccurred())
			return inner
		}

		It("rejects a nested Mutate instead of deadlocking", func() {
			err := guarded(func(reCtx context.Context) error {
				return srv.Mutate(reCtx, ws, func(*server.EntityTx) error { return nil })
			})
			Expect(err).To(MatchError(ContainSubstring("re-entrantly")))
		})

		It("rejects a nested Invoke instead of deadlocking", func() {
			err := guarded(func(reCtx context.Context) error {
				return srv.Invoke(reCtx, ws, "reenter", nil)
			})
			Expect(err).To(MatchError(ContainSubstring("re-entrantly")))
		})

		It("rejects a nested CollectGarbage instead of deadlocking", func() {
			err := guarded(func(reCtx context.Context) error {
				return srv.CollectGarbage(reCtx, ws)
			}, server.WithGC(time.Hour, 0))
			Expect(err).To(MatchError(ContainSubstring("re-entrantly")))
		})
	})

	Describe("under a retrying adapter", func() {
		BeforeEach(func() {
			sqlite, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "retry.db"))
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { Expect(sqlite.Close()).To(Succeed()) })
			store = &retryingAdapter{Adapter: sqlite}
			srv = newServer(store)
		})

		It("Push reports each mutation exactly once", func() {
			resp, err := client.Push(ctx, connect.NewRequest(pushOf(
				ws,
				[]*syncplanev1.Mutation{
					putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1"}),
					putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2"}),
				})))
			Expect(err).NotTo(HaveOccurred())
			Expect(pushResults(resp.Msg)).To(HaveLen(2), "a retried write tx must not duplicate results")
			Expect(notifier.notifies.Load()).To(BeEquivalentTo(1))
		})

		It("Mutate consumes one sequence and pokes once", func() {
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Put(&todov1.Todo{Id: "t1"})
			})).To(Succeed())
			Expect(currentSeq()).To(BeEquivalentTo(1), "the abandoned attempt's sequence rolled back")
			Expect(notifier.notifies.Load()).To(BeEquivalentTo(1))
		})
	})
})

var _ = Describe("EntityTx.Seq", func() {
	const ws = "ws-seq"
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	It("is 0 before the first write and stable across writes in one tx", func() {
		srv := newTestServer()
		var before, afterFirst, afterSecond uint64
		Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			before = tx.Seq()
			if err := tx.Put(&todov1.Todo{Id: "a"}); err != nil {
				return err
			}
			afterFirst = tx.Seq()
			if err := tx.Put(&todov1.Todo{Id: "b"}); err != nil {
				return err
			}
			afterSecond = tx.Seq()
			return nil
		})).To(Succeed())

		Expect(before).To(BeZero())
		Expect(afterFirst).NotTo(BeZero())
		Expect(afterSecond).To(Equal(afterFirst))

		// The recorded seq is the one stamped on the committed rows.
		var feedSeqs []uint64
		_, _, err := srv.Changes(ctx, ws, server.Cursor{}, 0, func(ch server.Change) error {
			feedSeqs = append(feedSeqs, ch.Seq)
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(feedSeqs).To(ConsistOf(afterFirst, afterFirst))
	})

	It("stays 0 in a read-only Mutate", func() {
		srv := newTestServer()
		Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			var todo todov1.Todo
			if _, err := tx.Get(&todo, "missing"); err != nil {
				return err
			}
			Expect(tx.Seq()).To(BeZero())
			return nil
		})).To(Succeed())
	})

	It("resets across write-tx retries so only the committed seq is observable", func() {
		store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "seq.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		srv := mustNewServer(&retryingAdapter{Adapter: store}, server.WithEntities(&todov1.Todo{}))

		var seqs []uint64
		Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			if err := tx.Put(&todov1.Todo{Id: "r1"}); err != nil {
				return err
			}
			seqs = append(seqs, tx.Seq())
			return nil
		})).To(Succeed())
		Expect(seqs).To(HaveLen(2), "retrying adapter runs the closure twice")
		// The discarded attempt's allocation rolled back with its savepoint;
		// what matters is that each attempt allocated for itself instead of
		// inheriting a stale captured value.
		Expect(seqs[0]).NotTo(BeZero())
		Expect(seqs[1]).NotTo(BeZero())

		var committed []uint64
		_, _, err = srv.Changes(ctx, ws, server.Cursor{}, 0, func(ch server.Change) error {
			committed = append(committed, ch.Seq)
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(committed).To(Equal([]uint64{seqs[1]}), "only the final attempt's seq commits")
	})

	It("reports the push-allocated seq inside mutators after their first write", func() {
		var before, after uint64
		srv := newTestServer(server.WithMutator("seq_probe", &todov1.Todo{},
			func(ctx context.Context, tx *server.EntityTx, args proto.Message) error {
				before = tx.Seq()
				if err := tx.Put(args.(*todov1.Todo)); err != nil {
					return err
				}
				after = tx.Seq()
				return nil
			}))

		resp, err := srv.Push(ctx, connect.NewRequest(pushOf(
			ws,
			[]*syncplanev1.Mutation{customMutation("c1", 1, "seq_probe", &todov1.Todo{Id: "p1"})})))
		Expect(err).NotTo(HaveOccurred())
		result := pushResults(resp.Msg)[0]
		Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))

		Expect(before).To(BeZero(), "no write yet, even though push pre-allocates")
		Expect(after).To(Equal(result.GetSeq()), "matches the seq push reports for the mutation")
	})
})
