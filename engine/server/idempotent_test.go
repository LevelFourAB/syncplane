package server_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	"github.com/levelfourab/syncplane/engine/internal/pubsub"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

var _ = Describe("Server.MutateIdempotent", func() {
	const ws = "ws-idem"
	var (
		ctx   context.Context
		srv   *server.Server
		store storage.Adapter
		runs  int
	)

	// put is the closure under test: it counts its own invocations so a spec
	// can tell "applied again" from "replayed the record".
	put := func(id, title string) func(tx *server.EntityTx) ([]byte, error) {
		return func(tx *server.EntityTx) ([]byte, error) {
			runs++
			if err := tx.Put(&todov1.Todo{Id: id, Title: title}); err != nil {
				return nil, err
			}
			return []byte("created " + id), nil
		}
	}

	titleOf := func(key string) string {
		GinkgoHelper()
		var todo todov1.Todo
		var found bool
		Expect(srv.View(ctx, ws, func(tx *server.ViewTx) error {
			var err error
			found, _, err = tx.Get(&todo, key)
			return err
		})).To(Succeed())
		if !found {
			return ""
		}
		return todo.GetTitle()
	}

	BeforeEach(func() {
		ctx = context.Background()
		runs = 0
		sqlite, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "idem.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(sqlite.Close()).To(Succeed()) })
		store = sqlite
		srv = mustNewServer(store, server.WithEntities(&todov1.Todo{}))
		DeferCleanup(func() { Expect(srv.Close()).To(Succeed()) })
	})

	It("applies the first call and replays its outcome on every retry", func() {
		first, err := srv.MutateIdempotent(ctx, ws, "req-1", put("t1", "one"))
		Expect(err).NotTo(HaveOccurred())
		Expect(first.Applied).To(BeTrue())
		Expect(first.Seq).To(BeEquivalentTo(1))
		Expect(first.Result).To(Equal([]byte("created t1")))

		// The retry carries a different payload; the key decides, not the body.
		retry, err := srv.MutateIdempotent(ctx, ws, "req-1", put("t1", "rewritten"))
		Expect(err).NotTo(HaveOccurred())
		Expect(retry.Applied).To(BeFalse())
		Expect(retry.Seq).To(Equal(first.Seq), "the sequence the ORIGINAL write took")
		Expect(retry.Result).To(Equal(first.Result))

		Expect(runs).To(Equal(1), "the closure must not run on a replay")
		Expect(titleOf("t1")).To(Equal("one"))
	})

	It("consumes no sequence on a replay", func() {
		_, err := srv.MutateIdempotent(ctx, ws, "req-1", put("t1", "one"))
		Expect(err).NotTo(HaveOccurred())
		_, err = srv.MutateIdempotent(ctx, ws, "req-1", put("t1", "one"))
		Expect(err).NotTo(HaveOccurred())

		var seq uint64
		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			var err error
			seq, err = tx.CurrentSeq()
			return err
		})).To(Succeed())
		Expect(seq).To(BeEquivalentTo(1))
	})

	It("treats distinct keys as distinct writes", func() {
		_, err := srv.MutateIdempotent(ctx, ws, "req-1", put("t1", "one"))
		Expect(err).NotTo(HaveOccurred())
		out, err := srv.MutateIdempotent(ctx, ws, "req-2", put("t2", "two"))
		Expect(err).NotTo(HaveOccurred())
		Expect(out.Applied).To(BeTrue())
		Expect(runs).To(Equal(2))
	})

	It("scopes keys to the workspace", func() {
		_, err := srv.MutateIdempotent(ctx, ws, "req-1", put("t1", "one"))
		Expect(err).NotTo(HaveOccurred())
		out, err := srv.MutateIdempotent(ctx, "other-ws", "req-1", put("t1", "one"))
		Expect(err).NotTo(HaveOccurred())
		Expect(out.Applied).To(BeTrue(), "the same key in another workspace is a different claim")
	})

	It("records a closure that wrote nothing, so its outcome replays too", func() {
		decide := func(tx *server.EntityTx) ([]byte, error) {
			runs++
			return []byte("nothing to do"), nil
		}
		first, err := srv.MutateIdempotent(ctx, ws, "req-1", decide)
		Expect(err).NotTo(HaveOccurred())
		Expect(first.Applied).To(BeTrue())
		Expect(first.Seq).To(BeZero(), "no write, no sequence")
		Expect(first.Result).To(Equal([]byte("nothing to do")))

		retry, err := srv.MutateIdempotent(ctx, ws, "req-1", decide)
		Expect(err).NotTo(HaveOccurred())
		Expect(retry.Applied).To(BeFalse())
		Expect(retry.Result).To(Equal([]byte("nothing to do")))
		Expect(runs).To(Equal(1))
	})

	It("leaves the key unclaimed when the closure fails, so a retry applies", func() {
		boom := errors.New("boom")
		_, err := srv.MutateIdempotent(ctx, ws, "req-1", func(tx *server.EntityTx) ([]byte, error) {
			runs++
			if err := tx.Put(&todov1.Todo{Id: "t1", Title: "half"}); err != nil {
				return nil, err
			}
			return nil, boom
		})
		Expect(err).To(MatchError(boom))
		Expect(titleOf("t1")).To(BeEmpty(), "the failed attempt rolled back")

		out, err := srv.MutateIdempotent(ctx, ws, "req-1", put("t1", "one"))
		Expect(err).NotTo(HaveOccurred())
		Expect(out.Applied).To(BeTrue(), "a failure is not an outcome to replay")
		Expect(runs).To(Equal(2))
		Expect(titleOf("t1")).To(Equal("one"))
	})

	It("does not claim the key for a rejection", func() {
		_, err := srv.MutateIdempotent(ctx, ws, "req-1", func(tx *server.EntityTx) ([]byte, error) {
			return nil, server.Reject(server.CodeConflict, "taken")
		})
		r, ok := server.AsRejection(err)
		Expect(ok).To(BeTrue())
		Expect(r.Code).To(Equal(server.CodeConflict))

		out, err := srv.MutateIdempotent(ctx, ws, "req-1", put("t1", "one"))
		Expect(err).NotTo(HaveOccurred())
		Expect(out.Applied).To(BeTrue())
	})

	It("applies once when concurrent retries race", func() {
		// Per-workspace write exclusion covers the whole closure, so the second
		// caller cannot read "unclaimed" while the first is mid-write.
		const callers = 8
		var (
			wg       sync.WaitGroup
			mu       sync.Mutex
			applied  int
			outcomes []server.WriteOutcome
		)
		for i := 0; i < callers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				out, err := srv.MutateIdempotent(ctx, ws, "req-1", func(tx *server.EntityTx) ([]byte, error) {
					return []byte("body"), tx.Put(&todov1.Todo{Id: "t1", Title: "one"})
				})
				mu.Lock()
				defer mu.Unlock()
				Expect(err).NotTo(HaveOccurred())
				if out.Applied {
					applied++
				}
				outcomes = append(outcomes, out)
			}()
		}
		wg.Wait()
		Expect(applied).To(Equal(1))
		for _, out := range outcomes {
			Expect(out.Seq).To(BeEquivalentTo(1))
			Expect(out.Result).To(Equal([]byte("body")))
		}
	})

	It("requires a workspace id and a key", func() {
		noop := func(tx *server.EntityTx) ([]byte, error) { return nil, nil }
		_, err := srv.MutateIdempotent(ctx, "", "req-1", noop)
		Expect(err).To(MatchError(ContainSubstring("workspace id")))
		_, err = srv.MutateIdempotent(ctx, ws, "", noop)
		Expect(err).To(MatchError(ContainSubstring("idempotency key is required")))
	})

	It("rejects an over-long key", func() {
		_, err := srv.MutateIdempotent(ctx, ws, strings.Repeat("k", server.MaxIdempotencyKeyBytes+1),
			func(tx *server.EntityTx) ([]byte, error) { return nil, nil })
		Expect(err).To(MatchError(ContainSubstring("over the")))
	})

	It("rejects a result over the value limit and applies nothing", func() {
		srv = mustNewServer(store,
			server.WithEntities(&todov1.Todo{}),
			server.WithLimits(server.Limits{MaxValueBytes: 16}))
		_, err := srv.MutateIdempotent(ctx, ws, "req-1", func(tx *server.EntityTx) ([]byte, error) {
			if err := tx.Put(&todov1.Todo{Id: "t1", Title: "one"}); err != nil {
				return nil, err
			}
			return make([]byte, 17), nil
		})
		Expect(err).To(MatchError(ContainSubstring("over the 16 byte limit")))
		Expect(titleOf("t1")).To(BeEmpty(), "the over-limit result rolled the write back with it")
	})

	It("rejects a re-entrant call instead of deadlocking", func() {
		reCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		DeferCleanup(cancel)
		var inner error
		// The mutator re-enters the server it is declared on, so it closes over
		// a variable New fills in rather than over srv directly.
		var reSrv *server.Server
		reSrv = mustNewServer(store,
			server.WithEntities(&todov1.Todo{}),
			server.WithMutator("reenter", nil,
				func(mctx context.Context, _ *server.EntityTx, _ proto.Message) error {
					_, inner = reSrv.MutateIdempotent(mctx, ws, "req-1",
						func(tx *server.EntityTx) ([]byte, error) { return nil, nil })
					return inner
				}))
		srv = reSrv
		Expect(srv.Invoke(reCtx, ws, "reenter", nil)).To(HaveOccurred())
		Expect(inner).To(MatchError(ContainSubstring("re-entrantly")))
	})

	Describe("notification", func() {
		var notifier *countingNotifier

		BeforeEach(func() {
			notifier = &countingNotifier{Local: pubsub.NewLocal()}
			srv = mustNewServer(store,
				server.WithEntities(&todov1.Todo{}),
				server.WithNotifier(notifier))
		})

		It("pokes once for the applied call and never for a replay", func() {
			_, err := srv.MutateIdempotent(ctx, ws, "req-1", put("t1", "one"))
			Expect(err).NotTo(HaveOccurred())
			Expect(notifier.notifies.Load()).To(BeEquivalentTo(1))
			_, err = srv.MutateIdempotent(ctx, ws, "req-1", put("t1", "one"))
			Expect(err).NotTo(HaveOccurred())
			Consistently(notifier.notifies.Load, "200ms").Should(BeEquivalentTo(1))
		})
	})

	Describe("under a retrying adapter", func() {
		BeforeEach(func() {
			sqlite, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "idem-retry.db"))
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { Expect(sqlite.Close()).To(Succeed()) })
			store = &retryingAdapter{Adapter: sqlite}
			srv = mustNewServer(store, server.WithEntities(&todov1.Todo{}))
		})

		It("reports the committed attempt's sequence, and still replays after", func() {
			first, err := srv.MutateIdempotent(ctx, ws, "req-1", put("t1", "one"))
			Expect(err).NotTo(HaveOccurred())
			Expect(first.Applied).To(BeTrue())
			Expect(first.Seq).To(BeEquivalentTo(1), "the abandoned attempt's sequence rolled back")
			Expect(runs).To(Equal(2), "the adapter runs the closure twice by contract")

			retry, err := srv.MutateIdempotent(ctx, ws, "req-1", put("t1", "rewritten"))
			Expect(err).NotTo(HaveOccurred())
			Expect(retry.Applied).To(BeFalse())
			Expect(retry.Seq).To(Equal(first.Seq))
			Expect(titleOf("t1")).To(Equal("one"))
		})
	})

	Describe("key retention", func() {
		It("lets a write through again once its key has been collected", func() {
			// A millisecond of retention plus a real pause: non-positive reads as
			// "unset" and would silently fall back to the default.
			srv = mustNewServer(store,
				server.WithEntities(&todov1.Todo{}),
				server.WithGC(-time.Second, 0),
				server.WithIdempotencyRetention(time.Millisecond))

			_, err := srv.MutateIdempotent(ctx, ws, "req-1", put("t1", "one"))
			Expect(err).NotTo(HaveOccurred())
			time.Sleep(5 * time.Millisecond)
			Expect(srv.CollectGarbage(ctx, ws)).To(Succeed())

			out, err := srv.MutateIdempotent(ctx, ws, "req-1", put("t1", "again"))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Applied).To(BeTrue(), "an expired key no longer absorbs its retry")
			Expect(titleOf("t1")).To(Equal("again"))
		})

		It("keeps keys a pass is not yet allowed to collect", func() {
			srv = mustNewServer(store,
				server.WithEntities(&todov1.Todo{}),
				server.WithGC(-time.Second, 0),
				server.WithIdempotencyRetention(time.Hour))

			_, err := srv.MutateIdempotent(ctx, ws, "req-1", put("t1", "one"))
			Expect(err).NotTo(HaveOccurred())
			Expect(srv.CollectGarbage(ctx, ws)).To(Succeed())

			out, err := srv.MutateIdempotent(ctx, ws, "req-1", put("t1", "again"))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Applied).To(BeFalse(), "idempotency retention is independent of tombstone retention")
		})
	})

	Describe("InvokeIdempotent", func() {
		BeforeEach(func() {
			srv = mustNewServer(store,
				server.WithEntities(&todov1.Todo{}),
				server.WithMutator("complete", &todov1.Todo{},
					func(ctx context.Context, tx *server.EntityTx, args proto.Message) error {
						runs++
						todo := args.(*todov1.Todo)
						todo.Completed = true
						return tx.Put(todo)
					}))
		})

		It("runs the mutator once across retries", func() {
			first, err := srv.InvokeIdempotent(ctx, ws, "evt-1", "complete", &todov1.Todo{Id: "t1", Title: "x"})
			Expect(err).NotTo(HaveOccurred())
			Expect(first.Applied).To(BeTrue())
			Expect(first.Seq).To(BeEquivalentTo(1))

			retry, err := srv.InvokeIdempotent(ctx, ws, "evt-1", "complete", &todov1.Todo{Id: "t1", Title: "x"})
			Expect(err).NotTo(HaveOccurred())
			Expect(retry.Applied).To(BeFalse())
			Expect(retry.Seq).To(Equal(first.Seq))
			Expect(runs).To(Equal(1))
		})

		It("hands a prototype-registered mutator a zeroed message when args are nil", func() {
			// Same args contract as Invoke and the push path — a body may
			// type-assert without a nil check whichever entry point ran it.
			_, err := srv.InvokeIdempotent(ctx, ws, "evt-1", "complete", nil)
			Expect(err).To(MatchError(ContainSubstring("every key field must be set")), "the zeroed Todo has no id to key on")
			Expect(runs).To(Equal(1), "the body ran rather than panicking on nil args")
		})

		It("checks the mutator name and args before opening a transaction", func() {
			_, err := srv.InvokeIdempotent(ctx, ws, "evt-1", "nope", nil)
			Expect(err).To(MatchError(ContainSubstring(`unknown mutator "nope"`)))
			// The failed lookup must not have claimed the key.
			out, err := srv.InvokeIdempotent(ctx, ws, "evt-1", "complete", &todov1.Todo{Id: "t1"})
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Applied).To(BeTrue())
		})
	})
})
