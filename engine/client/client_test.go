package client_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/blob"
	"github.com/levelfourab/syncplane/engine/client"
	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/text"
)

var _ = Describe("Client", func() {
	var (
		ctx context.Context
		srv *server.Server
		url string
		c   *client.Client
		ws  *client.Workspace
	)

	BeforeEach(func() {
		ctx = GinkgoT().Context()
		srv, url = startServer()
		var err error
		c, err = client.New(url)
		Expect(err).NotTo(HaveOccurred())
		ws = c.Workspace("acme")
	})

	getTodo := func(key string) (*todov1.Todo, bool) {
		row, ok, err := client.Get[*todov1.Todo](ctx, ws, key)
		Expect(err).NotTo(HaveOccurred())
		return row.Value, ok
	}

	Describe("New", func() {
		It("refuses a relative base URL", func() {
			_, err := client.New("/sync")
			Expect(err).To(HaveOccurred())
		})

		It("refuses a client id with a control character", func() {
			_, err := client.New(url, client.WithClientID("a\u0001b"))
			Expect(err).To(HaveOccurred())
		})

		It("mints a different client id per Client", func() {
			other, err := client.New(url)
			Expect(err).NotTo(HaveOccurred())
			Expect(other.ClientID()).NotTo(Equal(c.ClientID()))
		})
	})

	Describe("EntityKey", func() {
		It("derives the key the server stores the row under", func() {
			pair := &testdatav1.Pair{Ns: "a/b", Id: `c\d`, Label: "x"}
			entity, key, err := client.EntityKey(pair)
			Expect(err).NotTo(HaveOccurred())
			srvEntity, srvKey, err := srv.EntityKey(pair)
			Expect(err).NotTo(HaveOccurred())
			Expect(entity).To(Equal(srvEntity))
			Expect(key).To(Equal(srvKey))

			_, err = ws.Put(ctx, pair)
			Expect(err).NotTo(HaveOccurred())
			err = srv.View(server.Trusted(ctx), "acme", func(tx *server.ViewTx) error {
				got, ok, err := server.Get[*testdatav1.Pair](tx, key)
				Expect(err).NotTo(HaveOccurred())
				Expect(ok).To(BeTrue())
				Expect(got.GetLabel()).To(Equal("x"))
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		})

		It("refuses a message without entity options", func() {
			_, _, err := client.EntityKey(&todov1.CompleteAllArgs{})
			Expect(err).To(MatchError(ContainSubstring("no (syncplane.v1.entity)")))
		})

		It("refuses an empty key", func() {
			_, _, err := client.EntityKey(&todov1.Todo{Title: "no id"})
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("codes", func() {
		It("match the server's", func() {
			Expect(client.CodePermissionDenied).To(Equal(server.CodePermissionDenied))
			Expect(client.CodeAccessDenied).To(Equal(server.CodeAccessDenied))
			Expect(client.CodeInvalidArgument).To(Equal(server.CodeInvalidArgument))
			Expect(client.CodeConflict).To(Equal(server.CodeConflict))
			Expect(client.CodeTextFullStateRequired).To(Equal(server.CodeTextFullStateRequired))
			Expect(client.CodeTextStateStale).To(Equal(server.CodeTextStateStale))
			Expect(client.CodeWorkspaceAccessRevoked).To(Equal(server.CodeWorkspaceAccessRevoked))
			Expect(client.CodeWorkspaceNoAccess).To(Equal(server.CodeWorkspaceNoAccess))
			Expect(client.CodeMutationIDGap).To(Equal(server.CodeMutationIDGap))
			Expect(client.CodeInternal).To(Equal(server.CodeInternal))
			Expect(client.CodeMutationFailed).To(Equal(server.CodeMutationFailed))
		})
	})

	Describe("writes", func() {
		It("puts, patches and deletes a row", func() {
			res, err := ws.Put(ctx, &todov1.Todo{Id: "t1", Title: "first"})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.MutationID).To(Equal(uint64(1)))
			Expect(res.Seq).To(BeNumerically(">", 0))

			_, err = ws.Patch(ctx, &todov1.Todo{Id: "t1", Completed: true, Title: "ignored"}, []string{"completed"})
			Expect(err).NotTo(HaveOccurred())
			todo, ok := getTodo("t1")
			Expect(ok).To(BeTrue())
			Expect(todo.GetTitle()).To(Equal("first"))
			Expect(todo.GetCompleted()).To(BeTrue())

			res, err = ws.Delete(ctx, &todov1.Todo{}, "t1")
			Expect(err).NotTo(HaveOccurred())
			Expect(res.MutationID).To(Equal(uint64(3)))
			_, ok = getTodo("t1")
			Expect(ok).To(BeFalse())

			_, err = ws.Delete(ctx, &todov1.Todo{}, "t1")
			Expect(err).NotTo(HaveOccurred(), "deleting an absent row")
		})

		It("reports a rejection as a *RejectedError and keeps ids contiguous", func() {
			_, err := ws.Put(ctx, &todov1.Todo{Id: "t1", Title: "forbidden"})
			var rej *client.RejectedError
			Expect(errors.As(err, &rej)).To(BeTrue())
			Expect(rej.Code).To(Equal(client.CodePermissionDenied))
			Expect(rej.MutationID).To(Equal(uint64(1)))
			Expect(client.CodeOf(err)).To(Equal(client.CodePermissionDenied))

			res, err := ws.Put(ctx, &todov1.Todo{Id: "t1", Title: "fine"})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.MutationID).To(Equal(uint64(2)))
		})

		It("rejects a patch of an absent row", func() {
			_, err := ws.Patch(ctx, &todov1.Todo{Id: "nope", Completed: true}, []string{"completed"})
			Expect(client.CodeOf(err)).To(Equal(client.CodeConflict))
		})

		It("refuses an invalid key before sending anything", func() {
			_, err := ws.Delete(ctx, &todov1.Todo{}, "")
			Expect(err).To(HaveOccurred())
			res, err := ws.Put(ctx, &todov1.Todo{Id: "t1"})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.MutationID).To(Equal(uint64(1)))
		})

		Describe("compare-and-swap", func() {
			It("applies at the expected version and rejects a stale one", func() {
				first, err := ws.Put(ctx, &todov1.Todo{Id: "t1", Title: "a"})
				Expect(err).NotTo(HaveOccurred())

				_, err = ws.Put(ctx, &todov1.Todo{Id: "t1", Title: "new"}, client.WithExpectedVersion(0))
				Expect(client.CodeOf(err)).To(Equal(client.CodeConflict))

				second, err := ws.Patch(ctx, &todov1.Todo{Id: "t1", Title: "b"}, []string{"title"},
					client.WithExpectedVersion(first.Seq))
				Expect(err).NotTo(HaveOccurred())

				_, err = ws.Patch(ctx, &todov1.Todo{Id: "t1", Title: "c"}, []string{"title"},
					client.WithExpectedVersion(first.Seq))
				Expect(client.CodeOf(err)).To(Equal(client.CodeConflict))

				row, ok, err := client.Get[*todov1.Todo](ctx, ws, "t1")
				Expect(err).NotTo(HaveOccurred())
				Expect(ok).To(BeTrue())
				Expect(row.Version).To(Equal(second.Seq))
				Expect(row.Value.GetTitle()).To(Equal("b"))
			})

			It("guards a delete", func() {
				res, err := ws.Put(ctx, &todov1.Todo{Id: "t1"})
				Expect(err).NotTo(HaveOccurred())
				_, err = ws.Delete(ctx, &todov1.Todo{}, "t1", client.WithExpectedVersion(res.Seq+100))
				Expect(client.CodeOf(err)).To(Equal(client.CodeConflict))
				_, err = ws.Delete(ctx, &todov1.Todo{}, "t1", client.WithExpectedVersion(res.Seq))
				Expect(err).NotTo(HaveOccurred())
			})
		})

		Describe("Apply", func() {
			It("applies a batch at one sequence", func() {
				res, err := ws.Apply(ctx,
					client.Put(&todov1.Todo{Id: "t1", Title: "one"}),
					client.Put(&testdatav1.Widget{Id: "w1", Label: "w"}),
					client.Patch(&todov1.Todo{Id: "t1", Completed: true}, []string{"completed"}),
				)
				Expect(err).NotTo(HaveOccurred())
				todo, _, err := client.Get[*todov1.Todo](ctx, ws, "t1")
				Expect(err).NotTo(HaveOccurred())
				Expect(todo.Version).To(Equal(res.Seq))
				Expect(todo.Value.GetCompleted()).To(BeTrue())
				widget, ok, err := client.Get[*testdatav1.Widget](ctx, ws, "w1")
				Expect(err).NotTo(HaveOccurred())
				Expect(ok).To(BeTrue())
				Expect(widget.Version).To(Equal(res.Seq))
			})

			It("rejects the whole batch when one operation is rejected", func() {
				_, err := ws.Apply(ctx,
					client.Put(&testdatav1.Widget{Id: "w1"}),
					client.Put(&todov1.Todo{Id: "t1", Title: "forbidden"}),
				)
				Expect(client.CodeOf(err)).To(Equal(client.CodePermissionDenied))
				_, ok, err := client.Get[*testdatav1.Widget](ctx, ws, "w1")
				Expect(err).NotTo(HaveOccurred())
				Expect(ok).To(BeFalse())
			})

			It("refuses no operations and invalid ones", func() {
				_, err := ws.Apply(ctx)
				Expect(err).To(HaveOccurred())
				_, err = ws.Apply(ctx, client.Op{})
				Expect(err).To(HaveOccurred())
				_, err = ws.Apply(ctx, client.Put(&todov1.CompleteAllArgs{}))
				Expect(err).To(HaveOccurred())
			})
		})

		Describe("Invoke", func() {
			It("runs a custom mutator", func() {
				_, err := ws.Apply(ctx,
					client.Put(&todov1.Todo{Id: "t1"}),
					client.Put(&todov1.Todo{Id: "t2"}))
				Expect(err).NotTo(HaveOccurred())
				_, err = ws.Invoke(ctx, "complete_all", &todov1.CompleteAllArgs{})
				Expect(err).NotTo(HaveOccurred())
				for _, key := range []string{"t1", "t2"} {
					todo, ok := getTodo(key)
					Expect(ok).To(BeTrue())
					Expect(todo.GetCompleted()).To(BeTrue())
				}
			})

			It("reports a mutator's rejection and rolls back its writes", func() {
				_, err := ws.Invoke(ctx, "write_then_reject", &todov1.Todo{Id: "r1"})
				Expect(client.CodeOf(err)).To(Equal(client.CodeConflict))
				_, ok := getTodo("r1")
				Expect(ok).To(BeFalse())
			})

			It("reports a failing mutator as a *WorkspaceError and frees its id", func() {
				_, err := ws.Invoke(ctx, "fail_internal", nil)
				var wsErr *client.WorkspaceError
				Expect(errors.As(err, &wsErr)).To(BeTrue())
				Expect(wsErr.Code).To(Equal(client.CodeMutationFailed))

				res, err := ws.Put(ctx, &todov1.Todo{Id: "t1"})
				Expect(err).NotTo(HaveOccurred())
				Expect(res.MutationID).To(Equal(uint64(1)))
			})
		})

		It("reports a deleted workspace as a *WorkspaceError", func() {
			Expect(srv.DeleteWorkspace(ctx, "acme")).To(Succeed())
			_, err := ws.Put(ctx, &todov1.Todo{Id: "t1"})
			var wsErr *client.WorkspaceError
			Expect(errors.As(err, &wsErr)).To(BeTrue())
			Expect(wsErr.Code).To(Equal(client.CodeWorkspaceAccessRevoked))
		})

		It("keeps mutation ids contiguous under concurrent writes", func() {
			var wg sync.WaitGroup
			ids := make(chan uint64, 20)
			for i := range 20 {
				wg.Go(func() {
					defer GinkgoRecover()
					res, err := ws.Put(ctx, &todov1.Todo{Id: "t" + string(rune('a'+i))})
					Expect(err).NotTo(HaveOccurred())
					ids <- res.MutationID
				})
			}
			wg.Wait()
			close(ids)
			var got []uint64
			for id := range ids {
				got = append(got, id)
			}
			Expect(got).To(ConsistOf(func() []uint64 {
				var want []uint64
				for i := uint64(1); i <= 20; i++ {
					want = append(want, i)
				}
				return want
			}()))
		})

		Describe("with a set client id", func() {
			It("continues after the watermark an earlier Client left", func() {
				first, err := client.New(url, client.WithClientID("job-1"))
				Expect(err).NotTo(HaveOccurred())
				_, err = first.Workspace("acme").Put(ctx, &todov1.Todo{Id: "t1"})
				Expect(err).NotTo(HaveOccurred())
				_, err = first.Workspace("acme").Put(ctx, &todov1.Todo{Id: "t2"})
				Expect(err).NotTo(HaveOccurred())

				second, err := client.New(url, client.WithClientID("job-1"))
				Expect(err).NotTo(HaveOccurred())
				res, err := second.Workspace("acme").Put(ctx, &todov1.Todo{Id: "t3"})
				Expect(err).NotTo(HaveOccurred())
				Expect(res.MutationID).To(Equal(uint64(3)))
				_, ok := getTodo("t3")
				Expect(ok).To(BeTrue())
			})

			It("reports ErrClientIDInUse when another live Client shares the id", func() {
				a, err := client.New(url, client.WithClientID("job-2"))
				Expect(err).NotTo(HaveOccurred())
				b, err := client.New(url, client.WithClientID("job-2"))
				Expect(err).NotTo(HaveOccurred())
				_, err = a.Workspace("acme").Put(ctx, &todov1.Todo{Id: "a1"})
				Expect(err).NotTo(HaveOccurred())
				_, err = b.Workspace("acme").Put(ctx, &todov1.Todo{Id: "b1"})
				Expect(err).NotTo(HaveOccurred())

				_, err = a.Workspace("acme").Put(ctx, &todov1.Todo{Id: "a2"})
				Expect(err).To(MatchError(client.ErrClientIDInUse))
				_, ok := getTodo("a2")
				Expect(ok).To(BeFalse())

				res, err := a.Workspace("acme").Put(ctx, &todov1.Todo{Id: "a2"})
				Expect(err).NotTo(HaveOccurred())
				Expect(res.MutationID).To(Equal(uint64(3)))
			})
		})

		Describe("retries", func() {
			var lossy *lossyTransport

			BeforeEach(func() {
				lossy = &lossyTransport{base: http.DefaultTransport}
			})

			It("resends a push whose response was lost under the same id", func() {
				lossy.dropPushes.Store(1)
				rc, err := client.New(url, client.WithHTTPClient(&http.Client{Transport: lossy}))
				Expect(err).NotTo(HaveOccurred())
				rws := rc.Workspace("acme")

				res, err := rws.Put(ctx, &todov1.Todo{Id: "t1", Title: "once"})
				Expect(err).NotTo(HaveOccurred())
				Expect(res.MutationID).To(Equal(uint64(1)))
				Expect(res.AlreadyApplied).To(BeTrue())
				Expect(lossy.pushes.Load()).To(Equal(int64(2)))

				res, err = rws.Put(ctx, &todov1.Todo{Id: "t2"})
				Expect(err).NotTo(HaveOccurred())
				Expect(res.MutationID).To(Equal(uint64(2)))
				Expect(res.AlreadyApplied).To(BeFalse())
			})

			It("reports ErrUnconfirmed and settles the mutation with Confirm", func() {
				lossy.dropPushes.Store(1)
				rc, err := client.New(url,
					client.WithHTTPClient(&http.Client{Transport: lossy}),
					client.WithMaxAttempts(1))
				Expect(err).NotTo(HaveOccurred())
				rws := rc.Workspace("acme")

				_, err = rws.Put(ctx, &todov1.Todo{Id: "t1"})
				Expect(err).To(MatchError(client.ErrUnconfirmed))

				res, err := rws.Confirm(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(res).To(Equal(client.Result{MutationID: 1, AlreadyApplied: true}))
				res, err = rws.Confirm(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(res).To(Equal(client.Result{}))
			})

			It("settles an unconfirmed mutation before the next write", func() {
				lossy.failPushes.Store(1)
				rc, err := client.New(url,
					client.WithHTTPClient(&http.Client{Transport: lossy}),
					client.WithMaxAttempts(1))
				Expect(err).NotTo(HaveOccurred())
				rws := rc.Workspace("acme")

				_, err = rws.Put(ctx, &todov1.Todo{Id: "t1"})
				Expect(err).To(MatchError(client.ErrUnconfirmed))
				_, ok := getTodo("t1")
				Expect(ok).To(BeFalse())

				res, err := rws.Put(ctx, &todov1.Todo{Id: "t2"})
				Expect(err).NotTo(HaveOccurred())
				Expect(res.MutationID).To(Equal(uint64(2)))
				_, ok = getTodo("t1")
				Expect(ok).To(BeTrue())
			})

			It("keeps the id of an unconfirmed mutation whose resend the server refused", func() {
				lossy.dropPushes.Store(1)
				var pushes atomic.Int64
				refuseSecond := connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
					return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
						if strings.HasSuffix(req.Spec().Procedure, "/Push") && pushes.Add(1) == 2 {
							return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("refused"))
						}
						return next(ctx, req)
					}
				})
				rc, err := client.New(url,
					client.WithHTTPClient(&http.Client{Transport: lossy}),
					client.WithInterceptors(refuseSecond),
					client.WithMaxAttempts(1))
				Expect(err).NotTo(HaveOccurred())
				rws := rc.Workspace("acme")

				// The first push lands, but its answer is lost.
				_, err = rws.Put(ctx, &todov1.Todo{Id: "t1"})
				Expect(err).To(MatchError(client.ErrUnconfirmed))
				// The resend is refused: t1's outcome is still unknown, so t2
				// is not sent under t1's id.
				_, err = rws.Put(ctx, &todov1.Todo{Id: "t2"})
				Expect(err).To(MatchError(client.ErrUnconfirmed))
				Expect(err).NotTo(MatchError(client.ErrClientIDInUse))

				res, err := rws.Put(ctx, &todov1.Todo{Id: "t2"})
				Expect(err).NotTo(HaveOccurred())
				Expect(res.MutationID).To(Equal(uint64(2)))
				_, ok := getTodo("t2")
				Expect(ok).To(BeTrue())
			})

			It("frees the id of a resend whose mutator failed", func() {
				lossy.failPushes.Store(1)
				rc, err := client.New(url, client.WithHTTPClient(&http.Client{Transport: lossy}))
				Expect(err).NotTo(HaveOccurred())
				rws := rc.Workspace("acme")

				// The first attempt fails in transit, so the second is a resend.
				// The mutator runs after the watermark check: its failure
				// tells that no attempt applied.
				_, err = rws.Invoke(ctx, "fail_internal", nil)
				Expect(err).NotTo(MatchError(client.ErrUnconfirmed))
				Expect(client.CodeOf(err)).To(Equal(client.CodeMutationFailed))

				res, err := rws.Put(ctx, &todov1.Todo{Id: "t1"})
				Expect(err).NotTo(HaveOccurred())
				Expect(res.MutationID).To(Equal(uint64(1)))
			})

			It("takes an already-applied answer after an internal failure as its own", func() {
				var failed atomic.Bool
				internalAfterApply := connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
					return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
						resp, err := next(ctx, req)
						if err == nil && strings.HasSuffix(req.Spec().Procedure, "/Push") && failed.CompareAndSwap(false, true) {
							// The server applied the push, then reported an
							// internal failure, as when its commit outcome is
							// unknown.
							for _, wr := range resp.Any().(*syncplanev1.PushResponse).GetResults() {
								wr.Results, wr.ErrorCode, wr.ErrorMessage = nil, client.CodeInternal, "commit outcome unknown"
							}
						}
						return resp, err
					}
				})
				rc, err := client.New(url, client.WithInterceptors(internalAfterApply))
				Expect(err).NotTo(HaveOccurred())

				res, err := rc.Workspace("acme").Put(ctx, &todov1.Todo{Id: "t1"})
				Expect(err).NotTo(HaveOccurred())
				Expect(res).To(Equal(client.Result{MutationID: 1, AlreadyApplied: true}))
			})

			It("frees the id of a push the server refused as a request", func() {
				rc, err := client.New(url, client.WithInterceptors(failFirstPush(connect.CodeInvalidArgument)))
				Expect(err).NotTo(HaveOccurred())
				rws := rc.Workspace("acme")
				_, err = rws.Put(ctx, &todov1.Todo{Id: "t1"})
				Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
				res, err := rws.Put(ctx, &todov1.Todo{Id: "t1"})
				Expect(err).NotTo(HaveOccurred())
				Expect(res.MutationID).To(Equal(uint64(1)))
			})
		})
	})

	Describe("text", func() {
		bodyOf := func(key string) string {
			row, ok, err := client.Get[*testdatav1.Widget](ctx, ws, key)
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())
			s, err := text.PlainText(row.Value.GetBody())
			Expect(err).NotTo(HaveOccurred())
			return s
		}

		It("writes a text field from a string on put and patch", func() {
			_, err := ws.Put(ctx, &testdatav1.Widget{Id: "w1"}, client.WithText("body", "hello"))
			Expect(err).NotTo(HaveOccurred())
			Expect(bodyOf("w1")).To(Equal("hello"))

			_, err = ws.Patch(ctx, &testdatav1.Widget{Id: "w1"}, nil, client.WithText("body", "hello world"))
			Expect(err).NotTo(HaveOccurred())
			Expect(bodyOf("w1")).To(Equal("hello world"))
		})

		It("diffs against the stored document, edits of other writers included", func() {
			_, err := ws.Put(ctx, &testdatav1.Widget{Id: "w1"}, client.WithText("body", "abc"))
			Expect(err).NotTo(HaveOccurred())

			other, err := client.New(url)
			Expect(err).NotTo(HaveOccurred())
			_, err = other.Workspace("acme").Patch(ctx, &testdatav1.Widget{Id: "w1"}, nil,
				client.WithText("body", "abcd"))
			Expect(err).NotTo(HaveOccurred())

			_, err = ws.Patch(ctx, &testdatav1.Widget{Id: "w1"}, nil, client.WithText("body", "Xabcd"))
			Expect(err).NotTo(HaveOccurred())
			Expect(bodyOf("w1")).To(Equal("Xabcd"))
		})

		It("addresses a document inside a keyed list element", func() {
			row := &testdatav1.KeyedTextEntity{Id: "k1", Notes: []*testdatav1.KeyedNote{{Id: "n1"}, {Id: "n2"}}}
			_, err := ws.Put(ctx, row, client.WithText("notes.n2.body", "second"))
			Expect(err).NotTo(HaveOccurred())
			got, ok, err := client.Get[*testdatav1.KeyedTextEntity](ctx, ws, "k1")
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())
			Expect(got.Value.GetNotes()).To(HaveLen(2))
			s, err := text.PlainText(got.Value.GetNotes()[1].GetBody())
			Expect(err).NotTo(HaveOccurred())
			Expect(s).To(Equal("second"))
			Expect(got.Value.GetNotes()[0].GetBody()).To(BeEmpty())
			Expect(row.GetNotes()[1].GetBody()).To(BeEmpty(), "the caller's message is not changed")
		})

		It("refuses a path that is not a text field", func() {
			_, err := ws.Put(ctx, &testdatav1.Widget{Id: "w1"}, client.WithText("label", "x"))
			Expect(err).To(MatchError(ContainSubstring("no text field")))
		})
	})

	Describe("reads", func() {
		BeforeEach(func() {
			_, err := ws.Apply(ctx,
				client.Put(&todov1.Todo{Id: "t1", Title: "one", Completed: true}),
				client.Put(&todov1.Todo{Id: "t2", Title: "two"}),
				client.Put(&todov1.Todo{Id: "t3", Title: "three", Completed: true}),
				client.Put(&todov1.Todo{Id: "t4", Title: "secret"}),
			)
			Expect(err).NotTo(HaveOccurred())
		})

		It("gets one row and reports an absent or unreadable one", func() {
			todo, ok := getTodo("t2")
			Expect(ok).To(BeTrue())
			Expect(todo.GetTitle()).To(Equal("two"))
			_, ok = getTodo("missing")
			Expect(ok).To(BeFalse())
			_, ok = getTodo("t4")
			Expect(ok).To(BeFalse(), "the read filter hides it")
		})

		It("queries with a predicate and an order", func() {
			page, err := client.Query[*todov1.Todo](ctx, ws, query.Spec{
				Where:   query.Eq("completed", true),
				OrderBy: []*syncplanev1.OrderKey{query.Desc("title")},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(titles(page.Rows)).To(Equal([]string{"three", "one"}))
			Expect(page.HasMore).To(BeFalse())
		})

		It("passes the server's refusal of a bad query through", func() {
			_, err := client.Query[*todov1.Todo](ctx, ws, query.Spec{Where: query.Eq("nope", 1)})
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
		})

		It("pages through every row with QueryAll", func() {
			var got []string
			for row, err := range client.QueryAll[*todov1.Todo](ctx, ws, query.Spec{Limit: 1}) {
				Expect(err).NotTo(HaveOccurred())
				got = append(got, row.Value.GetTitle())
			}
			Expect(got).To(Equal([]string{"one", "two", "three"}))
		})

		It("bootstraps and pulls the changes after the snapshot", func() {
			snap, err := ws.Bootstrap(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(snap.Cursor).NotTo(BeEmpty())
			var keys []string
			for rc, err := range snap.Rows() {
				Expect(err).NotTo(HaveOccurred())
				keys = append(keys, rc.GetEntity()+"/"+rc.GetKey())
			}
			Expect(keys).To(ConsistOf("todo/t1", "todo/t2", "todo/t3"))

			_, err = ws.Delete(ctx, &todov1.Todo{}, "t1")
			Expect(err).NotTo(HaveOccurred())
			_, err = ws.Put(ctx, &todov1.Todo{Id: "t5", Title: "five"})
			Expect(err).NotTo(HaveOccurred())

			var changes []client.Row[*todov1.Todo]
			var cursor []byte
			for page, err := range ws.Pull(ctx, snap.Cursor, client.WithPageLimit(1)) {
				Expect(err).NotTo(HaveOccurred())
				for _, rc := range page.GetPatch().GetChanges() {
					row, err := client.DecodeRow[*todov1.Todo](rc)
					Expect(err).NotTo(HaveOccurred())
					changes = append(changes, row)
				}
				cursor = page.GetNextCursor()
			}
			Expect(changes).To(HaveLen(2))
			Expect(changes[0].Key).To(Equal("t1"))
			Expect(changes[0].Deleted).To(BeTrue())
			Expect(changes[1].Value.GetTitle()).To(Equal("five"))

			n := 0
			for _, err := range ws.Pull(ctx, cursor) {
				Expect(err).NotTo(HaveOccurred())
				n++
			}
			Expect(n).To(Equal(1), "one empty page when nothing changed")
		})

		It("pulls a page again from its own cursor when the page has more but no next cursor", func() {
			snap, err := ws.Bootstrap(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(snap.Close()).To(Succeed())
			_, err = ws.Put(ctx, &todov1.Todo{Id: "t5", Title: "five"})
			Expect(err).NotTo(HaveOccurred())
			_, err = ws.Put(ctx, &todov1.Todo{Id: "t6", Title: "six"})
			Expect(err).NotTo(HaveOccurred())

			var stripped atomic.Bool
			stripFirstCursor := connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
				return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
					resp, err := next(ctx, req)
					if err == nil {
						if page, ok := resp.Any().(*syncplanev1.PullResponse); ok && page.GetHasMore() && stripped.CompareAndSwap(false, true) {
							page.NextCursor = nil
						}
					}
					return resp, err
				}
			})
			rc, err := client.New(url, client.WithInterceptors(stripFirstCursor))
			Expect(err).NotTo(HaveOccurred())

			var keys []string
			for page, err := range rc.Workspace("acme").Pull(ctx, snap.Cursor, client.WithPageLimit(1)) {
				Expect(err).NotTo(HaveOccurred())
				for _, rc := range page.GetPatch().GetChanges() {
					keys = append(keys, rc.GetKey())
				}
			}
			// Not from the start of the workspace, which would bring t1 to t4.
			Expect(keys).To(Equal([]string{"t5", "t5", "t6"}))
		})

		It("scopes a bootstrap to subscriptions", func() {
			snap, err := ws.Bootstrap(ctx, client.WithSubscriptions(&syncplanev1.Subscription{
				Entity: "todo", KeySet: &syncplanev1.KeySet{Keys: []string{"t2"}},
			}))
			Expect(err).NotTo(HaveOccurred())
			defer snap.Close()
			var keys []string
			for rc, err := range snap.Rows() {
				Expect(err).NotTo(HaveOccurred())
				keys = append(keys, rc.GetKey())
			}
			Expect(keys).To(Equal([]string{"t2"}))
		})

		It("refuses DecodeRow of another entity's row", func() {
			_, err := client.DecodeRow[*testdatav1.Widget](&syncplanev1.RowChange{Entity: "todo", Key: "t1"})
			Expect(err).To(HaveOccurred())
		})

		It("subscribes: catches up, then streams live changes", func() {
			sctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			var seen []string
			caughtUp := false
			for ev, err := range c.Subscribe(sctx, []client.Scope{{Workspace: "acme"}}) {
				Expect(err).NotTo(HaveOccurred())
				Expect(ev.Workspace).To(Equal("acme"))
				switch {
				case ev.CaughtUp:
					caughtUp = true
					go func() {
						defer GinkgoRecover()
						_, err := ws.Put(ctx, &todov1.Todo{Id: "live", Title: "live"})
						Expect(err).NotTo(HaveOccurred())
					}()
				case ev.Page != nil:
					for _, rc := range ev.Page.GetPatch().GetChanges() {
						seen = append(seen, rc.GetKey())
					}
				}
				if caughtUp && len(seen) > 0 && seen[len(seen)-1] == "live" {
					cancel()
				}
			}
			Expect(caughtUp).To(BeTrue())
			Expect(seen).To(ContainElements("t1", "t2", "t3", "live"))
		})

		It("reports a dropped workspace and ends when no scope is left", func() {
			snap, err := ws.Bootstrap(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(snap.Close()).To(Succeed())
			sctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			var dropped error
			for ev, err := range c.Subscribe(sctx, []client.Scope{{Workspace: "acme", Cursor: snap.Cursor}}) {
				Expect(err).NotTo(HaveOccurred())
				if ev.CaughtUp {
					Expect(srv.DeleteWorkspace(ctx, "acme")).To(Succeed())
				}
				if ev.Dropped != nil {
					dropped = ev.Dropped
				}
			}
			Expect(sctx.Err()).NotTo(HaveOccurred(), "the stream ended on its own")
			Expect(dropped).To(HaveOccurred())
		})
	})

	Describe("blobs", func() {
		It("uploads a blob and downloads it through a row that refers to it", func() {
			data := bytes.Repeat([]byte("syncplane"), 1000)
			ref, err := ws.UploadBlob(ctx, bytes.NewReader(data), int64(len(data)), "text/plain")
			Expect(err).NotTo(HaveOccurred())
			Expect(ref.GetDigest()).To(HavePrefix("sp1:"))
			Expect(ref.GetSize()).To(Equal(int64(len(data))))

			again, err := ws.UploadBlob(ctx, bytes.NewReader(data), int64(len(data)), "text/plain")
			Expect(err).NotTo(HaveOccurred())
			Expect(again.GetDigest()).To(Equal(ref.GetDigest()))

			var out bytes.Buffer
			_, err = ws.DownloadBlob(ctx, ref.GetDigest(), &out)
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeNotFound), "no row refers to it yet")

			_, err = ws.Put(ctx, &testdatav1.BlobHolder{Id: "h1", Photo: ref})
			Expect(err).NotTo(HaveOccurred())
			n, err := ws.DownloadBlob(ctx, ref.GetDigest(), &out)
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(len(data))))
			Expect(out.Bytes()).To(Equal(data))
		})

		It("waits for a blob that is being uploaded", func() {
			data := bytes.Repeat([]byte("pending"), 500)
			m, err := blob.Compute(bytes.NewReader(data))
			Expect(err).NotTo(HaveOccurred())
			ref := &syncplanev1.BlobRef{Digest: m.Digest.String(), Size: m.Size}
			_, err = ws.Put(ctx, &testdatav1.BlobHolder{Id: "h2", Photo: ref})
			Expect(err).NotTo(HaveOccurred())
			// Staged with its one chunk still missing.
			_, err = c.BlobService().BeginUpload(ctx, connect.NewRequest(&syncplanev1.BeginUploadRequest{
				WorkspaceId: "acme", Digest: ref.Digest, Size: m.Size, ChunkHashes: m.ChunkHashes,
			}))
			Expect(err).NotTo(HaveOccurred())

			// A caller that gives up first gets its context's error.
			short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer cancel()
			_, err = ws.DownloadBlob(short, ref.Digest, io.Discard)
			Expect(err).To(MatchError(context.DeadlineExceeded))

			type result struct {
				out []byte
				err error
			}
			got := make(chan result, 1)
			go func() {
				var out bytes.Buffer
				_, err := ws.DownloadBlob(ctx, ref.Digest, &out)
				got <- result{out.Bytes(), err}
			}()
			Consistently(got, "200ms").ShouldNot(Receive())
			_, err = c.BlobService().PutChunk(ctx, connect.NewRequest(&syncplanev1.PutChunkRequest{
				WorkspaceId: "acme", Digest: ref.Digest, Index: 0, Data: data,
			}))
			Expect(err).NotTo(HaveOccurred())
			var r result
			Eventually(got, "5s").Should(Receive(&r))
			Expect(r.err).NotTo(HaveOccurred())
			Expect(r.out).To(Equal(data))
		})

		It("refuses a reader shorter than the size", func() {
			_, err := ws.UploadBlob(ctx, strings.NewReader("abc"), 10, "")
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("presence", func() {
		It("publishes presence and broadcasts to a watcher", func() {
			watcher := ws.Room("lobby")
			sender := c.Workspace("acme").Room("lobby")
			wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			var gotPresence, gotMessage bool
			for frame, err := range watcher.Watch(wctx) {
				Expect(err).NotTo(HaveOccurred())
				switch {
				case frame.GetSnapshot() != nil:
					_, err := sender.SetPresence(ctx, []byte("here"), 0)
					Expect(err).NotTo(HaveOccurred())
				case frame.GetUpdated() != nil:
					if frame.GetUpdated().GetSessionId() == sender.SessionID() {
						Expect(frame.GetUpdated().GetPresence().GetState()).To(Equal([]byte("here")))
						gotPresence = true
						Expect(sender.Broadcast(ctx, []byte("hi"))).To(Succeed())
					}
				case frame.GetMessage() != nil:
					Expect(frame.GetMessage().GetPayload()).To(Equal([]byte("hi")))
					gotMessage = true
					cancel()
				}
			}
			Expect(gotPresence).To(BeTrue())
			Expect(gotMessage).To(BeTrue())
		})
	})
})

func titles(rows []client.Row[*todov1.Todo]) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.Value.GetTitle()
	}
	return out
}

// lossyTransport forwards every request, then loses the response of the first
// dropPushes Push calls (the server applied them), or fails the first
// failPushes Push calls before they reach the server.
type lossyTransport struct {
	base       http.RoundTripper
	dropPushes atomic.Int64
	failPushes atomic.Int64
	pushes     atomic.Int64
}

func (t *lossyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if !strings.HasSuffix(req.URL.Path, "/Push") {
		return t.base.RoundTrip(req)
	}
	t.pushes.Add(1)
	if t.failPushes.Add(-1) >= 0 {
		return nil, errors.New("connection refused")
	}
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	if t.dropPushes.Add(-1) >= 0 {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		return nil, errors.New("connection reset")
	}
	return resp, nil
}

// failFirstPush fails the first Push with code before it is sent.
func failFirstPush(code connect.Code) connect.Interceptor {
	var done atomic.Bool
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			if strings.HasSuffix(req.Spec().Procedure, "/Push") && done.CompareAndSwap(false, true) {
				return nil, connect.NewError(code, errors.New("refused"))
			}
			return next(ctx, req)
		}
	})
}
