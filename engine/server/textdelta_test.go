package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"time"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/text"
)

// The contract every spec here checks: whatever a pull ships — full snapshot or
// delta state — a client that folds it into what it already held ends up with
// exactly the bytes the row holds on the server.
var _ = Describe("Downstream text delta state", func() {
	var (
		ctx    context.Context
		srv    *server.Server
		client syncplanev1connect.SyncServiceClient
	)

	newServerWith := func(opts ...server.Option) {
		srv = newTestServer(opts...)
		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	}

	BeforeEach(func() {
		ctx = context.Background()
		newServerWith()
	})

	push := func(mutations ...*syncplanev1.Mutation) []*syncplanev1.MutationResult {
		GinkgoHelper()
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, mutations)))
		Expect(err).NotTo(HaveOccurred())
		results := pushResults(resp.Msg)
		for _, r := range results {
			Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED),
				"setup push rejected: %s", r.GetErrorMessage())
		}
		return results
	}

	pull := func(cursor []byte, subs ...*syncplanev1.Subscription) *syncplanev1.PullResponse {
		GinkgoHelper()
		resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId:   ws,
			Cursor:        cursor,
			Subscriptions: subs,
		}))
		Expect(err).NotTo(HaveOccurred())
		return resp.Msg
	}

	// changeFor finds one row's change in a pull response.
	changeFor := func(resp *syncplanev1.PullResponse, key string) *syncplanev1.RowChange {
		GinkgoHelper()
		for _, rc := range resp.GetPatch().GetChanges() {
			if rc.GetEntity() == "widget" && rc.GetKey() == key {
				return rc
			}
		}
		Fail("no change for widget " + key)
		return nil
	}

	// reconstruct is the client half: fold a change's delta state back into the
	// row the reader already holds, returning the full row bytes it ends up
	// with. `held` is that reader's confirmed row (nil when it holds none).
	reconstruct := func(rc *syncplanev1.RowChange, held []byte) []byte {
		GinkgoHelper()
		var next testdatav1.Widget
		Expect(proto.Unmarshal(rc.GetValue(), &next)).To(Succeed())
		if len(rc.GetTextDeltas()) == 0 {
			return rc.GetValue()
		}
		var prev testdatav1.Widget
		if held != nil {
			Expect(proto.Unmarshal(held, &prev)).To(Succeed())
		}
		for _, fd := range rc.GetTextDeltas() {
			Expect(fd.GetField()).To(Equal("body"), "the test entity has one text field")
			if len(fd.GetFragment()) == 0 {
				next.Body = prev.GetBody()
				continue
			}
			merged, err := text.ApplyFragmentBytes(prev.GetBody(), fd.GetFragment())
			Expect(err).NotTo(HaveOccurred())
			next.Body = merged
		}
		return mustMarshal(&next)
	}

	// storedRow is the full snapshot a fresh reader gets — the ground truth
	// every reconstruction is compared against.
	storedRow := func(key string) []byte {
		GinkgoHelper()
		return changeFor(pull(nil), key).GetValue()
	}

	Describe("a reader catching up", func() {
		It("ships one edit as a fragment, and folding it reproduces the row", func() {
			base := bodyDoc("seed", "the quick brown fox jumps over the lazy dog")
			push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Body: base}))

			// The reader is caught up through the create.
			first := pull(nil)
			held := changeFor(first, "w1").GetValue()
			Expect(changeFor(first, "w1").GetTextDeltas()).To(BeEmpty(),
				"a row created past the cursor still ships whole the first time it is small")

			edited := editBody(base, "alice", func(d *text.Doc) {
				Expect(d.SetString("the quick brown fox leaps over the lazy dog")).To(Succeed())
			})
			push(patchMutation("c2", 1, "widget", "w1", &testdatav1.Widget{Body: edited}, "body"))

			rc := changeFor(pull(first.GetNextCursor()), "w1")
			Expect(rc.GetTextDeltas()).To(HaveLen(1))
			Expect(rc.GetTextDeltas()[0].GetFragment()).NotTo(BeEmpty())
			var stripped testdatav1.Widget
			Expect(proto.Unmarshal(rc.GetValue(), &stripped)).To(Succeed())
			Expect(stripped.GetBody()).To(BeEmpty(), "the document is not in the value")
			Expect(len(rc.GetValue())).To(BeNumerically("<", len(held)/2),
				"the point of the exercise is a smaller page")

			Expect(reconstruct(rc, held)).To(Equal(storedRow("w1")))
		})

		It("unions every write in the span into one fragment", func() {
			const opening = "it was the best of times, it was the worst of times"
			base := bodyDoc("seed", opening)
			push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Body: base}))
			first := pull(nil)
			held := changeFor(first, "w1").GetValue()

			// Three separate writes, each by a different peer, while the reader
			// is away.
			state := base
			for i, peer := range []string{"a", "b", "c"} {
				word := []string{" one", " two", " three"}[i]
				state = editBody(state, peer, func(d *text.Doc) {
					Expect(d.Insert(d.Len(), word)).To(Succeed())
				})
				push(patchMutation("c2", uint64(i+1), "widget", "w1",
					&testdatav1.Widget{Body: state}, "body"))
			}

			rc := changeFor(pull(first.GetNextCursor()), "w1")
			Expect(rc.GetTextDeltas()).To(HaveLen(1))
			got := reconstruct(rc, held)
			Expect(got).To(Equal(storedRow("w1")))
			var w testdatav1.Widget
			Expect(proto.Unmarshal(got, &w)).To(Succeed())
			Expect(plainBody(w.GetBody())).To(Equal(opening + " one two three"))
		})

		It("ships an empty fragment when only other fields changed", func() {
			base := bodyDoc("seed", "a document long enough that re-sending it would be a waste of everyone's bytes")
			push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Body: base}))
			first := pull(nil)
			held := changeFor(first, "w1").GetValue()

			push(patchMutation("c2", 1, "widget", "w1", &testdatav1.Widget{Label: "renamed"}, "label"))

			rc := changeFor(pull(first.GetNextCursor()), "w1")
			Expect(rc.GetTextDeltas()).To(HaveLen(1))
			Expect(rc.GetTextDeltas()[0].GetFragment()).To(BeEmpty(),
				"nothing moved, so the reader keeps what it holds")
			Expect(len(rc.GetValue())).To(BeNumerically("<", 40))
			Expect(reconstruct(rc, held)).To(Equal(storedRow("w1")))
		})

		It("leaves rows without text alone", func() {
			push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Label: "no text here"}))
			rc := changeFor(pull(nil), "w1")
			Expect(rc.GetTextDeltas()).To(BeEmpty())
		})
	})

	Describe("falling back to full state", func() {
		// Every one of these must still reproduce the stored row — the fallback
		// is a size decision, so correctness cannot depend on which side wins.
		It("ships the document when the reader's cursor predates the log", func() {
			newServerWith(server.WithGC(time.Hour, 0), server.WithTextDeltaRetention(time.Nanosecond))
			base := bodyDoc("seed", "the quick brown fox jumps over the lazy dog")
			push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Body: base}))
			first := pull(nil)

			edited := editBody(base, "alice", func(d *text.Doc) {
				Expect(d.SetString("the quick brown fox leaps over the lazy dog")).To(Succeed())
			})
			push(patchMutation("c2", 1, "widget", "w1", &testdatav1.Widget{Body: edited}, "body"))
			// The log is pruned out from under the reader before it returns.
			// (Log timestamps have millisecond resolution, so age the entry past
			// the nanosecond retention before collecting.)
			time.Sleep(5 * time.Millisecond)
			Expect(srv.CollectGarbage(ctx, ws)).To(Succeed())

			rc := changeFor(pull(first.GetNextCursor()), "w1")
			Expect(rc.GetTextDeltas()).To(BeEmpty())
			Expect(rc.GetValue()).To(Equal(storedRow("w1")))
		})

		It("ships the document when a write in the span could not be a delta", func() {
			base := bodyDoc("seed", "the quick brown fox jumps over the lazy dog")
			push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Body: base}))
			first := pull(nil)
			held := changeFor(first, "w1").GetValue()

			// A PUT with no document clears the field — the one text write that
			// removes state instead of adding it, which no fragment can express.
			push(putMutation("c2", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Label: "cleared"}))
			restored := bodyDoc("again", "starting over from nothing at all")
			push(patchMutation("c2", 2, "widget", "w1", &testdatav1.Widget{Body: restored}, "body"))

			rc := changeFor(pull(first.GetNextCursor()), "w1")
			Expect(rc.GetTextDeltas()).To(BeEmpty(), "the clearing write is a hole in the span")
			Expect(reconstruct(rc, held)).To(Equal(storedRow("w1")))
		})

		It("ships the document to a scoped subscription", func() {
			base := bodyDoc("seed", "the quick brown fox jumps over the lazy dog")
			push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{
				Id: "w1", Count: 5, Body: base,
			}))
			subs := []*syncplanev1.Subscription{widgetSub(countGte(1))}
			first := pull(nil, subs...)

			edited := editBody(base, "alice", func(d *text.Doc) {
				Expect(d.SetString("the quick brown fox leaps over the lazy dog")).To(Succeed())
			})
			push(patchMutation("c2", 1, "widget", "w1", &testdatav1.Widget{Body: edited}, "body"))

			rc := changeFor(pull(first.GetNextCursor(), subs...), "w1")
			Expect(rc.GetTextDeltas()).To(BeEmpty(),
				"a predicate could have hidden this row at the cursor, so its prior state is unknown")
			Expect(rc.GetValue()).To(Equal(storedRow("w1")))
		})

		It("ships the document on a re-creation after a delete", func() {
			base := bodyDoc("seed", "the quick brown fox jumps over the lazy dog")
			push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Body: base}))
			first := pull(nil)

			push(deleteMutation("c1", 2, "widget", "w1"))
			fresh := bodyDoc("again", "a whole new document under a whole new peer")
			push(putMutation("c1", 3, "widget", "w1", &testdatav1.Widget{Id: "w1", Body: fresh}))

			rc := changeFor(pull(first.GetNextCursor()), "w1")
			Expect(rc.GetTextDeltas()).To(BeEmpty(), "the delete broke the chain")
			Expect(rc.GetValue()).To(Equal(storedRow("w1")))
		})

		It("never ships delta state on bootstrap", func() {
			base := bodyDoc("seed", "the quick brown fox jumps over the lazy dog")
			push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Body: base}))
			push(patchMutation("c2", 1, "widget", "w1", &testdatav1.Widget{
				Body: editBody(base, "alice", func(d *text.Doc) {
					Expect(d.Insert(0, "x")).To(Succeed())
				}),
			}, "body"))

			stream, err := client.Bootstrap(ctx, connect.NewRequest(&syncplanev1.BootstrapRequest{
				WorkspaceId: ws,
			}))
			Expect(err).NotTo(HaveOccurred())
			seen := 0
			for stream.Receive() {
				for _, rc := range stream.Msg().GetChunk().GetRows() {
					Expect(rc.GetTextDeltas()).To(BeEmpty())
					if rc.GetKey() == "w1" {
						seen++
						Expect(rc.GetValue()).To(Equal(storedRow("w1")))
					}
				}
			}
			Expect(stream.Err()).NotTo(HaveOccurred())
			Expect(seen).To(Equal(1))
		})
	})

	Describe("the delta log itself", func() {
		It("folds several writes sharing one sequence (a batch) into one entry", func() {
			base := bodyDoc("seed", "start")
			push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Body: base}))
			first := pull(nil)
			held := changeFor(first, "w1").GetValue()

			// Two PATCHes of the same field in one batch mutation: they share a
			// sequence, so the log entry has to describe both.
			one := editBody(base, "a", func(d *text.Doc) {
				Expect(d.Insert(d.Len(), " one")).To(Succeed())
			})
			two := editBody(one, "b", func(d *text.Doc) {
				Expect(d.Insert(d.Len(), " two")).To(Succeed())
			})
			push(batchMutation("c2", 1,
				crudPatch("widget", "w1", &testdatav1.Widget{Body: one}, "body"),
				crudPatch("widget", "w1", &testdatav1.Widget{Body: two}, "body"),
			))

			rc := changeFor(pull(first.GetNextCursor()), "w1")
			got := reconstruct(rc, held)
			Expect(got).To(Equal(storedRow("w1")))
			var w testdatav1.Widget
			Expect(proto.Unmarshal(got, &w)).To(Succeed())
			Expect(plainBody(w.GetBody())).To(Equal("start one two"))
		})

		It("records nothing for a rejected mutation", func() {
			base := bodyDoc("seed", "the quick brown fox jumps over the lazy dog")
			push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Body: base}))
			first := pull(nil)
			held := changeFor(first, "w1").GetValue()

			// A rejected PATCH (junk bytes) rolls back inside its savepoint; its
			// log entry must roll back with it, or the next reader would fold a
			// delta describing a write that never happened.
			resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
				patchMutation("c2", 1, "widget", "w1", &testdatav1.Widget{Body: []byte{0xff}}, "body"),
			})))
			Expect(err).NotTo(HaveOccurred())
			Expect(pushResults(resp.Msg)[0].GetStatus()).
				To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))

			edited := editBody(base, "alice", func(d *text.Doc) {
				Expect(d.SetString("the quick brown fox leaps over the lazy dog")).To(Succeed())
			})
			push(patchMutation("c2", 2, "widget", "w1", &testdatav1.Widget{Body: edited}, "body"))

			rc := changeFor(pull(first.GetNextCursor()), "w1")
			Expect(reconstruct(rc, held)).To(Equal(storedRow("w1")))
		})

		It("covers trusted server-side writes too", func() {
			base := bodyDoc("seed", "the quick brown fox jumps over the lazy dog")
			push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Body: base}))
			first := pull(nil)
			held := changeFor(first, "w1").GetValue()

			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Patch(&testdatav1.Widget{Id: "w1"}, nil, nil,
					server.WithTextPeer("server"),
					server.WithTextString("body", "the quick brown fox leaps over the lazy dog"))
			})).To(Succeed())

			rc := changeFor(pull(first.GetNextCursor()), "w1")
			Expect(rc.GetTextDeltas()).To(HaveLen(1))
			Expect(reconstruct(rc, held)).To(Equal(storedRow("w1")))
		})
	})

	It("serves the same rows over Subscribe as over Pull", func() {
		base := bodyDoc("seed", "the quick brown fox jumps over the lazy dog")
		push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Body: base}))
		first := pull(nil)
		held := changeFor(first, "w1").GetValue()

		edited := editBody(base, "alice", func(d *text.Doc) {
			Expect(d.SetString("the quick brown fox leaps over the lazy dog")).To(Succeed())
		})
		push(patchMutation("c2", 1, "widget", "w1", &testdatav1.Widget{Body: edited}, "body"))

		streamCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		stream, err := client.Subscribe(streamCtx, connect.NewRequest(&syncplanev1.SubscribeRequest{
			ClientId: "c9",
			Scopes:   []*syncplanev1.WorkspaceScope{{WorkspaceId: ws, Cursor: first.GetNextCursor()}},
		}))
		Expect(err).NotTo(HaveOccurred())
		var rc *syncplanev1.RowChange
		for stream.Receive() {
			page := stream.Msg().GetPage()
			if page == nil {
				continue
			}
			for _, c := range page.GetPatch().GetChanges() {
				if c.GetKey() == "w1" {
					rc = c
				}
			}
			if rc != nil {
				break
			}
		}
		Expect(rc).NotTo(BeNil())
		Expect(rc.GetTextDeltas()).To(HaveLen(1))
		Expect(reconstruct(rc, held)).To(Equal(storedRow("w1")))
	})
})
