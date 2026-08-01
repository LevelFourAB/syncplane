package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"

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

// bodyDoc builds an encoded text document owned by peer with the given text.
func bodyDoc(peer, s string) []byte {
	d := text.New(peer)
	Expect(d.Insert(0, s)).To(Succeed())
	return d.Encode()
}

// editBody decodes state as peer, applies edit, and re-encodes.
func editBody(state []byte, peer string, edit func(d *text.Doc)) []byte {
	d, err := text.Decode(state, peer)
	Expect(err).NotTo(HaveOccurred())
	edit(d)
	return d.Encode()
}

// fragmentOf encodes what state carries beyond base (nil base = self-contained).
func fragmentOf(state, base []byte) []byte {
	GinkgoHelper()
	d, err := text.Decode(state, "")
	Expect(err).NotTo(HaveOccurred())
	var b *text.Doc
	if base != nil {
		b, err = text.Decode(base, "")
		Expect(err).NotTo(HaveOccurred())
	}
	frag, err := text.EncodeFragment(d, b)
	Expect(err).NotTo(HaveOccurred())
	return frag
}

func plainBody(state []byte) string {
	s, err := text.PlainText(state)
	Expect(err).NotTo(HaveOccurred())
	return s
}

var _ = Describe("Text fields", func() {
	It("auto-installs the text merger at registration", func() {
		// The path is occupied by the built-in merger, so declaring one there
		// reports a duplicate — proof the auto-install ran.
		Expect(newTestServerErr(server.WithMerger(&testdatav1.Widget{}, "body", concatDocs))).
			To(MatchError(ContainSubstring("already registered")))
	})

	Describe("over push", func() {
		var (
			ctx    context.Context
			srv    *server.Server
			client syncplanev1connect.SyncServiceClient
		)

		BeforeEach(func() {
			ctx = context.Background()
			srv = newTestServer()
			path, handler := srv.Handler()
			mux := http.NewServeMux()
			mux.Handle(path, handler)
			ts := httptest.NewServer(mux)
			DeferCleanup(ts.Close)
			client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
		})

		push := func(mutations ...*syncplanev1.Mutation) []*syncplanev1.MutationResult {
			GinkgoHelper()
			resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, mutations)))
			Expect(err).NotTo(HaveOccurred())
			return pushResults(resp.Msg)
		}

		getWidget := func(key string) *testdatav1.Widget {
			GinkgoHelper()
			resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
				WorkspaceId: ws,
			}))
			Expect(err).NotTo(HaveOccurred())
			for _, rc := range resp.Msg.GetPatch().GetChanges() {
				if rc.GetEntity() == "widget" && rc.GetKey() == key && !rc.GetTombstone() {
					var w testdatav1.Widget
					Expect(proto.Unmarshal(rc.GetValue(), &w)).To(Succeed())
					return &w
				}
			}
			Fail("widget " + key + " not found")
			return nil
		}

		It("merges concurrent PATCHes without interleaving", func() {
			base := bodyDoc("seed", "[]")
			push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{
				Id: "w1", Body: base,
			}))

			// Two clients each type into the same position of the same base
			// state, then push their whole-document PATCHes concurrently.
			aliceState := editBody(base, "alice", func(d *text.Doc) {
				Expect(d.Insert(1, "alice")).To(Succeed())
			})
			bobState := editBody(base, "bob", func(d *text.Doc) {
				Expect(d.Insert(1, "bob")).To(Succeed())
			})
			push(patchMutation("c2", 1, "widget", "w1", &testdatav1.Widget{Body: aliceState}, "body"))
			push(patchMutation("c3", 1, "widget", "w1", &testdatav1.Widget{Body: bobState}, "body"))

			got := plainBody(getWidget("w1").GetBody())
			Expect(got).To(Equal("[alicebob]"), "sequences must stay contiguous")
		})

		It("PUT unions text fields with the stored row instead of resetting", func() {
			base := bodyDoc("seed", "one two three")
			push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Label: "v1", Body: base}))

			// A PATCH from a client still in flight while another client PUTs a
			// row built from the same base: without the union-on-PUT the PUT
			// would reset the doc and the PATCH would union the old document
			// back in, duplicating the text (issue 097).
			patched := editBody(base, "client", func(d *text.Doc) {
				Expect(d.Insert(13, " four")).To(Succeed())
			})
			putState := editBody(base, "editor", func(d *text.Doc) {
				Expect(d.SetString("one TWO three")).To(Succeed())
			})
			push(putMutation("c2", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Label: "v2", Body: putState}))
			push(patchMutation("c3", 1, "widget", "w1", &testdatav1.Widget{Body: patched}, "body"))

			w := getWidget("w1")
			Expect(w.GetLabel()).To(Equal("v2"), "non-text fields keep replace semantics")
			Expect(plainBody(w.GetBody())).To(Equal("one TWO three four"))
		})

		It("rejects junk text bytes on PUT with invalid_argument", func() {
			results := push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{
				Id: "w1", Body: []byte{0xff, 0x00, 0x01},
			}))
			Expect(results).To(HaveLen(1))
			Expect(results[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(results[0].GetErrorCode()).To(Equal("invalid_argument"))
			Expect(results[0].GetSeq()).To(BeZero(), "a rejection must not consume a sequence")
		})

		It("rejects junk text bytes on PATCH with invalid_argument, even onto an absent field", func() {
			push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1"}))
			results := push(patchMutation("c2", 1, "widget", "w1", &testdatav1.Widget{
				Body: []byte{0xff},
			}, "body"))
			Expect(results).To(HaveLen(1))
			Expect(results[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(results[0].GetErrorCode()).To(Equal("invalid_argument"))
		})

		Describe("delta-state PATCHes", func() {
			It("reaches byte-identical state to the equivalent full-state PATCH", func() {
				base := bodyDoc("seed", "the quick brown fox")
				push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Body: base}))
				push(putMutation("c1", 2, "widget", "w2", &testdatav1.Widget{Id: "w2", Body: base}))

				full := editBody(base, "alice", func(d *text.Doc) {
					Expect(d.SetString("the quick brown fox jumps")).To(Succeed())
				})
				frag := fragmentOf(full, base)
				Expect(len(frag)).To(BeNumerically("<", len(full)), "a fragment must be the smaller payload")

				push(patchMutation("c2", 1, "widget", "w1", &testdatav1.Widget{Body: frag}, "body"))
				push(patchMutation("c3", 1, "widget", "w2", &testdatav1.Widget{Body: full}, "body"))

				Expect(getWidget("w1").GetBody()).To(Equal(getWidget("w2").GetBody()))
				Expect(plainBody(getWidget("w1").GetBody())).To(Equal("the quick brown fox jumps"))
			})

			It("applies a self-contained fragment onto a row carrying no document", func() {
				push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Label: "empty"}))
				frag := fragmentOf(bodyDoc("alice", "first words"), nil)

				results := push(patchMutation("c2", 1, "widget", "w1",
					&testdatav1.Widget{Body: frag}, "body"))
				Expect(results[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
				Expect(plainBody(getWidget("w1").GetBody())).To(Equal("first words"))
			})

			It("converges with a concurrent full-state PATCH", func() {
				base := bodyDoc("seed", "[]")
				push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Body: base}))

				aliceState := editBody(base, "alice", func(d *text.Doc) {
					Expect(d.Insert(1, "alice")).To(Succeed())
				})
				bobState := editBody(base, "bob", func(d *text.Doc) {
					Expect(d.Insert(1, "bob")).To(Succeed())
				})
				push(patchMutation("c2", 1, "widget", "w1",
					&testdatav1.Widget{Body: fragmentOf(aliceState, base)}, "body"))
				push(patchMutation("c3", 1, "widget", "w1",
					&testdatav1.Widget{Body: bobState}, "body"))

				Expect(plainBody(getWidget("w1").GetBody())).To(Equal("[alicebob]"))
			})

			It("is idempotent when the same fragment is redelivered", func() {
				base := bodyDoc("seed", "hello")
				push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Body: base}))
				frag := fragmentOf(editBody(base, "alice", func(d *text.Doc) {
					Expect(d.Insert(5, " world")).To(Succeed())
				}), base)

				push(patchMutation("c2", 1, "widget", "w1", &testdatav1.Widget{Body: frag}, "body"))
				after := getWidget("w1").GetBody()
				// A different client resending the same bytes is the shape a retry
				// past the watermark takes; the apply must be a no-op.
				push(patchMutation("c3", 1, "widget", "w1", &testdatav1.Widget{Body: frag}, "body"))
				Expect(getWidget("w1").GetBody()).To(Equal(after))
			})

			It("rejects a fragment the row is not built on, leaving the row untouched", func() {
				stored := bodyDoc("other", "unrelated")
				push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Body: stored}))

				// Cut against a base the server has never seen: the fragment's items
				// hang off parents this row does not carry.
				elsewhere := bodyDoc("seed", "hello")
				frag := fragmentOf(editBody(elsewhere, "alice", func(d *text.Doc) {
					Expect(d.Insert(5, " world")).To(Succeed())
				}), elsewhere)

				results := push(patchMutation("c2", 1, "widget", "w1",
					&testdatav1.Widget{Body: frag}, "body"))
				Expect(results).To(HaveLen(1))
				Expect(results[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
				Expect(results[0].GetErrorCode()).To(Equal("text_full_state_required"))
				Expect(results[0].GetSeq()).To(BeZero(), "a rejection must not consume a sequence")
				Expect(getWidget("w1").GetBody()).To(Equal(stored))
			})

			It("rejects a fragment in a field the mask does not name", func() {
				push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Label: "v1"}))
				frag := fragmentOf(bodyDoc("alice", "hidden"), nil)

				// Only "label" is masked, so the merger would never see the body:
				// delta state that cannot be unfolded must never be accepted.
				results := push(patchMutation("c2", 1, "widget", "w1",
					&testdatav1.Widget{Label: "v2", Body: frag}, "label"))
				Expect(results[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
				Expect(results[0].GetErrorCode()).To(Equal("text_full_state_required"))
				Expect(getWidget("w1").GetLabel()).To(Equal("v1"))
			})

			It("rejects a fragment on PUT, which always carries whole rows", func() {
				frag := fragmentOf(bodyDoc("alice", "hello"), nil)
				results := push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{
					Id: "w1", Body: frag,
				}))
				Expect(results[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
				Expect(results[0].GetErrorCode()).To(Equal("text_full_state_required"))
			})
		})

		It("keeps marks attached across concurrent edits", func() {
			base := bodyDoc("seed", "hello world")
			push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Body: base}))

			marked := editBody(base, "alice", func(d *text.Doc) {
				_, err := d.Mark(6, 11, "comment:c1", true, text.ExpandNone)
				Expect(err).NotTo(HaveOccurred())
			})
			edited := editBody(base, "bob", func(d *text.Doc) {
				Expect(d.Insert(6, "big ")).To(Succeed())
			})
			push(patchMutation("c2", 1, "widget", "w1", &testdatav1.Widget{Body: marked}, "body"))
			push(patchMutation("c3", 1, "widget", "w1", &testdatav1.Widget{Body: edited}, "body"))

			d, err := text.Decode(getWidget("w1").GetBody(), "")
			Expect(err).NotTo(HaveOccurred())
			Expect(d.PlainText()).To(Equal("hello big world"))
			Expect(d.Marks()).To(Equal([]text.Span{
				{Start: 10, End: 15, Key: "comment:c1", Value: true},
			}))
		})
	})

	Describe("EntityTx.Put", func() {
		It("merges text fields with the stored row instead of clobbering", func() {
			srv := newTestServer()
			ctx := context.Background()

			base := bodyDoc("seed", "one two three")
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Widget{Id: "w1", Label: "v1", Body: base})
			})).To(Succeed())

			// A client edit lands after the connector read its snapshot...
			clientState := editBody(base, "client", func(d *text.Doc) {
				Expect(d.Insert(13, " four")).To(Succeed())
			})
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Patch(&testdatav1.Widget{Id: "w1", Body: clientState}, []string{"body"}, nil)
			})).To(Succeed())

			// ...and the connector then Puts a whole row built from that stale
			// snapshot. The text field must union, not replace; other fields
			// keep PUT semantics.
			connectorState := editBody(base, "connector", func(d *text.Doc) {
				Expect(d.SetString("one TWO three")).To(Succeed())
			})
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Widget{Id: "w1", Label: "v2", Body: connectorState})
			})).To(Succeed())

			var w testdatav1.Widget
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				found, err := tx.Get(&w, "w1")
				Expect(found).To(BeTrue())
				return err
			})).To(Succeed())
			Expect(w.GetLabel()).To(Equal("v2"))
			Expect(plainBody(w.GetBody())).To(Equal("one TWO three four"))
		})

		It("keeps replace semantics when the stored row has no document", func() {
			srv := newTestServer()
			ctx := context.Background()
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Widget{Id: "w1", Label: "no-body"})
			})).To(Succeed())
			state := bodyDoc("writer", "fresh")
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Widget{Id: "w1", Body: state})
			})).To(Succeed())
			var w testdatav1.Widget
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				_, err := tx.Get(&w, "w1")
				return err
			})).To(Succeed())
			Expect(plainBody(w.GetBody())).To(Equal("fresh"))
		})
	})

	Describe("string writes (WithTextString)", func() {
		var (
			srv *server.Server
			ctx context.Context
		)
		BeforeEach(func() {
			srv = newTestServer()
			ctx = context.Background()
		})

		getW := func(key string) *testdatav1.Widget {
			GinkgoHelper()
			var w testdatav1.Widget
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				found, err := tx.Get(&w, key)
				Expect(found).To(BeTrue())
				return err
			})).To(Succeed())
			return &w
		}

		It("creates a document from a string on Put", func() {
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Widget{Id: "w1", Label: "l"},
					server.WithTextString("body", "hello"),
					server.WithTextPeer("writer:1"))
			})).To(Succeed())
			Expect(plainBody(getW("w1").GetBody())).To(Equal("hello"))
		})

		It("three-way merges a string Patch with a concurrent edit", func() {
			base := bodyDoc("seed", "one two three")
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Widget{Id: "w1", Body: base})
			})).To(Succeed())
			// A concurrent client edit lands first...
			clientState := editBody(base, "client", func(d *text.Doc) {
				Expect(d.Insert(13, " four")).To(Succeed())
			})
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Patch(&testdatav1.Widget{Id: "w1", Body: clientState}, []string{"body"}, nil)
			})).To(Succeed())
			// ...then a string write diffs against the stored (merged) row, so
			// the concurrent words survive. The mask is added automatically.
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Patch(&testdatav1.Widget{Id: "w1"}, nil, nil,
					server.WithTextString("body", "one TWO three four"),
					server.WithTextPeer("writer:1"))
			})).To(Succeed())
			Expect(plainBody(getW("w1").GetBody())).To(Equal("one TWO three four"))
		})

		It("string edits from the same peer converge across sequential writes", func() {
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Widget{Id: "w1"},
					server.WithTextString("body", "alpha beta"),
					server.WithTextPeer("writer:1"))
			})).To(Succeed())
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Patch(&testdatav1.Widget{Id: "w1"}, nil, nil,
					server.WithTextString("body", "alpha GAMMA beta"),
					server.WithTextPeer("writer:1"))
			})).To(Succeed())
			Expect(plainBody(getW("w1").GetBody())).To(Equal("alpha GAMMA beta"))
		})

		It("an empty string deletes all visible text but concurrent inserts survive", func() {
			base := bodyDoc("seed", "abc")
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Widget{Id: "w1", Body: base})
			})).To(Succeed())
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Patch(&testdatav1.Widget{Id: "w1"}, nil, nil,
					server.WithTextString("body", ""),
					server.WithTextPeer("writer:1"))
			})).To(Succeed())
			Expect(plainBody(getW("w1").GetBody())).To(Equal(""))

			// A concurrent insert built on the pre-delete base still unions in:
			// select-all-delete tombstones the old items, not the field.
			concurrent := editBody(base, "client", func(d *text.Doc) {
				Expect(d.Insert(3, "d")).To(Succeed())
			})
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Patch(&testdatav1.Widget{Id: "w1", Body: concurrent}, []string{"body"}, nil)
			})).To(Succeed())
			Expect(plainBody(getW("w1").GetBody())).To(Equal("d"))
		})

		It("requires WithTextPeer alongside WithTextString", func() {
			err := srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Widget{Id: "w1"},
					server.WithTextString("body", "hello"))
			})
			Expect(err).To(MatchError(ContainSubstring("WithTextPeer")))
		})

		It("rejects a string for a non-text field", func() {
			err := srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Widget{Id: "w1"},
					server.WithTextString("label", "hello"),
					server.WithTextPeer("writer:1"))
			})
			Expect(err).To(MatchError(ContainSubstring("has no text field")))
		})
	})
})
