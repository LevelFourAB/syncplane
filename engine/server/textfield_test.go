package server_test

import (
	"context"
	"encoding/hex"
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

	It("rejects a text field inside an unkeyed repeated element", func() {
		_, err := server.New(newTestStore(),
			server.WithEntities(&testdatav1.ListTextEntity{}))
		Expect(err).To(MatchError(ContainSubstring(
			`text field "notes.body" is inside repeated field "notes"`)))
	})

	It("accepts a text field inside a keyed list element and a map value", func() {
		_, err := server.New(newTestStore(),
			server.WithEntities(&testdatav1.KeyedTextEntity{}, &testdatav1.MapTextEntity{}))
		Expect(err).NotTo(HaveOccurred())
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
			// back in, duplicating the text.
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

		It("rejects a PATCH whose item uses counter 0 and keeps the stored text readable", func() {
			push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Body: bodyDoc("seed", "hello")}))

			// Item mallory:0 "a" under the root, and zed:1 "b" as its right
			// child. Every byte is well formed; only the counter is not.
			hostile, err := hex.DecodeString(
				"0102076d616c6c6f7279037a6564000102000001060161010101040000016200")
			Expect(err).NotTo(HaveOccurred())
			results := push(patchMutation("c2", 1, "widget", "w1", &testdatav1.Widget{Body: hostile}, "body"))
			Expect(results).To(HaveLen(1))
			Expect(results[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(results[0].GetErrorCode()).To(Equal("invalid_argument"))

			// A later honest write must still merge into the stored document.
			push(patchMutation("c3", 1, "widget", "w1", &testdatav1.Widget{
				Body: editBody(getWidget("w1").GetBody(), "alice", func(d *text.Doc) {
					Expect(d.Insert(5, "!")).To(Succeed())
				}),
			}, "body"))
			Expect(plainBody(getWidget("w1").GetBody())).To(Equal("hello!"))
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

		// A plain-string value says what a field should read as without saying
		// who wrote it, so applying one means minting edits under a peer the
		// server does not have. A client resolves it at enqueue; a payload that
		// carries one to the wire skipped that, and no resend fixes it.
		Describe("an unresolved plain-string value", func() {
			plain := func(s string) []byte {
				GinkgoHelper()
				b, err := text.EncodePlainString(s)
				Expect(err).NotTo(HaveOccurred())
				return b
			}

			It("is rejected on PATCH, leaving the row untouched", func() {
				stored := bodyDoc("alice", "hello")
				push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Body: stored}))

				results := push(patchMutation("c2", 1, "widget", "w1",
					&testdatav1.Widget{Body: plain("goodbye")}, "body"))
				Expect(results[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
				Expect(results[0].GetErrorCode()).To(Equal(server.CodeInvalidArgument))
				Expect(getWidget("w1").GetBody()).To(Equal(stored))
			})

			It("is rejected on PUT", func() {
				results := push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{
					Id: "w1", Body: plain("hello"),
				}))
				Expect(results[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
				Expect(results[0].GetErrorCode()).To(Equal(server.CodeInvalidArgument))
			})

			It("is rejected in a field the mask does not name", func() {
				// The guard runs over the bytes a client sent, not over what the
				// merge would reach — the same strictness the fragment case above
				// applies in this position.
				push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Label: "v1"}))
				results := push(patchMutation("c2", 1, "widget", "w1",
					&testdatav1.Widget{Label: "v2", Body: plain("hidden")}, "label"))
				Expect(results[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
				Expect(results[0].GetErrorCode()).To(Equal(server.CodeInvalidArgument))
				Expect(getWidget("w1").GetLabel()).To(Equal("v1"))
			})
		})

		Describe("inside keyed list elements and map values", func() {
			getKeyed := func(key string) *testdatav1.KeyedTextEntity {
				GinkgoHelper()
				resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
					WorkspaceId: ws,
				}))
				Expect(err).NotTo(HaveOccurred())
				for _, rc := range resp.Msg.GetPatch().GetChanges() {
					if rc.GetEntity() == "keyed_text" && rc.GetKey() == key && !rc.GetTombstone() {
						var e testdatav1.KeyedTextEntity
						Expect(proto.Unmarshal(rc.GetValue(), &e)).To(Succeed())
						return &e
					}
				}
				Fail("keyed_text " + key + " not found")
				return nil
			}

			noteOf := func(e *testdatav1.KeyedTextEntity, id string) *testdatav1.KeyedNote {
				GinkgoHelper()
				for _, n := range e.GetNotes() {
					if n.GetId() == id {
						return n
					}
				}
				Fail("note " + id + " not found")
				return nil
			}

			It("merges concurrent PATCHes to the same element's document", func() {
				base := bodyDoc("seed", "[]")
				push(putMutation("c1", 1, "keyed_text", "k1", &testdatav1.KeyedTextEntity{
					Id: "k1",
					Notes: []*testdatav1.KeyedNote{
						{Id: "n1", Label: "first", Body: base},
						{Id: "n2", Label: "second", Body: bodyDoc("seed", "other")},
					},
				}))

				aliceState := editBody(base, "alice", func(d *text.Doc) {
					Expect(d.Insert(1, "alice")).To(Succeed())
				})
				bobState := editBody(base, "bob", func(d *text.Doc) {
					Expect(d.Insert(1, "bob")).To(Succeed())
				})
				push(patchMutation("c2", 1, "keyed_text", "k1", &testdatav1.KeyedTextEntity{
					Notes: []*testdatav1.KeyedNote{{Id: "n1", Body: aliceState}},
				}, "notes.n1.body"))
				push(patchMutation("c3", 1, "keyed_text", "k1", &testdatav1.KeyedTextEntity{
					Notes: []*testdatav1.KeyedNote{{Id: "n1", Body: bobState}},
				}, "notes.n1.body"))

				e := getKeyed("k1")
				Expect(plainBody(noteOf(e, "n1").GetBody())).To(Equal("[alicebob]"))
				Expect(plainBody(noteOf(e, "n2").GetBody())).To(Equal("other"),
					"a sibling element's document must stay untouched")
				Expect(noteOf(e, "n1").GetLabel()).To(Equal("first"),
					"a body-only mask must leave the element's other fields alone")
			})

			It("PUT unions each element's document by key and drops removed elements", func() {
				base1 := bodyDoc("seed", "one two")
				base2 := bodyDoc("seed", "gone")
				push(putMutation("c1", 1, "keyed_text", "k1", &testdatav1.KeyedTextEntity{
					Id: "k1",
					Notes: []*testdatav1.KeyedNote{
						{Id: "n1", Label: "a", Body: base1},
						{Id: "n2", Label: "b", Body: base2},
					},
				}))

				// A concurrent PATCH lands on n1 before the PUT below...
				patched := editBody(base1, "client", func(d *text.Doc) {
					Expect(d.Insert(7, " three")).To(Succeed())
				})
				push(patchMutation("c2", 1, "keyed_text", "k1", &testdatav1.KeyedTextEntity{
					Notes: []*testdatav1.KeyedNote{{Id: "n1", Body: patched}},
				}, "notes.n1.body"))

				// ...and the PUT, built from the stale base, rewrites n1's text,
				// drops n2 and keeps replace semantics for labels.
				putState := editBody(base1, "editor", func(d *text.Doc) {
					Expect(d.SetString("ONE two")).To(Succeed())
				})
				push(putMutation("c3", 1, "keyed_text", "k1", &testdatav1.KeyedTextEntity{
					Id: "k1",
					Notes: []*testdatav1.KeyedNote{
						{Id: "n1", Label: "a2", Body: putState},
					},
				}))

				e := getKeyed("k1")
				Expect(e.GetNotes()).To(HaveLen(1), "the PUT dropped n2, document and all")
				Expect(noteOf(e, "n1").GetLabel()).To(Equal("a2"))
				Expect(plainBody(noteOf(e, "n1").GetBody())).To(Equal("ONE two three"),
					"the element's document unions with the stored one, matched by key")
			})

			It("replaces the element whole — document included — when the mask names the element", func() {
				base := bodyDoc("seed", "original")
				push(putMutation("c1", 1, "keyed_text", "k1", &testdatav1.KeyedTextEntity{
					Id:    "k1",
					Notes: []*testdatav1.KeyedNote{{Id: "n1", Label: "a", Body: base}},
				}))
				replacement := bodyDoc("editor", "replacement")
				push(patchMutation("c2", 1, "keyed_text", "k1", &testdatav1.KeyedTextEntity{
					Notes: []*testdatav1.KeyedNote{{Id: "n1", Label: "b", Body: replacement}},
				}, "notes.n1"))

				// An ancestor mask replaces its subtree — same contract as a mask
				// naming a singular message above a fixed text field.
				Expect(getKeyed("k1").GetNotes()[0].GetBody()).To(Equal(replacement))
			})

			It("applies a fragment PATCH addressed to an element's document", func() {
				base := bodyDoc("seed", "the quick fox")
				push(putMutation("c1", 1, "keyed_text", "k1", &testdatav1.KeyedTextEntity{
					Id:    "k1",
					Notes: []*testdatav1.KeyedNote{{Id: "n1", Body: base}},
				}))
				full := editBody(base, "alice", func(d *text.Doc) {
					Expect(d.SetString("the quick brown fox")).To(Succeed())
				})
				push(patchMutation("c2", 1, "keyed_text", "k1", &testdatav1.KeyedTextEntity{
					Notes: []*testdatav1.KeyedNote{{Id: "n1", Body: fragmentOf(full, base)}},
				}, "notes.n1.body"))

				Expect(plainBody(noteOf(getKeyed("k1"), "n1").GetBody())).
					To(Equal("the quick brown fox"))
			})

			It("rejects a fragment whose mask names only the whole element", func() {
				base := bodyDoc("seed", "hello")
				push(putMutation("c1", 1, "keyed_text", "k1", &testdatav1.KeyedTextEntity{
					Id:    "k1",
					Notes: []*testdatav1.KeyedNote{{Id: "n1", Body: base}},
				}))
				frag := fragmentOf(editBody(base, "alice", func(d *text.Doc) {
					Expect(d.Insert(5, " world")).To(Succeed())
				}), base)

				// "notes.n1" replaces the element whole, so the fragment would be
				// persisted as if it were a document.
				results := push(patchMutation("c2", 1, "keyed_text", "k1", &testdatav1.KeyedTextEntity{
					Notes: []*testdatav1.KeyedNote{{Id: "n1", Body: frag}},
				}, "notes.n1"))
				Expect(results[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
				Expect(results[0].GetErrorCode()).To(Equal("text_full_state_required"))
			})

			It("merges concurrent PATCHes to a map value's document", func() {
				base := bodyDoc("seed", "[]")
				push(putMutation("c1", 1, "keyed_text", "k1", &testdatav1.KeyedTextEntity{
					Id:       "k1",
					Sections: map[string]*testdatav1.TextHolder{"intro": {Body: base}},
				}))
				aliceState := editBody(base, "alice", func(d *text.Doc) {
					Expect(d.Insert(1, "alice")).To(Succeed())
				})
				bobState := editBody(base, "bob", func(d *text.Doc) {
					Expect(d.Insert(1, "bob")).To(Succeed())
				})
				push(patchMutation("c2", 1, "keyed_text", "k1", &testdatav1.KeyedTextEntity{
					Sections: map[string]*testdatav1.TextHolder{"intro": {Body: aliceState}},
				}, "sections.intro.body"))
				push(patchMutation("c3", 1, "keyed_text", "k1", &testdatav1.KeyedTextEntity{
					Sections: map[string]*testdatav1.TextHolder{"intro": {Body: bobState}},
				}, "sections.intro.body"))

				Expect(plainBody(getKeyed("k1").GetSections()["intro"].GetBody())).
					To(Equal("[alicebob]"))
			})

			It("reaches a document below a singular message inside an element", func() {
				base := bodyDoc("seed", "[]")
				push(putMutation("c1", 1, "keyed_text", "k1", &testdatav1.KeyedTextEntity{
					Id: "k1",
					Notes: []*testdatav1.KeyedNote{
						{Id: "n1", Deep: &testdatav1.TextHolder{Body: base}},
					},
				}))
				aliceState := editBody(base, "alice", func(d *text.Doc) {
					Expect(d.Insert(1, "alice")).To(Succeed())
				})
				bobState := editBody(base, "bob", func(d *text.Doc) {
					Expect(d.Insert(1, "bob")).To(Succeed())
				})
				push(patchMutation("c2", 1, "keyed_text", "k1", &testdatav1.KeyedTextEntity{
					Notes: []*testdatav1.KeyedNote{{Id: "n1", Deep: &testdatav1.TextHolder{Body: aliceState}}},
				}, "notes.n1.deep.body"))
				push(patchMutation("c3", 1, "keyed_text", "k1", &testdatav1.KeyedTextEntity{
					Notes: []*testdatav1.KeyedNote{{Id: "n1", Deep: &testdatav1.TextHolder{Body: bobState}}},
				}, "notes.n1.deep.body"))

				Expect(plainBody(noteOf(getKeyed("k1"), "n1").GetDeep().GetBody())).
					To(Equal("[alicebob]"))
			})

			It("rejects junk text bytes inside an element", func() {
				push(putMutation("c1", 1, "keyed_text", "k1", &testdatav1.KeyedTextEntity{
					Id: "k1", Notes: []*testdatav1.KeyedNote{{Id: "n1"}},
				}))
				results := push(patchMutation("c2", 1, "keyed_text", "k1", &testdatav1.KeyedTextEntity{
					Notes: []*testdatav1.KeyedNote{{Id: "n1", Body: []byte{0xff}}},
				}, "notes.n1.body"))
				Expect(results[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
				Expect(results[0].GetErrorCode()).To(Equal("invalid_argument"))
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

		It("writes a string into a keyed element's document by instance path", func() {
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.KeyedTextEntity{
					Id:    "k1",
					Notes: []*testdatav1.KeyedNote{{Id: "n1", Label: "a"}},
				},
					server.WithTextString("notes.n1.body", "hello"),
					server.WithTextPeer("writer:1"))
			})).To(Succeed())

			var e testdatav1.KeyedTextEntity
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				found, err := tx.Get(&e, "k1")
				Expect(found).To(BeTrue())
				return err
			})).To(Succeed())
			Expect(plainBody(e.GetNotes()[0].GetBody())).To(Equal("hello"))
		})

		It("three-way merges a string Patch on an element with a concurrent edit", func() {
			base := bodyDoc("seed", "one two three")
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.KeyedTextEntity{
					Id:    "k1",
					Notes: []*testdatav1.KeyedNote{{Id: "n1", Body: base}},
				})
			})).To(Succeed())
			clientState := editBody(base, "client", func(d *text.Doc) {
				Expect(d.Insert(13, " four")).To(Succeed())
			})
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Patch(&testdatav1.KeyedTextEntity{
					Id:    "k1",
					Notes: []*testdatav1.KeyedNote{{Id: "n1", Body: clientState}},
				}, []string{"notes.n1.body"}, nil)
			})).To(Succeed())
			// The string write's message must carry the addressed element; the
			// mask is added automatically.
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Patch(&testdatav1.KeyedTextEntity{
					Id:    "k1",
					Notes: []*testdatav1.KeyedNote{{Id: "n1"}},
				}, nil, nil,
					server.WithTextString("notes.n1.body", "one TWO three four"),
					server.WithTextPeer("writer:1"))
			})).To(Succeed())

			var e testdatav1.KeyedTextEntity
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				_, err := tx.Get(&e, "k1")
				return err
			})).To(Succeed())
			Expect(plainBody(e.GetNotes()[0].GetBody())).To(Equal("one TWO three four"))
		})

		It("rejects a string addressing an element the value does not carry", func() {
			err := srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.KeyedTextEntity{Id: "k1"},
					server.WithTextString("notes.n1.body", "hello"),
					server.WithTextPeer("writer:1"))
			})
			Expect(err).To(MatchError(ContainSubstring("addresses a list element the value does not carry")))
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
