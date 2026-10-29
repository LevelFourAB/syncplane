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
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/text"
)

var _ = Describe("Collaborative-text compaction", func() {
	var (
		ctx    context.Context
		srv    *server.Server
		client syncplanev1connect.SyncServiceClient
	)

	// A retention of a millisecond makes every write "quiet" after a short
	// sleep, which is what puts rows in reach of one manual collection pass.
	newServerWith := func(opts ...server.Option) {
		// Every push authenticates as one caller, so a row carries an actor a
		// maintenance pass could overwrite if it stamped itself.
		authn := server.AuthenticatorFunc(func(context.Context, http.Header) (server.Principal, error) {
			return server.Principal{Subject: "u_7"}, nil
		})
		srv = newTestServer(append([]server.Option{server.WithAuthenticator(authn)}, opts...)...)
		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	}

	BeforeEach(func() {
		ctx = context.Background()
		newServerWith(
			server.WithGC(time.Millisecond, 0),
			server.WithTextDeltaRetention(time.Millisecond),
			server.WithTextCompaction(1),
		)
	})

	pushRaw := func(mutations ...*syncplanev1.Mutation) []*syncplanev1.MutationResult {
		GinkgoHelper()
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, mutations)))
		Expect(err).NotTo(HaveOccurred())
		return pushResults(resp.Msg)
	}

	push := func(mutations ...*syncplanev1.Mutation) []*syncplanev1.MutationResult {
		GinkgoHelper()
		results := pushRaw(mutations...)
		for _, r := range results {
			Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED),
				"setup push rejected: %s", r.GetErrorMessage())
		}
		return results
	}

	pull := func(cursor []byte) *syncplanev1.PullResponse {
		GinkgoHelper()
		resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId: ws, Cursor: cursor,
		}))
		Expect(err).NotTo(HaveOccurred())
		return resp.Msg
	}

	// storedBody is the row's text field as a fresh reader sees it.
	storedBody := func(key string) []byte {
		GinkgoHelper()
		for _, rc := range pull(nil).GetPatch().GetChanges() {
			if rc.GetEntity() == "widget" && rc.GetKey() == key {
				var w testdatav1.Widget
				Expect(proto.Unmarshal(rc.GetValue(), &w)).To(Succeed())
				return w.GetBody()
			}
		}
		Fail("no widget " + key)
		return nil
	}

	plainText := func(b []byte) string {
		GinkgoHelper()
		s, err := text.PlainText(b)
		Expect(err).NotTo(HaveOccurred())
		return s
	}

	// tombstones counts the items a document holds that are not visible.
	tombstones := func(b []byte) int {
		GinkgoHelper()
		d, err := text.Decode(b, "")
		Expect(err).NotTo(HaveOccurred())
		// Compact on a throwaway copy reports how much is still reclaimable;
		// what a compacted document must report is zero.
		items, _ := d.Compact()
		return items
	}

	// GC prunes the delta log and advances the horizon; the compaction sweep
	// then reclaims below it. Two passes because they are two passes — see
	// WithTextCompaction.
	collect := func() {
		GinkgoHelper()
		time.Sleep(5 * time.Millisecond)
		Expect(srv.CollectGarbage(ctx, ws)).To(Succeed())
		_, err := srv.CompactText(ctx, ws)
		Expect(err).NotTo(HaveOccurred())
	}

	// seedTypedKey creates a row holding "hello world" and then deletes
	// " world", so the row carries six reclaimable tombstones. mutationID is the
	// first of the two ids the two writes consume. Returns the pre-compaction
	// document, which is what a replica that stops reading here still holds.
	seedTypedKey := func(key string, mutationID uint64) []byte {
		GinkgoHelper()
		full := bodyDoc("seed", "hello world")
		push(putMutation("c1", mutationID, "widget", key, &testdatav1.Widget{Id: key, Body: full}))
		trimmed := editBody(full, "seed", func(d *text.Doc) {
			Expect(d.Delete(5, 6)).To(Succeed())
		})
		push(patchMutation("c1", mutationID+1, "widget", key, &testdatav1.Widget{Body: trimmed}, "body"))
		return trimmed
	}

	seedTyped := func() []byte {
		GinkgoHelper()
		return seedTypedKey("w1", 1)
	}

	It("purges tombstones from a quiet row without changing its text", func() {
		before := seedTyped()
		Expect(tombstones(before)).To(Equal(6))

		collect()

		after := storedBody("w1")
		Expect(plainText(after)).To(Equal("hello"))
		Expect(tombstones(after)).To(BeZero(), "nothing left to reclaim")
		Expect(len(after)).To(BeNumerically("<", len(before)))
	})

	It("leaves a row alone when compaction is not enabled", func() {
		newServerWith(server.WithGC(time.Millisecond, 0), server.WithTextDeltaRetention(time.Millisecond))
		before := seedTyped()
		collect()
		Expect(storedBody("w1")).To(Equal(before))
	})

	It("leaves a row alone below the purge threshold", func() {
		newServerWith(
			server.WithGC(time.Millisecond, 0),
			server.WithTextDeltaRetention(time.Millisecond),
			server.WithTextCompaction(7),
		)
		before := seedTyped()
		collect()
		Expect(storedBody("w1")).To(Equal(before), "six reclaimable items is under the threshold of seven")
	})

	It("leaves a row written inside the retention window alone", func() {
		// One row on each side of the same cutoff, so the pass has to tell them
		// apart rather than find nothing quiet at all. The retention is wide
		// compared to a push: this spec is only meaningful while the row it
		// expects to survive is genuinely newer than the cutoff, and the
		// millisecond the rest of the file uses is shorter than a single write
		// takes under load, which would put both rows on the quiet side.
		newServerWith(
			server.WithGC(time.Minute, 0),
			server.WithTextDeltaRetention(100*time.Millisecond),
			server.WithTextCompaction(1),
		)
		seedTypedKey("w1", 1)
		time.Sleep(150 * time.Millisecond)
		fresh := seedTypedKey("w2", 3)

		Expect(srv.CollectGarbage(ctx, ws)).To(Succeed())
		_, err := srv.CompactText(ctx, ws)
		Expect(err).NotTo(HaveOccurred())

		Expect(storedBody("w2")).To(Equal(fresh), "w2 is newer than the delta log's prune cutoff")
		Expect(tombstones(storedBody("w1"))).To(BeZero(),
			"w1 was quiet and compacted in the same pass, so w2 was spared by its version, not by an empty horizon")
	})

	It("keeps the row's attribution, so a rewrite is not reported as a system write", func() {
		seedTyped()
		collect()

		var got *server.Change
		_, _, err := srv.Changes(ctx, ws, server.Cursor{}, 0, func(ch server.Change) error {
			if ch.Key == "w1" {
				c := ch
				got = &c
			}
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(got).NotTo(BeNil())
		// Compaction purges tombstones from the document and changes nothing an
		// app can see. Stamping the maintenance pass here would erase who wrote
		// the row and tell every feed consumer the engine did it.
		Expect(got.Actor).To(Equal(storage.Actor{Subject: "u_7"}))
	})

	It("is idempotent across passes", func() {
		seedTyped()
		collect()
		once := storedBody("w1")
		collect()
		Expect(storedBody("w1")).To(Equal(once), "a compacted row is not rewritten again")
	})

	Describe("writes measured against the floor", func() {
		It("accepts an edit from a replica that still carries the purged tombstones", func() {
			stale := seedTyped()
			collect()

			// The replica edits inside the surviving text; the six tombstones it
			// still holds are skipped rather than merged back in.
			edited := editBody(stale, "other", func(d *text.Doc) {
				Expect(d.Insert(2, "!")).To(Succeed())
			})
			push(patchMutation("c1", 3, "widget", "w1", &testdatav1.Widget{Body: edited}, "body"))

			after := storedBody("w1")
			Expect(plainText(after)).To(Equal("he!llo"))
			Expect(tombstones(after)).To(BeZero(), "the purged tombstones did not come back")
		})

		It("rejects an edit anchored to purged text, consuming no sequence", func() {
			stale := seedTyped()
			collect()
			seqBefore := pull(nil).GetPatch().GetChanges()[0].GetVersion()
			bodyBefore := storedBody("w1")

			// Appending attaches to the first character of the deleted run, which
			// the row no longer holds.
			edited := editBody(stale, "other", func(d *text.Doc) {
				Expect(d.Insert(d.Len(), "!")).To(Succeed())
			})
			results := pushRaw(patchMutation("c1", 3, "widget", "w1", &testdatav1.Widget{Body: edited}, "body"))
			Expect(results).To(HaveLen(1))
			Expect(results[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(results[0].GetErrorCode()).To(Equal(server.CodeTextStateStale))

			Expect(storedBody("w1")).To(Equal(bodyBefore), "the row is untouched")
			// The next write gets the sequence the rejection did not take.
			push(patchMutation("c1", 4, "widget", "w1",
				&testdatav1.Widget{Label: "x"}, "label"))
			Expect(pull(nil).GetPatch().GetChanges()[0].GetVersion()).To(Equal(seqBefore + 1))
		})

		It("rejects state that still holds a purged character as live", func() {
			full := bodyDoc("seed", "hello world")
			push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Body: full}))
			// This replica forks before the delete, so it holds the tail live.
			trimmed := editBody(full, "seed", func(d *text.Doc) {
				Expect(d.Delete(5, 6)).To(Succeed())
			})
			push(patchMutation("c1", 2, "widget", "w1", &testdatav1.Widget{Body: trimmed}, "body"))
			collect()

			results := pushRaw(patchMutation("c1", 3, "widget", "w1", &testdatav1.Widget{Body: full}, "body"))
			Expect(results[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(results[0].GetErrorCode()).To(Equal(server.CodeTextStateStale))
			Expect(plainText(storedBody("w1"))).To(Equal("hello"))
		})

		It("applies the floor to a PUT as well as a PATCH", func() {
			full := bodyDoc("seed", "hello world")
			push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Body: full}))
			trimmed := editBody(full, "seed", func(d *text.Doc) {
				Expect(d.Delete(5, 6)).To(Succeed())
			})
			push(patchMutation("c1", 2, "widget", "w1", &testdatav1.Widget{Body: trimmed}, "body"))
			collect()

			results := pushRaw(putMutation("c1", 3, "widget", "w1",
				&testdatav1.Widget{Id: "w1", Body: full}))
			Expect(results[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(results[0].GetErrorCode()).To(Equal(server.CodeTextStateStale))
		})

		It("accepts everything again from a replica that re-read the row", func() {
			seedTyped()
			collect()
			fresh := editBody(storedBody("w1"), "other", func(d *text.Doc) {
				Expect(d.Insert(d.Len(), " again")).To(Succeed())
			})
			push(patchMutation("c1", 3, "widget", "w1", &testdatav1.Widget{Body: fresh}, "body"))
			Expect(plainText(storedBody("w1"))).To(Equal("hello again"))
		})

		It("accepts new text typed into the field after a PATCH cleared it", func() {
			seedTyped()
			collect()
			push(patchMutation("c1", 3, "widget", "w1", &testdatav1.Widget{}, "body"))
			cleared := storedBody("w1")
			Expect(plainText(cleared)).To(BeEmpty())

			// The writer that typed the purged text types again, building on the
			// cleared field as a reader sees it.
			typed := editBody(cleared, "seed", func(d *text.Doc) {
				Expect(d.Insert(0, "new")).To(Succeed())
			})
			push(patchMutation("c1", 4, "widget", "w1", &testdatav1.Widget{Body: typed}, "body"))
			Expect(plainText(storedBody("w1"))).To(Equal("new"))
		})

		It("still refuses purged text after a PATCH cleared the field", func() {
			full := bodyDoc("seed", "hello world")
			push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Body: full}))
			trimmed := editBody(full, "seed", func(d *text.Doc) {
				Expect(d.Delete(5, 6)).To(Succeed())
			})
			push(patchMutation("c1", 2, "widget", "w1", &testdatav1.Widget{Body: trimmed}, "body"))
			collect()
			push(patchMutation("c1", 3, "widget", "w1", &testdatav1.Widget{}, "body"))

			results := pushRaw(patchMutation("c1", 4, "widget", "w1", &testdatav1.Widget{Body: full}, "body"))
			Expect(results[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(results[0].GetErrorCode()).To(Equal(server.CodeTextStateStale))
			Expect(plainText(storedBody("w1"))).To(BeEmpty())
		})

		It("drops the floor with the row, so a re-created row starts clean", func() {
			seedTyped()
			collect()
			push(deleteMutation("c1", 3, "widget", "w1"))
			// A fresh document from a peer whose counters start over would be
			// measured against the old floor if the delete had not cleared it.
			recreated := bodyDoc("seed", "brand new")
			push(putMutation("c1", 4, "widget", "w1",
				&testdatav1.Widget{Id: "w1", Body: recreated}))
			Expect(plainText(storedBody("w1"))).To(Equal("brand new"))
		})
	})

	Describe("documents inside keyed list elements", func() {
		getKeyed := func(key string) *testdatav1.KeyedTextEntity {
			GinkgoHelper()
			for _, rc := range pull(nil).GetPatch().GetChanges() {
				if rc.GetEntity() == "keyed_text" && rc.GetKey() == key {
					var e testdatav1.KeyedTextEntity
					Expect(proto.Unmarshal(rc.GetValue(), &e)).To(Succeed())
					return &e
				}
			}
			Fail("no keyed_text " + key)
			return nil
		}
		noteBody := func(key, id string) []byte {
			GinkgoHelper()
			for _, n := range getKeyed(key).GetNotes() {
				if n.GetId() == id {
					return n.GetBody()
				}
			}
			return nil
		}
		patchNote := func(mutationID uint64, id string, body []byte) *syncplanev1.Mutation {
			return patchMutation("c1", mutationID, "keyed_text", "k1", &testdatav1.KeyedTextEntity{
				Notes: []*testdatav1.KeyedNote{{Id: id, Body: body}},
			}, "notes."+id+".body")
		}

		// seedElements creates a row whose n1 document is typed and trimmed —
		// six reclaimable tombstones — while n2 stays quiet. Returns n1's
		// pre-compaction document.
		seedElements := func() []byte {
			GinkgoHelper()
			full := bodyDoc("seed", "hello world")
			push(putMutation("c1", 1, "keyed_text", "k1", &testdatav1.KeyedTextEntity{
				Id: "k1",
				Notes: []*testdatav1.KeyedNote{
					{Id: "n1", Body: full},
					{Id: "n2", Body: bodyDoc("seed", "quiet")},
				},
			}))
			trimmed := editBody(full, "seed", func(d *text.Doc) {
				Expect(d.Delete(5, 6)).To(Succeed())
			})
			push(patchNote(2, "n1", trimmed))
			return trimmed
		}

		It("compacts and floors each element's document on its own", func() {
			stale := seedElements()
			collect()

			after := noteBody("k1", "n1")
			Expect(plainText(after)).To(Equal("hello"))
			Expect(tombstones(after)).To(BeZero())

			// An edit anchored to n1's purged text is refused by n1's floor...
			edited := editBody(stale, "other", func(d *text.Doc) {
				Expect(d.Insert(d.Len(), "!")).To(Succeed())
			})
			results := pushRaw(patchNote(3, "n1", edited))
			Expect(results[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(results[0].GetErrorCode()).To(Equal(server.CodeTextStateStale))

			// ...while n2, never compacted, keeps accepting whatever merges.
			grown := editBody(noteBody("k1", "n2"), "other", func(d *text.Doc) {
				Expect(d.Insert(d.Len(), " still")).To(Succeed())
			})
			push(patchNote(4, "n2", grown))
			Expect(plainText(noteBody("k1", "n2"))).To(Equal("quiet still"))
		})

		It("drops the floor with the element, so a re-created element starts clean", func() {
			seedElements()
			collect()

			// Removing n1 (clear-on-absent under its mask path) ends the document.
			push(patchMutation("c1", 3, "keyed_text", "k1", &testdatav1.KeyedTextEntity{
				Notes: []*testdatav1.KeyedNote{},
			}, "notes.n1"))
			Expect(noteBody("k1", "n1")).To(BeNil())

			// A fresh document whose peer counters start over would be measured
			// against the old floor if the removal had not cleared it — this
			// merge runs against an absent base, where the floor is the only gate.
			recreated := bodyDoc("seed", "brand new")
			push(patchNote(4, "n1", recreated))
			Expect(plainText(noteBody("k1", "n1"))).To(Equal("brand new"))
		})

		It("ships full state to a reader whose span covers an element's purge", func() {
			seedElements()
			cursor := pull(nil).GetNextCursor()
			typed := editBody(noteBody("k1", "n1"), "seed", func(d *text.Doc) {
				Expect(d.Insert(d.Len(), "!")).To(Succeed())
			})
			push(patchNote(3, "n1", typed))
			collect()

			var change *syncplanev1.RowChange
			for _, rc := range pull(cursor).GetPatch().GetChanges() {
				if rc.GetKey() == "k1" {
					change = rc
				}
			}
			Expect(change).NotTo(BeNil())
			Expect(change.GetTextDeltas()).To(BeEmpty(),
				"a span containing a purge cannot be folded and must ship the documents")
			var e testdatav1.KeyedTextEntity
			Expect(proto.Unmarshal(change.GetValue(), &e)).To(Succeed())
			for _, n := range e.GetNotes() {
				Expect(n.GetBody()).NotTo(BeEmpty(), "every element ships its document")
			}
		})
	})

	It("ships full state to a reader whose span covers the compaction", func() {
		seedTyped()
		// A reader caught up through the create, before the trim and the purge.
		cursor := pull(nil).GetNextCursor()
		trimmed := editBody(storedBody("w1"), "seed", func(d *text.Doc) {
			Expect(d.Insert(d.Len(), "!")).To(Succeed())
		})
		push(patchMutation("c1", 3, "widget", "w1", &testdatav1.Widget{Body: trimmed}, "body"))
		collect()

		resp := pull(cursor)
		var change *syncplanev1.RowChange
		for _, rc := range resp.GetPatch().GetChanges() {
			if rc.GetKey() == "w1" {
				change = rc
			}
		}
		Expect(change).NotTo(BeNil())
		Expect(change.GetTextDeltas()).To(BeEmpty(),
			"a span containing a purge cannot be folded and must ship the document")
		var w testdatav1.Widget
		Expect(proto.Unmarshal(change.GetValue(), &w)).To(Succeed())
		Expect(w.GetBody()).To(Equal(storedBody("w1")))
	})

	It("compacts text written through a trusted EntityTx and refuses stale ones", func() {
		full := bodyDoc("seed", "hello world")
		push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Body: full}))
		trimmed := editBody(full, "seed", func(d *text.Doc) {
			Expect(d.Delete(5, 6)).To(Succeed())
		})
		push(patchMutation("c1", 2, "widget", "w1", &testdatav1.Widget{Body: trimmed}, "body"))
		collect()

		err := srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			return tx.Patch(&testdatav1.Widget{Id: "w1", Body: full}, []string{"body"}, nil)
		})
		Expect(err).To(HaveOccurred())
		rej, ok := server.AsRejection(err)
		Expect(ok).To(BeTrue(), "expected a rejection, got %v", err)
		Expect(rej.Code).To(Equal(server.CodeTextStateStale))
		Expect(plainText(storedBody("w1"))).To(Equal("hello"))
	})

	It("accepts new text in a field a trusted EntityTx.Patch cleared after the compaction", func() {
		seedTyped()
		collect()
		Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			return tx.Patch(&testdatav1.Widget{Id: "w1"}, []string{"body"}, nil)
		})).To(Succeed())

		typed := editBody(storedBody("w1"), "seed", func(d *text.Doc) {
			Expect(d.Insert(0, "new")).To(Succeed())
		})
		push(patchMutation("c1", 3, "widget", "w1", &testdatav1.Widget{Body: typed}, "body"))
		Expect(plainText(storedBody("w1"))).To(Equal("new"))
	})
})
