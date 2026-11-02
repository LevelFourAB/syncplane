package server_test

import (
	"context"
	"errors"
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

var _ = Describe("Replicate", func() {
	var (
		ctx    context.Context
		srv    *server.Server
		store  storage.Adapter
		client syncplanev1connect.SyncServiceClient
	)

	BeforeEach(func() {
		ctx = context.Background()
		srv, store = newTestServerStore()
		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	})

	replicate := func(fn func(tx *server.ReplicaTx) error) {
		GinkgoHelper()
		Expect(srv.Replicate(ctx, ws, fn)).To(Succeed())
	}
	put := func(entity, key string, msg proto.Message) {
		GinkgoHelper()
		replicate(func(tx *server.ReplicaTx) error { return tx.Put(entity, key, mustMarshal(msg)) })
	}
	read := func(fn func(tx storage.ReadTx)) {
		GinkgoHelper()
		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			fn(tx)
			return nil
		})).To(Succeed())
	}
	storedRow := func(entity, key string) storage.Row {
		GinkgoHelper()
		var row storage.Row
		read(func(tx storage.ReadTx) {
			var err error
			row, err = tx.GetRow(entity, key)
			Expect(err).NotTo(HaveOccurred())
		})
		return row
	}
	currentSeq := func() uint64 {
		GinkgoHelper()
		seq, err := srv.CurrentSeq(ctx, ws)
		Expect(err).NotTo(HaveOccurred())
		return seq
	}
	pull := func(cursor []byte) *syncplanev1.PullResponse {
		GinkgoHelper()
		resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws, Cursor: cursor}))
		Expect(err).NotTo(HaveOccurred())
		return resp.Msg
	}
	changeOf := func(resp *syncplanev1.PullResponse, key string) *syncplanev1.RowChange {
		GinkgoHelper()
		for _, rc := range resp.GetPatch().GetChanges() {
			if rc.GetKey() == key {
				return rc
			}
		}
		Fail("no change for " + key)
		return nil
	}

	It("stores the bytes it is given, a text field included", func() {
		base := bodyDoc("a", "hello world")
		Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
			return tx.Put(&testdatav1.Widget{Id: "w1", Body: editBody(base, "b", func(d *text.Doc) {
				Expect(d.Insert(d.Len(), "!")).To(Succeed())
			})})
		})).To(Succeed())

		// A plain PUT would union base with the stored document; a replica write
		// replaces it.
		want := mustMarshal(&testdatav1.Widget{Id: "w1", Label: "replicated", Body: base})
		replicate(func(tx *server.ReplicaTx) error { return tx.Put("widget", "w1", want) })
		row := storedRow("widget", "w1")
		Expect(row.Value).To(Equal(want))
		Expect(row.Actor).To(Equal(storage.Actor{System: "upstream"}))
	})

	It("takes one sequence per call and none for a call that writes nothing", func() {
		replicate(func(tx *server.ReplicaTx) error {
			Expect(tx.Seq()).To(BeZero())
			Expect(tx.Put("widget", "w1", mustMarshal(&testdatav1.Widget{Id: "w1"}))).To(Succeed())
			Expect(tx.Put("widget", "w2", mustMarshal(&testdatav1.Widget{Id: "w2"}))).To(Succeed())
			Expect(tx.Seq()).To(BeEquivalentTo(1))
			return nil
		})
		Expect(storedRow("widget", "w1").Version).To(BeEquivalentTo(1))
		Expect(storedRow("widget", "w2").Version).To(BeEquivalentTo(1))

		replicate(func(tx *server.ReplicaTx) error {
			// The bytes the row already holds, and a row that is not there.
			Expect(tx.Put("widget", "w1", mustMarshal(&testdatav1.Widget{Id: "w1"}))).To(Succeed())
			return tx.Delete("widget", "absent")
		})
		Expect(currentSeq()).To(BeEquivalentTo(1))
	})

	It("names the writer a TrustedAs context gives", func() {
		Expect(srv.Replicate(server.TrustedAs(ctx, "link:origin"), ws, func(tx *server.ReplicaTx) error {
			return tx.Put("widget", "w1", mustMarshal(&testdatav1.Widget{Id: "w1"}))
		})).To(Succeed())
		Expect(storedRow("widget", "w1").Actor).To(Equal(storage.Actor{System: "link:origin"}))
	})

	It("reads the row as the transaction holds it", func() {
		replicate(func(tx *server.ReplicaTx) error {
			_, found, err := tx.Get("widget", "w1")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeFalse())
			Expect(tx.Put("widget", "w1", mustMarshal(&testdatav1.Widget{Id: "w1"}))).To(Succeed())
			Expect(tx.Delete("widget", "w1")).To(Succeed())
			row, found, err := tx.Get("widget", "w1")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(row.Deleted).To(BeTrue())
			return nil
		})
	})

	It("reads state kept beside the rows through ReadState", func() {
		replicate(func(tx *server.ReplicaTx) error {
			return tx.Tx().PutConsumerShadow(server.ReservedConsumerPrefix+"link", "#state", "s", []byte("1"))
		})
		var got []byte
		Expect(srv.ReadState(ctx, ws, func(tx storage.ReadTx) error {
			var err error
			got, err = tx.ConsumerShadow(server.ReservedConsumerPrefix+"link", "#state", "s")
			return err
		})).To(Succeed())
		Expect(got).To(Equal([]byte("1")))
		Expect(srv.ReadState(ctx, "", func(storage.ReadTx) error { return nil })).NotTo(Succeed())
	})

	It("keeps a reserved workspace out of the workspace lists and off the wire", func() {
		reserved := server.ReservedWorkspacePrefix + "link"
		Expect(srv.Replicate(ctx, reserved, func(tx *server.ReplicaTx) error {
			return tx.Tx().PutConsumerShadow(server.ReservedConsumerPrefix+"link", "#state", "s", []byte("1"))
		})).To(Succeed())
		put("widget", "w1", &testdatav1.Widget{Id: "w1"})
		Expect(srv.WorkspaceIDs(ctx)).To(Equal([]string{ws}))
		var pending []string
		for id, err := range srv.PendingWorkspaces(ctx, "some-consumer") {
			Expect(err).NotTo(HaveOccurred())
			pending = append(pending, id)
		}
		Expect(pending).To(Equal([]string{ws}))
		Expect(srv.ConsumerPendingWorkspaces(ctx, "some-consumer", "", 10)).To(Equal([]string{ws}))
		_, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: reserved}))
		Expect(err).To(HaveOccurred())
	})

	// A caller pages until a page comes back short, and a reserved id sorts
	// before every valid one, so a page it was dropped from must be topped up.
	It("fills a page of pending workspaces past a reserved one", func() {
		reserved := server.ReservedWorkspacePrefix + "link"
		Expect(srv.Replicate(ctx, reserved, func(tx *server.ReplicaTx) error {
			return tx.Tx().PutConsumerShadow(server.ReservedConsumerPrefix+"link", "#state", "s", []byte("1"))
		})).To(Succeed())
		put("widget", "w1", &testdatav1.Widget{Id: "w1"})
		Expect(srv.ConsumerPendingWorkspaces(ctx, "some-consumer", "", 1)).To(Equal([]string{ws}))
	})

	It("commits shadows written through Tx with the rows, and rolls both back on error", func() {
		boom := errors.New("boom")
		err := srv.Replicate(ctx, ws, func(tx *server.ReplicaTx) error {
			Expect(tx.Put("widget", "w1", mustMarshal(&testdatav1.Widget{Id: "w1"}))).To(Succeed())
			Expect(tx.Tx().PutConsumerShadow(server.ReservedConsumerPrefix+"link", "#state", "s", []byte("1"))).To(Succeed())
			return boom
		})
		Expect(err).To(MatchError(boom))
		Expect(currentSeq()).To(BeZero())

		replicate(func(tx *server.ReplicaTx) error {
			Expect(tx.Put("widget", "w1", mustMarshal(&testdatav1.Widget{Id: "w1"}))).To(Succeed())
			return tx.Tx().PutConsumerShadow(server.ReservedConsumerPrefix+"link", "#state", "s", []byte("1"))
		})
		read(func(tx storage.ReadTx) {
			Expect(tx.ConsumerShadow(server.ReservedConsumerPrefix+"link", "#state", "s")).To(Equal([]byte("1")))
		})
	})

	It("refuses a value whose key fields do not derive the key, and writes nothing", func() {
		err := srv.Replicate(ctx, ws, func(tx *server.ReplicaTx) error {
			Expect(tx.Put("widget", "w1", mustMarshal(&testdatav1.Widget{Id: "w1"}))).To(Succeed())
			return tx.Put("widget", "w2", mustMarshal(&testdatav1.Widget{Id: "other"}))
		})
		Expect(err).To(MatchError(ContainSubstring("does not match key fields")))
		Expect(currentSeq()).To(BeZero())
		Expect(srv.Replicate(ctx, ws, func(tx *server.ReplicaTx) error {
			return tx.Put("nope", "k", nil)
		})).To(MatchError(ContainSubstring("not registered")))
	})

	It("notifies change subscribers", func() {
		ch, cancel := srv.SubscribeChanges(ws)
		defer cancel()
		put("widget", "w1", &testdatav1.Widget{Id: "w1"})
		Eventually(ch).WithTimeout(time.Second).Should(Receive())
	})

	It("keeps index entries exactly as a normal write does", func() {
		// The same rows through a normal write on one server and through
		// Replicate on another: every derived structure must match.
		ref, refStore := newTestServerStore()
		rows := []*testdatav1.Issue{
			{Id: "i1", State: "open", Rank: 3, Labels: []string{"a", "b"}},
			{Id: "i2", State: "done", Rank: 1},
		}
		for _, r := range rows {
			Expect(ref.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error { return tx.Put(r) })).To(Succeed())
			put("issue", r.GetId(), r)
		}
		edited := &testdatav1.Issue{Id: "i1", State: "done", Rank: 2}
		Expect(ref.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error { return tx.Put(edited) })).To(Succeed())
		put("issue", "i1", edited)
		Expect(ref.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
			return tx.Delete(&testdatav1.Issue{}, "i2")
		})).To(Succeed())
		replicate(func(tx *server.ReplicaTx) error { return tx.Delete("issue", "i2") })

		entries := func(s storage.Adapter, index string) []string {
			GinkgoHelper()
			var out []string
			Expect(s.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
				return tx.ScanIndex("issue", index, nil, nil, 0, storage.ScanAsc, func(e []byte, key string) error {
					out = append(out, string(e)+"|"+key)
					return nil
				})
			})).To(Succeed())
			return out
		}
		for _, index := range []string{"by_state_rank", "by_label_rank"} {
			want := entries(refStore, index)
			Expect(want).NotTo(BeEmpty())
			Expect(entries(store, index)).To(Equal(want), index)
		}
	})

	It("captures field history for an opted-in entity", func() {
		put("historied", "h1", &testdatav1.Historied{Id: "h1", Title: "one"})
		put("historied", "h1", &testdatav1.Historied{Id: "h1", Title: "two"})
		n := 0
		read(func(tx storage.ReadTx) {
			Expect(tx.ScanEntity("syncplane_field_history", func(storage.Row) error {
				n++
				return nil
			})).To(Succeed())
		})
		Expect(n).To(BeNumerically(">", 0))
	})

	Describe("collaborative text", func() {
		base := func() []byte { return bodyDoc("seed", "the quick brown fox jumps over the lazy dog") }

		It("ships a document that extends the stored one as a fragment", func() {
			put("widget", "w1", &testdatav1.Widget{Id: "w1", Body: base()})
			first := pull(nil)
			held := changeOf(first, "w1").GetValue()

			edited := editBody(base(), "alice", func(d *text.Doc) {
				Expect(d.SetString("the quick brown fox leaps over the lazy dog")).To(Succeed())
			})
			put("widget", "w1", &testdatav1.Widget{Id: "w1", Body: edited})

			rc := changeOf(pull(first.GetNextCursor()), "w1")
			Expect(rc.GetTextDeltas()).To(HaveLen(1))
			Expect(rc.GetTextDeltas()[0].GetFragment()).NotTo(BeEmpty())
			var prev testdatav1.Widget
			Expect(proto.Unmarshal(held, &prev)).To(Succeed())
			body, err := text.ApplyFragmentBytes(prev.GetBody(), rc.GetTextDeltas()[0].GetFragment())
			Expect(err).NotTo(HaveOccurred())
			Expect(plainBody(body)).To(Equal("the quick brown fox leaps over the lazy dog"))
		})

		It("ships a reverted document whole", func() {
			// A local edit the origin then refused: the replica writes the
			// document back as it was before the edit.
			put("widget", "w1", &testdatav1.Widget{Id: "w1", Body: base()})
			first := pull(nil)
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Patch(&testdatav1.Widget{Id: "w1", Body: editBody(base(), "local", func(d *text.Doc) {
					Expect(d.Insert(0, "LOCAL ")).To(Succeed())
				})}, []string{"body"}, nil)
			})).To(Succeed())
			edited := pull(first.GetNextCursor())
			Expect(plainBody(decodeWidget(storedRow("widget", "w1").Value).GetBody())).To(HavePrefix("LOCAL "))

			put("widget", "w1", &testdatav1.Widget{Id: "w1", Body: base()})

			for _, cursor := range [][]byte{first.GetNextCursor(), edited.GetNextCursor()} {
				rc := changeOf(pull(cursor), "w1")
				Expect(rc.GetTextDeltas()).To(BeEmpty(), "no fragment takes text away")
				Expect(rc.GetValue()).To(Equal(storedRow("widget", "w1").Value))
				Expect(plainBody(decodeWidget(rc.GetValue()).GetBody())).To(Equal("the quick brown fox jumps over the lazy dog"))
			}
		})

		It("serves a bootstrap the replicated rows", func() {
			put("widget", "w1", &testdatav1.Widget{Id: "w1", Body: base()})
			put("widget", "w2", &testdatav1.Widget{Id: "w2"})
			replicate(func(tx *server.ReplicaTx) error { return tx.Delete("widget", "w2") })

			stream, err := client.Bootstrap(ctx, connect.NewRequest(&syncplanev1.BootstrapRequest{WorkspaceId: ws}))
			Expect(err).NotTo(HaveOccurred())
			var rows []*syncplanev1.RowChange
			for stream.Receive() {
				if chunk := stream.Msg().GetChunk(); chunk != nil {
					rows = append(rows, chunk.GetRows()...)
				}
			}
			Expect(stream.Err()).NotTo(HaveOccurred())
			Expect(rows).To(HaveLen(1))
			Expect(rows[0].GetValue()).To(Equal(storedRow("widget", "w1").Value))
		})

		It("drops a compaction floor the new document does not cover, and keeps one it does", func() {
			put("widget", "w1", &testdatav1.Widget{Id: "w1", Body: base()})
			put("widget", "w2", &testdatav1.Widget{Id: "w2", Body: base()})
			Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
				for _, key := range []string{"w1", "w2"} {
					if err := tx.PutTextFloor("widget", key, "body", text.EncodeVV(map[string]uint64{"seed": 1})); err != nil {
						return err
					}
				}
				return nil
			})).To(Succeed())

			// w1 is replaced by a document from another history entirely; w2
			// grows.
			put("widget", "w1", &testdatav1.Widget{Id: "w1", Body: bodyDoc("other", "x")})
			put("widget", "w2", &testdatav1.Widget{Id: "w2", Body: editBody(base(), "bob", func(d *text.Doc) {
				Expect(d.Insert(0, "> ")).To(Succeed())
			})})
			read(func(tx storage.ReadTx) {
				Expect(tx.TextFloor("widget", "w1", "body")).To(BeNil())
				Expect(tx.TextFloor("widget", "w2", "body")).NotTo(BeNil())
			})

			// The replaced row accepts a write built on what it now holds.
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Patch(&testdatav1.Widget{Id: "w1", Body: editBody(bodyDoc("other", "x"), "carol", func(d *text.Doc) {
					Expect(d.Insert(1, "y")).To(Succeed())
				})}, []string{"body"}, nil)
			})).To(Succeed())
		})

		It("logs a gap and clears the floors of a deleted row", func() {
			put("widget", "w1", &testdatav1.Widget{Id: "w1", Body: base()})
			first := pull(nil)
			Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
				return tx.PutTextFloor("widget", "w1", "body", text.EncodeVV(map[string]uint64{"seed": 1}))
			})).To(Succeed())
			replicate(func(tx *server.ReplicaTx) error { return tx.Delete("widget", "w1") })
			read(func(tx storage.ReadTx) {
				Expect(tx.TextFloor("widget", "w1", "body")).To(BeNil())
				_, complete, err := tx.FieldDeltas("widget", "w1", "body", 1)
				Expect(err).NotTo(HaveOccurred())
				Expect(complete).To(BeFalse())
			})
			rc := changeOf(pull(first.GetNextCursor()), "w1")
			Expect(rc.GetTombstone()).To(BeTrue())
		})
	})
})
