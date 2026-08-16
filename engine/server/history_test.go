package server_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"time"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/text"
)

// historyReadPerms hides Historied rows whose title equals hidden, and answers
// with NO filter for every other entity — including the history entity itself,
// which is the shape every app is really in: it never declared that entity and
// has no reason to name it in a policy.
type historyReadPerms struct {
	server.AllowAll
	hidden string
	// asked records the entities a filter was requested for, so a spec can
	// assert the source entity's filter was resolved before the snapshot.
	asked []string
	// evaluated counts the rows the filter actually ran over, which is what a
	// spec asserting the source-row verdict is memoized watches.
	evaluated int
}

func (p *historyReadPerms) ReadFilter(_ context.Context, _ server.Principal, _, entity string) (server.RowFilter, error) {
	p.asked = append(p.asked, entity)
	if entity != "historied" {
		return nil, nil
	}
	return func(_ string, row proto.Message) bool {
		p.evaluated++
		h, ok := row.(*testdatav1.Historied)
		return !ok || h.GetTitle() != p.hidden
	}, nil
}

// Field history is captured on the server's write path, because that is the
// only place the before-state exists. These specs assert what a record says
// (which fields, which values, at which sequence) and what the entity holding
// them does NOT do — sync.
var _ = Describe("Field history", func() {
	var ctx context.Context

	BeforeEach(func() { ctx = context.Background() })

	// historyOf reads every history record for one source row, newest first —
	// the query a history panel runs, expressed against the in-process view so
	// a spec does not have to stand up an RPC client to read it.
	historyOf := func(srv *server.Server, ws, entity, key string) []*syncplanev1.FieldHistory {
		GinkgoHelper()
		var out []*syncplanev1.FieldHistory
		Expect(srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
			return tx.Query(&syncplanev1.FieldHistory{}, query.Spec{
				Where: query.And(
					query.Eq("source_entity", entity),
					query.Eq("source_key", key),
				),
				OrderBy: []*syncplanev1.OrderKey{query.Desc("seq")},
			}, func(msg proto.Message, _ uint64) error {
				out = append(out, msg.(*syncplanev1.FieldHistory))
				return nil
			})
		})).To(Succeed())
		return out
	}

	// newHistoryServer builds a test server with session folding OFF, which is
	// what every spec asserting per-WRITE capture needs: these push their writes
	// microseconds apart, and the default window would fold each burst into one
	// record. Folding has its own Describe below, and turning it off here is not
	// a workaround but the configuration those assertions are about.
	newHistoryServer := func(opts ...server.Option) *server.Server {
		GinkgoHelper()
		return newTestServer(append(
			[]server.Option{server.WithHistorySession(server.HistorySessionOff)}, opts...)...)
	}

	// plantHistory writes a record the engine's own capture path would never
	// produce — one naming an entity that has stopped opting into history, or
	// never did — through the trusted write path. Records outlive the option
	// that created them, so this is the state a schema change leaves behind.
	plantHistory := func(srv *server.Server, rec *syncplanev1.FieldHistory) {
		GinkgoHelper()
		Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
			return tx.Put(rec)
		})).To(Succeed())
	}

	// priorOf decodes a record's before-state as the source entity it describes
	// — the first two steps of a restore.
	priorOf := func(rec *syncplanev1.FieldHistory) *testdatav1.Historied {
		GinkgoHelper()
		var out testdatav1.Historied
		Expect(proto.Unmarshal(rec.GetPrior(), &out)).To(Succeed())
		return &out
	}

	newRPC := func(srv *server.Server) syncplanev1connect.SyncServiceClient {
		GinkgoHelper()
		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		return syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	}

	Describe("capture", func() {
		It("records a PATCH's masked fields, from the wire path", func() {
			srv := newHistoryServer()
			client := newRPC(srv)

			_, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
				putMutation("c1", 1, "historied", "h1",
					&testdatav1.Historied{Id: "h1", Title: "first", Note: "n1"}),
				patchMutation("c1", 2, "historied", "h1",
					&testdatav1.Historied{Id: "h1", Title: "second"}, "title"),
			})))
			Expect(err).NotTo(HaveOccurred())

			recs := historyOf(srv, ws, "historied", "h1")
			Expect(recs).To(HaveLen(2))
			// Newest first: the PATCH, holding what `title` was before it.
			Expect(recs[0].GetMask().GetPaths()).To(Equal([]string{"title"}))
			Expect(priorOf(recs[0]).GetTitle()).To(Equal("first"))
			Expect(priorOf(recs[0]).GetId()).To(Equal("h1"), "the prior always identifies its row")
			// The PUT that created the row: nothing came before it.
			Expect(recs[1].GetRowCreated()).To(BeTrue())
			Expect(recs[1].GetMask().GetPaths()).To(BeEmpty())
			Expect(recs[1].GetPrior()).To(BeEmpty())
			Expect(recs[0].GetRowCreated()).To(BeFalse())
		})

		It("records only the captured fields a PUT over a live row moved", func() {
			srv := newHistoryServer()
			client := newRPC(srv)

			_, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
				putMutation("c1", 1, "historied", "h1",
					&testdatav1.Historied{Id: "h1", Title: "first", Note: "kept"}),
				putMutation("c1", 2, "historied", "h1",
					&testdatav1.Historied{Id: "h1", Title: "second"}),
			})))
			Expect(err).NotTo(HaveOccurred())

			recs := historyOf(srv, ws, "historied", "h1")
			Expect(recs).To(HaveLen(2))
			// A PUT starts from the whole capture set — `note` is unmarked, so it
			// is out from the start — and the diff drops `body`, which both
			// writes left empty. What is left is what the PUT actually changed.
			Expect(recs[0].GetMask().GetPaths()).To(Equal([]string{"title"}))
			Expect(priorOf(recs[0]).GetTitle()).To(Equal("first"))
			Expect(priorOf(recs[0]).GetNote()).To(BeEmpty())
		})

		It("records nothing for a write that stores what is already there", func() {
			// The ordinary local-first shape: a client that re-PUTs the whole
			// row on every edit. Without the diff each of these lands a
			// full-capture-set record whose prior equals the row beside it.
			srv := newHistoryServer()
			client := newRPC(srv)

			row := &testdatav1.Historied{Id: "h1", Title: "first", Note: "n1"}
			_, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
				putMutation("c1", 1, "historied", "h1", row),
				putMutation("c1", 2, "historied", "h1", row),
				putMutation("c1", 3, "historied", "h1", row),
				// A PATCH re-sending a field's current value is the same
				// non-event, and it has to answer the same way.
				patchMutation("c1", 4, "historied", "h1",
					&testdatav1.Historied{Id: "h1", Title: "first"}, "title"),
			})))
			Expect(err).NotTo(HaveOccurred())

			recs := historyOf(srv, ws, "historied", "h1")
			Expect(recs).To(HaveLen(1), "only the write that created the row said anything")
			Expect(recs[0].GetRowCreated()).To(BeTrue())
		})

		It("leaves an unchanged text document out of the record", func() {
			// A text field compares as its encoded document. Re-putting the row
			// unions the same document with itself, which is a no-op on
			// canonical bytes — so a PUT that only moves `title` must not drag
			// `body` into the mask and claim the document changed.
			srv := newHistoryServer()
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Historied{Id: "h1", Title: "first"},
					server.WithTextString("body", "hello"), server.WithTextPeer("p1"))
			})).To(Succeed())
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Historied{Id: "h1", Title: "second"},
					server.WithTextString("body", "hello"), server.WithTextPeer("p1"))
			})).To(Succeed())

			recs := historyOf(srv, ws, "historied", "h1")
			Expect(recs).To(HaveLen(2))
			Expect(recs[0].GetMask().GetPaths()).To(Equal([]string{"title"}))
			Expect(priorOf(recs[0]).GetBody()).To(BeEmpty(),
				"a field the mask does not name carries no before-state")
		})

		It("reduces each element's document to its text in the before-state", func() {
			srv := newHistoryServer(server.WithEntities(&testdatav1.HistoriedKeyedText{}))
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.HistoriedKeyedText{
					Id:    "h1",
					Notes: []*testdatav1.KeyedNote{{Id: "n1"}, {Id: "n2"}},
				},
					server.WithTextString("notes.n1.body", "alpha"),
					server.WithTextString("notes.n2.body", "beta"),
					server.WithTextPeer("p1"))
			})).To(Succeed())
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Patch(&testdatav1.HistoriedKeyedText{
					Id:    "h1",
					Notes: []*testdatav1.KeyedNote{{Id: "n1"}},
				}, nil, nil,
					server.WithTextString("notes.n1.body", "ALPHA"),
					server.WithTextPeer("p1"))
			})).To(Succeed())

			recs := historyOf(srv, ws, "historied_keyed_text", "h1")
			Expect(recs).To(HaveLen(2))
			Expect(recs[0].GetMask().GetPaths()).To(Equal([]string{"notes.n1.body"}))
			var prior testdatav1.HistoriedKeyedText
			Expect(proto.Unmarshal(recs[0].GetPrior(), &prior)).To(Succeed())
			Expect(prior.GetNotes()).To(HaveLen(1), "only the addressed element is recorded")
			plain, err := text.PlainText(prior.GetNotes()[0].GetBody())
			Expect(err).NotTo(HaveOccurred())
			Expect(plain).To(Equal("alpha"))

			// The two steps of a restore: patch the prior back under its mask.
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Patch(&prior, recs[0].GetMask().GetPaths(), nil,
					server.WithTextPeer("p2"))
			})).To(Succeed())
			var restored testdatav1.HistoriedKeyedText
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				_, err := tx.Get(&restored, "h1")
				return err
			})).To(Succeed())
			restoredPlain, err := text.PlainText(restored.GetNotes()[0].GetBody())
			Expect(err).NotTo(HaveOccurred())
			Expect(restoredPlain).To(Equal("alpha"))
		})

		It("captures a oneof as its group", func() {
			// A oneof is one field to the merge and may only be masked by its
			// group name, so that is the only form the capture set — and any
			// record built from it — can use.
			//
			// Registered here rather than in testEntities: every spec that
			// enumerates the catalog would otherwise have to carry a fixture
			// only this one needs.
			srv := newHistoryServer(server.WithEntities(&testdatav1.HistoriedOneof{}))
			client := newRPC(srv)

			_, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
				putMutation("c1", 1, "historied_oneof", "o1", &testdatav1.HistoriedOneof{
					Id: "o1", Label: "l", Choice: &testdatav1.HistoriedOneof_ChoiceText{ChoiceText: "a"},
				}),
				// Switching members changes the group; `label` stands still.
				putMutation("c1", 2, "historied_oneof", "o1", &testdatav1.HistoriedOneof{
					Id: "o1", Label: "l", Choice: &testdatav1.HistoriedOneof_ChoiceNumber{ChoiceNumber: 7},
				}),
			})))
			Expect(err).NotTo(HaveOccurred())

			recs := historyOf(srv, ws, "historied_oneof", "o1")
			Expect(recs).To(HaveLen(2))
			Expect(recs[0].GetMask().GetPaths()).To(Equal([]string{"choice"}))
			var prior testdatav1.HistoriedOneof
			Expect(proto.Unmarshal(recs[0].GetPrior(), &prior)).To(Succeed())
			Expect(prior.GetChoiceText()).To(Equal("a"))
		})

		It("captures the writes an EntityTx makes", func() {
			srv := newHistoryServer()

			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Historied{Id: "h1", Title: "first"})
			})).To(Succeed())
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Patch(&testdatav1.Historied{Id: "h1", Title: "second"}, []string{"title"}, nil)
			})).To(Succeed())

			recs := historyOf(srv, ws, "historied", "h1")
			Expect(recs).To(HaveLen(2))
			Expect(recs[0].GetMask().GetPaths()).To(Equal([]string{"title"}))
			Expect(priorOf(recs[0]).GetTitle()).To(Equal("first"))
			Expect(recs[0].GetActorSystem()).To(Equal("system"), "a trusted write records itself as one")
		})

		It("records nothing for a write that touches no captured field", func() {
			srv := newHistoryServer()
			client := newRPC(srv)

			_, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
				putMutation("c1", 1, "historied", "h1", &testdatav1.Historied{Id: "h1", Note: "n1"}),
				// `note` carries no history mark, so this edit is invisible to
				// the log — the whole point of narrowing the capture set.
				patchMutation("c1", 2, "historied", "h1",
					&testdatav1.Historied{Id: "h1", Note: "n2"}, "note"),
			})))
			Expect(err).NotTo(HaveOccurred())

			Expect(historyOf(srv, ws, "historied", "h1")).To(HaveLen(1))
		})

		It("captures a whole entity's fields when every one of them is marked", func() {
			srv := newHistoryServer()
			client := newRPC(srv)

			_, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
				putMutation("c1", 1, "historied_all", "a1",
					&testdatav1.HistoriedAll{Id: "a1", Title: "first", Count: 1}),
				patchMutation("c1", 2, "historied_all", "a1",
					&testdatav1.HistoriedAll{Id: "a1", Count: 2}, "count"),
			})))
			Expect(err).NotTo(HaveOccurred())

			recs := historyOf(srv, ws, "historied_all", "a1")
			Expect(recs).To(HaveLen(2))
			Expect(recs[0].GetMask().GetPaths()).To(Equal([]string{"count"}))
			// The PUT before it recorded the row's creation; a PUT over a live
			// row lists every marked field it moved.
			Expect(recs[1].GetRowCreated()).To(BeTrue())

			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.HistoriedAll{Id: "a1", Title: "third"})
			})).To(Succeed())
			recs = historyOf(srv, ws, "historied_all", "a1")
			Expect(recs[0].GetMask().GetPaths()).To(Equal([]string{"title", "count"}))
		})

		It("refuses an entity that opts in and marks no field", func() {
			// Neither reading of an empty capture set is safe to guess at:
			// "everything" grows the log without the schema asking, "nothing"
			// leaves a panel permanently empty with nothing to point at.
			err := newTestServerErr(server.WithEntities(&testdatav1.HistoriedUnmarked{}))
			Expect(err).To(MatchError(ContainSubstring("marks no field")))
			Expect(err).To(MatchError(ContainSubstring("historied_unmarked")))
		})

		It("captures a text field a write option added to the mask", func() {
			// WithTextString adds its path to the mask before the write merges
			// or authorizes it, so capture has to see the EFFECTIVE mask —
			// otherwise the one kind of write that overwrites a document is the
			// one whose before-state goes unrecorded.
			srv := newHistoryServer()
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Historied{Id: "h1", Title: "t"},
					server.WithTextString("body", "hello"), server.WithTextPeer("p1"))
			})).To(Succeed())
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				// The caller's mask names only `title`; the option adds `body`.
				return tx.Patch(&testdatav1.Historied{Id: "h1", Title: "t2"}, []string{"title"}, nil,
					server.WithTextString("body", "goodbye"), server.WithTextPeer("p1"))
			})).To(Succeed())

			recs := historyOf(srv, ws, "historied", "h1")
			Expect(recs[0].GetMask().GetPaths()).To(ConsistOf("title", "body"))
			Expect(text.PlainString(priorOf(recs[0]).GetBody())).To(Equal("hello"))
		})

		Describe("a text field's before-state", func() {
			It("is a plain-string value, not the encoded document", func() {
				// Storing the document would be dead weight in both directions:
				// nothing can restore it as itself (a write back unions it with
				// the live document, where the delete markers win), and it
				// carries a peer table, a version vector and the tombstoned runs
				// holding every deleted character.
				srv := newHistoryServer()
				Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
					return tx.Put(&testdatav1.Historied{Id: "h1", Title: "t"},
						server.WithTextString("body", "the quick brown fox"),
						server.WithTextPeer("p1"))
				})).To(Succeed())
				Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
					return tx.Patch(&testdatav1.Historied{Id: "h1"}, nil, nil,
						server.WithTextString("body", "the lazy dog"), server.WithTextPeer("p1"))
				})).To(Succeed())

				recs := historyOf(srv, ws, "historied", "h1")
				Expect(recs[0].GetMask().GetPaths()).To(ConsistOf("body"))
				body := priorOf(recs[0]).GetBody()
				Expect(text.IsPlainString(body)).To(BeTrue())
				Expect(text.PlainString(body)).To(Equal("the quick brown fox"))
				// Not merely "renders as" — nothing that decodes as a document
				// is left in the value, and the decoder says so by name.
				_, err := text.Decode(body, "")
				Expect(err).To(MatchError(text.ErrPlainStringUnresolved))
				// The read path a history UI takes, which does not know which
				// form the field holds.
				Expect(text.PlainText(body)).To(Equal("the quick brown fox"))
			})

			It("is smaller than the document it came from", func() {
				// The point of the exercise. A document that has been edited
				// carries its deleted runs, so the gap widens with editing rather
				// than closing.
				srv := newHistoryServer()
				Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
					return tx.Put(&testdatav1.Historied{Id: "h1"},
						server.WithTextString("body", strings.Repeat("draft ", 200)),
						server.WithTextPeer("p1"))
				})).To(Succeed())
				for i := 0; i < 5; i++ {
					Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
						return tx.Patch(&testdatav1.Historied{Id: "h1"}, nil, nil,
							server.WithTextString("body", strings.Repeat(fmt.Sprintf("rev%d ", i), 200)),
							server.WithTextPeer("p1"))
					})).To(Succeed())
				}

				var stored int
				Expect(srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
					row := &testdatav1.Historied{}
					found, _, err := tx.Get(row, "h1")
					Expect(found).To(BeTrue())
					stored = len(row.GetBody())
					return err
				})).To(Succeed())

				recs := historyOf(srv, ws, "historied", "h1")
				Expect(recs[0].GetPrior()).NotTo(BeEmpty())
				Expect(len(recs[0].GetPrior())).To(BeNumerically("<", stored),
					"a record must not cost more than the row it describes")
			})

			It("reads back as unset when the document held nothing", func() {
				// An emptied document still encodes to bytes — its tombstones
				// outlive the text — and a restore has to treat "held nothing"
				// and "was never set" the same way.
				srv := newHistoryServer()
				Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
					return tx.Put(&testdatav1.Historied{Id: "h1"},
						server.WithTextString("body", "gone"), server.WithTextPeer("p1"))
				})).To(Succeed())
				Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
					return tx.Patch(&testdatav1.Historied{Id: "h1"}, nil, nil,
						server.WithTextString("body", ""), server.WithTextPeer("p1"))
				})).To(Succeed())
				Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
					return tx.Patch(&testdatav1.Historied{Id: "h1"}, nil, nil,
						server.WithTextString("body", "back"), server.WithTextPeer("p1"))
				})).To(Succeed())

				recs := historyOf(srv, ws, "historied", "h1")
				Expect(recs[0].GetMask().GetPaths()).To(ConsistOf("body"))
				Expect(priorOf(recs[0]).GetBody()).To(BeEmpty())
			})
		})

		// A restore is decode-then-PATCH-under-the-mask, with no per-field
		// handling: that generic shape working on an entity that HAS a text
		// field is what the plain-string value buys.
		Describe("restoring from a record", func() {
			It("restores every masked field, text included, without naming one", func() {
				srv := newHistoryServer()
				Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
					return tx.Put(&testdatav1.Historied{Id: "h1", Title: "first"},
						server.WithTextString("body", "the quick brown fox"),
						server.WithTextPeer("p1"))
				})).To(Succeed())
				Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
					return tx.Patch(&testdatav1.Historied{Id: "h1", Title: "second"}, []string{"title"}, nil,
						server.WithTextString("body", "the lazy dog"), server.WithTextPeer("p1"))
				})).To(Succeed())

				rec := historyOf(srv, ws, "historied", "h1")[0]
				Expect(rec.GetMask().GetPaths()).To(ConsistOf("title", "body"))

				Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
					return tx.Patch(priorOf(rec), rec.GetMask().GetPaths(), nil,
						server.WithTextPeer("restorer"))
				})).To(Succeed())

				Expect(srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
					row := &testdatav1.Historied{}
					found, _, err := tx.Get(row, "h1")
					Expect(found).To(BeTrue())
					Expect(row.GetTitle()).To(Equal("first"))
					// A real document again, not the value that was recorded.
					Expect(text.IsPlainString(row.GetBody())).To(BeFalse())
					doc, decErr := text.Decode(row.GetBody(), "")
					Expect(decErr).NotTo(HaveOccurred())
					Expect(doc.PlainText()).To(Equal("the quick brown fox"))
					return err
				})).To(Succeed())
			})

			It("lands as edits on the live document rather than replacing it", func() {
				// The restored value is diffed against what is stored, so the
				// common prefix keeps its original items: a restore is an edit,
				// which is what lets it merge rather than resurrect a whole
				// document under one writer.
				srv := newHistoryServer()
				Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
					return tx.Put(&testdatav1.Historied{Id: "h1"},
						server.WithTextString("body", "shared prefix, original tail"),
						server.WithTextPeer("p1"))
				})).To(Succeed())
				Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
					return tx.Patch(&testdatav1.Historied{Id: "h1"}, nil, nil,
						server.WithTextString("body", "shared prefix, replaced tail"),
						server.WithTextPeer("p1"))
				})).To(Succeed())

				rec := historyOf(srv, ws, "historied", "h1")[0]
				Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
					return tx.Patch(priorOf(rec), rec.GetMask().GetPaths(), nil,
						server.WithTextPeer("restorer"))
				})).To(Succeed())

				Expect(srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
					row := &testdatav1.Historied{}
					found, _, err := tx.Get(row, "h1")
					Expect(found).To(BeTrue())
					doc, decErr := text.Decode(row.GetBody(), "")
					Expect(decErr).NotTo(HaveOccurred())
					Expect(doc.PlainText()).To(Equal("shared prefix, original tail"))
					// Only the tail was rewritten, so the restorer speaks for
					// less than the whole document.
					Expect(doc.VersionVector()).To(HaveKey("p1"))
					return err
				})).To(Succeed())
			})

			It("refuses a restore that names no writer identity", func() {
				// The value says what the text was, never who is putting it
				// back, and the engine does not invent a peer.
				srv := newHistoryServer()
				Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
					return tx.Put(&testdatav1.Historied{Id: "h1"},
						server.WithTextString("body", "before"), server.WithTextPeer("p1"))
				})).To(Succeed())
				Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
					return tx.Patch(&testdatav1.Historied{Id: "h1"}, nil, nil,
						server.WithTextString("body", "after"), server.WithTextPeer("p1"))
				})).To(Succeed())

				rec := historyOf(srv, ws, "historied", "h1")[0]
				err := srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
					return tx.Patch(priorOf(rec), rec.GetMask().GetPaths(), nil)
				})
				Expect(err).To(MatchError(ContainSubstring("WithTextPeer")))
			})

			It("ignores a recorded text value the restore's mask leaves out", func() {
				// A PATCH writes what its mask names. The text value rides along
				// in the decoded record either way, and must not widen it.
				srv := newHistoryServer()
				Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
					return tx.Put(&testdatav1.Historied{Id: "h1", Title: "first"},
						server.WithTextString("body", "before"), server.WithTextPeer("p1"))
				})).To(Succeed())
				Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
					return tx.Patch(&testdatav1.Historied{Id: "h1", Title: "second"}, []string{"title"}, nil,
						server.WithTextString("body", "after"), server.WithTextPeer("p1"))
				})).To(Succeed())

				rec := historyOf(srv, ws, "historied", "h1")[0]
				Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
					return tx.Patch(priorOf(rec), []string{"title"}, nil,
						server.WithTextPeer("restorer"))
				})).To(Succeed())

				Expect(srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
					row := &testdatav1.Historied{}
					found, _, err := tx.Get(row, "h1")
					Expect(found).To(BeTrue())
					Expect(row.GetTitle()).To(Equal("first"))
					Expect(text.PlainText(row.GetBody())).To(Equal("after"))
					return err
				})).To(Succeed())
			})
		})
	})

	// A local-first client writes per edit, and for a text field an edit is a
	// keystroke. Folding a run of writes by one actor into the record the first
	// of them opened is what keeps the log proportional to editing sessions
	// rather than to keystrokes — these specs pin both what it collapses and
	// what it refuses to.
	Describe("session folding", func() {
		// typist and editor are two distinct actors, which is what lets a spec
		// create a row and then edit it without the creation record — whose
		// empty mask absorbs everything after it — swallowing the edits.
		typist := func() context.Context { return server.TrustedAs(ctx, "typist") }
		editor := func() context.Context { return server.TrustedAs(ctx, "editor") }

		It("folds a run of writes into the record the first one opened", func() {
			srv := newTestServer()
			Expect(srv.Mutate(typist(), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Historied{Id: "h1", Title: "first"})
			})).To(Succeed())

			for _, title := range []string{"second", "third", "fourth"} {
				Expect(srv.Mutate(editor(), ws, func(tx *server.EntityTx) error {
					return tx.Patch(&testdatav1.Historied{Id: "h1", Title: title}, []string{"title"}, nil)
				})).To(Succeed())
			}

			recs := historyOf(srv, ws, "historied", "h1")
			Expect(recs).To(HaveLen(2), "the creation, and one record for the run that followed")
			Expect(recs[0].GetMask().GetPaths()).To(Equal([]string{"title"}))
			Expect(priorOf(recs[0]).GetTitle()).To(Equal("first"),
				"the record says what the field held before the run, not before its last write")
			Expect(recs[0].GetWrites()).To(BeEquivalentTo(3))
			Expect(recs[0].GetThroughSeq()).To(BeNumerically(">", recs[0].GetSeq()),
				"the record spans the writes it folded")
			Expect(recs[0].GetActorSystem()).To(Equal("editor"))
		})

		It("keeps a typing session to one record", func() {
			// The case the feature exists for. Each of these writes carries the
			// whole document; unfolded, each would also RECORD one.
			srv := newTestServer()
			Expect(srv.Mutate(typist(), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Historied{Id: "h1", Title: "draft"})
			})).To(Succeed())

			typed := ""
			for _, ch := range "a paragraph typed one character at a time" {
				typed += string(ch)
				Expect(srv.Mutate(editor(), ws, func(tx *server.EntityTx) error {
					return tx.Patch(&testdatav1.Historied{Id: "h1"}, nil, nil,
						server.WithTextString("body", typed), server.WithTextPeer("p1"))
				})).To(Succeed())
			}

			recs := historyOf(srv, ws, "historied", "h1")
			Expect(recs).To(HaveLen(2))
			Expect(recs[0].GetMask().GetPaths()).To(ConsistOf("body"))
			Expect(recs[0].GetWrites()).To(BeEquivalentTo(len(typed)))
			Expect(priorOf(recs[0]).GetBody()).To(BeEmpty(),
				"the document was empty when the session opened")
		})

		It("widens the mask as a session touches more fields", func() {
			srv := newTestServer()
			Expect(srv.Mutate(typist(), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Historied{Id: "h1", Title: "first"},
					server.WithTextString("body", "opening line"), server.WithTextPeer("p1"))
			})).To(Succeed())

			Expect(srv.Mutate(editor(), ws, func(tx *server.EntityTx) error {
				return tx.Patch(&testdatav1.Historied{Id: "h1", Title: "second"}, []string{"title"}, nil)
			})).To(Succeed())
			Expect(srv.Mutate(editor(), ws, func(tx *server.EntityTx) error {
				return tx.Patch(&testdatav1.Historied{Id: "h1"}, nil, nil,
					server.WithTextString("body", "rewritten"), server.WithTextPeer("p2"))
			})).To(Succeed())

			recs := historyOf(srv, ws, "historied", "h1")
			Expect(recs).To(HaveLen(2))
			Expect(recs[0].GetMask().GetPaths()).To(ConsistOf("title", "body"))
			// Each field's before-state is as of the session's start, not as of
			// the write within it that first touched the field.
			Expect(priorOf(recs[0]).GetTitle()).To(Equal("first"))
			Expect(text.PlainString(priorOf(recs[0]).GetBody())).To(Equal("opening line"))
		})

		It("keeps a recorded sub-path when a later write claims its whole field", func() {
			// A capture set names top-level units, but a mask may point INSIDE one
			// — so a session can hold "settings.color" when a PUT folds in
			// proposing "settings". The wider path replaces the whole subtree, and
			// the earlier write's colour is the only record left of what the field
			// held when the session opened.
			srv := newTestServer(server.WithEntities(&testdatav1.HistoriedNested{}))
			Expect(srv.Mutate(typist(), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.HistoriedNested{
					Id: "n1", Settings: &testdatav1.Settings{Color: "red", Size: 1},
				})
			})).To(Succeed())

			Expect(srv.Mutate(editor(), ws, func(tx *server.EntityTx) error {
				return tx.Patch(&testdatav1.HistoriedNested{
					Id: "n1", Settings: &testdatav1.Settings{Color: "blue"},
				}, []string{"settings.color"}, nil)
			})).To(Succeed())
			Expect(srv.Mutate(editor(), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.HistoriedNested{
					Id: "n1", Settings: &testdatav1.Settings{Color: "blue", Size: 2},
				})
			})).To(Succeed())

			recs := historyOf(srv, ws, "historied_nested", "n1")
			Expect(recs).To(HaveLen(2))
			var prior testdatav1.HistoriedNested
			Expect(proto.Unmarshal(recs[0].GetPrior(), &prior)).To(Succeed())
			Expect(prior.GetSettings().GetColor()).To(Equal("red"),
				"the sub-path recorded first says what the session started from")
			Expect(prior.GetSettings().GetSize()).To(BeEquivalentTo(1),
				"and the folding write supplies the rest of the subtree")
			Expect(recs[0].GetMask().GetPaths()).To(ConsistOf("settings"),
				"the wider path subsumes the one it covers")
		})

		It("widens a mask across two sub-paths of one field", func() {
			srv := newTestServer(server.WithEntities(&testdatav1.HistoriedNested{}))
			Expect(srv.Mutate(typist(), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.HistoriedNested{
					Id: "n1", Settings: &testdatav1.Settings{Color: "red", Size: 1},
				})
			})).To(Succeed())

			Expect(srv.Mutate(editor(), ws, func(tx *server.EntityTx) error {
				return tx.Patch(&testdatav1.HistoriedNested{
					Id: "n1", Settings: &testdatav1.Settings{Color: "blue"},
				}, []string{"settings.color"}, nil)
			})).To(Succeed())
			Expect(srv.Mutate(editor(), ws, func(tx *server.EntityTx) error {
				return tx.Patch(&testdatav1.HistoriedNested{
					Id: "n1", Settings: &testdatav1.Settings{Size: 2},
				}, []string{"settings.size"}, nil)
			})).To(Succeed())

			recs := historyOf(srv, ws, "historied_nested", "n1")
			Expect(recs).To(HaveLen(2))
			Expect(recs[0].GetMask().GetPaths()).To(ConsistOf("settings.color", "settings.size"),
				"neither sub-path covers the other, so both are recorded")
			var prior testdatav1.HistoriedNested
			Expect(proto.Unmarshal(recs[0].GetPrior(), &prior)).To(Succeed())
			Expect(prior.GetSettings().GetColor()).To(Equal("red"))
			Expect(prior.GetSettings().GetSize()).To(BeEquivalentTo(1))
		})

		It("does not resume an actor's earlier session across another's write", func() {
			// One session is open per row at a time, so alternating writers fold
			// nothing. Resuming the typist's first record would be the wrong
			// answer twice over: its span [seq, through_seq] would claim to cover
			// the editor's write, and its prior would no longer be what the field
			// held before the writes it now covers.
			srv := newTestServer()
			Expect(srv.Mutate(typist(), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Historied{Id: "h1", Title: "first"})
			})).To(Succeed())
			for _, w := range []struct {
				who   context.Context
				title string
			}{
				{typist(), "typist one"},
				{editor(), "editor one"},
				{typist(), "typist two"},
			} {
				Expect(srv.Mutate(w.who, ws, func(tx *server.EntityTx) error {
					return tx.Patch(&testdatav1.Historied{Id: "h1", Title: w.title}, []string{"title"}, nil)
				})).To(Succeed())
			}

			recs := historyOf(srv, ws, "historied", "h1")
			Expect(recs).To(HaveLen(3))
			// Newest first: the typist's second run, the editor's, and the
			// creation record that absorbed the typist's first.
			Expect(recs[0].GetActorSystem()).To(Equal("typist"))
			Expect(priorOf(recs[0]).GetTitle()).To(Equal("editor one"),
				"a resumed session would have claimed the field still held what the typist left")
			Expect(recs[0].GetWrites()).To(BeEquivalentTo(1))
			Expect(recs[1].GetActorSystem()).To(Equal("editor"))
			Expect(priorOf(recs[1]).GetTitle()).To(Equal("typist one"))
			Expect(recs[2].GetRowCreated()).To(BeTrue())
			Expect(recs[2].GetWrites()).To(BeEquivalentTo(2), "the creation absorbed the write after it")
		})

		It("opens a new record for a second actor", func() {
			// A record's actor fields attribute every write folded into it, so
			// letting anyone extend it would file their edit under another name.
			srv := newTestServer()
			Expect(srv.Mutate(typist(), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Historied{Id: "h1", Title: "first"})
			})).To(Succeed())
			Expect(srv.Mutate(typist(), ws, func(tx *server.EntityTx) error {
				return tx.Patch(&testdatav1.Historied{Id: "h1", Title: "second"}, []string{"title"}, nil)
			})).To(Succeed())
			Expect(srv.Mutate(editor(), ws, func(tx *server.EntityTx) error {
				return tx.Patch(&testdatav1.Historied{Id: "h1", Title: "third"}, []string{"title"}, nil)
			})).To(Succeed())

			recs := historyOf(srv, ws, "historied", "h1")
			Expect(recs).To(HaveLen(2), "the typist's session, and the editor's own")
			Expect(recs[0].GetActorSystem()).To(Equal("editor"))
			Expect(priorOf(recs[0]).GetTitle()).To(Equal("second"))
			Expect(recs[1].GetActorSystem()).To(Equal("typist"))
		})

		It("folds writes one caller made from two clients into one record", func() {
			// client_id is an idempotency handle, not an identity: one user
			// typing in two browser tabs is one editing session.
			srv := newTestServer()
			user := server.WithPrincipal(ctx, server.Principal{Subject: "u_7"})
			Expect(srv.Mutate(typist(), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Historied{Id: "h1", Title: "first"})
			})).To(Succeed())

			for i, clientID := range []string{"c1", "c2"} {
				resp, err := srv.Push(user, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
					patchMutation(clientID, 1, "historied", "h1",
						&testdatav1.Historied{Id: "h1", Title: fmt.Sprintf("edit %d", i)}, "title"),
				})))
				Expect(err).NotTo(HaveOccurred())
				for _, r := range pushResults(resp.Msg) {
					Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED),
						"push rejected: %s", r.GetErrorMessage())
				}
			}

			recs := historyOf(srv, ws, "historied", "h1")
			Expect(recs).To(HaveLen(2), "the creation, and one record for the user's session")
			Expect(recs[0].GetActorSubject()).To(Equal("u_7"))
			Expect(recs[0].GetWrites()).To(BeEquivalentTo(2))
			Expect(priorOf(recs[0]).GetTitle()).To(Equal("first"))
		})

		It("opens a new record when a bypass runs under a caller's request", func() {
			// A restore a user triggered is not a keystroke in their typing
			// session, so the system label separates the two records even
			// though both carry the same subject.
			srv := newTestServer()
			user := server.WithPrincipal(ctx, server.Principal{Subject: "u_7"})
			Expect(srv.Mutate(typist(), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Historied{Id: "h1", Title: "first"})
			})).To(Succeed())
			Expect(srv.Mutate(user, ws, func(tx *server.EntityTx) error {
				return tx.Patch(&testdatav1.Historied{Id: "h1", Title: "typed"}, []string{"title"}, nil)
			})).To(Succeed())
			Expect(srv.Mutate(server.TrustedAs(user, "restore"), ws, func(tx *server.EntityTx) error {
				return tx.Patch(&testdatav1.Historied{Id: "h1", Title: "restored"}, []string{"title"}, nil)
			})).To(Succeed())

			recs := historyOf(srv, ws, "historied", "h1")
			Expect(recs).To(HaveLen(3), "the creation, the user's edit, and the restore")
			Expect(recs[0].GetActorSubject()).To(Equal("u_7"))
			Expect(recs[0].GetActorSystem()).To(Equal("restore"))
			Expect(recs[1].GetActorSubject()).To(Equal("u_7"))
			Expect(recs[1].GetActorSystem()).To(BeEmpty())
		})

		It("opens a new record once the window has passed", func() {
			// The window runs from when the record OPENED, not from its last
			// write, so continuous editing still yields a timeline rather than
			// one entry covering an afternoon.
			window := 40 * time.Millisecond
			srv := newTestServer(server.WithHistorySession(window))
			Expect(srv.Mutate(typist(), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Historied{Id: "h1", Title: "first"})
			})).To(Succeed())
			Expect(srv.Mutate(editor(), ws, func(tx *server.EntityTx) error {
				return tx.Patch(&testdatav1.Historied{Id: "h1", Title: "second"}, []string{"title"}, nil)
			})).To(Succeed())

			time.Sleep(3 * window)

			Expect(srv.Mutate(editor(), ws, func(tx *server.EntityTx) error {
				return tx.Patch(&testdatav1.Historied{Id: "h1", Title: "third"}, []string{"title"}, nil)
			})).To(Succeed())

			recs := historyOf(srv, ws, "historied", "h1")
			Expect(recs).To(HaveLen(3))
			Expect(priorOf(recs[0]).GetTitle()).To(Equal("second"),
				"the write after the window records what it actually overwrote")
			Expect(recs[0].GetWrites()).To(BeEquivalentTo(1))
		})

		It("absorbs a session that began before the row existed", func() {
			// row_created says the row did not exist when the session opened, and
			// nothing later can give a field an earlier value than "absent".
			srv := newTestServer()
			Expect(srv.Mutate(typist(), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Historied{Id: "h1", Title: "first"})
			})).To(Succeed())
			Expect(srv.Mutate(typist(), ws, func(tx *server.EntityTx) error {
				return tx.Patch(&testdatav1.Historied{Id: "h1", Title: "second"}, []string{"title"}, nil)
			})).To(Succeed())

			recs := historyOf(srv, ws, "historied", "h1")
			Expect(recs).To(HaveLen(1))
			Expect(recs[0].GetRowCreated()).To(BeTrue())
			Expect(recs[0].GetMask().GetPaths()).To(BeEmpty())
			Expect(recs[0].GetPrior()).To(BeEmpty())
			Expect(recs[0].GetWrites()).To(BeEquivalentTo(2))
		})

		It("starts a fresh record after a DELETE purged the one it was extending", func() {
			// The purge leaves the in-memory note pointing at a record that is
			// gone, which is exactly the staleness the note is allowed to have:
			// the capture re-reads and finds nothing rather than folding into it.
			srv := newTestServer()
			Expect(srv.Mutate(typist(), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Historied{Id: "h1", Title: "first"})
			})).To(Succeed())
			Expect(srv.Mutate(typist(), ws, func(tx *server.EntityTx) error {
				return tx.Delete(&testdatav1.Historied{}, "h1")
			})).To(Succeed())
			Expect(historyOf(srv, ws, "historied", "h1")).To(BeEmpty())

			Expect(srv.Mutate(typist(), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Historied{Id: "h1", Title: "recreated"})
			})).To(Succeed())

			recs := historyOf(srv, ws, "historied", "h1")
			Expect(recs).To(HaveLen(1))
			Expect(recs[0].GetWrites()).To(BeEquivalentTo(1))
			Expect(recs[0].GetRowCreated()).To(BeTrue(), "the row did not exist again")
		})

		It("records every write when folding is off", func() {
			srv := newHistoryServer()
			Expect(srv.Mutate(typist(), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Historied{Id: "h1", Title: "first"})
			})).To(Succeed())
			for _, title := range []string{"second", "third"} {
				Expect(srv.Mutate(typist(), ws, func(tx *server.EntityTx) error {
					return tx.Patch(&testdatav1.Historied{Id: "h1", Title: title}, []string{"title"}, nil)
				})).To(Succeed())
			}

			recs := historyOf(srv, ws, "historied", "h1")
			Expect(recs).To(HaveLen(3))
			for _, rec := range recs {
				Expect(rec.GetWrites()).To(BeEquivalentTo(1))
				Expect(rec.GetThroughSeq()).To(Equal(rec.GetSeq()))
			}
		})
	})

	Describe("sequencing", func() {
		It("lands at the triggering write's sequence", func() {
			srv := newHistoryServer()
			client := newRPC(srv)

			resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
				putMutation("c1", 1, "historied", "h1", &testdatav1.Historied{Id: "h1", Title: "a"}),
				patchMutation("c1", 2, "historied", "h1",
					&testdatav1.Historied{Id: "h1", Title: "b"}, "title"),
			})))
			Expect(err).NotTo(HaveOccurred())
			results := pushResults(resp.Msg)
			Expect(results).To(HaveLen(2))

			recs := historyOf(srv, ws, "historied", "h1")
			Expect(recs[0].GetSeq()).To(Equal(results[1].GetSeq()))
			Expect(recs[1].GetSeq()).To(Equal(results[0].GetSeq()))
		})

		It("keeps the earliest before-state when one sequence writes a row twice", func() {
			// A batch shares ONE sequence across its sub-mutations, so both
			// writes want the same history key. The record has to describe the
			// state before the FIRST of them, plus whatever fields only the
			// second touched.
			srv := newHistoryServer()
			client := newRPC(srv)

			_, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
				putMutation("c1", 1, "historied_all", "a1",
					&testdatav1.HistoriedAll{Id: "a1", Title: "first", Count: 1}),
				batchMutation("c1", 2,
					crudPatch("historied_all", "a1", &testdatav1.HistoriedAll{Id: "a1", Title: "second"}, "title"),
					crudPatch("historied_all", "a1", &testdatav1.HistoriedAll{Id: "a1", Count: 9}, "count"),
				),
			})))
			Expect(err).NotTo(HaveOccurred())

			recs := historyOf(srv, ws, "historied_all", "a1")
			Expect(recs).To(HaveLen(2), "the batch's two writes share one record")
			Expect(recs[0].GetMask().GetPaths()).To(ConsistOf("title", "count"))
			var prior testdatav1.HistoriedAll
			Expect(proto.Unmarshal(recs[0].GetPrior(), &prior)).To(Succeed())
			Expect(prior.GetTitle()).To(Equal("first"))
			Expect(prior.GetCount()).To(BeEquivalentTo(1))
		})

		It("pages a pull across the version the history shares with its trigger", func() {
			// A history record is written at its trigger's sequence, so a page
			// boundary can split the group. Nothing may be lost — which is what
			// ScanChanges' (Version, Entity, Key) tie-break already guarantees,
			// and this pins that history rows did not break it.
			srv := newHistoryServer()
			client := newRPC(srv)

			var muts []*syncplanev1.Mutation
			for i := 1; i <= 5; i++ {
				muts = append(muts, putMutation("c1", uint64(i), "historied", fmt.Sprintf("h%d", i),
					&testdatav1.Historied{Id: fmt.Sprintf("h%d", i), Title: "t"}))
			}
			_, err := client.Push(ctx, connect.NewRequest(pushOf(ws, muts)))
			Expect(err).NotTo(HaveOccurred())

			var keys []string
			var cursor []byte
			for {
				resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
					WorkspaceId: ws, Cursor: cursor, Limit: 2,
				}))
				Expect(err).NotTo(HaveOccurred())
				for _, rc := range resp.Msg.GetPatch().GetChanges() {
					keys = append(keys, rc.GetEntity()+"/"+rc.GetKey())
				}
				cursor = resp.Msg.GetNextCursor()
				if !resp.Msg.GetHasMore() {
					break
				}
			}
			Expect(keys).To(ConsistOf(
				"historied/h1", "historied/h2", "historied/h3", "historied/h4", "historied/h5"))
		})
	})

	Describe("the admission hook", func() {
		It("drops a record it rejects", func() {
			srv := newHistoryServer(server.WithHistoryFilter(
				func(_ context.Context, rec *syncplanev1.FieldHistory, _ server.EntityMeta) (*syncplanev1.FieldHistory, bool, error) {
					return rec, rec.GetSourceKey() != "h2", nil
				}))

			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				if err := tx.Put(&testdatav1.Historied{Id: "h1", Title: "a"}); err != nil {
					return err
				}
				return tx.Put(&testdatav1.Historied{Id: "h2", Title: "a"})
			})).To(Succeed())

			Expect(historyOf(srv, ws, "historied", "h1")).To(HaveLen(1))
			Expect(historyOf(srv, ws, "historied", "h2")).To(BeEmpty())
			// The write itself is unaffected — the record was dropped, not the row.
			Expect(srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
				found, _, err := tx.Get(&testdatav1.Historied{}, "h2")
				Expect(found).To(BeTrue())
				return err
			})).To(Succeed())
		})

		It("persists a record it rewrote", func() {
			srv := newHistoryServer(server.WithHistoryFilter(
				func(_ context.Context, rec *syncplanev1.FieldHistory, src server.EntityMeta) (*syncplanev1.FieldHistory, bool, error) {
					Expect(src.Name).To(Equal("historied"))
					// Redaction: keep the shape, drop the value.
					rec.Prior = nil
					rec.ActorSubject = "redacted"
					return rec, true, nil
				}))

			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Historied{Id: "h1", Title: "secret"})
			})).To(Succeed())

			recs := historyOf(srv, ws, "historied", "h1")
			Expect(recs).To(HaveLen(1))
			Expect(recs[0].GetPrior()).To(BeEmpty())
			Expect(recs[0].GetActorSubject()).To(Equal("redacted"))
		})

		It("keeps folding into a record whose mask it redacted away", func() {
			// A hook that strips every path leaves an empty mask on a record that
			// is not a creation. Only row_created makes a record absorbing, so the
			// write that folds in after the redaction still records what it
			// overwrote — a redaction policy costs the paths it redacts and
			// nothing else.
			srv := newTestServer(server.WithHistoryFilter(
				func(_ context.Context, rec *syncplanev1.FieldHistory, _ server.EntityMeta) (*syncplanev1.FieldHistory, bool, error) {
					if slices.Equal(rec.GetMask().GetPaths(), []string{"title"}) {
						rec.Mask = nil
						rec.Prior = nil
					}
					return rec, true, nil
				}))

			// The creator is a different actor, so the editor's first PATCH opens
			// a record of its own rather than folding into the creation.
			Expect(srv.Mutate(server.TrustedAs(ctx, "typist"), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.HistoriedAll{Id: "a1", Title: "first", Count: 1})
			})).To(Succeed())
			Expect(srv.Mutate(server.TrustedAs(ctx, "editor"), ws, func(tx *server.EntityTx) error {
				return tx.Patch(&testdatav1.HistoriedAll{Id: "a1", Title: "second"}, []string{"title"}, nil)
			})).To(Succeed())
			Expect(srv.Mutate(server.TrustedAs(ctx, "editor"), ws, func(tx *server.EntityTx) error {
				return tx.Patch(&testdatav1.HistoriedAll{Id: "a1", Count: 2}, []string{"count"}, nil)
			})).To(Succeed())

			recs := historyOf(srv, ws, "historied_all", "a1")
			Expect(recs).To(HaveLen(2), "the creation, and the session the first PATCH opened")
			Expect(recs[0].GetRowCreated()).To(BeFalse())
			Expect(recs[0].GetWrites()).To(BeEquivalentTo(2))
			Expect(recs[0].GetMask().GetPaths()).To(Equal([]string{"count"}),
				"the redacted title stays gone and the fold records what it overwrote")
			var prior testdatav1.HistoriedAll
			Expect(proto.Unmarshal(recs[0].GetPrior(), &prior)).To(Succeed())
			Expect(prior.GetCount()).To(BeEquivalentTo(1))
			Expect(recs[1].GetRowCreated()).To(BeTrue())
		})

		It("refuses to let the hook restate whether the row existed", func() {
			// row_created is what makes a record absorb the rest of its session, so
			// a hook that set it would silence every write after it.
			srv := newHistoryServer(server.WithHistoryFilter(
				func(_ context.Context, rec *syncplanev1.FieldHistory, _ server.EntityMeta) (*syncplanev1.FieldHistory, bool, error) {
					rec.RowCreated = true
					return rec, true, nil
				}))
			client := newRPC(srv)

			resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
				putMutation("c1", 1, "historied", "h1", &testdatav1.Historied{Id: "h1", Title: "a"}),
				patchMutation("c1", 2, "historied", "h1",
					&testdatav1.Historied{Id: "h1", Title: "b"}, "title"),
			})))
			Expect(err).NotTo(HaveOccurred())
			results := pushResults(resp.Msg)
			// The creation record already carries it, so the PUT is untouched and
			// only the PATCH — an update the hook tried to relabel — is rejected.
			Expect(results[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
			Expect(results[1].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(results[1].GetErrorCode()).To(Equal(server.CodeInternal))
			Expect(results[1].GetErrorMessage()).To(ContainSubstring("changed row_created"))
		})

		It("fails the write it cannot decide", func() {
			boom := errors.New("policy store is down")
			srv := newHistoryServer(server.WithHistoryFilter(
				func(_ context.Context, _ *syncplanev1.FieldHistory, _ server.EntityMeta) (*syncplanev1.FieldHistory, bool, error) {
					return nil, false, boom
				}))

			err := srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Historied{Id: "h1", Title: "a"})
			})
			Expect(err).To(MatchError(ContainSubstring("policy store is down")))
			// The row rolled back with its record: a write must not land
			// unrecorded because the hook could not answer.
			Expect(srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
				found, _, err := tx.Get(&testdatav1.Historied{}, "h1")
				Expect(found).To(BeFalse())
				return err
			})).To(Succeed())
		})

		It("fails the batch it cannot decide rather than acking it away", func() {
			// An error means the hook could not answer, which may be a dependency
			// having a bad minute. Acking would drop the client's write for good
			// over it, so the batch fails as "internal" — retryable — and the
			// client resends once the hook can answer.
			srv := newHistoryServer(server.WithHistoryFilter(
				func(_ context.Context, _ *syncplanev1.FieldHistory, _ server.EntityMeta) (*syncplanev1.FieldHistory, bool, error) {
					return nil, false, errors.New("policy store is down")
				}))
			client := newRPC(srv)

			resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
				putMutation("c1", 1, "historied", "h1", &testdatav1.Historied{Id: "h1", Title: "a"}),
			})))
			Expect(err).NotTo(HaveOccurred(), "the failure is scoped to the workspace, not the request")
			Expect(resp.Msg.GetResults()).To(HaveLen(1))
			batch := resp.Msg.GetResults()[0]
			Expect(batch.GetErrorCode()).To(Equal(server.CodeInternal))
			Expect(batch.GetResults()).To(BeEmpty(), "nothing was acked")
			Expect(batch.GetErrorMessage()).NotTo(ContainSubstring("policy store is down"),
				"the hook's wording is the server's business, not the caller's")

			// Not acked, so the same mutation id is still the one the client owes.
			pull, perr := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
				WorkspaceId: ws, ClientId: "c1",
			}))
			Expect(perr).NotTo(HaveOccurred())
			Expect(pull.Msg.GetLastAckedMutationId()).To(BeZero())
			Expect(pull.Msg.GetPatch().GetChanges()).To(BeEmpty())
		})

		It("acks the write a hook refuses permanently", func() {
			// A hook that fails deterministically on a record's SHAPE — a
			// redaction rule meeting a field type it did not expect — says so with
			// a Rejection, which acks: resending the identical mutation would fail
			// identically, so the queue drains instead of wedging.
			srv := newHistoryServer(server.WithHistoryFilter(
				func(_ context.Context, _ *syncplanev1.FieldHistory, _ server.EntityMeta) (*syncplanev1.FieldHistory, bool, error) {
					return nil, false, server.Reject(server.CodeInvalidArgument, "unexpected field type")
				}))
			client := newRPC(srv)

			resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
				putMutation("c1", 1, "historied", "h1", &testdatav1.Historied{Id: "h1", Title: "a"}),
			})))
			Expect(err).NotTo(HaveOccurred(), "a rejection travels in the response, not as an RPC error")
			results := pushResults(resp.Msg)
			Expect(results[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(results[0].GetErrorCode()).To(Equal(server.CodeInvalidArgument))
			Expect(results[0].GetErrorMessage()).To(ContainSubstring("unexpected field type"),
				"a rejection's message is the hook's own, and does reach the client")
			// Acked, so the watermark moved on and the next mutation id is taken.
			Expect(results[0].GetMutationId()).To(Equal(uint64(1)))
			Expect(historyOf(srv, ws, "historied", "h1")).To(BeEmpty())
		})

		It("refuses to let the hook move a record's identity", func() {
			// The key is what same-sequence merging folds on and what the DELETE
			// cascade purges by, so honouring a move would clobber whatever sits
			// at the new key and orphan this record from the purge.
			srv := newHistoryServer(server.WithHistoryFilter(
				func(_ context.Context, rec *syncplanev1.FieldHistory, _ server.EntityMeta) (*syncplanev1.FieldHistory, bool, error) {
					rec.SourceKey = "somewhere-else"
					return rec, true, nil
				}))
			client := newRPC(srv)

			resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
				putMutation("c1", 1, "historied", "h1", &testdatav1.Historied{Id: "h1", Title: "a"}),
			})))
			Expect(err).NotTo(HaveOccurred())
			results := pushResults(resp.Msg)
			Expect(results[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(results[0].GetErrorMessage()).To(ContainSubstring("moved the record"))

			Expect(historyOf(srv, ws, "historied", "h1")).To(BeEmpty())
			Expect(historyOf(srv, ws, "historied", "somewhere-else")).To(BeEmpty(),
				"and nothing landed under the key it tried to move to")
		})

		It("leaves one record behind when the adapter re-runs the write closure", func() {
			// The hook is contractually pure precisely because this happens.
			calls := 0
			store := &retryingAdapter{Adapter: newTestStore()}
			srv := mustNewServer(store, testEntities(), server.WithHistoryFilter(
				func(_ context.Context, rec *syncplanev1.FieldHistory, _ server.EntityMeta) (*syncplanev1.FieldHistory, bool, error) {
					calls++
					return rec, true, nil
				}))

			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Historied{Id: "h1", Title: "a"})
			})).To(Succeed())

			Expect(calls).To(BeNumerically(">", 1), "the closure was in fact retried")
			Expect(historyOf(srv, ws, "historied", "h1")).To(HaveLen(1))
		})
	})

	Describe("a source key with no room for a record's own key", func() {
		// A history key joins the entity name and the sequence onto the source
		// key, so a source key near the 512-byte limit leaves the record's key
		// no room. Capture stands in for such a key rather than failing the
		// write: history is an observability feature and must not decide whether
		// an application's write succeeds.
		//
		// 505 bytes is a legal row key whose history key would not be. The two
		// here differ only in their last byte, past where a stand-in truncates.
		longKey := strings.Repeat("k", 504) + "1"
		siblingKey := strings.Repeat("k", 504) + "2"

		It("captures for it, and lets the write land", func() {
			srv := newHistoryServer()
			client := newRPC(srv)

			resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
				putMutation("c1", 1, "historied", longKey,
					&testdatav1.Historied{Id: longKey, Title: "a"}),
				patchMutation("c1", 2, "historied", longKey,
					&testdatav1.Historied{Id: longKey, Title: "b"}, "title"),
			})))
			Expect(err).NotTo(HaveOccurred())
			for _, r := range pushResults(resp.Msg) {
				Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
			}

			recs := historyOf(srv, ws, "historied", longKey)
			Expect(recs).To(HaveLen(2))
			Expect(recs[0].GetSourceKey()).To(Equal(longKey),
				"a record carries the source key in full, whatever its own key is")
			Expect(priorOf(recs[0]).GetTitle()).To(Equal("a"))
		})

		It("folds its session into one record", func() {
			// The session probe derives the earlier record's key the same way
			// the capture derived it, or every write in the session opens a
			// record of its own.
			srv := newTestServer()
			editor := server.TrustedAs(ctx, "editor")

			Expect(srv.Mutate(server.TrustedAs(ctx, "typist"), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Historied{Id: longKey, Title: "first"})
			})).To(Succeed())
			for _, title := range []string{"second", "third"} {
				Expect(srv.Mutate(editor, ws, func(tx *server.EntityTx) error {
					return tx.Patch(&testdatav1.Historied{Id: longKey, Title: title}, []string{"title"}, nil)
				})).To(Succeed())
			}

			recs := historyOf(srv, ws, "historied", longKey)
			Expect(recs).To(HaveLen(2), "the creation, and one record for the run that followed")
			Expect(recs[0].GetWrites()).To(BeEquivalentTo(2))
			Expect(priorOf(recs[0]).GetTitle()).To(Equal("first"))
		})

		It("keeps it apart from a key it shares a truncation with", func() {
			// Two rows whose keys differ past the truncation point are still two
			// rows: their records must not merge, and a DELETE of one must not
			// take the other's log.
			srv := newHistoryServer()

			for _, key := range []string{longKey, siblingKey} {
				Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
					return tx.Put(&testdatav1.Historied{Id: key, Title: "a"})
				})).To(Succeed())
				Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
					return tx.Patch(&testdatav1.Historied{Id: key, Title: "b"}, []string{"title"}, nil)
				})).To(Succeed())
			}
			Expect(historyOf(srv, ws, "historied", longKey)).To(HaveLen(2))
			Expect(historyOf(srv, ws, "historied", siblingKey)).To(HaveLen(2))

			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Delete(&testdatav1.Historied{}, longKey)
			})).To(Succeed())

			Expect(historyOf(srv, ws, "historied", longKey)).To(BeEmpty())
			Expect(historyOf(srv, ws, "historied", siblingKey)).To(HaveLen(2),
				"the neighbouring row's log survives")
		})
	})

	Describe("the history entity never syncs", func() {
		It("is absent from bootstrap, pull and resync under full sync", func() {
			// Full sync is the case a subscription set cannot protect against,
			// so it is the one worth pinning: a client that names no
			// subscription still must not see a history row.
			srv := newHistoryServer()
			client := newRPC(srv)

			_, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
				putMutation("c1", 1, "historied", "h1", &testdatav1.Historied{Id: "h1", Title: "a"}),
			})))
			Expect(err).NotTo(HaveOccurred())
			// The record exists — this is about who can see it, not whether it
			// was written.
			Expect(historyOf(srv, ws, "historied", "h1")).To(HaveLen(1))

			stream, err := client.Bootstrap(ctx, connect.NewRequest(&syncplanev1.BootstrapRequest{
				WorkspaceId: ws,
			}))
			Expect(err).NotTo(HaveOccurred())
			var cursor []byte
			for stream.Receive() {
				frame := stream.Msg()
				if begin := frame.GetBegin(); begin != nil {
					Expect(begin.GetEntities()).NotTo(ContainElement("syncplane_field_history"))
					cursor = begin.GetCursor()
				}
				for _, rc := range frame.GetChunk().GetRows() {
					Expect(rc.GetEntity()).NotTo(Equal("syncplane_field_history"))
				}
			}
			Expect(stream.Err()).NotTo(HaveOccurred())

			// A pull from sequence zero: the forward scan sees the stored row
			// and has to skip it rather than tombstone it.
			resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws}))
			Expect(err).NotTo(HaveOccurred())
			for _, rc := range resp.Msg.GetPatch().GetChanges() {
				Expect(rc.GetEntity()).NotTo(Equal("syncplane_field_history"))
			}

			// And a resync, which diffs the old scope against the new one.
			resp, err = client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
				WorkspaceId:   ws,
				Cursor:        cursor,
				Subscriptions: []*syncplanev1.Subscription{{Entity: "historied"}},
			}))
			Expect(err).NotTo(HaveOccurred())
			for _, rc := range resp.Msg.GetPatch().GetChanges() {
				Expect(rc.GetEntity()).NotTo(Equal("syncplane_field_history"))
			}
			Expect(resp.Msg.GetPatch().GetDropEntities()).NotTo(ContainElement("syncplane_field_history"))
		})

		It("refuses a subscription that names it", func() {
			srv := newHistoryServer()
			client := newRPC(srv)

			_, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
				WorkspaceId:   ws,
				Subscriptions: []*syncplanev1.Subscription{{Entity: "syncplane_field_history"}},
			}))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
			Expect(err).To(MatchError(ContainSubstring("server-only")))
		})

		It("rejects a client CRUD that targets it", func() {
			srv := newHistoryServer()
			client := newRPC(srv)

			resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
				putMutation("c1", 1, "syncplane_field_history", "historied/h1/1",
					&syncplanev1.FieldHistory{SourceEntity: "historied", SourceKey: "h1", Seq: 1}),
			})))
			Expect(err).NotTo(HaveOccurred())
			results := pushResults(resp.Msg)
			Expect(results[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(results[0].GetErrorMessage()).To(ContainSubstring("server-only"))
		})

		It("is still reachable through the Query RPC", func() {
			// Queryable, not syncable: the read path resolves the entity by
			// name from the same catalog and must not have been narrowed too.
			srv := newHistoryServer()
			client := newRPC(srv)

			_, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
				putMutation("c1", 1, "historied", "h1", &testdatav1.Historied{Id: "h1", Title: "a"}),
				patchMutation("c1", 2, "historied", "h1",
					&testdatav1.Historied{Id: "h1", Title: "b"}, "title"),
			})))
			Expect(err).NotTo(HaveOccurred())

			resp, err := client.Query(ctx, connect.NewRequest(&syncplanev1.QueryRequest{
				WorkspaceId: ws,
				Entity:      "syncplane_field_history",
				Predicate: query.And(
					query.Eq("source_entity", "historied"),
					query.Eq("source_key", "h1"),
				),
				OrderBy: []*syncplanev1.OrderKey{query.Desc("seq")},
			}))
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Msg.GetRows()).To(HaveLen(2))

			var newest syncplanev1.FieldHistory
			Expect(proto.Unmarshal(resp.Msg.GetRows()[0].GetValue(), &newest)).To(Succeed())
			Expect(newest.GetMask().GetPaths()).To(Equal([]string{"title"}))
		})

		It("serves the history query from its declared index", func() {
			// The row keyspace renders `seq` as unpadded decimal, so it does not
			// sort by sequence — the by_source index is what makes the query
			// O(result) rather than a scan, and index use must not change the
			// answer.
			srv := newHistoryServer()
			client := newRPC(srv)

			// Twelve edits, so the sequences run past 9 — which is exactly where
			// an unpadded decimal key order and a sequence order disagree.
			muts := []*syncplanev1.Mutation{
				putMutation("c1", 1, "historied", "h1", &testdatav1.Historied{Id: "h1", Title: "v0"}),
			}
			for i := 1; i <= 12; i++ {
				muts = append(muts, patchMutation("c1", uint64(i+1), "historied", "h1",
					&testdatav1.Historied{Id: "h1", Title: fmt.Sprintf("v%d", i)}, "title"))
			}
			for _, m := range muts {
				// One push per mutation: each has to take its own sequence, or
				// they would all share one history key.
				_, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{m})))
				Expect(err).NotTo(HaveOccurred())
			}
			Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())

			recs := historyOf(srv, ws, "historied", "h1")
			Expect(recs).To(HaveLen(13))
			for i := 1; i < len(recs); i++ {
				Expect(recs[i-1].GetSeq()).To(BeNumerically(">", recs[i].GetSeq()))
			}
			Expect(recs[0].GetSeq()).To(BeNumerically(">", 9), "past where the keyspace stops agreeing")
		})
	})

	Describe("a record is read under its source row's permissions", func() {
		// Query is the only door a client has to a server-only entity, so it is
		// the only gate there is. Applying the HISTORY entity's own read filter
		// would be no gate at all: an app writes filters for the entities it
		// declared, and the engine registered this one without being asked, so
		// its filter is the default — allow all.
		var (
			perms  *historyReadPerms
			srv    *server.Server
			client syncplanev1connect.SyncServiceClient
		)

		// queryHistory runs the query a history panel runs, over the wire, as an
		// ordinary client rather than through the trusted in-process view.
		queryHistory := func(key string) []*syncplanev1.RowChange {
			GinkgoHelper()
			resp, err := client.Query(ctx, connect.NewRequest(&syncplanev1.QueryRequest{
				WorkspaceId: ws,
				Entity:      "syncplane_field_history",
				Predicate: query.And(
					query.Eq("source_entity", "historied"),
					query.Eq("source_key", key),
				),
				OrderBy: []*syncplanev1.OrderKey{query.Desc("seq")},
			}))
			Expect(err).NotTo(HaveOccurred())
			return resp.Msg.GetRows()
		}

		BeforeEach(func() {
			perms = &historyReadPerms{hidden: "classified"}
			srv = newHistoryServer(server.WithPermissions(perms))
			client = newRPC(srv)

			// h1 ends up hidden, h2 stays readable. Both have a create record
			// and an edit record, and both edits move a title the filter tests.
			_, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
				putMutation("c1", 1, "historied", "h1", &testdatav1.Historied{Id: "h1", Title: "public"}),
				patchMutation("c1", 2, "historied", "h1",
					&testdatav1.Historied{Id: "h1", Title: "classified"}, "title"),
				putMutation("c1", 3, "historied", "h2", &testdatav1.Historied{Id: "h2", Title: "fine"}),
				patchMutation("c1", 4, "historied", "h2",
					&testdatav1.Historied{Id: "h2", Title: "also fine"}, "title"),
			})))
			Expect(err).NotTo(HaveOccurred())
		})

		It("withholds every record of a row the caller may not read", func() {
			Expect(queryHistory("h1")).To(BeEmpty())
			// Including the create record, which carries no prior value: what
			// leaks otherwise is not only the values but the row's existence,
			// its edit history and who made each edit.
			Expect(historyOf(srv, ws, "historied", "h1")).To(HaveLen(2),
				"the records are there — trusted server-side code still sees them")
		})

		It("serves the records of a row the caller may read", func() {
			rows := queryHistory("h2")
			Expect(rows).To(HaveLen(2))
			var newest syncplanev1.FieldHistory
			Expect(proto.Unmarshal(rows[0].GetValue(), &newest)).To(Succeed())
			Expect(newest.GetMask().GetPaths()).To(Equal([]string{"title"}))
			Expect(priorOf(&newest).GetTitle()).To(Equal("fine"))
		})

		It("resolves the source entity's filter before the snapshot opens", func() {
			// The Permissions contract: a read filter is a hook, and a hook must
			// not run while a read transaction is pinned.
			perms.asked = nil
			Expect(queryHistory("h2")).To(HaveLen(2))
			Expect(perms.asked).To(ContainElement("historied"))
		})

		It("authorizes a page of one row's records against a single source read", func() {
			// The query a panel runs is N records naming the SAME source row, so
			// a verdict reached per record is a read and a decode per record
			// inside the read snapshot — for one answer.
			for i := range 3 {
				_, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
					patchMutation("c1", uint64(5+i), "historied", "h2",
						&testdatav1.Historied{Id: "h2", Title: fmt.Sprintf("edit %d", i)}, "title"),
				})))
				Expect(err).NotTo(HaveOccurred())
			}
			perms.evaluated = 0
			Expect(queryHistory("h2")).To(HaveLen(5))
			// One verdict per distinct source row the scan reaches, never one
			// per record: seven records here, over h2 and the h1 rows the scan
			// also authorizes (a record is authorized before the predicate
			// narrows it), and the filter runs twice. The bound rather than an
			// equality because how many records the scan reaches is the
			// planner's business.
			Expect(perms.evaluated).To(BeNumerically("<=", 2))
		})

		It("withholds a record whose source entity has left the catalog", func() {
			// Records outlive registrations — a deploy that stops declaring an
			// entity leaves its rows and its history behind. Nothing left can
			// decode the source row or say what a filter over it means, so there
			// is no way to establish that the caller may read it.
			plantHistory(srv, &syncplanev1.FieldHistory{
				SourceEntity: "retired",
				SourceKey:    "r1",
				Seq:          1,
			})
			resp, err := client.Query(ctx, connect.NewRequest(&syncplanev1.QueryRequest{
				WorkspaceId: ws,
				Entity:      "syncplane_field_history",
				Predicate:   query.Eq("source_entity", "retired"),
			}))
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Msg.GetRows()).To(BeEmpty())
		})
	})

	Describe("retention", func() {
		It("hard-deletes expired records without leaving a tombstone", func() {
			store := newTestStore()
			srv := mustNewServer(store, testEntities(),
				server.WithGC(time.Hour, 0),
				server.WithHistoryRetention(time.Millisecond))

			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Historied{Id: "h1", Title: "a"})
			})).To(Succeed())
			Expect(historyOf(srv, ws, "historied", "h1")).To(HaveLen(1))

			time.Sleep(5 * time.Millisecond)
			Expect(srv.CollectGarbage(ctx, ws)).To(Succeed())

			Expect(historyOf(srv, ws, "historied", "h1")).To(BeEmpty())
			// A tombstone would both survive here and invalidate every cursor
			// below it; expiring a large history must do neither.
			Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
				var seen []string
				if err := tx.ScanChanges(0, "", "", 0, func(r storage.Row) error {
					seen = append(seen, r.Entity)
					return nil
				}); err != nil {
					return err
				}
				Expect(seen).NotTo(ContainElement("syncplane_field_history"))
				horizon, err := tx.TombstoneHorizon()
				Expect(horizon).To(BeZero())
				return err
			})).To(Succeed())

			// The source row is untouched: retention expires the log, not the data.
			Expect(srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
				found, _, err := tx.Get(&testdatav1.Historied{}, "h1")
				Expect(found).To(BeTrue())
				return err
			})).To(Succeed())
		})

		It("expires nothing on a clock under HistoryForever", func() {
			srv := mustNewServer(newTestStore(), testEntities(),
				server.WithGC(time.Millisecond, 0),
				server.WithHistoryRetention(server.HistoryForever))

			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Historied{Id: "h1", Title: "a"})
			})).To(Succeed())

			time.Sleep(5 * time.Millisecond)
			Expect(srv.CollectGarbage(ctx, ws)).To(Succeed())
			// A retention shorter than the record's own age, and the record
			// survives anyway: only the row's deletion ends it.
			Expect(historyOf(srv, ws, "historied", "h1")).To(HaveLen(1))
		})

		It("takes the default rather than forever when nothing configured it", func() {
			// Zero and HistoryForever must stay different answers: "I never set
			// this" cannot silently opt an operator into unbounded growth.
			srv := mustNewServer(newTestStore(), testEntities(), server.WithGC(time.Hour, 0))

			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Historied{Id: "h1", Title: "a"})
			})).To(Succeed())
			Expect(srv.CollectGarbage(ctx, ws)).To(Succeed())
			// Kept, because the default is 90 days rather than a millisecond —
			// this pins that the pass ran and chose to keep it.
			Expect(historyOf(srv, ws, "historied", "h1")).To(HaveLen(1))
		})

		It("expires records the last entity to opt in left behind", func() {
			// Records are durable state; catalog membership is not. A deploy that
			// drops the last history: {} takes the history entity out of the
			// catalog, and with it every path that would otherwise reach those
			// records: nothing writes them, nothing reads them, and a DELETE of
			// the source row does not purge them. The age sweep is the only thing
			// left, so it must not be gated on the catalog too — or the records
			// are immortal, waiting to be inherited by whatever opts back in.
			store := newTestStore()
			srv := mustNewServer(store, testEntities())
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Historied{Id: "h1", Title: "a"})
			})).To(Succeed())
			Expect(historyOf(srv, ws, "historied", "h1")).To(HaveLen(1))

			// The next deploy, on the same store: a catalog where nothing opts in.
			next := mustNewServer(store,
				server.WithEntities(&testdatav1.Widget{}),
				server.WithGC(time.Hour, 0),
				server.WithHistoryRetention(time.Millisecond))

			time.Sleep(5 * time.Millisecond)
			Expect(next.CollectGarbage(ctx, ws)).To(Succeed())

			// Read back through the catalog that still knows the entity — the one
			// collecting cannot see what it swept.
			Expect(historyOf(srv, ws, "historied", "h1")).To(BeEmpty())
		})
	})

	Describe("a deleted row takes its history with it", func() {
		DescribeTable("on every delete path",
			func(del func(srv *server.Server, client syncplanev1connect.SyncServiceClient)) {
				srv := newHistoryServer()
				client := newRPC(srv)

				_, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
					putMutation("c1", 1, "historied", "h1", &testdatav1.Historied{Id: "h1", Title: "a"}),
					patchMutation("c1", 2, "historied", "h1",
						&testdatav1.Historied{Id: "h1", Title: "b"}, "title"),
					// A second row whose key SHARES a prefix with the first, so
					// a purge that bounded the key range loosely would take it
					// too. "h1" against "h10" is exactly that shape.
					putMutation("c1", 3, "historied", "h10", &testdatav1.Historied{Id: "h10", Title: "a"}),
				})))
				Expect(err).NotTo(HaveOccurred())
				Expect(historyOf(srv, ws, "historied", "h1")).To(HaveLen(2))

				del(srv, client)

				Expect(historyOf(srv, ws, "historied", "h1")).To(BeEmpty())
				Expect(historyOf(srv, ws, "historied", "h10")).To(HaveLen(1),
					"a neighbouring key must survive the purge")
			},
			Entry("the wire DELETE", func(_ *server.Server, client syncplanev1connect.SyncServiceClient) {
				GinkgoHelper()
				resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
					deleteMutation("c1", 4, "historied", "h1"),
				})))
				Expect(err).NotTo(HaveOccurred())
				Expect(pushResults(resp.Msg)[0].GetStatus()).
					To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
			}),
			Entry("EntityTx.Delete", func(srv *server.Server, _ syncplanev1connect.SyncServiceClient) {
				GinkgoHelper()
				Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
					return tx.Delete(&testdatav1.Historied{}, "h1")
				})).To(Succeed())
			}),
		)

		It("purges records left behind by an entity that has stopped opting in", func() {
			// Registration is additive, so an entity that drops history: {} in a
			// later deploy keeps everything already captured. Gating the purge on
			// the entity's CURRENT option would strand exactly those records:
			// never purged on delete, and — under HistoryForever, or once no
			// entity opts in at all — never expired either. Then the next row to
			// claim the key inherits a dead row's log.
			srv := newHistoryServer()

			plantHistory(srv, &syncplanev1.FieldHistory{
				SourceEntity: "widget", // no history: {} on this one
				SourceKey:    "w1",
				Seq:          1,
			})
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Widget{Id: "w1", Label: "a"})
			})).To(Succeed())
			Expect(historyOf(srv, ws, "widget", "w1")).To(HaveLen(1))

			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Delete(&testdatav1.Widget{}, "w1")
			})).To(Succeed())

			Expect(historyOf(srv, ws, "widget", "w1")).To(BeEmpty())
		})

		It("purges under HistoryForever too — that is the whole lifecycle", func() {
			srv := mustNewServer(newTestStore(), testEntities(),
				server.WithGC(time.Hour, 0),
				server.WithHistoryRetention(server.HistoryForever))

			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Historied{Id: "h1", Title: "a"})
			})).To(Succeed())
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Delete(&testdatav1.Historied{}, "h1")
			})).To(Succeed())

			Expect(historyOf(srv, ws, "historied", "h1")).To(BeEmpty())
		})

		It("leaves a re-created row a clean log", func() {
			// Keys are app-chosen, so re-creating one is ordinary — and
			// inheriting the previous row's history would offer a restore to a
			// value that belonged to something else.
			srv := newHistoryServer()

			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Historied{Id: "h1", Title: "old life"})
			})).To(Succeed())
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Patch(&testdatav1.Historied{Id: "h1", Title: "still old"}, []string{"title"}, nil)
			})).To(Succeed())
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Delete(&testdatav1.Historied{}, "h1")
			})).To(Succeed())
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Historied{Id: "h1", Title: "new life"})
			})).To(Succeed())

			recs := historyOf(srv, ws, "historied", "h1")
			Expect(recs).To(HaveLen(1))
			Expect(recs[0].GetRowCreated()).To(BeTrue(), "the re-create is a create")
		})

		It("rolls the purge back when the mutation carrying the delete rolls back", func() {
			// The records must not outlive the row — and must not predecease it
			// either. A batch is atomic across its sub-mutations, so a later one
			// that rejects takes the delete AND the purge back with it, which is
			// the only ordering that keeps the two from drifting apart.
			srv := newHistoryServer()
			client := newRPC(srv)

			_, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
				putMutation("c1", 1, "historied", "h1", &testdatav1.Historied{Id: "h1", Title: "a"}),
			})))
			Expect(err).NotTo(HaveOccurred())

			resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
				batchMutation("c1", 2,
					crudDelete("historied", "h1"),
					// PATCH is not an upsert: a missing row is a conflict, so
					// this rejects and unwinds the sub-mutation before it.
					crudPatch("historied", "gone", &testdatav1.Historied{Id: "gone", Title: "x"}, "title"),
				),
			})))
			Expect(err).NotTo(HaveOccurred())
			Expect(pushResults(resp.Msg)[0].GetStatus()).
				To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))

			Expect(srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
				found, _, err := tx.Get(&testdatav1.Historied{}, "h1")
				Expect(found).To(BeTrue(), "the delete rolled back")
				return err
			})).To(Succeed())
			Expect(historyOf(srv, ws, "historied", "h1")).To(HaveLen(1), "and so did the purge")
		})
	})
})
