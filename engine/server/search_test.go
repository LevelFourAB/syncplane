package server_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

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
)

var _ = Describe("Search", func() {
	const ws = "ws-search"

	Describe("registration", func() {
		It("rejects a search field over a repeated field", func() {
			err := newTestServerErr(server.WithEntities(&testdatav1.SearchRepeatedEntity{}))
			Expect(err).To(MatchError(ContainSubstring("search field")))
		})

		It("rejects a searchable mark on a numeric field", func() {
			err := newTestServerErr(server.WithEntities(&testdatav1.SearchNumberEntity{}))
			Expect(err).To(MatchError(ContainSubstring("not searchable")))
		})

		It("rejects a search declaration naming no fields", func() {
			err := newTestServerErr(server.WithEntities(&testdatav1.SearchEmptyEntity{}))
			Expect(err).To(MatchError(ContainSubstring("names no fields")))
		})
	})

	Describe("queries", func() {
		var (
			ctx    context.Context
			srv    *server.Server
			store  storage.Adapter
			client syncplanev1connect.SyncServiceClient
		)

		putArticle := func(id uint64, a *testdatav1.Article) *syncplanev1.Mutation {
			return putMutation("c1", id, "article", a.GetId(), a)
		}

		BeforeEach(func() {
			ctx = context.Background()
			srv, store = newTestServerStore(server.WithEntities(&testdatav1.Article{}, &testdatav1.Glossary{}))
			path, handler := srv.Handler()
			mux := http.NewServeMux()
			mux.Handle(path, handler)
			ts := httptest.NewServer(mux)
			DeferCleanup(ts.Close)
			client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)

			_, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
				putArticle(1, &testdatav1.Article{Id: "a1", Title: "Milk cow"}),
				putArticle(2, &testdatav1.Article{Id: "a2", Summary: "milk"}),
				putArticle(3, &testdatav1.Article{Id: "a3", Title: "water"}),
				putArticle(4, &testdatav1.Article{Id: "a4", Meta: &testdatav1.ArticleMeta{Author: "milk"}}),
			})))
			Expect(err).NotTo(HaveOccurred())
		})

		match := func(q string) *syncplanev1.Condition {
			return query.Match(q)
		}

		runQuery := func(req *syncplanev1.QueryRequest) *syncplanev1.QueryResponse {
			GinkgoHelper()
			req.WorkspaceId = ws
			resp, err := client.Query(ctx, connect.NewRequest(req))
			Expect(err).NotTo(HaveOccurred())
			return resp.Msg
		}

		keysOf := func(resp *syncplanev1.QueryResponse) []string {
			keys := make([]string, 0, len(resp.GetRows()))
			for _, rc := range resp.GetRows() {
				keys = append(keys, rc.GetKey())
			}
			return keys
		}

		It("filters by match and orders by relevance over the RPC", func() {
			resp := runQuery(&syncplanev1.QueryRequest{
				Entity:    "article",
				Predicate: match("milk"),
				OrderBy:   []*syncplanev1.OrderKey{query.Relevance()},
			})
			Expect(keysOf(resp)).To(Equal([]string{"a1", "a2", "a4"}))
		})

		It("pages a relevance ordering with cursors", func() {
			first := runQuery(&syncplanev1.QueryRequest{
				Entity:    "article",
				Predicate: match("milk"),
				OrderBy:   []*syncplanev1.OrderKey{query.Relevance()},
				Limit:     2,
			})
			Expect(keysOf(first)).To(Equal([]string{"a1", "a2"}))
			Expect(first.GetHasMore()).To(BeTrue())

			rest := runQuery(&syncplanev1.QueryRequest{
				Entity:    "article",
				Predicate: match("milk"),
				OrderBy:   []*syncplanev1.OrderKey{query.Relevance()},
				After:     first.GetEndCursor(),
			})
			Expect(keysOf(rest)).To(Equal([]string{"a4"}))
			Expect(rest.GetHasMore()).To(BeFalse())
		})

		It("rejects a relevance ordering without a match condition", func() {
			_, err := client.Query(ctx, connect.NewRequest(&syncplanev1.QueryRequest{
				WorkspaceId: ws,
				Entity:      "article",
				OrderBy:     []*syncplanev1.OrderKey{query.Relevance()},
			}))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
		})

		It("rejects a match on an entity without search fields", func() {
			_, err := client.Query(ctx, connect.NewRequest(&syncplanev1.QueryRequest{
				WorkspaceId: ws,
				Entity:      "widget",
				Predicate:   match("milk"),
			}))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
		})

		// exact is the posting range that holds key alone.
		exact := func(key string) storage.IndexRange {
			return storage.IndexRange{Lo: []byte(key), Hi: []byte(key + "\x00")}
		}
		postingRows := func(r storage.IndexRange) []string {
			GinkgoHelper()
			rows := []string{}
			Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
				return tx.ScanPostings("article", "__search", nil, []storage.IndexRange{r}, 0, func(rowKey string) error {
					rows = append(rows, rowKey)
					return nil
				})
			})).To(Succeed())
			return rows
		}
		postingCount := func(r storage.IndexRange) int {
			GinkgoHelper()
			var n []int
			Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
				var err error
				n, _, err = tx.PostingCounts("article", "__search", []storage.IndexRange{r}, 0)
				return err
			})).To(Succeed())
			return n[0]
		}

		It("maintains term postings on the write path", func() {
			// a1 "Milk cow" → milk, cow; a2 → milk; a3 → water; a4 → milk.
			Expect(postingCount(storage.IndexRange{})).To(Equal(5))
			Expect(postingCount(storage.IndexRange{Lo: []byte{0x01}, Hi: []byte{0x02}})).To(Equal(5),
				"term keys carry the term tag")
			Expect(postingRows(exact("\x01cow\x00\x00"))).To(ConsistOf("a1"))
			Expect(postingRows(exact("\x01water\x00\x00"))).To(ConsistOf("a3"))
		})

		It("stores one posting key per term, shared by the rows that hold it", func() {
			milk := exact("\x01milk\x00\x00")
			Expect(postingCount(milk)).To(Equal(3))
			Expect(postingRows(milk)).To(ConsistOf("a1", "a2", "a4"))

			Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
			Expect(keysOf(runQuery(&syncplanev1.QueryRequest{
				Entity:    "article",
				Predicate: match("milk"),
			}))).To(ConsistOf("a1", "a2", "a4"))
		})

		It("finds every overflow row from the shared overflow marker", func() {
			oversized := func(tag string) string {
				var b strings.Builder
				for i := 0; i < query.SearchTermsPerRow+40; i++ {
					fmt.Fprintf(&b, "%s%04d ", tag, i)
				}
				b.WriteString("needle")
				return b.String()
			}
			resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
				putArticle(5, &testdatav1.Article{Id: "big1", Summary: oversized("first")}),
				putArticle(6, &testdatav1.Article{Id: "big2", Summary: oversized("second")}),
				putArticle(7, &testdatav1.Article{Id: "small", Summary: "needle"}),
			})))
			Expect(err).NotTo(HaveOccurred())
			for _, r := range pushResults(resp.Msg) {
				Expect(r.GetErrorCode()).To(BeEmpty(), r.GetErrorMessage())
			}

			Expect(postingRows(storage.IndexRange{Lo: []byte{0x02}, Hi: []byte{0x03}})).To(ConsistOf("big1", "big2"))
			Expect(postingCount(exact("\x02"))).To(Equal(2), "the overflow marker is the tag alone")

			Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
			// "needle" is past the cap in both oversized rows, so only the
			// overflow union finds them; "small" holds a posting for it.
			Expect(keysOf(runQuery(&syncplanev1.QueryRequest{
				Entity:    "article",
				Predicate: match("needle"),
			}))).To(ConsistOf("big1", "big2", "small"))
			// The overflow rows are candidates of every probe, and re-checking
			// them against the predicate drops the ones that do not match.
			Expect(keysOf(runQuery(&syncplanev1.QueryRequest{
				Entity:    "article",
				Predicate: match("milk"),
			}))).To(ConsistOf("a1", "a2", "a4"))
			Expect(keysOf(runQuery(&syncplanev1.QueryRequest{
				Entity:    "article",
				Predicate: match("second0007"),
			}))).To(ConsistOf("big2"))
		})

		It("answers from the postings index once built, identically to the scan", func() {
			scan := runQuery(&syncplanev1.QueryRequest{
				Entity:    "article",
				Predicate: match("milk cow"),
				OrderBy:   []*syncplanev1.OrderKey{query.Relevance()},
			})
			Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
			indexed := runQuery(&syncplanev1.QueryRequest{
				Entity:    "article",
				Predicate: match("milk cow"),
				OrderBy:   []*syncplanev1.OrderKey{query.Relevance()},
			})
			Expect(keysOf(indexed)).To(Equal(keysOf(scan)))
			Expect(keysOf(indexed)).To(Equal([]string{"a1"}))

			// Prefix probing: the final term is a range, not a point.
			prefix := runQuery(&syncplanev1.QueryRequest{
				Entity:    "article",
				Predicate: match("co"),
			})
			Expect(keysOf(prefix)).To(Equal([]string{"a1"}))
		})

		It("answers a mix of common and rare terms from the rare one", func() {
			// The walk is bounded by its smallest term and skips reading
			// a posting list too wide to be worth narrowing with, so the terms
			// here differ by two orders of magnitude in how many rows carry
			// them — and the common one comes FIRST, where a walk that simply
			// seeded from the leading term would pick it.
			var muts []*syncplanev1.Mutation
			for i := 0; i < 100; i++ {
				// Ids continue the fixture's, without a gap: the watermark
				// rejects a client that skips one.
				muts = append(muts, putArticle(uint64(5+i), &testdatav1.Article{
					Id:      fmt.Sprintf("bulk%03d", i),
					Summary: "common filler",
				}))
			}
			muts = append(muts, putArticle(105, &testdatav1.Article{
				Id: "hit", Summary: "common needle",
			}))
			resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, muts)))
			Expect(err).NotTo(HaveOccurred())
			for _, r := range pushResults(resp.Msg) {
				Expect(r.GetErrorCode()).To(BeEmpty(), r.GetErrorMessage())
			}

			ask := func() []string {
				return keysOf(runQuery(&syncplanev1.QueryRequest{
					Entity:    "article",
					Predicate: match("common needle "),
				}))
			}
			scan := ask()
			Expect(scan).To(Equal([]string{"hit"}))
			Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
			Expect(ask()).To(Equal(scan))
		})

		It("stores the overflow marker for an oversized document and still matches it", func() {
			var b strings.Builder
			for i := 0; i < query.SearchTermsPerRow+40; i++ {
				fmt.Fprintf(&b, "unique%04d ", i)
			}
			b.WriteString("needle")
			resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
				putArticle(5, &testdatav1.Article{Id: "big", Summary: b.String()}),
			})))
			Expect(err).NotTo(HaveOccurred())
			for _, r := range pushResults(resp.Msg) {
				Expect(r.GetErrorCode()).To(BeEmpty(), r.GetErrorMessage())
			}

			Expect(postingRows(exact("\x02"))).To(Equal([]string{"big"}))
			Expect(postingRows(storage.IndexRange{})).To(ContainElement("big"))
			Expect(postingCount(storage.IndexRange{})).To(Equal(5+1), "an oversized row stores the overflow marker alone")

			Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
			// "needle" is past the cap, so no posting names it — the overflow
			// union is what keeps the row findable.
			got := runQuery(&syncplanev1.QueryRequest{
				Entity:    "article",
				Predicate: match("needle"),
			})
			Expect(keysOf(got)).To(Equal([]string{"big"}))
		})

		It("serves match and relevance through the in-process view", func() {
			var keys []string
			err := srv.View(ctx, ws, func(tx *server.ViewTx) error {
				return tx.Query(&testdatav1.Article{}, query.Spec{
					Where:   query.Match("milk"),
					OrderBy: []*syncplanev1.OrderKey{query.Relevance()},
				}, func(msg proto.Message, version uint64) error {
					keys = append(keys, msg.(*testdatav1.Article).GetId())
					return nil
				})
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(Equal([]string{"a1", "a2", "a4"}))
		})
	})
})
