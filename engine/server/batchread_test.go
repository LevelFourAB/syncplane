package server_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
)

// Every read that resolves keys discovered somewhere else — an index walk's
// candidates, a repeated forward ref, the members a cursor's set lost — goes
// through one batch read rather than a point read per key. On sqlitestore that
// is invisible: the rows come back the same and the point read was already
// in-process. It is only visible as COST, and only on an adapter where a point
// read is a network round trip, so these specs count reads at the storage seam
// instead of asserting on responses.
//
// The counts are relational on purpose. Pinning the batch size would pin an
// implementation detail; what must hold is that reads grow with batches and
// not with rows.
var _ = Describe("Batched row reads", func() {
	var (
		ctx     context.Context
		counter *readCountingAdapter
	)

	BeforeEach(func() {
		ctx = context.Background()
		counter = &readCountingAdapter{Adapter: newTestStore(), counts: map[string]int{}}
	})

	// serveCounting starts an RPC server over the counting store.
	serveCounting := func(opts ...server.Option) (*server.Server, syncplanev1connect.SyncServiceClient) {
		GinkgoHelper()
		srv := mustNewServer(counter, append([]server.Option{testEntities()}, opts...)...)
		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		return srv, syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	}

	idEq := func(id string) *syncplanev1.Condition {
		return &syncplanev1.Condition{Kind: &syncplanev1.Condition_Cmp{Cmp: &syncplanev1.CmpCond{
			Path:  []string{"id"},
			Op:    syncplanev1.CmpOp_CMP_OP_EQ,
			Value: &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_StringValue{StringValue: id}},
		}}}
	}

	// resolvedPerRead is how many rows one read at the seam brought back. It is
	// 1 for a point read per key and the batch size for a batched one, which is
	// the whole difference these specs are about.
	resolvedPerRead := func() float64 {
		reads := counter.count("batch") + counter.count("get")
		Expect(reads).To(BeNumerically(">", 0), "something must have read the rows")
		return float64(counter.count("batchkeys")+counter.count("get")) / float64(reads)
	}

	Describe("an index-served query", func() {
		const issues = 300
		var srv *server.Server

		BeforeEach(func() {
			srv, _ = serveCounting()
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				for i := 0; i < issues; i++ {
					if err := tx.Put(&testdatav1.Issue{
						Id:    fmt.Sprintf("i%04d", i),
						Title: fmt.Sprintf("issue %d", i),
						State: "open",
						Rank:  int32(i),
					}); err != nil {
						return err
					}
				}
				return nil
			})).To(Succeed())
			Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
			// Seeding and the build pass read too; only the query is under test.
			counter.reset()
		})

		runQuery := func(spec query.Spec) []string {
			GinkgoHelper()
			var keys []string
			Expect(srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
				return tx.Query(&testdatav1.Issue{}, spec, func(msg proto.Message, _ uint64) error {
					keys = append(keys, msg.(*testdatav1.Issue).GetId())
					return nil
				})
			})).To(Succeed())
			return keys
		}

		It("resolves the whole candidate set in a handful of reads", func() {
			Expect(runQuery(query.Spec{Where: query.Eq("state", "open")})).To(HaveLen(issues))

			Expect(counter.count("scan")).To(BeZero(), "the index served it")
			Expect(counter.count("get")).To(BeZero(), "no candidate is resolved on its own")
			Expect(counter.count("batchkeys")).To(Equal(issues), "every candidate is still resolved")
			Expect(resolvedPerRead()).To(BeNumerically(">", 10),
				"a read per candidate is a round trip per candidate on a networked adapter")
		})

		It("resolves no more than an ordered page can still hold", func() {
			// The other half of the trade: reading ahead by a whole batch would
			// make a limit-2 page fetch hundreds of rows to answer with two. An
			// ordered walk stops when the page is full, so it may only read
			// ahead by what the page still has room for.
			Expect(runQuery(query.Spec{
				Where:   query.Eq("state", "open"),
				OrderBy: []*syncplanev1.OrderKey{query.Desc("rank")},
				Limit:   2,
			})).To(Equal([]string{"i0299", "i0298"}))

			// The page (2), the has_more probe, and the boundary entries the
			// seek admits for the exact comparison to reject.
			Expect(counter.count("batchkeys")).To(BeNumerically("<=", 5))
		})
	})

	It("resolves a reverse follow hop in batches", func() {
		// One project with a wide fan-out of issues: the reverse hop probes the
		// ref index for the issues pointing back, then has to re-read each of
		// them against this snapshot.
		const fanOut = 300
		srv, client := serveCounting()
		Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
			if err := tx.Put(&testdatav1.Project{Id: "p1", Name: "One"}); err != nil {
				return err
			}
			for i := 0; i < fanOut; i++ {
				if err := tx.Put(&testdatav1.Issue{
					Id:      fmt.Sprintf("i%04d", i),
					Title:   "t",
					Project: &testdatav1.Project{Id: "p1"},
				}); err != nil {
					return err
				}
			}
			return nil
		})).To(Succeed())
		Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())

		counter.reset()
		resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId: ws,
			Subscriptions: []*syncplanev1.Subscription{{
				Entity: "project",
				Follow: &syncplanev1.FollowSpec{Paths: []*syncplanev1.RefPath{{Segments: []string{"issuesByProject"}}}},
			}},
		}))
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Msg.GetPatch().GetChanges()).To(HaveLen(fanOut + 1))

		Expect(counter.count("probe")).To(BeNumerically(">", 0), "the ref index served the hop")
		Expect(counter.count("batchkeys")).To(BeNumerically(">=", fanOut))
		Expect(resolvedPerRead()).To(BeNumerically(">", 10))
	})

	It("resolves a repeated forward ref in one batch", func() {
		// The stubs name their targets outright, so every key is known before
		// any of them is read — there is no reason to pay a read per element.
		const deps = 100
		srv, client := serveCounting()
		Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
			holder := &testdatav1.Issue{Id: "i0", Title: "holder"}
			for i := 1; i <= deps; i++ {
				id := fmt.Sprintf("d%04d", i)
				if err := tx.Put(&testdatav1.Issue{Id: id, Title: id}); err != nil {
					return err
				}
				holder.BlockedBy = append(holder.BlockedBy, &testdatav1.Issue{Id: id})
			}
			return tx.Put(holder)
		})).To(Succeed())

		counter.reset()
		resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId: ws,
			Subscriptions: []*syncplanev1.Subscription{{
				Entity:    "issue",
				Predicate: idEq("i0"),
				Follow: &syncplanev1.FollowSpec{
					Paths: []*syncplanev1.RefPath{{Segments: []string{"blockedBy"}}},
					Depth: 1,
				},
			}},
		}))
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Msg.GetPatch().GetChanges()).To(HaveLen(deps + 1))

		Expect(counter.count("batchkeys")).To(BeNumerically(">=", deps))
		Expect(resolvedPerRead()).To(BeNumerically(">", 10))
	})

	It("resolves the members a window dropped in batches", func() {
		// A window that turns over completely leaves as many departed members
		// as it is wide, and each one is re-read to decide whether it drops as
		// a tombstone, is hidden, or is still held by base scope.
		const width = 200
		widgets := func(from, to int) []*syncplanev1.Mutation {
			var muts []*syncplanev1.Mutation
			for i := from; i < to; i++ {
				muts = append(muts, putMutation("c1", uint64(i+1), "widget",
					fmt.Sprintf("w%04d", i),
					&testdatav1.Widget{Id: fmt.Sprintf("w%04d", i), Label: "w"}))
			}
			return muts
		}
		_, client := serveCounting()
		_, err := client.Push(ctx, connect.NewRequest(pushOf(ws, widgets(0, width))))
		Expect(err).NotTo(HaveOccurred())

		first, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId:   ws,
			Subscriptions: []*syncplanev1.Subscription{recencySub(width)},
		}))
		Expect(err).NotTo(HaveOccurred())
		Expect(first.Msg.GetPatch().GetChanges()).To(HaveLen(width))

		// Push a whole window's worth of newer rows, so every member of the
		// first window is displaced.
		_, err = client.Push(ctx, connect.NewRequest(pushOf(ws, widgets(width, 2*width))))
		Expect(err).NotTo(HaveOccurred())

		counter.reset()
		second, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId:   ws,
			Cursor:        first.Msg.GetNextCursor(),
			Subscriptions: []*syncplanev1.Subscription{recencySub(width)},
		}))
		Expect(err).NotTo(HaveOccurred())

		var dropped int
		for _, rc := range second.Msg.GetPatch().GetChanges() {
			if rc.GetTombstone() {
				dropped++
			}
		}
		Expect(dropped).To(Equal(width), "the whole first window left")
		Expect(counter.count("batchkeys")).To(BeNumerically(">=", width))
		Expect(resolvedPerRead()).To(BeNumerically(">", 10))
	})
})
