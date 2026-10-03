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

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	syncplanecursorv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/cursor/v1"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/internal/exampleapp"
)

// scopes builds a one-workspace-per-id scope set with no cursor and no
// subscription filter — full sync from the beginning, the shape most specs
// want.
func scopes(ids ...string) []*syncplanev1.WorkspaceScope {
	out := make([]*syncplanev1.WorkspaceScope, 0, len(ids))
	for _, id := range ids {
		out = append(out, &syncplanev1.WorkspaceScope{WorkspaceId: id})
	}
	return out
}

var _ = Describe("Subscribe", func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		client syncplanev1connect.SyncServiceClient
	)

	start := func(srv *server.Server) syncplanev1connect.SyncServiceClient {
		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		// Cancel the context (closing any open Subscribe stream) BEFORE closing
		// the httptest server, or ts.Close blocks waiting for the live stream.
		DeferCleanup(func() {
			cancel()
			ts.Close()
		})
		return syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	}

	pushTo := func(workspace string, mutations ...*syncplanev1.Mutation) {
		GinkgoHelper()
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(workspace, mutations)))
		Expect(err).NotTo(HaveOccurred())
		for _, r := range pushResults(resp.Msg) {
			Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
		}
	}

	push := func(mutations ...*syncplanev1.Mutation) {
		GinkgoHelper()
		pushTo(ws, mutations...)
	}

	// subscribe opens a stream that the surrounding context cancels on cleanup.
	subscribe := func(req *syncplanev1.SubscribeRequest) *connect.ServerStreamForClient[syncplanev1.SubscribeResponse] {
		GinkgoHelper()
		stream, err := client.Subscribe(ctx, connect.NewRequest(req))
		Expect(err).NotTo(HaveOccurred())
		return stream
	}

	// recv blocks for the next stream frame.
	recv := func(stream *connect.ServerStreamForClient[syncplanev1.SubscribeResponse]) *syncplanev1.SubscribeResponse {
		GinkgoHelper()
		Expect(stream.Receive()).To(BeTrue(), "stream closed early: %v", stream.Err())
		return stream.Msg()
	}

	// drainToCaughtUp reads frames until the named workspace reports caught_up,
	// returning every RowChange that workspace sent along the way.
	drainToCaughtUp := func(stream *connect.ServerStreamForClient[syncplanev1.SubscribeResponse], workspace string) []*syncplanev1.RowChange {
		GinkgoHelper()
		var rows []*syncplanev1.RowChange
		for {
			msg := recv(stream)
			Expect(msg.GetWorkspaceId()).To(Equal(workspace))
			if msg.GetCaughtUp() {
				return rows
			}
			Expect(msg.GetPage()).NotTo(BeNil())
			Expect(msg.GetPage().GetReset_()).To(BeFalse(),
				"reset arrives as a dropped frame on a stream, never as a page")
			rows = append(rows, msg.GetPage().GetPatch().GetChanges()...)
		}
	}

	// titles decodes the value rows, skipping tombstones. A fresh catch-up
	// tombstones read-filtered / out-of-predicate rows (pull's revocation
	// semantics) — harmless to a client holding no state, and excluded here.
	titles := func(changes []*syncplanev1.RowChange) []string {
		out := []string{}
		for _, rc := range changes {
			if rc.GetTombstone() {
				continue
			}
			var todo todov1.Todo
			Expect(proto.Unmarshal(rc.GetValue(), &todo)).To(Succeed())
			out = append(out, todo.GetTitle())
		}
		return out
	}

	// valueRows returns only the non-tombstone changes.
	valueRows := func(changes []*syncplanev1.RowChange) []*syncplanev1.RowChange {
		out := []*syncplanev1.RowChange{}
		for _, rc := range changes {
			if !rc.GetTombstone() {
				out = append(out, rc)
			}
		}
		return out
	}

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background())
		DeferCleanup(cancel)
	})

	It("streams catch-up pages then a caught_up marker", func() {
		client = start(newTestServer())
		push(
			putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one"}),
			putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "two"}),
		)

		stream := subscribe(&syncplanev1.SubscribeRequest{ClientId: "c1", Scopes: scopes(ws)})
		rows := drainToCaughtUp(stream, ws)
		Expect(titles(rows)).To(ConsistOf("one", "two"))
	})

	It("delivers the live tail when another client pushes", func() {
		client = start(newTestServer())
		stream := subscribe(&syncplanev1.SubscribeRequest{ClientId: "c1", Scopes: scopes(ws)})
		// Empty workspace: catch-up drains immediately to caught_up.
		Expect(drainToCaughtUp(stream, ws)).To(BeEmpty())

		// A commit from another client lands on the open stream without a pull.
		push(putMutation("c2", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "live"}))

		msg := recv(stream)
		Expect(msg.GetWorkspaceId()).To(Equal(ws))
		Expect(msg.GetCaughtUp()).To(BeFalse())
		Expect(titles(msg.GetPage().GetPatch().GetChanges())).To(Equal([]string{"live"}))
		Expect(msg.GetPage().GetSeq()).To(BeEquivalentTo(1))
	})

	It("paginates a catch-up burst larger than the limit", func() {
		client = start(newTestServer())
		var muts []*syncplanev1.Mutation
		for i := uint64(1); i <= 5; i++ {
			muts = append(muts, putMutation("c1", i, "todo",
				todoKey(i), &todov1.Todo{Id: todoKey(i), Title: todoKey(i)}))
		}
		push(muts...)

		// limit=2 forces three catch-up pages (2 + 2 + 1).
		stream := subscribe(&syncplanev1.SubscribeRequest{ClientId: "c1", Scopes: scopes(ws), Limit: 2})
		var pages int
		var rows []*syncplanev1.RowChange
		for {
			msg := recv(stream)
			if msg.GetCaughtUp() {
				break
			}
			pages++
			rows = append(rows, msg.GetPage().GetPatch().GetChanges()...)
		}
		Expect(pages).To(Equal(3))
		Expect(rows).To(HaveLen(5))
	})

	It("drops a workspace whose cursor is ahead of the store, with reason RESET", func() {
		client = start(newTestServer())
		push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "x"}))

		// A cursor from a larger, previous incarnation of the store at this URL.
		ahead := mustMarshal(&syncplanecursorv1.Cursor{Seq: 99})
		stream := subscribe(&syncplanev1.SubscribeRequest{
			ClientId: "c1",
			Scopes:   []*syncplanev1.WorkspaceScope{{WorkspaceId: ws, Cursor: ahead}},
		})

		msg := recv(stream)
		Expect(msg.GetWorkspaceId()).To(Equal(ws))
		Expect(msg.GetDropped().GetReason()).To(Equal(syncplanev1.WorkspaceDropped_REASON_RESET))
		// It was the only scope, so nothing is left to stream.
		Expect(stream.Receive()).To(BeFalse())
	})

	It("enforces the read filter per stream", func() {
		client = start(newTestServer(server.WithPermissions(exampleapp.NewPermissions())))
		push(
			putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "public"}),
			putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "top secret plans"}),
		)

		stream := subscribe(&syncplanev1.SubscribeRequest{ClientId: "c1", Scopes: scopes(ws)})
		Expect(titles(drainToCaughtUp(stream, ws))).To(Equal([]string{"public"}))
	})

	It("enforces subscription predicates per stream", func() {
		client = start(newTestServer())
		push(
			putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Count: 3}),
			putMutation("c1", 2, "widget", "w2", &testdatav1.Widget{Id: "w2", Count: 10}),
		)

		stream := subscribe(&syncplanev1.SubscribeRequest{
			ClientId: "c1",
			Scopes: []*syncplanev1.WorkspaceScope{{
				WorkspaceId:   ws,
				Subscriptions: []*syncplanev1.Subscription{widgetSub(countGte(5))},
			}},
		})
		rows := valueRows(drainToCaughtUp(stream, ws))
		Expect(rows).To(HaveLen(1))
		var w testdatav1.Widget
		Expect(proto.Unmarshal(rows[0].GetValue(), &w)).To(Succeed())
		Expect(w.GetId()).To(Equal("w2"))
	})

	// A heartbeat that is not positive keeps the default, as WithChunkSize and
	// WithRoomAnnounce do for theirs. Every binary in cmd/ passes its
	// -heartbeat flag straight through.
	DescribeTable("keeps the default heartbeat for a value that is not positive",
		func(hb time.Duration) {
			client = start(newTestServer(server.WithHeartbeat(hb)))
			stream := subscribe(&syncplanev1.SubscribeRequest{ClientId: "c1", Scopes: scopes(ws)})
			Expect(drainToCaughtUp(stream, ws)).To(BeEmpty())

			push(putMutation("c2", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "live"}))
			Expect(titles(recv(stream).GetPage().GetPatch().GetChanges())).To(Equal([]string{"live"}))
		},
		Entry("zero", time.Duration(0)),
		Entry("negative", -time.Second),
	)

	It("sends a content-free keepalive on the heartbeat", func() {
		client = start(newTestServer(server.WithHeartbeat(50 * time.Millisecond)))
		stream := subscribe(&syncplanev1.SubscribeRequest{ClientId: "c1", Scopes: scopes(ws)})
		Expect(drainToCaughtUp(stream, ws)).To(BeEmpty())

		// No further changes: the next message is a content-free heartbeat —
		// liveness only, carrying no page and no workspace, so the client never
		// rebases on it.
		msg := recv(stream)
		Expect(msg.GetHeartbeat()).To(BeTrue())
		Expect(msg.GetWorkspaceId()).To(BeEmpty())
		Expect(msg.GetCaughtUp()).To(BeFalse())
		Expect(msg.GetPage()).To(BeNil())
	})

	Describe("multiplexing several workspaces", func() {
		const (
			wsA = "mux-a"
			wsB = "mux-b"
		)

		// collect reads frames until every named workspace has reported
		// caught_up, returning each one's rows. This is what a real client does:
		// scopes catch up independently and in no guaranteed interleaving.
		collect := func(stream *connect.ServerStreamForClient[syncplanev1.SubscribeResponse], ids ...string) map[string][]*syncplanev1.RowChange {
			GinkgoHelper()
			pending := map[string]bool{}
			for _, id := range ids {
				pending[id] = true
			}
			rows := map[string][]*syncplanev1.RowChange{}
			for len(pending) > 0 {
				msg := recv(stream)
				Expect(msg.GetWorkspaceId()).To(BeElementOf(ids))
				if msg.GetCaughtUp() {
					delete(pending, msg.GetWorkspaceId())
					continue
				}
				Expect(msg.GetDropped()).To(BeNil(), "unexpected drop: %v", msg.GetDropped())
				// Catch-up sends a final page even when empty (it carries the
				// cursor and last_acked), so only record workspaces that
				// actually shipped rows.
				if changes := msg.GetPage().GetPatch().GetChanges(); len(changes) > 0 {
					rows[msg.GetWorkspaceId()] = append(rows[msg.GetWorkspaceId()], changes...)
				}
			}
			return rows
		}

		It("catches up every scope on one stream, each frame naming its workspace", func() {
			client = start(newTestServer())
			pushTo(wsA, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "in A"}))
			pushTo(wsB, putMutation("c1", 1, "todo", "t2", &todov1.Todo{Id: "t2", Title: "in B"}))

			stream := subscribe(&syncplanev1.SubscribeRequest{ClientId: "c1", Scopes: scopes(wsA, wsB)})
			rows := collect(stream, wsA, wsB)
			Expect(titles(rows[wsA])).To(Equal([]string{"in A"}))
			Expect(titles(rows[wsB])).To(Equal([]string{"in B"}))
		})

		It("delivers a live tail for each workspace independently", func() {
			client = start(newTestServer())
			stream := subscribe(&syncplanev1.SubscribeRequest{ClientId: "c1", Scopes: scopes(wsA, wsB)})
			Expect(collect(stream, wsA, wsB)).To(BeEmpty())

			// A write to B alone must wake B alone — the partitions share a
			// stream, not a sequence.
			pushTo(wsB, putMutation("c2", 1, "todo", "t9", &todov1.Todo{Id: "t9", Title: "only B"}))
			msg := recv(stream)
			Expect(msg.GetWorkspaceId()).To(Equal(wsB))
			Expect(titles(msg.GetPage().GetPatch().GetChanges())).To(Equal([]string{"only B"}))

			pushTo(wsA, putMutation("c2", 1, "todo", "t8", &todov1.Todo{Id: "t8", Title: "only A"}))
			msg = recv(stream)
			Expect(msg.GetWorkspaceId()).To(Equal(wsA))
			Expect(titles(msg.GetPage().GetPatch().GetChanges())).To(Equal([]string{"only A"}))
		})

		It("drops one workspace without ending the stream", func() {
			// The property multiplexing lives or dies on: a scope that cannot be
			// served must not take its siblings down. Here wsA's cursor is
			// unusable while wsB is fine.
			client = start(newTestServer())
			pushTo(wsB, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "survivor"}))

			ahead := mustMarshal(&syncplanecursorv1.Cursor{Seq: 99})
			stream := subscribe(&syncplanev1.SubscribeRequest{
				ClientId: "c1",
				Scopes: []*syncplanev1.WorkspaceScope{
					{WorkspaceId: wsA, Cursor: ahead},
					{WorkspaceId: wsB},
				},
			})

			var dropped *syncplanev1.WorkspaceDropped
			var survivorRows []*syncplanev1.RowChange
			for done := false; !done; {
				msg := recv(stream)
				switch {
				case msg.GetDropped() != nil:
					Expect(msg.GetWorkspaceId()).To(Equal(wsA))
					dropped = msg.GetDropped()
				case msg.GetCaughtUp():
					Expect(msg.GetWorkspaceId()).To(Equal(wsB))
					done = true
				default:
					Expect(msg.GetWorkspaceId()).To(Equal(wsB))
					survivorRows = append(survivorRows, msg.GetPage().GetPatch().GetChanges()...)
				}
			}
			Expect(dropped).NotTo(BeNil(), "wsA must be dropped explicitly")
			Expect(dropped.GetReason()).To(Equal(syncplanev1.WorkspaceDropped_REASON_RESET))
			Expect(titles(survivorRows)).To(Equal([]string{"survivor"}))

			// And wsB keeps streaming afterwards.
			pushTo(wsB, putMutation("c2", 1, "todo", "t2", &todov1.Todo{Id: "t2", Title: "still live"}))
			msg := recv(stream)
			Expect(msg.GetWorkspaceId()).To(Equal(wsB))
			Expect(titles(msg.GetPage().GetPatch().GetChanges())).To(Equal([]string{"still live"}))
		})

		It("drops a revoked workspace as ACCESS_REVOKED and keeps the rest", func() {
			perms := &gatePerms{verdict: func(_ server.Principal, w string) error {
				if w == wsA {
					return server.DenyWorkspace("not a member")
				}
				return nil
			}}
			client = start(newTestServer(server.WithPermissions(perms)))
			pushTo(wsB, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "allowed"}))

			stream := subscribe(&syncplanev1.SubscribeRequest{ClientId: "c1", Scopes: scopes(wsA, wsB)})

			msg := recv(stream)
			Expect(msg.GetWorkspaceId()).To(Equal(wsA))
			Expect(msg.GetDropped().GetReason()).To(Equal(syncplanev1.WorkspaceDropped_REASON_ACCESS_REVOKED))

			// wsB is unaffected.
			var rows []*syncplanev1.RowChange
			for {
				m := recv(stream)
				Expect(m.GetWorkspaceId()).To(Equal(wsB))
				if m.GetCaughtUp() {
					break
				}
				rows = append(rows, m.GetPage().GetPatch().GetChanges()...)
			}
			Expect(titles(rows)).To(Equal([]string{"allowed"}))
		})

		Describe("access withdrawn while the stream is open", func() {
			// A stream outlives the decision that admitted it, so the gate is
			// re-run on the heartbeat. These specs pin the two verdicts a live
			// re-check can produce, and that both stay scoped to one workspace.
			var (
				gate *midStreamGate
				srv  *server.Server
			)

			// nextScoped reads past the stream-level heartbeats a fast tick
			// produces, returning the next frame that names a workspace.
			nextScoped := func(stream *connect.ServerStreamForClient[syncplanev1.SubscribeResponse]) *syncplanev1.SubscribeResponse {
				GinkgoHelper()
				for {
					if msg := recv(stream); !msg.GetHeartbeat() {
						return msg
					}
				}
			}

			BeforeEach(func() {
				gate = &midStreamGate{}
				srv = newTestServer(
					server.WithPermissions(gate),
					server.WithHeartbeat(20*time.Millisecond),
				)
				client = start(srv)
			})

			It("drops the scope as ACCESS_REVOKED without waiting for a reconnect", func() {
				stream := subscribe(&syncplanev1.SubscribeRequest{ClientId: "c1", Scopes: scopes(wsA, wsB)})
				Expect(collect(stream, wsA, wsB)).To(BeEmpty())

				// Membership changes underneath a healthy stream. Nothing the
				// client does triggers this: the next heartbeat has to notice.
				gate.deny(wsA, "removed from the project")

				msg := nextScoped(stream)
				Expect(msg.GetWorkspaceId()).To(Equal(wsA))
				Expect(msg.GetDropped().GetReason()).
					To(Equal(syncplanev1.WorkspaceDropped_REASON_ACCESS_REVOKED))

				// The revoked scope goes quiet even though it keeps changing,
				// while its sibling streams on. wsA is written through a Trusted
				// call so the write lands at all: the gate now covers the
				// in-process path too, and the client's own push would be refused.
				Expect(srv.Mutate(server.Trusted(ctx), wsA, func(tx *server.EntityTx) error {
					return tx.Put(&todov1.Todo{Id: "t1", Title: "unseen"})
				})).To(Succeed())
				pushTo(wsB, putMutation("c2", 1, "todo", "t2", &todov1.Todo{Id: "t2", Title: "still live"}))
				msg = nextScoped(stream)
				Expect(msg.GetWorkspaceId()).To(Equal(wsB))
				Expect(titles(msg.GetPage().GetPatch().GetChanges())).To(Equal([]string{"still live"}))
			})

			It("drops the scope as ERROR when the gate cannot decide", func() {
				// An outage must not reach the client as revocation: that verdict
				// is a cue to stop syncing and offer a purge. ERROR asks for a
				// reopen instead, which meets the gate again.
				stream := subscribe(&syncplanev1.SubscribeRequest{ClientId: "c1", Scopes: scopes(wsA, wsB)})
				Expect(collect(stream, wsA, wsB)).To(BeEmpty())

				gate.fail(wsA, errors.New("membership service unreachable"))

				msg := nextScoped(stream)
				Expect(msg.GetWorkspaceId()).To(Equal(wsA))
				Expect(msg.GetDropped().GetReason()).To(Equal(syncplanev1.WorkspaceDropped_REASON_ERROR))
			})

			It("ends the stream once every scope has been revoked", func() {
				stream := subscribe(&syncplanev1.SubscribeRequest{ClientId: "c1", Scopes: scopes(wsA, wsB)})
				Expect(collect(stream, wsA, wsB)).To(BeEmpty())

				gate.denyAll("workspace deleted")

				seen := map[string]syncplanev1.WorkspaceDropped_Reason{}
				for stream.Receive() {
					if d := stream.Msg().GetDropped(); d != nil {
						seen[stream.Msg().GetWorkspaceId()] = d.GetReason()
					}
				}
				Expect(stream.Err()).NotTo(HaveOccurred(), "an inert stream closes cleanly")
				Expect(seen).To(Equal(map[string]syncplanev1.WorkspaceDropped_Reason{
					wsA: syncplanev1.WorkspaceDropped_REASON_ACCESS_REVOKED,
					wsB: syncplanev1.WorkspaceDropped_REASON_ACCESS_REVOKED,
				}))
			})
		})

		It("rejects a request with no scopes", func() {
			client = start(newTestServer())
			stream := subscribe(&syncplanev1.SubscribeRequest{ClientId: "c1"})
			Expect(stream.Receive()).To(BeFalse())
			Expect(connect.CodeOf(stream.Err())).To(Equal(connect.CodeInvalidArgument))
		})

		It("rejects a workspace named twice", func() {
			// Two cursors over one partition on one stream would leave the
			// client unable to tell the pages apart.
			client = start(newTestServer())
			stream := subscribe(&syncplanev1.SubscribeRequest{ClientId: "c1", Scopes: scopes(wsA, wsA)})
			Expect(stream.Receive()).To(BeFalse())
			Expect(connect.CodeOf(stream.Err())).To(Equal(connect.CodeInvalidArgument))
		})

		It("rejects more scopes than the limit allows", func() {
			client = start(newTestServer(server.WithLimits(server.Limits{MaxWorkspacesPerRequest: 2})))
			stream := subscribe(&syncplanev1.SubscribeRequest{
				ClientId: "c1",
				Scopes:   scopes("w1", "w2", "w3"),
			})
			Expect(stream.Receive()).To(BeFalse())
			Expect(connect.CodeOf(stream.Err())).To(Equal(connect.CodeInvalidArgument))
			Expect(stream.Err()).To(MatchError(ContainSubstring(
				"subscribe spans 3 workspaces, over MaxWorkspacesPerRequest (2); raise it with server.WithLimits")))
		})
	})
})

func todoKey(i uint64) string {
	return "t" + string(rune('0'+i))
}
