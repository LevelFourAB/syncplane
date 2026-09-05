package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/internal/lww"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
)

// Workspace-qualified references on the server: a Task's UserRef stubs may
// name another workspace. The server stores and projects them, and resolves
// matches() and follow inside the request's workspace alone — a stub naming
// another workspace resolves to nothing here, and never to a same-key row.
var _ = Describe("Workspace-qualified references", func() {
	const other = "ws-elsewhere"

	var (
		ctx    context.Context
		client syncplanev1connect.SyncServiceClient
	)

	startWith := func(opts ...server.Option) {
		srv := newTestServer(opts...)
		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	}

	push := func(mutations ...*syncplanev1.Mutation) {
		GinkgoHelper()
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, mutations)))
		Expect(err).NotTo(HaveOccurred())
		for _, r := range pushResults(resp.Msg) {
			Expect(r.GetErrorCode()).To(BeEmpty(), "mutation %d rejected: %s", r.GetMutationId(), r.GetErrorMessage())
		}
	}

	pull := func(subs ...*syncplanev1.Subscription) *syncplanev1.Patch {
		GinkgoHelper()
		resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId: ws, Subscriptions: subs,
		}))
		Expect(err).NotTo(HaveOccurred())
		return resp.Msg.GetPatch()
	}

	values := func(p *syncplanev1.Patch) []string {
		var out []string
		for _, rc := range p.GetChanges() {
			if !rc.GetTombstone() {
				out = append(out, rc.GetEntity()+"/"+rc.GetKey())
			}
		}
		return out
	}

	queryTasks := func(cond *syncplanev1.Condition) []string {
		GinkgoHelper()
		resp, err := client.Query(ctx, connect.NewRequest(&syncplanev1.QueryRequest{
			WorkspaceId: ws, Entity: "task", Predicate: cond,
		}))
		Expect(err).NotTo(HaveOccurred())
		keys := make([]string, 0, len(resp.Msg.GetRows()))
		for _, rc := range resp.Msg.GetRows() {
			keys = append(keys, rc.GetKey())
		}
		return keys
	}

	userRef := func(workspace, id string) *testdatav1.UserRef {
		return &testdatav1.UserRef{WorkspaceId: workspace, User: &testdatav1.User{Id: id}}
	}

	// seed plants one user and tasks pointing at it three ways: naming no
	// workspace, naming this one, and naming another. The last one shares the
	// key with the local user but points at a different row.
	seed := func() {
		GinkgoHelper()
		push(
			putMutation("c1", 1, "user", "u1", &testdatav1.User{Id: "u1", Name: "Maya"}),
			putMutation("c1", 2, "task", "t-bare", &testdatav1.Task{Id: "t-bare", Assignee: userRef("", "u1")}),
			putMutation("c1", 3, "task", "t-here", &testdatav1.Task{Id: "t-here", Assignee: userRef(ws, "u1")}),
			putMutation("c1", 4, "task", "t-away", &testdatav1.Task{Id: "t-away", Assignee: userRef(other, "u1")}),
			putMutation("c1", 5, "task", "t-watch-here", &testdatav1.Task{Id: "t-watch-here", Watchers: []*testdatav1.UserRef{userRef("", "u1")}}),
			putMutation("c1", 6, "task", "t-watch-away", &testdatav1.Task{Id: "t-watch-away", Watchers: []*testdatav1.UserRef{userRef(other, "u1")}}),
		)
	}

	BeforeEach(func() {
		ctx = context.Background()
	})

	Describe("storage", func() {
		BeforeEach(func() { startWith() })

		It("projects the inner stub down to its key and keeps the workspace", func() {
			push(putMutation("c1", 1, "task", "t1", &testdatav1.Task{
				Id:       "t1",
				Assignee: &testdatav1.UserRef{WorkspaceId: other, User: &testdatav1.User{Id: "u1", Name: "smuggled"}},
				Watchers: []*testdatav1.UserRef{{User: &testdatav1.User{Id: "u2", Name: "smuggled"}}},
			}))
			var got *testdatav1.Task
			for _, rc := range pull().GetChanges() {
				if rc.GetEntity() == "task" && rc.GetKey() == "t1" {
					got = &testdatav1.Task{}
					Expect(proto.Unmarshal(rc.GetValue(), got)).To(Succeed())
				}
			}
			Expect(got).NotTo(BeNil())
			Expect(got.GetAssignee().GetWorkspaceId()).To(Equal(other))
			Expect(got.GetAssignee().GetUser().GetId()).To(Equal("u1"))
			Expect(got.GetAssignee().GetUser().GetName()).To(BeEmpty())
			Expect(got.GetWatchers()[0].GetWorkspaceId()).To(BeEmpty())
			Expect(got.GetWatchers()[0].GetUser().GetName()).To(BeEmpty())
		})

		It("rejects a stub naming a workspace that can never be joined", func() {
			// The id fails the workspace-id rule, so no client could hold that
			// workspace and the reference could never resolve.
			resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
				putMutation("c1", 1, "task", "t-ok", &testdatav1.Task{Id: "t-ok", Assignee: userRef(other, "u1")}),
				putMutation("c1", 2, "task", "t-bad", &testdatav1.Task{Id: "t-bad", Assignee: userRef("acme/prod", "u1")}),
				putMutation("c1", 3, "task", "t-bad-watch", &testdatav1.Task{Id: "t-bad-watch", Watchers: []*testdatav1.UserRef{userRef("", "u1"), userRef("acme/prod", "u2")}}),
				patchMutation("c1", 4, "task", "t-ok", &testdatav1.Task{Assignee: userRef("acme/prod", "u1")}, "assignee"),
			})))
			Expect(err).NotTo(HaveOccurred())
			results := pushResults(resp.Msg)
			Expect(results).To(HaveLen(4))
			Expect(results[0].GetErrorCode()).To(BeEmpty())
			for _, r := range results[1:] {
				Expect(r.GetErrorCode()).To(Equal(server.CodeInvalidArgument), "mutation %d", r.GetMutationId())
				Expect(r.GetErrorMessage()).To(ContainSubstring(`workspace id "acme/prod"`))
			}
			Expect(values(pull())).To(ConsistOf("task/t-ok"))
		})
	})

	Describe("matches()", func() {
		BeforeEach(func() {
			startWith()
			seed()
		})

		It("resolves a stub in this workspace when it names it or names none", func() {
			Expect(queryTasks(query.Rel("assignee", query.Eq("name", "Maya")))).To(ConsistOf("t-bare", "t-here"))
		})

		It("resolves a repeated reference element by the workspace it names", func() {
			Expect(queryTasks(query.Rel("watchers", query.Eq("name", "Maya")))).To(ConsistOf("t-watch-here"))
		})

		It("compares a reference by the key path inside the reference message", func() {
			Expect(queryTasks(query.Eq("assignee.user.id", "u1"))).To(ConsistOf("t-bare", "t-here", "t-away"))
			Expect(queryTasks(query.Eq("assignee.workspace_id", other))).To(ConsistOf("t-away"))
		})

		It("refuses a path reaching past the stored stub", func() {
			_, err := client.Query(ctx, connect.NewRequest(&syncplanev1.QueryRequest{
				WorkspaceId: ws, Entity: "task", Predicate: query.Eq("assignee.user.name", "x"),
			}))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
			Expect(err).To(MatchError(ContainSubstring(`cannot descend through reference "user" into "name"`)))
		})

		It("refuses matches() on the entity field inside the reference message", func() {
			// Resolved as a plain reference, assignee.user would match t-away
			// against the local u1 — the workspace it names ignored.
			_, err := client.Query(ctx, connect.NewRequest(&syncplanev1.QueryRequest{
				WorkspaceId: ws, Entity: "task", Predicate: query.Rel("assignee.user", query.Eq("name", "Maya")),
			}))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
			Expect(err).To(MatchError(ContainSubstring(`matches names the reference field itself`)))
		})
	})

	Describe("follow", func() {
		followAssignee := &syncplanev1.Subscription{
			Entity: "task",
			Follow: &syncplanev1.FollowSpec{
				Paths: []*syncplanev1.RefPath{{Segments: []string{"assignee"}}},
				Depth: 1,
			},
		}
		usersByTasks := func(userID string) *syncplanev1.Subscription {
			return &syncplanev1.Subscription{
				Entity:    "user",
				Predicate: query.Eq("id", userID),
				Follow: &syncplanev1.FollowSpec{
					Paths: []*syncplanev1.RefPath{{Segments: []string{"tasksByAssignee"}}},
					Depth: 1,
				},
			}
		}

		for _, tc := range []struct {
			name string
			opts []server.Option
		}{
			{name: "served from the reference index", opts: nil},
			{name: "served by a scan", opts: []server.Option{server.WithLimits(server.Limits{DisableIndexes: true})}},
		} {
			tc := tc
			Describe(tc.name, func() {
				BeforeEach(func() {
					startWith(tc.opts...)
					seed()
					// A second user only tasks naming another workspace point at.
					push(
						putMutation("c1", 7, "user", "u9", &testdatav1.User{Id: "u9", Name: "Noor"}),
						putMutation("c1", 8, "task", "t-u9-away", &testdatav1.Task{Id: "t-u9-away", Assignee: userRef(other, "u9")}),
					)
				})

				It("follows a forward reference into this workspace only", func() {
					got := values(pull(followAssignee))
					Expect(got).To(ContainElement("user/u1"), "named by t-bare and t-here")
					Expect(got).NotTo(ContainElement("user/u9"), "named only by a stub pointing elsewhere")
				})

				It("follows a reverse relation to the rows whose stub resolves here", func() {
					Expect(values(pull(usersByTasks("u1")))).To(ConsistOf("user/u1", "task/t-bare", "task/t-here"))
					Expect(values(pull(usersByTasks("u9")))).To(ConsistOf("user/u9"))
				})
			})
		}
	})

	Describe("registration", func() {
		It("lowers a dynamic RefWorkspace field to a reference message", func() {
			fd, err := server.BuildDynamicFile(
				server.DynamicEntity{Name: "person", KeyFields: []string{"id"}, Fields: []server.DynamicField{
					{Name: "id", Number: 1, Kind: protoreflect.StringKind},
				}},
				server.DynamicEntity{Name: "job", KeyFields: []string{"id"}, Fields: []server.DynamicField{
					{Name: "id", Number: 1, Kind: protoreflect.StringKind},
					{Name: "owner", Number: 2, Ref: "person", RefWorkspace: true},
					{Name: "helpers", Number: 3, Ref: "person", RefWorkspace: true, Repeated: true},
				}},
			)
			Expect(err).NotTo(HaveOccurred())
			job := fd.Messages().ByName("Job")
			owner := lww.RefOf(job.Fields().ByName("owner"))
			Expect(owner).NotTo(BeNil())
			Expect(owner.Qualified()).To(BeTrue())
			Expect(owner.Target.Name()).To(BeEquivalentTo("Person"))
			Expect(owner.KeyPathNames(owner.KeyFields[0])).To(Equal([]string{"value", "id"}))
			Expect(lww.RefOf(job.Fields().ByName("helpers")).Field.IsList()).To(BeTrue())

			srv := newTestServer()
			Expect(srv.RegisterDynamicEntities(
				server.DynamicEntity{Name: "person", KeyFields: []string{"id"}, Fields: []server.DynamicField{
					{Name: "id", Number: 1, Kind: protoreflect.StringKind},
				}},
				server.DynamicEntity{Name: "job", KeyFields: []string{"id"}, Fields: []server.DynamicField{
					{Name: "id", Number: 1, Kind: protoreflect.StringKind},
					{Name: "owner", Number: 2, Ref: "person", RefWorkspace: true},
				}},
			)).To(Succeed())
		})

		It("rejects an entity holding a malformed reference message", func() {
			userOpts := &descriptorpb.MessageOptions{}
			proto.SetExtension(userOpts, syncplanev1.E_Entity, &syncplanev1.EntityOptions{Name: "member", KeyFields: []string{"id"}})
			refOpts := &descriptorpb.MessageOptions{}
			proto.SetExtension(refOpts, syncplanev1.E_Reference, &syncplanev1.ReferenceOptions{})
			holderOpts := &descriptorpb.MessageOptions{}
			proto.SetExtension(holderOpts, syncplanev1.E_Entity, &syncplanev1.EntityOptions{Name: "holder", KeyFields: []string{"id"}})
			str := func(name string, number int32) *descriptorpb.FieldDescriptorProto {
				return &descriptorpb.FieldDescriptorProto{
					Name: proto.String(name), Number: proto.Int32(number),
					Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					Type:  descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
				}
			}
			msg := func(name string, number int32, typeName string) *descriptorpb.FieldDescriptorProto {
				return &descriptorpb.FieldDescriptorProto{
					Name: proto.String(name), Number: proto.Int32(number),
					Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
					TypeName: proto.String(typeName),
				}
			}
			fd, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
				Name:    proto.String("bad/refs.proto"),
				Package: proto.String("bad"),
				Syntax:  proto.String("proto3"),
				MessageType: []*descriptorpb.DescriptorProto{
					{Name: proto.String("Member"), Options: userOpts, Field: []*descriptorpb.FieldDescriptorProto{str("id", 1)}},
					// The extra `label` breaks the shape: a reference message holds
					// the entity field and at most a workspace field.
					{Name: proto.String("MemberRef"), Options: refOpts, Field: []*descriptorpb.FieldDescriptorProto{
						msg("member", 1, ".bad.Member"), str("label", 2),
					}},
					{Name: proto.String("Holder"), Options: holderOpts, Field: []*descriptorpb.FieldDescriptorProto{
						str("id", 1), msg("owner", 2, ".bad.MemberRef"),
					}},
				},
			}, nil)
			Expect(err).NotTo(HaveOccurred())
			srv := newTestServer()
			Expect(srv.RegisterEntity(dynamicpb.NewMessage(fd.Messages().ByName("Member")))).To(Succeed())
			err = srv.RegisterEntity(dynamicpb.NewMessage(fd.Messages().ByName("Holder")))
			Expect(err).To(MatchError(ContainSubstring(`reference message bad.MemberRef: field "label" is neither the entity field nor a workspace field`)))
		})
	})
})
