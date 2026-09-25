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
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
)

// A polymorphic reference points at one of several entities. Three fixtures
// declare one: OneofRefEntity holds a oneof of entity-typed fields, Memo
// holds OwnerRef — a reference message whose target is a oneof beside a
// workspace — to-one and to-many, and Attachment mixes an entity-typed
// member, a workspace-qualified reference message and a plain string in one
// oneof. Each member is a case: the relation is followed by the oneof's name,
// a matches() names the case it crosses, and the reverse relations land on
// each target.
var _ = Describe("Polymorphic references", func() {
	const other = "ws-elsewhere"

	var (
		ctx    context.Context
		client syncplanev1connect.SyncServiceClient
	)

	startWith := func(opts ...server.Option) {
		srv := newTestServer(append([]server.Option{server.WithEntities(
			&testdatav1.OneofRefEntity{}, &testdatav1.Memo{}, &testdatav1.Attachment{},
		)}, opts...)...)
		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	}

	push := func(mutations ...*syncplanev1.Mutation) []*syncplanev1.MutationResult {
		GinkgoHelper()
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, mutations)))
		Expect(err).NotTo(HaveOccurred())
		return pushResults(resp.Msg)
	}

	pushOK := func(mutations ...*syncplanev1.Mutation) {
		GinkgoHelper()
		for _, r := range push(mutations...) {
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

	rowOf := func(entity, key string, into proto.Message) {
		GinkgoHelper()
		for _, rc := range pull(&syncplanev1.Subscription{Entity: entity}).GetChanges() {
			if rc.GetKey() == key {
				Expect(proto.Unmarshal(rc.GetValue(), into)).To(Succeed())
				return
			}
		}
		Fail("row " + entity + "/" + key + " not pulled")
	}

	queryKeys := func(entity string, cond *syncplanev1.Condition) []string {
		GinkgoHelper()
		resp, err := client.Query(ctx, connect.NewRequest(&syncplanev1.QueryRequest{
			WorkspaceId: ws, Entity: entity, Predicate: cond,
		}))
		Expect(err).NotTo(HaveOccurred())
		keys := make([]string, 0, len(resp.Msg.GetRows()))
		for _, rc := range resp.Msg.GetRows() {
			keys = append(keys, rc.GetKey())
		}
		return keys
	}

	queryErr := func(entity string, cond *syncplanev1.Condition) error {
		GinkgoHelper()
		_, err := client.Query(ctx, connect.NewRequest(&syncplanev1.QueryRequest{
			WorkspaceId: ws, Entity: entity, Predicate: cond,
		}))
		Expect(err).To(HaveOccurred())
		Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
		return err
	}

	ownerProject := func(workspace, id string) *testdatav1.OwnerRef {
		return &testdatav1.OwnerRef{WorkspaceId: workspace, Owner: &testdatav1.OwnerRef_Project{Project: &testdatav1.Project{Id: id}}}
	}
	ownerUser := func(workspace, id string) *testdatav1.OwnerRef {
		return &testdatav1.OwnerRef{WorkspaceId: workspace, Owner: &testdatav1.OwnerRef_User{User: &testdatav1.User{Id: id}}}
	}
	userRef := func(workspace, id string) *testdatav1.UserRef {
		return &testdatav1.UserRef{WorkspaceId: workspace, User: &testdatav1.User{Id: id}}
	}

	// seed plants one project and one user, and rows pointing at them through
	// every shape: each case of each fixture, a stub naming this workspace, one
	// naming none, and one naming another workspace — which shares the key
	// with the local row but points at a different one.
	seed := func() {
		GinkgoHelper()
		pushOK(
			putMutation("c1", 1, "project", "p1", &testdatav1.Project{Id: "p1", Name: "Alpha"}),
			putMutation("c1", 2, "user", "u1", &testdatav1.User{Id: "u1", Name: "Maya"}),

			putMutation("c1", 3, "oneof_ref", "r-proj", &testdatav1.OneofRefEntity{Id: "r-proj",
				Target: &testdatav1.OneofRefEntity_Project{Project: &testdatav1.Project{Id: "p1", Name: "stale copy"}}}),
			putMutation("c1", 4, "oneof_ref", "r-user", &testdatav1.OneofRefEntity{Id: "r-user",
				Target: &testdatav1.OneofRefEntity_User{User: &testdatav1.User{Id: "u1"}}}),
			putMutation("c1", 5, "oneof_ref", "r-none", &testdatav1.OneofRefEntity{Id: "r-none"}),

			putMutation("c1", 6, "memo", "m-proj-here", &testdatav1.Memo{Id: "m-proj-here", Owner: ownerProject(ws, "p1")}),
			putMutation("c1", 7, "memo", "m-user-bare", &testdatav1.Memo{Id: "m-user-bare", Owner: ownerUser("", "u1")}),
			putMutation("c1", 8, "memo", "m-user-away", &testdatav1.Memo{Id: "m-user-away", Owner: ownerUser(other, "u1")}),
			putMutation("c1", 9, "memo", "m-mentions", &testdatav1.Memo{Id: "m-mentions", Mentions: []*testdatav1.OwnerRef{
				ownerProject(ws, "p1"), ownerUser(other, "u1"), ownerUser("", "u1"),
			}}),

			putMutation("c1", 10, "attachment", "a-proj", &testdatav1.Attachment{Id: "a-proj",
				Parent: &testdatav1.Attachment_Project{Project: &testdatav1.Project{Id: "p1"}}}),
			putMutation("c1", 11, "attachment", "a-user-here", &testdatav1.Attachment{Id: "a-user-here",
				Parent: &testdatav1.Attachment_User{User: userRef(ws, "u1")}}),
			putMutation("c1", 12, "attachment", "a-user-away", &testdatav1.Attachment{Id: "a-user-away",
				Parent: &testdatav1.Attachment_User{User: userRef(other, "u1")}}),
			putMutation("c1", 13, "attachment", "a-label", &testdatav1.Attachment{Id: "a-label",
				Parent: &testdatav1.Attachment_Label{Label: "loose"}}),
		)
	}

	BeforeEach(func() {
		ctx = context.Background()
	})

	Describe("storage", func() {
		BeforeEach(func() {
			startWith()
			seed()
		})

		It("projects the active case down to its target's key", func() {
			var r testdatav1.OneofRefEntity
			rowOf("oneof_ref", "r-proj", &r)
			Expect(proto.Equal(r.GetProject(), &testdatav1.Project{Id: "p1"})).To(BeTrue(), "%v", r.GetProject())

			var m testdatav1.Memo
			rowOf("memo", "m-proj-here", &m)
			Expect(m.GetOwner().GetWorkspaceId()).To(Equal(ws))
			Expect(proto.Equal(m.GetOwner().GetProject(), &testdatav1.Project{Id: "p1"})).To(BeTrue())
			Expect(m.GetOwner().GetUser()).To(BeNil())

			var a testdatav1.Attachment
			rowOf("attachment", "a-label", &a)
			Expect(a.GetLabel()).To(Equal("loose"))
		})

		It("refuses a case naming a workspace no client could hold", func() {
			results := push(putMutation("c2", 1, "memo", "m-bad", &testdatav1.Memo{Id: "m-bad", Owner: ownerUser("no/such", "u1")}))
			Expect(results[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(results[0].GetErrorMessage()).To(ContainSubstring(`reference "owner"`))
		})

		It("switches the case of a oneof on the holder with a mask naming the group", func() {
			pushOK(patchMutation("c2", 1, "oneof_ref", "r-proj", &testdatav1.OneofRefEntity{
				Target: &testdatav1.OneofRefEntity_User{User: &testdatav1.User{Id: "u1"}},
			}, "target"))
			var r testdatav1.OneofRefEntity
			rowOf("oneof_ref", "r-proj", &r)
			Expect(r.GetUser().GetId()).To(Equal("u1"))
			Expect(r.GetProject()).To(BeNil())

			results := push(patchMutation("c2", 2, "oneof_ref", "r-proj", &testdatav1.OneofRefEntity{
				Target: &testdatav1.OneofRefEntity_Project{Project: &testdatav1.Project{Id: "p1"}},
			}, "project"))
			Expect(results[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(results[0].GetErrorMessage()).To(ContainSubstring(`member of oneof "target"`))
		})

		It("replaces a polymorphic reference message whole, whichever sub-path the mask names", func() {
			pushOK(patchMutation("c2", 1, "memo", "m-proj-here", &testdatav1.Memo{
				Owner: ownerUser("", "u1"),
			}, "owner.workspace_id"))
			var m testdatav1.Memo
			rowOf("memo", "m-proj-here", &m)
			Expect(m.GetOwner().GetWorkspaceId()).To(BeEmpty())
			Expect(m.GetOwner().GetUser().GetId()).To(Equal("u1"))
			Expect(m.GetOwner().GetProject()).To(BeNil())
		})

		It("adds and removes one element of a repeated polymorphic reference by its case and key", func() {
			// Drop the project mention and add a second user; the two user
			// mentions of u1 differ in the workspace they name and both stay.
			pushOK(patchMutation("c2", 1, "memo", "m-mentions", &testdatav1.Memo{
				Mentions: []*testdatav1.OwnerRef{ownerUser(other, "u1"), ownerUser("", "u1"), ownerUser("", "u2")},
			}, "mentions."+ws+"/project/p1", "mentions./user/u2"))
			var m testdatav1.Memo
			rowOf("memo", "m-mentions", &m)
			Expect(m.GetMentions()).To(HaveLen(3))
			for _, e := range m.GetMentions() {
				Expect(e.GetProject()).To(BeNil())
			}
			Expect(m.GetMentions()[2].GetUser().GetId()).To(Equal("u2"))
		})
	})

	Describe("matches()", func() {
		BeforeEach(func() {
			startWith()
			seed()
		})

		It("crosses one case of a oneof on the holder", func() {
			Expect(queryKeys("oneof_ref", query.Rel("project", query.Eq("name", "Alpha")))).To(ConsistOf("r-proj"))
			Expect(queryKeys("oneof_ref", query.Rel("user", query.Eq("name", "Maya")))).To(ConsistOf("r-user"))
			Expect(queryKeys("oneof_ref", query.Not(query.Rel("user", query.Eq("name", "Maya"))))).To(ConsistOf("r-proj", "r-none"))
		})

		It("crosses one case of a polymorphic reference message in the workspace it names", func() {
			Expect(queryKeys("memo", query.Rel("owner.project", query.Eq("name", "Alpha")))).To(ConsistOf("m-proj-here"))
			Expect(queryKeys("memo", query.Rel("owner.user", query.Eq("name", "Maya")))).To(ConsistOf("m-user-bare"))
			Expect(queryKeys("memo", query.Rel("mentions.user", query.Eq("name", "Maya")))).To(ConsistOf("m-mentions"))
			Expect(queryKeys("memo", query.Rel("mentions.project", query.Eq("name", "Beta")))).To(BeEmpty())
		})

		It("crosses the reference-message case of a mixed oneof by the workspace it names", func() {
			Expect(queryKeys("attachment", query.Rel("user", query.Eq("name", "Maya")))).To(ConsistOf("a-user-here"))
			Expect(queryKeys("attachment", query.Rel("project", query.Eq("name", "Alpha")))).To(ConsistOf("a-proj"))
		})

		It("compares a case by the key path inside the reference message", func() {
			Expect(queryKeys("memo", query.Eq("owner.project.id", "p1"))).To(ConsistOf("m-proj-here"))
			Expect(queryKeys("memo", query.Eq("owner.user.id", "u1"))).To(ConsistOf("m-user-bare", "m-user-away"))
			Expect(queryKeys("memo", query.Eq("owner.workspace_id", other))).To(ConsistOf("m-user-away"))
			Expect(queryKeys("attachment", query.Eq("user.user.id", "u1"))).To(ConsistOf("a-user-here", "a-user-away"))
			Expect(queryKeys("attachment", query.Eq("label", "loose"))).To(ConsistOf("a-label"))
		})

		It("refuses matches() on the polymorphic message itself", func() {
			err := queryErr("memo", query.Rel("owner", query.Eq("name", "Alpha")))
			Expect(err).To(MatchError(ContainSubstring(`matches must name one case of polymorphic reference "owner"`)))
			Expect(err).To(MatchError(ContainSubstring(`owner.project, owner.user`)))
		})

		It("refuses a path reaching past a case's stored stub", func() {
			err := queryErr("memo", query.Eq("owner.user.name", "x"))
			Expect(err).To(MatchError(ContainSubstring(`cannot descend through reference "user" into "name"`)))
		})
	})

	Describe("follow", func() {
		follow := func(entity string, pred *syncplanev1.Condition, segments ...string) *syncplanev1.Subscription {
			return &syncplanev1.Subscription{
				Entity:    entity,
				Predicate: pred,
				Follow: &syncplanev1.FollowSpec{
					Paths: []*syncplanev1.RefPath{{Segments: segments}},
					Depth: 1,
				},
			}
		}

		for _, tc := range []struct {
			name string
			opts []server.Option
		}{
			{name: "served from the reference indexes", opts: nil},
			{name: "served by a scan", opts: []server.Option{server.WithLimits(server.Limits{DisableIndexes: true})}},
		} {
			tc := tc
			Describe(tc.name, func() {
				BeforeEach(func() {
					startWith(tc.opts...)
					seed()
					// Rows only stubs pointing elsewhere name.
					pushOK(
						putMutation("c1", 14, "user", "u9", &testdatav1.User{Id: "u9", Name: "Noor"}),
						putMutation("c1", 15, "memo", "m-u9-away", &testdatav1.Memo{Id: "m-u9-away", Owner: ownerUser(other, "u9")}),
					)
				})

				It("follows a oneof on the holder by the group's name into every case's target", func() {
					Expect(values(pull(follow("oneof_ref", nil, "target")))).To(ConsistOf(
						"oneof_ref/r-proj", "oneof_ref/r-user", "oneof_ref/r-none", "project/p1", "user/u1"))
					Expect(values(pull(follow("oneof_ref", query.Eq("id", "r-user"), "target")))).To(ConsistOf(
						"oneof_ref/r-user", "user/u1"))
				})

				It("follows a polymorphic reference message into this workspace only", func() {
					got := values(pull(follow("memo", nil, "owner")))
					Expect(got).To(ContainElements("project/p1", "user/u1"))
					Expect(got).NotTo(ContainElement("user/u9"), "named only by a stub pointing elsewhere")
					Expect(values(pull(follow("memo", query.Eq("id", "m-mentions"), "mentions")))).To(ConsistOf(
						"memo/m-mentions", "project/p1", "user/u1"))
				})

				It("follows a mixed oneof through its reference cases and past its plain member", func() {
					Expect(values(pull(follow("attachment", nil, "parent")))).To(ConsistOf(
						"attachment/a-proj", "attachment/a-user-here", "attachment/a-user-away", "attachment/a-label",
						"project/p1", "user/u1"))
				})

				It("follows each case's reverse relation to the rows whose case points here", func() {
					p1 := query.Eq("id", "p1")
					u1 := query.Eq("id", "u1")
					Expect(values(pull(follow("project", p1, "oneofRefsByProject")))).To(ConsistOf("project/p1", "oneof_ref/r-proj"))
					Expect(values(pull(follow("user", u1, "oneofRefsByUser")))).To(ConsistOf("user/u1", "oneof_ref/r-user"))
					Expect(values(pull(follow("project", p1, "memosByOwner")))).To(ConsistOf("project/p1", "memo/m-proj-here"))
					Expect(values(pull(follow("user", u1, "memosByOwner")))).To(ConsistOf("user/u1", "memo/m-user-bare"))
					Expect(values(pull(follow("project", p1, "memosByMentions")))).To(ConsistOf("project/p1", "memo/m-mentions"))
					Expect(values(pull(follow("user", u1, "memosByMentions")))).To(ConsistOf("user/u1", "memo/m-mentions"))
					Expect(values(pull(follow("project", p1, "attachmentsByProject")))).To(ConsistOf("project/p1", "attachment/a-proj"))
					Expect(values(pull(follow("user", u1, "attachmentsByUser")))).To(ConsistOf("user/u1", "attachment/a-user-here"))
					Expect(values(pull(follow("user", query.Eq("id", "u9"), "memosByOwner")))).To(ConsistOf("user/u9"))
				})
			})
		}

		It("names the group, not its cases, among an entity's relations", func() {
			startWith()
			_, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
				WorkspaceId:   ws,
				Subscriptions: []*syncplanev1.Subscription{follow("attachment", nil, "project")},
			}))
			Expect(err).To(MatchError(ContainSubstring(`follow segment "project" is not a relation of entity "attachment" (relations: parent)`)))
		})
	})

	Describe("registration", func() {
		It("refuses a polymorphic reference message as a member of a oneof", func() {
			entityOpts := func(name string) *descriptorpb.MessageOptions {
				o := &descriptorpb.MessageOptions{}
				proto.SetExtension(o, syncplanev1.E_Entity, &syncplanev1.EntityOptions{Name: name, KeyFields: []string{"id"}})
				return o
			}
			refOpts := &descriptorpb.MessageOptions{}
			proto.SetExtension(refOpts, syncplanev1.E_Reference, &syncplanev1.ReferenceOptions{})
			str := func(name string, number int32) *descriptorpb.FieldDescriptorProto {
				return &descriptorpb.FieldDescriptorProto{
					Name: proto.String(name), Number: proto.Int32(number),
					Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					Type:  descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
				}
			}
			msg := func(name string, number int32, typeName string, oneof int32) *descriptorpb.FieldDescriptorProto {
				return &descriptorpb.FieldDescriptorProto{
					Name: proto.String(name), Number: proto.Int32(number),
					Label:      descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					Type:       descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
					TypeName:   proto.String(typeName),
					OneofIndex: proto.Int32(oneof),
				}
			}
			fd, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
				Name:    proto.String("nested/refs.proto"),
				Package: proto.String("nested"),
				Syntax:  proto.String("proto3"),
				MessageType: []*descriptorpb.DescriptorProto{
					{Name: proto.String("A"), Options: entityOpts("a"), Field: []*descriptorpb.FieldDescriptorProto{str("id", 1)}},
					{Name: proto.String("B"), Options: entityOpts("b"), Field: []*descriptorpb.FieldDescriptorProto{str("id", 1)}},
					{Name: proto.String("EitherRef"), Options: refOpts,
						OneofDecl: []*descriptorpb.OneofDescriptorProto{{Name: proto.String("target")}},
						Field:     []*descriptorpb.FieldDescriptorProto{msg("a", 1, ".nested.A", 0), msg("b", 2, ".nested.B", 0)}},
					{Name: proto.String("Holder"), Options: entityOpts("holder"),
						OneofDecl: []*descriptorpb.OneofDescriptorProto{{Name: proto.String("parent")}},
						Field: []*descriptorpb.FieldDescriptorProto{
							str("id", 1), msg("a", 2, ".nested.A", 0), msg("either", 3, ".nested.EitherRef", 0),
						}},
				},
			}, nil)
			Expect(err).NotTo(HaveOccurred())
			srv := newTestServer()
			Expect(srv.RegisterEntity(dynamicpb.NewMessage(fd.Messages().ByName("A")))).To(Succeed())
			Expect(srv.RegisterEntity(dynamicpb.NewMessage(fd.Messages().ByName("B")))).To(Succeed())
			err = srv.RegisterEntity(dynamicpb.NewMessage(fd.Messages().ByName("Holder")))
			Expect(err).To(MatchError(ContainSubstring(`reference field "either" is a member of oneof "parent" and names one of several targets itself`)))
		})
	})
})
