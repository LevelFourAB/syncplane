package server_test

import (
	"net/http/httptest"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/query"
)

// staffWithContact adds to Staff a oneof `contact` of `email` and `phone`,
// both recorded in history, and a guard that lets only edit_projects read it.
func staffWithContact(file *descriptorpb.FileDescriptorProto) {
	m := messageNamed(file, "Staff")
	oneof := int32(len(m.GetOneofDecl()))
	m.OneofDecl = append(m.OneofDecl, &descriptorpb.OneofDescriptorProto{Name: proto.String("contact")})
	historied := func() *descriptorpb.FieldOptions {
		opts := &descriptorpb.FieldOptions{}
		proto.SetExtension(opts, syncplanev1.E_FieldBehavior, &syncplanev1.FieldBehavior{History: true})
		return opts
	}
	for i, name := range []string{"email", "phone"} {
		m.Field = append(m.Field, &descriptorpb.FieldDescriptorProto{
			Name:       proto.String(name),
			JsonName:   proto.String(name),
			Number:     proto.Int32(int32(20 + i)),
			Label:      descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			Type:       descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
			OneofIndex: proto.Int32(oneof),
			Options:    historied(),
		})
	}
	access := proto.GetExtension(m.GetOptions(), syncplanev1.E_Entity).(*syncplanev1.EntityOptions).GetAccess()
	access.Guard = append(access.Guard, &syncplanev1.FieldGuard{
		Fields: []string{"contact"},
		Allow:  []*syncplanev1.AccessRule{{Read: true, Write: true, Requires: []string{"edit_projects"}}},
	})
}

var _ = Describe("Access model field guards over a oneof", func() {
	var (
		f     *accessFixture
		staff protoreflect.MessageDescriptor
	)

	BeforeEach(func() {
		f = &accessFixture{ctx: GinkgoT().Context(), store: newTestStore()}
		f.srv = mustNewServer(f.store, headerAuthenticator())
		_, err := f.srv.RegisterDescriptorSet(accessFileSet(staffWithContact))
		Expect(err).NotTo(HaveOccurred())
		meta, ok := f.srv.Entity("staff")
		Expect(ok).To(BeTrue())
		staff = meta.Descriptor
		fd := staff.ParentFile()
		ts := httptest.NewServer(f.srv.HTTPHandler())
		DeferCleanup(ts.Close)
		f.sync = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)

		// The server decodes rows with the descriptors it registered, so the
		// org rows go in as dynamic messages of the extended file.
		dynamic := func(msg proto.Message) proto.Message {
			out := dynamicpb.NewMessage(fd.Messages().ByName(msg.ProtoReflect().Descriptor().Name()))
			Expect(proto.Unmarshal(mustMarshal(msg), out)).To(Succeed())
			return out
		}
		f.seed(
			dynamic(&accessv1.OrgMember{Id: "om-member", UserId: "member", Role: "org_member"}),
			dynamic(&accessv1.Project{Id: "p1", Name: "one"}),
			dynamic(&accessv1.ProjectMember{Id: "pm-guest", Project: project("p1"), UserId: "guest", Role: "project_reader"}),
			dynamic(&accessv1.ProjectMember{Id: "pm-owner", Project: project("p1"), UserId: "owner", Role: "project_owner"}),
		)
	})

	// staffRow builds a Staff row of the extended schema.
	staffRow := func(id, project, name string, contact map[string]string) proto.Message {
		msg := dynamicpb.NewMessage(staff)
		msg.Set(staff.Fields().ByName("id"), protoreflect.ValueOfString(id))
		p := dynamicpb.NewMessage(staff.Fields().ByName("project").Message())
		p.Set(p.Descriptor().Fields().ByName("id"), protoreflect.ValueOfString(project))
		msg.Set(staff.Fields().ByName("project"), protoreflect.ValueOfMessage(p))
		msg.Set(staff.Fields().ByName("name"), protoreflect.ValueOfString(name))
		for field, value := range contact {
			msg.Set(staff.Fields().ByName(protoreflect.Name(field)), protoreflect.ValueOfString(value))
		}
		return msg
	}
	historyOf := func(user string, pred *syncplanev1.Condition) []*syncplanev1.FieldHistory {
		GinkgoHelper()
		cond := query.And(query.Eq("source_entity", "staff"), query.Eq("source_key", "s1"))
		if pred != nil {
			cond = query.And(cond, pred)
		}
		req := connect.NewRequest(&syncplanev1.QueryRequest{
			WorkspaceId: ws, Entity: "syncplane_field_history", Predicate: cond,
			OrderBy: []*syncplanev1.OrderKey{query.Asc("seq")},
		})
		as(user, req.Header())
		resp, err := f.sync.Query(f.ctx, req)
		Expect(err).NotTo(HaveOccurred())
		var out []*syncplanev1.FieldHistory
		for _, rc := range resp.Msg.GetRows() {
			var rec syncplanev1.FieldHistory
			Expect(proto.Unmarshal(rc.GetValue(), &rec)).To(Succeed())
			out = append(out, &rec)
		}
		return out
	}
	masks := func(recs []*syncplanev1.FieldHistory) [][]string {
		var out [][]string
		for _, r := range recs {
			out = append(out, r.GetMask().GetPaths())
		}
		return out
	}

	setEmail := func() {
		GinkgoHelper()
		results := f.push("owner", "c-owner", patchMutation("c-owner", 1, "staff", "s1",
			staffRow("s1", "p1", "", map[string]string{"email": "ann@example.com"}), "contact"))
		Expect(results).To(HaveLen(1))
		Expect(results[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED), results[0].GetErrorMessage())
	}

	It("keeps a change of a oneof a guard hides out of the history the caller reads", func() {
		// Given s1 in p1 created without a contact, and owner (edit_projects at
		// p1) then setting its email to "ann@example.com"
		f.seed(staffRow("s1", "p1", "ann", nil))
		setEmail()
		Expect(masks(historyOf("owner", nil))).To(Equal([][]string{nil, {"contact"}}))

		// When guest, who reads s1 but not contact, reads the history of s1
		guest := historyOf("guest", nil)

		// Then guest reads the creation record alone, and a predicate on the
		// mask matches nothing
		Expect(masks(guest)).To(Equal([][]string{nil}))
		Expect(historyOf("guest", query.Has("mask.paths", "contact"))).To(BeEmpty())
	})

	It("clears a oneof a guard hides and names it by the oneof", func() {
		// Given s1 with email "ann@example.com"
		f.seed(staffRow("s1", "p1", "ann", map[string]string{"email": "ann@example.com"}))

		// When guest pulls
		req := connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws, ClientId: "c-guest"})
		as("guest", req.Header())
		resp, err := f.sync.Pull(f.ctx, req)
		Expect(err).NotTo(HaveOccurred())

		// Then s1 arrives without its email, and hidden names contact
		var rc *syncplanev1.RowChange
		for _, c := range resp.Msg.GetPatch().GetChanges() {
			if c.GetEntity() == "staff" && c.GetKey() == "s1" {
				rc = c
			}
		}
		Expect(rc).NotTo(BeNil())
		Expect(rc.GetHidden()).To(ContainElement("contact"))
		got := dynamicpb.NewMessage(staff)
		Expect(proto.Unmarshal(rc.GetValue(), got)).To(Succeed())
		Expect(got.WhichOneof(staff.Oneofs().ByName("contact"))).To(BeNil())
		Expect(got.Get(staff.Fields().ByName("name")).String()).To(Equal("ann"))
	})

	DescribeTable("refuses a query that names a member of a oneof a guard hides",
		func(req *syncplanev1.QueryRequest) {
			// Given s1 with email "ann@example.com"
			f.seed(staffRow("s1", "p1", "ann", map[string]string{"email": "ann@example.com"}))

			// When guest names email or phone
			req.WorkspaceId, req.Entity = ws, "staff"
			r := connect.NewRequest(req)
			as("guest", r.Header())
			_, err := f.sync.Query(f.ctx, r)

			// Then the query is refused
			Expect(connect.CodeOf(err)).To(Equal(connect.CodePermissionDenied), "%v", err)
		},
		Entry("a predicate on a member", &syncplanev1.QueryRequest{Predicate: query.Eq("email", "ann@example.com")}),
		Entry("a presence check on a member", &syncplanev1.QueryRequest{Predicate: query.IsSet("phone")}),
		Entry("an order on a member", &syncplanev1.QueryRequest{OrderBy: []*syncplanev1.OrderKey{query.Asc("email")}}),
		Entry("a grouping on a member", &syncplanev1.QueryRequest{
			GroupBy: []string{"email"}, Aggregate: syncplanev1.AggregateOp_AGGREGATE_OP_COUNT,
		}),
	)
})
