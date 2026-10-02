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
	"github.com/levelfourab/syncplane/engine/server"
)

// staffWithNested adds to Staff a message field `addr` (city, zip), a
// repeated string `codes` and a map<string, string> `notes`, each behind a
// guard that lets only edit_projects read and write it.
func staffWithNested(file *descriptorpb.FileDescriptorProto) {
	m := messageNamed(file, "Staff")
	str := descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
	opt := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	rep := descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()
	msg := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
	field := func(name string, n int32, label *descriptorpb.FieldDescriptorProto_Label, typ *descriptorpb.FieldDescriptorProto_Type, typeName string) *descriptorpb.FieldDescriptorProto {
		fd := &descriptorpb.FieldDescriptorProto{
			Name: proto.String(name), JsonName: proto.String(name), Number: proto.Int32(n), Label: label, Type: typ,
		}
		if typeName != "" {
			fd.TypeName = proto.String(typeName)
		}
		return fd
	}
	m.NestedType = append(m.NestedType,
		&descriptorpb.DescriptorProto{
			Name:  proto.String("Addr"),
			Field: []*descriptorpb.FieldDescriptorProto{field("city", 1, opt, str, ""), field("zip", 2, opt, str, "")},
		},
		&descriptorpb.DescriptorProto{
			Name:    proto.String("NotesEntry"),
			Field:   []*descriptorpb.FieldDescriptorProto{field("key", 1, opt, str, ""), field("value", 2, opt, str, "")},
			Options: &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)},
		},
	)
	m.Field = append(m.Field,
		field("addr", 30, opt, msg, ".syncplane.testdata.access.v1.Staff.Addr"),
		field("codes", 31, rep, str, ""),
		field("notes", 32, rep, msg, ".syncplane.testdata.access.v1.Staff.NotesEntry"),
	)
	access := proto.GetExtension(m.GetOptions(), syncplanev1.E_Entity).(*syncplanev1.EntityOptions).GetAccess()
	access.Guard = append(access.Guard, &syncplanev1.FieldGuard{
		Fields: []string{"addr", "codes", "notes"},
		Allow:  []*syncplanev1.AccessRule{{Read: true, Write: true, Requires: []string{"edit_projects"}}},
	})
}

var _ = Describe("Access model field guards over nested, repeated and map fields", func() {
	var (
		f     *accessFixture
		staff protoreflect.MessageDescriptor
	)

	BeforeEach(func() {
		f = &accessFixture{ctx: GinkgoT().Context(), store: newTestStore()}
		f.srv = mustNewServer(f.store, headerAuthenticator())
		_, err := f.srv.RegisterDescriptorSet(accessFileSet(staffWithNested))
		Expect(err).NotTo(HaveOccurred())
		meta, ok := f.srv.Entity("staff")
		Expect(ok).To(BeTrue())
		staff = meta.Descriptor
		fd := staff.ParentFile()
		ts := httptest.NewServer(f.srv.HTTPHandler())
		DeferCleanup(ts.Close)
		f.sync = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)

		dynamic := func(msg proto.Message) proto.Message {
			out := dynamicpb.NewMessage(fd.Messages().ByName(msg.ProtoReflect().Descriptor().Name()))
			Expect(proto.Unmarshal(mustMarshal(msg), out)).To(Succeed())
			return out
		}
		f.seed(
			dynamic(&accessv1.Project{Id: "p1", Name: "one"}),
			dynamic(&accessv1.ProjectMember{Id: "pm-guest", Project: project("p1"), UserId: "guest", Role: "project_reader"}),
			dynamic(&accessv1.ProjectMember{Id: "pm-padmin", Project: project("p1"), UserId: "padmin", Role: "project_admin"}),
			dynamic(&accessv1.ProjectMember{Id: "pm-owner", Project: project("p1"), UserId: "owner", Role: "project_owner"}),
		)
		f.seed(nestedStaffRow(staff, "s1", "ann", "Oslo", []string{"c-7"}, map[string]string{"k": "secret"}))
	})

	field := func(name string) protoreflect.FieldDescriptor { return staff.Fields().ByName(protoreflect.Name(name)) }
	stored := func() *dynamicpb.Message {
		GinkgoHelper()
		out := dynamicpb.NewMessage(staff)
		Expect(f.srv.View(server.Trusted(f.ctx), ws, func(tx *server.ViewTx) error {
			found, _, err := tx.Get(out, "s1")
			Expect(found).To(BeTrue())
			return err
		})).To(Succeed())
		return out
	}
	queryAs := func(user string, req *syncplanev1.QueryRequest) error {
		req.WorkspaceId, req.Entity = ws, "staff"
		r := connect.NewRequest(req)
		as(user, r.Header())
		_, err := f.sync.Query(f.ctx, r)
		return err
	}
	pushAs := func(user string, mut *syncplanev1.Mutation) *syncplanev1.MutationResult {
		GinkgoHelper()
		results := f.push(user, "c-"+user, mut)
		Expect(results).To(HaveLen(1))
		return results[0]
	}

	Context("given s1 with addr.city Oslo, codes [c-7] and notes {k: secret}, all hidden from guest and padmin", func() {
		It("then guest pulls s1 with addr, codes and notes cleared and named hidden", func() {
			// When
			req := connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws, ClientId: "c-guest"})
			as("guest", req.Header())
			resp, err := f.sync.Pull(f.ctx, req)
			Expect(err).NotTo(HaveOccurred())

			// Then
			var rc *syncplanev1.RowChange
			for _, c := range resp.Msg.GetPatch().GetChanges() {
				if c.GetEntity() == "staff" {
					rc = c
				}
			}
			Expect(rc).NotTo(BeNil())
			Expect(rc.GetHidden()).To(ContainElements("addr", "codes", "notes"))
			got := dynamicpb.NewMessage(staff)
			Expect(proto.Unmarshal(rc.GetValue(), got)).To(Succeed())
			Expect(got.Has(field("addr"))).To(BeFalse())
			Expect(got.Get(field("codes")).List().Len()).To(BeZero())
			Expect(got.Get(field("notes")).Map().Len()).To(BeZero())
		})

		DescribeTable("then guest's query that names a hidden nested, repeated or map path is refused",
			func(req *syncplanev1.QueryRequest) {
				// When
				err := queryAs("guest", req)

				// Then
				Expect(connect.CodeOf(err)).To(Equal(connect.CodePermissionDenied), "%v", err)
			},
			Entry("a predicate on addr.city", &syncplanev1.QueryRequest{Predicate: query.Eq("addr.city", "Oslo")}),
			Entry("an order on addr.city", &syncplanev1.QueryRequest{OrderBy: []*syncplanev1.OrderKey{query.Asc("addr.city")}}),
			Entry("a membership test on codes", &syncplanev1.QueryRequest{Predicate: query.Has("codes", "c-7")}),
			Entry("a presence check on addr", &syncplanev1.QueryRequest{Predicate: query.IsSet("addr")}),
		)

		It("then padmin's PUT of s1 without the guarded fields keeps them stored", func() {
			// When
			next := nestedStaffRow(staff, "s1", "anna", "", nil, nil)
			r := pushAs("padmin", putMutation("c-padmin", 1, "staff", "s1", next))

			// Then
			Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED), r.GetErrorMessage())
			got := stored()
			Expect(got.Get(field("name")).String()).To(Equal("anna"))
			Expect(got.Get(field("addr")).Message().Get(field("addr").Message().Fields().ByName("city")).String()).To(Equal("Oslo"))
			Expect(got.Get(field("codes")).List().Len()).To(Equal(1))
			Expect(got.Get(field("notes")).Map().Len()).To(Equal(1))
		})

		DescribeTable("then padmin's PATCH with a mask that reaches into a guarded field is refused and stores nothing",
			func(path string) {
				// When
				next := nestedStaffRow(staff, "s1", "ann", "Bergen", nil, map[string]string{"k": "public"})
				r := pushAs("padmin", patchMutation("c-padmin", 1, "staff", "s1", next, path))

				// Then
				Expect(r.GetStatus()).NotTo(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
				got := stored()
				Expect(got.Get(field("addr")).Message().Get(field("addr").Message().Fields().ByName("city")).String()).To(Equal("Oslo"))
				Expect(got.Get(field("notes")).Map().Get(protoreflect.ValueOfString("k").MapKey()).String()).To(Equal("secret"))
			},
			Entry("addr.city", "addr.city"),
			Entry("notes.k", "notes.k"),
		)
	})
})

// nestedStaffRow builds a Staff row of the extended schema in p1.
func nestedStaffRow(staff protoreflect.MessageDescriptor, id, name, city string, codes []string, notes map[string]string) *dynamicpb.Message {
	msg := dynamicpb.NewMessage(staff)
	fields := staff.Fields()
	msg.Set(fields.ByName("id"), protoreflect.ValueOfString(id))
	p := dynamicpb.NewMessage(fields.ByName("project").Message())
	p.Set(p.Descriptor().Fields().ByName("id"), protoreflect.ValueOfString("p1"))
	msg.Set(fields.ByName("project"), protoreflect.ValueOfMessage(p))
	msg.Set(fields.ByName("name"), protoreflect.ValueOfString(name))
	if city != "" {
		a := dynamicpb.NewMessage(fields.ByName("addr").Message())
		a.Set(a.Descriptor().Fields().ByName("city"), protoreflect.ValueOfString(city))
		msg.Set(fields.ByName("addr"), protoreflect.ValueOfMessage(a))
	}
	list := msg.Mutable(fields.ByName("codes")).List()
	for _, c := range codes {
		list.Append(protoreflect.ValueOfString(c))
	}
	m := msg.Mutable(fields.ByName("notes")).Map()
	for k, v := range notes {
		m.Set(protoreflect.ValueOfString(k).MapKey(), protoreflect.ValueOfString(v))
	}
	return msg
}
