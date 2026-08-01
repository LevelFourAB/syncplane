package server_test

import (
	"context"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// The dynamic tier proves the engine is descriptor-driven end to end: an entity
// is declared at runtime, built into a dynamicpb message type, and served full
// CRUD through Push/Pull/Query with NO generated Go for the entity. Every value
// in these specs is built and read back through protoreflect on the dynamic
// descriptor.
var _ = Describe("Dynamic entity", func() {
	// noteSpec is a small CRUD schema: a string key, scalar fields exercising
	// per-field LWW, a repeated scalar, and an opaque bytes blob.
	noteSpec := server.DynamicEntity{
		Name:      "note",
		KeyFields: []string{"id"},
		Fields: []server.DynamicField{
			{Name: "id", Number: 1, Kind: protoreflect.StringKind},
			{Name: "title", Number: 2, Kind: protoreflect.StringKind},
			{Name: "priority", Number: 3, Kind: protoreflect.Int64Kind},
			{Name: "done", Number: 4, Kind: protoreflect.BoolKind},
			{Name: "tags", Number: 5, Kind: protoreflect.StringKind, Repeated: true},
			{Name: "blob", Number: 6, Kind: protoreflect.BytesKind, Opaque: true},
		},
	}

	Describe("BuildDynamicMessageType", func() {
		It("resolves (syncplane.v1.entity) options off the dynamic descriptor", func() {
			mt, err := server.BuildDynamicMessageType(noteSpec)
			Expect(err).NotTo(HaveOccurred())

			md := mt.Descriptor()
			Expect(string(md.FullName())).To(Equal("sync.dynamic.Note"))

			mopts, ok := md.Options().(*descriptorpb.MessageOptions)
			Expect(ok).To(BeTrue())
			eo, _ := proto.GetExtension(mopts, syncplanev1.E_Entity).(*syncplanev1.EntityOptions)
			Expect(eo).NotTo(BeNil())
			Expect(eo.GetName()).To(Equal("note"))
			Expect(eo.GetKeyFields()).To(Equal([]string{"id"}))
		})

		It("resolves (syncplane.v1.field_behavior) opaque off a dynamic field", func() {
			mt, err := server.BuildDynamicMessageType(noteSpec)
			Expect(err).NotTo(HaveOccurred())

			fd := mt.Descriptor().Fields().ByName("blob")
			Expect(fd).NotTo(BeNil())
			fopts, ok := fd.Options().(*descriptorpb.FieldOptions)
			Expect(ok).To(BeTrue())
			fb, _ := proto.GetExtension(fopts, syncplanev1.E_FieldBehavior).(*syncplanev1.FieldBehavior)
			Expect(fb.GetOpaque()).To(BeTrue())
		})

		It("resolves (syncplane.v1.field_behavior) text off a dynamic field", func() {
			mt, err := server.BuildDynamicMessageType(server.DynamicEntity{
				Name: "doc", KeyFields: []string{"id"},
				Fields: []server.DynamicField{
					{Name: "id", Number: 1, Kind: protoreflect.StringKind},
					{Name: "body", Number: 2, Kind: protoreflect.BytesKind, Text: true},
				},
			})
			Expect(err).NotTo(HaveOccurred())

			fd := mt.Descriptor().Fields().ByName("body")
			Expect(fd).NotTo(BeNil())
			fopts, ok := fd.Options().(*descriptorpb.FieldOptions)
			Expect(ok).To(BeTrue())
			fb, _ := proto.GetExtension(fopts, syncplanev1.E_FieldBehavior).(*syncplanev1.FieldBehavior)
			Expect(fb.GetText()).To(BeTrue())
			Expect(fb.GetOpaque()).To(BeFalse())
		})

		It("produces a message whose deterministic marshal is stable", func() {
			mt, err := server.BuildDynamicMessageType(noteSpec)
			Expect(err).NotTo(HaveOccurred())
			val := buildNote(mt, map[string]any{
				"id": "n1", "title": "x", "priority": int64(3), "tags": []string{"a", "b"},
			})
			mo := proto.MarshalOptions{Deterministic: true}
			a, err := mo.Marshal(val)
			Expect(err).NotTo(HaveOccurred())
			b, err := mo.Marshal(val)
			Expect(err).NotTo(HaveOccurred())
			Expect(a).To(Equal(b))
		})

		DescribeTable("rejects invalid specs",
			func(spec server.DynamicEntity, msg string) {
				_, err := server.BuildDynamicMessageType(spec)
				Expect(err).To(MatchError(ContainSubstring(msg)))
			},
			Entry("no name", server.DynamicEntity{KeyFields: []string{"id"}}, "needs a name"),
			Entry("no key fields", server.DynamicEntity{Name: "x"}, "no key_fields"),
			Entry("unknown key field",
				server.DynamicEntity{Name: "x", KeyFields: []string{"missing"},
					Fields: []server.DynamicField{{Name: "id", Number: 1, Kind: protoreflect.StringKind}}},
				`key field "missing" is not a declared field`),
			Entry("duplicate field number",
				server.DynamicEntity{Name: "x", KeyFields: []string{"id"}, Fields: []server.DynamicField{
					{Name: "id", Number: 1, Kind: protoreflect.StringKind},
					{Name: "other", Number: 1, Kind: protoreflect.StringKind},
				}}, "duplicate field number"),
			Entry("message-typed field",
				server.DynamicEntity{Name: "x", KeyFields: []string{"id"}, Fields: []server.DynamicField{
					{Name: "id", Number: 1, Kind: protoreflect.MessageKind},
				}}, "unsupported kind"),
			Entry("opaque on non-bytes field",
				server.DynamicEntity{Name: "x", KeyFields: []string{"id"}, Fields: []server.DynamicField{
					{Name: "id", Number: 1, Kind: protoreflect.StringKind, Opaque: true},
				}}, "opaque is only valid on a singular bytes field"),
			Entry("text on non-bytes field",
				server.DynamicEntity{Name: "x", KeyFields: []string{"id"}, Fields: []server.DynamicField{
					{Name: "id", Number: 1, Kind: protoreflect.StringKind},
					{Name: "body", Number: 2, Kind: protoreflect.StringKind, Text: true},
				}}, "text is only valid on a singular non-opaque bytes field"),
			Entry("text on a repeated field",
				server.DynamicEntity{Name: "x", KeyFields: []string{"id"}, Fields: []server.DynamicField{
					{Name: "id", Number: 1, Kind: protoreflect.StringKind},
					{Name: "body", Number: 2, Kind: protoreflect.BytesKind, Text: true, Repeated: true},
				}}, "text is only valid on a singular non-opaque bytes field"),
		)
	})

	Describe("full CRUD round-trip with no generated Go", func() {
		var (
			ctx context.Context
			srv *server.Server
			mt  protoreflect.MessageType
		)

		BeforeEach(func() {
			ctx = context.Background()
			srv = newTestServer()
			Expect(srv.RegisterDynamicEntity(noteSpec)).To(Succeed())
			var err error
			mt, err = server.BuildDynamicMessageType(noteSpec)
			Expect(err).NotTo(HaveOccurred())
		})

		push := func(muts ...*syncplanev1.Mutation) {
			GinkgoHelper()
			_, err := srv.Push(ctx, connect.NewRequest(pushOf(
				ws,
				muts)))
			Expect(err).NotTo(HaveOccurred())
		}

		pullRows := func() map[string]*syncplanev1.RowChange {
			GinkgoHelper()
			resp, err := srv.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws}))
			Expect(err).NotTo(HaveOccurred())
			out := map[string]*syncplanev1.RowChange{}
			for _, c := range resp.Msg.GetPatch().GetChanges() {
				out[c.GetKey()] = c
			}
			return out
		}

		It("serves put -> pull, decoding rows through the dynamic type", func() {
			val := buildNote(mt, map[string]any{
				"id": "n1", "title": "Buy milk", "priority": int64(5),
				"tags": []string{"home", "urgent"},
			})
			push(putMutation("c1", 1, "note", "n1", val))

			rows := pullRows()
			Expect(rows).To(HaveKey("n1"))
			got := decodeNote(mt, rows["n1"].GetValue())
			Expect(got["title"]).To(Equal("Buy milk"))
			Expect(got["priority"]).To(Equal(int64(5)))
			Expect(got["tags"]).To(Equal([]string{"home", "urgent"}))
		})

		It("applies a field-mask PATCH as per-field LWW", func() {
			push(putMutation("c1", 1, "note", "n1", buildNote(mt, map[string]any{
				"id": "n1", "title": "original", "priority": int64(1),
			})))
			// Patch only priority; title must survive untouched (per-field LWW).
			patch := buildNote(mt, map[string]any{"id": "n1", "priority": int64(9)})
			push(patchMutation("c1", 2, "note", "n1", patch, "priority"))

			got := decodeNote(mt, pullRows()["n1"].GetValue())
			Expect(got["priority"]).To(Equal(int64(9)))
			Expect(got["title"]).To(Equal("original"), "untouched field preserved across PATCH")
		})

		It("deletes a row, leaving a tombstone", func() {
			push(putMutation("c1", 1, "note", "n1", buildNote(mt, map[string]any{"id": "n1", "title": "x"})))
			push(deleteMutation("c1", 2, "note", "n1"))

			rows := pullRows()
			Expect(rows).To(HaveKey("n1"))
			Expect(rows["n1"].GetTombstone()).To(BeTrue())
		})

		It("evaluates a predicate Query against the dynamic descriptor", func() {
			push(
				putMutation("c1", 1, "note", "n1", buildNote(mt, map[string]any{"id": "n1", "title": "a", "priority": int64(1)})),
				putMutation("c1", 2, "note", "n2", buildNote(mt, map[string]any{"id": "n2", "title": "b", "priority": int64(7)})),
				putMutation("c1", 3, "note", "n3", buildNote(mt, map[string]any{"id": "n3", "title": "c", "priority": int64(5)})),
			)

			resp, err := srv.Query(ctx, connect.NewRequest(&syncplanev1.QueryRequest{
				WorkspaceId: ws,
				Entity:      "note",
				Predicate: &syncplanev1.Condition{Kind: &syncplanev1.Condition_Cmp{Cmp: &syncplanev1.CmpCond{
					Path:  []string{"priority"},
					Op:    syncplanev1.CmpOp_CMP_OP_GTE,
					Value: &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_IntValue{IntValue: 5}},
				}}},
				OrderBy: []*syncplanev1.OrderKey{{Path: []string{"priority"}, Dir: syncplanev1.Direction_DIRECTION_DESC}},
			}))
			Expect(err).NotTo(HaveOccurred())

			rows := resp.Msg.GetRows()
			Expect(rows).To(HaveLen(2), "only priority >= 5 match")
			Expect(rows[0].GetKey()).To(Equal("n2"), "ordered priority DESC")
			Expect(rows[1].GetKey()).To(Equal("n3"))
		})

		It("rejects a second registration of the same entity name", func() {
			Expect(srv.RegisterDynamicEntity(noteSpec)).To(MatchError(ContainSubstring("already registered")))
		})
	})
})

// buildNote constructs a dynamic note message from a field map and returns it
// as a proto.Message ready to marshal. Values must match the field kind:
// string, int64, bool, []string (repeated string), or []byte.
func buildNote(mt protoreflect.MessageType, fields map[string]any) proto.Message {
	GinkgoHelper()
	m := mt.New()
	fds := mt.Descriptor().Fields()
	for name, v := range fields {
		fd := fds.ByName(protoreflect.Name(name))
		Expect(fd).NotTo(BeNil(), "unknown field %q", name)
		switch val := v.(type) {
		case string:
			m.Set(fd, protoreflect.ValueOfString(val))
		case int64:
			m.Set(fd, protoreflect.ValueOfInt64(val))
		case bool:
			m.Set(fd, protoreflect.ValueOfBool(val))
		case []byte:
			m.Set(fd, protoreflect.ValueOfBytes(val))
		case []string:
			list := m.Mutable(fd).List()
			for _, s := range val {
				list.Append(protoreflect.ValueOfString(s))
			}
		default:
			Fail("unsupported test value type for field " + name)
		}
	}
	return m.Interface()
}

// decodeNote unmarshals a stored note value back into a plain map via the
// dynamic descriptor, mirroring buildNote's value types.
func decodeNote(mt protoreflect.MessageType, value []byte) map[string]any {
	GinkgoHelper()
	msg := mt.New().Interface()
	Expect(proto.Unmarshal(value, msg)).To(Succeed())
	out := map[string]any{}
	m := msg.ProtoReflect()
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsList() && fd.Kind() == protoreflect.StringKind:
			list := v.List()
			ss := make([]string, list.Len())
			for i := 0; i < list.Len(); i++ {
				ss[i] = list.Get(i).String()
			}
			out[string(fd.Name())] = ss
		case fd.Kind() == protoreflect.StringKind:
			out[string(fd.Name())] = v.String()
		case fd.Kind() == protoreflect.Int64Kind:
			out[string(fd.Name())] = v.Int()
		case fd.Kind() == protoreflect.BoolKind:
			out[string(fd.Name())] = v.Bool()
		case fd.Kind() == protoreflect.BytesKind:
			out[string(fd.Name())] = v.Bytes()
		}
		return true
	})
	return out
}
