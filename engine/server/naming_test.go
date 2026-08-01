package server_test

import (
	"context"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// The naming rules (engine/internal/naming, mirrored in client/src/naming.ts)
// are enforced where a violation is still explainable: registration for names
// and key fields, mutate time for row keys. These specs pin the enforcement
// points rather than the rules themselves, which the naming suite covers.
var _ = Describe("Naming rules", func() {
	Describe("at registration", func() {
		var srv *server.Server

		BeforeEach(func() { srv = newBareServer() })

		It("rejects an entity name that is not an identifier", func() {
			// A slash is the damaging one: it forges an extra segment in the
			// client's `c/<entity>/<ws>/<key>` layout.
			Expect(srv.RegisterEntity(dynamicpb.NewMessage(namedThing("a/b", "id")))).
				To(MatchError(ContainSubstring("not an identifier")))
			Expect(srv.RegisterEntity(dynamicpb.NewMessage(namedThing("todo list", "id")))).
				To(MatchError(ContainSubstring("not an identifier")))
		})

		It("rejects a key field whose value renders differently in Go and JS", func() {
			Expect(srv.RegisterEntity(dynamicpb.NewMessage(namedThing("thing", "raw")))).
				To(MatchError(ContainSubstring("must be a string, integer, bool or enum")))
			Expect(srv.RegisterEntity(dynamicpb.NewMessage(namedThing("thing", "ratio")))).
				To(MatchError(ContainSubstring("must be a string, integer, bool or enum")))
		})

		It("accepts the kinds both sides render alike", func() {
			for _, kf := range []string{"id", "count", "flag"} {
				srv := newBareServer()
				Expect(srv.RegisterEntity(dynamicpb.NewMessage(namedThing("thing", kf)))).To(Succeed(), kf)
			}
		})
	})

	Describe("on a client mutation", func() {
		var srv *server.Server
		ctx := context.Background()

		BeforeEach(func() { srv = newTestServer() })

		// A rejection is an ack, so a client that minted a bad key drains its
		// queue instead of retrying it forever.
		push := func(m *syncplanev1.Mutation) *syncplanev1.MutationResult {
			GinkgoHelper()
			resp, err := srv.Push(ctx, connect.NewRequest(pushOf("ws-naming", []*syncplanev1.Mutation{m})))
			Expect(err).NotTo(HaveOccurred())
			return pushResults(resp.Msg)[0]
		}

		It("rejects the empty key a write with unset key fields derives", func() {
			result := push(putMutation("c1", 1, "todo", "", &todov1.Todo{Title: "x"}))
			Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(result.GetErrorCode()).To(Equal(server.CodeInvalidArgument))
			Expect(result.GetErrorMessage()).To(ContainSubstring("every key field must be set"))
		})

		It("rejects a key carrying a control character", func() {
			// U+001F separates the fields of the client's change tokens.
			result := push(deleteMutation("c1", 1, "todo", "t\x1f1"))
			Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(result.GetErrorCode()).To(Equal(server.CodeInvalidArgument))
			Expect(result.GetErrorMessage()).To(ContainSubstring(`\u001f`))
		})

		It("rejects an over-long key", func() {
			long := ""
			for len(long) <= 512 {
				long += "0123456789"
			}
			result := push(putMutation("c1", 1, "todo", long, &todov1.Todo{Id: long}))
			Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(result.GetErrorMessage()).To(ContainSubstring("the limit is 512"))
		})
	})

	Describe("on a trusted write", func() {
		It("rejects a key a mutator derived or supplied", func() {
			srv := newTestServer(server.WithMutator("bad_key", &todov1.Todo{},
				func(_ context.Context, tx *server.EntityTx, args proto.Message) error {
					return tx.Put(args.(*todov1.Todo))
				}))
			ctx := context.Background()

			// Id unset, so keyOf derives "" — the mutator's bug, surfaced as a
			// rejection rather than a row filed under the entity prefix.
			err := srv.Invoke(ctx, "ws-naming", "bad_key", &todov1.Todo{Title: "no id"})
			Expect(err).To(HaveOccurred())
			rej, ok := server.AsRejection(err)
			Expect(ok).To(BeTrue())
			Expect(rej.Code).To(Equal(server.CodeInvalidArgument))
			Expect(rej.Message).To(ContainSubstring("every key field must be set"))
		})
	})
})

// namedThing builds a message annotated as an entity called name and keyed by
// keyField, standing in for a schema an app might declare. Its fields cover the
// kinds the key rules admit and refuse.
func namedThing(name, keyField string) protoreflect.MessageDescriptor {
	GinkgoHelper()
	opts := &descriptorpb.MessageOptions{}
	proto.SetExtension(opts, syncplanev1.E_Entity, &syncplanev1.EntityOptions{
		Name:      name,
		KeyFields: []string{keyField},
	})
	field := func(n string, num int32, t descriptorpb.FieldDescriptorProto_Type) *descriptorpb.FieldDescriptorProto {
		return &descriptorpb.FieldDescriptorProto{
			Name:     proto.String(n),
			Number:   proto.Int32(num),
			JsonName: proto.String(n),
			Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			Type:     t.Enum(),
		}
	}
	fd, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:    proto.String("naming/v1/thing.proto"),
		Package: proto.String("naming.v1"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name:    proto.String("Thing"),
			Options: opts,
			Field: []*descriptorpb.FieldDescriptorProto{
				field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING),
				field("count", 2, descriptorpb.FieldDescriptorProto_TYPE_INT64),
				field("flag", 3, descriptorpb.FieldDescriptorProto_TYPE_BOOL),
				field("raw", 4, descriptorpb.FieldDescriptorProto_TYPE_BYTES),
				field("ratio", 5, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE),
			},
		}},
	}, nil)
	Expect(err).NotTo(HaveOccurred())
	return fd.Messages().Get(0)
}
