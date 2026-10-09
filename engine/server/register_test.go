package server_test

import (
	"context"
	"path/filepath"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

// RegisterDescriptorSet is the Go counterpart of the TS schema builder's
// "the server ships its schema" round-trip: a self-contained FileDescriptorSet
// (descriptor.proto + options.proto + synthesized entity files) is ingested
// and served full CRUD with no generated Go. These specs assemble the same
// bundle the TS builder emits, in Go, from BuildDynamicMessageType.
var _ = Describe("RegisterDescriptorSet", func() {
	noteSpec := server.DynamicEntity{
		Name:      "note",
		KeyFields: []string{"id"},
		Fields: []server.DynamicField{
			{Name: "id", Number: 1, Kind: protoreflect.StringKind},
			{Name: "title", Number: 2, Kind: protoreflect.StringKind},
			{Name: "priority", Number: 3, Kind: protoreflect.Int64Kind},
			{Name: "tags", Number: 5, Kind: protoreflect.StringKind, Repeated: true},
			{Name: "blob", Number: 6, Kind: protoreflect.BytesKind, Opaque: true},
		},
	}

	var (
		ctx context.Context
		srv *server.Server
		mt  protoreflect.MessageType
	)

	BeforeEach(func() {
		ctx = context.Background()
		srv = newBareServer()
		var err error
		mt, err = server.BuildDynamicMessageType(noteSpec)
		Expect(err).NotTo(HaveOccurred())
	})

	push := func(muts ...*syncplanev1.Mutation) {
		GinkgoHelper()
		_, err := srv.Push(ctx, connect.NewRequest(pushOf(ws, muts)))
		Expect(err).NotTo(HaveOccurred())
	}
	pull := func() *syncplanev1.PullResponse {
		GinkgoHelper()
		resp, err := srv.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws}))
		Expect(err).NotTo(HaveOccurred())
		return resp.Msg
	}

	It("ingests builder-style bytes and serves CRUD", func() {
		names, err := srv.RegisterDescriptorSet(descriptorSetBytes(noteSpec))
		Expect(err).NotTo(HaveOccurred())
		Expect(names).To(Equal([]string{"note"}))

		val := buildNote(mt, map[string]any{
			"id": "n1", "title": "Buy milk", "priority": int64(5), "tags": []string{"a", "b"},
		})
		push(putMutation("c1", 1, "note", "n1", val))

		rows := map[string]*syncplanev1.RowChange{}
		for _, c := range pull().GetPatch().GetChanges() {
			rows[c.GetKey()] = c
		}
		Expect(rows).To(HaveKey("n1"))
		got := decodeNote(mt, rows["n1"].GetValue())
		Expect(got["title"]).To(Equal("Buy milk"))
		Expect(got["priority"]).To(Equal(int64(5)))
		Expect(got["tags"]).To(Equal([]string{"a", "b"}))
	})

	It("skips messages without (syncplane.v1.entity) and rejects an empty set", func() {
		_, err := srv.RegisterDescriptorSet(descriptorSetBytes())
		Expect(err).To(MatchError(ContainSubstring("no (syncplane.v1.entity) messages")))
	})

	It("advertises a stable revision derived from the bytes", func() {
		_, err := srv.RegisterDescriptorSet(descriptorSetBytes(noteSpec))
		Expect(err).NotTo(HaveOccurred())
		rev := pull().GetSchemaRevision()
		Expect(rev).NotTo(BeEmpty())

		// Re-registering the identical set is a no-op: same revision.
		_, err = srv.RegisterDescriptorSet(descriptorSetBytes(noteSpec))
		Expect(err).NotTo(HaveOccurred())
		Expect(pull().GetSchemaRevision()).To(Equal(rev))
	})

	It("accepts an additive evolution and bumps the revision", func() {
		_, err := srv.RegisterDescriptorSet(descriptorSetBytes(noteSpec))
		Expect(err).NotTo(HaveOccurred())
		before := pull().GetSchemaRevision()

		evolved := noteSpec
		evolved.Fields = append(append([]server.DynamicField{}, noteSpec.Fields...),
			server.DynamicField{Name: "archived", Number: 7, Kind: protoreflect.BoolKind})
		names, err := srv.RegisterDescriptorSet(descriptorSetBytes(evolved))
		Expect(err).NotTo(HaveOccurred())
		Expect(names).To(Equal([]string{"note"}))
		Expect(pull().GetSchemaRevision()).NotTo(Equal(before), "additive change advances the revision")

		// The new field is syncable: build a row through the evolved type.
		emt, err := server.BuildDynamicMessageType(evolved)
		Expect(err).NotTo(HaveOccurred())
		push(putMutation("c1", 1, "note", "n9", buildNote(emt, map[string]any{"id": "n9", "title": "x"})))
		rows := map[string]*syncplanev1.RowChange{}
		for _, c := range pull().GetPatch().GetChanges() {
			rows[c.GetKey()] = c
		}
		Expect(rows).To(HaveKey("n9"))
	})

	DescribeTable("rejects a conflicting (non-additive) change",
		func(mutate func(s *server.DynamicEntity), msg string) {
			_, err := srv.RegisterDescriptorSet(descriptorSetBytes(noteSpec))
			Expect(err).NotTo(HaveOccurred())
			rev := pull().GetSchemaRevision()

			conflicting := noteSpec
			conflicting.Fields = append([]server.DynamicField{}, noteSpec.Fields...)
			mutate(&conflicting)
			_, err = srv.RegisterDescriptorSet(descriptorSetBytes(conflicting))
			Expect(err).To(MatchError(ContainSubstring(msg)))
			Expect(pull().GetSchemaRevision()).To(Equal(rev), "a rejected change does not partially apply")
		},
		Entry("retyped field", func(s *server.DynamicEntity) {
			s.Fields[1].Kind = protoreflect.Int64Kind // title string -> int64
		}, "retyped"),
		Entry("removed field", func(s *server.DynamicEntity) {
			s.Fields = s.Fields[:len(s.Fields)-1] // drop blob
		}, "was removed"),
		Entry("changed key fields", func(s *server.DynamicEntity) {
			s.KeyFields = []string{"title"}
		}, "key_fields changed"),
	)
})

// newBareServer builds a server on a fresh SQLite store with NO entities
// registered, so a spec can register its own schema at runtime.
func newBareServer(opts ...server.Option) *server.Server {
	GinkgoHelper()
	store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "register.db"))
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
	srv, err := server.New(store, opts...)
	Expect(err).NotTo(HaveOccurred())
	return srv
}

// descriptorSetBytes assembles the self-contained FileDescriptorSet the TS
// builder produces — google/protobuf/descriptor.proto, then syncplane/v1/options.proto,
// then ONE synthesized file holding every entity — and marshals it, exactly the
// bytes a Go server ingests via RegisterDescriptorSet. Building all entities
// into a single file mirrors the TS builder and lets cross-entity references
// resolve.
func descriptorSetBytes(specs ...server.DynamicEntity) []byte {
	GinkgoHelper()
	descFile := (&descriptorpb.FileDescriptorProto{}).ProtoReflect().Descriptor().ParentFile()
	set := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{
			protodesc.ToFileDescriptorProto(descFile),
			protodesc.ToFileDescriptorProto(syncplanev1.File_syncplane_v1_query_proto),
			protodesc.ToFileDescriptorProto(syncplanev1.File_syncplane_v1_options_proto),
		},
	}
	if len(specs) > 0 {
		fd, err := server.BuildDynamicFile(specs...)
		Expect(err).NotTo(HaveOccurred())
		set.File = append(set.File, protodesc.ToFileDescriptorProto(fd))
	}
	raw, err := proto.Marshal(set)
	Expect(err).NotTo(HaveOccurred())
	return raw
}

// An app that vendors syncplane/v1/options.proto and generates its own Go package
// for it produces descriptors whose (syncplane.v1.entity) is present on the wire but
// bound to an extension type this build never sees. RegisterEntity has to
// accept those, and when it genuinely cannot read an annotation it has to say
// which of the two causes it is looking at.
var _ = Describe("RegisterEntity option resolution", func() {
	var srv *server.Server

	BeforeEach(func() {
		store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "opts.db"))
		Expect(err).NotTo(HaveOccurred())
		srv, err = server.New(store)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(srv.Close)
	})

	It("registers an entity whose options only survive as uninterpreted bytes", func() {
		msg := dynamicpb.NewMessage(foreignThing(uninterpretedOptions(&syncplanev1.EntityOptions{
			Name:      "thing",
			KeyFields: []string{"id"},
		})))

		Expect(srv.RegisterEntity(msg)).To(Succeed())
		// A second attempt proves it landed under the recovered name.
		Expect(srv.RegisterEntity(msg)).To(MatchError(ContainSubstring(`entity "thing" already registered`)))
	})

	It("points at the go_package mismatch when options are present but unreadable", func() {
		unreadable := &descriptorpb.MessageOptions{}
		raw := protowire.AppendTag(nil, 50999, protowire.BytesType)
		unreadable.ProtoReflect().SetUnknown(protowire.AppendString(raw, "x"))

		err := srv.RegisterEntity(dynamicpb.NewMessage(foreignThing(unreadable)))
		Expect(err).To(MatchError(ContainSubstring("vendored syncplane/v1/options.proto")))
		Expect(err).To(MatchError(ContainSubstring("github.com/levelfourab/syncplane/engine/gen/syncplane/v1")))
	})

	It("tells an unannotated message to annotate itself", func() {
		err := srv.RegisterEntity(dynamicpb.NewMessage(foreignThing(nil)))
		Expect(err).To(MatchError(ContainSubstring("annotate the message in your .proto")))
		Expect(err).NotTo(MatchError(ContainSubstring("go_package")))
	})
})

// uninterpretedOptions encodes (syncplane.v1.entity) to its wire form and hands the
// bytes back as options this build cannot interpret — byte-for-byte what a
// foreign extension type produces.
func uninterpretedOptions(eo *syncplanev1.EntityOptions) *descriptorpb.MessageOptions {
	GinkgoHelper()
	encoded := &descriptorpb.MessageOptions{}
	proto.SetExtension(encoded, syncplanev1.E_Entity, eo)
	raw, err := proto.Marshal(encoded)
	Expect(err).NotTo(HaveOccurred())

	foreign := &descriptorpb.MessageOptions{}
	foreign.ProtoReflect().SetUnknown(raw)
	return foreign
}

// foreignThing builds a one-field message carrying mopts, standing in for an
// entity generated outside this module.
func foreignThing(mopts *descriptorpb.MessageOptions) protoreflect.MessageDescriptor {
	GinkgoHelper()
	fd, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:    proto.String("foreign/v1/thing.proto"),
		Package: proto.String("foreign.v1"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name:    proto.String("Thing"),
			Options: mopts,
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name:     proto.String("id"),
				Number:   proto.Int32(1),
				JsonName: proto.String("id"),
				Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:     descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
			}},
		}},
	}, nil)
	Expect(err).NotTo(HaveOccurred())
	return fd.Messages().Get(0)
}
