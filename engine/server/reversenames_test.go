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

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// A reverse relation takes its name from the field holding the reference
// alone: the field's reverse_relation, else <plural(source)>By<Field> in
// camelCase.
// Names are read back from the relation list an unknown follow segment
// reports, and a clash surfaces from the same request, the first to build the
// reference graph. client/test/unit/refs.test.ts pins the same rule and the
// same wording for the client's registry.
var _ = Describe("Reverse relation names", func() {
	ctx := context.Background()

	idField := server.DynamicField{Name: "id", Number: 1, Kind: protoreflect.StringKind}
	entityOf := func(name string, fields ...server.DynamicField) server.DynamicEntity {
		return server.DynamicEntity{Name: name, KeyFields: []string{"id"}, Fields: append([]server.DynamicField{idField}, fields...)}
	}
	user := entityOf("user")

	// followUnknown follows a segment no entity has, so the error lists the
	// relations the entity does have, or reports why the graph did not build.
	followUnknown := func(srv *server.Server, entity string) error {
		GinkgoHelper()
		_, err := srv.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId: ws,
			Subscriptions: []*syncplanev1.Subscription{{
				Entity: entity,
				Follow: &syncplanev1.FollowSpec{Paths: []*syncplanev1.RefPath{{Segments: []string{"nope"}}}},
			}},
		}))
		Expect(err).To(HaveOccurred())
		return err
	}
	registerDynamic := func(specs ...server.DynamicEntity) *server.Server {
		GinkgoHelper()
		srv := newBareServer()
		Expect(srv.RegisterDynamicEntities(specs...)).To(Succeed())
		return srv
	}

	It("names a reverse relation after its source entity and field", func() {
		srv := registerDynamic(user, entityOf("post", server.DynamicField{Name: "author", Number: 2, Ref: "user"}))
		Expect(followUnknown(srv, "user")).To(MatchError(ContainSubstring(`(relations: postsByAuthor)`)))
	})

	It("spells a snake_case entity and field in camelCase", func() {
		srv := registerDynamic(user, entityOf("task_list", server.DynamicField{Name: "created_by", Number: 2, Ref: "user"}))
		Expect(followUnknown(srv, "user")).To(MatchError(ContainSubstring(`(relations: taskListsByCreatedBy)`)))
	})

	// The rule exists for this: a name is a wire contract that deployed clients
	// follow by, so a reference added later must not rename the ones before it.
	It("keeps the name of an existing reference when another joins it", func() {
		srv := registerDynamic(user, entityOf("post",
			server.DynamicField{Name: "author", Number: 2, Ref: "user"},
			server.DynamicField{Name: "editor", Number: 3, Ref: "user"},
		))
		Expect(followUnknown(srv, "user")).To(MatchError(ContainSubstring(`(relations: postsByAuthor, postsByEditor)`)))
	})

	It("uses the name a field sets", func() {
		srv := registerDynamic(user, entityOf("post",
			server.DynamicField{Name: "author", Number: 2, Ref: "user"},
			server.DynamicField{Name: "editor", Number: 3, Ref: "user", ReverseRelation: "editedPosts"},
		))
		Expect(followUnknown(srv, "user")).To(MatchError(ContainSubstring(`(relations: editedPosts, postsByAuthor)`)))
	})

	It("refuses two references of one entity that set one name on one target", func() {
		srv := registerDynamic(user, entityOf("post",
			server.DynamicField{Name: "author", Number: 2, Ref: "user", ReverseRelation: "posts"},
			server.DynamicField{Name: "editor", Number: 3, Ref: "user", ReverseRelation: "posts"},
		))
		err := followUnknown(srv, "user")
		Expect(connect.CodeOf(err)).To(Equal(connect.CodeFailedPrecondition))
		Expect(err).To(MatchError(ContainSubstring(
			`reverse relation "posts" on entity "user" is named by both post.author and post.editor; ` +
				`set (syncplane.v1.field_behavior).reverse_relation on one of them to give it another name`)))
	})

	It("refuses a name that another entity's reference already takes on the target", func() {
		srv := registerDynamic(user,
			entityOf("post", server.DynamicField{Name: "author", Number: 2, Ref: "user"}),
			entityOf("comment", server.DynamicField{Name: "author", Number: 2, Ref: "user", ReverseRelation: "postsByAuthor"}),
		)
		Expect(followUnknown(srv, "user")).To(MatchError(ContainSubstring(
			`reverse relation "postsByAuthor" on entity "user" is named by both comment.author and post.author`)))
	})

	It("refuses a name that is not an identifier", func() {
		err := newBareServer().RegisterDynamicEntities(user, entityOf("post",
			server.DynamicField{Name: "author", Number: 2, Ref: "user", ReverseRelation: "authored.posts"}))
		Expect(err).To(MatchError(ContainSubstring(
			`field "author": reverse relation name "authored.posts" is not an identifier`)))
	})

	It("refuses a runtime field that names a reverse relation without a Ref", func() {
		err := newBareServer().RegisterDynamicEntities(entityOf("post",
			server.DynamicField{Name: "title", Number: 2, Kind: protoreflect.StringKind, ReverseRelation: "posts"}))
		Expect(err).To(MatchError(ContainSubstring(`field "title": reverse_relation is only valid on a Ref field`)))
	})

	// The shapes a runtime schema cannot declare: a oneof, a reference message
	// with a oneof, and the option on a field that is not a reference.
	Describe("in a hand-written schema", func() {
		entityOpts := func(name string) *descriptorpb.MessageOptions {
			o := &descriptorpb.MessageOptions{}
			proto.SetExtension(o, syncplanev1.E_Entity, &syncplanev1.EntityOptions{Name: name, KeyFields: []string{"id"}})
			return o
		}
		refOpts := func() *descriptorpb.MessageOptions {
			o := &descriptorpb.MessageOptions{}
			proto.SetExtension(o, syncplanev1.E_Reference, &syncplanev1.ReferenceOptions{})
			return o
		}
		reverse := func(fdp *descriptorpb.FieldDescriptorProto, name string) *descriptorpb.FieldDescriptorProto {
			o := &descriptorpb.FieldOptions{}
			proto.SetExtension(o, syncplanev1.E_FieldBehavior, &syncplanev1.FieldBehavior{ReverseRelation: name})
			fdp.Options = o
			return fdp
		}
		str := func(name string, number int32) *descriptorpb.FieldDescriptorProto {
			return &descriptorpb.FieldDescriptorProto{
				Name: proto.String(name), Number: proto.Int32(number),
				Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:  descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
			}
		}
		// msg declares a message-typed field; a non-negative oneof makes it a
		// member of that oneof.
		msg := func(name string, number int32, typeName string, oneof int32) *descriptorpb.FieldDescriptorProto {
			f := &descriptorpb.FieldDescriptorProto{
				Name: proto.String(name), Number: proto.Int32(number),
				Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
				TypeName: proto.String(typeName),
			}
			if oneof >= 0 {
				f.OneofIndex = proto.Int32(oneof)
			}
			return f
		}
		userMsg := &descriptorpb.DescriptorProto{
			Name: proto.String("User"), Options: entityOpts("user"),
			Field: []*descriptorpb.FieldDescriptorProto{str("id", 1)},
		}
		// register builds one file of the messages and registers the entities
		// among them, User first.
		register := func(msgs ...*descriptorpb.DescriptorProto) (*server.Server, error) {
			GinkgoHelper()
			fd, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
				Name:        proto.String("names/names.proto"),
				Package:     proto.String("names"),
				Syntax:      proto.String("proto3"),
				MessageType: append([]*descriptorpb.DescriptorProto{userMsg}, msgs...),
			}, nil)
			Expect(err).NotTo(HaveOccurred())
			srv := newBareServer()
			for i := 0; i < fd.Messages().Len(); i++ {
				md := fd.Messages().Get(i)
				if !proto.HasExtension(md.Options(), syncplanev1.E_Entity) {
					continue
				}
				if err := srv.RegisterEntity(dynamicpb.NewMessage(md)); err != nil {
					return nil, err
				}
			}
			return srv, nil
		}
		oneof := func(name string) []*descriptorpb.OneofDescriptorProto {
			return []*descriptorpb.OneofDescriptorProto{{Name: proto.String(name)}}
		}

		It("names each member of a oneof after the member", func() {
			srv, err := register(&descriptorpb.DescriptorProto{
				Name: proto.String("Task"), Options: entityOpts("task"), OneofDecl: oneof("parent"),
				Field: []*descriptorpb.FieldDescriptorProto{
					str("id", 1), msg("owner", 2, ".names.User", 0), msg("reviewer", 3, ".names.User", 0),
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(followUnknown(srv, "user")).To(MatchError(ContainSubstring(`(relations: tasksByOwner, tasksByReviewer)`)))
		})

		It("uses the name a member of a oneof sets", func() {
			srv, err := register(&descriptorpb.DescriptorProto{
				Name: proto.String("Task"), Options: entityOpts("task"), OneofDecl: oneof("parent"),
				Field: []*descriptorpb.FieldDescriptorProto{
					str("id", 1), msg("owner", 2, ".names.User", 0), reverse(msg("reviewer", 3, ".names.User", 0), "reviewedTasks"),
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(followUnknown(srv, "user")).To(MatchError(ContainSubstring(`(relations: reviewedTasks, tasksByOwner)`)))
		})

		// One field gives every case of its reference message one name, so no
		// option on the field can tell two cases with one target apart.
		It("refuses two cases of one reference message that point at one entity", func() {
			srv, err := register(
				&descriptorpb.DescriptorProto{
					Name: proto.String("PersonRef"), Options: refOpts(), OneofDecl: oneof("person"),
					Field: []*descriptorpb.FieldDescriptorProto{
						msg("human", 1, ".names.User", 0), msg("bot", 2, ".names.User", 0),
					},
				},
				&descriptorpb.DescriptorProto{
					Name: proto.String("Task"), Options: entityOpts("task"),
					Field: []*descriptorpb.FieldDescriptorProto{str("id", 1), msg("assignee", 2, ".names.PersonRef", -1)},
				},
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(followUnknown(srv, "user")).To(MatchError(ContainSubstring(
				`reverse relation "tasksByAssignee" on entity "user" is named by both task.assignee.bot and task.assignee.human; ` +
					`task.assignee gives all its cases one reverse relation name, so cases that point at one entity need fields of their own`)))
		})

		It("refuses reverse_relation on a field that is not a reference", func() {
			_, err := register(&descriptorpb.DescriptorProto{
				Name: proto.String("Task"), Options: entityOpts("task"),
				Field: []*descriptorpb.FieldDescriptorProto{str("id", 1), reverse(str("title", 2), "tasks")},
			})
			Expect(err).To(MatchError(ContainSubstring(
				`field "title" sets reverse_relation, but it is not a reference; only a reference has a reverse relation`)))
		})

		It("refuses reverse_relation inside a reference message", func() {
			_, err := register(
				&descriptorpb.DescriptorProto{
					Name: proto.String("UserRef"), Options: refOpts(),
					Field: []*descriptorpb.FieldDescriptorProto{reverse(msg("user", 1, ".names.User", -1), "tasks")},
				},
				&descriptorpb.DescriptorProto{
					Name: proto.String("Task"), Options: entityOpts("task"),
					Field: []*descriptorpb.FieldDescriptorProto{str("id", 1), msg("assignee", 2, ".names.UserRef", -1)},
				},
			)
			Expect(err).To(MatchError(ContainSubstring(
				`field "user" of names.UserRef sets reverse_relation, but only a reference field declared on an entity has a reverse relation`)))
		})
	})
})
