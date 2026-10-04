package server_test

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// roomEntities registers the owner-rule schema of the access test proto:
// rooms with members, and posts, drafts and tickets below them, whose rules
// check who wrote or holds a row, and settings that each caller has one of.
func roomEntities() server.Option {
	return server.WithEntities(
		&accessv1.Room{}, &accessv1.RoomMember{}, &accessv1.Post{},
		&accessv1.Draft{}, &accessv1.Ticket{}, &accessv1.Setting{},
	)
}

func room(id string) *accessv1.Room { return &accessv1.Room{Id: id} }

func roomMember(id, roomID, user, role string) *accessv1.RoomMember {
	return &accessv1.RoomMember{Id: id, Room: room(roomID), UserId: user, Role: role}
}

// lackAll succeeds when a list holds none of the given elements.
func lackAll(elements ...any) OmegaMatcher {
	matchers := make([]OmegaMatcher, len(elements))
	for i, e := range elements {
		matchers[i] = Not(ContainElement(e))
	}
	return SatisfyAll(matchers...)
}

// seedRooms writes the rows every owner-rule spec starts from:
//
//   - viewer views r1, ann and bob post in r1, mod moderates r1, and ann
//     views r2;
//   - a post by ann and one by bob in r1;
//   - a draft by ann and one by bob in r1, and one by bob in r2;
//   - a ticket in r1 assigned to ann;
//   - the settings of ann and of bob.
func (f *accessFixture) seedRooms() {
	GinkgoHelper()
	f.seed(
		&accessv1.Room{Id: "r1", Name: "one"}, &accessv1.Room{Id: "r2", Name: "two"},
		roomMember("rm-viewer", "r1", "viewer", "room_viewer"),
		roomMember("rm-ann", "r1", "ann", "room_poster"),
		roomMember("rm-bob", "r1", "bob", "room_poster"),
		roomMember("rm-mod", "r1", "mod", "room_moderator"),
		roomMember("rm-ann2", "r2", "ann", "room_viewer"),
		&accessv1.Post{Id: "po-ann", Room: room("r1"), AuthorId: "ann", Body: "hello", Title: "first"},
		&accessv1.Post{Id: "po-bob", Room: room("r1"), AuthorId: "bob", Body: "hi", Title: "second"},
		&accessv1.Draft{Id: "d-ann", Room: room("r1"), AuthorId: "ann"},
		&accessv1.Draft{Id: "d-bob", Room: room("r1"), AuthorId: "bob"},
		&accessv1.Draft{Id: "d-bob2", Room: room("r2"), AuthorId: "bob"},
		&accessv1.Ticket{Id: "tk", Room: room("r1"), AssigneeId: "ann", Status: "open", Title: "fix"},
		&accessv1.Setting{UserId: "ann", Theme: "dark"},
		&accessv1.Setting{UserId: "bob", Theme: "light"},
	)
}

var _ = Describe("Access model registration of owner rules", func() {
	var srv *server.Server

	BeforeEach(func() {
		srv = newBareServer()
	})

	register := func(change func(*descriptorpb.FileDescriptorProto), opts ...server.RegisterOption) error {
		GinkgoHelper()
		_, err := srv.RegisterDescriptorSet(accessFileSet(change), opts...)
		return err
	}
	rulesOf := func(file *descriptorpb.FileDescriptorProto, message string) *syncplanev1.AccessOptions {
		GinkgoHelper()
		m := messageNamed(file, message)
		return proto.GetExtension(m.GetOptions(), syncplanev1.E_Entity).(*syncplanev1.EntityOptions).GetAccess()
	}
	// neverDecides returns the warnings about rules another rule always
	// decides for. The access test file holds other schemas, whose warnings
	// are not about rules.
	neverDecides := func(logs *recordingHandler) []string {
		var out []string
		for _, w := range logs.attrs("access model warning", "finding") {
			if strings.Contains(w, "never decides") {
				out = append(out, w)
			}
		}
		return out
	}
	creatorMark := func() *syncplanev1.FieldBehavior {
		return &syncplanev1.FieldBehavior{Creator: &syncplanev1.CreatorOptions{}}
	}

	It("registers the compiled schema without a warning", func() {
		logs := &recordingHandler{}
		mustNewServer(newTestStore(), server.WithLogger(slog.New(logs)), roomEntities())
		Expect(logs.count("access model warning")).To(BeZero())
	})

	It("registers the descriptor set", func() {
		Expect(register(nil)).To(Succeed())
	})

	DescribeTable("refuses a schema that breaks a rule of a check",
		func(change func(*descriptorpb.FileDescriptorProto), want string) {
			Expect(register(change)).To(MatchError(ContainSubstring(want)))
		},
		Entry("a subject field that does not exist", func(file *descriptorpb.FileDescriptorProto) {
			rulesOf(file, "Post").Allow[1].Subject = "writer_id"
		}, `access allow[1] subject: field "writer_id" not found`),
		Entry("a subject field that is not a string", func(file *descriptorpb.FileDescriptorProto) {
			rulesOf(file, "Post").Allow[1].Subject = "room"
		}, `access allow[1] subject: field "room" is message; it must be a string`),
		Entry("a fields list on a rule that names another operation", func(file *descriptorpb.FileDescriptorProto) {
			rulesOf(file, "Post").Allow[2].Delete = true
		}, `access allow[2] names fields, which only a rule whose one operation is update can name`),
		Entry("a fields list on a write rule", func(file *descriptorpb.FileDescriptorProto) {
			rule := rulesOf(file, "Post").Allow[2]
			rule.Update, rule.Write = false, true
		}, `access allow[2] names fields`),
		Entry("a fields list that names a key field", func(file *descriptorpb.FileDescriptorProto) {
			rulesOf(file, "Post").Allow[2].Fields = []string{"body", "id"}
		}, `access allow[2] fields: "id" is a key field`),
		Entry("a fields list that names no field", func(file *descriptorpb.FileDescriptorProto) {
			rulesOf(file, "Post").Allow[2].Fields = []string{"text"}
		}, `access allow[2] fields: "text" is not a field or a oneof`),
		Entry("a fields list that names a field twice", func(file *descriptorpb.FileDescriptorProto) {
			rulesOf(file, "Post").Allow[2].Fields = []string{"body", "body"}
		}, `access allow[2] fields: "body" is listed twice`),
		Entry("a fields list that names a oneof member", func(file *descriptorpb.FileDescriptorProto) {
			rules := rulesOf(file, "Card")
			rules.Allow = append(rules.Allow, &syncplanev1.AccessRule{
				Update: true, Source: &syncplanev1.AccessRule_Permission{Permission: "edit_cards"}, Fields: []string{"board"},
			})
		}, `field "board" is a member of oneof "place"; name the oneof`),
		Entry("two rules that differ only in order of their fields", func(file *descriptorpb.FileDescriptorProto) {
			rules := rulesOf(file, "Ticket")
			rules.Allow[2].Fields = []string{"status", "title"}
			rules.Allow = append(rules.Allow, &syncplanev1.AccessRule{
				Update: true, Source: &syncplanev1.AccessRule_Permission{Permission: "view_posts"},
				Subject: "assignee_id", Fields: []string{"title", "status"},
			})
		}, `access allow[2] and allow[3] are the same rule`),
		Entry("a creator field that is not a string", func(file *descriptorpb.FileDescriptorProto) {
			post := messageNamed(file, "Post")
			fieldNamed(post, "room").Options = withBehavior(&syncplanev1.FieldBehavior{
				AccessParent: &syncplanev1.AccessParentOptions{}, Creator: &syncplanev1.CreatorOptions{},
			})
		}, `creator: field "room" is message; it must be a string`),
		Entry("a creator mark on a key field", func(file *descriptorpb.FileDescriptorProto) {
			fieldNamed(messageNamed(file, "Setting"), "user_id").Options = withBehavior(creatorMark())
		}, `key field "user_id" is marked creator`),
		Entry("a creator mark beside a write policy", func(file *descriptorpb.FileDescriptorProto) {
			b := creatorMark()
			b.WritePolicy = syncplanev1.FieldWritePolicy_FIELD_WRITE_POLICY_TRUSTED_ONLY
			fieldNamed(messageNamed(file, "Post"), "author_id").Options = withBehavior(b)
		}, `field "author_id" is marked creator and write_policy`),
		Entry("a creator mark with fields this server does not know", func(file *descriptorpb.FileDescriptorProto) {
			b := creatorMark()
			b.Creator.ProtoReflect().SetUnknown(unknownField())
			fieldNamed(messageNamed(file, "Post"), "author_id").Options = withBehavior(b)
		}, `field "author_id" creator carries option fields this server does not know`),
	)

	It("warns about a rule that another rule always decides for", func() {
		logs := &recordingHandler{}
		srv = newBareServer(server.WithLogger(slog.New(logs)))
		Expect(register(func(file *descriptorpb.FileDescriptorProto) {
			rules := rulesOf(file, "Post")
			// Wider than allow[2] on every check, so allow[2] decides nothing.
			rules.Allow = append(rules.Allow, &syncplanev1.AccessRule{
				Update: true, Source: &syncplanev1.AccessRule_Permission{Permission: "post"},
				Subject: "author_id", Fields: []string{"body", "title"},
			})
		})).To(Succeed())
		Expect(neverDecides(logs)).To(ConsistOf(
			`entity "post": access allow[2] never decides update: allow[4] admits every caller and row it admits there`,
		))
	})

	It("warns about a rule whose permission implies the source of another", func() {
		logs := &recordingHandler{}
		srv = newBareServer(server.WithLogger(slog.New(logs)))
		Expect(register(func(file *descriptorpb.FileDescriptorProto) {
			rules := rulesOf(file, "Ticket")
			// Every role that grants moderate_posts grants view_posts, so the
			// view_posts rule admits every caller this one admits.
			rules.Allow = append(rules.Allow, &syncplanev1.AccessRule{
				Read: true, Source: &syncplanev1.AccessRule_Permission{Permission: "moderate_posts"},
			})
		})).To(Succeed())
		Expect(neverDecides(logs)).To(ConsistOf(
			`entity "ticket": access allow[3] never decides read: allow[0] admits every caller and row it admits there`,
		))
	})

	It("changes the access spec with a check, so the change needs the flag", func() {
		Expect(register(nil)).To(Succeed())
		change := func(file *descriptorpb.FileDescriptorProto) {
			rulesOf(file, "Ticket").Allow[2].Fields = []string{"status", "title"}
		}
		Expect(register(change)).To(MatchError(ContainSubstring(`entity "ticket" changes its access model`)))
		Expect(register(change, server.AllowAccessChange())).To(Succeed())
	})
})

var _ = Describe("Owner rules", func() {
	var (
		f       *accessFixture
		clients int
	)

	push := func(user string, m func(client string) *syncplanev1.Mutation) *syncplanev1.MutationResult {
		GinkgoHelper()
		clients++
		client := fmt.Sprintf("c%d", clients)
		results := f.push(user, client, m(client))
		Expect(results).To(HaveLen(1))
		return results[0]
	}
	put := func(user, entity, key string, value proto.Message) *syncplanev1.MutationResult {
		GinkgoHelper()
		return push(user, func(c string) *syncplanev1.Mutation { return putMutation(c, 1, entity, key, value) })
	}
	patch := func(user, entity, key string, value proto.Message, paths ...string) *syncplanev1.MutationResult {
		GinkgoHelper()
		return push(user, func(c string) *syncplanev1.Mutation { return patchMutation(c, 1, entity, key, value, paths...) })
	}
	del := func(user, entity, key string) *syncplanev1.MutationResult {
		GinkgoHelper()
		return push(user, func(c string) *syncplanev1.Mutation { return deleteMutation(c, 1, entity, key) })
	}
	applied := func(r *syncplanev1.MutationResult) {
		GinkgoHelper()
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED), r.GetErrorMessage())
	}
	denied := func(r *syncplanev1.MutationResult) string {
		GinkgoHelper()
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
		Expect(r.GetErrorCode()).To(Equal(server.CodePermissionDenied), r.GetErrorMessage())
		return r.GetErrorMessage()
	}
	get := func(dst proto.Message, key string) bool {
		GinkgoHelper()
		var found bool
		Expect(f.srv.View(server.Trusted(f.ctx), ws, func(tx *server.ViewTx) error {
			var err error
			found, _, err = tx.Get(dst, key)
			return err
		})).To(Succeed())
		return found
	}
	post := func(key string) *accessv1.Post {
		GinkgoHelper()
		var p accessv1.Post
		Expect(get(&p, key)).To(BeTrue(), "post %q", key)
		return &p
	}
	ticket := func(key string) *accessv1.Ticket {
		GinkgoHelper()
		var t accessv1.Ticket
		Expect(get(&t, key)).To(BeTrue(), "ticket %q", key)
		return &t
	}
	trusted := func(msgs ...proto.Message) { f.seed(msgs...) }

	BeforeEach(func() {
		f = newSchemaFixtureOn(newTestStore(), roomEntities())
		f.seedRooms()
	})

	Describe("a creator field", func() {
		It("records the caller who creates the row, whatever the write carries", func() {
			applied(put("ann", "post", "po-new", &accessv1.Post{Id: "po-new", Room: room("r1"), AuthorId: "bob", Body: "mine"}))

			Expect(post("po-new").GetAuthorId()).To(Equal("ann"))
		})

		It("keeps the stored value on a PUT that updates the row", func() {
			applied(put("mod", "post", "po-ann", &accessv1.Post{Id: "po-ann", Room: room("r1"), AuthorId: "mod", Body: "edited"}))

			Expect(post("po-ann").GetAuthorId()).To(Equal("ann"))
			Expect(post("po-ann").GetBody()).To(Equal("edited"))
		})

		It("refuses a PATCH that names it", func() {
			Expect(denied(patch("mod", "post", "po-ann", &accessv1.Post{AuthorId: "mod"}, "author_id"))).
				To(ContainSubstring(`field "author_id" may not be written by this caller`))

			Expect(post("po-ann").GetAuthorId()).To(Equal("ann"))
		})

		It("stores the value a trusted write carries", func() {
			trusted(&accessv1.Post{Id: "po-ann", Room: room("r1"), AuthorId: "bob"})

			Expect(post("po-ann").GetAuthorId()).To(Equal("bob"))
		})

		It("records the caller of an in-process write", func() {
			ctx := server.WithPrincipal(f.ctx, server.Principal{Subject: "bob"})
			Expect(f.srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Put(&accessv1.Draft{Id: "d-new", Room: room("r1")})
			})).To(Succeed())

			var d accessv1.Draft
			Expect(get(&d, "d-new")).To(BeTrue())
			Expect(d.GetAuthorId()).To(Equal("bob"))
		})

		It("records the primary subject that WithSubjects returns first", func() {
			f = newSchemaFixtureOn(newTestStore(), roomEntities(), server.WithSubjects(func(p server.Principal) []string {
				if p.Subject == "" {
					return nil
				}
				return []string{"user:" + p.Subject, p.Subject}
			}))
			f.seedRooms()

			applied(put("ann", "post", "po-new", &accessv1.Post{Id: "po-new", Room: room("r1"), Body: "mine"}))
			Expect(post("po-new").GetAuthorId()).To(Equal("user:ann"))
			// The subject check compares with every subject of the caller.
			applied(patch("ann", "post", "po-new", &accessv1.Post{Body: "edited"}, "body"))
		})
	})

	Describe("a subject check on a write rule", func() {
		It("lets the author edit the fields its rule lists", func() {
			applied(patch("ann", "post", "po-ann", &accessv1.Post{Body: "edited"}, "body"))

			Expect(post("po-ann").GetBody()).To(Equal("edited"))
		})

		It("refuses the author a field its rule does not list", func() {
			denied(patch("ann", "post", "po-ann", &accessv1.Post{Title: "renamed"}, "title"))

			Expect(post("po-ann").GetTitle()).To(Equal("first"))
		})

		It("refuses a caller who is not the author", func() {
			denied(patch("bob", "post", "po-ann", &accessv1.Post{Body: "edited"}, "body"))
			denied(del("bob", "post", "po-ann"))

			Expect(post("po-ann").GetBody()).To(Equal("hello"))
		})

		It("lets the author delete the row", func() {
			applied(del("ann", "post", "po-ann"))

			Expect(get(&accessv1.Post{}, "po-ann")).To(BeFalse())
		})

		It("lets a rule without the check admit a caller who is not the author", func() {
			applied(patch("mod", "post", "po-bob", &accessv1.Post{Title: "moderated"}, "title"))
			applied(del("mod", "post", "po-ann"))

			Expect(post("po-bob").GetTitle()).To(Equal("moderated"))
		})

		It("checks a create on the row as it will be stored", func() {
			// The creator stamp makes ann the author, which the create rule
			// needs.
			applied(put("ann", "post", "po-new", &accessv1.Post{Id: "po-new", Room: room("r1")}))
			// viewer is stamped as the author, but holds no permission to post.
			denied(put("viewer", "post", "po-v", &accessv1.Post{Id: "po-v", Room: room("r1")}))
		})

		It("refuses to create a row whose subject is someone else", func() {
			denied(put("ann", "setting", "bob", &accessv1.Setting{UserId: "bob", Theme: "pink"}))
			denied(put("ann", "setting", "carl", &accessv1.Setting{UserId: "carl", Theme: "pink"}))
			applied(put("ann", "setting", "ann", &accessv1.Setting{UserId: "ann", Theme: "pink"}))
		})

		It("lets a member leave the room by deleting their own grant", func() {
			denied(del("bob", "room_member", "rm-ann"))
			applied(del("bob", "room_member", "rm-bob"))

			Expect(get(&accessv1.RoomMember{}, "rm-bob")).To(BeFalse())
		})

		It("needs one rule to admit both sides of an update", func() {
			_, err := f.srv.RegisterDescriptorSet(accessFileSet(func(file *descriptorpb.FileDescriptorProto) {
				t := messageNamed(file, "Ticket")
				rules := proto.GetExtension(t.GetOptions(), syncplanev1.E_Entity).(*syncplanev1.EntityOptions).GetAccess()
				rules.Allow[2].Fields = nil
			}), server.AllowAccessChange())
			Expect(err).NotTo(HaveOccurred())

			// The stored row names ann; the new one would name bob.
			denied(patch("ann", "ticket", "tk", &accessv1.Ticket{AssigneeId: "bob"}, "assignee_id"))
			applied(patch("ann", "ticket", "tk", &accessv1.Ticket{Title: "fixed"}, "title"))

			Expect(ticket("tk").GetAssigneeId()).To(Equal("ann"))
		})
	})

	Describe("a fields check", func() {
		It("lets the assignee change the status and nothing else", func() {
			applied(patch("ann", "ticket", "tk", &accessv1.Ticket{Status: "done"}, "status"))
			denied(patch("ann", "ticket", "tk", &accessv1.Ticket{Title: "renamed"}, "title"))
			denied(patch("ann", "ticket", "tk", &accessv1.Ticket{AssigneeId: "bob"}, "assignee_id"))

			Expect(ticket("tk").GetStatus()).To(Equal("done"))
			Expect(ticket("tk").GetTitle()).To(Equal("fix"))
		})

		It("refuses a caller the subject check does not admit", func() {
			denied(patch("bob", "ticket", "tk", &accessv1.Ticket{Status: "done"}, "status"))
		})

		It("compares the fields a PUT changes", func() {
			stored := ticket("tk")
			stored.Status = "done"
			applied(put("ann", "ticket", "tk", stored))
			stored.Title = "renamed"
			denied(put("ann", "ticket", "tk", stored))

			Expect(ticket("tk").GetStatus()).To(Equal("done"))
			Expect(ticket("tk").GetTitle()).To(Equal("fix"))
		})

		It("refuses a PATCH whose mask no list covers before the app's policy", func() {
			policy := &recordingPolicy{}
			f = newSchemaFixtureOn(newTestStore(), roomEntities(), server.WithPermissions(policy))
			f.seedRooms()
			_, err := f.srv.RegisterDescriptorSet(accessFileSet(func(file *descriptorpb.FileDescriptorProto) {
				t := messageNamed(file, "Ticket")
				rules := proto.GetExtension(t.GetOptions(), syncplanev1.E_Entity).(*syncplanev1.EntityOptions).GetAccess()
				// Only the assignee's rule names update now.
				rules.Allow[1].Write, rules.Allow[1].Create, rules.Allow[1].Delete = false, true, true
			}), server.AllowAccessChange())
			Expect(err).NotTo(HaveOccurred())

			Expect(denied(patch("mod", "ticket", "tk", &accessv1.Ticket{Title: "renamed"}, "title"))).
				To(ContainSubstring(`entity "ticket" has no access rule that admits an update of "title" together`))
			Expect(policy.writes).To(BeEmpty())
		})
	})

	Describe("a subject check on a read rule", func() {
		It("reads only the rows that name the caller", func() {
			Expect(f.pull("ann")).To(ContainElement("draft/d-ann"))
			Expect(f.pull("ann")).To(lackAll("draft/d-bob", "draft/d-bob2"))
			Expect(f.pull("bob")).To(ContainElement("draft/d-bob"))
			// bob holds no permission in r2.
			Expect(f.pull("bob")).NotTo(ContainElement("draft/d-bob2"))
		})

		It("reads a row that names the caller only where the rule's permission is held", func() {
			trusted(roomMember("rm-bob2", "r2", "bob", "room_viewer"))

			Expect(f.pull("bob")).To(ContainElements("draft/d-bob", "draft/d-bob2"))
		})

		It("still checks the subject for a caller who holds the permission at the root", func() {
			f = newSchemaFixtureOn(newTestStore(), roomEntities(), server.WithGlobalRoles(
				func(_ context.Context, p server.Principal) ([]string, error) {
					if p.Subject == "staff" {
						return []string{"room_moderator"}, nil
					}
					return nil, nil
				}))
			f.seedRooms()
			trusted(&accessv1.Draft{Id: "d-staff", Room: room("r2"), AuthorId: "staff"})

			rows := f.pull("staff")
			Expect(rows).To(ContainElements("draft/d-staff", "post/po-ann", "post/po-bob"))
			Expect(rows).To(lackAll("draft/d-ann", "draft/d-bob", "draft/d-bob2"))
		})

		It("reads the rows of an admitted rule that name the caller", func() {
			Expect(f.pull("ann")).To(ContainElement("setting/ann"))
			Expect(f.pull("ann")).NotTo(ContainElement("setting/bob"))
		})

		It("narrows a bootstrap and a query to the rows that name the caller", func() {
			Expect(f.bootstrapRows("ann")).To(ContainElements("draft/d-ann", "setting/ann"))
			Expect(f.bootstrapRows("ann")).To(lackAll("draft/d-bob", "setting/bob"))
			Expect(f.queryKeys("ann", "draft")).To(Equal([]string{"d-ann"}))
			Expect(f.queryKeys("ann", "setting")).To(Equal([]string{"ann"}))
		})

		It("bootstraps the rows that name the caller without a scan", func() {
			store := &pageScanCountingStore{Adapter: newTestStore(), scanned: map[string]int{}}
			f = newSchemaFixtureOn(store, roomEntities())
			f.seedRooms()
			Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
			store.reset()

			Expect(f.bootstrapRows("ann")).To(ContainElements("setting/ann", "draft/d-ann"))
			Expect(store.count("setting")).To(BeZero())
			Expect(store.count("draft")).To(BeZero())
		})

		It("refuses to let a caller write a row that no longer names them", func() {
			denied(put("ann", "draft", "d-bob", &accessv1.Draft{Id: "d-bob", Room: room("r1"), Body: "taken"}))
			denied(del("ann", "draft", "d-bob"))
		})
	})

	Describe("sync of rows a subject check reads", func() {
		It("sends a row when its subject comes to name the caller, and removes it when it stops", func() {
			r := newAccessReplica(f, "ann")
			r.sync()
			Expect(r.view()).NotTo(ContainElement("draft/d-bob"))

			trusted(&accessv1.Draft{Id: "d-bob", Room: room("r1"), AuthorId: "ann"})
			r.sync()
			Expect(r.view()).To(ContainElement("draft/d-bob"))

			trusted(&accessv1.Draft{Id: "d-bob", Room: room("r1"), AuthorId: "bob"})
			r.sync()
			Expect(r.view()).NotTo(ContainElement("draft/d-bob"))
		})

		It("removes the caller's rows when the rule's permission is revoked, and sends them back when it returns", func() {
			r := newAccessReplica(f, "ann")
			r.sync()
			Expect(r.view()).To(ContainElement("draft/d-ann"))

			Expect(f.srv.Mutate(server.Trusted(f.ctx), ws, func(tx *server.EntityTx) error {
				return tx.Delete(&accessv1.RoomMember{}, "rm-ann")
			})).To(Succeed())
			r.sync()
			Expect(r.view()).NotTo(ContainElement("draft/d-ann"))
			Expect(r.resets).To(BeZero())

			trusted(roomMember("rm-ann", "r1", "ann", "room_poster"))
			r.sync()
			Expect(r.view()).To(ContainElement("draft/d-ann"))
		})

		It("resyncs the caller's rows when the rule's permission moves to the root", func() {
			staff := map[string]bool{}
			f = newSchemaFixtureOn(newTestStore(), roomEntities(), server.WithGlobalRoles(
				func(_ context.Context, p server.Principal) ([]string, error) {
					if staff[p.Subject] {
						return []string{"room_viewer"}, nil
					}
					return nil, nil
				}))
			f.seedRooms()
			r := newAccessReplica(f, "bob")
			r.sync()
			Expect(r.view()).NotTo(ContainElement("draft/d-bob2"))

			staff["bob"] = true
			r.sync()
			Expect(r.view()).To(ContainElement("draft/d-bob2"))
			Expect(r.view()).NotTo(ContainElement("draft/d-ann"))

			staff["bob"] = false
			r.sync()
			Expect(r.view()).NotTo(ContainElement("draft/d-bob2"))
			Expect(r.view()).To(ContainElement("draft/d-bob"))
		})
	})

	It("sends the caller's subjects with the primary subject first", func() {
		f = newSchemaFixtureOn(newTestStore(), roomEntities(), server.WithSubjects(func(p server.Principal) []string {
			return []string{"z:" + p.Subject, p.Subject, "a:" + p.Subject}
		}))
		f.seedRooms()
		req := connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws, ClientId: "c-ann"})
		as("ann", req.Header())
		resp, err := f.sync.Pull(f.ctx, req)
		Expect(err).NotTo(HaveOccurred())

		Expect(resp.Msg.GetCallerAccess().GetSubjects()).To(Equal([]string{"z:ann", "a:ann", "ann"}))
	})
})

var _ = Describe("A creator field outside the access model", func() {
	It("records the caller's primary subject, and keeps it from untrusted writes", func() {
		spec := server.DynamicEntity{
			Name: "memo", KeyFields: []string{"id"},
			Fields: []server.DynamicField{
				{Name: "id", Number: 1, Kind: protoreflect.StringKind},
				{Name: "author", Number: 2, Kind: protoreflect.StringKind, Creator: true},
				{Name: "body", Number: 3, Kind: protoreflect.StringKind},
			},
		}
		f := newSchemaFixtureOn(newTestStore(), server.WithSubjects(func(p server.Principal) []string {
			return []string{"user:" + p.Subject}
		}))
		Expect(f.srv.RegisterDynamicEntity(spec)).To(Succeed())
		mt, err := server.BuildDynamicMessageType(spec)
		Expect(err).NotTo(HaveOccurred())
		memo := func(fields map[string]any) proto.Message { return buildNote(mt, fields) }
		author := func() any {
			GinkgoHelper()
			stored := mt.New().Interface()
			Expect(f.srv.View(server.Trusted(f.ctx), ws, func(tx *server.ViewTx) error {
				_, _, err := tx.Get(stored, "m1")
				return err
			})).To(Succeed())
			return decodeNote(mt, mustMarshal(stored))["author"]
		}

		results := f.push("ann", "c1", putMutation("c1", 1, "memo", "m1", memo(map[string]any{"id": "m1", "author": "bob"})))
		Expect(results[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED), results[0].GetErrorMessage())
		Expect(author()).To(Equal("user:ann"))

		results = f.push("bob", "c2", patchMutation("c2", 1, "memo", "m1", memo(map[string]any{"author": "bob"}), "author"))
		Expect(results[0].GetErrorCode()).To(Equal(server.CodePermissionDenied))
		results = f.push("bob", "c3", putMutation("c3", 1, "memo", "m1", memo(map[string]any{"id": "m1", "author": "bob", "body": "x"})))
		Expect(results[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED), results[0].GetErrorMessage())
		Expect(author()).To(Equal("user:ann"))
	})
})
