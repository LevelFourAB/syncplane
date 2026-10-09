package server_test

import (
	"context"
	"fmt"
	"log/slog"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/text"
)

// threadEntities adds to the owner-rule schema the entities whose rules test
// fields of the row: threads in rooms, and bulletins on the whole workspace.
func threadEntities() server.Option {
	return server.WithEntities(&accessv1.Thread{}, &accessv1.Bulletin{})
}

// seedThreads writes, on top of seedRooms:
//
//   - in r1, an open thread, an archived one and a hidden one, and in r2 an
//     open one;
//   - a published bulletin by ann, a draft by ann and a draft by bob.
func (f *accessFixture) seedThreads() {
	GinkgoHelper()
	f.seedRooms()
	f.seed(
		&accessv1.Thread{Id: "th-open", Room: room("r1"), Title: "open"},
		&accessv1.Thread{Id: "th-archived", Room: room("r1"), Archived: true, Title: "archived"},
		&accessv1.Thread{Id: "th-hidden", Room: room("r1"), Hidden: true, Title: "hidden"},
		&accessv1.Thread{Id: "th-r2", Room: room("r2"), Title: "elsewhere"},
		&accessv1.Bulletin{Id: "bu-pub", AuthorId: "ann", Status: "published"},
		&accessv1.Bulletin{Id: "bu-ann", AuthorId: "ann", Status: "draft"},
		&accessv1.Bulletin{Id: "bu-bob", AuthorId: "bob", Status: "draft"},
	)
}

var _ = Describe("Access model registration of when checks", func() {
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

	It("registers the compiled schema without a warning", func() {
		logs := &recordingHandler{}
		mustNewServer(newTestStore(), server.WithLogger(slog.New(logs)), roomEntities(), threadEntities())
		Expect(logs.count("access model warning")).To(BeZero())
	})

	DescribeTable("refuses a when condition that cannot decide access on the row",
		func(when *syncplanev1.Condition, want string) {
			Expect(register(func(file *descriptorpb.FileDescriptorProto) {
				rulesOf(file, "Thread").Allow[0].When = when
			})).To(MatchError(And(ContainSubstring(`access allow[0] when:`), ContainSubstring(want))))
		},
		Entry("a field the entity does not have", query.Eq("closed", true), `closed`),
		Entry("a condition without a kind", &syncplanev1.Condition{}, `condition has no kind`),
		Entry("a rel condition, which reads another row",
			&syncplanev1.Condition{Kind: &syncplanev1.Condition_Rel{Rel: &syncplanev1.RelCond{
				Path: []string{"room"}, Cond: query.Eq("name", "one"),
			}}}, `reads another row`),
		Entry("a match condition inside another condition",
			query.Or(query.Eq("hidden", false), query.Match("secret")), `a match condition cannot decide access`),
	)

	It("refuses a when condition that carries fields this server does not know", func() {
		when := query.And(query.Eq("hidden", false))
		when.GetAnd().GetConds()[0].GetCmp().ProtoReflect().SetUnknown(unknownField())
		Expect(register(func(file *descriptorpb.FileDescriptorProto) {
			rulesOf(file, "Thread").Allow[0].When = when
		})).To(MatchError(ContainSubstring(`access allow[0] when: the condition carries fields this server does not know`)))
	})

	It("tells apart two rules that differ only in their when condition", func() {
		Expect(register(func(file *descriptorpb.FileDescriptorProto) {
			rules := rulesOf(file, "Thread")
			rules.Allow = append(rules.Allow, &syncplanev1.AccessRule{
				Read: true, Requires: []string{"view_posts"},
				When: query.Eq("title", "pinned"),
			})
		})).To(Succeed())
	})

	It("refuses two rules with the same when condition as the same rule", func() {
		Expect(register(func(file *descriptorpb.FileDescriptorProto) {
			rules := rulesOf(file, "Thread")
			rules.Allow = append(rules.Allow, &syncplanev1.AccessRule{
				Read: true, Requires: []string{"view_posts"},
				When: query.Eq("hidden", false),
			})
		})).To(MatchError(ContainSubstring(`access allow[0] and allow[4] are the same rule`)))
	})

	It("does not warn that a rule without the check decides for a rule with it", func() {
		logs := &recordingHandler{}
		srv = newBareServer(server.WithLogger(slog.New(logs)))
		Expect(register(func(file *descriptorpb.FileDescriptorProto) {
			rules := rulesOf(file, "Thread")
			// Wider than allow[0] on every check, so allow[0] decides nothing.
			rules.Allow = append(rules.Allow, &syncplanev1.AccessRule{
				Read: true, Requires: []string{"view_posts"},
			})
		})).To(Succeed())
		var findings []string
		for _, w := range logs.attrs("access model warning", "finding") {
			if w == `entity "thread": access allow[0] never decides read: allow[4] admits every caller and row it admits there` {
				findings = append(findings, w)
			}
		}
		Expect(findings).To(HaveLen(1))
	})

	It("changes the access spec with the condition, so the change needs the flag", func() {
		Expect(register(nil)).To(Succeed())
		change := func(file *descriptorpb.FileDescriptorProto) {
			rulesOf(file, "Thread").Allow[2].When = query.Eq("archived", true)
		}
		Expect(register(change)).To(MatchError(ContainSubstring(`entity "thread" changes its access model`)))
		Expect(register(change, server.AllowAccessChange())).To(Succeed())
	})
})

var _ = Describe("When checks", func() {
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
	denied := func(r *syncplanev1.MutationResult) {
		GinkgoHelper()
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
		Expect(r.GetErrorCode()).To(Equal(server.CodePermissionDenied), r.GetErrorMessage())
	}
	thread := func(key string) *accessv1.Thread {
		GinkgoHelper()
		var t accessv1.Thread
		var found bool
		Expect(f.srv.View(server.Trusted(f.ctx), ws, func(tx *server.ViewTx) error {
			var err error
			found, _, err = tx.Get(&t, key)
			return err
		})).To(Succeed())
		Expect(found).To(BeTrue(), "thread %q", key)
		return &t
	}
	trusted := func(msgs ...proto.Message) { f.seed(msgs...) }

	BeforeEach(func() {
		f = newSchemaFixtureOn(newTestStore(), roomEntities(), threadEntities())
		f.seedThreads()
	})

	Describe("on a write rule", func() {
		It("lets a poster edit a row that matches the condition", func() {
			applied(patch("ann", "thread", "th-open", &accessv1.Thread{Title: "renamed"}, "title"))

			Expect(thread("th-open").GetTitle()).To(Equal("renamed"))
		})

		It("refuses a poster a row that does not match the condition", func() {
			denied(patch("ann", "thread", "th-archived", &accessv1.Thread{Title: "renamed"}, "title"))
			denied(put("ann", "thread", "th-archived", &accessv1.Thread{Id: "th-archived", Room: room("r1"), Archived: true}))
			denied(del("ann", "thread", "th-archived"))

			Expect(thread("th-archived").GetTitle()).To(Equal("archived"))
		})

		It("refuses a write that makes the row stop matching", func() {
			denied(patch("ann", "thread", "th-open", &accessv1.Thread{Archived: true}, "archived"))
			stored := thread("th-open")
			stored.Archived = true
			denied(put("ann", "thread", "th-open", stored))

			Expect(thread("th-open").GetArchived()).To(BeFalse())
		})

		It("refuses a write that makes the row start matching", func() {
			denied(patch("ann", "thread", "th-archived", &accessv1.Thread{}, "archived"))

			Expect(thread("th-archived").GetArchived()).To(BeTrue())
		})

		It("checks a create on the row as it will be stored", func() {
			applied(put("ann", "thread", "th-new", &accessv1.Thread{Id: "th-new", Room: room("r1")}))
			denied(put("ann", "thread", "th-new2", &accessv1.Thread{Id: "th-new2", Room: room("r1"), Archived: true}))
		})

		It("lets a poster delete a row that matches the condition", func() {
			applied(del("ann", "thread", "th-open"))
		})

		It("lets a rule without the check archive and restore a row", func() {
			applied(patch("mod", "thread", "th-open", &accessv1.Thread{Archived: true}, "archived"))
			applied(patch("mod", "thread", "th-archived", &accessv1.Thread{}, "archived"))

			Expect(thread("th-open").GetArchived()).To(BeTrue())
			Expect(thread("th-archived").GetArchived()).To(BeFalse())
		})

		It("still checks the source of the rule", func() {
			denied(patch("viewer", "thread", "th-open", &accessv1.Thread{Title: "renamed"}, "title"))
		})
	})

	Describe("on a read rule", func() {
		It("reads only the rows that match the condition", func() {
			rows := f.pull("viewer")
			Expect(rows).To(ContainElements("thread/th-open", "thread/th-archived"))
			Expect(rows).To(lackAll("thread/th-hidden", "thread/th-r2"))
		})

		It("lets another rule without the check read every row", func() {
			Expect(f.pull("mod")).To(ContainElements("thread/th-open", "thread/th-hidden"))
		})

		It("narrows a bootstrap and a query to the rows that match", func() {
			Expect(f.bootstrapRows("viewer")).To(ContainElement("thread/th-open"))
			Expect(f.bootstrapRows("viewer")).NotTo(ContainElement("thread/th-hidden"))
			Expect(f.queryKeys("viewer", "thread")).To(Equal([]string{"th-archived", "th-open"}))
			Expect(f.queryKeys("ann", "thread")).To(Equal([]string{"th-archived", "th-open", "th-r2"}))
		})

		It("still checks the condition for a caller who holds the permission at the root", func() {
			f = newSchemaFixtureOn(newTestStore(), roomEntities(), threadEntities(), server.WithGlobalRoles(
				func(_ context.Context, p server.Principal) ([]string, error) {
					if p.Subject == "staff" {
						return []string{"room_viewer"}, nil
					}
					return nil, nil
				}))
			f.seedThreads()

			rows := f.pull("staff")
			Expect(rows).To(ContainElements("thread/th-open", "thread/th-r2"))
			Expect(rows).NotTo(ContainElement("thread/th-hidden"))
			Expect(f.queryKeys("staff", "thread")).To(Equal([]string{"th-archived", "th-open", "th-r2"}))
		})

		It("reads the rows of an admitted rule that match the condition", func() {
			Expect(f.pull("viewer")).To(ContainElement("bulletin/bu-pub"))
			Expect(f.pull("viewer")).To(lackAll("bulletin/bu-ann", "bulletin/bu-bob"))
			// Another rule admits the author to their own drafts.
			Expect(f.pull("bob")).To(ContainElements("bulletin/bu-pub", "bulletin/bu-bob"))
			Expect(f.pull("bob")).NotTo(ContainElement("bulletin/bu-ann"))
			Expect(f.bootstrapRows("bob")).To(ContainElements("bulletin/bu-pub", "bulletin/bu-bob"))
			Expect(f.bootstrapRows("bob")).NotTo(ContainElement("bulletin/bu-ann"))
			Expect(f.queryKeys("bob", "bulletin")).To(Equal([]string{"bu-bob", "bu-pub"}))
		})
	})

	Describe("sync of rows a when check reads", func() {
		It("removes a row when it stops matching, and sends it back when it matches again", func() {
			r := newAccessReplica(f, "viewer")
			r.sync()
			Expect(r.view()).To(ContainElement("thread/th-open"))

			trusted(&accessv1.Thread{Id: "th-open", Room: room("r1"), Hidden: true, Title: "open"})
			r.sync()
			Expect(r.view()).NotTo(ContainElement("thread/th-open"))

			trusted(&accessv1.Thread{Id: "th-open", Room: room("r1"), Title: "open"})
			r.sync()
			Expect(r.view()).To(ContainElement("thread/th-open"))
			Expect(r.resets).To(BeZero())
		})

		It("removes a row of an admitted rule when it stops matching", func() {
			r := newAccessReplica(f, "viewer")
			r.sync()
			Expect(r.view()).To(ContainElement("bulletin/bu-pub"))

			trusted(&accessv1.Bulletin{Id: "bu-pub", AuthorId: "ann", Status: "draft"})
			r.sync()
			Expect(r.view()).NotTo(ContainElement("bulletin/bu-pub"))
		})

		It("resyncs the rows that match when the rule's permission moves to the root and back", func() {
			staff := map[string]bool{}
			f = newSchemaFixtureOn(newTestStore(), roomEntities(), threadEntities(), server.WithGlobalRoles(
				func(_ context.Context, p server.Principal) ([]string, error) {
					if staff[p.Subject] {
						return []string{"room_viewer"}, nil
					}
					return nil, nil
				}))
			f.seedThreads()
			r := newAccessReplica(f, "bob")
			r.sync()
			Expect(r.view()).NotTo(ContainElement("thread/th-r2"))

			staff["bob"] = true
			r.sync()
			Expect(r.view()).To(ContainElement("thread/th-r2"))
			Expect(r.view()).NotTo(ContainElement("thread/th-hidden"))

			staff["bob"] = false
			r.sync()
			Expect(r.view()).NotTo(ContainElement("thread/th-r2"))
			Expect(r.view()).To(ContainElement("thread/th-open"))
		})
	})

	Describe("text delta state", func() {
		const opening = "notes long enough that sending all of them again for every edit would waste everyone's bytes"

		pullFrom := func(user string, cursor []byte) *syncplanev1.PullResponse {
			GinkgoHelper()
			req := connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws, ClientId: "c-" + user, Cursor: cursor})
			as(user, req.Header())
			resp, err := f.sync.Pull(f.ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Msg.GetReset_()).To(BeFalse())
			return resp.Msg
		}
		threadIn := func(resp *syncplanev1.PullResponse, key string) *syncplanev1.RowChange {
			for _, rc := range resp.GetPatch().GetChanges() {
				if rc.GetEntity() == "thread" && rc.GetKey() == key {
					return rc
				}
			}
			return nil
		}

		It("ships the whole document to a reader a write makes the row match for", func() {
			notes := bodyDoc("seed", opening)
			trusted(&accessv1.Thread{Id: "th-hidden", Room: room("r1"), Hidden: true, Title: "hidden", Notes: notes})
			first := pullFrom("viewer", nil)
			Expect(threadIn(first, "th-hidden")).To(BeNil(), "viewer cannot read a hidden thread")

			edited := editBody(notes, "mod", func(d *text.Doc) { Expect(d.SetString(opening + ", edited")).To(Succeed()) })
			applied(patch("mod", "thread", "th-hidden", &accessv1.Thread{Notes: edited}, "notes"))
			applied(patch("mod", "thread", "th-hidden", &accessv1.Thread{}, "hidden"))

			rc := threadIn(pullFrom("viewer", first.GetNextCursor()), "th-hidden")
			Expect(rc).NotTo(BeNil())
			Expect(rc.GetTextDeltas()).To(BeEmpty(),
				"the reader never held the thread, so there is nothing to fold a fragment into")
			var got accessv1.Thread
			Expect(proto.Unmarshal(rc.GetValue(), &got)).To(Succeed())
			Expect(plainBody(got.GetNotes())).To(Equal(opening + ", edited"))
		})
	})
})
