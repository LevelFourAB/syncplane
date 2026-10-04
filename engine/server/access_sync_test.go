package server_test

import (
	"context"
	"fmt"
	"maps"
	"sort"
	gosync "sync"
	"time"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
)

// epochHidingPerms hides the tasks named by hide through the application's
// read filter, and moves the filter epoch on every hide. It keeps no past
// epoch, so a cursor issued under an earlier one cannot be diffed against.
type epochHidingPerms struct {
	server.AllowAll
	mu     gosync.Mutex
	hidden map[string]bool
	epoch  uint64
}

func (p *epochHidingPerms) hide(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.hidden == nil {
		p.hidden = map[string]bool{}
	}
	p.hidden[key] = true
	p.epoch++
}

func (p *epochHidingPerms) FilterEpoch(context.Context, server.Principal, string) (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.epoch, nil
}

func (p *epochHidingPerms) ReadFilter(_ context.Context, _ server.Principal, _ string, entity string) (server.RowFilter, error) {
	if entity != "task" {
		return nil, nil
	}
	p.mu.Lock()
	hidden := maps.Clone(p.hidden)
	p.mu.Unlock()
	return func(key string, _ proto.Message) bool { return !hidden[key] }, nil
}

// memberHidingPerms hides every project_member row through the application's
// read filter, so a new grant row reaches its own subject as no row at all.
type memberHidingPerms struct{ server.AllowAll }

func (memberHidingPerms) ReadFilter(_ context.Context, _ server.Principal, _ string, entity string) (server.RowFilter, error) {
	if entity != "project_member" {
		return nil, nil
	}
	return func(string, proto.Message) bool { return false }, nil
}

// accessReplica is one client's copy of a workspace, kept by pulling as one
// caller and applying each page the way a client does.
type accessReplica struct {
	f      *accessFixture
	user   string
	cursor []byte
	rows   map[string]bool
	// drops and resets record what the pulls asked of the client.
	drops  []string
	resets int
}

func newAccessReplica(f *accessFixture, user string) *accessReplica {
	return &accessReplica{f: f, user: user, rows: map[string]bool{}}
}

// pullOnce pulls one page and applies it; limit 0 is the server default.
func (r *accessReplica) pullOnce(limit uint32) *syncplanev1.PullResponse {
	GinkgoHelper()
	req := connect.NewRequest(&syncplanev1.PullRequest{
		WorkspaceId: ws, ClientId: "c-" + r.user, Cursor: r.cursor, Limit: limit,
	})
	as(r.user, req.Header())
	resp, err := r.f.sync.Pull(r.f.ctx, req)
	Expect(err).NotTo(HaveOccurred())
	if resp.Msg.GetReset_() {
		r.resets++
		return resp.Msg
	}
	for _, entity := range resp.Msg.GetPatch().GetDropEntities() {
		r.drops = append(r.drops, entity)
		for k := range r.rows {
			if len(k) > len(entity) && k[:len(entity)+1] == entity+"/" {
				delete(r.rows, k)
			}
		}
	}
	for _, rc := range resp.Msg.GetPatch().GetChanges() {
		key := rc.GetEntity() + "/" + rc.GetKey()
		if rc.GetTombstone() {
			delete(r.rows, key)
		} else {
			r.rows[key] = true
		}
	}
	r.cursor = resp.Msg.GetNextCursor()
	return resp.Msg
}

// sync pulls until the server has nothing more, and fails on a reset.
func (r *accessReplica) sync() {
	GinkgoHelper()
	for range 100 {
		resp := r.pullOnce(0)
		Expect(resp.GetReset_()).To(BeFalse(), "the pull reset the client")
		if !resp.GetHasMore() {
			return
		}
	}
	Fail("the pulls did not settle")
}

func (r *accessReplica) view() []string {
	out := make([]string, 0, len(r.rows))
	for k := range r.rows {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

var _ = Describe("Access model sync", func() {
	var f *accessFixture

	revoke := func(entity, key string) {
		GinkgoHelper()
		Expect(f.srv.Mutate(server.Trusted(f.ctx), ws, func(tx *server.EntityTx) error {
			switch entity {
			case "project_member":
				return tx.Delete(&accessv1.ProjectMember{}, key)
			case "org_member":
				return tx.Delete(&accessv1.OrgMember{}, key)
			case "project":
				return tx.Delete(&accessv1.Project{}, key)
			}
			return fmt.Errorf("no entity %q", entity)
		})).To(Succeed())
	}

	// converged asserts that a replica kept in sync holds exactly what a fresh
	// pull as the same caller shows.
	converged := func(r *accessReplica) {
		GinkgoHelper()
		Expect(r.view()).To(Equal(f.pull(r.user)))
	}

	BeforeEach(func() {
		f = newAccessFixture()
		f.seedOrg()
		Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
	})

	It("sends the rows below a grant the caller gains", func() {
		guest := newAccessReplica(f, "guest")
		guest.sync()
		Expect(guest.view()).NotTo(ContainElement("task/t2"))

		f.seed(&accessv1.ProjectMember{Id: "pm-guest2", Project: project("p2"), UserId: "guest", Role: "project_reader"})
		guest.sync()
		Expect(guest.view()).To(ContainElements("project/p2", "task/t2", "project_member/pm-writer"))
		converged(guest)
		Expect(guest.resets).To(BeZero())
	})

	It("tombstones the rows below a grant the caller loses", func() {
		f.seed(&accessv1.ProjectMember{Id: "pm-guest2", Project: project("p2"), UserId: "guest", Role: "project_reader"})
		guest := newAccessReplica(f, "guest")
		guest.sync()
		Expect(guest.view()).To(ContainElement("task/t2"))

		revoke("project_member", "pm-guest2")
		guest.sync()
		Expect(guest.view()).NotTo(ContainElements("project/p2", "task/t2"))
		converged(guest)
	})

	It("tombstones the rows below an anchor that is deleted", func() {
		writer := newAccessReplica(f, "writer")
		// The writer keeps a grant elsewhere, so it is still admitted.
		f.seed(&accessv1.ProjectMember{Id: "pm-writer1", Project: project("p1"), UserId: "writer", Role: "project_reader"})
		writer.sync()
		Expect(writer.view()).To(ContainElement("task/t2"))

		revoke("project", "p2")
		writer.sync()
		Expect(writer.view()).NotTo(ContainElements("project/p2", "task/t2"))
		converged(writer)
	})

	It("drops an entity whose root grant the caller loses, and sends what it keeps", func() {
		f.seed(&accessv1.ProjectMember{Id: "pm-member1", Project: project("p1"), UserId: "member", Role: "project_reader"})
		member := newAccessReplica(f, "member")
		member.sync()
		Expect(member.view()).To(ContainElements("task/t1", "task/t2", "task/t3"))

		revoke("org_member", "om-member")
		member.sync()
		Expect(member.drops).To(ContainElement("task"))
		Expect(member.view()).To(ContainElement("task/t1"))
		Expect(member.view()).NotTo(ContainElements("task/t2", "task/t3"))
		converged(member)
	})

	It("sends every row of an entity whose root grant the caller gains", func() {
		guest := newAccessReplica(f, "guest")
		guest.sync()
		f.seed(&accessv1.OrgMember{Id: "om-guest", UserId: "guest", Role: "org_member"})
		guest.sync()
		Expect(guest.view()).To(ContainElements("task/t1", "task/t2", "task/t3", "label/l1"))
		converged(guest)
	})

	It("resets a client whose access moves again during a resync", func() {
		for i := range 5 {
			f.seed(&accessv1.Task{Id: fmt.Sprintf("x%d", i), Project: project("p2")})
		}
		guest := newAccessReplica(f, "guest")
		guest.sync()
		f.seed(&accessv1.ProjectMember{Id: "pm-guest2", Project: project("p2"), UserId: "guest", Role: "project_reader"})
		Expect(guest.pullOnce(1).GetHasMore()).To(BeTrue())

		f.seed(&accessv1.OrgMember{Id: "om-guest", UserId: "guest", Role: "org_member"})
		Expect(guest.pullOnce(1).GetReset_()).To(BeTrue())
	})

	It("resets a cursor presented by a caller it was not issued to", func() {
		guest := newAccessReplica(f, "guest")
		guest.sync()
		stolen := newAccessReplica(f, "writer")
		stolen.cursor = guest.cursor
		Expect(stolen.pullOnce(0).GetReset_()).To(BeTrue())
	})

	It("resets on a change of access when no cursor-state store holds the record", func() {
		f = newAccessFixture(server.WithCursorStateStore(nil))
		f.seedOrg()
		guest := newAccessReplica(f, "guest")
		guest.sync()
		guest.pullOnce(0)
		Expect(guest.resets).To(BeZero(), "an unchanged access needs no record")

		f.seed(&accessv1.ProjectMember{Id: "pm-guest2", Project: project("p2"), UserId: "guest", Role: "project_reader"})
		Expect(guest.pullOnce(0).GetReset_()).To(BeTrue())
	})

	It("resets a client whose access moved together with a filter epoch it cannot diff", func() {
		perms := &epochHidingPerms{}
		f = newAccessFixture(server.WithPermissions(perms))
		f.seedOrg()
		Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
		guest := newAccessReplica(f, "guest")
		guest.sync()
		Expect(guest.view()).To(ContainElement("task/t1"))

		// The new epoch hides t1, and the old one is gone: a resync of the
		// access alone would leave t1 on the client under a cursor that claims
		// the new epoch.
		perms.hide("t1")
		f.seed(&accessv1.ProjectMember{Id: "pm-guest2", Project: project("p2"), UserId: "guest", Role: "project_reader"})
		Expect(guest.pullOnce(0).GetReset_()).To(BeTrue())
	})

	It("reads only the rows below the anchors that moved", func() {
		counting := &pageScanCountingStore{Adapter: newTestStore(), scanned: map[string]int{}}
		f = newAccessFixtureOn(counting)
		f.seedOrg()
		for i := range 50 {
			f.seed(&accessv1.Task{Id: fmt.Sprintf("x%02d", i), Project: project("p1")})
		}
		Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
		writer := newAccessReplica(f, "writer")
		writer.sync()

		f.seed(&accessv1.ProjectMember{Id: "pm-writer3", Project: project("p3"), UserId: "writer", Role: "project_reader"},
			&accessv1.Project{Id: "p3"},
			&accessv1.Task{Id: "t9", Project: project("p3")})
		counting.reset()
		writer.sync()
		Expect(writer.view()).To(ContainElements("project/p3", "task/t9"))
		Expect(counting.count("task")).To(BeZero())
		converged(writer)
	})

	Context("the CallerAccess summary", func() {
		anchors := func(ca *syncplanev1.CallerAccess) map[string][]string {
			out := map[string][]string{}
			for _, p := range ca.GetPermissions() {
				if p.GetRoot() {
					out[p.GetPermission()] = []string{"*"}
					continue
				}
				for _, a := range p.GetAnchors() {
					for _, k := range a.GetKeys() {
						out[p.GetPermission()] = append(out[p.GetPermission()], a.GetEntity()+"/"+k)
					}
				}
			}
			return out
		}

		It("is sent on the first pull and again only when it changes", func() {
			guest := newAccessReplica(f, "guest")
			first := guest.pullOnce(0)
			Expect(anchors(first.GetCallerAccess())).To(Equal(map[string][]string{
				"view_members": {"project/p1"}, "view_projects": {"project/p1"}, "view_tasks": {"project/p1"},
			}))
			Expect(first.GetCallerAccess().GetSeq()).To(BeNumerically(">", 0))
			Expect(guest.pullOnce(0).GetCallerAccess()).To(BeNil())

			f.seed(&accessv1.OrgMember{Id: "om-guest", UserId: "guest", Role: "org_member"})
			var changed *syncplanev1.CallerAccess
			for range 10 {
				resp := guest.pullOnce(0)
				if resp.GetCallerAccess() != nil {
					changed = resp.GetCallerAccess()
				}
				if !resp.GetHasMore() {
					break
				}
			}
			Expect(anchors(changed)).To(HaveKeyWithValue("view_tasks", []string{"*"}))
		})

		It("rides on the Begin frame of a bootstrap", func() {
			req := connect.NewRequest(&syncplanev1.BootstrapRequest{WorkspaceId: ws, ClientId: "c-writer"})
			as("writer", req.Header())
			stream, err := f.sync.Bootstrap(f.ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(stream.Receive()).To(BeTrue())
			begin := stream.Msg().GetBegin()
			Expect(anchors(begin.GetCallerAccess())).To(HaveKeyWithValue("edit_tasks", []string{"project/p2"}))
			for stream.Receive() {
			}
			Expect(stream.Err()).NotTo(HaveOccurred())
		})

		It("reaches an open Subscribe stream when no row the caller can see changed", func() {
			f = newAccessFixture(server.WithPermissions(memberHidingPerms{}))
			f.seedOrg()
			writer := newAccessReplica(f, "writer")
			writer.sync()

			req := connect.NewRequest(&syncplanev1.SubscribeRequest{
				ClientId: "c-writer",
				Scopes:   []*syncplanev1.WorkspaceScope{{WorkspaceId: ws, Cursor: writer.cursor}},
			})
			as("writer", req.Header())
			ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
			defer cancel()
			stream, err := f.sync.Subscribe(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			for stream.Receive() && !stream.Msg().GetCaughtUp() {
			}

			// A second grant at p2 adds manage_members there and no read
			// permission, and its row is hidden from the writer.
			f.seed(&accessv1.ProjectMember{Id: "pm-writer2", Project: project("p2"), UserId: "writer", Role: "project_admin"})
			var changed *syncplanev1.CallerAccess
			for changed == nil && stream.Receive() {
				Expect(stream.Msg().GetDropped()).To(BeNil(), "the stream dropped the scope")
				changed = stream.Msg().GetPage().GetCallerAccess()
			}
			Expect(anchors(changed)).To(HaveKeyWithValue("manage_members", []string{"project/p2"}))
		})
	})

	It("resyncs an open Subscribe stream", func() {
		f = newAccessFixture(server.WithHeartbeat(50 * time.Millisecond))
		f.seedOrg()
		guest := newAccessReplica(f, "guest")
		guest.sync()

		req := connect.NewRequest(&syncplanev1.SubscribeRequest{
			ClientId: "c-guest",
			Scopes:   []*syncplanev1.WorkspaceScope{{WorkspaceId: ws, Cursor: guest.cursor}},
		})
		as("guest", req.Header())
		ctx, cancel := context.WithTimeout(f.ctx, 10*time.Second)
		defer cancel()
		stream, err := f.sync.Subscribe(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		f.seed(&accessv1.ProjectMember{Id: "pm-guest2", Project: project("p2"), UserId: "guest", Role: "project_reader"})
		seen := map[string]bool{}
		for stream.Receive() && !seen["task/t2"] {
			msg := stream.Msg()
			Expect(msg.GetDropped()).To(BeNil(), "the stream dropped the scope")
			for _, rc := range msg.GetPage().GetPatch().GetChanges() {
				if !rc.GetTombstone() {
					seen[rc.GetEntity()+"/"+rc.GetKey()] = true
				}
			}
		}
		Expect(seen).To(HaveKey("task/t2"))
	})
})

// A cursor records the read rule of every entity it was issued under. A
// change to the rule of an entity the cursor knows changes what its rows mean
// and resets it; anything else leaves the rows the client holds valid.
var _ = Describe("Access model sync across a policy change", func() {
	var (
		store  storage.Adapter
		before *accessFixture
		guest  *accessReplica
	)

	BeforeEach(func() {
		store = newTestStore()
		before = newAccessFixtureOn(store)
		before.seedOrg()
		guest = newAccessReplica(before, "guest")
		guest.sync()
	})

	// movedTo carries the guest's cursor to a server running another schema
	// on the same store.
	movedTo := func(f *accessFixture) *accessReplica {
		r := newAccessReplica(f, "guest")
		r.cursor, r.rows = guest.cursor, maps.Clone(guest.rows)
		return r
	}

	// reregistered returns a server on the same store whose access file
	// change has edited, registered with the access change allowed.
	reregistered := func(change func(*descriptorpb.FileDescriptorProto)) *accessFixture {
		GinkgoHelper()
		f := newAccessFixtureOn(store)
		_, err := f.srv.RegisterDescriptorSet(accessFileSet(change), server.AllowAccessChange())
		Expect(err).NotTo(HaveOccurred())
		return f
	}

	It("keeps a cursor across an entity added to the model", func() {
		after := newAccessFixtureOn(store, server.WithEntities(&accessv1.Team{}, &accessv1.TeamMember{}))
		moved := movedTo(after)
		moved.sync()
		Expect(moved.view()).To(Equal(after.pull("guest")))
	})

	// During a rolling deploy one client meets both schemas in turn.
	It("keeps a cursor that moves between the older and the newer schema", func() {
		after := newAccessFixtureOn(store, server.WithEntities(&accessv1.Team{}, &accessv1.TeamMember{}))
		newer := movedTo(after)
		newer.sync()

		older := newAccessReplica(before, "guest")
		older.cursor, older.rows = newer.cursor, newer.rows
		older.sync()
		newer.cursor = older.cursor
		newer.sync()
		Expect(newer.view()).To(Equal(after.pull("guest")))
	})

	// What a role grants decides the anchors a cursor records, not what the
	// anchors mean, so the change diffs like any change of grants.
	It("resyncs a cursor across a change to what a role grants", func() {
		Expect(guest.view()).To(ContainElement("task/t1"))
		after := reregistered(func(file *descriptorpb.FileDescriptorProto) {
			member := messageNamed(file, "ProjectMember")
			eo := proto.GetExtension(member.GetOptions(), syncplanev1.E_Entity).(*syncplanev1.EntityOptions)
			for _, role := range eo.GetGrant().GetRoles() {
				if role.GetName() == "project_reader" {
					role.Grants = []string{"view_projects", "view_members"}
				}
			}
		})
		moved := movedTo(after)
		moved.sync()
		Expect(moved.view()).NotTo(ContainElement("task/t1"))
		Expect(moved.view()).To(Equal(after.pull("guest")))
	})

	// A role holds the permissions of the roles it includes, so a change to an
	// included role reaches every caller that holds the including role.
	It("resyncs a cursor across a change to a role that the caller's role includes", func() {
		writer := newAccessReplica(before, "writer")
		writer.sync()
		Expect(writer.view()).To(ContainElement("task/t2"))
		after := reregistered(func(file *descriptorpb.FileDescriptorProto) {
			member := messageNamed(file, "ProjectMember")
			eo := proto.GetExtension(member.GetOptions(), syncplanev1.E_Entity).(*syncplanev1.EntityOptions)
			for _, role := range eo.GetGrant().GetRoles() {
				if role.GetName() == "project_reader" {
					role.Grants = []string{"view_projects", "view_members"}
				}
			}
		})
		moved := newAccessReplica(after, "writer")
		moved.cursor, moved.rows = writer.cursor, maps.Clone(writer.rows)
		moved.sync()
		Expect(moved.view()).NotTo(ContainElement("task/t2"))
		Expect(moved.view()).To(Equal(after.pull("writer")))
	})

	It("resets a cursor across a change to the read rule of an entity it knows", func() {
		after := reregistered(func(file *descriptorpb.FileDescriptorProto) {
			label := messageNamed(file, "Label")
			eo := proto.GetExtension(label.GetOptions(), syncplanev1.E_Entity).(*syncplanev1.EntityOptions)
			eo.GetAccess().Allow = append(eo.GetAccess().GetAllow(), &syncplanev1.AccessRule{
				Read: true, Source: &syncplanev1.AccessRule_Permission{Permission: "view_projects"},
			})
		})
		Expect(movedTo(after).pullOnce(0).GetReset_()).To(BeTrue())
	})

	It("resets a cursor across any model change without a cursor-state store", func() {
		bare := newAccessFixtureOn(store, server.WithCursorStateStore(nil))
		r := newAccessReplica(bare, "guest")
		r.sync()

		after := newAccessFixtureOn(store, server.WithCursorStateStore(nil),
			server.WithEntities(&accessv1.Team{}, &accessv1.TeamMember{}))
		moved := newAccessReplica(after, "guest")
		moved.cursor = r.cursor
		Expect(moved.pullOnce(0).GetReset_()).To(BeTrue())
	})
})
