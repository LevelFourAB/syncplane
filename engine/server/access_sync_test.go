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

	"github.com/levelfourab/syncplane/engine/cursorstate"
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

// puttingCursorState keeps the entries a cursor-state store is given, in
// the order it is given them.
type puttingCursorState struct {
	cursorstate.Store
	mu   gosync.Mutex
	puts [][]byte
}

func (c *puttingCursorState) Put(ctx context.Context, ws string, ref cursorstate.Ref, state []byte) error {
	c.mu.Lock()
	c.puts = append(c.puts, state)
	c.mu.Unlock()
	return c.Store.Put(ctx, ws, ref, state)
}

// taken returns the entries put since the last call.
func (c *puttingCursorState) taken() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.puts
	c.puts = nil
	return out
}

// accessReplica is one client's copy of a workspace, kept by pulling as one
// caller and applying each page the way a client does.
type accessReplica struct {
	f      *accessFixture
	user   string
	cursor []byte
	rows   map[string]bool
	// access is the CallerAccess the replica holds. A legacy replica sends
	// no PullRequest.caller_access_version, as a client that applies no
	// CallerAccessDelta does.
	access *syncplanev1.CallerAccess
	legacy bool
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
	if !r.legacy {
		req.Msg.CallerAccessVersion = proto.Uint64(r.access.GetVersion())
	}
	as(r.user, req.Header())
	resp, err := r.f.sync.Pull(r.f.ctx, req)
	Expect(err).NotTo(HaveOccurred())
	if resp.Msg.GetReset_() {
		r.resets++
		return resp.Msg
	}
	r.access = applyCallerAccess(r.access, resp.Msg.GetCallerAccess(), resp.Msg.GetCallerAccessDelta())
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

// syncDelta syncs like sync, and returns the one CallerAccessDelta the pages
// carried. It fails the spec when a page carries a whole CallerAccess.
func (r *accessReplica) syncDelta() *syncplanev1.CallerAccessDelta {
	GinkgoHelper()
	var delta *syncplanev1.CallerAccessDelta
	for range 100 {
		resp := r.pullOnce(0)
		Expect(resp.GetReset_()).To(BeFalse(), "the pull reset the client")
		Expect(resp.GetCallerAccess()).To(BeNil(), "the whole summary was sent")
		if d := resp.GetCallerAccessDelta(); d != nil {
			Expect(delta).To(BeNil(), "two pages carried a delta")
			delta = d
		}
		if !resp.GetHasMore() {
			Expect(delta).NotTo(BeNil(), "no page carried a delta")
			return delta
		}
	}
	Fail("the pulls did not settle")
	return nil
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
		It("sends each row once, in the group of the permissions held there and not at the root", func() {
			f.seed(
				&accessv1.Project{Id: "p3", Name: "three"},
				&accessv1.OrgMember{Id: "om-mia", UserId: "mia", Role: "org_member"},
				&accessv1.ProjectMember{Id: "pm-mia1", Project: project("p1"), UserId: "mia", Role: "project_reader"},
				&accessv1.ProjectMember{Id: "pm-mia2", Project: project("p2"), UserId: "mia", Role: "project_editor"},
				&accessv1.ProjectMember{Id: "pm-mia2b", Project: project("p2"), UserId: "mia", Role: "project_admin"},
				&accessv1.ProjectMember{Id: "pm-mia3", Project: project("p3"), UserId: "mia", Role: "project_writer"},
				&accessv1.ProjectMember{Id: "pm-admin", Project: project("p1"), UserId: "admin", Role: "project_reader"},
			)
			got := newAccessReplica(f, "mia").pullOnce(0).GetCallerAccess()
			want := &syncplanev1.CallerAccess{
				Seq:             got.GetSeq(),
				Version:         got.GetVersion(),
				Subjects:        []string{"mia"},
				RootPermissions: []string{"view_labels", "view_projects", "view_tasks"},
				MaxAccessDepth:  32,
				AnchorGroups: []*syncplanev1.AnchorGroup{
					{
						Permissions: []string{"edit_tasks", "manage_members", "view_members"},
						Anchors:     []*syncplanev1.AnchorRows{{Entity: "project", Keys: []string{"p2"}}},
					},
					{
						Permissions: []string{"edit_tasks", "view_members"},
						Anchors:     []*syncplanev1.AnchorRows{{Entity: "project", Keys: []string{"p3"}}},
					},
					{
						Permissions: []string{"view_members"},
						Anchors:     []*syncplanev1.AnchorRows{{Entity: "project", Keys: []string{"p1"}}},
					},
				},
			}
			Expect(proto.Equal(got, want)).To(BeTrue(), "%v", got)

			// org_admin at the root holds everything project_reader gives.
			admin := newAccessReplica(f, "admin").pullOnce(0).GetCallerAccess()
			Expect(admin.GetRootPermissions()).To(ContainElement("view_members"))
			Expect(admin.GetAnchorGroups()).To(BeEmpty())
		})

		It("is sent on the first pull and again only when it changes", func() {
			guest := newAccessReplica(f, "guest")
			first := guest.pullOnce(0)
			Expect(heldAt(first.GetCallerAccess())).To(Equal(map[string][]string{
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
			Expect(heldAt(changed)).To(HaveKeyWithValue("view_tasks", []string{"*"}))
		})

		// widen grants user project_reader at eight more projects. A summary
		// of a row or two is smaller than any delta, so the server sends it
		// whole.
		widen := func(user string) {
			GinkgoHelper()
			for i := range 8 {
				id := fmt.Sprintf("pw%d", i)
				f.seed(
					&accessv1.Project{Id: id},
					&accessv1.ProjectMember{Id: "pm-" + user + "-" + id, Project: project(id), UserId: user, Role: "project_reader"},
				)
			}
		}

		// accessOf is what a pull from an empty cursor shows user.
		accessOf := func(user string) *syncplanev1.CallerAccess {
			GinkgoHelper()
			return newAccessReplica(f, user).pullOnce(0).GetCallerAccess()
		}

		It("sends only the rows whose permissions changed", func() {
			widen("writer")
			writer := newAccessReplica(f, "writer")
			writer.sync()
			Expect(writer.access).NotTo(BeNil())

			f.seed(
				&accessv1.ProjectMember{Id: "pm-writer2", Project: project("p2"), UserId: "writer", Role: "project_admin"},
				&accessv1.ProjectMember{Id: "pm-writer3", Project: project("p1"), UserId: "writer", Role: "project_reader"},
			)
			delta := writer.syncDelta()
			Expect(delta.GetRemoved()).To(BeEmpty())
			Expect(heldAt(&syncplanev1.CallerAccess{AnchorGroups: delta.GetAnchorGroups()})).To(Equal(map[string][]string{
				"edit_tasks":     {"project/p2"},
				"manage_members": {"project/p2"},
				"view_members":   {"project/p1", "project/p2"},
				"view_projects":  {"project/p1", "project/p2"},
				"view_tasks":     {"project/p1", "project/p2"},
			}))
			Expect(sameCallerAccess(writer.access, accessOf("writer"))).To(BeTrue(), "%v", writer.access)
			Expect(writer.access.GetVersion()).To(Equal(accessOf("writer").GetVersion()))
		})

		It("removes the rows the caller no longer holds permissions at", func() {
			widen("writer")
			f.seed(&accessv1.ProjectMember{Id: "pm-writer3", Project: project("p1"), UserId: "writer", Role: "project_reader"})
			writer := newAccessReplica(f, "writer")
			writer.sync()

			revoke("project_member", "pm-writer3")
			delta := writer.syncDelta()
			Expect(delta.GetAnchorGroups()).To(BeEmpty())
			Expect(delta.GetRemoved()).To(HaveExactElements(
				&syncplanev1.AnchorRows{Entity: "project", Keys: []string{"p1"}},
			))
			Expect(sameCallerAccess(writer.access, accessOf("writer"))).To(BeTrue(), "%v", writer.access)
		})

		It("sends the whole summary when the root permissions change", func() {
			guest := newAccessReplica(f, "guest")
			guest.sync()
			f.seed(&accessv1.OrgMember{Id: "om-guest", UserId: "guest", Role: "org_member"})
			resp := guest.pullOnce(0)
			Expect(resp.GetCallerAccessDelta()).To(BeNil())
			Expect(resp.GetCallerAccess()).NotTo(BeNil())
		})

		It("sends the whole summary when no cursor-state store holds what the client has", func() {
			f = newAccessFixture(server.WithCursorStateStore(nil))
			f.seedOrg()
			widen("writer")
			writer := newAccessReplica(f, "writer")
			writer.sync()

			// manage_members is no read permission, so the rows do not resync.
			f.seed(&accessv1.ProjectMember{Id: "pm-writer2", Project: project("p2"), UserId: "writer", Role: "project_admin"})
			resp := writer.pullOnce(0)
			Expect(resp.GetReset_()).To(BeFalse())
			Expect(resp.GetCallerAccessDelta()).To(BeNil())
			Expect(heldAt(resp.GetCallerAccess())).To(HaveKeyWithValue("manage_members", []string{"project/p2"}))
		})

		It("stores only the summary when a permission no read depends on changes", func() {
			store := &puttingCursorState{Store: cursorstate.NewMemory(1 << 20)}
			f = newAccessFixture(server.WithCursorStateStore(store))
			f.seedOrg()
			writer := newAccessReplica(f, "writer")
			writer.sync()
			legacy := newAccessReplica(f, "writer")
			legacy.legacy = true
			legacy.sync()
			store.taken()

			f.seed(&accessv1.ProjectMember{Id: "pm-writer2", Project: project("p2"), UserId: "writer", Role: "project_admin"})
			legacy.sync()
			Expect(store.taken()).To(BeEmpty(), "a client that applies no delta needs no stored summary")
			writer.sync()
			puts := store.taken()
			Expect(puts).To(HaveLen(1))
			stored := &syncplanev1.CallerAccess{}
			Expect(proto.Unmarshal(puts[0], stored)).To(Succeed())
			Expect(stored.GetVersion()).To(Equal(writer.access.GetVersion()))
		})

		It("stores the summary on the first pull after a bootstrap, not on the bootstrap", func() {
			store := &puttingCursorState{Store: cursorstate.NewMemory(1 << 20)}
			f = newAccessFixture(server.WithCursorStateStore(store))
			f.seedOrg()
			widen("writer")
			// summaries counts the stored CallerAccess entries among puts.
			// Only those carry a version: a ReadAccess entry has no field 6.
			summaries := func(puts [][]byte) int {
				n := 0
				for _, put := range puts {
					ca := &syncplanev1.CallerAccess{}
					if proto.Unmarshal(put, ca) == nil && ca.GetVersion() != 0 {
						n++
					}
				}
				return n
			}

			req := connect.NewRequest(&syncplanev1.BootstrapRequest{WorkspaceId: ws, ClientId: "c-writer"})
			as("writer", req.Header())
			stream, err := f.sync.Bootstrap(f.ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(stream.Receive()).To(BeTrue())
			begin := stream.Msg().GetBegin()
			for stream.Receive() {
			}
			Expect(stream.Err()).NotTo(HaveOccurred())
			Expect(summaries(store.taken())).To(BeZero(), "the bootstrap stored a summary")

			writer := newAccessReplica(f, "writer")
			writer.cursor, writer.access = begin.GetCursor(), begin.GetCallerAccess()
			writer.sync()
			Expect(summaries(store.taken())).To(Equal(1))

			f.seed(&accessv1.ProjectMember{Id: "pm-writer2", Project: project("p2"), UserId: "writer", Role: "project_admin"})
			Expect(writer.syncDelta().GetBaseVersion()).To(Equal(begin.GetCallerAccess().GetVersion()))
		})

		It("sends the whole summary again to a client that lost it", func() {
			writer := newAccessReplica(f, "writer")
			writer.sync()
			Expect(writer.pullOnce(0).GetCallerAccess()).To(BeNil())

			writer.access = nil
			got := writer.pullOnce(0).GetCallerAccess()
			Expect(sameCallerAccess(got, accessOf("writer"))).To(BeTrue(), "%v", got)
		})

		It("sends the whole summary when the client holds another one than its cursor names", func() {
			widen("writer")
			writer := newAccessReplica(f, "writer")
			writer.sync()
			behind := writer.cursor

			// The client stores the summary a stream page carried, but not
			// the cursor of that page.
			f.seed(&accessv1.ProjectMember{Id: "pm-writer2", Project: project("p2"), UserId: "writer", Role: "project_admin"})
			writer.syncDelta()
			writer.cursor = behind

			f.seed(&accessv1.ProjectMember{Id: "pm-writer3", Project: project("p1"), UserId: "writer", Role: "project_reader"})
			resp := writer.pullOnce(0)
			Expect(resp.GetCallerAccessDelta()).To(BeNil())
			Expect(sameCallerAccess(resp.GetCallerAccess(), accessOf("writer"))).To(BeTrue(), "%v", resp.GetCallerAccess())
		})

		It("sends nothing to a client that holds the current summary under an older cursor", func() {
			writer := newAccessReplica(f, "writer")
			writer.sync()
			behind := writer.cursor
			f.seed(&accessv1.ProjectMember{Id: "pm-writer2", Project: project("p2"), UserId: "writer", Role: "project_admin"})
			writer.sync()

			writer.cursor = behind
			resp := writer.pullOnce(0)
			Expect(resp.GetCallerAccess()).To(BeNil())
			Expect(resp.GetCallerAccessDelta()).To(BeNil())
		})

		It("sends only whole summaries to a client that sends no version", func() {
			widen("writer")
			writer := newAccessReplica(f, "writer")
			writer.legacy = true
			writer.sync()
			Expect(writer.pullOnce(0).GetCallerAccess()).To(BeNil())

			f.seed(&accessv1.ProjectMember{Id: "pm-writer2", Project: project("p2"), UserId: "writer", Role: "project_admin"})
			resp := writer.pullOnce(0)
			Expect(resp.GetCallerAccessDelta()).To(BeNil())
			Expect(heldAt(resp.GetCallerAccess())).To(HaveKeyWithValue("manage_members", []string{"project/p2"}))
		})

		It("sends the version a delta applies to with the whole summary", func() {
			first := accessOf("writer")
			Expect(first.GetVersion()).NotTo(BeZero())
			Expect(accessOf("writer").GetVersion()).To(Equal(first.GetVersion()))
			Expect(accessOf("guest").GetVersion()).NotTo(Equal(first.GetVersion()))
		})

		It("rides on the Begin frame of a bootstrap", func() {
			req := connect.NewRequest(&syncplanev1.BootstrapRequest{WorkspaceId: ws, ClientId: "c-writer"})
			as("writer", req.Header())
			stream, err := f.sync.Bootstrap(f.ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(stream.Receive()).To(BeTrue())
			begin := stream.Msg().GetBegin()
			Expect(heldAt(begin.GetCallerAccess())).To(HaveKeyWithValue("edit_tasks", []string{"project/p2"}))
			for stream.Receive() {
			}
			Expect(stream.Err()).NotTo(HaveOccurred())
		})

		It("lets a snapshot resume when only a permission no read depends on changed", func() {
			f = newAccessFixture(server.WithChunkSize(1), server.WithPermissions(memberHidingPerms{}))
			f.seedOrg()
			// bootstrap takes at most chunks chunks, all of them for 0, and
			// returns the Begin frame and the resume token of the last chunk.
			bootstrap := func(resume *syncplanev1.BootstrapResume, chunks int) (*syncplanev1.BootstrapResponse_Begin, []byte) {
				GinkgoHelper()
				req := connect.NewRequest(&syncplanev1.BootstrapRequest{WorkspaceId: ws, ClientId: "c-writer", Resume: resume})
				as("writer", req.Header())
				stream, err := f.sync.Bootstrap(f.ctx, req)
				Expect(err).NotTo(HaveOccurred())
				defer func() { Expect(stream.Close()).To(Succeed()) }()
				var begin *syncplanev1.BootstrapResponse_Begin
				var token []byte
				for seen := 0; stream.Receive(); {
					if b := stream.Msg().GetBegin(); b != nil {
						begin = b
					}
					if c := stream.Msg().GetChunk(); c != nil {
						token = c.GetResumeToken()
						if seen++; seen == chunks {
							break
						}
					}
				}
				return begin, token
			}
			first, token := bootstrap(nil, 1)
			Expect(token).NotTo(BeEmpty())

			// manage_members is no read permission, and the new grant row is
			// hidden from the writer.
			f.seed(&accessv1.ProjectMember{Id: "pm-writer2", Project: project("p2"), UserId: "writer", Role: "project_admin"})
			second, _ := bootstrap(&syncplanev1.BootstrapResume{Cursor: first.GetCursor(), Token: token}, 0)
			Expect(second.GetResumed()).To(BeTrue())
			Expect(heldAt(second.GetCallerAccess())).To(HaveKeyWithValue("manage_members", []string{"project/p2"}))
		})

		It("reaches an open Subscribe stream when no row the caller can see changed", func() {
			f = newAccessFixture(server.WithPermissions(memberHidingPerms{}))
			f.seedOrg()
			widen("writer")
			writer := newAccessReplica(f, "writer")
			writer.sync()

			req := connect.NewRequest(&syncplanev1.SubscribeRequest{
				ClientId: "c-writer",
				Scopes: []*syncplanev1.WorkspaceScope{{
					WorkspaceId: ws, Cursor: writer.cursor,
					CallerAccessVersion: proto.Uint64(writer.access.GetVersion()),
				}},
				SparseCursor: true,
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
			var page *syncplanev1.PullResponse
			for page.GetCallerAccessDelta() == nil && stream.Receive() {
				Expect(stream.Msg().GetDropped()).To(BeNil(), "the stream dropped the scope")
				Expect(stream.Msg().GetPage().GetCallerAccess()).To(BeNil(), "the whole summary was sent")
				page = stream.Msg().GetPage()
			}
			changed := page.GetCallerAccessDelta()
			Expect(changed.GetBaseVersion()).To(Equal(writer.access.GetVersion()))
			Expect(heldAt(&syncplanev1.CallerAccess{AnchorGroups: changed.GetAnchorGroups()})).To(HaveKeyWithValue("manage_members", []string{"project/p2"}))

			// The frame carries its cursor, so a pull from it gets the next
			// change as a delta from this summary.
			Expect(page.GetNextCursor()).NotTo(BeEmpty())
			cancel()
			writer.access = applyCallerAccess(writer.access, nil, changed)
			writer.cursor = page.GetNextCursor()
			f.seed(&accessv1.ProjectMember{Id: "pm-writer3", Project: project("p1"), UserId: "writer", Role: "project_admin"})
			Expect(writer.syncDelta().GetBaseVersion()).To(Equal(changed.GetVersion()))
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
				Read: true, Requires: []string{"view_projects"},
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
