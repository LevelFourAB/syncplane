package server_test

import (
	"context"
	"fmt"
	"sort"
	"time"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
)

// newDeepFixtureOn is an access fixture over store whose org schema also has
// comments below tasks, two hops below their project.
func newDeepFixtureOn(store storage.Adapter, opts ...server.Option) *accessFixture {
	GinkgoHelper()
	return newAccessFixtureOn(store, append([]server.Option{server.WithEntities(&accessv1.Comment{})}, opts...)...)
}

// newFolderFixtureOn is a fixture over store running the folder schema of the
// access test proto: folders inside folders, docs in folders, and folder
// members who hold roles on one folder and everything below it.
func newFolderFixtureOn(store storage.Adapter, opts ...server.Option) *accessFixture {
	GinkgoHelper()
	return newSchemaFixtureOn(store, server.WithEntities(&accessv1.Folder{}, &accessv1.FolderMember{}, &accessv1.Doc{}), opts...)
}

func task(id string) *accessv1.Task     { return &accessv1.Task{Id: id} }
func folder(id string) *accessv1.Folder { return &accessv1.Folder{Id: id} }

// seedComments writes a comment below each task of seedOrg, and one below a
// task that does not exist.
func (f *accessFixture) seedComments() {
	GinkgoHelper()
	f.seed(
		&accessv1.Comment{Id: "c1", Task: task("t1"), Body: "on one"},
		&accessv1.Comment{Id: "c2", Task: task("t2"), Body: "on two"},
		&accessv1.Comment{Id: "c3", Task: task("t3"), Body: "on none"},
		&accessv1.Comment{Id: "c9", Task: task("t9"), Body: "on a missing task"},
	)
}

// seedFolders writes a folder tree:
//
//	top ─ mid ─ low
//	  └ side
//
// with a doc in each folder. viewer views mid, editor edits side, and boss
// manages top.
func (f *accessFixture) seedFolders() {
	GinkgoHelper()
	f.seed(
		&accessv1.Folder{Id: "top"},
		&accessv1.Folder{Id: "mid", Parent: folder("top")},
		&accessv1.Folder{Id: "low", Parent: folder("mid")},
		&accessv1.Folder{Id: "side", Parent: folder("top")},
		&accessv1.Doc{Id: "d-top", Folder: folder("top")},
		&accessv1.Doc{Id: "d-mid", Folder: folder("mid")},
		&accessv1.Doc{Id: "d-low", Folder: folder("low")},
		&accessv1.Doc{Id: "d-side", Folder: folder("side")},
		&accessv1.FolderMember{Id: "fm-viewer", Folder: folder("mid"), UserId: "viewer", Role: "folder_viewer"},
		&accessv1.FolderMember{Id: "fm-editor", Folder: folder("side"), UserId: "editor", Role: "folder_editor"},
		&accessv1.FolderMember{Id: "fm-boss", Folder: folder("top"), UserId: "boss", Role: "folder_manager"},
	)
}

// rowsOf keeps the rows of the named entity from "entity/key" strings.
func rowsOf(entity string, rows []string) []string {
	var out []string
	for _, r := range rows {
		if len(r) > len(entity) && r[:len(entity)+1] == entity+"/" {
			out = append(out, r)
		}
	}
	return out
}

var _ = Describe("Access model reads below more than one parent", func() {
	var f *accessFixture

	bootstrap := func(user string) []string {
		GinkgoHelper()
		req := connect.NewRequest(&syncplanev1.BootstrapRequest{WorkspaceId: ws, ClientId: "c-" + user})
		as(user, req.Header())
		stream, err := f.sync.Bootstrap(f.ctx, req)
		Expect(err).NotTo(HaveOccurred())
		var rows []string
		for stream.Receive() {
			if chunk, ok := stream.Msg().GetBody().(*syncplanev1.BootstrapResponse_Chunk_); ok {
				for _, rc := range chunk.Chunk.GetRows() {
					rows = append(rows, rc.GetEntity()+"/"+rc.GetKey())
				}
			}
		}
		Expect(stream.Err()).NotTo(HaveOccurred())
		sort.Strings(rows)
		return rows
	}

	queryKeys := func(user, entity string) []string {
		GinkgoHelper()
		req := connect.NewRequest(&syncplanev1.QueryRequest{WorkspaceId: ws, Entity: entity})
		as(user, req.Header())
		resp, err := f.sync.Query(f.ctx, req)
		Expect(err).NotTo(HaveOccurred())
		var keys []string
		for _, rc := range resp.Msg.GetRows() {
			keys = append(keys, rc.GetKey())
		}
		sort.Strings(keys)
		return keys
	}

	Context("comments below tasks below projects", func() {
		for _, indexed := range []bool{false, true} {
			Context(map[bool]string{false: "before the indexes are built", true: "with the indexes built"}[indexed], func() {
				BeforeEach(func() {
					f = newDeepFixtureOn(newTestStore())
					f.seedOrg()
					f.seedComments()
					if indexed {
						Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
					}
				})

				It("shows a project role the comments two hops below the project", func() {
					Expect(rowsOf("comment", f.pull("guest"))).To(Equal([]string{"comment/c1"}))
					Expect(rowsOf("comment", f.pull("writer"))).To(Equal([]string{"comment/c2"}))
				})

				It("shows a root role every comment, those below no project included", func() {
					Expect(rowsOf("comment", f.pull("member"))).To(Equal([]string{
						"comment/c1", "comment/c2", "comment/c3", "comment/c9",
					}))
				})

				It("bootstraps the comments two hops below the project", func() {
					Expect(rowsOf("comment", bootstrap("guest"))).To(Equal([]string{"comment/c1"}))
					Expect(rowsOf("comment", bootstrap("member"))).To(HaveLen(4))
				})

				It("answers a Query with the comments two hops below the project", func() {
					Expect(queryKeys("guest", "comment")).To(Equal([]string{"c1"}))
					Expect(queryKeys("member", "comment")).To(Equal([]string{"c1", "c2", "c3", "c9"}))
				})
			})
		}

		It("admits a room anchored on a comment the caller can read through its task", func() {
			f = newDeepFixtureOn(newTestStore())
			f.seedOrg()
			f.seedComments()
			watch := func(key string) *syncplanev1.WatchResponse {
				GinkgoHelper()
				req := connect.NewRequest(&syncplanev1.WatchRequest{
					SessionId: "s-" + key,
					Topics:    []*syncplanev1.Topic{{WorkspaceId: ws, Name: "cursors", Entity: "comment", Key: key}},
				})
				as("guest", req.Header())
				stream, err := f.presence.Watch(GinkgoT().Context(), req)
				Expect(err).NotTo(HaveOccurred())
				for stream.Receive() {
					if !stream.Msg().GetHeartbeat() {
						return stream.Msg()
					}
				}
				Fail("stream closed early: " + stream.Err().Error())
				return nil
			}
			Expect(watch("c1").GetSnapshot()).NotTo(BeNil())
			Expect(watch("c2").GetDropped().GetReason()).To(Equal(syncplanev1.TopicDropped_REASON_ACCESS_REVOKED))
		})

		It("bootstraps a caller below the root without scanning comments or tasks", func() {
			counting := &pageScanCountingStore{Adapter: newTestStore(), scanned: map[string]int{}}
			f = newDeepFixtureOn(counting)
			f.seedOrg()
			f.seedComments()
			for i := range 30 {
				f.seed(
					&accessv1.Task{Id: fmt.Sprintf("x%02d", i), Project: project("p2")},
					&accessv1.Comment{Id: fmt.Sprintf("cx%02d", i), Task: task(fmt.Sprintf("x%02d", i))},
				)
			}
			Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
			counting.reset()
			Expect(rowsOf("comment", bootstrap("guest"))).To(Equal([]string{"comment/c1"}))
			for _, entity := range []string{"comment", "task"} {
				Expect(counting.count(entity)).To(BeZero(), entity)
			}
		})
	})

	Context("a recursive folder tree", func() {
		BeforeEach(func() {
			f = newFolderFixtureOn(newTestStore())
			f.seedFolders()
		})

		It("shows a role on a folder that folder and everything below it", func() {
			Expect(f.pull("viewer")).To(Equal([]string{
				"doc/d-low", "doc/d-mid", "folder/low", "folder/mid", "folder_member/fm-viewer",
			}))
			Expect(f.pull("boss")).To(HaveLen(11))
		})

		It("bootstraps and queries what the role reaches", func() {
			Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
			Expect(bootstrap("viewer")).To(Equal([]string{
				"doc/d-low", "doc/d-mid", "folder/low", "folder/mid", "folder_member/fm-viewer",
			}))
			Expect(queryKeys("viewer", "doc")).To(Equal([]string{"d-low", "d-mid"}))
			Expect(queryKeys("editor", "doc")).To(Equal([]string{"d-side"}))
		})

		// A server with a lower limit than the one that wrote the rows, so rows
		// deeper than the limit exist.
		It("stops walking a chain at MaxAccessDepth", func() {
			store := newTestStore()
			newFolderFixtureOn(store).seedFolders()
			f = newFolderFixtureOn(store, server.WithLimits(server.Limits{MaxAccessDepth: 2}))
			// d-low is three hops below top: low, mid, top.
			Expect(rowsOf("doc", f.pull("boss"))).To(Equal([]string{"doc/d-mid", "doc/d-side", "doc/d-top"}))
		})
	})
})

var _ = Describe("Access model writes below more than one parent", func() {
	var (
		f       *accessFixture
		clients int
	)

	write := func(user string, muts ...func(client string, id uint64) *syncplanev1.Mutation) []*syncplanev1.MutationResult {
		GinkgoHelper()
		clients++
		client := fmt.Sprintf("c%d", clients)
		var batch []*syncplanev1.Mutation
		for i, m := range muts {
			batch = append(batch, m(client, uint64(i+1)))
		}
		return f.push(user, client, batch...)
	}
	put := func(entity, key string, value proto.Message) func(string, uint64) *syncplanev1.Mutation {
		return func(c string, id uint64) *syncplanev1.Mutation { return putMutation(c, id, entity, key, value) }
	}
	patch := func(entity, key string, value proto.Message, paths ...string) func(string, uint64) *syncplanev1.Mutation {
		return func(c string, id uint64) *syncplanev1.Mutation {
			return patchMutation(c, id, entity, key, value, paths...)
		}
	}
	del := func(entity, key string) func(string, uint64) *syncplanev1.Mutation {
		return func(c string, id uint64) *syncplanev1.Mutation { return deleteMutation(c, id, entity, key) }
	}
	one := func(user string, m func(string, uint64) *syncplanev1.Mutation) *syncplanev1.MutationResult {
		GinkgoHelper()
		results := write(user, m)
		Expect(results).To(HaveLen(1))
		return results[0]
	}
	applied := func(r *syncplanev1.MutationResult) {
		GinkgoHelper()
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED), r.GetErrorMessage())
	}
	refused := func(r *syncplanev1.MutationResult, code string) {
		GinkgoHelper()
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
		Expect(r.GetErrorCode()).To(Equal(code), r.GetErrorMessage())
	}
	comment := func(id, taskID string) *accessv1.Comment {
		return &accessv1.Comment{Id: id, Task: task(taskID), Body: "b " + id}
	}
	taskIn := func(id, projectID string) *accessv1.Task {
		return &accessv1.Task{Id: id, Project: project(projectID), Title: "t " + id}
	}

	Context("comments below tasks below projects", func() {
		BeforeEach(func() {
			f = newDeepFixtureOn(newTestStore())
			f.seedOrg()
			f.seedComments()
			f.seed(
				&accessv1.ProjectMember{Id: "pm-mover1", Project: project("p1"), UserId: "mover", Role: "project_writer"},
				&accessv1.ProjectMember{Id: "pm-mover2", Project: project("p2"), UserId: "mover", Role: "project_writer"},
				&accessv1.ProjectMember{Id: "pm-blind1", Project: project("p1"), UserId: "blind", Role: "project_editor"},
				&accessv1.ProjectMember{Id: "pm-blind2", Project: project("p2"), UserId: "blind", Role: "project_editor"},
			)
		})

		It("writes a comment where the caller holds the write permission two hops up", func() {
			applied(one("writer", put("comment", "n2", comment("n2", "t2"))))
			applied(one("writer", patch("comment", "c2", &accessv1.Comment{Id: "c2", Body: "edited"}, "body")))
			applied(one("writer", del("comment", "c2")))
		})

		It("refuses a comment write where the caller does not", func() {
			refused(one("writer", put("comment", "n1", comment("n1", "t1"))), server.CodePermissionDenied)
			refused(one("guest", put("comment", "n1", comment("n1", "t1"))), server.CodePermissionDenied)
			refused(one("writer", del("comment", "c1")), server.CodePermissionDenied)
			refused(one("writer", put("comment", "c2", comment("c2", "t1"))), server.CodePermissionDenied)
		})

		It("moves a task with the write permission at both ends and the read permission of the rows below it", func() {
			applied(one("mover", patch("task", "t1", taskIn("t1", "p2"), "project")))
			Expect(rowsOf("comment", f.pull("writer"))).To(Equal([]string{"comment/c1", "comment/c2"}))
		})

		It("refuses a move by a caller who cannot read the rows below the moved row", func() {
			refused(one("blind", patch("task", "t1", taskIn("t1", "p2"), "project")), server.CodePermissionDenied)
			applied(one("blind", patch("task", "t1", &accessv1.Task{Id: "t1", Title: "renamed"}, "title")))
		})

		It("refuses to delete a task while a comment below it is live", func() {
			r := one("writer", del("task", "t2"))
			refused(r, server.CodeConflict)
			Expect(r.GetErrorMessage()).To(ContainSubstring(`access parent of a live comment row`))
			results := write("writer", del("comment", "c2"), del("task", "t2"))
			applied(results[0])
			applied(results[1])
		})

		It("refuses to delete a project while a task below it is live", func() {
			refused(one("admin", del("project", "p2")), server.CodeConflict)
		})

		It("lets a trusted write delete a task with comments below it", func() {
			Expect(f.srv.Mutate(server.Trusted(f.ctx), ws, func(tx *server.EntityTx) error {
				return tx.Delete(&accessv1.Task{}, "t2")
			})).To(Succeed())
			Expect(rowsOf("comment", f.pull("writer"))).To(BeEmpty())
		})

		It("checks a mutator invocation at a comment against the chain above it", func() {
			ok := func(user, key string) bool {
				GinkgoHelper()
				var can bool
				ctx := server.WithPrincipal(f.ctx, server.Principal{Subject: user})
				Expect(f.srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
					var err error
					can, err = tx.Can("edit_tasks", &accessv1.Comment{}, key)
					return err
				})).To(Succeed())
				return can
			}
			Expect(ok("writer", "c2")).To(BeTrue())
			Expect(ok("writer", "c1")).To(BeFalse())
			Expect(ok("admin", "c9")).To(BeTrue())
		})
	})

	Context("a recursive folder tree", func() {
		BeforeEach(func() {
			f = newFolderFixtureOn(newTestStore())
			f.seedFolders()
		})

		It("refuses a move that would put a folder below itself, on every write path", func() {
			r := one("boss", patch("folder", "top", &accessv1.Folder{Id: "top", Parent: folder("low")}, "parent"))
			refused(r, server.CodeInvalidArgument)
			Expect(r.GetErrorMessage()).To(ContainSubstring("would sit below itself"))
			refused(one("boss", patch("folder", "mid", &accessv1.Folder{Id: "mid", Parent: folder("mid")}, "parent")),
				server.CodeInvalidArgument)
			err := f.srv.Mutate(server.Trusted(f.ctx), ws, func(tx *server.EntityTx) error {
				return tx.Put(&accessv1.Folder{Id: "top", Parent: folder("low")})
			})
			Expect(err).To(MatchError(ContainSubstring("would sit below itself")))
		})

		It("moves a folder to another place in the tree", func() {
			applied(one("boss", patch("folder", "low", &accessv1.Folder{Id: "low", Parent: folder("side")}, "parent")))
			Expect(rowsOf("doc", f.pull("editor"))).To(Equal([]string{"doc/d-low", "doc/d-side"}))
			Expect(rowsOf("doc", f.pull("viewer"))).To(Equal([]string{"doc/d-mid"}))
		})

		It("refuses a folder deeper than MaxAccessDepth", func() {
			f = newFolderFixtureOn(newTestStore(), server.WithLimits(server.Limits{MaxAccessDepth: 2}))
			f.seed(
				&accessv1.Folder{Id: "top"},
				&accessv1.Folder{Id: "mid", Parent: folder("top")},
				&accessv1.FolderMember{Id: "fm-boss", Folder: folder("top"), UserId: "boss", Role: "folder_manager"},
			)
			applied(one("boss", put("folder", "low", &accessv1.Folder{Id: "low", Parent: folder("mid")})))
			r := one("boss", put("folder", "lower", &accessv1.Folder{Id: "lower", Parent: folder("low")}))
			refused(r, server.CodeInvalidArgument)
			Expect(r.GetErrorMessage()).To(ContainSubstring("more than MaxAccessDepth (2) hops"))
		})

		It("grants a role on a folder below the one where the writer holds it", func() {
			applied(one("boss", put("folder_member", "fm-new", &accessv1.FolderMember{
				Id: "fm-new", Folder: folder("low"), UserId: "newbie", Role: "folder_editor",
			})))
			Expect(f.pull("newbie")).To(ContainElements("doc/d-low", "folder/low"))
			refused(one("viewer", put("folder_member", "fm-new2", &accessv1.FolderMember{
				Id: "fm-new2", Folder: folder("low"), UserId: "newbie2", Role: "folder_viewer",
			})), server.CodePermissionDenied)
		})
	})
})

var _ = Describe("Access model sync below more than one parent", func() {
	var f *accessFixture

	trusted := func(fn func(tx *server.EntityTx) error) {
		GinkgoHelper()
		Expect(f.srv.Mutate(server.Trusted(f.ctx), ws, fn)).To(Succeed())
	}
	moveTask := func(id, projectID string) {
		GinkgoHelper()
		trusted(func(tx *server.EntityTx) error {
			return tx.Patch(&accessv1.Task{Id: id, Project: project(projectID)}, []string{"project"}, nil)
		})
	}
	converged := func(r *accessReplica) {
		GinkgoHelper()
		Expect(r.view()).To(Equal(f.pull(r.user)))
		Expect(r.resets).To(BeZero())
	}
	// settled asserts that a replica's next pull has nothing to send: no
	// resync of moves its rows already reflect.
	settled := func(r *accessReplica) {
		GinkgoHelper()
		resp := r.pullOnce(0)
		Expect(resp.GetHasMore()).To(BeFalse())
		Expect(resp.GetPatch().GetChanges()).To(BeEmpty())
	}

	Context("comments below tasks below projects", func() {
		BeforeEach(func() {
			f = newDeepFixtureOn(newTestStore())
			f.seedOrg()
			f.seedComments()
			Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
		})

		It("moves the comments of a task that moves to another project", func() {
			guest := newAccessReplica(f, "guest")
			writer := newAccessReplica(f, "writer")
			guest.sync()
			writer.sync()
			Expect(guest.view()).To(ContainElement("comment/c1"))
			Expect(writer.view()).NotTo(ContainElement("comment/c1"))

			moveTask("t1", "p2")
			guest.sync()
			writer.sync()
			Expect(guest.view()).NotTo(ContainElements("task/t1", "comment/c1"))
			Expect(writer.view()).To(ContainElements("task/t1", "comment/c1"))
			converged(guest)
			converged(writer)
			settled(guest)
			settled(writer)
		})

		It("serves a move that changes nothing the caller reads as an ordinary page", func() {
			member := newAccessReplica(f, "member")
			guest := newAccessReplica(f, "guest")
			member.sync()
			guest.sync()

			moveTask("t2", "p1")
			moveTask("t2", "p2")
			Expect(member.pullOnce(0).GetHasMore()).To(BeFalse(), "a caller who reads at the root resynced")
			Expect(guest.pullOnce(0).GetHasMore()).To(BeFalse(), "a task that came back resynced")
			converged(member)
			converged(guest)
		})

		It("moves the comments of a task twice within one sequence", func() {
			guest := newAccessReplica(f, "guest")
			guest.sync()
			trusted(func(tx *server.EntityTx) error {
				if err := tx.Patch(&accessv1.Task{Id: "t1", Project: project("p2")}, []string{"project"}, nil); err != nil {
					return err
				}
				return tx.Patch(&accessv1.Task{Id: "t1", Project: project("p1")}, []string{"project"}, nil)
			})
			guest.sync()
			Expect(guest.view()).To(ContainElement("comment/c1"))
			converged(guest)
		})

		It("tombstones the comments of a task a trusted write deletes", func() {
			writer := newAccessReplica(f, "writer")
			writer.sync()
			Expect(writer.view()).To(ContainElement("comment/c2"))
			trusted(func(tx *server.EntityTx) error { return tx.Delete(&accessv1.Task{}, "t2") })
			writer.sync()
			Expect(writer.view()).NotTo(ContainElement("comment/c2"))
			converged(writer)
		})

		It("sends the comments a new task takes over", func() {
			guest := newAccessReplica(f, "guest")
			guest.sync()
			Expect(guest.view()).NotTo(ContainElement("comment/c9"))
			f.seed(&accessv1.Task{Id: "t9", Project: project("p1")})
			guest.sync()
			Expect(guest.view()).To(ContainElements("task/t9", "comment/c9"))
			converged(guest)
		})

		It("resyncs a move and a change of grants in one pass", func() {
			guest := newAccessReplica(f, "guest")
			guest.sync()
			moveTask("t2", "p1")
			f.seed(&accessv1.ProjectMember{Id: "pm-guest2", Project: project("p2"), UserId: "guest", Role: "project_reader"})
			moveTask("t1", "p2")
			guest.sync()
			Expect(guest.view()).To(ContainElements("comment/c1", "comment/c2"))
			converged(guest)
			settled(guest)
		})

		It("resets a client when a row moves again during a resync of its moves", func() {
			for i := range 5 {
				f.seed(&accessv1.Comment{Id: fmt.Sprintf("cx%d", i), Task: task("t2")})
			}
			guest := newAccessReplica(f, "guest")
			guest.sync()
			moveTask("t2", "p1")
			Expect(guest.pullOnce(1).GetHasMore()).To(BeTrue())
			moveTask("t2", "p2")
			Expect(guest.pullOnce(1).GetReset_()).To(BeTrue())
		})

		It("resyncs an open Subscribe stream when a task moves", func() {
			f = newDeepFixtureOn(newTestStore(), server.WithHeartbeat(50*time.Millisecond))
			f.seedOrg()
			f.seedComments()
			writer := newAccessReplica(f, "writer")
			writer.sync()

			req := connect.NewRequest(&syncplanev1.SubscribeRequest{
				ClientId: "c-writer",
				Scopes:   []*syncplanev1.WorkspaceScope{{WorkspaceId: ws, Cursor: writer.cursor}},
			})
			as("writer", req.Header())
			ctx, cancel := context.WithTimeout(f.ctx, 10*time.Second)
			defer cancel()
			stream, err := f.sync.Subscribe(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			moveTask("t1", "p2")
			seen := map[string]bool{}
			for stream.Receive() && !seen["comment/c1"] {
				msg := stream.Msg()
				Expect(msg.GetDropped()).To(BeNil(), "the stream dropped the scope")
				for _, rc := range msg.GetPage().GetPatch().GetChanges() {
					if !rc.GetTombstone() {
						seen[rc.GetEntity()+"/"+rc.GetKey()] = true
					}
				}
			}
			Expect(seen).To(HaveKey("comment/c1"))
		})

		It("purges the moves a collected tombstone horizon covers", func() {
			f = newDeepFixtureOn(newTestStore(), server.WithGC(-time.Second, 0))
			f.seedOrg()
			f.seedComments()
			moveTask("t1", "p2")
			trusted(func(tx *server.EntityTx) error { return tx.Delete(&accessv1.Comment{}, "c3") })
			Expect(countRows(f.store, accessMovesEntityName)).To(Equal(1))
			Expect(f.srv.CollectGarbage(f.ctx, ws)).To(Succeed())
			Expect(countRows(f.store, accessMovesEntityName)).To(BeZero())
		})
	})

	Context("a recursive folder tree", func() {
		BeforeEach(func() {
			f = newFolderFixtureOn(newTestStore())
			f.seedFolders()
			Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
		})

		It("moves a subtree to another folder", func() {
			viewer := newAccessReplica(f, "viewer")
			editor := newAccessReplica(f, "editor")
			viewer.sync()
			editor.sync()
			trusted(func(tx *server.EntityTx) error {
				return tx.Patch(&accessv1.Folder{Id: "low", Parent: folder("side")}, []string{"parent"}, nil)
			})
			viewer.sync()
			editor.sync()
			Expect(viewer.view()).NotTo(ContainElements("folder/low", "doc/d-low"))
			Expect(editor.view()).To(ContainElements("folder/low", "doc/d-low"))
			converged(viewer)
			converged(editor)
			settled(viewer)
		})
	})
})

// accessMovesEntityName is the storage entity of the server's access move
// log, which the specs count rows of.
const accessMovesEntityName = "syncplane_access_moves"

// countRows counts the live rows of entity in the test workspace, below the
// engine.
func countRows(store storage.Adapter, entity string) int {
	GinkgoHelper()
	n := 0
	Expect(store.WithReadTx(GinkgoT().Context(), ws, func(tx storage.ReadTx) error {
		return tx.ScanEntity(entity, func(storage.Row) error {
			n++
			return nil
		})
	})).To(Succeed())
	return n
}
