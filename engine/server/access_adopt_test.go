package server_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// A row whose access parent does not exist anchors at nothing, and only a
// caller who reads at the root reads it. A create of the missing parent puts
// those rows below it, as a move would.
//
// The fixture is seedOrg with seedComments: t1 is in p1, which writer cannot
// read; writer writes in p2; c9 is a comment on t9, which does not exist.
var _ = Describe("Access model creates of a row that rows already sit below", func() {
	var f *accessFixture

	BeforeEach(func() {
		f = newDeepFixtureOn(newTestStore())
		f.seedOrg()
		f.seedComments()
	})

	createTask := func(user, id, projectID string) *syncplanev1.MutationResult {
		GinkgoHelper()
		results := f.push(user, "c-"+user+"-"+id,
			putMutation("c-"+user+"-"+id, 1, "task", id, &accessv1.Task{Id: id, Project: project(projectID), Title: "new"}))
		Expect(results).To(HaveLen(1))
		return results[0]
	}

	It("does not show a caller a comment it could not read by creating the task the comment names", func() {
		// Given: c9 names t9, which does not exist, so writer does not read c9.
		Expect(f.pull("writer")).NotTo(ContainElement("comment/c9"))

		// When: writer creates t9 in p2, where it writes tasks.
		r := createTask("writer", "t9", "p2")
		Expect(r.GetErrorCode()).To(Equal(server.CodeConflict), r.GetStatus().String())

		// Then: writer still does not read c9, which nobody let it read.
		Expect(f.pull("writer")).NotTo(ContainElement("comment/c9"))
	})

	It("does not show a caller the comments of a deleted private task by creating a task with its key", func() {
		// Given: a trusted write deletes t1 in p1, and its comment c1 stays.
		Expect(f.srv.Mutate(server.Trusted(f.ctx), ws, func(tx *server.EntityTx) error {
			return tx.Delete(&accessv1.Task{}, "t1")
		})).To(Succeed())
		Expect(f.pull("writer")).NotTo(ContainElement("comment/c1"))

		// When: writer creates a task with the key t1 in p2.
		r := createTask("writer", "t1", "p2")
		Expect(r.GetErrorCode()).To(Equal(server.CodeConflict), r.GetStatus().String())

		// Then: writer does not read c1, which was written in p1.
		Expect(f.pull("writer")).NotTo(ContainElement("comment/c1"))
	})
})

var _ = Describe("Access model creates of a row that grant rows name as their anchor", func() {
	It("does not give a role to the subject of a grant left from a deleted folder when another caller creates a folder with its key", func() {
		// Given: old was a folder below top that mallory managed. A trusted
		// write deleted it, and the grant row that names it stayed. editor
		// edits top.
		f := newFolderFixtureOn(newTestStore())
		f.seed(
			&accessv1.Folder{Id: "top"},
			&accessv1.Folder{Id: "old", Parent: folder("top")},
			&accessv1.FolderMember{Id: "fm-mallory", Folder: folder("old"), UserId: "mallory", Role: "folder_manager"},
			&accessv1.FolderMember{Id: "fm-editor", Folder: folder("top"), UserId: "editor", Role: "folder_editor"},
		)
		Expect(f.srv.Mutate(server.Trusted(f.ctx), ws, func(tx *server.EntityTx) error {
			return tx.Delete(&accessv1.Folder{}, "old")
		})).To(Succeed())

		// When: editor creates a new folder below top with the key old, and a
		// doc in it.
		results := f.push("editor", "c-editor",
			putMutation("c-editor", 1, "folder", "old", &accessv1.Folder{Id: "old", Parent: folder("top"), Name: "new"}),
			putMutation("c-editor", 2, "doc", "d-new", &accessv1.Doc{Id: "d-new", Folder: folder("old"), Title: "plans"}))
		Expect(results).To(HaveLen(2))

		// Then: the create of the folder is refused as a conflict, and
		// mallory, whom nobody gave a role on the new folder, does not read
		// its doc. mallory's grant names a folder that does not exist, so a
		// pull by mallory can also fail with no access.
		Expect(results[0].GetErrorCode()).To(Equal(server.CodeConflict), results[0].GetStatus().String())
		rows, err := f.pullErr("mallory")
		if err == nil {
			Expect(rows).NotTo(ContainElement("doc/d-new"))
		}
	})
})
