package server_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// A move compares what the client's rows reflect with what holds now. The
// rows above a moved row can change too, after the move, while nothing sits
// below them any more. These specs pin that the comparison still reads those
// rows as the client's rows reflect them.
var _ = Describe("Access model sync of a move away from a row that changes afterwards", func() {
	var f *accessFixture

	trusted := func(fn func(tx *server.EntityTx) error) {
		GinkgoHelper()
		Expect(f.srv.Mutate(server.Trusted(f.ctx), ws, fn)).To(Succeed())
	}

	BeforeEach(func() {
		f = newSchemaFixtureOn(newTestStore(), withinEntities())
		f.seedDossiers()
		Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
	})

	It("removes a memo whose matter moved out of an open dossier that then turned private", func() {
		// Given dossier d-x, open, with matter m-x and memo memo-x below
		// it, which cora reads as a clerk
		f.seed(
			&accessv1.Dossier{Id: "d-x", Title: "x"},
			&accessv1.Matter{Id: "m-x", Dossier: dossier("d-x"), Title: "x"},
			&accessv1.Memo{Id: "memo-x", Matter: matter("m-x"), Title: "x"},
		)
		cora := newAccessReplica(f, "cora")
		cora.sync()
		Expect(cora.view()).To(ContainElement("memo/memo-x"))

		// When m-x moves to the private dossier d-priv, and d-x, with
		// nothing below it now, turns private
		trusted(func(tx *server.EntityTx) error {
			return tx.Patch(&accessv1.Matter{Id: "m-x", Dossier: dossier("d-priv")}, []string{"dossier"}, nil)
		})
		trusted(func(tx *server.EntityTx) error {
			return tx.Patch(&accessv1.Dossier{Id: "d-x", Private: true}, []string{"private"}, nil)
		})
		cora.sync()

		// Then cora no longer holds memo-x, which sits in a private dossier
		Expect(cora.view()).NotTo(ContainElement("memo/memo-x"))
		Expect(cora.view()).To(Equal(f.pull("cora")))
	})

	It("removes a memo whose matter was deleted from an open dossier that then turned private", func() {
		// Given memo-x below matter m-x in the open dossier d-x
		f.seed(
			&accessv1.Dossier{Id: "d-x", Title: "x"},
			&accessv1.Matter{Id: "m-x", Dossier: dossier("d-x"), Title: "x"},
			&accessv1.Memo{Id: "memo-x", Matter: matter("m-x"), Title: "x"},
		)
		cora := newAccessReplica(f, "cora")
		cora.sync()
		Expect(cora.view()).To(ContainElement("memo/memo-x"))

		// When a trusted write deletes m-x, and d-x then turns private
		trusted(func(tx *server.EntityTx) error { return tx.Delete(&accessv1.Matter{}, "m-x") })
		trusted(func(tx *server.EntityTx) error {
			return tx.Patch(&accessv1.Dossier{Id: "d-x", Private: true}, []string{"private"}, nil)
		})
		cora.sync()

		// Then cora no longer holds memo-x, which sits below no matter
		Expect(cora.view()).NotTo(ContainElement("memo/memo-x"))
		Expect(cora.view()).To(Equal(f.pull("cora")))
	})

	It("sends a memo whose matter moved out of a private dossier that then turned open", func() {
		// Given dossier d-y, private, with matter m-y and memo memo-y
		// below it, which cora does not read
		f.seed(
			&accessv1.Dossier{Id: "d-y", Title: "y", Private: true},
			&accessv1.Matter{Id: "m-y", Dossier: dossier("d-y"), Title: "y"},
			&accessv1.Memo{Id: "memo-y", Matter: matter("m-y"), Title: "y"},
		)
		cora := newAccessReplica(f, "cora")
		cora.sync()
		Expect(cora.view()).NotTo(ContainElement("memo/memo-y"))

		// When m-y moves to the open dossier d-open, and d-y, with nothing
		// below it now, turns open
		trusted(func(tx *server.EntityTx) error {
			return tx.Patch(&accessv1.Matter{Id: "m-y", Dossier: dossier("d-open")}, []string{"dossier"}, nil)
		})
		trusted(func(tx *server.EntityTx) error {
			return tx.Patch(&accessv1.Dossier{Id: "d-y", Private: false}, []string{"private"}, nil)
		})
		cora.sync()

		// Then cora holds memo-y, which sits in an open dossier now
		Expect(cora.view()).To(ContainElement("memo/memo-y"))
		Expect(cora.view()).To(Equal(f.pull("cora")))
	})

	It("sends a memo whose matter moved away from a dossier key that was then created open", func() {
		// Given matter m-z, which names the dossier d-new that does not
		// exist, with memo memo-z below it, which cora does not read
		f.seed(
			&accessv1.Matter{Id: "m-z", Dossier: dossier("d-new"), Title: "z"},
			&accessv1.Memo{Id: "memo-z", Matter: matter("m-z"), Title: "z"},
		)
		cora := newAccessReplica(f, "cora")
		cora.sync()
		Expect(cora.view()).NotTo(ContainElement("memo/memo-z"))

		// When m-z moves to the open dossier d-open, and d-new, with nothing
		// below it, is created open
		trusted(func(tx *server.EntityTx) error {
			return tx.Patch(&accessv1.Matter{Id: "m-z", Dossier: dossier("d-open")}, []string{"dossier"}, nil)
		})
		f.seed(&accessv1.Dossier{Id: "d-new", Title: "new"})
		cora.sync()

		// Then cora holds memo-z, which sits in an open dossier now
		Expect(cora.view()).To(ContainElement("memo/memo-z"))
		Expect(cora.view()).To(Equal(f.pull("cora")))
	})

	It("removes a doc whose folder moved out of a shared folder that then moved too", func() {
		// Given the folder tree fa ─ fb ─ fc with doc dx in fc, where
		// viewer reads fa and everything below it
		f = newFolderFixtureOn(newTestStore())
		f.seed(
			&accessv1.Folder{Id: "fa"},
			&accessv1.Folder{Id: "fb", Parent: folder("fa")},
			&accessv1.Folder{Id: "fc", Parent: folder("fb")},
			&accessv1.Folder{Id: "fz"},
			&accessv1.Doc{Id: "dx", Folder: folder("fc")},
			&accessv1.FolderMember{Id: "fm-viewer", Folder: folder("fa"), UserId: "viewer", Role: "folder_viewer"},
		)
		Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
		viewer := newAccessReplica(f, "viewer")
		viewer.sync()
		Expect(viewer.view()).To(ContainElement("doc/dx"))

		// When fc moves to fz, which viewer does not read, and fb, with
		// nothing below it now, moves to the top of the tree
		trusted(func(tx *server.EntityTx) error {
			return tx.Patch(&accessv1.Folder{Id: "fc", Parent: folder("fz")}, []string{"parent"}, nil)
		})
		trusted(func(tx *server.EntityTx) error {
			return tx.Patch(&accessv1.Folder{Id: "fb"}, []string{"parent"}, nil)
		})
		viewer.sync()

		// Then viewer no longer holds dx
		Expect(viewer.view()).NotTo(ContainElement("doc/dx"))
		Expect(viewer.view()).To(Equal(f.pull("viewer")))
	})
})
