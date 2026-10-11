package server_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// Limits.MaxAccessDepth says a write that would put a row deeper than the
// limit in a recursive chain is refused. A move of a row carries the rows
// below it, so it can put them deeper too.
var _ = Describe("Access model depth of the rows below a moved row", func() {
	var f *accessFixture

	// Given, with MaxAccessDepth 2:
	//
	//	top ─ mid ─ low
	//	  └ side
	//
	// low is two hops below top, the most the limit allows, and boss manages
	// top.
	BeforeEach(func() {
		f = newFolderFixtureOn(newTestStore(), server.WithLimits(server.Limits{MaxAccessDepth: 2}))
		f.seed(
			&accessv1.Folder{Id: "top"},
			&accessv1.Folder{Id: "mid", Parent: folder("top")},
			&accessv1.Folder{Id: "low", Parent: folder("mid")},
			&accessv1.Folder{Id: "side", Parent: folder("top")},
			&accessv1.FolderMember{Id: "fm-boss", Folder: folder("top"), UserId: "boss", Role: "folder_manager"},
		)
		Expect(f.pull("boss")).To(ContainElement("folder/low"))
	})

	It("refuses a move that would put a row below the moved row deeper than MaxAccessDepth", func() {
		// When: boss moves mid below side, which puts low three hops below top.
		results := f.push("boss", "c-boss",
			patchMutation("c-boss", 1, "folder", "mid", &accessv1.Folder{Id: "mid", Parent: folder("side")}, "parent"))

		// Then: the move is refused, and boss still reads low.
		Expect(results).To(HaveLen(1))
		Expect(results[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED),
			"low would sit three hops below top")
		Expect(results[0].GetErrorCode()).To(Equal(server.CodeInvalidArgument))
		Expect(f.pull("boss")).To(ContainElement("folder/low"))
	})

	It("refuses a trusted move that would put a row below the moved row deeper than MaxAccessDepth", func() {
		// When: a trusted write moves mid below side.
		err := f.srv.Mutate(server.Trusted(f.ctx), ws, func(tx *server.EntityTx) error {
			return tx.Patch(&accessv1.Folder{Id: "mid", Parent: folder("side")}, []string{"parent"}, nil)
		})

		// Then: it is refused as every other write path refuses a row deeper
		// than the limit.
		Expect(err).To(MatchError(ContainSubstring("MaxAccessDepth")))
	})

	It("allows a move that does not lengthen the path above the moved row", func() {
		// Given: below low sits deep, three hops below top, as a limit lowered
		// since can leave it, and other is a second folder without a parent.
		f.seed(&accessv1.Folder{Id: "other"})
		f.plant("folder", "deep", &accessv1.Folder{Id: "deep", Parent: folder("low")})

		// When: a trusted write moves mid from top to other.
		err := f.srv.Mutate(server.Trusted(f.ctx), ws, func(tx *server.EntityTx) error {
			return tx.Patch(&accessv1.Folder{Id: "mid", Parent: folder("other")}, []string{"parent"}, nil)
		})

		// Then: it is allowed, since no row below mid sits deeper than before.
		Expect(err).NotTo(HaveOccurred())
	})
})
