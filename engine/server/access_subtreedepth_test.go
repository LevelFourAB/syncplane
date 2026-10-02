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

// MaxAccessDepth bounds every row of a recursive chain, the rows of an entity
// below the recursive one included: a doc in a folder is one hop below it.
var _ = Describe("Access model depth of a non-recursive row below a recursive chain", func() {
	var f *accessFixture

	// Given, with MaxAccessDepth 2: top ─ mid ─ low, low two hops below top,
	// side without a parent, and boss manages top and side.
	BeforeEach(func() {
		f = newFolderFixtureOn(newTestStore(), server.WithLimits(server.Limits{MaxAccessDepth: 2}))
		f.seed(
			&accessv1.Folder{Id: "top"},
			&accessv1.Folder{Id: "mid", Parent: folder("top")},
			&accessv1.Folder{Id: "low", Parent: folder("mid")},
			&accessv1.Folder{Id: "side"},
			&accessv1.FolderMember{Id: "fm-boss", Folder: folder("top"), UserId: "boss", Role: "folder_manager"},
			&accessv1.FolderMember{Id: "fm-boss-side", Folder: folder("side"), UserId: "boss", Role: "folder_manager"},
		)
	})

	status := func(r *syncplanev1.MutationResult) string {
		return r.GetStatus().String() + " " + r.GetErrorCode()
	}

	It("refuses a doc that would sit three hops below top, and keeps a doc two hops below it", func() {
		// When: boss creates a doc in mid and a doc in low.
		rs := f.push("boss", "c-boss",
			putMutation("c-boss", 1, "doc", "d-mid", &accessv1.Doc{Id: "d-mid", Folder: folder("mid")}),
			putMutation("c-boss", 2, "doc", "d-low", &accessv1.Doc{Id: "d-low", Folder: folder("low")}),
		)

		// Then: the doc in mid applies and boss reads it; the doc in low is
		// refused, since no reader below the root could read it.
		Expect(status(rs[0])).To(Equal("MUTATION_STATUS_APPLIED "))
		Expect(f.pull("boss")).To(ContainElement("doc/d-mid"))
		Expect(status(rs[1])).To(Equal("MUTATION_STATUS_REJECTED invalid_argument"))
	})

	It("refuses a move of a doc into a folder at the limit", func() {
		// Given: d-side sits in side, one hop below it.
		f.seed(&accessv1.Doc{Id: "d-side", Folder: folder("side")})

		// When: boss moves d-side into low.
		rs := f.push("boss", "c-boss",
			patchMutation("c-boss", 1, "doc", "d-side", &accessv1.Doc{Id: "d-side", Folder: folder("low")}, "folder"))

		// Then: the move is refused.
		Expect(status(rs[0])).To(Equal("MUTATION_STATUS_REJECTED invalid_argument"))
	})

	It("refuses a move of a folder that would put a doc below it past the limit", func() {
		// Given: d-in-side sits in side.
		f.seed(&accessv1.Doc{Id: "d-in-side", Folder: folder("side")})

		// When: boss moves side into mid, which puts d-in-side three hops
		// below top.
		rs := f.push("boss", "c-boss",
			patchMutation("c-boss", 1, "folder", "side", &accessv1.Folder{Id: "side", Parent: folder("mid")}, "parent"))

		// Then: the move is refused.
		Expect(status(rs[0])).To(Equal("MUTATION_STATUS_REJECTED invalid_argument"))
	})
})
