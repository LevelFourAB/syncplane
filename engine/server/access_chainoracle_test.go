package server_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// A write that names a parent the caller cannot write at must not tell the
// caller where that parent sits.
var _ = Describe("Access model refusals of a write below a hidden parent", func() {
	var (
		f       *accessFixture
		clients int
	)

	// Given, with MaxAccessDepth 2, a hidden tree s1 ─ s2 ─ s3, and a folder
	// a that editor edits. nope does not exist.
	BeforeEach(func() {
		clients = 0
		f = newFolderFixtureOn(newTestStore(), server.WithLimits(server.Limits{MaxAccessDepth: 2}))
		f.seed(
			&accessv1.Folder{Id: "s1"},
			&accessv1.Folder{Id: "s2", Parent: folder("s1")},
			&accessv1.Folder{Id: "s3", Parent: folder("s2")},
			&accessv1.Folder{Id: "a"},
			&accessv1.FolderMember{Id: "fm-editor", Folder: folder("a"), UserId: "editor", Role: "folder_editor"},
		)
	})

	moveA := func(parent string) string {
		GinkgoHelper()
		clients++
		client := "c-oracle-" + string(rune('a'+clients))
		results := f.push("editor", client,
			patchMutation(client, 1, "folder", "a", &accessv1.Folder{Id: "a", Parent: folder(parent)}, "parent"))
		Expect(results).To(HaveLen(1))
		return results[0].GetStatus().String() + " " + results[0].GetErrorCode()
	}

	It("answers a move below a hidden parent too deep for the limit as a move below a missing parent", func() {
		// When: editor moves a below s3, where a would sit three hops below
		// s1, and below nope.
		deep := moveA("s3")
		missing := moveA("nope")

		// Then: the two answers are the same.
		Expect(deep).To(Equal(missing))
		Expect(missing).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED.String() + " " + server.CodeAccessDenied))
	})
})
