package server_test

import (
	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/text"
)

// A reader below the workspace root reads through the access model's filter.
// What every spec here checks: such a reader receives an edit as a fragment
// when it holds the page, and the whole page whenever it may not.
var _ = Describe("Text delta state for readers below the workspace root", func() {
	const opening = "a page long enough that sending all of it again for every edit would waste everyone's bytes"
	var (
		f      *accessFixture
		body   []byte
		nextID uint64
	)

	// seedOrg plus pages below its tasks: pg1 and pg3 below t1 in p1, which
	// guest reads, and pg2 below t2 in p2, which guest does not. editor reads
	// only the pages of p1 it owns, and owns none of them.
	BeforeEach(func() {
		f = newAccessFixture(server.WithEntities(&accessv1.Page{}))
		f.seedOrg()
		body = bodyDoc("seed", opening)
		nextID = 0
		f.seed(
			&accessv1.ProjectMember{Id: "pm-editor", Project: project("p1"), UserId: "editor", Role: "project_editor"},
			&accessv1.Page{Id: "pg1", Task: task("t1"), Body: body},
			&accessv1.Page{Id: "pg2", Task: task("t2"), Body: body},
			&accessv1.Page{Id: "pg3", Task: task("t1"), OwnerId: "someone", Body: body},
		)
	})

	pullFrom := func(user string, cursor []byte, limit uint32) *syncplanev1.PullResponse {
		GinkgoHelper()
		req := connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId: ws, ClientId: "c-" + user, Cursor: cursor, Limit: limit,
		})
		as(user, req.Header())
		resp, err := f.sync.Pull(f.ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Msg.GetReset_()).To(BeFalse())
		return resp.Msg
	}

	// pageIn returns the change a pull response carries for a page, nil when
	// it carries none.
	pageIn := func(resp *syncplanev1.PullResponse, key string) *syncplanev1.RowChange {
		for _, rc := range resp.GetPatch().GetChanges() {
			if rc.GetEntity() == "page" && rc.GetKey() == key {
				return rc
			}
		}
		return nil
	}

	mustPageIn := func(resp *syncplanev1.PullResponse, key string) *syncplanev1.RowChange {
		GinkgoHelper()
		rc := pageIn(resp, key)
		Expect(rc).NotTo(BeNil(), "no change for page "+key)
		return rc
	}

	// catchUp pulls as user from cursor, at most limit rows a page, until the
	// server has nothing more, and returns every change it received for the
	// page with key, in order.
	catchUp := func(user string, cursor []byte, limit uint32, key string) []*syncplanev1.RowChange {
		GinkgoHelper()
		var out []*syncplanev1.RowChange
		for range 50 {
			resp := pullFrom(user, cursor, limit)
			if rc := pageIn(resp, key); rc != nil {
				out = append(out, rc)
			}
			if !resp.GetHasMore() {
				return out
			}
			cursor = resp.GetNextCursor()
		}
		Fail("the pull did not catch up")
		return nil
	}

	// stored is the page as the server stores it, which every reader must end
	// up holding.
	stored := func(key string) []byte {
		GinkgoHelper()
		var value []byte
		Expect(f.store.WithReadTx(f.ctx, ws, func(tx storage.ReadTx) error {
			row, err := tx.GetRow("page", key)
			value = row.Value
			return err
		})).To(Succeed())
		return value
	}

	// reconstruct is the client half: it folds a change into the page the
	// reader holds, nil when it holds none, and returns the page it ends up
	// with.
	reconstruct := func(rc *syncplanev1.RowChange, held []byte) []byte {
		GinkgoHelper()
		if len(rc.GetTextDeltas()) == 0 {
			return rc.GetValue()
		}
		var next, prev accessv1.Page
		Expect(proto.Unmarshal(rc.GetValue(), &next)).To(Succeed())
		if held != nil {
			Expect(proto.Unmarshal(held, &prev)).To(Succeed())
		}
		for _, fd := range rc.GetTextDeltas() {
			Expect(fd.GetField()).To(Equal("body"))
			if len(fd.GetFragment()) == 0 {
				Expect(held).NotTo(BeNil(), "an unchanged document is one the reader must already hold")
				next.Body = prev.GetBody()
				continue
			}
			merged, err := text.ApplyFragmentBytes(prev.GetBody(), fd.GetFragment())
			Expect(err).NotTo(HaveOccurred())
			next.Body = merged
		}
		return mustMarshal(&next)
	}

	// pushAsAdmin sends mutations built from consecutive mutation ids as
	// admin, who edits every task, and expects them applied.
	pushAsAdmin := func(build ...func(id uint64) *syncplanev1.Mutation) {
		GinkgoHelper()
		muts := make([]*syncplanev1.Mutation, len(build))
		for i, b := range build {
			nextID++
			muts[i] = b(nextID)
		}
		for _, r := range f.push("admin", "c-admin", muts...) {
			Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED), r.GetErrorMessage())
		}
	}

	trusted := func(fn func(tx *server.EntityTx) error) {
		GinkgoHelper()
		Expect(f.srv.Mutate(server.Trusted(f.ctx), ws, fn)).To(Succeed())
	}

	edit := func(state []byte, to string) []byte {
		GinkgoHelper()
		return editBody(state, "admin", func(d *text.Doc) {
			Expect(d.SetString(to)).To(Succeed())
		})
	}

	It("ships an edit to a project member as a fragment", func() {
		first := pullFrom("guest", nil, 0)
		held := reconstruct(mustPageIn(first, "pg1"), nil)

		edited := edit(body, opening+", and then some")
		pushAsAdmin(func(id uint64) *syncplanev1.Mutation {
			return patchMutation("c-admin", id, "page", "pg1", &accessv1.Page{Body: edited}, "body")
		})

		rc := mustPageIn(pullFrom("guest", first.GetNextCursor(), 0), "pg1")
		Expect(rc.GetTextDeltas()).To(HaveLen(1))
		Expect(rc.GetTextDeltas()[0].GetFragment()).NotTo(BeEmpty())
		Expect(len(rc.GetValue())).To(BeNumerically("<", len(held)/2))
		Expect(reconstruct(rc, held)).To(Equal(stored("pg1")))
	})

	It("ships the whole page to every reader when its owner changes", func() {
		first := pullFrom("guest", nil, 0)
		held := reconstruct(mustPageIn(first, "pg1"), nil)

		pushAsAdmin(func(id uint64) *syncplanev1.Mutation {
			return patchMutation("c-admin", id, "page", "pg1", &accessv1.Page{OwnerId: "guest"}, "owner_id")
		})

		rc := mustPageIn(pullFrom("guest", first.GetNextCursor(), 0), "pg1")
		Expect(rc.GetTextDeltas()).To(BeEmpty(),
			"a read rule checks the owner, so the server cannot tell who held the page before")
		Expect(reconstruct(rc, held)).To(Equal(stored("pg1")))
	})

	DescribeTable("ships the whole page to a project member it moves in front of",
		func(move func(edited []byte)) {
			first := pullFrom("guest", nil, 0)
			Expect(pageIn(first, "pg2")).To(BeNil(), "guest cannot read p2")

			move(edit(body, opening+", moved"))

			rc := mustPageIn(pullFrom("guest", first.GetNextCursor(), 0), "pg2")
			Expect(rc.GetTextDeltas()).To(BeEmpty(),
				"the member never held the page, so there is nothing to fold a fragment into")
			Expect(rc.GetValue()).To(Equal(stored("pg2")))
		},
		Entry("by a PATCH of its task", func([]byte) {
			pushAsAdmin(func(id uint64) *syncplanev1.Mutation {
				return patchMutation("c-admin", id, "page", "pg2", &accessv1.Page{Task: task("t1")}, "task")
			})
		}),
		Entry("by a PATCH of its task and its body", func(edited []byte) {
			pushAsAdmin(func(id uint64) *syncplanev1.Mutation {
				return patchMutation("c-admin", id, "page", "pg2",
					&accessv1.Page{Task: task("t1"), Body: edited}, "task", "body")
			})
		}),
		Entry("by a PATCH of its task followed by an edit", func(edited []byte) {
			pushAsAdmin(func(id uint64) *syncplanev1.Mutation {
				return patchMutation("c-admin", id, "page", "pg2", &accessv1.Page{Task: task("t1")}, "task")
			}, func(id uint64) *syncplanev1.Mutation {
				return patchMutation("c-admin", id, "page", "pg2", &accessv1.Page{Body: edited}, "body")
			})
		}),
		Entry("by a PUT", func(edited []byte) {
			pushAsAdmin(func(id uint64) *syncplanev1.Mutation {
				return putMutation("c-admin", id, "page", "pg2", &accessv1.Page{Id: "pg2", Task: task("t1"), Body: edited})
			})
		}),
		Entry("by a trusted Patch", func([]byte) {
			trusted(func(tx *server.EntityTx) error {
				return tx.Patch(&accessv1.Page{Id: "pg2", Task: task("t1")}, []string{"task"}, nil)
			})
		}),
		Entry("by a trusted Put", func(edited []byte) {
			trusted(func(tx *server.EntityTx) error {
				return tx.Put(&accessv1.Page{Id: "pg2", Task: task("t1"), Body: edited})
			})
		}),
	)

	It("ships fragments again once the member holds the page that moved", func() {
		first := pullFrom("guest", nil, 0)
		trusted(func(tx *server.EntityTx) error {
			return tx.Patch(&accessv1.Page{Id: "pg2", Task: task("t1")}, []string{"task"}, nil)
		})
		second := pullFrom("guest", first.GetNextCursor(), 0)
		held := reconstruct(mustPageIn(second, "pg2"), nil)

		edited := edit(body, opening+", after the move")
		pushAsAdmin(func(id uint64) *syncplanev1.Mutation {
			return patchMutation("c-admin", id, "page", "pg2", &accessv1.Page{Body: edited}, "body")
		})

		rc := mustPageIn(pullFrom("guest", second.GetNextCursor(), 0), "pg2")
		Expect(rc.GetTextDeltas()).To(HaveLen(1))
		Expect(rc.GetTextDeltas()[0].GetFragment()).NotTo(BeEmpty())
		Expect(reconstruct(rc, held)).To(Equal(stored("pg2")))
	})

	It("ships the whole page to a reader who becomes its owner", func() {
		first := pullFrom("editor", nil, 0)
		Expect(pageIn(first, "pg3")).To(BeNil(), "editor reads only the pages it owns")

		trusted(func(tx *server.EntityTx) error {
			return tx.Patch(&accessv1.Page{Id: "pg3", OwnerId: "editor"}, []string{"owner_id"}, nil)
		})

		rc := mustPageIn(pullFrom("editor", first.GetNextCursor(), 0), "pg3")
		Expect(rc.GetTextDeltas()).To(BeEmpty())
		Expect(rc.GetValue()).To(Equal(stored("pg3")))
	})

	It("ships the whole page to a project member its task moves in front of", func() {
		first := pullFrom("guest", nil, 0)
		Expect(pageIn(first, "pg2")).To(BeNil())

		trusted(func(tx *server.EntityTx) error {
			return tx.Patch(&accessv1.Task{Id: "t2", Project: project("p1")}, []string{"project"}, nil)
		})
		edited := edit(body, opening+", on a task that moved")
		pushAsAdmin(func(id uint64) *syncplanev1.Mutation {
			return patchMutation("c-admin", id, "page", "pg2", &accessv1.Page{Body: edited}, "body")
		})

		changes := catchUp("guest", first.GetNextCursor(), 0, "pg2")
		Expect(changes).NotTo(BeEmpty())
		for _, rc := range changes {
			Expect(rc.GetTextDeltas()).To(BeEmpty())
		}
		Expect(changes[len(changes)-1].GetValue()).To(Equal(stored("pg2")))
	})

	It("ships the whole page on every page of the catch-up after a member gains its project", func() {
		first := pullFrom("guest", nil, 0)
		Expect(pageIn(first, "pg2")).To(BeNil())

		f.seed(&accessv1.ProjectMember{Id: "pm-guest2", Project: project("p2"), UserId: "guest", Role: "project_reader"})
		// A row guest cannot read, written before the edit, puts the page on
		// a later page of a catch-up served one row at a time.
		f.seed(&accessv1.Label{Id: "l2", Name: "filler"})
		edited := edit(body, opening+", in a project gained")
		pushAsAdmin(func(id uint64) *syncplanev1.Mutation {
			return patchMutation("c-admin", id, "page", "pg2", &accessv1.Page{Body: edited}, "body")
		})

		changes := catchUp("guest", first.GetNextCursor(), 1, "pg2")
		Expect(changes).NotTo(BeEmpty())
		for _, rc := range changes {
			Expect(rc.GetTextDeltas()).To(BeEmpty(),
				"the member never held the page, whichever page of the catch-up delivers it")
		}
		Expect(changes[len(changes)-1].GetValue()).To(Equal(stored("pg2")))
	})
})
