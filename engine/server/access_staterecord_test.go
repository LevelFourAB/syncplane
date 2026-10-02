package server_test

import (
	"database/sql"
	"fmt"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/cursorstate"
	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

var _ = Describe("Access model record of a cursor that keeps pulling", func() {
	It("resyncs a client that pulled every day for longer than the cursor-state retention when its access changes", func() {
		// Given a server on SQLite, whose cursor-state store keeps entries
		// for 30 days from when they were written
		path := filepath.Join(GinkgoT().TempDir(), "test.db")
		store, err := sqlitestore.Open(path)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		f := newAccessFixtureOn(store)
		f.seedOrg()
		Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
		db, err := sql.Open("sqlite", path)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(db.Close)

		// And guest, whose access stays the same, pulls once a day for 31
		// days: each pull reissues the record it was given on day 0
		guest := newAccessReplica(f, "guest")
		guest.sync()
		for range 31 {
			_, err := db.Exec(`UPDATE cursor_state SET written_at = written_at - 86400`)
			Expect(err).NotTo(HaveOccurred())
			guest.sync()
		}
		// And another caller's access changes, which writes an entry and
		// prunes what the store counts as aged
		writer := newAccessReplica(f, "writer")
		writer.sync()
		f.seed(&accessv1.ProjectMember{Id: "pm-writer1", Project: project("p1"), UserId: "writer", Role: "project_reader"})
		writer.sync()

		// When guest gains a grant
		f.seed(&accessv1.ProjectMember{Id: "pm-guest2", Project: project("p2"), UserId: "guest", Role: "project_reader"})
		resp := guest.pullOnce(0)

		// Then its next pull resyncs the rows below p2, as it does for a
		// client that changed access a day ago, and does not reset it
		Expect(resp.GetReset_()).To(BeFalse(), "a client that never went away was reset")
		guest.sync()
		Expect(guest.view()).To(ContainElement("task/t2"))
	})

	It("resyncs a client whose access record other clients' entries would push out of a bounded store", func() {
		// Given a server whose cursor-state store holds a few kilobytes and
		// evicts the least recently used entry
		f := newAccessFixture(server.WithCursorStateStore(cursorstate.NewMemory(4 << 10)))
		f.seedOrg()
		Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
		guest := newAccessReplica(f, "guest")
		guest.sync()

		// And guest keeps pulling with the same access, while the access of
		// other callers changes and they pull
		for i := range 40 {
			user := fmt.Sprintf("u%d", i)
			f.seed(&accessv1.ProjectMember{Id: "pm-" + user, Project: project("p1"), UserId: user, Role: "project_reader"})
			other := newAccessReplica(f, user)
			other.sync()
			f.seed(&accessv1.ProjectMember{Id: "pm-" + user + "-2", Project: project("p2"), UserId: user, Role: "project_reader"})
			other.sync()
			guest.sync()
		}

		// When guest gains a grant
		f.seed(&accessv1.ProjectMember{Id: "pm-guest2", Project: project("p2"), UserId: "guest", Role: "project_reader"})
		resp := guest.pullOnce(0)

		// Then it resyncs and is not reset: it used its record on every pull
		Expect(resp.GetReset_()).To(BeFalse(), "an active client's record was evicted as unused")
	})
})
