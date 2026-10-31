package sqlitestore_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	benchv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/bench/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

// indexSizeIssues is the number of issue rows the size fixture writes. It is
// large enough that per-page overhead averages out, and small enough to keep
// the spec under a second.
const indexSizeIssues = 2000

// indexSizeWS is shaped like a UUID, the common workspace id, because the
// workspace id is stored per entry and its length is part of what is measured.
const indexSizeWS = "0b7c6a2e-4f1d-4e8a-9c3b-5d2e1f0a9b8c"

// sizeKey makes a UUID-shaped row key, for the same reason as indexSizeWS.
func sizeKey(kind, i int) string {
	return fmt.Sprintf("%08x-0000-4000-8000-%012x", kind, i)
}

// indexObjects names every table and b-tree that holds index entries or what
// finds them: the entries, the per-row entry lists and the scope ids.
var indexObjects = []string{
	"index_entries",
	"index_row_entries",
	"index_scopes",
	"sqlite_autoindex_index_scopes_1",
}

// emptyObjectBytes is the size of a table or b-tree that holds no data: one
// page at SQLite's default page size.
const emptyObjectBytes = 4096

// indexSizeReport is what the size fixture measures, read back from the
// closed database file.
type indexSizeReport struct {
	// rowsBytes is the on-disk size of the rows table, the data the indexes
	// are derived from.
	rowsBytes int64
	// indexBytes is the on-disk size of every table and b-tree that holds
	// index entries or the bookkeeping that finds them.
	indexBytes int64
	// entries is the total number of index entries.
	entries int64
	// perIndex is the entry count of each index.
	perIndex map[string]int64
	// missingLead is, per index, the entries whose leading component is
	// missing: the entries a sparse index leaves out.
	missingLead map[string]int64
	// objects is the on-disk size of every table and b-tree in the file.
	objects map[string]int64
}

func (r indexSizeReport) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "rows: %d B, index storage: %d B (%.1fx rows), %d entries, %.1f B/entry\n",
		r.rowsBytes, r.indexBytes, float64(r.indexBytes)/float64(r.rowsBytes),
		r.entries, float64(r.indexBytes)/float64(r.entries))
	names := make([]string, 0, len(r.perIndex))
	for n := range r.perIndex {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		fmt.Fprintf(&b, "  %-22s %6d entries, %6d with the leading component missing\n",
			n, r.perIndex[n], r.missingLead[n])
	}
	objs := make([]string, 0, len(r.objects))
	for n, size := range r.objects {
		// One page is an empty table; listing those hides the ones that hold
		// data.
		if size > emptyObjectBytes {
			objs = append(objs, n)
		}
	}
	sort.Strings(objs)
	for _, n := range objs {
		fmt.Fprintf(&b, "  %-32s %9d B\n", n, r.objects[n])
	}
	return b.String()
}

// measureIndexSize writes the fixture through a server over a fresh store,
// closes both, and measures the file.
//
// The rows are shaped like a graph where most references are unset: every
// issue has a project and a reporter, half have an assignee, one in forty is
// blocked by other issues, and one in three has labels.
func measureIndexSize() indexSizeReport {
	ctx := context.Background()
	path := filepath.Join(GinkgoT().TempDir(), "size.db")
	store, err := sqlitestore.Open(path)
	Expect(err).NotTo(HaveOccurred())
	srv, err := server.New(store,
		server.WithEntities(&benchv1.Project{}, &benchv1.User{}, &benchv1.Issue{}))
	Expect(err).NotTo(HaveOccurred())

	const projects, users = 20, 50
	Expect(srv.Mutate(server.Trusted(ctx), indexSizeWS, func(tx *server.EntityTx) error {
		for i := 0; i < projects; i++ {
			if err := tx.Put(&benchv1.Project{Id: sizeKey(1, i), Name: "project", Status: "active"}); err != nil {
				return err
			}
		}
		for i := 0; i < users; i++ {
			if err := tx.Put(&benchv1.User{Id: sizeKey(2, i), Name: "user"}); err != nil {
				return err
			}
		}
		for i := 0; i < indexSizeIssues; i++ {
			issue := &benchv1.Issue{
				Id:          sizeKey(3, i),
				Title:       "issue",
				Body:        "a short body",
				Project:     &benchv1.Project{Id: sizeKey(1, i%projects)},
				Reporter:    &benchv1.User{Id: sizeKey(2, i%users)},
				State:       "open",
				Priority:    int32(i % 4),
				UpdatedAtMs: int64(i),
			}
			if i%2 == 0 {
				issue.Assignee = &benchv1.User{Id: sizeKey(2, (i/2)%users)}
			}
			if i%40 == 0 && i > 0 {
				issue.BlockedBy = []*benchv1.Issue{{Id: sizeKey(3, i-1)}}
			}
			if i%3 == 0 {
				issue.Labels = []string{"bug", "ui"}
			}
			if err := tx.Put(issue); err != nil {
				return err
			}
		}
		return nil
	})).To(Succeed())
	Expect(srv.Close()).To(Succeed())
	Expect(store.Close()).To(Succeed())

	db, err := sql.Open("sqlite", path)
	Expect(err).NotTo(HaveOccurred())
	defer db.Close()

	r := indexSizeReport{
		perIndex:    map[string]int64{},
		missingLead: map[string]int64{},
		objects:     map[string]int64{},
	}
	rows, err := db.Query(`SELECT name, SUM(pgsize) FROM dbstat GROUP BY name`)
	Expect(err).NotTo(HaveOccurred())
	for rows.Next() {
		var (
			name string
			size int64
		)
		Expect(rows.Scan(&name, &size)).To(Succeed())
		r.objects[name] = size
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	r.rowsBytes = r.objects["rows"]
	for _, name := range indexObjects {
		r.indexBytes += r.objects[name]
	}

	rows, err = db.Query(`
		SELECT s.idx, COUNT(*), SUM(substr(e.k, 1, 1) = x'ff')
		  FROM index_entries e JOIN index_scopes s ON s.id = e.scope
		 GROUP BY s.idx`)
	Expect(err).NotTo(HaveOccurred())
	for rows.Next() {
		var (
			idx            string
			count, missing int64
		)
		Expect(rows.Scan(&idx, &count, &missing)).To(Succeed())
		r.perIndex[idx] = count
		r.missingLead[idx] = missing
		r.entries += count
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	return r
}

// bytesPerEntryCeiling bounds the on-disk cost of one index entry in the size
// fixture. It sits about 10% above the measured value, so a change that grows
// entries fails here. Lower it when a change makes entries smaller.
const bytesPerEntryCeiling = 120

var _ = Describe("index storage size", func() {
	It("keeps index entries within the per-entry ceiling", func() {
		r := measureIndexSize()
		AddReportEntry("index storage", r.String())
		Expect(r.entries).To(BeNumerically(">", 0))
		Expect(float64(r.indexBytes)/float64(r.entries)).To(BeNumerically("<=", bytesPerEntryCeiling), r.String())
	})

	It("stores no reference entry for a row whose reference is unset", func() {
		r := measureIndexSize()
		for idx, missing := range r.missingLead {
			if strings.HasPrefix(idx, "__ref_") {
				Expect(missing).To(BeZero(), "index %s", idx)
			}
		}
		Expect(r.perIndex["__ref_blocked_by"]).To(BeNumerically("==", indexSizeIssues/40-1),
			"one entry per blocked issue, none for the rest")
	})
})
