package sqlitestore_test

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	benchv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/bench/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

// searchSizeDocs is the number of text-heavy issues the search size fixture
// writes into one workspace.
const searchSizeDocs = 2000

// The fixture of many small workspaces writes searchSizeSmallDocs issues,
// spread over searchSizeSmallWorkspaces workspaces. In a small workspace most
// terms are in one or two issues.
const (
	searchSizeSmallDocs       = 1000
	searchSizeSmallWorkspaces = 20
)

// searchSizeVocab is the number of distinct words the fixture draws from.
const searchSizeVocab = 20000

// searchSizeWords is the length of one document body, in words.
const searchSizeWords = 300

// searchVocab makes n pronounceable words of two to five syllables, the same
// for every run.
func searchVocab(rng *rand.Rand, n int) []string {
	syl := []string{"ka", "lo", "mi", "ne", "ru", "sa", "ti", "vo", "ze", "pra",
		"stel", "dor", "fin", "gal", "hus", "jen", "kor", "lin", "mor", "nes",
		"pol", "qui", "ras", "sor", "tan", "ul", "ver", "wen", "xi", "yor"}
	seen := map[string]struct{}{}
	out := make([]string, 0, n)
	for len(out) < n {
		var b strings.Builder
		for s := 2 + rng.IntN(4); s > 0; s-- {
			b.WriteString(syl[rng.IntN(len(syl))])
		}
		w := b.String()
		if _, dup := seen[w]; dup {
			continue
		}
		seen[w] = struct{}{}
		out = append(out, w)
	}
	return out
}

// searchSizeWS makes the id of workspace i, shaped like indexSizeWS.
func searchSizeWS(i int) string {
	return fmt.Sprintf("%08x%s", i, indexSizeWS[8:])
}

// measureSearchSize writes docs text-heavy issues through a server over a
// fresh store, issue i into workspace i modulo workspaces, and measures the
// file. Word frequency follows a Zipf distribution, the shape of
// natural-language text, so a document holds a mix of common and rare terms.
func measureSearchSize(docs, workspaces int) indexSizeReport {
	ctx := context.Background()
	path := filepath.Join(GinkgoT().TempDir(), "search.db")
	store, err := sqlitestore.Open(path)
	Expect(err).NotTo(HaveOccurred())
	srv, err := server.New(store,
		server.WithEntities(&benchv1.Project{}, &benchv1.User{}, &benchv1.Issue{}))
	Expect(err).NotTo(HaveOccurred())

	rng := rand.New(rand.NewPCG(7, 11))
	vocab := searchVocab(rng, searchSizeVocab)
	zipf := rand.NewZipf(rng, 1.07, 2, searchSizeVocab-1)
	text := func(n int) string {
		var b strings.Builder
		for i := 0; i < n; i++ {
			if i > 0 {
				b.WriteByte(' ')
			}
			b.WriteString(vocab[zipf.Uint64()])
		}
		return b.String()
	}

	const projects, users = 20, 50
	// Each workspace gets its issues in transactions of up to 200.
	for ws := 0; ws < workspaces; ws++ {
		var mine []int
		for i := ws; i < docs; i += workspaces {
			mine = append(mine, i)
		}
		for batch := 0; batch < len(mine); batch += 200 {
			Expect(srv.Mutate(server.Trusted(ctx), searchSizeWS(ws), func(tx *server.EntityTx) error {
				if batch == 0 {
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
				}
				for _, i := range mine[batch:min(batch+200, len(mine))] {
					issue := &benchv1.Issue{
						Id:          sizeKey(3, i),
						Title:       text(8),
						Body:        text(searchSizeWords),
						Project:     &benchv1.Project{Id: sizeKey(1, i%projects)},
						Reporter:    &benchv1.User{Id: sizeKey(2, i%users)},
						State:       "open",
						Priority:    int32(i % 4),
						UpdatedAtMs: int64(i),
					}
					if err := tx.Put(issue); err != nil {
						return err
					}
				}
				return nil
			})).To(Succeed())
		}
	}
	Expect(srv.Close()).To(Succeed())
	Expect(store.Close()).To(Succeed())
	return readIndexSize(path)
}

// readIndexSize measures the closed database file at path.
func readIndexSize(path string) indexSizeReport {
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
	var count int64
	Expect(db.QueryRow(`SELECT COUNT(*) FROM index_entries`).Scan(&count)).To(Succeed())
	r.entries = count
	countPostings(path, db, &r)
	return r
}

// postingBytes is the on-disk size of every table and b-tree that holds
// postings or what finds them.
func (r indexSizeReport) postingBytes() int64 {
	var n int64
	for name, size := range r.objects {
		if strings.Contains(name, "posting_") {
			n += size
		}
	}
	return n
}

// The ceilings bound the on-disk cost of one search posting in the two search
// size fixtures. Each sits about 10% above the measured value, so a change
// that grows the posting records fails here. Lower one when a change makes
// its postings smaller.
const (
	bytesPerPostingCeiling      = 3.0
	bytesPerPostingSmallCeiling = 7.4
)

var _ = Describe("search index storage size", func() {
	It("keeps the postings of a text-heavy workspace within the per-posting ceiling", func() {
		r := measureSearchSize(searchSizeDocs, 1)
		perPosting := float64(r.postingBytes()) / float64(r.postings)
		report := fmt.Sprintf("%.2f B/posting\n%s", perPosting, r.String())
		AddReportEntry("search index storage", report)
		fmt.Fprintf(GinkgoWriter, "%s", report)
		Expect(r.postings).To(BeNumerically(">", 0))
		Expect(perPosting).To(BeNumerically("<=", bytesPerPostingCeiling), report)
	})

	It("keeps the postings of many small workspaces within the per-posting ceiling", func() {
		r := measureSearchSize(searchSizeSmallDocs, searchSizeSmallWorkspaces)
		perPosting := float64(r.postingBytes()) / float64(r.postings)
		report := fmt.Sprintf("%.2f B/posting\n%s", perPosting, r.String())
		AddReportEntry("search index storage, small workspaces", report)
		fmt.Fprintf(GinkgoWriter, "%s", report)
		Expect(r.postings).To(BeNumerically(">", 0))
		Expect(perPosting).To(BeNumerically("<=", bytesPerPostingSmallCeiling), report)
	})
})
