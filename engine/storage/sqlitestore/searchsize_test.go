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
// writes.
const searchSizeDocs = 2000

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

// measureSearchSize writes text-heavy issues through a server over a fresh
// store and measures the file. Word frequency follows a Zipf distribution, the
// shape of natural-language text, so a document holds a mix of common and
// rare terms.
func measureSearchSize() indexSizeReport {
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
	for batch := 0; batch < searchSizeDocs; batch += 200 {
		Expect(srv.Mutate(server.Trusted(ctx), indexSizeWS, func(tx *server.EntityTx) error {
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
			for i := batch; i < batch+200; i++ {
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
	return r
}

var _ = Describe("search index storage size", func() {
	It("reports the size of a text-heavy search index", func() {
		r := measureSearchSize()
		AddReportEntry("search index storage", r.String())
		fmt.Fprintf(GinkgoWriter, "%s", r.String())
		Expect(r.entries).To(BeNumerically(">", 0))
	})
})
