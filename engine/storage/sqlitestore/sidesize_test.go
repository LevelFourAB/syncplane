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

	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

// sideSizeRows is the number of rows the side-table fixture keeps a shadow
// and a delta log for. It is large enough that the b-trees are several levels
// deep and per-page overhead averages out.
const sideSizeRows = 20000

// sideSizeConsumer is the name the fixture records shadows under, shaped like
// the consumer of a linked engine.
const sideSizeConsumer = "syncplane.upstream"

// shadowObjects and deltaObjects name every table and b-tree that holds the
// consumer shadows and the delta log, or what finds them.
var (
	shadowObjects = []string{
		"consumer_shadows",
		"consumer_scopes",
		"sqlite_autoindex_consumer_scopes_1",
	}
	deltaObjects = []string{
		"field_deltas",
		"field_deltas_gc",
		"field_ids",
		"field_scopes",
		"sqlite_autoindex_field_scopes_1",
	}
)

// sideSizeReport is what the side-table fixture measures, read back from the
// closed database file.
type sideSizeReport struct {
	shadows, deltas         int64
	shadowBytes, deltaBytes int64
	objects                 map[string]int64
}

func (r sideSizeReport) shadowPerEntry() float64 {
	return float64(r.shadowBytes) / float64(r.shadows)
}

func (r sideSizeReport) deltaPerEntry() float64 {
	return float64(r.deltaBytes) / float64(r.deltas)
}

func (r sideSizeReport) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "shadows: %d B, %d entries, %.1f B/entry\n", r.shadowBytes, r.shadows, r.shadowPerEntry())
	for _, n := range shadowObjects {
		fmt.Fprintf(&b, "  %-36s %9d B\n", n, r.objects[n])
	}
	fmt.Fprintf(&b, "delta log: %d B, %d entries, %.1f B/entry\n", r.deltaBytes, r.deltas, r.deltaPerEntry())
	for _, n := range deltaObjects {
		fmt.Fprintf(&b, "  %-36s %9d B\n", n, r.objects[n])
	}
	return b.String()
}

// measureSideSize fills a fresh store the way a linked engine with text rows
// does, closes it, and measures the file.
//
// Each row has a random UUID key, a shadow of 12 bytes, and a delta log for
// one text field. The log gets one entry when the row is written and about
// two more over time, at random rows and in ascending sequence order, so the
// entries of one field are not next to each other in write order. One entry
// in fifty has a fragment; the rest are gap markers. Then every row has its
// shadow written again, in random order, as each change of a row does.
func measureSideSize() sideSizeReport {
	ctx := context.Background()
	path := filepath.Join(GinkgoT().TempDir(), "side.db")
	store, err := sqlitestore.Open(path)
	Expect(err).NotTo(HaveOccurred())

	rng := rand.New(rand.NewPCG(3, 5))
	keys := make([]string, sideSizeRows)
	entities := make([]string, sideSizeRows)
	for i := range keys {
		keys[i] = fmt.Sprintf("%08x-%04x-4%03x-8%03x-%012x",
			rng.Uint32(), rng.Uint32()&0xffff, rng.Uint32()&0xfff, rng.Uint32()&0xfff, rng.Uint64()&0xffffffffffff)
		entities[i] = "todos"
		if i%3 == 0 {
			entities[i] = "notes"
		}
	}
	snapshot := func(i int) []byte {
		return []byte(fmt.Sprintf("%012d", i))
	}
	frag := func() []byte {
		if rng.IntN(50) == 0 {
			return []byte("a fragment of twenty")
		}
		return nil
	}

	const batch = 500
	r := sideSizeReport{objects: map[string]int64{}}
	var seq uint64
	write := func(n int, fn func(tx storage.WriteTx, i int) error) {
		for start := 0; start < n; start += batch {
			Expect(store.WithWriteTx(ctx, indexSizeWS, func(tx storage.WriteTx) error {
				for i := start; i < min(start+batch, n); i++ {
					if err := fn(tx, i); err != nil {
						return err
					}
				}
				return nil
			})).To(Succeed())
		}
	}
	write(sideSizeRows, func(tx storage.WriteTx, i int) error {
		seq++
		r.shadows++
		r.deltas++
		if err := tx.PutFieldDelta(entities[i], keys[i], "content", seq, frag()); err != nil {
			return err
		}
		return tx.PutConsumerShadow(sideSizeConsumer, entities[i], keys[i], snapshot(i))
	})
	write(2*sideSizeRows, func(tx storage.WriteTx, _ int) error {
		seq++
		r.deltas++
		i := rng.IntN(sideSizeRows)
		return tx.PutFieldDelta(entities[i], keys[i], "content", seq, frag())
	})
	write(sideSizeRows, func(tx storage.WriteTx, n int) error {
		i := rng.IntN(sideSizeRows)
		return tx.PutConsumerShadow(sideSizeConsumer, entities[i], keys[i], snapshot(n))
	})
	Expect(store.Close()).To(Succeed())

	db, err := sql.Open("sqlite", path)
	Expect(err).NotTo(HaveOccurred())
	defer db.Close()
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
	for _, n := range shadowObjects {
		r.shadowBytes += r.objects[n]
	}
	for _, n := range deltaObjects {
		r.deltaBytes += r.objects[n]
	}
	return r
}

// shadowBytesCeiling and deltaBytesCeiling bound the on-disk cost of one
// consumer shadow and of one delta-log entry in the side-table fixture. They
// sit about 10% above the measured values, so a change that grows entries
// fails here. Lower them when a change makes entries smaller.
const (
	shadowBytesCeiling = 77
	deltaBytesCeiling  = 72
)

var _ = Describe("side table size", func() {
	It("keeps shadows and delta-log entries within the per-entry ceilings", func() {
		r := measureSideSize()
		AddReportEntry("side tables", r.String())
		Expect(r.shadowPerEntry()).To(BeNumerically("<=", shadowBytesCeiling), r.String())
		Expect(r.deltaPerEntry()).To(BeNumerically("<=", deltaBytesCeiling), r.String())
	})
})
