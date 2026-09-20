package pgstore_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/levelfourab/syncplane/engine/internal/pgtest"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/pgstore"
)

// What replacing one row's index entries costs against a real Postgres.
//
// It is the hottest statement of a push: the server rewrites the whole entry
// set of every row a mutation touched, once per mutation, and a PATCH of one
// field moves three entries out of thirty-one. The corpus is sized so the
// primary key has the depth a deployment's has — a few hundred thousand entries
// — because the cost being measured is btree descents, and a table that fits in
// two pages has none.
//
// The shape to expect (Apple M5 Pro, Postgres 18 in Docker, 200 rows per
// transaction):
//
//	batch     per call
//	12.5 ms     62 µs
//
// A regression looks like the per-call number roughly doubling, which is what
// it costs to touch the primary key once per entry the row already holds
// instead of once per entry that changed.
//
// Requires SYNCPLANE_TEST_PG_DSN; skipped without it. The benchmark seeds its
// own schema on first run and reuses it after, and leaves it behind — a run
// churns entries between two generations, so repeated runs bloat the table
// until it is vacuumed, and a comparison of two binaries should VACUUM between
// them or seed a fresh database.
func BenchmarkPutIndexEntries(b *testing.B) {
	dsn := os.Getenv(pgtest.DSNEnv)
	if dsn == "" {
		b.Skipf("set %s to a Postgres this benchmark may create a schema in", pgtest.DSNEnv)
	}
	ctx := context.Background()

	store, err := pgstore.Open(ctx, benchIndexDSN(b, dsn))
	if err != nil {
		b.Fatalf("open store: %v", err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Fatalf("close store: %v", err)
		}
	})
	seedIndexBench(b, ctx, store)

	// Rows within one transaction are scattered rather than consecutive, which
	// is how a push's mutations reach the index: consecutive keys would share
	// leaf pages and measure a locality no real batch has.
	const stride = 1009
	// Generations must not repeat what an earlier run of this benchmark already
	// wrote: a call whose entries are already stored changes nothing, and a run
	// that replayed them would measure the no-op instead of the replacement.
	gen := int(time.Now().UnixNano() % (1 << 32))
	iter := 0
	b.ResetTimer()
	for b.Loop() {
		gen++
		iter++
		base := iter * benchIndexBatch
		err := store.WithWriteTx(ctx, benchIndexWorkspace, func(tx storage.WriteTx) error {
			for j := range benchIndexBatch {
				row := (base + j*stride) % benchIndexRows
				if err := tx.PutIndexEntries(benchIndexEntity, benchIndexKey(row),
					benchIndexEntries(row, gen)); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			b.Fatalf("write batch: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*benchIndexBatch), "ns/call")
}

const (
	// Rows times entries is the corpus: ~217k index entries, enough for a
	// primary key three levels deep.
	benchIndexRows      = 7000
	benchIndexSearch    = 22
	benchIndexBatch     = 200
	benchIndexSchema    = "pgstore_index_bench"
	benchIndexWorkspace = "bench"
	benchIndexEntity    = "issue"
)

func benchIndexKey(row int) string { return fmt.Sprintf("issue-%08d", row) }

// benchIndexEntries builds the entry set one row occupies, in the proportions
// an indexed entity really has: a handful of reference and field indexes, a
// search token per distinct word of the text, and three ordered indexes.
//
// gen appears in the ordered entries and nowhere else, so writing a row at a
// new generation moves exactly three of its thirty-one entries — the shape of a
// PATCH that touched one ordering field.
func benchIndexEntries(row, gen int) []storage.IndexEntry {
	key := benchIndexKey(row)
	entries := make([]storage.IndexEntry, 0, 6+benchIndexSearch+3)
	entry := func(index string, value string) {
		k := append([]byte{0x01}, value...)
		k = append(k, 0x00, 0x00)
		k = append(k, key...)
		entries = append(entries, storage.IndexEntry{Index: index, Key: append(k, 0x00, 0x00)})
	}

	entry("__ref_project", fmt.Sprintf("project-%08d", row%64))
	entry("__ref_assignee", fmt.Sprintf("user-%08d", row%512))
	entry("__ref_reporter", fmt.Sprintf("user-%08d", (row*7)%512))
	entry("__ref_blocked_by", "")
	entry("__field_state", []string{"open", "done", "blocked"}[row%3])
	entry("__field_labels", "")
	for i := range benchIndexSearch {
		entry("__search", fmt.Sprintf("w%02d%04d", i, row%1000))
	}
	// The three an ordering field moves.
	entry("by_project_updated", fmt.Sprintf("project-%08d/%012d", row%64, gen))
	entry("by_label_updated", fmt.Sprintf("%012d", gen))
	entry("by_state_priority", fmt.Sprintf("%s/%012d", []string{"open", "done", "blocked"}[row%3], gen))
	return entries
}

// benchIndexDSN points dsn at the benchmark's own schema, creating it if this
// is the first run. The schema is NOT dropped: seeding it is the expensive part
// and a rerun that reuses it starts measuring immediately.
func benchIndexDSN(b *testing.B, dsn string) string {
	b.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		b.Skipf("no Postgres at %s: %v", pgtest.DSNEnv, err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS "`+benchIndexSchema+`"`); err != nil {
		b.Fatalf("create schema: %v", err)
	}

	u, err := url.Parse(dsn)
	if err != nil {
		b.Fatalf("%s must be a URI (postgres://...): %v", pgtest.DSNEnv, err)
	}
	q := u.Query()
	q.Set("options", "-c search_path="+benchIndexSchema)
	u.RawQuery = q.Encode()
	return u.String()
}

// seedIndexBench fills the corpus, and does nothing when a previous run already
// did. Entries are written through the adapter rather than COPYed so the corpus
// is exactly what the server would have produced.
func seedIndexBench(b *testing.B, ctx context.Context, store *pgstore.Store) {
	b.Helper()
	want := benchIndexRows
	var have int
	err := store.WithReadTx(ctx, benchIndexWorkspace, func(tx storage.ReadTx) error {
		return tx.ScanEntity(benchIndexEntity, func(storage.Row) error {
			have++
			return nil
		})
	})
	if err != nil {
		b.Fatalf("count seeded rows: %v", err)
	}
	if have >= want {
		return
	}

	const perTx = 500
	for start := have; start < want; start += perTx {
		end := min(start+perTx, want)
		err := store.WithWriteTx(ctx, benchIndexWorkspace, func(tx storage.WriteTx) error {
			for row := start; row < end; row++ {
				// A row backs the entries: index entries exist only for live
				// rows, and the count above is what makes the seed resumable.
				if err := tx.PutRow(benchIndexEntity, benchIndexKey(row),
					[]byte(benchIndexKey(row)), uint64(row+1), storage.Actor{}); err != nil {
					return err
				}
				if err := tx.PutIndexEntries(benchIndexEntity, benchIndexKey(row),
					benchIndexEntries(row, 0)); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			b.Fatalf("seed rows %d..%d: %v", start, end, err)
		}
	}
}
