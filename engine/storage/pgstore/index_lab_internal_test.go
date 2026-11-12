package pgstore

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/levelfourab/syncplane/engine/internal/pgtest"
	"github.com/levelfourab/syncplane/engine/storage"
)

// TestIndexLab measures value index entries through the adapter against a real
// Postgres: the size of the index tables, the time of a bulk load, and the
// server time of a transaction that replaces 3 of the 31 entries of 1, 10 and
// 100 rows, inserts rows, deletes rows, and reads a range, and the wall time of
// transactions of many rows, of one row of many entries and of many calls for
// one row. The corpus is the one of BenchmarkPutIndexEntries, in one
// workspace.
//
//	PGSTORE_INDEX_LAB=20000 SYNCPLANE_TEST_PG_DSN=... go test ./engine/storage/pgstore -run TestIndexLab -v -count=1 -timeout 0
//
// The value is the number of rows. PGSTORE_INDEX_PARTS lists the WithPartitions
// moduli to compare (default "0,32"), PGSTORE_INDEX_ROUNDS scales the number
// of transactions (default 4) and PGSTORE_INDEX_OUT names a file that gets
// one JSON line of the medians per layout. PGSTORE_INDEX_LARGE lists sizes
// for the transaction of many calls for one row, such as "5000,20000", and
// makes the test measure the large transactions in place of the others.
// PGSTORE_INDEX_NO_ANALYZE loads the corpus with the statistics of empty
// tables, as a load does that is faster than autovacuum.
//
// The time of a write transaction is the time on the server clock from after
// its first read, which the test makes, to after COMMIT, which is one batch:
// it holds no network time. The time of a read is the plan and execution time
// of its statements from pg_stat_statements, so the server must load that
// extension, and nothing else may use the database while the test runs. Every
// iteration runs each layout, so that drift of the machine hits them alike.
// The server should run with autovacuum off; the test runs VACUUM between the
// phases.
func TestIndexLab(t *testing.T) {
	rows, _ := strconv.Atoi(os.Getenv("PGSTORE_INDEX_LAB"))
	dsn := os.Getenv(pgtest.DSNEnv)
	if rows <= 0 || dsn == "" {
		t.Skipf("set PGSTORE_INDEX_LAB and %s", pgtest.DSNEnv)
	}
	ctx := context.Background()
	parts := []int{0, 32}
	if v := os.Getenv("PGSTORE_INDEX_PARTS"); v != "" {
		parts = nil
		for _, p := range strings.Split(v, ",") {
			n, err := strconv.Atoi(p)
			if err != nil {
				t.Fatal(err)
			}
			parts = append(parts, n)
		}
	}
	rounds := 4
	if v, _ := strconv.Atoi(os.Getenv("PGSTORE_INDEX_ROUNDS")); v > 0 {
		rounds = v
	}
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)

	var labs []*indexLab
	for _, p := range parts {
		l := &indexLab{parts: p, schema: fmt.Sprintf("pgstore_index_lab_p%d", p), rows: rows, next: rows,
			gen: make([]int, rows), samples: map[string][]time.Duration{}}
		if _, err := admin.Exec(ctx, `DROP SCHEMA IF EXISTS "`+l.schema+`" CASCADE; CREATE SCHEMA "`+l.schema+`"`); err != nil {
			t.Fatal(err)
		}
		u, err := url.Parse(dsn)
		if err != nil {
			t.Fatal(err)
		}
		q := u.Query()
		q.Set("options", "-c search_path="+l.schema)
		u.RawQuery = q.Encode()
		var opts []Option
		if p > 0 {
			opts = append(opts, WithPartitions(p))
		}
		// One connection, so that every transaction finds its statements
		// prepared.
		opts = append(opts, WithMaxConns(1))
		if l.store, err = Open(ctx, u.String(), opts...); err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = l.store.Close()
			_, _ = admin.Exec(ctx, `DROP SCHEMA IF EXISTS "`+l.schema+`" CASCADE`)
		}()
		start := time.Now()
		for at := 0; at < rows; at += 500 {
			err := l.store.WithWriteTx(ctx, indexLabWS, func(tx storage.WriteTx) error {
				for row := at; row < min(at+500, rows); row++ {
					if err := tx.PutIndexEntries(indexLabEntity, indexLabKey(row), indexLabEntries(row, 0)); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			// Autovacuum is off, so the test gives the planner the statistics
			// autovacuum would: after the first transactions, then now and
			// then.
			if n := at / 500; (n == 0 || n == 2 || n%10 == 9) && os.Getenv("PGSTORE_INDEX_NO_ANALYZE") == "" {
				l.analyze(t, ctx, admin)
			}
		}
		l.load = time.Since(start)
		l.vacuum(t, ctx, admin)
		l.size = l.tableBytes(t, ctx, admin)
		labs = append(labs, l)
	}
	vacuumAll := func() {
		for _, l := range labs {
			l.vacuum(t, ctx, admin)
		}
	}
	// each runs one transaction of every layout per iteration.
	each := func(name string, iterations int, fn func(l *indexLab, it int) time.Duration) {
		for it := -6; it < iterations; it++ {
			for i := range labs {
				l := labs[(i+max(it, 0))%len(labs)]
				d := fn(l, it)
				// The first iterations fill the statement and plan caches.
				if it >= 0 {
					l.samples[name] = append(l.samples[name], d)
				}
			}
		}
	}

	if _, err := admin.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS pg_stat_statements`); err != nil {
		t.Fatal(err)
	}
	// calls holds the sizes of the transactions of many calls for one row;
	// with it set, the test measures the large transactions and nothing else.
	var calls []int
	for _, v := range strings.Split(os.Getenv("PGSTORE_INDEX_LARGE"), ",") {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			calls = append(calls, n)
		}
	}
	if len(calls) > 0 {
		rounds = 0
	}
	each("empty transaction", 25*rounds, func(l *indexLab, _ int) time.Duration {
		return l.write(t, ctx, func(storage.WriteTx) error { return nil })
	})
	for _, n := range []int{1, 10, 100} {
		name := fmt.Sprintf("replace 3 of 31, %d rows/tx", n)
		per := map[int]int{1: 60, 10: 30, 100: 8}[n]
		for r := 0; r < rounds; r++ {
			vacuumAll()
			each(name, per, func(l *indexLab, _ int) time.Duration {
				picked := l.pick(n)
				return l.write(t, ctx, func(tx storage.WriteTx) error {
					for _, row := range picked {
						l.gen[row]++
						if err := tx.PutIndexEntries(indexLabEntity, indexLabKey(row), indexLabEntries(row, l.gen[row])); err != nil {
							return err
						}
					}
					return nil
				})
			})
		}
	}
	for _, n := range []int{1, 100} {
		ins, del := fmt.Sprintf("insert, %d rows/tx", n), fmt.Sprintf("delete, %d rows/tx", n)
		per := map[int]int{1: 30, 100: 3}[n]
		for r := 0; r < rounds; r++ {
			vacuumAll()
			made := map[*indexLab][][]int{}
			each(ins, per, func(l *indexLab, _ int) time.Duration {
				var fresh []int
				for range n {
					fresh = append(fresh, l.next)
					l.next++
				}
				made[l] = append(made[l], fresh)
				return l.write(t, ctx, func(tx storage.WriteTx) error {
					for _, row := range fresh {
						if err := tx.PutIndexEntries(indexLabEntity, indexLabKey(row), indexLabEntries(row, 0)); err != nil {
							return err
						}
					}
					return nil
				})
			})
			each(del, per, func(l *indexLab, _ int) time.Duration {
				gone := made[l][0]
				made[l] = made[l][1:]
				return l.write(t, ctx, func(tx storage.WriteTx) error {
					for _, row := range gone {
						if err := tx.DeleteIndexEntries(indexLabEntity, indexLabKey(row)); err != nil {
							return err
						}
					}
					return nil
				})
			})
		}
	}
	vacuumAll()
	for _, probe := range []struct {
		name  string
		limit int
		order storage.ScanOrder
	}{
		{"read first 65 ascending", 65, storage.ScanAsc},
		{"read first 65 descending", 65, storage.ScanDesc},
		{"read a range of about 313", 0, storage.ScanAsc},
	} {
		at := 0
		each(probe.name, 15*rounds, func(l *indexLab, _ int) time.Duration {
			return l.reads(t, ctx, admin, 8, func(tx storage.ReadTx) error {
				at++
				lo := []byte(fmt.Sprintf("\x01project-%08d/", (at*3)%64))
				hi := append([]byte(nil), lo...)
				hi[len(hi)-1]++
				// The engine reads the state of an index before it scans it.
				if _, _, err := tx.IndexState(indexLabEntity, "by_project_updated"); err != nil {
					return err
				}
				return tx.ScanIndex(indexLabEntity, "by_project_updated", lo, hi, probe.limit, probe.order,
					func([]byte, string) error { return nil })
			})
		})
	}

	// Large transactions, by wall time: the time per row, entry or call must
	// not grow with the size of the transaction.
	large := map[*indexLab][]string{}
	for _, l := range labs {
		if len(calls) == 0 {
			break
		}
		wall := func(fn func(tx storage.WriteTx) error) time.Duration {
			start := time.Now()
			if err := l.store.WithWriteTx(ctx, indexLabWS, fn); err != nil {
				t.Fatal(err)
			}
			return time.Since(start)
		}
		note := func(format string, args ...any) { large[l] = append(large[l], fmt.Sprintf(format, args...)) }
		for _, n := range []int{125, 500, 2000} {
			first := l.next
			l.next += n
			d := wall(func(tx storage.WriteTx) error {
				for row := first; row < first+n; row++ {
					if err := tx.PutIndexEntries(indexLabEntity, indexLabKey(row), indexLabEntries(row, 0)); err != nil {
						return err
					}
				}
				return nil
			})
			r := wall(func(tx storage.WriteTx) error {
				for row := first; row < first+n; row++ {
					if err := tx.PutIndexEntries(indexLabEntity, indexLabKey(row), indexLabEntries(row, 1)); err != nil {
						return err
					}
				}
				return nil
			})
			note("%5d rows x 31 entries in one transaction: insert %7.1f ms (%5.1f us/row), replace 3 of 31 %7.1f ms (%5.1f us/row)",
				n, float64(d.Microseconds())/1e3, float64(d.Microseconds())/float64(n), float64(r.Microseconds())/1e3, float64(r.Microseconds())/float64(n))
		}
		for _, n := range []int{5000, 20000} {
			key := fmt.Sprintf("wide-%d", n)
			entries := func(gen int) []storage.IndexEntry {
				out := make([]storage.IndexEntry, n)
				for i := range out {
					v := fmt.Sprintf("%08d", i)
					if i%10 == 0 {
						v = fmt.Sprintf("%08d-%d", i, gen)
					}
					out[i] = storage.IndexEntry{Index: "wide", Key: []byte("\x01" + v + "\x00\x00" + key + "\x00\x00")}
				}
				return out
			}
			first, second := entries(0), entries(1)
			d := wall(func(tx storage.WriteTx) error { return tx.PutIndexEntries(indexLabEntity, key, first) })
			r := wall(func(tx storage.WriteTx) error { return tx.PutIndexEntries(indexLabEntity, key, second) })
			note("one row of %5d entries: insert %7.1f ms (%5.1f us/entry), replace a tenth %7.1f ms (%5.1f us/entry)",
				n, float64(d.Microseconds())/1e3, float64(d.Microseconds())/float64(n), float64(r.Microseconds())/1e3, float64(r.Microseconds())/float64(n))
		}
		for _, n := range calls {
			d := wall(func(tx storage.WriteTx) error {
				for i := range n {
					if err := tx.PutIndexEntries(indexLabEntity, "busy", []storage.IndexEntry{
						{Index: "busy", Key: []byte(fmt.Sprintf("\x01%08d\x00\x00busy\x00\x00", i))},
					}); err != nil {
						return err
					}
				}
				return nil
			})
			note("%5d calls for one row in one transaction: %7.1f ms (%5.2f us/call)",
				n, float64(d.Microseconds())/1e3, float64(d.Microseconds())/float64(n))
		}
	}

	names := []string{"empty transaction",
		"replace 3 of 31, 1 rows/tx", "replace 3 of 31, 10 rows/tx", "replace 3 of 31, 100 rows/tx",
		"insert, 1 rows/tx", "insert, 100 rows/tx", "delete, 1 rows/tx", "delete, 100 rows/tx",
		"read first 65 ascending", "read first 65 descending", "read a range of about 313"}
	out := os.Getenv("PGSTORE_INDEX_OUT")
	for _, l := range labs {
		fmt.Printf("\n== %d partitions: %d rows x 31 entries, load %v, index tables %.1f MB (%.1f B/entry)\n",
			l.parts, rows, l.load.Round(time.Millisecond), float64(l.size)/(1<<20), float64(l.size)/float64(rows*31))
		rec := map[string]any{"parts": l.parts, "load_ms": l.load.Milliseconds(), "bytes": l.size}
		for _, name := range names {
			s := l.samples[name]
			if len(s) == 0 {
				continue
			}
			sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
			q := func(p float64) float64 { return float64(s[int(float64(len(s)-1)*p)].Microseconds()) }
			fmt.Printf("%-30s n=%-4d server us/tx: p25 %7.0f  median %7.0f  p75 %7.0f\n", name, len(s), q(0.25), q(0.5), q(0.75))
			rec[name] = q(0.5)
		}
		for _, line := range large[l] {
			fmt.Println(line)
		}
		if out != "" {
			f, err := os.OpenFile(out, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
			if err != nil {
				t.Fatal(err)
			}
			b, _ := json.Marshal(rec)
			_, _ = f.Write(append(b, '\n'))
			_ = f.Close()
		}
	}
}

const (
	// A workspace id of the usual length: the id is part of what some layouts
	// store per entry.
	indexLabWS     = "0b7c6a2e-4f1d-4e8a-9c3b-5d2e1f0a9b8c"
	indexLabEntity = "issue"
	// indexLabClock stores the server clock in a setting of the session, and
	// indexLabElapsed reads the microseconds since.
	indexLabClock   = `SELECT set_config('pgstore_index_lab.t0', clock_timestamp()::text, false)`
	indexLabElapsed = `SELECT (extract(epoch FROM clock_timestamp() - current_setting('pgstore_index_lab.t0')::timestamptz) * 1000000)::int8`
)

// indexLab is one layout under measurement.
type indexLab struct {
	parts   int
	schema  string
	store   *Store
	rows    int
	next    int
	gen     []int
	salt    int
	load    time.Duration
	size    int64
	samples map[string][]time.Duration
}

func indexLabKey(row int) string { return fmt.Sprintf("issue-%08d", row) }

// indexLabEntries is the entry set of BenchmarkPutIndexEntries: 31 entries, of
// which the last three hold gen.
func indexLabEntries(row, gen int) []storage.IndexEntry {
	key := indexLabKey(row)
	entries := make([]storage.IndexEntry, 0, 31)
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
	for i := range 22 {
		entry("__search", fmt.Sprintf("w%02d%04d", i, row%1000))
	}
	entry("by_project_updated", fmt.Sprintf("project-%08d/%012d", row%64, gen))
	entry("by_label_updated", fmt.Sprintf("%012d", gen))
	entry("by_state_priority", fmt.Sprintf("%s/%012d", []string{"open", "done", "blocked"}[row%3], gen))
	return entries
}

// pick returns n rows of the loaded corpus, scattered as the rows of a push
// are.
func (l *indexLab) pick(n int) []int {
	l.salt++
	out := make([]int, n)
	for j := range out {
		out[j] = (l.salt*211 + j*1009) % l.rows
	}
	return out
}

// write runs fn as WithWriteTx does, after one read that every write
// transaction of the engine makes, and returns the server time from after
// that read to after COMMIT.
func (l *indexLab) write(t *testing.T, ctx context.Context, fn func(storage.WriteTx) error) time.Duration {
	t.Helper()
	tx, err := beginTx(ctx, l.store.pool, indexLabWS, beginWrite,
		queuedStmt{sql: `SELECT pg_advisory_xact_lock($1)`, args: []any{workspaceLockKey(indexLabWS)}})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.end()
	tx.scopes, tx.partitioned = l.store.scopes, l.store.partitioned
	if _, err := tx.CurrentSeq(); err != nil {
		t.Fatal(err)
	}
	if err := tx.queue(indexLabClock); err != nil {
		t.Fatal(err)
	}
	sends := tx.sends
	if err := fn(tx); err != nil {
		t.Fatal(err)
	}
	if err := tx.flush(); err != nil {
		t.Fatal(err)
	}
	if tx.sends != sends {
		t.Fatalf("the transaction made a round trip between its first read and COMMIT")
	}
	br, err := tx.sendWith(queuedStmt{sql: `COMMIT`}, queuedStmt{sql: indexLabElapsed})
	if err != nil {
		t.Fatal(err)
	}
	var us int64
	if _, err := br.Exec(); err != nil {
		t.Fatal(err)
	}
	if err := br.QueryRow().Scan(&us); err != nil {
		t.Fatal(err)
	}
	if err := br.Close(); err != nil {
		t.Fatal(err)
	}
	return time.Duration(us) * time.Microsecond
}

// reads runs n read transactions of fn and returns the plan and execution
// time of the statements of one.
func (l *indexLab) reads(t *testing.T, ctx context.Context, admin *pgx.Conn, n int, fn func(storage.ReadTx) error) time.Duration {
	t.Helper()
	if _, err := admin.Exec(ctx, `SELECT pg_stat_statements_reset()`); err != nil {
		t.Fatal(err)
	}
	for range n {
		if err := l.store.WithReadTx(ctx, indexLabWS, fn); err != nil {
			t.Fatal(err)
		}
	}
	var ms float64
	if err := admin.QueryRow(ctx, `SELECT COALESCE(sum(s.total_plan_time + s.total_exec_time), 0)::float8
		FROM pg_stat_statements s JOIN pg_database d ON d.oid = s.dbid
		WHERE d.datname = current_database() AND s.query NOT LIKE '%pg_stat_statements%'`).Scan(&ms); err != nil {
		t.Fatal(err)
	}
	return time.Duration(ms * float64(time.Millisecond) / float64(n))
}

// indexLabTables lists the tables of the schema that hold index entries.
func (l *indexLab) indexLabTables(t *testing.T, ctx context.Context, admin *pgx.Conn, kinds string) []string {
	t.Helper()
	rows, err := admin.Query(ctx, `SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relkind = ANY($2::"char"[]) AND c.relname ~ '^index_(entries|rows|scopes)'
		ORDER BY 1`, l.schema, strings.Split(kinds, ""))
	if err != nil {
		t.Fatal(err)
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	return names
}

func (l *indexLab) vacuum(t *testing.T, ctx context.Context, admin *pgx.Conn) {
	t.Helper()
	for _, name := range l.indexLabTables(t, ctx, admin, "r") {
		if _, err := admin.Exec(ctx, fmt.Sprintf(`VACUUM "%s"."%s"`, l.schema, name)); err != nil {
			t.Fatal(err)
		}
	}
	l.analyze(t, ctx, admin)
}

func (l *indexLab) analyze(t *testing.T, ctx context.Context, admin *pgx.Conn) {
	t.Helper()
	for _, name := range l.indexLabTables(t, ctx, admin, "rp") {
		if _, err := admin.Exec(ctx, fmt.Sprintf(`ANALYZE "%s"."%s"`, l.schema, name)); err != nil {
			t.Fatal(err)
		}
	}
}

// tableBytes is the size of the tables that hold index entries, with their
// indexes.
func (l *indexLab) tableBytes(t *testing.T, ctx context.Context, admin *pgx.Conn) int64 {
	t.Helper()
	var total int64
	for _, name := range l.indexLabTables(t, ctx, admin, "r") {
		var n int64
		if err := admin.QueryRow(ctx, `SELECT pg_total_relation_size(format('%I.%I', $1::text, $2::text)::regclass)`,
			l.schema, name).Scan(&n); err != nil {
			t.Fatal(err)
		}
		total += n
	}
	return total
}
