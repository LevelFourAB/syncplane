package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/levelfourab/syncplane/engine/internal/pgtest"
	"github.com/levelfourab/syncplane/engine/storage"
)

// The posting statements must keep their plan whatever the statistics: a
// generic plan made when the tables were empty runs on the tables when they
// are large. These specs plan every posting statement with a generic plan on
// empty tables and again on large ones, and compare the plans.
var _ = Describe("Postgres posting plans", func() {
	ctx := context.Background()

	for _, layout := range []struct {
		name string
		opts []Option
	}{{"unpartitioned", nil}, {"partitioned", []Option{WithPartitions(4)}}} {
		It("keeps the plan of every posting statement on empty and large tables, with a generic plan ("+layout.name+")", func() {
			setPostingFormat(4, 16, 6)
			dsn := pgtest.DSN()
			store, err := Open(ctx, dsn, layout.opts...)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
			stmts := postingStatements(ctx, dsn, store)

			plan := func() []string {
				conn := genericConn(ctx, dsn)
				out := make([]string, len(stmts))
				for i, s := range stmts {
					out[i] = planShape(ctx, conn, i, s)
				}
				return out
			}
			empty := plan()

			// About a million tuples in each posting table, over 2,000 scopes.
			for _, sql := range []string{
				`INSERT INTO posting_scopes (workspace_id, entity, idx, scope, next_row, row_count)
				 SELECT 'fill' || s, 'doc', 'search', 100000 + s, 500, 500 FROM generate_series(1, 2000) s`,
				`INSERT INTO posting_rows (scope, num, key)
				 SELECT 100000 + s, g, 'row' || g FROM generate_series(1, 2000) s, generate_series(1, 500) g`,
				`INSERT INTO posting_chunks (scope, n, chunk, k, ids)
				 SELECT 100000 + s, 1, 0, convert_to('key' || g, 'UTF8'), '\x01'::bytea FROM generate_series(1, 2000) s, generate_series(1, 500) g`,
				`INSERT INTO posting_pages (scope, lo, body)
				 SELECT 100000 + s, convert_to('key' || g, 'UTF8'), '\x0001610101'::bytea FROM generate_series(1, 2000) s, generate_series(1, 500) g`,
				`ANALYZE posting_scopes, posting_rows, posting_chunks, posting_pages`,
			} {
				_, err := store.pool.Exec(ctx, sql)
				Expect(err).NotTo(HaveOccurred(), firstLine(sql))
			}
			large := plan()

			var wrong []string
			for i, s := range stmts {
				fmt.Fprintf(GinkgoWriter, "%s\n  %s\n", firstLine(s.sql), large[i])
				if large[i] != empty[i] {
					wrong = append(wrong, fmt.Sprintf("%s\n  empty: %s\n  large: %s", firstLine(s.sql), empty[i], large[i]))
				}
				if strings.Contains(empty[i]+large[i], "Seq Scan on posting_") {
					wrong = append(wrong, fmt.Sprintf("%s scans a whole table", firstLine(s.sql)))
				}
			}
			Expect(wrong).To(BeEmpty())
		})
	}
})

// postingStatements runs every kind of posting read and write on store
// without committing the writes, and returns the posting statements they
// send or queue, with their arguments.
func postingStatements(ctx context.Context, dsn string, store *Store) []tracedQuery {
	GinkgoHelper()
	cfg, err := pgxpool.ParseConfig(dsn)
	Expect(err).NotTo(HaveOccurred())
	tracer := &tripTracer{}
	cfg.ConnConfig.Tracer = tracer
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(pool.Close)
	traced := &Store{pool: pool, scopes: newScopeCache()}

	Expect(store.WithWriteTx(ctx, "a", func(tx storage.WriteTx) error {
		for i := range 20 {
			add := byteKeys("common", fmt.Sprintf("p%02d", i))
			if i%5 == 0 {
				add = append(add, []byte("rare"))
			}
			if err := tx.PutPostings("doc", "search", fmt.Sprintf("r%02d", i), nil, add); err != nil {
				return err
			}
		}
		return nil
	})).To(Succeed())

	var stmts []tracedQuery
	rollback := errors.New("roll back")
	Expect(traced.WithWriteTx(ctx, "a", func(tx storage.WriteTx) error {
		Expect(tx.PutPostings("doc", "search", "r01", byteKeys("common", "p01"), byteKeys("rare"))).To(Succeed())
		Expect(tx.PutPostings("doc", "search", "new", nil, byteKeys("common", "fresh"))).To(Succeed())
		Expect(tx.DeletePostings("doc", "search", "r05", byteKeys("common", "rare", "p05"))).To(Succeed())
		t := tx.(*pgTx)
		tracer.reset()
		Expect(t.resolvePostings()).To(Succeed())
		stmts = append(stmts, tracer.taken()...)
		stmts = append(stmts, postingQueuedStmts(t.queued)...)
		return rollback
	})).To(MatchError(rollback))
	Expect(traced.WithWriteTx(ctx, "a", func(tx storage.WriteTx) error {
		Expect(tx.ClearPostings("doc", "search")).To(Succeed())
		stmts = append(stmts, postingQueuedStmts(tx.(*pgTx).queued)...)
		return rollback
	})).To(MatchError(rollback))
	for _, cache := range []string{"cold", "warm"} {
		if cache == "cold" {
			traced.scopes = newScopeCache()
		}
		Expect(traced.WithReadTx(ctx, "a", func(tx storage.ReadTx) error {
			tracer.reset()
			one := func(k string) storage.IndexRange {
				return storage.IndexRange{Lo: []byte(k), Hi: []byte(k + "\x00")}
			}
			_, _, err := tx.PostingCounts("doc", "search", []storage.IndexRange{one("common"), one("rare"), {}}, 10)
			Expect(err).NotTo(HaveOccurred())
			Expect(tx.ScanPostings("doc", "search", []storage.IndexRange{one("common"), one("rare")},
				[]storage.IndexRange{{Lo: []byte("p"), Hi: []byte("q")}}, 0, func(string) error { return nil })).To(Succeed())
			stmts = append(stmts, tracer.taken()...)
			return nil
		})).To(Succeed())
	}
	var scopes []int32
	rows, err := store.pool.Query(ctx, `SELECT scope FROM posting_scopes WHERE workspace_id = 'a'`)
	Expect(err).NotTo(HaveOccurred())
	scopes, err = pgx.CollectRows(rows, pgx.RowTo[int32])
	Expect(err).NotTo(HaveOccurred())
	stmts = append(stmts, postingQueuedStmts(scopedDeletes(scopes))...)

	// Each statement once, with the arguments of its first use.
	seen := map[string]bool{}
	var out []tracedQuery
	for _, s := range stmts {
		if !seen[s.sql] {
			seen[s.sql] = true
			out = append(out, s)
		}
	}
	return out
}

var partitionSuffix = regexp.MustCompile(`_p\d+`)

// planShape prepares s on conn and returns the shape of its plan: the node
// types in order, with the relations and indexes they read, partitions
// folded into their parent.
func planShape(ctx context.Context, conn *pgx.Conn, i int, s tracedQuery) string {
	GinkgoHelper()
	name := fmt.Sprintf("shape%d", i)
	_, err := conn.Exec(ctx, `PREPARE `+name+` AS `+s.sql)
	Expect(err).NotTo(HaveOccurred(), firstLine(s.sql))
	DeferCleanup(func() { _, _ = conn.Exec(context.Background(), `DEALLOCATE `+name) })
	exec := `EXECUTE ` + name
	if len(s.args) > 0 {
		ps := make([]string, len(s.args))
		for j, a := range s.args {
			ps[j] = sqlLiteral(a)
		}
		exec += "(" + strings.Join(ps, ", ") + ")"
	}
	var raw []byte
	Expect(conn.QueryRow(ctx, `EXPLAIN (FORMAT JSON) `+exec).Scan(&raw)).To(Succeed(), firstLine(s.sql))
	var plans []struct {
		Plan map[string]any `json:"Plan"`
	}
	Expect(json.Unmarshal(raw, &plans)).To(Succeed())
	var parts []string
	var walk func(node map[string]any, depth int)
	walk = func(node map[string]any, depth int) {
		item := fmt.Sprint(depth, ":", node["Node Type"])
		children, _ := node["Plans"].([]any)
		// A bitmap scan of one index probes it as an index scan does; the
		// planner takes either for a list of keys.
		if node["Node Type"] == "Bitmap Heap Scan" && len(children) == 1 {
			child, _ := children[0].(map[string]any)
			if child["Node Type"] == "Bitmap Index Scan" {
				item = fmt.Sprint(depth, ":Index Scan")
				node = map[string]any{"Relation Name": node["Relation Name"], "Index Name": child["Index Name"]}
				children = nil
			}
		}
		if rel, ok := node["Relation Name"].(string); ok {
			item += " on " + partitionSuffix.ReplaceAllString(rel, "")
		}
		if idx, ok := node["Index Name"].(string); ok {
			item += " using " + partitionSuffix.ReplaceAllString(idx, "")
		}
		// Appends over the partitions of one table differ only in how many
		// partitions they list.
		if len(parts) > 0 && parts[len(parts)-1] == item {
			return
		}
		parts = append(parts, item)
		for _, c := range children {
			if cm, ok := c.(map[string]any); ok {
				walk(cm, depth+1)
			}
		}
	}
	walk(plans[0].Plan, 0)
	return strings.Join(parts, ", ")
}
