package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/levelfourab/syncplane/engine/internal/pgtest"
	"github.com/levelfourab/syncplane/engine/storage"
)

// A partitioned layout hashes posting_rows, posting_chunks and posting_pages
// by scope. These
// specs run every posting statement under EXPLAIN ANALYZE with a generic plan,
// which is the plan pgx's statement cache can end up with and the one that
// cannot prune when it plans, and count the partitions each statement reads.
var _ = Describe("Postgres posting partition pruning", func() {
	ctx := context.Background()

	It("reads one partition per scope in every posting statement, also with a generic plan", func() {
		// common and x are in chunks, rare and the p keys in small pages.
		setPostingFormat(4, 16, 6)

		dsn := pgtest.DSN()
		store, err := Open(ctx, dsn, WithPartitions(8))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		cfg, err := pgxpool.ParseConfig(dsn)
		Expect(err).NotTo(HaveOccurred())
		tracer := &tripTracer{}
		cfg.ConnConfig.Tracer = tracer
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(pool.Close)
		traced := &Store{pool: pool, scopes: newScopeCache()}

		keys := func(ks ...string) [][]byte {
			out := make([][]byte, len(ks))
			for i, k := range ks {
				out[i] = []byte(k)
			}
			return out
		}
		for _, ws := range []string{"a", "b"} {
			Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
				for i := range 20 {
					row := fmt.Sprintf("r%02d", i)
					add := keys("common")
					if i%5 == 0 {
						add = append(add, []byte("rare"))
					}
					if i < 10 {
						add = append(add, []byte(fmt.Sprintf("p%02d", i)))
					}
					if err := tx.PutPostings("doc", "search", row, nil, add); err != nil {
						return err
					}
					if err := tx.PutPostings("doc", "other", row, nil, keys("x")); err != nil {
						return err
					}
				}
				return nil
			})).To(Succeed())
		}

		var stmts []tracedQuery
		rollback := errors.New("roll back")
		// The writer's read and its queued writes, without committing them.
		Expect(traced.WithWriteTx(ctx, "a", func(tx storage.WriteTx) error {
			Expect(tx.PutPostings("doc", "search", "r01", keys("common"), keys("rare"))).To(Succeed())
			Expect(tx.PutPostings("doc", "search", "new", nil, keys("common", "fresh"))).To(Succeed())
			Expect(tx.DeletePostings("doc", "search", "r05", keys("common", "rare", "p05"))).To(Succeed())
			// Empties and shrinks pages, so that pages merge and go.
			for _, i := range []int{1, 2, 3, 4, 6, 7} {
				row := fmt.Sprintf("r%02d", i)
				Expect(tx.PutPostings("doc", "search", row, keys(fmt.Sprintf("p%02d", i)), nil)).To(Succeed())
			}
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
		Expect(traced.WithReadTx(ctx, "a", func(tx storage.ReadTx) error {
			tracer.reset()
			one := func(k string) storage.IndexRange {
				return storage.IndexRange{Lo: []byte(k), Hi: []byte(k + "\x00")}
			}
			_, _, err := tx.PostingCounts("doc", "search", []storage.IndexRange{one("common"), one("rare"), {}}, 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(tx.ScanPostings("doc", "search", []storage.IndexRange{one("common"), one("rare")},
				[]storage.IndexRange{{Lo: []byte("f"), Hi: []byte("g")}}, 0, func(string) error { return nil })).To(Succeed())
			Expect(tx.ScanPostings("doc", "search", []storage.IndexRange{{}}, nil, 3, func(string) error { return nil })).To(Succeed())
			stmts = append(stmts, tracer.taken()...)
			return nil
		})).To(Succeed())
		var scopes []int32
		rows, err := store.pool.Query(ctx, `SELECT scope FROM posting_scopes WHERE workspace_id = 'a'`)
		Expect(err).NotTo(HaveOccurred())
		scopes, err = pgx.CollectRows(rows, pgx.RowTo[int32])
		Expect(err).NotTo(HaveOccurred())
		Expect(scopes).To(HaveLen(2))
		stmts = append(stmts, postingQueuedStmts(scopedDeletes(scopes))...)

		Expect(len(stmts)).To(BeNumerically(">=", 12))
		for _, want := range []string{
			"FROM posting_pages WHERE scope = $1 AND lo <= u.k", // the writer's read
			"INSERT INTO posting_pages",
			"DELETE FROM posting_pages p",
			"DELETE FROM posting_pages WHERE scope = (",  // ClearPostings
			"DELETE FROM posting_pages WHERE scope = $1", // DeleteWorkspace
			"x.n, x.body",   // PostingCounts
			"NULL::int8, 0", // ScanPostings
		} {
			Expect(slices.ContainsFunc(stmts, func(s tracedQuery) bool { return strings.Contains(s.sql, want) })).To(BeTrue(), want)
		}
		conn := genericConn(ctx, dsn)
		for i, s := range stmts {
			// Each statement runs on the stored state alone: the clear must
			// not empty the index the reads after it read.
			_, err = conn.Exec(ctx, `BEGIN`)
			Expect(err).NotTo(HaveOccurred())
			read := partitionsRead(ctx, conn, i, s)
			_, err = conn.Exec(ctx, `ROLLBACK`)
			Expect(err).NotTo(HaveOccurred())
			fmt.Fprintf(GinkgoWriter, "%v  %s\n", read, firstLine(s.sql))
			for parent, parts := range read {
				Expect(len(parts)).To(BeNumerically("<=", 1),
					"%s reads partitions %v of %s", firstLine(s.sql), parts, parent)
			}
		}
	})
})

// genericConn connects to dsn with every prepared statement on its generic
// plan.
func genericConn(ctx context.Context, dsn string) *pgx.Conn {
	GinkgoHelper()
	u, err := url.Parse(dsn)
	Expect(err).NotTo(HaveOccurred())
	q := u.Query()
	q.Set("options", q.Get("options")+" -c plan_cache_mode=force_generic_plan")
	u.RawQuery = q.Encode()
	conn, err := pgx.Connect(ctx, u.String())
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

var partitionName = regexp.MustCompile(`^(posting_rows|posting_chunks|posting_pages)_p\d+$`)

// partitionsRead prepares s, runs it under EXPLAIN ANALYZE and returns, for
// each partitioned posting table, the partitions a plan node ran on.
func partitionsRead(ctx context.Context, conn *pgx.Conn, i int, s tracedQuery) map[string][]string {
	GinkgoHelper()
	name := fmt.Sprintf("prune%d", i)
	_, err := conn.Exec(ctx, `PREPARE `+name+` AS `+s.sql)
	Expect(err).NotTo(HaveOccurred(), firstLine(s.sql))
	// EXECUTE takes no protocol parameters, so the values go in as literals.
	exec := `EXECUTE ` + name
	if len(s.args) > 0 {
		ps := make([]string, len(s.args))
		for j, a := range s.args {
			ps[j] = sqlLiteral(a)
		}
		exec += "(" + strings.Join(ps, ", ") + ")"
	}
	var raw []byte
	Expect(conn.QueryRow(ctx, `EXPLAIN (ANALYZE, FORMAT JSON) `+exec).Scan(&raw)).To(Succeed(), firstLine(s.sql))
	var plans []struct {
		Plan map[string]any `json:"Plan"`
	}
	Expect(json.Unmarshal(raw, &plans)).To(Succeed())
	read := map[string]map[string]bool{}
	var walk func(node map[string]any)
	walk = func(node map[string]any) {
		rel, _ := node["Relation Name"].(string)
		loops, _ := node["Actual Loops"].(float64)
		if m := partitionName.FindStringSubmatch(rel); m != nil && loops > 0 {
			if read[m[1]] == nil {
				read[m[1]] = map[string]bool{}
			}
			read[m[1]][rel] = true
		}
		children, _ := node["Plans"].([]any)
		for _, c := range children {
			if cm, ok := c.(map[string]any); ok {
				walk(cm)
			}
		}
	}
	walk(plans[0].Plan)
	out := map[string][]string{}
	for parent, parts := range read {
		for p := range parts {
			out[parent] = append(out[parent], p)
		}
	}
	return out
}

// sqlLiteral writes v, one of the argument types the posting statements use,
// as an SQL literal.
func sqlLiteral(v any) string {
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
	hex := func(b []byte) string { return fmt.Sprintf(`'\\x%x'`, b) }
	array := func(items []string) string {
		if len(items) == 0 {
			return "'{}'"
		}
		return "ARRAY[" + strings.Join(items, ", ") + "]"
	}
	switch v := v.(type) {
	case string:
		return quote(v)
	case []byte:
		return hex(v)
	case bool, int, int32, int64:
		return fmt.Sprint(v)
	case *int64:
		if v == nil {
			return "NULL"
		}
		return fmt.Sprint(*v)
	case []string:
		items := make([]string, len(v))
		for i, x := range v {
			items[i] = quote(x)
		}
		return array(items)
	case [][]byte:
		items := make([]string, len(v))
		for i, x := range v {
			items[i] = hex(x) + "::bytea"
		}
		return array(items)
	case []bool:
		items := make([]string, len(v))
		for i, x := range v {
			items[i] = fmt.Sprint(x)
		}
		return array(items)
	case []int32:
		items := make([]string, len(v))
		for i, x := range v {
			items[i] = fmt.Sprint(x)
		}
		return array(items)
	case []int64:
		items := make([]string, len(v))
		for i, x := range v {
			items[i] = fmt.Sprint(x)
		}
		return array(items)
	}
	panic(fmt.Sprintf("sqlLiteral: %T", v))
}

func firstLine(sql string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(sql), "\n")
	return line
}
