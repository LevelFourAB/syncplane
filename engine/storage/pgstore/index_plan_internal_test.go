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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/levelfourab/syncplane/engine/internal/pgtest"
	"github.com/levelfourab/syncplane/engine/storage"
)

// The statements on the index tables run on the plan kind of the layout, which
// the transaction sets for them alone (see forceGenericPlan). These specs pin
// what that rests on: that the plan cache obeys the setting for a statement in
// the same batch, that the plan is the same whatever the statistics were when
// it was made, and that on a partitioned layout a statement reads and locks
// the partitions of its workspace alone.

// indexLayouts are the layouts the specs of the index entries run on.
var indexLayouts = []struct {
	name string
	opts []Option
}{{"unpartitioned", nil}, {"partitioned", []Option{WithPartitions(4)}}}

// planCounts reads how often each prepared statement of the one connection of
// store ran on a generic and on a custom plan, by the first line of its text.
func planCounts(ctx context.Context, store *Store) map[string][2]int64 {
	GinkgoHelper()
	rows, err := store.pool.Query(ctx, `SELECT statement, generic_plans, custom_plans FROM pg_prepared_statements`)
	Expect(err).NotTo(HaveOccurred())
	defer rows.Close()
	out := map[string][2]int64{}
	for rows.Next() {
		var (
			sql             string
			generic, custom int64
		)
		Expect(rows.Scan(&sql, &generic, &custom)).To(Succeed())
		out[firstLine(sql)] = [2]int64{generic, custom}
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	return out
}

// planModeDSN returns dsn with plan_cache_mode set to mode for its sessions.
func planModeDSN(dsn, mode string) string {
	GinkgoHelper()
	u, err := url.Parse(dsn)
	Expect(err).NotTo(HaveOccurred())
	q := u.Query()
	q.Set("options", q.Get("options")+" -c plan_cache_mode="+mode)
	u.RawQuery = q.Encode()
	return u.String()
}

// planModeConn connects to dsn with every prepared statement on the plan kind
// of mode.
func planModeConn(ctx context.Context, dsn, mode string) *pgx.Conn {
	GinkgoHelper()
	conn, err := pgx.Connect(ctx, planModeDSN(dsn, mode))
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// indexEntry is an entry of index with the value v that ends with the row
// key, as the engine's entries do.
func indexEntry(index, v, rowKey string) storage.IndexEntry {
	return storage.IndexEntry{Index: index, Key: []byte("\x01" + v + indexEntryEnd + rowKey + indexEntryEnd)}
}

var _ = Describe("Postgres index entry plans", func() {
	ctx := context.Background()

	for _, layout := range indexLayouts {
		It("runs the entry statements on the plan kind of the layout from the first call, and the statements after them in the default mode of the session ("+layout.name+")", func() {
			// The session's own mode is the one the layout does not want, so
			// that the plan counts show which statements the transaction set
			// the mode for, and which run in the mode of the session.
			partitioned := len(layout.opts) > 0
			sessionMode, entryPlans := "force_custom_plan", [2]int64{1, 0}
			if partitioned {
				sessionMode, entryPlans = "force_generic_plan", [2]int64{0, 1}
			}
			// One connection, so every transaction runs on the session whose
			// prepared statements the spec reads.
			store, err := Open(ctx, planModeDSN(pgtest.DSN(), sessionMode), append([]Option{WithMaxConns(1)}, layout.opts...)...)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

			const runs = 7
			for i := range runs {
				Expect(store.WithWriteTx(ctx, "a", func(tx storage.WriteTx) error {
					key := fmt.Sprintf("r%02d", i)
					Expect(tx.PutIndexEntries("doc", key, []storage.IndexEntry{indexEntry("by_v", "v", key), tagEntry("t")})).To(Succeed())
					if i > 0 {
						Expect(tx.DeleteIndexEntries("doc", fmt.Sprintf("r%02d", i-1))).To(Succeed())
					}
					// Each read sends the entry statements and then itself, in
					// one batch, so it is a statement after them in the
					// transaction.
					Expect(rowKeys(tx, "doc", "by_v")).To(Equal([]string{key}))
					var mode string
					Expect(tx.(*pgTx).queryRow(`SHOW plan_cache_mode`).Scan(&mode)).To(Succeed())
					Expect(mode).To(Equal(sessionMode), "after the entry statements and an index read")
					Expect(rowKeys(tx, "doc", "by_tag")).To(Equal([]string{key}))
					Expect(tx.(*pgTx).queryRow(`SHOW plan_cache_mode`).Scan(&mode)).To(Succeed())
					Expect(mode).To(Equal(sessionMode), "after a read of row keys")
					return nil
				})).To(Succeed())
			}

			counts := planCounts(ctx, store)
			times := func(plans [2]int64, n int64) [2]int64 { return [2]int64{plans[0] * n, plans[1] * n} }
			Expect(counts).To(HaveKeyWithValue(firstLine(indexPutSQL), times(entryPlans, runs)),
				"generic and custom plans of the statement that replaces entries")
			Expect(counts).To(HaveKeyWithValue(firstLine(indexDeleteSQL), times(entryPlans, runs-1)))
			// The reads: in the mode of the session when unpartitioned, and on
			// a custom plan when partitioned.
			readPlans := times([2]int64{0, 1}, 2*runs)
			Expect(counts).To(HaveKeyWithValue(
				"SELECT k, rk FROM index_entries WHERE scope = $1 AND wsn = $2 ORDER BY k, rk LIMIT $3", readPlans))
			Expect(counts).To(HaveKeyWithValue(firstLine(indexRowKeysSQL), times([2]int64{0, 1}, runs)))
		})

		It("keeps the plan of every entry statement whatever the tables hold, on the plan kind of the layout ("+layout.name+")", func() {
			dsn := pgtest.DSN()
			store, err := Open(ctx, dsn, layout.opts...)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
			stmts := indexStatements(ctx, dsn, store)
			// A custom plan also depends on the arguments, which are those of
			// a small transaction here.
			mode := "force_generic_plan"
			if store.partitioned {
				mode = "force_custom_plan"
			}

			plan := func() []string {
				conn := planModeConn(ctx, dsn, mode)
				out := make([]string, len(stmts))
				for i, s := range stmts {
					out[i] = indexAccess(ctx, conn, i, s)
				}
				return out
			}
			// fill adds workspaces of rows rows with entries entries each,
			// with ids from base on.
			fill := func(base, workspaces, rows, entries int) {
				GinkgoHelper()
				for _, sql := range []string{
					fmt.Sprintf(`INSERT INTO index_scopes (workspace_id, entity, idx, scope)
					 SELECT 'fill%[1]d-' || s, '', '', %[1]d + s * 8 FROM generate_series(1, %[2]d) s`, base, workspaces),
					fmt.Sprintf(`INSERT INTO index_rows (wsn, ent, rid, key)
					 SELECT %[1]d + s * 8, %[1]d + s * 8 + 1, %[1]d::int8 * 1000 + s * %[3]d + g, 'row' || g
					 FROM generate_series(1, %[2]d) s, generate_series(0, %[3]d - 1) g`, base, workspaces, rows),
					fmt.Sprintf(`INSERT INTO index_entries (wsn, scope, rk, rid, k)
					 SELECT %[1]d + s * 8, %[1]d + s * 8 + 2 + e %% 5, -4, %[1]d::int8 * 1000 + s * %[3]d + g, convert_to('key' || e || 'row' || g, 'UTF8')
					 FROM generate_series(1, %[2]d) s, generate_series(0, %[3]d - 1) g, generate_series(1, %[4]d) e`, base, workspaces, rows, entries),
					`ANALYZE index_scopes, index_rows, index_entries`,
				} {
					_, err := store.pool.Exec(ctx, sql)
					Expect(err).NotTo(HaveOccurred(), firstLine(sql))
				}
			}
			plans := map[string][]string{"empty tables": plan()}
			// The average workspace decides what the planner expects of one. A
			// plan made for small ones must be the plan for large ones too: a
			// plan that reads a workspace whole costs nothing on a small one.
			fill(1000000, 20000, 2, 5)
			plans["20,000 workspaces of 10 entries"] = plan()
			fill(2000000, 200, 100, 50)
			plans["and 200 workspaces of 5,000 entries"] = plan()

			var wrong []string
			for i, s := range stmts {
				first := plans["empty tables"][i]
				fmt.Fprintf(GinkgoWriter, "%s\n  %s\n", firstLine(s.sql), first)
				for state, access := range plans {
					if access[i] != first {
						wrong = append(wrong, fmt.Sprintf("%s\n  empty tables: %s\n  %s: %s", firstLine(s.sql), first, state, access[i]))
					}
					if strings.Contains(access[i], "WHOLE") {
						wrong = append(wrong, fmt.Sprintf("%s reads a table whole with %s: %s", firstLine(s.sql), state, access[i]))
					}
				}
			}
			Expect(wrong).To(BeEmpty())
		})
	}

	It("reads one partition of each table in every entry statement", func() {
		dsn := pgtest.DSN()
		store, err := Open(ctx, dsn, WithPartitions(8))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		stmts := indexStatements(ctx, dsn, store)

		conn := planModeConn(ctx, dsn, "force_custom_plan")
		for i, s := range stmts {
			// Each statement runs on the stored state alone.
			_, err = conn.Exec(ctx, `BEGIN`)
			Expect(err).NotTo(HaveOccurred())
			read := indexPartitionsRead(ctx, conn, i, s)
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

// A partitioned layout is there so that maintenance can take one partition at
// a time. A statement that locks every partition before it prunes to its own
// waits for an exclusive lock on any of them, and its locks count against the
// lock table of the server for every partition.
var _ = Describe("Postgres index entry locks", func() {
	ctx := context.Background()

	// otherPartition returns a partition of table that holds no tuple.
	otherPartition := func(store *Store, table string) string {
		GinkgoHelper()
		var name string
		Expect(store.pool.QueryRow(ctx, `SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
			WHERE i.inhparent = to_regclass($1)
			  AND c.oid NOT IN (SELECT tableoid FROM `+table+`)
			ORDER BY 1 LIMIT 1`, table).Scan(&name)).To(Succeed())
		return name
	}

	It("writes, reads and deletes entries while another session holds an exclusive lock on another partition", func() {
		dsn := pgtest.DSN()
		seed, err := Open(ctx, dsn, WithPartitions(8))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(seed.Close()).To(Succeed()) })
		Expect(seed.WithWriteTx(ctx, "a", func(tx storage.WriteTx) error {
			for _, key := range []string{"r1", "r2", "r3"} {
				Expect(tx.PutRow("doc", key, []byte("v"), 1, storage.Actor{})).To(Succeed())
				Expect(tx.PutIndexEntries("doc", key, docEntries("v", key))).To(Succeed())
			}
			return nil
		})).To(Succeed())

		locker, err := pgx.Connect(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = locker.Close(context.Background()) })
		_, err = locker.Exec(ctx, `BEGIN`)
		Expect(err).NotTo(HaveOccurred())
		_, err = locker.Exec(ctx, fmt.Sprintf(`LOCK TABLE %s, %s IN ACCESS EXCLUSIVE MODE`,
			otherPartition(seed, "index_entries"), otherPartition(seed, "index_rows")))
		Expect(err).NotTo(HaveOccurred())

		// A store of its own, so that its connection sees each statement for
		// the first time, and a deadline in place of a wait for the lock.
		store, err := Open(ctx, dsn, WithMaxConns(1))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		Expect(store.partitioned).To(BeTrue())
		waited, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		// More than the five calls after which the plan cache picks a plan
		// kind by itself.
		for i := range 7 {
			Expect(store.WithWriteTx(waited, "a", func(tx storage.WriteTx) error {
				Expect(tx.PutIndexEntries("doc", "r1", docEntries(fmt.Sprint("v", i), "r1"))).To(Succeed())
				Expect(tx.PutIndexEntries("doc", fmt.Sprint("new", i), docEntries("v", fmt.Sprint("new", i)))).To(Succeed())
				if i > 0 {
					Expect(tx.DeleteIndexEntries("doc", fmt.Sprint("new", i-1))).To(Succeed())
				}
				return nil
			})).To(Succeed(), "a write of entries, run %d", i)
			Expect(store.WithReadTx(waited, "a", func(tx storage.ReadTx) error {
				Expect(rowKeys(tx, "doc", "by_v")).To(HaveLen(4))
				Expect(rowKeys(tx, "doc", "by_tag")).To(HaveLen(4))
				var ranged int
				Expect(tx.ScanIndexRanges("doc", "by_v", []storage.IndexRange{
					{Lo: []byte("\x01v"), Hi: []byte("\x01v1")}, {Lo: []byte("\x01v1"), Hi: []byte("\x01w")},
				}, func(int, []byte, string) error {
					ranged++
					return nil
				})).To(Succeed())
				Expect(ranged).To(Equal(4))
				return nil
			})).To(Succeed(), "reads of entries, run %d", i)
		}
		Expect(store.WithWriteTx(waited, "a", func(tx storage.WriteTx) error {
			n, _, err := tx.PurgeEntityRows("doc", "r2", time.Time{}, 0)
			Expect(n).To(Equal(1))
			return err
		})).To(Succeed(), "PurgeEntityRows")
		// DeleteWorkspace also empties tables this spec holds no lock on.
		Expect(store.DeleteWorkspace(waited, "a")).To(Succeed())
	})

	It("locks a number of relations in a write transaction that does not grow with the partitions", func() {
		// The relation locks one transaction holds before COMMIT, after a row
		// write, a replace and a delete of entries: in the first transaction
		// of a connection, in a later one, and in the one after an ANALYZE,
		// which makes every plan anew.
		measure := func(partitions int) (first, steady, analyzed int) {
			GinkgoHelper()
			dsn := pgtest.DSN()
			seed, err := Open(ctx, dsn, WithPartitions(partitions))
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { Expect(seed.Close()).To(Succeed()) })
			write := func(store *Store, i int) (locks int) {
				GinkgoHelper()
				Expect(store.WithWriteTx(ctx, "a", func(tx storage.WriteTx) error {
					t := tx.(*pgTx)
					seq, err := tx.NextSeq()
					Expect(err).NotTo(HaveOccurred())
					_, err = tx.IndexStates("doc")
					Expect(err).NotTo(HaveOccurred())
					Expect(tx.PutRow("doc", "r1", []byte("v"), seq, storage.Actor{})).To(Succeed())
					Expect(tx.PutIndexEntries("doc", "r1", docEntries(fmt.Sprint("v", i), "r1"))).To(Succeed())
					Expect(tx.PutIndexEntries("doc", fmt.Sprint("new", i), docEntries("v", fmt.Sprint("new", i)))).To(Succeed())
					Expect(tx.DeleteIndexEntries("doc", fmt.Sprint("new", i-1))).To(Succeed())
					Expect(t.resolveEntries()).To(Succeed())
					return t.queryRow(`SELECT count(*)::int4 FROM pg_locks
						WHERE pid = pg_backend_pid() AND locktype = 'relation'`).Scan(&locks)
				})).To(Succeed())
				return locks
			}
			write(seed, 0)

			store, err := Open(ctx, dsn, WithMaxConns(1))
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
			first = write(store, 1)
			for i := 2; i < 9; i++ {
				steady = write(store, i)
			}
			_, err = seed.pool.Exec(ctx, `ANALYZE index_entries, index_rows, rows`)
			Expect(err).NotTo(HaveOccurred())
			analyzed = write(store, 9)
			fmt.Fprintf(GinkgoWriter, "%d partitions: %d relation locks in the first transaction, %d later, %d after ANALYZE\n",
				partitions, first, steady, analyzed)
			return first, steady, analyzed
		}
		first8, steady8, analyzed8 := measure(8)
		first32, steady32, analyzed32 := measure(32)
		Expect([]int{first32, steady32, analyzed32}).To(Equal([]int{first8, steady8, analyzed8}))
		// The partitioned parents, one partition of each table the
		// transaction touches, and their indexes.
		Expect(max(first32, steady32, analyzed32)).To(BeNumerically("<=", 48))
	})
})

// indexStatements writes entries of two workspaces through store, then runs
// every kind of read and write of index entries without committing the
// writes, and returns the statements on index_rows and index_entries that
// they send or queue, with their arguments.
func indexStatements(ctx context.Context, dsn string, store *Store) []tracedQuery {
	GinkgoHelper()
	cfg, err := pgxpool.ParseConfig(dsn)
	Expect(err).NotTo(HaveOccurred())
	tracer := &indexTracer{}
	cfg.ConnConfig.Tracer = tracer
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(pool.Close)
	traced := &Store{pool: pool, scopes: newScopeCache(), partitioned: store.partitioned}

	for _, ws := range []string{"a", "b"} {
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			for i := range 20 {
				key := fmt.Sprintf("r%02d", i)
				if err := tx.PutRow("doc", key, []byte("v"), uint64(i+1), storage.Actor{}); err != nil {
					return err
				}
				if err := tx.PutIndexEntries("doc", key, []storage.IndexEntry{
					indexEntry("by_v", fmt.Sprintf("v%d", i%3), key),
					// An entry that does not end with the row key, so that the
					// read of row keys runs.
					{Index: "by_tag", Key: []byte{byte(i % 4)}},
				}); err != nil {
					return err
				}
			}
			return nil
		})).To(Succeed())
	}

	var stmts []tracedQuery
	rollback := errors.New("roll back")
	Expect(traced.WithWriteTx(ctx, "a", func(tx storage.WriteTx) error {
		Expect(tx.PutIndexEntries("doc", "r01", []storage.IndexEntry{indexEntry("by_v", "changed", "r01")})).To(Succeed())
		Expect(tx.PutIndexEntries("doc", "new", []storage.IndexEntry{indexEntry("by_v", "v", "new")})).To(Succeed())
		Expect(tx.DeleteIndexEntries("doc", "r05")).To(Succeed())
		t := tx.(*pgTx)
		Expect(t.resolveEntries()).To(Succeed())
		stmts = append(stmts, indexQueuedStmts(t.queued)...)
		tracer.reset()
		_, _, err := tx.PurgeEntityRows("doc", "r1", time.Time{}, 3)
		Expect(err).NotTo(HaveOccurred())
		stmts = append(stmts, tracer.taken()...)
		return rollback
	})).To(MatchError(rollback))
	Expect(traced.WithReadTx(ctx, "a", func(tx storage.ReadTx) error {
		tracer.reset()
		nothing := func([]byte, string) error { return nil }
		Expect(tx.ScanIndex("doc", "by_v", []byte("\x01v"), []byte("\x01w"), 5, storage.ScanAsc, nothing)).To(Succeed())
		Expect(tx.ScanIndex("doc", "by_tag", nil, nil, 0, storage.ScanDesc, nothing)).To(Succeed())
		Expect(tx.ScanIndexRanges("doc", "by_tag", []storage.IndexRange{
			{Lo: []byte{0}, Hi: []byte{1}}, {Lo: []byte{2}, Hi: []byte{3}},
		}, func(int, []byte, string) error { return nil })).To(Succeed())
		stmts = append(stmts, tracer.taken()...)
		return nil
	})).To(Succeed())
	scopes := map[indexKey]int32{}
	rows, err := store.pool.Query(ctx, `SELECT entity, idx, scope FROM index_scopes WHERE workspace_id = 'a'`)
	Expect(err).NotTo(HaveOccurred())
	for rows.Next() {
		var (
			k     indexKey
			scope int32
		)
		Expect(rows.Scan(&k.entity, &k.index, &scope)).To(Succeed())
		scopes[k] = scope
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	for _, stmt := range numberedDeletes(scopes) {
		stmts = append(stmts, tracedQuery{sql: stmt.sql, args: stmt.args})
	}

	// Each statement once, with the arguments of its first use.
	seen := map[string]bool{}
	var out []tracedQuery
	for _, s := range stmts {
		if !seen[s.sql] {
			seen[s.sql] = true
			out = append(out, s)
		}
	}
	// The two statements that write entries, the delete of PurgeEntityRows,
	// the three range reads, the read of row keys and the deletes of
	// DeleteWorkspace.
	Expect(len(out)).To(BeNumerically(">=", 9), "statements found: %d", len(out))
	return out
}

var (
	// indexPlanAlias matches the alias a plan puts before a column.
	indexPlanAlias = regexp.MustCompile(`\b[a-z]+(_\d+)?\.`)
	// indexPlanColumn matches the columns of the index tables in a condition.
	indexPlanColumn = regexp.MustCompile(`\b(wsn|ent|key|rid|scope|k|rk|ctid)\b`)
)

// indexAccess prepares s on conn and returns how its generic plan reaches
// index_rows and index_entries: one item per scan, in plan order, with the
// index and the columns the scan finds its tuples by. Partitions are folded
// into their parent.
//
// Two differences between plans are folded away, because both sides read the
// same tuples. A bitmap scan of one index reads what an index scan of it
// reads. And the DELETE of stale entries reads them by tuple id, or through
// the row-id index when the planner expects the table to be small; both reach
// only entries of the rows of the statement.
//
// A scan that finds its tuples by no column, or by the workspace number
// alone, reads more than the rows of the statement, and comes back marked
// WHOLE.
func indexAccess(ctx context.Context, conn *pgx.Conn, i int, s tracedQuery) string {
	GinkgoHelper()
	name := fmt.Sprintf("access%d", i)
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

	columns := func(node map[string]any, keys ...string) string {
		seen := map[string]bool{}
		var out []string
		for _, key := range keys {
			cond, _ := node[key].(string)
			for _, c := range indexPlanColumn.FindAllString(indexPlanAlias.ReplaceAllString(cond, ""), -1) {
				if !seen[c] {
					seen[c] = true
					out = append(out, c)
				}
			}
		}
		slices.Sort(out)
		return strings.Join(out, ",")
	}
	var items []string
	emit := func(rel, how, by string) {
		rel = partitionSuffix.ReplaceAllString(rel, "")
		if rel != "index_rows" && rel != "index_entries" {
			return
		}
		if by == "" || by == "wsn" {
			how = "WHOLE " + how
		}
		// The row-id index of index_entries has one column, and a scan by
		// tuple id stands in for it.
		if rel == "index_entries" && (by == "rid" || by == "ctid") {
			how, by = "by row id or tuple id", ""
		}
		item := rel + ": " + how
		if by != "" {
			item += " (" + by + ")"
		}
		// Appends over the partitions of one table repeat the scan.
		if len(items) == 0 || items[len(items)-1] != item {
			items = append(items, item)
		}
	}
	var walk func(node map[string]any, heap string)
	walk = func(node map[string]any, heap string) {
		kind, _ := node["Node Type"].(string)
		rel, _ := node["Relation Name"].(string)
		index, _ := node["Index Name"].(string)
		index = partitionSuffix.ReplaceAllString(index, "")
		switch kind {
		case "Bitmap Heap Scan":
			heap = rel
		case "Bitmap Index Scan":
			emit(heap, "index "+index, columns(node, "Index Cond"))
		case "Index Scan", "Index Only Scan":
			emit(rel, "index "+index, columns(node, "Index Cond"))
		case "Tid Scan", "Tid Range Scan":
			emit(rel, "tuple id", columns(node, "TID Cond"))
		case "Seq Scan":
			emit(rel, "seq scan", "")
		}
		children, _ := node["Plans"].([]any)
		for _, c := range children {
			if cm, ok := c.(map[string]any); ok {
				walk(cm, heap)
			}
		}
	}
	walk(plans[0].Plan, "")
	return strings.Join(items, "; ")
}

// indexTable matches the statements that read or write the index tables.
var indexTable = regexp.MustCompile(`\bindex_(rows|entries)\b`)

func indexQueuedStmts(queued []queuedStmt) []tracedQuery {
	var out []tracedQuery
	for _, q := range queued {
		if indexTable.MatchString(q.sql) {
			out = append(out, tracedQuery{sql: q.sql, args: q.args})
		}
	}
	return out
}

// indexTracer keeps the statements on the index tables that a pool sends.
type indexTracer struct {
	tripTracer
}

func (tr *indexTracer) keep(sql string, args []any) {
	if indexTable.MatchString(sql) {
		tr.stmts = append(tr.stmts, tracedQuery{sql: sql, args: args})
	}
}

func (tr *indexTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.keep(d.SQL, d.Args)
	return ctx
}

func (tr *indexTracer) TraceBatchStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceBatchStartData) context.Context {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	for _, q := range d.Batch.QueuedQueries {
		tr.keep(q.SQL, q.Arguments)
	}
	return ctx
}

var indexPartitionName = regexp.MustCompile(`^(index_rows|index_entries)_p\d+$`)

// indexPartitionsRead prepares s, runs it under EXPLAIN ANALYZE and returns,
// for each partitioned index table, the partitions a plan node ran on.
func indexPartitionsRead(ctx context.Context, conn *pgx.Conn, i int, s tracedQuery) map[string][]string {
	GinkgoHelper()
	name := fmt.Sprintf("indexprune%d", i)
	_, err := conn.Exec(ctx, `PREPARE `+name+` AS `+s.sql)
	Expect(err).NotTo(HaveOccurred(), firstLine(s.sql))
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
		if m := indexPartitionName.FindStringSubmatch(rel); m != nil && loops > 0 {
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
