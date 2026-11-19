package pgstore

import (
	"context"
	"strconv"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/levelfourab/syncplane/engine/internal/pgtest"
	"github.com/levelfourab/syncplane/engine/storage"
)

// These specs reach inside the package for the two things no caller can see:
// the DDL a layout generates, which needs no database at all, and the
// detection that makes an established database keep the layout it has.

var _ = Describe("Baseline schema", func() {
	It("leaves every table whole when no layout asks for partitions", func() {
		Expect(baselineSchema(layout{})).NotTo(ContainSubstring("PARTITION"))
	})

	It("partitions the workspace-scoped tables and nothing else", func() {
		ddl := baselineSchema(layout{partitions: 4})
		Expect(strings.Count(ddl, "PARTITION BY HASH (workspace_id)")).
			To(Equal(len(baselinePartitionedTables)))
		for _, table := range baselinePartitionedTables {
			Expect(strings.Count(ddl, "PARTITION OF "+table+" ")).To(Equal(4),
				"%s must have one partition per remainder", table)
		}
		Expect(ddl).To(ContainSubstring(
			"CREATE TABLE rows_p3 PARTITION OF rows FOR VALUES WITH (MODULUS 4, REMAINDER 3);"))
		// The index tables hold the workspace number in place of the id.
		Expect(strings.Count(ddl, "PARTITION BY HASH (wsn)")).
			To(Equal(len(baselineNumberPartitionedTables)))
		for _, table := range baselineNumberPartitionedTables {
			Expect(strings.Count(ddl, "PARTITION OF "+table+" ")).To(Equal(4),
				"%s must have one partition per remainder", table)
		}
	})

	It("pads partition names to the width of the largest remainder", func() {
		ddl := baselineSchema(layout{partitions: 16})
		Expect(ddl).To(ContainSubstring("CREATE TABLE rows_p00 "))
		Expect(ddl).To(ContainSubstring("CREATE TABLE rows_p15 "))
	})
})

var _ = Describe("Migration 5", func() {
	It("leaves the two tables whole when the layout is unpartitioned", func() {
		Expect(internedNamesSchema(layout{})).NotTo(ContainSubstring("PARTITION"))
	})

	It("partitions the two tables by the workspace number with the modulus it found", func() {
		ddl := internedNamesSchema(layout{partitions: 3})
		Expect(strings.Count(ddl, "PARTITION BY HASH (wsn)")).To(Equal(2))
		for _, table := range []string{"field_deltas", "consumer_shadows"} {
			Expect(ddl).To(ContainSubstring(
				"CREATE TABLE " + table + "_v5_p2 PARTITION OF " + table + "_v5 FOR VALUES WITH (MODULUS 3, REMAINDER 2);"))
			Expect(ddl).To(ContainSubstring("ALTER TABLE " + table + "_v5_p2 RENAME TO " + table + "_p2;"))
		}
	})
})

var _ = Describe("Migration 7", func() {
	ctx := context.Background()

	for _, layout := range indexLayouts {
		It("marks the rows that hold an entry with the row id, and indexes only them by row id ("+layout.name+")", func() {
			dsn := pgtest.DSN()
			// A database at migration 6, which holds the rows r1 and r2 of
			// one workspace. Each has an entry that ends with its row key,
			// and r1 also has one with its row id.
			full := migrations
			DeferCleanup(func() { migrations = full })
			migrations = full[:6]
			old, err := Open(ctx, dsn, layout.opts...)
			Expect(err).NotTo(HaveOccurred())
			scopes := map[string]int32{}
			rows, err := old.pool.Query(ctx, `INSERT INTO index_scopes (workspace_id, entity, idx, scope)
				SELECT 'ws', u.entity, u.idx, nextval('posting_scope_ids')::int4
				FROM unnest(ARRAY['', 'doc', 'doc', 'doc'], ARRAY['', '', 'by_v', 'by_tag']) AS u(entity, idx)
				RETURNING entity || '/' || idx, scope`)
			Expect(err).NotTo(HaveOccurred())
			for rows.Next() {
				var (
					name  string
					scope int32
				)
				Expect(rows.Scan(&name, &scope)).To(Succeed())
				scopes[name] = scope
			}
			Expect(rows.Err()).NotTo(HaveOccurred())
			wsn, ent := scopes["/"], scopes["doc/"]
			rids := map[string]int64{}
			for _, key := range []string{"r1", "r2"} {
				var rid int64
				Expect(old.pool.QueryRow(ctx, `INSERT INTO index_rows (wsn, ent, rid, key)
					VALUES ($1, $2, nextval('index_row_ids'), $3) RETURNING rid`, wsn, ent, key).Scan(&rid)).To(Succeed())
				rids[key] = rid
			}
			_, err = old.pool.Exec(ctx, `INSERT INTO index_entries (wsn, scope, rk, rid, k) VALUES
				($1, $2, -2, $4, $6), ($1, $3, $4, $4, 'x'), ($1, $2, -2, $5, $7)`,
				wsn, scopes["doc/by_v"], scopes["doc/by_tag"], rids["r1"], rids["r2"],
				[]byte("\x01a"+indexEntryEnd+"r1"+indexEntryEnd), []byte("\x01b"+indexEntryEnd+"r2"+indexEntryEnd))
			Expect(err).NotTo(HaveOccurred())
			Expect(old.Close()).To(Succeed())

			migrations = full
			store, err := Open(ctx, dsn)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
			Expect(store.partitioned).To(Equal(len(layout.opts) > 0))

			marks := func() map[string]bool {
				GinkgoHelper()
				out := map[string]bool{}
				rows, err := store.pool.Query(ctx, `SELECT key, by_id FROM index_rows`)
				Expect(err).NotTo(HaveOccurred())
				defer rows.Close()
				for rows.Next() {
					var (
						key  string
						mark bool
					)
					Expect(rows.Scan(&key, &mark)).To(Succeed())
					out[key] = mark
				}
				Expect(rows.Err()).NotTo(HaveOccurred())
				return out
			}
			Expect(marks()).To(Equal(map[string]bool{"r1": true, "r2": false}))
			var def string
			Expect(store.pool.QueryRow(ctx, `SELECT indexdef FROM pg_indexes
				WHERE schemaname = current_schema() AND indexname = 'index_rows_by_rid'`).Scan(&def)).To(Succeed())
			Expect(def).To(HaveSuffix("(rid, wsn) WHERE by_id"))
			var additive bool
			Expect(store.pool.QueryRow(ctx, `SELECT additive FROM schema_migrations WHERE version = 7`).Scan(&additive)).To(Succeed())
			Expect(additive).To(BeFalse())

			read := func(index string) []string {
				GinkgoHelper()
				var out []string
				Expect(store.WithReadTx(ctx, "ws", func(tx storage.ReadTx) error {
					out = rowKeys(tx, "doc", index)
					return nil
				})).To(Succeed())
				return out
			}
			Expect(read("by_tag")).To(Equal([]string{"r1"}))
			Expect(read("by_v")).To(Equal([]string{"r1", "r2"}))

			// The rows the step marked change and go as rows written after
			// it do.
			Expect(store.WithWriteTx(ctx, "ws", func(tx storage.WriteTx) error {
				Expect(tx.PutIndexEntries("doc", "r2", docEntries("b", "r2"))).To(Succeed())
				return tx.DeleteIndexEntries("doc", "r1")
			})).To(Succeed())
			Expect(marks()).To(Equal(map[string]bool{"r2": true}))
			Expect(read("by_tag")).To(Equal([]string{"r2"}))
			Expect(read("by_v")).To(Equal([]string{"r2"}))
		})
	}
})

var _ = Describe("Migration scripts", func() {
	It("creates the ledger in the first script only, and records every step", func() {
		Expect(migrationSQL(1, layout{})).To(ContainSubstring("CREATE TABLE IF NOT EXISTS schema_migrations"))
		for version := 1; version <= len(migrations); version++ {
			want := "INSERT INTO schema_migrations (version, additive) VALUES (" + strconv.Itoa(version) + ", "
			Expect(migrationSQL(version, layout{})).To(ContainSubstring(want))
		}
	})

	It("records each step's own compatibility", func() {
		// The value has to come from the step rather than from a default, or
		// the ledger cannot answer for a step this binary never carried.
		stub([]migration{
			{additive: true, sql: fixed(`SELECT 1;`)},
			{additive: false, sql: fixed(`SELECT 2;`)},
		})
		Expect(migrationSQL(1, layout{})).To(ContainSubstring("VALUES (1, true);"))
		Expect(migrationSQL(2, layout{})).To(ContainSubstring("VALUES (2, false);"))
	})

	It("keeps every shipped step additive but the ones decided otherwise", func() {
		// Not a law — a later step may need to drop something — but a step
		// that turns this red should be a decision, since it strands every
		// older binary in the fleet until the roll reaches it. The decided
		// ones are listed here.
		decided := map[int]bool{
			5: true, // field_deltas and consumer_shadows keyed by int ids
			7: true, // index_rows_by_rid holds only the rows marked by_id
			8: true, // rows keyed by the workspace number
		}
		for version, step := range migrations {
			Expect(step.additive).To(Equal(!decided[version+1]), "migration %d", version+1)
		}
	})

})

// stub replaces the package's migration list for one spec. CanServe and the
// ledger's compatibility column both answer from steps this binary carries, and
// every shipped step is additive — so the interesting cases have to be built.
func stub(list []migration) {
	GinkgoHelper()
	original := migrations
	migrations = list
	DeferCleanup(func() { migrations = original })
}

var _ = Describe("CanServe", func() {
	BeforeEach(func() {
		stub([]migration{
			{additive: true, sql: fixed(`SELECT 1;`)},  // 1
			{additive: true, sql: fixed(`SELECT 2;`)},  // 2
			{additive: false, sql: fixed(`SELECT 3;`)}, // 3, the breaking one
			{additive: true, sql: fixed(`SELECT 4;`)},  // 4
		})
	})

	It("clears a run of additive steps", func() {
		Expect(CanServe(1, 2)).To(BeTrue())
		Expect(CanServe(3, 4)).To(BeTrue())
	})

	It("refuses across a step that removed something", func() {
		Expect(CanServe(2, 3)).To(BeFalse())
		Expect(CanServe(1, 4)).To(BeFalse())
	})

	It("holds a binary at or above the schema", func() {
		Expect(CanServe(4, 4)).To(BeTrue())
		Expect(CanServe(4, 2)).To(BeTrue())
	})

	It("cannot answer for a schema it does not know", func() {
		// The whole reason the ledger carries the column: this is the question
		// an old binary asks at boot, and its own list cannot answer it.
		Expect(CanServe(4, 5)).To(BeFalse())
	})
})

var _ = Describe("Layout detection", func() {
	ctx := context.Background()

	// The layout a later migration would be generated for. Nothing else
	// observes it today, which is exactly why it is asserted here: a step
	// written for both layouts must be handed the one in front of it.
	detect := func(dsn string) layout {
		GinkgoHelper()
		pool, err := pgxpool.New(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(pool.Close)
		conn, err := pool.Acquire(ctx)
		Expect(err).NotTo(HaveOccurred())
		defer conn.Release()
		l, err := detectLayout(ctx, conn)
		Expect(err).NotTo(HaveOccurred())
		return l
	}

	It("reports no partitions before anything is migrated", func() {
		Expect(detect(pgtest.DSN())).To(Equal(layout{}))
	})

	It("reads back the modulus the database was created with", func() {
		dsn := pgtest.DSN()
		store, err := Open(ctx, dsn, WithPartitions(4))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		Expect(detect(dsn)).To(Equal(layout{partitions: 4}))
	})

	It("reports no partitions for an unpartitioned database", func() {
		dsn := pgtest.DSN()
		store, err := Open(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		Expect(detect(dsn)).To(Equal(layout{}))
	})
})
