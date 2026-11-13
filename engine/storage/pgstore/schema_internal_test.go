package pgstore

import (
	"context"
	"strconv"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/levelfourab/syncplane/engine/internal/pgtest"
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
