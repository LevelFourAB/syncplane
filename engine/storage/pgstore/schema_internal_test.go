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
		Expect(strings.Count(ddl, "PARTITION BY HASH (namespace, workspace_id)")).
			To(Equal(len(partitionedTables)))
		for _, table := range partitionedTables {
			Expect(strings.Count(ddl, "PARTITION OF "+table+" ")).To(Equal(4),
				"%s must have one partition per remainder", table)
		}
		Expect(ddl).To(ContainSubstring(
			"CREATE TABLE rows_p3 PARTITION OF rows FOR VALUES WITH (MODULUS 4, REMAINDER 3);"))
	})

	It("pads partition names to the width of the largest remainder", func() {
		ddl := baselineSchema(layout{partitions: 16})
		Expect(ddl).To(ContainSubstring("CREATE TABLE rows_p00 "))
		Expect(ddl).To(ContainSubstring("CREATE TABLE rows_p15 "))
	})
})

var _ = Describe("Migration scripts", func() {
	It("creates the ledger in the first script only, and records every step", func() {
		Expect(migrationSQL(1, layout{})).To(ContainSubstring("CREATE TABLE IF NOT EXISTS schema_migrations"))
		Expect(migrationSQL(2, layout{})).NotTo(ContainSubstring("CREATE TABLE IF NOT EXISTS schema_migrations"))
		for version := 1; version <= len(migrations); version++ {
			want := "INSERT INTO schema_migrations (version) VALUES (" + strconv.Itoa(version) + ");"
			Expect(migrationSQL(version, layout{})).To(ContainSubstring(want))
		}
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
