package pgstore_test

import (
	"context"
	"fmt"
	"log"

	"github.com/levelfourab/syncplane/engine/storage/pgstore"
)

// The DSN both examples migrate. Neither runs — they have no Output comment —
// so no Postgres is needed to build them.
const exampleDSN = "postgres://syncplane@db.internal:5432/syncplane"

// A deploy step owns the schema: it migrates once, choosing the table layout,
// and the nodes behind it never apply DDL of their own.
func ExampleMigrate() {
	ctx := context.Background()

	if err := pgstore.Migrate(ctx, exampleDSN, pgstore.WithPartitions(32)); err != nil {
		log.Fatal(err)
	}

	// On every node, at boot, and under a role with no privilege to change the
	// schema. WithoutMigrations is what makes the step above authoritative:
	// this fails rather than migrating if the step has not run.
	store, err := pgstore.Open(ctx, exampleDSN, pgstore.WithoutMigrations())
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
}

// Where the DDL is reviewed or applied by a migration tool, the same steps come
// out as SQL. Each script ends with its own row in schema_migrations, so a
// database they were applied to needs no stamping before Open accepts it.
func ExampleStatements() {
	migrations, err := pgstore.Statements(pgstore.WithPartitions(32))
	if err != nil {
		log.Fatal(err)
	}
	for _, m := range migrations {
		fmt.Printf("-- migration %d\n%s\n", m.Version, m.SQL)
	}
}
