// Package pgtest supplies the live Postgres the pgstore, pglisten, pgnotify and
// pgpresence suites run against. It is test-only support code, shared because
// every one of those packages needs the same database and none should own the
// others' container.
//
// A suite calls DSN() wherever it would otherwise hardcode a connection string.
// The database comes from one of two places:
//
//   - SYNCPLANE_TEST_PG_DSN, if set — an already-running Postgres (a compose
//     service, a CI service container, a local server). Nothing is torn down
//     but the schemas the suite created.
//   - a container started via testcontainers otherwise, once per test process.
//
// When neither is reachable the specs SKIP rather than fail. Postgres is not a
// build dependency of this repo — sqlitestore is the adapter that ships with no
// infrastructure — so a contributor without Docker still runs everything else.
// CI has Docker and therefore runs these for real.
//
// Each call returns a DSN scoped to a FRESH, EMPTY schema via search_path, and
// registers its teardown with Ginkgo. Schemas rather than databases because the
// conformance suite asks for an empty store more than a hundred times and
// CREATE DATABASE is far more expensive than CREATE SCHEMA — the isolation is
// the same for everything these adapters do, which is to say every table they
// create is unqualified and lands in the search_path's first entry.
package pgtest

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync"
	"sync/atomic"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// DSNEnv names the environment variable that points the suites at an existing
// Postgres instead of starting a container.
const DSNEnv = "SYNCPLANE_TEST_PG_DSN"

// image is the Postgres the container path runs. Pinned to a major version
// because the adapter's SQL is written against documented behavior, and an
// unpinned tag turns an upstream release into a surprise CI failure.
//
// Deliberately the Debian build and NOT -alpine. The conformance suite's whole
// collation argument — that key columns need COLLATE "C" or paged scans drop
// rows — is only observable on a libc that actually collates: Alpine's musl
// sorts en_US.utf8 byte-for-byte, so an adapter that forgot the collation
// passes there and breaks on the glibc Postgres a deployment really runs. The
// image is the difference between the specs having teeth and only looking like
// they do.
const image = "postgres:17"

var (
	startOnce sync.Once
	baseDSN   string
	startErr  error
	container *postgres.PostgresContainer

	schemaSeq atomic.Int64
)

// base returns the connection string for the shared server, starting the
// container on first use.
func base() (string, error) {
	startOnce.Do(func() {
		if dsn := os.Getenv(DSNEnv); dsn != "" {
			baseDSN = dsn
			return
		}
		ctx := context.Background()
		container, startErr = postgres.Run(ctx, image,
			postgres.WithDatabase("syncplane"),
			postgres.WithUsername("syncplane"),
			postgres.WithPassword("syncplane"),
			postgres.BasicWaitStrategies(),
		)
		if startErr != nil {
			startErr = fmt.Errorf("start %s: %w", image, startErr)
			return
		}
		baseDSN, startErr = container.ConnectionString(ctx, "sslmode=disable")
	})
	return baseDSN, startErr
}

// DSN returns a connection string for a fresh, empty schema on the shared
// server, and registers the schema's teardown with the running spec. It SKIPS
// the spec when no Postgres is reachable.
func DSN() string {
	GinkgoHelper()
	server, err := base()
	if err != nil {
		Skip(fmt.Sprintf("no Postgres available (set %s to use an existing server): %v", DSNEnv, err))
	}

	ctx := context.Background()
	name := fmt.Sprintf("sp_test_%d", schemaSeq.Add(1))
	exec := func(sql string) {
		GinkgoHelper()
		conn, err := pgx.Connect(ctx, server)
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = conn.Close(ctx) }()
		_, err = conn.Exec(ctx, sql)
		Expect(err).NotTo(HaveOccurred())
	}
	exec(`CREATE SCHEMA "` + name + `"`)
	// Runs after the spec's own store.Close(), which Ginkgo registered later
	// and therefore unwinds first — a DROP would otherwise block on the pool's
	// open connections.
	DeferCleanup(func() { exec(`DROP SCHEMA "` + name + `" CASCADE`) })

	return withSearchPath(server, name)
}

// ServerDSN returns a connection string for the shared server itself, with no
// schema of its own. It is for suites that need a database but no tables — the
// notifier, whose whole state is LISTEN/NOTIFY channels — and it SKIPS the spec
// when no Postgres is reachable, exactly as DSN does.
func ServerDSN() string {
	GinkgoHelper()
	server, err := base()
	if err != nil {
		Skip(fmt.Sprintf("no Postgres available (set %s to use an existing server): %v", DSNEnv, err))
	}
	return server
}

// UniqueName returns a fresh identifier with the given prefix, safe to use as a
// Postgres channel or schema name. Suites use it to keep specs that share one
// database from hearing each other.
func UniqueName(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, schemaSeq.Add(1))
}

// withSearchPath points a DSN at one schema. The adapters create and address
// every table unqualified, so this is what makes a schema a whole private
// store rather than a namespace they would have to know about.
func withSearchPath(dsn, schema string) string {
	u, err := url.Parse(dsn)
	Expect(err).NotTo(HaveOccurred(), "pgtest: DSN must be a URI (postgres://...)")
	q := u.Query()
	q.Set("options", "-c search_path="+schema)
	u.RawQuery = q.Encode()
	return u.String()
}

// Shutdown terminates the container started by this process, if any. Suites
// call it from AfterSuite. Testcontainers' reaper would eventually collect the
// container anyway; doing it explicitly keeps a developer's Docker clean
// between runs rather than at the reaper's convenience.
func Shutdown() {
	if container == nil {
		return
	}
	Expect(testcontainers.TerminateContainer(container)).To(Succeed())
	container = nil
}
