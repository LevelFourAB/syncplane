package auth_test

import (
	"context"
	"path/filepath"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/internal/auth"
	"github.com/levelfourab/syncplane/internal/pgtest"
)

func TestAuth(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Auth Store Suite")
}

var _ = AfterSuite(pgtest.Shutdown)

// Both engines run the same specs: the SQL is written once and the backends
// are only allowed to differ in what the specs cannot see.
var _ = describeStore("sqlite", func() *auth.Store {
	store, err := auth.OpenSQLite(filepath.Join(GinkgoT().TempDir(), "auth.db"))
	Expect(err).NotTo(HaveOccurred())
	return store
})

var _ = describeStore("postgres", func() *auth.Store {
	store, err := auth.OpenPostgres(context.Background(), pgtest.DSN())
	Expect(err).NotTo(HaveOccurred())
	return store
})

var _ = describeSessions("sqlite", func() *auth.Store {
	store, err := auth.OpenSQLite(filepath.Join(GinkgoT().TempDir(), "auth.db"))
	Expect(err).NotTo(HaveOccurred())
	return store
})

var _ = describeSessions("postgres", func() *auth.Store {
	store, err := auth.OpenPostgres(context.Background(), pgtest.DSN())
	Expect(err).NotTo(HaveOccurred())
	return store
})
