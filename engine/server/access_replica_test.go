package server_test

import (
	"context"
	"database/sql"
	"path/filepath"
	gosync "sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

// laggingReplicaStore is a SQLite primary with a read replica that holds a
// copy of the primary taken at one moment and never catches up. Reads go to
// the replica only while useReplica is set, and only when the replica is at
// or past the floor the server names, as a real replica reader routes them.
type laggingReplicaStore struct {
	storage.Adapter
	dir string

	mu         gosync.Mutex
	replica    storage.Adapter
	useReplica bool
}

func (s *laggingReplicaStore) Unwrap() storage.Adapter { return s.Adapter }

func (s *laggingReplicaStore) ReplicaReader() storage.ReplicaReader { return s }

// snapshot copies the primary into the replica as it is now.
func (s *laggingReplicaStore) snapshot(primaryPath string) {
	GinkgoHelper()
	db, err := sql.Open("sqlite", primaryPath)
	Expect(err).NotTo(HaveOccurred())
	defer db.Close()
	path := filepath.Join(s.dir, "replica.db")
	_, err = db.Exec(`VACUUM INTO ?`, path)
	Expect(err).NotTo(HaveOccurred())
	replica, err := sqlitestore.Open(path)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { Expect(replica.Close()).To(Succeed()) })
	s.mu.Lock()
	s.replica = replica
	s.mu.Unlock()
}

func (s *laggingReplicaStore) route(on bool) {
	s.mu.Lock()
	s.useReplica = on
	s.mu.Unlock()
}

func (s *laggingReplicaStore) WithReadTxMinSeq(ctx context.Context, ws string, minSeq uint64, fn func(storage.ReadTx) error) (storage.ReadRoute, error) {
	s.mu.Lock()
	replica, use := s.replica, s.useReplica
	s.mu.Unlock()
	if use && replica != nil {
		var current uint64
		if err := replica.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			var err error
			current, err = tx.CurrentSeq()
			return err
		}); err != nil {
			return storage.ReadRouteFallbackError, err
		}
		if current >= minSeq {
			return storage.ReadRouteReplica, replica.WithReadTx(ctx, ws, fn)
		}
	}
	return storage.ReadRouteFallbackStale, s.Adapter.WithReadTx(ctx, ws, fn)
}

func (s *laggingReplicaStore) WithReadTxAnchored(ctx context.Context, ws string, fn func(storage.ReadTx) error) (storage.ReadRoute, error) {
	return storage.ReadRouteFallbackStale, s.Adapter.WithReadTx(ctx, ws, fn)
}

var _ = Describe("Access model sync through a lagging read replica", func() {
	It("removes a comment a replica sent from before its task moved out of the caller's project", func() {
		// Given comments below tasks below projects on a primary with a read
		// replica, where guest reads p1, which holds t1, and has synced
		dir := GinkgoT().TempDir()
		primaryPath := filepath.Join(dir, "primary.db")
		primary, err := sqlitestore.Open(primaryPath)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(primary.Close()).To(Succeed()) })
		store := &laggingReplicaStore{Adapter: primary, dir: dir}
		f := newDeepFixtureOn(store)
		f.seedOrg()
		f.seedComments()
		Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
		guest := newAccessReplica(f, "guest")
		guest.sync()

		// And comment c-new is written on t1, the replica copies the primary,
		// and then t1 moves to p2, which guest does not read
		f.seed(&accessv1.Comment{Id: "c-new", Task: task("t1"), Body: "new"})
		store.snapshot(primaryPath)
		Expect(f.srv.Mutate(server.Trusted(f.ctx), ws, func(tx *server.EntityTx) error {
			return tx.Patch(&accessv1.Task{Id: "t1", Project: project("p2")}, []string{"project"}, nil)
		})).To(Succeed())

		// When guest pulls from the primary, then once from the replica,
		// then from the primary again
		Expect(guest.pullOnce(0).GetReset_()).To(BeFalse())
		store.route(true)
		Expect(guest.pullOnce(0).GetReset_()).To(BeFalse())
		store.route(false)
		guest.sync()

		// Then guest holds neither t1 nor its comments, which sit in p2 now
		Expect(guest.view()).NotTo(ContainElement("comment/c-new"))
		Expect(guest.view()).To(Equal(f.pull("guest")))
	})
})
