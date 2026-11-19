package pgstore

import (
	"context"
	"fmt"
	"slices"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/internal/pgtest"
	"github.com/levelfourab/syncplane/engine/storage"
)

// rows holds the workspace number in place of the workspace id, and a store
// caches the number of each workspace it reads (see rows.go). These specs pin
// what makes that safe across nodes: a number from the cache is checked in the
// snapshot of the read that binds it, and a write never binds one.
var _ = Describe("Postgres row workspace numbers", func() {
	ctx := context.Background()
	const ws = "ws"

	for _, layout := range indexLayouts {
		Describe(layout.name, func() {
			var a, b *Store
			BeforeEach(func() {
				dsn := pgtest.DSN()
				var err error
				a, err = Open(ctx, dsn, layout.opts...)
				Expect(err).NotTo(HaveOccurred())
				DeferCleanup(func() { Expect(a.Close()).To(Succeed()) })
				b, err = Open(ctx, dsn)
				Expect(err).NotTo(HaveOccurred())
				DeferCleanup(func() { Expect(b.Close()).To(Succeed()) })
			})

			put := func(store *Store, key string, version uint64) {
				GinkgoHelper()
				Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
					return tx.PutRow("doc", key, []byte(key), version, storage.Actor{})
				})).To(Succeed())
			}
			// changed reads every row of the workspace through each read of
			// rows, and fails unless they agree.
			changed := func(store *Store) []string {
				GinkgoHelper()
				var keys []string
				Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
					Expect(tx.ScanChanges(0, "", "", 0, func(r storage.Row) error {
						keys = append(keys, r.Key)
						return nil
					})).To(Succeed())
					var scanned []string
					Expect(tx.ScanEntity("doc", func(r storage.Row) error {
						scanned = append(scanned, r.Key)
						return nil
					})).To(Succeed())
					Expect(scanned).To(ConsistOf(keys))
					return nil
				})).To(Succeed())
				Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
					var got []string
					Expect(tx.GetRows("doc", append([]string{"old", "new"}, keys...), func(r storage.Row) error {
						got = append(got, r.Key)
						return nil
					})).To(Succeed())
					Expect(slices.Compact(slices.Sorted(slices.Values(got)))).To(ConsistOf(keys))
					return nil
				})).To(Succeed())
				Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
					counts, err := tx.(readOnlyTx).CountRows([]string{"doc"})
					Expect(err).NotTo(HaveOccurred())
					Expect(counts).To(Equal([]uint64{uint64(len(keys))}))
					return nil
				})).To(Succeed())
				for _, key := range append([]string{"old", "new"}, keys...) {
					Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
						_, err := tx.GetRow("doc", key)
						if slices.Contains(keys, key) {
							Expect(err).NotTo(HaveOccurred())
						} else {
							Expect(err).To(MatchError(storage.ErrNotFound))
						}
						return nil
					})).To(Succeed())
				}
				return keys
			}
			number := func(store *Store) (int32, bool) {
				GinkgoHelper()
				return store.numbers.get(ws, "", "")
			}

			It("gives a rewritten workspace a new number and reads none of its old rows", func() {
				put(a, "old", 1)
				Expect(changed(a)).To(Equal([]string{"old"}))
				first, ok := number(a)
				Expect(ok).To(BeTrue(), "a read caches the number")

				Expect(b.DeleteWorkspace(ctx, ws)).To(Succeed())
				Expect(changed(a)).To(BeEmpty(), "a number in the cache of a deleted workspace")
				_, ok = number(a)
				Expect(ok).To(BeFalse(), "the read drops the number the workspace no longer has")

				Expect(b.RestoreWorkspace(ctx, ws)).To(Succeed())
				put(a, "x", 1)
				Expect(changed(a)).To(Equal([]string{"x"}), "a write after the delete")
				x, _ := number(a)
				Expect(x).NotTo(Equal(first), "no number is given twice")

				Expect(b.DeleteWorkspace(ctx, ws)).To(Succeed())
				Expect(b.RestoreWorkspace(ctx, ws)).To(Succeed())
				put(b, "new", 1)
				// a holds the number of the generation b deleted.
				Expect(changed(a)).To(Equal([]string{"new"}), "a number in the cache of an earlier generation")
				second, ok := number(a)
				Expect(ok).To(BeTrue())
				Expect(second).NotTo(BeElementOf(first, x), "no number is given twice")
				Expect(changed(b)).To(Equal([]string{"new"}))
			})

			It("writes under the number the workspace has when the cache holds an older one", func() {
				put(a, "old", 1)
				Expect(changed(a)).To(Equal([]string{"old"}))
				Expect(b.DeleteWorkspace(ctx, ws)).To(Succeed())
				Expect(b.RestoreWorkspace(ctx, ws)).To(Succeed())
				put(b, "new", 1)

				// A write that reads first, and one that only writes, through a
				// store whose cache holds the number of the deleted generation.
				Expect(a.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
					_, err := tx.GetRow("doc", "old")
					Expect(err).To(MatchError(storage.ErrNotFound))
					return tx.PutRow("doc", "old", []byte("again"), 2, storage.Actor{})
				})).To(Succeed())
				a.numbers.put(ws, "", "", 1<<30)
				put(a, "blind", 3)
				Expect(b.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
					var keys []string
					Expect(tx.ScanChanges(0, "", "", 0, func(r storage.Row) error {
						keys = append(keys, r.Key)
						return nil
					})).To(Succeed())
					Expect(keys).To(Equal([]string{"new", "old", "blind"}))
					return nil
				})).To(Succeed())
			})

			It("rewinds the number a write gave inside a savepoint", func() {
				Expect(a.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
					Expect(tx.InSavepoint(func() error {
						Expect(tx.PutRow("doc", "gone", []byte("v"), 1, storage.Actor{})).To(Succeed())
						return fmt.Errorf("rejected")
					})).To(MatchError("rejected"))
					return tx.PutRow("doc", "kept", []byte("v"), 2, storage.Actor{})
				})).To(Succeed())
				Expect(changed(b)).To(Equal([]string{"kept"}))
			})
		})
	}

	It("locks a number of relations in a read of rows that does not grow with the partitions", func() {
		// The relation locks a read transaction holds after each read of
		// rows: the first, when the store's cache does not hold the number of
		// the workspace, and a later one, which binds the number of the
		// cache.
		measure := func(partitions int) []int {
			GinkgoHelper()
			dsn := pgtest.DSN()
			seed, err := Open(ctx, dsn, WithPartitions(partitions))
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { Expect(seed.Close()).To(Succeed()) })
			Expect(seed.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
				return tx.PutRow("doc", "k", []byte("v"), 1, storage.Actor{})
			})).To(Succeed())

			reads := []func(tx storage.ReadTx) error{
				func(tx storage.ReadTx) error { _, err := tx.GetRow("doc", "k"); return err },
				func(tx storage.ReadTx) error {
					return tx.GetRows("doc", []string{"k"}, func(storage.Row) error { return nil })
				},
				func(tx storage.ReadTx) error {
					return tx.ScanChanges(0, "", "", 10, func(storage.Row) error { return nil })
				},
				func(tx storage.ReadTx) error {
					return tx.ScanEntityByVersionDesc("doc", 10, func(storage.Row) error { return nil })
				},
			}
			var out []int
			for _, read := range reads {
				store, err := Open(ctx, dsn, WithMaxConns(1))
				Expect(err).NotTo(HaveOccurred())
				DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
				var locks int
				// The first run fills the cache, and the plan cache sees the
				// statement more than five times, after which it may choose
				// a generic plan.
				for i := range 8 {
					Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
						if err := read(tx); err != nil {
							return err
						}
						return tx.(readOnlyTx).ReadTx.(*pgTx).queryRow(`SELECT count(*)::int4 FROM pg_locks
							WHERE pid = pg_backend_pid() AND locktype = 'relation'`).Scan(&locks)
					})).To(Succeed())
					if i == 0 {
						out = append(out, locks)
					}
				}
				out = append(out, locks)
			}
			fmt.Fprintf(GinkgoWriter, "%d partitions: %v relation locks\n", partitions, out)
			return out
		}
		Expect(measure(32)).To(Equal(measure(8)))
	})
})
