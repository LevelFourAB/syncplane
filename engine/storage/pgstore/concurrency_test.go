package pgstore_test

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/internal/pgtest"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/pgstore"
)

// Two pgstore handles over one database stand in for two server nodes. The
// push path reads before it writes (the watermark check, the stored row a
// PATCH merges into), so the workspace lock has to cover the read as well as
// the write, on every node at once.
var _ = Describe("Concurrent writers to one workspace on two nodes", func() {
	It("loses no read-then-write update and leaves the sequence gap-free", func() {
		ctx := context.Background()
		dsn := pgtest.DSN()
		nodes := make([]*pgstore.Store, 2)
		for i := range nodes {
			s, err := pgstore.Open(ctx, dsn)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { Expect(s.Close()).To(Succeed()) })
			nodes[i] = s
		}

		const workers, perWorker = 6, 15
		total := len(nodes) * workers * perWorker

		// Given two nodes, each running several writers against workspace
		// "ws1", When every writer reads a counter row, increments it and also
		// writes one row of its own under a fresh sequence, Then the counter
		// ends at the number of writes, and the sequences are exactly 1..N.
		// Must NOT: lose an increment (two writers reading the same value) or
		// hand out one sequence twice.
		var wg sync.WaitGroup
		errs := make(chan error, total)
		for n, node := range nodes {
			for w := range workers {
				wg.Add(1)
				go func() {
					defer GinkgoRecover()
					defer wg.Done()
					for i := range perWorker {
						errs <- node.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
							count := 0
							row, err := tx.GetRow("counter", "c")
							switch {
							case err == nil:
								if count, err = strconv.Atoi(string(row.Value)); err != nil {
									return err
								}
							case err != storage.ErrNotFound:
								return err
							}
							mid, err := tx.ClientLastMutationID("shared")
							if err != nil {
								return err
							}
							seq, err := tx.NextSeq()
							if err != nil {
								return err
							}
							if err := tx.PutRow("counter", "c", []byte(strconv.Itoa(count+1)), seq, storage.Actor{}); err != nil {
								return err
							}
							key := fmt.Sprintf("n%d-w%d-%d", n, w, i)
							if err := tx.PutRow("item", key, []byte(key), seq, storage.Actor{}); err != nil {
								return err
							}
							return tx.SetClientLastMutationID("shared", mid+1)
						})
					}
				}()
			}
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			Expect(err).NotTo(HaveOccurred())
		}

		Expect(nodes[0].WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
			row, err := tx.GetRow("counter", "c")
			Expect(err).NotTo(HaveOccurred())
			Expect(string(row.Value)).To(Equal(strconv.Itoa(total)), "no increment may be lost")
			Expect(tx.ClientLastMutationID("shared")).To(BeEquivalentTo(total))
			Expect(tx.CurrentSeq()).To(BeEquivalentTo(total))

			seen := map[uint64]bool{}
			Expect(tx.ScanEntity("item", func(r storage.Row) error {
				Expect(seen[r.Version]).To(BeFalse(), "sequence %d handed out twice", r.Version)
				seen[r.Version] = true
				return nil
			})).To(Succeed())
			Expect(seen).To(HaveLen(total))
			for v := uint64(1); v <= uint64(total); v++ {
				Expect(seen).To(HaveKey(v))
			}
			return nil
		})).To(Succeed())
	})
})
