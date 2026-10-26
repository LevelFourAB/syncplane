// Package storagetest provides a shared Ginkgo behavior suite every
// storage.Adapter implementation must pass. Both shipped adapters register it
// verbatim, and a new one does the same:
//
//	var _ = storagetest.Conformance("postgres", func() storage.Adapter { ... })
package storagetest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/internal/naming"
	"github.com/levelfourab/syncplane/engine/storage"
)

// Conformance registers the adapter behavior specs. newAdapter is called
// per spec and must return a fresh, empty store; it is closed afterwards.
func Conformance(name string, newAdapter func() storage.Adapter) bool {
	return Describe("storage.Adapter conformance: "+name, func() {
		var (
			ctx   context.Context
			store storage.Adapter
		)

		BeforeEach(func() {
			ctx = context.Background()
			store = newAdapter()
			DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		})

		write := func(ws string, fn func(storage.WriteTx)) {
			GinkgoHelper()
			Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
				fn(tx)
				return nil
			})).To(Succeed())
		}
		read := func(ws string, fn func(storage.ReadTx)) {
			GinkgoHelper()
			Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
				fn(tx)
				return nil
			})).To(Succeed())
		}

		It("starts empty", func() {
			read("ws1", func(tx storage.ReadTx) {
				seq, err := tx.CurrentSeq()
				Expect(err).NotTo(HaveOccurred())
				Expect(seq).To(BeZero())
				horizon, err := tx.TombstoneHorizon()
				Expect(err).NotTo(HaveOccurred())
				Expect(horizon).To(BeZero())
				epoch, err := tx.StoreEpoch()
				Expect(err).NotTo(HaveOccurred())
				Expect(epoch).To(BeZero(), "an unwritten workspace has no store epoch")
				_, err = tx.GetRow("widget", "k")
				Expect(err).To(MatchError(storage.ErrNotFound))
				id, err := tx.ClientLastMutationID("c1")
				Expect(err).NotTo(HaveOccurred())
				Expect(id).To(BeZero())
			})
		})

		It("mints a stable, per-workspace store epoch on first write", func() {
			// First write mints it.
			write("ws1", func(tx storage.WriteTx) {
				Expect(tx.SetClientLastMutationID("c1", 1)).To(Succeed())
			})
			var first uint64
			read("ws1", func(tx storage.ReadTx) {
				epoch, err := tx.StoreEpoch()
				Expect(err).NotTo(HaveOccurred())
				Expect(epoch).NotTo(BeZero(), "first write mints a non-zero epoch")
				first = epoch
			})
			// Stable across later writes — a cursor must keep matching.
			write("ws1", func(tx storage.WriteTx) {
				Expect(tx.PutRow("widget", "k1", []byte("v"), 1, storage.Actor{})).To(Succeed())
			})
			read("ws1", func(tx storage.ReadTx) {
				epoch, err := tx.StoreEpoch()
				Expect(err).NotTo(HaveOccurred())
				Expect(epoch).To(Equal(first), "epoch is stable for the workspace lifetime")
			})
			// Distinct per workspace — a different lineage gets a different id.
			write("ws2", func(tx storage.WriteTx) {
				Expect(tx.SetClientLastMutationID("c1", 1)).To(Succeed())
			})
			read("ws2", func(tx storage.ReadTx) {
				epoch, err := tx.StoreEpoch()
				Expect(err).NotTo(HaveOccurred())
				Expect(epoch).NotTo(BeZero())
				Expect(epoch).NotTo(Equal(first), "epochs are per-workspace")
			})
		})

		It("allocates monotonic sequences per workspace", func() {
			write("ws1", func(tx storage.WriteTx) {
				for want := uint64(1); want <= 3; want++ {
					seq, err := tx.NextSeq()
					Expect(err).NotTo(HaveOccurred())
					Expect(seq).To(Equal(want))
				}
			})
			write("ws1", func(tx storage.WriteTx) {
				seq, err := tx.NextSeq()
				Expect(err).NotTo(HaveOccurred())
				Expect(seq).To(BeEquivalentTo(4), "sequence continues across transactions")
			})
			write("ws2", func(tx storage.WriteTx) {
				seq, err := tx.NextSeq()
				Expect(err).NotTo(HaveOccurred())
				Expect(seq).To(BeEquivalentTo(1), "sequences are per-workspace")
			})
			read("ws1", func(tx storage.ReadTx) {
				seq, err := tx.CurrentSeq()
				Expect(err).NotTo(HaveOccurred())
				Expect(seq).To(BeEquivalentTo(4))
			})
		})

		It("reports the sequences the CURRENT transaction allocated", func() {
			// An adapter may defer the write-back of its allocations, but not
			// past its own readers: a caller inside the transaction has to see
			// what it just took.
			write("ws1", func(tx storage.WriteTx) {
				for want := uint64(1); want <= 3; want++ {
					seq, err := tx.NextSeq()
					Expect(err).NotTo(HaveOccurred())
					Expect(seq).To(Equal(want))
					Expect(tx.CurrentSeq()).To(Equal(want))
				}
				Expect(tx.PutRow("widget", "k1", []byte("v"), 3, storage.Actor{})).To(Succeed())
			})
			read("ws1", func(tx storage.ReadTx) {
				Expect(tx.CurrentSeq()).To(BeEquivalentTo(3), "and persists all of them")
			})
		})

		It("reports the workspace position the CURRENT transaction moved", func() {
			// The three position scalars live in one place per workspace, so an
			// adapter may read them together and keep them for the transaction.
			// Keeping them may not outlive the transaction's own writes: the
			// first write mints the epoch, and a collection raises the horizon.
			write("ws1", func(tx storage.WriteTx) {
				Expect(tx.StoreEpoch()).To(BeZero(), "nothing has registered the workspace yet")
				seq, err := tx.NextSeq()
				Expect(err).NotTo(HaveOccurred())
				Expect(tx.StoreEpoch()).NotTo(BeZero(), "the first write minted the epoch")
				Expect(tx.CurrentSeq()).To(Equal(seq))
				Expect(tx.PutRow("widget", "k1", []byte("v"), seq, storage.Actor{})).To(Succeed())
				Expect(tx.DeleteRow("widget", "k1", seq, storage.Actor{})).To(Succeed())
			})
			write("ws1", func(tx storage.WriteTx) {
				Expect(tx.TombstoneHorizon()).To(BeZero())
				horizon, err := tx.CollectTombstones(time.Now().Add(time.Hour))
				Expect(err).NotTo(HaveOccurred())
				Expect(horizon).NotTo(BeZero())
				Expect(tx.TombstoneHorizon()).To(Equal(horizon))
			})
		})

		// A lineage mark names the write transaction that allocated a
		// sequence. The engine compares a cursor's mark with the store's, so
		// the failures that matter are a mark that changes for history the
		// store already holds (every client resets) and one that repeats for
		// new history (a cursor from a lost history passes).
		Describe("sequence lineage", func() {
			lineage := func(tx storage.ReadTx, seq uint64) uint64 {
				GinkgoHelper()
				mark, err := tx.SeqLineage(seq)
				Expect(err).NotTo(HaveOccurred())
				return mark
			}
			allocate := func(ws string, n int) {
				GinkgoHelper()
				write(ws, func(tx storage.WriteTx) {
					for range n {
						_, err := tx.NextSeq()
						Expect(err).NotTo(HaveOccurred())
					}
				})
			}

			It("reports no mark for an unwritten workspace or for sequence 0", func() {
				read("ws1", func(tx storage.ReadTx) {
					Expect(lineage(tx, 0)).To(BeZero())
					Expect(lineage(tx, 1)).To(BeZero())
				})
				allocate("ws1", 1)
				read("ws1", func(tx storage.ReadTx) {
					Expect(lineage(tx, 0)).To(BeZero())
				})
			})

			It("shares one mark across a transaction's sequences and mints a new one per transaction", func() {
				allocate("ws1", 3)
				allocate("ws1", 1)
				allocate("ws2", 1)
				read("ws1", func(tx storage.ReadTx) {
					first := lineage(tx, 1)
					Expect(first).NotTo(BeZero())
					Expect(lineage(tx, 2)).To(Equal(first))
					Expect(lineage(tx, 3)).To(Equal(first))
					Expect(lineage(tx, 4)).NotTo(BeZero())
					Expect(lineage(tx, 4)).NotTo(Equal(first))
				})
				read("ws2", func(tx storage.ReadTx) {
					Expect(lineage(tx, 1)).NotTo(BeZero())
				})
			})

			It("reports no mark above the current sequence", func() {
				allocate("ws1", 2)
				read("ws1", func(tx storage.ReadTx) {
					Expect(lineage(tx, 2)).NotTo(BeZero())
					Expect(lineage(tx, 3)).To(BeZero())
				})
			})

			It("keeps the mark of the last allocating transaction for a transaction that allocates nothing", func() {
				allocate("ws1", 1)
				var mark uint64
				read("ws1", func(tx storage.ReadTx) { mark = lineage(tx, 1) })
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutRow("widget", "k", []byte("v"), 1, storage.Actor{})).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					Expect(lineage(tx, 1)).To(Equal(mark))
				})
			})

			It("reports the transaction's own mark for the sequences it allocated", func() {
				allocate("ws1", 1)
				var before, own uint64
				read("ws1", func(tx storage.ReadTx) { before = lineage(tx, 1) })
				write("ws1", func(tx storage.WriteTx) {
					seq, err := tx.NextSeq()
					Expect(err).NotTo(HaveOccurred())
					own = lineage(tx, seq)
					Expect(own).NotTo(BeZero())
					Expect(own).NotTo(Equal(before))
					Expect(lineage(tx, 1)).To(Equal(before), "an earlier transaction's sequence keeps its mark")
				})
				read("ws1", func(tx storage.ReadTx) {
					Expect(lineage(tx, 2)).To(Equal(own), "the mark it reported is the mark it committed")
				})
			})

			It("leaves no mark for an allocation a savepoint rolled back", func() {
				rewind := errors.New("rewind")
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.InSavepoint(func() error {
						_, err := tx.NextSeq()
						Expect(err).NotTo(HaveOccurred())
						return rewind
					})).To(MatchError(rewind))
				})
				read("ws1", func(tx storage.ReadTx) {
					Expect(lineage(tx, 1)).To(BeZero())
				})

				var own uint64
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.InSavepoint(func() error {
						_, err := tx.NextSeq()
						Expect(err).NotTo(HaveOccurred())
						return rewind
					})).To(MatchError(rewind))
					seq, err := tx.NextSeq()
					Expect(err).NotTo(HaveOccurred())
					Expect(seq).To(BeEquivalentTo(1))
					own = lineage(tx, seq)
				})
				read("ws1", func(tx storage.ReadTx) {
					Expect(lineage(tx, 1)).NotTo(BeZero())
					Expect(lineage(tx, 1)).To(Equal(own))
				})
			})

			It("does not reuse the mark of a transaction that rolled back", func() {
				rollback := errors.New("rollback")
				var abandoned uint64
				Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
					seq, err := tx.NextSeq()
					Expect(err).NotTo(HaveOccurred())
					abandoned = lineage(tx, seq)
					return rollback
				})).To(MatchError(rollback))
				read("ws1", func(tx storage.ReadTx) {
					Expect(lineage(tx, 1)).To(BeZero())
				})
				allocate("ws1", 1)
				read("ws1", func(tx storage.ReadTx) {
					Expect(lineage(tx, 1)).NotTo(BeZero())
					Expect(lineage(tx, 1)).NotTo(Equal(abandoned))
				})
			})

			It("prunes the records of transactions committed before the cutoff", func() {
				allocate("ws1", 2)
				allocate("ws1", 1)
				allocate("ws2", 1)
				var marks [4]uint64
				read("ws1", func(tx storage.ReadTx) {
					for seq := range uint64(4) {
						marks[seq] = lineage(tx, seq)
					}
				})

				write("ws1", func(tx storage.WriteTx) {
					n, err := tx.PruneSeqLineage(time.Time{})
					Expect(err).NotTo(HaveOccurred())
					Expect(n).To(BeZero(), "a zero cutoff keeps every record")
					n, err = tx.PruneSeqLineage(time.Now().Add(-time.Hour))
					Expect(err).NotTo(HaveOccurred())
					Expect(n).To(BeZero())
				})
				read("ws1", func(tx storage.ReadTx) {
					for seq := range uint64(4) {
						Expect(lineage(tx, seq)).To(Equal(marks[seq]))
					}
				})

				write("ws1", func(tx storage.WriteTx) {
					n, err := tx.PruneSeqLineage(time.Now().Add(time.Hour))
					Expect(err).NotTo(HaveOccurred())
					Expect(n).To(Equal(2))
				})
				read("ws1", func(tx storage.ReadTx) {
					for seq := range uint64(4) {
						Expect(lineage(tx, seq)).To(BeZero())
					}
				})
				read("ws2", func(tx storage.ReadTx) {
					Expect(lineage(tx, 1)).NotTo(BeZero(), "pruning is per workspace")
				})
			})
		})

		It("reads back what the CURRENT transaction wrote", func() {
			// An adapter may defer its writes — batch them, buffer them — but not
			// past its own readers. The engine depends on this at every turn: a
			// push reads the row it is about to patch, an index build scans rows
			// a earlier mutation of the same transaction wrote.
			write("ws1", func(tx storage.WriteTx) {
				Expect(tx.PutRow("widget", "k1", []byte("v1"), 1, storage.Actor{})).To(Succeed())
				row, err := tx.GetRow("widget", "k1")
				Expect(err).NotTo(HaveOccurred())
				Expect(row.Value).To(Equal([]byte("v1")))

				Expect(tx.PutRow("widget", "k2", []byte("v2"), 2, storage.Actor{})).To(Succeed())
				var scanned []string
				Expect(tx.ScanEntity("widget", func(r storage.Row) error {
					scanned = append(scanned, r.Key)
					return nil
				})).To(Succeed())
				Expect(scanned).To(Equal([]string{"k1", "k2"}), "a scan sees them too")

				// And an overwrite of a row written moments ago reads as the
				// overwrite, not as either value alone.
				Expect(tx.PutRow("widget", "k1", []byte("v1b"), 3, storage.Actor{})).To(Succeed())
				row, err = tx.GetRow("widget", "k1")
				Expect(err).NotTo(HaveOccurred())
				Expect(row.Value).To(Equal([]byte("v1b")))

				Expect(tx.SetClientLastMutationID("c1", 9)).To(Succeed())
				Expect(tx.ClientLastMutationID("c1")).To(BeEquivalentTo(9))

				Expect(tx.DeleteRow("widget", "k2", 4, storage.Actor{})).To(Succeed())
				row, err = tx.GetRow("widget", "k2")
				Expect(err).NotTo(HaveOccurred())
				Expect(row.Deleted).To(BeTrue())
			})
			read("ws1", func(tx storage.ReadTx) {
				row, err := tx.GetRow("widget", "k1")
				Expect(err).NotTo(HaveOccurred())
				Expect(row.Value).To(Equal([]byte("v1b")))
				Expect(tx.ClientLastMutationID("c1")).To(BeEquivalentTo(9))
			})
		})

		It("round-trips rows", func() {
			write("ws1", func(tx storage.WriteTx) {
				Expect(tx.PutRow("widget", "k1", []byte("v1"), 1, storage.Actor{})).To(Succeed())
			})
			read("ws1", func(tx storage.ReadTx) {
				row, err := tx.GetRow("widget", "k1")
				Expect(err).NotTo(HaveOccurred())
				Expect(row.Value).To(Equal([]byte("v1")))
				Expect(row.Version).To(BeEquivalentTo(1))
				Expect(row.Deleted).To(BeFalse())
			})
		})

		It("overwrites rows in place", func() {
			write("ws1", func(tx storage.WriteTx) {
				Expect(tx.PutRow("widget", "k1", []byte("v1"), 1, storage.Actor{})).To(Succeed())
				Expect(tx.PutRow("widget", "k1", []byte("v2"), 2, storage.Actor{})).To(Succeed())
			})
			read("ws1", func(tx storage.ReadTx) {
				row, err := tx.GetRow("widget", "k1")
				Expect(err).NotTo(HaveOccurred())
				Expect(row.Value).To(Equal([]byte("v2")))
				Expect(row.Version).To(BeEquivalentTo(2))
			})
		})

		Describe("write attribution", func() {
			actor := storage.Actor{Subject: "user-7"}

			It("returns the write's actor from every read that yields a row", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutRow("widget", "k1", []byte("v1"), 1, actor)).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					row, err := tx.GetRow("widget", "k1")
					Expect(err).NotTo(HaveOccurred())
					Expect(row.Actor).To(Equal(actor), "GetRow")

					var scanned []storage.Row
					Expect(tx.ScanEntity("widget", func(r storage.Row) error {
						scanned = append(scanned, r)
						return nil
					})).To(Succeed())
					Expect(scanned).To(HaveLen(1))
					Expect(scanned[0].Actor).To(Equal(actor), "ScanEntity")

					scanned = nil
					Expect(tx.ScanEntityAfter("widget", "", 0, func(r storage.Row) error {
						scanned = append(scanned, r)
						return nil
					})).To(Succeed())
					Expect(scanned).To(HaveLen(1))
					Expect(scanned[0].Actor).To(Equal(actor), "ScanEntityAfter")

					scanned = nil
					Expect(tx.ScanChanges(0, "", "", 0, func(r storage.Row) error {
						scanned = append(scanned, r)
						return nil
					})).To(Succeed())
					Expect(scanned).To(HaveLen(1))
					Expect(scanned[0].Actor).To(Equal(actor), "ScanChanges")
				})
			})

			It("replaces the actor on overwrite: a row records its LAST writer only", func() {
				second := storage.Actor{Subject: "user-9"}
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutRow("widget", "k1", []byte("v1"), 1, actor)).To(Succeed())
					Expect(tx.PutRow("widget", "k1", []byte("v2"), 2, second)).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					row, err := tx.GetRow("widget", "k1")
					Expect(err).NotTo(HaveOccurred())
					Expect(row.Actor).To(Equal(second))
				})
			})

			It("attributes a tombstone to whoever deleted the row", func() {
				deleter := storage.Actor{System: "gc"}
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutRow("widget", "k1", []byte("v1"), 1, actor)).To(Succeed())
					Expect(tx.DeleteRow("widget", "k1", 2, deleter)).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					row, err := tx.GetRow("widget", "k1")
					Expect(err).NotTo(HaveOccurred())
					Expect(row.Deleted).To(BeTrue())
					Expect(row.Actor).To(Equal(deleter),
						"who deleted a row is the audit question a tombstone has to answer")
				})
			})

			It("stores a zero actor as unattributed rather than inventing one", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutRow("widget", "k1", []byte("v1"), 1, storage.Actor{})).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					row, err := tx.GetRow("widget", "k1")
					Expect(err).NotTo(HaveOccurred())
					Expect(row.Actor).To(Equal(storage.Actor{}))
				})
			})

			It("keeps a system write distinguishable from an unattributed one", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutRow("widget", "unnamed", []byte("v"), 1, storage.Actor{System: "system"})).To(Succeed())
					Expect(tx.PutRow("widget", "none", []byte("v"), 2, storage.Actor{})).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					row, err := tx.GetRow("widget", "unnamed")
					Expect(err).NotTo(HaveOccurred())
					Expect(row.Actor.System).To(Equal("system"), "unnamed system work is still system work")
					row, err = tx.GetRow("widget", "none")
					Expect(err).NotTo(HaveOccurred())
					Expect(row.Actor.System).To(BeEmpty())
				})
			})

			It("round-trips a caller and a system label together", func() {
				both := storage.Actor{Subject: "user-7", System: "connect:markdown"}
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutRow("widget", "k1", []byte("v"), 1, both)).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					row, err := tx.GetRow("widget", "k1")
					Expect(err).NotTo(HaveOccurred())
					Expect(row.Actor).To(Equal(both),
						"a bypass taken inside a request records the label AND whose request it ran under")
				})
			})
		})

		Describe("creation tracking", func() {
			It("stamps CreatedVersion on insert and returns it from every read that yields a row", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutRow("widget", "k1", []byte("v1"), 3, storage.Actor{})).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					row, err := tx.GetRow("widget", "k1")
					Expect(err).NotTo(HaveOccurred())
					Expect(row.CreatedVersion).To(BeEquivalentTo(3), "GetRow")

					var scanned []storage.Row
					collect := func(r storage.Row) error {
						scanned = append(scanned, r)
						return nil
					}
					Expect(tx.GetRows("widget", []string{"k1"}, collect)).To(Succeed())
					Expect(scanned).To(HaveLen(1))
					Expect(scanned[0].CreatedVersion).To(BeEquivalentTo(3), "GetRows")

					scanned = nil
					Expect(tx.ScanEntity("widget", collect)).To(Succeed())
					Expect(scanned).To(HaveLen(1))
					Expect(scanned[0].CreatedVersion).To(BeEquivalentTo(3), "ScanEntity")

					scanned = nil
					Expect(tx.ScanEntityByVersionDesc("widget", 0, collect)).To(Succeed())
					Expect(scanned).To(HaveLen(1))
					Expect(scanned[0].CreatedVersion).To(BeEquivalentTo(3), "ScanEntityByVersionDesc")

					scanned = nil
					Expect(tx.ScanChanges(0, "", "", 0, collect)).To(Succeed())
					Expect(scanned).To(HaveLen(1))
					Expect(scanned[0].CreatedVersion).To(BeEquivalentTo(3), "ScanChanges")
				})
			})

			It("keeps CreatedVersion across overwrite, deletion and re-creation", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutRow("widget", "k1", []byte("v1"), 1, storage.Actor{})).To(Succeed())
					Expect(tx.PutRow("widget", "k1", []byte("v2"), 2, storage.Actor{})).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					row, err := tx.GetRow("widget", "k1")
					Expect(err).NotTo(HaveOccurred())
					Expect(row.Version).To(BeEquivalentTo(2))
					Expect(row.CreatedVersion).To(BeEquivalentTo(1), "an overwrite keeps the creation")
				})
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.DeleteRow("widget", "k1", 3, storage.Actor{})).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					row, err := tx.GetRow("widget", "k1")
					Expect(err).NotTo(HaveOccurred())
					Expect(row.Deleted).To(BeTrue())
					Expect(row.CreatedVersion).To(BeEquivalentTo(1), "a tombstone keeps the creation")
				})
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutRow("widget", "k1", []byte("v3"), 4, storage.Actor{})).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					row, err := tx.GetRow("widget", "k1")
					Expect(err).NotTo(HaveOccurred())
					Expect(row.CreatedVersion).To(BeEquivalentTo(1),
						"re-creating over a tombstone keeps the original creation: a client whose cursor "+
							"predates the delete may still hold the old row, so the key must not read as born-later")
				})
			})

			It("stamps a tombstone written as the key's first state", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.DeleteRow("widget", "k1", 5, storage.Actor{})).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					row, err := tx.GetRow("widget", "k1")
					Expect(err).NotTo(HaveOccurred())
					Expect(row.CreatedVersion).To(BeEquivalentTo(5))
				})
			})

			It("restarts CreatedVersion once a collected tombstone removed the key's state", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutRow("widget", "k1", []byte("v1"), 1, storage.Actor{})).To(Succeed())
					Expect(tx.DeleteRow("widget", "k1", 2, storage.Actor{})).To(Succeed())
				})
				write("ws1", func(tx storage.WriteTx) {
					_, err := tx.CollectTombstones(time.Now().Add(time.Hour))
					Expect(err).NotTo(HaveOccurred())
				})
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutRow("widget", "k1", []byte("v2"), 3, storage.Actor{})).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					row, err := tx.GetRow("widget", "k1")
					Expect(err).NotTo(HaveOccurred())
					Expect(row.CreatedVersion).To(BeEquivalentTo(3),
						"with the tombstone gone the key has no stored state: the next write is a creation")
				})
			})
		})

		It("isolates workspaces", func() {
			write("ws1", func(tx storage.WriteTx) {
				Expect(tx.PutRow("widget", "k1", []byte("v1"), 1, storage.Actor{})).To(Succeed())
				Expect(tx.SetClientLastMutationID("c1", 7)).To(Succeed())
			})
			read("ws2", func(tx storage.ReadTx) {
				_, err := tx.GetRow("widget", "k1")
				Expect(err).To(MatchError(storage.ErrNotFound))
				id, err := tx.ClientLastMutationID("c1")
				Expect(err).NotTo(HaveOccurred())
				Expect(id).To(BeZero())
			})
		})

		It("writes tombstones that GetRow reports and ScanEntity hides", func() {
			write("ws1", func(tx storage.WriteTx) {
				Expect(tx.PutRow("widget", "k1", []byte("v1"), 1, storage.Actor{})).To(Succeed())
				Expect(tx.DeleteRow("widget", "k1", 2, storage.Actor{})).To(Succeed())
			})
			read("ws1", func(tx storage.ReadTx) {
				row, err := tx.GetRow("widget", "k1")
				Expect(err).NotTo(HaveOccurred())
				Expect(row.Deleted).To(BeTrue())
				Expect(row.Version).To(BeEquivalentTo(2))
				Expect(row.Value).To(BeEmpty(), "a tombstone carries no value (stale row bytes must not leak past a delete)")

				var keys []string
				Expect(tx.ScanEntity("widget", func(r storage.Row) error {
					keys = append(keys, r.Key)
					return nil
				})).To(Succeed())
				Expect(keys).To(BeEmpty())
			})
		})

		It("tombstones rows that never existed (delete-before-put ordering)", func() {
			write("ws1", func(tx storage.WriteTx) {
				Expect(tx.DeleteRow("widget", "ghost", 1, storage.Actor{})).To(Succeed())
			})
			read("ws1", func(tx storage.ReadTx) {
				row, err := tx.GetRow("widget", "ghost")
				Expect(err).NotTo(HaveOccurred())
				Expect(row.Deleted).To(BeTrue())
			})
		})

		// GetRows is a batch of point reads, and the engine resolves index-walk
		// candidates through it — so the two things it must not get wrong are
		// the ORDER it yields (the walk's order is the row order the query
		// answers in) and what it does with a key that has no row (skip, the
		// batch form of ErrNotFound).
		Describe("batch row reads", func() {
			collect := func(tx storage.ReadTx, entity string, keys ...string) []storage.Row {
				GinkgoHelper()
				var got []storage.Row
				Expect(tx.GetRows(entity, keys, func(r storage.Row) error {
					got = append(got, r)
					return nil
				})).To(Succeed())
				return got
			}
			keysOf := func(rows []storage.Row) []string {
				var keys []string
				for _, r := range rows {
					keys = append(keys, r.Key)
				}
				return keys
			}

			BeforeEach(func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutRow("widget", "a", []byte("va"), 1, storage.Actor{Subject: "user-7"})).To(Succeed())
					Expect(tx.PutRow("widget", "b", []byte("vb"), 2, storage.Actor{})).To(Succeed())
					Expect(tx.PutRow("widget", "c", []byte("vc"), 3, storage.Actor{})).To(Succeed())
					Expect(tx.DeleteRow("widget", "c", 4, storage.Actor{System: "gc"})).To(Succeed())
					Expect(tx.PutRow("todo", "a", []byte("t"), 5, storage.Actor{})).To(Succeed())
				})
			})

			It("yields rows in the order the keys were asked for, not storage order", func() {
				read("ws1", func(tx storage.ReadTx) {
					Expect(keysOf(collect(tx, "widget", "b", "a"))).To(Equal([]string{"b", "a"}),
						"an index walk's order is the order its query answers in")
				})
			})

			It("skips keys with no row rather than reporting them", func() {
				read("ws1", func(tx storage.ReadTx) {
					Expect(keysOf(collect(tx, "widget", "a", "missing", "b"))).To(Equal([]string{"a", "b"}))
					Expect(collect(tx, "widget", "missing")).To(BeEmpty())
					Expect(collect(tx, "widget")).To(BeEmpty())
				})
			})

			It("yields a repeated key once per occurrence", func() {
				read("ws1", func(tx storage.ReadTx) {
					Expect(keysOf(collect(tx, "widget", "a", "a"))).To(Equal([]string{"a", "a"}),
						"a repeated forward ref asks twice and expects two targets")
				})
			})

			It("yields tombstones, exactly as GetRow does", func() {
				read("ws1", func(tx storage.ReadTx) {
					rows := collect(tx, "widget", "c")
					Expect(rows).To(HaveLen(1))
					Expect(rows[0].Deleted).To(BeTrue())
					Expect(rows[0].Version).To(BeEquivalentTo(4))
					Expect(rows[0].Actor).To(Equal(storage.Actor{System: "gc"}))
				})
			})

			It("returns the same row GetRow does, actor included", func() {
				read("ws1", func(tx storage.ReadTx) {
					want, err := tx.GetRow("widget", "a")
					Expect(err).NotTo(HaveOccurred())
					Expect(collect(tx, "widget", "a")).To(Equal([]storage.Row{want}))
				})
			})

			// An adapter is free to answer a batch in several reads, and both
			// shipped ones do above a few hundred keys. Nothing about the
			// contract may change at that seam, so this asks for a batch wide
			// enough to cross it and puts a miss and a tombstone on both sides.
			It("keeps order, misses and tombstones across a batch of any width", func() {
				const n = 600
				write("ws1", func(tx storage.WriteTx) {
					for i := range n {
						Expect(tx.PutRow("wide", fmt.Sprintf("k%03d", i), []byte("v"), uint64(i+1), storage.Actor{})).To(Succeed())
					}
					Expect(tx.DeleteRow("wide", "k100", n+1, storage.Actor{System: "gc"})).To(Succeed())
					Expect(tx.DeleteRow("wide", "k500", n+2, storage.Actor{System: "gc"})).To(Succeed())
				})

				// Descending, so storage order cannot pass for the asked-for
				// order, with a key that has no row every hundred keys.
				var keys, want []string
				for i := n - 1; i >= 0; i-- {
					if i%100 == 0 {
						keys = append(keys, fmt.Sprintf("ghost%03d", i))
					}
					key := fmt.Sprintf("k%03d", i)
					keys = append(keys, key)
					want = append(want, key)
				}
				read("ws1", func(tx storage.ReadTx) {
					rows := collect(tx, "wide", keys...)
					Expect(keysOf(rows)).To(Equal(want))
					for _, r := range rows {
						Expect(r.Deleted).To(Equal(r.Key == "k100" || r.Key == "k500"),
							"a tombstone is yielded wherever in the batch it falls")
					}
				})
			})

			It("scopes keys to the entity and the workspace", func() {
				read("ws1", func(tx storage.ReadTx) {
					rows := collect(tx, "todo", "a")
					Expect(rows).To(HaveLen(1))
					Expect(rows[0].Value).To(Equal([]byte("t")), "a bare key means nothing outside its entity")
				})
				read("ws2", func(tx storage.ReadTx) {
					Expect(collect(tx, "widget", "a", "b")).To(BeEmpty())
				})
			})

			It("lets fn read the transaction it is being called from", func() {
				read("ws1", func(tx storage.ReadTx) {
					var seen []string
					Expect(tx.GetRows("widget", []string{"a", "b"}, func(r storage.Row) error {
						// The engine resolves a forward ref from inside this
						// callback, so a batch may not hold anything — an open
						// result set, a claimed connection — that a read from
						// the same transaction would block on.
						other, err := tx.GetRow("widget", "c")
						Expect(err).NotTo(HaveOccurred())
						Expect(other.Deleted).To(BeTrue())
						seen = append(seen, r.Key)
						return nil
					})).To(Succeed())
					Expect(seen).To(Equal([]string{"a", "b"}))
				})
			})

			It("stops at the first callback error and reports it", func() {
				boom := errors.New("boom")
				read("ws1", func(tx storage.ReadTx) {
					var seen []string
					err := tx.GetRows("widget", []string{"a", "b"}, func(r storage.Row) error {
						seen = append(seen, r.Key)
						return boom
					})
					Expect(err).To(MatchError(boom))
					Expect(seen).To(Equal([]string{"a"}))
				})
			})
		})

		It("scans entities in key order, scoped to the entity", func() {
			write("ws1", func(tx storage.WriteTx) {
				Expect(tx.PutRow("widget", "b", []byte("2"), 1, storage.Actor{})).To(Succeed())
				Expect(tx.PutRow("widget", "a", []byte("1"), 2, storage.Actor{})).To(Succeed())
				Expect(tx.PutRow("todo", "x", []byte("t"), 3, storage.Actor{})).To(Succeed())
			})
			read("ws1", func(tx storage.ReadTx) {
				var keys []string
				Expect(tx.ScanEntity("widget", func(r storage.Row) error {
					keys = append(keys, r.Key)
					return nil
				})).To(Succeed())
				Expect(keys).To(Equal([]string{"a", "b"}))
			})
		})

		It("scans changes after a sequence, in version order, including tombstones", func() {
			write("ws1", func(tx storage.WriteTx) {
				Expect(tx.PutRow("widget", "k1", []byte("v1"), 1, storage.Actor{})).To(Succeed())
				Expect(tx.PutRow("widget", "k2", []byte("v2"), 2, storage.Actor{})).To(Succeed())
				Expect(tx.DeleteRow("widget", "k1", 3, storage.Actor{})).To(Succeed())
			})
			read("ws1", func(tx storage.ReadTx) {
				var got []storage.Row
				Expect(tx.ScanChanges(1, "", "", 0, func(r storage.Row) error {
					got = append(got, r)
					return nil
				})).To(Succeed())
				Expect(got).To(HaveLen(2))
				Expect(got[0].Key).To(Equal("k2"))
				Expect(got[0].Version).To(BeEquivalentTo(2))
				Expect(got[0].Value).To(Equal([]byte("v2")), "a live change carries its value")
				Expect(got[1].Key).To(Equal("k1"))
				Expect(got[1].Deleted).To(BeTrue())
				Expect(got[1].Version).To(BeEquivalentTo(3))
				Expect(got[1].Value).To(BeEmpty(), "a tombstone change carries no value")
			})
		})

		It("limits change scans to the first N versions", func() {
			write("ws1", func(tx storage.WriteTx) {
				Expect(tx.PutRow("widget", "k1", []byte("v1"), 1, storage.Actor{})).To(Succeed())
				Expect(tx.PutRow("widget", "k2", []byte("v2"), 2, storage.Actor{})).To(Succeed())
				Expect(tx.PutRow("widget", "k3", []byte("v3"), 3, storage.Actor{})).To(Succeed())
			})
			read("ws1", func(tx storage.ReadTx) {
				var keys []string
				Expect(tx.ScanChanges(0, "", "", 2, func(r storage.Row) error {
					keys = append(keys, r.Key)
					return nil
				})).To(Succeed())
				Expect(keys).To(Equal([]string{"k1", "k2"}))
			})
		})

		It("breaks ties within one version by (entity, key), so a paged scan never drops same-version rows", func() {
			// Five rows share version 1 (a batch / multi-row mutator), one is
			// at version 2. A page limit that lands inside the shared-version
			// group must resume past the last delivered (entity, key) instead
			// of skipping the rest of the group.
			//
			// The keys within the group are the byte-order set the ScanEntity
			// spec above uses, for the same reason: the tie-break is bytewise,
			// and a cursor resuming under a locale collation would drop or
			// repeat rows across the page boundary rather than fail.
			write("ws1", func(tx storage.WriteTx) {
				for _, k := range []string{"b", "a", "Z", "_", "é"} {
					Expect(tx.PutRow("widget", k, []byte(k), 1, storage.Actor{})).To(Succeed())
				}
				Expect(tx.PutRow("todo", "z", []byte("z"), 1, storage.Actor{})).To(Succeed())
				Expect(tx.PutRow("widget", "c", []byte("c"), 2, storage.Actor{})).To(Succeed())
			})
			read("ws1", func(tx storage.ReadTx) {
				// The clean-boundary scan orders by (version, entity, key).
				var order [][2]string
				Expect(tx.ScanChanges(0, "", "", 0, func(r storage.Row) error {
					order = append(order, [2]string{r.Entity, r.Key})
					return nil
				})).To(Succeed())
				Expect(order).To(Equal([][2]string{
					// version 1, by (entity, key): "todo" precedes "widget", then
					// keys by byte value.
					{"todo", "z"},
					{"widget", "Z"}, {"widget", "_"}, {"widget", "a"}, {"widget", "b"}, {"widget", "é"},
					{"widget", "c"}, // version 2
				}))

				// Paginate with limit 2: the boundary splits version 1's group
				// after ("widget", "Z").
				var page1 [][2]string
				Expect(tx.ScanChanges(0, "", "", 2, func(r storage.Row) error {
					page1 = append(page1, [2]string{r.Entity, r.Key})
					return nil
				})).To(Succeed())
				Expect(page1).To(Equal([][2]string{{"todo", "z"}, {"widget", "Z"}}))

				// Resume from the sub-position: the rest of version 1 must come
				// back, none of it dropped.
				var page2 [][2]string
				Expect(tx.ScanChanges(1, "widget", "Z", 0, func(r storage.Row) error {
					page2 = append(page2, [2]string{r.Entity, r.Key})
					return nil
				})).To(Succeed())
				Expect(page2).To(Equal([][2]string{
					{"widget", "_"}, {"widget", "a"}, {"widget", "b"}, {"widget", "é"},
					{"widget", "c"},
				}))
			})
		})

		It("scans entities after a key with a limit, in key order", func() {
			write("ws1", func(tx storage.WriteTx) {
				Expect(tx.PutRow("widget", "a", []byte("1"), 1, storage.Actor{})).To(Succeed())
				Expect(tx.PutRow("widget", "b", []byte("2"), 2, storage.Actor{})).To(Succeed())
				Expect(tx.PutRow("widget", "c", []byte("3"), 3, storage.Actor{})).To(Succeed())
				Expect(tx.PutRow("widget", "d", []byte("4"), 4, storage.Actor{})).To(Succeed())
				Expect(tx.DeleteRow("widget", "c", 5, storage.Actor{})).To(Succeed())
			})
			read("ws1", func(tx storage.ReadTx) {
				var keys []string
				Expect(tx.ScanEntityAfter("widget", "a", 0, func(r storage.Row) error {
					keys = append(keys, r.Key)
					return nil
				})).To(Succeed())
				Expect(keys).To(Equal([]string{"b", "d"}), "after-key scan skips tombstones")

				keys = nil
				Expect(tx.ScanEntityAfter("widget", "", 2, func(r storage.Row) error {
					keys = append(keys, r.Key)
					return nil
				})).To(Succeed())
				Expect(keys).To(Equal([]string{"a", "b"}), "limit caps the page")

				keys = nil
				Expect(tx.ScanEntityAfter("widget", "d", 0, func(r storage.Row) error {
					keys = append(keys, r.Key)
					return nil
				})).To(Succeed())
				Expect(keys).To(BeEmpty(), "scan past the last key yields nothing")
			})
		})

		It("orders keys by byte (code point) value, not locale collation", func() {
			// The keys are chosen so byte order disagrees with a typical locale
			// collation: uppercase sorts before lowercase, '_' (0x5F) lands
			// between the two letter ranges, and a multi-byte rune sorts after
			// all ASCII. An adapter under a locale collation (a Postgres column
			// without COLLATE "C", say) would interleave these differently — and
			// since afterKey paging shares this order with the engine (Go's <,
			// which is bytewise), a divergent order silently drops or repeats
			// rows across a page boundary.
			byteOrder := []string{"A", "Z", "_", "a", "z", "é"}
			write("ws1", func(tx storage.WriteTx) {
				for i, k := range byteOrder {
					Expect(tx.PutRow("widget", k, []byte("v"), uint64(i+1), storage.Actor{})).To(Succeed())
				}
			})
			read("ws1", func(tx storage.ReadTx) {
				var keys []string
				Expect(tx.ScanEntity("widget", func(r storage.Row) error {
					keys = append(keys, r.Key)
					return nil
				})).To(Succeed())
				Expect(keys).To(Equal(byteOrder))

				// ScanEntityAfter must cut the range at the same byte order:
				// resuming after "Z" yields exactly the keys that sort after it.
				keys = nil
				Expect(tx.ScanEntityAfter("widget", "Z", 0, func(r storage.Row) error {
					keys = append(keys, r.Key)
					return nil
				})).To(Succeed())
				Expect(keys).To(Equal([]string{"_", "a", "z", "é"}))
			})
		})

		Describe("read snapshots and cancellation", func() {
			It("runs a read fn at most once and surfaces its error un-retried", func() {
				boom := errors.New("boom")
				calls := 0
				err := store.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
					calls++
					return boom
				})
				Expect(err).To(MatchError(boom))
				Expect(calls).To(Equal(1), "a failed read tx is surfaced, never retried (streaming callers cannot re-run)")
			})

			It("keeps a read snapshot stable against a concurrent commit", func() {
				// Seed one row and a non-zero sequence.
				write("ws1", func(tx storage.WriteTx) {
					_, err := tx.NextSeq()
					Expect(err).NotTo(HaveOccurred())
					Expect(tx.PutRow("widget", "k1", []byte("v1"), 1, storage.Actor{})).To(Succeed())
				})

				writerDone := make(chan error, 1)
				read("ws1", func(tx storage.ReadTx) {
					seqBefore, err := tx.CurrentSeq()
					Expect(err).NotTo(HaveOccurred())

					// Commit a second row from another goroutine while this
					// snapshot is open. A single-writer adapter blocks the writer
					// until the read tx releases; an MVCC adapter commits it
					// immediately. Pull reads horizon + CurrentSeq + ScanChanges
					// as one view, so either way this snapshot must observe none
					// of the concurrent write.
					started := make(chan struct{})
					go func() {
						close(started)
						writerDone <- store.WithWriteTx(ctx, "ws1", func(w storage.WriteTx) error {
							s, err := w.NextSeq()
							if err != nil {
								return err
							}
							return w.PutRow("widget", "k2", []byte("v2"), s, storage.Actor{})
						})
					}()
					<-started

					for i := 0; i < 50; i++ {
						seq, err := tx.CurrentSeq()
						Expect(err).NotTo(HaveOccurred())
						Expect(seq).To(Equal(seqBefore), "CurrentSeq moved under an open read snapshot")
						_, err = tx.GetRow("widget", "k2")
						Expect(err).To(MatchError(storage.ErrNotFound), "a concurrent commit leaked into an open snapshot")
						var keys []string
						Expect(tx.ScanEntity("widget", func(r storage.Row) error {
							keys = append(keys, r.Key)
							return nil
						})).To(Succeed())
						Expect(keys).To(Equal([]string{"k1"}), "ScanEntity saw a row committed after the snapshot opened")
					}
				})
				Expect(<-writerDone).To(Succeed())

				// Once the snapshot closes, a fresh read sees the concurrent write.
				read("ws1", func(tx storage.ReadTx) {
					row, err := tx.GetRow("widget", "k2")
					Expect(err).NotTo(HaveOccurred())
					Expect(row.Value).To(Equal([]byte("v2")))
					seq, err := tx.CurrentSeq()
					Expect(err).NotTo(HaveOccurred())
					Expect(seq).To(BeEquivalentTo(2))
				})
			})

			It("fails a cancelled-context call without committing", func() {
				cancelled, cancel := context.WithCancel(ctx)
				cancel()

				Expect(store.WithReadTx(cancelled, "ws1", func(storage.ReadTx) error {
					return nil
				})).NotTo(Succeed(), "a read on a cancelled context must fail")

				Expect(store.WithWriteTx(cancelled, "ws1", func(tx storage.WriteTx) error {
					return tx.PutRow("widget", "k", []byte("v"), 1, storage.Actor{})
				})).NotTo(Succeed(), "a write on a cancelled context must fail")

				// Whatever the cancelled write attempted must not have committed.
				read("ws1", func(tx storage.ReadTx) {
					_, err := tx.GetRow("widget", "k")
					Expect(err).To(MatchError(storage.ErrNotFound))
					seq, err := tx.CurrentSeq()
					Expect(err).NotTo(HaveOccurred())
					Expect(seq).To(BeZero())
				})
			})
		})

		Describe("re-entrant reads inside scan callbacks", func() {
			// The engine reads from inside its scan callbacks: ScanIndex's
			// callback re-reads each candidate row to apply read filters, and
			// ScanChanges's reads FieldDeltas to ship a text delta instead of a
			// document. SQLite satisfies this for free — several open statements
			// per connection are fine — so the requirement is invisible until an
			// adapter whose driver cannot interleave statements on one connection
			// passes every other spec and then fails on the first indexed query
			// and the first pull of a text entity. Hence these.
			//
			// Only reads. The engine collects a scan's rows and rewrites them
			// after the scan returns (Server.compactBatch), so writing during
			// iteration is deliberately not required of an adapter.
			BeforeEach(func() {
				write("ws1", func(tx storage.WriteTx) {
					for i, k := range []string{"k1", "k2", "k3"} {
						seq := uint64(i + 1)
						Expect(tx.PutRow("widget", k, []byte("v-"+k), seq, storage.Actor{})).To(Succeed())
						Expect(tx.PutIndexEntries("widget", k, []storage.IndexEntry{
							{Index: "by_label", Key: []byte{byte(i + 1)}},
						})).To(Succeed())
						Expect(tx.PutFieldDelta("widget", k, "body", seq, []byte("d-"+k))).To(Succeed())
					}
				})
			})

			// Mutator and compaction scans run inside a write transaction, and
			// WriteTx embeds ReadTx, so every spec here holds for both.
			eachTx := func(body func(storage.ReadTx)) {
				GinkgoHelper()
				read("ws1", body)
				write("ws1", func(tx storage.WriteTx) { body(tx) })
			}

			// The row a scan yields and the row a re-read returns are the same
			// row of the same snapshot; anything else means the nested read
			// escaped the transaction.
			expectSameRow := func(tx storage.ReadTx, scanned storage.Row) error {
				GinkgoHelper()
				row, err := tx.GetRow(scanned.Entity, scanned.Key)
				if err != nil {
					return err
				}
				Expect(row.Value).To(Equal(scanned.Value))
				Expect(row.Version).To(Equal(scanned.Version))
				Expect(row.Deleted).To(Equal(scanned.Deleted))
				return nil
			}

			It("serves GetRow from inside a ScanEntity callback", func() {
				eachTx(func(tx storage.ReadTx) {
					var keys []string
					Expect(tx.ScanEntity("widget", func(r storage.Row) error {
						keys = append(keys, r.Key)
						// Read the scanned row and one the scan has not reached
						// yet: a re-read must not be answerable from whatever the
						// cursor happens to hold.
						if err := expectSameRow(tx, r); err != nil {
							return err
						}
						ahead, err := tx.GetRow("widget", "k3")
						if err != nil {
							return err
						}
						Expect(ahead.Value).To(Equal([]byte("v-k3")))
						return nil
					})).To(Succeed())
					Expect(keys).To(Equal([]string{"k1", "k2", "k3"}), "the scan must survive its callback's reads")
				})
			})

			It("serves GetRow from inside a ScanEntityAfter callback", func() {
				eachTx(func(tx storage.ReadTx) {
					var keys []string
					Expect(tx.ScanEntityAfter("widget", "k1", 0, func(r storage.Row) error {
						keys = append(keys, r.Key)
						return expectSameRow(tx, r)
					})).To(Succeed())
					Expect(keys).To(Equal([]string{"k2", "k3"}))
				})
			})

			It("serves GetRow from inside a ScanEntityByVersionDesc callback", func() {
				eachTx(func(tx storage.ReadTx) {
					var keys []string
					Expect(tx.ScanEntityByVersionDesc("widget", 0, func(r storage.Row) error {
						keys = append(keys, r.Key)
						return expectSameRow(tx, r)
					})).To(Succeed())
					Expect(keys).To(Equal([]string{"k3", "k2", "k1"}))
				})
			})

			It("serves GetRow from inside a ScanChanges callback", func() {
				eachTx(func(tx storage.ReadTx) {
					var keys []string
					Expect(tx.ScanChanges(0, "", "", 0, func(r storage.Row) error {
						keys = append(keys, r.Key)
						return expectSameRow(tx, r)
					})).To(Succeed())
					Expect(keys).To(Equal([]string{"k1", "k2", "k3"}))
				})
			})

			It("serves FieldDeltas from inside a ScanChanges callback (the pull path's shape)", func() {
				eachTx(func(tx storage.ReadTx) {
					var keys []string
					Expect(tx.ScanChanges(0, "", "", 0, func(r storage.Row) error {
						keys = append(keys, r.Key)
						frags, complete, err := tx.FieldDeltas(r.Entity, r.Key, "body", 0)
						if err != nil {
							return err
						}
						Expect(complete).To(BeTrue())
						Expect(frags).To(Equal([][]byte{[]byte("d-" + r.Key)}))
						return nil
					})).To(Succeed())
					Expect(keys).To(Equal([]string{"k1", "k2", "k3"}))
				})
			})

			DescribeTable("serves GetRow from inside a ScanIndex callback (the indexed-query shape)",
				// Both directions, because a descending walk is a different
				// statement in every adapter and pages a query back from a
				// `before` cursor with the same re-read per candidate.
				func(order storage.ScanOrder, want []string) {
					eachTx(func(tx storage.ReadTx) {
						var keys []string
						Expect(tx.ScanIndex("widget", "by_label", nil, nil, 0, order, func(_ []byte, rowKey string) error {
							keys = append(keys, rowKey)
							row, err := tx.GetRow("widget", rowKey)
							if err != nil {
								return err
							}
							Expect(row.Value).To(Equal([]byte("v-" + rowKey)))
							return nil
						})).To(Succeed())
						Expect(keys).To(Equal(want))
					})
				},
				Entry("ascending", storage.ScanAsc, []string{"k1", "k2", "k3"}),
				Entry("descending", storage.ScanDesc, []string{"k3", "k2", "k1"}),
			)

			It("serves GetRows from inside a ScanIndex callback (the batched indexed-query shape)", func() {
				// The engine collects the walk's candidates and resolves them a
				// batch at a time, so the batch read is issued from inside the
				// callback exactly as the point read was.
				eachTx(func(tx storage.ReadTx) {
					var resolved []string
					Expect(tx.ScanIndex("widget", "by_label", nil, nil, 0, storage.ScanAsc, func(_ []byte, rowKey string) error {
						return tx.GetRows("widget", []string{rowKey, "k3"}, func(row storage.Row) error {
							resolved = append(resolved, row.Key)
							Expect(row.Value).To(Equal([]byte("v-" + row.Key)))
							return nil
						})
					})).To(Succeed())
					Expect(resolved).To(Equal([]string{"k1", "k3", "k2", "k3", "k3", "k3"}),
						"the walk must survive its callback's batch reads, in the order it asked for")
				})
			})

			It("serves GetRow from inside a ScanIndexRanges callback, through more entries than one internal batch", func() {
				// The engine finds the rows below a caller's anchors this way,
				// one range per anchor, and a caller with many anchors yields
				// more entries than any plausible page. The ranges are listed
				// out of key order, so the yield order shows whether pages
				// resume where the last one ended.
				const extra = 600
				write("ws1", func(tx storage.WriteTx) {
					for i := 0; i < extra; i++ {
						key := fmt.Sprintf("b%04d", i)
						Expect(tx.PutRow("widget", key, []byte("v-"+key), uint64(i+10), storage.Actor{})).To(Succeed())
						Expect(tx.PutIndexEntries("widget", key, []storage.IndexEntry{
							{Index: "by_parent", Key: []byte{byte(i % 3)}},
						})).To(Succeed())
					}
				})
				var want []string
				for _, parent := range []int{2, 0, 1} {
					for i := parent; i < extra; i += 3 {
						want = append(want, fmt.Sprintf("%d:b%04d", parent, i))
					}
				}
				ranges := []storage.IndexRange{
					{Lo: []byte{0x02}, Hi: []byte{0x03}},
					{Lo: []byte{0x00}, Hi: []byte{0x01}},
					{Lo: []byte{0x01}, Hi: []byte{0x02}},
				}
				eachTx(func(tx storage.ReadTx) {
					var got []string
					Expect(tx.ScanIndexRanges("widget", "by_parent", ranges, func(i int, _ []byte, rowKey string) error {
						// The range's parent byte, so that got reads like want.
						got = append(got, fmt.Sprintf("%d:%s", ranges[i].Lo[0], rowKey))
						row, err := tx.GetRow("widget", rowKey)
						if err != nil {
							return err
						}
						Expect(row.Value).To(Equal([]byte("v-" + rowKey)))
						return nil
					})).To(Succeed())
					Expect(got).To(Equal(want), "a callback that reads on every entry must not truncate or reorder the scan")
				})
			})

			It("serves reads from inside a GetRows callback", func() {
				// A batch read is itself a yielding read, so an adapter that
				// holds its result set open across the callback breaks the same
				// way a scan would.
				eachTx(func(tx storage.ReadTx) {
					var keys []string
					Expect(tx.GetRows("widget", []string{"k1", "k2"}, func(r storage.Row) error {
						keys = append(keys, r.Key)
						return expectSameRow(tx, r)
					})).To(Succeed())
					Expect(keys).To(Equal([]string{"k1", "k2"}))
				})
			})

			It("serves re-entrant reads through a scan longer than one internal batch", func() {
				// Every spec above scans three rows, which an adapter reading its
				// result set in pages would pass while only ever serving the
				// first one. Paging is the shape a driver that cannot interleave
				// statements gets pushed into — pgstore FETCHes from a
				// server-side cursor precisely so the connection is free while
				// the callback runs — and the page size is an implementation
				// detail no spec can name. So this scans more rows than any
				// plausible page and re-reads on every one, which fails unless
				// the callback is served BETWEEN pages as well as within one.
				const extra = 600
				write("ws1", func(tx storage.WriteTx) {
					for i := 0; i < extra; i++ {
						// Sorts before the BeforeEach's k1..k3, so the scan
						// crosses its page boundaries well before it ends.
						key := fmt.Sprintf("b%04d", i)
						Expect(tx.PutRow("widget", key, []byte(key), uint64(i+1), storage.Actor{})).To(Succeed())
					}
				})
				read("ws1", func(tx storage.ReadTx) {
					var seen int
					Expect(tx.ScanEntity("widget", func(r storage.Row) error {
						seen++
						return expectSameRow(tx, r)
					})).To(Succeed())
					Expect(seen).To(Equal(extra+3), "a callback that reads on every row must not truncate the scan")

					// The pull path's shape at the same length.
					seen = 0
					Expect(tx.ScanChanges(0, "", "", 0, func(r storage.Row) error {
						seen++
						_, _, err := tx.FieldDeltas(r.Entity, r.Key, "body", 0)
						return err
					})).To(Succeed())
					Expect(seen).To(Equal(extra + 3))
				})
			})
		})

		Describe("savepoints", func() {
			It("rolls back a savepoint's writes (and its sequences) on error", func() {
				boom := errors.New("boom")
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutRow("widget", "outside", []byte("v"), 1, storage.Actor{})).To(Succeed())
					err := tx.InSavepoint(func() error {
						seq, err := tx.NextSeq()
						Expect(err).NotTo(HaveOccurred())
						Expect(tx.PutRow("widget", "inside", []byte("v"), seq, storage.Actor{})).To(Succeed())
						return boom
					})
					Expect(err).To(MatchError(boom))
				})
				read("ws1", func(tx storage.ReadTx) {
					_, err := tx.GetRow("widget", "inside")
					Expect(err).To(MatchError(storage.ErrNotFound))
					row, err := tx.GetRow("widget", "outside")
					Expect(err).NotTo(HaveOccurred())
					Expect(row.Value).To(Equal([]byte("v")))
					seq, err := tx.CurrentSeq()
					Expect(err).NotTo(HaveOccurred())
					Expect(seq).To(BeZero(), "rolled-back savepoint must not consume a seq")
				})
			})

			It("releases a successful savepoint into the enclosing transaction", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.InSavepoint(func() error {
						return tx.PutRow("widget", "k1", []byte("v"), 1, storage.Actor{})
					})).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					_, err := tx.GetRow("widget", "k1")
					Expect(err).NotTo(HaveOccurred())
				})
			})

			It("nests savepoints; an inner rollback spares outer writes", func() {
				boom := errors.New("inner")
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.InSavepoint(func() error {
						if err := tx.PutRow("widget", "outer", []byte("v"), 1, storage.Actor{}); err != nil {
							return err
						}
						err := tx.InSavepoint(func() error {
							Expect(tx.PutRow("widget", "inner", []byte("v"), 2, storage.Actor{})).To(Succeed())
							return boom
						})
						Expect(err).To(MatchError(boom))
						return nil
					})).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					_, err := tx.GetRow("widget", "outer")
					Expect(err).NotTo(HaveOccurred())
					_, err = tx.GetRow("widget", "inner")
					Expect(err).To(MatchError(storage.ErrNotFound))
				})
			})

			It("hands a rolled-back savepoint's sequence to the next allocation", func() {
				// The shape a push batch takes: a rejected mutation between two
				// applied ones. Its rewind has to give the sequence back, or
				// the change log gaps at a version no row ever carries and
				// every client's cursor waits for it.
				boom := errors.New("boom")
				var first, second uint64
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.InSavepoint(func() error {
						var err error
						if first, err = tx.NextSeq(); err != nil {
							return err
						}
						return tx.PutRow("widget", "k1", []byte("v"), first, storage.Actor{})
					})).To(Succeed())
					Expect(tx.InSavepoint(func() error {
						seq, err := tx.NextSeq()
						Expect(err).NotTo(HaveOccurred())
						Expect(seq).To(Equal(first+1), "the rejected mutation takes the next one")
						Expect(tx.PutRow("widget", "doomed", []byte("v"), seq, storage.Actor{})).To(Succeed())
						return boom
					})).To(MatchError(boom))
					Expect(tx.InSavepoint(func() error {
						var err error
						if second, err = tx.NextSeq(); err != nil {
							return err
						}
						return tx.PutRow("widget", "k2", []byte("v"), second, storage.Actor{})
					})).To(Succeed())
				})
				Expect(first).To(BeEquivalentTo(1))
				Expect(second).To(BeEquivalentTo(2), "and gives it back when it rolls back")
				read("ws1", func(tx storage.ReadTx) {
					Expect(tx.CurrentSeq()).To(BeEquivalentTo(2))
					_, err := tx.GetRow("widget", "doomed")
					Expect(err).To(MatchError(storage.ErrNotFound))
				})
			})

			It("commits the writes around a savepoint rolled back LAST", func() {
				// A push whose final mutation is rejected still writes that
				// client's watermark afterwards, so the transaction ends with a
				// rewound savepoint behind it and one write in front of it.
				boom := errors.New("boom")
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.InSavepoint(func() error {
						seq, err := tx.NextSeq()
						Expect(err).NotTo(HaveOccurred())
						return tx.PutRow("widget", "kept", []byte("v"), seq, storage.Actor{})
					})).To(Succeed())
					Expect(tx.InSavepoint(func() error {
						seq, err := tx.NextSeq()
						Expect(err).NotTo(HaveOccurred())
						Expect(tx.PutRow("widget", "doomed", []byte("v"), seq, storage.Actor{})).To(Succeed())
						return boom
					})).To(MatchError(boom))
					Expect(tx.SetClientLastMutationID("c1", 7)).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					_, err := tx.GetRow("widget", "kept")
					Expect(err).NotTo(HaveOccurred())
					_, err = tx.GetRow("widget", "doomed")
					Expect(err).To(MatchError(storage.ErrNotFound))
					Expect(tx.ClientLastMutationID("c1")).To(BeEquivalentTo(7))
					Expect(tx.CurrentSeq()).To(BeEquivalentTo(1), "only the applied write consumed one")
				})
			})

			It("still commits writes made after a rolled-back savepoint", func() {
				boom := errors.New("boom")
				write("ws1", func(tx storage.WriteTx) {
					err := tx.InSavepoint(func() error {
						Expect(tx.PutRow("widget", "doomed", []byte("v"), 1, storage.Actor{})).To(Succeed())
						return boom
					})
					Expect(err).To(MatchError(boom))
					Expect(tx.PutRow("widget", "after", []byte("v"), 1, storage.Actor{})).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					_, err := tx.GetRow("widget", "after")
					Expect(err).NotTo(HaveOccurred())
					_, err = tx.GetRow("widget", "doomed")
					Expect(err).To(MatchError(storage.ErrNotFound))
				})
			})
		})

		Describe("tombstone collection", func() {
			It("deletes old tombstones, spares live rows, and advances the horizon", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutRow("widget", "live", []byte("v"), 1, storage.Actor{})).To(Succeed())
					Expect(tx.PutRow("widget", "dead", []byte("v"), 2, storage.Actor{})).To(Succeed())
					Expect(tx.DeleteRow("widget", "dead", 3, storage.Actor{})).To(Succeed())
				})
				write("ws1", func(tx storage.WriteTx) {
					horizon, err := tx.CollectTombstones(time.Now().Add(time.Hour))
					Expect(err).NotTo(HaveOccurred())
					Expect(horizon).To(BeEquivalentTo(3))
				})
				read("ws1", func(tx storage.ReadTx) {
					_, err := tx.GetRow("widget", "dead")
					Expect(err).To(MatchError(storage.ErrNotFound), "tombstone is gone")
					row, err := tx.GetRow("widget", "live")
					Expect(err).NotTo(HaveOccurred())
					Expect(row.Deleted).To(BeFalse())
					horizon, err := tx.TombstoneHorizon()
					Expect(err).NotTo(HaveOccurred())
					Expect(horizon).To(BeEquivalentTo(3))
				})
			})

			It("spares tombstones newer than the cutoff", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.DeleteRow("widget", "old", 1, storage.Actor{})).To(Succeed())
				})
				cutoff := time.Now().Add(time.Millisecond)
				time.Sleep(5 * time.Millisecond)
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.DeleteRow("widget", "new", 2, storage.Actor{})).To(Succeed())
				})
				write("ws1", func(tx storage.WriteTx) {
					horizon, err := tx.CollectTombstones(cutoff)
					Expect(err).NotTo(HaveOccurred())
					Expect(horizon).To(BeEquivalentTo(1))
				})
				read("ws1", func(tx storage.ReadTx) {
					_, err := tx.GetRow("widget", "old")
					Expect(err).To(MatchError(storage.ErrNotFound))
					row, err := tx.GetRow("widget", "new")
					Expect(err).NotTo(HaveOccurred())
					Expect(row.Deleted).To(BeTrue(), "newer tombstone survives")
				})
			})

			It("never regresses the horizon", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.DeleteRow("widget", "k1", 4, storage.Actor{})).To(Succeed())
				})
				write("ws1", func(tx storage.WriteTx) {
					horizon, err := tx.CollectTombstones(time.Now().Add(time.Hour))
					Expect(err).NotTo(HaveOccurred())
					Expect(horizon).To(BeEquivalentTo(4))
				})
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.DeleteRow("widget", "k2", 5, storage.Actor{})).To(Succeed())
				})
				write("ws1", func(tx storage.WriteTx) {
					horizon, err := tx.CollectTombstones(time.Now().Add(-time.Hour))
					Expect(err).NotTo(HaveOccurred())
					Expect(horizon).To(BeEquivalentTo(4), "no-op collection keeps the horizon")
				})
			})
		})

		Describe("entity row purging", func() {
			It("removes an entity's expired rows outright, leaving no tombstone", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutRow("hist", "h1", []byte("v"), 1, storage.Actor{})).To(Succeed())
					Expect(tx.PutRow("hist", "h2", []byte("v"), 2, storage.Actor{})).To(Succeed())
					// Another entity's rows are not this collection's business.
					Expect(tx.PutRow("widget", "w1", []byte("v"), 3, storage.Actor{})).To(Succeed())
				})
				write("ws1", func(tx storage.WriteTx) {
					removed, _, err := tx.PurgeEntityRows("hist", "", time.Now().Add(time.Hour), 0)
					Expect(err).NotTo(HaveOccurred())
					Expect(removed).To(Equal(2))
				})
				read("ws1", func(tx storage.ReadTx) {
					// Gone outright: a tombstone would read back as a Row with
					// Deleted set, which is exactly what must NOT happen.
					_, err := tx.GetRow("hist", "h1")
					Expect(err).To(MatchError(storage.ErrNotFound))
					_, err = tx.GetRow("hist", "h2")
					Expect(err).To(MatchError(storage.ErrNotFound))
					row, err := tx.GetRow("widget", "w1")
					Expect(err).NotTo(HaveOccurred())
					Expect(row.Deleted).To(BeFalse())
					// No tombstone was minted, so no cursor is invalidated.
					horizon, err := tx.TombstoneHorizon()
					Expect(err).NotTo(HaveOccurred())
					Expect(horizon).To(BeZero())
					var seen []string
					Expect(tx.ScanChanges(0, "", "", 0, func(r storage.Row) error {
						seen = append(seen, r.Entity+"/"+r.Key)
						return nil
					})).To(Succeed())
					Expect(seen).To(ConsistOf("widget/w1"))
				})
			})

			It("spares rows newer than the cutoff", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutRow("hist", "old", []byte("v"), 1, storage.Actor{})).To(Succeed())
				})
				cutoff := time.Now().Add(time.Millisecond)
				time.Sleep(5 * time.Millisecond)
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutRow("hist", "new", []byte("v"), 2, storage.Actor{})).To(Succeed())
				})
				write("ws1", func(tx storage.WriteTx) {
					removed, _, err := tx.PurgeEntityRows("hist", "", cutoff, 0)
					Expect(err).NotTo(HaveOccurred())
					Expect(removed).To(Equal(1))
				})
				read("ws1", func(tx storage.ReadTx) {
					_, err := tx.GetRow("hist", "old")
					Expect(err).To(MatchError(storage.ErrNotFound))
					_, err = tx.GetRow("hist", "new")
					Expect(err).NotTo(HaveOccurred())
				})
			})

			It("takes the collected rows' index entries with them", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutRow("hist", "h1", []byte("v"), 1, storage.Actor{})).To(Succeed())
					Expect(tx.PutIndexEntries("hist", "h1", []storage.IndexEntry{
						{Index: "by_source", Key: []byte("a")},
					})).To(Succeed())
					Expect(tx.PutRow("widget", "w1", []byte("v"), 2, storage.Actor{})).To(Succeed())
					Expect(tx.PutIndexEntries("widget", "w1", []storage.IndexEntry{
						{Index: "by_source", Key: []byte("a")},
					})).To(Succeed())
				})
				write("ws1", func(tx storage.WriteTx) {
					_, _, err := tx.PurgeEntityRows("hist", "", time.Now().Add(time.Hour), 0)
					Expect(err).NotTo(HaveOccurred())
				})
				read("ws1", func(tx storage.ReadTx) {
					var got []string
					Expect(tx.ScanIndex("hist", "by_source", nil, nil, 0, storage.ScanAsc,
						func(_ []byte, rowKey string) error {
							got = append(got, rowKey)
							return nil
						})).To(Succeed())
					Expect(got).To(BeEmpty(), "an entry outliving its row would leak")
					// An index name is per entity, so the other entity's
					// identically-named index is untouched.
					got = nil
					Expect(tx.ScanIndex("widget", "by_source", nil, nil, 0, storage.ScanAsc,
						func(_ []byte, rowKey string) error {
							got = append(got, rowKey)
							return nil
						})).To(Succeed())
					Expect(got).To(ConsistOf("w1"))
				})
			})

			It("keeps only the rows under a key prefix", func() {
				write("ws1", func(tx storage.WriteTx) {
					for _, k := range []string{"todo/t1/1", "todo/t1/2", "todo/t10/1", "todo/t2/1"} {
						Expect(tx.PutRow("hist", k, []byte("v"), 1, storage.Actor{})).To(Succeed())
					}
				})
				write("ws1", func(tx storage.WriteTx) {
					// The trailing separator is what keeps "t1" from taking
					// "t10" with it — the prefix is a byte range, not a
					// component-aware match, so the caller supplies the boundary.
					removed, _, err := tx.PurgeEntityRows("hist", "todo/t1/", time.Time{}, 0)
					Expect(err).NotTo(HaveOccurred())
					Expect(removed).To(Equal(2))
				})
				read("ws1", func(tx storage.ReadTx) {
					var left []string
					Expect(tx.ScanEntity("hist", func(r storage.Row) error {
						left = append(left, r.Key)
						return nil
					})).To(Succeed())
					Expect(left).To(ConsistOf("todo/t10/1", "todo/t2/1"))
				})
			})

			// Each prefix ends on a code point whose successor is not one byte
			// higher. An adapter that binds the end of the range as text must
			// still send valid UTF-8 there.
			DescribeTable("keeps only the rows under a non-ASCII key prefix",
				func(prefix string, under, outside []string) {
					write("ws1", func(tx storage.WriteTx) {
						for i, k := range append(append([]string{}, under...), outside...) {
							Expect(tx.PutRow("hist", k, []byte("v"), uint64(i+1), storage.Actor{})).To(Succeed())
						}
					})
					write("ws1", func(tx storage.WriteTx) {
						removed, _, err := tx.PurgeEntityRows("hist", prefix, time.Time{}, 0)
						Expect(err).NotTo(HaveOccurred())
						Expect(removed).To(Equal(len(under)))
					})
					read("ws1", func(tx storage.ReadTx) {
						var left []string
						Expect(tx.ScanEntity("hist", func(r storage.Row) error {
							left = append(left, r.Key)
							return nil
						})).To(Succeed())
						Expect(left).To(ConsistOf(outside))
					})
				},
				Entry("a last byte of 0xbf", "п",
					[]string{"п1", "п2"}, []string{"о1", "р1"}),
				Entry("the last code point below the surrogates", "퟿",
					[]string{"퟿1"}, []string{"퟾1", ""}),
				Entry("the highest code point after another", "a\U0010ffff",
					[]string{"a\U0010ffff1"}, []string{"a\U0010fffe", "b"}),
				Entry("nothing but the highest code point", "\U0010ffff",
					[]string{"\U0010ffff", "\U0010ffff\U0010ffff"}, []string{"\U0010fffe"}),
			)

			It("treats the zero time as no age bound at all", func() {
				// Not "before the epoch", which is what a naive UnixMilli() of
				// the zero time would say — and would delete nothing, silently.
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutRow("hist", "h1", []byte("v"), 1, storage.Actor{})).To(Succeed())
				})
				write("ws1", func(tx storage.WriteTx) {
					removed, _, err := tx.PurgeEntityRows("hist", "", time.Time{}, 0)
					Expect(err).NotTo(HaveOccurred())
					Expect(removed).To(Equal(1))
				})
			})

			It("applies the prefix and the age bound together", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutRow("hist", "todo/t1/1", []byte("v"), 1, storage.Actor{})).To(Succeed())
					Expect(tx.PutRow("hist", "todo/t2/1", []byte("v"), 2, storage.Actor{})).To(Succeed())
				})
				cutoff := time.Now().Add(time.Millisecond)
				time.Sleep(5 * time.Millisecond)
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutRow("hist", "todo/t1/2", []byte("v"), 3, storage.Actor{})).To(Succeed())
				})
				write("ws1", func(tx storage.WriteTx) {
					removed, _, err := tx.PurgeEntityRows("hist", "todo/t1/", cutoff, 0)
					Expect(err).NotTo(HaveOccurred())
					Expect(removed).To(Equal(1), "old AND under the prefix")
				})
				read("ws1", func(tx storage.ReadTx) {
					var left []string
					Expect(tx.ScanEntity("hist", func(r storage.Row) error {
						left = append(left, r.Key)
						return nil
					})).To(Succeed())
					Expect(left).To(ConsistOf("todo/t1/2", "todo/t2/1"))
				})
			})

			It("removes no more than the limit and reports what is left", func() {
				write("ws1", func(tx storage.WriteTx) {
					for _, k := range []string{"h1", "h2", "h3", "h4", "h5"} {
						Expect(tx.PutRow("hist", k, []byte("v"), 1, storage.Actor{})).To(Succeed())
					}
				})
				write("ws1", func(tx storage.WriteTx) {
					removed, more, err := tx.PurgeEntityRows("hist", "", time.Now().Add(time.Hour), 2)
					Expect(err).NotTo(HaveOccurred())
					Expect(removed).To(Equal(2), "the bound is a hard cap, not a hint")
					Expect(more).To(BeTrue())
				})
				read("ws1", func(tx storage.ReadTx) {
					var left []string
					Expect(tx.ScanEntity("hist", func(r storage.Row) error {
						left = append(left, r.Key)
						return nil
					})).To(Succeed())
					Expect(left).To(HaveLen(3), "the rest waits for the next call")
				})
			})

			It("drains the selection across bounded calls, index entries included", func() {
				write("ws1", func(tx storage.WriteTx) {
					for _, k := range []string{"h1", "h2", "h3", "h4", "h5"} {
						Expect(tx.PutRow("hist", k, []byte("v"), 1, storage.Actor{})).To(Succeed())
						Expect(tx.PutIndexEntries("hist", k, []storage.IndexEntry{
							{Index: "by_source", Key: []byte(k)},
						})).To(Succeed())
					}
				})
				// Each chunk is its own transaction, which is how the GC pass drains
				// a long history without holding one write open for all of it.
				total, more := 0, true
				for i := 0; more && i < 10; i++ {
					write("ws1", func(tx storage.WriteTx) {
						removed, m, err := tx.PurgeEntityRows("hist", "", time.Now().Add(time.Hour), 2)
						Expect(err).NotTo(HaveOccurred())
						total += removed
						more = m
					})
				}
				// The loop must end on its own: more is allowed to be conservative
				// (a call that removed exactly its limit may report it), but a call
				// that removes nothing must settle it.
				Expect(more).To(BeFalse())
				Expect(total).To(Equal(5))
				read("ws1", func(tx storage.ReadTx) {
					Expect(tx.ScanEntity("hist", func(storage.Row) error {
						return fmt.Errorf("unexpected row")
					})).To(Succeed())
					var got []string
					Expect(tx.ScanIndex("hist", "by_source", nil, nil, 0, storage.ScanAsc,
						func(_ []byte, rowKey string) error {
							got = append(got, rowKey)
							return nil
						})).To(Succeed())
					Expect(got).To(BeEmpty(), "a chunk must take its own rows' entries, no more and no less")
				})
			})

			It("applies the limit within the other narrowings", func() {
				write("ws1", func(tx storage.WriteTx) {
					for _, k := range []string{"todo/t1/1", "todo/t1/2", "todo/t2/1"} {
						Expect(tx.PutRow("hist", k, []byte("v"), 1, storage.Actor{})).To(Succeed())
					}
				})
				write("ws1", func(tx storage.WriteTx) {
					// A limit selects among the rows the prefix already chose; it
					// must not reach past it for the count.
					removed, more, err := tx.PurgeEntityRows("hist", "todo/t1/", time.Time{}, 5)
					Expect(err).NotTo(HaveOccurred())
					Expect(removed).To(Equal(2))
					Expect(more).To(BeFalse(), "a call under its limit has drained the selection")
				})
				read("ws1", func(tx storage.ReadTx) {
					var left []string
					Expect(tx.ScanEntity("hist", func(r storage.Row) error {
						left = append(left, r.Key)
						return nil
					})).To(Succeed())
					Expect(left).To(ConsistOf("todo/t2/1"))
				})
			})

			It("treats a limit of zero or less as no bound", func() {
				write("ws1", func(tx storage.WriteTx) {
					for _, k := range []string{"h1", "h2", "h3"} {
						Expect(tx.PutRow("hist", k, []byte("v"), 1, storage.Actor{})).To(Succeed())
					}
				})
				write("ws1", func(tx storage.WriteTx) {
					removed, more, err := tx.PurgeEntityRows("hist", "", time.Time{}, 0)
					Expect(err).NotTo(HaveOccurred())
					Expect(removed).To(Equal(3))
					// An unbounded call left nothing to come back for, so saying it
					// might have would send a drain loop round again for nothing.
					Expect(more).To(BeFalse())
				})
			})

			It("purges nothing for an entity or prefix that matches no row", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutRow("widget", "w1", []byte("v"), 1, storage.Actor{})).To(Succeed())
				})
				write("ws1", func(tx storage.WriteTx) {
					removed, _, err := tx.PurgeEntityRows("absent", "", time.Now().Add(time.Hour), 0)
					Expect(err).NotTo(HaveOccurred())
					Expect(removed).To(BeZero())
					removed, _, err = tx.PurgeEntityRows("widget", "nope/", time.Time{}, 0)
					Expect(err).NotTo(HaveOccurred())
					Expect(removed).To(BeZero())
				})
			})
		})

		It("lists workspaces that have been written", func() {
			ids, err := store.ListWorkspaceIDs(ctx, storage.Range{})
			Expect(err).NotTo(HaveOccurred())
			Expect(ids).To(BeEmpty())
			write("wsB", func(tx storage.WriteTx) {
				Expect(tx.PutRow("widget", "k", []byte("v"), 1, storage.Actor{})).To(Succeed())
			})
			write("wsA", func(tx storage.WriteTx) {
				Expect(tx.PutRow("widget", "k", []byte("v"), 1, storage.Actor{})).To(Succeed())
			})
			ids, err = store.ListWorkspaceIDs(ctx, storage.Range{})
			Expect(err).NotTo(HaveOccurred())
			Expect(ids).To(ConsistOf("wsA", "wsB"))
		})

		It("does not register a workspace a write transaction only read", func() {
			// Read-only write closures are ordinary (a Mutate that decides it
			// has nothing to do, a push whose every mutation was already
			// applied). Since workspace_id is unauthenticated wire input and
			// nothing ever removes a workspace, registering one here would let
			// any caller grow the GC pass without bound.
			write("ghost", func(tx storage.WriteTx) {
				_, err := tx.GetRow("widget", "k")
				Expect(err).To(MatchError(storage.ErrNotFound))
				id, err := tx.ClientLastMutationID("c1")
				Expect(err).NotTo(HaveOccurred())
				Expect(id).To(BeZero())
			})
			ids, err := store.ListWorkspaceIDs(ctx, storage.Range{})
			Expect(err).NotTo(HaveOccurred())
			Expect(ids).To(BeEmpty(), "a write tx that wrote nothing leaves no workspace behind")
			read("ghost", func(tx storage.ReadTx) {
				epoch, err := tx.StoreEpoch()
				Expect(err).NotTo(HaveOccurred())
				Expect(epoch).To(BeZero(), "no epoch is minted until something is written")
			})
			// A later real write registers it exactly as a first write would.
			write("ghost", func(tx storage.WriteTx) {
				Expect(tx.PutRow("widget", "k", []byte("v"), 1, storage.Actor{})).To(Succeed())
			})
			ids, err = store.ListWorkspaceIDs(ctx, storage.Range{})
			Expect(err).NotTo(HaveOccurred())
			Expect(ids).To(ConsistOf("ghost"))
		})

		It("does not register a workspace whose only write rolled back", func() {
			// A rejected mutation is applied inside a savepoint and rewound;
			// the push path then writes the client watermark OUTSIDE the
			// savepoint, so a batch of nothing but rejections still registers.
			// A closure whose every write is rewound must not.
			boom := errors.New("boom")
			write("ghost", func(tx storage.WriteTx) {
				Expect(tx.InSavepoint(func() error {
					seq, err := tx.NextSeq()
					Expect(err).NotTo(HaveOccurred())
					Expect(tx.PutRow("widget", "k", []byte("v"), seq, storage.Actor{})).To(Succeed())
					return boom
				})).To(MatchError(boom))
			})
			ids, err := store.ListWorkspaceIDs(ctx, storage.Range{})
			Expect(err).NotTo(HaveOccurred())
			Expect(ids).To(BeEmpty())
			// Sequence allocation still starts from scratch afterwards: the
			// rewind took the workspace's bookkeeping with it.
			write("ghost", func(tx storage.WriteTx) {
				seq, err := tx.NextSeq()
				Expect(err).NotTo(HaveOccurred())
				Expect(seq).To(BeEquivalentTo(1))
				Expect(tx.PutRow("widget", "k", []byte("v"), seq, storage.Actor{})).To(Succeed())
			})
			ids, err = store.ListWorkspaceIDs(ctx, storage.Range{})
			Expect(err).NotTo(HaveOccurred())
			Expect(ids).To(ConsistOf("ghost"))
		})

		// Every workspace-list query takes a Range, which is what lets one
		// server own part of a store's id space (see prefixstore) and still
		// pay only for its own part. A query that ignored either end would
		// serve another server's workspaces to this one.
		Describe("workspace-id ranges", func() {
			BeforeEach(func() {
				for _, ws := range []string{"a", "b", "c"} {
					write(ws, func(tx storage.WriteTx) {
						Expect(tx.PutRow("widget", "k", []byte("v"), 1, storage.Actor{})).To(Succeed())
					})
				}
			})

			It("excludes both ends of the range", func() {
				Expect(store.ListWorkspaceIDs(ctx, storage.Range{After: "a", Before: "c"})).
					To(ConsistOf("b"))
			})

			It("treats an empty end as unbounded", func() {
				Expect(store.ListWorkspaceIDs(ctx, storage.Range{After: "b"})).To(ConsistOf("c"))
				Expect(store.ListWorkspaceIDs(ctx, storage.Range{Before: "b"})).To(ConsistOf("a"))
			})

			It("bounds every work query", func() {
				// Each query is given a question every one of the three
				// workspaces answers yes to, so the range is the only thing
				// that can narrow the answer to one.
				rng := storage.Range{After: "a", Before: "c"}
				cutoff := storage.GCCutoffs{Clients: time.Now().Add(time.Hour)}
				for _, ws := range []string{"a", "b", "c"} {
					write(ws, func(tx storage.WriteTx) {
						Expect(tx.SetClientLastMutationID("c1", 1)).To(Succeed())
						Expect(tx.SetTextCompactMark(storage.TextCompactMark{MinPurged: 5})).To(Succeed())
					})
				}
				Expect(store.ListCollectableWorkspaceIDs(ctx, cutoff, rng, 10)).To(ConsistOf("b"))
				Expect(store.ListStaleIndexWorkspaceIDs(ctx, "mark", rng, 10)).To(ConsistOf("b"))
				Expect(store.ListCompactableWorkspaceIDs(ctx, 1, rng, 10)).To(ConsistOf("b"))
				Expect(store.ListPendingWorkspaceIDs(ctx, "indexer", rng, 10)).To(ConsistOf("b"))
			})
		})

		// The collectable query is how a maintenance pass finds its work without
		// opening a write transaction against every workspace that exists. An
		// omission here is state that is never reclaimed, so these specs are
		// about completeness first: one per kind the pass collects.
		Describe("collectable workspaces", func() {
			// Everything a pass collects is selected by age, so a cutoff in the
			// future selects whatever was just written and one in the past
			// selects nothing.
			future := func() time.Time { return time.Now().Add(time.Hour) }
			past := func() time.Time { return time.Now().Add(-time.Hour) }
			collectable := func(c storage.GCCutoffs, after string, limit int) []string {
				GinkgoHelper()
				ids, err := store.ListCollectableWorkspaceIDs(ctx, c, storage.Range{After: after}, limit)
				Expect(err).NotTo(HaveOccurred())
				return ids
			}
			// all is a pass that would collect every kind, which is what the
			// engine's fullest configuration amounts to.
			all := func() storage.GCCutoffs {
				return storage.GCCutoffs{
					Tombstones:      future(),
					Clients:         future(),
					IdempotencyKeys: future(),
					FieldDeltas:     future(),
					Entities:        []storage.EntityCutoff{{Entity: "audit", Before: future()}},
				}
			}

			DescribeTable("reports a workspace holding one collectable thing",
				func(plant func(tx storage.WriteTx)) {
					write("ws1", func(tx storage.WriteTx) { plant(tx) })
					Expect(collectable(all(), "", 10)).To(Equal([]string{"ws1"}))
				},
				Entry("a tombstone", func(tx storage.WriteTx) {
					Expect(tx.PutRow("widget", "k", []byte("v"), 1, storage.Actor{})).To(Succeed())
					Expect(tx.DeleteRow("widget", "k", 2, storage.Actor{})).To(Succeed())
				}),
				Entry("a client watermark", func(tx storage.WriteTx) {
					Expect(tx.SetClientLastMutationID("c1", 5)).To(Succeed())
				}),
				Entry("a client's recorded verdicts", func(tx storage.WriteTx) {
					Expect(tx.SetClientRejections("c1", []storage.ClientRejection{{MutationID: 1, Code: "conflict"}})).To(Succeed())
				}),
				Entry("an effect consumer position", func(tx storage.WriteTx) {
					Expect(tx.SetConsumerPosition("indexer", storage.ConsumerPosition{Seq: 1})).To(Succeed())
				}),
				Entry("an idempotency key", func(tx storage.WriteTx) {
					Expect(tx.PutIdempotencyRecord("k1", storage.IdempotencyRecord{Seq: 1})).To(Succeed())
				}),
				Entry("a text field delta", func(tx storage.WriteTx) {
					Expect(tx.PutFieldDelta("widget", "k", "body", 1, []byte("frag"))).To(Succeed())
				}),
				Entry("a live row of an aging entity", func(tx storage.WriteTx) {
					Expect(tx.PutRow("audit", "a1", []byte("v"), 1, storage.Actor{})).To(Succeed())
				}),
				Entry("a sequence lineage record", func(tx storage.WriteTx) {
					_, err := tx.NextSeq()
					Expect(err).NotTo(HaveOccurred())
				}),
			)

			It("leaves out a workspace whose lineage records were pruned", func() {
				write("ws1", func(tx storage.WriteTx) {
					_, err := tx.NextSeq()
					Expect(err).NotTo(HaveOccurred())
				})
				write("ws1", func(tx storage.WriteTx) {
					_, err := tx.PruneSeqLineage(future())
					Expect(err).NotTo(HaveOccurred())
				})
				Expect(collectable(storage.GCCutoffs{Tombstones: future()}, "", 10)).To(BeEmpty())
			})

			It("reports nothing for a store with no workspaces", func() {
				Expect(collectable(all(), "", 10)).To(BeEmpty())
			})

			It("leaves out a workspace holding only live rows", func() {
				// The case the whole query exists for: an idle workspace must
				// cost the pass nothing at all.
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutRow("widget", "k", []byte("v"), 1, storage.Actor{})).To(Succeed())
				})
				Expect(collectable(all(), "", 10)).To(BeEmpty())
			})

			It("leaves out a workspace whose candidates are all younger than the cutoffs", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutRow("widget", "k", []byte("v"), 1, storage.Actor{})).To(Succeed())
					Expect(tx.DeleteRow("widget", "k", 2, storage.Actor{})).To(Succeed())
					Expect(tx.SetClientLastMutationID("c1", 5)).To(Succeed())
					Expect(tx.PutIdempotencyRecord("k1", storage.IdempotencyRecord{Seq: 1})).To(Succeed())
					Expect(tx.PutFieldDelta("widget", "k", "body", 1, []byte("frag"))).To(Succeed())
				})
				Expect(collectable(storage.GCCutoffs{
					Tombstones:      past(),
					IdempotencyKeys: past(),
					FieldDeltas:     past(),
				}, "", 10)).To(BeEmpty())
			})

			It("treats a zero cutoff as keeping that kind forever", func() {
				// Which is not what its UnixMilli() says: the distinction is how
				// unbounded retention reaches an adapter, and reading it as an
				// epoch timestamp would collect everything instead.
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.SetClientLastMutationID("c1", 5)).To(Succeed())
				})
				Expect(collectable(storage.GCCutoffs{}, "", 10)).To(BeEmpty(),
					"a pass with no cutoffs at all collects nothing and has nowhere to go")
				Expect(collectable(storage.GCCutoffs{Clients: future()}, "", 10)).To(Equal([]string{"ws1"}))
			})

			It("leaves out an idle watermark on a pass that keeps watermarks", func() {
				// The default pass: verdicts expire on the tombstone cutoff, the
				// watermark itself is never collected. A bare watermark then
				// holds nothing for the pass to do.
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.SetClientLastMutationID("c1", 5)).To(Succeed())
				})
				Expect(collectable(storage.GCCutoffs{Tombstones: future()}, "", 10)).To(BeEmpty())
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.SetClientRejections("c1", []storage.ClientRejection{{MutationID: 5, Code: "conflict"}})).To(Succeed())
				})
				Expect(collectable(storage.GCCutoffs{Tombstones: future()}, "", 10)).To(Equal([]string{"ws1"}),
					"recorded verdicts are what the tombstone cutoff collects from a client")
			})

			It("names an entity cutoff's own entity", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutRow("widget", "k", []byte("v"), 1, storage.Actor{})).To(Succeed())
				})
				Expect(collectable(storage.GCCutoffs{
					Entities: []storage.EntityCutoff{{Entity: "audit", Before: future()}},
				}, "", 10)).To(BeEmpty(), "a live row of another entity is not the purge's to take")
			})

			It("pages in id order from an exclusive cursor", func() {
				for _, id := range []string{"ws3", "ws1", "ws2"} {
					write(id, func(tx storage.WriteTx) {
						Expect(tx.SetClientLastMutationID("c1", 1)).To(Succeed())
					})
				}
				Expect(collectable(all(), "", 2)).To(Equal([]string{"ws1", "ws2"}))
				Expect(collectable(all(), "ws2", 2)).To(Equal([]string{"ws3"}))
				Expect(collectable(all(), "ws3", 2)).To(BeEmpty(),
					"paging terminates rather than wrapping")
			})

			It("rejects a non-positive limit", func() {
				_, err := store.ListCollectableWorkspaceIDs(ctx, all(), storage.Range{}, 0)
				Expect(err).To(HaveOccurred())
				_, err = store.ListCollectableWorkspaceIDs(ctx, all(), storage.Range{}, -1)
				Expect(err).To(HaveOccurred())
			})
		})

		// The build mark is one opaque string per workspace, compared for
		// equality and nothing else. It is what lets the index pass skip a
		// workspace without reading the state of each of its indexes.
		Describe("index build mark", func() {
			stale := func(mark, after string, limit int) []string {
				GinkgoHelper()
				ids, err := store.ListStaleIndexWorkspaceIDs(ctx, mark, storage.Range{After: after}, limit)
				Expect(err).NotTo(HaveOccurred())
				return ids
			}
			seed := func(ws string) {
				GinkgoHelper()
				write(ws, func(tx storage.WriteTx) {
					Expect(tx.PutRow("widget", "k", []byte("v"), 1, storage.Actor{})).To(Succeed())
				})
			}

			It("reports a workspace that has never been marked", func() {
				seed("ws1")
				Expect(stale("m1", "", 10)).To(Equal([]string{"ws1"}))
			})

			It("drops a workspace once marked, and reports it again under a new mark", func() {
				seed("ws1")
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.SetIndexBuildMark("m1")).To(Succeed())
				})
				Expect(stale("m1", "", 10)).To(BeEmpty())
				Expect(stale("m2", "", 10)).To(Equal([]string{"ws1"}),
					"a changed declaration re-selects every workspace exactly once")
			})

			It("overwrites the mark rather than failing on the one already there", func() {
				seed("ws1")
				for _, m := range []string{"m1", "m2"} {
					write("ws1", func(tx storage.WriteTx) {
						Expect(tx.SetIndexBuildMark(m)).To(Succeed())
					})
				}
				Expect(stale("m2", "", 10)).To(BeEmpty())
			})

			It("does not register a workspace it marks", func() {
				// A workspace holding nothing has no rows to index, so marking
				// one is vacuous — and registering it here would let a build on
				// an arbitrary id mint a workspace every later pass iterates.
				write("ghost", func(tx storage.WriteTx) {
					Expect(tx.SetIndexBuildMark("m1")).To(Succeed())
				})
				Expect(store.ListWorkspaceIDs(ctx, storage.Range{})).To(BeEmpty())
			})

			It("keeps the mark across an unrelated write", func() {
				seed("ws1")
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.SetIndexBuildMark("m1")).To(Succeed())
				})
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutRow("widget", "k2", []byte("v"), 2, storage.Actor{})).To(Succeed())
				})
				Expect(stale("m1", "", 10)).To(BeEmpty(),
					"a row written after the build is indexed by the write path, not by a rebuild")
			})

			It("pages in id order from an exclusive cursor", func() {
				for _, id := range []string{"ws3", "ws1", "ws2"} {
					seed(id)
				}
				Expect(stale("m1", "", 2)).To(Equal([]string{"ws1", "ws2"}))
				Expect(stale("m1", "ws2", 2)).To(Equal([]string{"ws3"}))
				Expect(stale("m1", "ws3", 2)).To(BeEmpty())
			})

			It("rejects a non-positive limit", func() {
				_, err := store.ListStaleIndexWorkspaceIDs(ctx, "m1", storage.Range{}, 0)
				Expect(err).To(HaveOccurred())
				_, err = store.ListStaleIndexWorkspaceIDs(ctx, "m1", storage.Range{}, -1)
				Expect(err).To(HaveOccurred())
			})
		})

		// The compaction mark is a position in ScanChanges order plus the
		// threshold the sweep that recorded it ran under. What selects a
		// workspace is that position sitting below the field-delta horizon: the
		// prune has quietened rows the sweep has not yet considered.
		Describe("text compaction mark", func() {
			compactable := func(minPurged int, after string, limit int) []string {
				GinkgoHelper()
				ids, err := store.ListCompactableWorkspaceIDs(ctx, minPurged, storage.Range{After: after}, limit)
				Expect(err).NotTo(HaveOccurred())
				return ids
			}
			// Puts a row, logs a delta for it and prunes through it, which is
			// what a workspace with text edited past the retention window looks
			// like to the sweep.
			seedQuiet := func(ws string, seq uint64) {
				GinkgoHelper()
				write(ws, func(tx storage.WriteTx) {
					Expect(tx.PutRow("widget", "k", []byte("v"), seq, storage.Actor{})).To(Succeed())
					Expect(tx.PutFieldDelta("widget", "k", "body", seq, []byte("frag"))).To(Succeed())
				})
				write(ws, func(tx storage.WriteTx) {
					horizon, err := tx.PruneFieldDeltas(time.Now().Add(time.Hour))
					Expect(err).NotTo(HaveOccurred())
					Expect(horizon).To(BeEquivalentTo(seq))
				})
			}

			It("reports a workspace whose horizon has moved past its mark", func() {
				seedQuiet("ws1", 4)
				Expect(compactable(1, "", 10)).To(Equal([]string{"ws1"}))
			})

			It("omits a workspace whose log has never been pruned", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutRow("widget", "k", []byte("v"), 1, storage.Actor{})).To(Succeed())
				})
				Expect(compactable(1, "", 10)).To(BeEmpty(),
					"no prune, so no row is quiet and there is nothing to sweep")
			})

			It("drops a workspace once the mark reaches the horizon", func() {
				seedQuiet("ws1", 4)
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.SetTextCompactMark(storage.TextCompactMark{Seq: 4, MinPurged: 1})).To(Succeed())
				})
				Expect(compactable(1, "", 10)).To(BeEmpty())
			})

			It("keeps a workspace selected while the mark holds a sub-position", func() {
				seedQuiet("ws1", 4)
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.SetTextCompactMark(storage.TextCompactMark{
						Seq: 4, Entity: "widget", Key: "k", MinPurged: 1,
					})).To(Succeed())
				})
				Expect(compactable(1, "", 10)).To(Equal([]string{"ws1"}),
					"a mark inside a same-sequence group has rows after it, however far the horizon has come")
			})

			It("re-selects a workspace when the threshold is lowered, but not when it is raised", func() {
				seedQuiet("ws1", 4)
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.SetTextCompactMark(storage.TextCompactMark{Seq: 4, MinPurged: 10})).To(Succeed())
				})
				Expect(compactable(20, "", 10)).To(BeEmpty(),
					"a row skipped under 10 is skipped under 20 too")
				Expect(compactable(5, "", 10)).To(Equal([]string{"ws1"}),
					"a row skipped under 10 may clear 5")
			})

			It("round-trips the mark, sub-position and threshold", func() {
				seedQuiet("ws1", 4)
				want := storage.TextCompactMark{Seq: 3, Entity: "widget", Key: "k", MinPurged: 7}
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.SetTextCompactMark(want)).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					Expect(tx.TextCompactMark()).To(Equal(want))
				})
			})

			It("reads the zero mark for a workspace no sweep has reached", func() {
				seedQuiet("ws1", 4)
				read("ws1", func(tx storage.ReadTx) {
					Expect(tx.TextCompactMark()).To(Equal(storage.TextCompactMark{}))
				})
			})

			It("reports the horizon the prune advanced to", func() {
				seedQuiet("ws1", 4)
				read("ws1", func(tx storage.ReadTx) {
					Expect(tx.FieldDeltaHorizon()).To(BeEquivalentTo(4))
				})
			})

			It("reads a zero horizon before anything is pruned", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutRow("widget", "k", []byte("v"), 1, storage.Actor{})).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					Expect(tx.FieldDeltaHorizon()).To(BeEquivalentTo(0))
				})
			})

			It("does not register a workspace it marks", func() {
				write("ghost", func(tx storage.WriteTx) {
					Expect(tx.SetTextCompactMark(storage.TextCompactMark{Seq: 1})).To(Succeed())
				})
				Expect(store.ListWorkspaceIDs(ctx, storage.Range{})).To(BeEmpty())
			})

			It("pages in id order from an exclusive cursor", func() {
				for i, id := range []string{"ws3", "ws1", "ws2"} {
					seedQuiet(id, uint64(i+1))
				}
				Expect(compactable(1, "", 2)).To(Equal([]string{"ws1", "ws2"}))
				Expect(compactable(1, "ws2", 2)).To(Equal([]string{"ws3"}))
				Expect(compactable(1, "ws3", 2)).To(BeEmpty())
			})

			It("rejects a non-positive limit", func() {
				_, err := store.ListCompactableWorkspaceIDs(ctx, 1, storage.Range{}, 0)
				Expect(err).To(HaveOccurred())
				_, err = store.ListCompactableWorkspaceIDs(ctx, 1, storage.Range{}, -1)
				Expect(err).To(HaveOccurred())
			})
		})

		It("tracks client watermarks", func() {
			write("ws1", func(tx storage.WriteTx) {
				Expect(tx.SetClientLastMutationID("c1", 5)).To(Succeed())
			})
			write("ws1", func(tx storage.WriteTx) {
				Expect(tx.SetClientLastMutationID("c1", 6)).To(Succeed())
				Expect(tx.SetClientLastMutationID("c2", 1)).To(Succeed())
			})
			read("ws1", func(tx storage.ReadTx) {
				id, err := tx.ClientLastMutationID("c1")
				Expect(err).NotTo(HaveOccurred())
				Expect(id).To(BeEquivalentTo(6))
			})
		})

		It("records client rejections and replaces them wholesale", func() {
			write("ws1", func(tx storage.WriteTx) {
				Expect(tx.SetClientRejections("c1", []storage.ClientRejection{
					{MutationID: 4, Code: "conflict", Message: "stale version"},
					{MutationID: 7, Code: "permission_denied", Message: "not yours"},
				})).To(Succeed())
				Expect(tx.SetClientRejections("c2", []storage.ClientRejection{
					{MutationID: 1, Code: "invalid_argument"},
				})).To(Succeed())
			})
			read("ws1", func(tx storage.ReadTx) {
				recs, err := tx.ClientRejections("c1")
				Expect(err).NotTo(HaveOccurred())
				Expect(recs).To(Equal([]storage.ClientRejection{
					{MutationID: 4, Code: "conflict", Message: "stale version"},
					{MutationID: 7, Code: "permission_denied", Message: "not yours"},
				}))

				recs, err = tx.ClientRejections("c2")
				Expect(err).NotTo(HaveOccurred())
				Expect(recs).To(HaveLen(1))

				recs, err = tx.ClientRejections("never-seen")
				Expect(err).NotTo(HaveOccurred())
				Expect(recs).To(BeEmpty(), "an unknown client has no verdicts, and that is not an error")
			})

			// A replacement, not a merge: the engine keeps the whole list.
			write("ws1", func(tx storage.WriteTx) {
				Expect(tx.SetClientRejections("c1", []storage.ClientRejection{
					{MutationID: 9, Code: "conflict"},
				})).To(Succeed())
			})
			read("ws1", func(tx storage.ReadTx) {
				recs, err := tx.ClientRejections("c1")
				Expect(err).NotTo(HaveOccurred())
				Expect(recs).To(Equal([]storage.ClientRejection{{MutationID: 9, Code: "conflict"}}))
			})

			write("ws1", func(tx storage.WriteTx) {
				Expect(tx.SetClientRejections("c1", nil)).To(Succeed())
			})
			read("ws1", func(tx storage.ReadTx) {
				recs, err := tx.ClientRejections("c1")
				Expect(err).NotTo(HaveOccurred())
				Expect(recs).To(BeEmpty())
			})
		})

		It("keeps a client's watermark and its rejections independent", func() {
			write("ws1", func(tx storage.WriteTx) {
				Expect(tx.SetClientLastMutationID("c1", 3)).To(Succeed())
				Expect(tx.SetClientRejections("c1", []storage.ClientRejection{{MutationID: 3, Code: "conflict"}})).To(Succeed())
			})
			// Neither write may clobber the other's column.
			write("ws1", func(tx storage.WriteTx) {
				Expect(tx.SetClientLastMutationID("c1", 4)).To(Succeed())
			})
			read("ws1", func(tx storage.ReadTx) {
				id, err := tx.ClientLastMutationID("c1")
				Expect(err).NotTo(HaveOccurred())
				Expect(id).To(BeEquivalentTo(4))
				recs, err := tx.ClientRejections("c1")
				Expect(err).NotTo(HaveOccurred())
				Expect(recs).To(HaveLen(1))
			})
		})

		It("drops a pruned client's rejections with its watermark", func() {
			write("ws1", func(tx storage.WriteTx) {
				Expect(tx.SetClientLastMutationID("idle", 1)).To(Succeed())
				Expect(tx.SetClientRejections("idle", []storage.ClientRejection{{MutationID: 1, Code: "conflict"}})).To(Succeed())
			})
			write("ws1", func(tx storage.WriteTx) {
				removed, err := tx.CollectIdleClients(time.Now().Add(time.Hour))
				Expect(err).NotTo(HaveOccurred())
				Expect(removed).To(Equal(1))
			})
			read("ws1", func(tx storage.ReadTx) {
				recs, err := tx.ClientRejections("idle")
				Expect(err).NotTo(HaveOccurred())
				Expect(recs).To(BeEmpty())
			})
		})

		Describe("verdict expiry", func() {
			It("clears an idle client's rejections and keeps its watermark", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.SetClientLastMutationID("idle", 3)).To(Succeed())
					Expect(tx.SetClientRejections("idle", []storage.ClientRejection{{MutationID: 3, Code: "conflict"}})).To(Succeed())
				})
				write("ws1", func(tx storage.WriteTx) {
					expired, err := tx.ExpireClientRejections(time.Now().Add(time.Hour))
					Expect(err).NotTo(HaveOccurred())
					Expect(expired).To(Equal(1))
				})
				read("ws1", func(tx storage.ReadTx) {
					recs, err := tx.ClientRejections("idle")
					Expect(err).NotTo(HaveOccurred())
					Expect(recs).To(BeEmpty())
					id, err := tx.ClientLastMutationID("idle")
					Expect(err).NotTo(HaveOccurred())
					Expect(id).To(BeEquivalentTo(3), "the watermark is not the expiry's to take")
				})
			})

			It("counts only clients that held verdicts", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.SetClientLastMutationID("bare", 1)).To(Succeed())
					Expect(tx.SetClientLastMutationID("judged", 1)).To(Succeed())
					Expect(tx.SetClientRejections("judged", []storage.ClientRejection{{MutationID: 1, Code: "conflict"}})).To(Succeed())
				})
				write("ws1", func(tx storage.WriteTx) {
					expired, err := tx.ExpireClientRejections(time.Now().Add(time.Hour))
					Expect(err).NotTo(HaveOccurred())
					Expect(expired).To(Equal(1))
					expired, err = tx.ExpireClientRejections(time.Now().Add(time.Hour))
					Expect(err).NotTo(HaveOccurred())
					Expect(expired).To(BeZero(), "a second pass finds nothing left to expire")
				})
			})

			It("spares verdicts recorded since the cutoff", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.SetClientRejections("old", []storage.ClientRejection{{MutationID: 1, Code: "conflict"}})).To(Succeed())
				})
				cutoff := time.Now().Add(time.Millisecond)
				time.Sleep(5 * time.Millisecond)
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.SetClientRejections("old", []storage.ClientRejection{{MutationID: 2, Code: "conflict"}})).To(Succeed(), "refresh moves old past the cutoff")
					Expect(tx.SetClientRejections("fresh", []storage.ClientRejection{{MutationID: 1, Code: "conflict"}})).To(Succeed())
				})
				write("ws1", func(tx storage.WriteTx) {
					expired, err := tx.ExpireClientRejections(cutoff)
					Expect(err).NotTo(HaveOccurred())
					Expect(expired).To(BeZero())
				})
				read("ws1", func(tx storage.ReadTx) {
					recs, err := tx.ClientRejections("old")
					Expect(err).NotTo(HaveOccurred())
					Expect(recs).To(HaveLen(1))
					recs, err = tx.ClientRejections("fresh")
					Expect(err).NotTo(HaveOccurred())
					Expect(recs).To(HaveLen(1))
				})
			})

			It("does not make an idle client look active", func() {
				// The expiry runs on every pass, so if it refreshed the record's
				// last-written time an idle client would never be collectable.
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.SetClientLastMutationID("idle", 1)).To(Succeed())
					Expect(tx.SetClientRejections("idle", []storage.ClientRejection{{MutationID: 1, Code: "conflict"}})).To(Succeed())
				})
				cutoff := time.Now().Add(time.Millisecond)
				time.Sleep(5 * time.Millisecond)
				write("ws1", func(tx storage.WriteTx) {
					expired, err := tx.ExpireClientRejections(cutoff)
					Expect(err).NotTo(HaveOccurred())
					Expect(expired).To(Equal(1))
				})
				write("ws1", func(tx storage.WriteTx) {
					removed, err := tx.CollectIdleClients(cutoff)
					Expect(err).NotTo(HaveOccurred())
					Expect(removed).To(Equal(1), "the record is as idle as it was before the expiry")
				})
			})

			It("scopes expiry to the transaction's workspace", func() {
				for _, ws := range []string{"ws1", "ws2"} {
					write(ws, func(tx storage.WriteTx) {
						Expect(tx.SetClientRejections("c1", []storage.ClientRejection{{MutationID: 1, Code: "conflict"}})).To(Succeed())
					})
				}
				write("ws1", func(tx storage.WriteTx) {
					expired, err := tx.ExpireClientRejections(time.Now().Add(time.Hour))
					Expect(err).NotTo(HaveOccurred())
					Expect(expired).To(Equal(1))
				})
				read("ws2", func(tx storage.ReadTx) {
					recs, err := tx.ClientRejections("c1")
					Expect(err).NotTo(HaveOccurred())
					Expect(recs).To(HaveLen(1), "other workspaces keep their verdicts")
				})
			})
		})

		It("rolls back the whole transaction when fn errors", func() {
			boom := errors.New("boom")
			err := store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
				Expect(tx.PutRow("widget", "k1", []byte("v1"), 1, storage.Actor{})).To(Succeed())
				if _, err := tx.NextSeq(); err != nil {
					return err
				}
				return boom
			})
			Expect(err).To(MatchError(boom))
			read("ws1", func(tx storage.ReadTx) {
				_, err := tx.GetRow("widget", "k1")
				Expect(err).To(MatchError(storage.ErrNotFound))
				seq, err := tx.CurrentSeq()
				Expect(err).NotTo(HaveOccurred())
				Expect(seq).To(BeZero(), "allocated seq must roll back with the tx")
			})
		})

		It("supports concurrent writes to different workspaces", func() {
			done := make(chan error, 2)
			for _, ws := range []string{"wsA", "wsB"} {
				go func() {
					done <- store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
						for i := 0; i < 20; i++ {
							seq, err := tx.NextSeq()
							if err != nil {
								return err
							}
							if err := tx.PutRow("widget", "k", []byte("v"), seq, storage.Actor{}); err != nil {
								return err
							}
						}
						return nil
					})
				}()
			}
			Expect(<-done).To(Succeed())
			Expect(<-done).To(Succeed())
			for _, ws := range []string{"wsA", "wsB"} {
				read(ws, func(tx storage.ReadTx) {
					seq, err := tx.CurrentSeq()
					Expect(err).NotTo(HaveOccurred())
					Expect(seq).To(BeEquivalentTo(20))
				})
			}
		})

		It("serializes same-workspace write transactions for their whole duration", func() {
			// Read-then-write through WithWriteTx, mirroring the push path's
			// watermark check. The read happens BEFORE the first write: an
			// adapter that only locks the workspace at first write (instead of
			// before invoking fn, per the contract) lets two transactions read
			// the same watermark and lose an update — this spec then fails.
			const writers = 8
			done := make(chan error, writers)
			for i := 0; i < writers; i++ {
				go func() {
					done <- store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
						last, err := tx.ClientLastMutationID("c")
						if err != nil {
							return err
						}
						if _, err := tx.NextSeq(); err != nil {
							return err
						}
						return tx.SetClientLastMutationID("c", last+1)
					})
				}()
			}
			for i := 0; i < writers; i++ {
				Expect(<-done).To(Succeed())
			}
			read("ws1", func(tx storage.ReadTx) {
				id, err := tx.ClientLastMutationID("c")
				Expect(err).NotTo(HaveOccurred())
				Expect(id).To(BeEquivalentTo(writers), "a lost update means fn ran without mutual exclusion")
				seq, err := tx.CurrentSeq()
				Expect(err).NotTo(HaveOccurred())
				Expect(seq).To(BeEquivalentTo(writers))
			})
		})

		Describe("idle-client collection", func() {
			It("removes clients past the cutoff and reports the count", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.SetClientLastMutationID("c1", 3)).To(Succeed())
					Expect(tx.SetClientLastMutationID("c2", 5)).To(Succeed())
				})
				write("ws1", func(tx storage.WriteTx) {
					removed, err := tx.CollectIdleClients(time.Now().Add(time.Hour))
					Expect(err).NotTo(HaveOccurred())
					Expect(removed).To(Equal(2))
				})
				read("ws1", func(tx storage.ReadTx) {
					id, err := tx.ClientLastMutationID("c1")
					Expect(err).NotTo(HaveOccurred())
					Expect(id).To(BeZero(), "pruned client reads as unknown")
				})
			})

			It("spares clients updated since the cutoff", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.SetClientLastMutationID("old", 1)).To(Succeed())
				})
				cutoff := time.Now().Add(time.Millisecond)
				time.Sleep(5 * time.Millisecond)
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.SetClientLastMutationID("old", 2)).To(Succeed(), "refresh moves old past the cutoff")
					Expect(tx.SetClientLastMutationID("fresh", 1)).To(Succeed())
				})
				write("ws1", func(tx storage.WriteTx) {
					removed, err := tx.CollectIdleClients(cutoff)
					Expect(err).NotTo(HaveOccurred())
					Expect(removed).To(BeZero())
				})
				read("ws1", func(tx storage.ReadTx) {
					id, err := tx.ClientLastMutationID("old")
					Expect(err).NotTo(HaveOccurred())
					Expect(id).To(BeEquivalentTo(2))
					id, err = tx.ClientLastMutationID("fresh")
					Expect(err).NotTo(HaveOccurred())
					Expect(id).To(BeEquivalentTo(1))
				})
			})

			It("scopes collection to the transaction's workspace", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.SetClientLastMutationID("c1", 1)).To(Succeed())
				})
				write("ws2", func(tx storage.WriteTx) {
					Expect(tx.SetClientLastMutationID("c1", 9)).To(Succeed())
				})
				write("ws1", func(tx storage.WriteTx) {
					removed, err := tx.CollectIdleClients(time.Now().Add(time.Hour))
					Expect(err).NotTo(HaveOccurred())
					Expect(removed).To(Equal(1))
				})
				read("ws2", func(tx storage.ReadTx) {
					id, err := tx.ClientLastMutationID("c1")
					Expect(err).NotTo(HaveOccurred())
					Expect(id).To(BeEquivalentTo(9), "other workspaces keep their watermarks")
				})
			})
		})

		Describe("idempotency ledger", func() {
			It("reads an unclaimed key as absent", func() {
				read("ws1", func(tx storage.ReadTx) {
					_, found, err := tx.IdempotencyRecord("k")
					Expect(err).NotTo(HaveOccurred())
					Expect(found).To(BeFalse())
				})
			})

			It("replays the recorded outcome of a claimed key", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutIdempotencyRecord("k", storage.IdempotencyRecord{
						Seq: 7, Result: []byte("body"),
					})).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					rec, found, err := tx.IdempotencyRecord("k")
					Expect(err).NotTo(HaveOccurred())
					Expect(found).To(BeTrue())
					Expect(rec.Seq).To(BeEquivalentTo(7))
					Expect(rec.Result).To(Equal([]byte("body")))
				})
			})

			It("records a key claimed without a result as present but empty", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutIdempotencyRecord("k", storage.IdempotencyRecord{Seq: 3})).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					rec, found, err := tx.IdempotencyRecord("k")
					Expect(err).NotTo(HaveOccurred())
					Expect(found).To(BeTrue(), "no result is not the same as no claim")
					Expect(rec.Seq).To(BeEquivalentTo(3))
					Expect(rec.Result).To(BeEmpty())
				})
			})

			It("overwrites a re-claimed key, so a retried write closure is idempotent", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutIdempotencyRecord("k", storage.IdempotencyRecord{Seq: 1, Result: []byte("first")})).To(Succeed())
					Expect(tx.PutIdempotencyRecord("k", storage.IdempotencyRecord{Seq: 2, Result: []byte("second")})).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					rec, _, err := tx.IdempotencyRecord("k")
					Expect(err).NotTo(HaveOccurred())
					Expect(rec.Seq).To(BeEquivalentTo(2))
					Expect(rec.Result).To(Equal([]byte("second")))
				})
			})

			It("leaves the key unclaimed when its transaction rolls back", func() {
				// The claim is only as durable as the write it guards: a closure
				// that fails must leave the write retryable, not recorded.
				boom := errors.New("boom")
				Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
					Expect(tx.PutRow("widget", "k", []byte("v"), 1, storage.Actor{})).To(Succeed())
					Expect(tx.PutIdempotencyRecord("k", storage.IdempotencyRecord{Seq: 1})).To(Succeed())
					return boom
				})).To(MatchError(boom))
				read("ws1", func(tx storage.ReadTx) {
					_, found, err := tx.IdempotencyRecord("k")
					Expect(err).NotTo(HaveOccurred())
					Expect(found).To(BeFalse())
				})
			})

			It("scopes keys to the workspace", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutIdempotencyRecord("k", storage.IdempotencyRecord{Seq: 1})).To(Succeed())
				})
				read("ws2", func(tx storage.ReadTx) {
					_, found, err := tx.IdempotencyRecord("k")
					Expect(err).NotTo(HaveOccurred())
					Expect(found).To(BeFalse(), "the same key in another workspace is a different claim")
				})
			})

			It("registers the workspace, since a claim is persisted state", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutIdempotencyRecord("k", storage.IdempotencyRecord{Seq: 1})).To(Succeed())
				})
				Expect(store.ListWorkspaceIDs(ctx, storage.Range{})).To(ConsistOf("ws1"))
			})

			It("removes keys past the cutoff and reports the count", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutIdempotencyRecord("k1", storage.IdempotencyRecord{Seq: 1})).To(Succeed())
					Expect(tx.PutIdempotencyRecord("k2", storage.IdempotencyRecord{Seq: 2})).To(Succeed())
				})
				write("ws1", func(tx storage.WriteTx) {
					removed, err := tx.CollectIdempotencyKeys(time.Now().Add(time.Hour))
					Expect(err).NotTo(HaveOccurred())
					Expect(removed).To(Equal(2))
				})
				read("ws1", func(tx storage.ReadTx) {
					_, found, err := tx.IdempotencyRecord("k1")
					Expect(err).NotTo(HaveOccurred())
					Expect(found).To(BeFalse(), "an expired key reads as never claimed, so the write applies again")
				})
			})

			It("spares keys claimed since the cutoff", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutIdempotencyRecord("old", storage.IdempotencyRecord{Seq: 1})).To(Succeed())
				})
				cutoff := time.Now().Add(time.Millisecond)
				time.Sleep(5 * time.Millisecond)
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutIdempotencyRecord("fresh", storage.IdempotencyRecord{Seq: 2})).To(Succeed())
				})
				write("ws1", func(tx storage.WriteTx) {
					removed, err := tx.CollectIdempotencyKeys(cutoff)
					Expect(err).NotTo(HaveOccurred())
					Expect(removed).To(Equal(1), "only the key older than the cutoff goes")
				})
				read("ws1", func(tx storage.ReadTx) {
					_, found, err := tx.IdempotencyRecord("fresh")
					Expect(err).NotTo(HaveOccurred())
					Expect(found).To(BeTrue())
				})
			})

			It("scopes collection to the transaction's workspace", func() {
				for _, id := range []string{"ws1", "ws2"} {
					write(id, func(tx storage.WriteTx) {
						Expect(tx.PutIdempotencyRecord("k", storage.IdempotencyRecord{Seq: 1})).To(Succeed())
					})
				}
				write("ws1", func(tx storage.WriteTx) {
					removed, err := tx.CollectIdempotencyKeys(time.Now().Add(time.Hour))
					Expect(err).NotTo(HaveOccurred())
					Expect(removed).To(Equal(1))
				})
				read("ws2", func(tx storage.ReadTx) {
					_, found, err := tx.IdempotencyRecord("k")
					Expect(err).NotTo(HaveOccurred())
					Expect(found).To(BeTrue(), "other workspaces keep their claims")
				})
			})
		})

		Describe("effect-consumer state", func() {
			It("reads an unknown consumer as a first run", func() {
				read("ws1", func(tx storage.ReadTx) {
					_, found, err := tx.ConsumerPosition("indexer")
					Expect(err).NotTo(HaveOccurred())
					Expect(found).To(BeFalse())
					shadow, err := tx.ConsumerShadow("indexer", "widget", "k")
					Expect(err).NotTo(HaveOccurred())
					Expect(shadow).To(BeNil())
					all, err := tx.ListConsumerPositions()
					Expect(err).NotTo(HaveOccurred())
					Expect(all).To(BeEmpty())
				})
			})

			It("round-trips a position including its sub-position", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.SetConsumerPosition("indexer", storage.ConsumerPosition{
						Seq: 9, Entity: "widget", Key: "k4",
					})).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					pos, found, err := tx.ConsumerPosition("indexer")
					Expect(err).NotTo(HaveOccurred())
					Expect(found).To(BeTrue())
					Expect(pos.Consumer).To(Equal("indexer"))
					Expect(pos.Seq).To(BeEquivalentTo(9))
					Expect(pos.Entity).To(Equal("widget"))
					Expect(pos.Key).To(Equal("k4"))
					Expect(pos.UpdatedAt).To(BeTemporally("~", time.Now(), time.Minute),
						"the adapter stamps its own clock on write")
				})
			})

			It("overwrites a position in place and keeps consumers apart", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.SetConsumerPosition("indexer", storage.ConsumerPosition{Seq: 1})).To(Succeed())
					Expect(tx.SetConsumerPosition("indexer", storage.ConsumerPosition{Seq: 4})).To(Succeed())
					Expect(tx.SetConsumerPosition("webhook", storage.ConsumerPosition{Seq: 2})).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					all, err := tx.ListConsumerPositions()
					Expect(err).NotTo(HaveOccurred())
					Expect(all).To(HaveLen(2))
					Expect(all[0].Consumer).To(Equal("indexer"), "ordered by consumer name")
					Expect(all[0].Seq).To(BeEquivalentTo(4))
					Expect(all[1].Consumer).To(Equal("webhook"))
					Expect(all[1].Seq).To(BeEquivalentTo(2))
				})
			})

			It("keeps positions and shadows per workspace", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.SetConsumerPosition("indexer", storage.ConsumerPosition{Seq: 3})).To(Succeed())
					Expect(tx.PutConsumerShadow("indexer", "widget", "k", []byte("ws1"))).To(Succeed())
				})
				write("ws2", func(tx storage.WriteTx) {
					Expect(tx.SetConsumerPosition("indexer", storage.ConsumerPosition{Seq: 8})).To(Succeed())
					Expect(tx.PutConsumerShadow("indexer", "widget", "k", []byte("ws2"))).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					pos, _, err := tx.ConsumerPosition("indexer")
					Expect(err).NotTo(HaveOccurred())
					Expect(pos.Seq).To(BeEquivalentTo(3))
					shadow, err := tx.ConsumerShadow("indexer", "widget", "k")
					Expect(err).NotTo(HaveOccurred())
					Expect(shadow).To(Equal([]byte("ws1")))
				})
			})

			It("round-trips shadows and replaces them in place", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutConsumerShadow("indexer", "widget", "k", []byte("before"))).To(Succeed())
					Expect(tx.PutConsumerShadow("indexer", "widget", "k", []byte("after"))).To(Succeed())
					Expect(tx.PutConsumerShadow("indexer", "gadget", "k", []byte("other entity"))).To(Succeed())
					Expect(tx.PutConsumerShadow("webhook", "widget", "k", []byte("other consumer"))).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					shadow, err := tx.ConsumerShadow("indexer", "widget", "k")
					Expect(err).NotTo(HaveOccurred())
					Expect(shadow).To(Equal([]byte("after")))
					shadow, err = tx.ConsumerShadow("indexer", "gadget", "k")
					Expect(err).NotTo(HaveOccurred())
					Expect(shadow).To(Equal([]byte("other entity")))
					shadow, err = tx.ConsumerShadow("webhook", "widget", "k")
					Expect(err).NotTo(HaveOccurred())
					Expect(shadow).To(Equal([]byte("other consumer")))
				})
			})

			It("deletes one row's shadow without touching the rest", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutConsumerShadow("indexer", "widget", "gone", []byte("a"))).To(Succeed())
					Expect(tx.PutConsumerShadow("indexer", "widget", "kept", []byte("b"))).To(Succeed())
				})
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.DeleteConsumerShadow("indexer", "widget", "gone")).To(Succeed())
					Expect(tx.DeleteConsumerShadow("indexer", "widget", "never")).To(Succeed(),
						"deleting a shadow that does not exist is not an error")
				})
				read("ws1", func(tx storage.ReadTx) {
					shadow, err := tx.ConsumerShadow("indexer", "widget", "gone")
					Expect(err).NotTo(HaveOccurred())
					Expect(shadow).To(BeNil())
					shadow, err = tx.ConsumerShadow("indexer", "widget", "kept")
					Expect(err).NotTo(HaveOccurred())
					Expect(shadow).To(Equal([]byte("b")))
				})
			})

			It("commits a position and the shadows written with it atomically", func() {
				// The property the whole seam exists for: a pass that fails after
				// writing shadows must leave the position where it was, so the
				// change is redelivered against the shadows it was derived from.
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.SetConsumerPosition("indexer", storage.ConsumerPosition{Seq: 1})).To(Succeed())
					Expect(tx.PutConsumerShadow("indexer", "widget", "k", []byte("v1"))).To(Succeed())
				})
				err := store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
					if err := tx.PutConsumerShadow("indexer", "widget", "k", []byte("v2")); err != nil {
						return err
					}
					if err := tx.SetConsumerPosition("indexer", storage.ConsumerPosition{Seq: 2}); err != nil {
						return err
					}
					return errors.New("handler failed after the shadow write")
				})
				Expect(err).To(MatchError(ContainSubstring("handler failed")))
				read("ws1", func(tx storage.ReadTx) {
					pos, _, err := tx.ConsumerPosition("indexer")
					Expect(err).NotTo(HaveOccurred())
					Expect(pos.Seq).To(BeEquivalentTo(1), "the position rolled back with the shadow")
					shadow, err := tx.ConsumerShadow("indexer", "widget", "k")
					Expect(err).NotTo(HaveOccurred())
					Expect(shadow).To(Equal([]byte("v1")), "so redelivery still sees the old shadow")
				})
			})

			It("drops a consumer's position and every shadow it holds", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.SetConsumerPosition("indexer", storage.ConsumerPosition{Seq: 5})).To(Succeed())
					Expect(tx.PutConsumerShadow("indexer", "widget", "k1", []byte("a"))).To(Succeed())
					Expect(tx.PutConsumerShadow("indexer", "widget", "k2", []byte("b"))).To(Succeed())
					Expect(tx.SetConsumerPosition("webhook", storage.ConsumerPosition{Seq: 6})).To(Succeed())
					Expect(tx.PutConsumerShadow("webhook", "widget", "k1", []byte("c"))).To(Succeed())
				})
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.DeleteConsumer("indexer")).To(Succeed())
					Expect(tx.DeleteConsumer("absent")).To(Succeed(),
						"dropping a consumer that does not exist is not an error")
				})
				read("ws1", func(tx storage.ReadTx) {
					_, found, err := tx.ConsumerPosition("indexer")
					Expect(err).NotTo(HaveOccurred())
					Expect(found).To(BeFalse())
					for _, key := range []string{"k1", "k2"} {
						shadow, err := tx.ConsumerShadow("indexer", "widget", key)
						Expect(err).NotTo(HaveOccurred())
						Expect(shadow).To(BeNil())
					}
					pos, found, err := tx.ConsumerPosition("webhook")
					Expect(err).NotTo(HaveOccurred())
					Expect(found).To(BeTrue(), "other consumers are untouched")
					Expect(pos.Seq).To(BeEquivalentTo(6))
					shadow, err := tx.ConsumerShadow("webhook", "widget", "k1")
					Expect(err).NotTo(HaveOccurred())
					Expect(shadow).To(Equal([]byte("c")))
				})
			})

			Describe("idle-consumer collection", func() {
				It("removes stranded consumers with their shadows and reports the count", func() {
					write("ws1", func(tx storage.WriteTx) {
						Expect(tx.SetConsumerPosition("indexer", storage.ConsumerPosition{Seq: 1})).To(Succeed())
						Expect(tx.PutConsumerShadow("indexer", "widget", "k", []byte("a"))).To(Succeed())
						Expect(tx.SetConsumerPosition("webhook", storage.ConsumerPosition{Seq: 2})).To(Succeed())
					})
					write("ws1", func(tx storage.WriteTx) {
						removed, err := tx.CollectIdleConsumers(time.Now().Add(time.Hour), 5)
						Expect(err).NotTo(HaveOccurred())
						Expect(removed).To(Equal(2))
					})
					read("ws1", func(tx storage.ReadTx) {
						all, err := tx.ListConsumerPositions()
						Expect(err).NotTo(HaveOccurred())
						Expect(all).To(BeEmpty())
						shadow, err := tx.ConsumerShadow("indexer", "widget", "k")
						Expect(err).NotTo(HaveOccurred())
						Expect(shadow).To(BeNil(), "shadows go with their consumer")
					})
				})

				It("spares a stale consumer that is still at or above the horizon", func() {
					write("ws1", func(tx storage.WriteTx) {
						Expect(tx.SetConsumerPosition("caughtup", storage.ConsumerPosition{Seq: 7})).To(Succeed())
						Expect(tx.PutConsumerShadow("caughtup", "widget", "k", []byte("a"))).To(Succeed())
						Expect(tx.SetConsumerPosition("stranded", storage.ConsumerPosition{Seq: 2})).To(Succeed())
					})
					write("ws1", func(tx storage.WriteTx) {
						// Both are stale; only the one below the horizon goes. A
						// consumer with nothing to do writes nothing, so age
						// alone cannot mean "gone".
						removed, err := tx.CollectIdleConsumers(time.Now().Add(time.Hour), 7)
						Expect(err).NotTo(HaveOccurred())
						Expect(removed).To(Equal(1))
					})
					read("ws1", func(tx storage.ReadTx) {
						pos, found, err := tx.ConsumerPosition("caughtup")
						Expect(err).NotTo(HaveOccurred())
						Expect(found).To(BeTrue(), "at the horizon is not below it")
						Expect(pos.Seq).To(BeEquivalentTo(7))
						shadow, err := tx.ConsumerShadow("caughtup", "widget", "k")
						Expect(err).NotTo(HaveOccurred())
						Expect(shadow).To(Equal([]byte("a")), "a spared consumer keeps its shadows")
						_, found, err = tx.ConsumerPosition("stranded")
						Expect(err).NotTo(HaveOccurred())
						Expect(found).To(BeFalse())
					})
				})

				It("spares consumers that advanced since the cutoff", func() {
					write("ws1", func(tx storage.WriteTx) {
						Expect(tx.SetConsumerPosition("old", storage.ConsumerPosition{Seq: 1})).To(Succeed())
						Expect(tx.PutConsumerShadow("old", "widget", "k", []byte("a"))).To(Succeed())
					})
					cutoff := time.Now().Add(time.Millisecond)
					time.Sleep(5 * time.Millisecond)
					write("ws1", func(tx storage.WriteTx) {
						Expect(tx.SetConsumerPosition("old", storage.ConsumerPosition{Seq: 2})).To(Succeed(),
							"advancing moves old past the cutoff")
						Expect(tx.SetConsumerPosition("fresh", storage.ConsumerPosition{Seq: 1})).To(Succeed())
					})
					write("ws1", func(tx storage.WriteTx) {
						removed, err := tx.CollectIdleConsumers(cutoff, 99)
						Expect(err).NotTo(HaveOccurred())
						Expect(removed).To(BeZero(), "below the horizon, but not stale")
					})
					read("ws1", func(tx storage.ReadTx) {
						all, err := tx.ListConsumerPositions()
						Expect(err).NotTo(HaveOccurred())
						Expect(all).To(HaveLen(2))
						shadow, err := tx.ConsumerShadow("old", "widget", "k")
						Expect(err).NotTo(HaveOccurred())
						Expect(shadow).To(Equal([]byte("a")), "a spared consumer keeps its shadows")
					})
				})

				It("collects nothing in a workspace that has never collected a tombstone", func() {
					write("ws1", func(tx storage.WriteTx) {
						Expect(tx.SetConsumerPosition("retired", storage.ConsumerPosition{Seq: 0})).To(Succeed())
					})
					write("ws1", func(tx storage.WriteTx) {
						removed, err := tx.CollectIdleConsumers(time.Now().Add(time.Hour), 0)
						Expect(err).NotTo(HaveOccurred())
						Expect(removed).To(BeZero(), "a zero horizon has nothing below it")
					})
				})

				It("scopes collection to the transaction's workspace", func() {
					for _, id := range []string{"ws1", "ws2"} {
						write(id, func(tx storage.WriteTx) {
							Expect(tx.SetConsumerPosition("indexer", storage.ConsumerPosition{Seq: 1})).To(Succeed())
							Expect(tx.PutConsumerShadow("indexer", "widget", "k", []byte("a"))).To(Succeed())
						})
					}
					write("ws1", func(tx storage.WriteTx) {
						removed, err := tx.CollectIdleConsumers(time.Now().Add(time.Hour), 5)
						Expect(err).NotTo(HaveOccurred())
						Expect(removed).To(Equal(1))
					})
					read("ws2", func(tx storage.ReadTx) {
						_, found, err := tx.ConsumerPosition("indexer")
						Expect(err).NotTo(HaveOccurred())
						Expect(found).To(BeTrue(), "other workspaces keep their consumers")
						shadow, err := tx.ConsumerShadow("indexer", "widget", "k")
						Expect(err).NotTo(HaveOccurred())
						Expect(shadow).To(Equal([]byte("a")))
					})
				})
			})

			// The pending query is how a background consumer finds its work
			// without opening a transaction per workspace to discover that
			// almost none of them have any. An omission here is an effect that
			// never fires, so these specs are about completeness first.
			Describe("pending workspaces", func() {
				// seed writes one row at the next sequence, which is what moves
				// the workspace head the pending query compares against.
				seed := func(ws string, key string) {
					GinkgoHelper()
					write(ws, func(tx storage.WriteTx) {
						seq, err := tx.NextSeq()
						Expect(err).NotTo(HaveOccurred())
						Expect(tx.PutRow("widget", key, []byte("v"), seq, storage.Actor{})).To(Succeed())
					})
				}
				pending := func(consumer, after string, limit int) []string {
					GinkgoHelper()
					ids, err := store.ListPendingWorkspaceIDs(ctx, consumer, storage.Range{After: after}, limit)
					Expect(err).NotTo(HaveOccurred())
					return ids
				}

				It("reports a workspace the consumer has never acted on", func() {
					seed("ws1", "k")
					Expect(pending("indexer", "", 10)).To(Equal([]string{"ws1"}),
						"holding no position is a first run, not caught up")
				})

				It("reports nothing for a store with no workspaces", func() {
					Expect(pending("indexer", "", 10)).To(BeEmpty())
				})

				It("drops a workspace once the consumer reaches its head", func() {
					seed("ws1", "k")
					write("ws1", func(tx storage.WriteTx) {
						Expect(tx.SetConsumerPosition("indexer", storage.ConsumerPosition{Seq: 1})).To(Succeed())
					})
					Expect(pending("indexer", "", 10)).To(BeEmpty())

					seed("ws1", "k2")
					Expect(pending("indexer", "", 10)).To(Equal([]string{"ws1"}),
						"a write past the position brings it back")
				})

				It("reports a position parked inside a same-sequence group", func() {
					// A pass that failed part way through a page commits the
					// prefix it delivered, which is a sub-position: the sequence
					// has reached the head but the group has changes after it.
					// Reading that as caught up strands the rest of the group
					// until some unrelated write bumps the head.
					seed("ws1", "k")
					write("ws1", func(tx storage.WriteTx) {
						Expect(tx.SetConsumerPosition("indexer", storage.ConsumerPosition{
							Seq: 1, Entity: "widget", Key: "k",
						})).To(Succeed())
					})
					Expect(pending("indexer", "", 10)).To(Equal([]string{"ws1"}))
				})

				It("keeps consumers apart", func() {
					seed("ws1", "k")
					write("ws1", func(tx storage.WriteTx) {
						Expect(tx.SetConsumerPosition("indexer", storage.ConsumerPosition{Seq: 1})).To(Succeed())
					})
					Expect(pending("indexer", "", 10)).To(BeEmpty())
					Expect(pending("webhook", "", 10)).To(Equal([]string{"ws1"}),
						"one consumer catching up says nothing about another")
				})

				It("pages in id order from an exclusive cursor", func() {
					for _, id := range []string{"ws3", "ws1", "ws2"} {
						seed(id, "k")
					}
					Expect(pending("indexer", "", 2)).To(Equal([]string{"ws1", "ws2"}))
					Expect(pending("indexer", "ws2", 2)).To(Equal([]string{"ws3"}))
					Expect(pending("indexer", "ws3", 2)).To(BeEmpty(),
						"paging terminates rather than wrapping")
				})

				It("rejects a non-positive limit", func() {
					// The caller pages because the answer can be every
					// workspace; "unlimited" is the one thing it must not mean.
					_, err := store.ListPendingWorkspaceIDs(ctx, "indexer", storage.Range{}, 0)
					Expect(err).To(HaveOccurred())
					_, err = store.ListPendingWorkspaceIDs(ctx, "indexer", storage.Range{}, -1)
					Expect(err).To(HaveOccurred())
				})
			})
		})

		Describe("text field delta log", func() {
			It("returns an empty span as complete", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutRow("widget", "k", []byte("v"), 1, storage.Actor{})).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					frags, complete, err := tx.FieldDeltas("widget", "k", "body", 0)
					Expect(err).NotTo(HaveOccurred())
					Expect(complete).To(BeTrue(), "no writes to the field is a definite answer")
					Expect(frags).To(BeEmpty())
				})
			})

			It("returns the fragments above the cursor, oldest first", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutFieldDelta("widget", "k", "body", 1, []byte("one"))).To(Succeed())
					Expect(tx.PutFieldDelta("widget", "k", "body", 2, []byte("two"))).To(Succeed())
					Expect(tx.PutFieldDelta("widget", "k", "body", 3, []byte("three"))).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					frags, complete, err := tx.FieldDeltas("widget", "k", "body", 0)
					Expect(err).NotTo(HaveOccurred())
					Expect(complete).To(BeTrue())
					Expect(frags).To(Equal([][]byte{[]byte("one"), []byte("two"), []byte("three")}))

					frags, complete, err = tx.FieldDeltas("widget", "k", "body", 2)
					Expect(err).NotTo(HaveOccurred())
					Expect(complete).To(BeTrue())
					Expect(frags).To(Equal([][]byte{[]byte("three")}), "the span is exclusive of the cursor")
				})
			})

			It("keeps rows, keys and fields apart", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutFieldDelta("widget", "k1", "body", 1, []byte("a"))).To(Succeed())
					Expect(tx.PutFieldDelta("widget", "k2", "body", 2, []byte("b"))).To(Succeed())
					Expect(tx.PutFieldDelta("widget", "k1", "note", 3, []byte("c"))).To(Succeed())
					Expect(tx.PutFieldDelta("gadget", "k1", "body", 4, []byte("d"))).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					frags, _, err := tx.FieldDeltas("widget", "k1", "body", 0)
					Expect(err).NotTo(HaveOccurred())
					Expect(frags).To(Equal([][]byte{[]byte("a")}))
				})
			})

			It("overwrites an entry written twice at the same sequence", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutFieldDelta("widget", "k", "body", 1, []byte("first"))).To(Succeed())
					Expect(tx.PutFieldDelta("widget", "k", "body", 1, []byte("second"))).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					frags, complete, err := tx.FieldDeltas("widget", "k", "body", 0)
					Expect(err).NotTo(HaveOccurred())
					Expect(complete).To(BeTrue())
					Expect(frags).To(Equal([][]byte{[]byte("second")}), "a retried write closure is idempotent")
				})
			})

			It("reports a span containing a gap marker as incomplete", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutFieldDelta("widget", "k", "body", 1, []byte("one"))).To(Succeed())
					Expect(tx.PutFieldDelta("widget", "k", "body", 2, nil)).To(Succeed())
					Expect(tx.PutFieldDelta("widget", "k", "body", 3, []byte("three"))).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					frags, complete, err := tx.FieldDeltas("widget", "k", "body", 0)
					Expect(err).NotTo(HaveOccurred())
					Expect(complete).To(BeFalse())
					Expect(frags).To(BeEmpty(), "an incomplete span yields nothing to fold")

					frags, complete, err = tx.FieldDeltas("widget", "k", "body", 2)
					Expect(err).NotTo(HaveOccurred())
					Expect(complete).To(BeTrue(), "a span starting past the gap is usable again")
					Expect(frags).To(Equal([][]byte{[]byte("three")}))
				})
			})

			It("prunes entries past the cutoff and reports spans below the horizon incomplete", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutFieldDelta("widget", "k", "body", 1, []byte("one"))).To(Succeed())
					Expect(tx.PutFieldDelta("widget", "k", "body", 2, []byte("two"))).To(Succeed())
				})
				cutoff := time.Now().Add(time.Millisecond)
				time.Sleep(5 * time.Millisecond)
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutFieldDelta("widget", "k", "body", 3, []byte("three"))).To(Succeed())
				})
				write("ws1", func(tx storage.WriteTx) {
					horizon, err := tx.PruneFieldDeltas(cutoff)
					Expect(err).NotTo(HaveOccurred())
					Expect(horizon).To(BeEquivalentTo(2))
				})
				read("ws1", func(tx storage.ReadTx) {
					_, complete, err := tx.FieldDeltas("widget", "k", "body", 1)
					Expect(err).NotTo(HaveOccurred())
					Expect(complete).To(BeFalse(), "the span reaches below the pruned-through watermark")

					frags, complete, err := tx.FieldDeltas("widget", "k", "body", 2)
					Expect(err).NotTo(HaveOccurred())
					Expect(complete).To(BeTrue())
					Expect(frags).To(Equal([][]byte{[]byte("three")}))
				})
			})

			It("never regresses the horizon", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutFieldDelta("widget", "k", "body", 7, []byte("x"))).To(Succeed())
				})
				write("ws1", func(tx storage.WriteTx) {
					horizon, err := tx.PruneFieldDeltas(time.Now().Add(time.Hour))
					Expect(err).NotTo(HaveOccurred())
					Expect(horizon).To(BeEquivalentTo(7))
				})
				write("ws1", func(tx storage.WriteTx) {
					horizon, err := tx.PruneFieldDeltas(time.Now().Add(-time.Hour))
					Expect(err).NotTo(HaveOccurred())
					Expect(horizon).To(BeEquivalentTo(7), "an older cutoff running late must not lower it")
				})
			})

			It("scopes the log to the transaction's workspace", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutFieldDelta("widget", "k", "body", 1, []byte("ws1"))).To(Succeed())
				})
				write("ws2", func(tx storage.WriteTx) {
					Expect(tx.PutFieldDelta("widget", "k", "body", 1, []byte("ws2"))).To(Succeed())
				})
				write("ws1", func(tx storage.WriteTx) {
					_, err := tx.PruneFieldDeltas(time.Now().Add(time.Hour))
					Expect(err).NotTo(HaveOccurred())
				})
				read("ws2", func(tx storage.ReadTx) {
					frags, complete, err := tx.FieldDeltas("widget", "k", "body", 0)
					Expect(err).NotTo(HaveOccurred())
					Expect(complete).To(BeTrue(), "another workspace's prune is not ours")
					Expect(frags).To(Equal([][]byte{[]byte("ws2")}))
				})
			})

			It("rolls a delta back with its savepoint", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.InSavepoint(func() error {
						Expect(tx.PutFieldDelta("widget", "k", "body", 1, []byte("rolled back"))).To(Succeed())
						return errors.New("nope")
					})).To(MatchError("nope"))
				})
				read("ws1", func(tx storage.ReadTx) {
					frags, complete, err := tx.FieldDeltas("widget", "k", "body", 0)
					Expect(err).NotTo(HaveOccurred())
					Expect(complete).To(BeTrue())
					Expect(frags).To(BeEmpty())
				})
			})
		})

		Describe("text compaction floors", func() {
			It("reads absent as no floor", func() {
				read("ws1", func(tx storage.ReadTx) {
					vv, err := tx.TextFloor("widget", "k", "body")
					Expect(err).NotTo(HaveOccurred())
					Expect(vv).To(BeEmpty())
				})
			})

			It("stores, replaces and clears a floor per field", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutTextFloor("widget", "k", "body", []byte("first"))).To(Succeed())
					Expect(tx.PutTextFloor("widget", "k", "note", []byte("other"))).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					vv, err := tx.TextFloor("widget", "k", "body")
					Expect(err).NotTo(HaveOccurred())
					Expect(vv).To(Equal([]byte("first")))
				})
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutTextFloor("widget", "k", "body", []byte("second"))).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					vv, err := tx.TextFloor("widget", "k", "body")
					Expect(err).NotTo(HaveOccurred())
					Expect(vv).To(Equal([]byte("second")))
					vv, err = tx.TextFloor("widget", "k", "note")
					Expect(err).NotTo(HaveOccurred())
					Expect(vv).To(Equal([]byte("other")), "fields keep their own floors")
				})
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutTextFloor("widget", "k", "body", nil)).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					vv, err := tx.TextFloor("widget", "k", "body")
					Expect(err).NotTo(HaveOccurred())
					Expect(vv).To(BeEmpty(), "an empty vv clears the floor")
				})
			})

			It("keeps rows, entities and workspaces apart", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutTextFloor("widget", "k", "body", []byte("ws1"))).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					vv, err := tx.TextFloor("widget", "other", "body")
					Expect(err).NotTo(HaveOccurred())
					Expect(vv).To(BeEmpty())
					vv, err = tx.TextFloor("gadget", "k", "body")
					Expect(err).NotTo(HaveOccurred())
					Expect(vv).To(BeEmpty())
				})
				read("ws2", func(tx storage.ReadTx) {
					vv, err := tx.TextFloor("widget", "k", "body")
					Expect(err).NotTo(HaveOccurred())
					Expect(vv).To(BeEmpty())
				})
			})

			It("rolls a floor back with its savepoint", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.InSavepoint(func() error {
						Expect(tx.PutTextFloor("widget", "k", "body", []byte("rolled back"))).To(Succeed())
						return errors.New("nope")
					})).To(MatchError("nope"))
				})
				read("ws1", func(tx storage.ReadTx) {
					vv, err := tx.TextFloor("widget", "k", "body")
					Expect(err).NotTo(HaveOccurred())
					Expect(vv).To(BeEmpty())
				})
			})
		})

		Describe("secondary index entries", func() {
			// Entry keys are opaque bytes the adapter must order BYTEWISE. The
			// engine's encoder gives them a meaning; nothing here needs it, so
			// these are deliberately hand-written byte strings, including ones
			// that would sort differently under a text collation.
			entry := func(idx string, k ...byte) storage.IndexEntry {
				return storage.IndexEntry{Index: idx, Key: k}
			}
			scanOrdered := func(tx storage.ReadTx, entity, idx string, lo, hi []byte, order storage.ScanOrder) []string {
				GinkgoHelper()
				var got []string
				Expect(tx.ScanIndex(entity, idx, lo, hi, 0, order, func(_ []byte, rowKey string) error {
					got = append(got, rowKey)
					return nil
				})).To(Succeed())
				return got
			}
			scan := func(tx storage.ReadTx, entity, idx string, lo, hi []byte) []string {
				GinkgoHelper()
				return scanOrdered(tx, entity, idx, lo, hi, storage.ScanAsc)
			}
			// reversed is what a descending walk of the same range must yield,
			// element for element — the property the specs below assert rather
			// than restating each expected list by hand.
			reversed := func(in []string) []string {
				if len(in) == 0 {
					// The scan helper appends into a nil slice, so an empty
					// range comes back nil and must be mirrored as nil.
					return nil
				}
				out := make([]string, 0, len(in))
				for i := len(in) - 1; i >= 0; i-- {
					out = append(out, in[i])
				}
				return out
			}

			It("stores and scans back an entry at the size limit under the longest names", func() {
				// The longest workspace id a store sees is an app-prefixed one
				// (see prefixstore), and every name and key is at its limit.
				ws := strings.Repeat("w", naming.MaxAppIDBytes+1+naming.MaxWorkspaceIDBytes)
				entity := strings.Repeat("e", naming.MaxEntityNameLength)
				idx := strings.Repeat("i", naming.MaxEntityNameLength)
				rowKey := strings.Repeat("k", naming.MaxRowKeyBytes)
				// Bytes that do not compress, so a store that compresses
				// stored values cannot make the entry fit by shrinking it.
				k := make([]byte, storage.MaxIndexEntryBytes)
				_, _ = rand.New(rand.NewSource(1)).Read(k)

				write(ws, func(tx storage.WriteTx) {
					Expect(tx.PutIndexEntries(entity, rowKey, []storage.IndexEntry{{Index: idx, Key: k}})).To(Succeed())
				})
				read(ws, func(tx storage.ReadTx) {
					var entries [][]byte
					var keys []string
					Expect(tx.ScanIndex(entity, idx, nil, nil, 0, storage.ScanAsc, func(e []byte, rk string) error {
						entries = append(entries, bytes.Clone(e))
						keys = append(keys, rk)
						return nil
					})).To(Succeed())
					Expect(keys).To(Equal([]string{rowKey}))
					Expect(entries).To(Equal([][]byte{k}))
				})
			})

			It("reads an index nobody has written as empty", func() {
				read("ws1", func(tx storage.ReadTx) {
					Expect(scan(tx, "widget", "by_label", nil, nil)).To(BeEmpty())
					hash, ready, err := tx.IndexState("widget", "by_label")
					Expect(err).NotTo(HaveOccurred())
					Expect(hash).To(BeEmpty())
					Expect(ready).To(BeFalse())
				})
			})

			It("yields entries in bytewise key order", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutIndexEntries("widget", "k1", []storage.IndexEntry{entry("by_label", 0x02)})).To(Succeed())
					Expect(tx.PutIndexEntries("widget", "k2", []storage.IndexEntry{entry("by_label", 0x01)})).To(Succeed())
					Expect(tx.PutIndexEntries("widget", "k3", []storage.IndexEntry{entry("by_label", 0xff)})).To(Succeed())
					// 0x00 inside a key is ordinary data, not a terminator.
					Expect(tx.PutIndexEntries("widget", "k4", []storage.IndexEntry{entry("by_label", 0x01, 0x00)})).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					Expect(scan(tx, "widget", "by_label", nil, nil)).To(Equal([]string{"k2", "k4", "k1", "k3"}))
				})
			})

			It("bounds a scan by lo inclusive and hi exclusive", func() {
				write("ws1", func(tx storage.WriteTx) {
					for i, k := range []string{"k1", "k2", "k3"} {
						Expect(tx.PutIndexEntries("widget", k, []storage.IndexEntry{
							entry("by_label", byte(i+1)),
						})).To(Succeed())
					}
				})
				read("ws1", func(tx storage.ReadTx) {
					Expect(scan(tx, "widget", "by_label", []byte{0x02}, []byte{0x03})).To(Equal([]string{"k2"}))
					Expect(scan(tx, "widget", "by_label", []byte{0x02}, nil)).To(Equal([]string{"k2", "k3"}))
					Expect(scan(tx, "widget", "by_label", nil, []byte{0x02})).To(Equal([]string{"k1"}))
					Expect(scan(tx, "widget", "by_label", []byte{0x09}, nil)).To(BeEmpty())
				})
			})

			It("honours a scan limit", func() {
				write("ws1", func(tx storage.WriteTx) {
					for i, k := range []string{"k1", "k2", "k3"} {
						Expect(tx.PutIndexEntries("widget", k, []storage.IndexEntry{
							entry("by_label", byte(i+1)),
						})).To(Succeed())
					}
				})
				read("ws1", func(tx storage.ReadTx) {
					var got []string
					Expect(tx.ScanIndex("widget", "by_label", nil, nil, 2, storage.ScanAsc, func(_ []byte, rowKey string) error {
						got = append(got, rowKey)
						return nil
					})).To(Succeed())
					Expect(got).To(Equal([]string{"k1", "k2"}))
				})
			})

			Describe("descending", func() {
				// A descending walk is what pages a query back from a `before`
				// cursor. The bounds mean exactly what they mean ascending, so
				// the only thing that moves is the yield order and the end the
				// limit takes from.
				BeforeEach(func() {
					write("ws1", func(tx storage.WriteTx) {
						for i, k := range []string{"k1", "k2", "k3", "k4"} {
							Expect(tx.PutIndexEntries("widget", k, []storage.IndexEntry{
								entry("by_label", byte(i+1)),
							})).To(Succeed())
						}
						// Two entries at one key: reversing has to reverse the
						// tiebreak too, or the walk is not a mirror.
						Expect(tx.PutIndexEntries("widget", "k0", []storage.IndexEntry{
							entry("by_label", 0x02),
						})).To(Succeed())
					})
				})

				It("yields the exact mirror of the ascending walk", func() {
					read("ws1", func(tx storage.ReadTx) {
						asc := scan(tx, "widget", "by_label", nil, nil)
						Expect(scanOrdered(tx, "widget", "by_label", nil, nil, storage.ScanDesc)).
							To(Equal(reversed(asc)))
					})
				})

				It("keeps lo inclusive and hi exclusive", func() {
					read("ws1", func(tx storage.ReadTx) {
						for _, b := range [][2][]byte{
							{{0x02}, {0x04}},
							{{0x02}, nil},
							{nil, {0x03}},
							{{0x09}, nil},
						} {
							asc := scan(tx, "widget", "by_label", b[0], b[1])
							Expect(scanOrdered(tx, "widget", "by_label", b[0], b[1], storage.ScanDesc)).
								To(Equal(reversed(asc)), "lo=%v hi=%v", b[0], b[1])
						}
					})
				})

				It("takes the limit from the hi end", func() {
					read("ws1", func(tx storage.ReadTx) {
						var got []string
						Expect(tx.ScanIndex("widget", "by_label", nil, nil, 2, storage.ScanDesc,
							func(_ []byte, rowKey string) error {
								got = append(got, rowKey)
								return nil
							})).To(Succeed())
						Expect(got).To(Equal([]string{"k4", "k3"}))
					})
				})
			})

			It("replaces a row's WHOLE entry set, across indexes", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutIndexEntries("widget", "k1", []storage.IndexEntry{
						entry("by_label", 0x01),
						entry("by_count", 0x01),
					})).To(Succeed())
				})
				write("ws1", func(tx storage.WriteTx) {
					// The row now sits only in by_count, at a new key. Its old
					// by_label entry must go with the old set, even though this
					// call never mentions by_label.
					Expect(tx.PutIndexEntries("widget", "k1", []storage.IndexEntry{
						entry("by_count", 0x09),
					})).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					Expect(scan(tx, "widget", "by_label", nil, nil)).To(BeEmpty())
					Expect(scan(tx, "widget", "by_count", nil, nil)).To(Equal([]string{"k1"}))
					Expect(scan(tx, "widget", "by_count", []byte{0x09}, nil)).To(Equal([]string{"k1"}), "at the new key")
				})
			})

			It("stores one row's several entries in one index", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutIndexEntries("widget", "k1", []storage.IndexEntry{
						entry("by_tag", 0x01),
						entry("by_tag", 0x02),
					})).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					Expect(scan(tx, "widget", "by_tag", nil, nil)).To(Equal([]string{"k1", "k1"}))
				})
			})

			It("drops the entries a shrinking fan-out no longer occupies", func() {
				// The shape a row whose indexed list lost an element takes: the
				// replace-set has to remove the position that element held, or a
				// probe for it keeps finding a row that no longer matches.
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutIndexEntries("widget", "k1", []storage.IndexEntry{
						entry("by_tag", 0x01), entry("by_tag", 0x02), entry("by_tag", 0x03),
					})).To(Succeed())
				})
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutIndexEntries("widget", "k1", []storage.IndexEntry{
						entry("by_tag", 0x02),
					})).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					Expect(scan(tx, "widget", "by_tag", nil, nil)).To(Equal([]string{"k1"}))
					Expect(scan(tx, "widget", "by_tag", []byte{0x01}, []byte{0x02})).To(BeEmpty())
					Expect(scan(tx, "widget", "by_tag", []byte{0x03}, nil)).To(BeEmpty())
				})
			})

			It("replaces an overlapping set entry by entry", func() {
				// The shape an adapter that diffs the set instead of rewriting
				// it can get wrong: the departed entry has to go, the new one
				// has to appear, and the ones both sets hold have to survive in
				// exactly one copy.
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutIndexEntries("widget", "k1", []storage.IndexEntry{
						entry("by_tag", 0x0a), entry("by_tag", 0x0b), entry("by_label", 0x01),
					})).To(Succeed())
				})
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutIndexEntries("widget", "k1", []storage.IndexEntry{
						entry("by_tag", 0x0b), entry("by_label", 0x01), entry("by_count", 0x07),
					})).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					Expect(scan(tx, "widget", "by_tag", nil, nil)).To(Equal([]string{"k1"}), "0x0a departed, 0x0b kept once")
					Expect(scan(tx, "widget", "by_tag", []byte{0x0a}, []byte{0x0b})).To(BeEmpty())
					Expect(scan(tx, "widget", "by_label", nil, nil)).To(Equal([]string{"k1"}), "an entry in both sets survives")
					Expect(scan(tx, "widget", "by_count", nil, nil)).To(Equal([]string{"k1"}), "the new entry appears")
				})
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutIndexEntries("widget", "k1", nil)).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					Expect(scan(tx, "widget", "by_tag", nil, nil)).To(BeEmpty())
					Expect(scan(tx, "widget", "by_label", nil, nil)).To(BeEmpty())
					Expect(scan(tx, "widget", "by_count", nil, nil)).To(BeEmpty())
				})
			})

			It("replaces an overlapping set the SAME transaction wrote", func() {
				// A batch that writes one row twice — a PUT then a PATCH of the
				// same key — replaces its entries twice inside one transaction.
				// The second replacement diffs against what the first left, not
				// against what the transaction opened on.
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutIndexEntries("widget", "k1", []storage.IndexEntry{
						entry("by_tag", 0x0a), entry("by_tag", 0x0b), entry("by_label", 0x01),
					})).To(Succeed())
					Expect(tx.PutIndexEntries("widget", "k1", []storage.IndexEntry{
						entry("by_tag", 0x0b), entry("by_label", 0x01), entry("by_count", 0x07),
					})).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					Expect(scan(tx, "widget", "by_tag", nil, nil)).To(Equal([]string{"k1"}), "0x0a departed, 0x0b kept once")
					Expect(scan(tx, "widget", "by_tag", []byte{0x0a}, []byte{0x0b})).To(BeEmpty())
					Expect(scan(tx, "widget", "by_label", nil, nil)).To(Equal([]string{"k1"}), "an entry in both sets survives")
					Expect(scan(tx, "widget", "by_count", nil, nil)).To(Equal([]string{"k1"}), "the new entry appears")
				})
			})

			It("stores one copy of an entry the caller passed twice", func() {
				// An indexed list with a repeated element encodes the same
				// entry twice for one row. The write must not fail on its own
				// primary key, and the row must not appear twice at that key.
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutIndexEntries("widget", "k1", []storage.IndexEntry{
						entry("by_tag", 0x01), entry("by_tag", 0x01), entry("by_tag", 0x02),
					})).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					Expect(scan(tx, "widget", "by_tag", []byte{0x01}, []byte{0x02})).To(Equal([]string{"k1"}))
					Expect(scan(tx, "widget", "by_tag", nil, nil)).To(Equal([]string{"k1", "k1"}), "one entry per distinct key")
				})
				write("ws1", func(tx storage.WriteTx) {
					// Re-putting a set the row already holds leaves it as it is.
					Expect(tx.PutIndexEntries("widget", "k1", []storage.IndexEntry{
						entry("by_tag", 0x01), entry("by_tag", 0x02),
					})).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					Expect(scan(tx, "widget", "by_tag", nil, nil)).To(Equal([]string{"k1", "k1"}))
				})
			})

			It("deletes every entry of one row by key alone", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutIndexEntries("widget", "k1", []storage.IndexEntry{
						entry("by_label", 0x01), entry("by_count", 0x01),
					})).To(Succeed())
					Expect(tx.PutIndexEntries("widget", "k2", []storage.IndexEntry{
						entry("by_label", 0x02),
					})).To(Succeed())
				})
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.DeleteIndexEntries("widget", "k1")).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					Expect(scan(tx, "widget", "by_label", nil, nil)).To(Equal([]string{"k2"}))
					Expect(scan(tx, "widget", "by_count", nil, nil)).To(BeEmpty())
				})
			})

			It("treats deleting a row with no entries as a no-op", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.DeleteIndexEntries("widget", "never-existed")).To(Succeed())
				})
			})

			It("treats an empty entry set as a delete", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutIndexEntries("widget", "k1", []storage.IndexEntry{entry("by_label", 0x01)})).To(Succeed())
				})
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutIndexEntries("widget", "k1", nil)).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					Expect(scan(tx, "widget", "by_label", nil, nil)).To(BeEmpty())
				})
			})

			It("keeps entities, indexes and workspaces apart", func() {
				// Two entities naming one index is the NORMAL case, not a
				// pathology: the engine derives an index name from a field name,
				// so every entity with an "author" field marked indexed claims
				// "__field_author". An entry carries a bare row key, which means
				// nothing outside its own entity's keyspace — so a scan of one
				// entity's index must never see the other's entries, even at the
				// same name and the same key.
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutIndexEntries("widget", "k1", []storage.IndexEntry{entry("by_label", 0x01)})).To(Succeed())
					Expect(tx.PutIndexEntries("gadget", "k1", []storage.IndexEntry{entry("by_label", 0x02)})).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					Expect(scan(tx, "widget", "by_label", nil, nil)).To(Equal([]string{"k1"}))
					Expect(scan(tx, "gadget", "by_label", nil, nil)).To(Equal([]string{"k1"}))
					// The gadget entry sorts at 0x02 and the widget one at 0x01,
					// so a bound that selects one must exclude the other — the
					// two live in separate keyspaces, not one shared range.
					Expect(scan(tx, "widget", "by_label", []byte{0x02}, nil)).To(BeEmpty())
					Expect(scan(tx, "gadget", "by_label", nil, []byte{0x02})).To(BeEmpty())
					Expect(scan(tx, "widget", "by_other", nil, nil)).To(BeEmpty())
				})
				write("ws1", func(tx storage.WriteTx) {
					// Same key in another entity: deleting one must not touch
					// the other, which is why entries are keyed by both.
					Expect(tx.DeleteIndexEntries("widget", "k1")).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					Expect(scan(tx, "widget", "by_label", nil, nil)).To(BeEmpty())
					Expect(scan(tx, "gadget", "by_label", nil, nil)).To(Equal([]string{"k1"}), "the gadget entry survives")
				})
				read("ws2", func(tx storage.ReadTx) {
					Expect(scan(tx, "widget", "by_label", nil, nil)).To(BeEmpty())
					Expect(scan(tx, "gadget", "by_label", nil, nil)).To(BeEmpty())
				})
			})

			It("rolls entries back with their savepoint", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutIndexEntries("widget", "k1", []storage.IndexEntry{entry("by_label", 0x01)})).To(Succeed())
				})
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.InSavepoint(func() error {
						Expect(tx.PutIndexEntries("widget", "k1", []storage.IndexEntry{entry("by_label", 0x09)})).To(Succeed())
						Expect(tx.DeleteIndexEntries("widget", "k1")).To(Succeed())
						return errors.New("nope")
					})).To(MatchError("nope"))
				})
				read("ws1", func(tx storage.ReadTx) {
					Expect(scan(tx, "widget", "by_label", []byte{0x01}, []byte{0x02})).To(Equal([]string{"k1"}))
				})
			})

			It("round-trips index build state", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.SetIndexState("widget", "by_label", "hash1", false)).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					hash, ready, err := tx.IndexState("widget", "by_label")
					Expect(err).NotTo(HaveOccurred())
					Expect(hash).To(Equal("hash1"))
					Expect(ready).To(BeFalse())
				})
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.SetIndexState("widget", "by_label", "hash1", true)).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					hash, ready, err := tx.IndexState("widget", "by_label")
					Expect(err).NotTo(HaveOccurred())
					Expect(hash).To(Equal("hash1"))
					Expect(ready).To(BeTrue())
					hash, ready, err = tx.IndexState("widget", "by_count")
					Expect(err).NotTo(HaveOccurred())
					Expect(hash).To(BeEmpty())
					Expect(ready).To(BeFalse(), "state is per index")
					hash, ready, err = tx.IndexState("gadget", "by_label")
					Expect(err).NotTo(HaveOccurred())
					Expect(hash).To(BeEmpty())
					Expect(ready).To(BeFalse(), "state is per entity: one name, separate entries")
				})
				read("ws2", func(tx storage.ReadTx) {
					_, ready, err := tx.IndexState("widget", "by_label")
					Expect(err).NotTo(HaveOccurred())
					Expect(ready).To(BeFalse(), "state is per workspace")
				})
			})

			It("stops a scan when fn returns an error, propagating it", func() {
				write("ws1", func(tx storage.WriteTx) {
					for i, k := range []string{"k1", "k2", "k3"} {
						Expect(tx.PutIndexEntries("widget", k, []storage.IndexEntry{
							entry("by_label", byte(i+1)),
						})).To(Succeed())
					}
				})
				read("ws1", func(tx storage.ReadTx) {
					stop := errors.New("stop")
					seen := 0
					err := tx.ScanIndex("widget", "by_label", nil, nil, 0, storage.ScanAsc, func([]byte, string) error {
						seen++
						return stop
					})
					Expect(err).To(MatchError(stop))
					Expect(seen).To(Equal(1))
				})
			})

			Describe("over several ranges", func() {
				// What ScanIndexRanges yields, as "i:rowKey" per entry, so that a
				// failure shows which range an entry was reported under.
				scanRanges := func(tx storage.ReadTx, entity, idx string, ranges ...storage.IndexRange) []string {
					GinkgoHelper()
					var got []string
					Expect(tx.ScanIndexRanges(entity, idx, ranges, func(i int, _ []byte, rowKey string) error {
						got = append(got, fmt.Sprintf("%d:%s", i, rowKey))
						return nil
					})).To(Succeed())
					return got
				}

				BeforeEach(func() {
					write("ws1", func(tx storage.WriteTx) {
						for i, k := range []string{"k1", "k2", "k3", "k4", "k5"} {
							Expect(tx.PutIndexEntries("widget", k, []storage.IndexEntry{
								entry("by_label", byte(i+1)),
							})).To(Succeed())
						}
						// k0 shares k4's key, so the row key has to order the
						// two. Its by_count entry and gadget's entry sit at keys
						// the ranges below cover, and no scan of widget's
						// by_label may yield them.
						Expect(tx.PutIndexEntries("widget", "k0", []storage.IndexEntry{
							entry("by_label", 0x04), entry("by_count", 0x02),
						})).To(Succeed())
						Expect(tx.PutIndexEntries("gadget", "g1", []storage.IndexEntry{
							entry("by_label", 0x02),
						})).To(Succeed())
					})
				})

				It("yields range by range in the order given, each as ScanIndex yields it", func() {
					read("ws1", func(tx storage.ReadTx) {
						ranges := []storage.IndexRange{
							{Lo: []byte{0x04}, Hi: []byte{0x06}},
							{Lo: []byte{0x01}, Hi: []byte{0x02}},
							{Lo: nil, Hi: []byte{0x02}},
							{Lo: []byte{0x05}, Hi: nil},
							{Lo: nil, Hi: nil},
						}
						Expect(scanRanges(tx, "widget", "by_label", ranges...)).To(Equal([]string{
							"0:k0", "0:k4", "0:k5",
							"1:k1",
							"2:k1",
							"3:k5",
							"4:k1", "4:k2", "4:k3", "4:k0", "4:k4", "4:k5",
						}))
						for i, r := range ranges {
							// Alone, the range is at position 0.
							var want []string
							for _, rowKey := range scan(tx, "widget", "by_label", r.Lo, r.Hi) {
								want = append(want, "0:"+rowKey)
							}
							Expect(scanRanges(tx, "widget", "by_label", r)).To(Equal(want), "range %d alone", i)
						}
					})
				})

				It("yields an entry once for every range that holds it", func() {
					read("ws1", func(tx storage.ReadTx) {
						Expect(scanRanges(tx, "widget", "by_label",
							storage.IndexRange{Lo: []byte{0x02}, Hi: []byte{0x04}},
							storage.IndexRange{Lo: []byte{0x03}, Hi: []byte{0x05}},
							storage.IndexRange{Lo: []byte{0x02}, Hi: []byte{0x04}},
						)).To(Equal([]string{
							"0:k2", "0:k3",
							"1:k3", "1:k0", "1:k4",
							"2:k2", "2:k3",
						}))
					})
				})

				It("keeps the position of a range that holds nothing", func() {
					read("ws1", func(tx storage.ReadTx) {
						Expect(scanRanges(tx, "widget", "by_label",
							storage.IndexRange{Lo: []byte{0x09}, Hi: nil},
							storage.IndexRange{Lo: []byte{0x03}, Hi: []byte{0x03}},
							storage.IndexRange{Lo: []byte{0x04}, Hi: []byte{0x02}},
							storage.IndexRange{Lo: []byte{0x05}, Hi: []byte{0x06}},
							storage.IndexRange{Lo: []byte{0x00}, Hi: []byte{0x01}},
						)).To(Equal([]string{"3:k5"}))
					})
				})

				It("yields nothing for no ranges", func() {
					read("ws1", func(tx storage.ReadTx) {
						Expect(scanRanges(tx, "widget", "by_label")).To(BeEmpty())
						Expect(scanRanges(tx, "widget", "by_label", []storage.IndexRange{}...)).To(BeEmpty())
					})
				})

				It("keeps entities, indexes and workspaces apart", func() {
					read("ws1", func(tx storage.ReadTx) {
						all := []storage.IndexRange{{}, {Lo: []byte{0x02}, Hi: []byte{0x03}}}
						Expect(scanRanges(tx, "gadget", "by_label", all...)).To(Equal([]string{"0:g1", "1:g1"}))
						Expect(scanRanges(tx, "widget", "by_count", all...)).To(Equal([]string{"0:k0", "1:k0"}))
						Expect(scanRanges(tx, "widget", "by_other", all...)).To(BeEmpty())
					})
					read("ws2", func(tx storage.ReadTx) {
						Expect(scanRanges(tx, "widget", "by_label", storage.IndexRange{}, storage.IndexRange{})).To(BeEmpty())
					})
				})

				It("stops when fn returns an error, propagating it", func() {
					read("ws1", func(tx storage.ReadTx) {
						stop := errors.New("stop")
						seen := 0
						err := tx.ScanIndexRanges("widget", "by_label",
							[]storage.IndexRange{{Lo: []byte{0x01}, Hi: []byte{0x02}}, {}},
							func(int, []byte, string) error {
								seen++
								return stop
							})
						Expect(err).To(MatchError(stop))
						Expect(seen).To(Equal(1))
					})
				})
			})
		})

		Describe("recency-ordered entity scan", func() {
			It("yields live rows newest first, ties broken by key ascending", func() {
				// The version-5 group uses the byte-order key set: the tie-break
				// decides which same-version rows a `limit` window keeps, so it
				// has to be the engine's bytewise order and not a collation's.
				write("ws1", func(tx storage.WriteTx) {
					for _, k := range []string{"b", "a", "Z", "_", "é"} {
						Expect(tx.PutRow("widget", k, []byte(k), 5, storage.Actor{})).To(Succeed())
					}
					Expect(tx.PutRow("widget", "c", []byte("c"), 9, storage.Actor{})).To(Succeed())
					Expect(tx.PutRow("widget", "d", []byte("d"), 1, storage.Actor{})).To(Succeed())
					Expect(tx.PutRow("gadget", "z", []byte("z"), 99, storage.Actor{})).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					var keys []string
					Expect(tx.ScanEntityByVersionDesc("widget", 0, func(r storage.Row) error {
						keys = append(keys, r.Key)
						return nil
					})).To(Succeed())
					Expect(keys).To(Equal([]string{"c", "Z", "_", "a", "b", "é", "d"}),
						"one entity only, newest first, same-version rows by key")

					// A window that cuts inside the shared-version group keeps
					// the first keys in THAT order — the point of the tie-break.
					keys = nil
					Expect(tx.ScanEntityByVersionDesc("widget", 3, func(r storage.Row) error {
						keys = append(keys, r.Key)
						return nil
					})).To(Succeed())
					Expect(keys).To(Equal([]string{"c", "Z", "_"}))
				})
			})

			It("honours a limit and skips tombstones", func() {
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutRow("widget", "a", []byte("a"), 1, storage.Actor{})).To(Succeed())
					Expect(tx.PutRow("widget", "b", []byte("b"), 2, storage.Actor{})).To(Succeed())
					Expect(tx.PutRow("widget", "c", []byte("c"), 3, storage.Actor{})).To(Succeed())
					Expect(tx.DeleteRow("widget", "c", 4, storage.Actor{})).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					var keys []string
					Expect(tx.ScanEntityByVersionDesc("widget", 1, func(r storage.Row) error {
						keys = append(keys, r.Key)
						return nil
					})).To(Succeed())
					Expect(keys).To(Equal([]string{"b"}), "newest LIVE row")
				})
			})

			It("returns the row's value and attribution, like the other scans", func() {
				actor := storage.Actor{Subject: "sub"}
				write("ws1", func(tx storage.WriteTx) {
					Expect(tx.PutRow("widget", "a", []byte("v"), 1, actor)).To(Succeed())
				})
				read("ws1", func(tx storage.ReadTx) {
					var got storage.Row
					Expect(tx.ScanEntityByVersionDesc("widget", 0, func(r storage.Row) error {
						got = r
						return nil
					})).To(Succeed())
					Expect(got.Entity).To(Equal("widget"))
					Expect(got.Value).To(Equal([]byte("v")))
					Expect(got.Version).To(BeEquivalentTo(1))
					Expect(got.Actor).To(Equal(actor))
				})
			})
		})
	})
}
