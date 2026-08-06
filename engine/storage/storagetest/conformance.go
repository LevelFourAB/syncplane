// Package storagetest provides a shared Ginkgo behavior suite every
// storage.Adapter implementation must pass. Both shipped adapters register it
// verbatim, and a new one does the same:
//
//	var _ = storagetest.Conformance("postgres", func() storage.Adapter { ... })
package storagetest

import (
	"context"
	"errors"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

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
			// after the scan returns (Server.compactWorkspace), so writing during
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

			It("serves GetRow from inside a ScanIndex callback (the indexed-query shape)", func() {
				eachTx(func(tx storage.ReadTx) {
					var keys []string
					Expect(tx.ScanIndex("widget", "by_label", nil, nil, 0, func(_ []byte, rowKey string) error {
						keys = append(keys, rowKey)
						row, err := tx.GetRow("widget", rowKey)
						if err != nil {
							return err
						}
						Expect(row.Value).To(Equal([]byte("v-" + rowKey)))
						return nil
					})).To(Succeed())
					Expect(keys).To(Equal([]string{"k1", "k2", "k3"}))
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
					Expect(tx.ScanIndex("hist", "by_source", nil, nil, 0,
						func(_ []byte, rowKey string) error {
							got = append(got, rowKey)
							return nil
						})).To(Succeed())
					Expect(got).To(BeEmpty(), "an entry outliving its row would leak")
					// An index name is per entity, so the other entity's
					// identically-named index is untouched.
					got = nil
					Expect(tx.ScanIndex("widget", "by_source", nil, nil, 0,
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
					Expect(tx.ScanIndex("hist", "by_source", nil, nil, 0,
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
			ids, err := store.ListWorkspaceIDs(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(ids).To(BeEmpty())
			write("wsB", func(tx storage.WriteTx) {
				Expect(tx.PutRow("widget", "k", []byte("v"), 1, storage.Actor{})).To(Succeed())
			})
			write("wsA", func(tx storage.WriteTx) {
				Expect(tx.PutRow("widget", "k", []byte("v"), 1, storage.Actor{})).To(Succeed())
			})
			ids, err = store.ListWorkspaceIDs(ctx)
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
			ids, err := store.ListWorkspaceIDs(ctx)
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
			ids, err = store.ListWorkspaceIDs(ctx)
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
			ids, err := store.ListWorkspaceIDs(ctx)
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
			ids, err = store.ListWorkspaceIDs(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(ids).To(ConsistOf("ghost"))
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
				Expect(store.ListWorkspaceIDs(ctx)).To(ConsistOf("ws1"))
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
					ids, err := store.ListPendingWorkspaceIDs(ctx, consumer, after, limit)
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
					_, err := store.ListPendingWorkspaceIDs(ctx, "indexer", "", 0)
					Expect(err).To(HaveOccurred())
					_, err = store.ListPendingWorkspaceIDs(ctx, "indexer", "", -1)
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
			scan := func(tx storage.ReadTx, entity, idx string, lo, hi []byte) []string {
				GinkgoHelper()
				var got []string
				Expect(tx.ScanIndex(entity, idx, lo, hi, 0, func(_ []byte, rowKey string) error {
					got = append(got, rowKey)
					return nil
				})).To(Succeed())
				return got
			}

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
					Expect(tx.ScanIndex("widget", "by_label", nil, nil, 2, func(_ []byte, rowKey string) error {
						got = append(got, rowKey)
						return nil
					})).To(Succeed())
					Expect(got).To(Equal([]string{"k1", "k2"}))
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
					err := tx.ScanIndex("widget", "by_label", nil, nil, 0, func([]byte, string) error {
						seen++
						return stop
					})
					Expect(err).To(MatchError(stop))
					Expect(seen).To(Equal(1))
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

		Describe("app schema catalog", func() {
			It("starts empty", func() {
				got, err := store.ListAppSchemas(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(BeEmpty())
			})

			It("persists and lists registered schemas in app-id order", func() {
				Expect(store.PutAppSchema(ctx, "beta", []byte("b-set"), "rev-b")).To(Succeed())
				Expect(store.PutAppSchema(ctx, "alpha", []byte("a-set"), "rev-a")).To(Succeed())

				got, err := store.ListAppSchemas(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(HaveLen(2))
				Expect(got[0].AppID).To(Equal("alpha"))
				Expect(got[0].DescriptorSet).To(Equal([]byte("a-set")))
				Expect(got[0].Revision).To(Equal("rev-a"))
				Expect(got[1].AppID).To(Equal("beta"))
			})

			It("upserts an existing app in place", func() {
				Expect(store.PutAppSchema(ctx, "app", []byte("v1"), "rev1")).To(Succeed())
				Expect(store.PutAppSchema(ctx, "app", []byte("v2"), "rev2")).To(Succeed())

				got, err := store.ListAppSchemas(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(HaveLen(1))
				Expect(got[0].DescriptorSet).To(Equal([]byte("v2")))
				Expect(got[0].Revision).To(Equal("rev2"))
			})

			It("deletes a schema (and ignores a missing one)", func() {
				Expect(store.PutAppSchema(ctx, "app", []byte("v1"), "rev1")).To(Succeed())
				Expect(store.DeleteAppSchema(ctx, "app")).To(Succeed())
				Expect(store.DeleteAppSchema(ctx, "absent")).To(Succeed())

				got, err := store.ListAppSchemas(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(BeEmpty())
			})
		})
	})
}

// ConformanceNamespaced registers the namespace-isolation specs. newPair
// must return two adapters backed by the SAME underlying database but
// scoped to two different namespaces, plus a cleanup that owns closing the
// shared resources (the adapters' own Close may be a no-op for derived
// stores).
func ConformanceNamespaced(name string, newPair func() (a, b storage.Adapter, cleanup func())) bool {
	return Describe("storage.Adapter namespace conformance: "+name, func() {
		var (
			ctx  context.Context
			a, b storage.Adapter
		)

		BeforeEach(func() {
			ctx = context.Background()
			var cleanup func()
			a, b, cleanup = newPair()
			DeferCleanup(cleanup)
		})

		writeTo := func(s storage.Adapter, ws string, fn func(storage.WriteTx)) {
			GinkgoHelper()
			Expect(s.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
				fn(tx)
				return nil
			})).To(Succeed())
		}
		readFrom := func(s storage.Adapter, ws string, fn func(storage.ReadTx)) {
			GinkgoHelper()
			Expect(s.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
				fn(tx)
				return nil
			})).To(Succeed())
		}

		It("isolates rows between namespaces", func() {
			writeTo(a, "ws1", func(tx storage.WriteTx) {
				Expect(tx.PutRow("widget", "k1", []byte("from-a"), 1, storage.Actor{})).To(Succeed())
			})
			readFrom(b, "ws1", func(tx storage.ReadTx) {
				_, err := tx.GetRow("widget", "k1")
				Expect(err).To(MatchError(storage.ErrNotFound))
				var keys []string
				Expect(tx.ScanEntity("widget", func(r storage.Row) error {
					keys = append(keys, r.Key)
					return nil
				})).To(Succeed())
				Expect(keys).To(BeEmpty())
			})
			readFrom(a, "ws1", func(tx storage.ReadTx) {
				row, err := tx.GetRow("widget", "k1")
				Expect(err).NotTo(HaveOccurred())
				Expect(row.Value).To(Equal([]byte("from-a")))
			})
		})

		It("keeps sequences independent per namespace", func() {
			writeTo(a, "ws1", func(tx storage.WriteTx) {
				for i := 0; i < 3; i++ {
					_, err := tx.NextSeq()
					Expect(err).NotTo(HaveOccurred())
				}
			})
			writeTo(b, "ws1", func(tx storage.WriteTx) {
				seq, err := tx.NextSeq()
				Expect(err).NotTo(HaveOccurred())
				Expect(seq).To(BeEquivalentTo(1), "same workspace id, different namespace, fresh sequence")
			})
			readFrom(a, "ws1", func(tx storage.ReadTx) {
				seq, err := tx.CurrentSeq()
				Expect(err).NotTo(HaveOccurred())
				Expect(seq).To(BeEquivalentTo(3))
			})
		})

		It("keeps text field delta logs independent per namespace", func() {
			writeTo(a, "ws1", func(tx storage.WriteTx) {
				Expect(tx.PutFieldDelta("widget", "k", "body", 1, []byte("from-a"))).To(Succeed())
			})
			writeTo(b, "ws1", func(tx storage.WriteTx) {
				_, err := tx.PruneFieldDeltas(time.Now().Add(time.Hour))
				Expect(err).NotTo(HaveOccurred())
			})
			readFrom(b, "ws1", func(tx storage.ReadTx) {
				frags, _, err := tx.FieldDeltas("widget", "k", "body", 0)
				Expect(err).NotTo(HaveOccurred())
				Expect(frags).To(BeEmpty())
			})
			readFrom(a, "ws1", func(tx storage.ReadTx) {
				frags, complete, err := tx.FieldDeltas("widget", "k", "body", 0)
				Expect(err).NotTo(HaveOccurred())
				Expect(complete).To(BeTrue(), "the other namespace's prune is not ours")
				Expect(frags).To(Equal([][]byte{[]byte("from-a")}))
			})
		})

		It("keeps secondary index entries and state independent per namespace", func() {
			writeTo(a, "ws1", func(tx storage.WriteTx) {
				Expect(tx.PutIndexEntries("widget", "k1", []storage.IndexEntry{
					{Index: "by_label", Key: []byte{0x01}},
				})).To(Succeed())
				Expect(tx.SetIndexState("widget", "by_label", "hash-a", true)).To(Succeed())
			})
			readFrom(b, "ws1", func(tx storage.ReadTx) {
				var got []string
				Expect(tx.ScanIndex("widget", "by_label", nil, nil, 0, func(_ []byte, rowKey string) error {
					got = append(got, rowKey)
					return nil
				})).To(Succeed())
				Expect(got).To(BeEmpty())
				hash, ready, err := tx.IndexState("widget", "by_label")
				Expect(err).NotTo(HaveOccurred())
				Expect(hash).To(BeEmpty())
				Expect(ready).To(BeFalse())
			})
			// A delete in the other namespace must not reach ours.
			writeTo(b, "ws1", func(tx storage.WriteTx) {
				Expect(tx.DeleteIndexEntries("widget", "k1")).To(Succeed())
			})
			readFrom(a, "ws1", func(tx storage.ReadTx) {
				var got []string
				Expect(tx.ScanIndex("widget", "by_label", nil, nil, 0, func(_ []byte, rowKey string) error {
					got = append(got, rowKey)
					return nil
				})).To(Succeed())
				Expect(got).To(Equal([]string{"k1"}))
			})
		})

		It("keeps recency-ordered scans independent per namespace", func() {
			writeTo(a, "ws1", func(tx storage.WriteTx) {
				Expect(tx.PutRow("widget", "k1", []byte("from-a"), 1, storage.Actor{})).To(Succeed())
			})
			readFrom(b, "ws1", func(tx storage.ReadTx) {
				var keys []string
				Expect(tx.ScanEntityByVersionDesc("widget", 0, func(r storage.Row) error {
					keys = append(keys, r.Key)
					return nil
				})).To(Succeed())
				Expect(keys).To(BeEmpty())
			})
		})

		It("keeps text compaction floors independent per namespace", func() {
			writeTo(a, "ws1", func(tx storage.WriteTx) {
				Expect(tx.PutTextFloor("widget", "k", "body", []byte("from-a"))).To(Succeed())
			})
			readFrom(b, "ws1", func(tx storage.ReadTx) {
				vv, err := tx.TextFloor("widget", "k", "body")
				Expect(err).NotTo(HaveOccurred())
				Expect(vv).To(BeEmpty())
			})
			readFrom(a, "ws1", func(tx storage.ReadTx) {
				vv, err := tx.TextFloor("widget", "k", "body")
				Expect(err).NotTo(HaveOccurred())
				Expect(vv).To(Equal([]byte("from-a")))
			})
		})

		It("keeps client watermarks independent per namespace", func() {
			writeTo(a, "ws1", func(tx storage.WriteTx) {
				Expect(tx.SetClientLastMutationID("c1", 7)).To(Succeed())
			})
			readFrom(b, "ws1", func(tx storage.ReadTx) {
				id, err := tx.ClientLastMutationID("c1")
				Expect(err).NotTo(HaveOccurred())
				Expect(id).To(BeZero())
			})
		})

		It("scopes ListWorkspaceIDs to the namespace", func() {
			writeTo(a, "ws1", func(tx storage.WriteTx) {
				Expect(tx.PutRow("widget", "k", []byte("v"), 1, storage.Actor{})).To(Succeed())
			})
			writeTo(b, "ws2", func(tx storage.WriteTx) {
				Expect(tx.PutRow("widget", "k", []byte("v"), 1, storage.Actor{})).To(Succeed())
			})
			ids, err := a.ListWorkspaceIDs(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(ids).To(ConsistOf("ws1"))
			ids, err = b.ListWorkspaceIDs(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(ids).To(ConsistOf("ws2"))
		})

		It("scopes ListPendingWorkspaceIDs to the namespace", func() {
			// A consumer name is not namespaced, so two apps hosted in one
			// database routinely run the same one. Leaking a workspace across
			// would hand a consumer work in an app it knows nothing about.
			for _, s := range []storage.Adapter{a, b} {
				writeTo(s, "ws1", func(tx storage.WriteTx) {
					seq, err := tx.NextSeq()
					Expect(err).NotTo(HaveOccurred())
					Expect(tx.PutRow("widget", "k", []byte("v"), seq, storage.Actor{})).To(Succeed())
				})
			}
			writeTo(a, "ws1", func(tx storage.WriteTx) {
				Expect(tx.SetConsumerPosition("indexer", storage.ConsumerPosition{Seq: 1})).To(Succeed())
			})
			Expect(a.ListPendingWorkspaceIDs(ctx, "indexer", "", 10)).To(BeEmpty())
			Expect(b.ListPendingWorkspaceIDs(ctx, "indexer", "", 10)).To(ConsistOf("ws1"),
				"catching up in one namespace must not catch up in the other")
		})

		It("scopes tombstone GC to the namespace", func() {
			for _, s := range []storage.Adapter{a, b} {
				writeTo(s, "ws1", func(tx storage.WriteTx) {
					Expect(tx.DeleteRow("widget", "dead", 1, storage.Actor{})).To(Succeed())
				})
			}
			writeTo(a, "ws1", func(tx storage.WriteTx) {
				horizon, err := tx.CollectTombstones(time.Now().Add(time.Hour))
				Expect(err).NotTo(HaveOccurred())
				Expect(horizon).To(BeEquivalentTo(1))
			})
			readFrom(a, "ws1", func(tx storage.ReadTx) {
				_, err := tx.GetRow("widget", "dead")
				Expect(err).To(MatchError(storage.ErrNotFound))
			})
			readFrom(b, "ws1", func(tx storage.ReadTx) {
				row, err := tx.GetRow("widget", "dead")
				Expect(err).NotTo(HaveOccurred())
				Expect(row.Deleted).To(BeTrue(), "the other namespace keeps its tombstone")
				horizon, err := tx.TombstoneHorizon()
				Expect(err).NotTo(HaveOccurred())
				Expect(horizon).To(BeZero(), "the other namespace's horizon is untouched")
			})
		})

		It("scopes idempotency keys and their GC to the namespace", func() {
			for _, s := range []storage.Adapter{a, b} {
				writeTo(s, "ws1", func(tx storage.WriteTx) {
					Expect(tx.PutIdempotencyRecord("k", storage.IdempotencyRecord{Seq: 4})).To(Succeed())
				})
			}
			writeTo(a, "ws1", func(tx storage.WriteTx) {
				removed, err := tx.CollectIdempotencyKeys(time.Now().Add(time.Hour))
				Expect(err).NotTo(HaveOccurred())
				Expect(removed).To(Equal(1))
			})
			readFrom(b, "ws1", func(tx storage.ReadTx) {
				rec, found, err := tx.IdempotencyRecord("k")
				Expect(err).NotTo(HaveOccurred())
				Expect(found).To(BeTrue(), "the other namespace keeps its claim")
				Expect(rec.Seq).To(BeEquivalentTo(4))
			})
		})

		It("scopes idle-client GC to the namespace", func() {
			for _, s := range []storage.Adapter{a, b} {
				writeTo(s, "ws1", func(tx storage.WriteTx) {
					Expect(tx.SetClientLastMutationID("c1", 4)).To(Succeed())
				})
			}
			writeTo(a, "ws1", func(tx storage.WriteTx) {
				removed, err := tx.CollectIdleClients(time.Now().Add(time.Hour))
				Expect(err).NotTo(HaveOccurred())
				Expect(removed).To(Equal(1))
			})
			readFrom(b, "ws1", func(tx storage.ReadTx) {
				id, err := tx.ClientLastMutationID("c1")
				Expect(err).NotTo(HaveOccurred())
				Expect(id).To(BeEquivalentTo(4), "the other namespace keeps its watermark")
			})
		})
	})
}
