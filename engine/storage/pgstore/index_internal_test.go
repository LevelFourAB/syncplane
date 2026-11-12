package pgstore

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/internal/pgtest"
	"github.com/levelfourab/syncplane/engine/storage"
)

// The index entry calls of a transaction wait in a log and reach the server
// as one statement (see index.go). The conformance suite holds the adapter to
// what the calls must do; these specs hold it to what the log must not change:
// that every read in the transaction sees the calls before it, that a
// savepoint rewind takes back the calls it covers and nothing else, and that a
// failed write cannot be lost. They run on both layouts.

// scannedEntry is one entry a scan yields.
type scannedEntry struct {
	entry  string
	rowKey string
}

// scanIndex returns what ScanIndex yields for the whole of one index.
func scanIndex(tx storage.ReadTx, entity, index string, order storage.ScanOrder) []scannedEntry {
	GinkgoHelper()
	var out []scannedEntry
	Expect(tx.ScanIndex(entity, index, nil, nil, 0, order, func(entry []byte, rowKey string) error {
		out = append(out, scannedEntry{string(entry), rowKey})
		return nil
	})).To(Succeed())
	return out
}

// rowKeys returns the row keys ScanIndex yields for the whole of one index.
func rowKeys(tx storage.ReadTx, entity, index string) []string {
	GinkgoHelper()
	var out []string
	for _, e := range scanIndex(tx, entity, index, storage.ScanAsc) {
		out = append(out, e.rowKey)
	}
	return out
}

// tagEntry is an entry that does not end with its row key, so the adapter
// stores it with the row id. Rows that share tag hold byte-equal entries.
func tagEntry(tag string) storage.IndexEntry {
	return storage.IndexEntry{Index: "by_tag", Key: []byte(tag)}
}

// docEntries is the entry set the specs give a row: one entry that ends with
// the row key and one that does not.
func docEntries(v, rowKey string) []storage.IndexEntry {
	return []storage.IndexEntry{indexEntry("by_v", v, rowKey), tagEntry("t")}
}

// workspaceNumber reads the workspace number of ws, and false when it has
// none.
func workspaceNumber(ctx context.Context, store *Store, ws string) (int32, bool) {
	GinkgoHelper()
	var wsn *int32
	Expect(store.pool.QueryRow(ctx,
		`SELECT (SELECT scope FROM index_scopes WHERE workspace_id = $1 AND entity = '' AND idx = '')`, ws).Scan(&wsn)).To(Succeed())
	if wsn == nil {
		return 0, false
	}
	return *wsn, true
}

// numberedRows counts the tuples index_rows and index_entries hold for a
// workspace number.
func numberedRows(ctx context.Context, store *Store, wsn int32) (rows, entries int) {
	GinkgoHelper()
	Expect(store.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM index_rows WHERE wsn = $1),
		(SELECT count(*) FROM index_entries WHERE wsn = $1)`, wsn).Scan(&rows, &entries)).To(Succeed())
	return rows, entries
}

// indexTableRows counts the tuples the three index tables hold for ws.
func indexTableRows(ctx context.Context, store *Store, ws string) (scopes, rows, entries int) {
	GinkgoHelper()
	Expect(store.pool.QueryRow(ctx, `SELECT count(*) FROM index_scopes WHERE workspace_id = $1`, ws).Scan(&scopes)).To(Succeed())
	if wsn, ok := workspaceNumber(ctx, store, ws); ok {
		rows, entries = numberedRows(ctx, store, wsn)
	}
	return scopes, rows, entries
}

var _ = Describe("The row key of an index entry", func() {
	DescribeTable("is cut out of the entry when the entry ends with it and two zero bytes",
		func(k, rowKey string, ends bool) {
			Expect(endsWithRowKey([]byte(k), rowKey)).To(Equal(ends))
			if ends {
				Expect(entryRowKey([]byte(k), -int64(len(rowKey)))).To(Equal(rowKey))
			}
		},
		Entry("an entry of the engine", "\x01v\x00\x00r1\x00\x00", "r1", true),
		Entry("the row key alone", "r1\x00\x00", "r1", true),
		Entry("an empty row key", "\x01v\x00\x00\x00\x00", "", true),
		Entry("no end after the row key", "\x01v\x00\x00r1", "r1", false),
		Entry("one zero byte after the row key", "\x01v\x00\x00r1\x00", "r1", false),
		Entry("another row key of the same length", "\x01v\x00\x00r2\x00\x00", "r1", false),
		Entry("an entry shorter than the row key", "1\x00\x00", "r1", false),
		Entry("an entry shorter than the end", "\x00", "", false),
		Entry("an empty entry", "", "", false),
	)

	It("is refused at a length the entry cannot hold", func() {
		_, err := entryRowKey([]byte("r1\x00\x00"), -3)
		Expect(err).To(MatchError(ContainSubstring("does not fit")))
		_, err = entryRowKey([]byte("\x00"), 0)
		Expect(err).To(MatchError(ContainSubstring("does not fit")))
	})
})

var _ = Describe("Postgres index entries", func() {
	ctx := context.Background()
	const ws = "ws"

	for _, layout := range indexLayouts {
		Describe(layout.name, func() {
			var store *Store
			BeforeEach(func() {
				var err error
				store, err = Open(ctx, pgtest.DSN(), layout.opts...)
				Expect(err).NotTo(HaveOccurred())
				DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
			})
			write := func(fn func(t *pgTx) error) error {
				return store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error { return fn(tx.(*pgTx)) })
			}
			stored := func(entity, index string) []string {
				GinkgoHelper()
				var out []string
				Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
					out = rowKeys(tx, entity, index)
					return nil
				})).To(Succeed())
				return out
			}

			It("keeps the row key in the entry when the entry ends with it, and reads it from the row id when not", func() {
				// The engine writes a row key in an entry as CESU-8, so a key
				// with a code point above U+FFFF is not the tail of its entry.
				astral := "r\U0001F600"
				cesu := "r\xed\xa0\xbd\xed\xb8\x80"
				Expect(write(func(t *pgTx) error {
					Expect(t.PutIndexEntries("doc", "r1", []storage.IndexEntry{indexEntry("by_v", "a", "r1"), tagEntry("x")})).To(Succeed())
					Expect(t.PutIndexEntries("doc", "r2", []storage.IndexEntry{indexEntry("by_v", "b", "r2"), tagEntry("x")})).To(Succeed())
					Expect(t.PutIndexEntries("doc", astral, []storage.IndexEntry{
						{Index: "by_v", Key: []byte("\x01c" + indexEntryEnd + cesu + indexEntryEnd)},
					})).To(Succeed())
					return nil
				})).To(Succeed())

				var withKey, withID int
				Expect(store.pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE rk = -2), count(*) FILTER (WHERE rk > 0 AND rk = rid)
					FROM index_entries`).Scan(&withKey, &withID)).To(Succeed())
				Expect(withKey).To(Equal(2), "the two entries that end with a row key of two bytes")
				Expect(withID).To(Equal(3), "the two tag entries and the entry of the astral key")

				Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
					byV := scanIndex(tx, "doc", "by_v", storage.ScanAsc)
					Expect(byV).To(Equal([]scannedEntry{
						{"\x01a" + indexEntryEnd + "r1" + indexEntryEnd, "r1"},
						{"\x01b" + indexEntryEnd + "r2" + indexEntryEnd, "r2"},
						{"\x01c" + indexEntryEnd + cesu + indexEntryEnd, astral},
					}))
					// Byte-equal entries of two rows: both come out, in an
					// order that the descending scan mirrors.
					asc := scanIndex(tx, "doc", "by_tag", storage.ScanAsc)
					Expect(asc).To(ConsistOf(scannedEntry{"x", "r1"}, scannedEntry{"x", "r2"}))
					desc := scanIndex(tx, "doc", "by_tag", storage.ScanDesc)
					slices.Reverse(desc)
					Expect(desc).To(Equal(asc))
					var first []string
					Expect(tx.ScanIndex("doc", "by_tag", nil, nil, 1, storage.ScanAsc, func(_ []byte, rowKey string) error {
						first = append(first, rowKey)
						return nil
					})).To(Succeed())
					Expect(first).To(Equal([]string{asc[0].rowKey}))
					return nil
				})).To(Succeed())
			})

			It("keeps one entry of four rows apart when it ends with the row keys of three of them", func() {
				// "ab" and two zero bytes ends with the row keys "ab", "b" and
				// "", so those three rows store it with three key lengths, and
				// the row "zz" with its row id.
				same := []storage.IndexEntry{{Index: "by_v", Key: []byte("ab" + indexEntryEnd)}}
				Expect(write(func(t *pgTx) error {
					for _, key := range []string{"zz", "b", "", "ab"} {
						Expect(t.PutIndexEntries("doc", key, same)).To(Succeed())
					}
					return nil
				})).To(Succeed())
				var lengths []int64
				rows, err := store.pool.Query(ctx, `SELECT rk FROM index_entries WHERE rk <= 0 ORDER BY rk`)
				Expect(err).NotTo(HaveOccurred())
				for rows.Next() {
					var rk int64
					Expect(rows.Scan(&rk)).To(Succeed())
					lengths = append(lengths, rk)
				}
				Expect(rows.Err()).NotTo(HaveOccurred())
				Expect(lengths).To(Equal([]int64{-2, -1, 0}))
				Expect(stored("doc", "by_v")).To(Equal([]string{"ab", "b", "", "zz"}), "by key length, then the row with a row id")
				Expect(write(func(t *pgTx) error { return t.DeleteIndexEntries("doc", "b") })).To(Succeed())
				Expect(stored("doc", "by_v")).To(Equal([]string{"ab", "", "zz"}))
			})

			It("reads the row keys of entries that hold a row id on every page of a scan", func() {
				const rows = 3*scanBatch + 17
				Expect(write(func(t *pgTx) error {
					for i := range rows {
						Expect(t.PutIndexEntries("doc", fmt.Sprintf("r%04d", i), []storage.IndexEntry{tagEntry("x")})).To(Succeed())
					}
					return nil
				})).To(Succeed())
				want := make([]string, rows)
				for i := range want {
					want[i] = fmt.Sprintf("r%04d", i)
				}
				Expect(stored("doc", "by_tag")).To(ConsistOf(want), "one scan through a cursor")
				var ranged []string
				Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
					return tx.ScanIndexRanges("doc", "by_tag", []storage.IndexRange{
						{Lo: []byte("x"), Hi: []byte("y")}, {Lo: []byte("y"), Hi: []byte("z")},
					}, func(_ int, _ []byte, rowKey string) error {
						ranged = append(ranged, rowKey)
						return nil
					})
				})).To(Succeed())
				Expect(ranged).To(ConsistOf(want), "two ranges, paged by key")
			})

			It("gives a row that comes back a new row id", func() {
				rid := func() (id int64) {
					GinkgoHelper()
					Expect(store.pool.QueryRow(ctx, `SELECT rid FROM index_rows WHERE key = 'r1'`).Scan(&id)).To(Succeed())
					return id
				}
				Expect(write(func(t *pgTx) error { return t.PutIndexEntries("doc", "r1", docEntries("a", "r1")) })).To(Succeed())
				first := rid()
				Expect(write(func(t *pgTx) error { return t.PutIndexEntries("doc", "r1", docEntries("b", "r1")) })).To(Succeed())
				Expect(rid()).To(Equal(first), "a replace keeps the row id")
				Expect(write(func(t *pgTx) error { return t.DeleteIndexEntries("doc", "r1") })).To(Succeed())
				Expect(write(func(t *pgTx) error { return t.PutIndexEntries("doc", "r1", docEntries("c", "r1")) })).To(Succeed())
				Expect(rid()).To(BeNumerically(">", first))
				Expect(stored("doc", "by_tag")).To(Equal([]string{"r1"}))
			})

			Describe("a read in the transaction of the calls", func() {
				// Each reader of the index tables must write the logged calls
				// before it reads. The stored state has r1 and r2; the
				// transaction replaces r1, deletes r2 and adds r3, and deletes
				// and puts r4 in one go.
				BeforeEach(func() {
					Expect(write(func(t *pgTx) error {
						for _, key := range []string{"r1", "r2"} {
							Expect(t.PutRow("doc", key, []byte("v"), 1, storage.Actor{})).To(Succeed())
							Expect(t.PutIndexEntries("doc", key, docEntries("old", key))).To(Succeed())
						}
						return nil
					})).To(Succeed())
				})
				calls := func(t *pgTx) {
					GinkgoHelper()
					Expect(t.PutIndexEntries("doc", "r1", docEntries("new", "r1"))).To(Succeed())
					Expect(t.DeleteIndexEntries("doc", "r2")).To(Succeed())
					Expect(t.PutIndexEntries("doc", "r3", docEntries("new", "r3"))).To(Succeed())
					Expect(t.PutIndexEntries("doc", "r4", docEntries("first", "r4"))).To(Succeed())
					Expect(t.DeleteIndexEntries("doc", "r4")).To(Succeed())
					Expect(t.PutIndexEntries("doc", "r4", docEntries("last", "r4"))).To(Succeed())
				}
				want := []scannedEntry{
					{"\x01last" + indexEntryEnd + "r4" + indexEntryEnd, "r4"},
					{"\x01new" + indexEntryEnd + "r1" + indexEntryEnd, "r1"},
					{"\x01new" + indexEntryEnd + "r3" + indexEntryEnd, "r3"},
				}

				It("is seen by ScanIndex", func() {
					Expect(write(func(t *pgTx) error {
						calls(t)
						Expect(scanIndex(t, "doc", "by_v", storage.ScanAsc)).To(Equal(want))
						Expect(rowKeys(t, "doc", "by_tag")).To(ConsistOf("r1", "r3", "r4"))
						return nil
					})).To(Succeed())
				})

				It("is seen by ScanIndexRanges, over ranges with and without an upper bound", func() {
					Expect(write(func(t *pgTx) error {
						calls(t)
						for _, ranges := range [][]storage.IndexRange{
							{{Lo: []byte("\x01l"), Hi: []byte("\x01m")}, {Lo: []byte("\x01n"), Hi: []byte("\x01o")}},
							{{Lo: []byte("\x01l")}},
						} {
							var got []scannedEntry
							Expect(t.ScanIndexRanges("doc", "by_v", ranges, func(_ int, entry []byte, rowKey string) error {
								got = append(got, scannedEntry{string(entry), rowKey})
								return nil
							})).To(Succeed())
							Expect(got).To(Equal(want))
						}
						return nil
					})).To(Succeed())
				})

				It("is deleted by PurgeEntityRows with the rows it selects", func() {
					Expect(write(func(t *pgTx) error {
						calls(t)
						// r3 has entries from this transaction and a row to
						// select them by.
						Expect(t.PutRow("doc", "r3", []byte("v"), 2, storage.Actor{})).To(Succeed())
						n, _, err := t.PurgeEntityRows("doc", "", time.Time{}, 0)
						Expect(err).NotTo(HaveOccurred())
						Expect(n).To(Equal(3))
						// r4 holds entries and no row, so the purge leaves them.
						Expect(rowKeys(t, "doc", "by_v")).To(Equal([]string{"r4"}))
						return nil
					})).To(Succeed())
					Expect(stored("doc", "by_tag")).To(Equal([]string{"r4"}))
					_, rows, entries := indexTableRows(ctx, store, ws)
					Expect(rows).To(Equal(1))
					Expect(entries).To(Equal(2))
				})

				It("is written once, by the statement that COMMIT travels with", func() {
					Expect(write(func(t *pgTx) error {
						calls(t)
						Expect(t.resolveEntries()).To(Succeed())
						queued := len(t.queued)
						Expect(t.resolveEntries()).To(Succeed())
						Expect(t.queued).To(HaveLen(queued), "nothing is left to write")
						return nil
					})).To(Succeed())
					Expect(stored("doc", "by_v")).To(Equal([]string{"r4", "r1", "r3"}))
				})
			})

			Describe("a savepoint", func() {
				boom := errors.New("boom")

				It("takes back the calls it covers without a round trip, and keeps the ones before it", func() {
					Expect(write(func(t *pgTx) error {
						Expect(t.loadIndexScopes()).To(Succeed())
						Expect(t.PutIndexEntries("doc", "before", docEntries("v", "before"))).To(Succeed())
						sends := t.sends
						Expect(t.InSavepoint(func() error {
							Expect(t.PutIndexEntries("doc", "doomed", docEntries("v", "doomed"))).To(Succeed())
							Expect(t.DeleteIndexEntries("doc", "before")).To(Succeed())
							return boom
						})).To(MatchError(boom))
						Expect(t.sends).To(Equal(sends))
						Expect(t.entryLog).To(HaveLen(1))
						return nil
					})).To(Succeed())
					Expect(stored("doc", "by_v")).To(Equal([]string{"before"}))
				})

				It("that fails after a read wrote the log leaves the earlier calls to be written again", func() {
					Expect(write(func(t *pgTx) error {
						Expect(t.PutIndexEntries("doc", "before", docEntries("v", "before"))).To(Succeed())
						Expect(t.InSavepoint(func() error {
							Expect(t.PutIndexEntries("doc", "doomed", docEntries("v", "doomed"))).To(Succeed())
							// The read writes both calls, inside the savepoint.
							Expect(rowKeys(t, "doc", "by_v")).To(Equal([]string{"before", "doomed"}))
							return boom
						})).To(MatchError(boom))
						Expect(t.entryDone).To(BeZero(), "the call before the savepoint is not written any more")
						Expect(rowKeys(t, "doc", "by_v")).To(Equal([]string{"before"}))
						return nil
					})).To(Succeed())
					Expect(stored("doc", "by_v")).To(Equal([]string{"before"}))
					Expect(stored("doc", "by_tag")).To(Equal([]string{"before"}))
				})

				It("inside another takes back its own calls only, and goes with the outer one when that fails", func() {
					Expect(write(func(t *pgTx) error {
						Expect(t.PutIndexEntries("doc", "a", docEntries("v", "a"))).To(Succeed())
						Expect(t.InSavepoint(func() error {
							Expect(t.PutIndexEntries("doc", "b", docEntries("v", "b"))).To(Succeed())
							Expect(t.InSavepoint(func() error {
								Expect(t.PutIndexEntries("doc", "c", docEntries("v", "c"))).To(Succeed())
								Expect(rowKeys(t, "doc", "by_v")).To(Equal([]string{"a", "b", "c"}))
								return boom
							})).To(MatchError(boom))
							Expect(rowKeys(t, "doc", "by_v")).To(Equal([]string{"a", "b"}))
							return nil
						})).To(Succeed())
						Expect(t.InSavepoint(func() error {
							Expect(t.InSavepoint(func() error {
								Expect(t.PutIndexEntries("doc", "d", docEntries("v", "d"))).To(Succeed())
								Expect(rowKeys(t, "doc", "by_v")).To(Equal([]string{"a", "b", "d"}))
								return nil
							})).To(Succeed())
							Expect(t.DeleteIndexEntries("doc", "a")).To(Succeed())
							return boom
						})).To(MatchError(boom))
						return nil
					})).To(Succeed())
					Expect(stored("doc", "by_v")).To(Equal([]string{"a", "b"}))
				})

				It("that fails drops the ids of the indexes it made", func() {
					Expect(write(func(t *pgTx) error {
						Expect(t.InSavepoint(func() error {
							Expect(t.PutIndexEntries("doc", "doomed", docEntries("v", "doomed"))).To(Succeed())
							// The read makes the ids, inside the savepoint.
							Expect(rowKeys(t, "doc", "by_v")).To(Equal([]string{"doomed"}))
							return boom
						})).To(MatchError(boom))
						Expect(t.indexScopes).To(BeNil())
						return t.PutIndexEntries("doc", "kept", docEntries("v", "kept"))
					})).To(Succeed())
					// A second transaction finds the entries by the ids the
					// table holds, which are the ids they were written with.
					Expect(stored("doc", "by_v")).To(Equal([]string{"kept"}))
					scopes, rows, entries := indexTableRows(ctx, store, ws)
					Expect([]int{scopes, rows, entries}).To(Equal([]int{4, 1, 2}), "the workspace, the entity and two indexes; one row; two entries")
				})
			})

			Describe("a write that fails", func() {
				// An entry this long does not fit a b-tree tuple, so the
				// statement that writes it fails. The bytes are random, because
				// Postgres compresses a long value before it gives up on it.
				tooLong := func() []storage.IndexEntry {
					k := make([]byte, 6000)
					r := rand.New(rand.NewPCG(1, 2))
					for i := range k {
						k[i] = byte(r.Uint32())
					}
					return []storage.IndexEntry{{Index: "by_v", Key: k}}
				}
				nothingStored := func() {
					GinkgoHelper()
					Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
						_, err := tx.GetRow("doc", "ok")
						Expect(err).To(MatchError(storage.ErrNotFound))
						Expect(rowKeys(tx, "doc", "by_v")).To(BeEmpty())
						return nil
					})).To(Succeed())
				}

				It("at COMMIT fails the transaction", func() {
					err := write(func(t *pgTx) error {
						Expect(t.PutRow("doc", "ok", []byte("v"), 1, storage.Actor{})).To(Succeed())
						Expect(t.PutIndexEntries("doc", "ok", docEntries("v", "ok"))).To(Succeed())
						return t.PutIndexEntries("doc", "bad", tooLong())
					})
					Expect(err).To(MatchError(ContainSubstring("index row")))
					nothingStored()
				})

				It("in a savepoint fails the transaction also when the caller ignores the error of the savepoint", func() {
					err := write(func(t *pgTx) error {
						Expect(t.PutRow("doc", "ok", []byte("v"), 1, storage.Actor{})).To(Succeed())
						Expect(t.PutIndexEntries("doc", "ok", docEntries("v", "ok"))).To(Succeed())
						// The body of a mutator may do this: the savepoint
						// reports the failed write, and the body returns nil.
						_ = t.InSavepoint(func() error {
							Expect(t.PutIndexEntries("doc", "bad", tooLong())).To(Succeed())
							return t.ScanIndex("doc", "by_v", nil, nil, 0, storage.ScanAsc, func([]byte, string) error { return nil })
						})
						return nil
					})
					Expect(err).To(MatchError(ContainSubstring("index row")))
					nothingStored()
				})

				It("is not written when its savepoint is taken back first", func() {
					boom := errors.New("boom")
					Expect(write(func(t *pgTx) error {
						Expect(t.PutRow("doc", "ok", []byte("v"), 1, storage.Actor{})).To(Succeed())
						Expect(t.PutIndexEntries("doc", "ok", docEntries("v", "ok"))).To(Succeed())
						Expect(t.InSavepoint(func() error {
							Expect(t.PutIndexEntries("doc", "bad", tooLong())).To(Succeed())
							return boom
						})).To(MatchError(boom))
						return nil
					})).To(Succeed())
					Expect(stored("doc", "by_v")).To(Equal([]string{"ok"}))
				})
			})

			Describe("a large transaction", func() {
				It("writes more rows than one statement takes", func() {
					const rows = 2*indexRowsPerStatement + 201
					key := func(i int) string { return fmt.Sprintf("r%05d", i) }
					Expect(write(func(t *pgTx) error {
						for i := range rows {
							Expect(t.PutIndexEntries("doc", key(i), docEntries("a", key(i)))).To(Succeed())
						}
						Expect(t.resolveEntries()).To(Succeed())
						var puts int
						for _, q := range t.queued {
							if q.sql == indexPutSQL {
								puts++
							}
						}
						Expect(puts).To(Equal(3))
						return nil
					})).To(Succeed())
					Expect(stored("doc", "by_v")).To(HaveLen(rows))
					// Replace every third, delete every third, in one go.
					Expect(write(func(t *pgTx) error {
						for i := range rows {
							switch i % 3 {
							case 0:
								Expect(t.PutIndexEntries("doc", key(i), docEntries("b", key(i)))).To(Succeed())
							case 1:
								Expect(t.DeleteIndexEntries("doc", key(i))).To(Succeed())
							}
						}
						return nil
					})).To(Succeed())
					var a, b int
					Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
						for _, e := range scanIndex(tx, "doc", "by_v", storage.ScanAsc) {
							if e.entry[1] == 'a' {
								a++
							} else {
								b++
							}
						}
						return nil
					})).To(Succeed())
					Expect(a).To(Equal(rows / 3))
					Expect(b).To(Equal((rows + 2) / 3))
					Expect(stored("doc", "by_tag")).To(HaveLen(a + b))
				})

				It("writes the log when it holds too many entries, and a savepoint still takes its part back", func() {
					const perRow = 1000
					entries := func(row int) []storage.IndexEntry {
						out := make([]storage.IndexEntry, perRow)
						key := fmt.Sprintf("r%02d", row)
						for i := range out {
							out[i] = indexEntry("by_v", fmt.Sprintf("%04d", i), key)
						}
						return out
					}
					boom := errors.New("boom")
					Expect(write(func(t *pgTx) error {
						for row := range maxLoggedEntries/perRow + 2 {
							Expect(t.PutIndexEntries("doc", fmt.Sprintf("r%02d", row), entries(row))).To(Succeed())
						}
						Expect(t.sends).To(BeNumerically(">", 0), "the log was written and sent before COMMIT")
						logged, done, pending := len(t.entryLog), t.entryDone, t.entryPending
						Expect(pending).To(Equal(1+perRow), "one call is not written")
						Expect(t.InSavepoint(func() error {
							for row := 50; row < 50+maxLoggedEntries/perRow+2; row++ {
								Expect(t.PutIndexEntries("doc", fmt.Sprintf("r%02d", row), entries(row))).To(Succeed())
							}
							Expect(t.entryDone).To(BeNumerically(">", done), "the log was written in the savepoint")
							Expect(t.entryPending).NotTo(Equal(pending))
							return boom
						})).To(MatchError(boom))
						Expect(t.entryLog).To(HaveLen(logged))
						Expect(t.entryDone).To(Equal(done))
						Expect(t.entryPending).To(Equal(pending), "the count of the call before the savepoint is back")
						return nil
					})).To(Succeed())
					_, rows, kept := indexTableRows(ctx, store, ws)
					Expect(rows).To(Equal(maxLoggedEntries/perRow + 2))
					Expect(kept).To(Equal(rows * perRow))
				})

				It("lets go of the written calls outside a savepoint, and keeps them inside one", func() {
					boom := errors.New("boom")
					Expect(write(func(t *pgTx) error {
						Expect(t.PutIndexEntries("doc", "a", docEntries("v", "a"))).To(Succeed())
						Expect(t.DeleteIndexEntries("doc", "gone")).To(Succeed())
						Expect(t.resolveEntries()).To(Succeed())
						Expect(t.entryLog).To(BeEmpty())
						Expect(t.entryLog[:2]).To(Equal(make([]entryOp, 2)), "the log holds no entry of a written call")
						Expect(t.entryDone).To(BeZero())

						Expect(t.PutIndexEntries("doc", "b", docEntries("v", "b"))).To(Succeed())
						Expect(t.InSavepoint(func() error {
							Expect(t.PutIndexEntries("doc", "c", docEntries("v", "c"))).To(Succeed())
							Expect(t.resolveEntries()).To(Succeed())
							Expect(t.entryLog).To(HaveLen(2), "the rewind needs the call before the savepoint")
							Expect(t.entryDone).To(Equal(2))
							return boom
						})).To(MatchError(boom))
						Expect(t.entryLog).To(HaveLen(1))
						Expect(t.entryDone).To(BeZero())
						Expect(rowKeys(t, "doc", "by_v")).To(Equal([]string{"a", "b"}))
						Expect(t.entryLog).To(BeEmpty())
						return nil
					})).To(Succeed())
					Expect(stored("doc", "by_v")).To(Equal([]string{"a", "b"}))
				})

				It("keeps the last of many calls for one row", func() {
					Expect(write(func(t *pgTx) error {
						for i := range 5000 {
							Expect(t.PutIndexEntries("doc", "r1", docEntries(fmt.Sprintf("%04d", i), "r1"))).To(Succeed())
						}
						return nil
					})).To(Succeed())
					Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
						Expect(scanIndex(tx, "doc", "by_v", storage.ScanAsc)).To(Equal([]scannedEntry{
							{"\x014999" + indexEntryEnd + "r1" + indexEntryEnd, "r1"},
						}))
						return nil
					})).To(Succeed())
				})

				It("takes the same entry of a row twice", func() {
					Expect(write(func(t *pgTx) error {
						return t.PutIndexEntries("doc", "r1", append(docEntries("v", "r1"), docEntries("v", "r1")...))
					})).To(Succeed())
					_, _, entries := indexTableRows(ctx, store, ws)
					Expect(entries).To(Equal(2))
				})
			})

			It("writes rows of several entities with one statement, and keeps their entries apart", func() {
				Expect(write(func(t *pgTx) error {
					for _, entity := range []string{"doc", "note"} {
						for _, key := range []string{"r1", "r2"} {
							Expect(t.PutIndexEntries(entity, key, docEntries(entity, key))).To(Succeed())
						}
					}
					Expect(t.resolveEntries()).To(Succeed())
					var puts int
					for _, q := range t.queued {
						if q.sql == indexPutSQL {
							puts++
						}
					}
					Expect(puts).To(Equal(1))
					return nil
				})).To(Succeed())
				Expect(write(func(t *pgTx) error {
					Expect(t.DeleteIndexEntries("doc", "r1")).To(Succeed())
					return t.PutIndexEntries("note", "r2", docEntries("changed", "r2"))
				})).To(Succeed())
				Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
					Expect(scanIndex(tx, "doc", "by_v", storage.ScanAsc)).To(Equal([]scannedEntry{
						{"\x01doc" + indexEntryEnd + "r2" + indexEntryEnd, "r2"},
					}))
					Expect(scanIndex(tx, "note", "by_v", storage.ScanAsc)).To(Equal([]scannedEntry{
						{"\x01changed" + indexEntryEnd + "r2" + indexEntryEnd, "r2"},
						{"\x01note" + indexEntryEnd + "r1" + indexEntryEnd, "r1"},
					}))
					Expect(rowKeys(tx, "doc", "by_tag")).To(Equal([]string{"r2"}))
					Expect(rowKeys(tx, "note", "by_tag")).To(ConsistOf("r1", "r2"))
					return nil
				})).To(Succeed())
			})

			It("keeps the entries of many workspaces apart, also when they share a partition", func() {
				workspaces := make([]string, 12)
				for i := range workspaces {
					workspaces[i] = fmt.Sprintf("ws%02d", i)
				}
				put := func(w, v string, keys ...string) {
					GinkgoHelper()
					Expect(store.WithWriteTx(ctx, w, func(tx storage.WriteTx) error {
						for _, key := range keys {
							Expect(tx.PutRow("doc", key, []byte("v"), 1, storage.Actor{})).To(Succeed())
							Expect(tx.PutIndexEntries("doc", key, docEntries(v, key))).To(Succeed())
						}
						return nil
					})).To(Succeed())
				}
				// The same row keys and the same entries in each.
				for _, w := range workspaces {
					put(w, "v", "r1", "r2", "r3")
				}
				if len(layout.opts) > 0 {
					var partitions, most int
					Expect(store.pool.QueryRow(ctx, `SELECT count(*), max(n) FROM (
						SELECT tableoid, count(DISTINCT wsn) AS n FROM index_entries GROUP BY tableoid) e`).Scan(&partitions, &most)).To(Succeed())
					Expect(partitions).To(BeNumerically(">", 1))
					Expect(most).To(BeNumerically(">", 1), "the workspaces of one partition")
					var split int
					Expect(store.pool.QueryRow(ctx, `SELECT count(*) FROM (
						SELECT wsn FROM index_entries GROUP BY wsn HAVING count(DISTINCT tableoid) > 1) e`).Scan(&split)).To(Succeed())
					Expect(split).To(BeZero(), "a workspace is in one partition")
				}

				put(workspaces[1], "changed", "r1")
				Expect(store.WithWriteTx(ctx, workspaces[2], func(tx storage.WriteTx) error {
					return tx.DeleteIndexEntries("doc", "r2")
				})).To(Succeed())
				Expect(store.WithWriteTx(ctx, workspaces[3], func(tx storage.WriteTx) error {
					_, _, err := tx.PurgeEntityRows("doc", "r3", time.Time{}, 0)
					return err
				})).To(Succeed())
				wsn, ok := workspaceNumber(ctx, store, workspaces[4])
				Expect(ok).To(BeTrue())
				rows, entries := numberedRows(ctx, store, wsn)
				Expect([]int{rows, entries}).To(Equal([]int{3, 6}))
				Expect(store.DeleteWorkspace(ctx, workspaces[4])).To(Succeed())
				scopes, _, _ := indexTableRows(ctx, store, workspaces[4])
				rows, entries = numberedRows(ctx, store, wsn)
				Expect([]int{scopes, rows, entries}).To(Equal([]int{0, 0, 0}), "a deleted workspace leaves nothing")

				for i, w := range workspaces {
					want := []scannedEntry{
						{"\x01v" + indexEntryEnd + "r1" + indexEntryEnd, "r1"},
						{"\x01v" + indexEntryEnd + "r2" + indexEntryEnd, "r2"},
						{"\x01v" + indexEntryEnd + "r3" + indexEntryEnd, "r3"},
					}
					switch i {
					case 1:
						want = []scannedEntry{want[1], want[2], {"\x01changed" + indexEntryEnd + "r1" + indexEntryEnd, "r1"}}
						slices.SortFunc(want, func(a, b scannedEntry) int { return cmp.Compare(a.entry, b.entry) })
					case 2:
						want = []scannedEntry{want[0], want[2]}
					case 3:
						want = want[:2]
					case 4:
						want = nil
					}
					Expect(store.WithReadTx(ctx, w, func(tx storage.ReadTx) error {
						Expect(scanIndex(tx, "doc", "by_v", storage.ScanAsc)).To(Equal(want), w)
						Expect(rowKeys(tx, "doc", "by_tag")).To(HaveLen(len(want)), w)
						return nil
					})).To(Succeed())
				}
			})

			It("leaves no tuple of the rows PurgeEntityRows deletes", func() {
				Expect(write(func(t *pgTx) error {
					for i := range 10 {
						key := fmt.Sprintf("r%02d", i)
						Expect(t.PutRow("doc", key, []byte("v"), uint64(i+1), storage.Actor{})).To(Succeed())
						Expect(t.PutIndexEntries("doc", key, docEntries("v", key))).To(Succeed())
					}
					Expect(t.PutRow("note", "r00", []byte("v"), 11, storage.Actor{})).To(Succeed())
					return t.PutIndexEntries("note", "r00", docEntries("v", "r00"))
				})).To(Succeed())
				// In pieces, as the garbage collector asks for them.
				for {
					var more bool
					Expect(write(func(t *pgTx) error {
						var err error
						_, more, err = t.PurgeEntityRows("doc", "", time.Time{}, 4)
						return err
					})).To(Succeed())
					if !more {
						break
					}
				}
				Expect(stored("doc", "by_v")).To(BeEmpty())
				_, rows, entries := indexTableRows(ctx, store, ws)
				Expect([]int{rows, entries}).To(Equal([]int{1, 2}), "the row of the other entity")
				Expect(stored("note", "by_tag")).To(Equal([]string{"r00"}))
			})
		})
	}
})
