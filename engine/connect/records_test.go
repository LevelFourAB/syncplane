package connect_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	engineconnect "github.com/levelfourab/syncplane/engine/connect"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

func withRecords(fn func(r *engineconnect.Records)) func(*engineconnect.Config) {
	return func(cfg *engineconnect.Config) { fn(cfg.Records) }
}

var _ = Describe("Record mode", func() {
	const ws = "ws1"
	var ctx context.Context

	BeforeEach(func() { ctx = context.Background() })

	Describe("New", func() {
		It("refuses record mode without a WriterID or with a row source", func() {
			f := newRecFixture()
			_, err := engineconnect.New(engineconnect.Config{Server: f.srv, Records: &engineconnect.Records{
				Types: []engineconnect.RecordType{{Root: &testdatav1.Project{}, Store: f.store, Codec: f.codec}},
			}})
			Expect(err).To(MatchError(ContainSubstring("WriterID")))
			_, err = engineconnect.New(engineconnect.Config{Server: f.srv, WriterID: "x", Source: newTodoSource(), Records: &engineconnect.Records{
				Types: []engineconnect.RecordType{{Root: &testdatav1.Project{}, Store: f.store, Codec: f.codec}},
			}})
			Expect(err).To(HaveOccurred())
		})

		It("refuses a State", func() {
			f := newRecFixture()
			_, err := engineconnect.New(engineconnect.Config{Server: f.srv, WriterID: "x", State: keptState{}, Records: &engineconnect.Records{
				Types: []engineconnect.RecordType{{Root: &testdatav1.Project{}, Store: f.store, Codec: f.codec}},
			}})
			Expect(err).To(MatchError(ContainSubstring("State")))
		})

		It("refuses a RecordField that is not a string field", func() {
			f := newRecFixture()
			_, err := engineconnect.New(engineconnect.Config{Server: f.srv, WriterID: "x", Records: &engineconnect.Records{
				Types: []engineconnect.RecordType{{
					Root:    &testdatav1.Project{},
					Members: []engineconnect.Member{{Prototype: &testdatav1.Widget{}, RecordField: "count"}},
					Store:   f.store, Codec: f.codec,
				}},
			}})
			Expect(err).To(MatchError(ContainSubstring("not a string field")))
		})
	})

	Describe("ingest", func() {
		It("merges a record into its rows in one transaction, with its shadow", func() {
			f := newRecFixture()
			f.store.put(ws, "a.txt", "Title\none\ntwo")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())

			key := f.key(ctx, ws, "a.txt")
			Expect(key).NotTo(BeEmpty())
			Expect(f.lines(ctx, ws, key)).To(Equal("Title\none\ntwo"))
			versions := map[uint64]bool{}
			Expect(f.srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
				_, v, err := tx.Get(&testdatav1.Project{}, key)
				versions[v] = true
				for _, w := range f.widgetKeys(ctx, ws, key) {
					_, v, err := tx.Get(&testdatav1.Widget{}, w)
					if err != nil {
						return err
					}
					versions[v] = true
				}
				return err
			})).To(Succeed())
			Expect(versions).To(HaveLen(1), "the root and the members share one sequence")
		})

		It("writes nothing for its own echo, in either direction", func() {
			f := newRecFixture()
			f.store.put(ws, "a.txt", "T\none\ntwo")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			seq := f.seq(ctx, ws)

			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())
			Expect(f.seq(ctx, ws)).To(Equal(seq), "the drain skipped the connector's own merge")
			Expect(f.store.writeCount()).To(BeZero())

			key := f.key(ctx, ws, "a.txt")
			f.editLine(ctx, ws, f.widgetWith(ctx, ws, key, "one"), "uno")
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())
			Expect(f.content(ws, "a.txt")).To(Equal("T\nuno\ntwo"))
			seq = f.seq(ctx, ws)

			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())
			Expect(f.seq(ctx, ws)).To(Equal(seq), "the store's echo of the write is no edit")
			Expect(f.store.writeCount()).To(Equal(1))
		})

		It("keeps an engine edit and a store edit made at the same time", func() {
			f := newRecFixture()
			f.store.put(ws, "a.txt", "T\none\ntwo")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			key := f.key(ctx, ws, "a.txt")

			f.editLine(ctx, ws, f.widgetWith(ctx, ws, key, "one"), "one!")
			f.store.put(ws, "a.txt", "T\none\ntwo?\nthree")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())

			Expect(f.lines(ctx, ws, key)).To(Equal("T\none!\ntwo?\nthree"))
			Expect(f.content(ws, "a.txt")).To(Equal("T\none!\ntwo?\nthree"), "the merge was written back")

			writes, seq := f.store.writeCount(), f.seq(ctx, ws)
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(f.store.writeCount()).To(Equal(writes))
			Expect(f.seq(ctx, ws)).To(Equal(seq))
		})

		It("writes the merge back later when the write-back fails", func() {
			f := newRecFixture()
			f.store.put(ws, "a.txt", "T\none\ntwo")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			key := f.key(ctx, ws, "a.txt")

			f.editLine(ctx, ws, f.widgetWith(ctx, ws, key, "one"), "one!")
			f.store.put(ws, "a.txt", "T\none\ntwo?")
			f.store.fail(nil, errors.New("disk full"))
			Expect(f.conn.Reconcile(ctx, ws)).To(MatchError(ContainSubstring("disk full")))
			Expect(f.lines(ctx, ws, key)).To(Equal("T\none!\ntwo?"), "the merge committed")

			f.store.fail(nil, nil)
			f.restart()
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())
			Expect(f.content(ws, "a.txt")).To(Equal("T\none!\ntwo?"))
		})

		// Acceptance 6 with the raw Codec: the connector gives Merge the rows
		// as the engine holds them, and keeping a field the store does not
		// hold is the codec's part. RowCodec, which owns fields, covers the
		// connector's part of it.
		It("gives Merge the engine rows, with the fields the store does not hold", func() {
			var flags []bool
			f := newRecFixture()
			f.conn = f.connector(withRecords(func(r *engineconnect.Records) {
				r.Types[0].Codec = spyCodec{f.codec, func(in engineconnect.MergeInput) {
					for _, m := range in.Members {
						flags = append(flags, m.(*testdatav1.Widget).GetFlag())
					}
				}}
			}))
			f.store.put(ws, "a.txt", "T\none")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			key := f.key(ctx, ws, "a.txt")
			w := f.widgetWith(ctx, ws, key, "one")
			f.clientWrite(ctx, ws, func(tx *server.EntityTx) error {
				var row testdatav1.Widget
				if _, err := tx.Get(&row, w); err != nil {
					return err
				}
				row.Flag = true
				return tx.Put(&row)
			})

			flags = nil
			f.store.put(ws, "a.txt", "T\none more")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(flags).To(Equal([]bool{true}))
			_, widgets := f.rows(ctx, ws, key)
			Expect(widgets).To(HaveLen(1))
			Expect(bodyOf(widgets[0])).To(Equal("one more"))
			Expect(widgets[0].GetFlag()).To(BeTrue())
		})

		It("reads and merges no record whose fingerprint is unchanged", func() {
			f := newRecFixture()
			f.store.put(ws, "a.txt", "A\none")
			f.store.put(ws, "b.txt", "B\none")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			merges, renders := f.codec.calls()
			reads := f.store.readCount()

			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(f.store.readCount()).To(Equal(reads), "no content read")
			m, r := f.codec.calls()
			Expect(m).To(Equal(merges), "no Merge call")
			Expect(r).To(Equal(renders))
		})

		It("marks no record dirty for the rows of its own merge", func() {
			f := newRecFixture(withRecords(func(r *engineconnect.Records) { r.Quiet = time.Hour }))
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			f.store.put(ws, "a.txt", "A\none\ntwo")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			_, renders := f.codec.calls()

			Expect(f.conn.Drain(ctx, ws)).To(Succeed())
			Expect(snapshotShadows(ctx, f, ws)["dirty:project"]).To(BeEmpty())
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())
			_, r := f.codec.calls()
			Expect(r).To(Equal(renders), "no Render call")
		})

		It("merges no record whose store content an EngineWins first pass wrote after the list", func() {
			f := newRecFixture(withRecords(func(r *engineconnect.Records) { r.FirstPass = engineconnect.EngineWins }))
			f.addRecord(ctx, ws, "p1", "Engine", "x")
			f.store.put(ws, "Engine.txt", "Store\ny")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(f.content(ws, "Engine.txt")).To(Equal("Engine\nx"))
			merges, _ := f.codec.calls()
			Expect(merges).To(BeZero(), "the list was older than the write; the read is not")
		})

		It("makes one record of a store record that List yields twice", func() {
			f := newRecFixture()
			f.store.listTwice = true
			f.store.put(ws, "a.txt", "A\none")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(f.projects(ctx, ws)).To(HaveLen(1))

			f.store.put(ws, "a.txt", "A\ntwo")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(f.projects(ctx, ws)).To(HaveLen(1))
			Expect(f.lines(ctx, ws, f.key(ctx, ws, "a.txt"))).To(Equal("A\ntwo"))
		})

		It("writes no row and no shadow for a record the codec refuses", func() {
			f := newRecFixture()
			// After the first pass, which fails when the codec refuses a record.
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			f.codec.setFailOn("boom")
			f.store.put(ws, "a.txt", "A\nboom")
			f.store.put(ws, "b.txt", "B\nfine")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())

			Expect(f.key(ctx, ws, "a.txt")).To(BeEmpty())
			Expect(f.key(ctx, ws, "b.txt")).NotTo(BeEmpty(), "the other record of the batch was merged alone")
			var projects []string
			Expect(f.srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
				_, _, err := tx.ScanPage(&testdatav1.Project{}, "", 100, func(msg proto.Message, _ uint64) error {
					projects = append(projects, msg.(*testdatav1.Project).GetName())
					return nil
				})
				return err
			})).To(Succeed())
			Expect(projects).To(ConsistOf("B"))

			f.codec.setFailOn("")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(f.lines(ctx, ws, f.key(ctx, ws, "a.txt"))).To(Equal("A\nboom"))
		})

		It("rolls back the rows and the shadow of an update the codec refuses", func() {
			f := newRecFixture()
			f.store.put(ws, "a.txt", "A\none")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			key := f.key(ctx, ws, "a.txt")
			seq := f.seq(ctx, ws)

			f.codec.setFailOn("boom")
			f.store.put(ws, "a.txt", "A\none\nboom")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(f.seq(ctx, ws)).To(Equal(seq))
			Expect(f.lines(ctx, ws, key)).To(Equal("A\none"))

			// Had the shadow moved, the unchanged content would read as the
			// agreed state and be skipped.
			f.codec.setFailOn("")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(f.lines(ctx, ws, key)).To(Equal("A\none\nboom"))
		})

		It("writes nothing when it replays a merge whose shadow was lost in a crash", func() {
			f := newRecFixture()
			f.store.put(ws, "a.txt", "T\none\ntwo")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			key := f.key(ctx, ws, "a.txt")
			before := snapshotShadows(ctx, f, ws)

			f.store.put(ws, "a.txt", "T\nONE\ntwo\nthree")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			rows := f.widgetKeys(ctx, ws, key)
			restoreShadows(ctx, f, ws, before)

			seq, writes := f.seq(ctx, ws), f.store.writeCount()
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(f.seq(ctx, ws)).To(Equal(seq))
			Expect(f.store.writeCount()).To(Equal(writes))
			Expect(f.widgetKeys(ctx, ws, key)).To(Equal(rows))
			Expect(f.lines(ctx, ws, key)).To(Equal("T\nONE\ntwo\nthree"))
		})

		It("merges several small records in one transaction, within BatchRows", func() {
			f := newRecFixture(withRecords(func(r *engineconnect.Records) { r.BatchRows = 3 }))
			for i := range 5 {
				f.store.put(ws, fmt.Sprintf("r%d.txt", i), fmt.Sprintf("R%d\nline", i))
			}
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())

			versions := map[uint64]int{}
			Expect(f.srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
				_, _, err := tx.ScanPage(&testdatav1.Project{}, "", 100, func(_ proto.Message, v uint64) error {
					versions[v]++
					return nil
				})
				return err
			})).To(Succeed())
			// Two rows a record: two records reach the bound, the fifth is alone.
			Expect(versions).To(HaveLen(3))
			for i := range 5 {
				Expect(f.lines(ctx, ws, f.key(ctx, ws, fmt.Sprintf("r%d.txt", i)))).To(Equal(fmt.Sprintf("R%d\nline", i)))
			}
		})

		It("pauses between ingest transactions with IngestChunk and IngestPause", func() {
			f := newRecFixture(
				withRecords(func(r *engineconnect.Records) { r.BatchRows = 1 }),
				func(cfg *engineconnect.Config) { cfg.IngestChunk, cfg.IngestPause = 1, 20*time.Millisecond },
			)
			for i := range 4 {
				f.store.put(ws, fmt.Sprintf("r%d.txt", i), fmt.Sprintf("R%d", i))
			}
			start := time.Now()
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(time.Since(start)).To(BeNumerically(">=", 80*time.Millisecond))
		})

		It("ingests only the records Touch reports", func() {
			f := newRecFixture()
			f.store.put(ws, "a.txt", "A\none")
			f.store.put(ws, "b.txt", "B\none")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			a, b := f.key(ctx, ws, "a.txt"), f.key(ctx, ws, "b.txt")

			f.store.put(ws, "a.txt", "A\ntwo")
			f.store.put(ws, "b.txt", "B\ntwo")
			f.store.put(ws, "c.txt", "C")
			f.conn.Touch(ws, "a.txt", "c.txt")
			Expect(f.conn.Ingress(ctx, ws)).To(Succeed())

			Expect(f.lines(ctx, ws, a)).To(Equal("A\ntwo"))
			Expect(f.lines(ctx, ws, b)).To(Equal("B\none"))
			Expect(f.lines(ctx, ws, f.key(ctx, ws, "c.txt"))).To(Equal("C"))

			// The locators were consumed.
			f.store.put(ws, "a.txt", "A\nthree")
			Expect(f.conn.Ingress(ctx, ws)).To(Succeed())
			Expect(f.lines(ctx, ws, a)).To(Equal("A\ntwo"))
		})

		It("keeps touched locators for the next pass when ingest fails", func() {
			f := newRecFixture()
			f.store.put(ws, "a.txt", "A\none")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			f.store.put(ws, "a.txt", "A\ntwo")
			f.conn.Touch(ws, "a.txt")
			f.store.fail(errors.New("offline"), nil)
			Expect(f.conn.Ingress(ctx, ws)).NotTo(Succeed())
			f.store.fail(nil, nil)
			Expect(f.conn.Ingress(ctx, ws)).To(Succeed())
			Expect(f.lines(ctx, ws, f.key(ctx, ws, "a.txt"))).To(Equal("A\ntwo"))
		})
	})

	Describe("egress", func() {
		It("writes an engine-created record to the store", func() {
			f := newRecFixture()
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			f.addRecord(ctx, ws, "p1", "New", "x", "y")
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())
			Expect(f.content(ws, "New.txt")).To(Equal("New\nx\ny"))
			Expect(f.key(ctx, ws, "New.txt")).To(Equal("p1"))
		})

		It("batches the engine edits of a quiet period into one store write", func() {
			f := newRecFixture()
			f.store.put(ws, "a.txt", "T\none\ntwo")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			key := f.key(ctx, ws, "a.txt")
			one, two := f.widgetWith(ctx, ws, key, "one"), f.widgetWith(ctx, ws, key, "two")

			for _, s := range []string{"o", "on", "one!"} {
				f.editLine(ctx, ws, one, s)
				Expect(f.conn.Drain(ctx, ws)).To(Succeed())
				f.clock.advance(300 * time.Millisecond)
			}
			f.editLine(ctx, ws, two, "two!")
			Expect(f.conn.Drain(ctx, ws)).To(Succeed())
			Expect(f.store.writeCount()).To(BeZero())

			f.clock.advance(time.Second)
			Expect(f.conn.Drain(ctx, ws)).To(Succeed())
			Expect(f.store.writeCount()).To(Equal(1))
			Expect(f.content(ws, "a.txt")).To(Equal("T\none!\ntwo!"))
		})

		It("writes a record under continuous edits after MaxDelay", func() {
			f := newRecFixture(withRecords(func(r *engineconnect.Records) { r.MaxDelay = 3 * time.Second }))
			f.store.put(ws, "a.txt", "T\none")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			one := f.widgetWith(ctx, ws, f.key(ctx, ws, "a.txt"), "one")

			for i := range 6 {
				f.editLine(ctx, ws, one, fmt.Sprintf("edit %d", i))
				Expect(f.conn.Drain(ctx, ws)).To(Succeed())
				f.clock.advance(500 * time.Millisecond)
			}
			Expect(f.store.writeCount()).To(BeZero(), "each edit restarts the quiet period")
			f.editLine(ctx, ws, one, "edit 6")
			Expect(f.conn.Drain(ctx, ws)).To(Succeed())
			Expect(f.store.writeCount()).To(Equal(1))
			Expect(f.content(ws, "a.txt")).To(Equal("T\nedit 6"))
		})

		It("ingests first when the store changed since the last pass", func() {
			f := newRecFixture()
			f.store.put(ws, "a.txt", "T\none\ntwo")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			key := f.key(ctx, ws, "a.txt")

			f.editLine(ctx, ws, f.widgetWith(ctx, ws, key, "one"), "one!")
			f.store.put(ws, "a.txt", "T\none\ntwo?")
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())

			Expect(f.content(ws, "a.txt")).To(Equal("T\none!\ntwo?"), "the store edit stayed")
			Expect(f.lines(ctx, ws, key)).To(Equal("T\none!\ntwo?"))
		})

		It("finds the record of a deleted row after a restart", func() {
			f := newRecFixture()
			f.store.put(ws, "a.txt", "T\none\ntwo")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			key := f.key(ctx, ws, "a.txt")
			Expect(f.conn.Close()).To(Succeed())

			two := f.widgetWith(ctx, ws, key, "two")
			f.clientWrite(ctx, ws, func(tx *server.EntityTx) error { return tx.Delete(&testdatav1.Widget{}, two) })

			f.conn = f.connector()
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())
			Expect(f.content(ws, "a.txt")).To(Equal("T\none"))
		})

		It("writes pending egress on Close", func() {
			f := newRecFixture(withRecords(func(r *engineconnect.Records) { r.Quiet = time.Hour }))
			f.store.put(ws, "a.txt", "T\none")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			key := f.key(ctx, ws, "a.txt")

			f.editLine(ctx, ws, f.widgetWith(ctx, ws, key, "one"), "one!")
			Expect(f.conn.Drain(ctx, ws)).To(Succeed())
			Expect(f.content(ws, "a.txt")).To(Equal("T\none"), "the quiet period holds the write")

			Expect(f.conn.Close()).To(Succeed())
			Expect(f.content(ws, "a.txt")).To(Equal("T\none!"))
		})

		It("writes pending egress on Close after a pass that failed", func() {
			f := newRecFixture(withRecords(func(r *engineconnect.Records) { r.Quiet = time.Hour }))
			f.store.put(ws, "a.txt", "T\none")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			key := f.key(ctx, ws, "a.txt")
			f.editLine(ctx, ws, f.widgetWith(ctx, ws, key, "one"), "one!")
			Expect(f.conn.Drain(ctx, ws)).To(Succeed())

			f.store.fail(errors.New("offline"), nil)
			Expect(f.conn.Reconcile(ctx, ws)).NotTo(Succeed())
			f.store.fail(nil, nil)
			Expect(f.conn.Close()).To(Succeed())
			Expect(f.content(ws, "a.txt")).To(Equal("T\none!"))
		})

		It("runs no first pass on Flush or Forget", func() {
			f := newRecFixture()
			f.addRecord(ctx, ws, "p1", "Engine", "x")
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())
			f.conn.Forget(ws)
			Expect(f.lines(ctx, ws, "p1")).To(Equal("Engine\nx"), "a SourceWins first pass over the empty store would delete it")
			_, found, err := f.srv.ConsumerPosition(ctx, ws, "connect:"+recWriter)
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeFalse())
		})

		It("deletes the store record of a deleted root row", func() {
			f := newRecFixture()
			f.store.put(ws, "a.txt", "T\none")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			key := f.key(ctx, ws, "a.txt")
			f.clientWrite(ctx, ws, func(tx *server.EntityTx) error { return tx.Delete(&testdatav1.Project{}, key) })
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())
			Expect(f.store.locators(ws)).To(BeEmpty())
			Expect(f.key(ctx, ws, "a.txt")).To(BeEmpty())
		})

		It("marks every record for egress when the feed cursor is too old", func() {
			f := newRecFixtureFull([]server.Option{server.WithGC(50*time.Millisecond, 0)})
			f.store.put(ws, "a.txt", "A\none\ntwo")
			f.store.put(ws, "b.txt", "B\none")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			a, b := f.key(ctx, ws, "a.txt"), f.key(ctx, ws, "b.txt")

			two := f.widgetWith(ctx, ws, a, "two")
			f.clientWrite(ctx, ws, func(tx *server.EntityTx) error { return tx.Delete(&testdatav1.Widget{}, two) })
			f.editLine(ctx, ws, f.widgetWith(ctx, ws, b, "one"), "uno")
			time.Sleep(100 * time.Millisecond)
			// A consumer written inside the retention is not collected, even
			// below the horizon: it is one that is still catching up.
			pos, _, err := f.srv.ConsumerPosition(ctx, ws, "connect:"+recWriter)
			Expect(err).NotTo(HaveOccurred())
			Expect(f.srv.AdvanceConsumer(ctx, ws, "connect:"+recWriter, server.ConsumerUpdate{Cursor: pos})).To(Succeed())
			Expect(f.srv.CollectGarbage(ctx, ws)).To(Succeed())
			_, _, err = f.srv.Changes(ctx, ws, pos, 10, func(server.Change) error { return nil })
			Expect(err).To(MatchError(server.ErrCursorTooOld), "the spec reaches the horizon")

			Expect(f.conn.Flush(ctx, ws)).To(Succeed())
			Expect(f.content(ws, "a.txt")).To(Equal("A\none"))
			Expect(f.content(ws, "b.txt")).To(Equal("B\nuno"))
		})

		It("writes a record whose locator another record leaves in the same pass", func() {
			f := newRecFixture()
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			f.addRecord(ctx, ws, "p1", "X", "a")
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())

			// p0 sorts before p1, so egress meets it while p1 owns X.txt.
			f.clientWrite(ctx, ws, func(tx *server.EntityTx) error { return tx.Delete(&testdatav1.Project{}, "p1") })
			f.addRecord(ctx, ws, "p0", "X", "b")
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())
			Expect(f.content(ws, "X.txt")).To(Equal("X\nb"))
			Expect(f.key(ctx, ws, "X.txt")).To(Equal("p0"))
		})

		It("keeps the edit of a record whose locator another record holds, and writes it when the locator is free", func() {
			f := newRecFixture()
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			f.addRecord(ctx, ws, "p1", "X", "a")
			f.addRecord(ctx, ws, "p2", "X", "b")
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())
			Expect(f.content(ws, "X.txt")).To(Equal("X\na"))

			f.clientWrite(ctx, ws, func(tx *server.EntityTx) error { return tx.Delete(&testdatav1.Project{}, "p1") })
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())
			Expect(f.content(ws, "X.txt")).To(Equal("X\nb"))
		})

		It("ingests a store record the connector does not know instead of writing over it", func() {
			var overwrites int
			f := newRecFixture(withRecords(func(r *engineconnect.Records) {
				r.Overwrite = func(context.Context, engineconnect.Overwrite) error { overwrites++; return nil }
			}))
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			f.addRecord(ctx, ws, "p1", "X", "a")
			// Another program made X.txt after the last list.
			f.store.put(ws, "X.txt", "X\nmine")
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())

			Expect(f.content(ws, "X.txt")).To(Equal("X\nmine"))
			other := f.key(ctx, ws, "X.txt")
			Expect(other).NotTo(BeElementOf("", "p1"))
			Expect(f.lines(ctx, ws, other)).To(Equal("X\nmine"))
			Expect(f.lines(ctx, ws, "p1")).To(Equal("X\na"))
			Expect(overwrites).To(BeZero())

			f.clientWrite(ctx, ws, func(tx *server.EntityTx) error { return tx.Put(&testdatav1.Project{Id: "p1", Name: "Y"}) })
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())
			Expect(f.content(ws, "Y.txt")).To(Equal("Y\na"))
			Expect(f.content(ws, "X.txt")).To(Equal("X\nmine"))
		})

		It("writes both records of a row a client moved while the target record was ingested", func() {
			f := newRecFixture()
			f.store.put(ws, "a.txt", "A\nmover\nstay")
			f.store.put(ws, "b.txt", "B\nb1")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			a, b := f.key(ctx, ws, "a.txt"), f.key(ctx, ws, "b.txt")
			mover := f.widgetWith(ctx, ws, a, "mover")
			f.clientWrite(ctx, ws, func(tx *server.EntityTx) error {
				var w testdatav1.Widget
				if _, err := tx.Get(&w, mover); err != nil {
					return err
				}
				w.Label, w.Count = b, 5
				return tx.Put(&w)
			})

			// The ingest of b takes the row before a drain sees the move.
			f.store.put(ws, "b.txt", "B\nb1\nb2")
			f.conn.Touch(ws, "b.txt")
			Expect(f.conn.Ingress(ctx, ws)).To(Succeed())
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())
			Expect(f.content(ws, "b.txt")).To(Equal("B\nb1\nb2\nmover"))
			Expect(f.content(ws, "a.txt")).To(Equal("A\nstay"))
		})

		It("never writes an IngressOnly type to the store", func() {
			f := newRecFixture(withRecords(func(r *engineconnect.Records) { r.Types[0].Direction = engineconnect.IngressOnly }))
			f.store.put(ws, "a.txt", "T\none")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			key := f.key(ctx, ws, "a.txt")
			f.editLine(ctx, ws, f.widgetWith(ctx, ws, key, "one"), "uno")
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())
			Expect(f.store.writeCount()).To(BeZero())

			f.store.put(ws, "a.txt", "T2\none")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(f.lines(ctx, ws, key)).To(Equal("T2\nuno"))
			Expect(f.store.writeCount()).To(BeZero())
		})
	})

	Describe("deletes and renames", func() {
		It("deletes the rows of a record missing for longer than the grace", func() {
			f := newRecFixture()
			f.store.put(ws, "a.txt", "T\none")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			key := f.key(ctx, ws, "a.txt")

			f.store.remove(ws, "a.txt")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			root, _ := f.rows(ctx, ws, key)
			Expect(root).NotTo(BeNil(), "the grace holds the delete")

			f.clock.advance(3 * time.Second)
			Expect(f.conn.Drain(ctx, ws)).To(Succeed())
			root, widgets := f.rows(ctx, ws, key)
			Expect(root).To(BeNil())
			Expect(widgets).To(BeEmpty())
			Expect(f.key(ctx, ws, "a.txt")).To(BeEmpty())

			Expect(f.conn.Flush(ctx, ws)).To(Succeed())
			Expect(f.store.locators(ws)).To(BeEmpty(), "the delete does not come back")
		})

		It("keeps the rows of a record deleted and created again in the grace", func() {
			f := newRecFixture()
			f.store.put(ws, "a.txt", "T\none\ntwo")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			key := f.key(ctx, ws, "a.txt")
			rows := f.widgetKeys(ctx, ws, key)

			f.store.remove(ws, "a.txt")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			f.clock.advance(time.Second)
			f.store.put(ws, "a.txt", "T\none\ntwo\nthree")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			f.clock.advance(5 * time.Second)
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())

			Expect(f.key(ctx, ws, "a.txt")).To(Equal(key))
			Expect(f.widgetKeys(ctx, ws, key)[:2]).To(Equal(rows))
			Expect(f.lines(ctx, ws, key)).To(Equal("T\none\ntwo\nthree"))
		})

		It("keeps the record key and the row keys of a renamed record", func() {
			f := newRecFixture()
			f.store.put(ws, "a.txt", "T\none\ntwo")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			key := f.key(ctx, ws, "a.txt")
			rows := f.widgetKeys(ctx, ws, key)

			f.store.rename(ws, "a.txt", "b.txt")
			seq := f.seq(ctx, ws)
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			f.clock.advance(5 * time.Second)
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())

			Expect(f.key(ctx, ws, "b.txt")).To(Equal(key))
			Expect(f.key(ctx, ws, "a.txt")).To(BeEmpty())
			Expect(f.widgetKeys(ctx, ws, key)).To(Equal(rows))
			Expect(f.seq(ctx, ws)).To(Equal(seq), "a rename writes no row")
			Expect(f.store.locators(ws)).To(Equal([]string{"b.txt"}))

			f.editLine(ctx, ws, rows[0], "uno")
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())
			Expect(f.content(ws, "b.txt")).To(Equal("T\nuno\ntwo"))
		})

		It("finds a rename that Touch reports", func() {
			f := newRecFixture()
			f.store.put(ws, "a.txt", "T\none")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			key := f.key(ctx, ws, "a.txt")

			f.store.rename(ws, "a.txt", "b.txt")
			f.conn.Touch(ws, "b.txt", "a.txt")
			Expect(f.conn.Ingress(ctx, ws)).To(Succeed())
			Expect(f.key(ctx, ws, "b.txt")).To(Equal(key))
		})
	})

	Describe("a crash around a store write", func() {
		type writeCase struct {
			name string
			// prepare brings the fixture to the pass that writes and returns
			// the record key and the locator the write goes to.
			prepare func(f *recFixture) (key, target string)
			// run runs the pass that writes.
			run  func(f *recFixture) error
			want string
		}
		renamed := func(f *recFixture) string {
			f.codec.nameFiles = true
			f.store.put(ws, "Old.txt", "Old\nx\ny")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			key := f.key(ctx, ws, "Old.txt")
			f.clientWrite(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Project{Id: key, Name: "New"})
			})
			return key
		}
		cases := []writeCase{{
			name: "a new engine record",
			prepare: func(f *recFixture) (string, string) {
				Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
				f.addRecord(ctx, ws, "p1", "New", "x", "y")
				return "p1", "New.txt"
			},
			run:  func(f *recFixture) error { return f.conn.Flush(ctx, ws) },
			want: "New\nx\ny",
		}, {
			name:    "an engine-side rename",
			prepare: func(f *recFixture) (string, string) { return renamed(f), "New.txt" },
			run:     func(f *recFixture) error { return f.conn.Flush(ctx, ws) },
			want:    "New\nx\ny",
		}, {
			name: "a write-back to a new locator",
			prepare: func(f *recFixture) (string, string) {
				key := renamed(f)
				f.store.put(ws, "Old.txt", "Old\nx\ny\nz")
				return key, "New.txt"
			},
			run:  func(f *recFixture) error { return f.conn.Reconcile(ctx, ws) },
			want: "New\nx\ny\nz",
		}}
		for _, tc := range cases {
			for _, written := range []bool{false, true} {
				when := "before"
				if written {
					when = "after"
				}
				It(fmt.Sprintf("keeps the keys and writes nothing extra for %s after a crash %s the store write", tc.name, when), func() {
					f := newRecFixture()
					key, target := tc.prepare(f)
					var snap map[string]map[string][]byte
					f.store.setBeforeWrite(func(context.Context, engineconnect.Record, string) error {
						f.store.setBeforeWrite(nil)
						snap = snapshotShadows(ctx, f, ws)
						if !written {
							return errors.New("crash")
						}
						return nil
					})
					_ = tc.run(f)
					Expect(snap).NotTo(BeNil(), "the pass wrote to the store")
					rows := f.widgetKeys(ctx, ws, key)
					writes := f.store.writeCount()
					// The shadows as the crash left them.
					restoreShadows(ctx, f, ws, snap)
					f.restart()

					Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
					Expect(f.conn.Flush(ctx, ws)).To(Succeed())
					f.clock.advance(5 * time.Second)
					Expect(f.conn.Flush(ctx, ws)).To(Succeed())

					Expect(f.projects(ctx, ws)).To(HaveLen(1))
					Expect(f.key(ctx, ws, target)).To(Equal(key))
					Expect(f.widgetKeys(ctx, ws, key)).To(Equal(rows))
					Expect(f.lines(ctx, ws, key)).To(Equal(tc.want))
					Expect(f.store.locators(ws)).To(Equal([]string{target}))
					Expect(f.content(ws, target)).To(Equal(tc.want))
					if written {
						Expect(f.store.writeCount()).To(Equal(writes), "the write is not made again")
					} else {
						Expect(f.store.writeCount()).To(Equal(writes + 1))
					}

					seq, writes := f.seq(ctx, ws), f.store.writeCount()
					Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
					Expect(f.conn.Flush(ctx, ws)).To(Succeed())
					Expect(f.seq(ctx, ws)).To(Equal(seq))
					Expect(f.store.writeCount()).To(Equal(writes))
				})
			}
		}

		It("saves the bookkeeping of a store write that ends after Close cancelled the pass", func() {
			f := newRecFixture(withRecords(func(r *engineconnect.Records) { r.Quiet = -1 }), func(cfg *engineconnect.Config) {
				cfg.Workspaces = func(context.Context) ([]string, error) { return []string{ws}, nil }
			})
			f.clock = nil
			f.conn = f.connector()
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			writing := make(chan struct{})
			f.store.setBeforeWrite(func(ctx context.Context, _ engineconnect.Record, _ string) error {
				f.store.setBeforeWrite(nil)
				close(writing)
				// A store that does not watch the context ends its write
				// after the cancel.
				<-ctx.Done()
				return nil
			})
			Expect(f.conn.Start(ctx)).To(Succeed())
			f.addRecord(ctx, ws, "p1", "New", "x")
			Eventually(writing).Should(BeClosed())
			Expect(f.conn.Close()).To(Succeed())

			Expect(f.key(ctx, ws, "New.txt")).To(Equal("p1"))
			f.conn = f.connector()
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(f.projects(ctx, ws)).To(Equal(map[string]string{"p1": "New"}))
		})
	})

	Describe("renames that Touch reports in two passes", func() {
		It("keeps the keys when the new locator comes first", func() {
			f := newRecFixture()
			f.store.put(ws, "a.txt", "T\none\ntwo")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			key := f.key(ctx, ws, "a.txt")
			rows := f.widgetKeys(ctx, ws, key)

			f.store.rename(ws, "a.txt", "b.txt")
			f.conn.Touch(ws, "b.txt")
			Expect(f.conn.Ingress(ctx, ws)).To(Succeed())
			f.conn.Touch(ws, "a.txt")
			Expect(f.conn.Ingress(ctx, ws)).To(Succeed())
			f.clock.advance(5 * time.Second)
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())

			Expect(f.key(ctx, ws, "b.txt")).To(Equal(key))
			Expect(f.projects(ctx, ws)).To(HaveLen(1))
			Expect(f.widgetKeys(ctx, ws, key)).To(Equal(rows))
			Expect(f.store.locators(ws)).To(Equal([]string{"b.txt"}))
		})

		It("makes a new record of a copy", func() {
			f := newRecFixture()
			f.store.put(ws, "a.txt", "T\none")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			key := f.key(ctx, ws, "a.txt")

			f.store.put(ws, "b.txt", "T\none")
			f.conn.Touch(ws, "b.txt")
			Expect(f.conn.Ingress(ctx, ws)).To(Succeed())
			Expect(f.key(ctx, ws, "a.txt")).To(Equal(key))
			Expect(f.key(ctx, ws, "b.txt")).NotTo(BeElementOf("", key))
		})
	})

	Describe("restarts", func() {
		It("keeps the edits both sides made while the connector did not run", func() {
			f := newRecFixture()
			f.store.put(ws, "a.txt", "T\none\ntwo")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			key := f.key(ctx, ws, "a.txt")
			Expect(f.conn.Close()).To(Succeed())

			f.editLine(ctx, ws, f.widgetWith(ctx, ws, key, "one"), "one!")
			f.store.put(ws, "a.txt", "T\none\ntwo?")

			f.conn = f.connector()
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())
			Expect(f.content(ws, "a.txt")).To(Equal("T\none!\ntwo?"))
			Expect(f.lines(ctx, ws, key)).To(Equal("T\none!\ntwo?"))
		})

		It("keeps its bookkeeping through a garbage collection while it does not run", func() {
			f := newRecFixtureFull([]server.Option{server.WithGC(50*time.Millisecond, 0)})
			f.store.put(ws, "a.txt", "A\none\ntwo")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			a := f.key(ctx, ws, "a.txt")
			Expect(f.conn.Close()).To(Succeed())
			pos, _, err := f.srv.ConsumerPosition(ctx, ws, "connect:"+recWriter)
			Expect(err).NotTo(HaveOccurred())

			// A record a client created, which the store does not hold, and a
			// deleted row, whose tombstone moves the horizon past the position.
			f.addRecord(ctx, ws, "p9", "Client", "x")
			two := f.widgetWith(ctx, ws, a, "two")
			f.clientWrite(ctx, ws, func(tx *server.EntityTx) error { return tx.Delete(&testdatav1.Widget{}, two) })
			time.Sleep(100 * time.Millisecond)
			Expect(f.srv.CollectGarbage(ctx, ws)).To(Succeed())
			_, _, err = f.srv.Changes(ctx, ws, pos, 10, func(server.Change) error { return nil })
			Expect(err).To(MatchError(server.ErrCursorTooOld), "the spec reaches the horizon")
			_, found, err := f.srv.ConsumerPosition(ctx, ws, "connect:"+recWriter)
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue(), "the consumer is retained")
			Expect(f.key(ctx, ws, "a.txt")).To(Equal(a), "the shadows are kept")

			f.conn = f.connector()
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())
			Expect(f.lines(ctx, ws, "p9")).To(Equal("Client\nx"), "no first pass deletes the client's record")
			Expect(f.content(ws, "Client.txt")).To(Equal("Client\nx"))
			Expect(f.content(ws, "a.txt")).To(Equal("A\none"))
			Expect(f.lines(ctx, ws, a)).To(Equal("A\none"))
		})

		It("rebuilds lost bookkeeping from the rows with EngineWins", func() {
			f := newRecFixture()
			f.store.put(ws, "T.txt", "T\none")
			f.store.put(ws, "B.txt", "B")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			a := f.key(ctx, ws, "T.txt")
			Expect(f.conn.Close()).To(Succeed())
			Expect(f.srv.ResetConsumer(ctx, ws, "connect:"+recWriter)).To(Succeed())

			var overwrites []engineconnect.Overwrite
			f.conn = f.connector(withRecords(func(r *engineconnect.Records) {
				r.FirstPass = engineconnect.EngineWins
				r.Overwrite = func(_ context.Context, o engineconnect.Overwrite) error {
					overwrites = append(overwrites, o)
					return nil
				}
			}))
			seq := f.seq(ctx, ws)
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(f.seq(ctx, ws)).To(Equal(seq), "no row changed")
			Expect(f.store.writeCount()).To(BeZero(), "the store already held each render")
			Expect(overwrites).To(BeEmpty())
			Expect(f.key(ctx, ws, "T.txt")).To(Equal(a), "the shadows are back")

			f.editLine(ctx, ws, f.widgetWith(ctx, ws, a, "one"), "uno")
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())
			Expect(f.content(ws, "T.txt")).To(Equal("T\nuno"))
		})
	})

	Describe("first pass", func() {
		It("deletes the rows no store record holds with SourceWins", func() {
			f := newRecFixture()
			f.addRecord(ctx, ws, "p9", "Stale", "x")
			f.store.put(ws, "a.txt", "A\none")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())

			root, widgets := f.rows(ctx, ws, "p9")
			Expect(root).To(BeNil())
			Expect(widgets).To(BeEmpty())
			Expect(f.lines(ctx, ws, f.key(ctx, ws, "a.txt"))).To(Equal("A\none"))
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())
			Expect(f.store.locators(ws)).To(Equal([]string{"a.txt"}))
		})

		It("writes every record to an empty store and deletes no row with EngineWins", func() {
			f := newRecFixture(withRecords(func(r *engineconnect.Records) { r.FirstPass = engineconnect.EngineWins }))
			f.addRecord(ctx, ws, "p1", "One", "x", "y")
			f.addRecord(ctx, ws, "p2", "Two")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())

			Expect(f.content(ws, "One.txt")).To(Equal("One\nx\ny"))
			Expect(f.content(ws, "Two.txt")).To(Equal("Two"))
			Expect(f.lines(ctx, ws, "p1")).To(Equal("One\nx\ny"))
			Expect(f.lines(ctx, ws, "p2")).To(Equal("Two"))
		})

		It("gives the adopter the old content of each record EngineWins replaces", func() {
			var (
				overwrites []engineconnect.Overwrite
				f          *recFixture
			)
			f = newRecFixture(withRecords(func(r *engineconnect.Records) {
				r.FirstPass = engineconnect.EngineWins
				r.Overwrite = func(_ context.Context, o engineconnect.Overwrite) error {
					Expect(f.content(o.WorkspaceID, o.Locator)).To(Equal(string(o.Old)), "called before the write")
					overwrites = append(overwrites, o)
					return nil
				}
			}))
			f.addRecord(ctx, ws, "p1", "Engine", "x")
			f.addRecord(ctx, ws, "p2", "Same")
			f.store.put(ws, "Engine.txt", "Store\ny")
			f.store.put(ws, "Same.txt", "Same")
			f.store.put(ws, "q.txt", "Q\nonly in the store")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())

			Expect(overwrites).To(HaveLen(1))
			Expect(overwrites[0].Key).To(Equal("p1"))
			Expect(string(overwrites[0].Old)).To(Equal("Store\ny"))
			Expect(string(overwrites[0].New)).To(Equal("Engine\nx"))
			Expect(f.content(ws, "Engine.txt")).To(Equal("Engine\nx"))
			Expect(f.lines(ctx, ws, "p1")).To(Equal("Engine\nx"))
			Expect(f.lines(ctx, ws, f.key(ctx, ws, "q.txt"))).To(Equal("Q\nonly in the store"))
			Expect(f.store.writeCount()).To(Equal(1))
		})

		It("takes the policy of each workspace from ChooseFirstPass", func() {
			var asked []string
			f := newRecFixture(withRecords(func(r *engineconnect.Records) {
				r.FirstPass = engineconnect.SourceWins
				r.ChooseFirstPass = func(_ context.Context, workspaceID string) (engineconnect.FirstPass, error) {
					asked = append(asked, workspaceID)
					if workspaceID == "mirror" {
						return engineconnect.EngineWins, nil
					}
					return engineconnect.SourceWins, nil
				}
			}))
			for _, id := range []string{"import", "mirror"} {
				f.addRecord(ctx, id, "p9", "Engine", "x")
				f.store.put(id, "a.txt", "A\none")
				Expect(f.conn.Reconcile(ctx, id)).To(Succeed())
				Expect(f.conn.Flush(ctx, id)).To(Succeed())
			}
			Expect(asked).To(Equal([]string{"import", "mirror"}))

			root, _ := f.rows(ctx, "import", "p9")
			Expect(root).To(BeNil(), "SourceWins deletes the rows the store does not hold")
			Expect(f.store.locators("import")).To(Equal([]string{"a.txt"}))

			Expect(f.lines(ctx, "mirror", "p9")).To(Equal("Engine\nx"), "EngineWins deletes no row")
			Expect(f.content("mirror", "Engine.txt")).To(Equal("Engine\nx"))
			Expect(f.lines(ctx, "mirror", f.key(ctx, "mirror", "a.txt"))).To(Equal("A\none"))

			Expect(f.conn.Reconcile(ctx, "mirror")).To(Succeed())
			Expect(asked).To(HaveLen(2), "only a first pass asks")
		})

		It("fails the pass when ChooseFirstPass fails, and asks again on the next pass", func() {
			fail := errors.New("graph status unknown")
			f := newRecFixture(withRecords(func(r *engineconnect.Records) {
				r.ChooseFirstPass = func(context.Context, string) (engineconnect.FirstPass, error) {
					if fail != nil {
						return 0, fail
					}
					return engineconnect.EngineWins, nil
				}
			}))
			f.addRecord(ctx, ws, "p9", "Engine", "x")
			f.store.put(ws, "a.txt", "A\none")
			Expect(f.conn.Reconcile(ctx, ws)).To(MatchError(fail))
			_, found, err := f.srv.ConsumerPosition(ctx, ws, "connect:"+recWriter)
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeFalse())
			Expect(f.lines(ctx, ws, "p9")).To(Equal("Engine\nx"))
			Expect(f.store.writeCount()).To(BeZero())

			fail = nil
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(f.content(ws, "Engine.txt")).To(Equal("Engine\nx"))
			Expect(f.lines(ctx, ws, "p9")).To(Equal("Engine\nx"))
		})

		It("deletes no row with SourceWins when the codec refuses a record, and fails the pass", func() {
			f := newRecFixture()
			f.conn = f.connector(withRecords(func(r *engineconnect.Records) {
				r.Types[0].Codec = nameMatchCodec{f.codec}
			}))
			f.addRecord(ctx, ws, "p1", "A", "precious")
			f.addRecord(ctx, ws, "p9", "Stale", "x")
			f.codec.setFailOn("boom")
			f.store.put(ws, "A.txt", "A\nprecious\nboom")
			f.store.put(ws, "b.txt", "B\nfine")

			Expect(f.conn.Reconcile(ctx, ws)).To(MatchError(ContainSubstring("refused")))
			Expect(f.lines(ctx, ws, "p1")).To(Equal("A\nprecious"), "the record the codec refused would have matched p1")
			Expect(f.lines(ctx, ws, "p9")).To(Equal("Stale\nx"))
			_, found, err := f.srv.ConsumerPosition(ctx, ws, "connect:"+recWriter)
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeFalse(), "the next pass runs the first pass again")

			f.codec.setFailOn("")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(f.key(ctx, ws, "A.txt")).To(Equal("p1"))
			Expect(f.lines(ctx, ws, "p1")).To(Equal("A\nprecious\nboom"))
			root, _ := f.rows(ctx, ws, "p9")
			Expect(root).To(BeNil())
		})

		It("ingests no store record with EngineWins when the codec cannot render a root, and fails the pass", func() {
			f := newRecFixture(withRecords(func(r *engineconnect.Records) { r.FirstPass = engineconnect.EngineWins }))
			f.addRecord(ctx, ws, "p1", "Engine", "x")
			f.store.put(ws, "Engine.txt", "Engine\nstale")
			f.codec.setFailRender("Engine")

			Expect(f.conn.Reconcile(ctx, ws)).To(MatchError(ContainSubstring("refused")))
			Expect(f.projects(ctx, ws)).To(Equal(map[string]string{"p1": "Engine"}), "the stale file made no record")
			Expect(f.content(ws, "Engine.txt")).To(Equal("Engine\nstale"))

			f.codec.setFailRender("")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(f.projects(ctx, ws)).To(Equal(map[string]string{"p1": "Engine"}))
			Expect(f.content(ws, "Engine.txt")).To(Equal("Engine\nx"), "the engine wins")
			Expect(f.lines(ctx, ws, "p1")).To(Equal("Engine\nx"))
		})

		It("stops the write and fails the pass when Overwrite fails", func() {
			f := newRecFixture(withRecords(func(r *engineconnect.Records) {
				r.FirstPass = engineconnect.EngineWins
				r.Overwrite = func(context.Context, engineconnect.Overwrite) error { return errors.New("no backup") }
			}))
			f.addRecord(ctx, ws, "p1", "Engine")
			f.store.put(ws, "Engine.txt", "Store")
			Expect(f.conn.Reconcile(ctx, ws)).To(MatchError(ContainSubstring("no backup")))
			Expect(f.content(ws, "Engine.txt")).To(Equal("Store"))
		})
	})

	Describe("a started connector", func() {
		It("writes engine edits after the quiet period and ingests touched records", func() {
			f := newRecFixture(withRecords(func(r *engineconnect.Records) {
				r.Quiet = 20 * time.Millisecond
				r.DeleteGrace = 20 * time.Millisecond
			}), func(cfg *engineconnect.Config) {
				cfg.Workspaces = func(context.Context) ([]string, error) { return []string{ws}, nil }
			})
			f.clock = nil
			f.conn = f.connector()
			f.store.put(ws, "a.txt", "T\none")
			Expect(f.conn.Start(ctx)).To(Succeed())
			Eventually(func() string { return f.key(ctx, ws, "a.txt") }).ShouldNot(BeEmpty())
			key := f.key(ctx, ws, "a.txt")

			f.editLine(ctx, ws, f.widgetWith(ctx, ws, key, "one"), "uno")
			Eventually(func() string { c, _ := f.store.get(ws, "a.txt"); return c }).Should(Equal("T\nuno"))

			f.store.put(ws, "a.txt", "T\nuno\ndos")
			f.conn.Touch(ws, "a.txt")
			Eventually(func() string { return f.lines(ctx, ws, key) }).Should(Equal("T\nuno\ndos"))

			f.store.remove(ws, "a.txt")
			f.conn.Touch(ws, "a.txt")
			Eventually(func() *testdatav1.Project { root, _ := f.rows(ctx, ws, key); return root }).Should(BeNil())
		})
	})
})

// snapshotShadows copies every record-mode shadow of the test type.
func snapshotShadows(ctx context.Context, f *recFixture, ws string) map[string]map[string][]byte {
	GinkgoHelper()
	out := map[string]map[string][]byte{}
	for _, entity := range []string{"rec:project", "loc:project", "idx:widget", "dirty:project", "gone:project", "pend:project", "fp:project"} {
		out[entity] = map[string][]byte{}
		Expect(f.srv.ScanConsumerShadows(ctx, ws, "connect:"+recWriter, entity, func(key string, raw []byte) error {
			out[entity][key] = raw
			return nil
		})).To(Succeed())
	}
	return out
}

// restoreShadows puts the shadows back as snapshotShadows found them, as a
// crash between a merge and its shadow save would have left them.
func restoreShadows(ctx context.Context, f *recFixture, ws string, snap map[string]map[string][]byte) {
	GinkgoHelper()
	now := snapshotShadows(ctx, f, ws)
	var writes []server.ConsumerShadowWrite
	for entity, keys := range now {
		for key := range keys {
			if _, ok := snap[entity][key]; !ok {
				writes = append(writes, server.ConsumerShadowWrite{Entity: entity, Key: key})
			}
		}
	}
	for entity, keys := range snap {
		for key, raw := range keys {
			writes = append(writes, server.ConsumerShadowWrite{Entity: entity, Key: key, Snapshot: raw})
		}
	}
	pos, _, err := f.srv.ConsumerPosition(ctx, ws, "connect:"+recWriter)
	Expect(err).NotTo(HaveOccurred())
	Expect(f.srv.AdvanceConsumer(ctx, ws, "connect:"+recWriter, server.ConsumerUpdate{Cursor: pos, Shadows: writes})).To(Succeed())
}
