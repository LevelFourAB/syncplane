package connect_test

import (
	"context"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"

	engineconnect "github.com/levelfourab/syncplane/engine/connect"
	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
)

// overwriteLog collects the calls of Records.Overwrite.
type overwriteLog struct {
	mu    sync.Mutex
	calls []engineconnect.Overwrite
}

func (l *overwriteLog) record(_ context.Context, o engineconnect.Overwrite) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	o.Old, o.New = append([]byte(nil), o.Old...), append([]byte(nil), o.New...)
	l.calls = append(l.calls, o)
	return nil
}

func (l *overwriteLog) all() []engineconnect.Overwrite {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]engineconnect.Overwrite(nil), l.calls...)
}

// egressOnly sets the fixture's type EgressOnly and collects its overwrites.
func egressOnly(log *overwriteLog) func(*engineconnect.Config) {
	return withRecords(func(r *engineconnect.Records) {
		r.Types[0].Direction = engineconnect.EgressOnly
		r.Overwrite = log.record
	})
}

var _ = Describe("Record mode, EgressOnly", func() {
	const ws = "ws1"
	var ctx context.Context

	BeforeEach(func() { ctx = context.Background() })

	It("refuses EgressOnly on a row-mode binding", func() {
		f := newFixture()
		_, err := engineconnect.New(engineconnect.Config{
			Server: f.srv, State: f.st, Source: newTodoSource(),
			Bindings: []engineconnect.Binding{{Prototype: &todov1.Todo{}, Direction: engineconnect.EgressOnly}},
		})
		Expect(err).To(MatchError(ContainSubstring("EgressOnly")))
	})

	It("writes every root to the store in the first pass, and ingests and deletes nothing", func() {
		var log overwriteLog
		f := newRecFixture(egressOnly(&log))
		f.addRecord(ctx, ws, "p1", "One", "x")
		f.store.put(ws, "One.txt", "One\nold")
		f.store.put(ws, "stray.txt", "Stray\nonly in the store")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())

		Expect(f.content(ws, "One.txt")).To(Equal("One\nx"))
		Expect(f.content(ws, "stray.txt")).To(Equal("Stray\nonly in the store"), "a store record with no rows stays")
		Expect(f.projects(ctx, ws)).To(Equal(map[string]string{"p1": "One"}), "no store record became rows")
		Expect(log.all()).To(ConsistOf(MatchFields(IgnoreExtras, Fields{
			"Key": Equal("p1"), "Reason": Equal(engineconnect.OverwriteEgressOnly),
			"Old": Equal([]byte("One\nold")), "New": Equal([]byte("One\nx")),
		})))
	})

	It("writes engine edits to the store", func() {
		var log overwriteLog
		f := newRecFixture(egressOnly(&log))
		f.addRecord(ctx, ws, "p1", "One", "x")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		f.editLine(ctx, ws, "p1-w0", "x!")
		Expect(f.conn.Flush(ctx, ws)).To(Succeed())
		Expect(f.content(ws, "One.txt")).To(Equal("One\nx!"))
		Expect(log.all()).To(BeEmpty())
	})

	It("merges no store edit, and writes the engine's content over it on a list", func() {
		var log overwriteLog
		f := newRecFixture(egressOnly(&log))
		f.addRecord(ctx, ws, "p1", "One", "x")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		seq := f.seq(ctx, ws)

		f.store.put(ws, "One.txt", "One\nedited")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())

		Expect(f.seq(ctx, ws)).To(Equal(seq), "no row was written")
		Expect(f.content(ws, "One.txt")).To(Equal("One\nx"))
		Expect(log.all()).To(ConsistOf(MatchFields(IgnoreExtras, Fields{
			"Key": Equal("p1"), "Locator": Equal("One.txt"), "Reason": Equal(engineconnect.OverwriteEgressOnly),
			"Old": Equal([]byte("One\nedited")), "New": Equal([]byte("One\nx")),
		})))

		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.store.writeCount()).To(Equal(2), "the restored record is agreed")
	})

	It("writes the engine's content over a store edit that Touch reports", func() {
		var log overwriteLog
		f := newRecFixture(egressOnly(&log))
		f.addRecord(ctx, ws, "p1", "One", "x")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		seq := f.seq(ctx, ws)

		f.store.put(ws, "One.txt", "One\nedited")
		f.conn.Touch(ws, "One.txt", "new.txt")
		f.store.put(ws, "new.txt", "New")
		Expect(f.conn.Ingress(ctx, ws)).To(Succeed())

		Expect(f.seq(ctx, ws)).To(Equal(seq), "no row was written")
		Expect(f.content(ws, "One.txt")).To(Equal("One\nx"))
		Expect(f.content(ws, "new.txt")).To(Equal("New"))
		Expect(log.all()).To(HaveLen(1))
	})

	It("writes the engine's content over a store edit when it egresses an engine edit", func() {
		var log overwriteLog
		f := newRecFixture(egressOnly(&log))
		f.addRecord(ctx, ws, "p1", "One", "x", "y")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())

		f.store.put(ws, "One.txt", "One\nx\nstore")
		f.editLine(ctx, ws, "p1-w0", "x!")
		Expect(f.conn.Flush(ctx, ws)).To(Succeed())

		Expect(f.content(ws, "One.txt")).To(Equal("One\nx!\ny"))
		Expect(f.lines(ctx, ws, "p1")).To(Equal("One\nx!\ny"))
		Expect(log.all()).To(ConsistOf(MatchFields(IgnoreExtras, Fields{
			"Reason": Equal(engineconnect.OverwriteEgressOnly), "Old": Equal([]byte("One\nx\nstore")),
		})))
	})

	DescribeTable("writes no row for a missing mark an earlier Bidirectional run left, in a pass that only drains",
		func(back string, overwrites int) {
			f := newRecFixture(withRecords(func(r *engineconnect.Records) { r.DeleteGrace = time.Hour }))
			f.store.put(ws, "a.txt", "T\none")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			key := f.key(ctx, ws, "a.txt")
			f.store.remove(ws, "a.txt")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(f.lines(ctx, ws, key)).To(Equal("T\none"))
			if back != "" {
				f.store.put(ws, "a.txt", back)
			}

			var log overwriteLog
			f.restart(egressOnly(&log), withRecords(func(r *engineconnect.Records) { r.DeleteGrace = -1 }))
			seq := f.seq(ctx, ws)
			Expect(f.conn.Drain(ctx, ws)).To(Succeed())
			Expect(f.seq(ctx, ws)).To(Equal(seq), "an EgressOnly type wrote a row")
			Expect(f.lines(ctx, ws, key)).To(Equal("T\none"))

			Expect(f.conn.Flush(ctx, ws)).To(Succeed())
			Expect(f.seq(ctx, ws)).To(Equal(seq))
			Expect(f.content(ws, "a.txt")).To(Equal("T\none"), "the record is written again")
			Expect(log.all()).To(HaveLen(overwrites))
		},
		Entry("when the record is still missing", "", 0),
		Entry("when the record is back with other content", "T\nedited", 1),
	)

	It("writes a record the store deleted again, and keeps its rows", func() {
		var log overwriteLog
		f := newRecFixture(egressOnly(&log))
		f.addRecord(ctx, ws, "p1", "One", "x")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())

		f.store.remove(ws, "One.txt")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		f.clock.advance(time.Hour)
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())

		Expect(f.content(ws, "One.txt")).To(Equal("One\nx"))
		Expect(f.lines(ctx, ws, "p1")).To(Equal("One\nx"))
		Expect(log.all()).To(BeEmpty(), "the store held nothing to keep")
	})
})
