package effects_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/effects"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

var _ = Describe("Effects runtime", func() {
	const ws = "ws1"
	var ctx context.Context

	BeforeEach(func() { ctx = context.Background() })

	Describe("delivery", func() {
		It("delivers committed changes in feed order with after-state", func() {
			f := newFixture()
			rec := newRecorder()
			Expect(effects.On(f.rt, "widgets", rec.handle)).To(Succeed())

			f.put(ctx, ws, &testdatav1.Widget{Id: "w1", Label: "one"})
			f.put(ctx, ws, &testdatav1.Widget{Id: "w2", Label: "two"})

			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())
			Expect(rec.keys()).To(Equal([]string{"w1", "w2"}))
			evs := rec.events()
			Expect(evs[0].After.GetLabel()).To(Equal("one"))
			Expect(evs[0].Workspace).To(Equal(ws))
			Expect(evs[0].Seq).To(BeNumerically("<", evs[1].Seq))
			Expect(evs[0].Deleted).To(BeFalse())
		})

		It("advances the position so a change is not redelivered", func() {
			f := newFixture()
			rec := newRecorder()
			Expect(effects.On(f.rt, "widgets", rec.handle)).To(Succeed())

			f.put(ctx, ws, &testdatav1.Widget{Id: "w1"})
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())
			Expect(rec.keys()).To(Equal([]string{"w1"}), "the second pass had nothing to deliver")

			f.put(ctx, ws, &testdatav1.Widget{Id: "w2"})
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())
			Expect(rec.keys()).To(Equal([]string{"w1", "w2"}))
		})

		It("writes nothing when a pass has nothing to do", func() {
			f := newFixture()
			rec := newRecorder()
			Expect(effects.On(f.rt, "widgets", rec.handle)).To(Succeed())

			// An idle pass must not take the workspace's write lock: at N
			// consumers over M workspaces that is a steady drip of transactions
			// changing nothing. It is also what lets GC treat a stale position
			// as meaningful rather than as the normal state of a quiet consumer.
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())
			positions, err := f.srv.ConsumerPositions(ctx, ws)
			Expect(err).NotTo(HaveOccurred())
			Expect(positions).To(BeEmpty())

			f.put(ctx, ws, &testdatav1.Widget{Id: "w1"})
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())
			positions, err = f.srv.ConsumerPositions(ctx, ws)
			Expect(err).NotTo(HaveOccurred())
			Expect(positions).To(HaveLen(1), "a pass that delivered something records where it got to")
			Expect(positions[0].Consumer).To(Equal("widgets"))
		})

		It("delivers only the entities the consumer registered for", func() {
			f := newFixture()
			rec := newRecorder()
			Expect(effects.On(f.rt, "widgets", rec.handle)).To(Succeed())

			f.putPair(ctx, ws, &testdatav1.Pair{Ns: "n", Id: "p1"})
			f.put(ctx, ws, &testdatav1.Widget{Id: "w1"})

			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())
			Expect(rec.keys()).To(Equal([]string{"w1"}), "the Pair change advanced the position without being delivered")
		})

		It("reports deletes as tombstones", func() {
			f := newFixture()
			rec := newRecorder()
			Expect(effects.On(f.rt, "widgets", rec.handle)).To(Succeed())

			f.put(ctx, ws, &testdatav1.Widget{Id: "w1", Label: "one"})
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())
			f.delete(ctx, ws, "w1")
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())

			evs := rec.events()
			Expect(evs).To(HaveLen(2))
			Expect(evs[1].Deleted).To(BeTrue())
			Expect(evs[1].Key).To(Equal("w1"))
			Expect(evs[1].After).To(BeNil(), "a tombstone carries no state")
		})

		It("keeps a consumer's own writes visible through Actor", func() {
			f := newFixture()
			rec := newRecorder()
			Expect(effects.On(f.rt, "widgets", rec.handle)).To(Succeed())

			f.put(ctx, ws, &testdatav1.Widget{Id: "w1"})
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())
			Expect(rec.events()).To(HaveLen(1))
			// An in-process Mutate with no caller identity is unattributed; what
			// matters here is that the field round-trips for handlers that
			// suppress their own echo by name.
			Expect(rec.events()[0].Actor.System).To(BeFalse())
		})
	})

	Describe("failure handling", func() {
		It("wedges the consumer at the failing change and commits the prefix", func() {
			f := newFixture()
			rec := newRecorder()
			boom := errors.New("destination unreachable")
			rec.fail("w2", boom)
			Expect(effects.On(f.rt, "widgets", rec.handle)).To(Succeed())

			f.put(ctx, ws, &testdatav1.Widget{Id: "w1"})
			f.put(ctx, ws, &testdatav1.Widget{Id: "w2"})
			f.put(ctx, ws, &testdatav1.Widget{Id: "w3"})

			Expect(f.rt.Drain(ctx, "widgets", ws)).To(MatchError(ContainSubstring("destination unreachable")))
			Expect(rec.keys()).To(Equal([]string{"w1"}), "w3 is behind the wedge, not skipped")

			// Retrying while the destination is still down redelivers only the
			// failing change: the prefix was committed.
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(MatchError(ContainSubstring("destination unreachable")))
			Expect(rec.keys()).To(Equal([]string{"w1"}))

			rec.recover("w2")
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())
			Expect(rec.keys()).To(Equal([]string{"w1", "w2", "w3"}))
		})

		It("does not advance at all when the first change of a page fails", func() {
			f := newFixture()
			rec := newRecorder()
			rec.fail("w1", errors.New("nope"))
			Expect(effects.On(f.rt, "widgets", rec.handle)).To(Succeed())

			f.put(ctx, ws, &testdatav1.Widget{Id: "w1"})
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(MatchError(ContainSubstring("nope")))

			rec.recover("w1")
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())
			Expect(rec.keys()).To(Equal([]string{"w1"}), "the change was redelivered, not lost")
		})

		It("reports the wedge through Health, with the lag it is stuck at", func() {
			f := newFixture()
			rec := newRecorder()
			rec.fail("w1", errors.New("boom"))
			Expect(effects.On(f.rt, "widgets", rec.handle)).To(Succeed())

			f.put(ctx, ws, &testdatav1.Widget{Id: "w1"})
			f.put(ctx, ws, &testdatav1.Widget{Id: "w2"})
			Expect(f.rt.Drain(ctx, "widgets", ws)).NotTo(Succeed())

			h, ok := f.rt.Health("widgets", ws)
			Expect(ok).To(BeTrue())
			Expect(h.Consumer).To(Equal("widgets"))
			Expect(h.LastError).To(MatchError(ContainSubstring("boom")))
			Expect(h.ConsecutiveFailures).To(Equal(1))
			Expect(h.NextAttempt).NotTo(BeZero(), "a failed pass schedules a retry")
			Expect(h.Position).To(BeZero(), "nothing got through")
			Expect(h.Lag()).To(BeEquivalentTo(2), "both writes are waiting behind the wedge")

			rec.recover("w1")
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())
			h, _ = f.rt.Health("widgets", ws)
			Expect(h.LastError).To(BeNil())
			Expect(h.ConsecutiveFailures).To(BeZero())
			Expect(h.Lag()).To(BeZero(), "caught up")
		})

		It("wedges one consumer without touching another", func() {
			f := newFixture()
			stuck, moving := newRecorder(), newRecorder()
			stuck.fail("w1", errors.New("boom"))
			Expect(effects.On(f.rt, "stuck", stuck.handle)).To(Succeed())
			Expect(effects.On(f.rt, "moving", moving.handle)).To(Succeed())

			f.put(ctx, ws, &testdatav1.Widget{Id: "w1"})
			f.put(ctx, ws, &testdatav1.Widget{Id: "w2"})

			Expect(f.rt.Drain(ctx, "stuck", ws)).NotTo(Succeed())
			Expect(f.rt.Drain(ctx, "moving", ws)).To(Succeed())
			Expect(moving.keys()).To(Equal([]string{"w1", "w2"}), "each consumer has its own position")
			Expect(stuck.keys()).To(BeEmpty())
		})
	})

	Describe("watched fields", func() {
		It("reports a first sighting as unknown rather than unchanged", func() {
			f := newFixture()
			rec := newRecorder()
			Expect(effects.On(f.rt, "widgets", rec.handle, effects.Watch("label"))).To(Succeed())

			f.put(ctx, ws, &testdatav1.Widget{Id: "w1", Label: "one"})
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())

			ev := rec.events()[0]
			Expect(ev.FirstSeen).To(BeTrue())
			Expect(ev.Before).To(BeNil())
			Expect(ev.Changed).To(BeEmpty(), "nothing was known to compare against")
			Expect(ev.DidChange("label")).To(BeFalse())
		})

		It("reports the transition once it has a shadow to compare against", func() {
			f := newFixture()
			rec := newRecorder()
			Expect(effects.On(f.rt, "widgets", rec.handle, effects.Watch("label"))).To(Succeed())

			f.put(ctx, ws, &testdatav1.Widget{Id: "w1", Label: "one", Count: 1})
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())
			f.put(ctx, ws, &testdatav1.Widget{Id: "w1", Label: "two", Count: 1})
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())

			ev := rec.events()[1]
			Expect(ev.FirstSeen).To(BeFalse())
			Expect(ev.Before.GetLabel()).To(Equal("one"))
			Expect(ev.After.GetLabel()).To(Equal("two"))
			Expect(ev.Changed).To(Equal([]string{"label"}))
			Expect(ev.DidChange("label")).To(BeTrue())
		})

		It("stays quiet when the watched field did not move", func() {
			f := newFixture()
			rec := newRecorder()
			Expect(effects.On(f.rt, "widgets", rec.handle, effects.Watch("label"))).To(Succeed())

			f.put(ctx, ws, &testdatav1.Widget{Id: "w1", Label: "one", Count: 1})
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())
			f.put(ctx, ws, &testdatav1.Widget{Id: "w1", Label: "one", Count: 2})
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())

			ev := rec.events()[1]
			Expect(ev.Changed).To(BeEmpty(), "count is not watched")
			Expect(ev.After.GetCount()).To(BeEquivalentTo(2), "after-state is always complete")
		})

		It("keeps only the watched fields in the shadow", func() {
			f := newFixture()
			rec := newRecorder()
			Expect(effects.On(f.rt, "widgets", rec.handle, effects.Watch("label"))).To(Succeed())

			f.put(ctx, ws, &testdatav1.Widget{Id: "w1", Label: "one", Count: 7})
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())
			f.put(ctx, ws, &testdatav1.Widget{Id: "w1", Label: "two", Count: 7})
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())

			ev := rec.events()[1]
			Expect(ev.Before.GetLabel()).To(Equal("one"))
			Expect(ev.Before.GetCount()).To(BeZero(), "an unwatched field is absent from the shadow, not remembered as zero")
		})

		It("watches several fields independently", func() {
			f := newFixture()
			rec := newRecorder()
			Expect(effects.On(f.rt, "widgets", rec.handle, effects.Watch("label", "count"))).To(Succeed())

			f.put(ctx, ws, &testdatav1.Widget{Id: "w1", Label: "one", Count: 1})
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())
			f.put(ctx, ws, &testdatav1.Widget{Id: "w1", Label: "one", Count: 2})
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())

			ev := rec.events()[1]
			Expect(ev.Changed).To(Equal([]string{"count"}))
			Expect(ev.Before.GetCount()).To(BeEquivalentTo(1))
		})

		It("drops the shadow with the row, so a re-created key is a first sighting", func() {
			f := newFixture()
			rec := newRecorder()
			Expect(effects.On(f.rt, "widgets", rec.handle, effects.Watch("label"))).To(Succeed())

			f.put(ctx, ws, &testdatav1.Widget{Id: "w1", Label: "one"})
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())
			f.delete(ctx, ws, "w1")
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())

			del := rec.events()[1]
			Expect(del.Deleted).To(BeTrue())
			Expect(del.Before.GetLabel()).To(Equal("one"), "the delete still reports what was lost")
			Expect(del.Changed).To(Equal([]string{"label"}))

			f.put(ctx, ws, &testdatav1.Widget{Id: "w1", Label: "again"})
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())
			Expect(rec.events()[2].FirstSeen).To(BeTrue(),
				"the key came back with no memory of the deleted row")
		})

		It("keeps no shadow at all when nothing is watched", func() {
			f := newFixture()
			rec := newRecorder()
			Expect(effects.On(f.rt, "widgets", rec.handle)).To(Succeed())

			f.put(ctx, ws, &testdatav1.Widget{Id: "w1", Label: "one"})
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())
			f.put(ctx, ws, &testdatav1.Widget{Id: "w1", Label: "two"})
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())

			ev := rec.events()[1]
			Expect(ev.FirstSeen).To(BeFalse(), "a consumer with no memory is never 'first seeing' anything")
			Expect(ev.Before).To(BeNil())
			Expect(ev.Changed).To(BeEmpty())

			stored, err := f.srv.ConsumerShadow(ctx, ws, "widgets", "widget", "w1")
			Expect(err).NotTo(HaveOccurred())
			Expect(stored).To(BeNil(), "no shadow rows were written")
		})

		It("does not advance the shadow past a change the handler rejected", func() {
			f := newFixture()
			rec := newRecorder()
			Expect(effects.On(f.rt, "widgets", rec.handle, effects.Watch("label"))).To(Succeed())

			f.put(ctx, ws, &testdatav1.Widget{Id: "w1", Label: "one"})
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())

			rec.fail("w1", errors.New("boom"))
			f.put(ctx, ws, &testdatav1.Widget{Id: "w1", Label: "two"})
			Expect(f.rt.Drain(ctx, "widgets", ws)).NotTo(Succeed())

			// The retry must still see the transition. A shadow that advanced
			// with the failed delivery would make this look unchanged, and the
			// trigger would be lost silently — the failure the shared
			// transaction exists to prevent.
			rec.recover("w1")
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())
			ev := rec.events()[1]
			Expect(ev.Before.GetLabel()).To(Equal("one"))
			Expect(ev.Changed).To(Equal([]string{"label"}))
		})
	})

	Describe("registration", func() {
		It("rejects a duplicate consumer name", func() {
			f := newFixture()
			rec := newRecorder()
			Expect(effects.On(f.rt, "widgets", rec.handle)).To(Succeed())
			Expect(effects.On(f.rt, "widgets", rec.handle)).To(MatchError(ContainSubstring("registered twice")))
		})

		It("rejects an empty name and a missing handler", func() {
			f := newFixture()
			rec := newRecorder()
			Expect(effects.On(f.rt, "", rec.handle)).To(MatchError(ContainSubstring("name is required")))
			Expect(effects.On[*testdatav1.Widget](f.rt, "widgets", nil)).To(MatchError(ContainSubstring("needs a handler")))
		})

		It("rejects an entity the server does not know", func() {
			f := newFixture()
			err := effects.On(f.rt, "unknown", func(context.Context, effects.Event[*testdatav1.OneofKeyEntity]) error {
				return nil
			})
			Expect(err).To(HaveOccurred())
		})

		It("rejects a watched field the entity does not have", func() {
			f := newFixture()
			rec := newRecorder()
			err := effects.On(f.rt, "widgets", rec.handle, effects.Watch("nope"))
			Expect(err).To(MatchError(ContainSubstring("nope")))
		})

		It("rejects registration after Start", func() {
			f := newFixture()
			rec := newRecorder()
			Expect(effects.On(f.rt, "widgets", rec.handle)).To(Succeed())
			Expect(f.rt.Start(ctx)).To(Succeed())
			Expect(effects.On(f.rt, "late", rec.handle)).To(MatchError(ContainSubstring("after Start")))
		})

		It("refuses to start with nothing registered", func() {
			f := newFixture()
			Expect(f.rt.Start(ctx)).To(MatchError(ContainSubstring("no consumers")))
		})

		It("names an unknown consumer when driven by hand", func() {
			f := newFixture()
			Expect(f.rt.Drain(ctx, "absent", ws)).To(MatchError(ContainSubstring("no consumer named")))
		})
	})

	Describe("falling below the tombstone horizon", func() {
		// Negative retention puts the cutoff in the future, so every tombstone
		// written is immediately collectable — which is what drives the horizon
		// past a consumer's position.
		collectableGC := []server.Option{server.WithGC(-time.Second, 0)}

		It("resets and resumes at the head, reporting the gap", func() {
			f := newFixtureFull(collectableGC)
			rec := newRecorder()
			Expect(effects.On(f.rt, "widgets", rec.handle)).To(Succeed())

			f.put(ctx, ws, &testdatav1.Widget{Id: "w1"})
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())
			Expect(rec.keys()).To(Equal([]string{"w1"}))

			// A delete the consumer never saw, then collected: the change is
			// gone from the feed with nothing left to enumerate it.
			f.delete(ctx, ws, "w1")
			f.collectGarbage(ctx, ws)

			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())
			Expect(rec.keys()).To(Equal([]string{"w1"}),
				"the collected delete is permanently missed — there is no repair for an effect")

			// Resumed at the head: new changes flow again.
			f.put(ctx, ws, &testdatav1.Widget{Id: "w2"})
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())
			Expect(rec.keys()).To(Equal([]string{"w1", "w2"}))
		})

		It("drops the shadows it can no longer trust", func() {
			f := newFixtureFull(collectableGC)
			rec := newRecorder()
			Expect(effects.On(f.rt, "widgets", rec.handle, effects.Watch("label"))).To(Succeed())

			f.put(ctx, ws, &testdatav1.Widget{Id: "w1", Label: "one"})
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())
			Expect(rec.events()[0].FirstSeen).To(BeTrue())

			// Push the horizon past the consumer's position with a collected
			// tombstone for an unrelated row, and let it notice.
			f.put(ctx, ws, &testdatav1.Widget{Id: "gone"})
			f.delete(ctx, ws, "gone")
			f.collectGarbage(ctx, ws)
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())

			// Only now does a change to w1 arrive. Its shadow went with the
			// reset, so the consumer is meeting the row for the first time.
			f.put(ctx, ws, &testdatav1.Widget{Id: "w1", Label: "two"})
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())

			last := rec.events()[len(rec.events())-1]
			Expect(last.Key).To(Equal("w1"))
			Expect(last.After.GetLabel()).To(Equal("two"))
			Expect(last.FirstSeen).To(BeTrue(),
				"the shadow described a past the feed can no longer line up against")
			Expect(last.Changed).To(BeEmpty())
		})

		It("starts a new consumer against an existing workspace without replaying it", func() {
			f := newFixtureFull(collectableGC)
			// History the consumer was never around for, with a collected
			// tombstone so the horizon is above zero.
			f.put(ctx, ws, &testdatav1.Widget{Id: "old"})
			f.delete(ctx, ws, "old")
			f.collectGarbage(ctx, ws)

			rec := newRecorder()
			Expect(effects.On(f.rt, "widgets", rec.handle)).To(Succeed())
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())
			Expect(rec.keys()).To(BeEmpty(), "a first run resyncs rather than replaying history")

			f.put(ctx, ws, &testdatav1.Widget{Id: "new"})
			Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())
			Expect(rec.keys()).To(Equal([]string{"new"}))
		})
	})

	Describe("the sink seam", func() {
		It("delivers every entity to a sink registered without one", func() {
			f := newFixture()
			var got []effects.Change
			sink := effects.SinkFunc(func(_ context.Context, ch effects.Change) error {
				got = append(got, ch)
				return nil
			})
			Expect(f.rt.Register("everything", sink)).To(Succeed())

			f.put(ctx, ws, &testdatav1.Widget{Id: "w1"})
			f.putPair(ctx, ws, &testdatav1.Pair{Ns: "n", Id: "p1"})
			Expect(f.rt.Drain(ctx, "everything", ws)).To(Succeed())

			Expect(got).To(HaveLen(2), "an unfiltered consumer sees both entities")
			Expect(got[0].Entity).To(Equal("widget"))
			Expect(got[1].Entity).To(Equal("pair"))
		})

		It("bounds a sink to the entities it names", func() {
			f := newFixture()
			var got []effects.Change
			sink := effects.SinkFunc(func(_ context.Context, ch effects.Change) error {
				got = append(got, ch)
				return nil
			})
			Expect(f.rt.Register("pairs-only", sink, &testdatav1.Pair{})).To(Succeed())

			f.put(ctx, ws, &testdatav1.Widget{Id: "w1"})
			f.putPair(ctx, ws, &testdatav1.Pair{Ns: "n", Id: "p1"})
			Expect(f.rt.Drain(ctx, "pairs-only", ws)).To(Succeed())

			Expect(got).To(HaveLen(1))
			Expect(got[0].Entity).To(Equal("pair"))
		})

		It("rejects a sink registered without one", func() {
			f := newFixture()
			Expect(f.rt.Register("nil-sink", nil)).To(MatchError(ContainSubstring("needs a sink")))
		})
	})

	Describe("running in the background", func() {
		It("delivers on the commit signal without waiting for a tick", func() {
			f := newFixture(func(cfg *effects.Config) {
				// Long enough that a delivery inside the Eventually window can
				// only have come from the commit signal.
				cfg.Interval = time.Minute
			})
			rec := newRecorder()
			Expect(effects.On(f.rt, "widgets", rec.handle)).To(Succeed())

			// Start discovers workspaces that already exist; write first so the
			// runner is there to be signalled.
			f.put(ctx, ws, &testdatav1.Widget{Id: "w1"})
			Expect(f.rt.Start(ctx)).To(Succeed())
			Eventually(rec.keys).Should(Equal([]string{"w1"}))

			f.put(ctx, ws, &testdatav1.Widget{Id: "w2"})
			Eventually(rec.keys).Should(Equal([]string{"w1", "w2"}))
		})

		It("picks up a workspace it has never seen when kicked", func() {
			f := newFixture(func(cfg *effects.Config) { cfg.Interval = time.Minute })
			rec := newRecorder()
			Expect(effects.On(f.rt, "widgets", rec.handle)).To(Succeed())
			Expect(f.rt.Start(ctx)).To(Succeed())

			f.put(ctx, "later", &testdatav1.Widget{Id: "w9"})
			f.rt.Kick("later")
			Eventually(rec.keys).Should(Equal([]string{"w9"}))
		})

		It("reacts to a workspace created after Start without waiting for a poll", func() {
			// The poll is off entirely, so a delivery here can only have come
			// from the workspace announcing itself: nothing was subscribed to
			// "later" — nor even aware of it — when the write committed.
			f := newFixture(func(cfg *effects.Config) { cfg.Interval = -1 })
			rec := newRecorder()
			Expect(effects.On(f.rt, "widgets", rec.handle)).To(Succeed())
			Expect(f.rt.Start(ctx)).To(Succeed())

			f.put(ctx, "later", &testdatav1.Widget{Id: "w9"})
			Eventually(rec.keys).Should(Equal([]string{"w9"}))
		})

		It("keeps reacting to a workspace after its announcement", func() {
			// A workspace is announced once; from there the per-workspace
			// commit signal carries it, and a second write must not need a
			// second announcement to be delivered.
			f := newFixture(func(cfg *effects.Config) { cfg.Interval = -1 })
			rec := newRecorder()
			Expect(effects.On(f.rt, "widgets", rec.handle)).To(Succeed())
			Expect(f.rt.Start(ctx)).To(Succeed())

			f.put(ctx, "later", &testdatav1.Widget{Id: "w1"})
			Eventually(rec.keys).Should(Equal([]string{"w1"}))
			f.put(ctx, "later", &testdatav1.Widget{Id: "w2"})
			Eventually(rec.keys).Should(Equal([]string{"w1", "w2"}))
		})

		It("stops cleanly", func() {
			f := newFixture()
			rec := newRecorder()
			Expect(effects.On(f.rt, "widgets", rec.handle)).To(Succeed())
			Expect(f.rt.Start(ctx)).To(Succeed())
			Expect(f.rt.Close()).To(Succeed())
			Expect(f.rt.Close()).To(Succeed(), "Close is idempotent")
		})
	})

	// What a runtime holds while running has to track the work outstanding
	// rather than the store it runs against, or a consumer stops being
	// something you can point at a large deployment.
	Describe("scheduling and resident cost", func() {
		It("forgets a workspace once it has drained", func() {
			f := newFixture(func(cfg *effects.Config) { cfg.Interval = time.Minute })
			rec := newRecorder()
			Expect(effects.On(f.rt, "widgets", rec.handle)).To(Succeed())
			Expect(f.rt.Start(ctx)).To(Succeed())

			for _, id := range []string{"ws-a", "ws-b", "ws-c"} {
				f.put(ctx, id, &testdatav1.Widget{Id: "w-" + id})
			}
			Eventually(rec.keys).Should(ConsistOf("w-ws-a", "w-ws-b", "w-ws-c"))
			// The position and the shadows are durable, so a drained pair costs
			// nothing to rebuild and is worth nothing to keep. Holding one per
			// workspace ever written is how resident cost stops tracking the
			// work and starts tracking the store.
			Eventually(f.rt.Tracked).Should(BeZero())
		})

		It("keeps a failing pair, so a wedge stays visible", func() {
			f := newFixture(func(cfg *effects.Config) {
				cfg.Interval = time.Minute
				cfg.RetryMin = time.Minute // park it; the spec is about residency
			})
			rec := newRecorder()
			rec.fail("bad", errors.New("nope"))
			Expect(effects.On(f.rt, "widgets", rec.handle)).To(Succeed())
			Expect(f.rt.Start(ctx)).To(Succeed())

			f.put(ctx, ws, &testdatav1.Widget{Id: "bad"})
			Eventually(func() int { return f.rt.Tracked() }).Should(Equal(1))
			// Eviction is for pairs that finished, and a wedged consumer has
			// not: dropping it would take the only report of the wedge with it.
			Consistently(f.rt.Tracked, "200ms").Should(Equal(1))
			h, ok := f.rt.Health("widgets", ws)
			Expect(ok).To(BeTrue())
			Expect(h.LastError).To(MatchError(ContainSubstring("nope")))
		})

		It("bounds how many passes run at once", func() {
			const workspaces = 5
			f := newFixture(func(cfg *effects.Config) {
				cfg.Workers = 2
				cfg.Interval = time.Minute
			})
			b := newBlocker(workspaces)
			Expect(effects.On(f.rt, "widgets", b.handle)).To(Succeed())
			Expect(f.rt.Start(ctx)).To(Succeed())

			for i := 0; i < workspaces; i++ {
				f.put(ctx, fmt.Sprintf("ws-%d", i), &testdatav1.Widget{Id: "w"})
			}
			// The pool is the only bound on concurrency: a sweep can queue as
			// many pairs as there are workspaces with work, and nothing else
			// stops them all reaching storage and the sink at once.
			Eventually(b.inFlight).Should(Equal(2))
			Consistently(b.inFlight, "200ms").Should(Equal(2))

			b.releaseAll()
			Eventually(func() int { return len(b.entered) }, "2s").Should(Equal(workspaces))
			Expect(b.highWater()).To(Equal(2), "the pool bounds the peak, not just the steady state")
		})

		It("runs one workspace's passes serially however many triggers arrive", func() {
			f := newFixture(func(cfg *effects.Config) {
				cfg.Workers = 8
				cfg.Interval = time.Minute
			})
			b := newBlocker(16)
			Expect(effects.On(f.rt, "widgets", b.handle)).To(Succeed())
			Expect(f.rt.Start(ctx)).To(Succeed())

			f.put(ctx, ws, &testdatav1.Widget{Id: "w1"})
			Eventually(b.inFlight).Should(Equal(1))
			// Feed order per workspace is the contract; a second worker picking
			// the same workspace up while the first is mid-page would break it
			// however many workers are idle.
			for i := 2; i <= 5; i++ {
				f.put(ctx, ws, &testdatav1.Widget{Id: fmt.Sprintf("w%d", i)})
				f.rt.Kick(ws)
			}
			Consistently(b.inFlight, "200ms").Should(Equal(1))
			b.releaseAll()
			Eventually(func() int { return len(b.entered) }, "2s").Should(Equal(5))
		})

		It("keeps a hand-driven Drain out of a pass already running", func() {
			f := newFixture(func(cfg *effects.Config) {
				cfg.Workers = 2
				cfg.Interval = time.Minute
			})
			b := newBlocker(4)
			Expect(effects.On(f.rt, "widgets", b.handle)).To(Succeed())
			Expect(f.rt.Start(ctx)).To(Succeed())

			f.put(ctx, ws, &testdatav1.Widget{Id: "w1"})
			Eventually(b.inFlight).Should(Equal(1))

			done := make(chan error, 1)
			go func() {
				defer GinkgoRecover()
				done <- f.rt.Drain(ctx, "widgets", ws)
			}()
			// Drain is a manual pass, not an exemption from the ordering
			// contract: two passes over one workspace would deliver the same
			// change twice and race to record where they got to.
			Consistently(done, "200ms").ShouldNot(Receive())
			b.releaseAll()
			Eventually(done, "2s").Should(Receive(BeNil()))
		})

		It("finds work by sweeping when the notifier cannot report activity", func() {
			// A notifier without the Activity extension — a shared one that has
			// not grown it yet — leaves the sweep as the only way work is
			// found. It has to be enough on its own.
			f := newFixtureFull(
				[]server.Option{server.WithNotifier(newDeafNotifier())},
				func(cfg *effects.Config) { cfg.Interval = 20 * time.Millisecond },
			)
			rec := newRecorder()
			Expect(effects.On(f.rt, "widgets", rec.handle)).To(Succeed())
			Expect(f.rt.Start(ctx)).To(Succeed())

			f.put(ctx, ws, &testdatav1.Widget{Id: "w1"})
			Eventually(rec.keys, "2s").Should(Equal([]string{"w1"}))
			f.put(ctx, "later", &testdatav1.Widget{Id: "w2"})
			Eventually(rec.keys, "2s").Should(ConsistOf("w1", "w2"),
				"a workspace created afterwards is found the same way")
		})

		It("catches a consumer up on history written before it existed", func() {
			// Registering a consumer against a store that already holds
			// workspaces is the case the sweep cannot answer from activity:
			// nothing is being written, so nothing signals.
			f := newFixture(func(cfg *effects.Config) { cfg.Interval = time.Minute })
			for i := 0; i < 3; i++ {
				f.put(ctx, fmt.Sprintf("old-%d", i), &testdatav1.Widget{Id: "w"})
			}
			rec := newRecorder()
			Expect(effects.On(f.rt, "widgets", rec.handle)).To(Succeed())
			Expect(f.rt.Start(ctx)).To(Succeed())
			Eventually(rec.events).Should(HaveLen(3))
		})

		It("bounds the health it keeps for pairs that have finished", func() {
			const retention = 2
			f := newFixture(func(cfg *effects.Config) {
				cfg.Interval = time.Minute
				cfg.HealthRetention = retention
			})
			rec := newRecorder()
			Expect(effects.On(f.rt, "widgets", rec.handle)).To(Succeed())
			Expect(f.rt.Start(ctx)).To(Succeed())

			for i := 0; i < 5; i++ {
				f.put(ctx, fmt.Sprintf("ws-%d", i), &testdatav1.Widget{Id: "w"})
			}
			Eventually(rec.events).Should(HaveLen(5))
			Eventually(f.rt.Tracked).Should(BeZero())
			// Recent work stays reportable; the rest is forgotten rather than
			// accumulating an entry per workspace the process has ever drained.
			Eventually(f.rt.HealthAll).Should(HaveLen(retention))
		})
	})
})
