package connect_test

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
	"github.com/onsi/gomega/types"
	"google.golang.org/protobuf/proto"

	engineconnect "github.com/levelfourab/syncplane/engine/connect"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// refusals collects overwrites, and keeps content for keep.
func refusals(log *overwriteLog, keep time.Duration) func(*engineconnect.Config) {
	return withRecords(func(r *engineconnect.Records) {
		r.Overwrite = log.record
		r.KeepReplaced = keep
	})
}

var _ = Describe("Record mode, refused writes", func() {
	const ws = "ws1"
	var ctx context.Context

	BeforeEach(func() { ctx = context.Background() })

	// revertName puts the project's name back as an upstream link does after
	// the origin refused the connector's write.
	revertName := func(f *recFixture, key, name string) {
		GinkgoHelper()
		Expect(f.srv.Mutate(server.TrustedAs(ctx, "upstream"), ws, func(tx *server.EntityTx) error {
			var p testdatav1.Project
			if _, err := tx.Get(&p, key); err != nil {
				return err
			}
			next := proto.Clone(&p).(*testdatav1.Project)
			next.Name = name
			return tx.Put(next)
		})).To(Succeed())
	}

	// ingestEdit imports "T\none" and then ingests a store edit of the title.
	ingestEdit := func(f *recFixture) string {
		GinkgoHelper()
		f.store.put(ws, "a.txt", "T\none")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		key := f.key(ctx, ws, "a.txt")
		f.store.put(ws, "a.txt", "Refused\none")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.lines(ctx, ws, key)).To(Equal("Refused\none"))
		return key
	}

	refusedOf := func(key string) types.GomegaMatcher {
		return MatchAllFields(Fields{
			"WorkspaceID": Equal(ws), "Entity": Equal("project"), "Key": Equal(key), "Locator": Equal("a.txt"),
			"Reason": Equal(engineconnect.OverwriteRefused),
			"Old":    Equal([]byte("Refused\none")), "New": Equal([]byte("T\none")),
		})
	}

	It("gives the store content to Overwrite before egress writes the reverted rows over it", func() {
		var log overwriteLog
		f := newRecFixture(refusals(&log, 0))
		key := ingestEdit(f)

		revertName(f, key, "T")
		Expect(f.conn.Refused(ctx, ws, []engineconnect.RowRef{{Entity: "project", Key: key}})).To(Succeed())
		Expect(f.conn.Flush(ctx, ws)).To(Succeed())

		Expect(f.content(ws, "a.txt")).To(Equal("T\none"))
		Expect(log.all()).To(ConsistOf(refusedOf(key)))

		f.addRecord(ctx, ws, "p2", "Other")
		f.editLine(ctx, ws, f.widgetWith(ctx, ws, key, "one"), "uno")
		Expect(f.conn.Flush(ctx, ws)).To(Succeed())
		Expect(f.content(ws, "a.txt")).To(Equal("T\nuno"))
		Expect(log.all()).To(HaveLen(1), "the mark is cleared after use")
	})

	It("finds the record of a refused member row", func() {
		var log overwriteLog
		f := newRecFixture(refusals(&log, 0))
		f.store.put(ws, "a.txt", "T\none")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		key := f.key(ctx, ws, "a.txt")
		f.store.put(ws, "a.txt", "T\none\nadded")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		added := f.widgetWith(ctx, ws, key, "added")

		Expect(f.srv.Mutate(server.TrustedAs(ctx, "upstream"), ws, func(tx *server.EntityTx) error {
			return tx.Delete(&testdatav1.Widget{}, added)
		})).To(Succeed())
		Expect(f.conn.Refused(ctx, ws, []engineconnect.RowRef{{Entity: "widget", Key: added}, {Entity: "todo", Key: "x"}})).To(Succeed())
		Expect(f.conn.Flush(ctx, ws)).To(Succeed())

		Expect(f.content(ws, "a.txt")).To(Equal("T\none"))
		Expect(log.all()).To(ConsistOf(MatchFields(IgnoreExtras, Fields{
			"Reason": Equal(engineconnect.OverwriteRefused), "Old": Equal([]byte("T\none\nadded")),
		})))
	})

	It("keeps the mark through a restart", func() {
		var log overwriteLog
		f := newRecFixture(refusals(&log, 0))
		key := ingestEdit(f)
		revertName(f, key, "T")
		Expect(f.conn.Refused(ctx, ws, []engineconnect.RowRef{{Entity: "project", Key: key}})).To(Succeed())

		f.restart()
		Expect(f.conn.Flush(ctx, ws)).To(Succeed())
		Expect(log.all()).To(ConsistOf(refusedOf(key)))
	})

	It("gives the content egress replaced to Overwrite when the refusal comes after the egress", func() {
		var log overwriteLog
		f := newRecFixture(refusals(&log, time.Hour))
		key := ingestEdit(f)
		revertName(f, key, "T")
		Expect(f.conn.Flush(ctx, ws)).To(Succeed())
		Expect(f.content(ws, "a.txt")).To(Equal("T\none"))
		Expect(log.all()).To(BeEmpty())

		Expect(f.conn.Refused(ctx, ws, []engineconnect.RowRef{{Entity: "project", Key: key}})).To(Succeed())
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(log.all()).To(ConsistOf(refusedOf(key)))

		Expect(f.conn.Refused(ctx, ws, []engineconnect.RowRef{{Entity: "project", Key: key}})).To(Succeed())
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(log.all()).To(HaveLen(1), "the kept content is gone after use")
	})

	It("gives the content of a refused new record to Overwrite before egress deletes it", func() {
		for _, late := range []bool{false, true} {
			var log overwriteLog
			f := newRecFixture(refusals(&log, time.Hour))
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			f.store.put(ws, "new.txt", "New\nline")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			key := f.key(ctx, ws, "new.txt")
			rows := []engineconnect.RowRef{{Entity: "project", Key: key}, {Entity: "widget", Key: f.widgetWith(ctx, ws, key, "line")}}

			Expect(f.srv.Mutate(server.TrustedAs(ctx, "upstream"), ws, func(tx *server.EntityTx) error {
				for _, r := range rows[1:] {
					if err := tx.Delete(&testdatav1.Widget{}, r.Key); err != nil {
						return err
					}
				}
				return tx.Delete(&testdatav1.Project{}, key)
			})).To(Succeed())
			if late {
				Expect(f.conn.Flush(ctx, ws)).To(Succeed())
				Expect(f.store.locators(ws)).To(BeEmpty())
			}
			Expect(f.conn.Refused(ctx, ws, rows)).To(Succeed())
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())

			Expect(f.store.locators(ws)).To(BeEmpty(), "late=%v", late)
			Expect(log.all()).To(ConsistOf(MatchFields(IgnoreExtras, Fields{
				"Key": Equal(key), "Locator": Equal("new.txt"), "Reason": Equal(engineconnect.OverwriteRefused),
				"Old": Equal([]byte("New\nline")), "New": BeEmpty(),
			})), "late=%v", late)
		}
	})

	It("gives nothing to Overwrite for a refusal after the egress when it keeps no content", func() {
		var log overwriteLog
		f := newRecFixture(refusals(&log, 0))
		key := ingestEdit(f)
		revertName(f, key, "T")
		Expect(f.conn.Flush(ctx, ws)).To(Succeed())
		Expect(f.conn.Refused(ctx, ws, []engineconnect.RowRef{{Entity: "project", Key: key}})).To(Succeed())
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(log.all()).To(BeEmpty())

		f.editLine(ctx, ws, f.widgetWith(ctx, ws, key, "one"), "uno")
		Expect(f.conn.Flush(ctx, ws)).To(Succeed())
		Expect(log.all()).To(BeEmpty(), "the mark was dropped")
	})

	It("drops content it kept once KeepReplaced is over", func() {
		var log overwriteLog
		f := newRecFixture(refusals(&log, time.Minute))
		key := ingestEdit(f)
		revertName(f, key, "T")
		Expect(f.conn.Flush(ctx, ws)).To(Succeed())
		f.clock.advance(2 * time.Minute)
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())

		Expect(f.conn.Refused(ctx, ws, []engineconnect.RowRef{{Entity: "project", Key: key}})).To(Succeed())
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(log.all()).To(BeEmpty())
	})

	Describe("a refusal that comes while the connector uses an older one", func() {
		refusedMark := func(f *recFixture, key string) []byte {
			GinkgoHelper()
			raw, err := f.srv.ConsumerShadow(ctx, ws, "connect:"+recWriter, "refused:project", key)
			Expect(err).NotTo(HaveOccurred())
			return raw
		}

		It("is kept when it comes during an egress write", func() {
			var log overwriteLog
			f := newRecFixture(refusals(&log, time.Hour))
			key := ingestEdit(f)
			revertName(f, key, "T")
			Expect(f.conn.Refused(ctx, ws, []engineconnect.RowRef{{Entity: "project", Key: key}})).To(Succeed())

			once := true
			f.store.setBeforeWrite(func(context.Context, engineconnect.Record, string) error {
				if once {
					once = false
					// The host reverts another write of the record and reports it.
					Expect(f.srv.Mutate(server.TrustedAs(ctx, "upstream"), ws, func(tx *server.EntityTx) error {
						return tx.Put(&testdatav1.Project{Id: "p2", Name: "Other"})
					})).To(Succeed())
					Expect(f.conn.Refused(ctx, ws, []engineconnect.RowRef{{Entity: "project", Key: key}})).To(Succeed())
				}
				return nil
			})
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())
			Expect(log.all()).To(ConsistOf(refusedOf(key)))
			Expect(refusedMark(f, key)).NotTo(BeNil(), "the second mark")

			Expect(f.conn.Drain(ctx, ws)).To(Succeed())
			Expect(log.all()).To(ConsistOf(refusedOf(key), refusedOf(key)), "the second mark gets the content egress replaced")
			Expect(refusedMark(f, key)).To(BeNil())
		})

		It("is kept when it comes during an egress delete", func() {
			var log overwriteLog
			f := newRecFixture(refusals(&log, time.Hour))
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			f.store.put(ws, "new.txt", "New\nline")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			key := f.key(ctx, ws, "new.txt")
			line := f.widgetWith(ctx, ws, key, "line")
			Expect(f.srv.Mutate(server.TrustedAs(ctx, "upstream"), ws, func(tx *server.EntityTx) error {
				if err := tx.Delete(&testdatav1.Widget{}, line); err != nil {
					return err
				}
				return tx.Delete(&testdatav1.Project{}, key)
			})).To(Succeed())
			Expect(f.conn.Refused(ctx, ws, []engineconnect.RowRef{{Entity: "project", Key: key}})).To(Succeed())

			once := true
			f.store.beforeDelete = func(context.Context, string) {
				if once {
					once = false
					Expect(f.srv.Mutate(server.TrustedAs(ctx, "upstream"), ws, func(tx *server.EntityTx) error {
						return tx.Put(&testdatav1.Project{Id: "p2", Name: "Other"})
					})).To(Succeed())
					Expect(f.conn.Refused(ctx, ws, []engineconnect.RowRef{{Entity: "project", Key: key}})).To(Succeed())
				}
			}
			deleted := MatchFields(IgnoreExtras, Fields{
				"Key": Equal(key), "Reason": Equal(engineconnect.OverwriteRefused),
				"Old": Equal([]byte("New\nline")), "New": BeEmpty(),
			})
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())
			Expect(f.store.locators(ws)).NotTo(ContainElement("new.txt"))
			Expect(log.all()).To(ConsistOf(deleted))
			Expect(refusedMark(f, key)).NotTo(BeNil(), "the second mark")

			Expect(f.conn.Drain(ctx, ws)).To(Succeed())
			Expect(log.all()).To(ConsistOf(deleted, deleted))
			Expect(refusedMark(f, key)).To(BeNil())
		})

		It("is kept when it comes while a pass settles the older one", func() {
			var log overwriteLog
			var f *recFixture
			var key string
			fired := false
			hook := func(c context.Context, o engineconnect.Overwrite) error {
				if !fired && key != "" {
					fired = true
					// The origin refuses the next ingest too, and the host
					// reverts it and reports it.
					w := f.widgetWith(ctx, ws, key, "X")
					Expect(f.srv.Mutate(server.TrustedAs(ctx, "upstream"), ws, func(tx *server.EntityTx) error {
						var cur testdatav1.Widget
						if _, err := tx.Get(&cur, w); err != nil {
							return err
						}
						next := proto.Clone(&cur).(*testdatav1.Widget)
						if err := server.ApplyTextStrings(next, nil, &cur, map[string]string{"body": "one"}, "upstream"); err != nil {
							return err
						}
						return tx.Put(next)
					})).To(Succeed())
					Expect(f.conn.Refused(ctx, ws, []engineconnect.RowRef{{Entity: "widget", Key: w}})).To(Succeed())
				}
				return log.record(c, o)
			}
			f = newRecFixture(withRecords(func(r *engineconnect.Records) {
				r.Overwrite = hook
				r.KeepReplaced = time.Hour
			}))
			k := ingestEdit(f)
			revertName(f, k, "T")
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())
			Expect(f.conn.Refused(ctx, ws, []engineconnect.RowRef{{Entity: "project", Key: k}})).To(Succeed())

			// A second store edit, ingested before the pass that settles the
			// first refusal.
			f.store.put(ws, "a.txt", "T\nX")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			key = k
			Expect(f.conn.Drain(ctx, ws)).To(Succeed())
			Expect(fired).To(BeTrue())
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())

			Expect(f.content(ws, "a.txt")).To(Equal("T\none"))
			Expect(log.all()).To(ConsistOf(
				MatchFields(IgnoreExtras, Fields{
					"Reason": Equal(engineconnect.OverwriteRefused), "Old": Equal([]byte("Refused\none")),
				}),
				MatchFields(IgnoreExtras, Fields{
					"Reason": Equal(engineconnect.OverwriteRefused),
					"Old":    Equal([]byte("T\nX")), "New": Equal([]byte("T\none")),
				}),
			))
		})
	})

	It("refuses Refused in row mode", func() {
		f := newFixture()
		Expect(f.conn.Refused(ctx, ws, nil)).To(MatchError(ContainSubstring("record mode")))
	})
})
