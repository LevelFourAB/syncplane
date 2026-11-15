package server_test

import (
	"context"
	"errors"
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

var _ = Describe("Consumer shadows", func() {
	const ws = "ws-shadows"

	var (
		ctx      context.Context
		srv      *server.Server
		captured []server.CapturedWrite
	)

	BeforeEach(func() {
		ctx = server.Trusted(context.Background())
		captured = nil
		srv = newTestServer(server.WithWriteCapture(func(_ context.Context, w *server.CapturedWrite) error {
			c := *w
			c.Ops = append([]server.CapturedOp(nil), w.Ops...)
			c.Tx = nil
			captured = append(captured, c)
			return nil
		}))
		DeferCleanup(func() { Expect(srv.Close()).To(Succeed()) })
	})

	type shadow struct{ key, snapshot string }

	// scanAll reads every shadow through Server.ScanConsumerShadows.
	scanAll := func(consumer, entity string) []shadow {
		GinkgoHelper()
		var out []shadow
		Expect(srv.ScanConsumerShadows(ctx, ws, consumer, entity, func(key string, snapshot []byte) error {
			out = append(out, shadow{key, string(snapshot)})
			return nil
		})).To(Succeed())
		return out
	}

	putShadows := func(consumer, entity string, keys ...string) {
		GinkgoHelper()
		Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			for _, k := range keys {
				if err := tx.PutConsumerShadow(consumer, entity, k, []byte("s-"+k)); err != nil {
					return err
				}
			}
			return nil
		})).To(Succeed())
	}

	widgetExists := func(key string) bool {
		GinkgoHelper()
		var found bool
		Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			var err error
			found, err = tx.Get(&testdatav1.Widget{}, key)
			return err
		})).To(Succeed())
		return found
	}

	Describe("Server.ScanConsumerShadows", func() {
		It("yields one consumer's shadows of one entity in key order", func() {
			putShadows("indexer", "rec:widget", "c", "a", "b")
			putShadows("indexer", "idx:widget", "z")
			putShadows("webhook", "rec:widget", "q")
			Expect(scanAll("indexer", "rec:widget")).To(Equal([]shadow{{"a", "s-a"}, {"b", "s-b"}, {"c", "s-c"}}))
			Expect(scanAll("indexer", "idx:widget")).To(Equal([]shadow{{"z", "s-z"}}))
			Expect(scanAll("absent", "rec:widget")).To(BeEmpty())
		})

		It("reads past the store's internal page size", func() {
			keys := make([]string, 700)
			for i := range keys {
				keys[i] = fmt.Sprintf("k%04d", i)
			}
			putShadows("indexer", "rec:widget", keys...)
			got := scanAll("indexer", "rec:widget")
			Expect(got).To(HaveLen(len(keys)))
			for i, sh := range got {
				Expect(sh.key).To(Equal(keys[i]))
			}
		})

		It("stops at the first error fn returns and returns it", func() {
			putShadows("indexer", "rec:widget", "a", "b", "c")
			stop := errors.New("stop")
			var keys []string
			err := srv.ScanConsumerShadows(ctx, ws, "indexer", "rec:widget", func(key string, _ []byte) error {
				keys = append(keys, key)
				if key == "b" {
					return stop
				}
				return nil
			})
			Expect(err).To(MatchError(stop))
			Expect(keys).To(Equal([]string{"a", "b"}))
		})

		It("refuses a reserved or empty consumer name, an empty workspace and a nil fn", func() {
			noop := func(string, []byte) error { return nil }
			Expect(srv.ScanConsumerShadows(ctx, ws, server.ReservedConsumerPrefix+"x", "e", noop)).
				To(MatchError(ContainSubstring("reserves")))
			Expect(srv.ScanConsumerShadows(ctx, ws, "", "e", noop)).To(MatchError(ContainSubstring("consumer name is required")))
			Expect(srv.ScanConsumerShadows(ctx, "", "c", "e", noop)).To(MatchError(ContainSubstring("workspace id is required")))
			Expect(srv.ScanConsumerShadows(ctx, ws, "c", "e", nil)).To(HaveOccurred())
		})
	})

	Describe("EntityTx", func() {
		It("reads, replaces and deletes a shadow inside the transaction", func() {
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				got, err := tx.ConsumerShadow("indexer", "rec:widget", "k")
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(BeNil(), "no shadow yet")
				Expect(tx.PutConsumerShadow("indexer", "rec:widget", "k", []byte("v1"))).To(Succeed())
				Expect(tx.PutConsumerShadow("indexer", "rec:widget", "k", []byte("v2"))).To(Succeed())
				got, err = tx.ConsumerShadow("indexer", "rec:widget", "k")
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(Equal([]byte("v2")), "the transaction reads its own write")
				Expect(tx.PutConsumerShadow("indexer", "rec:widget", "gone", []byte("x"))).To(Succeed())
				Expect(tx.DeleteConsumerShadow("indexer", "rec:widget", "gone")).To(Succeed())
				Expect(tx.DeleteConsumerShadow("indexer", "rec:widget", "never")).To(Succeed())
				return nil
			})).To(Succeed())
			Expect(scanAll("indexer", "rec:widget")).To(Equal([]shadow{{"k", "v2"}}))
			got, err := srv.ConsumerShadow(ctx, ws, "indexer", "rec:widget", "k")
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal([]byte("v2")))
		})

		It("pages a scan by afterKey and limit, and sees the transaction's own writes", func() {
			putShadows("indexer", "rec:widget", "k1", "k2", "k3", "k4", "k5")
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				Expect(tx.DeleteConsumerShadow("indexer", "rec:widget", "k2")).To(Succeed())
				Expect(tx.PutConsumerShadow("indexer", "rec:widget", "k6", []byte("s-k6"))).To(Succeed())

				var pages [][]string
				after := ""
				for {
					var page []string
					last, more, err := tx.ScanConsumerShadows("indexer", "rec:widget", after, 2, func(key string, _ []byte) error {
						page = append(page, key)
						return nil
					})
					Expect(err).NotTo(HaveOccurred())
					pages = append(pages, page)
					if !more {
						Expect(last).To(Equal("k6"))
						break
					}
					after = last
				}
				Expect(pages).To(Equal([][]string{{"k1", "k3"}, {"k4", "k5"}, {"k6"}}))

				_, _, err := tx.ScanConsumerShadows("indexer", "rec:widget", "", 0, func(string, []byte) error { return nil })
				Expect(err).To(MatchError(ContainSubstring("limit must be positive")))
				return nil
			})).To(Succeed())
		})

		It("lets the scan callback write shadows", func() {
			putShadows("indexer", "rec:widget", "a", "b", "c")
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				var seen []string
				_, more, err := tx.ScanConsumerShadows("indexer", "rec:widget", "", 10, func(key string, _ []byte) error {
					seen = append(seen, key)
					if err := tx.PutConsumerShadow("indexer", "rec:widget", key+"x", []byte("new")); err != nil {
						return err
					}
					return tx.DeleteConsumerShadow("indexer", "rec:widget", key)
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(more).To(BeFalse())
				Expect(seen).To(Equal([]string{"a", "b", "c"}), "the page was read before fn ran")
				return nil
			})).To(Succeed())
			Expect(scanAll("indexer", "rec:widget")).To(Equal([]shadow{{"ax", "new"}, {"bx", "new"}, {"cx", "new"}}))
		})

		It("commits shadows with the rows of the same Mutate", func() {
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				if err := tx.Put(&testdatav1.Widget{Id: "w1", Label: "a"}); err != nil {
					return err
				}
				return tx.PutConsumerShadow("indexer", "rec:widget", "w1", []byte("fp1"))
			})).To(Succeed())
			Expect(widgetExists("w1")).To(BeTrue())
			Expect(scanAll("indexer", "rec:widget")).To(Equal([]shadow{{"w1", "fp1"}}))
		})

		It("rolls shadows back with the rows when the mutator returns an error", func() {
			putShadows("indexer", "rec:widget", "kept")
			boom := errors.New("boom")
			err := srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				if err := tx.Put(&testdatav1.Widget{Id: "w1", Label: "a"}); err != nil {
					return err
				}
				if err := tx.PutConsumerShadow("indexer", "rec:widget", "w1", []byte("fp1")); err != nil {
					return err
				}
				if err := tx.DeleteConsumerShadow("indexer", "rec:widget", "kept"); err != nil {
					return err
				}
				return boom
			})
			Expect(err).To(MatchError(boom))
			Expect(widgetExists("w1")).To(BeFalse())
			Expect(scanAll("indexer", "rec:widget")).To(Equal([]shadow{{"kept", "s-kept"}}))
		})

		It("takes no sequence, adds no change and is not captured when it writes only shadows", func() {
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Widget{Id: "w1", Label: "a"})
			})).To(Succeed())
			before, err := srv.CurrentSeq(ctx, ws)
			Expect(err).NotTo(HaveOccurred())
			Expect(before).NotTo(BeZero())
			captured = nil

			var seq uint64
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				if err := tx.PutConsumerShadow("indexer", "rec:widget", "w1", []byte("fp1")); err != nil {
					return err
				}
				seq = tx.Seq()
				return nil
			})).To(Succeed())

			Expect(seq).To(BeZero(), "the transaction allocated no sequence")
			Expect(srv.CurrentSeq(ctx, ws)).To(Equal(before))
			var changes int
			_, _, err = srv.Changes(ctx, ws, server.Cursor{Seq: before}, 0, func(server.Change) error {
				changes++
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(changes).To(BeZero(), "the change feed holds nothing new")
			Expect(captured).To(BeEmpty(), "the write capture did not run")
		})

		It("passes only the rows of a Mutate to the write capture, not its shadows", func() {
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				if err := tx.PutConsumerShadow("indexer", "rec:widget", "w1", []byte("fp1")); err != nil {
					return err
				}
				return tx.Put(&testdatav1.Widget{Id: "w1", Label: "a"})
			})).To(Succeed())
			Expect(captured).To(HaveLen(1))
			Expect(captured[0].Ops).To(HaveLen(1))
			Expect(captured[0].Ops[0].Entity).To(Equal("widget"))
			Expect(captured[0].Ops[0].Key).To(Equal("w1"))
		})

		It("refuses a reserved, empty or oversized consumer name", func() {
			long := strings.Repeat("c", server.MaxConsumerNameBytes+1)
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				for _, name := range []string{server.ReservedConsumerPrefix + "x", "", long} {
					_, err := tx.ConsumerShadow(name, "e", "k")
					Expect(err).To(HaveOccurred(), "ConsumerShadow(%q)", name)
					Expect(tx.PutConsumerShadow(name, "e", "k", []byte("v"))).NotTo(Succeed(), "PutConsumerShadow(%q)", name)
					Expect(tx.DeleteConsumerShadow(name, "e", "k")).NotTo(Succeed(), "DeleteConsumerShadow(%q)", name)
					_, _, err = tx.ScanConsumerShadows(name, "e", "", 1, func(string, []byte) error { return nil })
					Expect(err).To(HaveOccurred(), "ScanConsumerShadows(%q)", name)
				}
				return nil
			})).To(Succeed())
		})
	})
})
