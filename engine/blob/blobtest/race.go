package blobtest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/blob"
)

// raceRounds is how often a race spec repeats its race. Each round is a
// fresh chance for the interleaving that breaks a Store.
const raceRounds = 8

// raceSpecs registers the specs for calls that race on one name. Each spec
// checks that the end state is one that the calls run one after the other
// could also leave. pair returns the two handles the racing calls go
// through; they may be the same Store.
func raceSpecs(pair func() (blob.Store, blob.Store)) {
	Describe("racing calls", func() {
		h := helpers{ctx: context.Background()}
		ctx := context.Background()

		// race runs every fn at once and returns their errors in order.
		race := func(fns ...func() error) []error {
			errs := make([]error, len(fns))
			var start, done sync.WaitGroup
			start.Add(1)
			for i, fn := range fns {
				done.Add(1)
				go func() {
					defer GinkgoRecover()
					defer done.Done()
					start.Wait()
					errs[i] = fn()
				}()
			}
			start.Done()
			done.Wait()
			return errs
		}

		It("keeps one upload when Begin races Begin", func() {
			a, b := pair()
			two := fix("two")
			var fns []func() error
			for i := range 8 {
				s := []blob.Store{a, b}[i%2]
				fns = append(fns, func() error { return s.Begin(ctx, "ws", two.m) })
			}
			Expect(race(fns...)).To(HaveEach(Succeed()))
			Expect(h.listStaged(a, "ws")).To(ConsistOf(two.digest()))

			h.put(a, "ws", two, 1)
			u, _ := h.staged(a, "ws", two.digest())
			Expect(race(fns...)).To(HaveEach(Succeed()))
			again, have := h.staged(b, "ws", two.digest())
			Expect(have).To(Equal([]int{1}))
			Expect(again.StartedAt).To(BeTemporally("==", u.StartedAt))
		})

		It("commits once when PutChunk and Complete race", func() {
			a, b := pair()
			two := fix("two")
			h.begin(a, "ws", two)
			var fns []func() error
			for i := range 3 {
				s := []blob.Store{a, b}[i%2]
				fns = append(fns, func() error {
					for c := range two.m.ChunkHashes {
						err := s.PutChunk(ctx, "ws", two.digest(), c, bytes.NewReader(two.chunk(c)))
						// A racer that commits first leaves nothing staged.
						if err != nil && !errors.Is(err, blob.ErrNotFound) {
							return err
						}
					}
					return s.Complete(ctx, "ws", two.digest())
				})
			}
			Expect(race(fns...)).To(HaveEach(Succeed()))

			Expect(h.read(b, "ws", two.digest())).To(Equal(two.data))
			Expect(h.list(a, "ws")).To(ConsistOf(two.digest()))
			h.expectNotStaged(a, "ws", two.digest())
			h.expectNotStaged(b, "ws", two.digest())
		})

		It("commits or aborts when Complete races Abort", func() {
			a, b := pair()
			small := fix("small")
			for round := range raceRounds {
				h.begin(a, "ws", small)
				h.putAll(a, "ws", small)
				errs := race(
					func() error { return a.Complete(ctx, "ws", small.digest()) },
					func() error { return b.Abort(ctx, "ws", small.digest()) },
				)
				Expect(errs[1]).NotTo(HaveOccurred(), "round %d", round)
				h.expectNotStaged(a, "ws", small.digest())
				if errs[0] == nil {
					Expect(h.read(b, "ws", small.digest())).To(Equal(small.data), "round %d", round)
				} else {
					Expect(errs[0]).To(MatchError(blob.ErrNotFound), "round %d", round)
					h.expectGone(b, "ws", small.digest())
				}
				Expect(a.Delete(ctx, "ws", small.digest())).To(Succeed())
			}
		})

		It("leaves the whole blob or none when Complete races Delete", func() {
			a, b := pair()
			small := fix("small")
			for round := range raceRounds {
				h.begin(a, "ws", small)
				h.putAll(a, "ws", small)
				errs := race(
					func() error { return a.Complete(ctx, "ws", small.digest()) },
					func() error { return b.Delete(ctx, "ws", small.digest()) },
				)
				Expect(errs).To(HaveEach(Succeed()), "round %d", round)
				h.expectNotStaged(a, "ws", small.digest())
				if _, err := b.Stat(ctx, "ws", small.digest()); err == nil {
					Expect(h.read(a, "ws", small.digest())).To(Equal(small.data), "round %d", round)
					Expect(h.list(b, "ws")).To(ConsistOf(small.digest()))
				} else {
					Expect(err).To(MatchError(blob.ErrNotFound))
					h.expectGone(a, "ws", small.digest())
				}
				Expect(a.Delete(ctx, "ws", small.digest())).To(Succeed())
			}
		})

		It("commits and leaves nothing staged when Begin races Complete", func() {
			a, b := pair()
			small := fix("small")
			for round := range raceRounds {
				h.begin(a, "ws", small)
				h.putAll(a, "ws", small)
				errs := race(
					func() error { return a.Complete(ctx, "ws", small.digest()) },
					func() error { return b.Begin(ctx, "ws", small.m) },
				)
				Expect(errs).To(HaveEach(Succeed()), "round %d", round)
				Expect(h.read(b, "ws", small.digest())).To(Equal(small.data), "round %d", round)
				h.expectNotStaged(a, "ws", small.digest())
				Expect(a.Delete(ctx, "ws", small.digest())).To(Succeed())
			}
		})

		It("leaves no blob when SetUnreferencedSince races Delete", func() {
			a, b := pair()
			small := fix("small")
			for round := range raceRounds {
				h.commit(a, "ws", small)
				errs := race(
					func() error { return a.SetUnreferencedSince(ctx, "ws", small.digest(), newClock().Now()) },
					func() error { return b.Delete(ctx, "ws", small.digest()) },
				)
				if errs[0] != nil {
					Expect(errs[0]).To(MatchError(blob.ErrNotFound), "round %d", round)
				}
				Expect(errs[1]).NotTo(HaveOccurred(), "round %d", round)
				h.expectGone(a, "ws", small.digest())
				h.expectGone(b, "ws", small.digest())
			}
		})

		It("reads the whole blob or nothing when Open races Delete", func() {
			a, b := pair()
			two := fix("two")
			for round := range 3 {
				h.commit(a, "ws", two)
				var fns []func() error
				fns = append(fns, func() error { return b.Delete(ctx, "ws", two.digest()) })
				for range 3 {
					fns = append(fns, func() error {
						r, err := a.Open(ctx, "ws", two.digest())
						if err != nil {
							return err
						}
						defer r.Close()
						got, err := io.ReadAll(r)
						if err == nil && !bytes.Equal(got, two.data) {
							return errors.New("read bytes that are not the blob")
						}
						// A read error after the blob is deleted is allowed;
						// wrong bytes are not.
						return nil
					})
				}
				for i, err := range race(fns...) {
					if err != nil {
						Expect(err).To(MatchError(blob.ErrNotFound), "round %d, call %d", round, i)
					}
				}
				h.expectGone(a, "ws", two.digest())
			}
		})

		It("keeps a staged upload listed or removes it when Begin races DeleteWorkspace", func() {
			a, b := pair()
			small := fix("small")
			for round := range raceRounds {
				h.commit(a, "ws", fix("other"))
				errs := race(
					func() error { return a.Begin(ctx, "ws", small.m) },
					func() error { return b.DeleteWorkspace(ctx, "ws") },
				)
				Expect(errs).To(HaveEach(Succeed()), "round %d", round)
				h.expectGone(a, "ws", fix("other").digest())
				if u, _, err := b.Staged(ctx, "ws", small.digest()); err == nil {
					expectManifest(u.Manifest, small.m)
					Expect(h.workspaces(a)).To(ConsistOf("ws"), "round %d", round)
					Expect(h.listStaged(b, "ws")).To(ConsistOf(small.digest()))
				} else {
					Expect(err).To(MatchError(blob.ErrNotFound))
					Expect(slices.Contains(h.workspaces(a), "ws")).To(BeFalse(), "round %d", round)
				}
				Expect(a.DeleteWorkspace(ctx, "ws")).To(Succeed())
			}
		})
	})
}
