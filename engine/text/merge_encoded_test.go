package text_test

import (
	"math/rand"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/text"
)

// MergeEncoded exists so a caller holding a live document can merge into it
// without paying the decode that Merge does — engine/server keeps recently
// written documents decoded and takes exactly this path. That only stays
// correct while the two agree, so this differentiates them over random states:
// whatever Merge produces from bytes, decode-then-MergeEncoded-then-encode must
// produce byte-for-byte, including which error it reports.
var _ = Describe("MergeEncoded equivalence with Merge", func() {
	It("matches Merge over random full-state and fragment writes", func() {
		alphabet := []rune("abcdefghijklmnopqrstuvwxyzåäö🎉")
		fragments, fullStates := 0, 0

		for seed := int64(0); seed < 40; seed++ {
			rng := rand.New(rand.NewSource(seed))
			base := text.New("server")
			Expect(base.Insert(0, "shared starting text")).To(Succeed())

			for round := 0; round < 15; round++ {
				stored := base.Encode()

				// A writer that forked from the stored state and edited it.
				writer, err := text.Decode(stored, string(rune('a'+rng.Intn(4))))
				Expect(err).NotTo(HaveOccurred())
				for edits := 1 + rng.Intn(3); edits > 0; edits-- {
					l := writer.Len()
					switch {
					case l > 0 && rng.Intn(3) == 0:
						start := rng.Intn(l)
						Expect(writer.Delete(start, 1+rng.Intn(min(4, l-start)))).To(Succeed())
					case rng.Intn(5) == 0 && l > 0:
						start := rng.Intn(l)
						_, err := writer.Mark(start, start+1+rng.Intn(l-start),
							"k"+string(rune('0'+rng.Intn(3))), true, text.ExpandNone)
						Expect(err).NotTo(HaveOccurred())
					default:
						var sb strings.Builder
						for n := 1 + rng.Intn(5); n > 0; n-- {
							sb.WriteRune(alphabet[rng.Intn(len(alphabet))])
						}
						Expect(writer.Insert(rng.Intn(l+1), sb.String())).To(Succeed())
					}
				}

				// Half the writes travel as fragments, which is the shape the
				// server actually sees from a client that knows the base.
				incoming := writer.Encode()
				if rng.Intn(2) == 0 {
					incoming, err = text.EncodeFragment(writer, base)
					Expect(err).NotTo(HaveOccurred())
					fragments++
				} else {
					fullStates++
				}

				want, wantErr := text.Merge(stored, incoming)

				live, err := text.Decode(stored, "")
				Expect(err).NotTo(HaveOccurred())
				gotErr := live.MergeEncoded(incoming, nil)

				if wantErr != nil {
					Expect(gotErr).To(MatchError(wantErr.Error()),
						"seed %d round %d", seed, round)
					continue
				}
				Expect(gotErr).NotTo(HaveOccurred(), "seed %d round %d", seed, round)
				Expect(live.Encode()).To(Equal(want), "seed %d round %d", seed, round)

				base, err = text.Decode(want, "server")
				Expect(err).NotTo(HaveOccurred())
			}
		}
		// Both wire shapes were actually exercised.
		Expect(fragments).To(BeNumerically(">", 0))
		Expect(fullStates).To(BeNumerically(">", 0))
	})

	It("matches MergeWithFloor on a compacted field", func() {
		for seed := int64(0); seed < 20; seed++ {
			rng := rand.New(rand.NewSource(seed))
			base := text.New("server")
			Expect(base.Insert(0, "hello there world")).To(Succeed())

			// A replica that still holds what the compaction is about to purge.
			stale, err := text.Decode(base.Encode(), "stale")
			Expect(err).NotTo(HaveOccurred())
			Expect(stale.Insert(rng.Intn(stale.Len()+1), "x")).To(Succeed())

			Expect(base.Delete(5, 6)).To(Succeed())
			base.Compact()
			floor := base.VersionVector()
			stored := base.Encode()
			incoming := stale.Encode()

			want, wantErr := text.MergeWithFloor(stored, incoming, floor)

			live, err := text.Decode(stored, "")
			Expect(err).NotTo(HaveOccurred())
			gotErr := live.MergeEncoded(incoming, floor)

			if wantErr != nil {
				Expect(gotErr).To(MatchError(wantErr.Error()), "seed %d", seed)
				continue
			}
			Expect(gotErr).NotTo(HaveOccurred(), "seed %d", seed)
			Expect(live.Encode()).To(Equal(want), "seed %d", seed)
		}
	})
})
