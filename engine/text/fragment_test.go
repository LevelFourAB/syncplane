package text_test

import (
	"math/rand"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/text"
)

// applyEqualsMerge asserts the fragment invariant: for S ⊇ C,
// ApplyFragment(S, EncodeFragment(L, C)) == Merge(S, Encode(L)) byte-for-byte.
func applyEqualsMerge(s, l, c *text.Doc) []byte {
	GinkgoHelper()
	frag, err := text.EncodeFragment(l, c)
	Expect(err).NotTo(HaveOccurred())
	viaFragment, err := text.ApplyFragmentBytes(s.Encode(), frag)
	Expect(err).NotTo(HaveOccurred())
	viaMerge, err := text.Merge(s.Encode(), l.Encode())
	Expect(err).NotTo(HaveOccurred())
	Expect(viaFragment).To(Equal(viaMerge), "fragment apply diverged from full merge")
	return viaFragment
}

var _ = Describe("Fragments", func() {
	It("identifies fragment bytes", func() {
		d := text.New("a")
		Expect(d.Insert(0, "hi")).To(Succeed())
		frag, err := text.EncodeFragment(d, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(text.IsFragment(frag)).To(BeTrue())
		Expect(text.IsFragment(d.Encode())).To(BeFalse())
		Expect(text.IsFragment(nil)).To(BeFalse())
	})

	It("round-trips through decode and re-encode", func() {
		d := text.New("a")
		Expect(d.Insert(0, "hello world")).To(Succeed())
		Expect(d.Delete(2, 3)).To(Succeed())
		_, err := d.Mark(0, 4, "bold", true, text.ExpandAfter)
		Expect(err).NotTo(HaveOccurred())
		frag, err := text.EncodeFragment(d, nil)
		Expect(err).NotTo(HaveOccurred())
		f, err := text.DecodeFragment(frag)
		Expect(err).NotTo(HaveOccurred())
		Expect(f.Encode()).To(Equal(frag))
		Expect(f.VersionVector()).To(Equal(d.VersionVector()))
	})

	It("a self-contained fragment applied to an empty doc equals the source", func() {
		d := text.New("a")
		Expect(d.Insert(0, "hello")).To(Succeed())
		frag, err := text.EncodeFragment(d, nil)
		Expect(err).NotTo(HaveOccurred())
		out, err := text.ApplyFragmentBytes(nil, frag)
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(Equal(d.Encode()))
	})

	It("insert-only fragments apply like a merge", func() {
		c := text.New("seed")
		Expect(c.Insert(0, "hello world")).To(Succeed())
		l := fork(c, "alice")
		Expect(l.Insert(5, ", cruel")).To(Succeed())
		s := fork(c, "bob")
		Expect(s.Insert(11, "!")).To(Succeed())
		applyEqualsMerge(s, l, c)
	})

	It("tombstone flips travel as deleted items and apply like a merge", func() {
		c := text.New("seed")
		Expect(c.Insert(0, "hello world")).To(Succeed())
		l := fork(c, "alice")
		Expect(l.Delete(0, 6)).To(Succeed())
		s := fork(c, "bob")
		Expect(s.Insert(11, "!")).To(Succeed())
		merged := applyEqualsMerge(s, l, c)
		got, err := text.PlainText(merged)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal("world!"))
	})

	It("mark-only fragments with anchors to absent items apply like a merge", func() {
		c := text.New("seed")
		Expect(c.Insert(0, "hello world")).To(Succeed())
		l := fork(c, "alice")
		_, err := l.Mark(0, 5, "bold", true, text.ExpandNone)
		Expect(err).NotTo(HaveOccurred())
		s := fork(c, "bob")
		applyEqualsMerge(s, l, c)
	})

	It("an unchanged doc yields an empty no-op fragment", func() {
		c := text.New("seed")
		Expect(c.Insert(0, "abc")).To(Succeed())
		l := fork(c, "alice")
		frag, err := text.EncodeFragment(l, c)
		Expect(err).NotTo(HaveOccurred())
		s := fork(c, "bob")
		Expect(s.Insert(3, "d")).To(Succeed())
		before, err := text.Merge(s.Encode(), s.Encode())
		Expect(err).NotTo(HaveOccurred())
		after, err := text.ApplyFragmentBytes(s.Encode(), frag)
		Expect(err).NotTo(HaveOccurred())
		// The no-op fragment still carries l's vv (which equals c's) — already
		// covered by s, so the result is s unchanged.
		Expect(after).To(Equal(before))
	})

	It("rejects a base that is not an ancestor", func() {
		a := text.New("alice")
		Expect(a.Insert(0, "one")).To(Succeed())
		b := text.New("bob")
		Expect(b.Insert(0, "two")).To(Succeed())
		_, err := text.EncodeFragment(a, b)
		Expect(err).To(MatchError("text: fragment: base is not an ancestor of doc"))
	})

	It("rejects a base whose tombstone the doc lacks", func() {
		base := text.New("seed")
		Expect(base.Insert(0, "abc")).To(Succeed())
		newer := fork(base, "alice")
		Expect(base.Delete(0, 1)).To(Succeed())
		_, err := text.EncodeFragment(newer, base)
		Expect(err).To(MatchError("text: fragment: base is not an ancestor of doc"))
	})

	It("Decode refuses fragment bytes and DecodeFragment refuses documents", func() {
		d := text.New("a")
		Expect(d.Insert(0, "x")).To(Succeed())
		frag, err := text.EncodeFragment(d, nil)
		Expect(err).NotTo(HaveOccurred())
		_, err = text.Decode(frag, "")
		Expect(err).To(MatchError("text: fragment where full document expected"))
		_, err = text.DecodeFragment(d.Encode())
		Expect(err).To(MatchError("text: full document where fragment expected"))
	})

	It("apply rejects a fragment whose parent cannot resolve, leaving the doc untouched", func() {
		c := text.New("seed")
		Expect(c.Insert(0, "abc")).To(Succeed())
		l := fork(c, "alice")
		Expect(l.Insert(3, "d")).To(Succeed())
		frag, err := text.EncodeFragment(l, c)
		Expect(err).NotTo(HaveOccurred())
		// A receiver that lacks the base: the new item's parent is unknown.
		s := text.New("bob")
		Expect(s.Insert(0, "z")).To(Succeed())
		before := s.Encode()
		f, err := text.DecodeFragment(frag)
		Expect(err).NotTo(HaveOccurred())
		Expect(s.ApplyFragment(f)).To(MatchError("text: item alice:4 references unknown parent seed:3"))
		Expect(s.Encode()).To(Equal(before), "rejected apply must not mutate the receiver")
	})

	It("apply rejects a fragment whose anchor cannot resolve", func() {
		c := text.New("seed")
		Expect(c.Insert(0, "abc")).To(Succeed())
		l := fork(c, "alice")
		_, err := l.Mark(0, 3, "bold", true, text.ExpandNone)
		Expect(err).NotTo(HaveOccurred())
		frag, err := text.EncodeFragment(l, c)
		Expect(err).NotTo(HaveOccurred())
		s := text.New("bob")
		f, err := text.DecodeFragment(frag)
		Expect(err).NotTo(HaveOccurred())
		Expect(s.ApplyFragment(f)).To(MatchError("text: anchor references unknown item seed:1"))
	})

	It("apply rejects conflicting same-id items", func() {
		a := text.New("shared")
		Expect(a.Insert(0, "x")).To(Succeed())
		b := text.New("shared")
		Expect(b.Insert(0, "y")).To(Succeed())
		frag, err := text.EncodeFragment(b, nil)
		Expect(err).NotTo(HaveOccurred())
		f, err := text.DecodeFragment(frag)
		Expect(err).NotTo(HaveOccurred())
		Expect(a.ApplyFragment(f)).To(MatchError(
			"text: merge conflict: item shared:1 has different content on each side"))
	})

	It("MergeFragments unions like sequential application", func() {
		c := text.New("seed")
		Expect(c.Insert(0, "hello")).To(Succeed())
		l1 := fork(c, "alice")
		Expect(l1.Insert(5, " there")).To(Succeed())
		l2 := fork(l1, "alice")
		Expect(l2.Delete(0, 1)).To(Succeed())
		_, err := l2.Mark(0, 3, "b", true, text.ExpandNone)
		Expect(err).NotTo(HaveOccurred())

		f1, err := text.EncodeFragment(l1, c)
		Expect(err).NotTo(HaveOccurred())
		f2, err := text.EncodeFragment(l2, l1)
		Expect(err).NotTo(HaveOccurred())
		folded, err := text.MergeFragments(f1, f2)
		Expect(err).NotTo(HaveOccurred())
		reversed, err := text.MergeFragments(f2, f1)
		Expect(err).NotTo(HaveOccurred())
		Expect(folded).To(Equal(reversed), "MergeFragments must be commutative")

		s := fork(c, "bob")
		Expect(s.Insert(0, "!")).To(Succeed())
		sequential, err := text.ApplyFragmentBytes(s.Encode(), f1)
		Expect(err).NotTo(HaveOccurred())
		sequential, err = text.ApplyFragmentBytes(sequential, f2)
		Expect(err).NotTo(HaveOccurred())
		atOnce, err := text.ApplyFragmentBytes(s.Encode(), folded)
		Expect(err).NotTo(HaveOccurred())
		Expect(atOnce).To(Equal(sequential))
	})
})

var _ = Describe("Fragment convergence property", func() {
	It("pairwise fragment sync equals full-state merge over random histories", func() {
		const (
			replicas = 4
			rounds   = 30
			seeds    = 10
		)
		alphabet := []rune("abcdefghijklmnopqrstuvwxyzåäö🎉")
		for seed := int64(100); seed < 100+seeds; seed++ {
			rng := rand.New(rand.NewSource(seed))
			docs := make([]*text.Doc, replicas)
			// snapshots[i][j] is what i last knew of the pair (i,j)'s common
			// state — the base fragments are cut against.
			snapshots := make([][][]byte, replicas)
			for i := range docs {
				docs[i] = text.New(string(rune('A' + i)))
				snapshots[i] = make([][]byte, replicas)
			}
			for r := 0; r < rounds; r++ {
				for _, d := range docs {
					switch op := rng.Intn(10); {
					case op < 5: // insert
						var sb strings.Builder
						for n := 1 + rng.Intn(4); n > 0; n-- {
							sb.WriteRune(alphabet[rng.Intn(len(alphabet))])
						}
						Expect(d.Insert(rng.Intn(d.Len()+1), sb.String())).To(Succeed())
					case op < 7: // delete
						if l := d.Len(); l > 0 {
							idx := rng.Intn(l)
							Expect(d.Delete(idx, 1+rng.Intn(min(3, l-idx)))).To(Succeed())
						}
					case op < 8: // mark, and one time in three remove instead
						if l := d.Len(); l > 1 {
							s := rng.Intn(l - 1)
							e := s + 1 + rng.Intn(l-s-1)
							key := "k" + string(rune('0'+rng.Intn(3)))
							expand := text.Expand(rng.Intn(4))
							var err error
							if rng.Intn(3) == 0 {
								_, err = d.Unmark(s, e, key, expand)
							} else {
								_, err = d.Mark(s, e, key, rng.Intn(2) == 0, expand)
							}
							Expect(err).NotTo(HaveOccurred())
						}
					default: // sync a random pair via fragments, byte-compared to Merge
						i, j := rng.Intn(replicas), rng.Intn(replicas)
						if i == j {
							continue
						}
						base, err := text.Decode(snapshots[j][i], "")
						Expect(err).NotTo(HaveOccurred())
						frag, err := text.EncodeFragment(docs[j], base)
						Expect(err).NotTo(HaveOccurred())
						viaFragment, err := text.ApplyFragmentBytes(docs[i].Encode(), frag)
						Expect(err).NotTo(HaveOccurred())
						viaMerge, err := text.Merge(docs[i].Encode(), docs[j].Encode())
						Expect(err).NotTo(HaveOccurred())
						Expect(viaFragment).To(Equal(viaMerge),
							"seed %d round %d: fragment sync %d->%d diverged from merge", seed, r, j, i)
						// Fragment round-trip is stable.
						f, err := text.DecodeFragment(frag)
						Expect(err).NotTo(HaveOccurred())
						Expect(f.Encode()).To(Equal(frag))
						nd, err := text.Decode(viaMerge, docs[i].Peer())
						Expect(err).NotTo(HaveOccurred())
						docs[i] = nd
						// Both sides now share at least j's state.
						snapshots[i][j] = docs[j].Encode()
						snapshots[j][i] = docs[j].Encode()
					}
				}
			}
		}
	})
})
