package server

import (
	"math/rand"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/text"
)

// The cache exists to skip a decode, and its whole safety argument is that an
// entry is only usable when the bytes it was built from match the bytes being
// merged into. These exercise that argument directly: the cached merge must be
// byte-for-byte what the uncached one produces, in every sequence — including
// the ones where the cache is wrong, stale, evicted, or was never populated
// because a previous write failed.

func cacheDoc(peer, s string) *text.Doc {
	d := text.New(peer)
	if err := d.Insert(0, s); err != nil {
		panic(err)
	}
	return d
}

var _ = Describe("textDocCache", func() {
	const key = "ws\x00note\x00n1\x00body"

	It("serves a document only for the bytes it was built from", func() {
		c := newTextDocCache(1 << 20)
		doc := cacheDoc("a", "hello")
		encoded := doc.Encode()
		c.put(key, encoded, doc)

		other := cacheDoc("a", "goodbye").Encode()
		Expect(c.take(key, other)).To(BeNil(), "a mismatch must not hand over a document")
		// That miss also dropped the entry, so the matching bytes miss too.
		Expect(c.take(key, encoded)).To(BeNil())

		c.put(key, encoded, doc)
		Expect(c.take(key, encoded)).To(BeIdenticalTo(doc))
		// take is destructive: the caller now owns it.
		Expect(c.take(key, encoded)).To(BeNil())
	})

	It("keeps identically-keyed rows of different workspaces apart", func() {
		c := newTextDocCache(1 << 20)
		a := cacheDoc("a", "workspace a text")
		b := cacheDoc("b", "workspace b text")
		ka := textCacheKey("ws-a", "note", "n1", "body")
		kb := textCacheKey("ws-b", "note", "n1", "body")
		Expect(ka).NotTo(Equal(kb))

		c.put(ka, a.Encode(), a)
		c.put(kb, b.Encode(), b)
		Expect(c.take(ka, a.Encode())).To(BeIdenticalTo(a))
		Expect(c.take(kb, b.Encode())).To(BeIdenticalTo(b))
	})

	It("evicts least-recently-used entries to stay inside the budget", func() {
		// Three documents, budget for about two of them.
		docs := make([]*text.Doc, 3)
		keys := make([]string, 3)
		for i := range docs {
			docs[i] = cacheDoc("p", strings.Repeat("x", 200))
			keys[i] = textCacheKey("ws", "note", string(rune('a'+i)), "body")
		}
		size := int64(len(docs[0].Encode()))
		c := newTextDocCache(size*2 + size/2)

		for i, d := range docs {
			c.put(keys[i], d.Encode(), d)
		}
		Expect(c.used).To(BeNumerically("<=", c.budget))
		// The first one in is the one out.
		Expect(c.take(keys[0], docs[0].Encode())).To(BeNil())
		Expect(c.take(keys[2], docs[2].Encode())).To(BeIdenticalTo(docs[2]))
	})

	It("declines a document larger than the whole budget rather than thrashing", func() {
		small := cacheDoc("p", "tiny")
		big := cacheDoc("p", strings.Repeat("y", 5000))
		c := newTextDocCache(int64(len(small.Encode())) + 8)
		c.put("small", small.Encode(), small)
		c.put("big", big.Encode(), big)

		Expect(c.take("big", big.Encode())).To(BeNil())
		Expect(c.take("small", small.Encode())).To(BeIdenticalTo(small),
			"an oversized put must not evict what already fits")
	})

	It("forgets every text field of a deleted row", func() {
		c := newTextDocCache(1 << 20)
		body := cacheDoc("p", "body text")
		title := cacheDoc("p", "title text")
		c.put(textCacheKey("ws", "note", "n1", "body"), body.Encode(), body)
		c.put(textCacheKey("ws", "note", "n1", "title"), title.Encode(), title)

		c.forget("ws", "note", "n1", []textField{{path: "body"}, {path: "title"}})
		Expect(c.used).To(BeZero())
		Expect(c.take(textCacheKey("ws", "note", "n1", "body"), body.Encode())).To(BeNil())
		Expect(c.take(textCacheKey("ws", "note", "n1", "title"), title.Encode())).To(BeNil())
	})

	It("produces exactly what the uncached merge would, over a random write sequence", func() {
		alphabet := []rune("abcdefghijklmnopqrstuvwxyzåäö")
		// Small budget and several rows, so entries are genuinely evicted and
		// re-decoded partway through rather than every write hitting.
		c := newTextDocCache(2048)
		const rows = 4
		stored := make([][]byte, rows)
		for i := range stored {
			stored[i] = cacheDoc("server", "starting text").Encode()
		}

		rng := rand.New(rand.NewSource(11))
		for round := 0; round < 400; round++ {
			r := rng.Intn(rows)
			base := stored[r]

			writer, err := text.Decode(base, string(rune('a'+rng.Intn(3))))
			Expect(err).NotTo(HaveOccurred())
			l := writer.Len()
			if l > 0 && rng.Intn(4) == 0 {
				start := rng.Intn(l)
				Expect(writer.Delete(start, 1+rng.Intn(min(3, l-start)))).To(Succeed())
			} else {
				var sb strings.Builder
				for n := 1 + rng.Intn(4); n > 0; n-- {
					sb.WriteRune(alphabet[rng.Intn(len(alphabet))])
				}
				Expect(writer.Insert(rng.Intn(l+1), sb.String())).To(Succeed())
			}

			incoming := writer.Encode()
			if rng.Intn(2) == 0 {
				baseDoc, err := text.Decode(base, "")
				Expect(err).NotTo(HaveOccurred())
				incoming, err = text.EncodeFragment(writer, baseDoc)
				Expect(err).NotTo(HaveOccurred())
			}

			want, wantErr := text.MergeWithFloor(base, incoming, nil)
			got, gotErr := c.merge(
				textCacheKey("ws", "note", string(rune('a'+r)), "body"), base, incoming, nil)

			if wantErr != nil {
				Expect(gotErr).To(MatchError(wantErr.Error()), "round %d", round)
				continue
			}
			Expect(gotErr).NotTo(HaveOccurred(), "round %d", round)
			Expect(got).To(Equal(want), "round %d row %d", round, r)

			// A quarter of the writes "roll back": the merged state is thrown
			// away and the row keeps its old bytes, which is what a rejected
			// mutation or a retried write closure leaves behind. The cache now
			// holds an entry for bytes that were never stored, and the next
			// write to this row must still be correct.
			if rng.Intn(4) != 0 {
				stored[r] = got
			}
		}
	})

	It("is bypassed entirely when nil", func() {
		var c *textDocCache
		base := cacheDoc("a", "hello").Encode()
		next := cacheDoc("b", "hello there")
		incoming := next.Encode()

		want, err := text.MergeWithFloor(base, incoming, nil)
		Expect(err).NotTo(HaveOccurred())
		got, err := c.merge("k", base, incoming, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(want))
	})
})
