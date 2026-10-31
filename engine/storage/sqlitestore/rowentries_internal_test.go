package sqlitestore

import (
	"encoding/binary"
	"math"
	"math/rand/v2"
	"sort"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/storage"
)

// entrySet makes a set of row entries from scope and entry pairs.
func entrySet(pairs ...any) map[rowIndexEntry]struct{} {
	set := map[rowIndexEntry]struct{}{}
	for i := 0; i < len(pairs); i += 2 {
		set[rowIndexEntry{scope: int64(pairs[i].(int)), k: pairs[i+1].(string)}] = struct{}{}
	}
	return set
}

// groupEnds returns the lengths at which the list of set ends a group: the
// lengths of the lists of the sets that hold the first one, two and more
// scopes of set.
func groupEnds(rowKey string, set map[rowIndexEntry]struct{}) map[int]bool {
	var scopes []int64
	seen := map[int64]bool{}
	for e := range set {
		if !seen[e.scope] {
			seen[e.scope] = true
			scopes = append(scopes, e.scope)
		}
	}
	sort.Slice(scopes, func(i, j int) bool { return scopes[i] < scopes[j] })
	ends := map[int]bool{0: true}
	for i := range scopes {
		sub := map[rowIndexEntry]struct{}{}
		for e := range set {
			if e.scope <= scopes[i] {
				sub[e] = struct{}{}
			}
		}
		ends[len(encodeRowEntries(rowKey, sub))] = true
	}
	return ends
}

var _ = Describe("row entry lists", func() {
	roundTrip := func(rowKey string, set map[rowIndexEntry]struct{}) []byte {
		GinkgoHelper()
		b := encodeRowEntries(rowKey, set)
		Expect(encodeRowEntries(rowKey, set)).To(Equal(b), "the same set packs to the same bytes")
		got, err := decodeRowEntries(rowKey, b)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(set))
		return b
	}

	long := strings.Repeat("x", storage.MaxIndexEntryBytes)

	DescribeTable("round-trips a set",
		func(rowKey string, set map[rowIndexEntry]struct{}) { roundTrip(rowKey, set) },
		Entry("of one entry", "k1", entrySet(1, "a\x00\x00")),
		Entry("of one empty entry", "k1", entrySet(1, "")),
		Entry("of entries that are only a tail", "k1",
			entrySet(1, "\x00\x00", 1, "k1\x00\x00", 1, "")),
		Entry("over many scopes", "k1",
			entrySet(1, "a", 2, "a", 3, "b", 127, "c", 128, "d", 1<<40, "e", math.MaxInt64, "f")),
		Entry("of entries that are prefixes of each other", "k1",
			entrySet(1, "a", 1, "ab", 1, "abc", 1, "abcd", 1, "b", 1, "ba")),
		Entry("of entries with and without the row key", "k1",
			entrySet(1, "a\x00k1\x00\x00", 1, "a\x00k2\x00\x00", 1, "a\x00\x00", 1, "a", 1, "k1\x00", 2, "zk1\x00\x00")),
		Entry("of an entry equal to the part the previous one keeps", "k1",
			entrySet(1, "abc", 1, "abc\x00\x00", 1, "abck1\x00\x00", 1, "abck1")),
		Entry("of search terms", "k1",
			entrySet(5, "\x01apple\x00\x00", 5, "\x01apply\x00\x00", 5, "\x01banana\x00\x00", 5, "\x01ban\x00\x00")),
		Entry("for an empty row key", "",
			entrySet(1, "\x00\x00", 1, "a\x00\x00", 1, "a\x00\x00\x00\x00", 1, "a")),
		Entry("for a row key that holds the terminator", "k\x00\x00",
			entrySet(1, "ak\x00\x00\x00\x00", 1, "a\x00\x00", 1, "k\x00\x00")),
		Entry("of entries of the greatest length", "k1",
			entrySet(1, long, 1, long[:len(long)-1]+"y", 2, long[:len(long)-4]+"k1\x00\x00",
				2, long[:len(long)-2]+"\x00\x00", 3, long)),
		Entry("of non-UTF-8 bytes", "k\xff",
			entrySet(1, "\xff\xfe\x00", 1, "\xffk\xff\x00\x00", 2, "\x00")),
	)

	It("leaves out the shared bytes and the tail of an entry", func() {
		b := roundTrip("k1", entrySet(7, "\x01apple\x00\x00", 7, "\x01apply\x00\x00", 7, "x\x00k1\x00\x00"))
		Expect(b).To(Equal([]byte{
			7, 6<<3 | 2<<1 | 1, 0x01, 'a', 'p', 'p', 'l', 'e',
			5, 1<<3 | 2<<1 | 1, 'y',
			0, 2<<3 | 1<<1, 'x', 0,
		}))
	})

	It("round-trips random sets", func() {
		rng := rand.New(rand.NewPCG(3, 5))
		alphabet := "ab\x00\xffk"
		word := func(n int) string {
			var w strings.Builder
			for range n {
				w.WriteByte(alphabet[rng.IntN(len(alphabet))])
			}
			return w.String()
		}
		for range 2000 {
			rowKey := word(rng.IntN(4))
			set := map[rowIndexEntry]struct{}{}
			for range 1 + rng.IntN(40) {
				k := word(rng.IntN(8))
				switch rng.IntN(3) {
				case 0:
					k += rowKeySuffix(rowKey)
				case 1:
					k += indexStringEnd
				}
				set[rowIndexEntry{scope: int64(1 + rng.IntN(4)), k: k}] = struct{}{}
			}
			roundTrip(rowKey, set)
		}
	})

	It("rejects a list cut inside a group", func() {
		set := entrySet(1, "\x01apple\x00\x00", 1, "\x01apply\x00\x00", 1, long,
			300, "a\x00k1\x00\x00", 300, "b", 301, "\x00\x00")
		b := roundTrip("k1", set)
		ends := groupEnds("k1", set)
		Expect(ends).To(HaveLen(4))
		for n := 0; n < len(b); n++ {
			got, err := decodeRowEntries("k1", b[:n])
			if ends[n] {
				Expect(err).NotTo(HaveOccurred(), "cut at %d, the end of a group", n)
				Expect(len(got)).To(BeNumerically("<", len(set)))
			} else {
				Expect(err).To(HaveOccurred(), "cut at %d", n)
			}
		}
	})

	DescribeTable("rejects a list encodeRowEntries cannot make",
		func(b []byte, msg string) {
			_, err := decodeRowEntries("k1", b)
			Expect(err).To(MatchError(ContainSubstring(msg)))
		},
		Entry("an unknown tail", []byte{1, 1<<3 | 3<<1, 'a'}, "unknown entry tail"),
		Entry("a shared prefix longer than the previous entry",
			[]byte{1, 1<<3 | 1, 'a', 2, 0}, "shares 2 bytes with an entry of 1"),
		Entry("a scope that repeats", []byte{1, 1 << 3, 'a', 1, 1 << 3, 'b'}, "does not follow"),
		Entry("scopes out of order", []byte{2, 1 << 3, 'a', 1, 1 << 3, 'b'}, "does not follow"),
		Entry("a scope id beyond int64",
			binary.AppendUvarint(nil, math.MaxInt64+1), "out of range"),
		Entry("a varint that overflows",
			[]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01}, "truncated scope id"),
		Entry("a part longer than the list", []byte{1, 5 << 3, 'a'}, "truncated entry"),
		Entry("a list that ends after a scope id", []byte{1}, "truncated entry"),
		Entry("a list that ends inside a shared prefix", []byte{1, 1<<3 | 1, 'a', 0x80}, "truncated shared prefix"),
		Entry("a group that says more entries follow", []byte{1, 1<<3 | 1, 'a'}, "truncated group"),
	)

	It("returns an error or a set for garbled bytes, and never panics", func() {
		rng := rand.New(rand.NewPCG(9, 13))
		b := encodeRowEntries("k1", entrySet(1, "\x01apple\x00\x00", 1, "\x01apply\x00\x00",
			2, "a\x00k1\x00\x00", 2, "b", 300, "\x00\x00"))
		for range 5000 {
			g := append([]byte(nil), b...)
			for range 1 + rng.IntN(3) {
				g[rng.IntN(len(g))] = byte(rng.IntN(256))
			}
			Expect(func() { _, _ = decodeRowEntries("k1", g) }).NotTo(Panic())
		}
	})
})
