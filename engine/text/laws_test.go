package text_test

import (
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/text"
)

// The CRDT laws over seeded random multi-peer histories, with marks,
// setString rewrites, fragment syncs and unicode that stresses every index
// unit (astral code points, combining marks, ZWJ sequences, CRLF, NUL, BOM,
// U+FFFD, line separators).
//
// Every history is recorded as an explicit op script. A few of them are kept
// in historiesPath, which client/test/unit/text-laws.test.ts replays in
// TypeScript to the same bytes.

// historiesPath holds generated histories shared with the TS harness. Run
// the suite with -update to rewrite it after a change to the encoding.
const historiesPath = "../../testdata/text/histories.json"

// lawAlphabet is the code-point pool insertions draw from. Each entry is ONE
// code point, so a ZWJ emoji family is built by drawing several in a row.
var lawAlphabet = []string{
	"a", "b", "c", " ", "\n", "\r", "\u0000", "\ufeff", "\ufffd", "\u2028",
	"\u0301",                                 // combining acute
	"\u200d",                                 // zero-width joiner
	"\U0001F468", "\U0001F469", "\U0001F467", // man, woman, girl (ZWJ family parts)
	"\U0001F389", "\U00010000", "\uffff", "é", "ß", "中",
}

// lawPeers mixes peers whose UTF-8 and UTF-16 orders disagree (U+FFFF sorts
// before U+10000 in UTF-8 and after it in UTF-16), so sibling order and the
// peer table are both exercised across the two orders.
var lawPeers = []string{"A", "\uffff", "\U00010000", "b:0", "é"}

type lawOp struct {
	Kind   string   `json:"kind"`
	R      int      `json:"r"`
	J      int      `json:"j,omitempty"`
	Index  int      `json:"index,omitempty"`
	Length int      `json:"length,omitempty"`
	Text   string   `json:"text,omitempty"`
	Key    string   `json:"key,omitempty"`
	Bool   *bool    `json:"bool,omitempty"`
	Str    *string  `json:"str,omitempty"`
	Num    *float64 `json:"num,omitempty"`
	Expand int      `json:"expand,omitempty"`
}

type lawSpan struct {
	Start int    `json:"start"`
	End   int    `json:"end"`
	Key   string `json:"key"`
	Value any    `json:"value"`
}

type lawCase struct {
	Seed      int64     `json:"seed"`
	Rounds    int       `json:"rounds"`
	Peers     []string  `json:"peers"`
	Ops       []lawOp   `json:"ops"`
	Replicas  []string  `json:"replicas"`  // hex of each replica's final encoding
	Merged    string    `json:"merged"`    // hex of all replicas merged in index order
	Compacted string    `json:"compacted"` // hex of Merged after Compact
	Text      string    `json:"text"`
	Spans     []lawSpan `json:"spans"`
}

func randomRun(rng *rand.Rand) string {
	var sb strings.Builder
	for n := 1 + rng.Intn(5); n > 0; n-- {
		sb.WriteString(lawAlphabet[rng.Intn(len(lawAlphabet))])
	}
	return sb.String()
}

// mutateRunes returns s with a few random code-point edits, the input a
// whole-value writer hands SetString.
func mutateRunes(rng *rand.Rand, s string) string {
	r := []rune(s)
	for n := 1 + rng.Intn(3); n > 0; n-- {
		switch {
		case len(r) > 0 && rng.Intn(2) == 0:
			at := rng.Intn(len(r))
			end := min(len(r), at+1+rng.Intn(3))
			r = append(r[:at:at], r[end:]...)
		default:
			at := rng.Intn(len(r) + 1)
			ins := []rune(randomRun(rng))
			r = append(r[:at:at], append(ins, r[at:]...)...)
		}
	}
	return string(r)
}

// randomExpand picks an expand policy; a collapsed range only takes the two
// policies the API accepts for it.
func randomExpand(rng *rand.Rand, collapsed bool) int {
	if collapsed {
		return []int{int(text.ExpandNone), int(text.ExpandBoth)}[rng.Intn(2)]
	}
	return rng.Intn(4)
}

func mustSpans(d *text.Doc) []lawSpan {
	GinkgoHelper()
	spans, err := d.Marks()
	Expect(err).NotTo(HaveOccurred())
	out := make([]lawSpan, 0, len(spans))
	for _, s := range spans {
		out = append(out, lawSpan{Start: s.Start, End: s.End, Key: s.Key, Value: s.Value})
	}
	return out
}

func mustMerge(a, b []byte) []byte {
	GinkgoHelper()
	m, err := text.Merge(a, b)
	Expect(err).NotTo(HaveOccurred())
	return m
}

// runLawCase builds one random history, checking the laws as it goes, and
// returns its script and results.
func runLawCase(seed int64, rounds int) lawCase {
	GinkgoHelper()
	rng := rand.New(rand.NewSource(seed))
	nrep := 3 + rng.Intn(3)
	peers := lawPeers[:nrep]
	docs := make([]*text.Doc, nrep)
	snaps := make([][][]byte, nrep)
	for i := range docs {
		docs[i] = text.New(peers[i])
		snaps[i] = make([][]byte, nrep)
	}
	c := lawCase{Seed: seed, Rounds: rounds, Peers: peers}
	for round := 0; round < rounds; round++ {
		r := rng.Intn(nrep)
		d := docs[r]
		var op lawOp
		switch k := rng.Intn(20); {
		case k < 7:
			op = lawOp{Kind: "insert", R: r, Index: rng.Intn(d.Len() + 1), Text: randomRun(rng)}
			Expect(d.Insert(op.Index, op.Text)).To(Succeed())
		case k < 10:
			l := d.Len()
			if l == 0 {
				continue
			}
			at := rng.Intn(l)
			op = lawOp{Kind: "delete", R: r, Index: at, Length: 1 + rng.Intn(min(4, l-at))}
			Expect(d.Delete(op.Index, op.Length)).To(Succeed())
		case k < 12:
			l := d.Len()
			s := rng.Intn(l + 1)
			e := s + rng.Intn(l-s+1) // collapsed marks (s == e) included
			op = lawOp{Kind: "mark", R: r, Index: s, Length: e - s,
				Key: "k" + fmt.Sprint(rng.Intn(3)), Expand: randomExpand(rng, s == e)}
			var v any
			switch rng.Intn(3) {
			case 0:
				b := rng.Intn(2) == 0
				op.Bool, v = &b, b
			case 1:
				str := randomRun(rng)
				op.Str, v = &str, str
			default:
				f := float64(rng.Intn(100)) / 4
				op.Num, v = &f, f
			}
			_, err := d.Mark(s, e, op.Key, v, text.Expand(op.Expand))
			Expect(err).NotTo(HaveOccurred())
		case k < 13:
			l := d.Len()
			s := rng.Intn(l + 1)
			e := s + rng.Intn(l-s+1)
			op = lawOp{Kind: "unmark", R: r, Index: s, Length: e - s,
				Key: "k" + fmt.Sprint(rng.Intn(3)), Expand: randomExpand(rng, s == e)}
			_, err := d.Unmark(s, e, op.Key, text.Expand(op.Expand))
			Expect(err).NotTo(HaveOccurred())
		case k < 15:
			op = lawOp{Kind: "setString", R: r, Text: mutateRunes(rng, d.PlainText())}
			Expect(d.SetString(op.Text)).To(Succeed())
			Expect(d.PlainText()).To(Equal(op.Text))
		case k < 18:
			j := rng.Intn(nrep)
			if j == r {
				continue
			}
			op = lawOp{Kind: "sync", R: r, J: j}
			merged := mustMerge(d.Encode(), docs[j].Encode())
			// Commutative and idempotent at every step, not only at the end.
			Expect(mustMerge(docs[j].Encode(), d.Encode())).To(Equal(merged),
				"seed %d round %d: merge not commutative", seed, round)
			Expect(mustMerge(merged, docs[j].Encode())).To(Equal(merged),
				"seed %d round %d: merge not idempotent", seed, round)
			nd, err := text.Decode(merged, peers[r])
			Expect(err).NotTo(HaveOccurred())
			docs[r] = nd
			snaps[r][j], snaps[j][r] = docs[j].Encode(), docs[j].Encode()
		default:
			j := rng.Intn(nrep)
			if j == r {
				continue
			}
			op = lawOp{Kind: "syncFrag", R: r, J: j}
			base, err := text.Decode(snaps[r][j], "")
			Expect(err).NotTo(HaveOccurred())
			frag, err := text.EncodeFragment(docs[j], base)
			Expect(err).NotTo(HaveOccurred())
			via, err := text.ApplyFragmentBytes(d.Encode(), frag)
			Expect(err).NotTo(HaveOccurred())
			Expect(via).To(Equal(mustMerge(d.Encode(), docs[j].Encode())),
				"seed %d round %d: fragment sync diverged from merge", seed, round)
			nd, err := text.Decode(via, peers[r])
			Expect(err).NotTo(HaveOccurred())
			docs[r] = nd
			snaps[r][j], snaps[j][r] = docs[j].Encode(), docs[j].Encode()
		}
		c.Ops = append(c.Ops, op)
	}

	encs := make([][]byte, nrep)
	for i, d := range docs {
		encs[i] = d.Encode()
		c.Replicas = append(c.Replicas, hex.EncodeToString(encs[i]))
		// Decode/encode round-trips byte-for-byte.
		rt, err := text.Decode(encs[i], "")
		Expect(err).NotTo(HaveOccurred())
		Expect(rt.Encode()).To(Equal(encs[i]))
	}
	// Associativity: ((0∪1)∪2) == (0∪(1∪2)), and any permutation agrees.
	left := mustMerge(mustMerge(encs[0], encs[1]), encs[2])
	right := mustMerge(encs[0], mustMerge(encs[1], encs[2]))
	Expect(left).To(Equal(right), "seed %d: merge not associative", seed)
	merged := encs[0]
	for _, e := range encs[1:] {
		merged = mustMerge(merged, e)
	}
	shuffled := encs[len(encs)-1]
	for _, i := range rng.Perm(len(encs)) {
		shuffled = mustMerge(shuffled, encs[i])
	}
	Expect(shuffled).To(Equal(merged), "seed %d: merge order changed the result", seed)
	c.Merged = hex.EncodeToString(merged)

	md, err := text.Decode(merged, "")
	Expect(err).NotTo(HaveOccurred())
	c.Text = md.PlainText()
	c.Spans = mustSpans(md)
	md.Compact()
	// Compaction never changes what a reader sees.
	Expect(md.PlainText()).To(Equal(c.Text), "seed %d: compaction changed the text", seed)
	Expect(mustSpans(md)).To(Equal(c.Spans), "seed %d: compaction changed the marks", seed)
	c.Compacted = hex.EncodeToString(md.Encode())
	// Compaction is idempotent.
	again, err := text.Decode(md.Encode(), "")
	Expect(err).NotTo(HaveOccurred())
	items, marks := again.Compact()
	Expect(items+marks).To(BeZero(), "seed %d: second compaction reclaimed more", seed)
	return c
}

type historiesFile struct {
	Comment   string    `json:"comment"`
	Histories []lawCase `json:"histories"`
}

var _ = Describe("CRDT laws over random unicode histories", func() {
	It("keeps merge commutative, associative and idempotent, and compaction invisible", func() {
		for seed := int64(9000); seed < 9060; seed++ {
			runLawCase(seed, 120)
		}
	})

	It("reproduces the histories shared with the TS harness", func() {
		if *update {
			f := historiesFile{Comment: "Generated by engine/text/laws_test.go; rewrite with " +
				"`go test ./engine/text -update`. Each history is an op script over replicas " +
				"owned by peers, with the encoding of each replica at the end, of all of them " +
				"merged in order, and of that merge after compaction. Indices count code " +
				"points of visible text. client/test/unit/text-laws.test.ts replays every " +
				"script and must produce the same bytes."}
			for seed := int64(9100); seed < 9110; seed++ {
				f.Histories = append(f.Histories, runLawCase(seed, 60))
			}
			writeVectorFile(historiesPath, f)
		}
		var f historiesFile
		readVectorFile(historiesPath, &f)
		Expect(f.Histories).NotTo(BeEmpty())
		for _, want := range f.Histories {
			sameJSON(runLawCase(want.Seed, want.Rounds), want, "seed %d", want.Seed)
		}
	})
})
