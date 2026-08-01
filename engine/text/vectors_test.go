package text_test

import (
	"encoding/hex"
	"encoding/json"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/text"
)

// vectorsPath is the shared cross-language conformance file; the TS harness
// (client/test/unit/text-vectors.test.ts) runs the identical scripts and the
// encodedHex checks pin byte-identical canonical encodings.
const vectorsPath = "../../testdata/text/vectors.json"

type vectorFile struct {
	Cases   []vectorCase   `json:"cases"`
	Rejects []vectorReject `json:"rejects"`
}

// vectorReject is one payload both decoders must refuse, with the exact
// message each must produce. Documents the two languages disagree about are
// the whole failure mode this section exists to prevent: a document the
// engine stores and no browser can read looks like a client bug.
//
// Kinds: "" or "doc" runs the document decoder; "anchor" DecodeAnchor;
// "fragment" DecodeFragment; "applyFragment" applies Hex (a fragment) to
// DocHex (a full document); "mergeFragments" merges Hex with OtherHex;
// "merge" runs the merger seam with DocHex as the stored base and Hex as the
// incoming side.
type vectorReject struct {
	Name     string `json:"name"`
	Kind     string `json:"kind"`
	Hex      string `json:"hex"`
	DocHex   string `json:"docHex"`
	OtherHex string `json:"otherHex"`
	Error    string `json:"error"`
}

type vectorCase struct {
	Name   string        `json:"name"`
	Steps  []vectorStep  `json:"steps"`
	Checks []vectorCheck `json:"checks"`
}

type vectorStep struct {
	Op     string  `json:"op"`
	Doc    string  `json:"doc"`
	From   string  `json:"from"`
	Peer   *string `json:"peer"`
	Index  int     `json:"index"`
	Len    int     `json:"len"`
	Text   string  `json:"text"`
	Start  int     `json:"start"`
	End    int     `json:"end"`
	Key    string  `json:"key"`
	Value  any     `json:"value"`
	Expand string  `json:"expand"`
	Name   string  `json:"name"`
	Kind   string  `json:"kind"`
	Of     string  `json:"of"`
	Base   string  `json:"base"`
	As     string  `json:"as"`
	Frag   string  `json:"frag"`
	A      string  `json:"a"`
	B      string  `json:"b"`
	Purged int     `json:"purged"`
	// PurgedMarks is the mark-op half of a compact step's reclaim.
	PurgedMarks int `json:"purgedMarks"`
}

type vectorCheck struct {
	Check        string          `json:"check"`
	Doc          string          `json:"doc"`
	Other        string          `json:"other"`
	Anchor       string          `json:"anchor"`
	Frag         string          `json:"frag"`
	Prev         string          `json:"prev"`
	Next         string          `json:"next"`
	Value        json.RawMessage `json:"value"`
	Ops          json.RawMessage `json:"ops"`
	MarksChanged bool            `json:"marksChanged"`
	// At is the caret position for a "marksAt" check.
	At int `json:"at"`
}

func expandOf(s string) text.Expand {
	switch s {
	case "", "none":
		return text.ExpandNone
	case "before":
		return text.ExpandBefore
	case "after":
		return text.ExpandAfter
	case "both":
		return text.ExpandBoth
	}
	Fail("unknown expand " + s)
	return text.ExpandNone
}

// jsonSpan / jsonDeltaOp are the JSON shapes of checks (mirroring the TS
// harness): spans as {start,end,key,value}, delta ops as {retain}/{insert}/
// {delete}.
type jsonSpan struct {
	Start int    `json:"start"`
	End   int    `json:"end"`
	Key   string `json:"key"`
	Value any    `json:"value"`
}

func toJSONSpans(spans []text.Span) []jsonSpan {
	out := make([]jsonSpan, 0, len(spans))
	for _, s := range spans {
		out = append(out, jsonSpan{Start: s.Start, End: s.End, Key: s.Key, Value: s.Value})
	}
	return out
}

// jsonPendingMark / jsonPoint are the JSON shapes of the zero-width mark
// checks: {key,value} for a caret's active formatting, {index,key,value} for a
// resolved point.
type jsonPendingMark struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
}

type jsonPoint struct {
	Index int    `json:"index"`
	Key   string `json:"key"`
	Value any    `json:"value"`
}

func toJSONPending(marks []text.PendingMark) []jsonPendingMark {
	out := make([]jsonPendingMark, 0, len(marks))
	for _, m := range marks {
		out = append(out, jsonPendingMark{Key: m.Key, Value: m.Value})
	}
	return out
}

func toJSONPoints(points []text.Point) []jsonPoint {
	out := make([]jsonPoint, 0, len(points))
	for _, p := range points {
		out = append(out, jsonPoint{Index: p.Index, Key: p.Key, Value: p.Value})
	}
	return out
}

func toJSONOps(ops []text.DeltaOp) []map[string]any {
	out := make([]map[string]any, 0, len(ops))
	for _, op := range ops {
		switch op.Kind {
		case text.DeltaRetain:
			out = append(out, map[string]any{"retain": float64(op.N)})
		case text.DeltaInsert:
			out = append(out, map[string]any{"insert": op.Text})
		case text.DeltaDelete:
			out = append(out, map[string]any{"delete": float64(op.N)})
		}
	}
	return out
}

var _ = Describe("shared vectors", func() {
	raw, err := os.ReadFile(vectorsPath)
	if err != nil {
		panic(err)
	}
	var file vectorFile
	if err := json.Unmarshal(raw, &file); err != nil {
		panic(err)
	}

	for _, tc := range file.Cases {
		tc := tc
		It(tc.Name, func() {
			docs := map[string]*text.Doc{}
			anchors := map[string]text.Anchor{}
			anchorDocs := map[string]*text.Doc{}
			frags := map[string][]byte{}

			for _, step := range tc.Steps {
				switch step.Op {
				case "new":
					docs[step.Doc] = text.New(*step.Peer)
				case "fork":
					peer := ""
					if step.Peer != nil {
						peer = *step.Peer
					}
					d, err := text.Decode(docs[step.From].Encode(), peer)
					Expect(err).NotTo(HaveOccurred())
					docs[step.Doc] = d
				case "insert":
					Expect(docs[step.Doc].Insert(step.Index, step.Text)).To(Succeed())
				case "delete":
					Expect(docs[step.Doc].Delete(step.Index, step.Len)).To(Succeed())
				case "mark":
					_, err := docs[step.Doc].Mark(step.Start, step.End, step.Key, step.Value, expandOf(step.Expand))
					Expect(err).NotTo(HaveOccurred())
				case "unmark":
					_, err := docs[step.Doc].Unmark(step.Start, step.End, step.Key, expandOf(step.Expand))
					Expect(err).NotTo(HaveOccurred())
				case "merge":
					target := docs[step.Doc]
					other := docs[step.From]
					ab, err := text.Merge(target.Encode(), other.Encode())
					Expect(err).NotTo(HaveOccurred())
					ba, err := text.Merge(other.Encode(), target.Encode())
					Expect(err).NotTo(HaveOccurred())
					Expect(ab).To(Equal(ba), "merge must be commutative")
					merged, err := text.Decode(ab, target.Peer())
					Expect(err).NotTo(HaveOccurred())
					docs[step.Doc] = merged
				case "anchor":
					kind := text.AnchorBefore
					if step.Kind == "after" {
						kind = text.AnchorAfter
					}
					a, err := docs[step.Doc].AnchorAt(step.Index, kind)
					Expect(err).NotTo(HaveOccurred())
					anchors[step.Name] = a
					anchorDocs[step.Name] = docs[step.Doc]
				case "setString":
					Expect(docs[step.Doc].SetString(step.Text)).To(Succeed())
				case "compact":
					// "purged" and "purgedMarks" pin how much compaction
					// reclaimed of each, which is the rule both languages have to
					// agree on before the encoding can match.
					items, marks := docs[step.Doc].Compact()
					Expect(items).To(Equal(step.Purged), "items reclaimed")
					Expect(marks).To(Equal(step.PurgedMarks), "mark ops reclaimed")
				case "fragment":
					var base *text.Doc
					if step.Base != "" {
						base = docs[step.Base]
					}
					frag, err := text.EncodeFragment(docs[step.Of], base)
					Expect(err).NotTo(HaveOccurred())
					// Fragment decode → re-encode must be stable (canonical form).
					f, err := text.DecodeFragment(frag)
					Expect(err).NotTo(HaveOccurred())
					Expect(f.Encode()).To(Equal(frag), "fragment re-encode not stable")
					frags[step.As] = frag
				case "applyFragment":
					out, err := text.ApplyFragmentBytes(docs[step.Doc].Encode(), frags[step.Frag])
					Expect(err).NotTo(HaveOccurred())
					applied, err := text.Decode(out, docs[step.Doc].Peer())
					Expect(err).NotTo(HaveOccurred())
					docs[step.Doc] = applied
				case "mergeFragment":
					// Same union as applyFragment, but reached through the public
					// merger seam — this is the entry point the engine installs for
					// text fields, so it has to route delta state itself.
					out, err := text.Merge(docs[step.Doc].Encode(), frags[step.Frag])
					Expect(err).NotTo(HaveOccurred())
					merged, err := text.Decode(out, docs[step.Doc].Peer())
					Expect(err).NotTo(HaveOccurred())
					docs[step.Doc] = merged
				case "mergeFragments":
					ab, err := text.MergeFragments(frags[step.A], frags[step.B])
					Expect(err).NotTo(HaveOccurred())
					ba, err := text.MergeFragments(frags[step.B], frags[step.A])
					Expect(err).NotTo(HaveOccurred())
					Expect(ab).To(Equal(ba), "mergeFragments must be commutative")
					frags[step.As] = ab
				default:
					Fail("unknown step op " + step.Op)
				}
			}

			for _, check := range tc.Checks {
				switch check.Check {
				case "text":
					var want string
					Expect(json.Unmarshal(check.Value, &want)).To(Succeed())
					Expect(docs[check.Doc].PlainText()).To(Equal(want))
				case "len":
					var want int
					Expect(json.Unmarshal(check.Value, &want)).To(Succeed())
					Expect(docs[check.Doc].Len()).To(Equal(want))
				case "vv":
					var want map[string]uint64
					Expect(json.Unmarshal(check.Value, &want)).To(Succeed())
					Expect(docs[check.Doc].VersionVector()).To(Equal(want))
				case "marks":
					var want []jsonSpan
					Expect(json.Unmarshal(check.Value, &want)).To(Succeed())
					spans, err := docs[check.Doc].Marks()
					Expect(err).NotTo(HaveOccurred())
					Expect(toJSONSpans(spans)).To(Equal(want))
				case "marksAt":
					var want []jsonPendingMark
					Expect(json.Unmarshal(check.Value, &want)).To(Succeed())
					pending, err := docs[check.Doc].MarksAt(check.At)
					Expect(err).NotTo(HaveOccurred())
					Expect(toJSONPending(pending)).To(Equal(want))
				case "points":
					var want []jsonPoint
					Expect(json.Unmarshal(check.Value, &want)).To(Succeed())
					points, err := docs[check.Doc].Points()
					Expect(err).NotTo(HaveOccurred())
					Expect(toJSONPoints(points)).To(Equal(want))
				case "encodedHex":
					var want string
					Expect(json.Unmarshal(check.Value, &want)).To(Succeed())
					got := hex.EncodeToString(docs[check.Doc].Encode())
					Expect(got).To(Equal(want), "canonical encoding mismatch")
				case "sameEncoding":
					Expect(docs[check.Doc].Encode()).To(Equal(docs[check.Other].Encode()))
				case "resolve":
					var want int
					Expect(json.Unmarshal(check.Value, &want)).To(Succeed())
					idx, err := anchorDocs[check.Anchor].ResolveAnchor(anchors[check.Anchor])
					Expect(err).NotTo(HaveOccurred())
					Expect(idx).To(Equal(want))
				case "fragmentHex":
					var want string
					Expect(json.Unmarshal(check.Value, &want)).To(Succeed())
					got := hex.EncodeToString(frags[check.Frag])
					Expect(got).To(Equal(want), "canonical fragment encoding mismatch")
				case "anchorHex":
					var want string
					Expect(json.Unmarshal(check.Value, &want)).To(Succeed())
					enc := text.EncodeAnchor(anchors[check.Anchor])
					Expect(hex.EncodeToString(enc)).To(Equal(want))
					back, err := text.DecodeAnchor(enc)
					Expect(err).NotTo(HaveOccurred())
					Expect(back).To(Equal(anchors[check.Anchor]))
				case "diff":
					var wantOps []map[string]any
					Expect(json.Unmarshal(check.Ops, &wantOps)).To(Succeed())
					delta, err := text.Diff(docs[check.Prev], docs[check.Next])
					Expect(err).NotTo(HaveOccurred())
					Expect(toJSONOps(delta.Ops)).To(Equal(wantOps))
					Expect(delta.MarksChanged).To(Equal(check.MarksChanged))
				default:
					Fail("unknown check " + check.Check)
				}
			}

			// Every doc must re-encode stably (canonical form) at the end.
			for name, d := range docs {
				enc := d.Encode()
				rd, err := text.Decode(enc, "")
				Expect(err).NotTo(HaveOccurred())
				Expect(rd.Encode()).To(Equal(enc), "doc %q re-encode not stable", name)
			}
		})
	}

	for _, rj := range file.Rejects {
		rj := rj
		It("rejects "+rj.Name, func() {
			data, err := hex.DecodeString(rj.Hex)
			Expect(err).NotTo(HaveOccurred())
			switch rj.Kind {
			case "anchor":
				_, err = text.DecodeAnchor(data)
			case "fragment":
				_, err = text.DecodeFragment(data)
			case "applyFragment":
				doc, derr := hex.DecodeString(rj.DocHex)
				Expect(derr).NotTo(HaveOccurred())
				_, err = text.ApplyFragmentBytes(doc, data)
			case "mergeFragments":
				other, oerr := hex.DecodeString(rj.OtherHex)
				Expect(oerr).NotTo(HaveOccurred())
				_, err = text.MergeFragments(data, other)
			case "merge":
				base, berr := hex.DecodeString(rj.DocHex)
				Expect(berr).NotTo(HaveOccurred())
				_, err = text.Merge(base, data)
			default:
				_, err = text.Decode(data, "")
			}
			Expect(err).To(MatchError(rj.Error))
		})
	}
})
