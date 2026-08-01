package lww_test

import (
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/internal/lww"
)

// concatBlobs merges two serialized Blobs by concatenating data and note.
func concatBlobs(base, incoming []byte) ([]byte, error) {
	var b, in testdatav1.Blob
	if err := proto.Unmarshal(base, &b); err != nil {
		return nil, err
	}
	if err := proto.Unmarshal(incoming, &in); err != nil {
		return nil, err
	}
	b.Data = append(b.Data, in.Data...)
	b.Note += in.Note
	return proto.Marshal(&b)
}

var _ = Describe("MergeWithMergers", func() {
	docMergers := lww.Mergers{"doc": {Fn: concatBlobs}}

	It("calls the merger when both sides carry the opaque field", func() {
		dst := &testdatav1.Widget{Doc: &testdatav1.Blob{Data: []byte("A"), Note: "a"}}
		src := &testdatav1.Widget{Doc: &testdatav1.Blob{Data: []byte("B"), Note: "b"}}
		Expect(lww.MergeWithMergers(dst, src, []string{"doc"}, docMergers)).To(Succeed())
		Expect(dst.GetDoc().GetData()).To(Equal([]byte("AB")))
		Expect(dst.GetDoc().GetNote()).To(Equal("ab"))
	})

	It("keeps replace semantics when the destination lacks the field", func() {
		dst := &testdatav1.Widget{}
		src := &testdatav1.Widget{Doc: &testdatav1.Blob{Data: []byte("B")}}
		Expect(lww.MergeWithMergers(dst, src, []string{"doc"}, docMergers)).To(Succeed())
		Expect(dst.GetDoc().GetData()).To(Equal([]byte("B")))
	})

	It("runs a MergeAbsentBase merger against an empty base", func() {
		mergers := lww.Mergers{"doc": {Fn: concatBlobs, MergeAbsentBase: true}}
		dst := &testdatav1.Widget{}
		src := &testdatav1.Widget{Doc: &testdatav1.Blob{Data: []byte("B"), Note: "b"}}
		Expect(lww.MergeWithMergers(dst, src, []string{"doc"}, mergers)).To(Succeed())
		Expect(dst.GetDoc().GetData()).To(Equal([]byte("B")))
		Expect(dst.GetDoc().GetNote()).To(Equal("b"))
	})

	It("still clears a MergeAbsentBase field when the source lacks it", func() {
		mergers := lww.Mergers{"doc": {Fn: concatBlobs, MergeAbsentBase: true}}
		dst := &testdatav1.Widget{Doc: &testdatav1.Blob{Data: []byte("A")}}
		src := &testdatav1.Widget{}
		Expect(lww.MergeWithMergers(dst, src, []string{"doc"}, mergers)).To(Succeed())
		Expect(dst.GetDoc()).To(BeNil(), "a masked, absent source still means clear")
	})

	It("keeps clear semantics when the source lacks the field", func() {
		dst := &testdatav1.Widget{Doc: &testdatav1.Blob{Data: []byte("A")}}
		src := &testdatav1.Widget{}
		Expect(lww.MergeWithMergers(dst, src, []string{"doc"}, docMergers)).To(Succeed())
		Expect(dst.GetDoc()).To(BeNil())
	})

	It("does not call the merger for other masked fields", func() {
		dst := &testdatav1.Widget{Label: "old", Doc: &testdatav1.Blob{Data: []byte("A")}}
		src := &testdatav1.Widget{Label: "new", Doc: &testdatav1.Blob{Data: []byte("B")}}
		Expect(lww.MergeWithMergers(dst, src, []string{"label"}, docMergers)).To(Succeed())
		Expect(dst.GetLabel()).To(Equal("new"))
		Expect(dst.GetDoc().GetData()).To(Equal([]byte("A")), "unmasked opaque field stays put")
	})

	It("merges when the mask names a sub-path of the opaque field", func() {
		dst := &testdatav1.Widget{Doc: &testdatav1.Blob{Data: []byte("A")}}
		src := &testdatav1.Widget{Doc: &testdatav1.Blob{Data: []byte("B")}}
		Expect(lww.MergeWithMergers(dst, src, []string{"doc.data"}, docMergers)).To(Succeed())
		Expect(dst.GetDoc().GetData()).To(Equal([]byte("AB")), "opaque fields resolve whole even under sub-paths")
	})

	It("merges opaque bytes fields when both are non-empty", func() {
		mergers := lww.Mergers{"raw": {Fn: func(base, incoming []byte) ([]byte, error) {
			return append(append([]byte{}, base...), incoming...), nil
		}}}
		dst := &testdatav1.Widget{Raw: []byte("A")}
		src := &testdatav1.Widget{Raw: []byte("B")}
		Expect(lww.MergeWithMergers(dst, src, []string{"raw"}, mergers)).To(Succeed())
		Expect(dst.GetRaw()).To(Equal([]byte("AB")))

		// Empty bytes have no presence: plain replace applies.
		dst = &testdatav1.Widget{}
		src = &testdatav1.Widget{Raw: []byte("B")}
		Expect(lww.MergeWithMergers(dst, src, []string{"raw"}, mergers)).To(Succeed())
		Expect(dst.GetRaw()).To(Equal([]byte("B")))
	})

	It("threads the path prefix to nested opaque fields", func() {
		mergers := lww.Mergers{"settings.inner": {Fn: concatBlobs}}
		dst := &testdatav1.Widget{Settings: &testdatav1.Settings{
			Color: "red",
			Inner: &testdatav1.Blob{Data: []byte("A")},
		}}
		src := &testdatav1.Widget{Settings: &testdatav1.Settings{
			Inner: &testdatav1.Blob{Data: []byte("B")},
		}}
		Expect(lww.MergeWithMergers(dst, src, []string{"settings.inner"}, mergers)).To(Succeed())
		Expect(dst.GetSettings().GetInner().GetData()).To(Equal([]byte("AB")))
		Expect(dst.GetSettings().GetColor()).To(Equal("red"), "sibling of nested path must survive")
	})

	It("propagates merger errors", func() {
		boom := errors.New("merge exploded")
		mergers := lww.Mergers{"doc": {Fn: func([]byte, []byte) ([]byte, error) { return nil, boom }}}
		dst := &testdatav1.Widget{Doc: &testdatav1.Blob{Data: []byte("A")}}
		src := &testdatav1.Widget{Doc: &testdatav1.Blob{Data: []byte("B")}}
		Expect(lww.MergeWithMergers(dst, src, []string{"doc"}, mergers)).To(MatchError(boom))
	})

	It("rejects merged bytes that do not decode for message fields", func() {
		mergers := lww.Mergers{"doc": {Fn: func([]byte, []byte) ([]byte, error) {
			return []byte{0xff, 0xff, 0xff}, nil
		}}}
		dst := &testdatav1.Widget{Doc: &testdatav1.Blob{Data: []byte("A")}}
		src := &testdatav1.Widget{Doc: &testdatav1.Blob{Data: []byte("B")}}
		Expect(lww.MergeWithMergers(dst, src, []string{"doc"}, mergers)).To(MatchError(ContainSubstring("undecodable")))
	})

	It("behaves exactly like Merge with nil mergers", func() {
		dst := &testdatav1.Widget{Doc: &testdatav1.Blob{Data: []byte("A")}}
		src := &testdatav1.Widget{Doc: &testdatav1.Blob{Data: []byte("B")}}
		Expect(lww.MergeWithMergers(dst, src, []string{"doc"}, nil)).To(Succeed())
		Expect(dst.GetDoc().GetData()).To(Equal([]byte("B")), "no merger = replace whole")
	})
})
