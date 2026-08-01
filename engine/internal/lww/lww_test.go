package lww_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/reflect/protoreflect"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/internal/lww"
)

var _ = Describe("Merge", func() {
	var base *testdatav1.Widget

	BeforeEach(func() {
		base = &testdatav1.Widget{
			Id:    "w1",
			Label: "original",
			Count: 10,
			Settings: &testdatav1.Settings{
				Color: "red",
				Size:  4,
			},
			Doc:  &testdatav1.Blob{Data: []byte{1, 2, 3}, Note: "v1"},
			Tags: []string{"a", "b"},
		}
	})

	It("copies only masked scalar fields", func() {
		src := &testdatav1.Widget{Label: "patched", Count: 99}
		Expect(lww.Merge(base, src, []string{"label"})).To(Succeed())
		Expect(base.GetLabel()).To(Equal("patched"))
		Expect(base.GetCount()).To(BeEquivalentTo(10), "unmasked field must not change")
	})

	It("clears a masked field that is zero in the source", func() {
		src := &testdatav1.Widget{}
		Expect(lww.Merge(base, src, []string{"label"})).To(Succeed())
		Expect(base.GetLabel()).To(BeEmpty())
	})

	It("replaces repeated fields whole", func() {
		src := &testdatav1.Widget{Tags: []string{"x"}}
		Expect(lww.Merge(base, src, []string{"tags"})).To(Succeed())
		Expect(base.GetTags()).To(Equal([]string{"x"}))
	})

	It("merges nested paths without touching sibling fields", func() {
		src := &testdatav1.Widget{Settings: &testdatav1.Settings{Color: "blue"}}
		Expect(lww.Merge(base, src, []string{"settings.color"})).To(Succeed())
		Expect(base.GetSettings().GetColor()).To(Equal("blue"))
		Expect(base.GetSettings().GetSize()).To(BeEquivalentTo(4), "sibling of nested path must survive")
	})

	It("replaces a whole message field when masked at the top level", func() {
		src := &testdatav1.Widget{Settings: &testdatav1.Settings{Color: "green"}}
		Expect(lww.Merge(base, src, []string{"settings"})).To(Succeed())
		Expect(base.GetSettings().GetColor()).To(Equal("green"))
		Expect(base.GetSettings().GetSize()).To(BeZero(), "whole-message replace drops old sub-fields")
	})

	It("creates intermediate messages for nested paths on an empty base", func() {
		empty := &testdatav1.Widget{}
		src := &testdatav1.Widget{Settings: &testdatav1.Settings{Color: "blue"}}
		Expect(lww.Merge(empty, src, []string{"settings.color"})).To(Succeed())
		Expect(empty.GetSettings().GetColor()).To(Equal("blue"))
	})

	// Presence of a nested message is observable (isSet/isNotSet) and can drive
	// subscription predicates, so a leaf write must never move it on its own.
	It("does not create a nested message when the masked leaf is unset in the source", func() {
		empty := &testdatav1.Widget{Id: "w1"}
		src := &testdatav1.Widget{Settings: &testdatav1.Settings{}}
		Expect(lww.Merge(empty, src, []string{"settings.color"})).To(Succeed())
		Expect(empty.GetSettings()).To(BeNil(), "a no-op leaf write must not materialize the container")
	})

	It("does not create a nested message when the masked opaque leaf is unset in the source", func() {
		empty := &testdatav1.Widget{Id: "w1"}
		src := &testdatav1.Widget{Settings: &testdatav1.Settings{}}
		Expect(lww.Merge(empty, src, []string{"settings.inner.data"})).To(Succeed())
		Expect(empty.GetSettings()).To(BeNil())
	})

	It("drops a nested message emptied by clearing its last field", func() {
		base.Settings = &testdatav1.Settings{Color: "red"}
		src := &testdatav1.Widget{}
		Expect(lww.Merge(base, src, []string{"settings.color"})).To(Succeed())
		Expect(base.GetSettings()).To(BeNil(), "an emptied container must not stay present")
	})

	It("keeps a nested message whose siblings survive the clear", func() {
		src := &testdatav1.Widget{}
		Expect(lww.Merge(base, src, []string{"settings.color"})).To(Succeed())
		Expect(base.GetSettings()).NotTo(BeNil())
		Expect(base.GetSettings().GetSize()).To(BeEquivalentTo(4))
	})

	It("keeps a nested message that carries only unknown fields", func() {
		base.Settings = &testdatav1.Settings{Color: "red"}
		// Field 15, varint 1 — a field this binary's schema does not know.
		base.Settings.ProtoReflect().SetUnknown(protoreflect.RawFields{0x78, 0x01})
		src := &testdatav1.Widget{}
		Expect(lww.Merge(base, src, []string{"settings.color"})).To(Succeed())
		Expect(base.GetSettings()).NotTo(BeNil(), "a newer schema's fields are content, not emptiness")
	})

	It("replaces opaque fields whole even when the mask names sub-fields", func() {
		src := &testdatav1.Widget{Doc: &testdatav1.Blob{Note: "v2"}}
		Expect(lww.Merge(base, src, []string{"doc.note"})).To(Succeed())
		Expect(base.GetDoc().GetNote()).To(Equal("v2"))
		Expect(base.GetDoc().GetData()).To(BeEmpty(), "opaque field must be replaced whole, not merged")
	})

	It("clears an opaque field when the source has none", func() {
		src := &testdatav1.Widget{}
		Expect(lww.Merge(base, src, []string{"doc"})).To(Succeed())
		Expect(base.GetDoc()).To(BeNil())
	})

	It("does not alias source sub-messages into the destination", func() {
		src := &testdatav1.Widget{Settings: &testdatav1.Settings{Color: "blue"}}
		Expect(lww.Merge(base, src, []string{"settings"})).To(Succeed())
		src.Settings.Color = "mutated-after-merge"
		Expect(base.GetSettings().GetColor()).To(Equal("blue"))
	})

	It("rejects unknown fields in the mask", func() {
		src := &testdatav1.Widget{}
		Expect(lww.Merge(base, src, []string{"nope"})).NotTo(Succeed())
	})

	It("rejects sub-paths through scalar fields", func() {
		src := &testdatav1.Widget{}
		Expect(lww.Merge(base, src, []string{"label.x"})).NotTo(Succeed())
	})
})
