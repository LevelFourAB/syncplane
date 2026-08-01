package lww_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/internal/lww"
)

// A oneof is one field: masks name the GROUP, and the group is replaced whole.
// These pin the rule from both ends — what the group form does, and that the
// member form is refused rather than quietly doing something else.
var _ = Describe("Merge of oneofs", func() {
	var base *testdatav1.Widget

	BeforeEach(func() {
		base = &testdatav1.Widget{
			Id:     "w1",
			Label:  "original",
			Choice: &testdatav1.Widget_ChoiceText{ChoiceText: "hello"},
		}
	})

	It("replaces the active case with the source's", func() {
		src := &testdatav1.Widget{Choice: &testdatav1.Widget_ChoiceNumber{ChoiceNumber: 7}}
		Expect(lww.Merge(base, src, []string{"choice"})).To(Succeed())
		Expect(base.GetChoiceNumber()).To(BeEquivalentTo(7))
		Expect(base.GetChoiceText()).To(BeEmpty(), "the case that lost must be gone")
	})

	It("sets a case on a base that had none", func() {
		base.Choice = nil
		src := &testdatav1.Widget{Choice: &testdatav1.Widget_ChoiceText{ChoiceText: "set"}}
		Expect(lww.Merge(base, src, []string{"choice"})).To(Succeed())
		Expect(base.GetChoiceText()).To(Equal("set"))
	})

	// The whole reason the group is the unit: "the writer left the oneof empty"
	// is a statement the group can carry and a single member cannot.
	It("clears the group when the source has no case", func() {
		src := &testdatav1.Widget{}
		Expect(lww.Merge(base, src, []string{"choice"})).To(Succeed())
		Expect(base.GetChoice()).To(BeNil())
	})

	It("leaves the group alone when the mask does not name it", func() {
		src := &testdatav1.Widget{Label: "patched"}
		Expect(lww.Merge(base, src, []string{"label"})).To(Succeed())
		Expect(base.GetChoiceText()).To(Equal("hello"))
	})

	It("replaces a message case whole", func() {
		base.Choice = &testdatav1.Widget_ChoiceSettings{
			ChoiceSettings: &testdatav1.Settings{Color: "red", Size: 4},
		}
		src := &testdatav1.Widget{Choice: &testdatav1.Widget_ChoiceSettings{
			ChoiceSettings: &testdatav1.Settings{Color: "blue"},
		}}
		Expect(lww.Merge(base, src, []string{"choice"})).To(Succeed())
		Expect(base.GetChoiceSettings().GetColor()).To(Equal("blue"))
		Expect(base.GetChoiceSettings().GetSize()).To(BeZero(), "the group is atomic, not merged per field")
	})

	It("does not alias a source message case into the destination", func() {
		src := &testdatav1.Widget{Choice: &testdatav1.Widget_ChoiceSettings{
			ChoiceSettings: &testdatav1.Settings{Color: "blue"},
		}}
		Expect(lww.Merge(base, src, []string{"choice"})).To(Succeed())
		src.GetChoiceSettings().Color = "mutated-after-merge"
		Expect(base.GetChoiceSettings().GetColor()).To(Equal("blue"))
	})

	It("rejects a mask that names a member", func() {
		src := &testdatav1.Widget{Choice: &testdatav1.Widget_ChoiceText{ChoiceText: "x"}}
		err := lww.Merge(base, src, []string{"choice_text"})
		Expect(err).To(MatchError(ContainSubstring(`member of oneof "choice"`)))
		Expect(base.GetChoiceText()).To(Equal("hello"), "a rejected path must not half-apply")
	})

	It("rejects a sub-path into a message case", func() {
		src := &testdatav1.Widget{Choice: &testdatav1.Widget_ChoiceSettings{
			ChoiceSettings: &testdatav1.Settings{Color: "blue"},
		}}
		Expect(lww.Merge(base, src, []string{"choice.choice_settings.color"})).NotTo(Succeed())
	})

	// proto3 `optional` compiles to a synthetic oneof. Treating that as a real
	// group would make every optional field unmaskable, which is why every check
	// goes through lww.OneofOf.
	It("still masks an optional field by its own name", func() {
		base.Maybe = nil
		src := &testdatav1.Widget{Maybe: ptr("here")}
		Expect(lww.Merge(base, src, []string{"maybe"})).To(Succeed())
		Expect(base.GetMaybe()).To(Equal("here"))

		cleared := &testdatav1.Widget{}
		Expect(lww.Merge(base, cleared, []string{"maybe"})).To(Succeed())
		Expect(base.Maybe).To(BeNil(), "explicit presence must still be clearable")
	})
})

func ptr[T any](v T) *T { return &v }
