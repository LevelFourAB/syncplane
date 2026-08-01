package lww_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/reflect/protoreflect"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/internal/lww"
)

var _ = Describe("Reference detection", func() {
	field := func(m protoreflect.Message, name string) protoreflect.FieldDescriptor {
		fd := m.Descriptor().Fields().ByName(protoreflect.Name(name))
		Expect(fd).NotTo(BeNil())
		return fd
	}
	issue := (&testdatav1.Issue{}).ProtoReflect()
	widget := (&testdatav1.Widget{}).ProtoReflect()

	It("recognizes singular, repeated and composite entity-typed fields", func() {
		Expect(lww.IsRef(field(issue, "project"))).To(BeTrue())
		Expect(lww.IsRef(field(issue, "blocked_by"))).To(BeTrue())
		Expect(lww.IsRef(field(issue, "pair"))).To(BeTrue())
	})

	It("does not treat scalars or non-entity messages as refs", func() {
		Expect(lww.IsRef(field(issue, "title"))).To(BeFalse())
		Expect(lww.IsRef(field(widget, "settings"))).To(BeFalse()) // Settings is not an entity
		Expect(lww.IsRef(field(widget, "members"))).To(BeFalse())  // keyed list, Member not an entity
	})

	It("treats a repeated keyed list of an entity element as a ref (Widget.refs → Pair)", func() {
		Expect(lww.IsRef(field(widget, "refs"))).To(BeTrue())
	})

	It("resolves the target's key fields", func() {
		keys := lww.RefKeyFields(field(issue, "pair"))
		names := make([]string, len(keys))
		for i, k := range keys {
			names[i] = string(k.Name())
		}
		Expect(names).To(Equal([]string{"ns", "id"}))
	})
})
