package query

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
)

// Compile-time leaf classification (leafKind/singularKind) decides which
// operators a field accepts; runtime classification (extract/extractSingular)
// decides what happens once a row is in hand. They are two switches over the
// same descriptor, so they can drift — and drift is invisible: the compiler
// would accept a query the evaluator then answers with a silent false. This
// walks every field of the test schema and pins the two together.
func TestLeafKindMatchesExtract(t *testing.T) {
	for _, prototype := range []proto.Message{
		&testdatav1.Widget{}, &testdatav1.Issue{}, &testdatav1.Settings{},
	} {
		msg := prototype.ProtoReflect()
		desc := msg.Descriptor()
		for i := 0; i < desc.Fields().Len(); i++ {
			fd := desc.Fields().Get(i)
			static := leafKind([]protoreflect.FieldDescriptor{fd})
			runtime := extract(fd, msg.Get(fd)).kind
			if static != runtime {
				t.Errorf("%s: leafKind = %v, extract = %v", fd.FullName(), static, runtime)
			}
			if !fd.IsList() {
				continue
			}
			elemStatic := singularKind(fd)
			elemRuntime := extractSingular(fd, msg.Mutable(fd).List().NewElement()).kind
			if elemStatic != elemRuntime {
				t.Errorf("%s element: singularKind = %v, extractSingular = %v",
					fd.FullName(), elemStatic, elemRuntime)
			}
		}
	}
}
