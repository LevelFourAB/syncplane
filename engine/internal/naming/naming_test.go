package naming_test

import (
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/reflect/protoreflect"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/internal/naming"
)

// widgetField resolves one field of the Widget test entity, which carries a
// field of every kind the rules have an opinion about.
func widgetField(name string) protoreflect.FieldDescriptor {
	fd := (&testdatav1.Widget{}).ProtoReflect().Descriptor().Fields().ByName(protoreflect.Name(name))
	Expect(fd).NotTo(BeNil(), "no Widget field %q", name)
	return fd
}

var _ = Describe("ValidateEntityName", func() {
	It("accepts identifiers", func() {
		for _, name := range []string{"todo", "Todo", "todo_list", "_private", "a1", "t2m"} {
			Expect(naming.ValidateEntityName(name)).To(Succeed(), name)
		}
	})

	It("rejects an empty name", func() {
		Expect(naming.ValidateEntityName("")).To(MatchError(ContainSubstring("empty")))
	})

	// The one that corrupts the client's key layout: entity "a" row "b/x" and
	// entity "a/b" row "x" would occupy the same KV key.
	It("rejects a slash", func() {
		Expect(naming.ValidateEntityName("a/b")).To(MatchError(ContainSubstring("not an identifier")))
	})

	It("rejects punctuation, spaces and a leading digit", func() {
		for _, name := range []string{"todo-list", "todo.list", "todo list", "1todo", "todo!"} {
			Expect(naming.ValidateEntityName(name)).To(MatchError(ContainSubstring("not an identifier")), name)
		}
	})

	It("rejects a name past the length cap", func() {
		Expect(naming.ValidateEntityName(strings.Repeat("a", naming.MaxEntityNameLength))).To(Succeed())
		Expect(naming.ValidateEntityName(strings.Repeat("a", naming.MaxEntityNameLength+1))).
			To(MatchError(ContainSubstring("the limit is")))
	})
})

var _ = Describe("ValidateKeyFieldKind", func() {
	It("accepts strings, integers and bools", func() {
		for _, f := range []string{"id", "count", "big", "flag"} {
			Expect(naming.ValidateKeyFieldKind(widgetField(f))).To(Succeed(), f)
		}
	})

	// bytes and double are the cross-language hazard: both render, but not the
	// same way here as in JS.
	It("rejects bytes and floating point", func() {
		Expect(naming.ValidateKeyFieldKind(widgetField("raw"))).
			To(MatchError(ContainSubstring("must be a string, integer, bool or enum")))
		Expect(naming.ValidateKeyFieldKind(widgetField("ratio"))).
			To(MatchError(ContainSubstring("must be a string, integer, bool or enum")))
	})

	It("rejects messages, lists and maps", func() {
		Expect(naming.ValidateKeyFieldKind(widgetField("settings"))).
			To(MatchError(ContainSubstring("must be a string, integer, bool or enum")))
		Expect(naming.ValidateKeyFieldKind(widgetField("tags"))).
			To(MatchError(ContainSubstring("must be singular")))
		Expect(naming.ValidateKeyFieldKind(widgetField("attrs"))).
			To(MatchError(ContainSubstring("must be singular")))
	})
})

var _ = Describe("ValidateRowKey", func() {
	It("accepts ordinary keys, including composite and non-ASCII ones", func() {
		for _, key := range []string{"t1", "ns/id", `a\/b`, "åäö", "\U0001f600"} {
			Expect(naming.ValidateRowKey(key)).To(Succeed(), key)
		}
	})

	// What a write whose key field was never set derives.
	It("rejects the empty key", func() {
		Expect(naming.ValidateRowKey("")).To(MatchError(ContainSubstring("every key field must be set")))
	})

	It("rejects control characters", func() {
		Expect(naming.ValidateRowKey("a\x00b")).To(MatchError(ContainSubstring(`\u0000`)))
		// U+001F separates the fields of a cross-tab change token.
		Expect(naming.ValidateRowKey("a\x1fb")).To(MatchError(ContainSubstring(`\u001f`)))
		Expect(naming.ValidateRowKey("a\x7f")).To(MatchError(ContainSubstring(`\u007f`)))
	})

	It("rejects invalid UTF-8", func() {
		Expect(naming.ValidateRowKey(string([]byte{0x61, 0xff}))).
			To(MatchError(ContainSubstring("valid UTF-8")))
	})

	It("caps the length in bytes, not characters", func() {
		Expect(naming.ValidateRowKey(strings.Repeat("a", naming.MaxRowKeyBytes))).To(Succeed())
		Expect(naming.ValidateRowKey(strings.Repeat("a", naming.MaxRowKeyBytes+1))).
			To(MatchError(ContainSubstring("the limit is")))
		// Two bytes per character, so half the cap in characters is the limit.
		Expect(naming.ValidateRowKey(strings.Repeat("å", naming.MaxRowKeyBytes/2+1))).
			To(MatchError(ContainSubstring("the limit is")))
	})
})

var _ = Describe("ScalarKeyString", func() {
	It("renders each admitted kind the way the client does", func() {
		w := &testdatav1.Widget{Id: "w1", Count: -7, Big: 9007199254740993, Flag: true}
		msg := w.ProtoReflect()
		render := func(field string) string {
			fd := widgetField(field)
			return naming.ScalarKeyString(fd, msg.Get(fd))
		}
		Expect(render("id")).To(Equal("w1"))
		Expect(render("count")).To(Equal("-7"))
		// Past 2^53: the client holds this as a bigint and renders it base-10.
		Expect(render("big")).To(Equal("9007199254740993"))
		Expect(render("flag")).To(Equal("true"))
		Expect(naming.ScalarKeyString(widgetField("flag"),
			(&testdatav1.Widget{}).ProtoReflect().Get(widgetField("flag")))).To(Equal("false"))
	})
})

var _ = Describe("RowKey", func() {
	It("uses a single key field verbatim", func() {
		Expect(naming.RowKey([]string{"a/b"})).To(Equal("a/b"))
	})

	It("escapes the separator so composite tuples cannot collide", func() {
		Expect(naming.RowKey([]string{"a", "b"})).To(Equal("a/b"))
		Expect(naming.RowKey([]string{"a/b", "c"})).NotTo(Equal(naming.RowKey([]string{"a", "b/c"})))
		Expect(naming.RowKey([]string{`a\`, "b"})).To(Equal(`a\\/b`))
	})
})
