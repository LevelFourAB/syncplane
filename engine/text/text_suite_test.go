package text_test

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"testing"
	"unicode/utf16"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// update makes the specs that own a generated file under testdata/text
// rewrite it from the current implementation instead of checking against it:
//
//	go test ./engine/text -update
var update = flag.Bool("update", false, "rewrite the generated vector files under testdata/text")

func TestText(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Text Suite")
}

// readVectorFile decodes the JSON file at path into v.
func readVectorFile(path string, v any) {
	GinkgoHelper()
	b, err := os.ReadFile(path)
	Expect(err).NotTo(HaveOccurred())
	Expect(json.Unmarshal(b, v)).To(Succeed())
}

// writeVectorFile writes v to path as tab-indented JSON. Every non-ASCII code
// point is written as a \u escape, because the vectors hold invisible and
// combining characters that an editor or a diff would hide or mangle.
func writeVectorFile(path string, v any) {
	GinkgoHelper()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "\t")
	Expect(enc.Encode(v)).To(Succeed())
	var out bytes.Buffer
	for _, r := range buf.String() {
		switch {
		case r < 0x80:
			out.WriteRune(r)
		case r > 0xffff:
			hi, lo := utf16.EncodeRune(r)
			fmt.Fprintf(&out, "\\u%04x\\u%04x", hi, lo)
		default:
			fmt.Fprintf(&out, "\\u%04x", r)
		}
	}
	Expect(os.WriteFile(path, out.Bytes(), 0o644)).To(Succeed())
}

// sameJSON fails unless got and want encode to the same JSON, which compares
// a freshly built value with one read back from a file without caring about
// the Go types JSON decoding picked.
func sameJSON(got, want any, description ...any) {
	GinkgoHelper()
	g, err := json.Marshal(got)
	Expect(err).NotTo(HaveOccurred())
	w, err := json.Marshal(want)
	Expect(err).NotTo(HaveOccurred())
	Expect(string(g)).To(Equal(string(w)), description...)
}
