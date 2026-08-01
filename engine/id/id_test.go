package id_test

import (
	"encoding/base32"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/id"
)

var crockford = base32.NewEncoding("0123456789ABCDEFGHJKMNPQRSTVWXYZ").WithPadding(base32.NoPadding)

var _ = Describe("New", func() {
	It("produces 26-char Crockford base32", func() {
		got := id.New()
		Expect(got).To(HaveLen(26))
		Expect(got).To(MatchRegexp(`^[0-9A-HJKMNP-TV-Z]{26}$`))
	})

	It("decodes to a UUIDv7 with the right version and variant bits", func() {
		bytes, err := crockford.DecodeString(id.New())
		Expect(err).NotTo(HaveOccurred())
		Expect(bytes).To(HaveLen(16))
		Expect(bytes[6] & 0xf0).To(Equal(byte(0x70))) // version 7
		Expect(bytes[8] & 0xc0).To(Equal(byte(0x80))) // variant 10
	})

	It("is unique across many calls", func() {
		seen := make(map[string]struct{}, 10000)
		for i := 0; i < 10000; i++ {
			seen[id.New()] = struct{}{}
		}
		Expect(seen).To(HaveLen(10000))
	})

	It("sorts lexicographically by creation time", func() {
		a := id.New()
		time.Sleep(2 * time.Millisecond)
		b := id.New()
		Expect(b > a).To(BeTrue())
	})
})
