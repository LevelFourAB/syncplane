package auth

import (
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("password hashing", func() {
	It("round-trips through the PHC encoding", func() {
		h, err := hashPassword("correct horse")
		Expect(err).NotTo(HaveOccurred())
		Expect(h).To(HavePrefix("$argon2id$v=19$m=19456,t=2,p=1$"))

		ok, err := verifyPassword(h, "correct horse")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())

		ok, err = verifyPassword(h, "correct horsf")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeFalse())
	})

	It("salts every hash", func() {
		a, err := hashPassword("correct horse")
		Expect(err).NotTo(HaveOccurred())
		b, err := hashPassword("correct horse")
		Expect(err).NotTo(HaveOccurred())
		Expect(a).NotTo(Equal(b))
	})

	It("verifies a hash made under other cost parameters", func() {
		// The parameters travel with the hash, so raising currentParams later
		// keeps every stored password verifiable.
		h, err := hashPassword("correct horse")
		Expect(err).NotTo(HaveOccurred())
		parts := strings.Split(h, "$")
		Expect(parts).To(HaveLen(6))
		Expect(parts[3]).To(Equal("m=19456,t=2,p=1"))
		// A hash written with the same salt under different parameters yields a
		// different key, so it must be rejected rather than silently accepted.
		parts[3] = "m=8192,t=1,p=1"
		ok, err := verifyPassword(strings.Join(parts, "$"), "correct horse")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeFalse())
	})

	It("reports a malformed hash as an error, not a mismatch", func() {
		for _, bad := range []string{
			"",
			"plaintext",
			"$bcrypt$x$y$z$w",
			"$argon2id$v=18$m=19456,t=2,p=1$c2FsdA$aGFzaA",
			"$argon2id$v=19$m=x$c2FsdA$aGFzaA",
			"$argon2id$v=19$m=19456,t=2,p=1$***$aGFzaA",
		} {
			_, err := verifyPassword(bad, "correct horse")
			Expect(err).To(HaveOccurred(), "hash %q", bad)
		}
	})
})
