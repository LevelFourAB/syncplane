package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("JWT", func() {
	var (
		pub    ed25519.PublicKey
		key    ed25519.PrivateKey
		claims Claims
	)

	BeforeEach(func() {
		var err error
		pub, key, err = ed25519.GenerateKey(rand.Reader)
		Expect(err).NotTo(HaveOccurred())
		claims = Claims{Issuer: "https://sync.example", Subject: "user:1", IssuedAt: 100, ExpiresAt: 200, Session: "s1", Email: "a@example.com"}
	})

	// forge rebuilds a token from a header and payload, signed with key, so a
	// spec can vary the header the parser sees.
	forge := func(header map[string]any, payload any) string {
		GinkgoHelper()
		h, err := json.Marshal(header)
		Expect(err).NotTo(HaveOccurred())
		p, err := json.Marshal(payload)
		Expect(err).NotTo(HaveOccurred())
		input := b64.EncodeToString(h) + "." + b64.EncodeToString(p)
		return input + "." + b64.EncodeToString(ed25519.Sign(key, []byte(input)))
	}

	It("signs and verifies", func() {
		token, err := signJWT("k1", key, claims)
		Expect(err).NotTo(HaveOccurred())
		t, err := parseJWT(token)
		Expect(err).NotTo(HaveOccurred())
		Expect(t.kid).To(Equal("k1"))
		Expect(t.claims).To(Equal(claims))
		Expect(verifyJWT(t, pub)).To(BeTrue())
	})

	It("rejects a signature by another key or over altered claims", func() {
		token, err := signJWT("k1", key, claims)
		Expect(err).NotTo(HaveOccurred())
		otherPub, _, err := ed25519.GenerateKey(rand.Reader)
		Expect(err).NotTo(HaveOccurred())
		t, err := parseJWT(token)
		Expect(err).NotTo(HaveOccurred())
		Expect(verifyJWT(t, otherPub)).To(BeFalse())

		tampered := claims
		tampered.Admin = true
		p, err := json.Marshal(tampered)
		Expect(err).NotTo(HaveOccurred())
		parts := strings.Split(token, ".")
		parts[1] = b64.EncodeToString(p)
		t, err = parseJWT(strings.Join(parts, "."))
		Expect(err).NotTo(HaveOccurred())
		Expect(t.claims.Admin).To(BeTrue())
		Expect(verifyJWT(t, pub)).To(BeFalse())
	})

	It("refuses every header but the one it issues", func() {
		for name, header := range map[string]map[string]any{
			"alg none":     {"alg": "none", "typ": "JWT", "kid": "k1"},
			"alg HS256":    {"alg": "HS256", "typ": "JWT", "kid": "k1"},
			"no kid":       {"alg": "EdDSA", "typ": "JWT"},
			"wrong typ":    {"alg": "EdDSA", "typ": "JWE", "kid": "k1"},
			"extra field":  {"alg": "EdDSA", "typ": "JWT", "kid": "k1", "crit": []string{"x"}},
			"key by url":   {"alg": "EdDSA", "typ": "JWT", "kid": "k1", "jku": "https://evil.example"},
			"embedded key": {"alg": "EdDSA", "typ": "JWT", "kid": "k1", "jwk": map[string]any{}},
		} {
			_, err := parseJWT(forge(header, claims))
			Expect(err).To(HaveOccurred(), name)
		}
	})

	It("refuses malformed tokens", func() {
		good, err := signJWT("k1", key, claims)
		Expect(err).NotTo(HaveOccurred())
		parts := strings.Split(good, ".")
		for name, token := range map[string]string{
			"empty":            "",
			"two parts":        parts[0] + "." + parts[1],
			"four parts":       good + ".x",
			"bad header b64":   "!!." + parts[1] + "." + parts[2],
			"bad payload b64":  parts[0] + ".!!." + parts[2],
			"bad sig b64":      parts[0] + "." + parts[1] + ".!!",
			"short signature":  parts[0] + "." + parts[1] + "." + b64.EncodeToString([]byte("short")),
			"payload not json": parts[0] + "." + b64.EncodeToString([]byte("{")) + "." + parts[2],
		} {
			_, err := parseJWT(token)
			Expect(err).To(HaveOccurred(), name)
		}
	})
})
