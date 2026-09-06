package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"

	"golang.org/x/crypto/argon2"
)

// hashParams are the Argon2id cost parameters a hash is made with. They are
// written into every hash, so the verifier reads them back rather than
// assuming the current values; raising them later leaves old hashes
// verifiable.
type hashParams struct {
	memory  uint32 // KiB
	time    uint32
	threads uint8
	saltLen uint32
	keyLen  uint32
}

// currentParams follow the OWASP Password Storage Cheat Sheet's first
// recommended Argon2id configuration (19 MiB, 2 iterations, 1 lane).
var currentParams = hashParams{memory: 19 * 1024, time: 2, threads: 1, saltLen: 16, keyLen: 32}

// hashPassword renders password as a PHC string:
// $argon2id$v=19$m=<KiB>,t=<iterations>,p=<lanes>$<salt>$<hash>, both
// base64 without padding.
func hashPassword(password string) (string, error) {
	p := currentParams
	salt := make([]byte, p.saltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("auth: generate salt: %w", err)
	}
	key := argon2.IDKey([]byte(password), salt, p.time, p.memory, p.threads, p.keyLen)
	enc := base64.RawStdEncoding
	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version, p.memory, p.time, p.threads,
		enc.EncodeToString(salt), enc.EncodeToString(key)), nil
}

// verifyPassword reports whether password produced the PHC string encoded.
// The comparison is constant-time in the hash; a malformed encoding is an
// error, not a mismatch, because it means stored data is corrupt rather than
// that the caller guessed wrong.
func verifyPassword(encoded, password string) (bool, error) {
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[0] != "" || parts[1] != "argon2id" {
		return false, fmt.Errorf("auth: unsupported password hash format")
	}
	var version int
	if _, err := fmt.Sscanf(parts[2], "v=%d", &version); err != nil || version != argon2.Version {
		return false, fmt.Errorf("auth: unsupported argon2 version %q", parts[2])
	}
	var p hashParams
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &p.memory, &p.time, &p.threads); err != nil {
		return false, fmt.Errorf("auth: malformed argon2 parameters %q", parts[3])
	}
	enc := base64.RawStdEncoding
	salt, err := enc.DecodeString(parts[4])
	if err != nil {
		return false, fmt.Errorf("auth: malformed password salt: %w", err)
	}
	want, err := enc.DecodeString(parts[5])
	if err != nil {
		return false, fmt.Errorf("auth: malformed password hash: %w", err)
	}
	got := argon2.IDKey([]byte(password), salt, p.time, p.memory, p.threads, uint32(len(want)))
	return subtle.ConstantTimeCompare(got, want) == 1, nil
}

var (
	dummyOnce sync.Once
	dummyHash string
)

// verifyDummy spends the cost of a real verification against a hash no
// password matches. VerifyPassword runs it when the e-mail is unknown or the
// user has no password, so an unknown address takes as long to refuse as a
// wrong password and the response time does not say which it was.
func verifyDummy(password string) {
	dummyOnce.Do(func() {
		h, err := hashPassword(rand.Text())
		if err != nil {
			// Without entropy there is nothing to time against; the real
			// path fails the same way and the caller sees that error.
			return
		}
		dummyHash = h
	})
	if dummyHash != "" {
		_, _ = verifyPassword(dummyHash, password)
	}
}
