package auth

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// The access token is a JWS in compact serialization with exactly one
// algorithm, EdDSA over Ed25519, and one claim set, Claims. Everything the
// general JWT format allows beyond that — other algorithms, "none", nested
// tokens, unknown critical headers — is refused at parse time, so the
// verifier never has to decide what an attacker-chosen header means.

// jwtHeader is the protected header every token carries.
type jwtHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
	Kid string `json:"kid"`
}

// Claims is the payload of an access token. Subject is the user's principal
// subject, Session the id of the session that issued it. Admin, Email and
// Name are copies of the user record at issue time and are stale for at most
// the access lifetime. Times are Unix seconds, as the JWT registered claims
// prescribe.
type Claims struct {
	Issuer    string `json:"iss"`
	Subject   string `json:"sub"`
	IssuedAt  int64  `json:"iat"`
	ExpiresAt int64  `json:"exp"`
	Session   string `json:"sid"`
	Email     string `json:"email"`
	Name      string `json:"name,omitempty"`
	Admin     bool   `json:"admin,omitempty"`
}

var b64 = base64.RawURLEncoding

// signJWT renders claims as a compact JWS signed by key under the given key
// id.
func signJWT(kid string, key ed25519.PrivateKey, claims Claims) (string, error) {
	header, err := json.Marshal(jwtHeader{Alg: "EdDSA", Typ: "JWT", Kid: kid})
	if err != nil {
		return "", err
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	signingInput := b64.EncodeToString(header) + "." + b64.EncodeToString(payload)
	sig := ed25519.Sign(key, []byte(signingInput))
	return signingInput + "." + b64.EncodeToString(sig), nil
}

// parsedJWT is a token split and decoded but not yet verified. The claims in
// it are untrusted until verifyJWT has checked the signature.
type parsedJWT struct {
	kid          string
	claims       Claims
	signingInput string
	signature    []byte
}

var errMalformedJWT = errors.New("malformed token")

// parseJWT splits and decodes a compact JWS, refusing anything that is not an
// EdDSA-signed JWT with a key id.
func parseJWT(token string) (parsedJWT, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return parsedJWT{}, errMalformedJWT
	}
	headerJSON, err := b64.DecodeString(parts[0])
	if err != nil {
		return parsedJWT{}, errMalformedJWT
	}
	var header jwtHeader
	if err := strictUnmarshal(headerJSON, &header); err != nil {
		return parsedJWT{}, errMalformedJWT
	}
	if header.Alg != "EdDSA" || header.Typ != "JWT" || header.Kid == "" {
		return parsedJWT{}, fmt.Errorf("unsupported token header")
	}
	payloadJSON, err := b64.DecodeString(parts[1])
	if err != nil {
		return parsedJWT{}, errMalformedJWT
	}
	var claims Claims
	if err := json.Unmarshal(payloadJSON, &claims); err != nil {
		return parsedJWT{}, errMalformedJWT
	}
	sig, err := b64.DecodeString(parts[2])
	if err != nil || len(sig) != ed25519.SignatureSize {
		return parsedJWT{}, errMalformedJWT
	}
	return parsedJWT{
		kid:          header.Kid,
		claims:       claims,
		signingInput: parts[0] + "." + parts[1],
		signature:    sig,
	}, nil
}

// strictUnmarshal decodes JSON refusing fields the target does not declare.
// The header is where an attacker chooses what the verifier sees, so a header
// carrying anything beyond alg, typ and kid — a "crit", a "jku" — is refused
// rather than ignored.
func strictUnmarshal(data []byte, v any) error {
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

// verifyJWT checks the signature against pub. Expiry and issuer are the
// caller's to check: they need the clock and the configuration.
func verifyJWT(t parsedJWT, pub ed25519.PublicKey) bool {
	return ed25519.Verify(pub, []byte(t.signingInput), t.signature)
}
