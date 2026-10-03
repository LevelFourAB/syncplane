package server

import (
	"fmt"
	"strconv"

	"github.com/levelfourab/syncplane/engine/internal/naming"
)

// watermarkKey is the storage key of a client's idempotency watermark and of
// the verdicts recorded with it. An anonymous caller's key is its client_id;
// an authenticated caller's key also names its issuer and subject.
//
// The client_id is unauthenticated, and every reader of a collaborative-text
// document sees it in the peer ids. Keyed by the client_id alone, any
// principal could push under another's client_id and use up or jump past ids
// the owner has not sent yet, and the owner's own pushes would then be acked
// as ALREADY_APPLIED with nothing written. Anonymous callers share one
// watermark per client_id: there is no identity to divide them by.
//
// An authenticated key starts with U+001F, which checkClientID keeps out of
// every client_id, so it never equals an anonymous key. The length prefixes
// make it injective across (issuer, subject, client_id): an issuer or subject
// that holds the separator cannot shift bytes into its neighbour.
func watermarkKey(p Principal, clientID string) string {
	if clientID == "" || p.IsAnonymous() {
		return clientID
	}
	return callerPrefix(p) + clientID
}

// callerPrefix names a principal at the start of a storage key: U+001F, then
// the issuer and the subject, each after its length. A digit always follows
// the U+001F, and the length prefixes make the result injective across
// (issuer, subject, rest of the key).
func callerPrefix(p Principal) string {
	return "\u001f" + strconv.Itoa(len(p.Issuer)) + ":" + p.Issuer +
		strconv.Itoa(len(p.Subject)) + ":" + p.Subject
}

// checkClientID refuses a client_id longer than naming.MaxClientIDBytes, or one
// that holds a C0 control character (U+0000 to U+001F). U+001F opens every
// authenticated watermarkKey, and none of the range belongs in an installation
// handle. A byte compare is enough: no byte of a multi-byte UTF-8 sequence is
// below 0x80.
func checkClientID(clientID string) error {
	if len(clientID) > naming.MaxClientIDBytes {
		return fmt.Errorf("client_id is %d bytes, the limit is %d", len(clientID), naming.MaxClientIDBytes)
	}
	for i := 0; i < len(clientID); i++ {
		if clientID[i] < 0x20 {
			return fmt.Errorf("client_id holds control character %U at byte %d", rune(clientID[i]), i)
		}
	}
	return nil
}
