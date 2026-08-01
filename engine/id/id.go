// Package id generates time-ordered entity identifiers: a UUIDv7 (RFC 9562)
// rendered as 26-character Crockford base32. The leading 48 bits are a
// big-endian Unix-millisecond timestamp, so the encoded string sorts
// lexicographically by creation time (millisecond granularity), keeping
// storage index inserts local and giving rows a rough creation order.
//
// The TypeScript client (`client/src/id.ts`) emits the identical format, so
// identifiers are uniform whether minted by a client, a trusted server-side
// write, or a connector.
package id

import (
	"encoding/base32"

	"github.com/google/uuid"
)

// Crockford base32 (no ambiguous I/L/O/U). The alphabet is in ASCII order, so
// the encoded string compares byte-for-byte with the underlying UUID.
var encoding = base32.NewEncoding("0123456789ABCDEFGHJKMNPQRSTVWXYZ").WithPadding(base32.NoPadding)

// New returns a new entity id: a UUIDv7 encoded as 26-character Crockford
// base32, lexicographically sortable by creation time. It panics only if the
// system random source fails, which Go treats as fatal everywhere else too.
func New() string {
	u := uuid.Must(uuid.NewV7())
	return encoding.EncodeToString(u[:])
}
