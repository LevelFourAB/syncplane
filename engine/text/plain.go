package text

import (
	"errors"
	"fmt"
)

// Plain-string values (format version 3).
//
// A collaborative-text field normally holds a document, but a value sometimes
// has to say what the field should read as before anyone has decided who is
// writing it. Field history is the case that forced it: the server captures
// what a field held, and the identity that puts it back belongs to whoever
// restores. Applying the text means diffing it against the stored document and
// minting the difference under a peer, so the form carries the text and leaves
// the identity to the resolver.
//
// The tag makes that a legal value of the field rather than a convention two
// sides have to agree on out of band: a reader tells the forms apart by the
// first byte, PlainText reads either, and every path that would treat one as a
// document refuses it by name.
//
// Where it may appear is a matter of DIRECTION, not of storage. It is legal at
// rest — a field-history record's serialized before-state carries one, and that
// record is stored and served like any other row — and illegal as the incoming
// side of a write to a text field, which is what Merge refuses. No live text
// field ever stores one: the resolvers (EntityTx.Put/Patch, the TypeScript
// client's enqueue path) convert it under their own peer first.
//
// docs/explanation/collaborative-text.md carries the reasoning.

// plainStringVersion is the leading byte marking a plain-string value, beside
// formatVersion (1, a document) and fragmentVersion (2, delta state).
const plainStringVersion = 3

// ErrPlainStringUnresolved classifies a write carrying a plain-string value
// where a document was required. The remedy is to resolve it under a writer
// identity — the value cannot be applied without one — never to resend the same
// bytes.
var ErrPlainStringUnresolved = errors.New("text: unresolved plain-string value")

// unresolved tags an error as ErrPlainStringUnresolved while reporting the
// underlying message verbatim, exactly as stale does for ErrStateStale.
type unresolved struct{ error }

func (e unresolved) Unwrap() error { return e.error }

func (e unresolved) Is(target error) bool { return target == ErrPlainStringUnresolved }

// IsPlainString reports whether b is a plain-string value. Empty input is not
// one: an absent bytes field is a document nobody wrote yet.
func IsPlainString(b []byte) bool {
	return len(b) > 0 && b[0] == plainStringVersion
}

// EncodePlainString builds the plain-string value reading as s. It errors on
// invalid UTF-8, the same text the write entry points refuse (see
// InvalidUTF8Offset), so a value that could never be applied cannot be built.
//
// The empty string encodes to a one-byte value rather than to nothing, so
// "reads as empty" stays distinct from an absent field. Store no value at all
// to make the two indistinguishable.
func EncodePlainString(s string) ([]byte, error) {
	if err := requireWellFormed("plain string", s); err != nil {
		return nil, err
	}
	return append([]byte{plainStringVersion}, s...), nil
}

// PlainString returns the text a plain-string value reads as. It errors when
// data is not one — a document included, which PlainText reads instead — and on
// invalid UTF-8.
func PlainString(data []byte) (string, error) {
	if !IsPlainString(data) {
		return "", fmt.Errorf("text: not a plain-string value")
	}
	s := string(data[1:])
	if err := requireWellFormed("plain string", s); err != nil {
		return "", err
	}
	return s, nil
}
