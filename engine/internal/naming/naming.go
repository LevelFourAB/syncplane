// Package naming holds the naming rules of the sync engine: what an entity may
// be called, which fields may form a row key, how their values render into one,
// and what the resulting key may contain.
//
// The rules are a wire contract, not an implementation detail. Entity names and
// row keys travel in CrudMutation, address rows in storage, and are pasted into
// the client's KV key layout (`c/<entity>/<ws>/<key>` and friends) and its
// cross-tab change tokens. Both implementations therefore have to agree
// character for character: the TS mirror is client/src/naming.ts, and
// docs/reference/naming.md states the rules once for both. A change here is a
// change to all three.
//
// Validation runs where a mistake is still cheap to explain — registration for
// names and key fields, mutate time for row keys — because the failure modes it
// prevents are otherwise silent. An unset key field derives the empty key,
// which is the entity prefix itself; a slash in an entity name forges an extra
// key segment, so entity `a` row `b/x` and entity `a/b` row `x` land on one KV
// key; a control character collides with the change-token separator. None of
// these look like an error at the point they are made.
package naming

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	// MaxEntityNameLength caps an entity name. Names are hand-written schema
	// identifiers, so the cap only bounds what ends up repeated in every key.
	MaxEntityNameLength = 64
	// MaxRowKeyBytes caps a row key in UTF-8 bytes, escaping included. Keys are
	// stored, scanned and prefix-matched on both sides; an unbounded key is an
	// unbounded index entry. It is deliberately a constant rather than a server
	// limit: the client derives keys locally and must be able to reject one
	// without asking, which only works while both sides hold the same number.
	MaxRowKeyBytes = 512
)

// ValidateEntityName checks an entity's wire name — (syncplane.v1.entity).name,
// and CrudMutation.entity. It must be a non-empty identifier: an ASCII letter
// or underscore followed by letters, digits or underscores, at most
// MaxEntityNameLength of them.
//
// The shape is narrow on purpose. An entity name is a key segment on the client
// (so no `/`), a token in its change channel (so no separator characters), a
// property name on the generated db facade (`db.todo`), and — for schemas
// declared at runtime — is lowered to a proto message name by the TS schema
// builder. An identifier is the intersection of all four.
func ValidateEntityName(name string) error {
	if name == "" {
		return fmt.Errorf("entity name is empty")
	}
	if len(name) > MaxEntityNameLength {
		return fmt.Errorf("entity name %q is %d bytes, the limit is %d", name, len(name), MaxEntityNameLength)
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c == '_':
		case (c >= '0' && c <= '9') && i > 0:
		default:
			return fmt.Errorf(
				"entity name %q is not an identifier: use letters, digits and underscores, starting with a letter or underscore",
				name)
		}
	}
	return nil
}

// ValidateKeyFieldKind checks that a field may form (part of) a row key. Key
// fields must be singular strings, integers, booleans or enums — the kinds
// whose values ScalarKeyString renders identically in Go and JS.
//
// Everything else is refused rather than rendered on a best effort. A message,
// list or map has no canonical text form at all. Bytes and floats have one per
// language: protoreflect stringifies bytes as their raw octets while the client
// has a Uint8Array, and Go's %v and JS's String() disagree on float exponents
// (1e7 renders "1e+07" against "10000000"). A key that means one row on the
// server and another on the client is worse than a schema that will not
// register.
func ValidateKeyFieldKind(fd protoreflect.FieldDescriptor) error {
	if fd.IsMap() {
		return fmt.Errorf("key field %q must be singular, is a map", fd.Name())
	}
	if fd.IsList() {
		return fmt.Errorf("key field %q must be singular, is repeated", fd.Name())
	}
	switch fd.Kind() {
	case protoreflect.StringKind, protoreflect.BoolKind, protoreflect.EnumKind,
		protoreflect.Int32Kind, protoreflect.Int64Kind,
		protoreflect.Sint32Kind, protoreflect.Sint64Kind,
		protoreflect.Sfixed32Kind, protoreflect.Sfixed64Kind,
		protoreflect.Uint32Kind, protoreflect.Uint64Kind,
		protoreflect.Fixed32Kind, protoreflect.Fixed64Kind:
		return nil
	default:
		return fmt.Errorf(
			"key field %q must be a string, integer, bool or enum, is %s",
			fd.Name(), fd.Kind())
	}
}

// ValidateRowKey checks a derived or client-supplied row key. It must be
// non-empty, valid UTF-8, free of ASCII control characters, and at most
// MaxRowKeyBytes long.
//
// Empty is the one that bites in practice: it is what a write whose key field
// was never set derives, and it addresses a KV key identical to the entity's
// own prefix. Control characters are refused because they separate fields in
// the client's change tokens (U+001F) and turn logs and debug output into
// something that cannot be read back; a key is an identifier, so nothing is
// lost by keeping them out.
func ValidateRowKey(key string) error {
	if key == "" {
		return fmt.Errorf("row key is empty: every key field must be set")
	}
	if len(key) > MaxRowKeyBytes {
		return fmt.Errorf("row key is %d bytes, the limit is %d", len(key), MaxRowKeyBytes)
	}
	if !utf8.ValidString(key) {
		return fmt.Errorf("row key is not valid UTF-8")
	}
	for _, r := range key {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf(`row key contains the control character \u%04x`, r)
		}
	}
	return nil
}

// ScalarKeyString canonicalizes one key-field value as text: strings verbatim,
// integers base-10, bools "true"/"false", enums by number. It is the exact
// rendering the client performs, and the reason ValidateKeyFieldKind admits
// only these kinds.
//
// It also renders the identity of a keyed list's element (see
// engine/internal/lww), which is the same question asked of a nested field.
func ScalarKeyString(fd protoreflect.FieldDescriptor, v protoreflect.Value) string {
	switch fd.Kind() {
	case protoreflect.StringKind:
		return v.String()
	case protoreflect.BoolKind:
		if v.Bool() {
			return "true"
		}
		return "false"
	case protoreflect.Int32Kind, protoreflect.Int64Kind,
		protoreflect.Sint32Kind, protoreflect.Sint64Kind,
		protoreflect.Sfixed32Kind, protoreflect.Sfixed64Kind:
		return strconv.FormatInt(v.Int(), 10)
	case protoreflect.Uint32Kind, protoreflect.Uint64Kind,
		protoreflect.Fixed32Kind, protoreflect.Fixed64Kind:
		return strconv.FormatUint(v.Uint(), 10)
	case protoreflect.EnumKind:
		return strconv.FormatInt(int64(v.Enum()), 10)
	default:
		// Unreachable for a registered key field (ValidateKeyFieldKind refuses
		// the remaining kinds); kept total for mask segments, which reach here
		// for any scalar element type.
		return v.String()
	}
}

// RowKey joins the rendered key-field values of one row into its storage key. A
// single field is used verbatim — the common case, where the key reads as the
// id it is. Composite keys escape `\` and `/` in each part before joining with
// `/`, so distinct tuples like ("a/b", "c") and ("a", "b/c") cannot collide
// into one key.
func RowKey(parts []string) string {
	if len(parts) == 1 {
		return parts[0]
	}
	escaped := make([]string, len(parts))
	for i, p := range parts {
		escaped[i] = escapeKeyPart(p)
	}
	return strings.Join(escaped, "/")
}

// escapeKeyPart escapes the composite-key separator and the escape character
// itself.
func escapeKeyPart(s string) string {
	if !strings.ContainsAny(s, `\/`) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 4)
	for _, r := range s {
		if r == '\\' || r == '/' {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}
