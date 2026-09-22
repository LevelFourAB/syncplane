// Package naming holds the naming rules of the sync engine: what an entity may
// be called, what a workspace may be identified by, which fields may form a row
// key, how their values render into one, and what the resulting key may
// contain.
//
// The rules are a wire contract, not an implementation detail. Entity names and
// row keys travel in CrudMutation, workspace ids scope every RPC, all three
// address rows in storage, and all three are pasted into the client's KV key
// layout (`c/<entity>/<ws>/<key>` and friends) and its cross-tab change tokens.
// Both implementations therefore have to agree character for character: the TS
// mirror is client/src/naming.ts, and docs/reference/naming.md states the rules
// once for both. A change here is a change to all three.
//
// Validation runs where a mistake is still cheap to explain — registration for
// names and key fields, mutate time for row keys, the RPC boundary for
// workspace ids — because the failure modes it prevents are otherwise silent.
// An unset key field derives the empty key,
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
	// MaxWorkspaceIDBytes caps a workspace id. The id is the partition key, so
	// it repeats in every KV key segment, every storage row and every
	// notification payload. 128 holds a UUID or a namespaced id like
	// `org:acme:team:eng` with room to spare, while staying far below the
	// 8000-byte ceiling on a Postgres NOTIFY payload — past which pgnotify drops
	// the notification instead of waking the workspace's clients.
	MaxWorkspaceIDBytes = 128
	// MaxAppIDBytes caps an application id. A multi-app host prefixes every
	// workspace id with the app's (see server.Manager), so the two budgets add
	// up wherever a workspace id is stored.
	MaxAppIDBytes = 64
	// MaxActorSubjectBytes caps the subject recorded as a write's actor. The
	// subject is stored on every row and repeated in every field-history record,
	// so it is a per-row cost. 256 holds a UUID, an email address or a
	// namespaced service-account name.
	MaxActorSubjectBytes = 256
	// MaxActorSystemBytes caps the label a write made under the engine's trusted
	// bypass gives itself. It is written by the operator's own code and read by
	// humans and echo-suppression checks, so it is short by design.
	MaxActorSystemBytes = 64
	// MaxRoomNameBytes caps the name half of a room topic. The name rides in
	// every presence event on the bus, alongside the workspace id, the anchor
	// key, the publisher's subject and the app's own state — and a bus over
	// Postgres LISTEN/NOTIFY has to fit all of that in one 7900-byte payload,
	// with the state expanded by base64. 128 leaves that budget to the state,
	// which is the part an app cannot shorten.
	MaxRoomNameBytes = 128
	// MaxSessionIDBytes caps the id a presence session identifies itself by.
	// The id keys every record on the ephemeral plane, is echoed to every
	// watcher of a room the session is in, and rides the presence bus with the
	// room name and the app's state. 128 holds a UUID with room to spare.
	MaxSessionIDBytes = 128
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

// ValidateWorkspaceID checks the workspace id every workspace-scoped RPC
// carries. It must start with an ASCII letter or digit and continue with
// letters, digits, `_`, `-` or `:`, at most MaxWorkspaceIDBytes of them.
//
// The set is an allowlist rather than a ban on the characters known to hurt,
// because the id is pasted into places that later parse it back out and each
// exclusion answers one of them. U+001F separates the fields of the client's
// cross-tab change tokens and the workspace is the FIRST field, so an id
// holding one shifts every field after it and the notification wakes the wrong
// watchers (client/src/keys.ts parseTouchKey). `*` is the sentinel the client's
// watch layer uses for "every workspace", so a workspace named `*` shares a
// scan with the cross-workspace watchers of the same entity and one of the two
// is served the other's rows (client/src/watch.ts scanScope). `/` forges an
// extra segment in the `c/<entity>/<ws>/<key>` layout. `.` would be harmless
// there, but reaches the admin UI unescaped by url.PathEscape, where an id of
// `..` reads as a path traversal.
//
// Restricting the first character keeps an id from being pure punctuation: `-x`
// reads as a flag in a shell or a log line, and `.`/`..` cannot be spelled at
// all. Comparison is bytewise, with no case folding and no normalization, so
// `Acme` and `acme` are two workspaces — the same rule row keys follow.
//
// Widening this set later is safe; narrowing it is not, because the id is the
// partition key and stored data cannot be renamed into a new rule. An app whose
// ids do not fit — an email address, a domain — slugifies or hashes them into
// one.
//
// The in-process API (server.Mutate and the connect/effects paths that reach
// it) does not run this: those ids come from application code, at the same
// trust level as the schema it registers.
func ValidateWorkspaceID(id string) error {
	if id == "" {
		return fmt.Errorf("workspace id is empty")
	}
	if len(id) > MaxWorkspaceIDBytes {
		return fmt.Errorf("workspace id %q is %d bytes, the limit is %d", id, len(id), MaxWorkspaceIDBytes)
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case (c == '_' || c == '-' || c == ':') && i > 0:
		default:
			return fmt.Errorf(
				"workspace id %q is not valid: use letters, digits, `_`, `-` and `:`, starting with a letter or digit",
				id)
		}
	}
	return nil
}

// ValidateAppID checks an application id, which a multi-app host both routes
// by and prefixes workspace ids with. It takes the workspace-id alphabet, and
// for the same reasons — a shorter id, so that the pair still fits everywhere
// one workspace id used to.
//
// `/` above all: the host joins an app id and a workspace id with it, so an id
// holding one would let two applications land on the same stored workspace.
func ValidateAppID(id string) error {
	if id == "" {
		return fmt.Errorf("app id is empty")
	}
	if len(id) > MaxAppIDBytes {
		return fmt.Errorf("app id %q is %d bytes, the limit is %d", id, len(id), MaxAppIDBytes)
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case (c == '_' || c == '-' || c == ':') && i > 0:
		default:
			return fmt.Errorf(
				"app id %q is not valid: use letters, digits, `_`, `-` and `:`, starting with a letter or digit",
				id)
		}
	}
	return nil
}

// CutWorkspacePrefix removes a partition prefix from a workspace id and
// reports whether the id belongs to that partition. An id that does not begin
// with prefix, or that is the prefix alone, belongs to none and reports false.
// An empty prefix owns every id and returns it unchanged.
func CutWorkspacePrefix(id, prefix string) (string, bool) {
	if prefix == "" {
		return id, true
	}
	ws, found := strings.CutPrefix(id, prefix)
	return ws, found && ws != ""
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

// ValidateActorSubject checks the subject recorded as a write's actor. It must
// be valid UTF-8, free of ASCII control characters, and at most
// MaxActorSubjectBytes long. Empty is valid and means the write had no
// authenticated caller.
//
// The control-character rule is the one row keys follow, for the same reasons:
// U+001F separates the fields of the client's change tokens, and an actor rides
// the change feed into logs and rendered history, where a control character
// turns the output into something that cannot be read back.
func ValidateActorSubject(subject string) error {
	return validatePrintableField("actor subject", subject, MaxActorSubjectBytes)
}

// ValidateActorSystem checks the label a write made under the engine's trusted
// bypass gives itself. It must be valid UTF-8, free of ASCII control
// characters, and at most MaxActorSystemBytes long. Empty is valid and means a
// caller, not the bypass, made the write.
func ValidateActorSystem(system string) error {
	return validatePrintableField("actor system label", system, MaxActorSystemBytes)
}

// ValidateRoomName checks the name half of a room topic. It must be non-empty,
// valid UTF-8, free of ASCII control characters, and at most MaxRoomNameBytes
// long.
//
// Rooms are addressed by whatever string an app picks, so the rule is a
// character rule rather than an identifier rule: `doc:42/cursors` and
// `Board — Sprint 14` are both fine, and no registration declares a room ahead
// of time. Control characters are refused for the reason row keys refuse them —
// the name reaches logs, and the client's cross-tab room protocol separates
// fields with them — and the cap is what keeps one topic from consuming a bus
// payload the app's own state has to share.
//
// Comparison is bytewise, with no case folding and no normalization, so `Notes`
// and `notes` are two rooms.
func ValidateRoomName(name string) error {
	if name == "" {
		return fmt.Errorf("room name is empty")
	}
	return validatePrintableField("room name", name, MaxRoomNameBytes)
}

// ValidateSessionID checks the id a presence session identifies itself by. It
// must be non-empty, valid UTF-8, free of ASCII control characters, and at most
// MaxSessionIDBytes long.
//
// It is a character rule rather than an identifier rule because clients mint it
// however they like — a UUID, a device handle and a slot number, a hash. What
// it may not do is carry a control character, because the id is echoed to every
// watcher of every room the session is in and separates fields in the client's
// cross-tab room protocol.
//
// A session id is NOT an identity: it is self-asserted, like a client id, and
// the subject a participant is shown under is stamped by the server from the
// request principal instead.
func ValidateSessionID(id string) error {
	if id == "" {
		return fmt.Errorf("session id is empty")
	}
	return validatePrintableField("session id", id, MaxSessionIDBytes)
}

// validatePrintableField holds the rule the free-form names share — valid
// UTF-8, no ASCII control characters, a byte cap — because each of them ends up
// in a log line or beside a control character that separates fields. What
// differs is the cap and the noun in the message.
func validatePrintableField(what, s string, maxBytes int) error {
	if len(s) > maxBytes {
		return fmt.Errorf("%s is %d bytes, the limit is %d", what, len(s), maxBytes)
	}
	if !utf8.ValidString(s) {
		return fmt.Errorf("%s is not valid UTF-8", what)
	}
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf(`%s contains the control character \u%04x`, what, r)
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

// RowKeyPartLen reports how many bytes one part of a composite key occupies
// inside the joined key, the escaping RowKey applies included and the `/` that
// joins it excluded. It answers len(s) for the parts that carry neither `\` nor
// `/`, which is nearly all of them.
//
// It exists for callers deriving a key that has to fit MaxRowKeyBytes and needs
// to know how much of a part will: measuring the joined key instead only says
// whether an already-built key fits.
func RowKeyPartLen(s string) int {
	return len(s) + strings.Count(s, `\`) + strings.Count(s, "/")
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
