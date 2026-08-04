// Package lww merges PATCH mutations into row snapshots using field masks.
//
// Because the server linearizes mutations per workspace, applying masks in
// sequence order IS per-field last-writer-wins — no per-field timestamps
// exist anywhere. Fields marked opaque (syncplane.v1.field_behavior) are always
// replaced whole, never descended into; this is the escape hatch for
// embedding CRDT blobs later.
//
// A oneof is ONE field here, named in a mask by the group rather than by a
// member, and replaced whole (no sub-paths, even into a message member). The
// group has to be the unit: protobuf lets only one member hold a value, so a
// member-granular mask can say "a is set" but cannot say what "a is unset"
// means for its siblings, and the two answers ("b survives" / "the group is
// empty") are both defensible. Naming the group asks the question the data can
// answer. The cost is that a message inside a oneof merges atomically, which is
// also why nothing below a oneof may be keyed, text or a reference (registration
// refuses those — see engine/server).
package lww

import (
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/naming"
	"github.com/levelfourab/syncplane/internal/protoopts"
)

// MergeFunc combines the stored serialization of an opaque field with an
// incoming one (CRDT-style). Both slices are the field's raw value: the
// bytes content for bytes fields, the marshaled message for message fields.
type MergeFunc func(base, incoming []byte) ([]byte, error)

// Merger is one field's merge hook plus the gate it runs under.
type Merger struct {
	Fn MergeFunc
	// MergeAbsentBase runs Fn even when the stored row does not carry the
	// field, passing an empty base. Only safe for a merger whose empty state is
	// a real value rather than "no value" — the convergent-text merger, where
	// an empty document is the identity of the union, and where the incoming
	// side may be a delta-state fragment that has to be decoded rather than
	// stored raw. App-registered mergers keep the both-sides gate, so a
	// one-sided PATCH there stays plain LWW replace.
	MergeAbsentBase bool
}

// Mergers maps dotted opaque-field paths (e.g. "doc", "outer.doc") to merge
// hooks. A nil map replaces opaque fields whole (the default).
type Mergers map[string]Merger

// Merge copies the fields named by mask paths from src onto dst. Both must
// be the same message type. Paths use FieldMask semantics ("settings.color"),
// plus this package's extensions: keyed collection items ("tags.red") and
// oneof groups ("choice", never "choice_text"). A path that names a missing
// field is an error (the client sent a mask for a schema the server does not
// know).
func Merge(dst, src proto.Message, paths []string) error {
	return MergeWithMergers(dst, src, paths, nil)
}

// MergeWithMergers is Merge with per-field hooks: when a mask path lands on
// (or descends into) an opaque field that has a registered merger AND both
// sides carry a value, the merger combines them instead of replacing.
// One-sided cases keep plain LWW semantics: src-only replaces, dst-only is
// cleared by the zero-valued src — unless the merger sets MergeAbsentBase, in
// which case an src-only value still runs through it against an empty base.
// A dst-only value is always a clear: the mask says the writer means the field
// to be empty, and no merger can undo that.
func MergeWithMergers(dst, src proto.Message, paths []string, m Mergers) error {
	if dst.ProtoReflect().Descriptor().FullName() != src.ProtoReflect().Descriptor().FullName() {
		return fmt.Errorf("lww: type mismatch: %s vs %s",
			dst.ProtoReflect().Descriptor().FullName(), src.ProtoReflect().Descriptor().FullName())
	}
	for _, path := range paths {
		if err := mergePath(dst.ProtoReflect(), src.ProtoReflect(), path, "", m); err != nil {
			return fmt.Errorf("lww: path %q: %w", path, err)
		}
	}
	return nil
}

func mergePath(dst, src protoreflect.Message, path, prefix string, m Mergers) error {
	seg, rest := splitSeg(path)
	desc := dst.Descriptor()
	fd := desc.Fields().ByName(protoreflect.Name(seg))
	if fd == nil {
		// A oneof is one field for merge purposes, named by the GROUP: a mask
		// segment that resolves to no field may still be a oneof name.
		if od := desc.Oneofs().ByName(protoreflect.Name(seg)); od != nil && !od.IsSynthetic() {
			if rest != "" {
				return fmt.Errorf("oneof %q does not support sub-paths", seg)
			}
			copyOneof(dst, src, od)
			return nil
		}
		return fmt.Errorf("unknown field %q", seg)
	}
	if od := OneofOf(fd); od != nil {
		return fmt.Errorf("field %q is a member of oneof %q; mask the oneof instead", seg, od.Name())
	}

	opaque := isOpaque(fd)
	// Whole-replace cases: a bare path, or an opaque field (the escape hatch —
	// replaced/merged whole even when the mask names a sub-path into it).
	if rest == "" || opaque {
		mg, hasMerger := m[prefix+seg]
		if hasMerger && opaque && src.Has(fd) && (dst.Has(fd) || mg.MergeAbsentBase) {
			return applyMerger(dst, src, fd, mg.Fn)
		}
		copyField(dst, src, fd)
		return nil
	}

	// rest != "" and non-opaque: a collection merges per item, a singular
	// message descends, a scalar cannot be sub-pathed.
	if fd.IsMap() || fd.IsList() {
		return mergeItem(dst, src, fd, seg, rest, prefix, m)
	}
	if fd.Kind() != protoreflect.MessageKind {
		return fmt.Errorf("field %q does not support sub-paths", seg)
	}

	// Descend into a singular message field. An absent src submessage means the
	// writer cleared the subtree the mask points into, so the same path is
	// cleared on dst; the zero message stands in as the source either way.
	if !src.Has(fd) && !dst.Has(fd) {
		return nil
	}
	sub := dst.Mutable(fd).Message()
	if err := mergePath(sub, src.Get(fd).Message(), rest, prefix+seg+".", m); err != nil {
		return err
	}
	// A leaf write never moves its container's presence on its own: a container
	// left empty by the descent is cleared, whether it was already there or was
	// materialized by Mutable above. Presence is observable (isSet/isNotSet) and
	// can move a row in or out of a subscription predicate, so a merge that
	// touched nothing must leave it exactly as it found it. The trade-off is
	// that a deliberately present-but-empty message does not survive a masked
	// merge into it — see docs/explanation/convergence-model.md.
	if isEmptyMessage(sub) {
		dst.Clear(fd)
	}
	return nil
}

// isEmptyMessage reports whether a message carries no populated field and no
// unknown bytes. Unknown fields count as content: a row written by a newer
// schema must not lose a submessage because this binary cannot see inside it.
func isEmptyMessage(m protoreflect.Message) bool {
	empty := true
	m.Range(func(protoreflect.FieldDescriptor, protoreflect.Value) bool {
		empty = false
		return false
	})
	return empty && len(m.GetUnknown()) == 0
}

// mergeItem merges a single addressed item of a map or list field. afterField is
// the mask remainder after the field name: "<key>" for a whole-item write/clear,
// or "<key>.<deeper>" to descend into a message item. The leading "<key>" is the
// item identity — a (possibly escaped) map key, encoded message-list key, or
// scalar element value.
func mergeItem(
	dst, src protoreflect.Message,
	fd protoreflect.FieldDescriptor,
	fieldSeg, afterField, prefix string,
	m Mergers,
) error {
	itemKey, rest := splitSeg(afterField)
	switch {
	case fd.IsMap():
		return mergeMapItem(dst, src, fd, itemKey, rest, prefix+fieldSeg+".", m)
	case fd.Kind() == protoreflect.MessageKind:
		keyFields := keyFieldsFor(fd)
		if len(keyFields) == 0 {
			// A message list with no determinable identity cannot merge per item;
			// fall back to whole-replace (expansion never emits such a sub-path).
			copyField(dst, src, fd)
			return nil
		}
		return mergeKeyedListItem(dst, src, fd, keyFields, itemKey, rest, prefix+fieldSeg+".", m)
	default:
		return mergeScalarSetItem(dst, src, fd, itemKey, rest)
	}
}

// mergeMapItem applies one keyed map entry: src has the key => set/recurse,
// src lacks it => clear it on dst (clear-on-absent, mirroring the singular case).
func mergeMapItem(
	dst, src protoreflect.Message,
	fd protoreflect.FieldDescriptor,
	itemKey, rest, prefix string,
	m Mergers,
) error {
	mk, err := decodeMapKey(fd, itemKey)
	if err != nil {
		return err
	}
	sm := src.Get(fd).Map()
	if sm.Has(mk) {
		if rest == "" {
			dst.Mutable(fd).Map().Set(mk, deepCopyValue(fd.MapValue(), sm.Get(mk)))
			return nil
		}
		if fd.MapValue().Kind() != protoreflect.MessageKind {
			return fmt.Errorf("map %q value does not support sub-paths", fd.Name())
		}
		dstVal := dst.Mutable(fd).Map().Mutable(mk).Message()
		return mergePath(dstVal, sm.Get(mk).Message(), rest, prefix, m)
	}
	if dst.Has(fd) {
		dst.Mutable(fd).Map().Clear(mk)
	}
	return nil
}

// mergeKeyedListItem applies one addressed element of a keyed message list,
// matching by encoded identity. Existing dst elements keep their index.
func mergeKeyedListItem(
	dst, src protoreflect.Message,
	fd protoreflect.FieldDescriptor,
	keyFields []protoreflect.FieldDescriptor,
	itemKey, rest, prefix string,
	m Mergers,
) error {
	sl := src.Get(fd).List()
	srcIdx := -1
	for i := 0; i < sl.Len(); i++ {
		if encodeItemKey(sl.Get(i).Message(), keyFields) == itemKey {
			srcIdx = i
			break
		}
	}
	dl := dst.Mutable(fd).List()
	dstIdx := -1
	for i := 0; i < dl.Len(); i++ {
		if encodeItemKey(dl.Get(i).Message(), keyFields) == itemKey {
			dstIdx = i
			break
		}
	}

	if srcIdx == -1 {
		// src dropped the element: remove it from dst (clear-on-absent).
		if dstIdx != -1 {
			listRemoveAt(dl, dstIdx)
		}
		return nil
	}
	srcElem := sl.Get(srcIdx).Message()

	if rest == "" {
		cp := protoreflect.ValueOfMessage(proto.Clone(srcElem.Interface()).ProtoReflect())
		if dstIdx != -1 {
			dl.Set(dstIdx, cp)
		} else {
			dl.Append(cp)
		}
		return nil
	}

	// Descend into the element message, creating + seeding the key fields when
	// dst lacks it.
	var dstElem protoreflect.Message
	if dstIdx != -1 {
		dstElem = dl.Get(dstIdx).Message()
	} else {
		dstElem = dl.NewElement().Message()
		for _, kf := range keyFields {
			dstElem.Set(kf, srcElem.Get(kf))
		}
		dl.Append(protoreflect.ValueOfMessage(dstElem))
	}
	return mergePath(dstElem, srcElem, rest, prefix, m)
}

// mergeScalarSetItem applies one element of a scalar set list (identity is the
// value): src has it => ensure present once, src lacks it => remove all copies.
func mergeScalarSetItem(
	dst, src protoreflect.Message,
	fd protoreflect.FieldDescriptor,
	itemKey, rest string,
) error {
	if rest != "" {
		return fmt.Errorf("field %q scalar element does not support sub-paths", fd.Name())
	}
	sl := src.Get(fd).List()
	var srcVal protoreflect.Value
	found := false
	for i := 0; i < sl.Len(); i++ {
		if encodeScalarElem(fd, sl.Get(i)) == itemKey {
			srcVal = sl.Get(i)
			found = true
			break
		}
	}
	dl := dst.Mutable(fd).List()
	if found {
		for i := 0; i < dl.Len(); i++ {
			if encodeScalarElem(fd, dl.Get(i)) == itemKey {
				return nil // already present
			}
		}
		dl.Append(srcVal)
		return nil
	}
	// Remove every occurrence of the value from dst.
	kept := make([]protoreflect.Value, 0, dl.Len())
	for i := 0; i < dl.Len(); i++ {
		if encodeScalarElem(fd, dl.Get(i)) != itemKey {
			kept = append(kept, dl.Get(i))
		}
	}
	dl.Truncate(0)
	for _, v := range kept {
		dl.Append(v)
	}
	return nil
}

// listRemoveAt removes element idx from a mutable list, shifting the tail down.
func listRemoveAt(l protoreflect.List, idx int) {
	n := l.Len()
	for i := idx; i < n-1; i++ {
		l.Set(i, l.Get(i+1))
	}
	l.Truncate(n - 1)
}

// SplitPath splits a mask path into the segments mergePath would walk. Segments
// keep their escape sequences intact, exactly as the merge sees them: a field
// name never carries one, and an item key is decoded by whoever consumes it.
//
// Exported for callers that need to reason about where a mask POINTS without
// applying it — index maintenance skips a row rewrite when no mask path can
// reach an index component (server.entityInfo.maskTouchesIndexes), and that
// comparison is only sound if it segments a path the same way the merge does.
func SplitPath(path string) []string {
	segs := make([]string, 0, 4)
	for {
		seg, rest := splitSeg(path)
		segs = append(segs, seg)
		if rest == "" {
			return segs
		}
		path = rest
	}
}

// splitSeg splits path at the first unescaped '.'. The returned segment keeps
// its escape sequences intact (decode separately); rest is everything after the
// dot. A backslash escapes the following byte. Mirrors the TS splitter.
func splitSeg(path string) (seg, rest string) {
	for i := 0; i < len(path); i++ {
		switch path[i] {
		case '\\':
			i++ // skip the escaped byte
		case '.':
			return path[:i], path[i+1:]
		}
	}
	return path, ""
}

// keyFieldsFor resolves the identity fields of a keyed message list: the explicit
// key_field behavior, else the element entity's key_fields. Returns nil for a
// non-message list or when no identity can be determined.
func keyFieldsFor(fd protoreflect.FieldDescriptor) []protoreflect.FieldDescriptor {
	elem := fd.Message()
	if elem == nil {
		return nil
	}
	var names []string
	if b := fieldBehavior(fd); b != nil && len(b.GetKeyField()) > 0 {
		names = b.GetKeyField()
	} else if eo := entityOptions(elem); eo != nil {
		names = eo.GetKeyFields()
	}
	if len(names) == 0 {
		return nil
	}
	fields := make([]protoreflect.FieldDescriptor, 0, len(names))
	for _, n := range names {
		f := elem.Fields().ByName(protoreflect.Name(n))
		// An identity that is only present while its case is active is not an
		// identity: elements would collide on the zero value the moment a
		// sibling won. Treated as "no identity", so the list replaces whole
		// rather than merging elements onto each other. Registration reports it.
		if f == nil || OneofOf(f) != nil {
			return nil
		}
		fields = append(fields, f)
	}
	return fields
}

// RefKeyFields returns the key fields of the entity targeted by a reference
// field — a singular or repeated message field whose (element) message carries
// (syncplane.v1.entity) — resolved against that message's own descriptor. Returns
// nil when fd is not a reference (non-message, map, or a message that is not a
// registered entity). This is the single detection rule for a typed reference:
// no annotation, the target message simply being an entity makes the field a
// ref.
func RefKeyFields(fd protoreflect.FieldDescriptor) []protoreflect.FieldDescriptor {
	if fd.Kind() != protoreflect.MessageKind || fd.IsMap() {
		return nil
	}
	elem := fd.Message()
	eo := entityOptions(elem)
	if eo == nil || len(eo.GetKeyFields()) == 0 {
		return nil
	}
	fields := make([]protoreflect.FieldDescriptor, 0, len(eo.GetKeyFields()))
	for _, n := range eo.GetKeyFields() {
		f := elem.Fields().ByName(protoreflect.Name(n))
		// A target whose key is inside a oneof cannot be registered at all
		// (see engine/server), and a stub projected down to a field that is only
		// sometimes there would not identify a row. Not a ref, then.
		if f == nil || OneofOf(f) != nil {
			return nil
		}
		fields = append(fields, f)
	}
	return fields
}

// IsRef reports whether fd is a first-class typed reference (see RefKeyFields).
func IsRef(fd protoreflect.FieldDescriptor) bool {
	return RefKeyFields(fd) != nil
}

// encodeItemKey renders a message-list element's identity as a mask segment. A
// single key field escapes "\" and "." (matching string map keys); a composite
// key escapes "\", ".", and "/" per part and joins them with "/".
func encodeItemKey(msg protoreflect.Message, keyFields []protoreflect.FieldDescriptor) string {
	if len(keyFields) == 1 {
		return escapeKeySegment(scalarKeyString(keyFields[0], msg.Get(keyFields[0])), false)
	}
	parts := make([]string, len(keyFields))
	for i, kf := range keyFields {
		parts[i] = escapeKeySegment(scalarKeyString(kf, msg.Get(kf)), true)
	}
	return strings.Join(parts, "/")
}

// encodeScalarElem renders a scalar list element's value as a mask segment.
func encodeScalarElem(fd protoreflect.FieldDescriptor, v protoreflect.Value) string {
	return escapeKeySegment(scalarKeyString(fd, v), false)
}

// scalarKeyString canonicalizes a scalar value used as an identity: strings
// verbatim, integers base-10, bool true/false, enums by number. An item's
// identity inside a row and a row's own key ask the same question, so the
// rendering is the shared one (engine/internal/naming) rather than a second
// copy that could drift from what the client encodes.
func scalarKeyString(fd protoreflect.FieldDescriptor, v protoreflect.Value) string {
	return naming.ScalarKeyString(fd, v)
}

// escapeKeySegment escapes "\" and "." (and "/" when alsoSlash) so an encoded
// key survives the dot-splitter (and the composite "/" joiner).
func escapeKeySegment(s string, alsoSlash bool) string {
	var b strings.Builder
	for _, r := range s {
		if r == '\\' || r == '.' || (alsoSlash && r == '/') {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

// UnescapeSegment reverses the escaping SplitPath leaves intact: a backslash
// before any byte protects it, so `a\.b` is one segment holding "a.b".
//
// Exported for the query builders, which take a whole dotted path as one string
// and therefore have to undo the escaping the wire form does not need — a wire
// path is already a list of segments (query.Path).
func UnescapeSegment(s string) string { return decodeStringKey(s) }

// DecodeMapKey decodes an ESCAPED mask segment into a typed map key per the
// field's key kind. Exported for the query package, which addresses a map entry
// by key and must parse the non-string kinds exactly as the merge does — two
// spellings of the same int64 key would otherwise reach two different entries.
func DecodeMapKey(fd protoreflect.FieldDescriptor, seg string) (protoreflect.MapKey, error) {
	return decodeMapKey(fd, seg)
}

// decodeStringKey reverses escapeKeySegment for a single-part string key.
func decodeStringKey(s string) string {
	var b strings.Builder
	esc := false
	for _, r := range s {
		if esc {
			b.WriteRune(r)
			esc = false
			continue
		}
		if r == '\\' {
			esc = true
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// decodeMapKey decodes a mask segment into a typed map key per the field's key
// kind (string keys are unescaped; integral/bool keys parse canonical forms).
func decodeMapKey(fd protoreflect.FieldDescriptor, seg string) (protoreflect.MapKey, error) {
	kfd := fd.MapKey()
	switch kfd.Kind() {
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(decodeStringKey(seg)).MapKey(), nil
	case protoreflect.BoolKind:
		switch seg {
		case "true":
			return protoreflect.ValueOfBool(true).MapKey(), nil
		case "false":
			return protoreflect.ValueOfBool(false).MapKey(), nil
		}
		return protoreflect.MapKey{}, fmt.Errorf("invalid bool map key %q", seg)
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		n, err := strconv.ParseInt(seg, 10, 32)
		if err != nil {
			return protoreflect.MapKey{}, fmt.Errorf("invalid int32 map key %q: %w", seg, err)
		}
		return protoreflect.ValueOfInt32(int32(n)).MapKey(), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		n, err := strconv.ParseInt(seg, 10, 64)
		if err != nil {
			return protoreflect.MapKey{}, fmt.Errorf("invalid int64 map key %q: %w", seg, err)
		}
		return protoreflect.ValueOfInt64(n).MapKey(), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		n, err := strconv.ParseUint(seg, 10, 32)
		if err != nil {
			return protoreflect.MapKey{}, fmt.Errorf("invalid uint32 map key %q: %w", seg, err)
		}
		return protoreflect.ValueOfUint32(uint32(n)).MapKey(), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		n, err := strconv.ParseUint(seg, 10, 64)
		if err != nil {
			return protoreflect.MapKey{}, fmt.Errorf("invalid uint64 map key %q: %w", seg, err)
		}
		return protoreflect.ValueOfUint64(n).MapKey(), nil
	default:
		return protoreflect.MapKey{}, fmt.Errorf("unsupported map key kind %v", kfd.Kind())
	}
}

// applyMerger combines the two sides of an opaque field at the bytes level
// and stores the result on dst. Only bytes and singular message kinds are
// supported (enforced at registration).
func applyMerger(dst, src protoreflect.Message, fd protoreflect.FieldDescriptor, fn MergeFunc) error {
	switch {
	case fd.Kind() == protoreflect.BytesKind && !fd.IsList():
		merged, err := fn(dst.Get(fd).Bytes(), src.Get(fd).Bytes())
		if err != nil {
			return fmt.Errorf("merger for %q: %w", fd.Name(), err)
		}
		dst.Set(fd, protoreflect.ValueOfBytes(merged))
		return nil
	case fd.Kind() == protoreflect.MessageKind && !fd.IsList() && !fd.IsMap():
		base, err := proto.Marshal(dst.Get(fd).Message().Interface())
		if err != nil {
			return err
		}
		incoming, err := proto.Marshal(src.Get(fd).Message().Interface())
		if err != nil {
			return err
		}
		merged, err := fn(base, incoming)
		if err != nil {
			return fmt.Errorf("merger for %q: %w", fd.Name(), err)
		}
		out := dst.NewField(fd).Message()
		if err := proto.Unmarshal(merged, out.Interface()); err != nil {
			return fmt.Errorf("merger for %q returned undecodable bytes: %w", fd.Name(), err)
		}
		dst.Set(fd, protoreflect.ValueOfMessage(out))
		return nil
	default:
		return fmt.Errorf("field %q does not support mergers", fd.Name())
	}
}

// copyOneof replaces a whole oneof group on dst with src's state: the value of
// whichever member src has set, or nothing at all when src has no case. Setting
// a member clears the sibling dst held, which is what makes the group — rather
// than the member — the unit of last-writer-wins: a member-granular mask could
// say "a is unset" without saying which sibling, if any, takes its place.
func copyOneof(dst, src protoreflect.Message, od protoreflect.OneofDescriptor) {
	if fd := src.WhichOneof(od); fd != nil {
		copyField(dst, src, fd)
		return
	}
	if cur := dst.WhichOneof(od); cur != nil {
		dst.Clear(cur)
	}
}

func copyField(dst, src protoreflect.Message, fd protoreflect.FieldDescriptor) {
	if !src.Has(fd) {
		// For implicit-presence scalars Has is false at the zero value too;
		// clearing produces the same observable result as copying zero.
		dst.Clear(fd)
		return
	}
	switch {
	case fd.IsList():
		dl := dst.Mutable(fd).List()
		dl.Truncate(0)
		sl := src.Get(fd).List()
		for i := 0; i < sl.Len(); i++ {
			dl.Append(deepCopyValue(fd, sl.Get(i)))
		}
	case fd.IsMap():
		dm := dst.Mutable(fd).Map()
		dm.Range(func(k protoreflect.MapKey, _ protoreflect.Value) bool {
			dm.Clear(k)
			return true
		})
		sm := src.Get(fd).Map()
		sm.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
			if fd.MapValue().Kind() == protoreflect.MessageKind {
				dm.Set(k, protoreflect.ValueOfMessage(proto.Clone(v.Message().Interface()).ProtoReflect()))
			} else {
				dm.Set(k, v)
			}
			return true
		})
	case fd.Kind() == protoreflect.MessageKind:
		dst.Set(fd, protoreflect.ValueOfMessage(proto.Clone(src.Get(fd).Message().Interface()).ProtoReflect()))
	default:
		dst.Set(fd, src.Get(fd))
	}
}

func deepCopyValue(fd protoreflect.FieldDescriptor, v protoreflect.Value) protoreflect.Value {
	if fd.Kind() == protoreflect.MessageKind {
		return protoreflect.ValueOfMessage(proto.Clone(v.Message().Interface()).ProtoReflect())
	}
	return v
}

// isOpaque reports whether a field is replaced/merged whole: explicitly
// opaque, or a text field (text implies opaque — the encoded document is one
// atomic value merged by the auto-installed text merger).
func isOpaque(fd protoreflect.FieldDescriptor) bool {
	b := fieldBehavior(fd)
	return b.GetOpaque() || b.GetText()
}

// OneofOf returns the oneof a field is a member of, or nil. Synthetic oneofs
// are not oneofs for any purpose here: proto3 `optional` compiles to one, and
// such a field is an ordinary field with explicit presence. Every check that
// asks "is this a oneof member" must go through this, or it silently reclassifies
// every optional field in the schema.
func OneofOf(fd protoreflect.FieldDescriptor) protoreflect.OneofDescriptor {
	od := fd.ContainingOneof()
	if od == nil || od.IsSynthetic() {
		return nil
	}
	return od
}

// IsText reports whether a field carries (syncplane.v1.field_behavior).text — a
// bytes field holding an encoded engine/text document.
func IsText(fd protoreflect.FieldDescriptor) bool {
	return fieldBehavior(fd).GetText()
}

// fieldBehavior reads the (syncplane.v1.field_behavior) extension, or nil if absent.
func fieldBehavior(fd protoreflect.FieldDescriptor) *syncplanev1.FieldBehavior {
	return protoopts.FieldBehavior(fd)
}

// entityOptions reads the (syncplane.v1.entity) extension off a message, or nil.
func entityOptions(md protoreflect.MessageDescriptor) *syncplanev1.EntityOptions {
	return protoopts.Entity(md)
}
