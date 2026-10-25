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
// also why nothing below a oneof may be keyed or text (registration refuses
// those — see engine/server). A reference may be a oneof member: a stored stub
// is replaced whole anyway, and a oneof of references is how a POLYMORPHIC
// reference — one row pointing at one of several entities — is declared (see
// RefsOf).
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
	// FnAt, when non-nil, is called instead of Fn and additionally receives the
	// instance path of the value being merged: the mask form of the position,
	// with the item keys the descent crossed ("items.<key>.body"), whereas the
	// Mergers map is keyed by the schema path with keys stripped ("items.body").
	// One registered merger therefore serves every element of a keyed list or
	// map, and FnAt is how it can keep per-element state.
	FnAt func(instancePath string, base, incoming []byte) ([]byte, error)
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
		if err := mergePath(dst.ProtoReflect(), src.ProtoReflect(), path, "", "", m); err != nil {
			return fmt.Errorf("lww: path %q: %w", path, err)
		}
	}
	return nil
}

// mergePath applies one mask path. prefix is the schema-path prefix walked so
// far (field names only — the Mergers key space); instPrefix is the same walk
// with the item keys kept, which is what a Merger.FnAt receives.
func mergePath(dst, src protoreflect.Message, path, prefix, instPrefix string, m Mergers) error {
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
	collection := fd.IsMap() || fd.IsList()
	// An empty item key encodes as the empty segment, so the path of that item
	// ends in the separator ("attrs."). Only the absence of any separator makes
	// a bare path: reading "attrs." as "attrs" would replace the whole
	// collection with the writer's view of it.
	bare := rest == "" && (len(seg) == len(path) || !collection)
	if bare || ReplacedWhole(fd) {
		mg, hasMerger := m[prefix+seg]
		if hasMerger && opaque && src.Has(fd) && (dst.Has(fd) || mg.MergeAbsentBase) {
			fn := mg.Fn
			if mg.FnAt != nil {
				instPath := instPrefix + seg
				fn = func(base, incoming []byte) ([]byte, error) {
					return mg.FnAt(instPath, base, incoming)
				}
			}
			return applyMerger(dst, src, fd, fn)
		}
		copyField(dst, src, fd)
		return nil
	}

	// An item path and non-opaque: a collection merges per item, a singular
	// message descends, a scalar cannot be sub-pathed.
	if collection {
		return mergeItem(dst, src, fd, seg, rest, prefix, instPrefix, m)
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
	if err := mergePath(sub, src.Get(fd).Message(), rest, prefix+seg+".", instPrefix+seg+".", m); err != nil {
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
	fieldSeg, afterField, prefix, instPrefix string,
	m Mergers,
) error {
	itemKey, rest := splitSeg(afterField)
	// The schema prefix drops the item key — a merger serves every element —
	// while the instance prefix keeps it, escaped exactly as the mask carries it.
	itemInstPrefix := instPrefix + fieldSeg + "." + itemKey + "."
	switch {
	case fd.IsMap():
		return mergeMapItem(dst, src, fd, itemKey, rest, prefix+fieldSeg+".", itemInstPrefix, m)
	case IsRefMessageField(fd):
		return mergeRefSetItem(dst, src, fd, itemKey)
	case fd.Kind() == protoreflect.MessageKind:
		keyFields := keyFieldsFor(fd)
		if len(keyFields) == 0 {
			// A message list with no determinable identity cannot merge per item;
			// fall back to whole-replace (expansion never emits such a sub-path).
			copyField(dst, src, fd)
			return nil
		}
		return mergeKeyedListItem(dst, src, fd, keyFields, itemKey, rest, prefix+fieldSeg+".", itemInstPrefix, m)
	default:
		return mergeScalarSetItem(dst, src, fd, itemKey, rest)
	}
}

// mergeMapItem applies one keyed map entry: src has the key => set/recurse,
// src lacks it => clear it on dst (clear-on-absent, mirroring the singular case).
func mergeMapItem(
	dst, src protoreflect.Message,
	fd protoreflect.FieldDescriptor,
	itemKey, rest, prefix, instPrefix string,
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
		return mergePath(dstVal, sm.Get(mk).Message(), rest, prefix, instPrefix, m)
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
	itemKey, rest, prefix, instPrefix string,
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
	return mergePath(dstElem, srcElem, rest, prefix, instPrefix, m)
}

// mergeRefSetItem applies one element of a repeated reference message. The
// stored element is nothing but its identity — the workspace beside the target
// key, see Ref.ItemKey — so the list merges like a scalar set: src has the
// element => ensure present once, src lacks it => remove every copy. A sub-path
// below the element is not an error but addresses the element, the same way a
// sub-path into a singular reference message names the whole message.
func mergeRefSetItem(dst, src protoreflect.Message, fd protoreflect.FieldDescriptor, itemKey string) error {
	sl := src.Get(fd).List()
	var srcElem protoreflect.Value
	found := false
	for i := 0; i < sl.Len(); i++ {
		if RefItemKey(fd, sl.Get(i).Message()) == itemKey {
			srcElem = sl.Get(i)
			found = true
			break
		}
	}
	dl := dst.Mutable(fd).List()
	kept := make([]protoreflect.Value, 0, dl.Len()+1)
	for i := 0; i < dl.Len(); i++ {
		if RefItemKey(fd, dl.Get(i).Message()) != itemKey {
			kept = append(kept, dl.Get(i))
		}
	}
	if found {
		// Duplicates already in dst collapse to the one copy src carries: the
		// element is its identity, so one copy is the whole of the information.
		kept = append(kept, protoreflect.ValueOfMessage(proto.Clone(srcElem.Message().Interface()).ProtoReflect()))
	}
	dl.Truncate(0)
	for _, v := range kept {
		dl.Append(v)
	}
	return nil
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
// A path that ends in the separator gives no empty last segment, though the
// merge reads it as the empty key of a collection: "attrs." splits as "attrs",
// which names more of the row than the merge writes, never less.
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

// EqualField reports whether one field holds the same value in two messages of
// the same type. Comparison follows proto.Equal for every field shape: scalar,
// message, repeated and map, with an absent field never equal to a present one.
func EqualField(a, b protoreflect.Message, fd protoreflect.FieldDescriptor) bool {
	if a.Has(fd) != b.Has(fd) {
		return false
	}
	if !a.Has(fd) {
		return true
	}
	// Projecting into empty messages reuses proto.Equal's per-shape semantics
	// rather than restating them. Comparing the field's encoding instead would
	// be cheaper and wrong: map entries have no stable wire order, so two equal
	// maps routinely marshal to different bytes.
	am := a.Type().New()
	bm := b.Type().New()
	am.Set(fd, a.Get(fd))
	bm.Set(fd, b.Get(fd))
	return proto.Equal(am.Interface(), bm.Interface())
}

// EqualUnit reports whether the mask unit named by seg holds the same value in
// both messages. A unit is what ONE top-level mask segment may name — a field,
// or a oneof group (see mergePath) — so a caller narrowing a mask to the parts
// that moved splits at the same granularity the merge applies at.
//
// An unrecognized name reports false — a mask naming it could not be applied
// either, and "it differs" is the conservative answer for a caller deciding
// whether to act on the unit.
func EqualUnit(a, b protoreflect.Message, seg string) bool {
	desc := a.Descriptor()
	if fd := desc.Fields().ByName(protoreflect.Name(seg)); fd != nil {
		if od := OneofOf(fd); od != nil {
			// A member name is not a unit — the merge refuses it — but a caller
			// that resolved one anyway means the group.
			return equalOneof(a, b, od)
		}
		return EqualField(a, b, fd)
	}
	if od := desc.Oneofs().ByName(protoreflect.Name(seg)); od != nil && !od.IsSynthetic() {
		return equalOneof(a, b, od)
	}
	return false
}

// equalOneof compares a oneof group: which member is set, and then that
// member's value. A group is replaced whole by the merge, so it is compared
// whole here.
func equalOneof(a, b protoreflect.Message, od protoreflect.OneofDescriptor) bool {
	af, bf := a.WhichOneof(od), b.WhichOneof(od)
	if af != bf {
		return false
	}
	return af == nil || EqualField(a, b, af)
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

// Ref describes one typed reference to ONE entity: the entity it points at,
// and where inside the stored value the target's key and workspace sit.
//
// Two stored shapes make a reference. An ENTITY-TYPED field — a singular or
// repeated message field whose (element) message carries
// (syncplane.v1.entity) — stores a stub of the target itself, projected down
// to its key fields; Value is nil and the stub is the field value. A REFERENCE
// MESSAGE field — one whose (element) message carries (syncplane.v1.reference)
// — stores a wrapper holding the entity stub in Value and, when the message
// declares one, the target's workspace in Workspace. Consumers address the key
// through KeyPath and the stub through Stub, so the two shapes differ in
// nothing else.
//
// A POLYMORPHIC reference — one stored value that points at one of several
// entities — is a oneof of references, and each member is its own Ref, a
// CASE of it. The oneof sits either on the holder (Group is set, and Field is
// the member) or inside a reference message (Value is a member of the
// message's oneof, and Field is the message field every case shares). A case
// holds a value only while it is the active one, which every accessor here
// already reports: Field is absent, or Stub finds nothing.
type Ref struct {
	// Field is the reference field itself, singular or repeated.
	Field protoreflect.FieldDescriptor
	// Target is the referenced entity's message descriptor.
	Target protoreflect.MessageDescriptor
	// KeyFields are the target's key fields, declared on Target.
	KeyFields []protoreflect.FieldDescriptor
	// Value is the entity-typed field of a reference message that holds the
	// target stub; nil for an entity-typed reference.
	Value protoreflect.FieldDescriptor
	// Workspace is the reference message's workspace field, or nil when every
	// stored value resolves in the holder's own workspace.
	Workspace protoreflect.FieldDescriptor
	// Group is the holder's oneof Field is a member of, or nil.
	Group protoreflect.OneofDescriptor
}

// RefOf classifies fd as a typed reference to one entity, or returns nil when
// it is not one: a non-message field, a map, a message that is neither an
// entity nor a well-formed reference message, or a reference message whose
// target is a oneof of entities — a polymorphic reference has no single
// target, and RefsOf reports its cases. A reference message that breaks its
// rules (see CheckReferenceMessage) is not a reference either — registration
// reports it, and everything else sees an ordinary sub-message.
func RefOf(fd protoreflect.FieldDescriptor) *Ref {
	refs := RefsOf(fd)
	if len(refs) != 1 {
		return nil
	}
	return refs[0]
}

// RefsOf classifies fd as a typed reference and reports every case it can
// hold: nil when fd is not a reference, one Ref for a reference to one
// entity, and one per entity-typed member of the oneof for a reference
// message whose target is a oneof. A member of a oneof on the holder is
// classified on its own, with Group set; the other members of that oneof are
// its sibling cases.
func RefsOf(fd protoreflect.FieldDescriptor) []*Ref {
	if fd == nil || fd.Kind() != protoreflect.MessageKind || fd.IsMap() {
		return nil
	}
	elem := fd.Message()
	group := OneofOf(fd)
	// The reference declaration wins over an entity declaration on the same
	// message: such a message is refused at registration, and until then it
	// must not pass for an entity-typed stub.
	if referenceOptions(elem) == nil {
		if kf := entityKeyFields(elem); kf != nil {
			return []*Ref{{Field: fd, Target: elem, KeyFields: kf, Group: group}}
		}
		return nil
	}
	shape, err := referenceShapeOf(elem)
	if err != nil {
		return nil
	}
	refs := make([]*Ref, 0, len(shape.values))
	for _, v := range shape.values {
		refs = append(refs, &Ref{
			Field:     fd,
			Target:    v.field.Message(),
			KeyFields: v.keyFields,
			Value:     v.field,
			Workspace: shape.workspace,
			Group:     group,
		})
	}
	return refs
}

// Polymorphic reports whether this reference is one case of several: its
// Field is a member of a oneof on the holder, or its Value a member of the
// oneof inside a reference message.
func (r *Ref) Polymorphic() bool {
	return r.Group != nil || r.InnerCase()
}

// InnerCase reports whether the case is decided inside the stored value —
// the reference message's oneof — rather than by which holder field is set.
// Such cases share Field, and are told apart by Value.
func (r *Ref) InnerCase() bool {
	return r.Value != nil && OneofOf(r.Value) != nil
}

// CaseName is the proto name of the field that decides this case — Field for
// a member of the holder's oneof, Value for a member of a reference message's
// oneof — or "" for a reference to one entity.
func (r *Ref) CaseName() string {
	switch {
	case r.Group != nil:
		return string(r.Field.Name())
	case r.InnerCase():
		return string(r.Value.Name())
	}
	return ""
}

// CaseJSONName is CaseName in the accessor form a client spells it in.
func (r *Ref) CaseJSONName() string {
	switch {
	case r.Group != nil:
		return r.Field.JSONName()
	case r.InnerCase():
		return r.Value.JSONName()
	}
	return ""
}

// Qualified reports whether stored values may name a workspace other than the
// holder's: the reference message declares a workspace field.
func (r *Ref) Qualified() bool { return r.Workspace != nil }

// KeyPath returns the field steps from a stored reference value to one key
// field of the target: [kf] for an entity-typed reference, [Value, kf] for a
// reference message.
func (r *Ref) KeyPath(kf protoreflect.FieldDescriptor) []protoreflect.FieldDescriptor {
	if r.Value == nil {
		return []protoreflect.FieldDescriptor{kf}
	}
	return []protoreflect.FieldDescriptor{r.Value, kf}
}

// KeyPathNames is KeyPath as proto field names — the segments a query or index
// path spells the key in.
func (r *Ref) KeyPathNames(kf protoreflect.FieldDescriptor) []string {
	steps := r.KeyPath(kf)
	out := make([]string, len(steps))
	for i, s := range steps {
		out[i] = string(s.Name())
	}
	return out
}

// Stub returns the target stub inside one stored reference value: the value
// itself for an entity-typed reference, the embedded entity message for a
// reference message. Reports false for a reference message that holds no
// stub, which points at nothing.
func (r *Ref) Stub(value protoreflect.Message) (protoreflect.Message, bool) {
	if r.Value == nil {
		return value, true
	}
	if !value.Has(r.Value) {
		return nil, false
	}
	return value.Get(r.Value).Message(), true
}

// WorkspaceOf returns the workspace one stored reference value names, or ""
// when the reference has no workspace field or the value leaves it empty —
// both mean the holder's own workspace.
func (r *Ref) WorkspaceOf(value protoreflect.Message) string {
	if r.Workspace == nil {
		return ""
	}
	return value.Get(r.Workspace).String()
}

// ItemKey renders one stored value of a repeated reference message as the mask
// segment that addresses it, so a PATCH can add or remove an element on its
// own: the element's workspace and its key, escaped and joined the way a
// composite keyed-list identity is (see EncodeItemKey). The workspace part is
// always present, empty when the value names none, so a value with an empty
// workspace and one naming the holder's own are two elements even though
// they resolve to the same row. A reference without a workspace field renders
// the key alone, exactly as an element of a repeated entity-typed reference.
//
// A case of a polymorphic reference message puts its name between the
// workspace and the key, so two cases naming the same key are two elements;
// the workspace part is present whenever the message declares one, and the
// case part whenever the message's target is a oneof.
//
// Callers render every element through this, and the merge decodes with it;
// a segment produced any other way addresses no element. An element of a
// polymorphic reference message is rendered by RefItemKey, which picks the
// case the value holds.
func (r *Ref) ItemKey(value protoreflect.Message) string {
	// Get, not Stub: a value holding no stub still needs an identity, and Get
	// hands back the zero message where Stub reports nothing.
	stub := value
	if r.Value != nil {
		stub = value.Get(r.Value).Message()
	}
	if r.Workspace == nil && !r.InnerCase() {
		return encodeItemKey(stub, r.KeyFields)
	}
	parts := make([]string, 0, len(r.KeyFields)+2)
	if r.Workspace != nil {
		parts = append(parts, escapeKeySegment(value.Get(r.Workspace).String(), true))
	}
	if r.InnerCase() {
		parts = append(parts, escapeKeySegment(string(r.Value.Name()), true))
	}
	for _, kf := range r.KeyFields {
		parts = append(parts, escapeKeySegment(scalarKeyString(kf, stub.Get(kf)), true))
	}
	return strings.Join(parts, "/")
}

// RefItemKey renders one stored value of a repeated reference message field
// as the mask segment that addresses it (see Ref.ItemKey), whichever case the
// value holds. A value of a polymorphic reference message that holds no case
// renders its workspace, an empty case and no key. Panics when fd is not a
// reference message field.
func RefItemKey(fd protoreflect.FieldDescriptor, value protoreflect.Message) string {
	refs := RefsOf(fd)
	if len(refs) == 0 || refs[0].Value == nil {
		panic(fmt.Sprintf("lww: %s is not a reference message field", fd.FullName()))
	}
	for _, r := range refs {
		if len(refs) == 1 || value.Has(r.Value) {
			return r.ItemKey(value)
		}
	}
	// No case set: the shape ItemKey renders, with the case and the key both
	// empty, so the segment still has every part a decoder expects to split.
	r := refs[0]
	if r.Workspace == nil {
		return "/"
	}
	return escapeKeySegment(value.Get(r.Workspace).String(), true) + "//"
}

// ResolvesIn reports whether a stored reference value points into workspace,
// the holder's own: it names that workspace, or names none.
func (r *Ref) ResolvesIn(value protoreflect.Message, workspace string) bool {
	ws := r.WorkspaceOf(value)
	return ws == "" || ws == workspace
}

// Addressable reports whether a path may continue from the reference field
// into the field named: a key field of an entity-typed stub, or the value and
// workspace fields of a reference message — the key fields then sit under
// Value, which is itself an entity-typed reference.
func (r *Ref) Addressable(name string) bool {
	if r.Value != nil {
		return name == string(r.Value.Name()) ||
			(r.Workspace != nil && name == string(r.Workspace.Name()))
	}
	for _, kf := range r.KeyFields {
		if string(kf.Name()) == name {
			return true
		}
	}
	return false
}

// AddressableNames lists the fields Addressable admits, for error messages.
func (r *Ref) AddressableNames() []string {
	if r.Value != nil {
		out := []string{string(r.Value.Name())}
		if r.Workspace != nil {
			out = append(out, string(r.Workspace.Name()))
		}
		return out
	}
	out := make([]string, len(r.KeyFields))
	for i, kf := range r.KeyFields {
		out[i] = string(kf.Name())
	}
	return out
}

// RefKeyFields returns the key fields of the entity a reference field targets
// (see RefOf), or nil when fd is not a reference to one entity. The fields
// are the TARGET's: on a reference message they sit under Ref.Value, not on
// the stored value itself — address them through Ref.KeyPath.
func RefKeyFields(fd protoreflect.FieldDescriptor) []protoreflect.FieldDescriptor {
	if r := RefOf(fd); r != nil {
		return r.KeyFields
	}
	return nil
}

// IsRef reports whether fd is a first-class typed reference (see RefsOf),
// to one entity or to one of several.
func IsRef(fd protoreflect.FieldDescriptor) bool {
	return len(RefsOf(fd)) > 0
}

// IsRefMessageField reports whether fd is a reference whose stored shape is a
// reference message — the shape whose workspace and key are one merge unit: a
// singular field is replaced whole on every write, a repeated one merges per
// element with the whole element as the identity (see Ref.ItemKey).
func IsRefMessageField(fd protoreflect.FieldDescriptor) bool {
	refs := RefsOf(fd)
	return len(refs) > 0 && refs[0].Value != nil
}

// IsReferenceMessage reports whether md declares (syncplane.v1.reference),
// well-formed or not.
func IsReferenceMessage(md protoreflect.MessageDescriptor) bool {
	return referenceOptions(md) != nil
}

// CheckReferenceMessage validates the shape of a message declaring
// (syncplane.v1.reference): not itself an entity; its target named by
// exactly one singular entity-typed field, or by a oneof whose members are
// all singular entity-typed fields; at most one singular string field marked
// (syncplane.v1.field_behavior).workspace, outside any oneof; and no other
// fields. A message that declares nothing passes unless one of its fields is
// marked workspace, which only a reference message may carry.
func CheckReferenceMessage(md protoreflect.MessageDescriptor) error {
	if referenceOptions(md) == nil {
		fields := md.Fields()
		for i := 0; i < fields.Len(); i++ {
			if fd := fields.Get(i); fieldBehavior(fd).GetWorkspace() {
				return fmt.Errorf("field %q is marked workspace, but %s is not a reference message; the flag belongs inside a message declaring (syncplane.v1.reference)", fd.Name(), md.FullName())
			}
		}
		return nil
	}
	_, err := referenceShapeOf(md)
	return err
}

// referenceShape is the parsed form of a reference message: the entity-typed
// field naming its target, or — when the target is a oneof — one entry per
// member, in declaration order.
type referenceShape struct {
	values    []referenceValue
	workspace protoreflect.FieldDescriptor
}

// referenceValue is one entity-typed field of a reference message and the
// key fields of the entity it names.
type referenceValue struct {
	field     protoreflect.FieldDescriptor
	keyFields []protoreflect.FieldDescriptor
}

// referenceShapeOf reads a reference message's fields, or reports how the
// message breaks the rules CheckReferenceMessage states.
func referenceShapeOf(md protoreflect.MessageDescriptor) (referenceShape, error) {
	var shape referenceShape
	if entityOptions(md) != nil {
		return shape, fmt.Errorf("reference message %s is also an entity; a reference message wraps an entity, it is not one", md.FullName())
	}
	var group protoreflect.OneofDescriptor
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		od := OneofOf(fd)
		if fieldBehavior(fd).GetWorkspace() {
			if od != nil {
				return shape, fmt.Errorf("reference message %s: workspace field %q is a member of oneof %q; the workspace names where every case resolves, so it sits beside the oneof", md.FullName(), fd.Name(), od.Name())
			}
			if fd.Kind() != protoreflect.StringKind || fd.IsList() {
				return shape, fmt.Errorf("reference message %s: workspace field %q must be a singular string", md.FullName(), fd.Name())
			}
			if shape.workspace != nil {
				return shape, fmt.Errorf("reference message %s: fields %q and %q are both marked workspace; a reference names one workspace", md.FullName(), shape.workspace.Name(), fd.Name())
			}
			shape.workspace = fd
			continue
		}
		if fd.Kind() == protoreflect.MessageKind && !fd.IsList() && !fd.IsMap() {
			if kf := entityKeyFields(fd.Message()); kf != nil {
				// Several entity-typed fields name one target each only as the
				// members of ONE oneof: the stored value then holds exactly one
				// case, which is what makes it a reference rather than several.
				if len(shape.values) > 0 && (od == nil || od != group) {
					return shape, fmt.Errorf("reference message %s: fields %q and %q are both entity-typed; a reference names one target, or one of several as the members of a single oneof", md.FullName(), shape.values[0].field.Name(), fd.Name())
				}
				group = od
				shape.values = append(shape.values, referenceValue{field: fd, keyFields: kf})
				continue
			}
		}
		return shape, fmt.Errorf("reference message %s: field %q is neither the entity field nor a workspace field; a reference message holds exactly those", md.FullName(), fd.Name())
	}
	if len(shape.values) == 0 {
		return shape, fmt.Errorf("reference message %s has no entity-typed field to name its target", md.FullName())
	}
	return shape, nil
}

// entityKeyFields returns md's key fields when md is an entity whose key can
// identify a stub, else nil. This is the detection rule for an entity-typed
// reference: no annotation on the field, the target message being an entity
// makes it one.
func entityKeyFields(md protoreflect.MessageDescriptor) []protoreflect.FieldDescriptor {
	eo := entityOptions(md)
	if eo == nil || len(eo.GetKeyFields()) == 0 {
		return nil
	}
	fields := make([]protoreflect.FieldDescriptor, 0, len(eo.GetKeyFields()))
	for _, n := range eo.GetKeyFields() {
		f := md.Fields().ByName(protoreflect.Name(n))
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

// EncodeItemKey renders a keyed-list element's identity as the mask segment a
// path addressing it carries. Exported for instance-path construction (see
// Merger.FnAt): a consumer enumerating a row's elements must render each key
// exactly as the merge decodes it, or the two would address different elements.
func EncodeItemKey(msg protoreflect.Message, keyFields []protoreflect.FieldDescriptor) string {
	return encodeItemKey(msg, keyFields)
}

// EncodeMapKeySegment renders a map key as the mask segment a path addressing
// its entry carries — the inverse of DecodeMapKey. Exported for instance-path
// construction, like EncodeItemKey.
func EncodeMapKeySegment(fd protoreflect.FieldDescriptor, mk protoreflect.MapKey) string {
	return escapeKeySegment(scalarKeyString(fd.MapKey(), mk.Value()), false)
}

// KeyFieldsFor exposes keyFieldsFor: the identity fields of a keyed message
// list, or nil when the list has none (and so merges whole).
func KeyFieldsFor(fd protoreflect.FieldDescriptor) []protoreflect.FieldDescriptor {
	return keyFieldsFor(fd)
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

// ReplacedWhole reports whether Merge replaces fd whole even when a mask path
// names a sub-path into it. That is true for an opaque or text field, and for a
// singular reference message, whose workspace and key never split across a
// masked write. A repeated reference message merges per element instead, with
// the whole element as the identity.
func ReplacedWhole(fd protoreflect.FieldDescriptor) bool {
	return isOpaque(fd) || (IsRefMessageField(fd) && !fd.IsList())
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

func referenceOptions(md protoreflect.MessageDescriptor) *syncplanev1.ReferenceOptions {
	return protoopts.Reference(md)
}
