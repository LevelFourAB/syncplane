package query

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/levelfourab/syncplane/engine/internal/lww"
)

// Index entries: turning a row ordering into sortable bytes.
//
// An index entry is the byte string a row occupies in a secondary index. The
// one property it must have is that BYTE order over entries equals the row
// order Order.Compare produces over the same rows and the same key list —
// that equivalence is what lets a range scan replace a scan-and-sort, and
// nothing else about the encoding matters. It lives in this package, next to
// the comparator, because a change to one that is not mirrored in the other
// silently corrupts every index rather than failing; testdata/indexkey pins
// the bytes so that drift is caught.
//
//	entry     := component* rowKey
//	component := tag payload?          (no payload for the non-present tags)
//
//	tag         0x01 present   0xfd NaN   0xff missing   — never inverted
//	payload (ascending; for a desc component every payload byte is inverted,
//	         terminator included — the tag is not)
//	  string  : CESU-8, 0x00 escaped to 0x00 0xff, terminated 0x00 0x00
//	  bool    : 0x00 | 0x01
//	  number  : IEEE-754 float64 big-endian, sign-flipped
//	  bigint  : 8 bytes big-endian; signed flips the sign bit, unsigned as-is
//	rowKey    : CESU-8 escaped string, ALWAYS ascending
//
// Four choices carry the weight:
//
//   - CESU-8, not UTF-8. Byte order over CESU-8 equals UTF-16 code-unit
//     order, which is what CompareStringsUTF16 and JS `<` give. Plain UTF-8
//     sorts astral characters above every BMP one and would disagree.
//   - The tag sits OUTSIDE the inversion, so its three values encode the
//     order ranks (real < NaN < missing) directly. Order.Compare applies the
//     rank before the direction, so a desc component must too.
//   - The row-key suffix is never inverted, because Order.Compare breaks
//     final ties on the row key ascending whatever the directions were. It is
//     re-encoded rather than appended raw: an adapter's own key order is
//     UTF-8 byte order, which is a different order.
//   - A component's kind is static (singularKind over the leaf descriptor),
//     so each one has exactly one payload shape and two entries in the same
//     index are always comparable byte for byte.
//
// One row, several entries
//
// A component whose path FANS OUT has no single value per row, so such a row
// takes one POSITION PER ELEMENT — the posting list that makes "some assignee is
// Maya" a range read instead of a scan. Two things fan out, and they compose:
//
//   - a repeated field, one position per element; and
//   - a map, written `*` in the declared path, one position per ENTRY. That is
//     the shape user-defined ("custom") fields take. The `*` is followed by
//     `key` or `value`, naming the side of the entry the component stores, so an
//     index over custom fields declares the components it wants in the order it
//     wants them: `[custom.*.key, custom.*.value.number]` makes a probe on ONE
//     custom field a contiguous range, and `[custom.*.value.number,
//     custom.*.key]` makes "which custom field holds 5" one.
//
// Four rules keep it honest:
//
//   - Every component that fans out must fan out over the SAME chain, and they
//     fan out together, position by position. A cross product would pair one
//     element's id with another's role, and a query meant to describe one
//     element would match rows where no element does. A SHORTER chain is fine:
//     an entry-key component stops at the map and repeats across the positions
//     below it.
//   - A `*` may be followed by a repeated field INSIDE one entry
//     (custom.*.labels.values, the multi-select shape). That is not a cross
//     product either: the second fan lives inside one position of the first.
//   - A row whose collection is empty or unset still writes exactly one entry,
//     with the missing tag at each fan-out component. Every live row occupies at
//     least one position in every index, which is what lets a range that does
//     NOT constrain the fan-out component still see every row.
//   - Duplicate positions collapse: they would produce identical bytes, and an
//     entry is a position, not a count.
//
// An ordering cannot be read off a LIST fan-out component — a row sits at
// several places at once — so the planner only trusts one that the equality
// prefix has already pinned to a single element value. A MAP fan-out is
// different: a map holds at most one value per key, so once the entry key is
// pinned the row occupies exactly one position and the leaf IS an ordering.

// The tags double as the encoded order ranks: 0x01 < 0xfd < 0xff is
// rankReal < rankNaN < rankMissing, so a byte comparison of two entries
// reproduces Order.Compare's rank check without decoding anything.
const (
	tagPresent byte = 0x01
	tagNaN     byte = 0xfd
	tagMissing byte = 0xff
)

// indexEncodingVersion is folded into every SpecHash. Bumping it invalidates
// every stored index, which is the escape hatch for a change to the encoding
// that leaves the spec itself untouched.
const indexEncodingVersion = 1

// IndexComponent is one resolved component of an index key: a path through
// the message, and the direction its values are stored in.
//
// A path may FAN OUT: a step marked `fan` is a repeated field the component
// takes one value per element of, or — written `*` in the declared path — a map
// it takes one value per ENTRY of. Fan steps nest:
// `custom.*.value.labels.values` fans over every entry of `custom` and, within
// each entry, over every element of that entry's `labels.values`. There is no
// cross product in that, because the second fan lives inside one element of the
// first.
type IndexComponent struct {
	fields []pathStep
	desc   bool
}

// isEntryKey reports that this component stores the KEY of the map entry its
// path fans out over, rather than any value below it — the `key` selector,
// written `custom.*.key`. It is a property of the path rather than a flag beside
// it, so a component's meaning is fully determined by its fields.
func (c IndexComponent) isEntryKey() bool {
	if len(c.fields) == 0 {
		return false
	}
	return c.fields[len(c.fields)-1].entryKey
}

// Multi reports whether a row can occupy several positions because of this
// component.
func (c IndexComponent) Multi() bool {
	for _, st := range c.fields {
		if st.fan {
			return true
		}
	}
	return false
}

// fanLevel is one scope a component's path fans out over: the steps from the
// enclosing scope down to and including the collection itself.
type fanLevel struct {
	steps []pathStep
	isMap bool
}

func (f fanLevel) field() protoreflect.FieldDescriptor { return f.steps[len(f.steps)-1].fd }

// fanLevels splits a component path at its fan steps. The result describes how
// to enumerate the positions the component occupies; what is left after the last
// fan step (see fanRest) is resolved against each of them.
func fanLevels(fields []pathStep) []fanLevel {
	var out []fanLevel
	start := 0
	for i, st := range fields {
		if !st.fan {
			continue
		}
		out = append(out, fanLevel{steps: fields[start : i+1], isMap: st.fd.IsMap()})
		start = i + 1
	}
	return out
}

// fanRest is the path from the innermost fanned scope to the component's value.
// Empty means the fanned element (or map value) IS the value.
func fanRest(fields []pathStep) []pathStep {
	for i := len(fields) - 1; i >= 0; i-- {
		if fields[i].fan {
			return fields[i+1:]
		}
	}
	return fields
}

// leafField is the descriptor whose values this component stores. Everything
// that classifies or encodes a payload reads it rather than the path's last
// field, because for a map fan those differ: the key component stores the map's
// KEY type and a component ending at the fan step stores its VALUE type.
func (c IndexComponent) leafField() protoreflect.FieldDescriptor {
	levels := fanLevels(c.fields)
	rest := fanRest(c.fields)
	if len(levels) == 0 {
		return pathLeaf(rest)
	}
	last := levels[len(levels)-1]
	if c.isEntryKey() {
		return last.field().MapKey()
	}
	if len(rest) > 0 {
		return pathLeaf(rest)
	}
	if last.isMap {
		return last.field().MapValue()
	}
	// A repeated field's own descriptor: its Kind is the element kind, which is
	// what extractSingular converts an element with.
	return last.field()
}

// MaskTouches reports whether a field-mask path could change the value this
// component reads, given the path split into segments the way lww splits a mask
// (lww.SplitPath). It is what lets a PATCH whose mask reaches no index skip the
// entry rewrite entirely — a pure descriptor-and-name comparison, with no
// decode, no read and no encode.
//
// Two paths interact when either is a PREFIX of the other: a mask that stops
// above the component replaces the subtree the component reads through, and one
// that continues below it writes inside the value the component reads. Three
// cases make the comparison conservative where a bare name comparison would be
// wrong, and every one of them answers TRUE:
//
//   - A mask segment may name a ONEOF group rather than a field (lww merges a
//     oneof whole, by group name), so it changes any member the component reads.
//   - A mask may address one ITEM of a map or keyed list ("tags.red"), so a
//     segment following a repeated or map field is an item key, not a field
//     name, and lands inside the collection the component fans out over.
//   - A path that runs out mid-comparison is a prefix, whichever side it was.
//
// Anything else is a genuine divergence at a field the component does not read,
// and only that returns false.
func (c IndexComponent) MaskTouches(segs []string) bool {
	for i := 0; ; i++ {
		if i == len(segs) || i == len(c.fields) {
			// One path is a prefix of the other, which is the whole test.
			return true
		}
		fd := c.fields[i].fd
		if segs[i] == string(fd.Name()) {
			if fd.IsList() || fd.IsMap() {
				// Whatever follows addresses an item of this collection, so it
				// is inside what the component reads either way.
				return true
			}
			continue
		}
		if od := lww.OneofOf(fd); od != nil && segs[i] == string(od.Name()) {
			return true
		}
		return false
	}
}

// mapFanSegment is the segment that means "fan out over every entry of this
// map" in an index component's path. It cannot collide with a field name — no
// proto identifier is `*` — and it is only special HERE: in a condition path the
// segment after a map is an entry key, and `*` would simply be that key.
const mapFanSegment = "*"

// The selectors that may follow a `*`: which side of the map entry the
// component stores. They are the same two names the entry scope of an any()
// over a map offers, so one concept has one spelling in a declaration and in a
// query.
const (
	mapKeySegment   = "key"
	mapValueSegment = "value"
)

// CompileIndexComponent resolves one declared index key into the ONE component
// it contributes. It rejects the same leaves CompileOrder does — an index whose
// component could never take part in a comparison would sort every row into one
// bucket — plus collaborative text, which is comparable but would rewrite its
// entry on every keystroke.
//
// Unlike a condition path, this one may FAN OUT: through a repeated field (and
// may end at one), which is what an index over "some element's value" is made
// of, and through `*` after a map, which is what an index over user-defined
// fields is made of. At most one repeated field per component, since a second
// list would need a cross product within the component itself; a `*` nests
// inside one entry rather than crossing with it, so it composes with a list.
//
// A `*` is followed by `key` or `value`, so one declared path is one component
// and an index states its own layout:
//
//	[ custom.*.key, custom.*.value.number ]   one custom field: equality, ranges
//	                                          and an ordering within it
//	[ custom.*.value.number, custom.*.key ]   which custom field holds 5:
//	                                          equality, cross-key or named-key
//
// The two are complementary and neither subsumes the other, which is why the
// order is declared rather than fixed here: `[key, value]` clusters one field's
// values so they can be ranged and ordered, `[value, key]` clusters one VALUE so
// the fields holding it can be found.
func CompileIndexComponent(
	desc protoreflect.MessageDescriptor,
	path []string,
	descending bool,
) (IndexComponent, error) {
	fields, err := resolveIndexFields(desc, path)
	if err != nil {
		return IndexComponent{}, err
	}
	// A path ending at a single-key reference extends to that key field, exactly
	// as a comparison on the reference does, so an index and a query on the same
	// reference agree on what they are keyed by. For a to-many reference that
	// makes the component fan out over the stored target keys.
	if refKeys := lww.RefKeyFields(pathLeaf(fields)); len(refKeys) == 1 {
		fields = append(fields, pathStep{fd: refKeys[0]})
	}
	c := IndexComponent{fields: fields, desc: descending}
	leaf := c.leafField()
	if len(fanRest(fields)) == 0 && !fields[len(fields)-1].fd.IsMap() {
		// The leaf IS the repeated field, so the stored values are its elements
		// and it is the element kind that has to be comparable.
		if !comparableKind(singularKind(leaf)) {
			return IndexComponent{}, fmt.Errorf("path %v: %s cannot be indexed; %s",
				path, describeField(leaf), indexListHint(path, leaf))
		}
	} else if !comparableKind(singularKind(leaf)) {
		return IndexComponent{}, fmt.Errorf("path %v: %s is not comparable; %s",
			path, describeField(leaf), indexLeafHint(path, leaf))
	}
	if lww.IsText(leaf) {
		return IndexComponent{}, fmt.Errorf(
			"path %v: a collaborative text field cannot be indexed; every keystroke rewrites its value, so the entry would be rewritten with it",
			path)
	}
	return c, nil
}

// indexLeafHint names what to index instead of a leaf no comparison reaches.
func indexLeafHint(path []string, leaf protoreflect.FieldDescriptor) string {
	if leaf.Kind() == protoreflect.MessageKind {
		return fmt.Sprintf("index a scalar field inside it instead, e.g. %s.<field>", strings.Join(path, "."))
	}
	return "it is not comparable"
}

// indexListHint points a component ending at a list of messages at the path
// that would actually index something comparable.
func indexListHint(path []string, leaf protoreflect.FieldDescriptor) string {
	if leaf.Kind() != protoreflect.MessageKind {
		return "its elements are not comparable"
	}
	if refKeys := lww.RefKeyFields(leaf); len(refKeys) == 1 {
		return fmt.Sprintf("index its key field instead, %s.%s", strings.Join(path, "."), refKeys[0].Name())
	}
	return fmt.Sprintf("index a scalar field of the elements instead, e.g. %s.<field>", strings.Join(path, "."))
}

// resolveIndexFields is resolveFields with one extra power: it may FAN OUT,
// over a repeated field or — with a `*` segment — over every entry of a map,
// marking each such step. The reference rule is unchanged — a to-many reference
// stores only its target's key fields, so those are the only ones addressable
// under it.
//
// A map must be followed by `*`, and the `*` by `key` or `value`. An index
// component names a SHAPE, so a literal key in one would declare an index over a
// single entry, which is a different feature and not this one; the error points
// at the syntax that is meant. The selector is required rather than defaulted
// because a bare trailing `*` would be genuinely ambiguous — for
// `map<string, string> attrs`, `attrs.*` could mean either side of the entry.
func resolveIndexFields(desc protoreflect.MessageDescriptor, path []string) ([]pathStep, error) {
	if len(path) == 0 {
		return nil, fmt.Errorf("index component path is empty")
	}
	cur := desc
	listFans := 0
	fields := make([]pathStep, 0, len(path))
	for i := 0; i < len(path); i++ {
		seg := path[i]
		if cur == nil {
			return nil, fmt.Errorf("path %v: %q is not a message field, cannot descend into %q", path, path[i-1], seg)
		}
		fd := cur.Fields().ByName(protoreflect.Name(seg))
		if fd == nil {
			return nil, fmt.Errorf("path %v: %s has no field %q", path, cur.FullName(), seg)
		}
		if fd.IsMap() {
			st, next, err := resolveMapFan(path, i, fd)
			if err != nil {
				return nil, err
			}
			fields = append(fields, st)
			cur = next
			i += 2
			continue
		}
		if i < len(path)-1 {
			if refKeys := lww.RefKeyFields(fd); refKeys != nil && !isKeyField(refKeys, path[i+1]) {
				return nil, fmt.Errorf("path %v: cannot descend through reference %q into %q; only its key fields are addressable", path, seg, path[i+1])
			}
		}
		st := pathStep{fd: fd}
		switch {
		case fd.IsList():
			// A `*` nests inside ONE entry of the map above it, so it composes
			// with a list. Two lists do not: the second would have to be crossed
			// with the first, pairing values that never belonged together.
			if listFans > 0 {
				return nil, fmt.Errorf("path %v: %q is a second repeated field on one index component; index a path through one list at a time",
					path, seg)
			}
			listFans++
			st.fan = true
			if fd.Kind() == protoreflect.MessageKind {
				cur = fd.Message()
			} else {
				cur = nil
			}
		case fd.Kind() == protoreflect.MessageKind:
			cur = fd.Message()
		default:
			cur = nil
		}
		fields = append(fields, st)
	}
	return fields, nil
}

// resolveMapFan consumes the three segments a map contributes to an index path —
// the field, the `*`, and the `key`/`value` selector — and returns the fan step
// plus the descriptor the rest of the path resolves against (nil when nothing
// may follow).
//
// The selector is what makes one declared path one component. `key` ends the
// component at the entry's key; `value` continues into the entry's value, which
// a message value may be descended into and a scalar value may not.
func resolveMapFan(
	path []string,
	i int,
	fd protoreflect.FieldDescriptor,
) (pathStep, protoreflect.MessageDescriptor, error) {
	seg := path[i]
	shapes := fmt.Sprintf("%s.%s.%s or %s.%s.%s.<field>",
		seg, mapFanSegment, mapKeySegment, seg, mapFanSegment, mapValueSegment)
	if i+1 >= len(path) || path[i+1] != mapFanSegment {
		return pathStep{}, nil, fmt.Errorf(
			"path %v: %s must be followed by %q in an index path; an index over a map fans out over every entry, so write %s",
			path, describeField(fd), mapFanSegment, shapes)
	}
	if i+2 >= len(path) {
		return pathStep{}, nil, fmt.Errorf(
			"path %v: %q must be followed by %q or %q; a bare %q does not say which side of the entry the component stores, so write %s",
			path, mapFanSegment, mapKeySegment, mapValueSegment, mapFanSegment, shapes)
	}
	st := pathStep{fd: fd, fan: true}
	switch path[i+2] {
	case mapKeySegment:
		if i+3 < len(path) {
			return pathStep{}, nil, fmt.Errorf(
				"path %v: %q is the entry key of %s, a %s, so %q cannot follow it",
				path, mapKeySegment, describeField(fd), kindName(fd.MapKey()), path[i+3])
		}
		st.entryKey = true
		return st, nil, nil
	case mapValueSegment:
		vfd := fd.MapValue()
		if vfd.Kind() == protoreflect.MessageKind {
			return st, vfd.Message(), nil
		}
		if i+3 < len(path) {
			return pathStep{}, nil, fmt.Errorf(
				"path %v: %s has a %s value, so %q cannot be descended into",
				path, describeField(fd), kindName(vfd), path[i+2])
		}
		return st, nil, nil
	default:
		return pathStep{}, nil, fmt.Errorf(
			"path %v: %q after %q must be %q or %q, naming the side of the entry the component stores; write %s",
			path, path[i+2], mapFanSegment, mapKeySegment, mapValueSegment, shapes)
	}
}

// IndexEncoder turns rows into the entries of one index. It is immutable and
// safe to share across goroutines.
type IndexEncoder struct {
	comps []IndexComponent
	// fan is the chain of scopes the components fan out over, outermost first.
	// Holding it once is what makes the components fan out TOGETHER: each
	// collection is resolved a single time and every component reads the same
	// position of it.
	//
	// It is a CHAIN rather than a single field because a `*` over a map may be
	// followed by a repeated field inside one entry — the multi-select shape,
	// `custom.*.labels.values` — and each level lives inside one position of the
	// level above it, so nothing is crossed with anything.
	fan []fanLevel
}

// NewIndexEncoder builds an encoder over already-compiled components. An
// encoder with no components is legal and yields entries that are the row
// key alone.
//
// It fails when two components fan out over different chains. Their cross
// product would be the only way to store both, and a cross product pairs values
// that never belonged to the same element — so such an index is refused at
// declaration rather than answering a subtly wrong question at read time. A
// SHORTER chain is fine, and is the normal case: the entry-key component of a
// `custom.*.labels.values` index stops at the map, and its value simply repeats
// across the positions below it.
func NewIndexEncoder(comps []IndexComponent) (*IndexEncoder, error) {
	var fan []fanLevel
	for _, c := range comps {
		levels := fanLevels(c.fields)
		if len(levels) == 0 {
			continue
		}
		if !compatibleFan(fan, levels) {
			return nil, fmt.Errorf(
				"an index cannot fan out over both %s and %s; one fan-out chain per index",
				fanPathString(fan), fanPathString(levels))
		}
		if len(levels) > len(fan) {
			fan = levels
		}
	}
	return &IndexEncoder{comps: comps, fan: fan}, nil
}

// compatibleFan reports whether two chains describe the same nesting, one being
// a prefix of the other.
func compatibleFan(a, b []fanLevel) bool {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i].isMap != b[i].isMap || !sameFanSteps(a[i].steps, b[i].steps) {
			return false
		}
	}
	return true
}

// sameFanSteps compares two fan-chain paths for describing the same nesting.
//
// It ignores the entry-key selector, unlike samePath, and that is the whole
// reason it exists: a `custom.*.key` component and a `custom.*.value.number`
// component fan out over the SAME map and read one position of it. The selector
// says which half of that position each stores, not where either one is, so
// treating the two chains as different would refuse the very pairing the layout
// is made of.
func sameFanSteps(a, b []pathStep) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].fd.FullName() != b[i].fd.FullName() || a[i].fan != b[i].fan {
			return false
		}
	}
	return true
}

func fanPathString(levels []fanLevel) string {
	parts := make([]string, 0, len(levels))
	for _, l := range levels {
		p := fieldPathString(l.steps)
		if l.isMap {
			p += "." + mapFanSegment
		}
		parts = append(parts, p)
	}
	if len(parts) == 0 {
		return "(nothing)"
	}
	return strings.Join(parts, " → ")
}

// position is one place a row occupies in the index: one entry per level of the
// fan chain, outermost first. A position SHORTER than the chain means the
// collection at that depth was empty or absent, so every component reading below
// it takes the missing tag.
type position []posLevel

type posLevel struct {
	// key is the map entry's key; only set for a map level.
	key    protoreflect.MapKey
	hasKey bool
	// val is the element (a list level) or the entry's value (a map level).
	val protoreflect.Value
}

// Encode produces the entries msg occupies in this index — one per position of
// the fan chain, or a single entry when no component fans out. rowKey is the
// row's storage key, which terminates every entry so that entries are unique and
// tie exactly the way Order.Compare does.
//
// The result is never empty: a row whose collection is empty or unset still
// takes one position, with the missing tag at each fan-out component, so every
// live row is reachable through every index.
func (e *IndexEncoder) Encode(msg protoreflect.Message, rowKey string) [][]byte {
	positions := e.positions(msg)
	out := make([][]byte, 0, len(positions))
	seen := make(map[string]struct{}, len(positions))
	for _, p := range positions {
		entry := e.encodeAt(msg, rowKey, p)
		// Two positions can encode identically — duplicate list elements, or two
		// map entries no component distinguishes. An entry is a position, not a
		// count, so the second one would be a no-op write in one adapter and a
		// primary-key conflict in another.
		if _, dup := seen[string(entry)]; dup {
			continue
		}
		seen[string(entry)] = struct{}{}
		out = append(out, entry)
	}
	return out
}

// positions enumerates the fan chain against one row, depth first.
func (e *IndexEncoder) positions(msg protoreflect.Message) []position {
	if len(e.fan) == 0 {
		return []position{nil}
	}
	var out []position
	var walk func(scope protoreflect.Message, level int, cur position)
	walk = func(scope protoreflect.Message, level int, cur position) {
		if level == len(e.fan) || scope == nil {
			out = append(out, append(position(nil), cur...))
			return
		}
		fl := e.fan[level]
		v, found := resolveValue(scope, fl.steps)
		if !found {
			out = append(out, append(position(nil), cur...))
			return
		}
		if fl.isMap {
			if v.kind != rvMap || v.mapv.Len() == 0 {
				out = append(out, append(position(nil), cur...))
				return
			}
			// Sorted rather than in map order: Go randomizes map iteration, and
			// the entries a row writes have to be the same list every time or a
			// rewrite would churn storage for nothing.
			for _, k := range sortedMapKeys(fl.field(), v.mapv) {
				val := v.mapv.Get(k)
				walk(messageScope(fl.field().MapValue(), val), level+1,
					append(cur, posLevel{key: k, hasKey: true, val: val}))
			}
			return
		}
		if v.kind != rvList || v.list.Len() == 0 {
			out = append(out, append(position(nil), cur...))
			return
		}
		for i := 0; i < v.list.Len(); i++ {
			el := v.list.Get(i)
			walk(messageScope(fl.field(), el), level+1, append(cur, posLevel{val: el}))
		}
	}
	walk(msg, 0, nil)
	return out
}

// messageScope is the message a deeper fan level resolves against, or nil when
// the value is not a message (a scalar collection ends the chain).
func messageScope(fd protoreflect.FieldDescriptor, v protoreflect.Value) protoreflect.Message {
	if fd.Kind() != protoreflect.MessageKind && fd.Kind() != protoreflect.GroupKind {
		return nil
	}
	return v.Message()
}

// sortedMapKeys returns a map's keys in the order the KEY COMPONENT stores them,
// so the entries of one row come out in a stable order.
func sortedMapKeys(fd protoreflect.FieldDescriptor, m protoreflect.Map) []protoreflect.MapKey {
	keys := make([]protoreflect.MapKey, 0, m.Len())
	m.Range(func(k protoreflect.MapKey, _ protoreflect.Value) bool {
		keys = append(keys, k)
		return true
	})
	kfd := fd.MapKey()
	sort.Slice(keys, func(i, j int) bool {
		a := extractSingular(kfd, keys[i].Value())
		b := extractSingular(kfd, keys[j].Value())
		c, ok := compareScalar(a, b)
		if !ok {
			return false
		}
		return c < 0
	})
	return keys
}

// encodeAt builds one entry, reading each component from the position it belongs
// to. A position shorter than a component's fan depth means "no element there",
// which is how a row with an empty collection still gets its single entry.
func (e *IndexEncoder) encodeAt(msg protoreflect.Message, rowKey string, pos position) []byte {
	buf := make([]byte, 0, 16+8*len(e.comps))
	for _, c := range e.comps {
		v, found := c.valueAt(msg, pos)
		buf = appendComponent(buf, c, v, found)
	}
	return appendIndexString(buf, rowKey)
}

// valueAt resolves this component against one position of the encoder's fan
// chain.
func (c IndexComponent) valueAt(msg protoreflect.Message, pos position) (rowValue, bool) {
	levels := fanLevels(c.fields)
	if len(levels) == 0 {
		return resolveValue(msg, c.fields)
	}
	depth := len(levels)
	if depth > len(pos) {
		// The collection this component reads from was empty or absent.
		return rowValue{}, false
	}
	at := pos[depth-1]
	last := levels[depth-1]
	if c.isEntryKey() {
		return extractSingular(last.field().MapKey(), at.key.Value()), true
	}
	if rest := fanRest(c.fields); len(rest) > 0 {
		if last.isMap && last.field().MapValue().Kind() != protoreflect.MessageKind {
			return rowValue{}, false
		}
		return resolveValue(at.val.Message(), rest)
	}
	if last.isMap {
		return extractSingular(last.field().MapValue(), at.val), true
	}
	// The leaf is the repeated field itself, so the element IS the value.
	return extractSingular(last.field(), at.val), true
}

// Components exposes the compiled components, for a planner deciding whether
// this index serves a query.
func (e *IndexEncoder) Components() []IndexComponent { return e.comps }

func fieldPathString(fields []pathStep) string {
	parts := make([]string, len(fields))
	for i, st := range fields {
		parts[i] = string(st.fd.Name())
	}
	return strings.Join(parts, ".")
}

// EncodePrefix encodes the first len(values) components from explicit values
// — the leading equality terms of a probe — and returns the inclusive lower
// bound of the range holding every row that agrees with them. Pair it with
// UpperBound for the exclusive end.
//
// values[i] is a value of comps[i]'s LEAF field, so a probe on a reference
// index passes the target's key-field values straight through.
func (e *IndexEncoder) EncodePrefix(values []protoreflect.Value) ([]byte, error) {
	if len(values) > len(e.comps) {
		return nil, fmt.Errorf("index probe has %d values but the index has %d components", len(values), len(e.comps))
	}
	buf := make([]byte, 0, 8*len(values))
	for i, v := range values {
		c := e.comps[i]
		buf = appendComponent(buf, c, extractSingular(c.leafField(), v), true)
	}
	return buf, nil
}

// UpperBound is the exclusive end of the range of every entry starting with
// prefix. A nil result means "no upper bound" — prefix is all 0xff, so
// nothing sorts above it.
func UpperBound(prefix []byte) []byte {
	out := append([]byte(nil), prefix...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != 0xff {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}

// SpecHash fingerprints everything an index's stored bytes depend on and
// nothing else: the component paths, their directions, the payload shape each
// leaf resolves to, where the component fans out, and the encoding version. The
// fan-out position is derived from the path but not stated by it, so a field
// that becomes repeated keeps its path and changes how many entries a row has —
// exactly the drift a fingerprint exists to catch. A stored index whose hash no
// longer matches its declaration is stale, not wrong — its entries are simply
// not readable under the current spec, and the index rebuilds.
//
// The map-fan additions are APPENDED rather than folded into the existing
// fields, so an index shape that predates them fingerprints byte for byte as it
// did: the entries already stored for every reference, field mark and list
// index stay valid instead of all rebuilding at once for a feature they do not
// use.
//
// It is deliberately written in terms both languages state identically (proto
// field names, a payload-shape word) rather than in each language's own type
// enum, so client/src/query/indexkey.ts is a literal mirror. Nothing requires
// the two to agree — an index fingerprint never crosses the wire and each side
// only ever compares its own stored hash to its own declaration — but a
// fingerprint that differs between implementations for no reason is a trap
// for anyone reading them side by side.
func SpecHash(comps []IndexComponent) string {
	var b strings.Builder
	fmt.Fprintf(&b, "v%d", indexEncodingVersion)
	for _, c := range comps {
		b.WriteByte('|')
		for i, st := range c.fields {
			if i > 0 {
				b.WriteByte('.')
			}
			b.WriteString(string(st.fd.Name()))
			if st.fan && st.fd.IsMap() {
				b.WriteByte('.')
				b.WriteString(mapFanSegment)
			}
		}
		leaf := c.leafField()
		fans := fanPositions(c.fields)
		first := -1
		if len(fans) > 0 {
			first = fans[0]
		}
		fmt.Fprintf(&b, ":%s:%t:%t:%d", payloadShape(leaf), isUnsigned64(leaf), c.desc, first)
		if len(fans) > 1 {
			for _, at := range fans[1:] {
				fmt.Fprintf(&b, ":+%d", at)
			}
		}
		if c.isEntryKey() {
			// The selector is NOT rendered into the path above, so a `[key,
			// value]` layout fingerprints exactly as it did before the selector
			// was spelled out and its stored entries stay valid. The suffix is
			// still what tells `attrs.*.key` apart from `attrs.*.value`, which
			// have the same path and, on a map<string,string>, the same shape.
			b.WriteString(":key")
		}
	}
	return fingerprint(b.String())
}

// fanPositions lists the indices in fields that fan out, outermost first.
func fanPositions(fields []pathStep) []int {
	var out []int
	for i, st := range fields {
		if st.fan {
			out = append(out, i)
		}
	}
	return out
}

// payloadShape names the encoding a leaf's values take. Two proto kinds that
// share a shape (int32 and an enum, float and double) also share their bytes,
// so swapping one for the other must NOT invalidate the index.
func payloadShape(fd protoreflect.FieldDescriptor) string {
	switch singularKind(fd) {
	case rvString:
		return "string"
	case rvNumber:
		return "number"
	case rvBigint:
		return "bigint"
	case rvBool:
		return "bool"
	default:
		return "other"
	}
}

// fingerprint is FNV-1a/64 rendered as 16 hex digits. An index fingerprint
// only has to CHANGE when the spec does — it never crosses the wire and
// nothing authenticates against it — so a hash with no dependencies and a
// synchronous mirror in the client beats a cryptographic one.
func fingerprint(s string) string {
	const (
		offset uint64 = 14695981039346656037
		prime  uint64 = 1099511628211
	)
	h := offset
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime
	}
	return fmt.Sprintf("%016x", h)
}

func appendComponent(dst []byte, c IndexComponent, v rowValue, found bool) []byte {
	switch {
	case !found:
		return append(dst, tagMissing)
	case isNaN(v):
		return append(dst, tagNaN)
	case !comparableKind(v.kind):
		// Unreachable: CompileIndexComponent rejects every statically
		// incomparable leaf, and collaborative text — the one kind that can
		// degrade at runtime — outright. Encode it as missing rather than
		// panicking, since this runs on the write path.
		return append(dst, tagMissing)
	}
	dst = append(dst, tagPresent)
	start := len(dst)
	switch v.kind {
	case rvString:
		dst = appendIndexString(dst, v.str)
	case rvBool:
		if v.b {
			dst = append(dst, 0x01)
		} else {
			dst = append(dst, 0x00)
		}
	case rvNumber:
		dst = appendIndexFloat(dst, v.num)
	case rvBigint:
		dst = appendIndexBig(dst, v.big, isUnsigned64(c.leafField()))
	}
	if c.desc {
		invert(dst[start:])
	}
	return dst
}

// appendIndexString writes s as CESU-8: each UTF-16 code unit encoded as if
// it were a code point, which is what makes byte order equal
// CompareStringsUTF16's code-unit order.
func appendIndexString(dst []byte, s string) []byte {
	for _, r := range s {
		if r > 0xffff {
			r -= 0x10000
			dst = appendCodeUnit(dst, 0xd800+(r>>10))
			dst = appendCodeUnit(dst, 0xdc00+(r&0x3ff))
			continue
		}
		dst = appendCodeUnit(dst, r)
	}
	// A terminator no payload byte can contain, so no encoded string is ever
	// a prefix of another: "a" ends before "ab" diverges, and the desc
	// inversion — which only reverses order between strings where neither is
	// a prefix of the other — stays correct.
	return append(dst, 0x00, 0x00)
}

func appendCodeUnit(dst []byte, c rune) []byte {
	switch {
	case c == 0:
		// Escaped so a NUL cannot be read as half a terminator. 0xff keeps
		// the escape ABOVE the terminator, so "a" still sorts before
		// "a\u0000".
		return append(dst, 0x00, 0xff)
	case c < 0x80:
		return append(dst, byte(c))
	case c < 0x800:
		return append(dst, byte(0xc0|c>>6), byte(0x80|c&0x3f))
	default:
		return append(dst, byte(0xe0|c>>12), byte(0x80|(c>>6)&0x3f), byte(0x80|c&0x3f))
	}
}

// appendIndexFloat writes the standard order-preserving flip: negatives
// invert whole, non-negatives set the sign bit, so the unsigned byte order
// of the result is numeric order.
func appendIndexFloat(dst []byte, f float64) []byte {
	// -0.0 and +0.0 have different bit patterns but cmpFloat calls them
	// equal, so they must encode identically or the index would order two
	// rows the comparator ties.
	if f == 0 {
		f = 0
	}
	bits := math.Float64bits(f)
	if bits&(1<<63) != 0 {
		bits = ^bits
	} else {
		bits |= 1 << 63
	}
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], bits)
	return append(dst, tmp[:]...)
}

// appendIndexBig writes a 64-bit integer. A signed field flips the sign bit
// to bias it into unsigned order; an unsigned one is already in order. Which
// applies is a property of the field, so two entries in one index always
// agree.
func appendIndexBig(dst []byte, b *big.Int, unsigned bool) []byte {
	var u uint64
	if unsigned {
		u = b.Uint64()
	} else {
		u = uint64(b.Int64()) ^ (1 << 63)
	}
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], u)
	return append(dst, tmp[:]...)
}

func isUnsigned64(fd protoreflect.FieldDescriptor) bool {
	switch fd.Kind() {
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return true
	}
	return false
}

func invert(b []byte) {
	for i := range b {
		b[i] = ^b[i]
	}
}
