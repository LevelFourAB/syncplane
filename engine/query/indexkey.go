package query

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
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
// A component whose path runs through a repeated field has no single value per
// row, so such a row takes one POSITION PER ELEMENT — the posting list that
// makes "some assignee is Maya" a range read instead of a scan. Three rules
// keep that honest:
//
//   - Every component that fans out must fan out over the SAME repeated field,
//     and they fan out together, element by element. A cross product would pair
//     one element's id with another's role, and a query meant to describe one
//     element would match rows where no element does.
//   - A row whose list is empty or unset still writes exactly one entry, with
//     the missing tag at each fan-out component. Every live row occupies at
//     least one position in every index, which is what lets a range that does
//     NOT constrain the fan-out component still see every row.
//   - Duplicate elements collapse: they would produce identical bytes, and an
//     entry is a position, not a count.
//
// An ordering cannot be read off a fan-out component — a row sits at several
// places at once — so the planner only trusts one that the equality prefix has
// already pinned to a single element value.

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
type IndexComponent struct {
	fields []protoreflect.FieldDescriptor
	desc   bool
	// multiAt is the position in fields of the repeated field this component
	// fans out over, or -1 when the component yields one value per row. When it
	// is the last position the elements themselves are the values (a repeated
	// scalar); otherwise the rest of the path resolves against each element.
	multiAt int
}

// Multi reports whether a row can occupy several positions because of this
// component.
func (c IndexComponent) Multi() bool { return c.multiAt >= 0 }

// CompileIndexComponent resolves an index component's path against the
// message descriptor. It rejects the same leaves CompileOrder does — an
// index whose component could never take part in a comparison would sort
// every row into one bucket — plus collaborative text, which is comparable
// but would rewrite its entry on every keystroke.
//
// Unlike a condition path, this one may descend through a repeated field (and
// may end at one): that is what an index over "some element's value" is made
// of. At most one repeated field per component, since a second would need a
// cross product within the component itself.
func CompileIndexComponent(desc protoreflect.MessageDescriptor, path []string, descending bool) (IndexComponent, error) {
	fields, multiAt, err := resolveIndexFields(desc, path)
	if err != nil {
		return IndexComponent{}, err
	}
	// A path ending at a single-key reference extends to that key field, exactly
	// as a comparison on the reference does, so an index and a query on the same
	// reference agree on what they are keyed by. For a to-many reference that
	// makes the component fan out over the stored target keys.
	if refKeys := lww.RefKeyFields(fields[len(fields)-1]); len(refKeys) == 1 {
		fields = append(fields, refKeys[0])
	}
	leaf := fields[len(fields)-1]
	if multiAt == len(fields)-1 {
		// The leaf IS the repeated field, so the stored values are its elements
		// and it is the element kind that has to be comparable.
		if !comparableKind(singularKind(leaf)) {
			return IndexComponent{}, fmt.Errorf("path %v: %s cannot be indexed; %s",
				path, describeLeaf(leaf), indexListHint(path, leaf))
		}
	} else if err := requireComparable(path, fields); err != nil {
		return IndexComponent{}, err
	}
	if lww.IsText(leaf) {
		return IndexComponent{}, fmt.Errorf(
			"path %v: a collaborative text field cannot be indexed; every keystroke rewrites its value, so the entry would be rewritten with it",
			path)
	}
	return IndexComponent{fields: fields, desc: descending, multiAt: multiAt}, nil
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

// resolveIndexFields is resolveFields with one extra power: it may descend
// through a repeated message field, reporting where. The reference rule is
// unchanged — a to-many reference stores only its target's key fields, so those
// are the only ones addressable under it.
func resolveIndexFields(desc protoreflect.MessageDescriptor, path []string) ([]protoreflect.FieldDescriptor, int, error) {
	if len(path) == 0 {
		return nil, -1, fmt.Errorf("index component path is empty")
	}
	cur := desc
	multiAt := -1
	fields := make([]protoreflect.FieldDescriptor, 0, len(path))
	for i, seg := range path {
		if cur == nil {
			return nil, -1, fmt.Errorf("path %v: %q is not a message field, cannot descend into %q", path, path[i-1], seg)
		}
		fd := cur.Fields().ByName(protoreflect.Name(seg))
		if fd == nil {
			return nil, -1, fmt.Errorf("path %v: %s has no field %q", path, cur.FullName(), seg)
		}
		if i < len(path)-1 {
			if refKeys := lww.RefKeyFields(fd); refKeys != nil && !isKeyField(refKeys, path[i+1]) {
				return nil, -1, fmt.Errorf("path %v: cannot descend through reference %q into %q; only its key fields are addressable", path, seg, path[i+1])
			}
		}
		switch {
		case fd.IsMap():
			cur = nil
		case fd.IsList():
			if multiAt >= 0 {
				return nil, -1, fmt.Errorf("path %v: %q is a second repeated field on one index component; index a path through one list at a time",
					path, seg)
			}
			multiAt = i
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
		fields = append(fields, fd)
	}
	return fields, multiAt, nil
}

// IndexEncoder turns rows into the entries of one index. It is immutable and
// safe to share across goroutines.
type IndexEncoder struct {
	comps []IndexComponent
	// fanFields is the path to the repeated field every fan-out component
	// descends through, or nil when no component does. Holding it once is what
	// makes the components fan out TOGETHER: the list is resolved a single time
	// and every component reads the same element.
	fanFields []protoreflect.FieldDescriptor
}

// NewIndexEncoder builds an encoder over already-compiled components. An
// encoder with no components is legal and yields entries that are the row
// key alone.
//
// It fails when two components fan out over different repeated fields. Their
// cross product would be the only way to store both, and a cross product pairs
// values that never belonged to the same element — so such an index is refused
// at declaration rather than answering a subtly wrong question at read time.
func NewIndexEncoder(comps []IndexComponent) (*IndexEncoder, error) {
	var fan []protoreflect.FieldDescriptor
	for _, c := range comps {
		if !c.Multi() {
			continue
		}
		path := c.fields[:c.multiAt+1]
		if fan == nil {
			fan = path
			continue
		}
		if !sameFields(fan, path) {
			return nil, fmt.Errorf(
				"an index cannot fan out over both %s and %s; one repeated field per index",
				fieldPathString(fan), fieldPathString(path))
		}
	}
	return &IndexEncoder{comps: comps, fanFields: fan}, nil
}

// Components exposes the compiled components, for a planner deciding whether
// this index serves a query.
func (e *IndexEncoder) Components() []IndexComponent { return e.comps }

// Encode produces the entries msg occupies in this index — one per element of
// the fanned-out list, or a single entry when no component fans out. rowKey is
// the row's storage key, which terminates every entry so that entries are
// unique and tie exactly the way Order.Compare does.
//
// The result is never empty: a row whose list is empty or unset still takes one
// position, with the missing tag at each fan-out component, so every live row
// is reachable through every index.
func (e *IndexEncoder) Encode(msg protoreflect.Message, rowKey string) [][]byte {
	if e.fanFields == nil {
		return [][]byte{e.encodeAt(msg, rowKey, nil, 0)}
	}
	v, found := resolveValue(msg, e.fanFields)
	if !found || v.kind != rvList || v.list.Len() == 0 {
		return [][]byte{e.encodeAt(msg, rowKey, nil, 0)}
	}
	out := make([][]byte, 0, v.list.Len())
	seen := make(map[string]struct{}, v.list.Len())
	for i := 0; i < v.list.Len(); i++ {
		entry := e.encodeAt(msg, rowKey, v.list, i)
		// Duplicate elements encode identically. An entry is a position, not a
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

// encodeAt builds one entry, reading fan-out components from element i of list.
// A nil list means "no element", which is how a row with an empty list still
// gets its single entry.
func (e *IndexEncoder) encodeAt(msg protoreflect.Message, rowKey string, list protoreflect.List, i int) []byte {
	buf := make([]byte, 0, 16+8*len(e.comps))
	for _, c := range e.comps {
		var v rowValue
		var found bool
		switch {
		case !c.Multi():
			v, found = resolveValue(msg, c.fields)
		case list == nil:
			found = false
		case c.multiAt == len(c.fields)-1:
			// The leaf is the list itself, so the element IS the value.
			v, found = extractSingular(c.fields[c.multiAt], list.Get(i)), true
		default:
			v, found = resolveValue(list.Get(i).Message(), c.fields[c.multiAt+1:])
		}
		buf = appendComponent(buf, c, v, found)
	}
	return appendIndexString(buf, rowKey)
}

func fieldPathString(fields []protoreflect.FieldDescriptor) string {
	parts := make([]string, len(fields))
	for i, fd := range fields {
		parts[i] = string(fd.Name())
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
		buf = appendComponent(buf, c, extractSingular(c.fields[len(c.fields)-1], v), true)
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
		for i, fd := range c.fields {
			if i > 0 {
				b.WriteByte('.')
			}
			b.WriteString(string(fd.Name()))
		}
		leaf := c.fields[len(c.fields)-1]
		fmt.Fprintf(&b, ":%s:%t:%t:%d", payloadShape(leaf), isUnsigned64(leaf), c.desc, c.multiAt)
	}
	return fingerprint(b.String())
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
		dst = appendIndexBig(dst, v.big, isUnsigned64(c.fields[len(c.fields)-1]))
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
