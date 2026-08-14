package query

import (
	"errors"
	"fmt"
	"unicode/utf16"

	"google.golang.org/protobuf/reflect/protoreflect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
)

// Order is a compiled multi-key row ordering.
type Order struct {
	keys []orderKey
	// cursorEnc encodes a row's position in this ordering; see Order.Cursor.
	// Built once at compile time because a query encodes one per candidate row.
	cursorEnc *IndexEncoder
	// cursorSpec is SpecHash over the same components, the ordering half of
	// Shape.
	cursorSpec string
}

type orderKey struct {
	fields []pathStep
	desc   bool
	// rank orders by match rank instead of a field — the relevance key. The
	// other members are unused when it is set.
	rank *Matcher
}

// CompileOrder validates order keys against the message descriptor and
// resolves their paths once. An empty key list is valid (ties on row key). A
// key on an incomparable leaf is rejected rather than accepted: it would tie
// every pair, so the result would silently come back in row-key order.
//
// A relevance key is rejected here: it binds to the predicate's match
// condition, which this surface does not hold. Use CompileOrderWith.
func CompileOrder(desc protoreflect.MessageDescriptor, keys []*syncplanev1.OrderKey) (*Order, error) {
	return CompileOrderWith(desc, keys, nil)
}

// CompileOrderWith is CompileOrder for orderings that may rank by relevance:
// a relevance key resolves against predicate, which must contain exactly one
// match condition. A nil predicate (or one without a match) rejects relevance
// keys with an error naming what is missing.
func CompileOrderWith(desc protoreflect.MessageDescriptor, keys []*syncplanev1.OrderKey, predicate *Compiled) (*Order, error) {
	o := &Order{keys: make([]orderKey, 0, len(keys))}
	for _, k := range keys {
		if k.GetRelevance() {
			if len(k.GetPath()) > 0 {
				return nil, fmt.Errorf("a relevance order key takes no path, got %v", k.GetPath())
			}
			var ms []*Matcher
			if predicate != nil {
				ms = predicate.Matchers()
			}
			if len(ms) != 1 {
				return nil, fmt.Errorf("ordering by relevance needs exactly one match condition in the predicate, found %d", len(ms))
			}
			switch k.GetDir() {
			case syncplanev1.Direction_DIRECTION_ASC, syncplanev1.Direction_DIRECTION_DESC:
			default:
				return nil, fmt.Errorf("relevance order key needs a direction")
			}
			o.keys = append(o.keys, orderKey{rank: ms[0], desc: k.GetDir() == syncplanev1.Direction_DIRECTION_DESC})
			continue
		}
		fields, err := resolveFields(desc, k.GetPath())
		if err != nil {
			return nil, err
		}
		if err := requireComparable(k.GetPath(), fields); err != nil {
			return nil, err
		}
		switch k.GetDir() {
		case syncplanev1.Direction_DIRECTION_ASC, syncplanev1.Direction_DIRECTION_DESC:
		default:
			return nil, fmt.Errorf("order key %v needs a direction", k.GetPath())
		}
		o.keys = append(o.keys, orderKey{fields: fields, desc: k.GetDir() == syncplanev1.Direction_DIRECTION_DESC})
	}
	comps := o.cursorComponents()
	enc, err := NewIndexEncoder(comps)
	if err != nil {
		return nil, err
	}
	o.cursorEnc = enc
	o.cursorSpec = SpecHash(comps)
	return o, nil
}

// Compare mirrors the client's compareRows: values rank real < NaN <
// missing and the rank decides before the direction does, so NaN and missing
// both sort last under asc and desc alike; equal non-real ranks and real
// ties fall through to the next key and finally to the row key ascending
// (UTF-16 order).
func (o *Order) Compare(a, b protoreflect.Message, keyA, keyB string) int {
	for _, k := range o.keys {
		if k.rank != nil {
			ra, rb := k.rank.Rank(a), k.rank.Rank(b)
			if ra != rb {
				c := 1
				if ra < rb {
					c = -1
				}
				if k.desc {
					return -c
				}
				return c
			}
			continue
		}
		va, foundA := resolveValue(a, k.fields)
		vb, foundB := resolveValue(b, k.fields)
		ra, rb := orderRank(va, foundA), orderRank(vb, foundB)
		if ra != rb {
			if ra < rb {
				return -1
			}
			return 1
		}
		if ra != rankReal {
			// Both NaN or both missing: nothing to distinguish them by.
			continue
		}
		c, ok := compareScalar(va, vb)
		if !ok {
			// CompileOrder rejects incomparable leaves and orderRank has
			// already taken NaN out, so this only fires when two rows disagree
			// at runtime — a text field that decodes in one row and is corrupt
			// in the other. Tie rather than error: an undecodable document is a
			// data problem, not a bad query.
			continue
		}
		if c != 0 {
			if k.desc {
				return -c
			}
			return c
		}
	}
	return CompareStringsUTF16(keyA, keyB)
}

// Order ranks, the coarse bucket a resolved value falls into before its
// value is looked at. The rank is compared BEFORE the key's direction is
// applied, which is what keeps NaN and missing at the end of the result under
// both asc and desc — and what lets the index encoder give them fixed tags
// (indexkey.go) outside the descending inversion.
const (
	rankReal    = 0
	rankNaN     = 1
	rankMissing = 2
)

func orderRank(v rowValue, found bool) int {
	switch {
	case !found:
		return rankMissing
	case isNaN(v):
		return rankNaN
	default:
		return rankReal
	}
}

// Less is Compare < 0, for sort.Slice.
func (o *Order) Less(a, b protoreflect.Message, keyA, keyB string) bool {
	return o.Compare(a, b, keyA, keyB) < 0
}

// Query cursors: a row's position in an ordering, as opaque bytes.
//
// A cursor says WHERE a page ended rather than how many rows to discard before
// it, so the next page is unaffected by rows inserted ahead of it. It is the
// index entry (indexkey.go) the row would occupy in an index whose components
// are exactly this ordering's keys, framed with a version byte and a shape
// fingerprint:
//
//	cursor := 0x01 shape entry
//	shape  := 16 ASCII hex digits — Shape, over the entity and the order keys
//
// Byte order over entries equals Compare order, which is the one property the
// entry format guarantees and the only one a cursor needs. So "after this
// cursor" is bytes.Compare(rowEntry, cursorEntry) > 0 and "before" is < 0, and
// the same bytes serve as either bound.
//
// Unlike an index, a cursor may be built over a collaborative-text order key.
// CompileIndexComponent rejects one because a keystroke would rewrite the
// stored entry; a cursor is computed and thrown away, so that cost never
// arises, and resolveValue projects text to its plain string for the
// comparator and the encoder alike.

// cursorVersion is the first byte of every encoded cursor. Bumping it makes
// every outstanding cursor decode as unreadable rather than as something else.
const cursorVersion = 0x01

// cursorShapeHexLen is the width of the shape fingerprint inside a cursor.
const cursorShapeHexLen = 16

// ErrCursorShape reports a cursor minted for a different entity or a different
// ordering than the query replaying it. Nothing is lost by it: a query run
// without the cursor issues one that fits.
var ErrCursorShape = errors.New("query: cursor belongs to a different entity or ordering")

// cursorComponents restates the order keys as index components. The two are
// structurally the same thing — a resolved path and a direction — and building
// them here rather than through CompileIndexComponent is what admits a
// collaborative-text order key.
func (o *Order) cursorComponents() []IndexComponent {
	comps := make([]IndexComponent, len(o.keys))
	for i, k := range o.keys {
		comps[i] = IndexComponent{fields: k.fields, desc: k.desc, rank: k.rank}
	}
	return comps
}

// Shape fingerprints the (entity, ordering) pair a cursor is valid against, as
// 16 hex digits. It is a non-security fingerprint like SpecHash: a mismatch
// costs a reissued cursor, so nothing escalates if two shapes collide.
func (o *Order) Shape(entity string) string {
	return fingerprint(o.cursorSpec + "|" + entity)
}

// Cursor is msg's position in this ordering, unframed — the bytes rows are
// compared against. EncodeCursor is the form a client is handed.
//
// It cannot fail: an order key never fans out, because CompileOrder rejects the
// repeated and map leaves that would make it, so a row occupies exactly one
// position.
func (o *Order) Cursor(msg protoreflect.Message, rowKey string) []byte {
	return o.cursorEnc.encodeAt(msg, rowKey, nil)
}

// EncodeCursor frames msg's position for a client: the version byte, the shape
// for entity, and the entry Cursor returns.
func (o *Order) EncodeCursor(entity string, msg protoreflect.Message, rowKey string) []byte {
	entry := o.Cursor(msg, rowKey)
	out := make([]byte, 0, 1+cursorShapeHexLen+len(entry))
	out = append(out, cursorVersion)
	out = append(out, o.Shape(entity)...)
	return append(out, entry...)
}

// DecodeCursor unwraps a cursor a client sent back, returning the entry to
// compare rows against. It reports ErrCursorShape for a cursor belonging to
// another entity or ordering, and a descriptive error for bytes that are not a
// cursor at all.
//
// It does not bound the length: a caller handling untrusted input caps that
// before calling, since a malformed cursor is otherwise as much work as it is
// long.
func (o *Order) DecodeCursor(entity string, cursor []byte) ([]byte, error) {
	if len(cursor) < 1+cursorShapeHexLen {
		return nil, fmt.Errorf("query: cursor is %d bytes, too short to be one", len(cursor))
	}
	if cursor[0] != cursorVersion {
		return nil, fmt.Errorf("query: cursor version %#02x is not %#02x", cursor[0], cursorVersion)
	}
	if string(cursor[1:1+cursorShapeHexLen]) != o.Shape(entity) {
		return nil, ErrCursorShape
	}
	entry := cursor[1+cursorShapeHexLen:]
	if _, _, err := o.cursorEnc.SplitEntry(entry); err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	return entry, nil
}

// CursorPrefix is the component half of an entry from Cursor or DecodeCursor:
// the position without the row-key tiebreak. An index seek composes this rather
// than the whole entry — see IndexEncoder.SplitEntry.
func (o *Order) CursorPrefix(entry []byte) ([]byte, error) {
	comps, _, err := o.cursorEnc.SplitEntry(entry)
	return comps, err
}

// CompareStringsUTF16 orders strings by UTF-16 code unit — what JS `<`/`>`
// do — instead of Go's native UTF-8 byte order. The two diverge when
// exactly one side contains astral-plane characters (surrogate pairs start
// at 0xD800, below later BMP code points; in UTF-8 astral characters sort
// above all BMP ones). The shared vectors pin the astral cases.
//
// One divergence this cannot close: a JS string holding a LONE surrogate is
// not valid UTF-8, so it reaches the server as U+FFFD. The client compares
// the original code unit and the server compares the replacement, and no
// protojson vector can express the difference — a query filtering on such a
// string can legitimately disagree between optimistic local evaluation and
// the server's answer.
func CompareStringsUTF16(a, b string) int {
	// Identical byte order and code-unit order for ASCII; the cheap check
	// covers the common case (row keys, ids).
	if isASCII(a) && isASCII(b) {
		switch {
		case a < b:
			return -1
		case a > b:
			return 1
		default:
			return 0
		}
	}
	ua := utf16.Encode([]rune(a))
	ub := utf16.Encode([]rune(b))
	for i := 0; i < len(ua) && i < len(ub); i++ {
		switch {
		case ua[i] < ub[i]:
			return -1
		case ua[i] > ub[i]:
			return 1
		}
	}
	switch {
	case len(ua) < len(ub):
		return -1
	case len(ua) > len(ub):
		return 1
	default:
		return 0
	}
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}
