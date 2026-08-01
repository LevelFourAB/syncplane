package query

import (
	"fmt"
	"unicode/utf16"

	"google.golang.org/protobuf/reflect/protoreflect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
)

// Order is a compiled multi-key row ordering.
type Order struct {
	keys []orderKey
}

type orderKey struct {
	fields []protoreflect.FieldDescriptor
	desc   bool
}

// CompileOrder validates order keys against the message descriptor and
// resolves their paths once. An empty key list is valid (ties on row key). A
// key on an incomparable leaf is rejected rather than accepted: it would tie
// every pair, so the result would silently come back in row-key order.
func CompileOrder(desc protoreflect.MessageDescriptor, keys []*syncplanev1.OrderKey) (*Order, error) {
	o := &Order{keys: make([]orderKey, 0, len(keys))}
	for _, k := range keys {
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
	return o, nil
}

// Compare mirrors the client's compareRows: values rank real < NaN <
// missing and the rank decides before the direction does, so NaN and missing
// both sort last under asc and desc alike; equal non-real ranks and real
// ties fall through to the next key and finally to the row key ascending
// (UTF-16 order).
func (o *Order) Compare(a, b protoreflect.Message, keyA, keyB string) int {
	for _, k := range o.keys {
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
