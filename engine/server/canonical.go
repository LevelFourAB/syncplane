package server

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"slices"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// The canonical encoding of a message is the input of every hash the engine
// persists for the access model: Cursor.access_hash, Cursor.access_policy and
// CallerAccess.version. It is defined here, apart from the protobuf wire
// format, whose deterministic output protobuf-go does not promise to keep
// across releases. A hash that changed with a library upgrade would make every
// client resync or reset at once.
//
// The encoding reads the message's field values, not its encoded bytes:
//
//   - each field that is set, in field-number order: its number as a
//     uvarint, then its value;
//   - a list: its length as a uvarint, then each element;
//   - a map: its length as a uvarint, then each entry's key and value, in the
//     order of the keys' encodings;
//   - a bool: one byte, 0 or 1;
//   - a signed integer or an enum: the zigzag uvarint of its value;
//   - an unsigned integer: its uvarint;
//   - a float or a double: its IEEE 754 bits, big-endian, in 4 or 8 bytes;
//   - a string or bytes: the length as a uvarint, then the bytes;
//   - a message: the length of its encoding as a uvarint, then the encoding.
//
// A field that is not set adds nothing, so a field added to a message later
// leaves the encoding of every value that does not set it unchanged. Fields
// the build does not know add nothing either. canonical_internal_test.go pins
// the encoding byte for byte.

// appendCanonical appends the canonical encoding of m to b.
func appendCanonical(b []byte, m protoreflect.Message) []byte {
	fields := m.Descriptor().Fields()
	order := make([]protoreflect.FieldDescriptor, fields.Len())
	for i := range order {
		order[i] = fields.Get(i)
	}
	slices.SortFunc(order, func(x, y protoreflect.FieldDescriptor) int {
		return int(x.Number()) - int(y.Number())
	})
	for _, fd := range order {
		if !m.Has(fd) {
			continue
		}
		b = binary.AppendUvarint(b, uint64(fd.Number()))
		v := m.Get(fd)
		switch {
		case fd.IsList():
			l := v.List()
			b = binary.AppendUvarint(b, uint64(l.Len()))
			for i := range l.Len() {
				b = appendCanonicalValue(b, fd, l.Get(i))
			}
		case fd.IsMap():
			type entry struct{ key, value []byte }
			var entries []entry
			v.Map().Range(func(k protoreflect.MapKey, mv protoreflect.Value) bool {
				entries = append(entries, entry{
					key:   appendCanonicalValue(nil, fd.MapKey(), k.Value()),
					value: appendCanonicalValue(nil, fd.MapValue(), mv),
				})
				return true
			})
			slices.SortFunc(entries, func(x, y entry) int { return bytes.Compare(x.key, y.key) })
			b = binary.AppendUvarint(b, uint64(len(entries)))
			for _, e := range entries {
				b = append(append(b, e.key...), e.value...)
			}
		default:
			b = appendCanonicalValue(b, fd, v)
		}
	}
	return b
}

// appendCanonicalValue appends the canonical encoding of v, one value of the
// kind of fd, to b.
func appendCanonicalValue(b []byte, fd protoreflect.FieldDescriptor, v protoreflect.Value) []byte {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		if v.Bool() {
			return append(b, 1)
		}
		return append(b, 0)
	case protoreflect.EnumKind:
		return binary.AppendVarint(b, int64(v.Enum()))
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return binary.AppendVarint(b, v.Int())
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind, protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return binary.AppendUvarint(b, v.Uint())
	case protoreflect.FloatKind:
		return binary.BigEndian.AppendUint32(b, math.Float32bits(float32(v.Float())))
	case protoreflect.DoubleKind:
		return binary.BigEndian.AppendUint64(b, math.Float64bits(v.Float()))
	case protoreflect.StringKind:
		s := v.String()
		return append(binary.AppendUvarint(b, uint64(len(s))), s...)
	case protoreflect.BytesKind:
		raw := v.Bytes()
		return append(binary.AppendUvarint(b, uint64(len(raw))), raw...)
	default: // MessageKind, GroupKind
		inner := appendCanonical(nil, v.Message())
		return append(binary.AppendUvarint(b, uint64(len(inner))), inner...)
	}
}

// canonicalHash returns the first 8 bytes, big-endian, of the SHA-256 of the
// length of domain as a uvarint, domain, and the canonical encoding of m.
// domain keeps the hashes of different records apart.
func canonicalHash(domain string, m protoreflect.Message) uint64 {
	raw := append(binary.AppendUvarint(nil, uint64(len(domain))), domain...)
	raw = appendCanonical(raw, m)
	sum := sha256.Sum256(raw)
	return binary.BigEndian.Uint64(sum[:8])
}
