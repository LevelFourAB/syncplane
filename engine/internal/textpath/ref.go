package textpath

import (
	"fmt"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/levelfourab/syncplane/engine/internal/lww"
)

// Ref addresses one text document inside a message by instance path. The
// zero Ref addresses nothing; use ParseRef. A Ref is immutable and safe for
// concurrent use.
type Ref struct {
	path  string
	steps []refStep
}

type refStep struct {
	fd protoreflect.FieldDescriptor
	// mapKey addresses the entry of a map step.
	mapKey protoreflect.MapKey
	// itemKey (encoded form) and keyFields address the element of a keyed
	// list step.
	itemKey   string
	keyFields []protoreflect.FieldDescriptor
}

// ParseRef resolves an instance path against desc. It returns an error, as
// `<message name> has no text field "<path>"`, when path does not end on a
// (syncplane.v1.field_behavior).text field, names a field desc does not have,
// or crosses a list without a key, a map without message values, or a map
// key that does not decode.
func ParseRef(desc protoreflect.MessageDescriptor, path string) (Ref, error) {
	bad := func() (Ref, error) {
		return Ref{}, fmt.Errorf("%s has no text field %q", desc.FullName(), path)
	}
	segs := lww.SplitPath(path)
	ref := Ref{path: path}
	cur := desc
	for i := 0; i < len(segs); i++ {
		fd := cur.Fields().ByName(protoreflect.Name(segs[i]))
		if fd == nil {
			return bad()
		}
		last := i == len(segs)-1
		st := refStep{fd: fd}
		switch {
		case fd.IsMap():
			if last || fd.MapValue().Kind() != protoreflect.MessageKind {
				return bad()
			}
			i++
			mk, err := lww.DecodeMapKey(fd, segs[i])
			if err != nil {
				return bad()
			}
			st.mapKey = mk
			cur = fd.MapValue().Message()
			last = i == len(segs)-1
		case fd.IsList():
			st.keyFields = lww.KeyFieldsFor(fd)
			if last || len(st.keyFields) == 0 {
				return bad()
			}
			i++
			st.itemKey = segs[i]
			cur = fd.Message()
			last = i == len(segs)-1
		case fd.Kind() == protoreflect.MessageKind:
			cur = fd.Message()
		case !last:
			return bad()
		}
		if last && (fd.IsMap() || fd.IsList() || !lww.IsText(fd)) {
			return bad()
		}
		ref.steps = append(ref.steps, st)
	}
	return ref, nil
}

// Bytes returns the document bytes in msg, or nil when the field, or a
// message, list element or map entry on the way to it, is absent.
func (r Ref) Bytes(msg protoreflect.Message) []byte {
	cur := msg
	for i, st := range r.steps {
		if i == len(r.steps)-1 {
			return cur.Get(st.fd).Bytes()
		}
		if !cur.Has(st.fd) {
			return nil
		}
		switch {
		case st.fd.IsMap():
			mm := cur.Get(st.fd).Map()
			if !mm.Has(st.mapKey) {
				return nil
			}
			cur = mm.Get(st.mapKey).Message()
		case st.fd.IsList():
			elem, ok := findElem(cur.Get(st.fd).List(), st.keyFields, st.itemKey)
			if !ok {
				return nil
			}
			cur = elem
		default:
			cur = cur.Get(st.fd).Message()
		}
	}
	return nil
}

// Set writes the document bytes in msg, creating any unset singular message
// on the way. value is retained. A list element or map entry on the way must
// already be in msg, because an element is more than its key: Set returns an
// error, as `text field "<path>" addresses a list element the value does not
// carry` (or `a map entry`), when one is missing. Set may have created
// singular messages in msg before it returns that error.
func (r Ref) Set(msg protoreflect.Message, value []byte) error {
	cur := msg
	for i, st := range r.steps {
		if i == len(r.steps)-1 {
			cur.Set(st.fd, protoreflect.ValueOfBytes(value))
			return nil
		}
		switch {
		case st.fd.IsMap():
			mm := cur.Mutable(st.fd).Map()
			if !mm.Has(st.mapKey) {
				return fmt.Errorf("text field %q addresses a map entry the value does not carry", r.path)
			}
			cur = mm.Mutable(st.mapKey).Message()
		case st.fd.IsList():
			elem, ok := findElem(cur.Mutable(st.fd).List(), st.keyFields, st.itemKey)
			if !ok {
				return fmt.Errorf("text field %q addresses a list element the value does not carry", r.path)
			}
			cur = elem
		default:
			cur = cur.Mutable(st.fd).Message()
		}
	}
	return nil
}

// findElem returns the first element of list whose encoded key is itemKey.
func findElem(
	list protoreflect.List,
	keyFields []protoreflect.FieldDescriptor,
	itemKey string,
) (protoreflect.Message, bool) {
	for i := 0; i < list.Len(); i++ {
		elem := list.Get(i).Message()
		if lww.EncodeItemKey(elem, keyFields) == itemKey {
			return elem, true
		}
	}
	return nil, false
}
