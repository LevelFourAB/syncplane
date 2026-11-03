// Package textpath finds the collaborative-text documents of an entity row and
// reads and writes them by instance path.
//
// A (syncplane.v1.field_behavior).text field holds one document per row when
// the path to it crosses only singular messages, and one document per element
// when it crosses a keyed message list or a message-valued map. An instance
// path names one such document: the dotted schema path with the element key
// inserted after each list or map it crosses ("notes.<key>.body"), in the
// segment encoding of engine/internal/lww. That is the form a PATCH mask and a
// FieldDelta name a document by.
//
// Field and Instance enumerate the documents a row holds; Ref resolves one
// instance path given from outside. Nothing here is safe for concurrent use
// on the same message, as protoreflect messages are not.
package textpath

import (
	"fmt"
	"sort"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/levelfourab/syncplane/engine/internal/lww"
	"github.com/levelfourab/syncplane/engine/text"
	"github.com/levelfourab/syncplane/internal/protoopts"
)

// Field is one collaborative-text field of a message type, as Collect finds
// it. A Field is immutable and safe for concurrent use.
type Field struct {
	// Path is the dotted schema path, without element keys.
	Path  string
	steps []step
	// fds are the step descriptors, in order, held once so that instance
	// tails are subslices instead of per-call rebuilds.
	fds    []protoreflect.FieldDescriptor
	nested bool
}

// step is one field on a text field's path, plus the element identity when
// that field is a keyed message list. A map step is recognized by
// fd.IsMap(); any other step is a singular descent.
type step struct {
	fd        protoreflect.FieldDescriptor
	keyFields []protoreflect.FieldDescriptor
}

func (st step) container() bool { return st.fd.IsMap() || len(st.keyFields) > 0 }

// Nested reports whether the path to f crosses a keyed list or a map, so that
// a row holds one document per element instead of exactly one.
func (f Field) Nested() bool { return f.nested }

// Candidate is a text field Collect found, handed to the caller's check
// before Collect accepts it.
type Candidate struct {
	// Path is the dotted schema path.
	Path string
	// Field is the text field itself.
	Field protoreflect.FieldDescriptor
	// Oneof names the oneof the field or a message on its path is a member
	// of, or is empty when there is none.
	Oneof string
	// UnkeyedList is the dotted schema path of the innermost message list
	// without a key on the field's path, or is empty when there is none.
	UnkeyedList string
}

// Collect returns every text field of md, in field order with nested fields
// after the field that holds them.
//
// The walk descends through singular message fields, keyed and unkeyed
// message lists and message-valued maps, and enters no opaque field, no
// reference and no message type already on the path. It calls check for
// every text field it finds, oneof members and elements of unkeyed lists
// included, and returns the first error check returns, unchanged.
func Collect(md protoreflect.MessageDescriptor, check func(Candidate) error) ([]Field, error) {
	var out []Field
	if err := collect(md, "", "", "", nil, check, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func collect(
	desc protoreflect.MessageDescriptor,
	prefix, oneof, unkeyed string,
	steps []step,
	check func(Candidate) error,
	out *[]Field,
) error {
	fields := desc.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		behavior := protoopts.FieldBehavior(fd)
		memberOf := oneof
		if od := lww.OneofOf(fd); od != nil {
			memberOf = string(od.Name())
		}
		name := prefix + string(fd.Name())
		if behavior.GetText() {
			if err := check(Candidate{Path: name, Field: fd, Oneof: memberOf, UnkeyedList: unkeyed}); err != nil {
				return err
			}
			fieldSteps := make([]step, len(steps)+1)
			copy(fieldSteps, steps)
			fieldSteps[len(steps)] = step{fd: fd}
			*out = append(*out, newField(name, fieldSteps))
			continue
		}
		// The walk enters oneof members and unkeyed list elements only so that
		// check sees a text field there. Opaque messages are replaced whole and
		// references project down to key fields, so neither holds a document.
		var sub protoreflect.MessageDescriptor
		enclosing := unkeyed
		st := step{fd: fd}
		switch {
		case behavior.GetOpaque() || lww.IsRef(fd):
		case fd.IsMap():
			if fd.MapValue().Kind() == protoreflect.MessageKind {
				sub = fd.MapValue().Message()
			}
		case fd.IsList():
			if fd.Kind() == protoreflect.MessageKind {
				sub = fd.Message()
				st.keyFields = lww.KeyFieldsFor(fd)
				if len(st.keyFields) == 0 {
					enclosing = name
				}
			}
		case fd.Kind() == protoreflect.MessageKind:
			sub = fd.Message()
		}
		if sub == nil || onChain(steps, sub.FullName()) {
			continue
		}
		if err := collect(sub, name+".", memberOf, enclosing, append(steps, st), check, out); err != nil {
			return err
		}
	}
	return nil
}

func newField(path string, steps []step) Field {
	f := Field{Path: path, steps: steps, fds: make([]protoreflect.FieldDescriptor, len(steps))}
	for i, st := range steps {
		f.fds[i] = st.fd
		if st.container() {
			f.nested = true
		}
	}
	return f
}

// onChain reports whether the walk is already inside a message of type name,
// which stops it on a recursive schema. The type compared is the one the walk
// enters: a map field's value message, not its synthetic entry message.
func onChain(steps []step, name protoreflect.FullName) bool {
	for _, st := range steps {
		var md protoreflect.MessageDescriptor
		switch {
		case st.fd.IsMap():
			if st.fd.MapValue().Kind() == protoreflect.MessageKind {
				md = st.fd.MapValue().Message()
			}
		case st.fd.Kind() == protoreflect.MessageKind:
			md = st.fd.Message()
		}
		if md != nil && md.FullName() == name {
			return true
		}
	}
	return false
}

// Instance is one document position of a Field in one message. Set and Clear
// write through to the message the Instance came from.
type Instance struct {
	// Path is the instance path.
	Path   string
	holder protoreflect.Message
	tail   []protoreflect.FieldDescriptor
}

// Bytes returns the document bytes, or nil when the field or a singular
// message on the way to it is unset.
func (ti Instance) Bytes() []byte {
	cur := ti.holder
	for i, fd := range ti.tail {
		if i == len(ti.tail)-1 {
			return cur.Get(fd).Bytes()
		}
		if !cur.Has(fd) {
			return nil
		}
		cur = cur.Get(fd).Message()
	}
	return nil
}

// Set writes the document bytes, creating any unset singular message on the
// way. b is retained.
func (ti Instance) Set(b []byte) {
	cur := ti.holder
	for i, fd := range ti.tail {
		if i == len(ti.tail)-1 {
			cur.Set(fd, protoreflect.ValueOfBytes(b))
			return
		}
		cur = cur.Mutable(fd).Message()
	}
}

// Clear unsets the document. It creates nothing: an unset message on the way
// already means the document is unset.
func (ti Instance) Clear() {
	cur := ti.holder
	for i, fd := range ti.tail {
		if i == len(ti.tail)-1 {
			cur.Clear(fd)
			return
		}
		if !cur.Has(fd) {
			return
		}
		cur = cur.Mutable(fd).Message()
	}
}

// Instances returns every document position of f in msg, in walk order: list
// order, and map entries sorted by encoded key. A field that is not Nested
// has exactly one position, set or not; a Nested field has one per element
// msg holds, so none when a list or map on the way is empty or unset. Two
// elements with the same key give two instances with the same path.
//
// Enumeration creates nothing in msg.
func (f Field) Instances(msg protoreflect.Message) []Instance {
	if !f.nested {
		return []Instance{{Path: f.Path, holder: msg, tail: f.fds}}
	}
	var out []Instance
	f.walk(msg, 0, "", &out)
	return out
}

func (f Field) walk(cur protoreflect.Message, from int, prefix string, out *[]Instance) {
	next := -1
	for i := from; i < len(f.steps); i++ {
		if f.steps[i].container() {
			next = i
			break
		}
	}
	if next == -1 {
		path := prefix
		for i := from; i < len(f.steps); i++ {
			if i > from {
				path += "."
			}
			path += string(f.steps[i].fd.Name())
		}
		*out = append(*out, Instance{Path: path, holder: cur, tail: f.fds[from:]})
		return
	}
	// Holders are taken with Mutable so that Set writes through to msg. Each
	// one is checked with Has first, so the walk creates nothing.
	for i := from; i < next; i++ {
		if !cur.Has(f.steps[i].fd) {
			return
		}
		cur = cur.Mutable(f.steps[i].fd).Message()
		prefix += string(f.steps[i].fd.Name()) + "."
	}
	st := f.steps[next]
	if !cur.Has(st.fd) {
		return
	}
	prefix += string(st.fd.Name()) + "."
	if st.fd.IsMap() {
		mm := cur.Mutable(st.fd).Map()
		segs := make([]string, 0, mm.Len())
		keys := make(map[string]protoreflect.MapKey, mm.Len())
		mm.Range(func(mk protoreflect.MapKey, _ protoreflect.Value) bool {
			seg := lww.EncodeMapKeySegment(st.fd, mk)
			segs = append(segs, seg)
			keys[seg] = mk
			return true
		})
		sort.Strings(segs)
		for _, seg := range segs {
			f.walk(mm.Mutable(keys[seg]).Message(), next+1, prefix+seg+".", out)
		}
		return
	}
	list := cur.Mutable(st.fd).List()
	for j := 0; j < list.Len(); j++ {
		elem := list.Get(j).Message()
		f.walk(elem, next+1, prefix+lww.EncodeItemKey(elem, st.keyFields)+".", out)
	}
}

// Instances returns the instances of every field in fields, field by field,
// each in the order Field.Instances gives.
func Instances(fields []Field, msg protoreflect.Message) []Instance {
	var out []Instance
	for _, f := range fields {
		out = append(out, f.Instances(msg)...)
	}
	return out
}

// Snapshot returns the non-empty documents of msg by instance path. When two
// elements share a key, the first one wins. It returns nil for empty fields,
// and an empty map when msg holds no document.
//
// The returned slices are the ones msg holds; a later Set on msg replaces
// them without changing them.
func Snapshot(fields []Field, msg protoreflect.Message) map[string][]byte {
	if len(fields) == 0 {
		return nil
	}
	out := make(map[string][]byte, len(fields))
	for _, f := range fields {
		for _, ti := range f.Instances(msg) {
			if b := ti.Bytes(); len(b) > 0 {
				if _, ok := out[ti.Path]; !ok {
					out[ti.Path] = b
				}
			}
		}
	}
	return out
}

// Union unions every document of base into next in place, pairing documents
// by instance path. Where both hold a document, next gets the result of
// merge(path, baseDoc, nextDoc). Where only base holds one, next gets base's.
// A position only next holds keeps next's, and an element base holds and next
// does not stays absent.
//
// Union returns the first error merge returns, wrapped with the instance path
// as `text field "<path>": <err>`. next may then hold the merges done before
// it.
func Union(
	fields []Field,
	base, next protoreflect.Message,
	merge func(path string, base, next []byte) ([]byte, error),
) error {
	for _, f := range fields {
		var have map[string][]byte
		for _, bi := range f.Instances(base) {
			if b := bi.Bytes(); len(b) > 0 {
				if have == nil {
					have = make(map[string][]byte)
				}
				if _, ok := have[bi.Path]; !ok {
					have[bi.Path] = b
				}
			}
		}
		if have == nil {
			continue
		}
		for _, ni := range f.Instances(next) {
			b := have[ni.Path]
			if len(b) == 0 {
				continue
			}
			n := ni.Bytes()
			if len(n) == 0 {
				ni.Set(b)
				continue
			}
			merged, err := merge(ni.Path, b, n)
			if err != nil {
				return fmt.Errorf("text field %q: %w", ni.Path, err)
			}
			ni.Set(merged)
		}
	}
	return nil
}

// KeepClearedVersions replaces each document prev holds and next has cleared
// with an empty document that carries the cleared one's version vector, so a
// writer that reads the cleared field allocates counters past the old ones.
// prev is a Snapshot taken before the write; next is edited in place. A
// document whose element next no longer holds is not touched.
//
// It returns an error, as `text field "<path>": stored document: <err>`, when
// a document in prev does not decode.
func KeepClearedVersions(fields []Field, prev map[string][]byte, next protoreflect.Message) error {
	if len(prev) == 0 {
		return nil
	}
	for _, f := range fields {
		for _, ti := range f.Instances(next) {
			before := prev[ti.Path]
			if len(before) == 0 || len(ti.Bytes()) > 0 {
				continue
			}
			d, err := text.Decode(before, "")
			if err != nil {
				return fmt.Errorf("text field %q: stored document: %w", ti.Path, err)
			}
			empty := text.New("")
			empty.ObserveVersion(d.VersionVector())
			ti.Set(empty.Encode())
		}
	}
	return nil
}
