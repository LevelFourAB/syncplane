package rebase

import (
	"bytes"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/levelfourab/syncplane/engine/internal/lww"
	"github.com/levelfourab/syncplane/engine/internal/textpath"
	"github.com/levelfourab/syncplane/engine/text"
)

// Model is what replay needs to know about one entity. It is immutable after
// NewModel and safe for concurrent use, so one Model per entity can be built
// once and shared.
type Model struct {
	desc    protoreflect.MessageDescriptor
	msgType protoreflect.MessageType
	text    []textpath.Field
	refs    []*lww.Ref
	mergers lww.Mergers
}

// NewModel describes the entity whose rows are messages of md.
//
// mergers holds the entity's app-registered merge hooks, keyed by the dotted
// schema path of an opaque field, as the engine's WithMerger declares them; nil
// means none. Every collaborative-text field gets the text union without being
// named. The map is copied.
//
// NewModel returns an error for a text field the engine refuses to register:
// one that is not a singular bytes field, or that sits inside a oneof or an
// unkeyed message list. It also returns an error when mergers names a text
// field.
func NewModel(md protoreflect.MessageDescriptor, mergers lww.Mergers) (*Model, error) {
	textFields, err := textpath.Collect(md, checkTextField)
	if err != nil {
		return nil, fmt.Errorf("rebase: entity %s: %w", md.FullName(), err)
	}
	m := &Model{desc: md, msgType: messageType(md), text: textFields}
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		m.refs = append(m.refs, lww.RefsOf(fields.Get(i))...)
	}
	if len(mergers) > 0 || len(m.text) > 0 {
		m.mergers = make(lww.Mergers, len(mergers)+len(m.text))
		for path, mg := range mergers {
			m.mergers[path] = mg
		}
		for _, tf := range m.text {
			if _, ok := m.mergers[tf.Path]; ok {
				return nil, fmt.Errorf("rebase: entity %s: a merger names text field %q", md.FullName(), tf.Path)
			}
			m.mergers[tf.Path] = lww.Merger{Fn: text.Merge, MergeAbsentBase: true}
		}
	}
	return m, nil
}

// Descriptor returns the message descriptor the Model was built from.
func (m *Model) Descriptor() protoreflect.MessageDescriptor { return m.desc }

// HasText reports whether the entity declares a collaborative-text field.
func (m *Model) HasText() bool { return len(m.text) > 0 }

// TextDocuments returns every non-empty collaborative-text document of an
// encoded row, keyed by instance path: the dotted schema path, with the element
// key inserted after each keyed list or map the path crosses
// ("notes.<key>.body"). That is the form a PATCH mask and a FieldDelta name a
// document by. It returns nil for an entity without text fields, and an error
// when row does not decode.
func (m *Model) TextDocuments(row []byte) (map[string][]byte, error) {
	if len(m.text) == 0 {
		return nil, nil
	}
	msg, err := m.decode(row)
	if err != nil {
		return nil, err
	}
	return textpath.Snapshot(m.text, msg), nil
}

// Equal reports whether a and b are the same row state: both absent, or both
// present with the same field values. Two encodings of one message compare
// equal, which bytes.Equal does not promise for a row with a map field. A
// value that does not decode equals only identical bytes.
func (m *Model) Equal(a, b Row) bool {
	if a.Present != b.Present {
		return false
	}
	if !a.Present || bytes.Equal(a.Value, b.Value) {
		return true
	}
	am, err := m.decode(a.Value)
	if err != nil {
		return false
	}
	bm, err := m.decode(b.Value)
	if err != nil {
		return false
	}
	ab, err := marshal(am)
	if err != nil {
		return false
	}
	bb, err := marshal(bm)
	if err != nil {
		return false
	}
	return bytes.Equal(ab, bb)
}

// ClearUnits returns the encoded row value with each unit that units names
// cleared: a top-level field by its name, or every field of a oneof by the
// oneof's name. A name that is neither is ignored. It returns value unchanged
// when units is empty, and a deterministic encoding otherwise. It returns an
// error when value does not decode.
func (m *Model) ClearUnits(value []byte, units []string) ([]byte, error) {
	if len(units) == 0 {
		return value, nil
	}
	msg, err := m.decode(value)
	if err != nil {
		return nil, err
	}
	for _, name := range units {
		if od := m.desc.Oneofs().ByName(protoreflect.Name(name)); od != nil {
			if fd := msg.WhichOneof(od); fd != nil {
				msg.Clear(fd)
			}
			continue
		}
		if fd := m.desc.Fields().ByName(protoreflect.Name(name)); fd != nil {
			msg.Clear(fd)
		}
	}
	return marshal(msg)
}

// messageType prefers the generated type registered for md, which is faster
// than a dynamic message, and falls back to dynamicpb for a descriptor no
// generated type carries (a schema registered at runtime).
func messageType(md protoreflect.MessageDescriptor) protoreflect.MessageType {
	if mt, err := protoregistry.GlobalTypes.FindMessageByName(md.FullName()); err == nil && mt.Descriptor() == md {
		return mt
	}
	return dynamicpb.NewMessageType(md)
}

func (m *Model) decode(b []byte) (protoreflect.Message, error) {
	msg := m.msgType.New()
	if err := proto.Unmarshal(b, msg.Interface()); err != nil {
		return nil, fmt.Errorf("rebase: decode %s: %w", m.desc.FullName(), err)
	}
	return msg, nil
}

// marshal is the one encoder of this package. Deterministic, so a row this
// package builds twice from the same inputs is the same bytes both times.
func marshal(msg protoreflect.Message) ([]byte, error) {
	return proto.MarshalOptions{Deterministic: true}.Marshal(msg.Interface())
}

// projectRefs strips every reference stub in msg down to its target's key
// fields, as the engine does before it stores a row. Only top-level fields
// hold references.
func (m *Model) projectRefs(msg protoreflect.Message) {
	for _, ref := range m.refs {
		if !msg.Has(ref.Field) {
			continue
		}
		project := func(value protoreflect.Message) {
			stub, ok := ref.Stub(value)
			if !ok {
				return
			}
			keep := make(map[protoreflect.FieldNumber]bool, len(ref.KeyFields))
			for _, kf := range ref.KeyFields {
				keep[kf.Number()] = true
			}
			fields := stub.Descriptor().Fields()
			for i := 0; i < fields.Len(); i++ {
				if fd := fields.Get(i); !keep[fd.Number()] {
					stub.Clear(fd)
				}
			}
		}
		if ref.Field.IsList() {
			list := msg.Mutable(ref.Field).List()
			for i := 0; i < list.Len(); i++ {
				project(list.Get(i).Message())
			}
			continue
		}
		project(msg.Mutable(ref.Field).Message())
	}
}

// checkTextField refuses a text field the engine refuses to register: one that
// is not a singular bytes field, or that sits inside a oneof or an unkeyed
// message list.
func checkTextField(c textpath.Candidate) error {
	fd := c.Field
	switch {
	case fd.IsList() || fd.IsMap() || fd.Kind() != protoreflect.BytesKind:
		return fmt.Errorf("text field %q must be a singular bytes field", c.Path)
	case c.Oneof != "":
		return fmt.Errorf("text field %q is inside a oneof", c.Path)
	case c.UnkeyedList != "":
		return fmt.Errorf("text field %q is inside a message list without a key", c.Path)
	}
	return nil
}

// covers reports whether a PATCH mask writes the document at an instance
// path: a mask path names it, or names a field or element it sits inside.
// Segments compare whole, escapes intact, so a key holding an escaped dot
// cannot pass for a path boundary.
func covers(mask []string, path string) bool {
	segs := lww.SplitPath(path)
	for _, p := range mask {
		ps := lww.SplitPath(p)
		if len(ps) > len(segs) {
			continue
		}
		match := true
		for i := range ps {
			if ps[i] != segs[i] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
