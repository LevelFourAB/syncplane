package connect

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/levelfourab/syncplane/engine/internal/lww"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/text"
)

// fieldDiff returns the names of top-level fields that differ between base
// and next (messages of the same type) — the field-mask granularity the
// engine's per-field LWW resolves at. Collaborative-text fields are skipped:
// sources report those as plain strings (Item.TextFields), never as encoded
// documents on the message, so the byte comparison here would be meaningless
// (ingest diffs them against the base's decoded text instead).
//
// A oneof is diffed as one field and named by its group, which is the only
// form the merge accepts (see engine/internal/lww).
func fieldDiff(base, next proto.Message) []string {
	bm, nm := base.ProtoReflect(), next.ProtoReflect()
	desc := bm.Descriptor()
	fields := desc.Fields()
	var paths []string
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if lww.IsText(fd) || lww.OneofOf(fd) != nil {
			continue
		}
		if !lww.EqualField(bm, nm, fd) {
			paths = append(paths, string(fd.Name()))
		}
	}
	oneofs := desc.Oneofs()
	for i := 0; i < oneofs.Len(); i++ {
		od := oneofs.Get(i)
		if od.IsSynthetic() {
			continue
		}
		bf, nf := bm.WhichOneof(od), nm.WhichOneof(od)
		if bf != nf || (bf != nil && !lww.EqualField(bm, nm, bf)) {
			paths = append(paths, string(od.Name()))
		}
	}
	return paths
}

// applyFields copies the named top-level fields from src onto dst, clearing
// fields src does not carry. A path may name a oneof group, in which case the
// whole group moves — the same unit fieldDiff emits and the merge applies.
func applyFields(dst, src proto.Message, paths []string) {
	dm, sm := dst.ProtoReflect(), src.ProtoReflect()
	desc := sm.Descriptor()
	fields := desc.Fields()
	for _, p := range paths {
		fd := fields.ByName(protoreflect.Name(p))
		if fd == nil {
			if od := desc.Oneofs().ByName(protoreflect.Name(p)); od != nil && !od.IsSynthetic() {
				applyOneof(dm, sm, od)
			}
			continue
		}
		if sm.Has(fd) {
			dm.Set(fd, sm.Get(fd))
		} else {
			dm.Clear(fd)
		}
	}
}

func applyOneof(dm, sm protoreflect.Message, od protoreflect.OneofDescriptor) {
	if fd := sm.WhichOneof(od); fd != nil {
		dm.Set(fd, sm.Get(fd))
		return
	}
	if cur := dm.WhichOneof(od); cur != nil {
		dm.Clear(cur)
	}
}

// ownedFields resolves Binding.Fields against the entity's descriptor into the
// set of units the source owns and the subset of them that are not text
// fields. Key fields join the set without being named: a source reports them
// on every item, and they cannot differ between two versions of one row. nil
// names means every field, and returns a nil set.
func ownedFields(meta server.EntityMeta, names []string) (map[string]bool, []string, error) {
	if len(names) == 0 {
		return nil, nil, nil
	}
	desc := meta.Descriptor
	owned := make(map[string]bool, len(names)+len(meta.KeyFields))
	var units []string
	add := func(name string, isText bool) {
		if owned[name] {
			return
		}
		owned[name] = true
		if !isText {
			units = append(units, name)
		}
	}
	for _, fd := range meta.KeyFields {
		add(string(fd.Name()), false)
	}
	for _, name := range names {
		if fd := desc.Fields().ByName(protoreflect.Name(name)); fd != nil {
			if od := lww.OneofOf(fd); od != nil {
				return nil, nil, fmt.Errorf("connect: bind %q: field %q is a member of oneof %q; name the oneof",
					meta.Name, name, od.Name())
			}
			add(name, lww.IsText(fd))
			continue
		}
		if od := desc.Oneofs().ByName(protoreflect.Name(name)); od != nil && !od.IsSynthetic() {
			add(name, false)
			continue
		}
		return nil, nil, fmt.Errorf("connect: bind %q: %s has no field or oneof %q", meta.Name, desc.FullName(), name)
	}
	return owned, units, nil
}

// ownedPaths returns the paths b owns, in their order: all of them when b
// owns every field.
func (b binding) ownedPaths(paths []string) []string {
	if b.owned == nil {
		return paths
	}
	var out []string
	for _, p := range paths {
		if b.owned[p] {
			out = append(out, p)
		}
	}
	return out
}

// ownedTexts returns the entries of texts that name a field b owns: all of
// them when b owns every field.
func (b binding) ownedTexts(texts map[string]string) map[string]string {
	if b.owned == nil {
		return texts
	}
	var out map[string]string
	for name, s := range texts {
		if b.owned[name] {
			if out == nil {
				out = make(map[string]string, len(texts))
			}
			out[name] = s
		}
	}
	return out
}

// ownedTextFields returns the collaborative-text fields of msg that b owns.
// Only for a binding with an owned set.
func (b binding) ownedTextFields(msg proto.Message) []protoreflect.FieldDescriptor {
	var out []protoreflect.FieldDescriptor
	fields := msg.ProtoReflect().Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		if fd := fields.Get(i); lww.IsText(fd) && b.owned[string(fd.Name())] {
			out = append(out, fd)
		}
	}
	return out
}

// equalOwned reports whether x and y, engine messages of b's entity, agree on
// every field b owns. Text fields compare by their visible string, the only
// form a source holds. Without an owned set it is proto.Equal.
func (b binding) equalOwned(x, y proto.Message) (bool, error) {
	if b.owned == nil {
		return proto.Equal(x, y), nil
	}
	if len(b.ownedPaths(fieldDiff(x, y))) > 0 {
		return false, nil
	}
	for _, fd := range b.ownedTextFields(x) {
		xs, err := text.PlainText(x.ProtoReflect().Get(fd).Bytes())
		if err != nil {
			return false, err
		}
		ys, err := text.PlainText(y.ProtoReflect().Get(fd).Bytes())
		if err != nil {
			return false, err
		}
		if xs != ys {
			return false, nil
		}
	}
	return true, nil
}
