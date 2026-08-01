package connect

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/levelfourab/syncplane/engine/internal/lww"
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
		if !equalField(bm, nm, fd) {
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
		if bf != nf || (bf != nil && !equalField(bm, nm, bf)) {
			paths = append(paths, string(od.Name()))
		}
	}
	return paths
}

// equalField compares one field across two messages by projecting it into
// otherwise-empty messages and comparing those — this reuses proto.Equal's
// semantics for every field shape (scalar, message, repeated, map).
func equalField(a, b protoreflect.Message, fd protoreflect.FieldDescriptor) bool {
	if a.Has(fd) != b.Has(fd) {
		return false
	}
	if !a.Has(fd) {
		return true
	}
	am := a.Type().New()
	bm := b.Type().New()
	am.Set(fd, a.Get(fd))
	bm.Set(fd, b.Get(fd))
	return proto.Equal(am.Interface(), bm.Interface())
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
