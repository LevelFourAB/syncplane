package server

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/lww"
)

// protected returns the entity's effective protected set for this caller. The
// result is owned by the gate: callers read it and must not retain or mutate it.
//
// A nil gate protects nothing, and neither does an entity that marks no field
// under a policy that implements no hook — the common case, which allocates
// nothing and consults nobody.
func (g *writeGate) protected(info *entityInfo) ([]string, error) {
	if g == nil {
		return nil, nil
	}
	if g.fieldHook == nil {
		return info.protectedFields, nil
	}
	if fields, ok := g.fieldMemo[info.name]; ok {
		return fields, nil
	}
	extra, err := g.fieldHook.ProtectedFields(g.ctx, g.p, g.ws, info.name)
	if err != nil {
		return nil, fmt.Errorf("server: entity %q: resolving protected fields: %w", info.name, err)
	}
	fields, err := unionProtected(info, extra)
	if err != nil {
		return nil, err
	}
	if g.fieldMemo == nil {
		g.fieldMemo = map[string][]string{}
	}
	g.fieldMemo[info.name] = fields
	return fields, nil
}

// unionProtected folds a hook's answer into the schema's, canonicalizing each
// name the way registration canonicalizes a mark: a oneof member becomes its
// group, which is the only unit a mask names.
//
// A name that resolves to no field, or to a key field, is an error rather than a
// no-op. The hook states a restriction; a typo or a field a later schema version
// dropped would otherwise read as "nothing to protect", and silently admitting
// the write is the wrong direction for a permission control to fail in.
func unionProtected(info *entityInfo, extra []string) ([]string, error) {
	if len(extra) == 0 {
		return info.protectedFields, nil
	}
	desc := info.msgType.Descriptor()
	fields := make([]string, len(info.protectedFields), len(info.protectedFields)+len(extra))
	copy(fields, info.protectedFields)
	seen := make(map[string]bool, len(fields)+len(extra))
	for _, name := range fields {
		seen[name] = true
	}
	for _, name := range extra {
		canonical, err := canonicalProtectedName(info, desc, name)
		if err != nil {
			return nil, err
		}
		if seen[canonical] {
			continue
		}
		seen[canonical] = true
		fields = append(fields, canonical)
	}
	return fields, nil
}

// canonicalProtectedName resolves one name a hook returned to the unit a mask
// path can name: a top-level field, or the group a oneof member belongs to.
func canonicalProtectedName(
	info *entityInfo,
	desc protoreflect.MessageDescriptor,
	name string,
) (string, error) {
	if strings.Contains(name, ".") {
		return "", fmt.Errorf("server: entity %q: ProtectedFields returned %q; protection is declared on "+
			"top-level fields, and a mask path is matched by its first segment", info.name, name)
	}
	if od := desc.Oneofs().ByName(protoreflect.Name(name)); od != nil {
		return name, nil
	}
	fd := desc.Fields().ByName(protoreflect.Name(name))
	if fd == nil {
		return "", fmt.Errorf("server: entity %q: ProtectedFields returned %q, which is not a field of %s",
			info.name, name, desc.FullName())
	}
	for _, kf := range info.keyFields {
		if kf.Name() == fd.Name() {
			return "", fmt.Errorf("server: entity %q: ProtectedFields returned key field %q; a key field can "+
				"never be modified in place, so protecting one cannot take effect", info.name, name)
		}
	}
	if od := lww.OneofOf(fd); od != nil {
		return string(od.Name()), nil
	}
	return name, nil
}

// maskProtected returns the first mask path that writes into a protected field,
// and whether there was one.
//
// A path is matched by its FIRST segment: naming the field replaces it, and
// naming something below it writes into it. That is also why protection is
// declared on top-level fields — the rule has to hold for a mask the client
// chose, not only for the paths a schema anticipated.
func maskProtected(paths, protected []string) (string, bool) {
	if len(protected) == 0 {
		return "", false
	}
	for _, path := range paths {
		if protectedPath(path, protected) {
			return path, true
		}
	}
	return "", false
}

// protectedPath reports whether one path writes into a protected field.
func protectedPath(path string, protected []string) bool {
	head := protectedHead(path)
	for _, name := range protected {
		if head == name {
			return true
		}
	}
	return false
}

// refuseProtectedMask rejects a PATCH whose mask reaches a protected field.
//
// Only PATCH: it is the op that names what it writes, so a mask reaching one of
// these fields is a request to do the forbidden thing, and the client should ack
// and drop it rather than retry a write that can never land. A PUT names no
// field and keeps the stored value instead.
func refuseProtectedMask(info *entityInfo, crud *syncplanev1.CrudMutation, protected []string) error {
	if crud.GetOp() != syncplanev1.CrudOp_CRUD_OP_PATCH {
		return nil
	}
	path, hit := maskProtected(crud.GetFieldMask().GetPaths(), protected)
	if !hit {
		return nil
	}
	return Reject(CodePermissionDenied, fmt.Sprintf(
		"entity %q: field %q may not be written by this caller, so a PATCH masking %q is refused; "+
			"drop the path from the mask, or make the change through a custom mutator",
		info.name, protectedHead(path), path))
}

// protectedHead is the unit a mask path was matched against — the path itself
// for a top-level field, and the field a deeper path writes into.
func protectedHead(path string) string {
	if i := strings.IndexByte(path, '.'); i >= 0 {
		return path[:i]
	}
	return path
}

// carryProtectedFields replaces every protected field of msg with the value the
// stored row holds, which is what makes a PUT of a row carrying one land for the
// fields the caller may write and change nothing for the fields it may not. A
// nil base is an absent row: the fields are cleared, so a client that creates a
// row cannot seed one either.
//
// It runs before the text guard and the text union, so bytes a client sent in a
// protected collaborative-text field are gone before anything decodes them; the
// union then folds the stored document into itself, which Merge defines as
// identity.
//
// base is read, never retained: the values copied out come from a private clone,
// so the caller's message and the row it decoded stay independent.
func carryProtectedFields(info *entityInfo, protected []string, base, msg proto.Message) error {
	if len(protected) == 0 {
		return nil
	}
	desc := info.msgType.Descriptor()
	dst := msg.ProtoReflect()
	var src protoreflect.Message
	if base != nil {
		src = proto.Clone(base).ProtoReflect()
	}
	for _, name := range protected {
		if od := desc.Oneofs().ByName(protoreflect.Name(name)); od != nil {
			if fd := dst.WhichOneof(od); fd != nil {
				dst.Clear(fd)
			}
			if src != nil {
				if fd := src.WhichOneof(od); fd != nil {
					dst.Set(fd, src.Get(fd))
				}
			}
			continue
		}
		fd := desc.Fields().ByName(protoreflect.Name(name))
		if fd == nil {
			return fmt.Errorf("server: entity %q: protected field %q is not a field of %s",
				info.name, name, desc.FullName())
		}
		dst.Clear(fd)
		if src != nil && src.Has(fd) {
			dst.Set(fd, src.Get(fd))
		}
	}
	return nil
}
