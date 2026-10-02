package server

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strconv"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/lww"
	"github.com/levelfourab/syncplane/engine/internal/naming"
	"github.com/levelfourab/syncplane/internal/protoopts"
)

// entityAccess is an entity's place in the access model: its
// (syncplane.v1.entity).access option and its access_parent field, checked
// and resolved at registration. Nil on an entity that declares no access
// option.
type entityAccess struct {
	// unrestricted is access.unrestricted: the entity is outside the model and
	// read and write are empty.
	unrestricted bool
	read, write  string
	// parent is the reference marked access_parent, nil when rows anchor at the
	// workspace root. parentEntity is the wire name of the entity it points
	// at, read off the target type's own option so registration order does not
	// matter; whether that entity is registered and in the model is a catalog
	// question, answered by compileAccessModel.
	parent       *refField
	parentEntity string
}

// depth is the number of access_parent hops from a row to the anchor that
// carries no parent of its own. The model accepts depths 0 and 1.
func (a *entityAccess) depth() int {
	if a.parent == nil {
		return 0
	}
	return 1
}

// grantSpec is an entity's (syncplane.v1.entity).grant option, checked and
// resolved at registration: which fields of a grant row hold its subject and
// its role, and what each declared role grants.
type grantSpec struct {
	subject protoreflect.FieldDescriptor
	// role is a string or enum field. An enum value names the role spelled
	// like the value.
	role protoreflect.FieldDescriptor
	// roles maps a declared role name to the permissions it grants, sorted.
	roles map[string][]string
	// roleNames are the declared role names in declaration order, for errors
	// and for the fingerprint, which sorts them.
	roleNames []string
}

// accessOf checks an entity's access option and its access_parent field, and
// resolves the parent reference. It needs info.refFields, so it runs after
// buildEntityInfo has collected them.
//
// Every rule here is local to the entity. The rules that need the rest of the
// schema — the parent entity is registered and in the model, depth, role
// names and permission coverage — are compileAccessModel's.
func accessOf(desc protoreflect.MessageDescriptor, eo *syncplanev1.EntityOptions, info *entityInfo) (*entityAccess, error) {
	parent, err := accessParentOf(desc, info)
	if err != nil {
		return nil, err
	}
	opt := eo.GetAccess()
	if opt == nil {
		if parent != nil {
			return nil, fmt.Errorf("field %q is marked access_parent, but the entity declares no access option; "+
				"declare access: { read: \"...\" write: \"...\" }", parent.fd.Name())
		}
		return nil, nil
	}
	// An option this build does not know could narrow access. Ignoring it would
	// widen access on an older server than the one the schema was written for,
	// so it is refused instead.
	if err := refuseUnknownOptionFields("access", opt); err != nil {
		return nil, err
	}
	a := &entityAccess{unrestricted: opt.GetUnrestricted(), read: opt.GetRead(), write: opt.GetWrite()}
	if a.unrestricted {
		if a.read != "" || a.write != "" {
			return nil, fmt.Errorf("access sets unrestricted together with read or write; an unrestricted entity names no permissions")
		}
		if parent != nil {
			return nil, fmt.Errorf("field %q is marked access_parent, but access is unrestricted; "+
				"an unrestricted entity has no place in the access model", parent.fd.Name())
		}
		return a, nil
	}
	if a.read == "" || a.write == "" {
		return nil, fmt.Errorf("access must name both a read and a write permission, or set unrestricted")
	}
	for _, p := range []string{a.read, a.write} {
		if err := naming.ValidatePermissionName(p); err != nil {
			return nil, fmt.Errorf("access: %w", err)
		}
	}
	if parent != nil {
		a.parent = parent
		a.parentEntity = protoopts.Entity(parent.ref.Target).GetName()
	}
	return a, nil
}

// accessParentOf finds the top-level field marked access_parent and returns
// its reference, or nil when no field carries the mark. It also refuses the
// mark on a field nested inside a message field, where it cannot take effect.
//
// The shapes refused here are ones the model does not resolve: a to-many or
// polymorphic parent has several candidate anchors, and a workspace-qualified
// one can name a row outside the workspace. Each is a validation rule rather
// than a structural limit, so accepting one later is additive.
func accessParentOf(desc protoreflect.MessageDescriptor, info *entityInfo) (*refField, error) {
	var found *refField
	fields := desc.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		b := fieldBehaviorOf(fd)
		if b.GetAccessParent() == nil {
			continue
		}
		if err := refuseUnknownOptionFields(fmt.Sprintf("field %q access_parent", fd.Name()), b.GetAccessParent()); err != nil {
			return nil, err
		}
		if found != nil {
			return nil, fmt.Errorf("fields %q and %q are both marked access_parent; an entity has at most one",
				found.fd.Name(), fd.Name())
		}
		if fd.IsList() || fd.IsMap() {
			return nil, fmt.Errorf("access_parent field %q is repeated; an access parent is one row", fd.Name())
		}
		if od := lww.OneofOf(fd); od != nil {
			return nil, fmt.Errorf("access_parent field %q is a member of oneof %q; an access parent cannot be inside a oneof",
				fd.Name(), od.Name())
		}
		refs := lww.RefsOf(fd)
		if len(refs) != 1 || refs[0].Value != nil {
			return nil, fmt.Errorf("access_parent field %q must be a field whose type is an entity message", fd.Name())
		}
		// A protected field keeps its stored value on a PUT and the zero value on
		// a create, so every row a client created would anchor at nothing.
		if b.GetWritePolicy() == syncplanev1.FieldWritePolicy_FIELD_WRITE_POLICY_TRUSTED_ONLY {
			return nil, fmt.Errorf("access_parent field %q is marked write_policy TRUSTED_ONLY; "+
				"a client could then create rows only without a parent", fd.Name())
		}
		for j := range info.refFields {
			if info.refFields[j].fd == fd {
				found = &info.refFields[j]
				break
			}
		}
	}
	if err := refuseNestedAccessParents(desc); err != nil {
		return nil, err
	}
	return found, nil
}

// refuseNestedAccessParents refuses access_parent on a field of a message
// nested in the entity. The walk does not enter entity-typed fields: those
// are other entities, checked when they register.
func refuseNestedAccessParents(desc protoreflect.MessageDescriptor) error {
	seen := map[protoreflect.FullName]bool{desc.FullName(): true}
	var walk func(md protoreflect.MessageDescriptor) error
	walk = func(md protoreflect.MessageDescriptor) error {
		fields := md.Fields()
		for i := 0; i < fields.Len(); i++ {
			fd := fields.Get(i)
			var sub protoreflect.MessageDescriptor
			switch {
			case fd.IsMap():
				if fd.MapValue().Kind() == protoreflect.MessageKind {
					sub = fd.MapValue().Message()
				}
			case fd.Kind() == protoreflect.MessageKind:
				sub = fd.Message()
			}
			if sub == nil || seen[sub.FullName()] || protoopts.Entity(sub) != nil {
				continue
			}
			seen[sub.FullName()] = true
			subFields := sub.Fields()
			for j := 0; j < subFields.Len(); j++ {
				if f := subFields.Get(j); fieldBehaviorOf(f).GetAccessParent() != nil {
					return fmt.Errorf("field %q of %s is marked access_parent, but only a field declared on the entity itself can be an access parent",
						f.Name(), sub.FullName())
				}
			}
			if err := walk(sub); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(desc)
}

// grantOf checks an entity's grant option and resolves its subject and role
// fields. A grant entity must itself sit in the model, so its rows are read
// and written under permissions of their own.
func grantOf(desc protoreflect.MessageDescriptor, eo *syncplanev1.EntityOptions, access *entityAccess) (*grantSpec, error) {
	opt := eo.GetGrant()
	if opt == nil {
		return nil, nil
	}
	if err := refuseUnknownOptionFields("grant", opt); err != nil {
		return nil, err
	}
	if access == nil || access.unrestricted {
		return nil, fmt.Errorf("a grant entity must declare access with a read and a write permission")
	}
	subject, err := grantField(desc, "subject", opt.GetSubject())
	if err != nil {
		return nil, err
	}
	if subject.Kind() != protoreflect.StringKind {
		return nil, fmt.Errorf("grant subject field %q is %s; it must be a string", subject.Name(), subject.Kind())
	}
	role, err := grantField(desc, "role", opt.GetRole())
	if err != nil {
		return nil, err
	}
	if role.Kind() != protoreflect.StringKind && role.Kind() != protoreflect.EnumKind {
		return nil, fmt.Errorf("grant role field %q is %s; it must be a string or an enum", role.Name(), role.Kind())
	}
	if role == subject {
		return nil, fmt.Errorf("grant names field %q as both subject and role", role.Name())
	}
	if len(opt.GetRoles()) == 0 {
		return nil, fmt.Errorf("grant declares no roles")
	}
	g := &grantSpec{subject: subject, role: role, roles: make(map[string][]string, len(opt.GetRoles()))}
	for _, r := range opt.GetRoles() {
		if err := refuseUnknownOptionFields("grant role", r); err != nil {
			return nil, err
		}
		if err := naming.ValidateRoleName(r.GetName()); err != nil {
			return nil, fmt.Errorf("grant: %w", err)
		}
		if _, dup := g.roles[r.GetName()]; dup {
			return nil, fmt.Errorf("grant declares role %q twice", r.GetName())
		}
		if role.Kind() == protoreflect.EnumKind && role.Enum().Values().ByName(protoreflect.Name(r.GetName())) == nil {
			return nil, fmt.Errorf("grant role %q is not a value of enum %s, which role field %q holds",
				r.GetName(), role.Enum().FullName(), role.Name())
		}
		if len(r.GetGrants()) == 0 {
			return nil, fmt.Errorf("grant role %q grants no permissions", r.GetName())
		}
		perms := slices.Clone(r.GetGrants())
		sort.Strings(perms)
		for i, p := range perms {
			if err := naming.ValidatePermissionName(p); err != nil {
				return nil, fmt.Errorf("grant role %q: %w", r.GetName(), err)
			}
			if i > 0 && perms[i-1] == p {
				return nil, fmt.Errorf("grant role %q grants %q twice", r.GetName(), p)
			}
		}
		g.roles[r.GetName()] = perms
		g.roleNames = append(g.roleNames, r.GetName())
	}
	return g, nil
}

// grantField resolves the field a grant option names for one of its parts.
// The field must be present on every row, which rules out a oneof member, and
// hold one value, which rules out a list or a map.
func grantField(desc protoreflect.MessageDescriptor, part, name string) (protoreflect.FieldDescriptor, error) {
	if name == "" {
		return nil, fmt.Errorf("grant names no %s field", part)
	}
	fd := desc.Fields().ByName(protoreflect.Name(name))
	if fd == nil {
		return nil, fmt.Errorf("grant %s field %q not found", part, name)
	}
	if fd.IsList() || fd.IsMap() {
		return nil, fmt.Errorf("grant %s field %q is repeated; it must hold one value", part, name)
	}
	if od := lww.OneofOf(fd); od != nil {
		return nil, fmt.Errorf("grant %s field %q is a member of oneof %q; it must be set on every row", part, name, od.Name())
	}
	return fd, nil
}

// refuseUnknownOptionFields refuses an option message that carries fields
// this build does not know.
func refuseUnknownOptionFields(what string, m interface {
	ProtoReflect() protoreflect.Message
}) error {
	if len(m.ProtoReflect().GetUnknown()) > 0 {
		return fmt.Errorf("%s carries option fields this server does not know; upgrade the server before using them", what)
	}
	return nil
}

// accessModel is a catalog's access model, compiled once per catalog swap. A
// nil model means the schema does not use access control: no entity declares
// access or grant.
type accessModel struct {
	// roles maps every declared role name to its definition. Names are unique
	// across the schema, which is what lets a global role be named alone.
	roles map[string]*modelRole
	// grantEntities are the entities whose rows grant roles, sorted by name.
	grantEntities []*entityInfo
	// fingerprint hashes everything that decides what a caller can read and
	// write: each entity's permissions and parent, and each role's
	// permissions. Two catalogs with the same fingerprint compute the same
	// visibility from the same rows. Never 0 for a model.
	fingerprint uint64
	// warnings are findings that do not make the model invalid but are likely
	// mistakes. Registration logs them.
	warnings []string
}

// modelRole is one declared role and the grant entity that declares it.
type modelRole struct {
	name   string
	entity string
	perms  []string
}

// compileAccessModel checks the rules of the access model that span entities
// and compiles the result. It returns nil, nil for a schema that does not use
// the model.
//
// The catalog may be half built: RegisterEntity adds one entity at a time, so
// a schema can fail here until its last entity arrives. The caller decides
// whether that is an error to report or a state to hold (see
// catalog.accessErr).
func compileAccessModel(c *catalog) (*accessModel, error) {
	names := make([]string, 0, len(c.entities))
	inUse := false
	for name, info := range c.entities {
		if isEngineEntity(info) {
			continue
		}
		names = append(names, name)
		if info.access != nil || info.grant != nil {
			inUse = true
		}
	}
	if !inUse {
		return nil, nil
	}
	sort.Strings(names)

	m := &accessModel{roles: map[string]*modelRole{}}
	used := map[string][]string{}       // permission → entities that name it
	depths := map[string]map[int]bool{} // permission → anchor depths of those entities
	for _, name := range names {
		info := c.entities[name]
		a := info.access
		if a == nil {
			return nil, fmt.Errorf("server: entity %q declares no access option; in a schema that uses the access model "+
				"every entity declares access: { read: \"...\" write: \"...\" } or access: { unrestricted: true }", name)
		}
		if a.unrestricted {
			continue
		}
		if a.parent != nil {
			target, ok := c.byType[a.parent.ref.Target.FullName()]
			if !ok {
				return nil, fmt.Errorf("server: entity %q access_parent field %q points at %s, which is not a registered entity",
					name, a.parent.fd.Name(), a.parent.ref.Target.FullName())
			}
			if target.access == nil || target.access.unrestricted {
				return nil, fmt.Errorf("server: entity %q access_parent field %q points at entity %q, which is not in the access model; "+
					"declare access with a read and a write permission on %q", name, a.parent.fd.Name(), target.name, target.name)
			}
			if target.access.parent != nil {
				return nil, fmt.Errorf("server: entity %q access_parent field %q points at entity %q, which has an access_parent of its own; "+
					"an access parent must anchor at the workspace root", name, a.parent.fd.Name(), target.name)
			}
		}
		for _, p := range []string{a.read, a.write} {
			if !slices.Contains(used[p], name) {
				used[p] = append(used[p], name)
			}
			if depths[p] == nil {
				depths[p] = map[int]bool{}
			}
			depths[p][a.depth()] = true
		}
		if info.grant == nil {
			continue
		}
		m.grantEntities = append(m.grantEntities, info)
		for _, role := range info.grant.roleNames {
			if prev, dup := m.roles[role]; dup {
				return nil, fmt.Errorf("server: role %q is declared by both entity %q and entity %q; role names are unique across the schema",
					role, prev.entity, name)
			}
			m.roles[role] = &modelRole{name: role, entity: name, perms: info.grant.roles[role]}
		}
	}

	granted := map[string]bool{}
	for _, r := range m.roles {
		for _, p := range r.perms {
			granted[p] = true
		}
	}
	perms := make([]string, 0, len(used))
	for p := range used {
		perms = append(perms, p)
	}
	sort.Strings(perms)
	for _, p := range perms {
		if !granted[p] {
			return nil, fmt.Errorf("server: permission %q of entity %q is granted by no role; declare a role that grants it",
				p, used[p][0])
		}
	}
	roleNames := make([]string, 0, len(m.roles))
	for name := range m.roles {
		roleNames = append(roleNames, name)
	}
	sort.Strings(roleNames)
	for _, name := range roleNames {
		for _, p := range m.roles[name].perms {
			if used[p] == nil {
				return nil, fmt.Errorf("server: role %q of entity %q grants permission %q, which no entity uses",
					name, m.roles[name].entity, p)
			}
		}
	}
	for _, p := range perms {
		if len(depths[p]) > 1 {
			m.warnings = append(m.warnings, fmt.Sprintf(
				"permission %q governs entities at different anchor depths (%s): a role that grants it at the workspace root "+
					"reaches the rows below every anchor, so give each depth a permission of its own unless that is intended",
				p, strings.Join(used[p], ", ")))
		}
	}

	var spec strings.Builder
	for _, name := range names {
		spec.WriteString(accessSpecOf(c.entities[name]))
		spec.WriteByte('\n')
	}
	sum := sha256.Sum256([]byte(spec.String()))
	m.fingerprint = binary.BigEndian.Uint64(sum[:8])
	if m.fingerprint == 0 {
		m.fingerprint = 1
	}
	return m, nil
}

// accessSpecOf renders everything about one entity that decides access, in a
// canonical form: two entities with equal specs grant and require the same
// permissions on the same rows. It feeds both the model fingerprint and the
// evolution check, so a change one sees the other sees too.
func accessSpecOf(info *entityInfo) string {
	var b strings.Builder
	b.WriteString(info.name)
	switch a := info.access; {
	case a == nil:
		b.WriteString(" access=none")
	case a.unrestricted:
		b.WriteString(" access=unrestricted")
	default:
		b.WriteString(" read=" + a.read + " write=" + a.write)
		if a.parent != nil {
			b.WriteString(" parent=" + string(a.parent.fd.Name()) + ">" + a.parentEntity)
		}
	}
	if g := info.grant; g != nil {
		b.WriteString(" subject=" + string(g.subject.Name()) + " role=" + string(g.role.Name()))
		roles := slices.Clone(g.roleNames)
		sort.Strings(roles)
		for _, r := range roles {
			b.WriteString(" " + r + "=" + strings.Join(g.roles[r], ","))
			// A stored enum role is a number. The name it reads as is what picks
			// the role, so renumbering a value regrants every row that holds it.
			if g.role.Kind() == protoreflect.EnumKind {
				num := g.role.Enum().Values().ByName(protoreflect.Name(r)).Number()
				b.WriteString("#" + strconv.Itoa(int(num)))
			}
		}
	}
	return b.String()
}

// validateAccessEvolution refuses a change to the access model of an entity
// that is already registered, unless the registration allows it. Access
// decides which rows each client holds, so a change is a decision an operator
// makes on purpose rather than one an additive schema update makes in
// passing.
func validateAccessEvolution(old, next *entityInfo, allow bool) error {
	if allow {
		return nil
	}
	if accessSpecOf(old) != accessSpecOf(next) {
		return fmt.Errorf("server: entity %q changes its access model (access, grant or access_parent); "+
			"register with the access change allowed to apply it", next.name)
	}
	return nil
}

// logAccessWarnings logs the findings of a valid access model that are likely
// mistakes. Registration calls it once the schema is whole.
func (s *Server) logAccessWarnings(c *catalog) {
	if c.access == nil {
		return
	}
	for _, w := range c.access.warnings {
		s.log().LogAttrs(context.Background(), slog.LevelWarn, "access model warning",
			slog.String("component", "schema"), slog.String("finding", w))
	}
}

// isEngineEntity reports whether info is an entity the engine registers for
// itself rather than one a schema declares. The access model does not govern
// one: the engine writes its rows and decides who reads them.
func isEngineEntity(info *entityInfo) bool {
	return info.name == historyEntityName &&
		info.msgType.Descriptor().FullName() == (&syncplanev1.FieldHistory{}).ProtoReflect().Descriptor().FullName()
}
