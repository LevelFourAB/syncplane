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

	syncplanecursorv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/cursor/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/lww"
	"github.com/levelfourab/syncplane/engine/internal/naming"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/internal/protoopts"
)

// entityAccess is an entity's place in the access model: its
// (syncplane.v1.entity).access rules and its access_parent field, checked and
// resolved at registration. Nil on an entity that declares no access option.
type entityAccess struct {
	// rules are the entity's access rules, in declaration order.
	rules []accessRule
	// named holds every operation at least one rule admits, with opWrite
	// spread over the write operations it covers.
	named accessOp
	// read is who the read rules admit.
	read readers
	// parents are the cases of the access parent, in field order, and empty
	// when rows anchor at the workspace root: one case for a reference to one
	// entity, one per case of a polymorphic reference. A row holds at most one
	// case, and the case it holds names the row it sits below. Whether each
	// target is registered and in the model is a catalog question, answered by
	// compileAccessModel.
	parents []parentCase
	// parentName names the access parent in masks and errors: the marked
	// field, or the holder's oneof whose members are the cases.
	parentName string
}

// accessOp is a set of the operations an access rule admits.
type accessOp uint8

const (
	opRead accessOp = 1 << iota
	opCreate
	opUpdate
	opDelete
	// opWrite is a rule's write flag: every write operation, including any a
	// later version adds. It is a bit of its own rather than the three
	// operations it covers now, so that the access spec tells a rule that names
	// write apart from one that names create, update and delete.
	opWrite
)

// accessOpNames names each flag of an access rule, in field order.
var accessOpNames = []struct {
	op   accessOp
	name string
}{{opRead, "read"}, {opCreate, "create"}, {opUpdate, "update"}, {opDelete, "delete"}, {opWrite, "write"}}

// accessRule is one rule of an entity's access option.
type accessRule struct {
	ops accessOp
	// permission is the permission the rule needs, "" for a rule that admits
	// every admitted caller.
	permission string
}

// admitted reports whether the rule admits every caller admitted to the
// workspace.
func (r accessRule) admitted() bool {
	return r.permission == ""
}

// names reports whether the rule admits op, one of opRead, opCreate, opUpdate
// and opDelete.
func (r accessRule) names(op accessOp) bool {
	return r.ops&op != 0 || (op != opRead && r.ops&opWrite != 0)
}

// spec renders the rule in a canonical form: two rules with equal specs admit
// the same operations to the same callers.
func (r accessRule) spec() string {
	var ops []string
	for _, o := range accessOpNames {
		if r.ops&o.op != 0 {
			ops = append(ops, o.name)
		}
	}
	source := "admitted"
	if !r.admitted() {
		source = "permission=" + r.permission
	}
	return strings.Join(ops, ",") + ":" + source
}

// names reports whether at least one rule admits op, one of opRead, opCreate,
// opUpdate and opDelete, whatever the rule's source.
func (a *entityAccess) names(op accessOp) bool {
	return a.named&op != 0
}

// readers is who may read the rows of an entity: a caller that holds one of
// perms where a row sits, or every admitted caller when all is set.
type readers struct {
	// perms are the permissions of the entity's read rules, sorted and without
	// repeats. Empty when all is set.
	perms []string
	all   bool
}

// key renders r in a canonical form: entities with equal keys are read by
// the same callers at the same anchors. "*" cannot be a permission name.
func (r readers) key() string {
	if r.all {
		return "*"
	}
	return strings.Join(r.perms, ",")
}

// parentCase is one case of an entity's access parent: the reference a row
// holds when it sits below a row of entity, the wire name read off the target
// type's own option so registration order does not matter.
type parentCase struct {
	ref    *refField
	entity string
}

// hasParent reports whether rows of the entity sit below other rows rather
// than at the workspace root.
func (a *entityAccess) hasParent() bool {
	return len(a.parents) > 0
}

// parentFieldSet reports whether row sets a field of the access parent,
// whether or not the value names a key.
func (a *entityAccess) parentFieldSet(row protoreflect.Message) bool {
	for _, p := range a.parents {
		if row.Has(p.ref.fd) {
			return true
		}
	}
	return false
}

// parentRule renders the access parent's cases in a canonical form, for the
// read rule and the access spec: each case as its path (see refCasePath) and
// target entity, sorted. A reference to one entity renders as
// "field>entity".
func (a *entityAccess) parentRule() string {
	cases := make([]string, len(a.parents))
	for i, p := range a.parents {
		cases[i] = refCasePath(p.ref.ref) + ">" + p.entity
	}
	sort.Strings(cases)
	return strings.Join(cases, ",")
}

// parentDescription names an access parent in an error: the marked field, or
// the oneof whose members are the cases.
func parentDescription(parents []parentCase, name string) string {
	if len(parents) > 0 && lww.OneofOf(parents[0].ref.fd) != nil {
		return fmt.Sprintf("oneof %q", name)
	}
	return fmt.Sprintf("field %q", name)
}

// depthUnbounded is the access depth of an entity whose chain of parent
// entities runs into a cycle, such as a folder whose parent is a folder. Its
// rows can sit any number of hops below the root, up to
// Limits.MaxAccessDepth.
const depthUnbounded = -1

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
	// and for the evolution check, which sorts them.
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
	parents, parentName, err := accessParentOf(desc, info)
	if err != nil {
		return nil, err
	}
	opt := eo.GetAccess()
	if opt == nil {
		if parents != nil {
			return nil, fmt.Errorf("%s is marked access_parent, but the entity declares no access option; "+
				"declare access: { allow: [...] }", parentDescription(parents, parentName))
		}
		return nil, nil
	}
	// An option this build does not know could narrow access. Ignoring it would
	// widen access on an older server than the one the schema was written for,
	// so it is refused instead.
	if err := refuseUnknownOptionFields("access", opt); err != nil {
		return nil, err
	}
	if len(opt.GetAllow()) == 0 {
		return nil, fmt.Errorf("access declares no rule; add at least one rule to allow")
	}
	a := &entityAccess{parents: parents, parentName: parentName}
	seen := map[string]int{}
	reads := map[string]bool{}
	for i, r := range opt.GetAllow() {
		rule, err := accessRuleOf(i, r)
		if err != nil {
			return nil, err
		}
		spec := rule.spec()
		if j, dup := seen[spec]; dup {
			return nil, fmt.Errorf("access allow[%d] and allow[%d] are the same rule", j, i)
		}
		seen[spec] = i
		a.rules = append(a.rules, rule)
		for _, op := range []accessOp{opRead, opCreate, opUpdate, opDelete} {
			if rule.names(op) {
				a.named |= op
			}
		}
		if rule.names(opRead) {
			if rule.admitted() {
				a.read.all = true
			} else {
				reads[rule.permission] = true
			}
		}
	}
	if !a.read.all {
		for p := range reads {
			a.read.perms = append(a.read.perms, p)
		}
		sort.Strings(a.read.perms)
	}
	return a, nil
}

// accessRuleOf checks the rule at index i of an access option's allow list.
func accessRuleOf(i int, r *syncplanev1.AccessRule) (accessRule, error) {
	where := fmt.Sprintf("access allow[%d]", i)
	if err := refuseUnknownOptionFields(where, r); err != nil {
		return accessRule{}, err
	}
	var rule accessRule
	for _, flag := range []struct {
		set bool
		op  accessOp
	}{{r.GetRead(), opRead}, {r.GetCreate(), opCreate}, {r.GetUpdate(), opUpdate}, {r.GetDelete(), opDelete}, {r.GetWrite(), opWrite}} {
		if flag.set {
			rule.ops |= flag.op
		}
	}
	if rule.ops == 0 {
		return accessRule{}, fmt.Errorf("%s names no operation; set read, create, update, delete or write", where)
	}
	switch src := r.GetSource().(type) {
	case *syncplanev1.AccessRule_Permission:
		if err := naming.ValidatePermissionName(src.Permission); err != nil {
			return accessRule{}, fmt.Errorf("%s: %w", where, err)
		}
		rule.permission = src.Permission
	case *syncplanev1.AccessRule_Admitted:
		if src.Admitted != nil {
			if err := refuseUnknownOptionFields(where+" admitted", src.Admitted); err != nil {
				return accessRule{}, err
			}
		}
	default:
		return accessRule{}, fmt.Errorf("%s names no source; set permission or admitted", where)
	}
	return rule, nil
}

// accessParentOf finds the access parent of an entity: the cases of the
// reference marked access_parent, and the name of the parent (see
// entityAccess.parentName). It returns nil when no field carries the mark.
//
// The parent is one top-level field, or every member of one oneof on the
// entity. Either way it is polymorphic when it can hold more than one case: a
// marked field whose type is a reference message with a oneof of entities, or
// a oneof whose members are each a reference. A oneof with an unmarked member
// is refused, so that every case a row can hold names the row it sits below.
// accessParentOf also refuses the mark on a field nested inside a message
// field, where it cannot take effect.
//
// The shapes refused here are ones the model does not resolve: a to-many
// parent has several anchors at once, and a workspace-qualified one can name
// a row outside the workspace. Each is a validation rule rather than a
// structural limit, so accepting one later is additive.
func accessParentOf(desc protoreflect.MessageDescriptor, info *entityInfo) ([]parentCase, string, error) {
	var marked []protoreflect.FieldDescriptor
	fields := desc.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		b := fieldBehaviorOf(fd)
		if b.GetAccessParent() == nil {
			continue
		}
		if err := refuseUnknownOptionFields(fmt.Sprintf("field %q access_parent", fd.Name()), b.GetAccessParent()); err != nil {
			return nil, "", err
		}
		// A oneof's members share one parent; anything else marked twice is
		// a second parent.
		if len(marked) > 0 {
			if od := lww.OneofOf(fd); od == nil || lww.OneofOf(marked[0]) != od {
				return nil, "", fmt.Errorf("fields %q and %q are both marked access_parent; an entity has at most one, "+
					"which is one field or the members of one oneof", marked[0].Name(), fd.Name())
			}
		}
		if fd.IsList() || fd.IsMap() {
			return nil, "", fmt.Errorf("access_parent field %q is repeated; an access parent is one row", fd.Name())
		}
		refs := lww.RefsOf(fd)
		if len(refs) == 0 {
			return nil, "", fmt.Errorf("access_parent field %q must be a reference: a field whose type is an entity message "+
				"or a reference message", fd.Name())
		}
		if refs[0].Workspace != nil {
			return nil, "", fmt.Errorf("access_parent field %q is a reference message with a workspace field; "+
				"an access parent is a row of the holder's own workspace", fd.Name())
		}
		// A protected field keeps its stored value on a PUT and the zero value on
		// a create, so every row a client created would anchor at nothing.
		if b.GetWritePolicy() == syncplanev1.FieldWritePolicy_FIELD_WRITE_POLICY_TRUSTED_ONLY {
			return nil, "", fmt.Errorf("access_parent field %q is marked write_policy TRUSTED_ONLY; "+
				"a client could then create rows only without a parent", fd.Name())
		}
		marked = append(marked, fd)
	}
	if err := refuseNestedAccessParents(desc); err != nil {
		return nil, "", err
	}
	if len(marked) == 0 {
		return nil, "", nil
	}
	name := string(marked[0].Name())
	if od := lww.OneofOf(marked[0]); od != nil {
		name = string(od.Name())
		members := od.Fields()
		for i := 0; i < members.Len(); i++ {
			if m := members.Get(i); fieldBehaviorOf(m).GetAccessParent() == nil {
				return nil, "", fmt.Errorf("access_parent field %q is a member of oneof %q, whose member %q is not marked; "+
					"mark every member, so that each case names the row it sits below",
					marked[0].Name(), od.Name(), m.Name())
			}
		}
	}
	var cases []parentCase
	for i := range info.refFields {
		if rf := &info.refFields[i]; slices.Contains(marked, rf.fd) {
			cases = append(cases, parentCase{ref: rf, entity: protoopts.Entity(rf.ref.Target).GetName()})
		}
	}
	return cases, name, nil
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
	if access == nil {
		return nil, fmt.Errorf("a grant entity must declare access")
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

// grantSubjectIndexName is the index the engine derives over a grant entity's
// subject field, so resolving a caller's grants probes its own rows instead
// of scanning the entity. It shares the name an indexed mark on the field
// would give, so marking the field as well adds no second index.
func grantSubjectIndexName(g *grantSpec) string {
	return fieldIndexName(g.subject)
}

// buildGrantIndex derives the subject index of a grant entity. It runs after
// buildDeclaredIndexes, so a field the schema marks indexed already has it.
func buildGrantIndex(info *entityInfo) error {
	if info.grant == nil || info.indexByName(grantSubjectIndexName(info.grant)) != nil {
		return nil
	}
	desc := info.msgType.Descriptor()
	c, err := query.CompileIndexComponent(desc, []string{string(info.grant.subject.Name())}, false)
	if err != nil {
		return fmt.Errorf("grant subject field %q: %w", info.grant.subject.Name(), err)
	}
	comps := []query.IndexComponent{c}
	enc, err := query.NewIndexEncoder(comps)
	if err != nil {
		return fmt.Errorf("grant subject field %q: %w", info.grant.subject.Name(), err)
	}
	info.indexes = append(info.indexes, compiledIndex{
		name:     grantSubjectIndexName(info.grant),
		specHash: query.SpecHash(comps),
		encoder:  enc,
	})
	return nil
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
	// parents maps the name of each entity with an access parent to the
	// edges of its cases, in field order. anchors holds the names of their
	// targets: the entities whose rows can be anchors, so a write to one can
	// change what a caller holds.
	parents map[string][]*accessEdge
	anchors map[string]bool
	// entities maps the name of each entity in the model to its definition.
	entities map[string]*entityInfo
	// children maps the name of each anchor entity to the edges that point at
	// it, sorted by child name and then in field order. below maps it to
	// every entity whose rows can sit any number of hops below its rows,
	// sorted by name, itself included when the chain is recursive.
	children map[string][]*accessEdge
	below    map[string][]string
	// depths maps each model entity to the largest number of access_parent
	// hops from its rows to an entity without a parent, or depthUnbounded.
	depths map[string]int
	// moving is set when some entity has an access parent and rows that can
	// sit below its rows, so moving one of its rows moves rows below it (see
	// the access move log).
	moving bool
	// readPerms are the permissions model entities read by, sorted. They are
	// what a cursor records of a caller's access.
	readPerms []string
	// readRules are the read rules of the model entities, sorted by entity
	// (see readRuleOf), and fingerprint is their hash (see readPolicyOf). Two
	// models with the same fingerprint read the same rows from the same
	// anchors. What roles grant is not part of it: a cursor records that as
	// the anchors a caller holds.
	readRules   []*syncplanecursorv1.EntityReadRule
	fingerprint uint64
	// warnings are findings that do not make the model invalid but are likely
	// mistakes. Registration logs them.
	warnings []string
}

// accessEdge is one case of an access parent in a compiled model: a row of
// child whose ref holds a value sits below the row of target it names.
type accessEdge struct {
	child, target *entityInfo
	ref           *refField
}

// parentOf returns the row that msg's access parent names, given the edges
// of its entity's cases. ok is false when no case is set or the set case
// names no key.
func parentOf(msg protoreflect.Message, edges []*accessEdge) (anchorRow, bool) {
	for _, e := range edges {
		if !msg.Has(e.ref.fd) {
			continue
		}
		// The cases inside one reference message share the field, and only the
		// active one yields a stub.
		stub, ok := e.ref.ref.Stub(msg.Get(e.ref.fd).Message())
		if !ok {
			continue
		}
		key := e.target.keyOf(stub)
		return anchorRow{entity: e.target.name, key: key}, key != ""
	}
	return anchorRow{}, false
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
//
// mutators are the server's mutators. A MutatorAccess counts as a use of its
// permission, and is checked against the model here. maxDepth is
// Limits.MaxAccessDepth: an entity whose chain of parent entities is longer
// is refused, since none of its rows could anchor anywhere.
func compileAccessModel(c *catalog, mutators map[string]*mutatorInfo, maxDepth int) (*accessModel, error) {
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

	m := &accessModel{
		roles:    map[string]*modelRole{},
		entities: map[string]*entityInfo{},
		parents:  map[string][]*accessEdge{},
		anchors:  map[string]bool{},
		children: map[string][]*accessEdge{},
		depths:   map[string]int{},
	}
	used := map[string][]string{} // permission → entities that name it
	for _, name := range names {
		info := c.entities[name]
		a := info.access
		if a == nil {
			return nil, fmt.Errorf("server: entity %q declares no access option; in a schema that uses the access model "+
				"every entity declares access: { allow: [...] }", name)
		}
		m.entities[name] = info
		for _, p := range a.parents {
			target, ok := c.byType[p.ref.ref.Target.FullName()]
			if !ok {
				return nil, fmt.Errorf("server: entity %q access_parent field %q points at %s, which is not a registered entity",
					name, refCasePath(p.ref.ref), p.ref.ref.Target.FullName())
			}
			if target.access == nil {
				return nil, fmt.Errorf("server: entity %q access_parent field %q points at entity %q, which is not in the access model; "+
					"declare access on %q", name, refCasePath(p.ref.ref), target.name, target.name)
			}
			e := &accessEdge{child: info, target: target, ref: p.ref}
			m.parents[name] = append(m.parents[name], e)
			m.anchors[target.name] = true
			m.children[target.name] = append(m.children[target.name], e)
		}
		for _, r := range a.rules {
			if p := r.permission; p != "" && !slices.Contains(used[p], name) {
				used[p] = append(used[p], name)
			}
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

	for _, name := range names {
		if c.entities[name].access == nil {
			continue
		}
		d := m.depthOf(name)
		if d > maxDepth {
			return nil, fmt.Errorf("server: entity %q can sit %d access_parent hops below an entity without a parent, "+
				"more than MaxAccessDepth (%d) allows", name, d, maxDepth)
		}
	}
	m.below = map[string][]string{}
	for anchor := range m.anchors {
		m.below[anchor] = m.entitiesBelow(anchor)
		if len(m.parents[anchor]) > 0 {
			m.moving = true
		}
	}

	mutatorNames := make([]string, 0, len(mutators))
	for name, mi := range mutators {
		if mi.access != nil {
			mutatorNames = append(mutatorNames, name)
		}
	}
	sort.Strings(mutatorNames)
	for _, name := range mutatorNames {
		ma := mutators[name].access
		if err := checkMutatorAccess(c, mutators[name], ma); err != nil {
			return nil, err
		}
		if !slices.Contains(used[ma.Permission], "mutator "+name) {
			used[ma.Permission] = append(used[ma.Permission], "mutator "+name)
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
			return nil, fmt.Errorf("server: permission %q of %s is granted by no role; declare a role that grants it",
				p, usedBy(used[p][0]))
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
				return nil, fmt.Errorf("server: role %q of entity %q grants permission %q, which no access rule or mutator uses",
					name, m.roles[name].entity, p)
			}
		}
	}
	for _, name := range names {
		for _, p := range c.entities[name].access.read.perms {
			if !slices.Contains(m.readPerms, p) {
				m.readPerms = append(m.readPerms, p)
			}
		}
	}
	sort.Strings(m.readPerms)
	// One permission on a row and on the rows below it is the common case: a
	// comment is read with its task. Two entities that are not on one chain
	// and sit at different depths are the likely mistake, such as one
	// view_members for the org's roster and for every project's.
	for _, p := range perms {
		users := entitiesOnly(used[p])
		apart := false
		for i := 0; i < len(users) && !apart; i++ {
			for _, v := range users[i+1:] {
				u := users[i]
				if m.depths[u] != m.depths[v] && !slices.Contains(m.below[u], v) && !slices.Contains(m.below[v], u) {
					apart = true
					break
				}
			}
		}
		if apart {
			m.warnings = append(m.warnings, fmt.Sprintf(
				"permission %q governs entities at different anchor depths that do not sit below one another (%s): "+
					"a role that grants it above one of them reaches the rows of the others below every anchor, "+
					"so give each a permission of its own unless that is intended",
				p, strings.Join(users, ", ")))
		}
	}

	for _, name := range names {
		m.readRules = append(m.readRules, &syncplanecursorv1.EntityReadRule{Entity: name, Rule: readRuleOf(c.entities[name])})
	}
	m.fingerprint = readPolicyOf(m.readRules)
	return m, nil
}

// depthOf returns the largest number of access_parent hops from rows of the
// named entity to an entity without a parent, over every case of every
// parent on the way, or depthUnbounded when some chain of parent entities
// runs into a cycle. It memoizes into m.depths, so it runs once m.parents is
// complete.
func (m *accessModel) depthOf(name string) int {
	if d, ok := m.depths[name]; ok {
		return d
	}
	// Marked before the walk, so a chain that leads back here reads as one
	// that never ends. An entity the walk reaches this mark from has a chain
	// that runs into a cycle through name, so none is marked wrongly.
	m.depths[name] = depthUnbounded
	d := 0
	for _, e := range m.parents[name] {
		up := m.depthOf(e.target.name)
		if up == depthUnbounded {
			d = depthUnbounded
			break
		}
		d = max(d, up+1)
	}
	m.depths[name] = d
	return d
}

// entitiesBelow returns, sorted, every entity whose rows can sit below rows
// of anchor: its children, their children, and so on.
func (m *accessModel) entitiesBelow(anchor string) []string {
	seen := map[string]bool{}
	queue := []string{anchor}
	for len(queue) > 0 {
		at := queue[0]
		queue = queue[1:]
		for _, e := range m.children[at] {
			if !seen[e.child.name] {
				seen[e.child.name] = true
				queue = append(queue, e.child.name)
			}
		}
	}
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// readersBelow returns who may read the entities whose rows can sit below
// rows of the named entity, sorted by key and without repeats.
func (m *accessModel) readersBelow(name string) []readers {
	var out []readers
	seen := map[string]bool{}
	for _, below := range m.below[name] {
		r := m.entities[below].access.read
		if k := r.key(); !seen[k] {
			seen[k] = true
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key() < out[j].key() })
	return out
}

// readRuleOf renders what decides which rows of a model entity a caller
// reads, in a canonical form: two entities with equal rules read the same
// rows from the same anchors.
func readRuleOf(info *entityInfo) string {
	a := info.access
	rule := "read=" + a.read.key()
	if a.hasParent() {
		rule += " parent=" + a.parentRule()
	}
	return rule
}

// readPolicyOf fingerprints read rules sorted by entity. Never 0, which a
// cursor reserves for a schema without the access model.
func readPolicyOf(rules []*syncplanecursorv1.EntityReadRule) uint64 {
	h := sha256.New()
	for _, r := range rules {
		h.Write([]byte(r.GetEntity() + " " + r.GetRule() + "\n"))
	}
	if p := binary.BigEndian.Uint64(h.Sum(nil)[:8]); p != 0 {
		return p
	}
	return 1
}

// sameSharedReadRules reports whether every entity that both rule lists name
// reads by the same rule. Both are sorted by entity. An entity only one side
// names leaves the rows a client holds valid: a server reads no rows of an
// entity it does not know, and rows of an entity new to the model reach the
// client like any other change.
func sameSharedReadRules(a, b []*syncplanecursorv1.EntityReadRule) bool {
	for len(a) > 0 && len(b) > 0 {
		switch x, y := a[0], b[0]; {
		case x.GetEntity() < y.GetEntity():
			a = a[1:]
		case x.GetEntity() > y.GetEntity():
			b = b[1:]
		case x.GetRule() != y.GetRule():
			return false
		default:
			a, b = a[1:], b[1:]
		}
	}
	return true
}

// usedBy names what a permission's first user is, for errors: an entity name,
// or "mutator <name>".
func usedBy(user string) string {
	if strings.HasPrefix(user, "mutator ") {
		return user
	}
	return fmt.Sprintf("entity %q", user)
}

// entitiesOnly drops the mutators from a permission's users.
func entitiesOnly(users []string) []string {
	out := make([]string, 0, len(users))
	for _, u := range users {
		if !strings.HasPrefix(u, "mutator ") {
			out = append(out, u)
		}
	}
	return out
}

// checkMutatorAccess checks one mutator's MutatorAccess against the catalog:
// its row entity is in the model, and its key field is a singular string
// field of the args.
func checkMutatorAccess(c *catalog, mi *mutatorInfo, ma *MutatorAccess) error {
	if err := naming.ValidatePermissionName(ma.Permission); err != nil {
		return fmt.Errorf("server: mutator %q MutatorAccess: %w", mi.name, err)
	}
	if (ma.Entity == "") != (ma.KeyField == "") {
		return fmt.Errorf("server: mutator %q MutatorAccess names an Entity without a KeyField or a KeyField without an Entity", mi.name)
	}
	if ma.Entity == "" {
		return nil
	}
	info, ok := c.entities[ma.Entity]
	if !ok {
		return fmt.Errorf("server: mutator %q MutatorAccess names entity %q, which is not registered", mi.name, ma.Entity)
	}
	if info.access == nil {
		return fmt.Errorf("server: mutator %q MutatorAccess names entity %q, which is not in the access model", mi.name, ma.Entity)
	}
	if mi.args == nil {
		return fmt.Errorf("server: mutator %q MutatorAccess names key field %q, but the mutator takes no args", mi.name, ma.KeyField)
	}
	fd := mi.args.ProtoReflect().Descriptor().Fields().ByName(protoreflect.Name(ma.KeyField))
	if fd == nil || fd.Kind() != protoreflect.StringKind || fd.IsList() || fd.IsMap() {
		return fmt.Errorf("server: mutator %q MutatorAccess key field %q must be a singular string field of %s",
			mi.name, ma.KeyField, mi.args.ProtoReflect().Descriptor().FullName())
	}
	return nil
}

// accessSpecOf renders everything about one entity that decides access, in a
// canonical form: two entities with equal specs grant and require the same
// permissions on the same rows. The evolution check compares it.
func accessSpecOf(info *entityInfo) string {
	var b strings.Builder
	b.WriteString(info.name)
	switch a := info.access; {
	case a == nil:
		b.WriteString(" access=none")
	default:
		rules := make([]string, len(a.rules))
		for i, r := range a.rules {
			rules[i] = r.spec()
		}
		sort.Strings(rules)
		b.WriteString(" allow=" + strings.Join(rules, ";"))
		if a.hasParent() {
			b.WriteString(" parent=" + a.parentRule())
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
