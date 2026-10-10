package server

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	syncplanecursorv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/cursor/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/lww"
	"github.com/levelfourab/syncplane/engine/internal/naming"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/internal/protoopts"
)

// entityAccess is an entity's place in the access model: its
// (syncplane.v1.entity).access rules and access parents, checked and resolved
// at registration. Nil on an entity that declares no access option.
type entityAccess struct {
	// rules are the entity's access rules, in declaration order.
	rules []accessRule
	// named holds every operation at least one rule admits, with opWrite
	// spread over the write operations it covers.
	named accessOp
	// read is who the read rules admit.
	read readers
	// parents are the cases of the access parents, in field order, and empty
	// when rows anchor at the workspace root: one case for a reference to one
	// entity, one per case of a polymorphic reference. Each unit holds at most
	// one case, or one per element of a repeated reference, and each case a row
	// holds names a row it sits below. Whether each target is registered and
	// in the model is a catalog question, answered by compileAccessModel.
	parents []parentCase
	// parentUnits name the access parents in masks and errors, in field order:
	// each named field, and each named oneof. A grant entity has at most one.
	parentUnits []string
	// guards are the entity's field guards, in declaration order.
	guards []fieldGuard
	// guardOf maps each unit a guard restricts to that guard's index in
	// guards. Nil when the entity declares no guard.
	guardOf map[string]int
}

// fieldGuard is one field guard of an entity: the units it restricts, the
// rules that admit writing them and who reads them.
type fieldGuard struct {
	// units are the units the guard restricts, sorted: the names of top-level
	// fields outside a oneof, and of oneofs.
	units []string
	// rules are the guard's rules that admit a write, in declaration order,
	// without opRead.
	rules []accessRule
	// reads are the guard's rules that admit reading, in declaration order,
	// each with opRead alone. Empty for a guard that hides its units from
	// every client.
	reads []accessRule
	// read is who reads the units of a row the caller reads, sorted as the
	// readers of an entity are: read.all for a guard with a read rule that
	// needs no permission and sets no check.
	read readers
	// named holds opCreate and opUpdate when at least one rule names them.
	named accessOp
}

// hides reports whether the guard can hide its units from a caller that reads
// the row: no read rule of the guard admits every admitted caller on every
// row.
func (g *fieldGuard) hides() bool {
	return !g.read.all
}

// spec renders the guard in a canonical form: two guards with equal specs
// restrict the same units for the same callers.
func (g fieldGuard) spec() string {
	rules := make([]string, len(g.rules))
	for i, r := range g.rules {
		rules[i] = r.spec()
	}
	sort.Strings(rules)
	spec := strings.Join(g.units, ",") + "{" + strings.Join(rules, ";") + "}"
	if g.hides() {
		spec += "read=" + g.read.key()
	}
	return spec
}

// hidesFields reports whether a guard of the entity can hide its units from
// a caller that reads the row.
func (a *entityAccess) hidesFields() bool {
	return a != nil && slices.ContainsFunc(a.guards, func(g fieldGuard) bool { return g.hides() })
}

// readerSets returns who reads the rows of the entity, then, for each guard
// that can hide its units, who reads those units: every set whose anchors
// decide what a caller receives of a row.
func (a *entityAccess) readerSets() []readers {
	out := []readers{a.read}
	for i := range a.guards {
		if g := &a.guards[i]; g.hides() {
			out = append(out, g.read)
		}
	}
	return out
}

// guardFor returns the guard that restricts unit, or nil when no guard does.
func (a *entityAccess) guardFor(unit string) *fieldGuard {
	i, ok := a.guardOf[unit]
	if !ok {
		return nil
	}
	return &a.guards[i]
}

// labeledRule is a rule of an entity together with where the access option
// declares it, for errors and warnings.
type labeledRule struct {
	where string
	rule  accessRule
}

// allRules returns the entity's rules and the rules of its guards, each with
// where the access option declares it.
func (a *entityAccess) allRules() []labeledRule {
	out := make([]labeledRule, 0, len(a.rules))
	for i, r := range a.rules {
		out = append(out, labeledRule{where: fmt.Sprintf("access allow[%d]", i), rule: r})
	}
	for i, g := range a.guards {
		for _, r := range g.rules {
			out = append(out, labeledRule{where: fmt.Sprintf("access guard[%d]", i), rule: r})
		}
		for _, r := range g.reads {
			out = append(out, labeledRule{where: fmt.Sprintf("access guard[%d]", i), rule: r})
		}
	}
	return out
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
	// requires are the permissions the rule needs, sorted. Empty for a rule
	// that admits every admitted caller.
	requires []string
	// callerIs is the field that must hold one of the caller's subjects, nil
	// when the rule sets no caller_is check.
	callerIs protoreflect.FieldDescriptor
	// fields are the units an update the rule admits may change, sorted: the
	// names of top-level fields outside a oneof, and of oneofs. Nil when the
	// rule sets no fields check.
	fields []string
	// when is the condition a row must match, nil when the rule sets no when
	// check. whenSpec renders it in a canonical form, "" without one.
	when     *query.Compiled
	whenSpec string
	// whenReads are the units the when check reads on the row, in the order
	// the condition names them. Nil without a when check.
	whenReads []string
	// within are the rule's within entries, sorted by spec. Nil when the rule
	// sets none. Each needs the catalog to resolve: see accessModel.withins.
	within []withinEntry
}

// withinEntry is one entry of an access rule's within list, as the rule
// declares it: the entity of the row the rule's row must sit within, and the
// checks that row must pass.
type withinEntry struct {
	entity string
	// callerIs is the name of the field that must hold one of the caller's
	// subjects, "" without a caller_is check.
	callerIs string
	// when is the condition, nil without a when check. whenSpec renders it in
	// a canonical form, "" without one.
	when     *syncplanev1.Condition
	whenSpec string
}

// spec renders the entry in a canonical form: entries with equal specs pass
// the same rows. "{", "}" and ";" cannot be part of an entity or field name,
// and a when spec is hex.
func (w withinEntry) spec() string {
	return w.entity + "{caller_is=" + w.callerIs + ",when=" + w.whenSpec + "}"
}

// admitted reports whether the rule admits every caller admitted to the
// workspace.
func (r accessRule) admitted() bool {
	return len(r.requires) == 0
}

// names reports whether the rule admits op, one of opRead, opCreate, opUpdate
// and opDelete.
func (r accessRule) names(op accessOp) bool {
	return r.ops&op != 0 || (op != opRead && r.ops&opWrite != 0)
}

// checksRow reports whether the rule reads the row's fields to decide: it
// sets a caller_is check or a when check.
func (r accessRule) checksRow() bool {
	return r.callerIs != nil || r.when != nil
}

// checksAbove reports whether the rule reads rows at or above the row to
// decide: it sets a within entry.
func (r accessRule) checksAbove() bool {
	return len(r.within) > 0
}

// withinSpec renders the rule's within entries in a canonical form, "" for a
// rule without one.
func (r accessRule) withinSpec() string {
	specs := make([]string, len(r.within))
	for i, w := range r.within {
		specs[i] = w.spec()
	}
	return strings.Join(specs, ";")
}

// withinCovers reports whether every within entry of r is one of o's too:
// r's entries admit every row o's admit.
func (r accessRule) withinCovers(o accessRule) bool {
	for _, w := range r.within {
		if !slices.ContainsFunc(o.within, func(x withinEntry) bool { return x.spec() == w.spec() }) {
			return false
		}
	}
	return true
}

// joint reports whether the rule needs more than one permission.
func (r accessRule) joint() bool {
	return len(r.requires) > 1
}

// source renders who the rule admits: the permissions it needs joined by
// "+", or "admitted" for a rule that admits every admitted caller.
func (r accessRule) source() string {
	if r.admitted() {
		return "admitted"
	}
	return strings.Join(r.requires, "+")
}

// rowHolds reports whether row passes the rule's checks on the row: the
// subject field holds one of subjects, and the row matches the when
// condition. A check the rule does not set holds. subjects are sorted, and an
// empty subject value never matches.
func (r accessRule) rowHolds(subjects []string, row protoreflect.Message) bool {
	if r.callerIs != nil && !subjectMatches(subjects, row.Get(r.callerIs).String()) {
		return false
	}
	return r.when == nil || r.when.Eval(row)
}

// subjectMatches reports whether a subject field holding value names one of
// subjects, which are sorted. An empty value never matches.
func subjectMatches(subjects []string, value string) bool {
	if value == "" {
		return false
	}
	_, found := slices.BinarySearch(subjects, value)
	return found
}

// fieldsAllow reports whether an update that changes the units in changed
// passes the rule's fields check.
func (r accessRule) fieldsAllow(changed []string) bool {
	if r.fields == nil {
		return true
	}
	for _, u := range changed {
		if _, found := slices.BinarySearch(r.fields, u); !found {
			return false
		}
	}
	return true
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
		source = "requires=" + r.source()
	}
	spec := strings.Join(ops, ",") + ":" + source
	if r.callerIs != nil {
		spec += ":caller_is=" + string(r.callerIs.Name())
	}
	if r.fields != nil {
		spec += ":fields=" + strings.Join(r.fields, ",")
	}
	if r.when != nil {
		spec += ":when=" + r.whenSpec
	}
	if r.within != nil {
		spec += ":within=" + r.withinSpec()
	}
	return spec
}

// names reports whether at least one rule admits op, one of opRead, opCreate,
// opUpdate and opDelete, whatever the rule's source.
func (a *entityAccess) names(op accessOp) bool {
	return a.named&op != 0
}

// maskCovered reports whether some update rule's fields check allows a PATCH
// with the mask paths, whatever the rule's source and caller_is check: the rule
// has no fields list, or its list names the unit each path writes into. A key
// field is left out, since a PATCH that changes one is refused anyway. When
// no rule allows the mask, heads are its units, sorted.
func (a *entityAccess) maskCovered(info *entityInfo, paths []string) (heads []string, ok bool) {
	for _, path := range paths {
		head := protectedHead(path)
		if slices.Contains(heads, head) || slices.ContainsFunc(info.keyFields, func(fd protoreflect.FieldDescriptor) bool {
			return string(fd.Name()) == head
		}) {
			continue
		}
		heads = append(heads, head)
	}
	sort.Strings(heads)
	for _, r := range a.rules {
		if r.names(opUpdate) && r.fieldsAllow(heads) {
			return nil, true
		}
	}
	return heads, false
}

// maskClosed returns the first unit a PATCH with the mask paths writes into
// whose guard admits no update to any caller, and whether there is one. A
// guard with an update rule is decided on the row; see
// accessWrite.checkGuardedMask.
func (a *entityAccess) maskClosed(paths []string) (string, bool) {
	for _, path := range paths {
		head := protectedHead(path)
		if g := a.guardFor(head); g != nil && g.named&opUpdate == 0 {
			return head, true
		}
	}
	return "", false
}

// readers is who may read the rows of an entity: a caller that holds one of
// perms where a row sits, every admitted caller when all is set, and the
// callers that one of checked admits.
type readers struct {
	// perms are the permissions of the entity's read rules that check nothing
	// on the row, sorted and without repeats. Empty when all is set.
	perms []string
	all   bool
	// checked are the entity's read rules with a caller_is check or a when
	// check, or that need more than one permission, sorted by spec. Empty when
	// all is set, since all admits every row they could.
	checked []accessRule
}

// key renders r in a canonical form: entities with equal keys are read by
// the same callers at the same anchors. "*" cannot be a permission name, and
// "|" cannot be part of a rule spec's names.
func (r readers) key() string {
	if r.all {
		return "*"
	}
	key := strings.Join(r.perms, ",")
	for _, o := range r.checked {
		key += "|" + o.spec()
	}
	return key
}

// loose returns r with every check on the row dropped: who may read some row
// of the entity where a row sits. It is a superset of r, for the checks that
// cannot read each row. Each rule of checked stays there without its checks
// on the row, since a rule that checks the row admits other rows than perms
// do, and a rule that needs more than one permission holds where all of them
// do. A rule keeps its within entries, which read the rows above the row and
// so can change with them. A rule leaves checked when one of perms admits
// every caller it admits, and two rules with the same source and the same
// within entries are kept once.
func (r readers) loose() readers {
	if r.all || len(r.checked) == 0 {
		return readers{perms: r.perms, all: r.all}
	}
	out := readers{perms: r.perms}
	for _, o := range r.checked {
		if slices.ContainsFunc(o.requires, func(p string) bool { return slices.Contains(out.perms, p) }) ||
			slices.ContainsFunc(out.checked, func(c accessRule) bool {
				return c.source() == o.source() && c.withinSpec() == o.withinSpec()
			}) {
			continue
		}
		out.checked = append(out.checked, accessRule{ops: opRead, requires: o.requires, within: o.within})
	}
	sort.Slice(out.checked, func(i, j int) bool { return out.checked[i].spec() < out.checked[j].spec() })
	return out
}

// sources renders each way r admits a caller, for an error: each of perms,
// then the permissions of each rule in checked joined by "+".
func (r readers) sources() []string {
	out := slices.Clone(r.perms)
	for _, o := range r.checked {
		out = append(out, o.source())
	}
	return out
}

// named returns every permission r names, sorted and without repeats: those
// of perms and those the rules in checked need.
func (r readers) named() []string {
	out := slices.Clone(r.perms)
	for _, o := range r.checked {
		for _, p := range o.requires {
			if !slices.Contains(out, p) {
				out = append(out, p)
			}
		}
	}
	sort.Strings(out)
	return out
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

// parentRule renders the access parents' cases in a canonical form, for the
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

// depthUnbounded is the access depth of an entity whose chain of parent
// entities runs into a cycle, such as a folder whose parent is a folder. Its
// rows can sit any number of hops below the root, up to
// Limits.MaxAccessDepth.
const depthUnbounded = -1

// grantSpec is an entity's (syncplane.v1.entity).grant option, checked and
// resolved at registration: which fields of a grant row hold its subject and
// its role, and what each declared role grants.
type grantSpec struct {
	// subjects are the cases of the subject, in field order: one for a field,
	// one per member of a oneof, and one per target of a reference message
	// that holds a oneof of entities. A row holds at most one case. Whether
	// each team case points at an entity with members is a catalog question,
	// answered by compileAccessModel.
	subjects []subjectCase
	// subjectName names the subject in masks and errors: the field, or the
	// oneof whose members are the cases.
	subjectName string
	// role is a string or enum field. An enum value names the role spelled
	// like the value.
	role protoreflect.FieldDescriptor
	// roles maps a declared role name to the permissions it grants, sorted,
	// with the permissions of the roles it includes added.
	roles map[string][]string
	// roleNames are the declared role names in declaration order, for errors
	// and for the evolution check, which sorts them.
	roleNames []string
}

// subjectCase is one case of a grant entity's subject: a string field, which
// names one subject of a caller, or a reference, which names a team.
type subjectCase struct {
	fd protoreflect.FieldDescriptor
	// team is the reference of a case that names a team, nil for a string
	// field.
	team *refField
}

// indexName is the index that resolving a caller's grants probes for the
// case: the index the engine derives over a string case, which has the name
// an indexed mark on the field would give, or the reference index of a team
// case.
func (c subjectCase) indexName() string {
	if c.team != nil {
		return refIndexName(c.team.ref)
	}
	return fieldIndexName(c.fd)
}

// stringCases returns the cases of g that name a subject of a caller.
func (g *grantSpec) stringCases() []protoreflect.FieldDescriptor {
	var out []protoreflect.FieldDescriptor
	for _, c := range g.subjects {
		if c.team == nil {
			out = append(out, c.fd)
		}
	}
	return out
}

// namesSubject reports whether row's subject names one of subjects, which
// are sorted: a string case holds one of them.
func (g *grantSpec) namesSubject(subjects []string, row protoreflect.Message) bool {
	for _, c := range g.subjects {
		if c.team == nil && subjectMatches(subjects, row.Get(c.fd).String()) {
			return true
		}
	}
	return false
}

// subjectSet reports whether the row of entity holds a subject: a string
// case that is not empty, or a team case whose reference names a key. model
// is the compiled access model, which knows the target of each team case;
// without one, a team case that is set counts.
func (g *grantSpec) subjectSet(row protoreflect.Message, model *accessModel, entity string) bool {
	for _, c := range g.subjects {
		switch {
		case c.team == nil:
			if row.Get(c.fd).String() != "" {
				return true
			}
		case model == nil:
			if row.Has(c.fd) {
				return true
			}
		}
	}
	if model == nil {
		return false
	}
	_, ok := parentOf(row, model.teamSubjects[entity])
	return ok
}

// subjectSpec renders the subject in a canonical form for the access spec: its
// name, then each case as its path (see refCasePath) and, for a team case,
// its target entity. A subject that is one string field renders as its name.
func (g *grantSpec) subjectSpec() string {
	if len(g.subjects) == 1 && g.subjects[0].team == nil {
		return g.subjectName
	}
	cases := make([]string, len(g.subjects))
	for i, c := range g.subjects {
		cases[i] = string(c.fd.Name())
		if c.team != nil {
			cases[i] = refCasePath(c.team.ref) + ">" + protoopts.Entity(c.team.ref.Target).GetName()
		}
	}
	return g.subjectName + "(" + strings.Join(cases, ",") + ")"
}

// accessOf checks an entity's access option and resolves its access parents. It needs info.refFields, so it runs after
// buildEntityInfo has collected them.
//
// Every rule here is local to the entity. The rules that need the rest of the
// schema — the parent entity is registered and in the model, depth, role
// names and permission coverage — are compileAccessModel's.
func accessOf(desc protoreflect.MessageDescriptor, eo *syncplanev1.EntityOptions, info *entityInfo) (*entityAccess, error) {
	opt := eo.GetAccess()
	if opt == nil {
		return nil, nil
	}
	// An option this build does not know could narrow access. Ignoring it would
	// widen access on an older server than the one the schema was written for,
	// so it is refused instead.
	if err := refuseUnknownOptionFields("access", opt); err != nil {
		return nil, err
	}
	parents, units, err := accessParentOf(desc, info, opt.GetParents())
	if err != nil {
		return nil, err
	}
	// A grant row applies at one anchor, which the checks of a grant and of a
	// team membership compare as one row.
	if eo.GetGrant() != nil {
		const one = "a grant entity has one access parent: one singular field or one oneof"
		if len(units) > 1 {
			return nil, fmt.Errorf("%s, and access parents names %q and %q", one, units[0], units[1])
		}
		if len(parents) > 0 && parents[0].ref.fd.IsList() {
			return nil, fmt.Errorf("%s, and access parent %q is repeated", one, units[0])
		}
	}
	if len(opt.GetAllow()) == 0 {
		return nil, fmt.Errorf("access declares no rule; add at least one rule to allow")
	}
	a := &entityAccess{parents: parents, parentUnits: units}
	seen := map[string]int{}
	for i, r := range opt.GetAllow() {
		rule, err := accessRuleOf(i, r, desc, info.keyFields)
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
	}
	a.read = readersOf(a.rules)
	if err := a.guardsOf(desc, info.keyFields, opt.GetGuard()); err != nil {
		return nil, err
	}
	if err := a.refuseHiddenChecks(); err != nil {
		return nil, err
	}
	return a, nil
}

// readersOf returns who the rules that name read admit.
func readersOf(rules []accessRule) readers {
	var r readers
	perms := map[string]bool{}
	for _, rule := range rules {
		switch {
		case !rule.names(opRead):
		case rule.checksRow() || rule.checksAbove() || rule.joint():
			r.checked = append(r.checked, rule)
		case rule.admitted():
			r.all = true
		default:
			perms[rule.requires[0]] = true
		}
	}
	if r.all {
		r.checked = nil
		return r
	}
	for p := range perms {
		r.perms = append(r.perms, p)
	}
	sort.Strings(r.perms)
	sort.Slice(r.checked, func(i, j int) bool { return r.checked[i].spec() < r.checked[j].spec() })
	return r
}

// refuseHiddenChecks refuses a guard that can hide a field that a rule of
// the entity reads to decide: a client decides access from the rows it holds,
// and cannot decide on a field it does not receive.
func (a *entityAccess) refuseHiddenChecks() error {
	for i, r := range a.rules {
		for _, name := range ruleReads(r) {
			if g := a.guardFor(name); g != nil && g.hides() {
				return fmt.Errorf("access allow[%d] reads %q, which a guard can hide; "+
					"a field that a rule checks is one every reader of the row reads", i, name)
			}
		}
	}
	return nil
}

// ruleReads returns the units that a rule's caller_is and when checks read on
// the row, in the order the rule names them.
func ruleReads(r accessRule) []string {
	var out []string
	if r.callerIs != nil {
		out = append(out, unitOf(r.callerIs))
	}
	for _, name := range r.whenReads {
		if !slices.Contains(out, name) {
			out = append(out, name)
		}
	}
	return out
}

// guardsOf checks the field guards of an access option and records them on a.
// It runs after accessParentOf, which a.parentUnits come from.
func (a *entityAccess) guardsOf(desc protoreflect.MessageDescriptor, keyFields []protoreflect.FieldDescriptor, guards []*syncplanev1.FieldGuard) error {
	for i, g := range guards {
		where := fmt.Sprintf("access guard[%d]", i)
		if err := refuseUnknownOptionFields(where, g); err != nil {
			return err
		}
		if len(g.GetFields()) == 0 {
			return fmt.Errorf("%s names no field; list the fields it restricts", where)
		}
		units, err := ruleFieldsOf(desc, keyFields, g.GetFields())
		if err != nil {
			return fmt.Errorf("%s fields: %w", where, err)
		}
		for _, u := range units {
			if j, dup := a.guardOf[u]; dup {
				return fmt.Errorf("%s and access guard[%d] both name %q; a field has at most one guard", where, j, u)
			}
			// A client could then create rows only without a parent, and every
			// row it created would anchor at nothing.
			if slices.Contains(a.parentUnits, u) {
				return fmt.Errorf("%s names %q, which is an access parent; a guard cannot restrict it", where, u)
			}
			// The create stamps a creator field and no write changes it later,
			// so a guard could not take effect.
			if fd := desc.Fields().ByName(protoreflect.Name(u)); fd != nil && fieldBehaviorOf(fd).GetCreator() != nil {
				return fmt.Errorf("%s names %q, which is a creator field; a guard cannot restrict it", where, u)
			}
		}
		guard := fieldGuard{units: units}
		seen := map[string]int{}
		for j, r := range g.GetAllow() {
			rw := fmt.Sprintf("%s allow[%d]", where, j)
			rule, err := accessRuleOf(j, r, desc, keyFields)
			if err != nil {
				return fmt.Errorf("%s: %w", where, err)
			}
			switch {
			case rule.ops&opDelete != 0:
				return fmt.Errorf("%s names delete; a guard restricts writing a field, and a delete writes none", rw)
			case rule.fields != nil:
				return fmt.Errorf("%s names fields; the guard's own fields are the ones it restricts", rw)
			}
			spec := rule.spec()
			if k, dup := seen[spec]; dup {
				return fmt.Errorf("%s allow[%d] and allow[%d] are the same rule", where, k, j)
			}
			seen[spec] = j
			if rule.ops&opRead != 0 {
				read := rule
				read.ops = opRead
				guard.reads = append(guard.reads, read)
				rule.ops &^= opRead
				if rule.ops == 0 {
					continue
				}
			}
			guard.rules = append(guard.rules, rule)
			for _, op := range []accessOp{opCreate, opUpdate} {
				if rule.names(op) {
					guard.named |= op
				}
			}
		}
		guard.read = readersOf(guard.reads)
		if a.guardOf == nil {
			a.guardOf = map[string]int{}
		}
		for _, u := range units {
			a.guardOf[u] = len(a.guards)
		}
		a.guards = append(a.guards, guard)
	}
	// Once every guard is known: a check that read a guarded field would depend
	// on the value a guard decides about.
	for i, g := range guards {
		for j, r := range g.GetAllow() {
			reads := slices.Clone(conditionHeads(r.GetWhen()))
			if name := r.GetCallerIs(); name != "" {
				reads = append(reads, name)
			}
			for _, name := range reads {
				unit := name
				if fd := desc.Fields().ByName(protoreflect.Name(name)); fd != nil {
					unit = unitOf(fd)
				}
				if _, guarded := a.guardOf[unit]; guarded {
					return fmt.Errorf("access guard[%d] allow[%d] reads %q, which a guard restricts; "+
						"a guard's checks read only fields that no guard restricts", i, j, name)
				}
			}
		}
	}
	return nil
}

// conditionHeads returns the first segment of each path that cond reads on
// the row, in the order cond names them. An exists condition reads the list
// at its path, and its inner condition reads that list's elements, so only
// its path counts.
func conditionHeads(cond *syncplanev1.Condition) []string {
	if cond == nil {
		return nil
	}
	var out []string
	var walk func(m protoreflect.Message)
	walk = func(m protoreflect.Message) {
		m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
			switch {
			case fd.Name() == "path" && fd.IsList() && fd.Kind() == protoreflect.StringKind:
				if v.List().Len() > 0 {
					out = append(out, v.List().Get(0).String())
				}
			case fd.Message() == nil:
			case m.Descriptor().FullName() == "syncplane.v1.ExistsCond":
			case fd.IsList():
				for i := 0; i < v.List().Len(); i++ {
					walk(v.List().Get(i).Message())
				}
			default:
				walk(v.Message())
			}
			return true
		})
	}
	walk(cond.ProtoReflect())
	return out
}

// unitOf returns the unit a top-level field belongs to: its oneof's name, or
// its own name outside a oneof.
func unitOf(fd protoreflect.FieldDescriptor) string {
	if od := lww.OneofOf(fd); od != nil {
		return string(od.Name())
	}
	return string(fd.Name())
}

// accessRuleOf checks the rule at index i of an access option's allow list.
// desc is the entity's message and keyFields its key fields, which a rule's
// checks may not name.
func accessRuleOf(i int, r *syncplanev1.AccessRule, desc protoreflect.MessageDescriptor, keyFields []protoreflect.FieldDescriptor) (accessRule, error) {
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
	switch {
	case len(r.GetRequires()) > 0 && r.GetAdmitted() != nil:
		return accessRule{}, fmt.Errorf("%s sets both requires and admitted; set exactly one", where)
	case r.GetAdmitted() != nil:
		if err := refuseUnknownOptionFields(where+" admitted", r.GetAdmitted()); err != nil {
			return accessRule{}, err
		}
	case len(r.GetRequires()) == 0:
		return accessRule{}, fmt.Errorf("%s names no source; set requires or admitted", where)
	}
	for _, p := range r.GetRequires() {
		if err := naming.ValidatePermissionName(p); err != nil {
			return accessRule{}, fmt.Errorf("%s requires: %w", where, err)
		}
		if slices.Contains(rule.requires, p) {
			return accessRule{}, fmt.Errorf("%s requires %q twice", where, p)
		}
		rule.requires = append(rule.requires, p)
	}
	sort.Strings(rule.requires)
	if name := r.GetCallerIs(); name != "" {
		fd, err := stringField(desc, name)
		if err != nil {
			return accessRule{}, fmt.Errorf("%s caller_is: %w", where, err)
		}
		rule.callerIs = fd
	}
	if r.GetFields() != nil {
		if rule.ops != opUpdate {
			return accessRule{}, fmt.Errorf("%s names fields, which only a rule whose one operation is update can name", where)
		}
		fields, err := ruleFieldsOf(desc, keyFields, r.GetFields())
		if err != nil {
			return accessRule{}, fmt.Errorf("%s fields: %w", where, err)
		}
		rule.fields = fields
	}
	if r.GetWhen() != nil {
		when, spec, err := ruleWhenOf(desc, r.GetWhen())
		if err != nil {
			return accessRule{}, fmt.Errorf("%s when: %w", where, err)
		}
		rule.when, rule.whenSpec = when, spec
		for _, name := range conditionHeads(r.GetWhen()) {
			unit := name
			if fd := desc.Fields().ByName(protoreflect.Name(name)); fd != nil {
				unit = unitOf(fd)
			}
			if !slices.Contains(rule.whenReads, unit) {
				rule.whenReads = append(rule.whenReads, unit)
			}
		}
	}
	for j, w := range r.GetWithin() {
		entry, err := withinEntryOf(fmt.Sprintf("%s within[%d]", where, j), w)
		if err != nil {
			return accessRule{}, err
		}
		if slices.ContainsFunc(rule.within, func(x withinEntry) bool { return x.spec() == entry.spec() }) {
			return accessRule{}, fmt.Errorf("%s within[%d] repeats an earlier entry", where, j)
		}
		rule.within = append(rule.within, entry)
	}
	sort.Slice(rule.within, func(i, j int) bool { return rule.within[i].spec() < rule.within[j].spec() })
	return rule, nil
}

// withinEntryOf checks what one within entry of an access rule says by
// itself: an entity, at least one check, and a when condition that reads only
// a row. That the entity is in the model and above the rule's entity, and
// that its fields fit the checks, is compileAccessModel's to check.
func withinEntryOf(where string, w *syncplanev1.AccessWithin) (withinEntry, error) {
	if err := refuseUnknownOptionFields(where, w); err != nil {
		return withinEntry{}, err
	}
	if w.GetEntity() == "" {
		return withinEntry{}, fmt.Errorf("%s names no entity", where)
	}
	if w.GetCallerIs() == "" && w.GetWhen() == nil {
		return withinEntry{}, fmt.Errorf("%s sets no check; set caller_is or when", where)
	}
	entry := withinEntry{entity: w.GetEntity(), callerIs: w.GetCallerIs()}
	if cond := w.GetWhen(); cond != nil {
		if err := refuseUnknownCondFields(cond.ProtoReflect()); err != nil {
			return withinEntry{}, fmt.Errorf("%s when: %w", where, err)
		}
		if err := refuseNonRowConds(cond); err != nil {
			return withinEntry{}, fmt.Errorf("%s when: %w", where, err)
		}
		raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(cond)
		if err != nil {
			return withinEntry{}, err
		}
		entry.when, entry.whenSpec = cond, hex.EncodeToString(raw)
	}
	return entry, nil
}

// ruleWhenOf compiles the when condition of an access rule against the
// entity's message, and renders it in a canonical form for the rule's spec.
// The condition must read only the row, so a rel and a match condition are
// refused. So are fields this build does not know in any message of the
// condition, since they could narrow it.
func ruleWhenOf(desc protoreflect.MessageDescriptor, cond *syncplanev1.Condition) (*query.Compiled, string, error) {
	if err := refuseUnknownCondFields(cond.ProtoReflect()); err != nil {
		return nil, "", err
	}
	if err := refuseNonRowConds(cond); err != nil {
		return nil, "", err
	}
	compiled, err := query.Compile(desc, cond)
	if err != nil {
		return nil, "", err
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(cond)
	if err != nil {
		return nil, "", err
	}
	return compiled, hex.EncodeToString(raw), nil
}

// refuseUnknownCondFields refuses a condition in which any message carries
// fields this build does not know.
func refuseUnknownCondFields(m protoreflect.Message) error {
	if len(m.GetUnknown()) > 0 {
		return fmt.Errorf("the condition carries fields this server does not know; upgrade the server before using them")
	}
	var err error
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.Message() == nil:
		case fd.IsList():
			for i := 0; i < v.List().Len() && err == nil; i++ {
				err = refuseUnknownCondFields(v.List().Get(i).Message())
			}
		default:
			err = refuseUnknownCondFields(v.Message())
		}
		return err == nil
	})
	return err
}

// refuseNonRowConds refuses a rel condition or a match condition anywhere in
// cond.
func refuseNonRowConds(cond *syncplanev1.Condition) error {
	switch kind := cond.GetKind().(type) {
	case *syncplanev1.Condition_Rel:
		return fmt.Errorf("path %v: a rel condition reads another row; a when condition reads only the row", kind.Rel.GetPath())
	case *syncplanev1.Condition_Match:
		return fmt.Errorf("a match condition cannot decide access")
	case *syncplanev1.Condition_And:
		for _, sub := range kind.And.GetConds() {
			if err := refuseNonRowConds(sub); err != nil {
				return err
			}
		}
	case *syncplanev1.Condition_Or:
		for _, sub := range kind.Or.GetConds() {
			if err := refuseNonRowConds(sub); err != nil {
				return err
			}
		}
	case *syncplanev1.Condition_Not:
		return refuseNonRowConds(kind.Not)
	}
	return nil
}

// singularStringField resolves a field that is set on every row: a
// top-level, singular string field outside any oneof.
func singularStringField(desc protoreflect.MessageDescriptor, name string) (protoreflect.FieldDescriptor, error) {
	fd, err := stringField(desc, name)
	if err != nil {
		return nil, err
	}
	if od := lww.OneofOf(fd); od != nil {
		return nil, fmt.Errorf("field %q is a member of oneof %q; it must be set on every row", name, od.Name())
	}
	return fd, nil
}

// stringField resolves a field that a check reads on each row: a top-level,
// singular string field. A member of a oneof reads as empty on a row that
// holds another case, which no caller_is check matches.
func stringField(desc protoreflect.MessageDescriptor, name string) (protoreflect.FieldDescriptor, error) {
	fd := desc.Fields().ByName(protoreflect.Name(name))
	switch {
	case fd == nil:
		return nil, fmt.Errorf("field %q not found", name)
	case fd.IsList() || fd.IsMap():
		return nil, fmt.Errorf("field %q is repeated; it must hold one value", name)
	case fd.Kind() != protoreflect.StringKind:
		return nil, fmt.Errorf("field %q is %s; it must be a string", name, fd.Kind())
	}
	return fd, nil
}

// ruleFieldsOf checks the fields list of an access rule and returns it
// sorted: each entry names a top-level field outside a oneof, or a oneof,
// which are the units a mask names. A key field cannot be listed, since no
// update changes one.
func ruleFieldsOf(desc protoreflect.MessageDescriptor, keyFields []protoreflect.FieldDescriptor, names []string) ([]string, error) {
	if len(names) == 0 {
		return nil, fmt.Errorf("the list is empty")
	}
	out := make([]string, 0, len(names))
	for _, name := range names {
		if desc.Oneofs().ByName(protoreflect.Name(name)) != nil {
			out = append(out, name)
			continue
		}
		fd := desc.Fields().ByName(protoreflect.Name(name))
		switch {
		case fd == nil:
			return nil, fmt.Errorf("%q is not a field or a oneof of %s", name, desc.FullName())
		case lww.OneofOf(fd) != nil:
			return nil, fmt.Errorf("field %q is a member of oneof %q; name the oneof", name, lww.OneofOf(fd).Name())
		case slices.Contains(keyFields, fd):
			return nil, fmt.Errorf("%q is a key field, which no update changes", name)
		}
		out = append(out, name)
	}
	sort.Strings(out)
	for i := 1; i < len(out); i++ {
		if out[i] == out[i-1] {
			return nil, fmt.Errorf("%q is listed twice", out[i])
		}
	}
	return out, nil
}

// changedUnits returns, sorted, the units of an entity's rows that differ
// between stored and final: the top-level fields outside a oneof, and the
// oneofs, by name. A oneof differs when the rows hold different cases or
// different values in the same case.
func changedUnits(desc protoreflect.MessageDescriptor, stored, final protoreflect.Message) []string {
	var out []string
	fields := desc.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if lww.OneofOf(fd) != nil {
			continue
		}
		if !fieldEqual(fd, stored, final) {
			out = append(out, string(fd.Name()))
		}
	}
	oneofs := desc.Oneofs()
	for i := 0; i < oneofs.Len(); i++ {
		od := oneofs.Get(i)
		if od.IsSynthetic() {
			// A proto3 optional field is its own unit, compared above.
			continue
		}
		a, b := stored.WhichOneof(od), final.WhichOneof(od)
		if a != b || (a != nil && !fieldEqual(a, stored, final)) {
			out = append(out, string(od.Name()))
		}
	}
	sort.Strings(out)
	return out
}

// fieldEqual reports whether field fd holds the same value in a and b. A
// field with explicit presence that one row sets and the other does not
// differs, even when the set value is the default.
func fieldEqual(fd protoreflect.FieldDescriptor, a, b protoreflect.Message) bool {
	if a.Has(fd) != b.Has(fd) {
		return false
	}
	return a.Get(fd).Equal(b.Get(fd))
}

// accessParentOf resolves the access parents that names lists: the cases of
// the named references, in field order, and the units that hold them in field
// order (see entityAccess.parentUnits). It returns nil when names is empty.
//
// Each name is a top-level field outside any oneof, or a oneof whose members
// are each a reference. A unit is polymorphic when it can hold more than one
// case: a field whose type is a reference message with a oneof of entities,
// or a oneof of references.
//
// A workspace-qualified reference is refused, since it can name a row outside
// the workspace. That is a validation rule rather than a structural limit, so
// accepting it later is additive.
func accessParentOf(desc protoreflect.MessageDescriptor, info *entityInfo, names []string) ([]parentCase, []string, error) {
	if len(names) == 0 {
		return nil, nil, nil
	}
	var marked []protoreflect.FieldDescriptor
	named := map[string]int{}
	for i, name := range names {
		where := fmt.Sprintf("access parents[%d]", i)
		if j, dup := named[name]; dup {
			return nil, nil, fmt.Errorf("%s and parents[%d] both name %q", where, j, name)
		}
		named[name] = i
		var members []protoreflect.FieldDescriptor
		if od := desc.Oneofs().ByName(protoreflect.Name(name)); od != nil && !od.IsSynthetic() {
			for j := 0; j < od.Fields().Len(); j++ {
				members = append(members, od.Fields().Get(j))
			}
		} else {
			fd := desc.Fields().ByName(protoreflect.Name(name))
			switch {
			case fd == nil:
				return nil, nil, fmt.Errorf("%s names %q, which is not a field or a oneof of %s", where, name, desc.FullName())
			case lww.OneofOf(fd) != nil:
				return nil, nil, fmt.Errorf("%s names field %q, a member of oneof %q; name the oneof",
					where, name, lww.OneofOf(fd).Name())
			case fd.IsMap():
				return nil, nil, fmt.Errorf("%s names field %q, which is a map; name a reference or a repeated reference",
					where, name)
			}
			members = append(members, fd)
		}
		for _, fd := range members {
			refs := lww.RefsOf(fd)
			if len(refs) == 0 {
				return nil, nil, fmt.Errorf("%s: field %q must be a reference: a field whose type is an entity message "+
					"or a reference message", where, fd.Name())
			}
			if refs[0].Workspace != nil {
				return nil, nil, fmt.Errorf("%s: field %q is a reference message with a workspace field; "+
					"an access parent is a row of the holder's own workspace", where, fd.Name())
			}
			marked = append(marked, fd)
		}
	}
	var units []string
	fields := desc.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if !slices.Contains(marked, fd) {
			continue
		}
		unit := string(fd.Name())
		if od := lww.OneofOf(fd); od != nil {
			unit = string(od.Name())
		}
		if !slices.Contains(units, unit) {
			units = append(units, unit)
		}
	}
	var cases []parentCase
	for i := range info.refFields {
		if rf := &info.refFields[i]; slices.Contains(marked, rf.fd) {
			cases = append(cases, parentCase{ref: rf, entity: protoopts.Entity(rf.ref.Target).GetName()})
		}
	}
	return cases, units, nil
}

// refuseNestedMarks refuses a field behavior that only a field of the entity
// itself can carry, on a field of a message nested in the entity. mark names
// the behavior in the error, and has reports whether a field carries it. The
// walk does not enter entity-typed fields: those are other entities, checked
// when they register.
func refuseNestedMarks(desc protoreflect.MessageDescriptor, mark string, has func(*syncplanev1.FieldBehavior) bool) error {
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
				if f := subFields.Get(j); has(fieldBehaviorOf(f)) {
					return fmt.Errorf("field %q of %s is marked %s, but only a field declared on the entity itself can carry it",
						f.Name(), sub.FullName(), mark)
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
// and written under permissions of their own. It needs info.refFields and
// info.access.
func grantOf(desc protoreflect.MessageDescriptor, eo *syncplanev1.EntityOptions, info *entityInfo) (*grantSpec, error) {
	opt := eo.GetGrant()
	if opt == nil {
		return nil, nil
	}
	if err := refuseUnknownOptionFields("grant", opt); err != nil {
		return nil, err
	}
	if info.access == nil {
		return nil, fmt.Errorf("a grant entity must declare access")
	}
	subjects, err := grantSubjectOf(desc, info, opt.GetSubject())
	if err != nil {
		return nil, err
	}
	role, err := grantField(desc, "role", opt.GetRole())
	if err != nil {
		return nil, err
	}
	if role.Kind() != protoreflect.StringKind && role.Kind() != protoreflect.EnumKind {
		return nil, fmt.Errorf("grant role field %q is %s; it must be a string or an enum", role.Name(), role.Kind())
	}
	if slices.ContainsFunc(subjects, func(c subjectCase) bool { return c.fd == role }) {
		return nil, fmt.Errorf("grant names field %q as both subject and role", role.Name())
	}
	for _, fd := range append([]protoreflect.FieldDescriptor{role}, casesOf(subjects)...) {
		if g := info.access.guardFor(unitOf(fd)); g != nil && g.hides() {
			return nil, fmt.Errorf("grant field %q is restricted by a guard that can hide it; "+
				"the subject and role of a grant row are read by every reader of the row", unitOf(fd))
		}
	}
	if len(opt.GetRoles()) == 0 {
		return nil, fmt.Errorf("grant declares no roles")
	}
	g := &grantSpec{
		subjects:    subjects,
		subjectName: opt.GetSubject(),
		role:        role,
		roles:       make(map[string][]string, len(opt.GetRoles())),
	}
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
		if len(r.GetGrants()) == 0 && len(r.GetIncludes()) == 0 {
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
	if err := expandRoleIncludes(opt.GetRoles(), g.roles); err != nil {
		return nil, err
	}
	return g, nil
}

// expandRoleIncludes adds to each role in perms the permissions of every role
// it includes, directly or through other roles, and keeps each list sorted
// and without duplicates. perms holds the permissions each role of roles
// grants directly. An include names a role of the same grant option, so that
// accessSpecOf of one entity covers everything its roles grant.
func expandRoleIncludes(roles []*syncplanev1.Role, perms map[string][]string) error {
	includes := make(map[string][]string, len(roles))
	for _, r := range roles {
		seen := map[string]bool{}
		for _, inc := range r.GetIncludes() {
			if _, ok := perms[inc]; !ok {
				return fmt.Errorf("grant role %q includes %q, which is not a role of this grant; "+
					"a role includes only roles declared beside it", r.GetName(), inc)
			}
			if seen[inc] {
				return fmt.Errorf("grant role %q includes %q twice", r.GetName(), inc)
			}
			seen[inc] = true
		}
		includes[r.GetName()] = r.GetIncludes()
	}
	expanded := make(map[string][]string, len(roles))
	var expand func(name string, path []string) ([]string, error)
	expand = func(name string, path []string) ([]string, error) {
		if done, ok := expanded[name]; ok {
			return done, nil
		}
		if i := slices.Index(path, name); i >= 0 {
			return nil, fmt.Errorf("grant role %q includes itself through %s",
				name, strings.Join(append(slices.Clone(path[i:]), name), " → "))
		}
		path = append(path, name)
		out := slices.Clone(perms[name])
		for _, inc := range includes[name] {
			more, err := expand(inc, path)
			if err != nil {
				return nil, err
			}
			out = append(out, more...)
		}
		sort.Strings(out)
		out = slices.Compact(out)
		expanded[name] = out
		return out, nil
	}
	for _, r := range roles {
		if _, err := expand(r.GetName(), nil); err != nil {
			return err
		}
	}
	for name, p := range expanded {
		perms[name] = p
	}
	return nil
}

// grantSubjectOf resolves the subject a grant option names: a field, or a
// oneof whose members are the cases. Each case is a top-level, singular
// string field, or a reference without a workspace field. A reference message
// that holds a oneof of entities gives one case per target.
func grantSubjectOf(desc protoreflect.MessageDescriptor, info *entityInfo, name string) ([]subjectCase, error) {
	if name == "" {
		return nil, fmt.Errorf("grant names no subject field")
	}
	var fields []protoreflect.FieldDescriptor
	if od := desc.Oneofs().ByName(protoreflect.Name(name)); od != nil && !od.IsSynthetic() {
		for i := 0; i < od.Fields().Len(); i++ {
			fields = append(fields, od.Fields().Get(i))
		}
	} else {
		fd := desc.Fields().ByName(protoreflect.Name(name))
		if fd == nil {
			return nil, fmt.Errorf("grant subject field %q not found", name)
		}
		if od := lww.OneofOf(fd); od != nil {
			return nil, fmt.Errorf("grant subject field %q is a member of oneof %q; name the oneof", name, od.Name())
		}
		fields = append(fields, fd)
	}
	var cases []subjectCase
	for _, fd := range fields {
		if fd.IsList() || fd.IsMap() {
			return nil, fmt.Errorf("grant subject field %q is repeated; it must hold one value", fd.Name())
		}
		if fd.Kind() == protoreflect.StringKind {
			cases = append(cases, subjectCase{fd: fd})
			continue
		}
		refs := lww.RefsOf(fd)
		if len(refs) == 0 {
			return nil, fmt.Errorf("grant subject field %q is %s; it must be a string or a reference", fd.Name(), fd.Kind())
		}
		if refs[0].Workspace != nil {
			return nil, fmt.Errorf("grant subject field %q is a reference message with a workspace field; "+
				"a team is a row of the grant row's own workspace", fd.Name())
		}
		for i := range info.refFields {
			if rf := &info.refFields[i]; rf.fd == fd {
				cases = append(cases, subjectCase{fd: fd, team: rf})
			}
		}
	}
	return cases, nil
}

// membersOf checks an entity's members option and returns the name of the
// entity whose rows make members, "" when the entity declares none. Whether
// that entity is a grant entity anchored at this one is a catalog question,
// answered by compileAccessModel.
func membersOf(eo *syncplanev1.EntityOptions, access *entityAccess) (string, error) {
	opt := eo.GetMembers()
	if opt == nil {
		return "", nil
	}
	if err := refuseUnknownOptionFields("members", opt); err != nil {
		return "", err
	}
	if access == nil {
		return "", fmt.Errorf("an entity with members must declare access")
	}
	if opt.GetEntity() == "" {
		return "", fmt.Errorf("members names no entity")
	}
	return opt.GetEntity(), nil
}

// buildSubjectIndexes derives an index over each field the access model
// looks rows up by subject in: each string case of a grant entity's subject,
// so resolving a caller's grants probes its own rows, and the subject field
// of each read rule with a caller_is check, the entity's and its guards', so
// a read narrows to the rows that name the caller. A team case is a reference, which has its reference index
// already. Each index has the name an indexed mark on the field would give.
// It runs after buildDeclaredIndexes, so a field the schema marks indexed
// already has its index.
func buildSubjectIndexes(info *entityInfo) error {
	var fields []protoreflect.FieldDescriptor
	if info.grant != nil {
		fields = append(fields, info.grant.stringCases()...)
	}
	if info.access != nil {
		for _, r := range info.access.readerSets() {
			for _, o := range r.checked {
				if o.callerIs != nil && !slices.Contains(fields, o.callerIs) {
					fields = append(fields, o.callerIs)
				}
			}
		}
	}
	desc := info.msgType.Descriptor()
	for _, fd := range fields {
		if info.indexByName(fieldIndexName(fd)) != nil {
			continue
		}
		c, err := query.CompileIndexComponent(desc, []string{string(fd.Name())}, false)
		if err != nil {
			return fmt.Errorf("subject field %q: %w", fd.Name(), err)
		}
		comps := []query.IndexComponent{c}
		enc, err := query.NewIndexEncoder(comps)
		if err != nil {
			return fmt.Errorf("subject field %q: %w", fd.Name(), err)
		}
		info.indexes = append(info.indexes, compiledIndex{
			name:     fieldIndexName(fd),
			specHash: query.SpecHash(comps),
			encoder:  enc,
		})
	}
	return nil
}

// casesOf returns the field of each subject case.
func casesOf(cases []subjectCase) []protoreflect.FieldDescriptor {
	out := make([]protoreflect.FieldDescriptor, len(cases))
	for i, c := range cases {
		out[i] = c.fd
	}
	return out
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
	// across the schema, which is what lets a root role be named alone.
	roles map[string]*modelRole
	// granted maps every permission some role grants, directly or through an
	// include, to the names of those roles, sorted. A permission it lacks is
	// one no caller can hold.
	granted map[string][]string
	// grantEntities are the entities whose rows grant roles, sorted by name.
	grantEntities []*entityInfo
	// teamOf maps the name of each member entity to the team entity whose
	// members its rows make.
	teamOf map[string]string
	// teamSubjects maps the name of each grant entity whose subject can name a
	// team to the edges of its team cases, in field order: a row whose case
	// names a team row grants to the members of that row. subjectsOf maps the
	// name of each team entity to the edges that point at it, sorted by grant
	// entity name and then in field order.
	teamSubjects map[string][]*accessEdge
	subjectsOf   map[string][]*accessEdge
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
	// depths maps each model entity to the largest number of access parent
	// hops from its rows to an entity without a parent, or depthUnbounded.
	depths map[string]int
	// moving is set when a write to a row can change what the rows below it
	// show without a write to them, which the access move log records: some
	// entity has an access parent and rows that can sit below its rows, so
	// moving one of its rows moves rows below it, or some rule has a within
	// entry.
	moving bool
	// withins maps the spec of each within entry of every rule to the entry
	// resolved against the catalog. withinOn maps the name of each entity a
	// within entry names to the distinct entries that name it, sorted by spec.
	withins  map[string]*withinCheck
	withinOn map[string][]*withinCheck
	// readPerms are the permissions model entities read by, sorted. They are
	// what a cursor records of a caller's access.
	readPerms []string
	// readRules are the read rules of every entity, the ones outside the
	// model included, sorted by entity (see readRuleOf), and fingerprint is
	// their hash (see readPolicyOf). Two models with the same fingerprint read
	// the same rows from the same anchors. What roles grant is not part of
	// it: a cursor records that as the anchors a caller holds.
	readRules   []*syncplanecursorv1.EntityReadRule
	fingerprint uint64
	// warnings are findings that do not make the model invalid but are likely
	// mistakes. Registration logs them.
	warnings []string
	// unknownRootRoles are the undeclared root roles a warning was logged for
	// (see warnUnknownRootRole). They are the only part of the model that
	// changes after it is compiled.
	unknownRootRolesMu sync.Mutex
	unknownRootRoles   map[string]struct{}
}

// requiresPermission reports whether the workspace gate admits only a caller
// that holds a permission: the schema declares a role. Without one no caller
// can hold a permission, and the application's workspace hook alone admits
// callers. A nil model requires none.
func (m *accessModel) requiresPermission() bool {
	return m != nil && len(m.roles) > 0
}

// accessEdge is one case of an access parent in a compiled model: a row of
// child whose ref holds a value sits below the row of target it names.
type accessEdge struct {
	child, target *entityInfo
	ref           *refField
}

// parentsOf returns the rows that msg's access parents name, given the edges
// of its entity's cases, sorted by entity and key and without repeats. A case
// that is unset or names no key names no row.
func parentsOf(msg protoreflect.Message, edges []*accessEdge) []anchorRow {
	var out []anchorRow
	add := func(e *accessEdge, value protoreflect.Message) {
		// The cases inside one reference message share the field, and only the
		// active one yields a stub.
		if stub, ok := e.ref.ref.Stub(value); ok {
			if key := e.target.keyOf(stub); key != "" {
				out = append(out, anchorRow{entity: e.target.name, key: key})
			}
		}
	}
	for _, e := range edges {
		if !msg.Has(e.ref.fd) {
			continue
		}
		if !e.ref.fd.IsList() {
			add(e, msg.Get(e.ref.fd).Message())
			continue
		}
		list := msg.Get(e.ref.fd).List()
		for i := 0; i < list.Len(); i++ {
			add(e, list.Get(i).Message())
		}
	}
	return compactRows(out)
}

// parentOf returns the first row that msg's access parents name, given the
// edges of its entity's cases. ok is false when they name none. It serves the
// references that hold one row, such as a grant row's anchor.
func parentOf(msg protoreflect.Message, edges []*accessEdge) (anchorRow, bool) {
	rows := parentsOf(msg, edges)
	if len(rows) == 0 {
		return anchorRow{}, false
	}
	return rows[0], true
}

// compactRows sorts rows by entity and key and drops repeats, in place.
func compactRows(rows []anchorRow) []anchorRow {
	if len(rows) < 2 {
		return rows
	}
	slices.SortFunc(rows, func(a, b anchorRow) int {
		if c := strings.Compare(a.entity, b.entity); c != 0 {
			return c
		}
		return strings.Compare(a.key, b.key)
	})
	return slices.Compact(rows)
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
// An entity that declares no access is outside the model: no rule and no
// anchor applies to its rows, so every caller the workspace gate admits reads
// and writes them, as in a schema without the model. strict refuses such an
// entity instead (see WithStrictAccess). In a schema that declares a role the
// gate admits only callers that hold a permission, and each outside entity is
// open to all of them, which the model reports as a warning.
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
func compileAccessModel(c *catalog, mutators map[string]*mutatorInfo, maxDepth int, strict bool) (*accessModel, error) {
	all := make([]string, 0, len(c.entities))
	inUse := false
	for name, info := range c.entities {
		if isEngineEntity(info) {
			continue
		}
		all = append(all, name)
		if info.access != nil || info.grant != nil {
			inUse = true
		}
	}
	if !inUse {
		return nil, nil
	}
	sort.Strings(all)
	// names are the entities in the model; outside the ones that declare no
	// access. A grant entity declares access (see grantOf), so it is never
	// outside.
	var names, outside []string
	for _, name := range all {
		if c.entities[name].access == nil {
			if strict {
				return nil, fmt.Errorf("server: entity %q declares no access option; the server is built with WithStrictAccess, "+
					"so in a schema that uses the access model every entity declares access: { allow: [...] }", name)
			}
			outside = append(outside, name)
			continue
		}
		names = append(names, name)
	}

	m := &accessModel{
		roles:        map[string]*modelRole{},
		teamOf:       map[string]string{},
		teamSubjects: map[string][]*accessEdge{},
		subjectsOf:   map[string][]*accessEdge{},
		entities:     map[string]*entityInfo{},
		parents:      map[string][]*accessEdge{},
		anchors:      map[string]bool{},
		children:     map[string][]*accessEdge{},
		depths:       map[string]int{},
	}
	used := map[string][]string{} // permission → entities that name it
	for _, name := range names {
		info := c.entities[name]
		a := info.access
		m.entities[name] = info
		for _, p := range a.parents {
			target, ok := c.byType[p.ref.ref.Target.FullName()]
			if !ok {
				return nil, fmt.Errorf("server: entity %q access parent %q points at %s, which is not a registered entity",
					name, refCasePath(p.ref.ref), p.ref.ref.Target.FullName())
			}
			if target.access == nil {
				return nil, fmt.Errorf("server: entity %q access parent %q points at entity %q, which is not in the access model; "+
					"declare access on %q", name, refCasePath(p.ref.ref), target.name, target.name)
			}
			e := &accessEdge{child: info, target: target, ref: p.ref}
			m.parents[name] = append(m.parents[name], e)
			m.anchors[target.name] = true
			m.children[target.name] = append(m.children[target.name], e)
		}
		for _, r := range a.allRules() {
			for _, p := range r.rule.requires {
				if !slices.Contains(used[p], name) {
					used[p] = append(used[p], name)
				}
			}
		}
		if info.members != "" {
			if err := checkMembers(c, info); err != nil {
				return nil, err
			}
			m.teamOf[info.members] = name
		}
		if info.grant == nil {
			continue
		}
		for _, sc := range info.grant.subjects {
			if sc.team == nil {
				continue
			}
			target, ok := c.byType[sc.team.ref.Target.FullName()]
			if !ok {
				return nil, fmt.Errorf("server: entity %q grant subject field %q points at %s, which is not a registered entity",
					name, refCasePath(sc.team.ref), sc.team.ref.Target.FullName())
			}
			if target.members == "" {
				return nil, fmt.Errorf("server: entity %q grant subject field %q points at entity %q, which declares no members; "+
					"declare members on %q to grant to its members", name, refCasePath(sc.team.ref), target.name, target.name)
			}
			e := &accessEdge{child: info, target: target, ref: sc.team}
			m.teamSubjects[name] = append(m.teamSubjects[name], e)
			m.subjectsOf[target.name] = append(m.subjectsOf[target.name], e)
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
		d := m.depthOf(name)
		if d > maxDepth {
			return nil, fmt.Errorf("server: entity %q can sit %d access parent hops below an entity without a parent, "+
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
	if err := m.resolveWithins(names); err != nil {
		return nil, err
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
		if !ma.Admitted && !slices.Contains(used[ma.Permission], "mutator "+name) {
			used[ma.Permission] = append(used[ma.Permission], "mutator "+name)
		}
	}

	roleNames := make([]string, 0, len(m.roles))
	for name := range m.roles {
		roleNames = append(roleNames, name)
	}
	sort.Strings(roleNames)
	m.granted = map[string][]string{}
	for _, name := range roleNames {
		for _, p := range m.roles[name].perms {
			m.granted[p] = append(m.granted[p], name)
		}
	}
	perms := make([]string, 0, len(used))
	for p := range used {
		perms = append(perms, p)
	}
	sort.Strings(perms)
	for _, p := range perms {
		if m.granted[p] == nil {
			return nil, fmt.Errorf("server: permission %q of %s is granted by no role; declare a role that grants it",
				p, usedBy(used[p][0]))
		}
	}
	// A permission that only code asks about is valid, so it is not refused.
	// The warning is what is left of the check against a misspelled grant:
	// the reverse check above catches it unless another role spells the name
	// right.
	unused := make([]string, 0, len(m.granted))
	for p := range m.granted {
		if used[p] == nil {
			unused = append(unused, p)
		}
	}
	sort.Strings(unused)
	for _, p := range unused {
		roles := make([]string, len(m.granted[p]))
		for i, r := range m.granted[p] {
			roles[i] = strconv.Quote(r)
		}
		which := "role " + roles[0] + " grants"
		if len(roles) > 1 {
			which = "roles " + strings.Join(roles, ", ") + " grant"
		}
		m.warnings = append(m.warnings, fmt.Sprintf(
			"permission %q, which %s, is used by no access rule or mutator; "+
				"only EntityTx.Can and the client's hasPermission ask about it, so check its spelling if neither does",
			p, which))
	}
	for _, name := range names {
		for _, r := range c.entities[name].access.readerSets() {
			for _, p := range r.loose().named() {
				if !slices.Contains(m.readPerms, p) {
					m.readPerms = append(m.readPerms, p)
				}
			}
		}
	}
	sort.Strings(m.readPerms)
	for _, name := range names {
		m.warnings = append(m.warnings, m.undecidingRules(c.entities[name])...)
		m.warnings = append(m.warnings, m.impliedPermissions(c.entities[name])...)
	}
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

	if len(m.roles) > 0 {
		for _, name := range outside {
			m.warnings = append(m.warnings, fmt.Sprintf(
				"entity %q declares no access option, so every caller that holds a permission in a workspace reads and writes its rows; "+
					"declare access on it, or build the server with WithStrictAccess to refuse it", name))
		}
	}

	// The record covers the entities outside the model as well, so a cursor
	// resets when an entity it holds rows of joins or leaves the model, and
	// not when an entity is added outside it.
	for _, name := range all {
		m.readRules = append(m.readRules, &syncplanecursorv1.EntityReadRule{Entity: name, Rule: readRuleOf(c.entities[name])})
	}
	m.fingerprint = readPolicyOf(m.readRules)
	return m, nil
}

// withinCheck is a within entry resolved against the catalog: the entity
// whose rows it reads, and the checks such a row must pass.
type withinCheck struct {
	spec   string
	target *entityInfo
	// callerIs is nil without a caller_is check.
	callerIs protoreflect.FieldDescriptor
	// when is nil without a when check. whenSpec renders it as the entry's
	// spec does.
	when     *query.Compiled
	whenSpec string
}

// resolveWithins resolves the within entries of the rules of the named
// entities into m.withins and m.withinOn. An entry must name an entity in the
// model whose rows the rule's entity can sit below, and its checks must fit
// that entity's fields. It runs once m.below is complete.
func (m *accessModel) resolveWithins(names []string) error {
	m.withins = map[string]*withinCheck{}
	m.withinOn = map[string][]*withinCheck{}
	for _, name := range names {
		for _, r := range m.entities[name].access.allRules() {
			for j, w := range r.rule.within {
				where := fmt.Sprintf("server: entity %q %s within[%d]", name, r.where, j)
				target, ok := m.entities[w.entity]
				switch {
				case !ok:
					return fmt.Errorf("%s names entity %q, which is not a registered entity in the access model", where, w.entity)
				case !slices.Contains(m.below[w.entity], name) && w.entity == name:
					return fmt.Errorf("%s names the rule's own entity, whose rows do not sit below rows of their own entity; "+
						"check the row itself with caller_is or when", where)
				case !slices.Contains(m.below[w.entity], name):
					return fmt.Errorf("%s names entity %q, whose rows %q rows cannot sit below", where, w.entity, name)
				}
				spec := w.spec()
				if _, done := m.withins[spec]; done {
					continue
				}
				check := &withinCheck{spec: spec, target: target, whenSpec: w.whenSpec}
				desc := target.msgType.Descriptor()
				if w.callerIs != "" {
					fd, err := stringField(desc, w.callerIs)
					if err != nil {
						return fmt.Errorf("%s caller_is: entity %q: %w", where, w.entity, err)
					}
					check.callerIs = fd
				}
				reads := conditionHeads(w.when)
				if w.callerIs != "" {
					reads = append(reads, w.callerIs)
				}
				for _, field := range reads {
					unit := field
					if fd := desc.Fields().ByName(protoreflect.Name(field)); fd != nil {
						unit = unitOf(fd)
					}
					if g := target.access.guardFor(unit); g != nil && g.hides() {
						return fmt.Errorf("%s reads %q of entity %q, which a guard can hide; "+
							"a field that a within entry checks is one every reader of the row reads", where, field, w.entity)
					}
				}
				if w.when != nil {
					compiled, err := query.Compile(desc, w.when)
					if err != nil {
						return fmt.Errorf("%s when: entity %q: %w", where, w.entity, err)
					}
					check.when = compiled
				}
				m.withins[spec] = check
				m.withinOn[w.entity] = append(m.withinOn[w.entity], check)
				m.moving = true
			}
		}
	}
	for _, checks := range m.withinOn {
		sort.Slice(checks, func(i, j int) bool { return checks[i].spec < checks[j].spec })
	}
	return nil
}

// checkMembers checks the members option of the team entity info against the
// catalog: it names a grant entity whose access parent points at info alone,
// and whose subject is one string field. A member entity with a team case
// would make a team a member of a team, which the model does not resolve.
func checkMembers(c *catalog, info *entityInfo) error {
	member, ok := c.entities[info.members]
	switch {
	case !ok:
		return fmt.Errorf("server: entity %q members names entity %q, which is not registered", info.name, info.members)
	case member.grant == nil:
		return fmt.Errorf("server: entity %q members names entity %q, which is not a grant entity; "+
			"its rows must grant a role at the team they make their subject a member of", info.name, member.name)
	case member.access == nil || len(member.access.parents) != 1 || member.access.parents[0].entity != info.name:
		return fmt.Errorf("server: entity %q members names entity %q, whose access parent does not point at %q alone",
			info.name, member.name, info.name)
	case len(member.grant.subjects) != 1 || member.grant.subjects[0].team != nil:
		return fmt.Errorf("server: entity %q members names entity %q, whose grant subject is not one string field; "+
			"a team cannot be a member of a team", info.name, member.name)
	}
	return nil
}

// undecidingRules returns a warning for each rule of info that never decides
// an operation it names, because another rule of info names the operation and
// admits every caller and row it admits. Such a rule is likely a mistake: a
// narrower rule written beside a wider one reads as if it narrows the wider
// one, and it does not. m.roles must be complete.
func (m *accessModel) undecidingRules(info *entityInfo) []string {
	rules := info.access.rules
	var out []string
	for i, r := range rules {
		byRule := map[int][]string{}
		var deciders []int
		for _, o := range accessOpNames[:4] {
			if !r.names(o.op) {
				continue
			}
			for j, other := range rules {
				if j != i && other.names(o.op) && m.covers(other, r) {
					if byRule[j] == nil {
						deciders = append(deciders, j)
					}
					byRule[j] = append(byRule[j], o.name)
					break
				}
			}
		}
		for _, j := range deciders {
			out = append(out, fmt.Sprintf(
				"entity %q: access allow[%d] never decides %s: allow[%d] admits every caller and row it admits there",
				info.name, i, strings.Join(byRule[j], ", "), j))
		}
	}
	return out
}

// covers reports whether rule a admits every caller and row that rule b
// admits, for an operation both name: each permission a needs is held
// wherever b's permissions are, and each check a sets is one b sets as well,
// or a looser one.
func (m *accessModel) covers(a, b accessRule) bool {
	switch {
	case a.admitted():
	case b.admitted():
		return false
	default:
		for _, q := range a.requires {
			if !slices.ContainsFunc(b.requires, func(p string) bool { return m.implies(p, q) }) {
				return false
			}
		}
	}
	if a.callerIs != nil && a.callerIs != b.callerIs {
		return false
	}
	if a.when != nil && a.whenSpec != b.whenSpec {
		return false
	}
	if !a.withinCovers(b) {
		return false
	}
	if a.fields != nil && (b.fields == nil || !b.fieldsWithin(a.fields)) {
		return false
	}
	return true
}

// impliedPermissions returns a warning for each rule of info that needs two
// permissions where every role that grants one also grants the other. The
// rule admits the callers it would admit without the implied one, so it reads
// as stricter than it is.
func (m *accessModel) impliedPermissions(info *entityInfo) []string {
	var out []string
	for i, r := range info.access.rules {
		perms := r.requires
		for _, q := range perms {
			p := ""
			for _, other := range perms {
				if other != q && m.implies(other, q) {
					p = other
					break
				}
			}
			if p != "" {
				out = append(out, fmt.Sprintf(
					"entity %q: access allow[%d] needs %q and %q together, but every role that grants %q grants %q as well, "+
						"so the rule admits the same callers as one that needs %q alone",
					info.name, i, p, q, p, q, p))
				break
			}
		}
	}
	return out
}

// fieldsWithin reports whether every unit r's fields check allows is in
// fields, which is sorted. A rule without the check allows every unit.
func (r accessRule) fieldsWithin(fields []string) bool {
	if r.fields == nil {
		return false
	}
	for _, f := range r.fields {
		if _, found := slices.BinarySearch(fields, f); !found {
			return false
		}
	}
	return true
}

// implies reports whether a caller that holds permission p at a row holds q
// there as well: p is q, or every role that grants p also grants q. Global
// roles are declared roles too, so the answer covers them.
func (m *accessModel) implies(p, q string) bool {
	if p == q {
		return true
	}
	granted := false
	for _, r := range m.roles {
		if _, ok := slices.BinarySearch(r.perms, p); !ok {
			continue
		}
		granted = true
		if _, ok := slices.BinarySearch(r.perms, q); !ok {
			return false
		}
	}
	return granted
}

// depthOf returns the largest number of access parent hops from rows of the
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

// readersBelow returns who may read some row of each entity whose rows can
// sit below rows of the named entity, and who may read the units of each
// guard of such an entity that can hide them, with checks on the row dropped
// (see readers.loose), sorted by key and without repeats.
func (m *accessModel) readersBelow(name string) []readers {
	var out []readers
	seen := map[string]bool{}
	for _, below := range m.below[name] {
		for _, set := range m.entities[below].access.readerSets() {
			r := set.loose()
			if k := r.key(); !seen[k] {
				seen[k] = true
				out = append(out, r)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key() < out[j].key() })
	return out
}

// readRuleOf renders what decides which rows of an entity a caller reads, and
// which of their fields, in a canonical form: two entities with equal rules
// read the same rows and fields from the same anchors. An entity outside the
// model reads every row.
func readRuleOf(info *entityInfo) string {
	a := info.access
	if a == nil {
		return "open"
	}
	rule := "read=" + a.read.key()
	if a.hasParent() {
		rule += " parent=" + a.parentRule()
	}
	var guards []string
	for _, g := range a.guards {
		if g.hides() {
			guards = append(guards, strings.Join(g.units, ",")+"{"+g.read.key()+"}")
		}
	}
	if len(guards) > 0 {
		sort.Strings(guards)
		rule += " hide=" + strings.Join(guards, ";")
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
// it sets Admitted alone, or a permission whose row entity is in the model and
// whose key field is a singular string field of the args.
func checkMutatorAccess(c *catalog, mi *mutatorInfo, ma *MutatorAccess) error {
	if ma.Admitted {
		if ma.Permission != "" || ma.Entity != "" || ma.KeyField != "" {
			return fmt.Errorf("server: mutator %q MutatorAccess sets Admitted together with a Permission, Entity or KeyField; "+
				"set Admitted alone, or a Permission", mi.name)
		}
		return nil
	}
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
		if len(a.guards) > 0 {
			guards := make([]string, len(a.guards))
			for i, g := range a.guards {
				guards[i] = g.spec()
			}
			sort.Strings(guards)
			b.WriteString(" guard=" + strings.Join(guards, ";"))
		}
		if a.hasParent() {
			b.WriteString(" parent=" + a.parentRule())
		}
	}
	if info.members != "" {
		b.WriteString(" members=" + info.members)
	}
	if g := info.grant; g != nil {
		b.WriteString(" subject=" + g.subjectSpec() + " role=" + string(g.role.Name()))
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

// allowAccessChangeHint ends a registration error that AllowAccessChange
// lifts, naming the option on every path that registers a schema.
const allowAccessChangeHint = "register with server.AllowAccessChange() " +
	"(allow_access_change on AdminService.Register, allowAccessChange in registerSchema) to apply it"

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
		return fmt.Errorf("server: entity %q changes its access model (access or grant); %s",
			next.name, allowAccessChangeHint)
	}
	return nil
}

// gateState is how the workspace gate of a catalog admits callers.
type gateState uint8

const (
	// gateUnset is the state of a catalog that has not yet held an entity of
	// the application's under a valid access model.
	gateUnset gateState = iota
	// gateHook admits every caller the application's workspace hook admits.
	gateHook
	// gateGrants also refuses a caller that holds no permission (see
	// requiresPermission).
	gateGrants
)

// settleGate records how the workspace gate of c admits callers, once c holds
// an entity of the application's under a valid access model. Unless allow is
// set, it refuses a state that differs from the one c recorded before.
//
// A catalog whose model is invalid keeps the state recorded before it, so a
// run of RegisterEntity calls that passes through an incomplete model is
// judged against the last schema that served callers, not against the
// incomplete one, which served nobody.
func (c *catalog) settleGate(allow bool) error {
	if c.accessErr != nil || !hasAppEntity(c) {
		return nil
	}
	next := gateHook
	if c.access.requiresPermission() {
		next = gateGrants
	}
	if !allow && c.gate != gateUnset && c.gate != next {
		if next == gateGrants {
			return fmt.Errorf("server: the schema declares its first role, so the workspace gate starts to refuse "+
				"every caller that holds no permission; %s", allowAccessChangeHint)
		}
		return fmt.Errorf("server: the schema declares no role any more, so the workspace gate admits every caller "+
			"the application's workspace hook admits; %s", allowAccessChangeHint)
	}
	c.gate = next
	return nil
}

// hasAppEntity reports whether c holds an entity that the application
// registered.
func hasAppEntity(c *catalog) bool {
	for _, info := range c.entities {
		if !isEngineEntity(info) {
			return true
		}
	}
	return false
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
