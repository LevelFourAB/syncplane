package admin

import (
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/internal/admin/ui"
)

// filter.go is the condition builder behind the browse table's filter bar: it
// discovers which paths of an entity can be filtered, which operators each of
// them admits, and turns the submitted values into an engine query condition —
// the same predicate a sync client would send, so a filtered browse means
// exactly what the equivalent Query RPC means.
//
// Filter state lives entirely in the URL query string, so a filtered view is
// bookmarkable, survives a reload, and can be carried by the "Load more"
// cursor. The parameters use the same dotted/indexed shape the row editor binds
// (see form.go), so indicesFor reads both:
//
//	match           "all" (AND, the default) or "any" (OR)
//	f[0].path       dotted proto path, e.g. "settings.color"
//	f[0].op         operator token, e.g. "contains"
//	f[0].v          the operand; absent for the presence/emptiness operators,
//	                and repeated once per choice where several are allowed
//	f[0].s[1].path  a group's sub-conditions, named against the ELEMENT
//
// Two shapes are worth knowing before reading the rest.
//
// A REFERENCE is one field, not two. The picker offers "project", never
// "project · id": a reference stores its target's key, so the operand is a
// target row chosen through the same picker the row editor uses, and the
// condition it builds compares project.id underneath. The key path stays
// resolvable so a filter bookmarked before that still reads.
//
// A GROUP is how a repeated message list is asked about. Its operand is a
// block of sub-conditions, all evaluated against the SAME element, compiled to
// one any(). That scoping is the point: two ordinary rows over one list ask
// whether some element matches the first AND some (possibly other) element
// matches the second, which is a weaker and usually unintended question — see
// ExistsCond in syncplane/v1/query.proto.
//
// A row that is still being built — no field chosen, or no operand where one is
// needed — is carried through the UI but contributes no predicate, so a
// half-built condition never changes what the table shows. An operand that will
// not parse as the field's type is reported instead of dropped: silently
// ignoring it would widen the result set behind the user's back.

// valueClass is the kind of literal a field's operators compare against. It
// decides both which operators are offered and which control renders the
// operand.
type valueClass int

const (
	vcNone   valueClass = iota // nothing comparable (message, opaque bytes, message elements)
	vcString                   // string, or a collaborative-text field via its plain text
	vcNumber                   // any integer or floating-point kind
	vcEnum
	vcBool
	vcRef // a single-key reference, compared by the target key it stores
)

// filterField is one selectable path in the condition builder: where it points,
// what it compares against, and the shape that decides its operator list.
type filterField struct {
	path  string // dotted proto path, the wire form of the condition path
	label string
	fd    protoreflect.FieldDescriptor // the leaf the path resolves to

	// value is the leaf's comparable class — for a repeated field, its
	// ELEMENT's class, since membership compares elements.
	value    valueClass
	repeated bool
	message  bool // singular message or reference: presence is all that applies
	presence bool // the leaf has explicit presence, so is set / is not set mean something
	hidden   bool // resolvable, but not offered in the field picker

	// refEntity and refKeys describe a typed reference, singular or repeated.
	// A row stores only the target's key fields, so a comparison runs against
	// the key rather than against the reference itself (refKeyPath).
	refEntity string
	refKeys   []protoreflect.FieldDescriptor
}

// classOf maps a leaf to the literal class it compares against, mirroring the
// engine's own leaf classification (engine/query singularKind): a
// collaborative-text field compares as its visible string, an enum as its
// number, and everything numeric as a number.
func classOf(fd protoreflect.FieldDescriptor) valueClass {
	switch fd.Kind() {
	case protoreflect.StringKind:
		return vcString
	case protoreflect.BoolKind:
		return vcBool
	case protoreflect.EnumKind:
		return vcEnum
	case protoreflect.BytesKind:
		if isText(fd) && !fd.IsList() {
			return vcString
		}
		return vcNone
	default:
		if isNumeric(fd.Kind()) {
			return vcNumber
		}
		return vcNone
	}
}

// classify describes fd as a filterable leaf, reporting false when no operator
// applies to it at all. Maps are excluded because the only condition they admit
// is isSet, which is constitutionally true for them; opaque fields because
// their bytes are replaced whole and mean nothing to compare.
func classify(fd protoreflect.FieldDescriptor) (filterField, bool) {
	f := filterField{fd: fd, presence: fd.HasPresence()}
	if fd.IsMap() || isOpaque(fd) {
		return filterField{}, false
	}
	if keys := server.RefKeyFields(fd); keys != nil {
		f.refKeys, f.refEntity = keys, entityName(fd.Message())
		f.repeated, f.message = fd.IsList(), !fd.IsList()
		// Only a single-key reference has one comparable value per element —
		// the target key the row stores — so only that one can be compared
		// against a chosen row. A composite key has none, which is why the
		// engine rejects membership over one and points at any() instead.
		if len(keys) == 1 {
			f.value = vcRef
		}
		return f, true
	}
	switch {
	case fd.IsList():
		f.repeated = true
		f.value = classOf(fd)
	case fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind:
		f.message = true
	default:
		f.value = classOf(fd)
		if f.value == vcNone {
			return filterField{}, false
		}
	}
	return f, true
}

// group reports whether the field is asked about through an element-scoped
// group — an any() condition — rather than through an operator on the field
// itself. That is every repeated message list without a single comparable
// element: a keyed message list, and a composite-key reference list, whose
// contents the engine can only reach through a scoped sub-condition.
func (f filterField) group() bool {
	return f.repeated && f.value != vcRef &&
		(f.fd.Kind() == protoreflect.MessageKind || f.fd.Kind() == protoreflect.GroupKind)
}

// refKeyPath is the path a reference comparison actually runs against. A
// singular reference is compared through the target key field it stores; a
// repeated one keeps its own path, because membership is asked of the LIST and
// the engine desugars it to an exists over that same key.
func (f filterField) refKeyPath() string {
	if f.repeated {
		return f.path
	}
	return f.path + "." + string(f.refKeys[0].Name())
}

// filterFields lists every path of md the filter bar knows, in proto order:
// each top-level field, followed by the leaves one level under it.
//
// Two levels is the depth: the engine's path resolver cannot descend through a
// repeated field at all, and anything deeper would turn a one-line picker into
// a tree. A repeated field appears as itself — as membership, emptiness, or a
// group — never as a prefix.
func filterFields(md protoreflect.MessageDescriptor) []filterField {
	var out []filterField
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		f, ok := classify(fd)
		if !ok {
			continue
		}
		f.path, f.label = string(fd.Name()), humanLabel(fd.Name())
		out = append(out, f)
		switch {
		case f.refKeys != nil:
			// The reference IS the field the picker offers, so its key fields
			// are carried hidden — addressable, so a filter bookmarked when
			// they were offered still reads, but out of the field list.
			// Repeated references have no addressable key path of their own.
			if f.repeated {
				continue
			}
			for _, kf := range f.refKeys {
				s, ok := classify(kf)
				if !ok {
					continue
				}
				s.path = f.path + "." + string(kf.Name())
				s.label = f.label + " · " + humanLabel(kf.Name())
				s.hidden = true
				out = append(out, s)
			}
		case f.message:
			for _, sub := range nestedFields(fd) {
				s, ok := classify(sub)
				// A nested plain message would need a third level to be
				// useful, so the picker stops at the leaves it can compare —
				// and at references, which are a leaf in their own right.
				if !ok || (s.message && s.refKeys == nil) {
					continue
				}
				s.path = f.path + "." + string(sub.Name())
				s.label = f.label + " · " + humanLabel(sub.Name())
				out = append(out, s)
			}
		}
	}
	return out
}

// nestedFields returns the fields one level under a singular, non-reference
// message.
func nestedFields(fd protoreflect.FieldDescriptor) []protoreflect.FieldDescriptor {
	fields := fd.Message().Fields()
	out := make([]protoreflect.FieldDescriptor, 0, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		out = append(out, fields.Get(i))
	}
	return out
}

// elemFields lists the paths a group's sub-conditions can name: the fields of
// ONE element of the repeated message, resolved against the element rather
// than against the row.
//
// A reference element carries only its target's key fields, and the engine
// rejects a sub-condition addressing anything else (requireRefKeyPaths in
// engine/query), so those are all a reference list offers. Groups do not nest:
// an element's own repeated message lists are left out rather than growing a
// group inside a group.
func elemFields(fd protoreflect.FieldDescriptor) []filterField {
	if keys := server.RefKeyFields(fd); keys != nil {
		out := make([]filterField, 0, len(keys))
		for _, kf := range keys {
			f, ok := classify(kf)
			if !ok {
				continue
			}
			f.path, f.label = string(kf.Name()), humanLabel(kf.Name())
			out = append(out, f)
		}
		return out
	}
	var out []filterField
	for _, f := range filterFields(fd.Message()) {
		if f.hidden || f.group() {
			continue
		}
		out = append(out, f)
	}
	return out
}

// filterOp is an operator token, submitted as f[i].op.
type filterOp string

const (
	opEq          filterOp = "eq"
	opNeq         filterOp = "neq"
	opGt          filterOp = "gt"
	opGte         filterOp = "gte"
	opLt          filterOp = "lt"
	opLte         filterOp = "lte"
	opContains    filterOp = "contains"
	opNotContains filterOp = "not_contains"
	opStartsWith  filterOp = "starts_with"
	opEndsWith    filterOp = "ends_with"
	opIn          filterOp = "in"
	opHas         filterOp = "has"
	opHasAny      filterOp = "has_any"
	opHasAll      filterOp = "has_all"
	opAny         filterOp = "any"
	opIsEmpty     filterOp = "is_empty"
	opIsNotEmpty  filterOp = "is_not_empty"
	opIsSet       filterOp = "is_set"
	opIsNotSet    filterOp = "is_not_set"
)

// opLabels are the words the operator select shows. They read as the middle of
// a sentence — "title contains milk" — rather than as engine vocabulary.
var opLabels = map[filterOp]string{
	opEq:          "is",
	opNeq:         "is not",
	opGt:          ">",
	opGte:         "≥",
	opLt:          "<",
	opLte:         "≤",
	opContains:    "contains",
	opNotContains: "does not contain",
	opStartsWith:  "starts with",
	opEndsWith:    "ends with",
	opIn:          "is any of",
	opHas:         "has",
	opHasAny:      "has any of",
	opHasAll:      "has all of",
	opAny:         "has an item where",
	opIsEmpty:     "is empty",
	opIsNotEmpty:  "is not empty",
	opIsSet:       "is set",
	opIsNotSet:    "is not set",
}

// multiValue reports whether op takes a list of operands rather than a single
// one — entered comma separated, or chosen one chip at a time.
func multiValue(op filterOp) bool {
	return op == opIn || op == opHasAny || op == opHasAll
}

// ops lists the operators offered for this field, most useful first — the first
// one is what a freshly picked field defaults to.
//
// The multi-value operators are offered where the operand is free text
// (strings and numbers) or a chosen row (references). On an enum or a bool the
// operand is a chosen option, and a list of chosen options wants a control the
// bar has no room for; "is" twice under "match any" says the same thing.
func (f filterField) ops() []filterOp {
	switch {
	case f.group():
		return []filterOp{opAny, opIsEmpty, opIsNotEmpty}
	case f.value == vcRef && f.repeated:
		return []filterOp{opHas, opHasAny, opHasAll, opIsEmpty, opIsNotEmpty}
	case f.value == vcRef:
		return []filterOp{opEq, opIn, opIsSet, opIsNotSet}
	case f.message:
		return []filterOp{opIsSet, opIsNotSet}
	case f.repeated:
		var ops []filterOp
		switch f.value {
		case vcNone: // a list of opaque bytes: only its length
		case vcString, vcNumber:
			ops = append(ops, opHas, opHasAny, opHasAll)
		default:
			ops = append(ops, opHas)
		}
		return append(ops, opIsEmpty, opIsNotEmpty)
	}
	var ops []filterOp
	switch f.value {
	case vcString:
		ops = []filterOp{opContains, opNotContains, opEq, opNeq, opStartsWith, opEndsWith, opIn}
	case vcNumber:
		ops = []filterOp{opEq, opNeq, opGt, opGte, opLt, opLte, opIn}
	case vcEnum:
		ops = []filterOp{opEq, opNeq}
	case vcBool:
		ops = []filterOp{opEq}
	}
	if f.presence {
		// Offered only where presence is a real question: an implicit-presence
		// scalar is always set, so the operator would be a false lead.
		ops = append(ops, opIsSet, opIsNotSet)
	}
	return ops
}

// control is the operand control op needs on this field. A group takes none:
// its operand is the block of sub-conditions the row renders instead.
func (f filterField) control(op filterOp) ui.ValueKind {
	switch op {
	case opIsSet, opIsNotSet, opIsEmpty, opIsNotEmpty, opAny:
		return ui.ValueNone
	}
	if f.value == vcRef {
		return ui.ValueRef
	}
	if multiValue(op) {
		return ui.ValueList
	}
	switch f.value {
	case vcBool:
		return ui.ValueBool
	case vcEnum:
		return ui.ValueEnum
	case vcNumber:
		return ui.ValueNumber
	default:
		return ui.ValueText
	}
}

// operand renders the operand control for op at input-name base name, carrying
// the current values so a re-rendered row keeps what the user chose or typed.
func (f filterField) operand(name string, op filterOp, values []string, refs refContext) ui.ValueControl {
	ctl := ui.ValueControl{Kind: f.control(op), Value: firstOf(values)}
	switch ctl.Kind {
	case ui.ValueRef:
		ctl.Ref = f.refControl(name, op, values, refs)
	case ui.ValueEnum:
		ctl.Options = enumFilterOptions(f.fd, ctl.Value)
	case ui.ValueList:
		ctl.Placeholder = "a, b, c"
	case ui.ValueText, ui.ValueNumber:
		ctl.Placeholder = "value"
	}
	return ctl
}

// refControl is the picker that chooses a reference operand — the same
// component the row editor uses, in its compact form. Each chip carries a
// target key, which is what the condition compares against and what the URL
// holds; the labels start as those keys and are replaced by the targets'
// display names once the rows can be read (fillRefLabels).
func (f filterField) refControl(name string, op filterOp, values []string, refs refContext) ui.RefControl {
	key := string(f.refKeys[0].Name())
	c := ui.RefControl{
		Name: name + ".v", Entity: f.refEntity, Keys: []string{key},
		PickerURL: refs.pickerURL(f.refEntity), Multi: multiValue(op), Inline: true,
	}
	for _, v := range operandList(op, values) {
		c.Items = append(c.Items, ui.RefItem{
			Values: map[string]string{key: v}, Label: v, Color: colorSlot(v), Initials: initials(v),
		})
		if !c.Multi {
			break // a single-valued operator holds one target
		}
	}
	return c
}

// enumFilterOptions lists an enum leaf's values for the operand select. The
// option value is the enum NUMBER, which is what the engine compares against.
func enumFilterOptions(fd protoreflect.FieldDescriptor, selected string) []ui.EnumOption {
	vals := fd.Enum().Values()
	out := make([]ui.EnumOption, 0, vals.Len())
	for i := 0; i < vals.Len(); i++ {
		v := vals.Get(i)
		num := strconv.FormatInt(int64(v.Number()), 10)
		out = append(out, ui.EnumOption{
			Value:    num,
			Label:    enumLabel(string(v.Name())),
			Selected: num == selected,
		})
	}
	return out
}

// condition turns one operator and its operands into an engine condition.
func (f filterField) condition(op filterOp, values []string) (*syncplanev1.Condition, error) {
	switch op {
	case opIsSet:
		return query.IsSet(f.path), nil
	case opIsNotSet:
		return query.IsNotSet(f.path), nil
	case opIsEmpty:
		return query.IsEmpty(f.path), nil
	case opIsNotEmpty:
		return query.Not(query.IsEmpty(f.path)), nil
	}
	raw := firstOf(values)
	switch op {
	case opContains:
		return query.Contains(f.path, raw), nil
	case opNotContains:
		// Not inverts the MATCH, not the meaning: a row whose field is unset
		// matches "does not contain", the same way it fails "contains".
		return query.Not(query.Contains(f.path, raw)), nil
	case opStartsWith:
		return query.StartsWith(f.path, raw), nil
	case opEndsWith:
		return query.EndsWith(f.path, raw), nil
	}

	list := operandList(op, values)
	if len(list) == 0 {
		return nil, fmt.Errorf("needs at least one value")
	}
	if f.value == vcRef {
		// A reference is compared by the target key it stores, so the operands
		// parse as that key field even though the path may name the list.
		return leafCond(f.refKeyPath(), f.refKeys[0], op, list)
	}
	return leafCond(f.path, f.fd, op, list)
}

// leafCond builds a value-carrying condition at path from operands parsed as
// the type fd holds. path and fd come apart wherever the condition names
// something other than the value it compares: a reference names the field
// while its key field holds the type, and membership names the list while its
// elements do.
func leafCond(path string, fd protoreflect.FieldDescriptor, op filterOp, values []string) (*syncplanev1.Condition, error) {
	switch classOf(fd) {
	case vcString:
		return typedCond(path, op, values)
	case vcBool:
		vs, err := parseAll(values, parseFilterBool)
		if err != nil {
			return nil, err
		}
		return typedCond(path, op, vs)
	case vcEnum:
		vs, err := parseAll(values, func(s string) (int32, error) { return enumNumber(fd, s) })
		if err != nil {
			return nil, err
		}
		return typedCond(path, op, vs)
	case vcNumber:
		return numberCond(path, fd, op, values)
	}
	return nil, fmt.Errorf("%s cannot be compared", humanLabel(fd.Name()))
}

// numberCond builds a numeric condition in the width the leaf actually holds,
// so a 64-bit id is compared as an integer rather than through a float.
func numberCond(path string, fd protoreflect.FieldDescriptor, op filterOp, values []string) (*syncplanev1.Condition, error) {
	switch fd.Kind() {
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		vs, err := parseAll(values, parseFilterUint)
		if err != nil {
			return nil, err
		}
		return typedCond(path, op, vs)
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		vs, err := parseAll(values, parseFilterFloat)
		if err != nil {
			return nil, err
		}
		return typedCond(path, op, vs)
	default:
		vs, err := parseAll(values, parseFilterInt)
		if err != nil {
			return nil, err
		}
		return typedCond(path, op, vs)
	}
}

// typedCond builds the condition for a value-carrying operator from operands
// already parsed to the leaf's Go type. The single-operand operators read
// vs[0]; callers guarantee at least one.
func typedCond[T query.Value](path string, op filterOp, vs []T) (*syncplanev1.Condition, error) {
	switch op {
	case opEq:
		return query.Eq(path, vs[0]), nil
	case opNeq:
		return query.Neq(path, vs[0]), nil
	case opGt:
		return query.Gt(path, vs[0]), nil
	case opGte:
		return query.Gte(path, vs[0]), nil
	case opLt:
		return query.Lt(path, vs[0]), nil
	case opLte:
		return query.Lte(path, vs[0]), nil
	case opIn:
		return query.In(path, vs...), nil
	case opHas:
		return query.Has(path, vs[0]), nil
	case opHasAny:
		return query.HasAny(path, vs...), nil
	case opHasAll:
		return query.HasAll(path, vs...), nil
	}
	return nil, fmt.Errorf("unknown operator %q", op)
}

// operandList reads a row's operands. The reference picker submits one f[i].v
// per chosen target, so several values are already a list; a lone operand is
// split on commas for the multi-value operators, which keeps a hand-written
// URL working, and taken verbatim otherwise.
func operandList(op filterOp, values []string) []string {
	if len(values) > 1 {
		out := make([]string, 0, len(values))
		for _, v := range values {
			if s := strings.TrimSpace(v); s != "" {
				out = append(out, s)
			}
		}
		return out
	}
	if len(values) == 0 || strings.TrimSpace(values[0]) == "" {
		return nil
	}
	if multiValue(op) {
		return splitList(values[0])
	}
	return []string{values[0]}
}

// splitList reads the comma-separated operand of a multi-value operator,
// dropping blanks so a trailing comma is not a value.
func splitList(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		if s := strings.TrimSpace(part); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func firstOf(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func parseAll[T any](raw []string, parse func(string) (T, error)) ([]T, error) {
	out := make([]T, 0, len(raw))
	for _, s := range raw {
		v, err := parse(strings.TrimSpace(s))
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func parseFilterBool(s string) (bool, error) {
	switch strings.ToLower(s) {
	case "true", "yes", "1":
		return true, nil
	case "false", "no", "0":
		return false, nil
	}
	return false, fmt.Errorf("%q is not true or false", s)
}

func parseFilterInt(s string) (int64, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%q is not a whole number", s)
	}
	return n, nil
}

func parseFilterUint(s string) (uint64, error) {
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%q is not a non-negative whole number", s)
	}
	return n, nil
}

func parseFilterFloat(s string) (float64, error) {
	n, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("%q is not a number", s)
	}
	return n, nil
}

// enumNumber resolves an enum operand: the select submits the number, while a
// hand-written URL may name the value.
func enumNumber(fd protoreflect.FieldDescriptor, s string) (int32, error) {
	if n, err := strconv.ParseInt(s, 10, 32); err == nil {
		return int32(n), nil
	}
	if ev := fd.Enum().Values().ByName(protoreflect.Name(s)); ev != nil {
		return int32(ev.Number()), nil
	}
	return 0, fmt.Errorf("%q is not a value of %s", s, fd.Enum().Name())
}

// refContext locates the rows a reference operand picks from: the app and
// workspace being browsed. The zero value leaves a reference operand without a
// picker, which is what a render outside a workspace (and the unit tests) get.
type refContext struct{ appID, ws string }

func (c refContext) pickerURL(target string) string {
	if c.appID == "" || c.ws == "" || target == "" {
		return ""
	}
	return fmt.Sprintf("/apps/%s/ws/%s/e/%s/picker", c.appID, c.ws, target)
}

// fieldSet is the set of paths one condition scope can name: the row's own
// fields, or — inside a group — the fields of one element.
type fieldSet struct {
	fields []filterField
	byPath map[string]filterField
}

func newFieldSet(fields []filterField) fieldSet {
	s := fieldSet{fields: fields, byPath: make(map[string]filterField, len(fields))}
	for _, f := range fields {
		s.byPath[f.path] = f
	}
	return s
}

// options lists what the field select offers, which is every path but the ones
// carried hidden.
func (s fieldSet) options() []ui.FilterFieldOption {
	out := make([]ui.FilterFieldOption, 0, len(s.fields))
	for _, f := range s.fields {
		if f.hidden {
			continue
		}
		out = append(out, ui.FilterFieldOption{Path: f.path, Label: f.label})
	}
	return out
}

// resolve settles a row's operator against this scope and compiles its
// predicate, returning the field it named so the caller can tell a group from
// an ordinary row. An operator that does not apply to the chosen field falls
// back to that field's default, which is what makes changing the field alone
// leave a usable row behind.
//
// A group stops short of its predicate: its operand is the sub-rows, which
// only the caller holds.
func (s fieldSet) resolve(c parsedCond) (parsedCond, filterField) {
	if c.path == "" {
		return c, filterField{}
	}
	f, ok := s.byPath[c.path]
	if !ok {
		c.err = fmt.Sprintf("%q cannot be filtered", c.path)
		return c, filterField{}
	}
	ops := f.ops()
	if len(ops) == 0 {
		return c, f
	}
	if !slices.Contains(ops, c.op) {
		c.op = ops[0]
	}
	if c.op == opAny {
		return c, f
	}
	if f.control(c.op) != ui.ValueNone && len(operandList(c.op, c.values)) == 0 {
		return c, f // still being built
	}
	cond, err := f.condition(c.op, c.values)
	if err != nil {
		c.err = err.Error()
		return c, f
	}
	c.cond = cond
	return c, f
}

// filterState is the parsed filter of one browse request: the paths that can be
// filtered, the conditions the user built, and how they combine.
type filterState struct {
	row   fieldSet
	elems map[string]fieldSet // element scopes, keyed by their group field's path
	conds []parsedCond
	match string
}

// parsedCond is one submitted condition row. cond is nil while the row is
// incomplete or in error, which is what keeps it out of the predicate.
type parsedCond struct {
	index  int
	path   string
	op     filterOp
	values []string // every f[i].v submitted; several where the operator allows it
	err    string
	cond   *syncplanev1.Condition
	// group marks a row whose operand is subs — sub-conditions evaluated
	// against one element of the repeated field it names.
	group bool
	subs  []parsedCond
}

// newFilterState reads the filter parameters out of a browse request's query.
func newFilterState(md protoreflect.MessageDescriptor, q url.Values) filterState {
	s := filterState{
		row:   newFieldSet(filterFields(md)),
		elems: map[string]fieldSet{},
		match: ui.MatchAll,
	}
	if q.Get("match") == ui.MatchAny {
		s.match = ui.MatchAny
	}
	for _, f := range s.row.fields {
		if f.group() {
			s.elems[f.path] = newFieldSet(elemFields(f.fd))
		}
	}
	for _, i := range indicesFor(q, "f") {
		name := fmt.Sprintf("f[%d]", i)
		s.conds = append(s.conds, s.resolve(parsedCond{
			index:  i,
			path:   strings.TrimSpace(q.Get(name + ".path")),
			op:     filterOp(q.Get(name + ".op")),
			values: q[name+".v"],
		}, name, q))
	}
	return s
}

// resolve settles one top-level row, descending into a group's sub-rows.
func (s filterState) resolve(c parsedCond, name string, q url.Values) parsedCond {
	c, f := s.row.resolve(c)
	if c.op != opAny || !f.group() {
		return c
	}
	c.group = true
	return s.resolveGroup(f, c, name, q)
}

// resolveGroup reads a group's sub-rows and folds them into one any(). Every
// complete sub-condition joins an AND inside the SAME element scope: that is
// what a group says, and what two ordinary rows over the same list cannot say.
// A group with nothing complete in it contributes no predicate, like any other
// half-built row.
func (s filterState) resolveGroup(f filterField, c parsedCond, name string, q url.Values) parsedCond {
	elems := s.elems[f.path]
	base := name + ".s"
	var conds []*syncplanev1.Condition
	bad := false
	for _, j := range indicesFor(q, base) {
		sub := fmt.Sprintf("%s[%d]", base, j)
		p, _ := elems.resolve(parsedCond{
			index:  j,
			path:   strings.TrimSpace(q.Get(sub + ".path")),
			op:     filterOp(q.Get(sub + ".op")),
			values: q[sub+".v"],
		})
		c.subs = append(c.subs, p)
		bad = bad || p.err != ""
		if p.cond != nil {
			conds = append(conds, p.cond)
		}
	}
	if len(c.subs) == 0 {
		// A group the user has only just opened carries one blank sub-row, so
		// it is usable without a round trip to add one.
		c.subs = []parsedCond{{}}
	}
	if bad || len(conds) == 0 {
		return c
	}
	inner := conds[0]
	if len(conds) > 1 {
		inner = query.And(conds...)
	}
	c.cond = query.Any(f.path, inner)
	return c
}

// condition combines the complete rows into the predicate the browse query
// runs, or nil when the filter selects everything.
func (s filterState) condition() *syncplanev1.Condition {
	var conds []*syncplanev1.Condition
	for _, c := range s.conds {
		if c.cond != nil {
			conds = append(conds, c.cond)
		}
	}
	switch {
	case len(conds) == 0:
		return nil
	case len(conds) == 1:
		return conds[0]
	case s.match == ui.MatchAny:
		return query.Or(conds...)
	default:
		return query.And(conds...)
	}
}

// err is the first condition that could not be read, empty when they all could.
// A filter that fails to parse is not applied at all: the table reports the
// problem rather than showing rows the user did not ask for.
func (s filterState) err() string {
	for _, c := range s.conds {
		if c.err != "" {
			return c.err
		}
		for _, sub := range c.subs {
			if sub.err != "" {
				return sub.err
			}
		}
	}
	return ""
}

// view renders the filter bar. action is where the form submits (the entity's
// browse URL), rowURL the endpoint that re-renders a single condition row, and
// refs locates the target rows a reference operand picks from.
func (s filterState) view(action, rowURL string, refs refContext) ui.FilterView {
	v := ui.FilterView{Action: action, RowURL: rowURL, Match: s.match, Fields: s.row.options()}
	for _, c := range s.conds {
		v.Conds = append(v.Conds, s.condView(c, refs))
		if c.cond != nil {
			v.Active = true
		}
	}
	// The blank row "+ Condition" clones; admin.js substitutes the __i__ index
	// placeholder in its input names, exactly as for a repeated form field.
	v.Template = ui.FilterCondView{Name: "f[__i__]"}
	return v
}

func (s filterState) condView(c parsedCond, refs refContext) ui.FilterCondView {
	name := fmt.Sprintf("f[%d]", c.index)
	out := ui.FilterCondView{Name: name, Path: c.path, Error: c.err}
	f, ok := s.row.byPath[c.path]
	if !ok {
		return out
	}
	out.Ops = opOptions(f.ops(), c.op)
	if !c.group {
		out.Value = f.operand(name, c.op, c.values, refs)
		return out
	}
	out.Group = true
	elems := s.elems[f.path]
	out.SubFields = elems.options()
	for _, sub := range c.subs {
		out.Subs = append(out.Subs, subCondView(elems, name, sub, refs))
	}
	blank := ui.FilterCondView{Name: name + ".s[__i__]"}
	out.SubTemplate = &blank
	return out
}

// subCondView renders one sub-row of a group against the element's field set.
func subCondView(elems fieldSet, groupName string, c parsedCond, refs refContext) ui.FilterCondView {
	name := fmt.Sprintf("%s.s[%d]", groupName, c.index)
	out := ui.FilterCondView{Name: name, Path: c.path, Error: c.err}
	f, ok := elems.byPath[c.path]
	if !ok {
		return out
	}
	out.Ops = opOptions(f.ops(), c.op)
	out.Value = f.operand(name, c.op, c.values, refs)
	return out
}

func opOptions(ops []filterOp, selected filterOp) []ui.FilterOpOption {
	out := make([]ui.FilterOpOption, 0, len(ops))
	for _, op := range ops {
		out = append(out, ui.FilterOpOption{
			Value:    string(op),
			Label:    opLabels[op],
			Selected: op == selected,
		})
	}
	return out
}
