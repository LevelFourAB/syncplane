// Package query builds, compiles and evaluates syncplane.v1.Condition predicates
// against protoreflect messages. It is the one query implementation in the
// engine: the Query RPC, ViewTx.Query and application code all go through it,
// so a predicate means the same thing wherever it is asked.
//
// Build predicates with the constructors (Eq, In, Contains, And, …) rather
// than assembling Condition protos by hand:
//
//	rows, err := srv.View(ctx, ws, func(tx *server.ViewTx) error {
//		return tx.Query(&todov1.Todo{}, query.Spec{
//			Where:   query.And(query.Eq("done", false), query.Contains("title", "milk")),
//			OrderBy: []*syncplanev1.OrderKey{query.Desc("created_at_ms")},
//			Limit:   20,
//		}, func(msg proto.Message, version uint64) error { … })
//	})
//
// Cost to know before reaching for it: nothing plans a predicate against a
// secondary index yet, so every query scans and decodes every live row of the
// entity — O(entity) in time however selective the predicate is and however
// small the limit. Memory is bounded to Offset+Limit rows when an order is
// given. That is fine for a background job and a trap in a hot HTTP handler
// over a large entity. (Indexes themselves exist and are maintained; see
// indexkey.go for the entry encoding and engine/server for what reads them.)
//
// Semantics mirror the client evaluator (client/src/query/evaluate.ts)
// exactly; the shared conformance vectors in testdata/queryeval enforce the
// parity. The load-bearing rules:
//
//   - An unset intermediate message makes every condition on the path false,
//     except isNotSet, which is true.
//   - A question about the CONTENTS of message elements is spelled with an
//     exists condition, which scopes an element and evaluates a sub-condition
//     against it. The quantifier is explicit so that two conditions can be said
//     to describe the same element; see ExistsCond in the proto for why a bare
//     path through a repeated field is not enough. has/hasAny/hasAll over a
//     repeated single-key reference desugar to one, since a reference compares
//     by its target's key.
//   - Values keep their client-side kind: 64-bit integer fields are bigints,
//     everything else numeric is a float64. Mixed number/bigint compares as
//     big integer when the float is integral, otherwise as float64 (with the
//     same precision loss as JS Number(bigint)).
//   - Strings order by UTF-16 code unit (JS `<`), not UTF-8 bytes.
//   - NaN is NOT COMPARABLE. A NaN on either side makes gt/gte/lt/lte false,
//     the same way eq fails and neq succeeds, so no comparison operator ever
//     matches it. For ordering it is not a tie either: values rank
//     real < NaN < missing, and the rank decides before the direction does,
//     so NaN sorts after every real value and before missing under both asc
//     and desc. Two NaNs (like two missing values) tie and fall through to
//     the next order key. That total order is what lets a row ordering be
//     encoded into sortable index keys. ±Inf is ordinary and compares as you
//     would expect.
//   - A mismatch between a condition's LITERAL and the field's value makes the
//     condition false (orderings tie), never an error: eq("x") against an
//     int32 field is a legitimate no-match. A field that could never take part
//     in the comparison at all — bytes, message, map, repeated — is a
//     different thing and is rejected by Compile instead of quietly matching
//     nothing (see requireComparable).
package query

import (
	"fmt"
	"math"
	"math/big"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/lww"
	"github.com/levelfourab/syncplane/engine/text"
)

// Compiled is a predicate with all paths resolved to descriptor chains.
type Compiled struct {
	root node // nil means "match everything"
}

// Compile validates the condition against the message descriptor and
// resolves every path once. A nil condition compiles to match-all. Errors
// (unknown fields, descents through non-message fields, operators the leaf
// can never satisfy, malformed conditions) belong at the RPC boundary as
// InvalidArgument.
func Compile(desc protoreflect.MessageDescriptor, cond *syncplanev1.Condition) (*Compiled, error) {
	if cond == nil {
		return &Compiled{}, nil
	}
	root, err := compileCond(desc, cond)
	if err != nil {
		return nil, err
	}
	return &Compiled{root: root}, nil
}

// Eval reports whether the message matches the predicate.
func (c *Compiled) Eval(msg protoreflect.Message) bool {
	if c.root == nil {
		return true
	}
	return c.root.eval(msg)
}

type node interface {
	eval(msg protoreflect.Message) bool
}

func compileCond(desc protoreflect.MessageDescriptor, cond *syncplanev1.Condition) (node, error) {
	switch kind := cond.GetKind().(type) {
	case *syncplanev1.Condition_Cmp:
		c := kind.Cmp
		fields, err := resolveFields(desc, c.GetPath())
		if err != nil {
			return nil, err
		}
		if c.GetOp() == syncplanev1.CmpOp_CMP_OP_UNSPECIFIED {
			return nil, fmt.Errorf("cmp condition needs an op")
		}
		if err := requireComparable(c.GetPath(), fields); err != nil {
			return nil, err
		}
		val, err := condValueOf(c.GetValue())
		if err != nil {
			return nil, err
		}
		return &cmpNode{fields: fields, op: c.GetOp(), value: val}, nil
	case *syncplanev1.Condition_In:
		c := kind.In
		fields, err := resolveFields(desc, c.GetPath())
		if err != nil {
			return nil, err
		}
		if err := requireComparable(c.GetPath(), fields); err != nil {
			return nil, err
		}
		values, err := condValuesOf(c.GetValues())
		if err != nil {
			return nil, err
		}
		return &inNode{fields: fields, values: values}, nil
	case *syncplanev1.Condition_Str:
		c := kind.Str
		fields, err := resolveFields(desc, c.GetPath())
		if err != nil {
			return nil, err
		}
		if c.GetOp() == syncplanev1.StrOp_STR_OP_UNSPECIFIED {
			return nil, fmt.Errorf("str condition needs an op")
		}
		if k := leafKind(fields); k != rvString {
			return nil, fmt.Errorf("path %v: startsWith/endsWith/contains need a string or text field, got %s",
				c.GetPath(), describeLeaf(fields[len(fields)-1]))
		}
		return &strNode{fields: fields, op: c.GetOp(), value: c.GetValue()}, nil
	case *syncplanev1.Condition_Rep:
		c := kind.Rep
		fields, err := resolveFields(desc, c.GetPath())
		if err != nil {
			return nil, err
		}
		leaf := fields[len(fields)-1]
		if leafKind(fields) != rvList {
			return nil, fmt.Errorf("path %v: has/hasAny/hasAll/isEmpty need a repeated field, got %s",
				c.GetPath(), describeLeaf(leaf))
		}
		values, err := condValuesOf(c.GetValues())
		if err != nil {
			return nil, err
		}
		switch c.GetOp() {
		case syncplanev1.RepOp_REP_OP_HAS:
			if len(values) != 1 {
				return nil, fmt.Errorf("rep has condition needs exactly one value")
			}
		case syncplanev1.RepOp_REP_OP_HAS_ANY, syncplanev1.RepOp_REP_OP_HAS_ALL, syncplanev1.RepOp_REP_OP_IS_EMPTY:
		default:
			return nil, fmt.Errorf("rep condition needs an op")
		}
		// isEmpty asks about the list itself, so it works on any element type.
		if c.GetOp() == syncplanev1.RepOp_REP_OP_IS_EMPTY {
			return &repNode{fields: fields, op: c.GetOp(), values: values}, nil
		}
		// Membership compares elements. A repeated single-key reference has one
		// comparable value per element — the target key the row stores — so it
		// becomes an exists over that key field, the same rule that points a
		// comparison on a singular reference at its key. Anything else needs
		// comparable elements outright.
		if refKeys := lww.RefKeyFields(leaf); len(refKeys) == 1 {
			return refMembershipNode(fields, refKeys[0], c.GetOp(), values)
		}
		if !comparableKind(singularKind(leaf)) {
			return nil, fmt.Errorf("path %v: has/hasAny/hasAll need a repeated field with comparable elements, got %s; %s",
				c.GetPath(), describeLeaf(leaf), membershipHint(c.GetPath(), leaf))
		}
		return &repNode{fields: fields, op: c.GetOp(), values: values}, nil
	case *syncplanev1.Condition_Exists:
		return compileExists(desc, kind.Exists)
	case *syncplanev1.Condition_Present:
		c := kind.Present
		fields, err := resolveFields(desc, c.GetPath())
		if err != nil {
			return nil, err
		}
		if c.GetOp() == syncplanev1.PresentOp_PRESENT_OP_UNSPECIFIED {
			return nil, fmt.Errorf("present condition needs an op")
		}
		return &presentNode{fields: fields, op: c.GetOp()}, nil
	case *syncplanev1.Condition_And:
		conds, err := compileConds(desc, kind.And.GetConds())
		if err != nil {
			return nil, err
		}
		return &andNode{conds: conds}, nil
	case *syncplanev1.Condition_Or:
		conds, err := compileConds(desc, kind.Or.GetConds())
		if err != nil {
			return nil, err
		}
		return &orNode{conds: conds}, nil
	case *syncplanev1.Condition_Not:
		inner, err := compileCond(desc, kind.Not)
		if err != nil {
			return nil, err
		}
		return &notNode{cond: inner}, nil
	default:
		return nil, fmt.Errorf("condition has no kind")
	}
}

// compileExists resolves an exists condition: a path to a repeated message
// field, and a sub-condition whose own paths are relative to the ELEMENT.
//
// The element type is where the sub-condition compiles, so every rule that
// applies to a row — unknown fields, incomparable leaves, nested exists —
// applies to it unchanged, one descriptor down.
func compileExists(desc protoreflect.MessageDescriptor, c *syncplanev1.ExistsCond) (node, error) {
	fields, err := resolveFields(desc, c.GetPath())
	if err != nil {
		return nil, err
	}
	leaf := fields[len(fields)-1]
	if !leaf.IsList() || leaf.Kind() != protoreflect.MessageKind {
		return nil, fmt.Errorf("path %v: any() needs a repeated message field, got %s; %s",
			c.GetPath(), describeLeaf(leaf), existsHint(leaf))
	}
	if c.GetCond() == nil {
		return nil, fmt.Errorf("path %v: any() needs a condition", c.GetPath())
	}
	// A reference element stores only the target's key fields, so the same rule
	// that stops a path descending through a reference stops a sub-condition
	// reaching past one: those fields are simply not there to match.
	if refKeys := lww.RefKeyFields(leaf); refKeys != nil {
		if err := requireRefKeyPaths(c.GetPath(), refKeys, c.GetCond()); err != nil {
			return nil, err
		}
	}
	inner, err := compileCond(leaf.Message(), c.GetCond())
	if err != nil {
		return nil, err
	}
	return &existsNode{fields: fields, cond: inner}, nil
}

// requireRefKeyPaths rejects a sub-condition that addresses anything but the
// reference's key fields. Without it such a condition compiles and then matches
// nothing, since a stored reference element carries the key fields alone.
func requireRefKeyPaths(path []string, keyFields []protoreflect.FieldDescriptor, cond *syncplanev1.Condition) error {
	var walk func(*syncplanev1.Condition) error
	check := func(sub []string) error {
		if len(sub) == 1 && isKeyField(keyFields, sub[0]) {
			return nil
		}
		return fmt.Errorf("path %v: an element of a reference list holds only its key fields, so any() cannot ask about %q",
			path, strings.Join(sub, "."))
	}
	walk = func(c *syncplanev1.Condition) error {
		switch kind := c.GetKind().(type) {
		case *syncplanev1.Condition_Cmp:
			return check(kind.Cmp.GetPath())
		case *syncplanev1.Condition_In:
			return check(kind.In.GetPath())
		case *syncplanev1.Condition_Str:
			return check(kind.Str.GetPath())
		case *syncplanev1.Condition_Rep:
			return check(kind.Rep.GetPath())
		case *syncplanev1.Condition_Present:
			return check(kind.Present.GetPath())
		case *syncplanev1.Condition_Exists:
			return check(kind.Exists.GetPath())
		case *syncplanev1.Condition_And:
			for _, sub := range kind.And.GetConds() {
				if err := walk(sub); err != nil {
					return err
				}
			}
			return nil
		case *syncplanev1.Condition_Or:
			for _, sub := range kind.Or.GetConds() {
				if err := walk(sub); err != nil {
					return err
				}
			}
			return nil
		case *syncplanev1.Condition_Not:
			return walk(kind.Not)
		default:
			return fmt.Errorf("condition has no kind")
		}
	}
	return walk(cond)
}

// refMembershipNode rewrites membership over a repeated single-key reference
// into exists conditions on the target key. hasAll is a conjunction of them
// rather than one pass: "holds all of these" is one existential per value, and
// no single element satisfies more than one.
func refMembershipNode(
	fields []protoreflect.FieldDescriptor,
	keyField protoreflect.FieldDescriptor,
	op syncplanev1.RepOp,
	values []condValue,
) (node, error) {
	key := []protoreflect.FieldDescriptor{keyField}
	switch op {
	case syncplanev1.RepOp_REP_OP_HAS:
		return &existsNode{fields: fields, cond: &cmpNode{
			fields: key, op: syncplanev1.CmpOp_CMP_OP_EQ, value: values[0],
		}}, nil
	case syncplanev1.RepOp_REP_OP_HAS_ANY:
		return &existsNode{fields: fields, cond: &inNode{fields: key, values: values}}, nil
	case syncplanev1.RepOp_REP_OP_HAS_ALL:
		conds := make([]node, 0, len(values))
		for _, v := range values {
			conds = append(conds, &existsNode{fields: fields, cond: &cmpNode{
				fields: key, op: syncplanev1.CmpOp_CMP_OP_EQ, value: v,
			}})
		}
		return &andNode{conds: conds}, nil
	}
	return nil, fmt.Errorf("rep condition needs an op")
}

// membershipHint names the way to ask about elements the operator cannot
// compare, which differs by why it cannot.
func membershipHint(path []string, leaf protoreflect.FieldDescriptor) string {
	if leaf.Kind() == protoreflect.MessageKind {
		if len(lww.RefKeyFields(leaf)) > 1 {
			return "a composite-key reference has no single comparable value, so use any() on its key fields"
		}
		return fmt.Sprintf("use any(), e.g. any(%s, <condition on an element>)", strings.Join(path, "."))
	}
	return "use isEmpty"
}

// existsHint points a misapplied any() at the operator that does work.
func existsHint(leaf protoreflect.FieldDescriptor) string {
	if leaf.IsList() {
		return "use has/hasAny/hasAll for a repeated scalar"
	}
	return "any() asks about the elements of a list"
}

func compileConds(desc protoreflect.MessageDescriptor, conds []*syncplanev1.Condition) ([]node, error) {
	out := make([]node, 0, len(conds))
	for _, c := range conds {
		n, err := compileCond(desc, c)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, nil
}

// resolveFields turns a path of proto field names into a descriptor chain.
// Mirrors the client's translatePath validation: every segment but the last
// must be a singular message field.
func resolveFields(desc protoreflect.MessageDescriptor, path []string) ([]protoreflect.FieldDescriptor, error) {
	if len(path) == 0 {
		return nil, fmt.Errorf("condition path is empty")
	}
	cur := desc
	fields := make([]protoreflect.FieldDescriptor, 0, len(path))
	for i, seg := range path {
		if cur == nil {
			return nil, fmt.Errorf("path %v: %q is not a message field, cannot descend into %q", path, path[i-1], seg)
		}
		fd := cur.Fields().ByName(protoreflect.Name(seg))
		if fd == nil {
			return nil, fmt.Errorf("path %v: %s has no field %q", path, cur.FullName(), seg)
		}
		// A reference stores only its target's key fields, so only those are
		// addressable: a path that descends through a ref into any other field
		// can never match and is rejected. A path that ENDS at a ref is allowed
		// (presence checks; scalar compares are incomparable, like any message).
		if i < len(path)-1 {
			if refKeys := lww.RefKeyFields(fd); refKeys != nil && !isKeyField(refKeys, path[i+1]) {
				return nil, fmt.Errorf("path %v: cannot descend through reference %q into %q; only its key fields are addressable", path, seg, path[i+1])
			}
		}
		if fd.Kind() == protoreflect.MessageKind && !fd.IsList() && !fd.IsMap() {
			cur = fd.Message()
		} else {
			cur = nil
		}
		fields = append(fields, fd)
	}
	return fields, nil
}

// leafKind classifies the value a resolved path yields, from the descriptors
// alone. It must agree with what extract produces at runtime (a consistency
// test over every test-schema field pins that).
func leafKind(fields []protoreflect.FieldDescriptor) rvKind {
	fd := fields[len(fields)-1]
	if fd.IsList() {
		return rvList
	}
	if fd.IsMap() {
		return rvOther
	}
	return singularKind(fd)
}

// singularKind is the static counterpart of extractSingular: for a repeated
// field it classifies the ELEMENT, since fd.Kind() is the element kind.
func singularKind(fd protoreflect.FieldDescriptor) rvKind {
	switch fd.Kind() {
	case protoreflect.StringKind:
		return rvString
	case protoreflect.BoolKind:
		return rvBool
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
		protoreflect.FloatKind, protoreflect.DoubleKind,
		protoreflect.EnumKind:
		return rvNumber
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return rvBigint
	case protoreflect.BytesKind:
		// Collaborative text compares as its visible plain string. A corrupt
		// document degrades to rvOther at runtime, which is a data problem, not
		// a query the caller should have written differently.
		if lww.IsText(fd) && !fd.IsList() {
			return rvString
		}
		return rvOther
	default:
		return rvOther
	}
}

func comparableKind(k rvKind) bool {
	switch k {
	case rvString, rvNumber, rvBigint, rvBool:
		return true
	}
	return false
}

// requireComparable rejects a leaf that can never take part in a scalar
// comparison. Without it, eq/neq/in and orderings against bytes, messages,
// maps and repeated fields compile happily and then match nothing (or tie
// every pair, so an ordering silently degrades to row-key order), leaving the
// caller to guess why. The error names the operator that does work instead.
func requireComparable(path []string, fields []protoreflect.FieldDescriptor) error {
	if comparableKind(leafKind(fields)) {
		return nil
	}
	return fmt.Errorf("path %v: %s is not comparable; %s",
		path, describeLeaf(fields[len(fields)-1]), comparableHint(path, fields[len(fields)-1]))
}

// comparableHint names the way to ask the question that WAS meant, since the
// alternative differs by leaf kind.
func comparableHint(path []string, fd protoreflect.FieldDescriptor) string {
	switch {
	case fd.IsList():
		if fd.Kind() == protoreflect.MessageKind {
			return fmt.Sprintf("use isEmpty, or any(%s, <condition on an element>)", strings.Join(path, "."))
		}
		return "use has/hasAny/hasAll/isEmpty"
	case fd.IsMap():
		// isSet on a map is constitutionally true (implicit presence), so
		// offering it would be a false lead.
		return "map fields cannot be queried"
	case fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind:
		if refKeys := lww.RefKeyFields(fd); len(refKeys) == 1 {
			return fmt.Sprintf("use isSet/isNotSet, or compare its key field %s.%s",
				strings.Join(path, "."), refKeys[0].Name())
		}
		if len(lww.RefKeyFields(fd)) > 1 {
			return "use isSet/isNotSet; a composite-key reference has no single comparable value"
		}
		return "use isSet/isNotSet, or compare a scalar field inside it"
	default: // bytes
		return "use isSet/isNotSet"
	}
}

// describeLeaf names a field the way the schema author wrote it, so the error
// is recognizable against the .proto.
func describeLeaf(fd protoreflect.FieldDescriptor) string {
	switch {
	case fd.IsMap():
		return fmt.Sprintf("map<%s, %s> field %s",
			fd.MapKey().Kind(), fd.MapValue().Kind(), fd.Name())
	case fd.IsList():
		return fmt.Sprintf("repeated %s field %s", kindName(fd), fd.Name())
	default:
		return fmt.Sprintf("%s field %s", kindName(fd), fd.Name())
	}
}

func kindName(fd protoreflect.FieldDescriptor) string {
	switch fd.Kind() {
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return string(fd.Message().FullName())
	case protoreflect.EnumKind:
		return string(fd.Enum().FullName())
	case protoreflect.BytesKind:
		if lww.IsText(fd) {
			return "text"
		}
		return "bytes"
	default:
		return fd.Kind().String()
	}
}

func isKeyField(keyFields []protoreflect.FieldDescriptor, name string) bool {
	for _, kf := range keyFields {
		if string(kf.Name()) == name {
			return true
		}
	}
	return false
}

// resolveValue walks the descriptor chain through msg. The boolean mirrors
// the client's "found": false on an unset intermediate message or an unset
// explicit-presence leaf; implicit-presence leaves (including lists and
// maps) are always found at their zero value, like the materialized
// defaults on generated JS shapes.
func resolveValue(msg protoreflect.Message, fields []protoreflect.FieldDescriptor) (rowValue, bool) {
	cur := msg
	for i, fd := range fields {
		if i == len(fields)-1 {
			if fd.HasPresence() && !cur.Has(fd) {
				return rowValue{}, false
			}
			return extract(fd, cur.Get(fd)), true
		}
		if !cur.Has(fd) {
			return rowValue{}, false
		}
		cur = cur.Get(fd).Message()
	}
	return rowValue{}, false // unreachable: paths are never empty
}

type cmpNode struct {
	fields []protoreflect.FieldDescriptor
	op     syncplanev1.CmpOp
	value  condValue
}

func (n *cmpNode) eval(msg protoreflect.Message) bool {
	v, found := resolveValue(msg, n.fields)
	if !found {
		return false
	}
	switch n.op {
	case syncplanev1.CmpOp_CMP_OP_EQ:
		return equalsScalar(v, n.value)
	case syncplanev1.CmpOp_CMP_OP_NEQ:
		return !equalsScalar(v, n.value)
	}
	c, ok := compareScalar(v, n.value)
	if !ok {
		return false
	}
	switch n.op {
	case syncplanev1.CmpOp_CMP_OP_GT:
		return c > 0
	case syncplanev1.CmpOp_CMP_OP_GTE:
		return c >= 0
	case syncplanev1.CmpOp_CMP_OP_LT:
		return c < 0
	case syncplanev1.CmpOp_CMP_OP_LTE:
		return c <= 0
	}
	return false
}

type inNode struct {
	fields []protoreflect.FieldDescriptor
	values []condValue
}

func (n *inNode) eval(msg protoreflect.Message) bool {
	v, found := resolveValue(msg, n.fields)
	if !found {
		return false
	}
	for _, cv := range n.values {
		if equalsScalar(v, cv) {
			return true
		}
	}
	return false
}

type strNode struct {
	fields []protoreflect.FieldDescriptor
	op     syncplanev1.StrOp
	value  string
}

func (n *strNode) eval(msg protoreflect.Message) bool {
	v, found := resolveValue(msg, n.fields)
	if !found || v.kind != rvString {
		return false
	}
	// Byte-level prefix/suffix/substring matches code-unit-level for valid
	// UTF-8, which proto strings are.
	switch n.op {
	case syncplanev1.StrOp_STR_OP_STARTS_WITH:
		return strings.HasPrefix(v.str, n.value)
	case syncplanev1.StrOp_STR_OP_ENDS_WITH:
		return strings.HasSuffix(v.str, n.value)
	case syncplanev1.StrOp_STR_OP_CONTAINS:
		return strings.Contains(v.str, n.value)
	}
	return false
}

type repNode struct {
	fields []protoreflect.FieldDescriptor
	op     syncplanev1.RepOp
	values []condValue
}

func (n *repNode) eval(msg protoreflect.Message) bool {
	v, found := resolveValue(msg, n.fields)
	if !found || v.kind != rvList {
		return false
	}
	has := func(cv condValue) bool {
		for i := 0; i < v.list.Len(); i++ {
			if equalsScalar(extractSingular(v.elem, v.list.Get(i)), cv) {
				return true
			}
		}
		return false
	}
	switch n.op {
	case syncplanev1.RepOp_REP_OP_IS_EMPTY:
		return v.list.Len() == 0
	case syncplanev1.RepOp_REP_OP_HAS:
		return has(n.values[0])
	case syncplanev1.RepOp_REP_OP_HAS_ANY:
		for _, cv := range n.values {
			if has(cv) {
				return true
			}
		}
		return false
	case syncplanev1.RepOp_REP_OP_HAS_ALL:
		for _, cv := range n.values {
			if !has(cv) {
				return false
			}
		}
		return true
	}
	return false
}

// existsNode is the one node whose child evaluates against something other
// than the row: each element of the resolved list in turn. An unset or empty
// list matches nothing, like every other condition on a missing value — which
// also makes not(any(…)) read as "no element matches".
type existsNode struct {
	fields []protoreflect.FieldDescriptor
	cond   node
}

func (n *existsNode) eval(msg protoreflect.Message) bool {
	v, found := resolveValue(msg, n.fields)
	if !found || v.kind != rvList {
		return false
	}
	for i := 0; i < v.list.Len(); i++ {
		if n.cond.eval(v.list.Get(i).Message()) {
			return true
		}
	}
	return false
}

type presentNode struct {
	fields []protoreflect.FieldDescriptor
	op     syncplanev1.PresentOp
}

func (n *presentNode) eval(msg protoreflect.Message) bool {
	_, found := resolveValue(msg, n.fields)
	if n.op == syncplanev1.PresentOp_PRESENT_OP_IS_SET {
		return found
	}
	return !found
}

type andNode struct{ conds []node }

func (n *andNode) eval(msg protoreflect.Message) bool {
	for _, c := range n.conds {
		if !c.eval(msg) {
			return false
		}
	}
	return true
}

type orNode struct{ conds []node }

func (n *orNode) eval(msg protoreflect.Message) bool {
	for _, c := range n.conds {
		if c.eval(msg) {
			return true
		}
	}
	return false
}

type notNode struct{ cond node }

func (n *notNode) eval(msg protoreflect.Message) bool {
	return !n.cond.eval(msg)
}

// --- value model -----------------------------------------------------------

type rvKind int

const (
	rvOther rvKind = iota // messages, bytes, maps: present but incomparable
	rvString
	rvNumber // 32-bit ints, floats, doubles, enums — JS number
	rvBigint // 64-bit ints — JS bigint
	rvBool
	rvList
)

// rowValue is a leaf value pulled out of a row message.
type rowValue struct {
	kind rvKind
	str  string
	num  float64
	big  *big.Int
	b    bool
	list protoreflect.List
	elem protoreflect.FieldDescriptor
}

func extract(fd protoreflect.FieldDescriptor, v protoreflect.Value) rowValue {
	if fd.IsList() {
		return rowValue{kind: rvList, list: v.List(), elem: fd}
	}
	if fd.IsMap() {
		return rowValue{kind: rvOther}
	}
	return extractSingular(fd, v)
}

// extractSingular converts one scalar value; for repeated fields it converts
// an ELEMENT (fd is the repeated field descriptor, whose Kind is the element
// kind).
func extractSingular(fd protoreflect.FieldDescriptor, v protoreflect.Value) rowValue {
	switch fd.Kind() {
	case protoreflect.StringKind:
		return rowValue{kind: rvString, str: v.String()}
	case protoreflect.BoolKind:
		return rowValue{kind: rvBool, b: v.Bool()}
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return rowValue{kind: rvNumber, num: float64(v.Int())}
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return rowValue{kind: rvNumber, num: float64(v.Uint())}
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return rowValue{kind: rvNumber, num: v.Float()}
	case protoreflect.EnumKind:
		return rowValue{kind: rvNumber, num: float64(v.Enum())}
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return rowValue{kind: rvBigint, big: big.NewInt(v.Int())}
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return rowValue{kind: rvBigint, big: new(big.Int).SetUint64(v.Uint())}
	case protoreflect.BytesKind:
		// A collaborative-text field compares as its visible plain string, so
		// contains/eq/ordering work on what the user sees. Undecodable state
		// (never produced by the engine) stays incomparable rather than
		// erroring — mirror of the client evaluator.
		if lww.IsText(fd) && !fd.IsList() {
			if s, err := text.PlainText(v.Bytes()); err == nil {
				return rowValue{kind: rvString, str: s}
			}
		}
		return rowValue{kind: rvOther}
	default:
		return rowValue{kind: rvOther}
	}
}

// condValue is a scalar literal from the wire condition; it reuses the
// rowValue representation (string/number/bigint/bool kinds only).
type condValue = rowValue

func condValueOf(qv *syncplanev1.QueryValue) (condValue, error) {
	switch kind := qv.GetKind().(type) {
	case *syncplanev1.QueryValue_StringValue:
		return condValue{kind: rvString, str: kind.StringValue}, nil
	case *syncplanev1.QueryValue_DoubleValue:
		return condValue{kind: rvNumber, num: kind.DoubleValue}, nil
	case *syncplanev1.QueryValue_IntValue:
		return condValue{kind: rvBigint, big: big.NewInt(kind.IntValue)}, nil
	case *syncplanev1.QueryValue_UintValue:
		return condValue{kind: rvBigint, big: new(big.Int).SetUint64(kind.UintValue)}, nil
	case *syncplanev1.QueryValue_BoolValue:
		return condValue{kind: rvBool, b: kind.BoolValue}, nil
	default:
		return condValue{}, fmt.Errorf("query value has no kind")
	}
}

func condValuesOf(qvs []*syncplanev1.QueryValue) ([]condValue, error) {
	out := make([]condValue, 0, len(qvs))
	for _, qv := range qvs {
		cv, err := condValueOf(qv)
		if err != nil {
			return nil, err
		}
		out = append(out, cv)
	}
	return out, nil
}

// --- coercion --------------------------------------------------------------

// isIntegralFloat mirrors JS Number.isInteger (false for NaN and ±Inf).
func isIntegralFloat(f float64) bool {
	return f == math.Trunc(f) && !math.IsInf(f, 0)
}

func bigFromFloat(f float64) *big.Int {
	i, _ := big.NewFloat(f).Int(nil)
	return i
}

// floatFromBig rounds to nearest-even, matching JS Number(bigint).
func floatFromBig(b *big.Int) float64 {
	f, _ := new(big.Float).SetInt(b).Float64()
	return f
}

// equalsScalar mirrors the client: `===` after number/bigint normalization.
func equalsScalar(row rowValue, cond condValue) bool {
	switch {
	case row.kind == rvBigint && cond.kind == rvNumber:
		if isIntegralFloat(cond.num) {
			return row.big.Cmp(bigFromFloat(cond.num)) == 0
		}
		return floatFromBig(row.big) == cond.num
	case row.kind == rvNumber && cond.kind == rvBigint:
		if isIntegralFloat(row.num) {
			return bigFromFloat(row.num).Cmp(cond.big) == 0
		}
		return row.num == floatFromBig(cond.big)
	case row.kind != cond.kind:
		return false
	case row.kind == rvString:
		return row.str == cond.str
	case row.kind == rvNumber:
		return row.num == cond.num
	case row.kind == rvBigint:
		return row.big.Cmp(cond.big) == 0
	case row.kind == rvBool:
		return row.b == cond.b
	default:
		return false
	}
}

// compareScalar mirrors the client's compareValues. The boolean is false
// when the values are incomparable: conditions treat that as false,
// orderings as a rank (see orderRank) rather than a tie.
func compareScalar(a, b rowValue) (int, bool) {
	// NaN participates in no comparison at all, which is what gives the row
	// ordering a total order and makes it encodable as index keys. Checked
	// here rather than in cmpFloat so the mixed number/bigint paths — where a
	// NaN never reaches cmpFloat, because isIntegralFloat(NaN) is false — get
	// the same answer.
	if isNaN(a) || isNaN(b) {
		return 0, false
	}
	switch {
	case a.kind == rvNumber && b.kind == rvBigint:
		if isIntegralFloat(a.num) {
			return bigFromFloat(a.num).Cmp(b.big), true
		}
		return cmpFloat(a.num, floatFromBig(b.big)), true
	case a.kind == rvBigint && b.kind == rvNumber:
		if isIntegralFloat(b.num) {
			return a.big.Cmp(bigFromFloat(b.num)), true
		}
		return cmpFloat(floatFromBig(a.big), b.num), true
	case a.kind != b.kind:
		return 0, false
	case a.kind == rvString:
		return CompareStringsUTF16(a.str, b.str), true
	case a.kind == rvNumber:
		return cmpFloat(a.num, b.num), true
	case a.kind == rvBigint:
		return a.big.Cmp(b.big), true
	case a.kind == rvBool:
		return cmpFloat(boolToFloat(a.b), boolToFloat(b.b)), true
	default:
		return 0, false
	}
}

// isNaN reports whether the value is the float NaN. Only rvNumber can hold
// it: bigints have no NaN, and a float64 field is the only source of one.
func isNaN(v rowValue) bool {
	return v.kind == rvNumber && math.IsNaN(v.num)
}

// cmpFloat mirrors JS `a < b ? -1 : a > b ? 1 : 0`. compareScalar filters NaN
// out before this is reached, so the "neither less nor greater" fallthrough
// only ever means equal.
func cmpFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
