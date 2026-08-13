package query

import (
	"fmt"

	"google.golang.org/protobuf/reflect/protoreflect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/lww"
)

// Rel matches rows where at least one row referenced through the ref field at
// path exists and satisfies cond — a one-hop semi-join. path names a forward
// reference field (singular or repeated); cond's paths are relative to the
// TARGET entity's row, the same scoping Any gives its element:
//
//	// issues assigned to anyone on this team
//	query.Rel("assignee", query.Eq("team_id", "t1"))
//	// tasks blocked by an unresolved task (repeated ref: SOME target matches)
//	query.Rel("blocked_by", query.Eq("done", false))
//
// An unset to-one ref, an orphaned stub and a deleted target all make the
// condition false. One hop only: a Rel inside a Rel, or inside an Any, is
// rejected at compile time.
//
// Unlike every other condition it needs the target entity's rows to answer, so
// it is accepted only where those are in reach — the Query RPC and the
// ViewTx/EntityTx/ApplyTx Query methods, which resolve it against current
// server state before evaluating. Compile rejects it anywhere else (orderings,
// subscription predicates, index specs).
func Rel(path string, cond *syncplanev1.Condition) *syncplanev1.Condition {
	return &syncplanev1.Condition{Kind: &syncplanev1.Condition_Rel{Rel: &syncplanev1.RelCond{
		Path: Path(path),
		Cond: cond,
	}}}
}

// RelNode is one rel condition found in a predicate: the ref field it crosses
// and the inner condition selecting target rows. See ExtractRels.
type RelNode struct {
	// Cond is the rel condition's own message inside the predicate tree —
	// the identity a rewrite replaces (see RewriteRels).
	Cond *syncplanev1.RelCond
	// Field is the resolved forward reference field the path ends at.
	Field protoreflect.FieldDescriptor
	// Target is the referenced entity's message descriptor.
	Target protoreflect.MessageDescriptor
	// KeyFields are the target's key fields, the values a stored stub carries.
	KeyFields []protoreflect.FieldDescriptor
}

// Repeated reports whether the ref is to-many.
func (n *RelNode) Repeated() bool { return n.Field.IsList() }

// ExtractRels finds and validates every rel condition in cond, in evaluation
// order. Each node's path must resolve to a forward reference field of desc,
// its inner condition must compile against the target descriptor, and a rel
// nested inside another rel or inside an exists is rejected — the semi-join is
// one hop, scoped to the row.
//
// An empty result means the predicate is row-local and needs no pre-pass.
func ExtractRels(desc protoreflect.MessageDescriptor, cond *syncplanev1.Condition) ([]*RelNode, error) {
	if cond == nil {
		return nil, nil
	}
	var out []*RelNode
	if err := extractRels(desc, cond, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func extractRels(desc protoreflect.MessageDescriptor, cond *syncplanev1.Condition, out *[]*RelNode) error {
	switch kind := cond.GetKind().(type) {
	case *syncplanev1.Condition_And:
		for _, sub := range kind.And.GetConds() {
			if err := extractRels(desc, sub, out); err != nil {
				return err
			}
		}
	case *syncplanev1.Condition_Or:
		for _, sub := range kind.Or.GetConds() {
			if err := extractRels(desc, sub, out); err != nil {
				return err
			}
		}
	case *syncplanev1.Condition_Not:
		return extractRels(desc, kind.Not, out)
	case *syncplanev1.Condition_Rel:
		node, err := resolveRel(desc, kind.Rel)
		if err != nil {
			return err
		}
		*out = append(*out, node)
	}
	// Every other kind is row-local, and a rel below an exists is rejected when
	// the exists compiles (compileCond has no rel case inside an element scope).
	return nil
}

// resolveRel validates one rel condition: the path ends at a forward reference
// field, the inner condition compiles against the target row, and no rel nests
// inside it.
func resolveRel(desc protoreflect.MessageDescriptor, c *syncplanev1.RelCond) (*RelNode, error) {
	fields, err := resolveFields(desc, c.GetPath())
	if err != nil {
		return nil, err
	}
	leaf := pathLeaf(fields)
	refKeys := lww.RefKeyFields(leaf)
	if len(refKeys) == 0 {
		return nil, fmt.Errorf("path %v: matches needs a reference field, got %s",
			c.GetPath(), describeLeaf(fields[len(fields)-1]))
	}
	if c.GetCond() == nil {
		return nil, fmt.Errorf("path %v: matches needs a condition over the target entity", c.GetPath())
	}
	if hasRel(c.GetCond()) {
		return nil, fmt.Errorf("path %v: matches cannot nest inside matches; the semi-join is one hop", c.GetPath())
	}
	if _, err := Compile(leaf.Message(), c.GetCond()); err != nil {
		return nil, fmt.Errorf("path %v: %w", c.GetPath(), err)
	}
	return &RelNode{Cond: c, Field: leaf, Target: leaf.Message(), KeyFields: refKeys}, nil
}

// hasRel reports whether any rel condition appears anywhere in the tree,
// including below an exists — where extraction does not descend because
// compilation rejects it there anyway.
func hasRel(cond *syncplanev1.Condition) bool {
	switch kind := cond.GetKind().(type) {
	case *syncplanev1.Condition_Rel:
		return true
	case *syncplanev1.Condition_And:
		for _, sub := range kind.And.GetConds() {
			if hasRel(sub) {
				return true
			}
		}
	case *syncplanev1.Condition_Or:
		for _, sub := range kind.Or.GetConds() {
			if hasRel(sub) {
				return true
			}
		}
	case *syncplanev1.Condition_Not:
		return hasRel(kind.Not)
	case *syncplanev1.Condition_Exists:
		if c := kind.Exists.GetCond(); c != nil {
			return hasRel(c)
		}
	}
	return false
}

// RelReplacement builds the row-local condition a rel node collapses to once
// the pre-pass has selected its matching targets: a membership test on the
// stored ref stubs. targets holds one value tuple per matching target row, in
// the node's KeyFields order — read the values off the target rows with
// TargetKeyValues. No matching targets means the node matches no row, which
// the empty membership test already says.
//
// A single-key ref becomes in(path.key, K) — for a repeated ref, an exists
// over the element's key field — the exact shape the reverse-relation
// push-down produces, so the per-ref-field index serves it. A composite-key
// ref becomes a disjunction of key-tuple equalities: comparing whole tuples is
// what keeps a matching id under the wrong ns from matching.
func RelReplacement(node *RelNode, targets [][]*syncplanev1.QueryValue) *syncplanev1.Condition {
	path := node.Cond.GetPath()
	if len(node.KeyFields) == 1 {
		values := make([]*syncplanev1.QueryValue, len(targets))
		for i, t := range targets {
			values[i] = t[0]
		}
		keyName := string(node.KeyFields[0].Name())
		if node.Repeated() {
			return existsAt(path, &syncplanev1.Condition{
				Kind: &syncplanev1.Condition_In{In: &syncplanev1.InCond{
					Path:   []string{keyName},
					Values: values,
				}},
			})
		}
		return &syncplanev1.Condition{Kind: &syncplanev1.Condition_In{In: &syncplanev1.InCond{
			Path:   append(append([]string{}, path...), keyName),
			Values: values,
		}}}
	}
	conds := make([]*syncplanev1.Condition, len(targets))
	for i, t := range targets {
		eqs := make([]*syncplanev1.Condition, len(node.KeyFields))
		for j, kf := range node.KeyFields {
			keyPath := []string{string(kf.Name())}
			if !node.Repeated() {
				keyPath = append(append([]string{}, path...), string(kf.Name()))
			}
			eqs[j] = &syncplanev1.Condition{Kind: &syncplanev1.Condition_Cmp{Cmp: &syncplanev1.CmpCond{
				Path:  keyPath,
				Op:    syncplanev1.CmpOp_CMP_OP_EQ,
				Value: t[j],
			}}}
		}
		conds[i] = And(eqs...)
	}
	if node.Repeated() {
		return existsAt(path, Or(conds...))
	}
	return Or(conds...)
}

// existsAt is Any over an already-split path — the rel path arrives as wire
// segments, so joining and re-splitting it would mangle a segment holding a
// dot.
func existsAt(path []string, cond *syncplanev1.Condition) *syncplanev1.Condition {
	return &syncplanev1.Condition{Kind: &syncplanev1.Condition_Exists{Exists: &syncplanev1.ExistsCond{
		Path: path,
		Cond: cond,
	}}}
}

// TargetKeyValues reads one matching target row's key values in the node's
// KeyFields order, as the wire literals the replacement condition compares
// with. The kinds mirror what a client would send for the same values, so the
// coercion rules the evaluators share apply unchanged.
func TargetKeyValues(node *RelNode, msg protoreflect.Message) []*syncplanev1.QueryValue {
	out := make([]*syncplanev1.QueryValue, len(node.KeyFields))
	for i, kf := range node.KeyFields {
		out[i] = queryValueOf(kf, msg.Get(kf))
	}
	return out
}

// queryValueOf converts one scalar field value to its wire literal form,
// preserving the kind the evaluators coerce on (see valueOf).
func queryValueOf(fd protoreflect.FieldDescriptor, v protoreflect.Value) *syncplanev1.QueryValue {
	switch fd.Kind() {
	case protoreflect.StringKind:
		return &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_StringValue{StringValue: v.String()}}
	case protoreflect.BoolKind:
		return &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_BoolValue{BoolValue: v.Bool()}}
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.EnumKind:
		return &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_DoubleValue{DoubleValue: float64(v.Int())}}
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_DoubleValue{DoubleValue: float64(v.Uint())}}
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_DoubleValue{DoubleValue: v.Float()}}
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_IntValue{IntValue: v.Int()}}
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_UintValue{UintValue: v.Uint()}}
	default:
		// Key fields are scalar by registration; nothing else reaches here.
		return &syncplanev1.QueryValue{}
	}
}

// RewriteRels returns cond with every rel node replaced by the row-local
// condition replacements maps it to (keyed by the RelCond message identity, as
// ExtractRels reported it). Subtrees without a rel are shared, not copied; the
// input is never mutated. A rel absent from the map is left in place, so the
// caller's Compile rejects it rather than silently matching nothing.
func RewriteRels(cond *syncplanev1.Condition, replacements map[*syncplanev1.RelCond]*syncplanev1.Condition) *syncplanev1.Condition {
	if cond == nil {
		return nil
	}
	switch kind := cond.GetKind().(type) {
	case *syncplanev1.Condition_Rel:
		if repl, ok := replacements[kind.Rel]; ok {
			return repl
		}
		return cond
	case *syncplanev1.Condition_And:
		return &syncplanev1.Condition{Kind: &syncplanev1.Condition_And{And: &syncplanev1.ConditionList{
			Conds: rewriteRelList(kind.And.GetConds(), replacements),
		}}}
	case *syncplanev1.Condition_Or:
		return &syncplanev1.Condition{Kind: &syncplanev1.Condition_Or{Or: &syncplanev1.ConditionList{
			Conds: rewriteRelList(kind.Or.GetConds(), replacements),
		}}}
	case *syncplanev1.Condition_Not:
		return &syncplanev1.Condition{Kind: &syncplanev1.Condition_Not{
			Not: RewriteRels(kind.Not, replacements),
		}}
	default:
		return cond
	}
}

func rewriteRelList(conds []*syncplanev1.Condition, replacements map[*syncplanev1.RelCond]*syncplanev1.Condition) []*syncplanev1.Condition {
	out := make([]*syncplanev1.Condition, len(conds))
	for i, c := range conds {
		out[i] = RewriteRels(c, replacements)
	}
	return out
}
