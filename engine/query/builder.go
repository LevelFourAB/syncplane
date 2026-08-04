package query

import (
	"reflect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/lww"
)

// Spec is a whole query: an optional predicate, an optional ordering, and a
// page. It is what ViewTx.Query takes, and it mirrors the QueryRequest fields
// a client sends, so server-side code and client code express the same query
// the same way.
type Spec struct {
	// Where selects rows. Nil matches every live row of the entity.
	Where *syncplanev1.Condition
	// OrderBy sorts the result. Missing values sort last regardless of
	// direction, ties fall through to the next key and finally to the row key
	// ascending. Empty means row-key order.
	OrderBy []*syncplanev1.OrderKey
	// Limit caps the returned rows. Zero or negative means no cap — which,
	// combined with an Offset, is the only way to ask for an unbounded result.
	Limit int
	// Offset drops the first Offset rows of the ordering. It costs what it
	// says: the rows are still scanned, decoded and compared, so deep paging
	// re-does the work of every page before it. Prefer a more selective
	// predicate, or a range condition on the order key ("keyset pagination").
	Offset int
}

// Value is the set of Go types that can appear as a query literal. Constraining
// the constructors to it makes an unsupported literal a compile error instead
// of a runtime one, which is why none of them return an error.
type Value interface {
	~string | ~bool |
		~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}

// valueOf converts a Go literal to its wire form, preserving the kind the
// evaluator coerces on: signed integers travel as int_value, unsigned as
// uint_value, floats as double_value.
//
// It switches on the reflected KIND rather than the concrete type so a named
// type (type Priority int32, and therefore every generated enum) converts like
// its underlying type instead of falling off the end.
func valueOf[T Value](v T) *syncplanev1.QueryValue {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.String:
		return &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_StringValue{StringValue: rv.String()}}
	case reflect.Bool:
		return &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_BoolValue{BoolValue: rv.Bool()}}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_IntValue{IntValue: rv.Int()}}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_UintValue{UintValue: rv.Uint()}}
	case reflect.Float32, reflect.Float64:
		return &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_DoubleValue{DoubleValue: rv.Float()}}
	default:
		// Unreachable: the Value constraint admits no other kinds, so this
		// fires only if someone widens it without widening the switch.
		panic("query: unsupported literal kind " + rv.Kind().String())
	}
}

// Path splits a dotted field path into the segments the wire form carries.
// Segments are PROTO field names ("created_at_ms"), not Go struct field names
// — the same names a client sends. Compile rejects a path that names an
// unknown field, so a typo fails at compile time, not silently at match time.
//
// A segment following a MAP field is the entry key rather than a field name
// ("custom_fields.priority.number"), and a key is arbitrary data, so a dot
// inside one is escaped with a backslash: `custom_fields.due\.date.text`
// addresses the key "due.date". The escaping is only needed here — the wire
// form is already a list of segments, so a key travels verbatim in it — and it
// is the same rule an lww field-mask path uses for the same reason.
func Path(path string) []string {
	segs := lww.SplitPath(path)
	for i, s := range segs {
		segs[i] = lww.UnescapeSegment(s)
	}
	return segs
}

func cmp[T Value](path string, op syncplanev1.CmpOp, v T) *syncplanev1.Condition {
	return &syncplanev1.Condition{Kind: &syncplanev1.Condition_Cmp{Cmp: &syncplanev1.CmpCond{
		Path:  Path(path),
		Op:    op,
		Value: valueOf(v),
	}}}
}

// Eq matches rows whose field equals v.
func Eq[T Value](path string, v T) *syncplanev1.Condition {
	return cmp(path, syncplanev1.CmpOp_CMP_OP_EQ, v)
}

// Neq matches rows whose field differs from v. An unset field does NOT match:
// every condition on a missing value is false, including this one.
func Neq[T Value](path string, v T) *syncplanev1.Condition {
	return cmp(path, syncplanev1.CmpOp_CMP_OP_NEQ, v)
}

// Gt matches rows whose field is greater than v.
func Gt[T Value](path string, v T) *syncplanev1.Condition {
	return cmp(path, syncplanev1.CmpOp_CMP_OP_GT, v)
}

// Gte matches rows whose field is greater than or equal to v.
func Gte[T Value](path string, v T) *syncplanev1.Condition {
	return cmp(path, syncplanev1.CmpOp_CMP_OP_GTE, v)
}

// Lt matches rows whose field is less than v.
func Lt[T Value](path string, v T) *syncplanev1.Condition {
	return cmp(path, syncplanev1.CmpOp_CMP_OP_LT, v)
}

// Lte matches rows whose field is less than or equal to v.
func Lte[T Value](path string, v T) *syncplanev1.Condition {
	return cmp(path, syncplanev1.CmpOp_CMP_OP_LTE, v)
}

// In matches rows whose field equals any of vs. An empty vs matches nothing.
func In[T Value](path string, vs ...T) *syncplanev1.Condition {
	values := make([]*syncplanev1.QueryValue, len(vs))
	for i, v := range vs {
		values[i] = valueOf(v)
	}
	return &syncplanev1.Condition{Kind: &syncplanev1.Condition_In{In: &syncplanev1.InCond{
		Path:   Path(path),
		Values: values,
	}}}
}

func str(path string, op syncplanev1.StrOp, v string) *syncplanev1.Condition {
	return &syncplanev1.Condition{Kind: &syncplanev1.Condition_Str{Str: &syncplanev1.StrCond{
		Path:  Path(path),
		Op:    op,
		Value: v,
	}}}
}

// StartsWith matches a string (or collaborative-text) field by prefix,
// case-sensitively.
func StartsWith(path, prefix string) *syncplanev1.Condition {
	return str(path, syncplanev1.StrOp_STR_OP_STARTS_WITH, prefix)
}

// EndsWith matches a string (or collaborative-text) field by suffix,
// case-sensitively.
func EndsWith(path, suffix string) *syncplanev1.Condition {
	return str(path, syncplanev1.StrOp_STR_OP_ENDS_WITH, suffix)
}

// Contains matches a string (or collaborative-text) field by substring,
// case-sensitively. It is a substring test per row, not search: no
// tokenization, no stemming, no ranking, and no index behind it.
func Contains(path, substr string) *syncplanev1.Condition {
	return str(path, syncplanev1.StrOp_STR_OP_CONTAINS, substr)
}

func rep[T Value](path string, op syncplanev1.RepOp, vs []T) *syncplanev1.Condition {
	values := make([]*syncplanev1.QueryValue, len(vs))
	for i, v := range vs {
		values[i] = valueOf(v)
	}
	return &syncplanev1.Condition{Kind: &syncplanev1.Condition_Rep{Rep: &syncplanev1.RepCond{
		Path:   Path(path),
		Op:     op,
		Values: values,
	}}}
}

// Has matches rows whose repeated field contains v.
func Has[T Value](path string, v T) *syncplanev1.Condition {
	return rep(path, syncplanev1.RepOp_REP_OP_HAS, []T{v})
}

// HasAny matches rows whose repeated field contains at least one of vs.
func HasAny[T Value](path string, vs ...T) *syncplanev1.Condition {
	return rep(path, syncplanev1.RepOp_REP_OP_HAS_ANY, vs)
}

// HasAll matches rows whose repeated field contains every one of vs.
func HasAll[T Value](path string, vs ...T) *syncplanev1.Condition {
	return rep(path, syncplanev1.RepOp_REP_OP_HAS_ALL, vs)
}

// Any matches rows where at least one element of the repeated message field at
// path satisfies cond. The paths inside cond are relative to the ELEMENT:
//
//	query.Any("members", query.And(query.Eq("uid", "u1"), query.Gt("role", 2)))
//
// It is the only way to ask about the contents of message elements, and the
// element scope is what lets two conditions describe the SAME element —
// And(Any(…eq…), Any(…gt…)) is a different, weaker question. Over a repeated
// single-key reference, Has is the shorter spelling of the same thing.
//
// Over a MAP the element is one ENTRY, whose sides the sub-condition addresses
// as `key` and `value` — the same two words an index component's `*` selector
// uses:
//
//	query.Any("custom", query.Eq("value.number", 5))              // which field holds 5
//	query.Any("custom", query.And(
//		query.StartsWith("key", "cf_"), query.Eq("value.checked", true)))
//
// That is the mirror of addressing one entry by key: Eq("custom.priority.number",
// 5) asks whether THIS field holds 5, and this asks whether ANY does. It is a
// predicate, not a projection — the answer never says which entry matched.
func Any(path string, cond *syncplanev1.Condition) *syncplanev1.Condition {
	return &syncplanev1.Condition{Kind: &syncplanev1.Condition_Exists{Exists: &syncplanev1.ExistsCond{
		Path: Path(path),
		Cond: cond,
	}}}
}

// IsEmpty matches rows whose repeated field has no elements, or whose MAP field
// has no entries — it asks about the collection itself, so both answer it, and
// on a map it is the one thing isSet cannot say (a map is always set).
func IsEmpty(path string) *syncplanev1.Condition {
	return rep[string](path, syncplanev1.RepOp_REP_OP_IS_EMPTY, nil)
}

// IsSet matches rows where the field is present. Presence means what proto
// means by it: an explicit-presence field (optional, message, reference) is
// set only when written, while an implicit-presence scalar, list or map is
// ALWAYS set — IsSet on one is constitutionally true, not "has a non-zero
// value". Compare against the zero value for that.
func IsSet(path string) *syncplanev1.Condition {
	return present(path, syncplanev1.PresentOp_PRESENT_OP_IS_SET)
}

// IsNotSet matches rows where the field is absent. See IsSet for what
// presence means.
func IsNotSet(path string) *syncplanev1.Condition {
	return present(path, syncplanev1.PresentOp_PRESENT_OP_IS_NOT_SET)
}

func present(path string, op syncplanev1.PresentOp) *syncplanev1.Condition {
	return &syncplanev1.Condition{Kind: &syncplanev1.Condition_Present{Present: &syncplanev1.PresentCond{
		Path: Path(path),
		Op:   op,
	}}}
}

// And matches rows satisfying every condition. No arguments matches everything.
func And(conds ...*syncplanev1.Condition) *syncplanev1.Condition {
	return &syncplanev1.Condition{Kind: &syncplanev1.Condition_And{
		And: &syncplanev1.ConditionList{Conds: conds},
	}}
}

// Or matches rows satisfying at least one condition. No arguments matches
// nothing.
func Or(conds ...*syncplanev1.Condition) *syncplanev1.Condition {
	return &syncplanev1.Condition{Kind: &syncplanev1.Condition_Or{
		Or: &syncplanev1.ConditionList{Conds: conds},
	}}
}

// Not inverts a condition. Note that it inverts the MATCH, not the field's
// meaning: because every condition on a missing value is false, Not(Eq(...))
// matches rows where the field is unset, which Neq does not.
func Not(cond *syncplanev1.Condition) *syncplanev1.Condition {
	return &syncplanev1.Condition{Kind: &syncplanev1.Condition_Not{Not: cond}}
}

// Asc orders by a field, ascending.
func Asc(path string) *syncplanev1.OrderKey {
	return &syncplanev1.OrderKey{Path: Path(path), Dir: syncplanev1.Direction_DIRECTION_ASC}
}

// Desc orders by a field, descending.
func Desc(path string) *syncplanev1.OrderKey {
	return &syncplanev1.OrderKey{Path: Path(path), Dir: syncplanev1.Direction_DIRECTION_DESC}
}
