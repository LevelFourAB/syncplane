// Plain-data condition AST produced by the field-proxy DSL.
//
// Paths are JS localNames (the property names on generated message shapes,
// e.g. ["settings", "color"]) — if server push-down ever lands these need to
// be translated to proto field names via the descriptors. The AST is meant to
// be JSON-serializable, but `bigint` values need a JSON replacer (not
// implemented yet).

/** A nested field path as JS localName segments. */
export type Path = string[];

/** Scalar values that can appear in conditions. */
export type Scalar = string | number | bigint | boolean;

export type CmpOp = 'eq' | 'neq' | 'gt' | 'gte' | 'lt' | 'lte';
export type StrOp = 'startsWith' | 'endsWith' | 'contains';

export type Cond =
	| { kind: 'cmp'; path: Path; op: CmpOp; value: Scalar }
	| { kind: 'in'; path: Path; values: Scalar[] }
	| { kind: 'str'; path: Path; op: StrOp; value: string }
	| { kind: 'rep'; path: Path; op: 'has'; value: Scalar }
	| { kind: 'rep'; path: Path; op: 'hasAny' | 'hasAll'; values: Scalar[] }
	| { kind: 'rep'; path: Path; op: 'isEmpty' }
	// `cond`'s paths are relative to the ELEMENT, not the row — the one place
	// in the AST where a path is scoped to something other than the row. That
	// is what lets two conditions describe the same element; see ExistsCond in
	// the proto for why an implicit quantifier cannot.
	| { kind: 'exists'; path: Path; cond: Cond }
	| { kind: 'present'; path: Path; op: 'isSet' | 'isNotSet' }
	| { kind: 'and' | 'or'; conds: Cond[] }
	| { kind: 'not'; cond: Cond };

export interface OrderKey {
	path: Path;
	dir: 'asc' | 'desc';
}

/** A fully-materialized query, independent of the typed DSL. */
export interface CompiledQuery {
	where?: Cond;
	orderBy?: OrderKey[];
	limit?: number;
	offset?: number;
}
