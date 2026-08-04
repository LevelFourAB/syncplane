// Runtime side of the field-proxy DSL. The proxy is schema-agnostic: it does
// no descriptor introspection because the type layer (types.ts) already
// validates field paths and operator availability statically. Property access
// extends the recorded path; access to a known operator name returns an
// AST-builder function instead.
//
// Caveat (mirrored at the type level): a proto field literally named after an
// operator (`eq`, `has`, `isSet`, ...) is shadowed and unreachable. The names
// are exported as RESERVED_QUERY_NAMES so entity registration can warn about a
// schema that hits one, instead of the field just disappearing from the DSL.

import type { CmpOp, CompiledQuery, Cond, Path, Scalar, StrOp } from './ast.js';
import type { Query, QueryRef } from './types.js';

type OpBuilder = (path: Path) => unknown;

const OPERATORS: Record<string, OpBuilder> = {};

for (const op of [ 'eq', 'neq', 'gt', 'gte', 'lt', 'lte' ] as const) {
	OPERATORS[op] = (path) => (value: Scalar): Cond => ({ kind: 'cmp', path, op: op as CmpOp, value });
}
for (const op of [ 'startsWith', 'endsWith', 'contains' ] as const) {
	OPERATORS[op] = (path) => (value: string): Cond => ({ kind: 'str', path, op: op as StrOp, value });
}
OPERATORS.in = (path) => (values: Scalar[]): Cond => ({ kind: 'in', path, values });
OPERATORS.has = (path) => (value: Scalar): Cond => ({ kind: 'rep', path, op: 'has', value });
for (const op of [ 'hasAny', 'hasAll' ] as const) {
	OPERATORS[op] = (path) => (values: Scalar[]): Cond => ({ kind: 'rep', path, op, values });
}
OPERATORS.isEmpty = (path) => (): Cond => ({ kind: 'rep', path, op: 'isEmpty' });
// The element gets a reference rooted at the EMPTY path, so the sub-condition
// records paths relative to it — which is the form the AST, the wire and both
// evaluators expect. Nesting works for free: an any() inside an any() starts a
// fresh empty root against its own element.
//
// A map needs nothing extra here: its element is the ENTRY, whose two sides are
// ordinary first segments, so `e.key` and `e.value.number` record ['key'] and
// ['value', 'number'] through the same empty root a list element uses.
OPERATORS.any = (path) => (build: (element: unknown) => Cond): Cond => ({
	kind: 'exists',
	path,
	cond: build(fieldProxy([])),
});
// One entry of a map. It appends the key as an ordinary path segment, which is
// exactly what the segment after a map field means to both evaluators — an
// explicit accessor rather than property access so that a key colliding with an
// operator name cannot be silently swallowed by the `get` trap below.
OPERATORS.at = (path) => (key: string) => fieldProxy([ ...path, key ]);
OPERATORS.isSet = (path) => (): Cond => ({ kind: 'present', path, op: 'isSet' });
OPERATORS.isNotSet = (path) => (): Cond => ({ kind: 'present', path, op: 'isNotSet' });
OPERATORS.asc = (path) => () => ({ path, dir: 'asc' as const });
OPERATORS.desc = (path) => () => ({ path, dir: 'desc' as const });

/**
 * Names the query DSL claims. A proto field with one of these localNames is
 * unreachable through the DSL — the proxy returns the operator and the type
 * layer offers the operator surface — so registration warns when a schema has
 * one rather than letting the field silently vanish from queries.
 */
export const RESERVED_QUERY_NAMES: ReadonlySet<string> = new Set(Object.keys(OPERATORS));

function fieldProxy(path: Path): unknown {
	return new Proxy(Object.create(null) as object, {
		get(_target, prop) {
			if (typeof prop !== 'string') return undefined;
			const op = OPERATORS[prop];
			if (op !== undefined) return op(path);
			return fieldProxy([ ...path, prop ]);
		},
	});
}

/** Root field reference for a message shape. */
export function rootRef<M>(): QueryRef<M> {
	return fieldProxy([]) as QueryRef<M>;
}

function flatten(kind: 'and' | 'or', conds: Cond[]): Cond[] {
	return conds.flatMap((c) => (c.kind === kind ? c.conds : [ c ]));
}

/** All conditions must hold. Nested `and`s are flattened. */
export function and(...conds: Cond[]): Cond {
	return { kind: 'and', conds: flatten('and', conds) };
}

/** At least one condition must hold. Nested `or`s are flattened. */
export function or(...conds: Cond[]): Cond {
	return { kind: 'or', conds: flatten('or', conds) };
}

/** Negates a condition. */
export function not(cond: Cond): Cond {
	return { kind: 'not', cond };
}

/** Invokes the where/orderBy lambdas once and returns the plain-data query. */
export function compileQuery<M>(q: Query<M>): CompiledQuery {
	const ref = rootRef<M>();
	const out: CompiledQuery = {};
	if (q.where) out.where = q.where(ref);
	if (q.orderBy) out.orderBy = q.orderBy(ref);
	if (q.limit !== undefined) out.limit = q.limit;
	if (q.offset !== undefined) out.offset = q.offset;
	return out;
}
