// Local evaluator for the condition AST: filter → sort → slice over decoded
// rows. All semantics are deliberately simple and documented here:
//
// - An unset intermediate message makes every condition on the path false,
//   except `isNotSet`, which is true.
// - Numeric comparisons are native within a kind (number vs number, bigint vs
//   bigint). Mixed number/bigint compares as BigInt when the number is
//   integral, otherwise as Number (precision caveat: Number(bigint) may
//   round for values beyond 2^53).
// - String ops are case-sensitive; ordering is by code unit (`<`/`>`), which
//   matches the key sort in read.ts. A string holding a LONE surrogate is not
//   valid UTF-8, so it reaches the server as U+FFFD: this evaluator compares
//   the original code unit and the server compares the replacement, which is
//   the one case where a local verdict and a queryServer verdict can
//   legitimately differ.
// - `has`/`in` use `===` after numeric normalization — no deep equality. A
//   question about the CONTENTS of message elements is spelled `exists`, which
//   scopes one element and evaluates a sub-condition against it; membership in
//   a repeated single-key reference desugars to one in prepareQuery.
// - NaN is NOT COMPARABLE. A NaN on either side makes `gt`/`gte`/`lt`/`lte`
//   false, matching JS's native semantics and the way `eq` already fails, so
//   no comparison operator ever matches it. For ordering it is not a tie
//   either: values rank real < NaN < missing, and the rank decides before the
//   direction does, so NaN sorts after every real value and before missing
//   under both `asc` and `desc`. Two NaNs (like two missing values) tie and
//   fall through to the next order key. That total order is what lets a row
//   ordering be encoded into sortable index keys, and the Go evaluator
//   matches. ±Infinity is ordinary and compares as expected.
//
// A mismatch between a condition's LITERAL and the field's value is a
// legitimate no-match and evaluates false here. A field that could never take
// part in the comparison at all (bytes, message, map, repeated) never reaches
// this file: prepareQuery rejects it against the schema first, so nothing
// below has to decide what "eq against a map" means.

import type { DescField, DescMessage } from '@bufbuild/protobuf';

import { isTextField } from '../mutations.js';
import type { Row } from '../read.js';
import { plainTextOf } from '../text/index.js';
import type { CompiledQuery, Cond, OrderKey, Path, Scalar } from './ast.js';

interface Resolved {
	found: boolean;
	value?: unknown;
}

/**
 * A resolution that also carries the leaf's descriptor, for the callers that
 * need the element type — `exists` here, the fan-out encoder in indexkey.ts.
 *
 * Kept apart from {@link Resolved} deliberately: a DescField is a node in a
 * cyclic descriptor graph, so a result carrying one cannot be deep-compared,
 * and resolvePath's result is compared that way all over the suite.
 */
export interface ResolvedWithField extends Resolved {
	field?: DescField;
}

/**
 * Walks a localName path through a decoded message. When the schema is
 * provided and the leaf is a collaborative-text field
 * ((syncplane.v1.field_behavior).text), the encoded document resolves to its
 * visible plain string so string ops and ordering see what the user sees —
 * mirroring the Go evaluator. Undecodable state stays incomparable.
 *
 * A oneof member has no property of its own — protobuf-es puts the group in one
 * `{ case, value }` property — so it is read through the group, and resolves to
 * "not found" unless its case is the active one. That is what the Go evaluator
 * reports for the same path (protoreflect Has/Get on the member), and reading
 * the raw property instead would answer every such query with a silent false.
 */
export function resolvePath(row: unknown, path: Path, schema?: DescMessage): Resolved {
	const { found, value } = resolveWithField(row, path, schema);
	return found ? { found, value } : { found };
}

/** resolvePath, plus the leaf's descriptor. See {@link ResolvedWithField} for
 * why the two are separate functions rather than one wider result. */
export function resolveWithField(
	row: unknown,
	path: Path,
	schema?: DescMessage,
): ResolvedWithField {
	let current: unknown = row;
	let desc: DescMessage | undefined = schema;
	let field: DescField | undefined;
	for (const segment of path) {
		if (current === null || current === undefined || typeof current !== 'object') {
			return { found: false };
		}
		const rec = current as Record<string, unknown>;
		field = desc?.fields.find((f) => f.localName === segment);
		if (field?.oneof) {
			const group = rec[field.oneof.localName] as { case?: string; value?: unknown } | undefined;
			current = group?.case === segment ? group.value : undefined;
		} else {
			current = rec[segment];
		}
		desc = field?.fieldKind === 'message' ? field.message : undefined;
	}
	if (current === undefined) return { found: false };
	if (field && current instanceof Uint8Array && isTextField(field)) {
		try {
			return { found: true, value: plainTextOf(current), field };
		} catch {
			// Corrupt document: present but incomparable, like other bytes.
		}
	}
	return { found: true, value: current, field };
}

/** `===` with number/bigint normalization. */
function equalsScalar(a: unknown, b: Scalar): boolean {
	if (typeof a === 'bigint' && typeof b === 'number') {
		return Number.isInteger(b) ? a === BigInt(b) : Number(a) === b;
	}
	if (typeof a === 'number' && typeof b === 'bigint') {
		return Number.isInteger(a) ? BigInt(a) === b : a === Number(b);
	}
	return a === b;
}

/**
 * Compares two values of the same logical kind. Returns undefined when the
 * values are incomparable (type mismatch, or NaN on either side); callers
 * treat that as condition-false (filtering) or a rank (ordering, see
 * orderRank).
 */
export function compareValues(a: unknown, b: unknown): number | undefined {
	// NaN participates in no comparison at all, which is what gives the row
	// ordering a total order and makes it encodable as index keys. Checked
	// here rather than inside `cmp` so the mixed number/bigint paths — which
	// route a NaN through `Number(bigint)` rather than through `cmp` on two
	// numbers — get the same answer.
	if (isNaNValue(a) || isNaNValue(b)) return undefined;
	if (typeof a === 'number' && typeof b === 'bigint') {
		if (Number.isInteger(a)) return cmp(BigInt(a), b);
		return cmp(a, Number(b));
	}
	if (typeof a === 'bigint' && typeof b === 'number') {
		if (Number.isInteger(b)) return cmp(a, BigInt(b));
		return cmp(Number(a), b);
	}
	if (typeof a !== typeof b) return undefined;
	if (typeof a === 'number' || typeof a === 'bigint' || typeof a === 'string') {
		return cmp(a, b as typeof a);
	}
	if (typeof a === 'boolean') {
		return cmp(Number(a), Number(b));
	}
	return undefined;
}

function cmp<T extends number | bigint | string>(a: T, b: T): number {
	return a < b ? -1 : a > b ? 1 : 0;
}

/** Only a `number` can be NaN — a bigint has no such value. */
function isNaNValue(v: unknown): boolean {
	return typeof v === 'number' && Number.isNaN(v);
}

/**
 * Order ranks, the coarse bucket a resolved value falls into before its value
 * is looked at. The rank is compared BEFORE the key's direction is applied,
 * which is what keeps NaN and missing at the end of the result under both
 * `asc` and `desc` — and what lets the index encoder give them fixed tags
 * (indexkey.ts) outside the descending inversion.
 */
const RANK_REAL = 0;
const RANK_NAN = 1;
const RANK_MISSING = 2;

function orderRank(r: Resolved): number {
	if (!r.found) return RANK_MISSING;
	return isNaNValue(r.value) ? RANK_NAN : RANK_REAL;
}

/** Evaluates a condition against one decoded row value. */
export function evalCond(row: unknown, cond: Cond, schema?: DescMessage): boolean {
	switch (cond.kind) {
		case 'and':
			return cond.conds.every((c) => evalCond(row, c, schema));
		case 'or':
			return cond.conds.some((c) => evalCond(row, c, schema));
		case 'not':
			return !evalCond(row, cond.cond, schema);
		case 'present': {
			const { found } = resolvePath(row, cond.path, schema);
			return cond.op === 'isSet' ? found : !found;
		}
		case 'cmp': {
			const { found, value } = resolvePath(row, cond.path, schema);
			if (!found) return false;
			if (cond.op === 'eq') return equalsScalar(value, cond.value);
			if (cond.op === 'neq') return !equalsScalar(value, cond.value);
			const c = compareValues(value, cond.value);
			if (c === undefined) return false;
			switch (cond.op) {
				case 'gt':
					return c > 0;
				case 'gte':
					return c >= 0;
				case 'lt':
					return c < 0;
				case 'lte':
					return c <= 0;
			}
			return false;
		}
		case 'in': {
			const { found, value } = resolvePath(row, cond.path, schema);
			if (!found) return false;
			return cond.values.some((v) => equalsScalar(value, v));
		}
		case 'str': {
			const { found, value } = resolvePath(row, cond.path, schema);
			if (!found || typeof value !== 'string') return false;
			switch (cond.op) {
				case 'startsWith':
					return value.startsWith(cond.value);
				case 'endsWith':
					return value.endsWith(cond.value);
				case 'contains':
					return value.includes(cond.value);
			}
			return false;
		}
		case 'rep': {
			const { found, value } = resolvePath(row, cond.path, schema);
			if (!found || !Array.isArray(value)) return false;
			switch (cond.op) {
				case 'isEmpty':
					return value.length === 0;
				case 'has':
					return value.some((e) => equalsScalar(e, cond.value));
				case 'hasAny':
					return cond.values.some((v) => value.some((e) => equalsScalar(e, v)));
				case 'hasAll':
					return cond.values.every((v) => value.some((e) => equalsScalar(e, v)));
			}
			return false;
		}
		case 'exists': {
			const { found, value, field } = resolveWithField(row, cond.path, schema);
			// An unset or empty list matches nothing, like every other condition
			// on a missing value — which also makes not(any(…)) read as "no
			// element matches".
			if (!found || !Array.isArray(value)) return false;
			const element = field?.fieldKind === 'list' && field.listKind === 'message'
				? field.message
				: undefined;
			return value.some((e) => evalCond(e, cond.cond, element));
		}
	}
}

/**
 * Multi-key row comparator. Values rank real < NaN < missing and the rank
 * decides before the direction does, so NaN and missing both sort last under
 * `asc` and `desc` alike; equal non-real ranks and real ties fall through to
 * the next key and finally to the row key ascending.
 */
export function compareRows<T>(
	a: Row<T>,
	b: Row<T>,
	orderBy: OrderKey[],
	schema?: DescMessage,
): number {
	for (const key of orderBy) {
		const ra = resolvePath(a.value, key.path, schema);
		const rb = resolvePath(b.value, key.path, schema);
		const rankA = orderRank(ra);
		const rankB = orderRank(rb);
		if (rankA !== rankB) return rankA < rankB ? -1 : 1;
		// Both NaN or both missing: nothing to distinguish them by.
		if (rankA !== RANK_REAL) continue;
		const c = compareValues(ra.value, rb.value) ?? 0;
		if (c !== 0) return key.dir === 'desc' ? -c : c;
	}
	return cmp(a.key, b.key);
}

/** filter → sort → slice. Input rows are already key-ordered. */
export function runQuery<T>(rows: Row<T>[], q: CompiledQuery, schema?: DescMessage): Row<T>[] {
	let out = q.where
		? rows.filter((r) => evalCond(r.value, q.where as Cond, schema))
		: rows.slice();
	if (q.orderBy && q.orderBy.length > 0) {
		const orderBy = q.orderBy;
		out = out.sort((a, b) => compareRows(a, b, orderBy, schema));
	}
	const start = q.offset ?? 0;
	const end = q.limit !== undefined ? start + q.limit : undefined;
	if (start !== 0 || end !== undefined) {
		out = out.slice(start, end);
	}
	return out;
}
