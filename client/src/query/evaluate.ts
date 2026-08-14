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
// part in the comparison at all (bytes, message, a whole map, repeated) never
// reaches this file: prepareQuery rejects it against the schema first, so
// nothing below has to decide what "eq against a map" means.
//
// A path segment following a MAP field is the entry KEY — the convention lww
// mask paths already use, so there is one rule rather than two — and one entry
// is an ordinary value. Because an entry has explicit presence, a path naming
// one variant of an app-defined oneof value ("custom.priority.number") reads as
// NOT FOUND on a row whose entry holds another variant, which is what lets the
// value type be app-defined: no cross-variant comparison ever has to exist.
//
// The mirror question — "which entry holds X" rather than "does THIS entry hold
// X" — is an `exists` over the map, whose element is the ENTRY and whose
// sub-condition addresses `key` and `value`. It answers whether some entry
// matched, never which one; nothing here projects.

import { type DescField, type DescMessage, ScalarType } from '@bufbuild/protobuf';

import { isTextField } from '../mutations.js';
import type { Row } from '../read.js';
import { analyze, analyzeQuery, type QueryTerm } from '../search/index.js';
import { plainTextOf } from '../text/index.js';
import type { CompiledQuery, Cond, OrderKey, PageWindow, Path, Scalar } from './ast.js';
import { type SearchDecl, searchDeclOf } from './match.js';

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
	// Set on the step after a map field: the next segment is the entry KEY, and
	// this carries whether that key is a string (used verbatim) and the value's
	// message type (so a path may continue into it).
	let mapAt: { stringKey: boolean; valueOf: DescMessage | undefined } | undefined;
	for (const segment of path) {
		if (current === null || current === undefined || typeof current !== 'object') {
			return { found: false };
		}
		const rec = current as Record<string, unknown>;
		if (mapAt) {
			// protobuf-es represents a map as a plain object, so an entry is a
			// property lookup — but a non-string key is stored under its CANONICAL
			// decimal form, and a path may spell it any way the parser accepts
			// ("007"). Normalizing is what makes both spellings reach the same
			// entry, which is what the Go side does by parsing the segment to the
			// key type.
			const key = mapAt.stringKey ? segment : normalizeNumericKey(segment);
			current = Object.hasOwn(rec, key) ? rec[key] : undefined;
			desc = mapAt.valueOf;
			field = undefined;
			mapAt = undefined;
			continue;
		}
		field = desc?.fields.find((f) => f.localName === segment);
		if (field?.oneof) {
			const group = rec[field.oneof.localName] as { case?: string; value?: unknown } | undefined;
			current = group?.case === segment ? group.value : undefined;
		} else {
			current = rec[segment];
		}
		if (field?.fieldKind === 'map') {
			mapAt = {
				// ScalarType.STRING === 9 — the proto field-type number, which
				// protobuf-es reuses verbatim.
				stringKey: field.mapKey === 9,
				valueOf: field.mapKind === 'message' ? field.message : undefined,
			};
			desc = undefined;
			continue;
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

/**
 * Canonicalizes a non-string map key so a path finds the entry however it was
 * spelled. protobuf-es stores an integral key under its own decimal form, so
 * "007" would miss an entry keyed 7 — while the Go side parses the segment to
 * the key type and finds it. Bool keys are already canonical.
 *
 * An unparseable key is left as it is and simply matches nothing; the schema
 * resolution (wire.ts) is what rejects one outright, so this is the fallback for
 * an AST that never went through it.
 */
function normalizeNumericKey(segment: string): string {
	if (segment === 'true' || segment === 'false') return segment;
	try {
		return BigInt(segment).toString();
	} catch {
		return segment;
	}
}

/** A map field, narrowed. */
type MapField = Extract<DescField, { fieldKind: 'map' }>;

/**
 * One ENTRY of a map, as an `exists` over that map scopes it.
 *
 * protobuf-es surfaces no message type for a map entry — proto's synthetic entry
 * type is not part of the generated shapes — so the entry travels as the
 * (key, value) pair it is, and the first segment of a sub-condition path picks
 * the side it means. The Go evaluator carries the same pair for the same reason.
 */
interface EntryScope {
	map: MapField;
	key: Scalar;
	value: unknown;
}

/** The two segments a sub-condition inside a map entry may start at. They are
 * the same words an index component's `*` selector uses. */
const ENTRY_KEY = 'key';
const ENTRY_VALUE = 'value';

/**
 * A map's property key as the value its declared key type gives it.
 *
 * protobuf-es stores every key as a property STRING, so an int-keyed map would
 * otherwise compare its keys as strings and `e.key.gt(5)` would see "5". This is
 * the inverse of {@link normalizeNumericKey}, which converts the other way for
 * an entry addressed by key.
 */
function typedMapKey(field: MapField, raw: string): Scalar {
	switch (field.mapKey) {
		case ScalarType.STRING:
			return raw;
		case ScalarType.BOOL:
			return raw === 'true';
		case ScalarType.INT64:
		case ScalarType.UINT64:
		case ScalarType.SINT64:
		case ScalarType.FIXED64:
		case ScalarType.SFIXED64:
			return BigInt(raw);
		default:
			return Number(raw);
	}
}

/**
 * Walks a path through whatever scope a condition is being evaluated in.
 *
 * A MESSAGE scope is resolveWithField unchanged. An ENTRY scope resolves the
 * first segment against the pair itself — `key` or `value` — and everything
 * below `value` is an ordinary path through the value's message type, which is
 * what makes `value.number` inside an any() mean exactly what
 * `custom.<key>.number` means from the row.
 */
function resolveIn(
	row: unknown,
	path: Path,
	schema: DescMessage | undefined,
	entry: EntryScope | undefined,
): ResolvedWithField {
	if (!entry) return resolveWithField(row, path, schema);
	const [ head, ...rest ] = path;
	if (head === ENTRY_KEY) {
		// A key is a scalar, so nothing can follow it; prepareQuery rejects such
		// a path before this is reached.
		return rest.length > 0 ? { found: false } : { found: true, value: entry.key };
	}
	if (head !== ENTRY_VALUE) return { found: false };
	if (rest.length === 0) {
		// An entry always HAS a value, whatever it holds.
		return { found: true, value: entry.value };
	}
	if (entry.map.mapKind !== 'message') return { found: false };
	return resolveWithField(entry.value, rest, entry.map.message);
}

/** `===` with number/bigint normalization. */
export function equalsScalar(a: unknown, b: Scalar): boolean {
	if (typeof a === 'bigint' && typeof b === 'number') {
		return Number.isInteger(b) ? a === BigInt(b) : Number(a) === b;
	}
	if (typeof a === 'number' && typeof b === 'bigint') {
		return Number.isInteger(a) ? BigInt(a) === b : a === Number(b);
	}
	return a === b;
}

/**
 * Where hashing a membership test starts paying for itself. Below it the set's
 * construction and the per-row hash cost more than the comparisons they
 * replace; above it the linear scan is what the row count multiplies.
 *
 * It matters most for a list nobody wrote by hand: a `matches` condition
 * collapses to a membership test holding a key per matching target row, and a
 * query no index serves tests every candidate row against all of them.
 */
const IN_SET_THRESHOLD = 16;

/**
 * Canonical membership keys per `in` condition, built on the first row and
 * dropped with the condition itself. Queries build a fresh AST per run, so
 * nothing here outlives the query that needed it.
 *
 * `null` marks a list that must be scanned rather than hashed.
 */
const membershipSets = new WeakMap<readonly Scalar[], Set<string> | null>();

function membershipSet(values: readonly Scalar[]): Set<string> | undefined {
	if (values.length < IN_SET_THRESHOLD) return undefined;
	const cached = membershipSets.get(values);
	if (cached !== undefined) return cached ?? undefined;
	const set = new Set<string>();
	for (const v of values) {
		const key = scalarKey(v);
		if (key === undefined) {
			// A value equalsScalar decides by something other than its
			// canonical key. Rare enough that declining the whole set is
			// cheaper than reasoning about a mixed lookup.
			membershipSets.set(values, null);
			return undefined;
		}
		set.add(key);
	}
	membershipSets.set(values, set);
	return set;
}

/**
 * The canonical membership key of a scalar: two values share one exactly when
 * equalsScalar calls them equal. Undefined for a value whose equality no key
 * can decide — a NaN, which equals nothing including itself, an infinity,
 * which a bigint out of Number range would also have to match, and anything
 * that is not a scalar.
 *
 * The number/bigint pair is what the normalization is for: they compare across
 * types after an integer check, so an integral number and a bigint of the same
 * magnitude have to land on the same key.
 */
export function scalarKey(v: unknown): string | undefined {
	switch (typeof v) {
		case 'string':
			return 's' + v;
		case 'boolean':
			return v ? 'b1' : 'b0';
		case 'bigint':
			return 'i' + v.toString();
		case 'number':
			if (Number.isInteger(v)) return 'i' + BigInt(v).toString();
			if (!Number.isFinite(v)) return undefined;
			// Number -> string is the shortest round-tripping form, so two
			// keys are equal exactly when the doubles are.
			return 'f' + String(v);
		default:
			return undefined;
	}
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

/** A map, as protobuf-es represents one: a plain object of entries. */
function isPlainMap(v: unknown): boolean {
	return typeof v === 'object' && v !== null && !Array.isArray(v) && !(v instanceof Uint8Array);
}

/** Cap on how many distinct terms of a query a match requires; terms past it
 * are ignored rather than rejected. Mirror of engine/query maxMatchTerms. */
const MAX_MATCH_TERMS = 32;

const RANK_BYTE_MAX = 255;

/**
 * One compiled match condition: the declaration it reads and the required
 * terms of its query, with ranks memoized per row value object. Mirror of
 * engine/query's Matcher; testdata/search/vectors.json pins the rank values
 * of both, because ranks encode into query cursors.
 */
export class Matcher {
	readonly terms: QueryTerm[];
	private readonly ranks = new WeakMap<object, number>();

	constructor(readonly decl: SearchDecl, query: string) {
		this.terms = requiredTerms(analyzeQuery(query, decl.options));
	}

	/** Whether every required term occurs in some declared field of the row. A
	 * matcher with no terms matches every row. */
	matches(row: unknown, schema: DescMessage | undefined): boolean {
		return Math.floor(this.rank(row, schema) / 2 ** 40) === 0;
	}

	/**
	 * Scores the row against the query: a 48-bit integer where SMALLER IS
	 * BETTER, so ascending rank order is best-match-first. Byte for byte, most
	 * significant first: terms missing entirely, terms found only as a prefix,
	 * first declared field containing every term (255 when none does), smallest
	 * token window covering every term there, earliest matched position there,
	 * and 255 minus the total matching-token count — each capped at 255. The
	 * rank reads the row alone, never the store, which is what keeps a client
	 * ranking over its synced subset identical to the server's over everything.
	 */
	rank(row: unknown, schema: DescMessage | undefined): number {
		if (typeof row === 'object' && row !== null) {
			const cached = this.ranks.get(row);
			if (cached !== undefined) return cached;
			const r = this.computeRank(row, schema);
			this.ranks.set(row, r);
			return r;
		}
		return this.computeRank(row, schema);
	}

	private computeRank(row: unknown, schema: DescMessage | undefined): number {
		const n = this.terms.length;
		if (n === 0) return 0;
		const matched: boolean[] = new Array<boolean>(n).fill(false);
		const exact: boolean[] = new Array<boolean>(n).fill(false);
		let occ = 0;
		let attribute = RANK_BYTE_MAX;
		let proximity = RANK_BYTE_MAX;
		let first = RANK_BYTE_MAX;

		for (let fi = 0; fi < this.decl.fields.length; fi++) {
			const { found, value } = resolvePath(row, this.decl.fields[fi]!.path, schema);
			if (!found || typeof value !== 'string') continue;
			const tokens = analyze(value, this.decl.options);
			if (tokens.length === 0) continue;
			const events: { pos: number; term: number }[] = [];
			const seen: boolean[] = new Array<boolean>(n).fill(false);
			let inField = 0;
			for (let pos = 0; pos < tokens.length; pos++) {
				const tok = tokens[pos]!;
				for (let j = 0; j < n; j++) {
					const t = this.terms[j]!;
					if (t.prefix ? !tok.startsWith(t.term) : tok !== t.term) continue;
					events.push({ pos, term: j });
					occ++;
					matched[j] = true;
					if (tok === t.term) exact[j] = true;
					if (!seen[j]) {
						seen[j] = true;
						inField++;
					}
				}
			}
			if (attribute === RANK_BYTE_MAX && inField === n) {
				// 254, not 255: the sentinel byte must stay worse than any real
				// field, however many are declared.
				attribute = Math.min(fi, RANK_BYTE_MAX - 1);
				proximity = Math.min(minWindow(events, n), RANK_BYTE_MAX);
				first = Math.min(events[0]!.pos, RANK_BYTE_MAX);
			}
		}

		let miss = 0;
		let prefixOnly = 0;
		for (let j = 0; j < n; j++) {
			if (!matched[j]) miss++;
			else if (!exact[j]) prefixOnly++;
		}
		miss = Math.min(miss, RANK_BYTE_MAX);
		prefixOnly = Math.min(prefixOnly, RANK_BYTE_MAX);
		const occComp = RANK_BYTE_MAX - Math.min(occ, RANK_BYTE_MAX);
		// Multiplication, not shifts: the rank is 48 bits and JS bitwise
		// operators truncate to 32.
		return ((((miss * 256 + prefixOnly) * 256 + attribute) * 256 + proximity) * 256 + first)
				* 256
			+ occComp;
	}

	/**
	 * Fingerprints everything this matcher's ranks depend on — declaration and
	 * required terms — for the cursor shape. Non-ASCII code units render as
	 * \uXXXX escapes because the two fingerprint implementations walk different
	 * units (Go UTF-8 bytes, TS UTF-16 code units) and only agree on ASCII.
	 */
	specString(): string {
		let s = this.decl.spec + '?';
		this.terms.forEach((t, i) => {
			if (i > 0) s += ' ';
			for (let j = 0; j < t.term.length; j++) {
				const u = t.term.charCodeAt(j);
				s += u < 0x80 ? t.term[j]! : '\\u' + u.toString(16).padStart(4, '0');
			}
			if (t.prefix) s += '*';
		});
		return s;
	}
}

/**
 * Reduces an analyzed query to the terms a row must satisfy: duplicates
 * collapse, a prefix term subsumed by an equal exact term drops, and
 * everything past MAX_MATCH_TERMS is ignored.
 */
function requiredTerms(terms: QueryTerm[]): QueryTerm[] {
	const exact = new Set(terms.filter((t) => !t.prefix).map((t) => t.term));
	const out: QueryTerm[] = [];
	const seen = new Set<string>();
	for (const t of terms) {
		if (t.prefix && exact.has(t.term)) continue;
		if (seen.has(t.term)) continue;
		seen.add(t.term);
		out.push(t);
		if (out.length === MAX_MATCH_TERMS) break;
	}
	return out;
}

/**
 * The smallest position span covering at least one event of every term — the
 * classic two-pointer minimal cover. Events must be sorted by position
 * (generation order guarantees it) and cover all n terms.
 */
function minWindow(events: { pos: number; term: number }[], n: number): number {
	if (n === 1) return 0;
	const counts: number[] = new Array<number>(n).fill(0);
	let have = 0;
	let best = Number.MAX_SAFE_INTEGER;
	let l = 0;
	for (let r = 0; r < events.length; r++) {
		if (counts[events[r]!.term]! === 0) have++;
		counts[events[r]!.term]!++;
		while (have === n) {
			const w = events[r]!.pos - events[l]!.pos;
			if (w < best) best = w;
			counts[events[l]!.term]!--;
			if (counts[events[l]!.term] === 0) have--;
			l++;
		}
	}
	return best;
}

const matchers = new WeakMap<Cond, Matcher>();

/**
 * The compiled matcher of a match condition, built once per condition object
 * and memoized. Throws when the entity declares no search fields, or when no
 * schema is available to read the declaration from.
 */
export function matcherOf(
	cond: Cond & { kind: 'match' },
	schema: DescMessage | undefined,
): Matcher {
	const cached = matchers.get(cond);
	if (cached) return cached;
	if (!schema) throw new Error('match needs the entity schema to resolve its search fields');
	const decl = searchDeclOf(schema);
	if (!decl) {
		throw new Error(
			`match: ${schema.typeName} declares no search fields; mark fields searchable or declare EntityOptions.search`,
		);
	}
	const m = new Matcher(decl, cond.query);
	matchers.set(cond, m);
	return m;
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

/**
 * Evaluates a condition against one decoded row value.
 *
 * `entry` scopes the condition to one map ENTRY instead of a message — what an
 * `exists` over a map gives its sub-condition. It is internal to this recursion;
 * every caller outside starts from a message.
 */
export function evalCond(
	row: unknown,
	cond: Cond,
	schema?: DescMessage,
	entry?: EntryScope,
): boolean {
	switch (cond.kind) {
		case 'and':
			return cond.conds.every((c) => evalCond(row, c, schema, entry));
		case 'or':
			return cond.conds.some((c) => evalCond(row, c, schema, entry));
		case 'not':
			return !evalCond(row, cond.cond, schema, entry);
		case 'present': {
			const { found } = resolveIn(row, cond.path, schema, entry);
			return cond.op === 'isSet' ? found : !found;
		}
		case 'cmp': {
			const { found, value } = resolveIn(row, cond.path, schema, entry);
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
			const { found, value } = resolveIn(row, cond.path, schema, entry);
			if (!found) return false;
			const set = membershipSet(cond.values);
			if (set) {
				// A row value with no canonical key equals none of the values
				// in the set, which all have one — the same answer the scan
				// below gives.
				const key = scalarKey(value);
				return key !== undefined && set.has(key);
			}
			return cond.values.some((v) => equalsScalar(value, v));
		}
		case 'str': {
			const { found, value } = resolveIn(row, cond.path, schema, entry);
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
			const { found, value } = resolveIn(row, cond.path, schema, entry);
			if (!found) return false;
			if (!Array.isArray(value)) {
				// prepareQuery admits a map for isEmpty alone, so this is the only
				// operator that can reach one. A map is a plain object here, which
				// is why the array check cannot stand in for the collection check.
				return cond.op === 'isEmpty' && isPlainMap(value)
					&& Object.keys(value as object).length === 0;
			}
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
		case 'rel':
			// A rel condition is not row-local: the execute pipeline rewrites it
			// to a membership test on the stored stubs before anything evaluates
			// (query/rel.ts), so one reaching this evaluator is a missed rewrite,
			// not a query that can be answered.
			throw new Error(
				`query path ${cond.path.join('.')}: matches() must be resolved before evaluation`,
			);
		case 'match':
			// prepareQuery rejects a match inside any(); a map entry has no
			// searchable fields, so answer false rather than throw.
			if (entry) return false;
			return matcherOf(cond, schema).matches(row, schema);
		case 'exists': {
			const { found, value, field } = resolveIn(row, cond.path, schema, entry);
			// An unset or empty collection matches nothing, like every other
			// condition on a missing value — which also makes not(any(…)) read as
			// "no element matches".
			if (!found) return false;
			if (Array.isArray(value)) {
				const element = field?.fieldKind === 'list' && field.listKind === 'message'
					? field.message
					: undefined;
				return value.some((e) => evalCond(e, cond.cond, element));
			}
			// A MAP: the element is one ENTRY, and the sub-condition addresses
			// its two sides. The key comes back typed, because protobuf-es stores
			// it as a property string and `e.key.gt(5)` has to see a number.
			if (field?.fieldKind !== 'map' || !isPlainMap(value)) return false;
			const map = field;
			return Object.entries(value as Record<string, unknown>).some(([ raw, v ]) =>
				evalCond(undefined, cond.cond, undefined, {
					map,
					key: typedMapKey(map, raw),
					value: v,
				})
			);
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
		if (key.relevance) {
			if (key.match?.kind !== 'match') {
				throw new Error('a relevance order key must be prepared against its query (prepareQuery)');
			}
			const m = matcherOf(key.match, schema);
			const c = cmp(m.rank(a.value, schema), m.rank(b.value, schema));
			if (c !== 0) return key.dir === 'desc' ? -c : c;
			continue;
		}
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
export function runQuery<T>(
	rows: Row<T>[],
	q: CompiledQuery,
	schema?: DescMessage,
	window?: PageWindow,
): Row<T>[] {
	return runQueryPage(rows, q, schema, window).rows;
}

/**
 * `runQuery`, with the number of rows that matched before the page was cut and
 * whether any were left over — both free here, because the whole match set
 * exists either way. A query carrying `total: false` still gets `undefined`
 * for it: the number costs nothing on this path, but the index path it must
 * agree with cannot say the same.
 *
 * `window` is the query's cursor bounds (see pageWindowOf). Rows outside them
 * are not matches at all: `total` counts what is INSIDE the bounds, so a paged
 * scroller sizes itself against the range it is walking.
 */
export function runQueryPage<T>(
	rows: Row<T>[],
	q: CompiledQuery,
	schema?: DescMessage,
	window?: PageWindow,
): { rows: Row<T>[]; total: number | undefined; hasMore: boolean } {
	let out = q.where
		? rows.filter((r) => evalCond(r.value, q.where as Cond, schema))
		: rows.slice();
	if (window) out = out.filter((r) => window.within(r.value, r.key));
	if (q.orderBy && q.orderBy.length > 0) {
		const orderBy = q.orderBy;
		out = out.sort((a, b) => compareRows(a, b, orderBy, schema));
	}
	const total = q.total === false ? undefined : out.length;
	return { ...pageOf(out, q, window), total };
}

/**
 * Cuts the sorted match set down to the page. A backward page — `before` with
 * no `after` — takes its limit from the END of the range, which is what makes
 * it the page PRECEDING the bound; every other page takes offset then limit
 * from the start. The rows stay in the query's order either way.
 */
function pageOf<T>(
	out: Row<T>[],
	q: CompiledQuery,
	window: PageWindow | undefined,
): { rows: Row<T>[]; hasMore: boolean } {
	if (q.limit !== undefined && window?.backward) {
		return out.length > q.limit
			? { rows: out.slice(out.length - q.limit), hasMore: true }
			: { rows: out, hasMore: false };
	}
	const start = q.offset ?? 0;
	const end = q.limit !== undefined ? start + q.limit : undefined;
	return {
		rows: start !== 0 || end !== undefined ? out.slice(start, end) : out,
		hasMore: end !== undefined && out.length > end,
	};
}
