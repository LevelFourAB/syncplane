import { type DescField, type DescMessage, type DescOneof, ScalarType } from '@bufbuild/protobuf';

import { isTextField, refKeyFields } from '../mutations.js';
import type { CompiledQuery, Cond, OrderKey, Path } from './ast.js';

/**
 * Translates a compiled query's paths from JS localNames (the property names
 * on generated message shapes, e.g. ["createdAtMs"]) to proto field names
 * (["created_at_ms"]) using the schema's descriptors. This is the wire form
 * the server-side query push-down consumes (see SyncClient.queryServer).
 *
 * Throws on paths that name unknown fields or descend through non-message
 * fields.
 */
export function toWirePaths(schema: DescMessage, compiled: CompiledQuery): CompiledQuery {
	const out: CompiledQuery = {};
	if (compiled.where) out.where = translateCond(messageScope(schema), compiled.where);
	if (compiled.orderBy) {
		out.orderBy = compiled.orderBy.map(
			(key): OrderKey => ({ path: translatePath(messageScope(schema), key.path), dir: key.dir }),
		);
	}
	if (compiled.limit !== undefined) out.limit = compiled.limit;
	if (compiled.offset !== undefined) out.offset = compiled.offset;
	if (compiled.after !== undefined) out.after = compiled.after;
	if (compiled.before !== undefined) out.before = compiled.before;
	return out;
}

/**
 * What a condition's paths resolve against: a message — the row, or one element
 * of a repeated message field — or one ENTRY of a map, which is what an `any()`
 * over a map scopes its sub-condition to.
 *
 * The entry case exists because protobuf-es surfaces no message type for a map
 * entry: proto's synthetic entry type is not part of the generated shapes, so
 * the two sides are named here (`key`, `value`) rather than looked up on a
 * descriptor. The Go compiler resolves the same two words through the map's
 * MapKey/MapValue descriptors, which is why both sides agree on the spelling
 * without sharing an implementation.
 */
type CondScope =
	| { kind: 'message'; schema: DescMessage }
	| { kind: 'entry'; map: MapField };

/** A map field, narrowed. */
type MapField = Extract<DescField, { fieldKind: 'map' }>;

const messageScope = (schema: DescMessage): CondScope => ({ kind: 'message', schema });

/** The two segments a sub-condition inside a map entry may start at. They are
 * the same words an index component's `*` selector uses. */
const ENTRY_KEY = 'key';
const ENTRY_VALUE = 'value';

function translateCond(scope: CondScope, cond: Cond): Cond {
	switch (cond.kind) {
		case 'cmp':
		case 'in':
		case 'str':
		case 'present':
			return { ...cond, path: translatePath(scope, cond.path) };
		case 'rep':
			return { ...cond, path: translatePath(scope, cond.path) };
		case 'exists': {
			// The sub-condition's paths are relative to the ELEMENT, so they
			// translate against the element scope rather than this one.
			const leaf = translatePathLeaf(scope, cond.path);
			const element = elementScopeOf(leaf.field);
			if (!element) {
				throw new Error(
					`query path ${cond.path.join('.')}: any() needs a repeated message field or a map`,
				);
			}
			return { kind: 'exists', path: leaf.path, cond: translateCond(element, cond.cond) };
		}
		case 'and':
		case 'or':
			return { ...cond, conds: cond.conds.map((c) => translateCond(scope, c)) };
		case 'not':
			return { ...cond, cond: translateCond(scope, cond.cond) };
	}
}

/**
 * Resolves a compiled query against its schema — the step between the typed
 * DSL and any evaluation of the result, local or pushed down. It does two
 * things, both mirrored by the Go compiler (queryeval.Compile), so a query
 * behaves the same wherever it runs:
 *
 * 1. **Reference leaves compare by their target's key.** A path that ENDS at a
 *    singular, single-key reference is extended with that key field
 *    (issue.project → issue.project.id), so both the local evaluator and the
 *    wire form compare on the stored key. Descending THROUGH a ref into
 *    anything but its key fields is rejected (a ref stores only keys, so such
 *    a path could never match); a composite-key ref is incomparable as a
 *    scalar, so only isSet/isNotSet works and the path stays at the bare ref.
 * 2. **Operators the leaf could never satisfy are rejected.** Comparing or
 *    ordering by bytes, a message, a map or a repeated field would otherwise
 *    match nothing — or, in an ordering, tie every pair and silently degrade
 *    to row-key order — with nothing to tell the developer why.
 *
 * Operates on localName paths (the compiled-query form); run before
 * toWirePaths and before local evaluation.
 */
export function prepareQuery(schema: DescMessage, compiled: CompiledQuery): CompiledQuery {
	const out: CompiledQuery = {};
	if (compiled.where) out.where = prepareCond(messageScope(schema), compiled.where);
	if (compiled.orderBy) {
		out.orderBy = compiled.orderBy.map((key): OrderKey => {
			const leaf = resolveLeafRef(schema, key.path, true);
			// An incomparable order key ties every pair, so the rows would come
			// back in row-key order as if no ordering had been asked for.
			requireComparable(key.path, leaf);
			return { path: leaf.path, dir: key.dir };
		});
	}
	if (compiled.limit !== undefined) out.limit = compiled.limit;
	if (compiled.offset !== undefined) out.offset = compiled.offset;
	if (compiled.after !== undefined) out.after = compiled.after;
	if (compiled.before !== undefined) out.before = compiled.before;
	if (compiled.total !== undefined) out.total = compiled.total;
	if ((out.offset ?? 0) > 0 && (out.after !== undefined || out.before !== undefined)) {
		// The two describe a page in incompatible terms — a rank and a position
		// — so any resolution here would be an invention. Failing says which of
		// them to drop.
		throw new Error(
			'query: offset cannot be combined with after/before; page with the cursors, '
				+ 'or drop them to page by rank',
		);
	}
	return out;
}

function prepareCond(scope: CondScope, cond: Cond): Cond {
	switch (cond.kind) {
		case 'cmp':
		case 'in': {
			const leaf = resolveInScope(scope, cond.path, true);
			requireComparable(cond.path, leaf);
			return { ...cond, path: leaf.path };
		}
		case 'str': {
			const leaf = resolveInScope(scope, cond.path, true);
			if (leafKind(leaf) !== 'string') {
				throw new Error(
					`query path ${
						cond.path.join('.')
					}: startsWith/endsWith/contains need a string or text field, got ${describeLeaf(leaf)}`,
				);
			}
			return { ...cond, path: leaf.path };
		}
		case 'rep': {
			// rep operates on the collection itself, so the path stays at the bare
			// field.
			const leaf = resolveInScope(scope, cond.path, false);
			const kind = leafKind(leaf);
			// isEmpty asks about a COLLECTION, which a map is as much as a list is;
			// membership asks about elements, which only a list has.
			const isEmpty = cond.op === 'isEmpty';
			if (kind !== 'list' && !(isEmpty && kind === 'map')) {
				throw new Error(
					`query path ${cond.path.join('.')}: has/hasAny/hasAll/isEmpty need a repeated field, got ${
						describeLeaf(leaf)
					}`,
				);
			}
			// isEmpty works on any element type, so it needs nothing below.
			if (isEmpty) return { ...cond, path: leaf.path };
			// Membership compares elements. A repeated single-key reference has
			// one comparable value per element — the target key the row stores —
			// so it becomes an exists over that key field, the same rule that
			// points a comparison on a singular reference at its key. The typed
			// DSL never emits this shape (it offers any() on message lists, which
			// the type layer can check), but a Go client or a hand-built AST can.
			const refKeys = refKeyFields(leaf.field);
			if (refKeys.length === 1) return refMembershipCond(leaf.path, refKeys[0]!.localName, cond);
			if (!isComparable(elementKind(leaf.field))) {
				throw new Error(
					`query path ${
						cond.path.join('.')
					}: has/hasAny/hasAll need a repeated field with comparable elements, got ${
						describeLeaf(leaf)
					}; ${membershipHint(cond.path, leaf.field)}`,
				);
			}
			return { ...cond, path: leaf.path };
		}
		case 'exists': {
			const leaf = resolveInScope(scope, cond.path, false);
			const element = elementScopeOf(leaf.field, leaf);
			if (!element) {
				throw new Error(
					`query path ${cond.path.join('.')}: any() needs a repeated message field or a map, got ${
						describeLeaf(leaf)
					}; ${existsHint(leaf.field)}`,
				);
			}
			if (element.kind === 'entry') {
				// The element is the ENTRY, so a sub-condition starts at one of its
				// two sides. Checking up front lets the error name the map and the
				// two words that work, which resolving each path separately could
				// only do one path at a time.
				requireEntryPaths(cond.path, element.map, cond.cond);
			} else {
				// A reference element stores only the target's key fields, so the
				// same rule that stops a path descending through a reference stops
				// a sub-condition reaching past one: those fields are not there.
				const refKeys = refKeyFields(leaf.field);
				if (refKeys.length > 0) {
					requireRefKeyPaths(cond.path, refKeys.map((f) => f.localName), cond.cond);
				}
			}
			return { kind: 'exists', path: leaf.path, cond: prepareCond(element, cond.cond) };
		}
		case 'present':
			// Presence works on every leaf, so nothing to check.
			return { ...cond, path: resolveInScope(scope, cond.path, false).path };
		case 'and':
		case 'or':
			return { ...cond, conds: cond.conds.map((c) => prepareCond(scope, c)) };
		case 'not':
			return { ...cond, cond: prepareCond(scope, cond.cond) };
	}
}

/**
 * Resolves a path against whatever scope the condition is in.
 *
 * A MESSAGE scope is resolveLeafRef unchanged. An ENTRY scope resolves the first
 * segment against the pair itself, and everything below `value` is an ordinary
 * path through the value's message type — which is what makes `value.number`
 * inside an any() mean exactly what `custom.<key>.number` means from the row.
 */
function resolveInScope(scope: CondScope, path: Path, appendKey: boolean): ResolvedLeaf {
	if (scope.kind === 'message') return resolveLeafRef(scope.schema, path, appendKey);
	const map = scope.map;
	const [ head, ...rest ] = path;
	if (head === ENTRY_KEY) {
		if (rest.length > 0) {
			throw new Error(
				`query path ${path.join('.')}: "${ENTRY_KEY}" is the entry key of map field "${map.name}", `
					+ `a scalar, so "${rest[0]}" cannot follow it`,
			);
		}
		return {
			path: [ ENTRY_KEY ],
			protoPath: [ ENTRY_KEY ],
			field: map,
			keyed: false,
			entryKey: true,
			fans: [],
		};
	}
	if (head !== ENTRY_VALUE) {
		throw entryPathError(path, map, path);
	}
	if (rest.length === 0) {
		// The entry's value as a whole. For a scalar-valued map that is an
		// ordinary value; for a message-valued one requireComparable rejects it,
		// the same way a bare message leaf is rejected anywhere else.
		return {
			path: [ ENTRY_VALUE ],
			protoPath: [ ENTRY_VALUE ],
			field: map,
			keyed: true,
			entryKey: false,
			fans: [],
		};
	}
	if (map.mapKind !== 'message') {
		throw new Error(
			`query path ${path.join('.')}: map field "${map.name}" has a scalar value, so `
				+ `"${ENTRY_VALUE}" cannot be descended into`,
		);
	}
	const inner = resolveLeafRef(map.message, rest, appendKey);
	return {
		path: [ ENTRY_VALUE, ...inner.path ],
		protoPath: [ ENTRY_VALUE, ...inner.protoPath ],
		field: inner.field,
		keyed: inner.keyed,
		entryKey: false,
		fans: [],
	};
}

/**
 * The scope an `any()` gives its sub-condition, or undefined when the leaf has
 * no elements to scope.
 *
 * `leaf` disambiguates a map that a path ADDRESSED ONE ENTRY of — `custom.x` is
 * a value, not a collection — from a bare one, which is the collection itself.
 */
function elementScopeOf(field: DescField, leaf?: LeafRef): CondScope | undefined {
	if (field.fieldKind === 'list' && field.listKind === 'message') {
		return messageScope(field.message);
	}
	if (field.fieldKind === 'map' && !leaf?.keyed && !leaf?.entryKey) {
		return { kind: 'entry', map: field };
	}
	return undefined;
}

/**
 * Rejects a sub-condition addressing anything but the two sides of a map entry.
 *
 * resolveInScope would reject each such path anyway; checking the tree first is
 * what lets the error name the map and both words rather than only the segment
 * it tripped on.
 */
function requireEntryPaths(path: Path, map: MapField, cond: Cond): void {
	const check = (sub: Path): void => {
		if (sub[0] === ENTRY_KEY || sub[0] === ENTRY_VALUE) return;
		throw entryPathError(path, map, sub);
	};
	switch (cond.kind) {
		case 'cmp':
		case 'in':
		case 'str':
		case 'rep':
		case 'present':
		case 'exists':
			check(cond.path);
			return;
		case 'and':
		case 'or':
			for (const c of cond.conds) requireEntryPaths(path, map, c);
			return;
		case 'not':
			requireEntryPaths(path, map, cond.cond);
	}
}

function entryPathError(path: Path, map: MapField, sub: Path): Error {
	return new Error(
		`query path ${path.join('.')}: any() over map field "${map.name}" scopes one ENTRY, so a `
			+ `sub-condition starts at "${ENTRY_KEY}" or "${ENTRY_VALUE}", not "${sub.join('.')}"`,
	);
}

/**
 * Rewrites membership over a repeated single-key reference into exists
 * conditions on the target key. hasAll is a conjunction of them rather than one
 * pass: "holds all of these" is one existential per value, and no single
 * element satisfies more than one.
 */
function refMembershipCond(
	path: Path,
	keyName: string,
	cond: Extract<Cond, { kind: 'rep' }>,
): Cond {
	const key = [ keyName ];
	switch (cond.op) {
		case 'has':
			return { kind: 'exists', path, cond: { kind: 'cmp', path: key, op: 'eq', value: cond.value } };
		case 'hasAny':
			return { kind: 'exists', path, cond: { kind: 'in', path: key, values: cond.values } };
		case 'hasAll':
			return {
				kind: 'and',
				conds: cond.values.map((v): Cond => ({
					kind: 'exists',
					path,
					cond: { kind: 'cmp', path: key, op: 'eq', value: v },
				})),
			};
		default:
			// isEmpty is handled before this is reached.
			throw new Error(`query path ${path.join('.')}: unsupported membership operator`);
	}
}

/**
 * Rejects an any() sub-condition that addresses anything but the reference's
 * key fields. Without it such a condition prepares cleanly and then matches
 * nothing, since a stored reference element carries the key fields alone.
 */
function requireRefKeyPaths(path: Path, keyNames: string[], cond: Cond): void {
	const check = (sub: Path): void => {
		if (sub.length === 1 && keyNames.includes(sub[0]!)) return;
		throw new Error(
			`query path ${
				path.join('.')
			}: an element of a reference list holds only its key fields, so any() cannot ask about "${
				sub.join('.')
			}"`,
		);
	};
	switch (cond.kind) {
		case 'cmp':
		case 'in':
		case 'str':
		case 'rep':
		case 'present':
		case 'exists':
			check(cond.path);
			return;
		case 'and':
		case 'or':
			for (const c of cond.conds) requireRefKeyPaths(path, keyNames, c);
			return;
		case 'not':
			requireRefKeyPaths(path, keyNames, cond.cond);
	}
}

/** Names the way to ask about elements the operator cannot compare, which
 * differs by why it cannot. */
function membershipHint(path: Path, field: DescField): string {
	if (field.fieldKind === 'list' && field.listKind === 'message') {
		if (refKeyFields(field).length > 1) {
			return 'a composite-key reference has no single comparable value, so use any() on its key fields';
		}
		return `use any(), e.g. ${path.join('.')}.any((e) => …)`;
	}
	return 'use isEmpty';
}

/** Points a misapplied any() at the operator that does work. */
function existsHint(field: DescField): string {
	if (field.fieldKind === 'list') return 'use has/hasAny/hasAll for a repeated scalar';
	return 'any() asks about the elements of a list, or the entries of a map';
}

/** The kinds a leaf value can take, mirroring the Go evaluator's rvKind. */
export type LeafKind = 'string' | 'number' | 'bigint' | 'bool' | 'list' | 'map' | 'other';

function isComparable(kind: LeafKind): boolean {
	return kind === 'string' || kind === 'number' || kind === 'bigint' || kind === 'bool';
}

/**
 * What a resolved path ends at: the field, plus whether a MAP KEY was consumed
 * after it.
 *
 * A map key has no descriptor of its own, so the field alone cannot say what a
 * path yields — `attrs` is a map and `attrs.color` is a string, and both end at
 * the same DescField. `keyed` is the missing bit, and every classification below
 * reads it: a keyed leaf behaves exactly like a field of the map's VALUE type,
 * which is what lets an app-defined map value need no engine value type at all.
 */
export interface LeafRef {
	field: DescField;
	keyed: boolean;
	/**
	 * Set when the leaf is the KEY of a map entry rather than anything below it
	 * — the `key` side of an any() over a map, and the `key` selector of an index
	 * component. `field` is the map, since protobuf-es gives a map key no
	 * descriptor of its own.
	 */
	entryKey?: boolean;
}

/**
 * Rejects a repeated leaf whose ELEMENTS could never take part in a comparison.
 * An index component ending at a list stores one entry per element, so it is
 * the element kind that has to be comparable, not the list's.
 */
export function requireComparableElement(path: Path, field: DescField): void {
	if (isComparable(elementKind(field))) return;
	throw new Error(
		`index path ${path.join('.')}: ${describeLeaf({ field, keyed: false })} cannot be indexed; ${
			field.fieldKind === 'list' && field.listKind === 'message'
				? `index a scalar field of the elements instead, e.g. ${path.join('.')}.<field>`
				: 'its elements are not comparable'
		}`,
	);
}

export function leafKind(leaf: LeafRef): LeafKind {
	const { field, keyed } = leaf;
	if (field.fieldKind === 'map') {
		if (leaf.entryKey) return mapKeyKind(field);
		return keyed ? mapValueKind(field) : 'map';
	}
	if (field.fieldKind === 'list') return 'list';
	return elementKind(field);
}

/** Classifies a map's VALUE type — what one addressed entry yields. */
function mapValueKind(field: Extract<DescField, { fieldKind: 'map' }>): LeafKind {
	if (field.mapKind === 'message') return 'other';
	if (field.mapKind === 'enum') return 'number';
	return scalarKind(field.scalar, field);
}

/**
 * Classifies a field's singular value — for a repeated field, its ELEMENT.
 * Classification follows the PROTO type, not the JS representation, so it
 * agrees with the Go compiler even where protobuf-es would surface a value
 * differently.
 */
export function elementKind(field: DescField): LeafKind {
	if (field.fieldKind === 'message' || field.fieldKind === 'map') return 'other';
	if (field.fieldKind === 'enum') return 'number';
	if (field.fieldKind === 'list') {
		if (field.listKind === 'message') return 'other';
		if (field.listKind === 'enum') return 'number';
		return scalarKind(field.scalar, field);
	}
	return scalarKind(field.scalar, field);
}

function scalarKind(scalar: ScalarType, field: DescField): LeafKind {
	switch (scalar) {
		case ScalarType.STRING:
			return 'string';
		case ScalarType.BOOL:
			return 'bool';
		case ScalarType.INT64:
		case ScalarType.UINT64:
		case ScalarType.SINT64:
		case ScalarType.FIXED64:
		case ScalarType.SFIXED64:
			return 'bigint';
		case ScalarType.BYTES:
			// Collaborative text compares as its visible plain string, so string
			// ops and ordering see what the user sees.
			return isTextField(field) ? 'string' : 'other';
		default:
			return 'number';
	}
}

/** Rejects a leaf that can never take part in a scalar comparison. */
export function requireComparable(path: Path, leaf: LeafRef): void {
	if (isComparable(leafKind(leaf))) return;
	throw new Error(
		`query path ${path.join('.')}: ${describeLeaf(leaf)} is not comparable; ${
			comparableHint(path, leaf)
		}`,
	);
}

/** Names the way to ask the question that was meant, per leaf kind. */
function comparableHint(path: Path, leaf: LeafRef): string {
	const { field } = leaf;
	if (field.fieldKind === 'map' && !leaf.keyed) {
		// A whole map compares against nothing, and isSet on one is
		// constitutionally true (implicit presence), so neither is worth
		// offering. What was meant is almost always one ENTRY — by key if the
		// caller knows which, with any() if the question is which one.
		return `address one entry by key instead, e.g. ${path.join('.')}.<key>, ask about any entry `
			+ `with ${path.join('.')}.any((e) => …), or use isEmpty`;
	}
	if (field.fieldKind === 'list') {
		if (field.listKind === 'message') return `use isEmpty, or ${path.join('.')}.any((e) => …)`;
		return 'use has/hasAny/hasAll/isEmpty';
	}
	if (field.fieldKind === 'message') {
		const refKeys = refKeyFields(field);
		if (refKeys.length === 1) {
			return `use isSet/isNotSet, or compare its key field ${path.join('.')}.${refKeys[0]!.localName}`;
		}
		if (refKeys.length > 1) {
			return 'use isSet/isNotSet; a composite-key reference has no single comparable value';
		}
		return 'use isSet/isNotSet, or compare a scalar field inside it';
	}
	return 'use isSet/isNotSet';
}

/** Names what a resolved path ends at, the way the schema author wrote it, so
 * the error is recognizable against the .proto. */
function describeLeaf(leaf: LeafRef): string {
	const field = leaf.field;
	if (leaf.entryKey && field.fieldKind === 'map') {
		return `the ${scalarName(field.mapKey, field)} key of map field ${field.name}`;
	}
	if (leaf.keyed && field.fieldKind === 'map') {
		return `the ${mapValueName(field)} value of map field ${field.name}`;
	}
	switch (field.fieldKind) {
		case 'map':
			return `map field ${field.name}`;
		case 'list':
			return `repeated ${listElementName(field)} field ${field.name}`;
		case 'message':
			return `${field.message.typeName} field ${field.name}`;
		case 'enum':
			return `${field.enum.typeName} field ${field.name}`;
		default:
			return `${scalarName(field.scalar, field)} field ${field.name}`;
	}
}

/**
 * Rejects a map-entry key that its key type could never hold. Without it the
 * path resolves cleanly and then matches nothing, since no stored entry can be
 * keyed by an unparseable value — and the Go compiler rejects it, so accepting
 * one here would put a query on the wire that fails at the server instead.
 *
 * A STRING key admits anything, which is why it is not checked.
 */
function requireMapKey(path: Path, keyType: ScalarType, seg: string): void {
	if (keyType === ScalarType.STRING) return;
	const ok = keyType === ScalarType.BOOL
		? seg === 'true' || seg === 'false'
		: /^-?\d+$/.test(seg);
	if (ok) return;
	throw new Error(
		`query path ${path.join('.')}: invalid ${
			ScalarType[keyType]?.toLowerCase() ?? 'scalar'
		} map key "${seg}"`,
	);
}

function mapValueName(field: Extract<DescField, { fieldKind: 'map' }>): string {
	if (field.mapKind === 'message') return field.message.typeName;
	if (field.mapKind === 'enum') return field.enum.typeName;
	return scalarName(field.scalar, field);
}

function listElementName(field: Extract<DescField, { fieldKind: 'list' }>): string {
	if (field.listKind === 'message') return field.message.typeName;
	if (field.listKind === 'enum') return field.enum.typeName;
	return scalarName(field.scalar, field);
}

function scalarName(scalar: ScalarType, field: DescField): string {
	if (scalar === ScalarType.BYTES && isTextField(field)) return 'text';
	return ScalarType[scalar]?.toLowerCase() ?? 'scalar';
}

export interface ResolvedLeaf extends LeafRef {
	/** localName segments — the form the local evaluator resolves against. */
	path: Path;
	/** The same segments as PROTO field names, which is the form the wire and
	 * anything shared with the Go side speak. */
	protoPath: Path;
	/** The positions in `path` the component fans out over, outermost first.
	 * Only ever populated when `throughLists` was asked for — index components
	 * are the callers. */
	fans: FanStep[];
	/**
	 * True when the path ended at the `key` selector of a map fan, so what the
	 * component stores is the entry's KEY rather than anything below it. Index
	 * components only; a condition path names one entry by key instead.
	 */
	entryKey: boolean;
}

/** One scope an index component fans out over: a repeated field, or a map
 * written `*` in the declared path. */
export interface FanStep {
	/** Position in the resolved path of the collection itself. */
	at: number;
	isMap: boolean;
	/** Payload shape of a map's KEY, which is what the entry-key component
	 * stores and what orders one row's entries. Undefined for a list. */
	keyKind?: LeafKind;
}

/** The segment that means "every entry of this map" in an index component's
 * path. No proto identifier is `*`, so it can never collide with a field name —
 * and it is only special here: in a CONDITION path the segment after a map is an
 * entry key, and `*` would simply be that key. */
export const MAP_FAN_SEGMENT = '*';

/**
 * The selectors that may follow a `*`: which side of the map entry the
 * component stores. They are the same two names the entry scope of an `any()`
 * over a map offers, so one concept has one spelling in a declaration and in a
 * query.
 */
export const MAP_KEY_SEGMENT = 'key';
export const MAP_VALUE_SEGMENT = 'value';

/** The payload shape of a map's key. */
function mapKeyKind(field: Extract<DescField, { fieldKind: 'map' }>): LeafKind {
	return scalarKind(field.mapKey, field);
}

/**
 * Resolves a localName path to its leaf.
 *
 * `appendKey` extends a path ending at a single-key reference with that key
 * field, so a comparison on a reference lands on the value the row stores.
 *
 * `throughLists` switches the whole resolver into INDEX-COMPONENT mode, where a
 * path names a shape rather than one row's data. Two things change with it:
 *
 *   - a repeated field may be descended through (and ended at), reported in
 *     `fans` — a condition path that ran through a list would have no single
 *     value, and "some element" is spelled with an exists condition instead; and
 *   - a MAP must be followed by `*` and then by `key` or `value`, and becomes a
 *     fan too, where a condition path takes the next segment as one entry's key.
 *     The selector is required rather than defaulted because a bare trailing `*`
 *     would be genuinely ambiguous — for `map<string, string> attrs`, `attrs.*`
 *     could mean either side of the entry.
 */
export function resolveLeafRef(
	schema: DescMessage,
	path: Path,
	appendKey: boolean,
	throughLists = false,
): ResolvedLeaf {
	let desc: DescMessage | undefined = schema;
	const out: string[] = [];
	const protoOut: string[] = [];
	let leaf: DescField | undefined;
	let keyed = false;
	let entryKey = false;
	const fans: FanStep[] = [];
	// Set on the step after a map field: the next segment is the entry KEY.
	// Carries the value's message type when there is one, so a path may continue
	// into it — `custom.priority.number` is three steps, and only the first is a
	// field lookup.
	let mapAt: { keyType: ScalarType; valueOf: DescMessage | undefined } | undefined;
	// The group of a oneof segment consumed on the previous step. Reading is
	// member-granular — "which case is active" is just "is this member set" —
	// so the group contributes no segment of its own and both forms resolve to
	// the same leaf: the typed DSL writes `w.choice.choiceText` (the group is a
	// property on the message shape), while an index spec read off a descriptor
	// names `choice_text` on its own.
	let group: DescOneof | undefined;
	for (let i = 0; i < path.length; i++) {
		const seg = path[i]!;
		if (mapAt) {
			// An entry key, not a field name: there is no descriptor to look it
			// up in and no case convention to translate, so it passes through
			// verbatim in both forms.
			requireMapKey(path, mapAt.keyType, seg);
			out.push(seg);
			protoOut.push(seg);
			keyed = true;
			desc = mapAt.valueOf;
			mapAt = undefined;
			continue;
		}
		if (desc === undefined) {
			throw new Error(
				`query path ${path.join('.')}: "${
					path[i - 1]
				}" is not a message field, cannot descend into "${seg}"`,
			);
		}
		const field: DescField | undefined = desc.fields.find((f) => f.localName === seg);
		if (!field) {
			const oneof = group ? undefined : desc.oneofs.find((o) => o.localName === seg);
			if (oneof) {
				if (i === path.length - 1) {
					throw new Error(
						`query path ${path.join('.')}: "${seg}" is a oneof, not a field; name the member `
							+ `to test (${oneof.fields.map((f) => f.localName).join(', ')})`,
					);
				}
				group = oneof;
				continue;
			}
			throw new Error(`query path ${path.join('.')}: ${desc.typeName} has no field "${seg}"`);
		}
		if (group && field.oneof?.localName !== group.localName) {
			throw new Error(
				`query path ${path.join('.')}: "${seg}" is not a member of oneof "${group.name}"`,
			);
		}
		group = undefined;
		out.push(seg);
		protoOut.push(field.name);
		leaf = field;
		keyed = false;
		entryKey = false;
		if (field.fieldKind === 'map') {
			if (throughLists) {
				// An index component fans out over EVERY entry, and has to say so:
				// a literal key would declare an index over a single entry, which
				// is a different feature. Neither the `*` nor the selector after it
				// becomes a path segment — the path stays the chain of FIELDS, and
				// `fans` plus `entryKey` carry the shape.
				const shapes = `${seg}.${MAP_FAN_SEGMENT}.${MAP_KEY_SEGMENT} or `
					+ `${seg}.${MAP_FAN_SEGMENT}.${MAP_VALUE_SEGMENT}.<field>`;
				if (path[i + 1] !== MAP_FAN_SEGMENT) {
					throw new Error(
						`index path ${path.join('.')}: map field "${seg}" must be followed by `
							+ `"${MAP_FAN_SEGMENT}"; an index over a map fans out over every entry, so `
							+ `write ${shapes}`,
					);
				}
				const selector = path[i + 2];
				if (selector === undefined) {
					throw new Error(
						`index path ${path.join('.')}: "${MAP_FAN_SEGMENT}" must be followed by `
							+ `"${MAP_KEY_SEGMENT}" or "${MAP_VALUE_SEGMENT}"; a bare "${MAP_FAN_SEGMENT}" `
							+ `does not say which side of the entry the component stores, so write ${shapes}`,
					);
				}
				fans.push({ at: out.length - 1, isMap: true, keyKind: mapKeyKind(field) });
				if (selector === MAP_KEY_SEGMENT) {
					if (i + 3 < path.length) {
						throw new Error(
							`index path ${path.join('.')}: "${MAP_KEY_SEGMENT}" is the entry key of map `
								+ `field "${seg}", a scalar, so "${path[i + 3]}" cannot follow it`,
						);
					}
					entryKey = true;
					i += 2;
					desc = undefined;
					continue;
				}
				if (selector !== MAP_VALUE_SEGMENT) {
					throw new Error(
						`index path ${path.join('.')}: "${selector}" after "${MAP_FAN_SEGMENT}" must be `
							+ `"${MAP_KEY_SEGMENT}" or "${MAP_VALUE_SEGMENT}", naming the side of the entry `
							+ `the component stores; write ${shapes}`,
					);
				}
				if (field.mapKind !== 'message' && i + 3 < path.length) {
					throw new Error(
						`index path ${path.join('.')}: map field "${seg}" has a scalar value, so `
							+ `"${MAP_VALUE_SEGMENT}" cannot be descended into`,
					);
				}
				i += 2;
				desc = field.mapKind === 'message' ? field.message : undefined;
				continue;
			}
			if (i < path.length - 1) {
				mapAt = {
					keyType: field.mapKey,
					valueOf: field.mapKind === 'message' ? field.message : undefined,
				};
				desc = undefined;
				continue;
			}
		}
		const refKeys = refKeyFields(field);
		const isLast = i === path.length - 1;
		if (!isLast && refKeys.length > 0) {
			const next = path[i + 1];
			if (!refKeys.some((kf) => kf.localName === next)) {
				throw new Error(
					`query path ${
						path.join('.')
					}: cannot descend through reference "${seg}" into "${next}"; only its key fields are addressable`,
				);
			}
		}
		if (field.fieldKind === 'list' && throughLists) {
			// A `*` nests inside ONE entry of the map above it, so it composes
			// with a list. Two lists do not: the second would have to be crossed
			// with the first, pairing values that never belonged together.
			if (fans.some((f) => !f.isMap)) {
				throw new Error(
					`query path ${
						path.join('.')
					}: "${seg}" is a second repeated field on one index component; index a path through one list at a time`,
				);
			}
			fans.push({ at: out.length - 1, isMap: false });
		}
		if (isLast && appendKey && refKeys.length > 0) {
			if (field.fieldKind !== 'message' && !(field.fieldKind === 'list' && throughLists)) {
				throw new Error(`query path ${path.join('.')}: repeated reference "${seg}" is not comparable`);
			}
			if (refKeys.length !== 1) {
				throw new Error(
					`query path ${
						path.join('.')
					}: reference "${seg}" has a composite key; only isSet/isNotSet is supported`,
				);
			}
			out.push(refKeys[0]!.localName);
			protoOut.push(refKeys[0]!.name);
			leaf = refKeys[0]!;
		}
		desc = field.fieldKind === 'message'
			? field.message
			: throughLists && field.fieldKind === 'list' && field.listKind === 'message'
			? field.message
			: undefined;
	}
	if (!leaf) throw new Error('query path is empty');
	if (mapAt) {
		// The path ended ON the key step, which only happens for a trailing
		// segment the loop never got to consume — unreachable, since a map field
		// only sets mapAt when a segment follows it.
		throw new Error(`query path ${path.join('.')}: map entry key is missing`);
	}
	return { path: out, protoPath: protoOut, field: leaf, keyed, entryKey, fans };
}

/**
 * The inverse of the wire translation: PROTO field-name segments to the
 * localName segments everything local resolves against.
 *
 * Index specs are the callers. They are read off a descriptor (or stated in a
 * .proto), so they speak proto names, while `resolveLeafRef` and the evaluator
 * look fields up by localName — for `sort_key` those differ, and matching by the
 * wrong one finds nothing at all. It descends through repeated message fields
 * for the same reason `resolveLeafRef` does with `throughLists`: an index
 * component may run through a list, so translating its path must too.
 *
 * `*` and the `key`/`value` selector after it are not field names, so they have
 * no case convention to translate and pass through verbatim. Validating them is
 * `resolveLeafRef`'s job, which runs on the result — this only has to consume
 * them so the segment after a `value` is looked up on the map's VALUE type
 * rather than on the selector.
 */
export function localPathOf(schema: DescMessage, path: Path): Path {
	let desc: DescMessage | undefined = schema;
	const out: Path = [];
	for (let i = 0; i < path.length; i++) {
		const seg = path[i]!;
		if (desc === undefined) {
			// Nothing left to look the segment up in, so there is no localName to
			// translate it to and passing it through verbatim is the only correct
			// answer. Whether the path is legal at all is resolveLeafRef's call,
			// and it runs on this result — reporting here would only mean a worse
			// message for the same mistake.
			out.push(seg);
			continue;
		}
		const field: DescField | undefined = desc.fields.find((f) => f.name === seg);
		if (!field) {
			throw new Error(`query path ${path.join('.')}: ${desc.typeName} has no field "${seg}"`);
		}
		out.push(field.localName);
		if (field.fieldKind === 'map' && i < path.length - 1) {
			const valueOf: DescMessage | undefined = field.mapKind === 'message'
				? field.message
				: undefined;
			if (path[i + 1] === MAP_FAN_SEGMENT) {
				// An index fan: `*` then `key` or `value`. Both travel verbatim,
				// and only `value` continues into the entry's type.
				out.push(path[i + 1]!);
				const selector = path[i + 2];
				if (selector === undefined) {
					desc = undefined;
					i += 1;
					continue;
				}
				out.push(selector);
				desc = selector === MAP_VALUE_SEGMENT ? valueOf : undefined;
				i += 2;
				continue;
			}
			// A literal entry key: the same string in both forms, since there is
			// no field to look it up on.
			out.push(path[i + 1]!);
			desc = valueOf;
			i += 1;
			continue;
		}
		desc = field.fieldKind === 'message'
			? field.message
			: field.fieldKind === 'list' && field.listKind === 'message'
			? field.message
			: undefined;
	}
	return out;
}

function translatePath(scope: CondScope, path: Path): Path {
	return translatePathLeaf(scope, path).path;
}

/**
 * translatePath, plus the leaf's descriptor — which `exists` needs to translate
 * its sub-condition against the element scope.
 *
 * Inside a map ENTRY the first segment is `key` or `value`, neither of which is
 * a field name, so both travel verbatim and only `value` continues into the
 * map's value type.
 */
function translatePathLeaf(scope: CondScope, path: Path): { path: Path; field: DescField } {
	if (scope.kind === 'entry') {
		const [ head, ...rest ] = path;
		if (head === ENTRY_KEY || rest.length === 0 || scope.map.mapKind !== 'message') {
			return { path: [ head! ], field: scope.map };
		}
		const inner = translatePathLeaf(messageScope(scope.map.message), rest);
		return { path: [ ENTRY_VALUE, ...inner.path ], field: inner.field };
	}
	let desc: DescMessage | undefined = scope.schema;
	let leaf: DescField | undefined;
	let mapAt: { valueOf: DescMessage | undefined } | undefined;
	const out = path.map((seg, i) => {
		if (mapAt) {
			// An entry key: the same string in both forms, so it travels verbatim.
			desc = mapAt.valueOf;
			mapAt = undefined;
			return seg;
		}
		if (desc === undefined) {
			throw new Error(
				`query path ${path.join('.')}: "${
					path[i - 1]
				}" is not a message field, cannot descend into "${seg}"`,
			);
		}
		const field = desc.fields.find((f) => f.localName === seg);
		if (!field) {
			throw new Error(`query path ${path.join('.')}: ${desc.typeName} has no field "${seg}"`);
		}
		leaf = field;
		if (field.fieldKind === 'map' && i < path.length - 1) {
			mapAt = { valueOf: field.mapKind === 'message' ? field.message : undefined };
			desc = undefined;
			return field.name;
		}
		desc = field.fieldKind === 'message' ? field.message : undefined;
		return field.name;
	});
	if (!leaf) throw new Error('query path is empty');
	return { path: out, field: leaf };
}
