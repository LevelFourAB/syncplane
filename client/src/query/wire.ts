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
	if (compiled.where) out.where = translateCond(schema, compiled.where);
	if (compiled.orderBy) {
		out.orderBy = compiled.orderBy.map(
			(key): OrderKey => ({ path: translatePath(schema, key.path), dir: key.dir }),
		);
	}
	if (compiled.limit !== undefined) out.limit = compiled.limit;
	if (compiled.offset !== undefined) out.offset = compiled.offset;
	return out;
}

function translateCond(schema: DescMessage, cond: Cond): Cond {
	switch (cond.kind) {
		case 'cmp':
		case 'in':
		case 'str':
		case 'present':
			return { ...cond, path: translatePath(schema, cond.path) };
		case 'rep':
			return { ...cond, path: translatePath(schema, cond.path) };
		case 'exists': {
			// The sub-condition's paths are relative to the ELEMENT, so they
			// translate against the element type rather than the row's.
			const leaf = translatePathLeaf(schema, cond.path);
			const element = messageListElement(leaf.field);
			if (!element) {
				throw new Error(
					`query path ${cond.path.join('.')}: any() needs a repeated message field`,
				);
			}
			return { kind: 'exists', path: leaf.path, cond: translateCond(element, cond.cond) };
		}
		case 'and':
		case 'or':
			return { ...cond, conds: cond.conds.map((c) => translateCond(schema, c)) };
		case 'not':
			return { ...cond, cond: translateCond(schema, cond.cond) };
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
	if (compiled.where) out.where = prepareCond(schema, compiled.where);
	if (compiled.orderBy) {
		out.orderBy = compiled.orderBy.map((key): OrderKey => {
			const leaf = resolveLeafRef(schema, key.path, true);
			// An incomparable order key ties every pair, so the rows would come
			// back in row-key order as if no ordering had been asked for.
			requireComparable(key.path, leaf.field);
			return { path: leaf.path, dir: key.dir };
		});
	}
	if (compiled.limit !== undefined) out.limit = compiled.limit;
	if (compiled.offset !== undefined) out.offset = compiled.offset;
	return out;
}

function prepareCond(schema: DescMessage, cond: Cond): Cond {
	switch (cond.kind) {
		case 'cmp':
		case 'in': {
			const leaf = resolveLeafRef(schema, cond.path, true);
			requireComparable(cond.path, leaf.field);
			return { ...cond, path: leaf.path };
		}
		case 'str': {
			const leaf = resolveLeafRef(schema, cond.path, true);
			if (leafKind(leaf.field) !== 'string') {
				throw new Error(
					`query path ${
						cond.path.join('.')
					}: startsWith/endsWith/contains need a string or text field, got ${describeLeaf(leaf.field)}`,
				);
			}
			return { ...cond, path: leaf.path };
		}
		case 'rep': {
			// rep operates on the list itself, so the path stays at the bare field.
			const leaf = resolveLeafRef(schema, cond.path, false);
			if (leafKind(leaf.field) !== 'list') {
				throw new Error(
					`query path ${cond.path.join('.')}: has/hasAny/hasAll/isEmpty need a repeated field, got ${
						describeLeaf(leaf.field)
					}`,
				);
			}
			// isEmpty asks about the list itself, so it works on any element type.
			if (cond.op === 'isEmpty') return { ...cond, path: leaf.path };
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
						describeLeaf(leaf.field)
					}; ${membershipHint(cond.path, leaf.field)}`,
				);
			}
			return { ...cond, path: leaf.path };
		}
		case 'exists': {
			const leaf = resolveLeafRef(schema, cond.path, false);
			const element = messageListElement(leaf.field);
			if (!element) {
				throw new Error(
					`query path ${cond.path.join('.')}: any() needs a repeated message field, got ${
						describeLeaf(leaf.field)
					}; ${existsHint(leaf.field)}`,
				);
			}
			// A reference element stores only the target's key fields, so the same
			// rule that stops a path descending through a reference stops a
			// sub-condition reaching past one: those fields are not there to match.
			const refKeys = refKeyFields(leaf.field);
			if (refKeys.length > 0) {
				requireRefKeyPaths(cond.path, refKeys.map((f) => f.localName), cond.cond);
			}
			return { kind: 'exists', path: leaf.path, cond: prepareCond(element, cond.cond) };
		}
		case 'present':
			// Presence works on every leaf, so nothing to check.
			return { ...cond, path: resolveLeafRef(schema, cond.path, false).path };
		case 'and':
		case 'or':
			return { ...cond, conds: cond.conds.map((c) => prepareCond(schema, c)) };
		case 'not':
			return { ...cond, cond: prepareCond(schema, cond.cond) };
	}
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
	return 'any() asks about the elements of a list';
}

/** The kinds a leaf value can take, mirroring the Go evaluator's rvKind. */
export type LeafKind = 'string' | 'number' | 'bigint' | 'bool' | 'list' | 'other';

function isComparable(kind: LeafKind): boolean {
	return kind === 'string' || kind === 'number' || kind === 'bigint' || kind === 'bool';
}

/**
 * Rejects a repeated leaf whose ELEMENTS could never take part in a comparison.
 * An index component ending at a list stores one entry per element, so it is
 * the element kind that has to be comparable, not the list's.
 */
export function requireComparableElement(path: Path, field: DescField): void {
	if (isComparable(elementKind(field))) return;
	throw new Error(
		`index path ${path.join('.')}: ${describeLeaf(field)} cannot be indexed; ${
			field.fieldKind === 'list' && field.listKind === 'message'
				? `index a scalar field of the elements instead, e.g. ${path.join('.')}.<field>`
				: 'its elements are not comparable'
		}`,
	);
}

export function leafKind(field: DescField): LeafKind {
	if (field.fieldKind === 'list') return 'list';
	if (field.fieldKind === 'map') return 'other';
	return elementKind(field);
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
export function requireComparable(path: Path, field: DescField): void {
	if (isComparable(leafKind(field))) return;
	throw new Error(
		`query path ${path.join('.')}: ${describeLeaf(field)} is not comparable; ${
			comparableHint(path, field)
		}`,
	);
}

/** Names the way to ask the question that was meant, per leaf kind. */
function comparableHint(path: Path, field: DescField): string {
	if (field.fieldKind === 'list') {
		if (field.listKind === 'message') return `use isEmpty, or ${path.join('.')}.any((e) => …)`;
		return 'use has/hasAny/hasAll/isEmpty';
	}
	// isSet on a map is constitutionally true (implicit presence), so offering
	// it would be a false lead.
	if (field.fieldKind === 'map') return 'map fields cannot be queried';
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

/** Names a field the way the schema author wrote it, so the error is
 * recognizable against the .proto. */
function describeLeaf(field: DescField): string {
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

function listElementName(field: Extract<DescField, { fieldKind: 'list' }>): string {
	if (field.listKind === 'message') return field.message.typeName;
	if (field.listKind === 'enum') return field.enum.typeName;
	return scalarName(field.scalar, field);
}

function scalarName(scalar: ScalarType, field: DescField): string {
	if (scalar === ScalarType.BYTES && isTextField(field)) return 'text';
	return ScalarType[scalar]?.toLowerCase() ?? 'scalar';
}

export interface ResolvedLeaf {
	/** localName segments — the form the local evaluator resolves against. */
	path: Path;
	/** The same segments as PROTO field names, which is the form the wire and
	 * anything shared with the Go side speak. */
	protoPath: Path;
	field: DescField;
	/** Position in `path` of the repeated field the leaf sits under, or -1 when
	 * the path yields one value per row. Only ever set when `throughLists` was
	 * asked for — index components are the callers. */
	multiAt: number;
}

/**
 * Resolves a localName path to its leaf.
 *
 * `appendKey` extends a path ending at a single-key reference with that key
 * field, so a comparison on a reference lands on the value the row stores.
 *
 * `throughLists` lets the path descend through a repeated message field (and
 * end at a repeated one), reporting where in `multiAt`. Only an index component
 * may: a condition path that ran through a list would have no single value, and
 * "some element" is spelled with an exists condition instead.
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
	let multiAt = -1;
	// The group of a oneof segment consumed on the previous step. Reading is
	// member-granular — "which case is active" is just "is this member set" —
	// so the group contributes no segment of its own and both forms resolve to
	// the same leaf: the typed DSL writes `w.choice.choiceText` (the group is a
	// property on the message shape), while an index spec read off a descriptor
	// names `choice_text` on its own.
	let group: DescOneof | undefined;
	for (let i = 0; i < path.length; i++) {
		const seg = path[i]!;
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
			if (multiAt >= 0) {
				throw new Error(
					`query path ${
						path.join('.')
					}: "${seg}" is a second repeated field on one index component; index a path through one list at a time`,
				);
			}
			multiAt = out.length - 1;
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
	return { path: out, protoPath: protoOut, field: leaf, multiAt };
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
 */
export function localPathOf(schema: DescMessage, path: Path): Path {
	let desc: DescMessage | undefined = schema;
	return path.map((seg, i) => {
		if (desc === undefined) {
			throw new Error(
				`query path ${path.join('.')}: "${
					path[i - 1]
				}" is not a message field, cannot descend into "${seg}"`,
			);
		}
		const field = desc.fields.find((f) => f.name === seg);
		if (!field) {
			throw new Error(`query path ${path.join('.')}: ${desc.typeName} has no field "${seg}"`);
		}
		desc = field.fieldKind === 'message'
			? field.message
			: field.fieldKind === 'list' && field.listKind === 'message'
			? field.message
			: undefined;
		return field.localName;
	});
}

function translatePath(schema: DescMessage, path: Path): Path {
	return translatePathLeaf(schema, path).path;
}

/** translatePath, plus the leaf's descriptor — which `exists` needs to
 * translate its sub-condition against the element type. */
function translatePathLeaf(schema: DescMessage, path: Path): { path: Path; field: DescField } {
	let desc: DescMessage | undefined = schema;
	let leaf: DescField | undefined;
	const out = path.map((seg, i) => {
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
		desc = field.fieldKind === 'message' ? field.message : undefined;
		leaf = field;
		return field.name;
	});
	if (!leaf) throw new Error('query path is empty');
	return { path: out, field: leaf };
}

/** The element type of a repeated message field, or undefined for anything
 * else. `exists` is only legal over the former, which prepareQuery enforces —
 * this is the accessor both it and the wire translation share. */
function messageListElement(field: DescField): DescMessage | undefined {
	return field.fieldKind === 'list' && field.listKind === 'message' ? field.message : undefined;
}
