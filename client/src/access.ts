/**
 * Client-side predictions of the access model: whether the caller holds a
 * permission at a row, and whether an entity's access rules admit an
 * operation, from the CallerAccess the server last sent for the workspace.
 * The server decides every read and write itself; these answers only let an
 * app shape its UI before it asks.
 */

import {
	create,
	type DescField,
	type DescMessage,
	fromBinary,
	type Message,
	toBinary,
} from '@bufbuild/protobuf';

import {
	type CrudMutation,
	CrudOp,
	type CustomMutation,
	type Mutation,
} from './gen/syncplane/v1/mutation_pb.js';
import type { Condition } from './gen/syncplane/v1/query_pb.js';
import {
	type AnchorGroup,
	AnchorGroupSchema,
	type AnchorRows,
	AnchorRowsSchema,
	type CallerAccess,
	type CallerAccessDelta,
	CallerAccessSchema,
} from './gen/syncplane/v1/service_pb.js';
import {
	type AccessRuleDef,
	type EntityDef,
	entityDefOf,
	entityOptionsOf,
	type EntityRegistry,
	fieldValueOf,
	type GrantSpec,
	type GuardDef,
	keyOf,
	refShapes,
	refStub,
	type WithinDef,
} from './mutations.js';
import type { Cond } from './query/ast.js';
import { evalCond } from './query/evaluate.js';
import { condFromProto } from './query/proto.js';
import { condLocalNames, prepareQuery } from './query/wire.js';
import type { TouchSet } from './touch.js';

/**
 * A prediction about the caller's access: `'yes'`, `'no'`, or `'unknown'`
 * when the client cannot tell. {@link CanDetail} also gives the
 * {@link UnknownReason}.
 */
export type CanAnswer = 'yes' | 'no' | 'unknown';

/**
 * Why a prediction is `'unknown'`. When more than one applies, the answer
 * gives one of them.
 *
 * - `'loading'`: the hook has no answer yet. Only the React hooks give it.
 * - `'no_summary'`: no summary of the caller's access has arrived for the
 *   workspace.
 * - `'pending_grant'`: a local write the server has not acked can change the
 *   caller's grants: a write to a grant row, a delete of a row that other
 *   rows sit below, or a custom mutation in a schema with grant entities.
 * - `'row_not_held'`: a row the answer depends on is not held locally: the
 *   row itself, a row up its chain of access parents, or a row a `within`
 *   entry reads.
 * - `'new_value'`: the update changes a field a `callerIs` check reads,
 *   the access parent, or a field a condition reads, and the new value
 *   decides.
 * - `'chain_too_deep'`: the chain of access parents is longer than the
 *   server's `MaxAccessDepth`, which the summary carries, or
 *   {@link MAX_ACCESS_CHAIN} rows when it does not.
 * - `'error'`: the read of the local store failed. Only the React hooks give
 *   it; the client methods reject.
 */
export type UnknownReason =
	| 'loading'
	| 'no_summary'
	| 'pending_grant'
	| 'row_not_held'
	| 'new_value'
	| 'chain_too_deep'
	| 'error';

/**
 * A prediction with the reason for an `'unknown'` answer. Equal answers are
 * the same object, so a hook that returns one keeps a stable reference.
 */
export type CanDetail =
	| { readonly answer: 'yes' | 'no'; readonly reason?: undefined }
	| UnknownDetail;

/** An `'unknown'` prediction and why it is unknown. */
export interface UnknownDetail {
	readonly answer: 'unknown';
	readonly reason: UnknownReason;
}

/** A prediction as the decision functions give it: an unknown one says why. */
export type Verdict = 'yes' | 'no' | UnknownDetail;

const YES_DETAIL: CanDetail = Object.freeze({ answer: 'yes' });
const NO_DETAIL: CanDetail = Object.freeze({ answer: 'no' });
const unknowns = new Map<UnknownReason, UnknownDetail>();

/** The unknown verdict for `reason`; equal reasons give the same object. */
export function unknownBecause(reason: UnknownReason): UnknownDetail {
	let out = unknowns.get(reason);
	if (out === undefined) unknowns.set(reason, out = Object.freeze({ answer: 'unknown', reason }));
	return out;
}

/** The plain answer of `verdict`. */
export function answerOf(verdict: Verdict): CanAnswer {
	return typeof verdict === 'string' ? verdict : 'unknown';
}

/** `verdict` as a {@link CanDetail}; equal verdicts give the same object. */
export function detailOf(verdict: Verdict): CanDetail {
	return verdict === 'yes' ? YES_DETAIL : verdict === 'no' ? NO_DETAIL : verdict;
}

/** An operation on a row that an entity's access rules decide. */
export type AccessOperation = 'read' | 'create' | 'update' | 'delete';

/**
 * How many rows up a row's chains of access parents `can` walks on each path
 * before it answers `'unknown'`, when the summary does not carry the server's
 * `MaxAccessDepth`: the server's default.
 */
export const MAX_ACCESS_CHAIN = 32;

/**
 * The entity name under which a commit announces, in the touch tokens that
 * reach watchers and other tabs, a change of what the client can tell about
 * the caller's grants: a write of the stored summary, or a write that
 * {@link pendingChangesAccess} counts entering or leaving the pending queue.
 * Not an identifier, so it never names a real entity.
 */
export const ACCESS_TOUCH_ENTITY = '#access';

/** A CallerAccess indexed for lookups. */
export class AccessSummary {
	private readonly roots: ReadonlySet<string>;
	/** The permissions of each anchor group, by its index in the message. */
	private readonly groups: readonly ReadonlySet<string>[];
	/** The index of the anchor group of each row, by entity and key. */
	private readonly rows = new Map<string, Map<string, number>>();
	/** The permissions held at one or more rows of each entity, by entity. */
	private readonly entities = new Map<string, Set<string>>();
	/** The caller's subjects, the primary subject first. */
	readonly subjects: readonly string[];
	/**
	 * How many access parent hops above a row the server follows: its
	 * `MaxAccessDepth`, or {@link MAX_ACCESS_CHAIN} for a summary that does
	 * not carry it.
	 */
	readonly maxAccessDepth: number;
	/**
	 * Whether the caller holds no permission, at the root or at any row. In a
	 * schema that declares a role, the server then refuses every request in
	 * the workspace.
	 */
	readonly holdsNothing: boolean;

	constructor(access: CallerAccess) {
		this.subjects = access.subjects;
		this.maxAccessDepth = access.maxAccessDepth > 0 ? access.maxAccessDepth : MAX_ACCESS_CHAIN;
		this.roots = new Set(access.rootPermissions);
		this.groups = access.anchorGroups.map((g) => new Set(g.permissions));
		// Nested maps and not one map of entity and key joined: a joined string
		// costs an allocation for each key here and for each lookup in holds,
		// and the keys the decode made can go in as they are.
		for (let i = 0; i < access.anchorGroups.length; i++) {
			for (const a of access.anchorGroups[i]!.anchors) {
				let keys = this.rows.get(a.entity);
				if (keys === undefined) this.rows.set(a.entity, keys = new Map());
				for (const key of a.keys) keys.set(key, i);
				if (a.keys.length === 0) continue;
				let held = this.entities.get(a.entity);
				if (held === undefined) this.entities.set(a.entity, held = new Set());
				for (const permission of this.groups[i]!) held.add(permission);
			}
		}
		this.holdsNothing = this.roots.size === 0 && this.entities.size === 0;
	}

	/** Whether the caller holds `permission` at the workspace root. */
	holdsRoot(permission: string): boolean {
		return this.roots.has(permission);
	}

	/**
	 * Whether the caller holds `permission` at the row of `entity` with `key`
	 * itself, or at the root.
	 */
	holds(permission: string, entity: string, key: string): boolean {
		if (this.roots.has(permission)) return true;
		const group = this.rows.get(entity)?.get(key);
		return group !== undefined && this.groups[group]!.has(permission);
	}

	/**
	 * Whether the caller holds `permission` at the root, or at one or more
	 * rows of an entity in `entities`.
	 */
	holdsAtAny(permission: string, entities: Iterable<string>): boolean {
		if (this.roots.has(permission)) return true;
		for (const entity of entities) {
			if (this.entities.get(entity)?.has(permission)) return true;
		}
		return false;
	}
}

/** An encoded CallerAccess and the {@link AccessSummary} of it. */
export interface DecodedAccess {
	readonly raw: Uint8Array;
	/** CallerAccess.version; 0 for a summary from a server that sends none. */
	readonly version: bigint;
	readonly summary: AccessSummary;
}

/**
 * Encodes `access` to store it. The result's summary is made from `access`
 * when it is first read, with no decode of `raw`, so do not change `access`
 * or `raw` after the call. Given to {@link decodeAccess} as `last`, the
 * result comes back for equal bytes with no decode.
 */
export function encodeAccess(access: CallerAccess): DecodedAccess {
	return new EncodedAccess(toBinary(CallerAccessSchema, access), access);
}

class EncodedAccess implements DecodedAccess {
	private access: CallerAccess | undefined;
	private built: AccessSummary | undefined;
	readonly version: bigint;

	constructor(readonly raw: Uint8Array, access: CallerAccess) {
		this.access = access;
		this.version = access.version;
	}

	get summary(): AccessSummary {
		if (this.built === undefined) {
			this.built = new AccessSummary(this.access!);
			// The summary keeps what it reads of the message, so the message
			// need not stay in memory.
			this.access = undefined;
		}
		return this.built;
	}
}

/**
 * Decodes the CallerAccess encoded as `raw`, or returns `last` when
 * `last.raw` holds the same bytes as `raw`. The result keeps `raw`, so do not
 * change `raw` after the call. Throws what `fromBinary` throws for bytes that
 * are not a CallerAccess.
 *
 * Cost when it returns `last`: one pass over the bytes and no decode.
 */
export function decodeAccess(raw: Uint8Array, last: DecodedAccess | undefined): DecodedAccess {
	if (last !== undefined && sameBytes(last.raw, raw)) return last;
	const access = fromBinary(CallerAccessSchema, raw);
	return { raw, version: access.version, summary: new AccessSummary(access) };
}

/**
 * Applies `delta` to `held`, as CallerAccessDelta documents it, and returns
 * the new summary with the delta's seq and version. `held` is not changed.
 * The caller checks that `held.version` is `delta.baseVersion`: the result
 * is wrong for any other summary.
 *
 * The result lists its groups and rows in the order CallerAccess documents,
 * so a summary equal to one the server sends whole encodes to the same
 * bytes.
 */
export function applyAccessDelta(held: CallerAccess, delta: CallerAccessDelta): CallerAccess {
	const rows = new Map<string, AnchoredRow>();
	const put = (groups: AnchorGroup[]) => {
		for (const group of groups) {
			for (const anchors of group.anchors) {
				for (const key of anchors.keys) {
					rows.set(`${anchors.entity}\u0000${key}`, {
						entity: anchors.entity,
						key,
						permissions: group.permissions,
					});
				}
			}
		}
	};
	put(held.anchorGroups);
	put(delta.anchorGroups);
	for (const anchors of delta.removed) {
		for (const key of anchors.keys) rows.delete(`${anchors.entity}\u0000${key}`);
	}
	return create(CallerAccessSchema, {
		seq: delta.seq,
		subjects: held.subjects,
		rootPermissions: held.rootPermissions,
		anchorGroups: anchorGroupsOf(rows.values()),
		version: delta.version,
		maxAccessDepth: held.maxAccessDepth,
	});
}

/** One row and the permissions the caller holds there. */
export interface AnchoredRow {
	readonly entity: string;
	readonly key: string;
	/** Sorted by {@link compareUtf8}, without duplicates. */
	readonly permissions: readonly string[];
}

/**
 * Groups `rows` by their permissions, ordered as CallerAccess.anchor_groups
 * documents, so the groups encode to the bytes the server sends for the same
 * rows. Each row is listed once.
 */
export function anchorGroupsOf(rows: Iterable<AnchoredRow>): AnchorGroup[] {
	const groups = new Map<string, { permissions: readonly string[]; rows: AnchoredRow[] }>();
	for (const row of rows) {
		const id = row.permissions.join('\u0000');
		let group = groups.get(id);
		if (group === undefined) {
			group = { permissions: row.permissions, rows: [] };
			groups.set(id, group);
		}
		group.rows.push(row);
	}
	return [ ...groups.values() ]
		.sort((a, b) => comparePermissions(a.permissions, b.permissions))
		.map((group) =>
			create(AnchorGroupSchema, {
				permissions: [ ...group.permissions ],
				anchors: rowsByEntity(group.rows),
			})
		);
}

/** Orders permission lists element by element, a list before the longer lists it begins. */
function comparePermissions(a: readonly string[], b: readonly string[]): number {
	for (let i = 0; i < a.length && i < b.length; i++) {
		if (a[i] !== b[i]) return compareUtf8(a[i]!, b[i]!);
	}
	return a.length - b.length;
}

/** Orders strings by their UTF-8 bytes, which is how the server sorts. */
export function compareUtf8(a: string, b: string): number {
	const n = Math.min(a.length, b.length);
	for (let i = 0; i < n; i++) {
		const x = a.charCodeAt(i);
		const y = b.charCodeAt(i);
		if (x !== y) return utf8Rank(x) - utf8Rank(y);
	}
	return a.length - b.length;
}

/**
 * Maps a UTF-16 code unit to a rank in UTF-8 byte order. Code units order as
 * UTF-8 does, except that a surrogate stands for a code point above U+FFFF,
 * which sorts after U+E000 to U+FFFF. Comparing ranks does not allocate, as
 * encoding each key would, and a sort compares each key many times.
 */
function utf8Rank(unit: number): number {
	if (unit >= 0xd800 && unit <= 0xdfff) return unit + 0x2000;
	if (unit >= 0xe000) return unit - 0x800;
	return unit;
}

function rowsByEntity(rows: AnchoredRow[]): AnchorRows[] {
	rows.sort((a, b) => compareUtf8(a.entity, b.entity) || compareUtf8(a.key, b.key));
	const out: AnchorRows[] = [];
	for (const row of rows) {
		const last = out[out.length - 1];
		if (last !== undefined && last.entity === row.entity) {
			last.keys.push(row.key);
		} else {
			out.push(create(AnchorRowsSchema, { entity: row.entity, keys: [ row.key ] }));
		}
	}
	return out;
}

function sameBytes(a: Uint8Array, b: Uint8Array): boolean {
	if (a === b) return true;
	const n = a.byteLength;
	if (n !== b.byteLength) return false;
	let i = 0;
	// Four bytes at a time when the two views start on a word boundary. This
	// is approximately five times faster than a byte loop, and the cost is
	// important: a CallerAccess with thousands of anchors is more than 100 KB,
	// and over a store that keeps no generations, the prediction after each
	// local write compares it again.
	if (a.byteOffset % 4 === 0 && b.byteOffset % 4 === 0) {
		const words = n >>> 2;
		const wa = new Uint32Array(a.buffer, a.byteOffset, words);
		const wb = new Uint32Array(b.buffer, b.byteOffset, words);
		for (; i < words; i++) {
			if (wa[i] !== wb[i]) return false;
		}
		i = words << 2;
	}
	for (; i < n; i++) {
		if (a[i] !== b[i]) return false;
	}
	return true;
}

/**
 * One row of an operation, as {@link decideRules} reads it: whether the
 * caller holds each permission at the row or up its chain, the value of each
 * field a `callerIs` check reads, an {@link UnknownDetail} where the client
 * cannot know it, whether the row matches a rule's `when` condition, and
 * whether it sits within rows that pass a rule's `within` entries.
 */
export interface RuleRow {
	held(permission: string): Verdict;
	fieldValue(field: string): string | UnknownDetail;
	matches(rule: AccessRuleDef): Verdict;
	within(rule: AccessRuleDef): Verdict;
}

/**
 * Decides whether one of an entity's access rules admits `op`. The rule the
 * server applies, with an unknown verdict where the client lacks an input;
 * testdata/access/rules.json pins the two against each other.
 *
 * A rule admits an operation it names when the caller holds each permission in
 * its `requires` on the row, its `callerIs` check passes, the row matches its
 * `when` condition and sits within rows that pass its `within` entries.
 * The `callerIs` check passes when `subjects` holds the field value, which
 * must not be empty. `row` is the row read or deleted, the row a create
 * will store, or the stored row of an update. An update also needs the same
 * rule to admit `final`, the row it will store, and its fields check to allow
 * `changed`, the units the update changes; `null` stands for fields that no
 * list covers.
 */
export function decideRules(
	rules: readonly AccessRuleDef[],
	op: AccessOperation,
	subjects: readonly string[],
	row: RuleRow,
	final?: RuleRow,
	changed?: readonly string[] | null,
): Verdict {
	const admits = (rule: AccessRuleDef, side: RuleRow): Verdict => {
		let callerIs: Verdict = 'yes';
		if (rule.callerIs !== undefined) {
			const value = side.fieldValue(rule.callerIs.name);
			callerIs = typeof value !== 'string'
				? value
				: value !== '' && subjects.includes(value)
				? 'yes'
				: 'no';
		}
		const when: Verdict = rule.when === undefined ? 'yes' : side.matches(rule);
		const within: Verdict = rule.within.length === 0 ? 'yes' : side.within(rule);
		let source: Verdict = 'yes';
		for (const permission of rule.requires) source = and(source, side.held(permission));
		return and(and(and(callerIs, when), within), source);
	};
	let answer: Verdict = 'no';
	for (const rule of rules) {
		if (!rule[op]) continue;
		let ruleAnswer = admits(rule, row);
		if (op === 'update') {
			if (rule.fields !== undefined) {
				const listed = rule.fields;
				const allowed = changed !== null && changed !== undefined
					&& changed.every((u) => listed.includes(u));
				ruleAnswer = and(ruleAnswer, allowed ? 'yes' : 'no');
			}
			ruleAnswer = and(
				ruleAnswer,
				final === undefined ? unknownBecause('new_value') : admits(rule, final),
			);
		}
		if (ruleAnswer === 'yes') return 'yes';
		if (ruleAnswer !== 'no' && answer === 'no') answer = ruleAnswer;
	}
	return answer;
}

/**
 * Decides whether an update that changes the units in `changed` is admitted:
 * one of the entity's `rules` admits it as {@link decideRules} decides, and for
 * each of `guards` that restricts a unit in `changed`, one of its rules admits
 * the update on `row`, the stored row, and on `final`. A guard with no update
 * rule admits no update. `null` in `changed` stands for fields that no fields
 * list covers, which no guard is taken to restrict.
 */
export function decideUpdate(
	rules: readonly AccessRuleDef[],
	guards: readonly GuardDef[],
	subjects: readonly string[],
	row: RuleRow,
	final: RuleRow,
	changed: readonly string[] | null,
): Verdict {
	let answer = decideRules(rules, 'update', subjects, row, final, changed);
	if (changed === null) return answer;
	for (const guard of guards) {
		if (answer === 'no') return answer;
		if (!guard.units.some((u) => changed.includes(u))) continue;
		answer = and(answer, decideRules(guard.rules, 'update', subjects, row, final, changed));
	}
	return answer;
}

/**
 * Decides whether the caller reads the units in `fields` of `row`: one of the
 * entity's `rules` admits reading the row as {@link decideRules} decides, and
 * for each of `guards` that restricts a unit in `fields`, one of its rules
 * admits reading the row too. A guard with no read rule admits no read.
 */
export function decideRead(
	rules: readonly AccessRuleDef[],
	guards: readonly GuardDef[],
	subjects: readonly string[],
	row: RuleRow,
	fields: readonly string[],
): Verdict {
	let answer = decideRules(rules, 'read', subjects, row);
	for (const guard of guards) {
		if (answer === 'no') return answer;
		if (!guard.units.some((u) => fields.includes(u))) continue;
		answer = and(answer, decideRules(guard.rules, 'read', subjects, row));
	}
	return answer;
}

/** `'no'` when either is, else the first unknown of the two, else `'yes'`. */
export function and(a: Verdict, b: Verdict): Verdict {
	if (a === 'no' || b === 'no') return 'no';
	return a === 'yes' ? b : a;
}

/** `'yes'` when either is, else the first unknown of the two, else `'no'`. */
function or(a: Verdict, b: Verdict): Verdict {
	if (a === 'yes' || b === 'yes') return 'yes';
	return a === 'no' ? b : a;
}

/** One row of a workspace. */
export interface AccessRow {
	readonly entity: string;
	readonly key: string;
}

/**
 * What a row's access parents name: the entity and key of each row it sits
 * below, with the entity's definition, sorted by entity and key without
 * repeats. Empty for a row whose entity has no access parent or whose
 * references are unset, and `undefined` when the row is not known.
 */
export type ParentRefs = readonly (AccessRow & { def: EntityDef })[] | undefined;

/**
 * The rows above a row for `canAt`: `parents` holds the rows its access
 * parents name, and `above` the same for a row above it. Either is
 * `undefined` for a row the client does not hold, so the rows further up
 * that path are not known. `row` gives the value of a row above it, and is
 * `undefined`, or absent, where the client does not know it.
 *
 * `entitiesAbove` gives the entities whose rows can sit above a row of an
 * entity, and is `undefined`, or absent, where the client does not know
 * them. Where a path stops at a row the client does not hold, they decide
 * whether a row further up can change the answer.
 */
export interface AccessChain {
	readonly parents: readonly AccessRow[] | undefined;
	/**
	 * How many hops above the row a walk follows before it answers
	 * `'unknown'`: {@link AccessSummary.maxAccessDepth}.
	 * @defaultValue {@link MAX_ACCESS_CHAIN}
	 */
	readonly maxHops?: number;
	above(row: AccessRow): readonly AccessRow[] | undefined;
	row?(row: AccessRow): { def: EntityDef; row: Message } | undefined;
	entitiesAbove?(entity: string): ReadonlySet<string> | undefined;
}

/** A chain whose rows the client does not know. */
export const UNKNOWN_CHAIN: AccessChain = { parents: undefined, above: () => undefined };

/**
 * Decides `permission` at the row of `entity` with `key`, given the rows
 * above it: at the workspace root, at the row itself, or at a row above it on
 * any path. The rule the server applies, with an unknown verdict where the
 * client lacks an input.
 */
export function canAt(
	summary: AccessSummary | undefined,
	permission: string,
	entity: string,
	key: string,
	chain: AccessChain,
): Verdict {
	if (summary === undefined) return unknownBecause('no_summary');
	if (summary.holds(permission, entity, key)) return 'yes';
	if (chain.parents === undefined) {
		return heldAbove(summary, permission, entity, chain)
			? unknownBecause('row_not_held')
			: 'no';
	}
	return reach(summary, permission, chain.parents, chain);
}

/**
 * Decides `permission` at the row of `entity` with `key` through each of its
 * parents on its own: at the workspace root, at the row itself, or at that
 * parent or a row above it. A row without a parent needs the root or the
 * row. A create needs its permissions this way. The rule the server applies,
 * with an unknown verdict where the client lacks an input.
 */
export function canAtEach(
	summary: AccessSummary | undefined,
	permission: string,
	entity: string,
	key: string,
	chain: AccessChain,
): Verdict {
	if (summary === undefined) return unknownBecause('no_summary');
	if (summary.holds(permission, entity, key)) return 'yes';
	if (chain.parents === undefined) {
		return heldAbove(summary, permission, entity, chain)
			? unknownBecause('row_not_held')
			: 'no';
	}
	if (chain.parents.length === 0) return 'no';
	let answer: Verdict = 'yes';
	for (const parent of chain.parents) {
		answer = and(answer, reach(summary, permission, [ parent ], chain));
	}
	return answer;
}

/**
 * Whether `permission` is held at one of `start` or a row above them, at
 * most `chain.maxHops` hops above the row they sit above, or the summary's
 * `maxAccessDepth` when the chain sets none.
 */
function reach(
	summary: AccessSummary,
	permission: string,
	start: readonly AccessRow[],
	chain: AccessChain,
): Verdict {
	const seen = new Set<string>();
	const fresh = (rows: readonly AccessRow[]) =>
		rows.filter((row) => {
			const id = accessRowId(row);
			if (seen.has(id)) return false;
			seen.add(id);
			return true;
		});
	let complete = true;
	let level = fresh(start);
	for (let hops = 1; level.length > 0; hops++) {
		for (const row of level) {
			if (summary.holds(permission, row.entity, row.key)) return 'yes';
		}
		const next: AccessRow[] = [];
		for (const row of level) {
			const up = chain.above(row);
			if (up === undefined) {
				if (heldAbove(summary, permission, row.entity, chain)) complete = false;
			} else next.push(...fresh(up));
		}
		if (hops >= (chain.maxHops ?? summary.maxAccessDepth) && next.length > 0) {
			return unknownBecause('chain_too_deep');
		}
		level = next;
	}
	return complete ? 'no' : unknownBecause('row_not_held');
}

/**
 * Whether the caller can hold `permission` at a row above a row of `entity`:
 * it holds the permission at the root or at a row of an entity whose rows can
 * sit above, or the chain does not know those entities.
 */
function heldAbove(
	summary: AccessSummary,
	permission: string,
	entity: string,
	chain: AccessChain,
): boolean {
	const above = chain.entitiesAbove?.(entity);
	return above === undefined || summary.holdsAtAny(permission, above);
}

/** Whether a row of `entity` can sit above a row of `below`, or the chain does not know. */
function canSitAbove(entity: string, below: string, chain: AccessChain): boolean {
	return chain.entitiesAbove?.(below)?.has(entity) ?? true;
}

/** A string that names `row` alone, for maps keyed by row. */
export function accessRowId(row: AccessRow): string {
	return JSON.stringify([ row.entity, row.key ]);
}

/**
 * The row of `def` with `key` as {@link decideRules} reads it from local
 * data: a permission is decided by {@link canAt} over `chain`, or by
 * {@link canAtEach} with `throughEach`, the `within` entries by
 * {@link withinAt}, or by {@link withinAtEach} with `throughEach`, and a field
 * a `callerIs` check reads and a `when` condition read `row`, which is unknown
 * when the client does not hold the row.
 */
export function ruleRowOf(
	summary: AccessSummary,
	def: EntityDef,
	key: string,
	row: Message | undefined,
	chain: AccessChain,
	throughEach = false,
): RuleRow {
	const decide = throughEach ? canAtEach : canAt;
	return {
		held: (permission) => decide(summary, permission, def.name, key, chain),
		fieldValue: (name) => {
			if (row === undefined) return unknownBecause('row_not_held');
			const field = def.schema.fields.find((f) => f.name === name);
			const value = field === undefined
				? undefined
				: fieldValueOf(row as unknown as Record<string, unknown>, field);
			return typeof value === 'string' ? value : '';
		},
		matches: (rule) => {
			if (rule.when === undefined) return 'yes';
			if (row === undefined) return unknownBecause('row_not_held');
			return evalCond(row, ruleWhenOf(def.schema, rule.when).cond, def.schema) ? 'yes' : 'no';
		},
		within: (rule) =>
			(throughEach ? withinAtEach : withinAt)(summary.subjects, def, key, row, chain, rule),
	};
}

/**
 * {@link withinAt} through each of the row's access parents on its own: each
 * entry holds at the row itself, or at that parent or a row above it. A write
 * that adds a row below a parent shows the row to the readers of that parent,
 * so the server checks the entries there and not only through another parent.
 */
export function withinAtEach(
	subjects: readonly string[],
	def: EntityDef,
	key: string,
	row: Message | undefined,
	chain: AccessChain,
	rule: AccessRuleDef,
): Verdict {
	const parents = chain.parents;
	if (parents === undefined || parents.length < 2) {
		return withinAt(subjects, def, key, row, chain, rule);
	}
	let answer: Verdict = 'yes';
	for (const parent of parents) {
		answer = and(answer, withinAt(subjects, def, key, row, chainFrom(chain, [ parent ]), rule));
		if (answer === 'no') return 'no';
	}
	return answer;
}

/** `chain`, with `parents` as the rows directly above the row. */
function chainFrom(chain: AccessChain, parents: readonly AccessRow[] | undefined): AccessChain {
	return {
		parents,
		maxHops: chain.maxHops,
		above: (at) => chain.above(at),
		row: chain.row === undefined ? undefined : (at) => chain.row!(at),
		entitiesAbove: chain.entitiesAbove === undefined ? undefined : (e) => chain.entitiesAbove!(e),
	};
}

/**
 * Whether a chain of access parents from `def` can lead back to an entity it
 * passes: rows of `def` can then sit any number of hops deep. The server
 * refuses a client write of such a row that adds an access parent the caller
 * does not read (see {@link parentsRead}).
 */
export function inRecursiveChain(def: EntityDef): boolean {
	const graph = entitiesAboveOf(def);
	for (const name of [ def.name, ...(graph.get(def.name) ?? []) ]) {
		if (graph.get(name)?.has(name)) return true;
	}
	return false;
}

/**
 * Whether the caller reads each row of `parents`, the rows directly above a
 * row whose rows above it `chain` holds: the row is held locally, live, and a
 * read rule of its entity admits the caller there. An entity outside the
 * access model is read by every caller. A parent the client does not hold
 * answers `'unknown'`, as it can be missing or hidden.
 */
export function parentsRead(
	summary: AccessSummary,
	parents: ParentRefs,
	chain: AccessChain,
): Verdict {
	if (parents === undefined) return unknownBecause('row_not_held');
	let answer: Verdict = 'yes';
	for (const parent of parents) {
		const rules = parent.def.accessRules;
		if (rules === undefined) continue;
		const held = chain.row?.(parent);
		if (held === undefined) {
			answer = and(answer, unknownBecause('row_not_held'));
			continue;
		}
		const at = ruleRowOf(
			summary,
			held.def,
			parent.key,
			held.row,
			chainFrom(chain, chain.above(parent)),
		);
		answer = and(answer, decideRules(rules, 'read', summary.subjects, at));
		if (answer === 'no') return 'no';
	}
	return answer;
}

/**
 * Decides the `within` entries of `rule` for the row of `def` with `key` whose
 * value is `row`, undefined when the client does not hold it, given the rows
 * above it: each entry needs {@link withinReach} to find a row of its entity
 * that passes its checks for a caller with `subjects`. The rule the server
 * applies, with an unknown verdict where the client lacks an input.
 */
export function withinAt(
	subjects: readonly string[],
	def: EntityDef,
	key: string,
	row: Message | undefined,
	chain: AccessChain,
	rule: AccessRuleDef,
): Verdict {
	let answer: Verdict = 'yes';
	for (const entry of rule.within) {
		const passes = (at: AccessRow): Verdict => {
			if (at.entity === def.name && at.key === key) {
				if (row === undefined) return unknownBecause('row_not_held');
				return passesWithin(subjects, def, row, entry) ? 'yes' : 'no';
			}
			const held = chain.row?.(at);
			if (held === undefined) return unknownBecause('row_not_held');
			return passesWithin(subjects, held.def, held.row, entry) ? 'yes' : 'no';
		};
		answer = and(answer, withinReach({ entity: def.name, key }, entry.entity, chain, passes));
		if (answer === 'no') return 'no';
	}
	return answer;
}

/**
 * Decides whether `row` itself, or a row above it on any path of at most
 * `chain.maxHops` hops, is a row of `entity` that `passes` admits. `passes`
 * is asked only for rows of `entity`. The walk the server's
 * accessChains.withinAll makes; testdata/access/within.json pins the two.
 */
export function withinReach(
	row: AccessRow,
	entity: string,
	chain: AccessChain,
	passes: (at: AccessRow) => Verdict,
): Verdict {
	// The first unknown met, which the answer is when no row passes.
	let missing: UnknownDetail | undefined;
	const decide = (at: AccessRow): boolean => {
		const answer = passes(at);
		if (typeof answer !== 'string') missing ??= answer;
		return answer === 'yes';
	};
	if (row.entity === entity && decide(row)) return 'yes';
	if (chain.parents === undefined) {
		if (missing === undefined && !canSitAbove(entity, row.entity, chain)) return 'no';
		return missing ?? unknownBecause('row_not_held');
	}
	const seen = new Set<string>();
	const fresh = (rows: readonly AccessRow[]) =>
		rows.filter((at) => {
			const id = accessRowId(at);
			if (seen.has(id)) return false;
			seen.add(id);
			return true;
		});
	let level = fresh(chain.parents);
	for (let hops = 1; level.length > 0; hops++) {
		for (const at of level) {
			if (at.entity === entity && decide(at)) return 'yes';
		}
		const next: AccessRow[] = [];
		for (const at of level) {
			const up = chain.above(at);
			if (up === undefined) {
				if (canSitAbove(entity, at.entity, chain)) missing ??= unknownBecause('row_not_held');
			} else next.push(...fresh(up));
		}
		if (hops >= (chain.maxHops ?? MAX_ACCESS_CHAIN) && next.length > 0) {
			return unknownBecause('chain_too_deep');
		}
		level = next;
	}
	return missing ?? 'no';
}

/** Whether `row`, a row of `def`, passes the checks of `entry` for a caller with `subjects`. */
function passesWithin(
	subjects: readonly string[],
	def: EntityDef,
	row: Message,
	entry: WithinDef,
): boolean {
	if (entry.callerIs !== undefined) {
		const field = def.schema.fields.find((f) => f.name === entry.callerIs);
		const value = field === undefined
			? undefined
			: fieldValueOf(row as unknown as Record<string, unknown>, field);
		if (typeof value !== 'string' || value === '' || !subjects.includes(value)) return false;
	}
	return entry.when === undefined
		|| evalCond(row, ruleWhenOf(def.schema, entry.when).cond, def.schema);
}

/**
 * Whether an update of a row of `def` that changes the units in `changed` can
 * change whether the row passes a `within` entry of `rule` on its own entity:
 * the field its `callerIs` check names or its condition reads one of them.
 * `changed` holds proto names, and a oneof counts by its name.
 */
export function withinReads(
	def: EntityDef,
	rule: AccessRuleDef,
	changed: readonly string[],
): boolean {
	return rule.within.some((entry) => {
		if (entry.entity !== def.name) return false;
		if (entry.callerIs !== undefined) {
			const field = def.schema.fields.find((f) => f.name === entry.callerIs);
			if (changed.includes(field?.oneof?.name ?? entry.callerIs)) return true;
		}
		return entry.when !== undefined
			&& ruleWhenOf(def.schema, entry.when).units.some((u) => changed.includes(u));
	});
}

/** The entities the `within` entries of `def`'s access rules name. */
export function withinEntities(def: EntityDef): ReadonlySet<string> {
	const out = new Set<string>();
	for (const rule of def.accessRules ?? []) {
		for (const entry of rule.within) out.add(entry.entity);
	}
	return out;
}

/** A rule's `when` condition, compiled for {@link evalCond}. */
interface RuleWhen {
	cond: Cond;
	/** The units the condition reads, by proto name: fields, and oneofs for their members. */
	units: readonly string[];
}

const ruleWhens = new WeakMap<Condition, RuleWhen>();

/**
 * Compiles a rule's `when` condition against the entity's schema. The server
 * refuses a condition that does not compile at registration, so one that
 * reaches the client compiles.
 */
function ruleWhenOf(schema: DescMessage, when: Condition): RuleWhen {
	let out = ruleWhens.get(when);
	if (out === undefined) {
		const cond = prepareQuery(schema, { where: condLocalNames(schema, condFromProto(when)) }).where!;
		const units = new Set<string>();
		for (const head of condHeads(condFromProto(when))) {
			const field = schema.fields.find((f) => f.name === head);
			if (field !== undefined) units.add(field.oneof?.name ?? field.name);
		}
		out = { cond, units: [ ...units ] };
		ruleWhens.set(when, out);
	}
	return out;
}

/** The first segment of every path of `cond` that starts at the row. */
function condHeads(cond: Cond): string[] {
	switch (cond.kind) {
		case 'and':
		case 'or':
			return cond.conds.flatMap(condHeads);
		case 'not':
			return condHeads(cond.cond);
		case 'match':
			return [];
		default:
			// An exists or rel sub-condition is scoped to an element or another
			// row, so only its own path starts at the row.
			return cond.path.length === 0 ? [] : [ cond.path[0]! ];
	}
}

/**
 * Whether an update that changes the units in `changed` can change whether
 * the row matches the rule's `when` condition: the condition reads one of
 * them. `changed` holds proto names, and a oneof counts by its name.
 */
export function whenReads(
	schema: DescMessage,
	rule: AccessRuleDef,
	changed: readonly string[],
): boolean {
	if (rule.when === undefined) return false;
	return ruleWhenOf(schema, rule.when).units.some((u) => changed.includes(u));
}

/**
 * The inputs a prediction for a row of `def` reads from the row itself, as a
 * string: whether the client holds the row, what its access parents name,
 * and with `checks`, the value of each field that a check of the entity's
 * access rules or field guards reads from the row: a `callerIs` field, a
 * field a `when` condition reads, and the same for each `within` entry on the
 * entity itself. `row` is undefined when the client does not hold the row.
 * Two rows with equal strings give the same {@link ruleRowOf} and
 * {@link parentRefsOf}, so a change of the row that keeps the string cannot
 * change an answer for it.
 */
export function accessInputsOf(
	def: EntityDef,
	row: Message | undefined,
	checks: boolean,
): string {
	if (row === undefined) return '';
	const parents = parentRefsOf(def, row) ?? [];
	const inputs: unknown[] = [ parents.map((p) => [ p.entity, p.key ]) ];
	if (checks) {
		const units = checkedUnitsOf(def);
		if (units.length > 0) {
			// The checked units copied into an empty row and encoded: equal
			// bytes are equal values, of any field kind.
			const rec = row as unknown as Record<string, unknown>;
			const pick = create(def.schema) as unknown as Record<string, unknown>;
			for (const unit of units) pick[unit] = rec[unit];
			inputs.push(toBinary(def.schema, pick as unknown as Message).join(','));
		}
	}
	return JSON.stringify(inputs);
}

const checkedUnits = new WeakMap<DescMessage, readonly string[]>();

/**
 * The top-level units of `def`, by accessor name, that a check of its access
 * rules or field guards reads from the row: a `callerIs` field, a unit a
 * `when` condition reads, and the same for each `within` entry on `def`
 * itself. A oneof counts by its name.
 */
function checkedUnitsOf(def: EntityDef): readonly string[] {
	let out = checkedUnits.get(def.schema);
	if (out === undefined) {
		const protoNames = new Set<string>();
		const add = (name: string) => {
			const field = def.schema.fields.find((f) => f.name === name);
			protoNames.add(field?.oneof?.name ?? name);
		};
		const rules = [ ...def.accessRules ?? [], ...def.guards.flatMap((g) => g.rules) ];
		for (const rule of rules) {
			if (rule.callerIs !== undefined) add(rule.callerIs.name);
			if (rule.when !== undefined) ruleWhenOf(def.schema, rule.when).units.forEach(add);
			for (const entry of rule.within) {
				if (entry.entity !== def.name) continue;
				if (entry.callerIs !== undefined) add(entry.callerIs);
				if (entry.when !== undefined) ruleWhenOf(def.schema, entry.when).units.forEach(add);
			}
		}
		const local: string[] = [];
		for (const field of def.schema.fields) {
			if (field.oneof === undefined && protoNames.has(field.name)) local.push(field.localName);
		}
		for (const oneof of def.schema.oneofs) {
			if (protoNames.has(oneof.name)) local.push(oneof.localName);
		}
		out = local;
		checkedUnits.set(def.schema, out);
	}
	return out;
}

/**
 * The units a write changes to move a row of `def` to other access parents:
 * each field and each oneof that `access.parents` names, by proto name, in
 * field order. Empty for an entity without an access parent.
 */
export function accessParentUnits(def: EntityDef): string[] {
	return [ ...new Set(accessParentFields(def.schema).map((f) => f.oneof?.name ?? f.name)) ];
}

/**
 * The units an update changes, by proto name, for fields named by accessor
 * name: a top-level field outside any oneof names itself, and a oneof or a
 * member of one names the oneof.
 *
 * Throws on a name that is not a top-level field or oneof of `schema`.
 */
export function updateUnitsOf(schema: DescMessage, names: readonly string[]): string[] {
	const units = new Set<string>();
	for (const name of names) {
		const field = schema.fields.find((f) => f.localName === name);
		const oneof = field?.oneof ?? schema.oneofs.find((o) => o.localName === name);
		if (oneof !== undefined) {
			units.add(oneof.name);
		} else if (field !== undefined) {
			units.add(field.name);
		} else {
			// A proto name works in an explicit patch mask, so it is the likely
			// mistake here.
			const byProto = schema.fields.find((f) => f.name === name)
				?? schema.oneofs.find((o) => o.name === name);
			throw new Error(
				`can: unknown field "${name}" on ${schema.typeName}`
					+ (byProto === undefined ? '' : `; name it by its accessor name, "${byProto.localName}"`),
			);
		}
	}
	return [ ...units ];
}

const parentFields = new WeakMap<DescMessage, readonly DescField[]>();

/**
 * The fields the entity's `access.parents` names, in field order: each named
 * singular or repeated field, and every member of each named oneof. Empty for
 * a message without an `access` option.
 */
export function accessParentFields(schema: DescMessage): readonly DescField[] {
	let fields = parentFields.get(schema);
	if (fields === undefined) {
		const names = new Set(entityOptionsOf(schema)?.access?.parents ?? []);
		fields = names.size === 0
			? []
			: schema.fields.filter((f) => names.has(f.oneof?.name ?? f.name));
		parentFields.set(schema, fields);
	}
	return fields;
}

/**
 * What a row's access parents name, from the cases the row holds. `row` is
 * the row's value, or undefined when the row is not held locally.
 */
export function parentRefsOf(def: EntityDef, row: Message | undefined): ParentRefs {
	const fields = accessParentFields(def.schema);
	if (fields.length === 0) return [];
	if (row === undefined) return undefined;
	const out: (AccessRow & { def: EntityDef })[] = [];
	const seen = new Set<string>();
	const add = (field: DescField, value: unknown) => {
		if (value === undefined || value === null) return;
		// The cases inside one reference message share the field, and only the
		// active one yields a stub.
		for (const shape of refShapes(field)) {
			const stub = refStub(shape, value as Message);
			if (stub === undefined) continue;
			const target = targetDefOf(shape.targetSchema);
			const key = keyOf(target, stub);
			const ref = { entity: target.name, key, def: target };
			if (key !== '' && !seen.has(accessRowId(ref))) {
				seen.add(accessRowId(ref));
				out.push(ref);
			}
			return;
		}
	};
	for (const field of fields) {
		const value = fieldValueOf(row as unknown as Record<string, unknown>, field);
		if (field.fieldKind === 'list') {
			for (const element of (value ?? []) as unknown[]) add(field, element);
		} else add(field, value);
	}
	const order = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);
	return out.sort((a, b) => order(a.entity, b.entity) || order(a.key, b.key));
}

const entityGraphs = new WeakMap<DescMessage, ReadonlyMap<string, ReadonlySet<string>>>();

/**
 * The entities whose rows can sit above a row of each entity a chain from
 * `def` reaches, `def` included, by entity: the entities its access parents
 * point at, the entities their access parents point at, and so on. An entity
 * is above itself when a chain of its access parents leads back to it. Give
 * the result's `get` to {@link AccessChain.entitiesAbove}.
 */
export function entitiesAboveOf(def: EntityDef): ReadonlyMap<string, ReadonlySet<string>> {
	let graph = entityGraphs.get(def.schema);
	if (graph === undefined) {
		const parents = new Map<string, Set<string>>();
		const queue = [ def ];
		for (let at = queue.shift(); at !== undefined; at = queue.shift()) {
			if (parents.has(at.name)) continue;
			const names = new Set<string>();
			parents.set(at.name, names);
			for (const field of accessParentFields(at.schema)) {
				for (const shape of refShapes(field)) {
					const target = targetDefOf(shape.targetSchema);
					names.add(target.name);
					queue.push(target);
				}
			}
		}
		const out = new Map<string, ReadonlySet<string>>();
		for (const [ name, direct ] of parents) {
			const above = new Set<string>();
			const todo = [ ...direct ];
			for (let up = todo.pop(); up !== undefined; up = todo.pop()) {
				if (above.has(up)) continue;
				above.add(up);
				todo.push(...parents.get(up)!);
			}
			out.set(name, above);
		}
		graph = out;
		entityGraphs.set(def.schema, graph);
	}
	return graph;
}

const targetDefs = new WeakMap<DescMessage, EntityDef>();

/**
 * The entity an access parent points at, read from the target's own entity
 * options rather than the registry: a client need not register the entity
 * its anchors are rows of.
 */
function targetDefOf(schema: DescMessage): EntityDef {
	let def = targetDefs.get(schema);
	if (def === undefined) {
		def = entityDefOf(schema);
		targetDefs.set(schema, def);
	}
	return def;
}

/**
 * Whether a local write the server has not acked can change what the caller
 * holds: a write to a grant row, a delete of a row that rows of another
 * entity sit below, or a custom mutation that `custom` says can change it.
 * Without `custom`, a custom mutation can change it in a schema with grant
 * entities or such rows.
 */
export function pendingChangesAccess(
	registry: EntityRegistry,
	pending: readonly Mutation[],
	custom?: (mutation: CustomMutation) => boolean,
): boolean {
	const anchors = anchorEntities(registry);
	return pending.some((m) =>
		custom !== undefined && m.payload.case === 'custom'
			? custom(m.payload.value)
			: changesAccess(registry, anchors, m)
	);
}

/**
 * Whether a local write of a row of `entity` can change what the caller
 * holds: a write to a grant row, or a delete (`del`) of a row that rows of
 * another entity sit below.
 */
export function writeChangesAccess(
	registry: EntityRegistry,
	entity: string,
	del: boolean,
): boolean {
	if (registry.get(entity)?.grant) return true;
	return del && anchorEntities(registry).has(entity);
}

/**
 * How many of `mutations` can change what the caller holds, each one decided
 * as {@link pendingChangesAccess} decides it.
 */
export function countAccessChanges(
	registry: EntityRegistry,
	mutations: readonly Mutation[],
): number {
	const anchors = anchorEntities(registry);
	let count = 0;
	for (const m of mutations) {
		if (changesAccess(registry, anchors, m)) count++;
	}
	return count;
}

function changesAccess(
	registry: EntityRegistry,
	anchors: ReadonlySet<string>,
	m: Mutation,
): boolean {
	const touches = (crud: CrudMutation): boolean => {
		if (registry.get(crud.entity)?.grant) return true;
		return crud.op === CrudOp.DELETE && anchors.has(crud.entity);
	};
	switch (m.payload.case) {
		case 'crud':
			return touches(m.payload.value);
		case 'batch':
			return m.payload.value.mutations.some(touches);
		case 'custom':
			return anchors.size > 0 || hasGrantEntities(registry);
		default:
			return false;
	}
}

const grantRegistries = new WeakMap<EntityRegistry, boolean>();

/** Whether `registry` holds a grant entity. */
export function hasGrantEntities(registry: EntityRegistry): boolean {
	let out = grantRegistries.get(registry);
	if (out === undefined) {
		out = registry.names().some((name) => registry.get(name)!.grant);
		grantRegistries.set(registry, out);
	}
	return out;
}

/**
 * Records in `touched` that a commit adds `mutations` to the pending queue of
 * `workspaceId`, or removes them from it, when one of them can change what
 * the caller holds (see {@link pendingChangesAccess}).
 */
export function touchPendingAccess(
	touched: TouchSet,
	registry: EntityRegistry,
	workspaceId: string,
	mutations: readonly Mutation[],
): void {
	if (pendingChangesAccess(registry, mutations)) touched.entity(workspaceId, ACCESS_TOUCH_ENTITY);
}

const anchorSets = new WeakMap<EntityRegistry, Set<string>>();

/**
 * The entities some case of some entity's access parent points at: the
 * entities whose rows form the chains a prediction walks.
 */
export function anchorEntities(registry: EntityRegistry): ReadonlySet<string> {
	let set = anchorSets.get(registry);
	if (set === undefined) {
		set = new Set();
		for (const name of registry.names()) {
			for (const field of accessParentFields(registry.get(name)!.schema)) {
				for (const shape of refShapes(field)) set.add(targetDefOf(shape.targetSchema).name);
			}
		}
		anchorSets.set(registry, set);
	}
	return set;
}

const childSets = new WeakMap<EntityRegistry, Map<string, readonly EntityDef[]>>();

/**
 * The registered entities whose access parents can point at `entity`: the
 * entities whose rows can sit directly below its rows.
 */
export function childEntitiesOf(registry: EntityRegistry, entity: string): readonly EntityDef[] {
	let byEntity = childSets.get(registry);
	if (byEntity === undefined) {
		byEntity = new Map();
		childSets.set(registry, byEntity);
	}
	let out = byEntity.get(entity);
	if (out === undefined) {
		out = registry.names().map((name) => registry.get(name)!).filter((def) =>
			accessParentFields(def.schema).some((field) =>
				refShapes(field).some((shape) => targetDefOf(shape.targetSchema).name === entity)
			)
		);
		byEntity.set(entity, out);
	}
	return out;
}

/**
 * Who reads rows of an entity, or the fields of one of its guards, as the
 * server groups the read rules: every admitted caller, a caller with one of
 * `perms`, or a caller that one of `checked` admits, which are the rules with
 * a check on the row, a `within` entry, or more than one permission.
 */
interface Readers {
	readonly all: boolean;
	readonly perms: readonly string[];
	readonly checked: readonly AccessRuleDef[];
}

/** The {@link Readers} of the read rules among `rules`. */
function readersOf(rules: readonly AccessRuleDef[]): Readers {
	const perms = new Set<string>();
	const checked: AccessRuleDef[] = [];
	for (const rule of rules) {
		if (!rule.read) continue;
		if (
			rule.callerIs !== undefined || rule.when !== undefined || rule.within.length > 0
			|| rule.requires.length > 1
		) checked.push(rule);
		else if (rule.requires.length === 0) return { all: true, perms: [], checked: [] };
		else perms.add(rule.requires[0]!);
	}
	return { all: false, perms: [ ...perms ], checked };
}

/**
 * Who reads every row where a row sits: `r` without the rules that check the
 * row with `callerIs` or `when`, which admit a caller on some rows only, and
 * without the rules that one of `r.perms` admits every caller of.
 */
function everyRowReaders(r: Readers): Readers {
	if (r.all) return r;
	return {
		all: false,
		perms: r.perms,
		checked: r.checked.filter((o) =>
			o.callerIs === undefined && o.when === undefined
			&& !o.requires.some((p) => r.perms.includes(p))
		),
	};
}

/**
 * Whether `r` can admit other callers when the row sits elsewhere: a rule of
 * it needs a permission, which a caller holds at the root or at a row up the
 * row's chain, or sets a `within` entry, which reads the rows above the row.
 * Any other rule reads only the row and the caller, so a move shows what `r`
 * reads to nobody new.
 */
function dependsOnChain(r: Readers): boolean {
	return !r.all
		&& (r.perms.length > 0 || r.checked.some((o) => o.requires.length > 0 || o.within.length > 0));
}

/**
 * The guards of `def` that can hide their fields from a caller that reads the
 * row: each guard whose read rules do not admit every admitted caller.
 */
function hidingGuards(def: EntityDef): readonly GuardDef[] {
	return def.guards.filter((g) => !readersOf(g.rules).all);
}

const belowReaders = new WeakMap<EntityRegistry, Map<string, readonly Readers[]>>();

/**
 * Who reads every row of each registered entity whose rows can sit below rows
 * of `entity`, any number of hops down, and every field of each guard of such
 * an entity that can hide its fields (see {@link everyRowReaders}), without
 * the sets whose readers do not depend on where a row sits (see
 * {@link dependsOnChain}).
 */
function moveReadersBelow(registry: EntityRegistry, entity: string): readonly Readers[] {
	let byEntity = belowReaders.get(registry);
	if (byEntity === undefined) belowReaders.set(registry, byEntity = new Map());
	let out = byEntity.get(entity);
	if (out === undefined) {
		const children = new Map<string, Set<string>>();
		for (const name of registry.names()) {
			for (const field of accessParentFields(registry.get(name)!.schema)) {
				for (const shape of refShapes(field)) {
					const target = targetDefOf(shape.targetSchema).name;
					let set = children.get(target);
					if (set === undefined) children.set(target, set = new Set());
					set.add(name);
				}
			}
		}
		const below = new Set<string>();
		const queue = [ entity ];
		for (let at = queue.shift(); at !== undefined; at = queue.shift()) {
			for (const child of children.get(at) ?? []) {
				if (!below.has(child)) {
					below.add(child);
					queue.push(child);
				}
			}
		}
		const readers: Readers[] = [];
		for (const name of below) {
			const def = registry.get(name)!;
			// An entity with an access parent declares access.
			for (const set of [ def.accessRules ?? [], ...hidingGuards(def).map((g) => g.rules) ]) {
				const r = readersOf(set);
				if (dependsOnChain(r)) readers.push(everyRowReaders(r));
			}
		}
		out = readers.filter((r) => !r.all);
		byEntity.set(entity, out);
	}
	return out;
}

const withinInputs = new WeakMap<EntityRegistry, Map<string, ReadonlySet<string>>>();

/**
 * The units of a row of `entity`, by proto name, that a `within` entry naming
 * `entity` reads, in a rule or a guard rule of any registered entity: the
 * field its `callerIs` names and the units its condition reads.
 */
function withinInputUnits(registry: EntityRegistry, entity: string): ReadonlySet<string> {
	let byEntity = withinInputs.get(registry);
	if (byEntity === undefined) withinInputs.set(registry, byEntity = new Map());
	let out = byEntity.get(entity);
	if (out === undefined) {
		const units = new Set<string>();
		const target = registry.get(entity);
		if (target !== undefined) {
			for (const name of registry.names()) {
				const def = registry.get(name)!;
				for (const rule of [ ...def.accessRules ?? [], ...def.guards.flatMap((g) => g.rules) ]) {
					for (const entry of rule.within) {
						if (entry.entity !== entity) continue;
						if (entry.callerIs !== undefined) {
							const field = target.schema.fields.find((f) => f.name === entry.callerIs);
							units.add(field?.oneof?.name ?? entry.callerIs);
						}
						if (entry.when !== undefined) {
							for (const u of ruleWhenOf(target.schema, entry.when).units) units.add(u);
						}
					}
				}
			}
		}
		out = units;
		byEntity.set(entity, out);
	}
	return out;
}

/**
 * Decides whether the caller holds what `readers` needs at the row of `def`
 * with `key`, whose value is `row`, given the rows above it: for each entry,
 * one of its permissions, or every permission of one of its checked rules and
 * the rule's `within` entries.
 */
function decideReadersAt(
	summary: AccessSummary,
	def: EntityDef,
	key: string,
	row: Message | undefined,
	chain: AccessChain,
	readers: readonly Readers[],
): Verdict {
	let answer: Verdict = 'yes';
	for (const r of readers) {
		let one: Verdict = 'no';
		for (const p of r.perms) {
			if (one === 'yes') break;
			one = or(one, canAt(summary, p, def.name, key, chain));
		}
		for (const o of r.checked) {
			if (one === 'yes') break;
			let rule: Verdict = 'yes';
			for (const p of o.requires) rule = and(rule, canAt(summary, p, def.name, key, chain));
			if (rule !== 'no' && o.within.length > 0) {
				rule = and(rule, withinAt(summary.subjects, def, key, row, chain, o));
			}
			one = or(one, rule);
		}
		answer = and(answer, one);
		if (answer === 'no') return 'no';
	}
	return answer;
}

/**
 * The units of a grant row of `def` that decide what the row grants, by proto
 * name: the subject, the role and the access parents. Empty for an entity
 * that is not a grant entity.
 */
function grantUnitsOf(def: EntityDef): readonly string[] {
	const spec = def.grantSpec;
	if (spec === undefined) return [];
	const out = [ spec.subjectUnit, ...accessParentUnits(def) ];
	if (spec.roleField !== undefined) out.push(spec.roleField.name);
	return out;
}

/**
 * The role a grant row of `spec` holds; empty for none. An enum number holds
 * the role declared on any of its names.
 */
function grantRoleOf(spec: GrantSpec, row: Message): string {
	const field = spec.roleField;
	if (field === undefined) return '';
	const value = fieldValueOf(row as unknown as Record<string, unknown>, field);
	if (field.fieldKind === 'enum') {
		// An alias inserted before the declared name would otherwise read the
		// number as a name no role declares.
		const names = field.enum.values.filter((v) => v.number === value);
		return (names.find((v) => spec.roles.has(v.name)) ?? names[0])?.name ?? '';
	}
	return typeof value === 'string' ? value : '';
}

/**
 * Decides the server's subset rule for grant row `row` of `def`, given the
 * rows above it: the caller must hold every permission of the row's role
 * where the row applies, at the workspace root for an entity without an
 * access parent, else at the row its access parent names or a row above
 * that. A row whose role grants nothing, or whose access parent is unset,
 * passes, as does a row of an entity that is not a grant entity. `row` is
 * undefined when the client does not hold it.
 */
export function grantSubsetAt(
	summary: AccessSummary,
	def: EntityDef,
	row: Message | undefined,
	chain: AccessChain,
): Verdict {
	const spec = def.grantSpec;
	if (spec === undefined) return 'yes';
	if (row === undefined) return unknownBecause('row_not_held');
	const perms = spec.roles.get(grantRoleOf(spec, row)) ?? [];
	if (perms.length === 0) return 'yes';
	let at: AccessRow | undefined;
	if (accessParentFields(def.schema).length > 0) {
		const parents = chain.parents;
		if (parents === undefined) return unknownBecause('row_not_held');
		if (parents.length === 0) return 'yes';
		at = parents[0]!;
	}
	let answer: Verdict = 'yes';
	for (const p of perms) {
		answer = and(
			answer,
			at === undefined ? (summary.holdsRoot(p) ? 'yes' : 'no') : reach(summary, p, [ at ], chain),
		);
		if (answer === 'no') return 'no';
	}
	return answer;
}

/**
 * Decides the checks the server makes on an update beside the entity's rules
 * and guards, for an update of the row of `def` with `key` that changes the
 * units in `changed`, by proto name. `row` is the stored row, undefined when
 * the client does not hold it, `stored` is the row as {@link decideRules}
 * reads it, and `moves` tells whether the update changes the access parent.
 * The new value of a changed unit is not known.
 *
 * - A move needs the right to read the row where it is, and each field a
 *   guard can hide whose readers depend on where the row sits.
 * - A move, or a change of a unit that a `within` entry naming the entity
 *   reads, needs the right to read every row of each entity whose rows can
 *   sit below the row, where the row is.
 * - A change of a unit that a read rule of a hiding guard checks needs the
 *   right to read the guard's fields before and after the update.
 * - On a grant entity, a change of the subject, the role or the access parent
 *   needs every permission of the stored role where the row applies, and of
 *   the new role where it will apply.
 */
export function decideUpdateChecks(
	registry: EntityRegistry,
	summary: AccessSummary,
	def: EntityDef,
	key: string,
	row: Message | undefined,
	stored: RuleRow,
	chain: AccessChain,
	changed: readonly string[],
	moves: boolean,
): Verdict {
	const subjects = summary.subjects;
	const newValue = unknownBecause('new_value');
	let answer: Verdict = 'yes';
	if (moves) {
		// A guard whose readers do not depend on where the row sits shows its
		// fields to the same callers after the move.
		const moving = hidingGuards(def).filter((g) => dependsOnChain(readersOf(g.rules)));
		const hidden = moving.flatMap((g) => g.units);
		answer = decideRead(def.accessRules ?? [], def.guards, subjects, stored, hidden);
	}
	if (
		answer !== 'no' && (moves || changed.some((u) => withinInputUnits(registry, def.name).has(u)))
	) {
		answer = and(
			answer,
			decideReadersAt(summary, def, key, row, chain, moveReadersBelow(registry, def.name)),
		);
	}
	for (const g of hidingGuards(def)) {
		if (answer === 'no') return 'no';
		const inputs = g.rules.filter((r) => r.read).flatMap((r) => [
			...(r.callerIs === undefined ? [] : [ r.callerIs.oneof?.name ?? r.callerIs.name ]),
			...(r.when === undefined ? [] : ruleWhenOf(def.schema, r.when).units),
		]);
		if (!inputs.some((u) => changed.includes(u))) continue;
		answer = and(answer, and(decideRules(g.rules, 'read', subjects, stored), newValue));
	}
	const grantUnits = grantUnitsOf(def);
	if (answer !== 'no' && changed.some((u) => grantUnits.includes(u))) {
		answer = and(answer, grantSubsetAt(summary, def, row, chain));
		// The subject alone leaves the role and where the row applies as they are.
		const spec = def.grantSpec!;
		const decides = [
			...accessParentUnits(def),
			...(spec.roleField === undefined ? [] : [ spec.roleField.name ]),
		];
		if (changed.some((u) => decides.includes(u))) answer = and(answer, newValue);
	}
	return answer;
}
