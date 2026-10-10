/**
 * Client-side predictions of the access model: whether the caller holds a
 * permission at a row, and whether an entity's access rules admit an
 * operation, from the CallerAccess the server last sent for the workspace.
 * The server decides every read and write itself; these answers only let an
 * app shape its UI before it asks.
 */

import {
	type DescField,
	type DescMessage,
	fromBinary,
	type Message,
	toBinary,
} from '@bufbuild/protobuf';

import { type CrudMutation, CrudOp, type Mutation } from './gen/syncplane/v1/mutation_pb.js';
import type { Condition } from './gen/syncplane/v1/query_pb.js';
import { type CallerAccess, CallerAccessSchema } from './gen/syncplane/v1/service_pb.js';
import {
	type AccessRuleDef,
	type EntityDef,
	entityDefOf,
	type EntityRegistry,
	fieldBehaviorOf,
	fieldValueOf,
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
 * when the client cannot tell — no summary has arrived for the workspace, a
 * row the answer depends on is not held locally, or local writes the server
 * has not acked yet can change the caller's grants.
 */
export type CanAnswer = 'yes' | 'no' | 'unknown';

/** An operation on a row that an entity's access rules decide. */
export type AccessOperation = 'read' | 'create' | 'update' | 'delete';

/**
 * How many rows up a row's chains of access parents `can` walks on each path
 * before it answers `'unknown'`: the server's default `MaxAccessDepth`.
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

	constructor(access: CallerAccess) {
		this.subjects = access.subjects;
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

	constructor(readonly raw: Uint8Array, access: CallerAccess) {
		this.access = access;
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
	return { raw, summary: new AccessSummary(fromBinary(CallerAccessSchema, raw)) };
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
 * subject field, `undefined` where the client cannot know it, whether the
 * row matches a rule's `when` condition, and whether it sits within rows that
 * pass a rule's `within` entries.
 */
export interface RuleRow {
	held(permission: string): CanAnswer;
	subject(field: string): string | undefined;
	matches(rule: AccessRuleDef): CanAnswer;
	within(rule: AccessRuleDef): CanAnswer;
}

/**
 * Decides whether one of an entity's access rules admits `op`. The rule the
 * server applies, with `'unknown'` where the client lacks an input;
 * testdata/access/rules.json pins the two against each other.
 *
 * A rule admits an operation it names when the caller holds each permission in
 * its `requires` on the row, its subject check passes, the row matches its
 * `when` condition and sits within rows that pass its `within` entries:
 * `subjects` holds the subject field's value,
 * which must not be empty. `row` is the row read or deleted, the row a create
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
): CanAnswer {
	const admits = (rule: AccessRuleDef, side: RuleRow): CanAnswer => {
		let subject: CanAnswer = 'yes';
		if (rule.subject !== undefined) {
			const value = side.subject(rule.subject.name);
			subject = value === undefined
				? 'unknown'
				: value !== '' && subjects.includes(value)
				? 'yes'
				: 'no';
		}
		const when: CanAnswer = rule.when === undefined ? 'yes' : side.matches(rule);
		const within: CanAnswer = rule.within.length === 0 ? 'yes' : side.within(rule);
		let source: CanAnswer = 'yes';
		for (const permission of rule.requires) source = and(source, side.held(permission));
		return and(and(and(subject, when), within), source);
	};
	let answer: CanAnswer = 'no';
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
			ruleAnswer = and(ruleAnswer, final === undefined ? 'unknown' : admits(rule, final));
		}
		if (ruleAnswer === 'yes') return 'yes';
		if (ruleAnswer === 'unknown') answer = 'unknown';
	}
	return answer;
}

function and(a: CanAnswer, b: CanAnswer): CanAnswer {
	if (a === 'no' || b === 'no') return 'no';
	return a === 'unknown' || b === 'unknown' ? 'unknown' : 'yes';
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
	above(row: AccessRow): readonly AccessRow[] | undefined;
	row?(row: AccessRow): { def: EntityDef; row: Message } | undefined;
	entitiesAbove?(entity: string): ReadonlySet<string> | undefined;
}

/** A chain whose rows the client does not know. */
export const UNKNOWN_CHAIN: AccessChain = { parents: undefined, above: () => undefined };

/**
 * Decides `permission` at the row of `entity` with `key`, given the rows
 * above it: at the workspace root, at the row itself, or at a row above it on
 * any path. The rule the server applies, with `'unknown'` where the client
 * lacks an input.
 */
export function canAt(
	summary: AccessSummary | undefined,
	permission: string,
	entity: string,
	key: string,
	chain: AccessChain,
): CanAnswer {
	if (summary === undefined) return 'unknown';
	if (summary.holds(permission, entity, key)) return 'yes';
	if (chain.parents === undefined) {
		return heldAbove(summary, permission, entity, chain)
			? 'unknown'
			: 'no';
	}
	return reach(summary, permission, chain.parents, chain);
}

/**
 * Decides `permission` at the row of `entity` with `key` through each of its
 * parents on its own: at the workspace root, at the row itself, or at that
 * parent or a row above it. A row without a parent needs the root or the
 * row. A create needs its permissions this way. The rule the server applies,
 * with `'unknown'` where the client lacks an input.
 */
export function canAtEach(
	summary: AccessSummary | undefined,
	permission: string,
	entity: string,
	key: string,
	chain: AccessChain,
): CanAnswer {
	if (summary === undefined) return 'unknown';
	if (summary.holds(permission, entity, key)) return 'yes';
	if (chain.parents === undefined) {
		return heldAbove(summary, permission, entity, chain)
			? 'unknown'
			: 'no';
	}
	if (chain.parents.length === 0) return 'no';
	let answer: CanAnswer = 'yes';
	for (const parent of chain.parents) {
		answer = and(answer, reach(summary, permission, [ parent ], chain));
	}
	return answer;
}

/**
 * Whether `permission` is held at one of `start` or a row above them, at
 * most MAX_ACCESS_CHAIN hops above the row they sit above.
 */
function reach(
	summary: AccessSummary,
	permission: string,
	start: readonly AccessRow[],
	chain: AccessChain,
): CanAnswer {
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
		if (hops >= MAX_ACCESS_CHAIN && next.length > 0) return 'unknown';
		level = next;
	}
	return complete ? 'no' : 'unknown';
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
 * {@link canAtEach} with `throughEach`, and a subject field and a `when`
 * condition read `row`, which is unknown when the client does not hold the
 * row.
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
		subject: (name) => {
			if (row === undefined) return undefined;
			const field = def.schema.fields.find((f) => f.name === name);
			const value = field === undefined
				? undefined
				: fieldValueOf(row as unknown as Record<string, unknown>, field);
			return typeof value === 'string' ? value : '';
		},
		matches: (rule) => {
			if (rule.when === undefined) return 'yes';
			if (row === undefined) return 'unknown';
			return evalCond(row, ruleWhenOf(def.schema, rule.when).cond, def.schema) ? 'yes' : 'no';
		},
		within: (rule) => withinAt(summary.subjects, def, key, row, chain, rule),
	};
}

/**
 * Decides the `within` entries of `rule` for the row of `def` with `key` whose
 * value is `row`, undefined when the client does not hold it, given the rows
 * above it: each entry needs {@link withinReach} to find a row of its entity
 * that passes its checks for a caller with `subjects`. The rule the server
 * applies, with `'unknown'` where the client lacks an input.
 */
export function withinAt(
	subjects: readonly string[],
	def: EntityDef,
	key: string,
	row: Message | undefined,
	chain: AccessChain,
	rule: AccessRuleDef,
): CanAnswer {
	let answer: CanAnswer = 'yes';
	for (const entry of rule.within) {
		const passes = (at: AccessRow): CanAnswer => {
			if (at.entity === def.name && at.key === key) {
				if (row === undefined) return 'unknown';
				return passesWithin(subjects, def, row, entry) ? 'yes' : 'no';
			}
			const held = chain.row?.(at);
			if (held === undefined) return 'unknown';
			return passesWithin(subjects, held.def, held.row, entry) ? 'yes' : 'no';
		};
		answer = and(answer, withinReach({ entity: def.name, key }, entry.entity, chain, passes));
		if (answer === 'no') return 'no';
	}
	return answer;
}

/**
 * Decides whether `row` itself, or a row above it on any path of at most
 * MAX_ACCESS_CHAIN hops, is a row of `entity` that `passes` admits. `passes`
 * is asked only for rows of `entity`. The walk the server's
 * accessChains.withinAll makes; testdata/access/within.json pins the two.
 */
export function withinReach(
	row: AccessRow,
	entity: string,
	chain: AccessChain,
	passes: (at: AccessRow) => CanAnswer,
): CanAnswer {
	let complete = true;
	const decide = (at: AccessRow): boolean => {
		const answer = passes(at);
		if (answer === 'unknown') complete = false;
		return answer === 'yes';
	};
	if (row.entity === entity && decide(row)) return 'yes';
	if (chain.parents === undefined) {
		return complete && !canSitAbove(entity, row.entity, chain) ? 'no' : 'unknown';
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
				if (canSitAbove(entity, at.entity, chain)) complete = false;
			} else next.push(...fresh(up));
		}
		if (hops >= MAX_ACCESS_CHAIN && next.length > 0) return 'unknown';
		level = next;
	}
	return complete ? 'no' : 'unknown';
}

/** Whether `row`, a row of `def`, passes the checks of `entry` for a caller with `subjects`. */
function passesWithin(
	subjects: readonly string[],
	def: EntityDef,
	row: Message,
	entry: WithinDef,
): boolean {
	if (entry.subject !== undefined) {
		const field = def.schema.fields.find((f) => f.name === entry.subject);
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
 * the entry's subject field or condition reads one of them. `changed` holds
 * proto names, and a oneof counts by its name.
 */
export function withinReads(
	def: EntityDef,
	rule: AccessRuleDef,
	changed: readonly string[],
): boolean {
	return rule.within.some((entry) => {
		if (entry.entity !== def.name) return false;
		if (entry.subject !== undefined) {
			const field = def.schema.fields.find((f) => f.name === entry.subject);
			if (changed.includes(field?.oneof?.name ?? entry.subject)) return true;
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
 * and with `subjects`, the value of each subject field the entity's access
 * rules name. `row` is undefined when the client does not hold the row. Two
 * rows with equal strings give the same {@link ruleRowOf} and
 * {@link parentRefsOf}, so a change of the row that keeps the string cannot
 * change an answer for it.
 */
export function accessInputsOf(
	def: EntityDef,
	row: Message | undefined,
	subjects: boolean,
): string {
	if (row === undefined) return '';
	const parents = parentRefsOf(def, row) ?? [];
	const inputs: (string | string[][])[] = [ parents.map((p) => [ p.entity, p.key ]) ];
	if (subjects) {
		const rec = row as unknown as Record<string, unknown>;
		for (const rule of def.accessRules ?? []) {
			if (rule.subject !== undefined) {
				// Read as ruleRowOf reads it.
				const value = fieldValueOf(rec, rule.subject);
				inputs.push(typeof value === 'string' ? value : '');
			}
			for (const entry of rule.within) {
				if (entry.entity !== def.name || entry.subject === undefined) continue;
				const field = def.schema.fields.find((f) => f.name === entry.subject);
				const value = field === undefined ? undefined : fieldValueOf(rec, field);
				inputs.push(typeof value === 'string' ? value : '');
			}
		}
	}
	return JSON.stringify(inputs);
}

/**
 * The units a write changes to move a row of `def` to other access parents:
 * each marked field, and each oneof whose members are cases. Empty for an
 * entity without an access parent.
 */
export function accessParentUnits(def: EntityDef): string[] {
	return [ ...new Set(accessParentFields(def.schema).map((f) => f.oneof?.name ?? f.name)) ];
}

const parentFields = new WeakMap<DescMessage, readonly DescField[]>();

/**
 * The fields of an entity marked `access_parent`: singular and repeated
 * fields, and the members of oneofs, in field order.
 */
export function accessParentFields(schema: DescMessage): readonly DescField[] {
	let fields = parentFields.get(schema);
	if (fields === undefined) {
		fields = schema.fields.filter((f) => fieldBehaviorOf(f)?.accessParent !== undefined);
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
 * holds: a write to a grant row, or a delete of a row that rows of another
 * entity sit below.
 */
export function pendingChangesAccess(
	registry: EntityRegistry,
	pending: readonly Mutation[],
): boolean {
	const anchors = anchorEntities(registry);
	return pending.some((m) => changesAccess(registry, anchors, m));
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
		const def = registry.get(crud.entity);
		if (def?.grant) return true;
		return crud.op === CrudOp.DELETE && anchors.has(crud.entity);
	};
	switch (m.payload.case) {
		case 'crud':
			return touches(m.payload.value);
		case 'batch':
			return m.payload.value.mutations.some(touches);
		default:
			return false;
	}
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
