/**
 * Client-side predictions of the access model: whether the caller holds a
 * permission at a row, and whether an entity's access rules admit an
 * operation, from the CallerAccess the server last sent for the workspace.
 * The server decides every read and write itself; these answers only let an
 * app shape its UI before it asks.
 */

import type { DescField, DescMessage, Message } from '@bufbuild/protobuf';

import { type CrudMutation, CrudOp, type Mutation } from './gen/syncplane/v1/mutation_pb.js';
import type { CallerAccess } from './gen/syncplane/v1/service_pb.js';
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
} from './mutations.js';
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
 * How many rows up a row's chain of access parents `can` walks before it
 * answers `'unknown'`: the server's default `MaxAccessDepth`.
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
	private readonly roots = new Set<string>();
	private readonly anchors = new Map<string, Set<string>>();
	/** The caller's subjects, the primary subject first. */
	readonly subjects: readonly string[];

	constructor(access: CallerAccess) {
		this.subjects = access.subjects;
		for (const p of access.permissions) {
			if (p.root) {
				this.roots.add(p.permission);
				continue;
			}
			const set = new Set<string>();
			for (const a of p.anchors) {
				for (const key of a.keys) set.add(anchorId(a.entity, key));
			}
			this.anchors.set(p.permission, set);
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
		return this.roots.has(permission)
			|| (this.anchors.get(permission)?.has(anchorId(entity, key)) ?? false);
	}
}

/**
 * One row of an operation, as {@link decideRules} reads it: whether the
 * caller holds each permission at the row or up its chain, and the value of
 * each subject field, `undefined` where the client cannot know it.
 */
export interface RuleRow {
	held(permission: string): CanAnswer;
	subject(field: string): string | undefined;
}

/**
 * Decides whether one of an entity's access rules admits `op`. The rule the
 * server applies, with `'unknown'` where the client lacks an input;
 * testdata/access/rules.json pins the two against each other.
 *
 * A rule admits an operation it names when its source holds on the row and
 * its subject check passes: `subjects` holds the subject field's value, which
 * must not be empty. `row` is the row read or deleted, the row a create will
 * store, or the stored row of an update. An update also needs the same rule
 * to admit `final`, the row it will store, and its fields check to allow
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
		const source: CanAnswer = rule.permission === undefined ? 'yes' : side.held(rule.permission);
		return and(subject, source);
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

function anchorId(entity: string, key: string): string {
	return `${entity}/${key}`;
}

/**
 * What a row's access parent names: the entity and key of the row it sits
 * below, with the entity's definition, `null` for a row whose entity has no
 * access parent or whose reference is unset, and `undefined` when the row is
 * not known.
 */
export type ParentRef = { entity: string; key: string; def: EntityDef } | null | undefined;

/**
 * The rows above a row for `canAt`, nearest first: the row its access parent
 * names, the row that row's access parent names, and so on. `complete` is
 * false when the walk stopped at a row the client does not hold, so rows
 * further up are not known.
 */
export interface AccessChain {
	readonly rows: readonly { entity: string; key: string }[];
	readonly complete: boolean;
}

/**
 * Decides `permission` at the row of `entity` with `key`, given the rows
 * above it. The rule the server applies, with `'unknown'` where the client
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
	for (const row of chain.rows) {
		if (summary.holds(permission, row.entity, row.key)) return 'yes';
	}
	return chain.complete ? 'no' : 'unknown';
}

/**
 * The row of `def` with `key` as {@link decideRules} reads it from local
 * data: a permission is decided by {@link canAt} over `chain`, and a subject
 * field reads `row`, which is unknown when the client does not hold the row.
 */
export function ruleRowOf(
	summary: AccessSummary,
	def: EntityDef,
	key: string,
	row: Message | undefined,
	chain: AccessChain,
): RuleRow {
	return {
		held: (permission) => canAt(summary, permission, def.name, key, chain),
		subject: (name) => {
			if (row === undefined) return undefined;
			const field = def.schema.fields.find((f) => f.name === name);
			const value = field === undefined
				? undefined
				: fieldValueOf(row as unknown as Record<string, unknown>, field);
			return typeof value === 'string' ? value : '';
		},
	};
}

/**
 * The units a write changes to move a row of `def` to another access parent:
 * the marked field, or the oneof whose members are the cases. Empty for an
 * entity without an access parent.
 */
export function accessParentUnits(def: EntityDef): string[] {
	return [ ...new Set(accessParentFields(def.schema).map((f) => f.oneof?.name ?? f.name)) ];
}

const parentFields = new WeakMap<DescMessage, readonly DescField[]>();

/**
 * The fields of an entity marked `access_parent`: one field, the members of
 * one oneof, or none.
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
 * What a row's access parent names, from the case the row holds. `row` is
 * the row's value, or undefined when the row is not held locally.
 */
export function parentRefOf(def: EntityDef, row: Message | undefined): ParentRef {
	const fields = accessParentFields(def.schema);
	if (fields.length === 0) return null;
	if (row === undefined) return undefined;
	for (const field of fields) {
		const value = fieldValueOf(row as unknown as Record<string, unknown>, field);
		if (value === undefined || value === null) continue;
		// The cases inside one reference message share the field, and only the
		// active one yields a stub.
		for (const shape of refShapes(field)) {
			const stub = refStub(shape, value as Message);
			if (stub === undefined) continue;
			const target = targetDefOf(shape.targetSchema);
			const key = keyOf(target, stub);
			return key === '' ? null : { entity: target.name, key, def: target };
		}
	}
	return null;
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
	const touches = (crud: CrudMutation): boolean => {
		const def = registry.get(crud.entity);
		if (def?.grant) return true;
		return crud.op === CrudOp.DELETE && anchors.has(crud.entity);
	};
	for (const m of pending) {
		switch (m.payload.case) {
			case 'crud':
				if (touches(m.payload.value)) return true;
				break;
			case 'batch':
				if (m.payload.value.mutations.some(touches)) return true;
				break;
		}
	}
	return false;
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
