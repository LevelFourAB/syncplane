/**
 * Client-side predictions of the access model: whether the caller holds a
 * permission at a row, from the CallerAccess the server last sent for the
 * workspace. The server decides every read and write itself; these answers
 * only let an app shape its UI before it asks.
 */

import type { DescField, DescMessage, Message } from '@bufbuild/protobuf';

import { type CrudMutation, CrudOp, type Mutation } from './gen/syncplane/v1/mutation_pb.js';
import type { CallerAccess } from './gen/syncplane/v1/service_pb.js';
import {
	type EntityDef,
	entityDefOf,
	type EntityRegistry,
	fieldBehaviorOf,
	fieldValueOf,
	keyOf,
	refShapes,
	refStub,
} from './mutations.js';

/**
 * A prediction of whether the caller holds a permission at a row: `'yes'`,
 * `'no'`, or `'unknown'` when the client cannot tell — no summary has
 * arrived for the workspace, the row whose access parent decides it is not
 * held locally, or local writes the server has not acked yet can change the
 * caller's grants.
 */
export type CanAnswer = 'yes' | 'no' | 'unknown';

/**
 * How many rows up a row's chain of access parents `can` walks before it
 * answers `'unknown'`: the server's default `MaxAccessDepth`.
 */
export const MAX_ACCESS_CHAIN = 32;

/**
 * The entity name a summary write is announced under, in the touch tokens
 * that reach watchers and other tabs. Not an identifier, so it never names a
 * real entity.
 */
export const ACCESS_TOUCH_ENTITY = '#access';

/** A CallerAccess indexed for lookups. */
export class AccessSummary {
	private readonly roots = new Set<string>();
	private readonly anchors = new Map<string, Set<string>>();

	constructor(access: CallerAccess) {
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

const anchorSets = new WeakMap<EntityRegistry, Set<string>>();

/** The entities some case of some entity's access parent points at. */
function anchorEntities(registry: EntityRegistry): Set<string> {
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
