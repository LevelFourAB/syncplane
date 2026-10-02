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
	keyOf,
	refShape,
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
 * The parent a row sits below for `canAt`: the entity and key its access
 * parent names, `null` for a row whose entity has no access parent or whose
 * reference is unset, and `undefined` when the row is not known.
 */
export type ParentRef = { entity: string; key: string } | null | undefined;

/**
 * Decides `permission` at the row of `entity` with `key`, given what the
 * row's access parent names. The rule the server applies, with `'unknown'`
 * where the client lacks an input.
 */
export function canAt(
	summary: AccessSummary | undefined,
	permission: string,
	entity: string,
	key: string,
	parent: ParentRef,
): CanAnswer {
	if (summary === undefined) return 'unknown';
	if (summary.holds(permission, entity, key)) return 'yes';
	if (parent === undefined) return 'unknown';
	if (parent === null) return 'no';
	return summary.holds(permission, parent.entity, parent.key) ? 'yes' : 'no';
}

const parentFields = new WeakMap<DescMessage, DescField | null>();

/** The field of an entity marked `access_parent`, or undefined when it has none. */
export function accessParentField(schema: DescMessage): DescField | undefined {
	let field = parentFields.get(schema);
	if (field === undefined) {
		field = schema.fields.find((f) => fieldBehaviorOf(f)?.accessParent !== undefined) ?? null;
		parentFields.set(schema, field);
	}
	return field ?? undefined;
}

/**
 * What a row's access parent names, for `canAt`. `row` is the row's value, or
 * undefined when the row is not held locally.
 */
export function parentRefOf(def: EntityDef, row: Message | undefined): ParentRef {
	const field = accessParentField(def.schema);
	if (field === undefined) return null;
	if (row === undefined) return undefined;
	const value = (row as unknown as Record<string, unknown>)[field.localName];
	const shape = refShape(field);
	if (value === undefined || value === null || shape === undefined) return null;
	const stub = refStub(shape, value as Message);
	if (stub === undefined) return null;
	const target = targetDefOf(shape.targetSchema);
	const key = keyOf(target, stub);
	return key === '' ? null : { entity: target.name, key };
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

/** The entities some entity's access parent points at. */
function anchorEntities(registry: EntityRegistry): Set<string> {
	let set = anchorSets.get(registry);
	if (set === undefined) {
		set = new Set();
		for (const name of registry.names()) {
			const def = registry.get(name)!;
			const field = accessParentField(def.schema);
			const target = field === undefined ? undefined : refShape(field)?.targetSchema;
			if (target !== undefined) set.add(targetDefOf(target).name);
		}
		anchorSets.set(registry, set);
	}
	return set;
}
