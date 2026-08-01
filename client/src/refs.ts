// Runtime reference resolvers built off the EntityRegistry's reference graph.
//
// A reference stores only its target's key fields (see mutations.projectRefs);
// these resolvers turn that stored key back into the live target row from the
// merged optimistic view. Everything here is ORPHAN-TOLERANT: a missing target
// (deleted, never synced, or out of scope) resolves to undefined/empty, never
// an error — a missing row is indistinguishable from "not synced here".
//
// References never cross a workspace: the workspace is the hard partition key
// and a stored ref carries only key fields, which are unique within one
// partition. Every resolver therefore takes the holder's workspace and resolves
// inside it.

import type { DescMessage, Message } from '@bufbuild/protobuf';

import { type EntityRegistry, keyOf } from './mutations.js';
import { and, type Query } from './query/index.js';
import type { Row } from './read.js';

/** The slice of SyncClient the resolvers need; lets them be tested in isolation. */
export interface RefClient {
	getRow(schema: DescMessage, workspaceId: string, key: string): Promise<Row<Message> | undefined>;
	query(schema: DescMessage, workspaceId: string, q: Query<Message>): Promise<Row<Message>[]>;
}

/** An ergonomic include tree: `{ project: true, blockedBy: { project: true } }`. */
export interface IncludeSpec {
	[relation: string]: true | IncludeSpec;
}

/** A row plus its resolved references (parallel to the row, never on the proto). */
export interface HydratedRow<T = unknown> extends Row<T> {
	refs: Record<string, HydratedRow | HydratedRow[] | undefined>;
}

type Rec = Record<string, unknown>;

/**
 * Resolves a forward reference of a holder row to its live target(s). To-one
 * returns the target row or undefined (unset/orphan); to-many returns the rows
 * that resolve (orphans dropped).
 */
export async function loadForward(
	client: RefClient,
	registry: EntityRegistry,
	sourceEntity: string,
	workspaceId: string,
	fieldLocalName: string,
	holder: Message,
): Promise<Row<Message> | Row<Message>[] | undefined> {
	const ref = registry.forwardRef(sourceEntity, fieldLocalName);
	if (!ref) throw new Error(`entity "${sourceEntity}" has no reference field "${fieldLocalName}"`);
	const targetDef = registry.get(ref.targetEntity)!;
	const rec = holder as unknown as Rec;
	if (ref.repeated) {
		const stubs = (rec[fieldLocalName] as Message[] | undefined) ?? [];
		const resolved = await Promise.all(
			stubs.map((stub) => client.getRow(ref.targetSchema, workspaceId, keyOf(targetDef, stub))),
		);
		return resolved.filter((r): r is Row<Message> => r !== undefined);
	}
	const stub = rec[fieldLocalName] as Message | undefined;
	if (stub === undefined) return undefined;
	return client.getRow(ref.targetSchema, workspaceId, keyOf(targetDef, stub));
}

/**
 * Resolves a reverse relation: the source rows whose forward ref points at this
 * target, ANDed with an optional caller query.
 *
 * Two strategies, because only one shape is expressible as a predicate. A
 * to-one ref onto a single-key target has a comparable scalar leaf, so it
 * becomes a scalar-eq the store can push down (prepareQuery expands the ref to
 * `ref.<targetKey>`). A to-many ref has no scalar leaf, and a composite-key ref
 * is deliberately incomparable in the query DSL (isSet-only, per the P1 ref
 * rules) — both fall back to scanning the source entity and re-deriving each
 * stub's key, which is what the server's reverse-follow hop does anyway
 * (`refPointsAt` in engine/server/closure.go; there is no secondary index on
 * either side). Comparing whole derived keys is also what makes composite
 * targets correct: a matching `id` under a different `ns` is a different row.
 */
export async function resolveReverse(
	client: RefClient,
	registry: EntityRegistry,
	targetEntity: string,
	workspaceId: string,
	relationName: string,
	target: Message,
	q?: Query<Message>,
): Promise<Row<Message>[]> {
	const rev = registry.reverseRef(targetEntity, relationName);
	if (!rev) throw new Error(`entity "${targetEntity}" has no reverse relation "${relationName}"`);
	const targetDef = registry.get(targetEntity)!;
	const viaLocal = rev.viaField;

	if (!rev.repeated && targetDef.keyFields.length === 1) {
		const keyFieldLocal = targetDef.keyFields[0]!.localName;
		const keyValue = (target as unknown as Rec)[keyFieldLocal];
		const where = (s: Record<string, { eq(v: unknown): ReturnType<typeof and> }>) => {
			const pred = s[viaLocal]!.eq(keyValue);
			return q?.where ? and(pred, q.where(s as never)) : pred;
		};
		return client.query(rev.sourceSchema, workspaceId, {
			where: where as never,
			orderBy: q?.orderBy,
			limit: q?.limit,
			offset: q?.offset,
		});
	}

	// Scan-and-filter fallback. limit/offset are applied AFTER the relation
	// filter so they page the matched rows — the same rows the pushdown path
	// would have paged. orderBy still runs in the query, and filtering
	// preserves relative order, so the page is ordered either way.
	const holderKey = keyOf(targetDef, target);
	const candidates = await client.query(rev.sourceSchema, workspaceId, {
		where: q?.where,
		orderBy: q?.orderBy,
	});
	const matched = candidates.filter((row) =>
		refPointsAt(row.value, viaLocal, targetDef, holderKey, rev.repeated)
	);
	const offset = q?.offset ?? 0;
	return q?.limit === undefined ? matched.slice(offset) : matched.slice(offset, offset + q.limit);
}

/**
 * Whether a source row's ref field references holderKey, by re-deriving each
 * stub's key with the TARGET entity's key derivation. Mirrors the server's
 * `refPointsAt` so both sides agree on what a reverse hop matches.
 */
function refPointsAt(
	source: Message,
	viaLocal: string,
	targetDef: Parameters<typeof keyOf>[0],
	holderKey: string,
	repeated: boolean,
): boolean {
	const value = (source as unknown as Rec)[viaLocal];
	if (value === undefined || value === null) return false;
	if (repeated) {
		return (value as Message[]).some((stub) => keyOf(targetDef, stub) === holderKey);
	}
	return keyOf(targetDef, value as Message) === holderKey;
}

/**
 * Hydrates rows against an include tree, attaching resolved references under
 * each row's `refs`. Forward relations point-look-up the target; reverse
 * relations scan the source. Nested includes recurse into the resolved rows.
 * Each row's references resolve inside that row's own workspace, so hydrating a
 * cross-workspace result set stays correct row by row.
 */
export async function hydrate(
	client: RefClient,
	registry: EntityRegistry,
	entity: string,
	rows: Row<Message>[],
	include: IncludeSpec,
): Promise<HydratedRow[]> {
	return Promise.all(rows.map((row) => hydrateRow(client, registry, entity, row, include)));
}

async function hydrateRow(
	client: RefClient,
	registry: EntityRegistry,
	entity: string,
	row: Row<Message>,
	include: IncludeSpec,
): Promise<HydratedRow> {
	const refs: Record<string, HydratedRow | HydratedRow[] | undefined> = {};
	for (const [ relation, nested ] of Object.entries(include)) {
		const fwd = registry.forwardRef(entity, relation);
		if (fwd) {
			const resolved = await loadForward(
				client,
				registry,
				entity,
				row.workspaceId,
				relation,
				row.value,
			);
			refs[relation] = await hydrateForward(client, registry, fwd.targetEntity, resolved, nested);
			continue;
		}
		const rev = registry.reverseRef(entity, relation);
		if (rev) {
			const matched = await resolveReverse(
				client,
				registry,
				entity,
				row.workspaceId,
				relation,
				row.value,
			);
			refs[relation] = await hydrateList(client, registry, rev.sourceEntity, matched, nested);
			continue;
		}
		throw new Error(`include: entity "${entity}" has no reference or reverse relation "${relation}"`);
	}
	return { ...row, refs };
}

async function hydrateForward(
	client: RefClient,
	registry: EntityRegistry,
	targetEntity: string,
	resolved: Row<Message> | Row<Message>[] | undefined,
	nested: true | IncludeSpec,
): Promise<HydratedRow | HydratedRow[] | undefined> {
	if (resolved === undefined) return undefined;
	if (Array.isArray(resolved)) return hydrateList(client, registry, targetEntity, resolved, nested);
	if (nested === true) return toHydrated(resolved);
	return (await hydrate(client, registry, targetEntity, [ resolved ], nested))[0];
}

async function hydrateList(
	client: RefClient,
	registry: EntityRegistry,
	entity: string,
	rows: Row<Message>[],
	nested: true | IncludeSpec,
): Promise<HydratedRow[]> {
	if (nested === true) return rows.map(toHydrated);
	return hydrate(client, registry, entity, rows, nested);
}

function toHydrated(row: Row<Message>): HydratedRow {
	return { ...row, refs: {} };
}
