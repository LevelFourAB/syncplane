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

import { type EntityRegistry, type ForwardRef, keyOf, type ReverseRef } from './mutations.js';
import type { Cond, OrderKey } from './query/ast.js';
import { and, compareRows, compileQuery, evalCond, type Query } from './query/index.js';
import { prepareQuery } from './query/wire.js';
import type { Row } from './read.js';

/** The slice of SyncClient the resolvers need; lets them be tested in isolation. */
export interface RefClient {
	getRow(schema: DescMessage, workspaceId: string, key: string): Promise<Row<Message> | undefined>;
	query(schema: DescMessage, workspaceId: string, q: Query<Message>): Promise<Row<Message>[]>;
}

/**
 * An include tree of bare relation names: `{ project: true, blockedBy:
 * { project: true } }`. The shape a subscription's `follow` takes; a query
 * `include` additionally accepts per-relation options (see Include).
 */
export interface IncludeSpec {
	[relation: string]: true | IncludeSpec;
}

// Deliberately loose, like SpecRef: an include tree names its relations as
// plain strings, so there is no schema for TypeScript to bind the target's
// field types to.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type IncludeRef = any;

/** Condition over a relation's target entity, same shape as a query `where`. */
export type IncludeWhere = (w: IncludeRef) => Cond;

/** Ordering over a relation's target entity, same shape as a query `orderBy`. */
export type IncludeOrderBy = (w: IncludeRef) => OrderKey[];

/**
 * An include tree whose relations may carry options. A key is a relation name
 * — a forward ref field or a reverse relation — and its value is `true`, a
 * nested tree, or a tree carrying `$`-prefixed options alongside further
 * relations:
 *
 * ```ts
 * {
 *   project: true,
 *   comments: { $orderBy: (c) => [ c.createdAt.desc() ], $limit: 5, author: true },
 * }
 * ```
 *
 * `$where` keeps only target rows matching the condition, `$orderBy` orders
 * them and `$limit`/`$offset` page them — each holder's list independently,
 * with the same semantics as a query over the target entity. Options need a
 * relation that resolves to a list (a reverse relation or a to-many ref);
 * hydration throws on a to-one ref, and on an option key at the tree root. A
 * `$` key can never collide with a relation: no proto field or derived
 * relation name starts with it.
 *
 * Every IncludeSpec is a valid Include. The option callbacks receive an
 * untyped field reference — relation names are plain strings, so there is no
 * schema to infer the target's fields from.
 */
export interface Include {
	[relation: string]:
		| true
		| Include
		| IncludeWhere
		| IncludeOrderBy
		| number
		| undefined;
	$where?: IncludeWhere;
	$orderBy?: IncludeOrderBy;
	$limit?: number;
	$offset?: number;
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
 * each row's `refs`. Forward relations resolve by point lookup, deduplicated
 * across the whole call; a reverse relation costs one query per workspace the
 * rows span, never one per row. Each row's references resolve inside that
 * row's own workspace, so hydrating a cross-workspace result set stays correct
 * row by row.
 *
 * Rows referencing one target through the same relation get the SAME hydrated
 * object, and its nested relations resolve once — safe because delivered rows
 * are read-only (mutating one would edit every holder's view). The scope is
 * one hydrate — reads under it are not a snapshot (each takes its own shared
 * read), so a longer-lived cache could serve a row staler than the store.
 *
 * Relation options (`$where`/`$orderBy`/`$limit`/`$offset`, see Include) apply
 * to each holder's list independently; hydration throws when one is set on a
 * to-one ref, or when an option key sits at the tree root.
 */
export async function hydrate(
	client: RefClient,
	registry: EntityRegistry,
	entity: string,
	rows: Row<Message>[],
	include: Include,
): Promise<HydratedRow[]> {
	const holders = rows.map(toHydrated);
	await attachRelations(memoized(client), registry, entity, holders, include);
	return holders;
}

/**
 * A RefClient whose getRow answers repeats — and concurrent duplicates, by
 * sharing the promise — from a cache. Queries pass through: batching leaves at
 * most one reverse query per (relation, workspace), nothing left to memoize.
 */
function memoized(client: RefClient): RefClient {
	const cache = new Map<string, Promise<Row<Message> | undefined>>();
	return {
		getRow: (schema, workspaceId, key) => {
			const at = `${schema.typeName}\u0000${workspaceId}\u0000${key}`;
			let row = cache.get(at);
			if (row === undefined) {
				row = client.getRow(schema, workspaceId, key);
				cache.set(at, row);
			}
			return row;
		},
		query: (schema, workspaceId, q) => client.query(schema, workspaceId, q),
	};
}

/** One include node, split into its options and the nested tree beneath them. */
interface RelationNode {
	where?: IncludeWhere;
	orderBy?: IncludeOrderBy;
	limit?: number;
	offset?: number;
	hasOptions: boolean;
	nested?: Include;
}

const OPTION_KEYS = new Set([ '$where', '$orderBy', '$limit', '$offset' ]);

function parseNode(relation: string, value: Exclude<Include[string], undefined>): RelationNode {
	if (value === true) return { hasOptions: false };
	if (typeof value !== 'object' || value === null) {
		throw new Error(`include: relation "${relation}" must be true or a nested include tree`);
	}
	const node: RelationNode = { hasOptions: false };
	for (const [ key, v ] of Object.entries(value)) {
		if (v === undefined) continue;
		if (!key.startsWith('$')) {
			(node.nested ??= {})[key] = v as Include[string];
			continue;
		}
		if (!OPTION_KEYS.has(key)) {
			throw new Error(`include: relation "${relation}" carries unknown option "${key}"`);
		}
		node.hasOptions = true;
		switch (key) {
			case '$where':
				node.where = v as IncludeWhere;
				break;
			case '$orderBy':
				node.orderBy = v as IncludeOrderBy;
				break;
			case '$limit':
				node.limit = v as number;
				break;
			default:
				node.offset = v as number;
		}
	}
	return node;
}

/** Resolves every relation of one include level for the given holders. */
async function attachRelations(
	client: RefClient,
	registry: EntityRegistry,
	entity: string,
	holders: HydratedRow[],
	include: Include,
): Promise<void> {
	if (holders.length === 0) return;
	await Promise.all(
		Object.entries(include).map(([ relation, value ]) => {
			if (value === undefined) return undefined;
			if (relation.startsWith('$')) {
				throw new Error(`include: option "${relation}" applies to a relation, not the tree root`);
			}
			const node = parseNode(relation, value);
			const fwd = registry.forwardRef(entity, relation);
			if (fwd) return attachForward(client, registry, fwd, relation, holders, node);
			const rev = registry.reverseRef(entity, relation);
			if (rev) return attachReverse(client, registry, entity, rev, relation, holders, node);
			throw new Error(
				`include: entity "${entity}" has no reference or reverse relation "${relation}"`,
			);
		}),
	);
}

/**
 * Resolves a forward relation for all holders. Stub keys go through the
 * memoized point reads, so a target shared between holders is read once, and
 * `shared` hands every holder the same hydrated object. Options are evaluated
 * in memory over the resolved rows — the targets are point reads, so there is
 * no query to push them into.
 */
async function attachForward(
	client: RefClient,
	registry: EntityRegistry,
	fwd: ForwardRef,
	relation: string,
	holders: HydratedRow[],
	node: RelationNode,
): Promise<void> {
	if (!fwd.repeated && node.hasOptions) {
		throw new Error(
			`include: relation "${relation}" resolves to one row; `
				+ `$where/$orderBy/$limit/$offset need a list relation`,
		);
	}
	const targetDef = registry.get(fwd.targetEntity)!;
	const opts = node.hasOptions ? prepareNodeOptions(fwd.targetSchema, node) : undefined;
	const shared: SharedRows = new Map();
	await Promise.all(holders.map(async (holder) => {
		const rec = holder.value as unknown as Rec;
		if (fwd.repeated) {
			const stubs = (rec[relation] as Message[] | undefined) ?? [];
			const resolved = await Promise.all(
				stubs.map((stub) =>
					client.getRow(fwd.targetSchema, holder.workspaceId, keyOf(targetDef, stub))
				),
			);
			let rows = resolved.filter((r): r is Row<Message> => r !== undefined);
			if (opts) rows = applyOptions(rows, opts, fwd.targetSchema);
			holder.refs[relation] = rows.map((row) => sharedHydrated(shared, row));
		} else {
			const stub = rec[relation] as Message | undefined;
			const row = stub === undefined
				? undefined
				: await client.getRow(fwd.targetSchema, holder.workspaceId, keyOf(targetDef, stub));
			holder.refs[relation] = row === undefined ? undefined : sharedHydrated(shared, row);
		}
	}));
	if (node.nested) {
		await attachRelations(client, registry, fwd.targetEntity, sharedValues(shared), node.nested);
	}
}

/**
 * Resolves a reverse relation for all holders — one query per workspace the
 * holders span, not one per holder.
 *
 * Same two strategies as resolveReverse, batched: the pushdown shape becomes a
 * membership test over every holder's key (`via IN (k1..kn)`), and the
 * fallback scans once, bucketing each source row under the target keys it
 * points at. `$where`/`$orderBy` join the query on both paths so an index can
 * serve them; `$limit`/`$offset` page each holder's bucket, not the query, so
 * they apply after bucketing — to the same rows per-holder queries would have
 * paged.
 */
async function attachReverse(
	client: RefClient,
	registry: EntityRegistry,
	targetEntity: string,
	rev: ReverseRef,
	relation: string,
	holders: HydratedRow[],
	node: RelationNode,
): Promise<void> {
	const targetDef = registry.get(targetEntity)!;
	const viaLocal = rev.viaField;
	const shared: SharedRows = new Map();

	const byWs = new Map<string, HydratedRow[]>();
	for (const holder of holders) {
		let group = byWs.get(holder.workspaceId);
		if (group === undefined) byWs.set(holder.workspaceId, group = []);
		group.push(holder);
	}

	await Promise.all([ ...byWs.entries() ].map(async ([ workspaceId, group ]) => {
		const wanted = new Map<string, HydratedRow[]>();
		for (const holder of group) {
			const k = keyOf(targetDef, holder.value as Message);
			let list = wanted.get(k);
			if (list === undefined) wanted.set(k, list = []);
			list.push(holder);
		}

		const matched = new Map<string, Row<Message>[]>();
		const add = (k: string, row: Row<Message>): void => {
			if (!wanted.has(k)) return;
			let list = matched.get(k);
			if (list === undefined) matched.set(k, list = []);
			list.push(row);
		};

		if (!rev.repeated && targetDef.keyFields.length === 1) {
			const keyFieldLocal = targetDef.keyFields[0]!.localName;
			const values = [ ...new Set(group.map((h) => (h.value as unknown as Rec)[keyFieldLocal])) ];
			const where = (s: Record<string, { in(v: unknown[]): Cond }>) => {
				const pred = s[viaLocal]!.in(values);
				return node.where ? and(pred, node.where(s)) : pred;
			};
			const rows = await client.query(rev.sourceSchema, workspaceId, {
				where: where as never,
				orderBy: node.orderBy,
			});
			for (const row of rows) {
				const stub = (row.value as unknown as Rec)[viaLocal] as Message | undefined;
				if (stub === undefined || stub === null) continue;
				add(keyOf(targetDef, stub), row);
			}
		} else {
			const rows = await client.query(rev.sourceSchema, workspaceId, {
				where: node.where,
				orderBy: node.orderBy,
			});
			for (const row of rows) {
				for (const k of pointedKeys(row.value, viaLocal, targetDef, rev.repeated)) add(k, row);
			}
		}

		for (const [ k, hs ] of wanted) {
			const page = pageOf(matched.get(k) ?? [], node);
			const hydratedList = page.map((row) => sharedHydrated(shared, row));
			for (const holder of hs) holder.refs[relation] = hydratedList;
		}
	}));

	if (node.nested) {
		await attachRelations(client, registry, rev.sourceEntity, sharedValues(shared), node.nested);
	}
}

/**
 * Derived keys of every target a source row's ref field points at,
 * deduplicated — a repeated ref naming one target twice matches it once, as
 * refPointsAt's `some` does. Same key derivation as refPointsAt, in bulk.
 */
function pointedKeys(
	source: Message,
	viaLocal: string,
	targetDef: Parameters<typeof keyOf>[0],
	repeated: boolean,
): string[] {
	const value = (source as unknown as Rec)[viaLocal];
	if (value === undefined || value === null) return [];
	if (!repeated) return [ keyOf(targetDef, value as Message) ];
	return [ ...new Set((value as Message[]).map((stub) => keyOf(targetDef, stub))) ];
}

/** Node options resolved against the relation's target schema. */
interface PreparedOptions {
	where?: Cond;
	orderBy?: OrderKey[];
	limit?: number;
	offset?: number;
}

function prepareNodeOptions(schema: DescMessage, node: RelationNode): PreparedOptions {
	const compiled = prepareQuery(
		schema,
		compileQuery({ where: node.where, orderBy: node.orderBy } as Query<Message>),
	);
	return {
		where: compiled.where,
		orderBy: compiled.orderBy,
		limit: node.limit,
		offset: node.offset,
	};
}

/** filter → sort → page, matching the query evaluator's semantics. */
function applyOptions(
	rows: Row<Message>[],
	opts: PreparedOptions,
	schema: DescMessage,
): Row<Message>[] {
	const where = opts.where;
	const orderBy = opts.orderBy;
	let out = where === undefined ? rows : rows.filter((r) => evalCond(r.value, where, schema));
	if (orderBy !== undefined && orderBy.length > 0) {
		out = [ ...out ].sort((a, b) => compareRows(a, b, orderBy, schema));
	}
	return pageOf(out, opts);
}

function pageOf<T>(rows: T[], page: { limit?: number; offset?: number }): T[] {
	const offset = page.offset ?? 0;
	return page.limit === undefined ? rows.slice(offset) : rows.slice(offset, offset + page.limit);
}

/** Hydrated rows shared within one relation, keyed workspace → row key. */
type SharedRows = Map<string, Map<string, HydratedRow>>;

function sharedValues(shared: SharedRows): HydratedRow[] {
	return [ ...shared.values() ].flatMap((perWs) => [ ...perWs.values() ]);
}

function sharedHydrated(shared: SharedRows, row: Row<Message>): HydratedRow {
	let perWs = shared.get(row.workspaceId);
	if (perWs === undefined) shared.set(row.workspaceId, perWs = new Map());
	let h = perWs.get(row.key);
	if (h === undefined) perWs.set(row.key, h = toHydrated(row));
	return h;
}

function toHydrated(row: Row<Message>): HydratedRow {
	return { ...row, refs: {} };
}
