// Runtime reference resolvers built off the EntityRegistry's reference graph.
//
// A reference stores only its target's key fields (see mutations.projectRefs);
// these resolvers turn that stored key back into the live target row from the
// merged optimistic view. Everything here is ORPHAN-TOLERANT: a missing target
// (deleted, never synced, or out of scope) resolves to undefined/empty, never
// an error — a missing row is indistinguishable from "not synced here".
//
// A stored reference resolves in the workspace it names: the holder's own for
// an entity-typed stub, which carries key fields alone and so can only address
// its own partition, and the workspace a reference message records when it
// records one (see (syncplane.v1.reference)). Every resolver therefore takes
// the holder's workspace and resolves each stub in the workspace it points
// into, from whatever this client holds.

import type { DescMessage, Message } from '@bufbuild/protobuf';

import {
	type EntityRegistry,
	fieldValueOf,
	type ForwardRef,
	keyOf,
	refQualified,
	refStub,
	refWorkspaceOf,
	type ReverseRef,
} from './mutations.js';
import type { Cond, OrderKey } from './query/ast.js';
import { and, compareRows, compileQuery, evalCond, hasRel, type Query } from './query/index.js';
import { prepareQuery } from './query/wire.js';
import type { Row } from './read.js';

/** The slice of SyncClient the resolvers need; lets them be tested in isolation. */
export interface RefClient {
	/**
	 * Rows of one entity from one workspace, read together. The result is
	 * parallel to `keys`, with undefined in the slot of every key that resolves
	 * to no row; duplicate keys each get their own slot.
	 */
	getRows(
		schema: DescMessage,
		workspaceId: string,
		keys: readonly string[],
	): Promise<(Row<Message> | undefined)[]>;
	query(schema: DescMessage, workspaceId: string, q: Query<Message>): Promise<Row<Message>[]>;
	/** A read across every workspace the client holds; what a workspace-qualified reverse relation walks. */
	queryAll(schema: DescMessage, q: Query<Message>): Promise<Row<Message>[]>;
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
 * The stored values a holder row carries for one case of a forward relation:
 * none for an unset field or an inactive case, one for a to-one, one per
 * element for a to-many. A polymorphic reference message's elements are read
 * by every case, and each element answers to the one it holds (see refStub).
 */
function storedValues(holder: Message, fwd: ForwardRef): Message[] {
	const at = fieldValueOf(holder as unknown as Rec, fwd.field);
	if (at === undefined || at === null) return [];
	return Array.isArray(at) ? at as Message[] : [ at as Message ];
}

/**
 * Resolves a forward relation of a holder row to its live target(s). To-one
 * returns the target row or undefined (unset/orphan); to-many returns the rows
 * that resolve (orphans dropped). A polymorphic relation resolves the case the
 * row holds, in that case's target entity.
 */
export async function loadForward(
	client: RefClient,
	registry: EntityRegistry,
	sourceEntity: string,
	workspaceId: string,
	relation: string,
	holder: Message,
): Promise<Row<Message> | Row<Message>[] | undefined> {
	const cases = registry.forwardCasesOf(sourceEntity, relation);
	if (cases.length === 0) {
		throw new Error(`entity "${sourceEntity}" has no reference field "${relation}"`);
	}
	const resolve = async (fwd: ForwardRef, value: Message): Promise<Row<Message> | undefined> => {
		const stub = refStub(fwd.shape, value);
		if (stub === undefined) return undefined;
		const rows = await client.getRows(
			fwd.targetSchema,
			refWorkspaceOf(fwd.shape, value, workspaceId),
			[ keyOf(registry.get(fwd.targetEntity)!, stub) ],
		);
		return rows[0];
	};
	const resolved: (Row<Message> | undefined)[] = [];
	for (const fwd of cases) {
		resolved.push(
			...await Promise.all(storedValues(holder, fwd).map((value) => resolve(fwd, value))),
		);
	}
	const rows = resolved.filter((r): r is Row<Message> => r !== undefined);
	return cases[0]!.repeated ? rows : rows[0];
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
 *
 * A workspace-qualified reference is held by rows in any workspace the client
 * holds, so its sources are read across all of them and each stub is checked
 * for the workspace it resolves in as well as its key. The key equality is
 * still pushed down where the shape allows; the workspace test runs over the
 * rows it returns.
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
	const qualified = refQualified(rev.shape);
	const run = (query: Query<Message>) =>
		qualified
			? client.queryAll(rev.sourceSchema, query)
			: client.query(rev.sourceSchema, workspaceId, query);

	const pushKey = !rev.repeated && targetDef.keyFields.length === 1;
	let where = q?.where;
	if (pushKey) {
		const keyFieldLocal = targetDef.keyFields[0]!.localName;
		const keyValue = (target as unknown as Rec)[keyFieldLocal];
		where = ((s: Rec) => {
			const pred = viaRef(s, rev).eq(keyValue);
			return q?.where ? and(pred, q.where(s as never)) : pred;
		}) as never;
	}
	if (pushKey && !qualified) {
		return run({ where, orderBy: q?.orderBy, limit: q?.limit, offset: q?.offset });
	}

	// Scan-and-filter fallback. limit/offset are applied AFTER the relation
	// filter so they page the matched rows — the same rows the pushdown path
	// would have paged. orderBy still runs in the query, and filtering
	// preserves relative order, so the page is ordered either way.
	const holderKey = keyOf(targetDef, target);
	const candidates = await run({ where, orderBy: q?.orderBy });
	const matched = candidates.filter((row) =>
		refPointsAt(row.value, rev, targetDef, holderKey, workspaceId, row.workspaceId)
	);
	const offset = q?.offset ?? 0;
	return q?.limit === undefined ? matched.slice(offset) : matched.slice(offset, offset + q.limit);
}

/**
 * Whether a source row's ref field references holderKey in targetWorkspace, by
 * re-deriving each stub's key with the TARGET entity's key derivation and the
 * workspace it resolves in from the holder's. Mirrors the server's
 * `refPointsAt` so both sides agree on what a reverse hop matches.
 */
function refPointsAt(
	source: Message,
	rev: ReverseRef,
	targetDef: Parameters<typeof keyOf>[0],
	holderKey: string,
	targetWorkspace: string,
	holderWorkspace: string,
): boolean {
	return pointedKeys(source, rev, targetDef, targetWorkspace, holderWorkspace)
		.includes(holderKey);
}

/**
 * The query-builder reference to the case a reverse relation reverses: the
 * builder walked along the relation's viaPath — through the holder's oneof, or
 * into the reference message's — to the field the pushdown compares.
 */
function viaRef(s: Rec, rev: ReverseRef): { eq(v: unknown): Cond; in(v: unknown[]): Cond } {
	let at: unknown = s;
	for (const seg of rev.viaPath) at = (at as Rec)[seg];
	return at as { eq(v: unknown): Cond; in(v: unknown[]): Cond };
}

/**
 * Hydrates rows against an include tree, attaching resolved references under
 * each row's `refs`. A forward relation costs one batched read per workspace
 * its stubs point into, a reverse relation one query per workspace the rows
 * span — neither one per row. Each stub resolves in the workspace it points
 * into — the holder's own unless a workspace-qualified reference names another
 * — so hydrating a cross-workspace result set stays correct row by row.
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
 * A RefClient whose getRows asks only for the keys no earlier call has asked
 * for, and answers the rest — including the duplicates of a call still in
 * flight — from a cache of per-key promises. A batch the cache covers entirely
 * reads nothing. Queries pass through: batching leaves at most one reverse
 * query per (relation, workspace), nothing left to memoize.
 */
function memoized(client: RefClient): RefClient {
	const cache = new Map<string, Promise<Row<Message> | undefined>>();
	return {
		getRows: (schema, workspaceId, keys) => {
			const at = (key: string) => `${schema.typeName}\u0000${workspaceId}\u0000${key}`;
			const missing = [ ...new Set(keys.filter((key) => !cache.has(at(key)))) ];
			if (missing.length > 0) {
				const batch = client.getRows(schema, workspaceId, missing);
				// Cached before the batch is awaited, so a key repeated later in this
				// call — or by the next relation of this level — finds the promise
				// rather than issuing a second read.
				missing.forEach((key, i) => cache.set(at(key), batch.then((rows) => rows[i])));
			}
			return Promise.all(keys.map((key) => cache.get(at(key))!));
		},
		query: (schema, workspaceId, q) => client.query(schema, workspaceId, q),
		queryAll: (schema, q) => client.queryAll(schema, q),
	};
}

/** A stored stub reduced to what reads it back: the case it holds, its target's workspace and key. */
interface Want {
	target: ForwardRef;
	workspaceId: string;
	key: string;
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
			const cases = registry.forwardCasesOf(entity, relation);
			if (cases.length > 0) return attachForward(client, registry, cases, relation, holders, node);
			const rev = registry.reverseRef(entity, relation);
			if (rev) return attachReverse(client, registry, entity, rev, relation, holders, node);
			throw new Error(
				`include: entity "${entity}" has no reference or reverse relation "${relation}"`,
			);
		}),
	);
}

/**
 * Resolves a forward relation for all holders in one batched read per target
 * entity and workspace its stubs point into, whatever the holder count. A
 * target named by several holders — or twice by one repeated ref — is read once
 * and `shared` hands every holder the same hydrated object. Options are
 * evaluated in memory over the resolved rows: a batched read is not a query to
 * push them into.
 *
 * A polymorphic relation arrives as its cases. Each holder's stored values are
 * read by every case and answer to the one they hold, so a to-one row resolves
 * in whichever target entity its case names, and a to-many row's elements may
 * each resolve in a different one. Options on a polymorphic list relation are
 * refused: the cases resolve to rows of different entities, and a condition or
 * an ordering is written against one.
 */
async function attachForward(
	client: RefClient,
	registry: EntityRegistry,
	cases: ForwardRef[],
	relation: string,
	holders: HydratedRow[],
	node: RelationNode,
): Promise<void> {
	const repeated = cases[0]!.repeated;
	if (!repeated && node.hasOptions) {
		throw new Error(
			`include: relation "${relation}" resolves to one row; `
				+ `$where/$orderBy/$limit/$offset need a list relation`,
		);
	}
	if (cases.length > 1 && node.hasOptions) {
		throw new Error(
			`include: relation "${relation}" resolves to rows of ${
				cases.map((c) => `"${c.targetEntity}"`).join(' or ')
			}; $where/$orderBy/$limit/$offset need a relation to one entity`,
		);
	}
	const opts = node.hasOptions ? prepareNodeOptions(cases[0]!.targetSchema, node) : undefined;
	const shared: SharedRows = new Map();

	// Every stub of every holder, in holder order and — for a repeated ref — in
	// element order, so the resolved rows scatter back positionally. An unset
	// stub keeps its slot rather than collapsing the list it sits in.
	const wanted: (Want | undefined)[][] = holders.map((holder) => {
		const out: (Want | undefined)[] = [];
		for (const fwd of cases) {
			const targetDef = registry.get(fwd.targetEntity)!;
			for (const value of storedValues(holder.value as Message, fwd)) {
				const stub = refStub(fwd.shape, value);
				// A to-one polymorphic message with no case set keeps its one
				// slot; an element of a to-many one that holds no case is a
				// stub pointing nowhere, like any orphan.
				if (stub === undefined) {
					if (!repeated) out.push(undefined);
					continue;
				}
				out.push({
					target: fwd,
					workspaceId: refWorkspaceOf(fwd.shape, value, holder.workspaceId),
					key: keyOf(targetDef, stub),
				});
			}
		}
		if (!repeated && out.length === 0) out.push(undefined);
		return out;
	});

	// One read per target entity and workspace the stubs name — the holder's
	// own for an entity-typed stub, another for every workspace a qualified
	// reference points into.
	const keysByRead = new Map<string, string[]>();
	const slots = new Map<string, Map<string, number>>();
	const readOf = (w: Want): string => `${w.target.targetSchema.typeName}\u0000${w.workspaceId}`;
	for (const list of wanted) {
		for (const w of list) {
			if (w === undefined) continue;
			const read = readOf(w);
			let keys = keysByRead.get(read);
			if (keys === undefined) {
				keysByRead.set(read, keys = []);
				slots.set(read, new Map());
			}
			const index = slots.get(read)!;
			if (!index.has(w.key)) index.set(w.key, keys.push(w.key) - 1);
		}
	}
	const rowsByRead = new Map<string, (Row<Message> | undefined)[]>();
	const reads = new Map<string, Want>();
	for (const list of wanted) {
		for (const w of list) if (w !== undefined && !reads.has(readOf(w))) reads.set(readOf(w), w);
	}
	await Promise.all([ ...reads.entries() ].map(async ([ read, w ]) => {
		rowsByRead.set(
			read,
			await client.getRows(w.target.targetSchema, w.workspaceId, keysByRead.get(read)!),
		);
	}));
	const resolved = (w: Want | undefined): Row<Message> | undefined => {
		if (w === undefined) return undefined;
		return rowsByRead.get(readOf(w))![slots.get(readOf(w))!.get(w.key)!];
	};

	holders.forEach((holder, i) => {
		const list = wanted[i]!;
		if (repeated) {
			let rows = list.map(resolved).filter((r): r is Row<Message> => r !== undefined);
			if (opts) rows = applyOptions(rows, opts, cases[0]!.targetSchema);
			holder.refs[relation] = rows.map((row) => sharedHydrated(shared, row));
		} else {
			const row = list.map(resolved).find((r) => r !== undefined);
			holder.refs[relation] = row === undefined ? undefined : sharedHydrated(shared, row);
		}
	});
	if (node.nested) {
		// The nested tree is resolved against each target entity the rows
		// belong to: a case's rows continue through that entity's relations.
		const byEntity = new Map<string, HydratedRow[]>();
		for (const row of sharedValues(shared)) {
			const entity = registry.bySchema(schemaOf(cases, row)).name;
			let group = byEntity.get(entity);
			if (group === undefined) byEntity.set(entity, group = []);
			group.push(row);
		}
		await Promise.all(
			[ ...byEntity.entries() ].map(([ entity, rows ]) =>
				attachRelations(client, registry, entity, rows, node.nested!)
			),
		);
	}
}

/** The schema of a resolved target row, from the case that read it. */
function schemaOf(cases: ForwardRef[], row: Row<unknown>): DescMessage {
	if (cases.length === 1) return cases[0]!.targetSchema;
	const typeName = (row.value as Message).$typeName;
	return cases.find((c) => c.targetSchema.typeName === typeName)!.targetSchema;
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
	const shared: SharedRows = new Map();

	const byWs = new Map<string, HydratedRow[]>();
	for (const holder of holders) {
		let group = byWs.get(holder.workspaceId);
		if (group === undefined) byWs.set(holder.workspaceId, group = []);
		group.push(holder);
	}

	// A workspace-qualified reference is held by rows in any workspace the
	// client holds, so each group's sources are read across all of them; the
	// workspace a stub resolves in is then checked alongside its key.
	const qualified = refQualified(rev.shape);
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
		const run = (query: Query<Message>) =>
			qualified
				? client.queryAll(rev.sourceSchema, query)
				: client.query(rev.sourceSchema, workspaceId, query);

		let where = node.where;
		if (!rev.repeated && targetDef.keyFields.length === 1) {
			const keyFieldLocal = targetDef.keyFields[0]!.localName;
			const values = [ ...new Set(group.map((h) => (h.value as unknown as Rec)[keyFieldLocal])) ];
			where = (s: Rec) => {
				const pred = viaRef(s, rev).in(values);
				return node.where ? and(pred, node.where(s)) : pred;
			};
		}
		const rows = await run({ where: where as never, orderBy: node.orderBy });
		for (const row of rows) {
			const keys = pointedKeys(row.value, rev, targetDef, workspaceId, row.workspaceId);
			for (const k of keys) add(k, row);
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
 * Derived keys of every target in targetWorkspace that a source row's ref field
 * points at, deduplicated — a repeated ref naming one target twice matches it
 * once. A stub resolving in another workspace points at a different row
 * however its key reads, so it is left out; a stub names no workspace resolves
 * in the holder's.
 */
function pointedKeys(
	source: Message,
	rev: ReverseRef,
	targetDef: Parameters<typeof keyOf>[0],
	targetWorkspace: string,
	holderWorkspace: string,
): string[] {
	const field = fieldValueOf(source as unknown as Rec, rev.holderField);
	if (field === undefined || field === null) return [];
	const values = Array.isArray(field) ? field as Message[] : [ field as Message ];
	const out = new Set<string>();
	for (const value of values) {
		const stub = refStub(rev.shape, value);
		if (stub === undefined) continue;
		if (refWorkspaceOf(rev.shape, value, holderWorkspace) !== targetWorkspace) continue;
		out.add(keyOf(targetDef, stub));
	}
	return [ ...out ];
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
	// Hydration filters resolved rows in memory, with no read of its own for a
	// semi-join's pre-pass to ride on. Nest the include a level deeper instead.
	if (hasRel(compiled.where)) {
		throw new Error('include: $where cannot use matches(); nest the relation instead');
	}
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
