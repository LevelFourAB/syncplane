import type {
	DescField,
	DescMessage,
	Message,
	MessageInitShape,
	MessageShape,
} from '@bufbuild/protobuf';
import { ScalarType } from '@bufbuild/protobuf';

import type {
	CasOptions,
	HoldHandle,
	MaskedPatchOptions,
	PatchOptions,
	UpdateOptions,
	UpdateRecipe,
	WorkspaceClient,
} from './client.js';
import type { RowSet } from './coverage.js';
import { newId } from './id.js';
import type { EntityDef, EntityRegistry, PatchInit, TextInit } from './mutations.js';
import type {
	Group,
	GroupQuery,
	GroupRowsQuery,
	Query,
	RowGroup,
	Scalar,
	TotalOf,
} from './query/index.js';
import type { RefTargetOf } from './query/types.js';
import type { Row } from './read.js';
import type { Include } from './refs.js';
import type { TextHandle } from './text/handle.js';
import type { WatchCallback, WatchHandle, WatchOptions } from './watch.js';

/** Last segment of a fully-qualified proto type name. */
type SimpleName<T extends string> = T extends `${string}.${infer Rest}` ? SimpleName<Rest> : T;

/** Property name an entity gets on `client.db`: "example.todo.v1.Todo" → "todo". */
export type EntityKey<D extends DescMessage> = Uncapitalize<
	SimpleName<MessageShape<D>['$typeName']>
>;

/**
 * Reference accessors for an entity. A forward ref (a field that points at
 * another entity) exposes `.load(holder)` to resolve the live target(s); a
 * reverse relation (the auto-named inverse, surfaced on the target) is a
 * callable returning the source rows that point here.
 *
 * Forward refs are typed from the message shape, so `refs.project.load(issue)`
 * returns `Row<Project>` with no cast. Reverse relations cannot be: their names
 * are coined by the registry from the ref graph (issuesByAssignee), which the
 * generated types know nothing about, so they arrive through the index
 * signature as `Row<Message>[]`.
 *
 * The index signature also means an unknown name type-checks. It fails loudly
 * at runtime rather than as "cannot read properties of undefined" — including
 * for a message field that is not a registered entity, which looks like a
 * forward ref to the type system because reference-ness lives in a proto
 * option the TS types do not carry.
 */
export type ForwardRefAccessor = {
	load(holder: Message): Promise<Row<Message> | Row<Message>[] | undefined>;
};
export type ReverseRelAccessor = (target: Message, q?: Query<Message>) => Promise<Row<Message>[]>;

/** Field names of M whose type is a message — the ones that can be refs. */
type MessageFieldKeys<M> = {
	[K in Exclude<keyof M, '$typeName' | '$unknown'>]: [NonNullable<M[K]>] extends
		[{ $typeName: string }] ? K
		: [NonNullable<M[K]>] extends [ReadonlyArray<infer E>]
			? ([E] extends [{ $typeName: string }] ? K : never)
		: never;
}[Exclude<keyof M, '$typeName' | '$unknown'>];

/**
 * A to-one ref resolves to one row (or none); a to-many ref to a list. A
 * reference message resolves to the entity it wraps — see RefTargetOf.
 */
type TypedForwardRef<V, H> = [NonNullable<V>] extends [ReadonlyArray<infer E>]
	? { load(holder: H): Promise<Row<RefTargetOf<E>>[]> }
	: { load(holder: H): Promise<Row<RefTargetOf<NonNullable<V>>> | undefined> };

export type EntityRefs<D extends DescMessage = DescMessage> =
	& {
		[K in MessageFieldKeys<MessageShape<D>>]: TypedForwardRef<
			MessageShape<D>[K],
			MessageShape<D>
		>;
	}
	& Record<string, ForwardRefAccessor | ReverseRelAccessor>;

/** Schema-bound view of one entity. */
export interface EntityApi<D extends DescMessage> {
	get(key: string): Promise<MessageShape<D> | undefined>;
	getRow(key: string): Promise<Row<MessageShape<D>> | undefined>;
	/**
	 * Every row of the entity the local store holds. The result carries
	 * `coverage`: with a bounded subscription "every row it holds" is not
	 * "every row there is", and only that flag can tell the two apart.
	 */
	scan(): Promise<RowSet<MessageShape<D>>>;
	query<const Q extends Query<MessageShape<D>>>(
		q: Q,
		opts?: { include?: Include },
	): Promise<RowSet<MessageShape<D>, TotalOf<Q>>>;
	/**
	 * Runs the query AGAINST THE SERVER instead of the local store — the way to
	 * reach rows partial sync did not bring down (search across everything, open
	 * an id that is not in the current view).
	 *
	 * Results are server-state snapshots: not merged with the optimistic
	 * overlay, not stored locally, and they never feed watchers. Requires
	 * connectivity. Ordering matches the local evaluator exactly.
	 *
	 * Unavailable inside a transaction, which has no network of its own.
	 */
	queryServer<const Q extends Query<MessageShape<D>>>(
		q: Q,
	): Promise<RowSet<MessageShape<D>, TotalOf<Q>>>;
	/**
	 * Groups the matching rows by one path and counts each group — a board
	 * column's badge, a facet sidebar's numbers.
	 *
	 * Groups come back in ascending value order with the no-value group last.
	 * See {@link GroupQuery} for what a group is, and what a repeated `by` path
	 * does to the counts.
	 */
	groupCount(q: GroupQuery<MessageShape<D>>): Promise<Group[]>;
	/**
	 * The distinct values the matching rows hold at one path, in ascending order
	 * — what a filter dropdown offers. `undefined` is among them when some
	 * matching row holds no value there.
	 */
	distinct(q: GroupQuery<MessageShape<D>>): Promise<(Scalar | undefined)[]>;
	/** {@link EntityApi.groupCount} against the server, for rows partial sync did
	 * not bring down. See {@link EntityApi.queryServer} for what a server read
	 * does and does not do. */
	groupCountServer(q: GroupQuery<MessageShape<D>>): Promise<Group[]>;
	/** {@link EntityApi.distinct} against the server. */
	distinctServer(q: GroupQuery<MessageShape<D>>): Promise<(Scalar | undefined)[]>;
	/**
	 * The first rows of each group — a board column's preview, the top item of
	 * every project.
	 *
	 * The groups are {@link EntityApi.groupCount}'s; `q.orderBy` decides which
	 * rows come first inside one and `q.perGroup` how many. See
	 * {@link GroupRowsQuery} for what an index makes of the read.
	 */
	groupRows(q: GroupRowsQuery<MessageShape<D>>): Promise<RowGroup<MessageShape<D>>[]>;
	/** {@link EntityApi.groupRows} against the server, for rows partial sync did
	 * not bring down. */
	groupRowsServer(q: GroupRowsQuery<MessageShape<D>>): Promise<RowGroup<MessageShape<D>>[]>;
	/** {@link EntityApi.groupRows}, live: emits after every commit that moves a
	 * group's head. */
	watchGroupRows(
		q: GroupRowsQuery<MessageShape<D>>,
		cb: (groups: RowGroup<MessageShape<D>>[]) => void,
		opts?: { onError?: (error: unknown) => void },
	): WatchHandle;
	/** {@link EntityApi.groupCount}, live: emits after every commit that changes
	 * the groups. */
	watchGroupCount(
		q: GroupQuery<MessageShape<D>>,
		cb: (groups: Group[]) => void,
		opts?: { onError?: (error: unknown) => void },
	): WatchHandle;
	/** {@link EntityApi.distinct}, live. */
	watchDistinct(
		q: GroupQuery<MessageShape<D>>,
		cb: (values: (Scalar | undefined)[]) => void,
		opts?: { onError?: (error: unknown) => void },
	): WatchHandle;
	watch(cb: WatchCallback<MessageShape<D>>, opts?: WatchOptions): WatchHandle;
	watchQuery<const Q extends Query<MessageShape<D>>>(
		q: Q,
		cb: WatchCallback<MessageShape<D>, TotalOf<Q>>,
		opts?: WatchOptions,
	): WatchHandle;
	/**
	 * Opens a live handle over a collaborative text field
	 * ((syncplane.v1.field_behavior).text) of an existing row. The handle applies
	 * edits locally, ships them as ordinary PATCHes, and delivers remote
	 * changes as in-place deltas. Indices are UTF-16 string indices.
	 */
	text(key: string, field: string): Promise<TextHandle>;
	/**
	 * Brings one row into scope on demand until the handle is released — the
	 * way to open a row found by `queryServer` for editing without changing the
	 * declared subscriptions. See {@link WorkspaceClient.hold} for what a hold
	 * does and does not do.
	 */
	hold(key: string): HoldHandle;
	refs: EntityRefs<D>;
	mutate: {
		/**
		 * Insert a new entity, minting its id for you. The entity must have a
		 * single string key field; omit it (or leave it empty) and `create`
		 * fills it with a fresh time-ordered id, then PUTs the row. A key you
		 * do supply is respected. Returns the key that was written.
		 *
		 * For composite or non-string keys, or to set the key yourself, use `put`.
		 */
		create(value: TextInit<MessageInitShape<D>>, opts?: CasOptions): Promise<string>;
		put(value: MessageShape<D> | TextInit<MessageInitShape<D>>, opts?: CasOptions): Promise<void>;
		/**
		 * Merges the given fields into a row, leaving every other field alone.
		 * `init` carries the fields to change, not a whole row — see PatchInit,
		 * and `update` for read-modify-write.
		 *
		 * Does not create rows: a patch of a row that is absent or deleted changes
		 * nothing locally and is rejected as `conflict`. Use `put` to create.
		 */
		patch(key: string, init: PatchInit<D>, opts?: PatchOptions): Promise<void>;
		/**
		 * Merges named fields under a mask the caller supplies, from any message.
		 * The mask decides what is written, so this is the way to copy named
		 * fields off a row that was read.
		 */
		patch(
			key: string,
			init: TextInit<MessageInitShape<D>>,
			opts: MaskedPatchOptions,
		): Promise<void>;
		/**
		 * Read-modify-write: reads the row, hands a draft to `recipe`, and PATCHes
		 * only the fields the recipe changed. Nothing changed, nothing written;
		 * a missing row throws. See CrudMutateApi.update for why this, and not a
		 * PUT of an edited row, is the safe way to save an edit.
		 */
		update(key: string, recipe: UpdateRecipe<D>, opts?: UpdateOptions): Promise<void>;
		del(key: string, opts?: CasOptions): Promise<void>;
	};
}

export type EntityApis<E extends readonly DescMessage[]> = {
	[D in E[number] as EntityKey<D>]: EntityApi<D>;
};

/** The subset of SyncClient's schema-first API the facade delegates to. */
interface SchemaApi {
	get<D extends DescMessage>(schema: D, key: string): Promise<MessageShape<D> | undefined>;
	getRow<D extends DescMessage>(schema: D, key: string): Promise<Row<MessageShape<D>> | undefined>;
	scan<D extends DescMessage>(schema: D): Promise<RowSet<MessageShape<D>>>;
	query<D extends DescMessage, const Q extends Query<MessageShape<D>>>(
		schema: D,
		q: Q,
		opts?: { include?: Include },
	): Promise<RowSet<MessageShape<D>, TotalOf<Q>>>;
	/** Absent on facades with no transport of their own (transactions). */
	queryServer?<D extends DescMessage, const Q extends Query<MessageShape<D>>>(
		schema: D,
		q: Q,
	): Promise<RowSet<MessageShape<D>, TotalOf<Q>>>;
	groupCount<D extends DescMessage>(
		schema: D,
		q: GroupQuery<MessageShape<D>>,
	): Promise<Group[]>;
	distinct<D extends DescMessage>(
		schema: D,
		q: GroupQuery<MessageShape<D>>,
	): Promise<(Scalar | undefined)[]>;
	/** Absent on facades with no transport of their own (transactions). */
	groupCountServer?<D extends DescMessage>(
		schema: D,
		q: GroupQuery<MessageShape<D>>,
	): Promise<Group[]>;
	/** Absent on facades with no transport of their own (transactions). */
	distinctServer?<D extends DescMessage>(
		schema: D,
		q: GroupQuery<MessageShape<D>>,
	): Promise<(Scalar | undefined)[]>;
	groupRows<D extends DescMessage>(
		schema: D,
		q: GroupRowsQuery<MessageShape<D>>,
	): Promise<RowGroup<MessageShape<D>>[]>;
	/** Absent on facades with no transport of their own (transactions). */
	groupRowsServer?<D extends DescMessage>(
		schema: D,
		q: GroupRowsQuery<MessageShape<D>>,
	): Promise<RowGroup<MessageShape<D>>[]>;
	/** Absent on facades that cannot host a live read (transactions). */
	watchGroupRows?<D extends DescMessage>(
		schema: D,
		q: GroupRowsQuery<MessageShape<D>>,
		cb: (groups: RowGroup<MessageShape<D>>[]) => void,
		opts?: { onError?: (error: unknown) => void },
	): WatchHandle;
	/** Absent on facades that cannot host a live read (transactions). */
	watchGroupCount?<D extends DescMessage>(
		schema: D,
		q: GroupQuery<MessageShape<D>>,
		cb: (groups: Group[]) => void,
		opts?: { onError?: (error: unknown) => void },
	): WatchHandle;
	/** Absent on facades that cannot host a live read (transactions). */
	watchDistinct?<D extends DescMessage>(
		schema: D,
		q: GroupQuery<MessageShape<D>>,
		cb: (values: (Scalar | undefined)[]) => void,
		opts?: { onError?: (error: unknown) => void },
	): WatchHandle;
	watch<D extends DescMessage>(
		schema: D,
		cb: WatchCallback<MessageShape<D>>,
		opts?: WatchOptions,
	): WatchHandle;
	watchQuery<D extends DescMessage, const Q extends Query<MessageShape<D>>>(
		schema: D,
		q: Q,
		cb: WatchCallback<MessageShape<D>, TotalOf<Q>>,
		opts?: WatchOptions,
	): WatchHandle;
	resolveRef<D extends DescMessage>(
		holderSchema: D,
		fieldLocalName: string,
		holder: MessageShape<D>,
	): Promise<Row<Message> | Row<Message>[] | undefined>;
	resolveReverseRef<D extends DescMessage>(
		targetSchema: D,
		relationName: string,
		target: MessageShape<D>,
		q?: Query<Message>,
	): Promise<Row<Message>[]>;
	/** Absent on facades that cannot host a live handle (transactions). */
	textHandle?<D extends DescMessage>(schema: D, key: string, field: string): Promise<TextHandle>;
	/** Absent on facades that cannot change scope (transactions). */
	hold?<D extends DescMessage>(schema: D, key: string): HoldHandle;
	mutate: {
		put<D extends DescMessage>(
			schema: D,
			value: MessageShape<D> | TextInit<MessageInitShape<D>>,
			opts?: CasOptions,
		): Promise<void>;
		// Wide on purpose: this is the internal delegation seam, and the facade
		// above is where the narrowing (PatchInit) that users see is declared.
		patch<D extends DescMessage>(
			schema: D,
			key: string,
			init: TextInit<MessageInitShape<D>>,
			opts?: PatchOptions,
		): Promise<void>;
		update<D extends DescMessage>(
			schema: D,
			key: string,
			recipe: UpdateRecipe<D>,
			opts?: UpdateOptions,
		): Promise<void>;
		del(schema: DescMessage, key: string, opts?: CasOptions): Promise<void>;
	};
}

/** The error a read the transaction facade cannot host raises. */
function unavailable(entity: string, method: string, what: string): Error {
	return new Error(`client.db.${entity}.${method}: ${what} are not available in transactions`);
}

function uncapitalize(name: string): string {
	return name.charAt(0).toLowerCase() + name.slice(1);
}

/** The entity's key field iff it's a single string field — the only shape `create` can auto-fill. */
function singleStringKeyField(def: EntityDef): DescField | undefined {
	if (def.keyFields.length !== 1) return undefined;
	const f = def.keyFields[0]!;
	return f.fieldKind === 'scalar' && f.scalar === ScalarType.STRING ? f : undefined;
}

export function buildEntityApis<E extends readonly DescMessage[]>(
	entities: E,
	client: SchemaApi,
	registry: EntityRegistry,
): EntityApis<E> {
	const apis: Record<string, EntityApi<DescMessage>> = {};
	for (const schema of entities) {
		const key = uncapitalize(schema.name);
		if (Object.prototype.hasOwnProperty.call(apis, key)) {
			throw new Error(
				`client.db: "${schema.typeName}" maps to "${key}" which is already taken; `
					+ `use the schema-first API (client.get(Schema, ...) etc.) for entities with clashing names`,
			);
		}
		// refs is lazy: it touches the registry's ref graph only on first access,
		// so the simple-name collision check above stays the first failure.
		let refsCache: EntityRefs | undefined;
		// The lone string key field `create` auto-fills, resolved lazily (same as
		// refs) so the simple-name collision check above stays the first failure.
		// undefined = not yet computed; null = entity has no auto-fillable key.
		let autoKeyField: DescField | null | undefined;
		const resolveAutoKey = ():
			| DescField
			| null => (autoKeyField ??= singleStringKeyField(registry.bySchema(schema)) ?? null);
		apis[key] = {
			get: (k) => client.get(schema, k),
			getRow: (k) => client.getRow(schema, k),
			scan: () => client.scan(schema),
			query: (q, opts) => client.query(schema, q, opts),
			queryServer: (q) => {
				if (!client.queryServer) {
					throw new Error(
						`client.db.${key}.queryServer: server queries are not available in transactions`,
					);
				}
				return client.queryServer(schema, q);
			},
			groupCount: (q) => client.groupCount(schema, q),
			distinct: (q) => client.distinct(schema, q),
			groupRows: (q) => client.groupRows(schema, q),
			groupRowsServer: (q) => {
				if (!client.groupRowsServer) throw unavailable(key, 'groupRowsServer', 'server reads');
				return client.groupRowsServer(schema, q);
			},
			watchGroupRows: (q, cb, opts) => {
				if (!client.watchGroupRows) throw unavailable(key, 'watchGroupRows', 'live reads');
				return client.watchGroupRows(schema, q, cb, opts);
			},
			groupCountServer: (q) => {
				if (!client.groupCountServer) throw unavailable(key, 'groupCountServer', 'server reads');
				return client.groupCountServer(schema, q);
			},
			distinctServer: (q) => {
				if (!client.distinctServer) throw unavailable(key, 'distinctServer', 'server reads');
				return client.distinctServer(schema, q);
			},
			watchGroupCount: (q, cb, opts) => {
				if (!client.watchGroupCount) throw unavailable(key, 'watchGroupCount', 'live reads');
				return client.watchGroupCount(schema, q, cb, opts);
			},
			watchDistinct: (q, cb, opts) => {
				if (!client.watchDistinct) throw unavailable(key, 'watchDistinct', 'live reads');
				return client.watchDistinct(schema, q, cb, opts);
			},
			watch: (cb, opts) => client.watch(schema, cb, opts),
			watchQuery: (q, cb, opts) => client.watchQuery(schema, q, cb, opts),
			text: (k, field) => {
				if (!client.textHandle) {
					throw new Error(
						`client.db.${key}.text: live text handles are not available in transactions`,
					);
				}
				return client.textHandle(schema, k, field);
			},
			hold: (k) => {
				if (!client.hold) throw unavailable(key, 'hold', 'holds');
				return client.hold(schema, k);
			},
			get refs(): EntityRefs {
				return (refsCache ??= buildRefs(schema, client, registry));
			},
			mutate: {
				create: async (value, opts) => {
					const keyField = resolveAutoKey();
					if (!keyField) {
						throw new Error(
							`client.db.${key}.mutate.create: requires a single string key field; `
								+ `use put() with an explicit key`,
						);
					}
					const rec = { ...(value as Record<string, unknown>) };
					const supplied = rec[keyField.localName];
					const id = supplied === undefined || supplied === '' ? newId() : String(supplied);
					rec[keyField.localName] = id;
					await client.mutate.put(schema, rec as MessageInitShape<typeof schema>, opts);
					return id;
				},
				put: (value, opts) => client.mutate.put(schema, value, opts),
				// The facade's `patch` is overloaded (narrow init, or any message
				// under an explicit mask); one wide implementation serves both, so
				// the cast is to the overloaded property type, not around a check.
				patch: ((k: string, init: TextInit<MessageInitShape<typeof schema>>, opts?: PatchOptions) =>
					client.mutate.patch(schema, k, init, opts)) as EntityApi<
						typeof schema
					>['mutate']['patch'],
				update: (k, recipe, opts) =>
					client.mutate.update(schema, k, recipe, opts),
				del: (k, opts) => client.mutate.del(schema, k, opts),
			},
		};
	}
	return apis as EntityApis<E>;
}

/** Builds an entity's reference accessors from the registry's ref graph. */
function buildRefs(schema: DescMessage, client: SchemaApi, registry: EntityRegistry): EntityRefs {
	const def = registry.bySchema(schema);
	const refs: Record<string, ForwardRefAccessor | ReverseRelAccessor> = {};
	for (const fwd of registry.forwardRefsOf(def.name)) {
		refs[fwd.localName] = {
			load: (holder) =>
				client.resolveRef(schema, fwd.localName, holder as MessageShape<typeof schema>),
		};
	}
	for (const rev of registry.reverseRefsOf(def.name)) {
		// Unreachable in practice: the registry rejects a reverse name that
		// collides with a ref field when it builds the graph, which is where the
		// clash belongs (this getter is lazy, so it would only fire for an app
		// that happens to touch this one entity's `.refs`). Kept as a backstop
		// because silently overwriting a forward accessor with a reverse one
		// would change what the app reads.
		if (Object.prototype.hasOwnProperty.call(refs, rev.name)) {
			throw new Error(
				`client.db.${uncapitalize(schema.name)}.refs: "${rev.name}" collides with a ref field`,
			);
		}
		refs[rev.name] = (target, q) =>
			client.resolveReverseRef(schema, rev.name, target as MessageShape<typeof schema>, q);
	}
	// The surface is string-keyed (reverse-relation names are registry artifacts
	// and a plain message field is indistinguishable from a reference in the TS
	// types), so an unknown name type-checks. Name what went wrong here rather
	// than letting it surface as "cannot read properties of undefined".
	return new Proxy(refs, {
		get(target, prop) {
			if (typeof prop !== 'string' || prop in target) {
				return Reflect.get(target, prop) as unknown;
			}
			const known = Object.keys(target);
			throw new Error(
				`client.db.${uncapitalize(schema.name)}.refs.${prop}: not a reference field or reverse `
					+ `relation of ${schema.typeName}. A message field is only a reference when its target `
					+ `message declares (syncplane.v1.entity). Known: ${
						known.length > 0 ? known.join(', ') : '(none)'
					}`,
			);
		},
	}) as EntityRefs;
}
