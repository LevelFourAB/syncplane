// Type-level mapping from generated message shapes to operator surfaces.
//
// Two caveats to know before writing a schema:
//
//   - Operator names are reserved. A proto field literally named `eq`, `has`,
//     `isSet`, … is shadowed by the operator here and in the runtime proxy, so
//     it cannot be filtered or ordered on at all. Registering such a schema
//     logs a warning naming the field (see RESERVED_QUERY_NAMES); the field is
//     still readable and writable, only unqueryable.
//   - Enums fall in as numbers, so it is `t.status.eq(2)`, not
//     `t.status.eq(Status.DONE)`. Generated enums are numeric at runtime and
//     the value reaches the wire correctly either way; what is lost is the
//     compiler checking that the number belongs to that enum.

import type { Cond, OrderKey, Scalar } from './ast.js';
import type { QueryCursor } from './cursor.js';

/** Fields that can appear in orderBy. */
export interface OrderOps {
	asc(): OrderKey;
	desc(): OrderKey;
}

export interface StringOps extends OrderOps {
	eq(value: string): Cond;
	neq(value: string): Cond;
	in(values: string[]): Cond;
	startsWith(value: string): Cond;
	endsWith(value: string): Cond;
	contains(value: string): Cond;
}

export interface NumericOps<V extends number | bigint> extends OrderOps {
	eq(value: V): Cond;
	neq(value: V): Cond;
	in(values: V[]): Cond;
	gt(value: V): Cond;
	gte(value: V): Cond;
	lt(value: V): Cond;
	lte(value: V): Cond;
}

export interface BoolOps extends OrderOps {
	eq(value: boolean): Cond;
	neq(value: boolean): Cond;
}

export interface RepeatedScalarOps<E extends Scalar> {
	has(value: E): Cond;
	hasAny(values: E[]): Cond;
	hasAll(values: E[]): Cond;
	isEmpty(): Cond;
}

/**
 * Repeated message fields. Membership would need deep equality, so instead of
 * comparing whole elements you scope one and ask about its fields:
 *
 * ```ts
 * w.assignees.any((a) => a.id.eq('maya'))
 * w.lineItems.any((i) => and(i.sku.eq('A'), i.qty.gt(3)))
 * ```
 *
 * The element is explicit so that two conditions can be said to describe the
 * SAME element — `and(w.lineItems.any(…sku…), w.lineItems.any(…qty…))` is a
 * different, weaker question, and once the conditions are separate terms
 * nothing records that they were meant to be about one element.
 *
 * This is also how a to-many reference is filtered: the element is the target's
 * stub, so only its key fields are addressable, and `any((a) => a.id.eq(…))` is
 * the list counterpart of `w.project.id.eq(…)`.
 */
export interface RepeatedMessageOps<E> {
	any(cond: (element: MsgRef<E>) => Cond): Cond;
	isEmpty(): Cond;
	/**
	 * One-hop semi-join over a to-many reference: true when SOME referenced
	 * target row exists and satisfies the condition. See {@link RelOps.matches}
	 * for the contract; on a repeated field that is not a reference it throws
	 * at query time, like `any()` on a non-list.
	 */
	matches(cond: (target: MsgRef<E>) => Cond): Cond;
}

/** Bytes and singular message fields. */
export interface PresenceOps {
	isSet(): Cond;
	isNotSet(): Cond;
}

/**
 * The one-hop semi-join, offered on forward reference fields: true when at
 * least one referenced target row EXISTS and satisfies the condition. The
 * callback's reference is rooted at the target entity's row, so every field of
 * the target is addressable — unlike drilling into the field itself, which
 * reaches only the key fields a stored stub carries:
 *
 * ```ts
 * // issues assigned to anyone on this team
 * db.issue.query({ where: (i) => i.assignee.matches((a) => a.teamId.eq(t)) })
 * ```
 *
 * An unset ref and an orphaned stub contribute nothing, so the condition is
 * false for them — and `not(matches(…))` therefore also admits rows with no
 * reference at all. One hop only: a `matches` inside a `matches` or inside an
 * `any()` throws, as does one on a message field that is not a reference —
 * reference-ness lives in a proto option the TS types cannot see, so the check
 * is at query time.
 *
 * A local query crossing a reference is answered from the target rows the
 * store holds; when partial sync bounds those, the result reports the
 * `'relation'` coverage bound. See the coverage rule in
 * docs/reference/typescript-client.md.
 */
export interface RelOps<T> {
	matches(cond: (target: MsgRef<T>) => Cond): Cond;
}

/**
 * Map fields — the shape user-defined ("custom") fields take. One ENTRY is
 * addressed by key, and is then an ordinary value with its own operators:
 *
 * ```ts
 * i.custom.at('priority').kind.number.gt(3)
 * i.custom.at('team').kind.text.eq('platform')
 * i.custom.at('labels').kind.tags.values.hasAny([ 'ui' ])
 * ```
 *
 * `at()` rather than property access, for two reasons. A key that collides with
 * an operator name (`eq`, `in`, `has`, `isSet`, …) would be silently swallowed
 * by the runtime proxy — a trap that depends on the DATA, not on the schema. And
 * `Record<string, V>` is structurally permissive enough that plain property
 * access would type-check against surfaces it should not.
 *
 * When the value is an app-defined oneof — the usual shape, because a custom
 * field holds one of several types — a query names the VARIANT it means, exactly
 * as it would for a oneof anywhere else. A member has explicit presence, so an
 * entry holding a different variant simply reads as not found: the condition is
 * false and the ordering ranks it last. The consequence to know is that there is
 * no ordering by "whatever the value is" across variants — `orderBy` must name
 * one — which is a non-loss when a custom field has one declared type in the
 * app's own field-definition table.
 */
export interface MapOps<MV> {
	/** One entry, by key. Absent keys are simply not found. */
	at(key: string): FieldOps<MV> & PresenceOps;
	/**
	 * True when SOME entry satisfies the condition. The element is the ENTRY, so
	 * the condition addresses its two sides:
	 *
	 * ```ts
	 * i.custom.any((e) => e.value.kind.number.eq(5))     // which field holds 5
	 * i.custom.any((e) => and(e.key.startsWith('cf_'), e.value.kind.checked.eq(true)))
	 * i.custom.any((e) => e.key.in([ 'priority', 'severity' ]))  // any of these set
	 * ```
	 *
	 * This is the mirror of `at()`: `at('priority').kind.number.eq(5)` asks
	 * whether THAT field holds 5, and this asks whether ANY does. As with a
	 * repeated message field the entry scope is explicit, so that two conditions
	 * can be said to describe the same entry — `and(m.any(…key…), m.any(…value…))`
	 * is a different, weaker question.
	 *
	 * Comparing the key as a VALUE also removes the dot-escaping `at()` needs for
	 * a key containing a dot, and gives `in`/`startsWith` over keys for free.
	 *
	 * It is a predicate, not a projection: a match says some entry satisfied the
	 * condition, never which one. An app that needs the name reads it off the
	 * row's map.
	 */
	any(cond: (entry: MapEntryRef<MV>) => Cond): Cond;
	/** True when the map holds no entries. `isSet` cannot say this: a map has
	 * implicit presence, so it is always set. */
	isEmpty(): Cond;
}

/** The two sides of one map entry, as {@link MapOps.any} scopes them. */
export interface MapEntryRef<MV> {
	key: MapKeyOps;
	value: FieldOps<MV> & PresenceOps;
}

/**
 * Operators on a map entry's KEY.
 *
 * The surface is stated rather than derived because a generated map types its
 * keys as `string` whatever the .proto declared — protobuf-es keys the object by
 * the key's string form — so the row shape cannot say what a key really is. The
 * VALUE reaching the evaluator is the declared type: on a `map<int64, V>` the
 * entry key is a bigint, so `e.key.gte(5n)` is the spelling that matches, and
 * the string operators are only meaningful on a string-keyed map (they are
 * rejected at prepare time on any other, like every other leaf/operator
 * mismatch).
 */
export interface MapKeyOps {
	eq(value: Scalar): Cond;
	neq(value: Scalar): Cond;
	in(values: Scalar[]): Cond;
	gt(value: Scalar): Cond;
	gte(value: Scalar): Cond;
	lt(value: Scalar): Cond;
	lte(value: Scalar): Cond;
	startsWith(value: string): Cond;
	endsWith(value: string): Cond;
	contains(value: string): Cond;
}

/**
 * Maps a message field's value type to its operator surface. All checks are
 * non-distributive (`[X] extends [Y]`) so unions like `boolean` and
 * `Settings | undefined` resolve as a whole; `NonNullable` strips the
 * optionality so `settings?: Settings` can be both drilled into and tested
 * with isSet()/isNotSet().
 *
 * The MAP check has to come before the message ones and cannot be written as
 * `extends [Record<string, unknown>]`: a map is generated as an index signature,
 * and an index signature satisfies `{ $typeName: string }` structurally, so
 * `map<string, string>` would otherwise be read as a message. `string extends
 * keyof V` is the property only an index signature has.
 */
export type FieldOps<V> = [NonNullable<V>] extends [string] ? StringOps
	: [NonNullable<V>] extends [number] ? NumericOps<number>
	: [NonNullable<V>] extends [bigint] ? NumericOps<bigint>
	: [NonNullable<V>] extends [boolean] ? BoolOps
	: [NonNullable<V>] extends [Uint8Array] ? PresenceOps
	: [NonNullable<V>] extends [ReadonlyArray<infer E>] ? [E] extends [Scalar] ? RepeatedScalarOps<E>
		: RepeatedMessageOps<E>
	: string extends keyof NonNullable<V> ? MapOps<NonNullable<V>[string]>
	: [NonNullable<V>] extends [{ case: string | undefined }] ? OneofRef<NonNullable<V>>
	// Whatever is left and is an object is a message. Matching on `object`
	// rather than on `$typeName` is what makes a sub-message declared with the
	// TS schema builder drillable too: its inferred row shape is a plain object,
	// with no generated brand to key off. RelOps rides on every message field
	// because reference-ness lives in a proto option the TS types cannot see;
	// matches() on a non-reference throws at query time.
	: [NonNullable<V>] extends [object] ? MsgRef<NonNullable<V>> & PresenceOps & RelOps<NonNullable<V>>
	: never; // scalars no branch above claimed: unsupported for now

/**
 * Operator surface of a oneof group: one entry per case, each carrying the
 * member's own operators plus presence.
 *
 * Reading is member-granular even though WRITING is not (a mask names the
 * group, never a member — see engine/internal/lww): asking "which case is set"
 * is unambiguous where "set only this member" is not. `isSet()` on a member IS
 * the case test — `w.choice.choiceText.isSet()` — and it is offered on every
 * member, including scalars, because a oneof member has explicit presence
 * whatever its kind.
 */
export type OneofRef<V extends { case: string | undefined }> = {
	[K in NonNullable<V['case']>]: Extract<V, { case: K }> extends { value: infer Val }
		? FieldOps<Val> & PresenceOps
		: never;
};

/** Recursive field-proxy surface for a message shape. */
export type MsgRef<M> = {
	[K in Exclude<keyof M, '$typeName' | '$unknown'>]-?: FieldOps<M[K]>;
};

/**
 * Entity-level operators, offered on the query root alone: full-text search
 * reads the entity's declared search fields, so it has no field to hang off.
 */
export interface EntityQueryOps {
	/**
	 * Full-text match over the entity's declared search fields
	 * ((syncplane.v1.field_behavior).searchable marks and EntityOptions.search).
	 * Every term of `query` must occur, case- and accent-insensitively per the
	 * entity's analyzer; the final term matches as a prefix when the query ends
	 * inside it, so search-as-you-type needs no separate mode. An empty query
	 * matches every row. Throws at query time on an entity that declares no
	 * search fields.
	 */
	match(query: string): Cond;
	/**
	 * Orders best match first. Requires exactly one {@link match} in the
	 * query's `where` — or, in a {@link WorkspaceQuery}, in every workspace's
	 * predicate, each row then ranking against its own workspace's. Rows ranking
	 * equally fall through to the next key and finally the row key.
	 */
	relevance(): OrderKey;
}

/** The root reference handed to `where`/`orderBy` lambdas. */
export type QueryRef<M> = MsgRef<M> & EntityQueryOps;

/** Typed query as written by users; compiled via compileQuery(). */
export interface Query<M> {
	where?: (w: QueryRef<M>) => Cond;
	orderBy?: (w: QueryRef<M>) => OrderKey[];
	limit?: number;
	/**
	 * Rows to skip after ordering — the way to jump to an arbitrary rank, which
	 * a cursor structurally cannot do. For walking pages in sequence prefer
	 * `after`/`before`: an offset re-does the work of every page before it, and
	 * a row inserted ahead of the page shifts every later one. Setting it
	 * alongside either cursor throws.
	 */
	offset?: number;
	/**
	 * Exclusive lower bound: only rows sorting strictly after this position are
	 * returned. Take it from an earlier result of the SAME query — its
	 * `endCursor` to step forward, its `startCursor` to bound the page below —
	 * and `limit` then counts forward from it.
	 *
	 * A cursor is tied to the (entity, orderBy) pair it was issued under; one
	 * replayed against a different entity or ordering throws CursorShapeError.
	 */
	after?: QueryCursor;
	/**
	 * Exclusive upper bound, with the same rules as `after`.
	 *
	 * With `before` alone, `limit` counts BACKWARD from it, so passing a
	 * result's `startCursor` returns the page preceding it. With both bounds set
	 * the query reads a bounded range and `limit` counts forward from `after`.
	 * Rows come back in `orderBy` order whichever bounds are set.
	 */
	before?: QueryCursor;
	/**
	 * Whether the result reports `total`. `false` makes it undefined — counting
	 * the matches past the page costs time linear in their number, which on a
	 * large match set is most of what an indexed page pays — while the rows,
	 * cursors and `hasMore` are unaffected. Pass it on every page whose consumer
	 * does not size anything with the total.
	 * @defaultValue true
	 */
	total?: boolean;
}

/**
 * The field surfaces a grouping key may name: anything comparable, and a
 * repeated scalar, which groups per element.
 *
 * Stated as the operator surfaces rather than as a value type because that is
 * what the proxy hands back — and it is what rejects the rest. A message, a
 * map, bytes and a repeated message have no group: nothing identifies one of
 * their values, so a facet over them would be a single bucket.
 */
export type GroupKeyRef = OrderOps | RepeatedScalarOps<Scalar>;

/**
 * A read that answers with the GROUPS of a match set instead of its rows —
 * a board column's badge, a facet sidebar, the values a filter dropdown offers.
 *
 * Groups come back in ascending value order with the no-value group last, and
 * `limit` caps how many. There is no ordering or cursor of its own: a group has
 * no position, and the value order is what makes a limited read meaningful.
 *
 * A `by` path through a REPEATED field groups per ELEMENT — a row holding three
 * labels joins three groups — so the counts do not sum to the number of
 * matching rows. That is what a label facet wants.
 *
 * Only groups the rows produce exist. A status no row currently holds is absent
 * rather than zero: the set of possible values is the app's (an enum, a status
 * entity) to join these counts against.
 *
 * @example
 * ```ts
 * const byStatus = await db.issue.groupCount({
 * 	by: (i) => i.status,
 * 	where: (i) => i.projectId.eq(projectId),
 * });
 * ```
 */
export interface GroupQuery<M> {
	/** The field the match set is partitioned by, named without an operator. */
	by: (w: QueryRef<M>) => GroupKeyRef;
	where?: (w: QueryRef<M>) => Cond;
	/** Maximum groups to return, counted from the first in value order. */
	limit?: number;
}

/**
 * A cross-workspace query whose predicate is chosen per workspace, for a
 * condition naming something workspace-scoped — a row id, which is a different
 * literal in each partition.
 *
 * Accepted wherever a `Query` is by the cross-workspace reads, and only there:
 * it requires the workspace set it is built against (`opts.workspaces`) and
 * throws without one. `byWorkspace` is called once per named workspace, and a
 * workspace it returns `undefined` for is left out of the read.
 *
 * Order, page and total describe the merged result, so `orderBy`, `limit`,
 * `offset` and `total` stay shared; only the predicate varies. `after`/`before`
 * are absent because no position spans workspaces.
 *
 * Ordering by {@link EntityQueryOps.relevance} works here too: every workspace's
 * predicate then needs its own single {@link EntityQueryOps.match}, and each row
 * ranks against the one its workspace was read under.
 *
 * @example
 * ```ts
 * const mine = db.task.useQueryAll({
 * 	orderBy: (t) => [ t.dueAt.asc() ],
 * 	byWorkspace: (ws) => {
 * 		const me = memberIdIn(ws);
 * 		return me && ((t) => t.assignee.eq(me));
 * 	},
 * }, { workspaces });
 * ```
 */
export interface WorkspaceQuery<M> extends Omit<Query<M>, 'where' | 'after' | 'before'> {
	byWorkspace: (workspaceId: string) => ((w: QueryRef<M>) => Cond) | undefined;
}

/**
 * What a result's `total` is for a given query: `undefined` once the query
 * opts out with `total: false`, `number` otherwise. A flag only known at
 * runtime types as `number`; guard the read where the flag can be false.
 */
export type TotalOf<Q> = Q extends { total: false } ? undefined : number;
