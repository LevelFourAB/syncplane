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
}

/** Bytes and singular message fields. */
export interface PresenceOps {
	isSet(): Cond;
	isNotSet(): Cond;
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
	// with no generated brand to key off.
	: [NonNullable<V>] extends [object] ? MsgRef<NonNullable<V>> & PresenceOps
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

/** The root reference handed to `where`/`orderBy` lambdas. */
export type QueryRef<M> = MsgRef<M>;

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
