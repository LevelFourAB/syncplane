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
 * Maps a message field's value type to its operator surface. All checks are
 * non-distributive (`[X] extends [Y]`) so unions like `boolean` and
 * `Settings | undefined` resolve as a whole; `NonNullable` strips the
 * optionality so `settings?: Settings` can be both drilled into and tested
 * with isSet()/isNotSet().
 */
export type FieldOps<V> = [NonNullable<V>] extends [string] ? StringOps
	: [NonNullable<V>] extends [number] ? NumericOps<number>
	: [NonNullable<V>] extends [bigint] ? NumericOps<bigint>
	: [NonNullable<V>] extends [boolean] ? BoolOps
	: [NonNullable<V>] extends [Uint8Array] ? PresenceOps
	: [NonNullable<V>] extends [ReadonlyArray<infer E>] ? [E] extends [Scalar] ? RepeatedScalarOps<E>
		: RepeatedMessageOps<E>
	: [NonNullable<V>] extends [{ case: string | undefined }] ? OneofRef<NonNullable<V>>
	: [NonNullable<V>] extends [{ $typeName: string }] ? MsgRef<NonNullable<V>> & PresenceOps
	: never; // maps and anything else: unsupported for now

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
	offset?: number;
}
