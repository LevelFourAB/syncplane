// Converts between the plain-data condition AST (with WIRE paths — proto
// field names from toWirePaths, not JS localNames) and the syncplane.v1 proto
// messages the server consumes. The QueryValue branch preserves the JS kind:
// numbers travel as doubles, bigints as sint64/uint64, so both evaluators
// apply identical coercion rules.

import { create } from '@bufbuild/protobuf';

import {
	CmpCondSchema,
	CmpOp as PbCmpOp,
	type Condition,
	ConditionListSchema,
	ConditionSchema,
	Direction,
	ExistsCondSchema,
	InCondSchema,
	type OrderKey as PbOrderKey,
	OrderKeySchema,
	PresentCondSchema,
	PresentOp,
	type QueryValue,
	QueryValueSchema,
	RelCondSchema,
	RepCondSchema,
	RepOp,
	StrCondSchema,
	StrOp as PbStrOp,
} from '../gen/syncplane/v1/query_pb.js';
import type { CmpOp, Cond, OrderKey, Scalar, StrOp } from './ast.js';

const INT64_MIN = -(2n ** 63n);
const INT64_MAX = 2n ** 63n - 1n;
const UINT64_MAX = 2n ** 64n - 1n;

export function scalarToProto(value: Scalar): QueryValue {
	switch (typeof value) {
		case 'string':
			return create(QueryValueSchema, { kind: { case: 'stringValue', value } });
		case 'number':
			return create(QueryValueSchema, { kind: { case: 'doubleValue', value } });
		case 'boolean':
			return create(QueryValueSchema, { kind: { case: 'boolValue', value } });
		case 'bigint':
			if (value >= INT64_MIN && value <= INT64_MAX) {
				return create(QueryValueSchema, { kind: { case: 'intValue', value } });
			}
			if (value > INT64_MAX && value <= UINT64_MAX) {
				return create(QueryValueSchema, { kind: { case: 'uintValue', value } });
			}
			throw new Error(`query value ${value} is outside the 64-bit integer range`);
	}
}

export function scalarFromProto(value: QueryValue | undefined): Scalar {
	switch (value?.kind.case) {
		case 'stringValue':
		case 'doubleValue':
		case 'boolValue':
		case 'intValue':
		case 'uintValue':
			return value.kind.value;
		default:
			throw new Error('query value has no kind');
	}
}

const CMP_TO_PROTO: Record<CmpOp, PbCmpOp> = {
	eq: PbCmpOp.EQ,
	neq: PbCmpOp.NEQ,
	gt: PbCmpOp.GT,
	gte: PbCmpOp.GTE,
	lt: PbCmpOp.LT,
	lte: PbCmpOp.LTE,
};
const CMP_FROM_PROTO = new Map<PbCmpOp, CmpOp>(
	(Object.entries(CMP_TO_PROTO) as [CmpOp, PbCmpOp][]).map(([ k, v ]) => [ v, k ]),
);

const STR_TO_PROTO: Record<StrOp, PbStrOp> = {
	startsWith: PbStrOp.STARTS_WITH,
	endsWith: PbStrOp.ENDS_WITH,
	contains: PbStrOp.CONTAINS,
};
const STR_FROM_PROTO = new Map<PbStrOp, StrOp>(
	(Object.entries(STR_TO_PROTO) as [StrOp, PbStrOp][]).map(([ k, v ]) => [ v, k ]),
);

/** AST (wire paths) → proto. Throws on values outside the wire range. */
export function condToProto(cond: Cond): Condition {
	switch (cond.kind) {
		case 'cmp':
			return create(ConditionSchema, {
				kind: {
					case: 'cmp',
					value: create(CmpCondSchema, {
						path: cond.path,
						op: CMP_TO_PROTO[cond.op],
						value: scalarToProto(cond.value),
					}),
				},
			});
		case 'in':
			return create(ConditionSchema, {
				kind: {
					case: 'in',
					value: create(InCondSchema, { path: cond.path, values: cond.values.map(scalarToProto) }),
				},
			});
		case 'str':
			return create(ConditionSchema, {
				kind: {
					case: 'str',
					value: create(StrCondSchema, {
						path: cond.path,
						op: STR_TO_PROTO[cond.op],
						value: cond.value,
					}),
				},
			});
		case 'rep': {
			let op: RepOp;
			let values: Scalar[];
			switch (cond.op) {
				case 'has':
					op = RepOp.HAS;
					values = [ cond.value ];
					break;
				case 'hasAny':
					op = RepOp.HAS_ANY;
					values = cond.values;
					break;
				case 'hasAll':
					op = RepOp.HAS_ALL;
					values = cond.values;
					break;
				case 'isEmpty':
					op = RepOp.IS_EMPTY;
					values = [];
					break;
			}
			return create(ConditionSchema, {
				kind: {
					case: 'rep',
					value: create(RepCondSchema, { path: cond.path, op, values: values.map(scalarToProto) }),
				},
			});
		}
		case 'exists':
			return create(ConditionSchema, {
				kind: {
					case: 'exists',
					value: create(ExistsCondSchema, { path: cond.path, cond: condToProto(cond.cond) }),
				},
			});
		case 'rel':
			return create(ConditionSchema, {
				kind: {
					case: 'rel',
					value: create(RelCondSchema, { path: cond.path, cond: condToProto(cond.cond) }),
				},
			});
		case 'present':
			return create(ConditionSchema, {
				kind: {
					case: 'present',
					value: create(PresentCondSchema, {
						path: cond.path,
						op: cond.op === 'isSet' ? PresentOp.IS_SET : PresentOp.IS_NOT_SET,
					}),
				},
			});
		case 'and':
		case 'or':
			return create(ConditionSchema, {
				kind: {
					case: cond.kind,
					value: create(ConditionListSchema, { conds: cond.conds.map(condToProto) }),
				},
			});
		case 'not':
			return create(ConditionSchema, {
				kind: { case: 'not', value: condToProto(cond.cond) },
			});
	}
}

/** Proto → AST (wire paths). Throws on malformed messages. */
export function condFromProto(cond: Condition): Cond {
	const kind = cond.kind;
	switch (kind.case) {
		case 'cmp': {
			const op = CMP_FROM_PROTO.get(kind.value.op);
			if (!op) throw new Error('cmp condition needs an op');
			return { kind: 'cmp', path: kind.value.path, op, value: scalarFromProto(kind.value.value) };
		}
		case 'in':
			return { kind: 'in', path: kind.value.path, values: kind.value.values.map(scalarFromProto) };
		case 'str': {
			const op = STR_FROM_PROTO.get(kind.value.op);
			if (!op) throw new Error('str condition needs an op');
			return { kind: 'str', path: kind.value.path, op, value: kind.value.value };
		}
		case 'rep': {
			const { path, values } = kind.value;
			switch (kind.value.op) {
				case RepOp.HAS: {
					if (values.length !== 1) throw new Error('rep has condition needs exactly one value');
					return { kind: 'rep', path, op: 'has', value: scalarFromProto(values[0]) };
				}
				case RepOp.HAS_ANY:
					return { kind: 'rep', path, op: 'hasAny', values: values.map(scalarFromProto) };
				case RepOp.HAS_ALL:
					return { kind: 'rep', path, op: 'hasAll', values: values.map(scalarFromProto) };
				case RepOp.IS_EMPTY:
					return { kind: 'rep', path, op: 'isEmpty' };
				default:
					throw new Error('rep condition needs an op');
			}
		}
		case 'exists': {
			if (!kind.value.cond) throw new Error('exists condition needs a condition');
			return { kind: 'exists', path: kind.value.path, cond: condFromProto(kind.value.cond) };
		}
		case 'rel': {
			if (!kind.value.cond) throw new Error('rel condition needs a condition');
			return { kind: 'rel', path: kind.value.path, cond: condFromProto(kind.value.cond) };
		}
		case 'present': {
			switch (kind.value.op) {
				case PresentOp.IS_SET:
					return { kind: 'present', path: kind.value.path, op: 'isSet' };
				case PresentOp.IS_NOT_SET:
					return { kind: 'present', path: kind.value.path, op: 'isNotSet' };
				default:
					throw new Error('present condition needs an op');
			}
		}
		case 'and':
		case 'or':
			return { kind: kind.case, conds: kind.value.conds.map(condFromProto) };
		case 'not':
			return { kind: 'not', cond: condFromProto(kind.value) };
		default:
			throw new Error('condition has no kind');
	}
}

/** OrderKey AST (wire paths) → proto. */
export function orderKeyToProto(key: OrderKey): PbOrderKey {
	return create(OrderKeySchema, {
		path: key.path,
		dir: key.dir === 'desc' ? Direction.DESC : Direction.ASC,
	});
}

/** Proto → OrderKey AST (wire paths). */
export function orderKeyFromProto(key: PbOrderKey): OrderKey {
	switch (key.dir) {
		case Direction.ASC:
			return { path: key.path, dir: 'asc' };
		case Direction.DESC:
			return { path: key.path, dir: 'desc' };
		default:
			throw new Error('order key needs a direction');
	}
}
