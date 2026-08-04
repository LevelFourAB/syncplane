export type { CmpOp, CompiledQuery, Cond, OrderKey, Path, Scalar, StrOp } from './ast.js';
export { compareRows, compareValues, evalCond, resolvePath, runQuery } from './evaluate.js';
export type { IndexComponent, IndexKind } from './indexkey.js';
export { compileIndexComponent, IndexEncoder, specHash, upperBound } from './indexkey.js';
export type { IndexRange, PlannableIndex, QueryPlan } from './plan.js';
export { planQuery } from './plan.js';
export { and, compileQuery, not, or, rootRef } from './proxy.js';
export type {
	BoolOps,
	FieldOps,
	MapEntryRef,
	MapKeyOps,
	MapOps,
	MsgRef,
	NumericOps,
	OrderOps,
	PresenceOps,
	Query,
	QueryRef,
	RepeatedMessageOps,
	RepeatedScalarOps,
	StringOps,
} from './types.js';
