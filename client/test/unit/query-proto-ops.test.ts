import { create } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import {
	type CmpOp,
	ConditionSchema,
	type PresentOp,
} from '../../src/gen/syncplane/v1/query_pb.js';
import { condFromProto } from '../../src/query/proto.js';

// A newer query.proto can add an op. Read as a known op, it could widen what a
// condition, or a not around it, admits, so the client refuses it and says why.
describe('condFromProto and an op value this build does not declare', () => {
	it('refuses it as unknown, apart from a missing op', () => {
		// An op value the generated enum does not name, as an older build reads it.
		const cmp = (op: number) =>
			create(ConditionSchema, { kind: { case: 'cmp', value: { path: [ 'a' ], op: op as CmpOp } } });
		expect(() => condFromProto(cmp(99))).toThrow(
			'cmp condition op 99 is unknown to this client; upgrade the client before using it',
		);
		expect(() => condFromProto(cmp(0))).toThrow('cmp condition needs an op');

		const present = create(ConditionSchema, {
			kind: { case: 'present', value: { path: [ 'a' ], op: 42 as PresentOp } },
		});
		expect(() => condFromProto(present)).toThrow('present condition op 42 is unknown');
	});
});
