// Conformance vectors shared with the Go engine
// (engine/server/writepolicy_vectors_internal_test.go) — the client refuses a
// write locally, the server refuses it authoritatively, and the two must
// classify identically. A client that classified differently would either block
// a write the server would have taken, or enqueue one it can only reject.

import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { describe, expect, it } from 'vitest';

import { CrudOp } from '../../src/gen/syncplane/v1/mutation_pb.js';
import { assertWritePolicy, type EntityDef, type WritePolicies } from '../../src/mutations.js';

interface PolicyCase {
	name: string;
	op: 'PUT' | 'PATCH' | 'DELETE' | 'UNSPECIFIED';
	expectedVersion: string | null;
	policies: WritePolicies;
	protected?: string[];
	mask?: string[];
	want: 'allow' | 'refuse';
}

const vectorsPath = join(
	dirname(fileURLToPath(import.meta.url)),
	'../../../testdata/writepolicy/vectors.json',
);
const vectors = JSON.parse(readFileSync(vectorsPath, 'utf-8')) as { cases: PolicyCase[] };

const opOf = (op: PolicyCase['op']): CrudOp =>
	op === 'PUT'
		? CrudOp.PUT
		: op === 'PATCH'
		? CrudOp.PATCH
		: op === 'DELETE'
		? CrudOp.DELETE
		: CrudOp.UNSPECIFIED;

/** Only `name`, `policies` and `protectedFields` are read by the classifier, so
 * the rest of the EntityDef stays unbuilt rather than stubbed. */
const defWith = (policies: WritePolicies, protectedFields: string[]): EntityDef =>
	({ name: 'vector', policies, protectedFields }) as unknown as EntityDef;

describe('write policy conformance vectors', () => {
	it('has cases', () => {
		expect(vectors.cases.length).toBeGreaterThan(0);
	});

	for (const tc of vectors.cases) {
		it(tc.name, () => {
			const expectedVersion = tc.expectedVersion === null ? undefined : BigInt(tc.expectedVersion);
			const call = () =>
				assertWritePolicy(
					defWith(tc.policies, tc.protected ?? []),
					opOf(tc.op),
					expectedVersion,
					tc.mask,
				);
			if (tc.want === 'allow') {
				expect(call).not.toThrow();
			} else {
				expect(call).toThrow(/vector/);
			}
		});
	}
});
