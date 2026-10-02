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
import {
	type AccessRuleDef,
	assertWritePolicy,
	closedUnits,
	type EntityDef,
	type GuardDef,
	type WriteAccess,
} from '../../src/mutations.js';

interface PolicyCase {
	name: string;
	op: 'PUT' | 'PATCH' | 'DELETE' | 'UNSPECIFIED';
	expectedVersion: string | null;
	access?: WriteAccess;
	keyFields?: string[];
	protected?: string[];
	guards?: { fields: string[]; update: boolean }[];
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

/** A guard as the vectors state it: its units, and one update rule when a
 * rule of it names update, whatever that rule's source. */
const guardOf = (g: { fields: string[]; update: boolean }): GuardDef => ({
	units: g.fields,
	rules: g.update ? [ { update: true } as AccessRuleDef ] : [],
});

/** Only `name`, `keyFields` (by name), `writeAccess` and `protectedFields` are
 * read by the classifier, so the rest of the EntityDef stays unbuilt rather
 * than stubbed. `protectedFields` is built as entityDefOf builds it: the
 * creator fields, and the units of each guard without an update rule. */
const defWith = (
	writeAccess: WriteAccess | undefined,
	protectedFields: string[],
	guards: GuardDef[],
	keyFields: string[],
): EntityDef =>
	({
		name: 'vector',
		writeAccess,
		protectedFields: [ ...protectedFields, ...closedUnits(guards, 'update') ],
		keyFields: keyFields.map((name) => ({ name })),
	}) as unknown as EntityDef;

describe('write policy conformance vectors', () => {
	it('has cases', () => {
		expect(vectors.cases.length).toBeGreaterThan(0);
	});

	for (const tc of vectors.cases) {
		it(tc.name, () => {
			const expectedVersion = tc.expectedVersion === null ? undefined : BigInt(tc.expectedVersion);
			const call = () =>
				assertWritePolicy(
					defWith(tc.access, tc.protected ?? [], (tc.guards ?? []).map(guardOf), tc.keyFields ?? []),
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
