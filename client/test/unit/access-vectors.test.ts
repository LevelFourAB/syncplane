// Conformance vectors shared with the Go engine
// (engine/server/access_internal_test.go): the engine decides whether a caller
// holds a permission at a row, and the client predicts the same answer from the
// summary the engine sent. Where both can decide they must agree; only the
// client can lack an input, and answers 'unknown' for it.

import { create } from '@bufbuild/protobuf';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { describe, expect, it } from 'vitest';

import { AccessSummary, type CanAnswer, canAt, type ParentRef } from '../../src/access.js';
import { CallerAccessSchema } from '../../src/gen/syncplane/v1/service_pb.js';

interface AccessCase {
	name: string;
	access: Record<string, { root?: boolean; anchors?: Record<string, string[]> }> | null;
	permission: string;
	entity: string;
	key: string;
	parent: { entity: string; key: string } | null | 'unknown';
	want: CanAnswer;
}

const vectorsPath = join(
	dirname(fileURLToPath(import.meta.url)),
	'../../../testdata/access/can.json',
);
const vectors = JSON.parse(readFileSync(vectorsPath, 'utf-8')) as { cases: AccessCase[] };

function summaryOf(access: AccessCase['access']): AccessSummary | undefined {
	if (access === null) return undefined;
	return new AccessSummary(create(CallerAccessSchema, {
		permissions: Object.entries(access).map(([ permission, p ]) => ({
			permission,
			root: p.root ?? false,
			anchors: Object.entries(p.anchors ?? {}).map(([ entity, keys ]) => ({ entity, keys })),
		})),
	}));
}

describe('access conformance vectors', () => {
	for (const c of vectors.cases) {
		it(c.name, () => {
			const parent: ParentRef = c.parent === 'unknown' ? undefined : c.parent;
			expect(canAt(summaryOf(c.access), c.permission, c.entity, c.key, parent)).toBe(c.want);
		});
	}
});
