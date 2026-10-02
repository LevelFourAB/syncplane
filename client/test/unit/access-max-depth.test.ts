// CallerAccess carries the server's MaxAccessDepth, and a prediction follows
// as many access parent hops as it allows: the default of 32 when the summary
// carries none.

import { create } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { AccessSummary, applyAccessDelta, MAX_ACCESS_CHAIN } from '../../src/access.js';
import { FolderSchema } from '../../src/gen/syncplane/testdata/access/v1/access_pb.js';
import {
	CallerAccessDeltaSchema,
	CallerAccessSchema,
} from '../../src/gen/syncplane/v1/service_pb.js';
import { accessDeltaOf, callerAccessOf } from '../../src/testing/access.js';
import { createTestClient, TestServer } from '../../src/testing/index.js';

describe('AccessSummary.maxAccessDepth', () => {
	it('is the limit the summary carries', () => {
		expect(new AccessSummary(create(CallerAccessSchema, { maxAccessDepth: 5 })).maxAccessDepth).toBe(
			5,
		);
	});

	it('is the default for a summary from a server that sends none', () => {
		expect(new AccessSummary(create(CallerAccessSchema, {})).maxAccessDepth).toBe(MAX_ACCESS_CHAIN);
	});

	it('survives a delta', () => {
		const held = create(CallerAccessSchema, { maxAccessDepth: 5, version: 1n });
		const out = applyAccessDelta(
			held,
			create(CallerAccessDeltaSchema, { baseVersion: 1n, version: 2n }),
		);
		expect(out.maxAccessDepth).toBe(5);
	});
});

describe('the test kit server', () => {
	it('sends no delta between summaries with different limits', () => {
		const held = callerAccessOf({ subjects: [ 'ann' ] }, 32);
		const now = callerAccessOf({ subjects: [ 'ann' ] }, 4);
		expect(now.maxAccessDepth).toBe(4);
		expect(accessDeltaOf(held, now)).toBeUndefined();
	});

	// f0 ← f1 ← … ← f5, with view_folders held at f0 alone.
	async function chain(maxAccessDepth?: number) {
		const server = new TestServer({
			entities: [ FolderSchema ],
			...(maxAccessDepth === undefined ? {} : { maxAccessDepth }),
		});
		const t = await createTestClient({ server });
		t.server.put(FolderSchema, { id: 'f0' });
		for (let i = 1; i <= 5; i++) {
			t.server.put(FolderSchema, { id: `f${i}`, parent: { id: `f${i - 1}` } });
		}
		t.server.setCallerAccess({
			permissions: [ {
				permission: 'view_folders',
				anchors: [ { entity: 'folder', keys: [ 'f0' ] } ],
			} ],
		});
		await t.sync();
		return t;
	}

	it('makes the client stop where its maxAccessDepth stops', async () => {
		// Given a server whose limit is 3
		const t = await chain(3);

		// Then a permission 3 hops up counts, and one 5 hops up is past the limit
		expect(await t.client.hasPermission(FolderSchema, 'f3', 'view_folders')).toBe('yes');
		expect(await t.client.hasPermission(FolderSchema, 'f5', 'view_folders', { detail: true }))
			.toEqual({ answer: 'unknown', reason: 'chain_too_deep' });
		await t.close();
	});

	it('follows the default limit when none is set', async () => {
		const t = await chain();
		expect(await t.client.hasPermission(FolderSchema, 'f5', 'view_folders')).toBe('yes');
		await t.close();
	});
});
