// Under the access model, the server refuses a delete of a row it no longer
// holds live with access_denied, as it refuses a delete of a row the caller
// may not see. A client whose delete crosses the server's own delete of the
// row gets that refusal: it must report the rejection and show nothing else,
// as the row is gone either way.

import { createConnectTransport } from '@connectrpc/connect-node';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { type RejectionInfo, SyncClient } from '../../src/client.js';
import {
	ProjectMemberSchema,
	ProjectSchema,
} from '../../src/gen/syncplane/testdata/access/v1/access_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { startTestServer, type TestServer } from '../support/testserver.js';
import { freshWorkspace } from './helpers.js';

let server: TestServer;
beforeAll(async () => {
	server = await startTestServer([ '-access' ]);
});
afterAll(() => server?.stop());

async function trusted(workspace: string, writes: object[]): Promise<void> {
	const res = await fetch(`${server.url}/debug/trusted`, {
		method: 'POST',
		body: JSON.stringify({ workspace, writes }),
	});
	if (!res.ok) throw new Error(`trusted write failed: ${await res.text()}`);
}

describe('a delete of a row the server deleted first', () => {
	it('is reported as rejected, and the row stays gone', async () => {
		// Given ada, who manages the members of p1, holding member row pm-rita
		const ws = freshWorkspace();
		await trusted(ws, [
			{ entity: 'project', value: { id: 'p1', name: 'one' } },
			{
				entity: 'project_member',
				value: { id: 'pm-ada', project: { id: 'p1' }, userId: 'ada', role: 'project_admin' },
			},
			{
				entity: 'project_member',
				value: { id: 'pm-rita', project: { id: 'p1' }, userId: 'rita', role: 'project_reader' },
			},
		]);
		const rejections: RejectionInfo[] = [];
		const ada = new SyncClient({
			transport: createConnectTransport({
				baseUrl: server.url,
				httpVersion: '1.1',
				interceptors: [ (next) => (req) => {
					req.header.set('Authorization', 'Bearer ada');
					return next(req);
				} ],
			}),
			workspaceId: ws,
			kvStore: new MemoryKVStore(),
			entities: [ ProjectSchema, ProjectMemberSchema ],
			pollIntervalMs: 0,
			streamEnabled: false,
			onMutationRejected: (info) => rejections.push(info),
		});
		await ada.start();
		await ada.syncNow();
		expect(await ada.get(ProjectMemberSchema, 'pm-rita')).toBeDefined();

		// When the server deletes the row, and ada deletes it before she pulls
		await trusted(ws, [ { entity: 'project_member', key: 'pm-rita', delete: true } ]);
		await ada.mutate.del(ProjectMemberSchema, 'pm-rita');
		await ada.syncNow();
		await ada.syncNow();

		// Then the delete is reported as refused, the row is gone, and nothing
		// else changed: no queued write and no sync error
		expect(rejections.map((r) => [ r.errorCode, r.targets.map((t) => t.key) ])).toEqual([
			[ 'access_denied', [ 'pm-rita' ] ],
		]);
		expect(await ada.get(ProjectMemberSchema, 'pm-rita')).toBeUndefined();
		expect(await ada.get(ProjectMemberSchema, 'pm-ada')).toBeDefined();
		expect(ada.status().pendingMutations).toBe(0);
		expect(ada.status().lastError).toBeUndefined();
		await ada.close();
	});
});
