// The server stops following access parents after Limits.MaxAccessDepth hops,
// and CallerAccess carries that limit. These specs run against a server whose
// limit is 3, not the default 32, and pin that can() stops where the server
// stops: a permission held further up than the limit does not count.

import { createConnectTransport } from '@connectrpc/connect-node';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { type RejectionInfo, SyncClient } from '../../src/client.js';
import {
	FolderMemberSchema,
	FolderSchema,
} from '../../src/gen/syncplane/testdata/access/v1/access_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { startTestServer, type TestServer } from '../support/testserver.js';
import { freshWorkspace } from './helpers.js';

let server: TestServer;
beforeAll(async () => {
	server = await startTestServer([ '-access', '-max-access-depth=3' ]);
});
afterAll(() => server?.stop());

const open: SyncClient[] = [];
afterEach(async () => {
	await Promise.all(open.splice(0).map((c) => c.close()));
});

async function clientAs(
	user: string,
	ws: string,
	rejections: RejectionInfo[],
): Promise<SyncClient> {
	const client = new SyncClient({
		transport: createConnectTransport({
			baseUrl: server.url,
			httpVersion: '1.1',
			interceptors: [ (next) => (req) => {
				req.header.set('Authorization', `Bearer ${user}`);
				return next(req);
			} ],
		}),
		workspaceId: ws,
		kvStore: new MemoryKVStore(),
		entities: [ FolderSchema, FolderMemberSchema ],
		pollIntervalMs: 0,
		streamEnabled: false,
		onMutationRejected: (info) => rejections.push(info),
	});
	open.push(client);
	await client.start();
	await client.syncNow();
	return client;
}

async function trusted(workspace: string, writes: object[]): Promise<void> {
	const res = await fetch(`${server.url}/debug/trusted`, {
		method: 'POST',
		body: JSON.stringify({ workspace, writes }),
	});
	if (!res.ok) throw new Error(`trusted write failed: ${await res.text()}`);
}

describe('a prediction on a server with a MaxAccessDepth of 3', () => {
	// The folders f0 ← f1 ← f2 ← f3, each below the one before it, so f3 sits
	// as deep as the limit allows. eve edits f0 and every folder below it.
	async function seed(ws: string): Promise<void> {
		const folders: object[] = [ { entity: 'folder', value: { id: 'f0', name: 'f0' } } ];
		for (let i = 1; i <= 3; i++) {
			folders.push({
				entity: 'folder',
				value: { id: `f${i}`, name: `f${i}`, parent: { id: `f${i - 1}` } },
			});
		}
		await trusted(ws, [
			...folders,
			{
				entity: 'folder_member',
				value: { id: 'fm-edit', folder: { id: 'f0' }, userId: 'eve', role: 'folder_editor' },
			},
		]);
	}

	it('does not answer yes for a create past the limit', async () => {
		// Given eve's client, which holds every folder
		const ws = freshWorkspace();
		await seed(ws);
		const rejections: RejectionInfo[] = [];
		const eve = await clientAs('eve', ws, rejections);
		expect(await eve.get(FolderSchema, 'f3')).toBeDefined();

		// When she asks about a folder below f2, 3 hops below f0, and one
		// below f3, 4 hops below it, and then creates both
		const near = { id: 'g3', name: 'g3', parent: { id: 'f2' } };
		const far = { id: 'g4', name: 'g4', parent: { id: 'f3' } };
		const nearAnswer = await eve.canCreate(FolderSchema, near);
		const farAnswer = await eve.canCreate(FolderSchema, far, { detail: true });
		await eve.mutate.put(FolderSchema, near);
		await eve.mutate.put(FolderSchema, far);
		await eve.syncNow();

		// Then the server applies the first and refuses the second, and the
		// predictions follow the server's limit, not the default of 32
		expect(rejections.flatMap((r) => r.targets.map((t) => `${t.entity}/${t.key}`))).toEqual([
			'folder/g4',
		]);
		expect(nearAnswer).toBe('yes');
		expect(farAnswer).toEqual({ answer: 'unknown', reason: 'chain_too_deep' });
	});
});
