// Two SyncClients on one store stand in for two tabs. The stored CallerAccess
// is shared state: a summary one tab stores, whole or by delta, is the one the
// other predicts from and names on its next pull.

import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import {
	OrgMemberSchema,
	ProjectMemberSchema,
	ProjectSchema,
	TaskSchema,
} from '../../src/gen/syncplane/testdata/access/v1/access_pb.js';
import { META_CLIENT_ID } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { createTestServer } from '../../src/testing/index.js';

const entities = [ OrgMemberSchema, ProjectSchema, ProjectMemberSchema, TaskSchema ] as const;

const reader = (...projects: string[]) => ({
	subjects: [ 'guest' ],
	roles: projects.map((p) => ({ role: 'project_reader', at: [ ProjectSchema, p ] as const })),
});

async function tabs() {
	const server = createTestServer({ entities, serve: 'readable' });
	server.put(ProjectSchema, { id: 'p1' });
	server.put(ProjectSchema, { id: 'p2' });
	server.put(ProjectSchema, { id: 'p3' });
	server.put(TaskSchema, { id: 't1', project: { id: 'p1' } });
	server.put(TaskSchema, { id: 't2', project: { id: 'p2' } });
	server.put(TaskSchema, { id: 't3', project: { id: 'p3' } });
	server.setCallerAccess(reader('p1'));
	const kv = new MemoryKVStore();
	const seed = await kv.write();
	await seed.put(META_CLIENT_ID, 'tab-client');
	await seed.commit();
	const make = () =>
		new SyncClient({
			entities,
			transport: server.transport(),
			kvStore: kv,
			workspaceId: 'main',
			streamEnabled: false,
			presenceEnabled: false,
			pollIntervalMs: 0,
			pushDebounceMs: 2 ** 31 - 1,
			pushMaxDelayMs: 2 ** 31 - 1,
		});
	const a = make();
	const b = make();
	await a.start();
	await b.start();
	return { server, a, b };
}

describe('given two tabs on one store, both synced as a reader of p1', () => {
	describe('when the caller gains p2 and only tab B pulls, then gains p3 and only tab A pulls', () => {
		it('then both tabs predict from the newest summary', async () => {
			// Given
			const { server, a, b } = await tabs();
			try {
				await a.syncNow();
				await b.syncNow();
				expect(await a.can(TaskSchema, 't1', 'read')).toBe('yes');

				// When
				server.setCallerAccess(reader('p1', 'p2'));
				await b.syncNow();
				expect(await a.can(TaskSchema, 't2', 'read')).toBe('yes');
				server.setCallerAccess(reader('p1', 'p2', 'p3'));
				await a.syncNow();

				// Then
				for (const tab of [ a, b ]) {
					expect(await tab.can(TaskSchema, 't1', 'read')).toBe('yes');
					expect(await tab.can(TaskSchema, 't2', 'read')).toBe('yes');
					expect(await tab.can(TaskSchema, 't3', 'read')).toBe('yes');
				}
			} finally {
				await a.close();
				await b.close();
			}
		});
	});

	describe('when the caller loses p1 and gains p2, and both tabs pull', () => {
		it('then neither tab predicts p1 any more', async () => {
			// Given
			const { server, a, b } = await tabs();
			try {
				await a.syncNow();
				await b.syncNow();

				// When
				server.setCallerAccess(reader('p2'));
				await Promise.all([ a.syncNow(), b.syncNow() ]);

				// Then
				for (const tab of [ a, b ]) {
					expect(await tab.hasPermission(ProjectSchema, 'p1', 'view_tasks')).toBe('no');
					expect(await tab.hasPermission(ProjectSchema, 'p2', 'view_tasks')).toBe('yes');
				}
			} finally {
				await a.close();
				await b.close();
			}
		});
	});
});
