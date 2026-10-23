// The stored CallerAccess and the hidden fields of rows, through the
// IndexedDB backend (fake-indexeddb). A page reload is a new SyncClient on a
// new connection to the same database, with no network: what it predicts and
// shows comes from the store alone.

import 'fake-indexeddb/auto';

import { Code, ConnectError, type Transport } from '@connectrpc/connect';
import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import {
	OrgMemberSchema,
	ProjectMemberSchema,
	ProjectSchema,
	StaffSchema,
	TaskSchema,
} from '../../src/gen/syncplane/testdata/access/v1/access_pb.js';
import { IDBKVStore } from '../../src/kv/idb.js';
import { createTestServer } from '../../src/testing/index.js';

const entities = [
	OrgMemberSchema,
	ProjectSchema,
	ProjectMemberSchema,
	TaskSchema,
	StaffSchema,
] as const;

const offline: Transport = {
	unary: () => Promise.reject(new ConnectError('offline', Code.Unavailable)),
	stream: () => Promise.reject(new ConnectError('offline', Code.Unavailable)),
} as unknown as Transport;

function client(kvStore: IDBKVStore, transport: Transport): SyncClient<typeof entities> {
	return new SyncClient({
		entities,
		transport,
		kvStore,
		workspaceId: 'main',
		streamEnabled: false,
		presenceEnabled: false,
		pollIntervalMs: 0,
		pushDebounceMs: 2 ** 31 - 1,
		pushMaxDelayMs: 2 ** 31 - 1,
	});
}

function seeded() {
	const server = createTestServer({ entities, serve: 'readable' });
	server.put(ProjectSchema, { id: 'p1' });
	server.put(ProjectSchema, { id: 'p2' });
	server.put(TaskSchema, { id: 't1', project: { id: 'p1' } });
	server.put(TaskSchema, { id: 't2', project: { id: 'p2' } });
	server.put(StaffSchema, {
		id: 's1',
		project: { id: 'p1' },
		name: 'ann',
		salary: 5000n,
		personId: 'guest',
	});
	return server;
}

const reader = (...projects: string[]) => ({
	subjects: [ 'guest' ],
	roles: projects.map((p) => ({ role: 'project_reader', at: [ ProjectSchema, p ] as const })),
});

describe('given a client on IndexedDB that synced as a reader of p1', () => {
	describe('when the page reloads with no network', () => {
		it('then can() answers from the stored summary and rows keep their hidden fields', async () => {
			// Given
			const name = `access-idb-${Date.now()}-a`;
			const server = seeded();
			server.setCallerAccess(reader('p1'));
			const first = client(await IDBKVStore.open(name), server.transport());
			await first.start();
			await first.syncNow();
			expect(await first.can(TaskSchema, 't1', 'read')).toBe('yes');
			await first.close();

			// When
			const second = client(await IDBKVStore.open(name), offline);
			try {
				// Then
				expect(await second.can(TaskSchema, 't1', 'read')).toBe('yes');
				expect(await second.can(TaskSchema, 't1', 'update')).toBe('no');
				expect(await second.can(StaffSchema, 's1', 'read', { fields: [ 'salary' ] })).toBe('no');
				const row = await second.getRow(StaffSchema, 's1');
				expect(row?.hidden).toEqual([ 'bio', 'bonus', 'pin', 'salary' ]);
				expect(row?.value.salary).toBe(0n);
			} finally {
				await second.close();
			}
		});
	});

	describe('when a later pull carries a delta that adds p2, and the page reloads with no network', () => {
		it('then can() answers from the summary with the delta applied', async () => {
			// Given
			const name = `access-idb-${Date.now()}-b`;
			const server = seeded();
			server.setCallerAccess(reader('p1'));
			const first = client(await IDBKVStore.open(name), server.transport());
			await first.start();
			await first.syncNow();
			expect(await first.getRow(TaskSchema, 't2')).toBeUndefined();

			// When
			server.setCallerAccess(reader('p1', 'p2'));
			await first.syncNow();
			expect(await first.can(TaskSchema, 't2', 'read')).toBe('yes');
			await first.close();
			const second = client(await IDBKVStore.open(name), offline);
			try {
				// Then
				expect(await second.can(TaskSchema, 't2', 'read')).toBe('yes');
				expect(await second.can(TaskSchema, 't1', 'read')).toBe('yes');
			} finally {
				await second.close();
			}
		});
	});
});
