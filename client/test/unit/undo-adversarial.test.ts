// Edge cases of saved undo scopes and the undo journal: saves that race a
// close, eviction of a key that a scope holds, the bounds of the dead-token
// record, stored values that do not decode, and clock changes. A restart is a
// new SyncClient on the same IndexedDB database (fake-indexeddb clones every
// value as a browser does).

import 'fake-indexeddb/auto';

import { Code, ConnectError, type Transport } from '@connectrpc/connect';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { NoteSchema, TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import {
	OrgMemberSchema,
	ProjectMemberSchema,
	ProjectSchema,
	StaffSchema,
} from '../../src/gen/syncplane/testdata/access/v1/access_pb.js';
import { UndoRefusedError } from '../../src/index.js';
import { metaUndoScopePrefix, metaUndoTokenCounter } from '../../src/keys.js';
import { IDBKVStore } from '../../src/kv/idb.js';
import type { KVStore } from '../../src/kv/store.js';
import { MAX_DEAD_TOKENS, noteDeadUndoTokens } from '../../src/optimistic/undojournal.js';
import { createTestServer } from '../../src/testing/index.js';
import { SAVE_DEBOUNCE_MS } from '../../src/undo-persist.js';
import type { UndoPersistOptions } from '../../src/undo.js';

const entities = [ TodoSchema, NoteSchema ] as const;
type Client = SyncClient<typeof entities>;

let dbSeq = 0;
const opened: { close(): Promise<void> }[] = [];

afterEach(async () => {
	for (const c of opened.splice(0)) await c.close();
	vi.restoreAllMocks();
});

/** Delegates to `t`, but every push fails as if the network were down. */
function failingPushes(t: Transport): Transport {
	return {
		unary: (method, ...rest) =>
			method.localName === 'push'
				? Promise.reject(new ConnectError('offline', Code.Unavailable))
				: t.unary(method, ...rest),
		stream: t.stream.bind(t),
	} as Transport;
}

function newDb(): string {
	return `undo-adversarial-${Date.now()}-${dbSeq++}`;
}

async function open(
	db: string | KVStore,
	opts: { transport?: Transport; persist?: UndoPersistOptions } = {},
): Promise<Client> {
	const client = new SyncClient({
		entities,
		kvStore: typeof db === 'string' ? await IDBKVStore.open(db) : db,
		workspaceId: 'w1',
		transport: opts.transport,
		streamEnabled: false,
		presenceEnabled: false,
		pollIntervalMs: 0,
		pushDebounceMs: 2 ** 31 - 1,
		pushMaxDelayMs: 2 ** 31 - 1,
		undo: { persist: opts.persist },
	});
	opened.push(client);
	return client;
}

async function shut(client: { close(): Promise<void> }): Promise<void> {
	opened.splice(opened.indexOf(client), 1);
	await client.close();
}

async function storedScopeKeys(db: string): Promise<string[]> {
	const kv = await IDBKVStore.open(db);
	const tx = await kv.read();
	const keys: string[] = [];
	try {
		for await (const [ k ] of tx.scan(metaUndoScopePrefix('w1'))) keys.push(k);
	} finally {
		tx.release();
	}
	await kv.close();
	return keys;
}

/** Writes `fn`'s changes to the database `db` in one transaction. */
async function writeTo(
	db: string,
	fn: (tx: Awaited<ReturnType<KVStore['write']>>) => Promise<void>,
) {
	const kv = await IDBKVStore.open(db);
	const tx = await kv.write();
	await fn(tx);
	await tx.commit();
	await kv.close();
}

const settle = () => new Promise((r) => setTimeout(r, SAVE_DEBOUNCE_MS * 3));

describe('a saved undo scope that closes while a save runs', () => {
	it('keeps the saved stacks when close() comes after the debounced save started', async () => {
		// Given a saved scope with two steps, the first already saved,
		const db = newDb();
		const real = await IDBKVStore.open(db);
		let gate: Promise<void> | undefined;
		let entered: (() => void) | undefined;
		const store: KVStore = {
			read: () => real.read(),
			write: async () => {
				if (gate !== undefined) {
					entered?.();
					await gate;
				}
				return real.write();
			},
			close: () => real.close(),
		};
		const client = await open(store);
		const page = await client.restoreUndoScope('page:1');
		await page.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		await settle();
		await page.mutate.patch(TodoSchema, 't1', { title: 'two' });

		// and the debounced save of the second step is in flight, waiting for its
		// write transaction,
		let openGate!: () => void;
		gate = new Promise((r) => (openGate = r));
		await new Promise<void>((r) => (entered = r));

		// When the scope closes before that save takes its snapshot,
		page.close();
		gate = undefined;
		openGate();
		await shut(client);
		expect(await storedScopeKeys(db)).toEqual([ `${metaUndoScopePrefix('w1')}page%3A1` ]);

		// Then a restart restores both steps.
		const next = await open(db);
		const again = await next.restoreUndoScope('page:1');
		expect(again.canUndo()).toBe(true);
		expect(await again.undo()).toBe(true);
		expect((await next.get(TodoSchema, 't1'))?.title).toBe('one');
		expect(await again.undo()).toBe(true);
		expect(await next.get(TodoSchema, 't1')).toBeUndefined();
	});
});

describe('maxScopes and a scope that is open', () => {
	it('keeps the stacks of a held scope that closes after other saves evicted its record', async () => {
		// Given room for two saved scopes, and scope "a" open and saved,
		const db = newDb();
		const client = await open(db, { persist: { maxScopes: 2 } });
		const a = await client.restoreUndoScope('a');
		await a.mutate.put(TodoSchema, { id: 'ta', title: 'a' });
		await settle();

		// and two other scopes that saved and closed after it,
		for (const key of [ 'b', 'c' ]) {
			await new Promise((r) => setTimeout(r, 5));
			const s = await client.restoreUndoScope(key);
			await s.mutate.put(TodoSchema, { id: `t${key}`, title: key });
			s.close();
		}
		await settle();

		// When "a" closes with no change since its last save,
		a.close();
		await shut(client);
		expect(await storedScopeKeys(db)).toContain(`${metaUndoScopePrefix('w1')}a`);

		// Then its stacks are still saved: close() writes them to the store.
		const next = await open(db, { persist: { maxScopes: 2 } });
		const again = await next.restoreUndoScope('a');
		expect(again.canUndo()).toBe(true);
	});
});

describe('the dead-token record and a saved scope', () => {
	it('still drops a step whose write was rejected after MAX_DEAD_TOKENS later deaths', async () => {
		// Given a saved step whose write the server rejected while no tab held it,
		const server = createTestServer({ entities });
		const db = newDb();
		const offline = await open(db, { transport: failingPushes(server.transport()) });
		const page = await offline.restoreUndoScope('page:1');
		await page.mutate.put(TodoSchema, { id: 't1', title: 'refused' });
		await shut(offline);
		server.reject({ key: 't1' });
		const online = await open(db, { transport: server.transport() });
		await online.start();
		await online.syncNow();
		expect(await online.get(TodoSchema, 't1')).toBeUndefined();
		await shut(online);

		// and MAX_DEAD_TOKENS other mutations of the workspace died after it,
		const later = Date.now() + 1000;
		await writeTo(db, (tx) =>
			noteDeadUndoTokens(
				tx,
				'w1',
				Array.from({ length: MAX_DEAD_TOKENS }, (_, i) => BigInt(later) * 1024n + BigInt(i)),
				later,
			));

		// When the scope is restored,
		const next = await open(db);
		const again = await next.restoreUndoScope('page:1');

		// Then the rejected step does not come back.
		expect(again.canUndo()).toBe(false);
	});
});

describe('stored values that do not decode', () => {
	it('still writes when the stored undo-token counter is not a number', async () => {
		// Given a store whose undo-token counter holds a malformed value,
		const db = newDb();
		await writeTo(db, (tx) => tx.put(metaUndoTokenCounter('w1'), 'not-a-number'));

		// When an undo-enabled client writes a row,
		const client = await open(db);
		const write = client.mutate.put(TodoSchema, { id: 't1', title: 'one' });

		// Then the write commits and can be undone, as a malformed undo journal
		// reads as empty rather than failing.
		await expect(write).resolves.toBeUndefined();
		expect(client.canUndo()).toBe(true);
	});

	it('reads a saved record of another version as empty stacks', async () => {
		const db = newDb();
		await writeTo(db, (tx) =>
			tx.put(`${metaUndoScopePrefix('w1')}page%3A1`, {
				v: 2,
				savedAt: Date.now(),
				refs: [],
				undo: [ { g: 1, t: '1', at: Date.now(), kind: 'future' } ],
				redo: [],
			}));
		const client = await open(db);
		const scope = await client.restoreUndoScope('page:1');
		expect(scope.saved).toBe(true);
		expect(scope.canUndo()).toBe(false);
		expect(await scope.undo()).toBe(false);
	});
});

describe('claims within one tab', () => {
	it('gives exactly one of two concurrent restores of a key the saved stacks', async () => {
		const db = newDb();
		const first = await open(db);
		const page = await first.restoreUndoScope('page:1');
		await page.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		await shut(first);

		const client = await open(db);
		const [ x, y ] = await Promise.all([
			client.restoreUndoScope('page:1'),
			client.restoreUndoScope('page:1'),
		]);
		expect([ x.saved, y.saved ].sort()).toEqual([ false, true ]);
		const held = x.saved ? x : y;
		const other = x.saved ? y : x;
		expect(held.canUndo()).toBe(true);
		expect(other.canUndo()).toBe(false);
	});
});

describe('clock changes', () => {
	it('drops a saved step before a custom mutation made after the clock went back', async () => {
		// Given a saved step,
		const db = newDb();
		const first = await open(db);
		const page = await first.restoreUndoScope('page:1');
		await page.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		page.close();

		// When the clock goes back an hour and a custom mutation commits,
		const now = Date.now();
		vi.spyOn(Date, 'now').mockReturnValue(now - 60 * 60_000);
		await first.mutate.custom('anything', TodoSchema, { id: 't9' });
		await shut(first);

		// Then the restored scope does not hold the step: tokens rise with the
		// counter, not with the clock.
		const second = await open(db);
		const again = await second.restoreUndoScope('page:1');
		expect(again.canUndo()).toBe(false);
	});

	it('keeps a saved step recorded after the clock went back past a custom mutation', async () => {
		const db = newDb();
		const first = await open(db);
		await first.mutate.custom('anything', TodoSchema, { id: 't9' });
		const now = Date.now();
		vi.spyOn(Date, 'now').mockReturnValue(now - 60 * 60_000);
		const page = await first.restoreUndoScope('page:1');
		await page.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		await shut(first);
		vi.restoreAllMocks();

		const second = await open(db);
		const again = await second.restoreUndoScope('page:1');
		expect(again.canUndo()).toBe(true);
	});
});

describe('a saved delete of a row with hidden fields', () => {
	it('is still refused after a restart, and the step before it still undoes', async () => {
		const accessEntities = [
			OrgMemberSchema,
			ProjectSchema,
			ProjectMemberSchema,
			StaffSchema,
		] as const;
		const server = createTestServer({ entities: accessEntities, serve: 'readable' });
		server.put(ProjectSchema, { id: 'p1' });
		server.put(StaffSchema, {
			id: 's1',
			project: { id: 'p1' },
			name: 'ann',
			salary: 5000n,
			bonus: 7n,
		});
		server.setCallerAccess({
			subjects: [ 'guest' ],
			roles: [ { role: 'project_reader', at: [ ProjectSchema, 'p1' ] as const } ],
		});
		const db = newDb();
		const make = async () => {
			const c = new SyncClient({
				entities: accessEntities,
				kvStore: await IDBKVStore.open(db),
				workspaceId: 'main',
				transport: failingPushes(server.transport()),
				streamEnabled: false,
				presenceEnabled: false,
				pollIntervalMs: 0,
				pushDebounceMs: 2 ** 31 - 1,
				pushMaxDelayMs: 2 ** 31 - 1,
				undo: {},
			});
			opened.push(c);
			return c;
		};
		const first = await make();
		await first.start();
		await first.syncNow();
		expect((await first.getRow(StaffSchema, 's1'))?.hidden).toContain('bonus');
		const page = await first.restoreUndoScope('page:1');
		await page.db.staff.mutate.patch('s1', { name: 'anna' });
		await page.db.staff.mutate.del('s1');
		await shut(first);

		const second = await make();
		const again = await second.restoreUndoScope('page:1');
		await expect(again.undo()).rejects.toBeInstanceOf(UndoRefusedError);
		expect(await second.getRow(StaffSchema, 's1')).toBeUndefined();
		expect(await again.undo()).toBe(true);
		expect(await again.undo()).toBe(false);
	});
});

describe('eviction by maxScopes', () => {
	it('never deletes the record of the scope that saves', async () => {
		const db = newDb();
		const client = await open(db, { persist: { maxScopes: 1 } });
		for (const key of [ 'p1', 'p2' ]) {
			const s = await client.restoreUndoScope(key);
			await s.mutate.put(TodoSchema, { id: key, title: key });
			s.close();
			await new Promise((r) => setTimeout(r, 5));
		}
		await shut(client);
		expect(await storedScopeKeys(db)).toEqual([ `${metaUndoScopePrefix('w1')}p2` ]);
	});
});
