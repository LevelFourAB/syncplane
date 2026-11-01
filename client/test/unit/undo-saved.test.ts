// Saved undo scopes: restoreUndoScope() stacks that outlive the client. A
// restart is a new SyncClient on the same IndexedDB database (fake-indexeddb,
// which structured-clones every value the way a browser does; MemoryKVStore
// would keep references and hide a record that does not clone).
//
// Node has no shared lock manager, so claims here go through the in-process
// registry, per store object. That is enough for two clients in one process
// that share a store; exclusion across real tabs is the e2e suite's to prove.

import 'fake-indexeddb/auto';

import { Code, ConnectError, type Transport } from '@connectrpc/connect';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { NoteSchema, TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { metaUndoScope, metaUndoScopePrefix } from '../../src/keys.js';
import { IDBKVStore } from '../../src/kv/idb.js';
import type { KVStore } from '../../src/kv/store.js';
import { savedUndoRefTokens } from '../../src/optimistic/undosaved.js';
import { createTestServer } from '../../src/testing/index.js';
import { encodeTextDoc, TextDoc } from '../../src/text/index.js';
import { SAVE_DEBOUNCE_MS } from '../../src/undo-persist.js';
import type { UndoPersistOptions } from '../../src/undo.js';

const entities = [ TodoSchema, NoteSchema ] as const;
type Client = SyncClient<typeof entities>;

let dbSeq = 0;
const opened: Client[] = [];

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

/** A fresh database name; each test gets its own store. */
function newDb(): string {
	return `undo-saved-${Date.now()}-${dbSeq++}`;
}

async function open(
	db: string | KVStore,
	opts: { transport?: Transport; persist?: UndoPersistOptions; undo?: boolean } = {},
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
		undo: opts.undo === false ? undefined : { persist: opts.persist },
	});
	opened.push(client);
	return client;
}

/** Closes `client` and takes it off the cleanup list. */
async function shut(client: Client): Promise<void> {
	opened.splice(opened.indexOf(client), 1);
	await client.close();
}

async function storedKeys(db: string): Promise<string[]> {
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

describe('restoreUndoScope', () => {
	it('brings back both stacks after a restart', async () => {
		const db = newDb();
		const first = await open(db);
		const page = await first.restoreUndoScope('page:1');
		expect(page.saved).toBe(true);
		expect(page.key).toBe('page:1');
		await page.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		await page.mutate.patch(TodoSchema, 't1', { title: 'two' });
		await page.mutate.put(TodoSchema, { id: 't2', title: 'gone' });
		await page.mutate.del(TodoSchema, 't2');
		// One step on the redo stack.
		expect(await page.undo()).toBe(true);
		expect((await first.get(TodoSchema, 't2'))?.title).toBe('gone');
		await shut(first);

		const second = await open(db);
		const again = await second.restoreUndoScope('page:1');
		expect(again.saved).toBe(true);
		expect(again.canUndo()).toBe(true);
		expect(again.canRedo()).toBe(true);

		// Redo the delete, then walk the whole undo stack back down.
		expect(await again.redo()).toBe(true);
		expect(await second.get(TodoSchema, 't2')).toBeUndefined();
		expect(await again.undo()).toBe(true);
		expect((await second.get(TodoSchema, 't2'))?.title).toBe('gone');
		expect(await again.undo()).toBe(true);
		expect(await second.get(TodoSchema, 't2')).toBeUndefined();
		expect(await again.undo()).toBe(true);
		expect((await second.get(TodoSchema, 't1'))?.title).toBe('one');
		expect(await again.undo()).toBe(true);
		expect(await second.get(TodoSchema, 't1')).toBeUndefined();
		expect(await again.undo()).toBe(false);
	});

	it('restores a transaction as one step', async () => {
		const db = newDb();
		const first = await open(db);
		const page = await first.restoreUndoScope('page:1');
		await page.transact(async (tx) => {
			await tx.mutate.put(TodoSchema, { id: 'a', title: 'a' });
			await tx.mutate.put(TodoSchema, { id: 'b', title: 'b' });
		});
		await shut(first);

		const second = await open(db);
		const again = await second.restoreUndoScope('page:1');
		expect(await again.undo()).toBe(true);
		expect(await second.get(TodoSchema, 'a')).toBeUndefined();
		expect(await second.get(TodoSchema, 'b')).toBeUndefined();
		expect(again.canUndo()).toBe(false);
	});

	it('restores a text edit', async () => {
		const db = newDb();
		const first = await open(db);
		const seed = new TextDoc('seed');
		seed.insert(0, 'hello');
		await first.mutate.put(NoteSchema, { id: 'n1', body: encodeTextDoc(seed) });
		const page = await first.restoreUndoScope('page:1');
		const handle = await page.db.note.text('n1', 'body');
		await handle.insert(5, ' world');
		await handle.delete(0, 1);
		handle.close();
		await shut(first);

		const second = await open(db);
		const again = await second.restoreUndoScope('page:1');
		const view = await second.db.note.text('n1', 'body');
		expect(view.value).toBe('ello world');
		while (await again.undo()) {
			// Typing may have coalesced into one step or two.
		}
		await vi.waitFor(() => {
			expect(view.value).toBe('hello');
		});
		view.close();
	});

	it('gives a second open scope on the same key an unsaved, empty scope', async () => {
		// One store object for both clients: on a server runtime, that is how two
		// clients share a store.
		const store = await IDBKVStore.open(newDb());
		const client = await open(store);
		const page = await client.restoreUndoScope('page:1');
		await page.mutate.put(TodoSchema, { id: 't1', title: 'one' });

		const other = await open(store);
		const blocked = await other.restoreUndoScope('page:1');
		expect(blocked.saved).toBe(false);
		expect(blocked.key).toBe('page:1');
		expect(blocked.canUndo()).toBe(false);
		// Its writes are not saved under the key.
		await blocked.mutate.put(TodoSchema, { id: 't2', title: 'two' });
		blocked.close();

		page.close();
		const next = await client.restoreUndoScope('page:1');
		expect(next.saved).toBe(true);
		expect(await next.undo()).toBe(true);
		expect(await client.get(TodoSchema, 't1')).toBeUndefined();
		expect(await next.undo()).toBe(false);
	});

	it('keeps the saved stacks when a scope closes, so the same tab can restore at once', async () => {
		const db = newDb();
		const client = await open(db);
		const page = await client.restoreUndoScope('page:1');
		await page.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		page.close();
		expect(page.closed).toBe(true);
		expect(page.canUndo()).toBe(false);

		const again = await client.restoreUndoScope('page:1');
		expect(again.saved).toBe(true);
		expect(again.canUndo()).toBe(true);
	});

	it('saves without a close, after the debounce', async () => {
		const db = newDb();
		const client = await open(db);
		const page = await client.restoreUndoScope('page:1');
		await page.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		await vi.waitFor(async () => {
			expect(await storedKeys(db)).toEqual([ metaUndoScope('w1', 'page:1') ]);
		}, { timeout: SAVE_DEBOUNCE_MS * 20 });
	});

	it('deletes the saved stacks on discard()', async () => {
		const db = newDb();
		const client = await open(db);
		const page = await client.restoreUndoScope('page:1');
		await page.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		page.close();
		const reopened = await client.restoreUndoScope('page:1');
		expect(reopened.canUndo()).toBe(true);

		await reopened.discard();
		expect(reopened.closed).toBe(true);
		expect(await storedKeys(db)).toEqual([]);
		const fresh = await client.restoreUndoScope('page:1');
		expect(fresh.saved).toBe(true);
		expect(fresh.canUndo()).toBe(false);
	});

	it('discardUndoScope() deletes a key no scope holds, and refuses a held one', async () => {
		const db = newDb();
		const client = await open(db);
		const page = await client.restoreUndoScope('page:1');
		await page.mutate.put(TodoSchema, { id: 't1', title: 'one' });

		expect(await client.discardUndoScope('page:1')).toBe(false);
		page.close();
		expect(await client.discardUndoScope('page:1')).toBe(true);
		expect(await storedKeys(db)).toEqual([]);
		expect(await client.discardUndoScope('never-saved')).toBe(true);
	});

	it('keeps one key apart from another that only looks alike', async () => {
		const db = newDb();
		const client = await open(db);
		const a = await client.restoreUndoScope('a/b');
		const b = await client.restoreUndoScope('a');
		expect(a.saved && b.saved).toBe(true);
		await a.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		a.close();
		b.close();
		const again = await client.restoreUndoScope('a');
		expect(again.canUndo()).toBe(false);
	});

	it('drops steps older than maxAgeMs', async () => {
		const db = newDb();
		const first = await open(db);
		const page = await first.restoreUndoScope('page:1');
		await page.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		await shut(first);
		await new Promise((r) => setTimeout(r, 5));

		const second = await open(db, { persist: { maxAgeMs: 1 } });
		const again = await second.restoreUndoScope('page:1');
		expect(again.canUndo()).toBe(false);
		// The stale record goes too, with the next save.
		again.close();
		await shut(second);
		expect(await storedKeys(db)).toEqual([]);
	});

	it('never keeps a step longer than seven days, whatever maxAgeMs says', async () => {
		const db = newDb();
		const first = await open(db);
		const page = await first.restoreUndoScope('page:1');
		await page.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		await shut(first);

		const now = Date.now();
		vi.spyOn(Date, 'now').mockReturnValue(now + 8 * 24 * 60 * 60_000);
		const second = await open(db, { persist: { maxAgeMs: 30 * 24 * 60 * 60_000 } });
		const again = await second.restoreUndoScope('page:1');
		expect(again.canUndo()).toBe(false);
	});

	it('deletes the least recently saved scopes past maxScopes', async () => {
		const db = newDb();
		const client = await open(db, { persist: { maxScopes: 2 } });
		for (const key of [ 'p1', 'p2', 'p3' ]) {
			const scope = await client.restoreUndoScope(key);
			await scope.mutate.put(TodoSchema, { id: key, title: key });
			scope.close();
			// Distinct save times.
			await new Promise((r) => setTimeout(r, 5));
		}
		await shut(client);
		expect((await storedKeys(db)).sort()).toEqual(
			[ metaUndoScope('w1', 'p2'), metaUndoScope('w1', 'p3') ].sort(),
		);
	});

	it('drops steps recorded before a later custom mutation', async () => {
		const db = newDb();
		const first = await open(db);
		const page = await first.restoreUndoScope('page:1');
		await page.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		page.close();
		await first.mutate.custom('anything', TodoSchema, { id: 't9' });
		await shut(first);

		const second = await open(db);
		const again = await second.restoreUndoScope('page:1');
		expect(again.canUndo()).toBe(false);
	});

	it('writes nothing back after purgeLocalData()', async () => {
		const db = newDb();
		const client = await open(db);
		const page = await client.restoreUndoScope('page:1');
		// Not saved yet: the debounce is still running when the purge starts.
		await page.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		opened.splice(opened.indexOf(client), 1);
		await client.purgeLocalData();
		await new Promise((r) => setTimeout(r, SAVE_DEBOUNCE_MS * 3));
		expect(await storedKeys(db)).toEqual([]);
	});

	it('writes nothing back after a workspace purge', async () => {
		const db = newDb();
		const client = await open(db);
		const page = await client.restoreUndoScope('page:1');
		await page.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		await client.removeWorkspace('w1', { purge: true });
		expect(page.closed).toBe(true);
		await new Promise((r) => setTimeout(r, SAVE_DEBOUNCE_MS * 3));
		expect(await storedKeys(db)).toEqual([]);
	});

	for (const purge of [ 'purgeLocalData', 'removeWorkspace' ] as const) {
		it(`writes nothing back when ${purge}() comes right after close()`, async () => {
			const db = newDb();
			const client = await open(db);
			const page = await client.restoreUndoScope('page:1');
			await page.mutate.put(TodoSchema, { id: 't1', title: 'one' });
			// close() queues a save of the unsaved step; the purge must wait for it,
			// or the save commits into the purged store.
			page.close();
			opened.splice(opened.indexOf(client), 1);
			await (purge === 'purgeLocalData'
				? client.purgeLocalData()
				: client.removeWorkspace('w1', { purge: true }));
			await new Promise((r) => setTimeout(r, SAVE_DEBOUNCE_MS * 3));
			expect(await storedKeys(db)).toEqual([]);
			if (purge === 'removeWorkspace') await client.close();
		});
	}

	it('lets clients on separate stores each claim the same key', async () => {
		const a = await open(newDb());
		const b = await open(newDb());
		const pageA = await a.restoreUndoScope('page:1');
		const pageB = await b.restoreUndoScope('page:1');
		expect(pageA.saved).toBe(true);
		expect(pageB.saved).toBe(true);
	});

	it('refuses without the undo option', async () => {
		const client = await open(newDb(), { undo: false });
		await expect(client.restoreUndoScope('page:1')).rejects.toThrow(/undo/);
	});
});

/**
 * A client that queues writes compaction may fold. A started client pushes
 * what it queued when it closes, and a first start on a store with no format
 * stamp counts every queued write as pushed; compaction folds neither. So the
 * store is stamped by a client that queues nothing, and the writer never starts.
 */
async function queueWriter(db: string, transport: Transport): Promise<Client> {
	const stamp = await open(db, { transport: failingPushes(transport) });
	await stamp.start();
	await shut(stamp);
	return open(db, { transport: failingPushes(transport) });
}

describe('a saved scope and the server', () => {
	it('drops a step whose write was rejected while no tab held the scope', async () => {
		const server = createTestServer({ entities });
		const db = newDb();
		const offline = await open(db, { transport: failingPushes(server.transport()) });
		const page = await offline.restoreUndoScope('page:1');
		await page.mutate.put(TodoSchema, { id: 'ok', title: 'kept' });
		await page.mutate.put(TodoSchema, { id: 't1', title: 'refused' });
		await shut(offline);

		server.reject({ key: 't1' });
		const online = await open(db, { transport: server.transport() });
		await online.start();
		await online.syncNow();
		expect(await online.get(TodoSchema, 't1')).toBeUndefined();

		const again = await online.restoreUndoScope('page:1');
		// The refused step is gone; the one below it still replays.
		expect(await again.undo()).toBe(true);
		expect(await online.get(TodoSchema, 'ok')).toBeUndefined();
		expect(await again.undo()).toBe(false);
	});

	it('drops a step whose write is rejected after the restore', async () => {
		const server = createTestServer({ entities });
		const db = newDb();
		const offline = await open(db, { transport: failingPushes(server.transport()) });
		const page = await offline.restoreUndoScope('page:1');
		await page.mutate.put(TodoSchema, { id: 't1', title: 'refused' });
		await shut(offline);

		server.reject({ key: 't1' });
		const online = await open(db, { transport: server.transport() });
		const again = await online.restoreUndoScope('page:1');
		expect(again.canUndo()).toBe(true);
		await online.start();
		await online.syncNow();
		expect(again.canUndo()).toBe(false);
		expect(await again.undo()).toBe(false);
	});

	it('keeps the saved snapshot steps from being folded by compaction', async () => {
		const server = createTestServer({ entities });
		const db = newDb();
		const client = await queueWriter(db, server.transport());
		const page = await client.restoreUndoScope('page:1');
		await page.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		await page.mutate.put(TodoSchema, { id: 't1', title: 'two' });
		page.close();
		await shut(client);

		// A new client holds no stack in memory, and has no intent record that
		// names the tokens: only the saved record protects them.
		const next = await open(db, { transport: failingPushes(server.transport()) });
		await next.start();
		const kv = await IDBKVStore.open(db);
		const tx = await kv.read();
		const refs = await savedUndoRefTokens(tx, 'w1');
		tx.release();
		await kv.close();
		expect(refs.size).toBe(2);
		await next.syncNow().catch(() => {});
		expect(await next.pendingCount()).toBe(2);
	});

	it('lets compaction fold the writes of a discarded scope', async () => {
		const server = createTestServer({ entities });
		const db = newDb();
		const client = await queueWriter(db, server.transport());
		const page = await client.restoreUndoScope('page:1');
		await page.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		await page.mutate.put(TodoSchema, { id: 't1', title: 'two' });
		await page.discard();
		await shut(client);

		const next = await open(db, { transport: failingPushes(server.transport()) });
		await next.start();
		await next.syncNow().catch(() => {});
		expect(await next.pendingCount()).toBe(1);
	});
});
