// A saved undo scope against the real server: the restart is a new client on
// the same IndexedDB database (fake-indexeddb), and the server's rejection of a
// queued write reaches the restored stacks.

import 'fake-indexeddb/auto';

import { describe, expect, it } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { IDBKVStore } from '../../src/kv/idb.js';
import { freshWorkspace, newClient } from './helpers.js';

let seq = 0;

describe('a saved undo scope over sync', () => {
	it('drops the step of a write the server rejects after a restart', async () => {
		const ws = freshWorkspace();
		const db = `it-undo-saved-${Date.now()}-${seq++}`;
		const open = async () => newClient(ws, { kvStore: await IDBKVStore.open(db), undo: {} });

		// Stamp the store, so the writer below can queue without starting: a
		// started client pushes what it queued when it closes.
		const stamp = await open();
		await stamp.start();
		await stamp.close();

		const writer = await open();
		const page = await writer.restoreUndoScope('page:1');
		await page.mutate.put(TodoSchema, { id: 'ok', title: 'fine' });
		await page.mutate.put(TodoSchema, { id: 'bad', title: 'forbidden fruit' });
		await writer.close();

		const next = await open();
		const again = await next.restoreUndoScope('page:1');
		expect(again.saved).toBe(true);
		await next.start();
		await next.syncNow(); // the server rejects 'bad'
		expect(await next.get(TodoSchema, 'bad')).toBeUndefined();
		expect((await next.get(TodoSchema, 'ok'))?.title).toBe('fine');

		// The rejected step is gone; the step below it undoes the first put.
		expect(await again.undo()).toBe(true);
		await next.syncNow();
		expect(await next.get(TodoSchema, 'ok')).toBeUndefined();
		expect(await again.undo()).toBe(false);

		// A second client sees the server converge on the undo.
		const peer = newClient(ws);
		await peer.start();
		await peer.syncNow();
		expect(await peer.get(TodoSchema, 'ok')).toBeUndefined();
		await peer.close();
		await next.close();
	});
});
