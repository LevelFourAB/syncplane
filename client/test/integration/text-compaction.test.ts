// Compaction over the real wire: the server purges tombstones from a quiet
// collaborative-text field, and a client that was holding pre-compaction state
// keeps working — either because its write skips the purged tombstones, or
// because the rejection is recovered into a re-applied edit.
//
// The pass is triggered explicitly (POST /debug/gc) rather than waited for, so
// the test controls the moment; the testserver runs with a retention short
// enough that a write made a moment ago is already quiet.

import { createConnectTransport } from '@connectrpc/connect-node';
import { inject } from 'vitest';
import { describe, expect, it } from 'vitest';

import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import {
	compactDoc,
	decodeTextDoc,
	encodeTextDoc,
	plainTextOf,
	TextDoc,
} from '../../src/text/index.js';
import { freshWorkspace, newClient } from './helpers.js';

async function collectGarbage(ws: string): Promise<void> {
	// The delta-log retention is a millisecond; give the write a moment to fall
	// outside it so the row counts as quiet.
	await new Promise((resolve) => setTimeout(resolve, 10));
	const res = await fetch(`${inject('serverUrl')}/debug/gc`, {
		method: 'POST',
		body: JSON.stringify({ workspace: ws }),
	});
	if (!res.ok) throw new Error(`gc failed: ${res.status} ${await res.text()}`);
}

function tombstones(bytes: Uint8Array | undefined): number {
	return compactDoc(decodeTextDoc(bytes, '')).items;
}

describe('collaborative-text compaction', () => {
	it('purges a quiet document without changing what clients read', async () => {
		const ws = freshWorkspace();
		const client = newClient(ws);
		await client.start();

		const d = new TextDoc('seed');
		d.insert(0, 'hello world');
		await client.mutate.put(WidgetSchema, { id: 'w1', body: encodeTextDoc(d) });
		await client.syncNow();
		d.delete(5, 6);
		await client.mutate.patch(WidgetSchema, 'w1', { body: encodeTextDoc(d) });
		await client.syncNow();
		expect(tombstones((await client.get(WidgetSchema, 'w1'))?.body)).toBe(6);

		await collectGarbage(ws);
		// A client that has never seen the row gets the compacted document.
		const fresh = newClient(ws);
		await fresh.start();
		await fresh.syncNow();
		const body = (await fresh.get(WidgetSchema, 'w1'))?.body;
		expect(plainTextOf(body)).toBe('hello');
		expect(tombstones(body)).toBe(0);

		await fresh.close();
		await client.close();
	});

	it('accepts an edit from a client still holding the purged tombstones', async () => {
		const ws = freshWorkspace();
		const stale = newClient(ws);
		await stale.start();

		const d = new TextDoc('seed');
		d.insert(0, 'hello world');
		await stale.mutate.put(WidgetSchema, { id: 'w1', body: encodeTextDoc(d) });
		await stale.syncNow();
		d.delete(5, 6);
		await stale.mutate.patch(WidgetSchema, 'w1', { body: encodeTextDoc(d) });
		await stale.syncNow();

		await collectGarbage(ws);

		// The client never pulled the compacted row: it edits its own state,
		// which still carries the six tombstones the server dropped.
		const local = decodeTextDoc((await stale.get(WidgetSchema, 'w1'))?.body, 'stale');
		local.insert(2, '!');
		await stale.mutate.patch(WidgetSchema, 'w1', { body: encodeTextDoc(local) });
		await stale.syncNow();

		const observer = newClient(ws);
		await observer.start();
		await observer.syncNow();
		const body = (await observer.get(WidgetSchema, 'w1'))?.body;
		expect(plainTextOf(body)).toBe('he!llo');
		expect(tombstones(body), 'the purged tombstones did not come back').toBe(0);

		await observer.close();
		await stale.close();
	});

	it('re-applies an edit the server refused as built on purged state', async () => {
		const ws = freshWorkspace();
		const stale = newClient(ws);
		await stale.start();

		const d = new TextDoc('seed');
		d.insert(0, 'hello world');
		await stale.mutate.put(WidgetSchema, { id: 'w1', body: encodeTextDoc(d) });
		await stale.syncNow();
		d.delete(5, 6);
		await stale.mutate.patch(WidgetSchema, 'w1', { body: encodeTextDoc(d) });
		await stale.syncNow();

		await collectGarbage(ws);

		// Appending attaches to the first character of the deleted run, which the
		// server no longer holds — the write cannot be applied as sent.
		const local = decodeTextDoc((await stale.get(WidgetSchema, 'w1'))?.body, 'stale');
		local.insert(local.length, ' again');
		await stale.mutate.patch(WidgetSchema, 'w1', { body: encodeTextDoc(local) });
		// First cycle: the push is refused, the text is captured and a resync
		// armed. Second cycle: the resync lands and the edit is re-applied.
		await stale.syncNow();
		await stale.syncNow();
		await stale.syncNow();

		const observer = newClient(ws);
		await observer.start();
		await observer.syncNow();
		const body = (await observer.get(WidgetSchema, 'w1'))?.body;
		expect(plainTextOf(body), 'the text survived the rejection').toBe('hello again');
		expect(tombstones(body)).toBe(0);
		expect(plainTextOf((await stale.get(WidgetSchema, 'w1'))?.body)).toBe('hello again');

		await observer.close();
		await stale.close();
	});

	it('keeps what another peer wrote while re-applying a refused edit', async () => {
		const ws = freshWorkspace();
		const stale = newClient(ws);
		await stale.start();

		const d = new TextDoc('seed');
		d.insert(0, 'hello world');
		await stale.mutate.put(WidgetSchema, { id: 'w1', body: encodeTextDoc(d) });
		await stale.syncNow();
		d.delete(5, 6);
		await stale.mutate.patch(WidgetSchema, 'w1', { body: encodeTextDoc(d) });
		await stale.syncNow();

		// While the stale client is offline, bob prefixes the text, and the row
		// then goes quiet and is compacted.
		const bob = newClient(ws);
		await bob.start();
		await bob.syncNow();
		const theirs = decodeTextDoc((await bob.get(WidgetSchema, 'w1'))?.body, 'bob');
		theirs.insert(0, 'Bob: ');
		await bob.mutate.patch(WidgetSchema, 'w1', { body: encodeTextDoc(theirs) });
		await bob.syncNow();
		await collectGarbage(ws);

		// The stale client appends to the state it still holds, which the server
		// refuses as built on purged state.
		const local = decodeTextDoc((await stale.get(WidgetSchema, 'w1'))?.body, 'stale');
		expect(local.plainText()).toBe('hello');
		local.insert(local.length, ' again');
		await stale.mutate.patch(WidgetSchema, 'w1', { body: encodeTextDoc(local) });
		for (let i = 0; i < 4; i++) await stale.syncNow();

		// Only the refused edit is re-applied, so bob's prefix survives.
		const observer = newClient(ws);
		await observer.start();
		await observer.syncNow();
		expect(plainTextOf((await observer.get(WidgetSchema, 'w1'))?.body)).toBe('Bob: hello again');

		await observer.close();
		await bob.close();
		await stale.close();
	});

	it('keeps a refused edit when the tab closes before it is re-applied', async () => {
		const ws = freshWorkspace();
		const store = new MemoryKVStore();
		const stale = newClient(ws, { kvStore: store });
		await stale.start();

		const d = new TextDoc('seed');
		d.insert(0, 'hello world');
		await stale.mutate.put(WidgetSchema, { id: 'w1', body: encodeTextDoc(d) });
		await stale.syncNow();
		d.delete(5, 6);
		await stale.mutate.patch(WidgetSchema, 'w1', { body: encodeTextDoc(d) });
		await stale.syncNow();
		await collectGarbage(ws);

		const local = decodeTextDoc((await stale.get(WidgetSchema, 'w1'))?.body, 'stale');
		local.insert(local.length, ' again');
		await stale.mutate.patch(WidgetSchema, 'w1', { body: encodeTextDoc(local) });

		// Sync until the refused edit is rolled back and not yet re-applied.
		let rolledBack = false;
		for (let i = 0; i < 4 && !rolledBack; i++) {
			await stale.syncNow();
			rolledBack = !plainTextOf((await stale.get(WidgetSchema, 'w1'))?.body).includes('again');
		}
		expect(rolledBack, 'reached the window between rollback and re-apply').toBe(true);

		// The tab dies there, without close(), and the app opens the same store.
		const reopened = newClient(ws, { kvStore: store });
		await reopened.start();
		for (let i = 0; i < 4; i++) await reopened.syncNow();

		const observer = newClient(ws);
		await observer.start();
		await observer.syncNow();
		expect(plainTextOf((await observer.get(WidgetSchema, 'w1'))?.body)).toBe('hello again');

		await observer.close();
		await reopened.close();
	});

	it('re-applies a refused edit once, even when the store is opened again', async () => {
		const ws = freshWorkspace();
		const store = new MemoryKVStore();
		const stale = newClient(ws, { kvStore: store });
		await stale.start();

		const d = new TextDoc('seed');
		d.insert(0, 'hello world');
		await stale.mutate.put(WidgetSchema, { id: 'w1', body: encodeTextDoc(d) });
		await stale.syncNow();
		d.delete(5, 6);
		await stale.mutate.patch(WidgetSchema, 'w1', { body: encodeTextDoc(d) });
		await stale.syncNow();
		await collectGarbage(ws);

		const local = decodeTextDoc((await stale.get(WidgetSchema, 'w1'))?.body, 'stale');
		local.insert(local.length, ' again');
		await stale.mutate.patch(WidgetSchema, 'w1', { body: encodeTextDoc(local) });
		for (let i = 0; i < 4; i++) await stale.syncNow();
		await stale.close();

		const reopened = newClient(ws, { kvStore: store });
		await reopened.start();
		for (let i = 0; i < 4; i++) await reopened.syncNow();

		const observer = newClient(ws);
		await observer.start();
		await observer.syncNow();
		expect(plainTextOf((await observer.get(WidgetSchema, 'w1'))?.body)).toBe('hello again');

		await observer.close();
		await reopened.close();
	});

	it("keeps a closed handle's offline typing when the row is compacted before the push", async () => {
		const ws = freshWorkspace();
		const alice = newClient(ws);
		await alice.start();

		const d = new TextDoc('seed');
		d.insert(0, 'hello world');
		await alice.mutate.put(WidgetSchema, { id: 'w1', body: encodeTextDoc(d) });
		await alice.syncNow();
		d.delete(5, 6);
		await alice.mutate.patch(WidgetSchema, 'w1', { body: encodeTextDoc(d) });
		await alice.syncNow();

		// Offline, alice types "!" through a live handle and closes it, so the edit
		// is only a fragment in her queue.
		const h = await alice.text(WidgetSchema, 'w1', 'body');
		await h.insert(5, '!');
		await h.close();
		expect(plainTextOf((await alice.get(WidgetSchema, 'w1'))?.body)).toBe('hello!');

		await collectGarbage(ws);

		for (let i = 0; i < 4; i++) await alice.syncNow();

		const observer = newClient(ws);
		await observer.start();
		await observer.syncNow();
		expect(plainTextOf((await observer.get(WidgetSchema, 'w1'))?.body)).toBe('hello!');
		expect(plainTextOf((await alice.get(WidgetSchema, 'w1'))?.body)).toBe('hello!');

		await observer.close();
		await alice.close();
	});

	it("keeps a closed handle's typing when the compacted row is pulled before the push", async () => {
		const ws = freshWorkspace();
		// Pushes can be made to fail, so one cycle pulls without having pushed: a
		// flaky network, or a stream that delivers before the push.
		let failPush = false;
		const transport = createConnectTransport({
			baseUrl: inject('serverUrl'),
			httpVersion: '1.1',
			interceptors: [ (next) => (req) => {
				if (failPush && req.method.name === 'Push') throw new Error('push offline');
				return next(req);
			} ],
		});
		const alice = newClient(ws, { transport });
		await alice.start();

		const d = new TextDoc('seed');
		d.insert(0, 'hello world');
		await alice.mutate.put(WidgetSchema, { id: 'w1', body: encodeTextDoc(d) });
		await alice.syncNow();
		d.delete(5, 6);
		await alice.mutate.patch(WidgetSchema, 'w1', { body: encodeTextDoc(d) });
		await alice.syncNow();

		// Typed where the deleted run starts, so the queued fragment hangs off a
		// tombstone the compaction purges.
		const h = await alice.text(WidgetSchema, 'w1', 'body');
		await h.insert(5, '!');
		await h.close();
		await collectGarbage(ws);

		failPush = true;
		await alice.syncNow().catch(() => {});
		failPush = false;
		expect(plainTextOf((await alice.get(WidgetSchema, 'w1'))?.body)).toBe('hello!');

		for (let i = 0; i < 4; i++) await alice.syncNow();
		const observer = newClient(ws);
		await observer.start();
		await observer.syncNow();
		expect(plainTextOf((await observer.get(WidgetSchema, 'w1'))?.body)).toBe('hello!');

		await observer.close();
		await alice.close();
	});

	it('accepts new text typed into the field after a PATCH cleared it', async () => {
		const ws = freshWorkspace();
		const alice = newClient(ws);
		await alice.start();

		// Alice's leased peer typed and deleted, so the floor covers its counters.
		await alice.mutate.put(WidgetSchema, { id: 'w1' });
		await alice.syncNow();
		let h = await alice.text(WidgetSchema, 'w1', 'body');
		await h.insert(0, 'hello world');
		await h.delete(5, 6);
		await h.close();
		await alice.syncNow();
		await collectGarbage(ws);
		await alice.syncNow();

		await alice.mutate.patch(WidgetSchema, 'w1', { body: new Uint8Array() });
		await alice.syncNow();
		expect(plainTextOf((await alice.get(WidgetSchema, 'w1'))?.body)).toBe('');

		h = await alice.text(WidgetSchema, 'w1', 'body');
		await h.insert(0, 'new');
		await h.close();
		for (let i = 0; i < 3; i++) await alice.syncNow();

		const observer = newClient(ws);
		await observer.start();
		await observer.syncNow();
		expect(plainTextOf((await observer.get(WidgetSchema, 'w1'))?.body)).toBe('new');

		await observer.close();
		await alice.close();
	});

	it('keeps a clear and the text typed after it when neither was pushed yet', async () => {
		const ws = freshWorkspace();
		const alice = newClient(ws);
		await alice.start();

		await alice.mutate.put(WidgetSchema, { id: 'w1' });
		await alice.syncNow();
		let h = await alice.text(WidgetSchema, 'w1', 'body');
		await h.insert(0, 'hello world');
		await h.delete(5, 6);
		await h.close();
		await alice.syncNow();

		await alice.mutate.patch(WidgetSchema, 'w1', { body: new Uint8Array() });
		h = await alice.text(WidgetSchema, 'w1', 'body');
		await h.insert(0, 'new');
		await h.close();
		expect(plainTextOf((await alice.get(WidgetSchema, 'w1'))?.body)).toBe('new');
		for (let i = 0; i < 3; i++) await alice.syncNow();

		const observer = newClient(ws);
		await observer.start();
		await observer.syncNow();
		expect(plainTextOf((await observer.get(WidgetSchema, 'w1'))?.body)).toBe('new');
		expect(plainTextOf((await alice.get(WidgetSchema, 'w1'))?.body)).toBe('new');

		await observer.close();
		await alice.close();
	});

	it('accepts text typed into a compacted field right after a local clear', async () => {
		const ws = freshWorkspace();
		const alice = newClient(ws);
		await alice.start();

		await alice.mutate.put(WidgetSchema, { id: 'w1' });
		await alice.syncNow();
		let h = await alice.text(WidgetSchema, 'w1', 'body');
		await h.insert(0, 'hello world');
		await h.delete(5, 6);
		await h.close();
		await alice.syncNow();
		await collectGarbage(ws);
		await alice.syncNow();

		// The clear and the typing reach the server in one push, so the typing
		// is built on the local clear, not on the one the server stored.
		await alice.mutate.patch(WidgetSchema, 'w1', { body: new Uint8Array() });
		h = await alice.text(WidgetSchema, 'w1', 'body');
		await h.insert(0, 'new');
		await h.close();
		for (let i = 0; i < 3; i++) await alice.syncNow();

		const observer = newClient(ws);
		await observer.start();
		await observer.syncNow();
		expect(plainTextOf((await observer.get(WidgetSchema, 'w1'))?.body)).toBe('new');
		expect(plainTextOf((await alice.get(WidgetSchema, 'w1'))?.body)).toBe('new');

		await observer.close();
		await alice.close();
	});
});
