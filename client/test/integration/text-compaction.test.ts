// Compaction over the real wire: the server purges tombstones from a quiet
// collaborative-text field, and a client that was holding pre-compaction state
// keeps working — either because its write skips the purged tombstones, or
// because the rejection is recovered into a re-applied edit.
//
// The pass is triggered explicitly (POST /debug/gc) rather than waited for, so
// the test controls the moment; the testserver runs with a retention short
// enough that a write made a moment ago is already quiet.

import { inject } from 'vitest';
import { describe, expect, it } from 'vitest';

import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
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
});
