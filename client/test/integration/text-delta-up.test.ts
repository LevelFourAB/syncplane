// Upstream delta state over the real wire: a small edit to a large
// collaborative-text document ships a small PATCH, the engine unfolds it into
// the same row a full-state PATCH would have produced, and concurrent writers
// still converge.
//
// The payload claim is asserted against the testserver's own byte counters
// (/debug/rpc). Measuring on the client could only prove that a small request
// was sent, not that the server accepted it as the whole edit.

import { inject } from 'vitest';
import { describe, expect, it } from 'vitest';

import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import {
	decodeTextDoc,
	encodeTextDoc,
	mergeText,
	plainTextOf,
	TextDoc,
} from '../../src/text/index.js';
import { freshWorkspace, newClient } from './helpers.js';

interface RPCStats {
	total: Record<string, number>;
	bytesIn: Record<string, number>;
	bytesOut: Record<string, number>;
	/** Push request bytes keyed by workspace — the only counter isolated from
	 * the other suites sharing this server. */
	pushBytesIn: Record<string, number>;
}

async function pushBytes(ws: string): Promise<number> {
	const res = await fetch(`${inject('serverUrl')}/debug/rpc`);
	if (!res.ok) throw new Error(`rpc stats failed: ${res.status}`);
	return ((await res.json()) as RPCStats).pushBytesIn[ws] ?? 0;
}

/** Trusted server-side string write (EntityTx + WithTextString). */
async function serverTextString(ws: string, key: string, value: string): Promise<void> {
	const res = await fetch(`${inject('serverUrl')}/debug/textstring`, {
		method: 'POST',
		body: JSON.stringify({ workspace: ws, key, field: 'body', value }),
	});
	if (!res.ok) throw new Error(`textstring failed: ${res.status} ${await res.text()}`);
}

function bigDoc(peer: string, repeats: number): Uint8Array {
	const d = new TextDoc(peer);
	d.insert(0, 'lorem ipsum dolor sit amet '.repeat(repeats));
	return encodeTextDoc(d);
}

describe('upstream text delta state', () => {
	it('ships a small PATCH for a small edit to a large document', async () => {
		const ws = freshWorkspace();
		const client = newClient(ws);
		await client.start();

		const base = bigDoc('seed', 400);
		await client.mutate.put(WidgetSchema, { id: 'w1', body: base });
		await client.syncNow();

		// The PUT carried the whole document; the PATCH after it must not.
		const before = await pushBytes(ws);
		expect(before).toBeGreaterThan(base.length);
		const edited = decodeTextDoc((await client.get(WidgetSchema, 'w1'))?.body, 'alice');
		edited.insert(0, 'HELLO ');
		await client.mutate.patch(WidgetSchema, 'w1', { body: encodeTextDoc(edited) });
		await client.syncNow();
		const patchBytes = (await pushBytes(ws)) - before;

		expect(patchBytes).toBeGreaterThan(0);
		expect(patchBytes).toBeLessThan(base.length * 0.2);

		// And the row really did move — a small payload that changed nothing
		// would satisfy the size assertion alone.
		const fresh = newClient(ws);
		await fresh.start();
		await fresh.syncNow();
		expect(plainTextOf((await fresh.get(WidgetSchema, 'w1'))?.body)).toMatch(/^HELLO lorem/);

		await fresh.close();
		await client.close();
	});

	it('reaches the same row a full-state PATCH would have', async () => {
		const ws = freshWorkspace();
		const client = newClient(ws);
		await client.start();

		const base = bigDoc('seed', 400);
		await client.mutate.put(WidgetSchema, { id: 'w1', body: base });
		await client.syncNow();

		const confirmed = (await client.get(WidgetSchema, 'w1'))!.body;
		const edited = decodeTextDoc(confirmed, 'alice');
		edited.insert(0, 'HELLO ');
		const local = encodeTextDoc(edited);
		await client.mutate.patch(WidgetSchema, 'w1', { body: local });
		await client.syncNow();

		// Bootstrap a second client so what we compare is the stored row, not
		// this client's optimistic view of it.
		const fresh = newClient(ws);
		await fresh.start();
		await fresh.syncNow();
		const stored = (await fresh.get(WidgetSchema, 'w1'))!.body;
		expect(Array.from(stored)).toEqual(Array.from(mergeText(confirmed, local)));

		await fresh.close();
		await client.close();
	});

	it('applies onto a row that has moved past the state the fragment was cut against', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();
		await b.start();

		await a.mutate.put(WidgetSchema, { id: 'w1', body: bigDoc('seed', 100) });
		await a.syncNow();
		await b.syncNow();
		const bBase = (await b.get(WidgetSchema, 'w1'))!.body;

		// a moves the server's row on...
		const aDoc = decodeTextDoc(bBase, 'alice');
		aDoc.insert(0, '* ');
		await a.mutate.patch(WidgetSchema, 'w1', { body: encodeTextDoc(aDoc) });
		await a.syncNow();

		// ...while b still diffs against the row it last confirmed. The fragment
		// is cut against an ancestor of what the server now holds, which is
		// exactly the relation that makes it applicable.
		const bDoc = decodeTextDoc(bBase, 'bob');
		bDoc.insert([ ...bDoc.plainText() ].length, ' END');
		await b.mutate.patch(WidgetSchema, 'w1', { body: encodeTextDoc(bDoc) });
		await b.syncNow();
		await b.syncNow();
		await a.syncNow();

		const text = plainTextOf((await b.get(WidgetSchema, 'w1'))?.body);
		expect(text.startsWith('* lorem')).toBe(true);
		expect(text.endsWith(' END')).toBe(true);
		expect(plainTextOf((await a.get(WidgetSchema, 'w1'))?.body)).toBe(text);

		await b.close();
		await a.close();
	});

	it('converges with a trusted server-side string write', async () => {
		const ws = freshWorkspace();
		const client = newClient(ws);
		await client.start();

		await client.mutate.put(WidgetSchema, { id: 'w1', body: 'one two three' });
		await client.syncNow();

		const edited = decodeTextDoc((await client.get(WidgetSchema, 'w1'))?.body, 'alice');
		edited.insert(13, ' four');
		await client.mutate.patch(WidgetSchema, 'w1', { body: encodeTextDoc(edited) });
		await client.syncNow();

		// The server re-diffs against what it now holds, so its string write is
		// a two-ended edit of the client's text rather than a reset of it.
		await serverTextString(ws, 'w1', '* one two three four!');
		await client.syncNow();

		expect(plainTextOf((await client.get(WidgetSchema, 'w1'))?.body)).toBe('* one two three four!');
		await client.close();
	});
});
