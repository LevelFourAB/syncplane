// Downstream delta state over the real wire: a client that steps away while a
// large collaborative-text document is edited catches up on a small pull, and
// lands on exactly the bytes a full-state pull would have written.
//
// The payload claim is asserted against the testserver's own byte counters
// (/debug/rpc, `pullBytesOut` keyed by workspace) — a client-side measurement
// could only show that a small response arrived, not that it carried the whole
// change.

import { inject } from 'vitest';
import { describe, expect, it } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { decodeTextDoc, encodeTextDoc, plainTextOf, TextDoc } from '../../src/text/index.js';
import { freshWorkspace, newClient } from './helpers.js';

interface RPCStats {
	pullBytesOut: Record<string, number>;
}

async function pullBytes(ws: string): Promise<number> {
	const res = await fetch(`${inject('serverUrl')}/debug/rpc`);
	if (!res.ok) throw new Error(`rpc stats failed: ${res.status}`);
	return ((await res.json()) as RPCStats).pullBytesOut[ws] ?? 0;
}

function bigDoc(peer: string, repeats: number): Uint8Array {
	const d = new TextDoc(peer);
	d.insert(0, 'lorem ipsum dolor sit amet '.repeat(repeats));
	return encodeTextDoc(d);
}

/** Edits a client's confirmed document under a fresh writer identity and
 * pushes the result as a full-state PATCH. */
async function typeInto(
	client: ReturnType<typeof newClient>,
	peer: string,
	fn: (d: TextDoc) => void,
): Promise<void> {
	const d = decodeTextDoc((await client.get(WidgetSchema, 'w1'))?.body, peer);
	fn(d);
	await client.mutate.patch(WidgetSchema, 'w1', { body: encodeTextDoc(d) });
	await client.syncNow();
}

describe('downstream text delta state', () => {
	it('catches a returning reader up on a fraction of the document', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws);
		const reader = newClient(ws);
		await writer.start();
		await reader.start();

		const base = bigDoc('seed', 400);
		await writer.mutate.put(WidgetSchema, { id: 'w1', body: base });
		await writer.syncNow();
		// The reader takes the document once, the expensive way.
		await reader.syncNow();
		expect((await reader.get(WidgetSchema, 'w1'))?.body?.length).toBeGreaterThan(base.length / 2);

		const before = await pullBytes(ws);
		await typeInto(writer, 'alice', (d) => d.insert(0, 'HELLO '));
		await reader.syncNow();
		const caughtUpBytes = (await pullBytes(ws)) - before;

		expect(caughtUpBytes).toBeGreaterThan(0);
		expect(caughtUpBytes).toBeLessThan(base.length * 0.2);
		expect(plainTextOf((await reader.get(WidgetSchema, 'w1'))?.body)).toMatch(/^HELLO lorem/);

		await reader.close();
		await writer.close();
	});

	it('reaches byte-identical state to a reader that pulled the full row', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws);
		const reader = newClient(ws);
		await writer.start();
		await reader.start();

		await writer.mutate.put(WidgetSchema, { id: 'w1', body: bigDoc('seed', 200) });
		await writer.syncNow();
		await reader.syncNow();

		// Several writes while the reader is away — the span the fold has to
		// cover is more than one fragment.
		await typeInto(writer, 'alice', (d) => d.insert(0, 'ONE '));
		await typeInto(writer, 'bob', (d) => d.insert(4, 'TWO '));
		await typeInto(writer, 'carol', (d) => d.setString(`THREE ${d.plainText()}`));
		await reader.syncNow();

		// The control: a client that has held nothing gets the whole row.
		const control = newClient(ws);
		await control.start();
		await control.syncNow();

		const viaDeltas = (await reader.get(WidgetSchema, 'w1'))!.body!;
		const viaFullState = (await control.get(WidgetSchema, 'w1'))!.body!;
		expect(Array.from(viaDeltas)).toEqual(Array.from(viaFullState));
		expect(plainTextOf(viaDeltas)).toMatch(/^THREE ONE TWO lorem/);

		await control.close();
		await reader.close();
		await writer.close();
	});

	it('ships nothing of the document when only another field moved', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws);
		const reader = newClient(ws);
		await writer.start();
		await reader.start();

		const base = bigDoc('seed', 400);
		await writer.mutate.put(WidgetSchema, { id: 'w1', body: base });
		await writer.syncNow();
		await reader.syncNow();

		const before = await pullBytes(ws);
		await writer.mutate.patch(WidgetSchema, 'w1', { label: 'renamed' });
		await writer.syncNow();
		await reader.syncNow();

		expect((await pullBytes(ws)) - before).toBeLessThan(500);
		const row = (await reader.get(WidgetSchema, 'w1'))!;
		expect(row.label).toBe('renamed');
		expect(Array.from(row.body!)).toEqual(Array.from(base));

		await reader.close();
		await writer.close();
	});

	// Tabs of different builds share one store, so a build routinely syncs rows
	// for an entity only its neighbour declares — and it holds no schema to fold
	// a fragment into. Delta state for such an entity would leave it re-
	// bootstrapping the whole workspace on every edit, so it asks not to be sent
	// any.
	it('is not sent for an entity this build cannot decode', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws);
		await writer.start();
		await writer.mutate.put(WidgetSchema, { id: 'w1', body: bigDoc('seed', 100) });
		await writer.syncNow();

		// The newer build is what puts "widget" in the store's entity set.
		const kv = new MemoryKVStore();
		const newer = newClient(ws, { kvStore: kv });
		await newer.start();
		await newer.syncNow();
		await newer.close();

		let bootstraps = 0;
		let resets = 0;
		const older = newClient(ws, {
			entities: [ TodoSchema ],
			kvStore: kv,
			onSyncEvent: (e) => {
				if (e.type === 'bootstrap') bootstraps++;
				if (e.type === 'pull' && e.reset) resets++;
			},
		});
		await older.start();
		await older.syncNow();
		bootstraps = 0;
		resets = 0;

		await typeInto(writer, 'alice', (d) => d.insert(0, 'HELLO '));
		await older.syncNow();
		expect({ bootstraps, resets }).toEqual({ bootstraps: 0, resets: 0 });
		await older.close();

		// What the older build stored is what the build that CAN read it sees:
		// reopening the same store registers nothing new, so nothing re-fetches.
		const after = newClient(ws, { kvStore: kv });
		await after.start();
		expect(plainTextOf((await after.get(WidgetSchema, 'w1'))?.body)).toMatch(/^HELLO lorem/);

		await after.close();
		await writer.close();
	});

	it('converges when the reader is also writing', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();
		await b.start();

		await a.mutate.put(WidgetSchema, { id: 'w1', body: bigDoc('seed', 100) });
		await a.syncNow();
		await b.syncNow();

		// Both sides edit from the same base; each then pulls the other's edit
		// as delta state on top of its own pending work.
		const aDoc = decodeTextDoc((await a.get(WidgetSchema, 'w1'))?.body, 'alice');
		aDoc.insert(0, 'A ');
		const bDoc = decodeTextDoc((await b.get(WidgetSchema, 'w1'))?.body, 'bob');
		bDoc.insert([ ...bDoc.plainText() ].length, ' B');
		await a.mutate.patch(WidgetSchema, 'w1', { body: encodeTextDoc(aDoc) });
		await b.mutate.patch(WidgetSchema, 'w1', { body: encodeTextDoc(bDoc) });
		await a.syncNow();
		await b.syncNow();
		await b.syncNow();
		await a.syncNow();

		const text = plainTextOf((await a.get(WidgetSchema, 'w1'))?.body);
		expect(text.startsWith('A lorem')).toBe(true);
		expect(text.endsWith(' B')).toBe(true);
		expect(plainTextOf((await b.get(WidgetSchema, 'w1'))?.body)).toBe(text);

		await b.close();
		await a.close();
	});
});
