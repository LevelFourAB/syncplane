// String-boundary text writes across the wire: TS string writes converge
// with Go EntityTx string writes (via the testserver's /debug/textstring
// endpoint, a stand-in for a connector/admin writer), and a PUT racing an
// in-flight PATCH neither interleaves nor duplicates text on either side.

import { inject } from 'vitest';
import { describe, expect, it } from 'vitest';

import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { decodeTextDoc, encodeTextDoc, plainTextOf, TextDoc } from '../../src/text/index.js';
import { freshWorkspace, newClient } from './helpers.js';

function bodyDoc(peer: string, s: string): Uint8Array {
	const d = new TextDoc(peer);
	d.insert(0, s);
	return encodeTextDoc(d);
}

/** Trusted server-side string write (EntityTx + WithTextString). */
async function serverTextString(ws: string, key: string, value: string): Promise<void> {
	const res = await fetch(`${inject('serverUrl')}/debug/textstring`, {
		method: 'POST',
		body: JSON.stringify({ workspace: ws, key, field: 'body', value }),
	});
	if (!res.ok) throw new Error(`textstring failed: ${res.status} ${await res.text()}`);
}

describe('string writes over the wire', () => {
	it('TS string writes and Go EntityTx string writes converge', async () => {
		const ws = freshWorkspace();
		const client = newClient(ws);
		await client.start();

		// Client creates the doc from a string...
		await client.mutate.put(WidgetSchema, { id: 'w1', body: 'one two three' });
		await client.syncNow();

		// ...the server string-writes a two-ended edit (Go three-way diff)...
		await serverTextString(ws, 'w1', '* one two three!');

		// ...while the client string-patches the middle before seeing it.
		await client.mutate.patch(WidgetSchema, 'w1', { body: 'one TWO three' });
		await client.syncNow();
		await client.syncNow();

		const row = await client.get(WidgetSchema, 'w1');
		expect(plainTextOf(row?.body)).toBe('* one TWO three!');
		await client.close();
	});

	it('server-side string write creates the row when absent', async () => {
		const ws = freshWorkspace();
		const client = newClient(ws);
		await client.start();
		await serverTextString(ws, 'w9', 'made on the server');
		await client.syncNow();
		expect(plainTextOf((await client.get(WidgetSchema, 'w9'))?.body)).toBe('made on the server');
		await client.close();
	});

	it('the 097 repro: PUT racing an in-flight PATCH no longer duplicates text', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();
		await b.start();

		const base = bodyDoc('seed', 'Groceries: milk, eggs, bread');
		await a.mutate.put(WidgetSchema, { id: 'w1', body: base });
		await a.syncNow();
		await b.syncNow();

		// b types into the middle (PATCH built, not yet pushed)...
		const bBase = (await b.get(WidgetSchema, 'w1'))?.body;
		const bDoc = decodeTextDoc(bBase, 'bob');
		bDoc.insert(17, 'cheese, ');
		await b.mutate.patch(WidgetSchema, 'w1', { body: encodeTextDoc(bDoc) });

		// ...while a PUTs a whole row built from the shared base with edits at
		// both ends. Pre-fix the PUT reset the doc and b's PATCH unioned the
		// old document back in, duplicating the whole text.
		const aDoc = decodeTextDoc(base, 'alice');
		aDoc.setString('* Groceries: milk, eggs, bread.');
		await a.mutate.put(WidgetSchema, { id: 'w1', body: encodeTextDoc(aDoc) });

		await a.syncNow();
		await b.syncNow();
		await a.syncNow();

		const got = plainTextOf((await a.get(WidgetSchema, 'w1'))?.body);
		expect(got).toBe('* Groceries: milk, cheese, eggs, bread.');
		expect(plainTextOf((await b.get(WidgetSchema, 'w1'))?.body)).toBe(got);
		await a.close();
		await b.close();
	});

	it('junk text bytes are rejected, not stored', async () => {
		const ws = freshWorkspace();
		const rejections: string[] = [];
		const client = newClient(ws, {
			onMutationRejected: (info) => rejections.push(info.errorCode),
		});
		await client.start();
		// The row starts without a document, so the junk PATCH raw-replaces
		// locally (nothing to merge against) and reaches the server, whose
		// ingress validation must reject it before it can brick the row.
		await client.mutate.put(WidgetSchema, { id: 'w1', label: 'ok' });
		await client.syncNow();

		await client.mutate.patch(WidgetSchema, 'w1', { body: new Uint8Array([ 0xff, 0x01 ]) });
		await client.syncNow();
		expect(rejections).toEqual([ 'invalid_argument' ]);

		// The rollback cleared the junk locally and the stored row is intact.
		await client.syncNow();
		const row = await client.get(WidgetSchema, 'w1');
		expect(row?.label).toBe('ok');
		expect(row?.body?.length ?? 0).toBe(0);
		await client.close();
	});

	// U+FEFF is also the UTF-8 byte-order mark. A decoder that strips a
	// leading BOM drops it from the head of a run, and the run no longer
	// matches its item count.
	it('a Go string write that starts with U+FEFF reads back unchanged', async () => {
		const ws = freshWorkspace();
		// The text a connector copies from a file saved with a byte-order mark.
		await serverTextString(ws, 'w1', '﻿# Notes');

		const client = newClient(ws);
		await client.start();
		await client.syncNow();
		const row = await client.get(WidgetSchema, 'w1');
		expect(plainTextOf(row?.body)).toBe('﻿# Notes');
		await client.close();
	});

	it('text one client writes with a leading U+FEFF reads back unchanged on another', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		await a.start();
		await a.mutate.put(WidgetSchema, { id: 'w1', body: '﻿hello' });
		await a.syncNow();
		await a.close();

		const b = newClient(ws);
		await b.start();
		await b.syncNow();
		const row = await b.get(WidgetSchema, 'w1');
		expect(plainTextOf(row?.body)).toBe('﻿hello');
		await b.close();
	});
});
