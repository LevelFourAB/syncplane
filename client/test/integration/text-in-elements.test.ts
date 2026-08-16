// Collaborative text inside keyed-list elements and map values, across the
// wire: both sides address one document per element by instance path
// ("notes.<id>.body"), concurrent edits merge element by element, and the
// upstream delta path (a handle's fragment PATCH) is accepted by the real
// server.

import { describe, expect, it } from 'vitest';

import { KeyedTextEntitySchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { decodeTextDoc, encodeTextDoc, plainTextOf, TextDoc } from '../../src/text/index.js';
import { freshWorkspace, newClient } from './helpers.js';

function bodyDoc(peer: string, s: string): Uint8Array {
	const d = new TextDoc(peer);
	d.insert(0, s);
	return encodeTextDoc(d);
}

describe('text in keyed list elements over the wire', () => {
	it('concurrent edits to one element merge; siblings stay untouched', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();
		await b.start();

		await a.mutate.put(KeyedTextEntitySchema, {
			id: 'k1',
			notes: [
				{ id: 'n1', label: 'first', body: bodyDoc('seed', '[]') },
				{ id: 'n2', label: 'second', body: bodyDoc('seed', 'other') },
			],
		});
		await a.syncNow();
		await b.syncNow();

		// Both type into the same position of the same element, offline of
		// each other; the pushes land in some order and must union.
		const aHandle = await a.text(KeyedTextEntitySchema, 'k1', 'notes.n1.body');
		const bHandle = await b.text(KeyedTextEntitySchema, 'k1', 'notes.n1.body');
		await aHandle.insert(1, 'alice');
		await bHandle.insert(1, 'bob');
		aHandle.close();
		bHandle.close();

		await a.syncNow();
		await b.syncNow();
		await a.syncNow();

		const rowA = await a.get(KeyedTextEntitySchema, 'k1');
		const rowB = await b.get(KeyedTextEntitySchema, 'k1');
		const n1 = (r: typeof rowA) => r?.notes.find((n) => n.id === 'n1');
		const n2 = (r: typeof rowA) => r?.notes.find((n) => n.id === 'n2');
		// The peers are random leases, so which insert wins the position
		// tie-break is not pinned — no interleaving and convergence are.
		expect(plainTextOf(n1(rowA)?.body)).toMatch(/^\[(alicebob|bobalice)\]$/);
		expect(plainTextOf(n1(rowB)?.body)).toBe(plainTextOf(n1(rowA)?.body));
		expect(n1(rowA)?.label).toBe('first');
		expect(plainTextOf(n2(rowA)?.body)).toBe('other');
	});

	it('a string patch on one element three-way merges on the server', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();
		await b.start();

		await a.mutate.put(KeyedTextEntitySchema, {
			id: 'k1',
			notes: [ { id: 'n1', body: 'one two three' } ],
		});
		await a.syncNow();
		await b.syncNow();

		// b appends via a raw document PATCH while a string-patches the middle
		// without having seen b's edit.
		const bBase = (await b.get(KeyedTextEntitySchema, 'k1'))?.notes[0]?.body;
		const bDoc = decodeTextDoc(bBase, 'bob');
		bDoc.insert(13, ' four');
		await b.mutate.patch(KeyedTextEntitySchema, 'k1', {
			notes: [ { id: 'n1', body: encodeTextDoc(bDoc) } ],
		});
		await b.syncNow();

		await a.mutate.patch(KeyedTextEntitySchema, 'k1', {
			notes: [ { id: 'n1', body: 'one TWO three' } ],
		});
		await a.syncNow();
		await a.syncNow();

		const row = await a.get(KeyedTextEntitySchema, 'k1');
		expect(plainTextOf(row?.notes[0]?.body)).toBe('one TWO three four');
	});

	it('map values carry their own documents', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();
		await b.start();

		await a.mutate.put(KeyedTextEntitySchema, {
			id: 'k1',
			sections: { intro: { body: bodyDoc('seed', '[]') } },
		});
		await a.syncNow();
		await b.syncNow();

		const aHandle = await a.text(KeyedTextEntitySchema, 'k1', 'sections.intro.body');
		const bHandle = await b.text(KeyedTextEntitySchema, 'k1', 'sections.intro.body');
		await aHandle.insert(1, 'alice');
		await bHandle.insert(1, 'bob');
		aHandle.close();
		bHandle.close();

		await a.syncNow();
		await b.syncNow();
		await a.syncNow();

		const rowA = await a.get(KeyedTextEntitySchema, 'k1');
		const rowB = await b.get(KeyedTextEntitySchema, 'k1');
		expect(plainTextOf(rowA?.sections['intro']?.body)).toMatch(/^\[(alicebob|bobalice)\]$/);
		expect(plainTextOf(rowB?.sections['intro']?.body))
			.toBe(plainTextOf(rowA?.sections['intro']?.body));
	});
});
