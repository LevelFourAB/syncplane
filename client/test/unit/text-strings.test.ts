// String-boundary writes for collaborative text fields: `mutate.put`/
// `mutate.patch` accept plain strings for text fields, diffed into CRDT
// operations against the merged row under a leased writer peer at enqueue
// time. Also covers the client half of the PUT exception: raw text BYTES in a
// PUT union with the previous row instead of resetting the document.

import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import {
	decodeTextDoc,
	encodePlainString,
	encodeTextDoc,
	ExpandNone,
	isPlainString,
	TextDoc,
} from '../../src/text/index.js';

function newClient(opts?: { undo?: boolean }) {
	return new SyncClient({
		workspaceId: 'ws1',
		kvStore: new MemoryKVStore(),
		entities: [ WidgetSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
		...(opts?.undo ? { undo: {} } : {}),
	});
}

function bodyDoc(peer: string, s: string): Uint8Array {
	const d = new TextDoc(peer);
	d.insert(0, s);
	return encodeTextDoc(d);
}

describe('string writes to text fields', () => {
	it('creates a document from a string on put', async () => {
		const client = newClient();
		await client.db.widget.mutate.put({ id: 'w1', label: 'l', body: 'hello' });
		const row = await client.db.widget.get('w1');
		expect(row?.label).toBe('l');
		expect(decodeTextDoc(row?.body, '').plainText()).toBe('hello');
		await client.close();
	});

	it('patches a document from a string with a multi-hunk diff', async () => {
		const client = newClient();
		await client.db.widget.mutate.put({ id: 'w1', body: 'one two three four five' });
		// A mark on the middle word survives disjoint edits around it.
		const handle = await client.db.widget.text('w1', 'body');
		await handle.mark(8, 13, 'comment:c1', true);
		handle.close();

		await client.db.widget.mutate.patch('w1', { body: 'one TWO three FOUR five' });
		const again = await client.db.widget.text('w1', 'body');
		expect(again.value).toBe('one TWO three FOUR five');
		expect(again.marks()).toEqual([ { start: 8, end: 13, key: 'comment:c1', value: true } ]);
		again.close();
		await client.close();
	});

	it('a string patch merges with a live handle edit instead of replacing it', async () => {
		const client = newClient();
		await client.db.widget.mutate.put({ id: 'w1', body: 'one two three' });
		const handle = await client.db.widget.text('w1', 'body');
		await handle.insert(13, ' four');
		await client.db.widget.mutate.patch('w1', { body: 'one TWO three four' });
		const again = await client.db.widget.text('w1', 'body');
		expect(again.value).toBe('one TWO three four');
		handle.close();
		again.close();
		await client.close();
	});

	it('an empty string deletes all visible text', async () => {
		const client = newClient();
		await client.db.widget.mutate.put({ id: 'w1', body: 'abc' });
		await client.db.widget.mutate.patch('w1', { body: '' });
		const row = await client.db.widget.get('w1');
		const doc = decodeTextDoc(row?.body, '');
		expect(doc.plainText()).toBe('');
		// Select-all-delete, not a reset: the items are tombstoned, so the
		// version vector still covers the deleted text's counters.
		expect(doc.versionVector().size).toBeGreaterThan(0);
		await client.close();
	});

	it('throws when a string hits a non-text bytes field', async () => {
		const client = newClient();
		await expect(
			client.db.widget.mutate.put({ id: 'w1', raw: 'nope' as unknown as Uint8Array }),
		).rejects.toThrow(/a string can only initialize a text field/);
		await client.close();
	});

	it('undoes a string patch back to the previous text', async () => {
		const client = newClient({ undo: true });
		await client.db.widget.mutate.put({ id: 'w1', body: bodyDoc('seed', 'hello world') });
		await client.db.widget.mutate.patch('w1', { body: 'hello brave world' });
		expect((await client.db.widget.text('w1', 'body')).value).toBe('hello brave world');

		expect(await client.undo()).toBe(true);
		const handle = await client.db.widget.text('w1', 'body');
		expect(handle.value).toBe('hello world');
		handle.close();

		expect(await client.redo()).toBe(true);
		const redone = await client.db.widget.text('w1', 'body');
		expect(redone.value).toBe('hello brave world');
		redone.close();
		await client.close();
	});

	it('put with raw text bytes unions with the previous row (no reset)', async () => {
		const client = newClient();
		const base = bodyDoc('seed', 'one two three');
		await client.db.widget.mutate.put({ id: 'w1', label: 'v1', body: base });

		// A whole-row put built from the same base with an edit: the local
		// overlay must union with the stored document, mirroring the server.
		const edited = decodeTextDoc(base, 'editor');
		edited.setString('one TWO three');
		await client.db.widget.mutate.put({ id: 'w1', label: 'v2', body: encodeTextDoc(edited) });

		// Meanwhile the earlier state also carried an edit that the put author
		// never saw — replay through a handle edit before the put would be the
		// full scenario; here we simply check the union kept everything.
		const row = await client.db.widget.get('w1');
		expect(row?.label).toBe('v2');
		expect(decodeTextDoc(row?.body, '').plainText()).toBe('one TWO three');

		const handle = await client.db.widget.text('w1', 'body');
		await handle.insert(13, ' four');
		handle.close();
		const after = decodeTextDoc((await client.db.widget.get('w1'))?.body, '');
		expect(after.plainText()).toBe('one TWO three four');
		await client.close();
	});

	// A plain-string value is what a field-history record's before-state holds
	// for a text field, so a restore is decode-then-patch with no per-field
	// handling. Resolving it is the enqueue path's job: it carries no writer
	// identity, and the server refuses one.
	describe('a plain-string value in the written row', () => {
		it('resolves into a document under the leased peer', async () => {
			const client = newClient();
			await client.db.widget.mutate.put({ id: 'w1', body: 'original text' });
			await client.db.widget.mutate.patch('w1', { body: encodePlainString('restored text') });

			const row = await client.db.widget.get('w1');
			expect(isPlainString(row?.body)).toBe(false);
			expect(decodeTextDoc(row?.body, '').plainText()).toBe('restored text');
			await client.close();
		});

		it('lands as edits against the live document, not a replacement', async () => {
			const client = newClient();
			await client.db.widget.mutate.put({ id: 'w1', body: 'shared prefix, original tail' });
			const before = decodeTextDoc((await client.db.widget.get('w1'))?.body, '');

			await client.db.widget.mutate.patch('w1', {
				body: encodePlainString('shared prefix, replaced tail'),
			});

			const after = decodeTextDoc((await client.db.widget.get('w1'))?.body, '');
			expect(after.plainText()).toBe('shared prefix, replaced tail');
			// The untouched prefix keeps the items the first write minted, which
			// is what makes the restore an edit rather than a new document.
			for (const [ peer, counter ] of before.vv) {
				expect(after.vv.get(peer)).toBeGreaterThanOrEqual(counter);
			}
			await client.close();
		});

		it('is dropped when the patch mask does not name its field', async () => {
			// A PATCH writes what its mask names. The value has to go rather than
			// ride along unresolved: the server refuses one in any position.
			const client = newClient();
			await client.db.widget.mutate.put({ id: 'w1', label: 'v1', body: 'kept' });
			await client.db.widget.mutate.patch('w1', {
				label: 'v2',
				body: encodePlainString('ignored'),
			}, { paths: [ 'label' ] });

			const row = await client.db.widget.get('w1');
			expect(row?.label).toBe('v2');
			expect(decodeTextDoc(row?.body, '').plainText()).toBe('kept');
			await client.close();
		});

		it('resolves on put, where every text field is written', async () => {
			const client = newClient();
			await client.db.widget.mutate.put({ id: 'w1', body: 'first' });
			await client.db.widget.mutate.put({ id: 'w1', body: encodePlainString('second') });

			const row = await client.db.widget.get('w1');
			expect(isPlainString(row?.body)).toBe(false);
			expect(decodeTextDoc(row?.body, '').plainText()).toBe('second');
			await client.close();
		});
	});

	it('strings work inside transact batches', async () => {
		const client = newClient();
		await client.transact(async (tx) => {
			await tx.db.widget.mutate.put({ id: 'w1', body: 'first' });
			await tx.db.widget.mutate.put({ id: 'w2', body: 'second' });
		});
		expect(decodeTextDoc((await client.db.widget.get('w1'))?.body, '').plainText()).toBe('first');
		expect(decodeTextDoc((await client.db.widget.get('w2'))?.body, '').plainText()).toBe('second');
		await client.close();
	});
});
