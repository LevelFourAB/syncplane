import { fromBinary, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it, vi } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { MutationSchema } from '../../src/gen/syncplane/v1/mutation_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { readPending } from '../../src/optimistic/queue.js';
import {
	decodeTextDoc,
	encodeTextDoc,
	isFragment,
	plainTextOf,
	type TextDelta,
	TextDoc,
} from '../../src/text/index.js';

function newClient(kvStore: MemoryKVStore = new MemoryKVStore()) {
	return new SyncClient({
		workspaceId: 'ws1',
		kvStore,
		entities: [ WidgetSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
	});
}

function bodyDoc(peer: string, s: string): Uint8Array {
	const d = new TextDoc(peer);
	d.insert(0, s);
	return encodeTextDoc(d);
}

describe('client.db.<entity>.text', () => {
	it('rejects non-text fields and missing rows', async () => {
		const client = newClient();
		await expect(client.db.widget.text('w1', 'label')).rejects.toThrow(/not a text field/);
		await expect(client.db.widget.text('missing', 'body')).rejects.toThrow(/row not found/);
		await client.close();
	});

	it('edits locally and persists through the overlay', async () => {
		const client = newClient();
		await client.db.widget.mutate.put({ id: 'w1', body: bodyDoc('seed', 'hello') });
		const handle = await client.db.widget.text('w1', 'body');
		expect(handle.value).toBe('hello');

		await handle.insert(5, ' world');
		await handle.delete(0, 1);
		await handle.insert(0, 'H');
		expect(handle.value).toBe('Hello world');

		// The PATCHes really landed: a fresh handle over the stored row agrees.
		const again = await client.db.widget.text('w1', 'body');
		expect(again.value).toBe('Hello world');

		handle.close();
		again.close();
		await client.close();
	});

	it('converts UTF-16 indices at the boundary', async () => {
		const client = newClient();
		await client.db.widget.mutate.put({ id: 'w1', body: bodyDoc('seed', 'a🎉b') });
		const handle = await client.db.widget.text('w1', 'body');

		// '🎉' is one code point but two UTF-16 units; index 3 is after it.
		await handle.insert(3, 'X');
		expect(handle.value).toBe('a🎉Xb');
		await handle.mark(1, 3, 'comment:c1', true);
		expect(handle.marks()).toEqual([ { start: 1, end: 3, key: 'comment:c1', value: true } ]);
		expect(() => handle.anchorAt(2)).toThrow(/surrogate/);

		const anchor = handle.anchorAt(3);
		expect(handle.resolve(anchor)).toBe(3);

		handle.close();
		await client.close();
	});

	it('emits deltas for local edits', async () => {
		const client = newClient();
		await client.db.widget.mutate.put({ id: 'w1', body: bodyDoc('seed', 'abc') });
		const handle = await client.db.widget.text('w1', 'body');
		const deltas: TextDelta[] = [];
		handle.subscribe((delta) => deltas.push(delta));

		await handle.insert(3, 'd');
		await handle.delete(0, 2);
		await handle.mark(0, 2, 'm', true);

		expect(deltas).toEqual([
			{ ops: [ { retain: 3 }, { insert: 'd' } ], marksChanged: false },
			{ ops: [ { delete: 2 } ], marksChanged: false },
			{ ops: [], marksChanged: true },
		]);

		handle.close();
		await client.close();
	});

	it('flags mark changes on edits once the document carries marks', async () => {
		const client = newClient();
		await client.db.widget.mutate.put({ id: 'w1', body: bodyDoc('seed', 'abcdef') });
		const handle = await client.db.widget.text('w1', 'body');
		await handle.mark(1, 4, 'bold', true);

		const deltas: TextDelta[] = [];
		handle.subscribe((delta) => deltas.push(delta));

		// Spans are resolved against the item order, so this insert widens the
		// bold run — the binding has to re-read marks even though no mark op was
		// added.
		await handle.insert(2, 'X');
		await handle.delete(0, 1);
		expect(deltas).toEqual([
			{ ops: [ { retain: 2 }, { insert: 'X' } ], marksChanged: true },
			{ ops: [ { delete: 1 } ], marksChanged: true },
		]);
		expect(handle.marks()).toEqual([ { start: 0, end: 4, key: 'bold', value: true } ]);

		handle.close();
		await client.close();
	});

	it('merges remote changes arriving through the watch and emits their diff', async () => {
		const client = newClient();
		const base = bodyDoc('seed', 'hello world');
		await client.db.widget.mutate.put({ id: 'w1', body: base });
		const handle = await client.db.widget.text('w1', 'body');

		const deltas: TextDelta[] = [];
		const values: string[] = [];
		handle.subscribe((delta, value) => {
			deltas.push(delta);
			values.push(value);
		});

		// A "remote" edit: another peer's state patched into the same row
		// (locally this exercises the same watch → merge → diff path a pulled
		// server change takes).
		const remote = decodeTextDoc(base, 'bob');
		remote.insert(0, '>> ');
		await client.db.widget.mutate.patch('w1', { body: encodeTextDoc(remote) });

		await vi.waitFor(() => {
			expect(handle.value).toBe('>> hello world');
		});
		expect(deltas).toEqual([
			{ ops: [ { insert: '>> ' } ], marksChanged: false },
		]);
		expect(values).toEqual([ '>> hello world' ]);

		handle.close();
		await client.close();
	});

	it('isolates a throwing subscriber from the edit and from other subscribers', async () => {
		const client = newClient();
		const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
		try {
			await client.db.widget.mutate.put({ id: 'w1', body: bodyDoc('seed', 'abc') });
			const handle = await client.db.widget.text('w1', 'body');
			handle.subscribe(() => {
				throw new Error('boom');
			});
			const values: string[] = [];
			handle.subscribe((_delta, value) => values.push(value));

			// The doc has already changed when subscribers run, so a throw must not
			// escape the edit — it would leave the local doc ahead of the store with
			// no PATCH queued — and must not cost the later subscriber its delta.
			await handle.insert(3, 'd');
			expect(values).toEqual([ 'abcd' ]);
			expect(errSpy).toHaveBeenCalled();

			// The PATCH went out regardless: a fresh handle sees the edit.
			const again = await client.db.widget.text('w1', 'body');
			expect(again.value).toBe('abcd');

			// Same isolation on the remote path (watch → merge → diff).
			const remote = decodeTextDoc(bodyDoc('seed', 'abc'), 'bob');
			remote.insert(0, '>');
			await client.db.widget.mutate.patch('w1', { body: encodeTextDoc(remote) });
			await vi.waitFor(() => {
				expect(values).toEqual([ 'abcd', '>abcd' ]);
			});

			handle.close();
			again.close();
			await client.close();
		} finally {
			errSpy.mockRestore();
		}
	});

	// Item ids are (peer, counter) and counters come from the document's own
	// version vector, so two handles behind ONE peer mint the same id for
	// different content — a merge error, or worse a silent divergence. Each
	// handle therefore leases its own identity for as long as it is open.
	describe('writer identities', () => {
		it('gives two handles on the same field different peers', async () => {
			const client = newClient();
			await client.db.widget.mutate.put({ id: 'w1', body: bodyDoc('seed', 'abc') });
			const one = await client.db.widget.text('w1', 'body');
			const two = await client.db.widget.text('w1', 'body');

			expect(one.doc.peer).not.toBe(two.doc.peer);
			const clientId = await client.clientId();
			expect(one.doc.peer.startsWith(`${clientId}:`)).toBe(true);
			expect(two.doc.peer.startsWith(`${clientId}:`)).toBe(true);

			// And both editors' work survives, rather than one of them wedging
			// the field.
			await one.insert(0, 'X');
			await two.insert(0, 'Y');
			await vi.waitFor(async () => {
				const again = await client.db.widget.text('w1', 'body');
				expect(again.value).toContain('X');
				expect(again.value).toContain('Y');
				expect(again.value).toContain('abc');
				again.close();
			});

			one.close();
			two.close();
			await client.close();
		});

		it('returns the identity to the pool on close', async () => {
			const client = newClient();
			await client.db.widget.mutate.put({ id: 'w1', body: bodyDoc('seed', 'abc') });
			const first = await client.db.widget.text('w1', 'body');
			const peer = first.doc.peer;
			first.close();

			// Reuse is what keeps a document's peer table from growing by one
			// entry every time someone opens the editor.
			const second = await client.db.widget.text('w1', 'body');
			expect(second.doc.peer).toBe(peer);
			second.close();
			await client.close();
		});
	});

	it('keeps concurrent local and remote edits (union, not clobber)', async () => {
		const client = newClient();
		const base = bodyDoc('seed', 'abc');
		await client.db.widget.mutate.put({ id: 'w1', body: base });
		const handle = await client.db.widget.text('w1', 'body');

		// Local edit through the handle and a remote state built from the
		// same base land together.
		await handle.insert(3, 'X');
		const remote = decodeTextDoc(base, 'bob');
		remote.insert(0, 'Y');
		await client.db.widget.mutate.patch('w1', { body: encodeTextDoc(remote) });

		await vi.waitFor(() => {
			expect(handle.value).toBe('YabcX');
		});
		const again = await client.db.widget.text('w1', 'body');
		expect(again.value).toBe('YabcX');

		handle.close();
		again.close();
		await client.close();
	});

	// What a keystroke costs locally. The handle queues the delta for its one
	// edit, so the pending entry is the size of the keystroke; everything a
	// reader touches — the overlay, the stored row — still holds a whole
	// document.
	describe('what an edit writes locally', () => {
		/** A document big enough that queueing it per keystroke would show. */
		const big = (): Uint8Array => bodyDoc('seed', 'lorem ipsum dolor sit amet '.repeat(200));

		async function queuedBytes(kv: MemoryKVStore): Promise<number[]> {
			const tx = await kv.read();
			try {
				return (await readPending(tx, 'ws1')).map((m) => toBinary(MutationSchema, m).length);
			} finally {
				tx.release();
			}
		}

		it('queues bytes proportional to the edit, not to the document', async () => {
			const kv = new MemoryKVStore();
			const client = newClient(kv);
			const body = big();
			await client.db.widget.mutate.put({ id: 'w1', body });
			const handle = await client.db.widget.text('w1', 'body');

			const beforeEdits = (await queuedBytes(kv)).length;
			await handle.insert(0, 'a');
			await handle.insert(1, 'b');
			await handle.insert(2, 'c');

			const sizes = (await queuedBytes(kv)).slice(beforeEdits);
			expect(sizes).toHaveLength(3);
			for (const size of sizes) {
				expect(size).toBeLessThan(body.length / 20);
			}

			handle.close();
			await client.close();
		});

		it('leaves the row a whole document that any reader can decode', async () => {
			const kv = new MemoryKVStore();
			const client = newClient(kv);
			await client.db.widget.mutate.put({ id: 'w1', body: bodyDoc('seed', 'hello') });
			const handle = await client.db.widget.text('w1', 'body');
			await handle.insert(5, ' world');

			// Read back through the ordinary row API — no handle, no fragment
			// awareness anywhere on this path.
			const row = await client.db.widget.get('w1');
			expect(plainTextOf(row!.body)).toBe('hello world');
			expect(isFragment(row!.body)).toBe(false);

			handle.close();
			await client.close();
		});

		it('queues a whole document when the field had none', async () => {
			// The first edit to a row created without text has an empty base. The
			// fragment still has to expand rather than replace, or the row would be
			// left holding delta state.
			const kv = new MemoryKVStore();
			const client = newClient(kv);
			await client.db.widget.mutate.put({ id: 'w1', body: new Uint8Array() });
			const handle = await client.db.widget.text('w1', 'body');
			await handle.insert(0, 'first');

			const row = await client.db.widget.get('w1');
			expect(isFragment(row!.body)).toBe(false);
			expect(plainTextOf(row!.body)).toBe('first');

			handle.close();
			await client.close();
		});

		it('keeps typing into "a/b" out of a write of "a" built from what was read', async () => {
			// A row key may hold '/', so row "a"'s text log prefix also covers
			// row "a/b". The first keystroke into an empty document has no origin
			// and would apply to any document it was folded into.
			const kv = new MemoryKVStore();
			const client = newClient(kv);
			await client.db.widget.mutate.put({ id: 'a', body: bodyDoc('dev1', 'A') });
			await client.db.widget.mutate.put({ id: 'a/b', body: encodeTextDoc(new TextDoc('dev2')) });
			const handle = await client.db.widget.text('a/b', 'body');
			await handle.insert(0, 'leak');
			handle.close();

			const rowA = (await client.db.widget.get('a'))!;
			expect(plainTextOf(rowA.body)).toBe('A');
			await client.db.widget.mutate.put({ ...rowA, label: 'renamed' });

			const tx = await kv.read();
			const queued = await readPending(tx, 'ws1');
			tx.release();
			const lastOfA = queued
				.filter((m) => m.payload.case === 'crud' && m.payload.value.key === 'a')
				.at(-1)!;
			const value = (lastOfA.payload.value as { value: Uint8Array }).value;
			expect(plainTextOf(fromBinary(WidgetSchema, value).body)).toBe('A');
			await client.close();
		});
	});
});
