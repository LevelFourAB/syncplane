// The annotation pattern: range as a "comment:<id>" mark inside the text
// document, content as an ordinary entity row keyed by the same id, both
// written in one transact() batch. See src/text/annotations.ts.

import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { AnnotationSchema, NoteSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import {
	commentIdOf,
	commentKey,
	commentRanges,
	joinAnnotations,
} from '../../src/text/annotations.js';
import {
	decodeTextDoc,
	encodeTextDoc,
	ExpandNone,
	TextDoc,
} from '../../src/text/index.js';

function newClient() {
	return new SyncClient({
		workspaceId: 'ws1',
		kvStore: new MemoryKVStore(),
		entities: [ NoteSchema, AnnotationSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
	});
}

describe('comment keys', () => {
	it('round-trips ids through commentKey/commentIdOf', () => {
		expect(commentKey('a1')).toBe('comment:a1');
		expect(commentIdOf('comment:a1')).toBe('a1');
		expect(commentIdOf('bold')).toBeUndefined();
	});

	it('extracts and joins ranges from resolved spans', () => {
		const spans = [
			{ start: 0, end: 4, key: 'bold', value: true as const },
			{ start: 2, end: 8, key: 'comment:a1', value: true as const },
			{ start: 6, end: 9, key: 'comment:a2', value: true as const },
		];
		expect(commentRanges(spans)).toEqual([
			{ id: 'a1', start: 2, end: 8 },
			{ id: 'a2', start: 6, end: 9 },
		]);
		const rows = [ { id: 'a2', body: 'second' }, { id: 'a1', body: 'first' }, {
			id: 'a3',
			body: 'orphan',
		} ];
		const joined = joinAnnotations(rows, (r) => r.id, spans);
		expect(joined.map((j) => [ j.id, j.ranges ])).toEqual([
			[ 'a1', [ { start: 2, end: 8 } ] ],
			[ 'a2', [ { start: 6, end: 9 } ] ],
			[ 'a3', [] ], // orphaned (no surviving mark) sorts last
		]);
	});
});

describe('annotation pattern end to end', () => {
	it('writes mark and row in one batch, and the range follows the text', async () => {
		const client = newClient();
		const seed = new TextDoc('seed');
		seed.insert(0, 'hello world');
		await client.db.note.mutate.put({ id: 'n1', body: encodeTextDoc(seed) });

		// One atomic batch: the marked document state and the comment row.
		const clientId = await client.clientId();
		await client.transact(async (tx) => {
			const note = await tx.db.note.get('n1');
			const doc = decodeTextDoc(note!.body, clientId);
			doc.mark(6, 11, commentKey('a1'), true, ExpandNone);
			await tx.db.note.mutate.patch('n1', { body: encodeTextDoc(doc) });
			await tx.db.annotation.mutate.put({
				id: 'a1',
				note: { id: 'n1' },
				body: 'nice word',
				author: 'alice',
			});
		});

		const handle = await client.db.note.text('n1', 'body');
		// A later edit before the range: the comment stays on "world".
		await handle.insert(0, '>> ');
		const rows = await client.db.annotation.scan();
		const joined = joinAnnotations(rows.map((r) => r.value), (r) => r.id, handle.marks());
		expect(joined).toHaveLength(1);
		expect(joined[0]!.ranges).toEqual([ { start: 9, end: 14 } ]);
		expect(handle.value.slice(9, 14)).toBe('world');
		expect(joined[0]!.row.body).toBe('nice word');

		handle.close();
		await client.close();
	});

	it('unmark orphans the row but keeps it listable', async () => {
		const client = newClient();
		const seed = new TextDoc('seed');
		seed.insert(0, 'abc');
		await client.db.note.mutate.put({ id: 'n1', body: encodeTextDoc(seed) });
		await client.db.annotation.mutate.put({ id: 'a1', body: 'gone' });

		const handle = await client.db.note.text('n1', 'body');
		await handle.mark(0, 3, commentKey('a1'), true);
		await handle.unmark(0, 3, commentKey('a1'));

		const rows = await client.db.annotation.scan();
		const joined = joinAnnotations(rows.map((r) => r.value), (r) => r.id, handle.marks());
		expect(joined).toEqual([ { id: 'a1', row: rows[0]!.value, ranges: [] } ]);

		handle.close();
		await client.close();
	});
});
