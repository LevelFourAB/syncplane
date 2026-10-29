// Op-level undo for text fields: edits record symbolic inverses that
// materialise against the then-current document, so undo removes exactly the
// local edit and leaves concurrent peers' work intact (a snapshot inverse
// would merge to a silent no-op on a state CRDT).

import { describe, expect, it, vi } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { AnnotationSchema, NoteSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { commentKey, joinAnnotations } from '../../src/text/annotations.js';
import {
	decodeTextDoc,
	encodeTextDoc,
	ExpandNone,
	mergeInto,
	TextDoc,
} from '../../src/text/index.js';
import {
	applyTextInverse,
	recordMarkInverse,
	type TextInverseOp,
} from '../../src/text/undo-ops.js';

function newClient() {
	return new SyncClient({
		workspaceId: 'ws1',
		kvStore: new MemoryKVStore(),
		entities: [ WidgetSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
		undo: {},
	});
}

function newNotesClient() {
	return new SyncClient({
		workspaceId: 'ws1',
		kvStore: new MemoryKVStore(),
		entities: [ NoteSchema, AnnotationSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
		undo: {},
	});
}

function bodyDoc(peer: string, s: string): Uint8Array {
	const d = new TextDoc(peer);
	d.insert(0, s);
	return encodeTextDoc(d);
}

/** A replica of d that writes as peer. */
function fork(d: TextDoc, peer: string): TextDoc {
	return decodeTextDoc(encodeTextDoc(d), peer);
}

describe('text undo', () => {
	it('undoes an insert and redoes it', async () => {
		const client = newClient();
		await client.db.widget.mutate.put({ id: 'w1', body: bodyDoc('seed', 'hello') });
		const handle = await client.db.widget.text('w1', 'body');

		await handle.insert(5, ' world');
		expect(handle.value).toBe('hello world');

		expect(await client.undo()).toBe(true);
		await vi.waitFor(() => {
			expect(handle.value).toBe('hello');
		});

		expect(await client.redo()).toBe(true);
		await vi.waitFor(() => {
			expect(handle.value).toBe('hello world');
		});

		handle.close();
		await client.close();
	});

	it('undoes a delete by re-inserting the content between survivors', async () => {
		const client = newClient();
		await client.db.widget.mutate.put({ id: 'w1', body: bodyDoc('seed', 'hello world') });
		const handle = await client.db.widget.text('w1', 'body');

		await handle.delete(5, 6);
		expect(handle.value).toBe('hello');

		expect(await client.undo()).toBe(true);
		await vi.waitFor(() => {
			expect(handle.value).toBe('hello world');
		});

		handle.close();
		await client.close();
	});

	it('undo removes only local edits and leaves a concurrent peer edit intact', async () => {
		const client = newClient();
		const base = bodyDoc('seed', 'hello world');
		await client.db.widget.mutate.put({ id: 'w1', body: base });
		const handle = await client.db.widget.text('w1', 'body');

		// Local edit, then a "remote" peer edit merged in through the row.
		await handle.insert(11, '!');
		const remote = decodeTextDoc(base, 'bob');
		remote.insert(0, '>> ');
		await client.db.widget.mutate.patch('w1', { body: encodeTextDoc(remote) });
		await vi.waitFor(() => {
			expect(handle.value).toBe('>> hello world!');
		});

		// Two undo steps queued locally: the remote-merge patch recorded a
		// snapshot inverse (a no-op union), the handle insert a symbolic one.
		// Undoing both must remove only the local '!'.
		expect(await client.undo()).toBe(true);
		expect(await client.undo()).toBe(true);
		await vi.waitFor(() => {
			expect(handle.value).toBe('>> hello world');
		});

		handle.close();
		await client.close();
	});

	it('undoing a mark keeps the mark a peer applied over part of it afterwards', async () => {
		const client = newClient();
		const base = bodyDoc('seed', 'hello world');
		await client.db.widget.mutate.put({ id: 'w1', body: base });
		const handle = await client.db.widget.text('w1', 'body');

		await handle.mark(0, 11, 'color', 'red');
		// bob has seen the red: his blue has the higher id.
		const remote = decodeTextDoc((await client.db.widget.get('w1'))!.body, 'bob');
		remote.mark(6, 11, 'color', 'blue', ExpandNone);
		await client.db.widget.mutate.patch('w1', { body: encodeTextDoc(remote) });
		await vi.waitFor(() => {
			expect(handle.marks()).toEqual([
				{ start: 0, end: 6, key: 'color', value: 'red' },
				{ start: 6, end: 11, key: 'color', value: 'blue' },
			]);
		});

		// The remote-merge patch's snapshot inverse is a no-op union; the
		// second step undoes the red.
		expect(await client.undo()).toBe(true);
		expect(await client.undo()).toBe(true);
		await vi.waitFor(() => {
			expect(handle.marks()).toEqual([ { start: 6, end: 11, key: 'color', value: 'blue' } ]);
		});

		handle.close();
		await client.close();
	});

	it('coalesces rapid typing into one undo group per word', async () => {
		const client = newClient();
		await client.db.widget.mutate.put({ id: 'w1', body: bodyDoc('seed', '') });
		const handle = await client.db.widget.text('w1', 'body');

		for (const ch of 'one') await handle.insert(handle.value.length, ch);
		await handle.insert(handle.value.length, ' ');
		for (const ch of 'two') await handle.insert(handle.value.length, ch);
		expect(handle.value).toBe('one two');

		// One undo removes the whole second word...
		expect(await client.undo()).toBe(true);
		await vi.waitFor(() => {
			expect(handle.value).toBe('one ');
		});
		// ...and the next removes the first word plus its trailing space
		// (the boundary insert joined the open group).
		expect(await client.undo()).toBe(true);
		await vi.waitFor(() => {
			expect(handle.value).toBe('');
		});

		handle.close();
		await client.close();
	});

	// A whole-value writer — useText().setText, or a rich-text binding
	// serialising its document — rewrites the whole field on every keystroke, so
	// the word rule has to come off the diff rather than off the argument.
	it('coalesces a whole-value writer into one undo group per word', async () => {
		const client = newClient();
		await client.db.widget.mutate.put({ id: 'w1', body: bodyDoc('seed', '') });
		const handle = await client.db.widget.text('w1', 'body');

		let typed = '';
		for (const ch of 'one two') {
			typed += ch;
			await handle.setString(typed);
		}
		expect(handle.value).toBe('one two');

		expect(await client.undo()).toBe(true);
		await vi.waitFor(() => {
			expect(handle.value).toBe('one ');
		});
		expect(await client.undo()).toBe(true);
		await vi.waitFor(() => {
			expect(handle.value).toBe('');
		});

		handle.close();
		await client.close();
	});

	// A run of backspaces coalesces into ONE undo group holding several
	// reinsert inverses over the same field. They all materialise before any of
	// them is enqueued, so each has to continue from the document the previous
	// one produced — materialising each against the unchanged stored row would
	// allocate the same item ids twice for different content and wedge the
	// field on the merge.
	it('undoes a coalesced run of deletes in one step', async () => {
		const client = newClient();
		await client.db.widget.mutate.put({ id: 'w1', body: bodyDoc('seed', 'abcdef') });
		const handle = await client.db.widget.text('w1', 'body');

		await handle.delete(5, 1); // 'f'
		await handle.delete(4, 1); // 'e'
		await handle.delete(3, 1); // 'd'
		expect(handle.value).toBe('abc');

		expect(await client.undo()).toBe(true);
		await vi.waitFor(() => {
			expect(handle.value).toBe('abcdef');
		});

		// The store agrees, so the PATCHes really merged rather than throwing.
		const again = await client.db.widget.text('w1', 'body');
		expect(again.value).toBe('abcdef');

		expect(await client.redo()).toBe(true);
		await vi.waitFor(() => {
			expect(handle.value).toBe('abc');
		});

		handle.close();
		again.close();
		await client.close();
	});

	it('undoes mark and unmark by restoring prior resolved values', async () => {
		const client = newClient();
		await client.db.widget.mutate.put({ id: 'w1', body: bodyDoc('seed', 'abcdef') });
		const handle = await client.db.widget.text('w1', 'body');

		await handle.mark(0, 6, 'color', 'red');
		await handle.mark(2, 4, 'color', 'blue');
		expect(handle.marks()).toEqual([
			{ start: 0, end: 2, key: 'color', value: 'red' },
			{ start: 2, end: 4, key: 'color', value: 'blue' },
			{ start: 4, end: 6, key: 'color', value: 'red' },
		]);

		// Undo the blue overlay: the red mark it overwrote is restored.
		expect(await client.undo()).toBe(true);
		await vi.waitFor(() => {
			expect(handle.marks()).toEqual([ { start: 0, end: 6, key: 'color', value: 'red' } ]);
		});
		// Undo the red mark entirely.
		expect(await client.undo()).toBe(true);
		await vi.waitFor(() => {
			expect(handle.marks()).toEqual([]);
		});
		// Redo brings red back.
		expect(await client.redo()).toBe(true);
		await vi.waitFor(() => {
			expect(handle.marks()).toEqual([ { start: 0, end: 6, key: 'color', value: 'red' } ]);
		});

		handle.close();
		await client.close();
	});

	// The re-inserted content is new items, so the mark ops over the deleted
	// ones resolve to nothing; the inverse carries the marks that covered the
	// text at delete time and re-applies them over the restored range. Both
	// marks here STRADDLE the deleted range, which is the case that needs the
	// restore to be exact rather than additive: their anchors survive, so the
	// re-inserted items fall inside them and would otherwise come back covered
	// by both.
	it('restores the marks over text a delete removed', async () => {
		const client = newClient();
		await client.db.widget.mutate.put({ id: 'w1', body: bodyDoc('seed', 'hello world') });
		const handle = await client.db.widget.text('w1', 'body');

		await handle.mark(0, 5, 'bold', true, ExpandNone);
		await handle.mark(6, 11, 'color', 'red', ExpandNone);
		await handle.delete(4, 4); // 'o wo' — the tail of bold, the head of red

		expect(handle.value).toBe('hellrld');
		expect(handle.marks()).toEqual([
			{ start: 0, end: 4, key: 'bold', value: true },
			{ start: 4, end: 7, key: 'color', value: 'red' },
		]);

		expect(await client.undo()).toBe(true);
		await vi.waitFor(() => {
			expect(handle.value).toBe('hello world');
		});
		expect(handle.marks()).toEqual([
			{ start: 0, end: 5, key: 'bold', value: true },
			{ start: 6, end: 11, key: 'color', value: 'red' },
		]);

		handle.close();
		await client.close();
	});

	// Every cycle mints fresh items and fresh mark ops, so the restore has to
	// come from the marks resolved at THAT delete, not from the original record.
	it('keeps marks across a delete/undo/redo/undo cycle', async () => {
		const client = newClient();
		await client.db.widget.mutate.put({ id: 'w1', body: bodyDoc('seed', 'abcdef') });
		const handle = await client.db.widget.text('w1', 'body');

		await handle.mark(1, 5, 'bold', true, ExpandNone);
		await handle.delete(2, 2);
		expect(await client.undo()).toBe(true);
		await vi.waitFor(() => {
			expect(handle.value).toBe('abcdef');
		});
		expect(await client.redo()).toBe(true);
		await vi.waitFor(() => {
			expect(handle.value).toBe('abef');
		});
		expect(await client.undo()).toBe(true);
		await vi.waitFor(() => {
			expect(handle.value).toBe('abcdef');
		});
		expect(handle.marks()).toEqual([ { start: 1, end: 5, key: 'bold', value: true } ]);

		handle.close();
		await client.close();
	});

	// The comment case the annotation pattern is built on: a deleted commented
	// range is orphaned, and undoing the delete un-orphans it.
	it('restores a comment range when its text is deleted and undone', async () => {
		const client = newNotesClient();
		const clientId = await client.clientId();
		await client.db.note.mutate.put({ id: 'n1', body: bodyDoc('seed', 'hello world') });
		await client.db.annotation.mutate.put({ id: 'a1', body: 'a comment' });

		const handle = await client.db.note.text('n1', 'body');
		await handle.mark(6, 11, commentKey('a1'), true, ExpandNone);
		expect(joinAnnotations([ { id: 'a1' } ], (r) => r.id, handle.marks())[0]!.ranges).toEqual([
			{ start: 6, end: 11 },
		]);

		await handle.delete(5, 6);
		expect(joinAnnotations([ { id: 'a1' } ], (r) => r.id, handle.marks())[0]!.ranges).toEqual([]);

		expect(await client.undo()).toBe(true);
		await vi.waitFor(() => {
			expect(handle.value).toBe('hello world');
		});
		expect(joinAnnotations([ { id: 'a1' } ], (r) => r.id, handle.marks())[0]!.ranges).toEqual([
			{ start: 6, end: 11 },
		]);

		handle.close();
		await client.close();
	});

	it('undoes an annotation batch (mark + row) as one step', async () => {
		const client = newNotesClient();
		const seed = bodyDoc('seed', 'hello world');
		await client.db.note.mutate.put({ id: 'n1', body: seed });

		const clientId = await client.clientId();
		await client.transact(async (tx) => {
			const note = await tx.db.note.get('n1');
			const doc = decodeTextDoc(note!.body, clientId);
			doc.mark(6, 11, commentKey('a1'), true, ExpandNone);
			await tx.db.note.mutate.patch('n1', { body: encodeTextDoc(doc) });
			await tx.db.annotation.mutate.put({ id: 'a1', body: 'a comment' });
		});
		expect(await client.db.annotation.get('a1')).toBeDefined();

		// One undo removes both the row and (via the snapshot inverse being a
		// union no-op... the mark op remains in the doc but the ROW is gone,
		// so the comment no longer renders — the annotation pattern's undo
		// contract is row-level).
		expect(await client.undo()).toBe(true);
		expect(await client.db.annotation.get('a1')).toBeUndefined();

		await client.close();
	});
});

describe('text inverses after a peer edited on top of the local edit', () => {
	it('keeps text a peer typed inside the run being undone', () => {
		const alice = new TextDoc('alice');
		const range = alice.insert(0, 'hello');
		const inverse: TextInverseOp[] = [
			{ type: 'deleteItems', peer: range.peer, start: range.start, count: range.count },
		];
		const bob = fork(alice, 'bob');
		bob.insert(2, 'XX');
		mergeInto(alice, bob);
		expect(alice.plainText()).toBe('heXXllo');

		applyTextInverse(alice, inverse);

		expect(alice.plainText()).toBe('XX');
	});

	it('keeps a mark a peer applied over part of the range after the local mark', () => {
		const alice = new TextDoc('alice');
		alice.insert(0, 'hello world');
		const inverse = [ recordMarkInverse(alice, 0, 11, 'color', 'red') ];
		alice.mark(0, 11, 'color', 'red', ExpandNone);
		const bob = fork(alice, 'bob');
		bob.mark(6, 11, 'color', 'blue', ExpandNone);
		mergeInto(alice, bob);
		expect(alice.resolveMarks()).toEqual([
			{ start: 0, end: 6, key: 'color', value: 'red' },
			{ start: 6, end: 11, key: 'color', value: 'blue' },
		]);

		applyTextInverse(alice, inverse);

		expect(alice.resolveMarks()).toEqual([ { start: 6, end: 11, key: 'color', value: 'blue' } ]);
	});

	it('redoes the local mark only where the undo cleared it', () => {
		const alice = new TextDoc('alice');
		alice.insert(0, 'hello world');
		const inverse = [ recordMarkInverse(alice, 0, 11, 'color', 'red') ];
		alice.mark(0, 11, 'color', 'red', ExpandNone);
		const bob = fork(alice, 'bob');
		bob.mark(6, 11, 'color', 'blue', ExpandNone);
		mergeInto(alice, bob);

		const redo = applyTextInverse(alice, inverse);
		applyTextInverse(alice, redo);

		expect(alice.resolveMarks()).toEqual([
			{ start: 0, end: 6, key: 'color', value: 'red' },
			{ start: 6, end: 11, key: 'color', value: 'blue' },
		]);
	});

	it('keeps a mark a peer applied after the local unmark', () => {
		const alice = new TextDoc('alice');
		alice.insert(0, 'hello world');
		alice.mark(0, 11, 'bold', true, ExpandNone);
		const inverse = [ recordMarkInverse(alice, 0, 11, 'bold', null) ];
		alice.unmark(0, 11, 'bold', ExpandNone);
		const bob = fork(alice, 'bob');
		bob.mark(0, 5, 'bold', false, ExpandNone);
		mergeInto(alice, bob);

		applyTextInverse(alice, inverse);

		expect(alice.resolveMarks()).toEqual([
			{ start: 0, end: 5, key: 'bold', value: false },
			{ start: 5, end: 11, key: 'bold', value: true },
		]);
	});
});
