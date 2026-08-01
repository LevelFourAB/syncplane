// mutate.update() — read-modify-write that ships as a NARROW patch. The point
// of the verb is the mask: the recipe edits a whole draft row, but only the
// fields it actually changed reach the wire, so a concurrent writer's change to
// an untouched field survives where a PUT of the same draft would clobber it.
//
// The mask itself is asserted straight off the queued mutation — no server
// needed, because the mask is decided at build time.

import { fromBinary } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { type CrudMutation, CrudOp } from '../../src/gen/syncplane/v1/mutation_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { readPending } from '../../src/optimistic/queue.js';

const WS = 'ws1';

function newClient(kv: MemoryKVStore) {
	return new SyncClient({
		workspaceId: WS,
		kvStore: kv,
		entities: [ WidgetSchema, TodoSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
	});
}

/** The crud mutations sitting in the queue, in order. */
async function queued(kv: MemoryKVStore): Promise<CrudMutation[]> {
	const tx = await kv.read();
	try {
		return (await readPending(tx, WS))
			.map((m) => m.payload)
			.filter((p) => p.case === 'crud')
			.map((p) => p.value as CrudMutation);
	} finally {
		tx.release();
	}
}

/** The mask of the last queued crud (the one the update produced). */
async function lastMask(kv: MemoryKVStore): Promise<string[] | undefined> {
	const all = await queued(kv);
	return all.at(-1)?.fieldMask?.paths;
}

describe('mutate.update', () => {
	it('masks only the fields the recipe changed', async () => {
		const kv = new MemoryKVStore();
		const c = newClient(kv);
		await c.db.widget.mutate.put({ id: 'w1', label: 'before', count: 3, flag: true });

		await c.db.widget.mutate.update('w1', (w) => {
			w.label = 'after';
		});

		expect(await lastMask(kv)).toEqual([ 'label' ]);
		const w = await c.db.widget.get('w1');
		expect(w?.label).toBe('after');
		expect(w?.count).toBe(3); // untouched, and never named in the mask
		expect(w?.flag).toBe(true);
		await c.close();
	});

	it('queues a PATCH, not a PUT', async () => {
		const kv = new MemoryKVStore();
		const c = newClient(kv);
		await c.db.widget.mutate.put({ id: 'w1', label: 'a' });
		await c.db.widget.mutate.update('w1', (w) => {
			w.label = 'b';
		});
		expect((await queued(kv)).at(-1)?.op).toBe(CrudOp.PATCH);
		await c.close();
	});

	it('masks several changed fields at once', async () => {
		const kv = new MemoryKVStore();
		const c = newClient(kv);
		await c.db.widget.mutate.put({ id: 'w1', label: 'a', count: 1, big: 2n });

		await c.db.widget.mutate.update('w1', (w) => {
			w.label = 'b';
			w.count = 9;
			w.big = 7n;
		});

		expect(new Set(await lastMask(kv))).toEqual(new Set([ 'label', 'count', 'big' ]));
		await c.close();
	});

	it('diffs against the merged view, so successive updates compose', async () => {
		// The base is read through the optimistic overlay, not the confirmed row:
		// the second recipe sees the first one's edit and its own diff stays narrow.
		const kv = new MemoryKVStore();
		const c = newClient(kv);
		await c.db.widget.mutate.put({ id: 'w1', label: 'a', count: 1 });

		await c.db.widget.mutate.update('w1', (w) => {
			w.label = 'b';
		});
		await c.db.widget.mutate.update('w1', (w) => {
			expect(w.label).toBe('b'); // the pending edit is visible
			w.count = 2;
		});

		expect(await lastMask(kv)).toEqual([ 'count' ]);
		const w = await c.db.widget.get('w1');
		expect(w?.label).toBe('b');
		expect(w?.count).toBe(2);
		await c.close();
	});

	it('writes nothing when the recipe changes nothing', async () => {
		const kv = new MemoryKVStore();
		const c = newClient(kv);
		await c.db.widget.mutate.put({ id: 'w1', label: 'a' });
		const before = (await queued(kv)).length;

		await c.db.widget.mutate.update('w1', (w) => {
			w.label = 'a'; // same value
		});
		await c.db.widget.mutate.update('w1', () => {}); // touched nothing

		expect((await queued(kv)).length).toBe(before);
		await c.close();
	});

	it('narrows a changed collection to the touched items', async () => {
		// The recipe pushes one tag onto a list; the enqueue path expands the bare
		// collection path, so the mask names the added element rather than the
		// whole field — adding never whole-replaces.
		const kv = new MemoryKVStore();
		const c = newClient(kv);
		await c.db.widget.mutate.put({ id: 'w1', tags: [ 'a', 'b' ] });

		await c.db.widget.mutate.update('w1', (w) => {
			w.tags.push('c');
		});

		expect(await lastMask(kv)).toEqual([ 'tags.c' ]);
		expect((await c.db.widget.get('w1'))?.tags).toEqual([ 'a', 'b', 'c' ]);
		await c.close();
	});

	it('narrows a changed map to the touched key', async () => {
		const kv = new MemoryKVStore();
		const c = newClient(kv);
		await c.db.widget.mutate.put({ id: 'w1', attrs: { a: '1', b: '2' } });

		await c.db.widget.mutate.update('w1', (w) => {
			w.attrs.b = '9';
		});

		expect(await lastMask(kv)).toEqual([ 'attrs.b' ]);
		await c.close();
	});

	it('narrows a changed keyed message list to the touched item', async () => {
		const kv = new MemoryKVStore();
		const c = newClient(kv);
		await c.db.widget.mutate.put({
			id: 'w1',
			members: [ { uid: 'u1', name: 'one' }, { uid: 'u2', name: 'two' } ],
		});

		await c.db.widget.mutate.update('w1', (w) => {
			w.members[1]!.name = 'TWO';
		});

		expect(await lastMask(kv)).toEqual([ 'members.u2' ]);
		await c.close();
	});

	it('detects a change inside a submessage as a whole-field write', async () => {
		const kv = new MemoryKVStore();
		const c = newClient(kv);
		await c.db.widget.mutate.put({ id: 'w1', settings: { color: 'red', size: 2 } });

		await c.db.widget.mutate.update('w1', (w) => {
			w.settings!.color = 'blue';
		});

		expect(await lastMask(kv)).toEqual([ 'settings' ]);
		expect((await c.db.widget.get('w1'))?.settings?.size).toBe(2);
		await c.close();
	});

	it('sees presence changes on an explicit-presence field', async () => {
		const kv = new MemoryKVStore();
		const c = newClient(kv);
		await c.db.widget.mutate.put({ id: 'w1' });

		await c.db.widget.mutate.update('w1', (w) => {
			w.maybe = '';
		});

		// Set-to-empty is a presence change, not a no-op.
		expect(await lastMask(kv)).toEqual([ 'maybe' ]);
		await c.close();
	});

	it('does not mutate the stored row when the recipe throws', async () => {
		const kv = new MemoryKVStore();
		const c = newClient(kv);
		await c.db.widget.mutate.put({ id: 'w1', label: 'kept' });
		const before = (await queued(kv)).length;

		await expect(c.db.widget.mutate.update('w1', (w) => {
			w.label = 'lost';
			throw new Error('boom');
		})).rejects.toThrow('boom');

		expect((await c.db.widget.get('w1'))?.label).toBe('kept');
		expect((await queued(kv)).length).toBe(before);
		await c.close();
	});

	it('throws on a missing row rather than writing a partial one', async () => {
		const kv = new MemoryKVStore();
		const c = newClient(kv);
		await expect(c.db.widget.mutate.update('nope', (w) => {
			w.label = 'x';
		})).rejects.toThrow(/no row "nope"/);
		await c.close();
	});

	it('refuses a recipe that changes a key field', async () => {
		const kv = new MemoryKVStore();
		const c = newClient(kv);
		await c.db.widget.mutate.put({ id: 'w1', label: 'a' });
		await expect(c.db.widget.mutate.update('w1', (w) => {
			w.id = 'w2';
		})).rejects.toThrow(/key field "id"/);
		await c.close();
	});

	it('leaves the caller-visible row untouched by the draft', async () => {
		// The recipe edits a clone; a row the app already holds must not change
		// under it.
		const kv = new MemoryKVStore();
		const c = newClient(kv);
		await c.db.widget.mutate.put({ id: 'w1', label: 'a' });
		const held = await c.db.widget.get('w1');

		await c.db.widget.mutate.update('w1', (w) => {
			w.label = 'b';
		});

		expect(held?.label).toBe('a');
		await c.close();
	});

	it('carries expectedVersion through when asked', async () => {
		const kv = new MemoryKVStore();
		const c = newClient(kv);
		await c.db.widget.mutate.put({ id: 'w1', label: 'a' });
		await c.db.widget.mutate.update('w1', (w) => {
			w.label = 'b';
		}, { expectedVersion: 3n });
		expect((await queued(kv)).at(-1)?.expectedVersion).toBe(3n);
		await c.close();
	});

	it('is available on the schema-first API too', async () => {
		const kv = new MemoryKVStore();
		const c = newClient(kv);
		await c.mutate.put(TodoSchema, { id: 't1', title: 'a', completed: false });
		await c.mutate.update(TodoSchema, 't1', (t) => {
			t.completed = true;
		});
		expect(await lastMask(kv)).toEqual([ 'completed' ]);
		expect((await c.get(TodoSchema, 't1'))?.title).toBe('a');
		await c.close();
	});

	it('joins a transaction batch', async () => {
		const kv = new MemoryKVStore();
		const c = newClient(kv);
		await c.db.widget.mutate.put({ id: 'w1', label: 'a', count: 1 });
		const before = (await queued(kv)).length;

		await c.transact(async (tx) => {
			await tx.mutate.update(WidgetSchema, 'w1', (w) => {
				w.label = 'b';
			});
			await tx.mutate.put(WidgetSchema, { id: 'w2', label: 'new' });
		});

		// One batch mutation, not two crud ones.
		const tx2 = await kv.read();
		const pending = await readPending(tx2, WS);
		tx2.release();
		expect(pending.at(-1)?.payload.case).toBe('batch');
		expect((await queued(kv)).length).toBe(before);
		expect((await c.db.widget.get('w1'))?.label).toBe('b');
		expect((await c.db.widget.get('w1'))?.count).toBe(1);
		await c.close();
	});

	it('carries a text field written as a string through the text merge', async () => {
		const kv = new MemoryKVStore();
		const c = newClient(kv);
		await c.db.widget.mutate.put({ id: 'w1', body: 'hello' });

		await c.db.widget.mutate.update('w1', (w) => {
			// Text fields are bytes on the draft; a string is the same escape the
			// other write verbs accept, diffed into CRDT ops at enqueue time.
			(w as unknown as { body: string }).body = 'hello world';
		});

		const handle = await c.db.widget.text('w1', 'body');
		expect(handle.value).toBe('hello world');
		handle.close();
		await c.close();
	});
});

describe('mutate.patch with a whole message', () => {
	it('rejects a message instance when the mask would be derived from it', async () => {
		const kv = new MemoryKVStore();
		const c = newClient(kv);
		await c.db.widget.mutate.put({ id: 'w1', label: 'a' });
		const row = (await c.db.widget.get('w1'))!;
		row.label = 'b';

		await expect(
			// @ts-expect-error — a whole message is not a patch init (that is the point).
			c.db.widget.mutate.patch('w1', row),
		).rejects.toThrow(/is a whole .*Widget message, not the fields to change/);
		await c.close();
	});

	it('names update() and put() in the error so the fix is in the message', async () => {
		const kv = new MemoryKVStore();
		const c = newClient(kv);
		await c.db.widget.mutate.put({ id: 'w1', label: 'a' });
		const row = (await c.db.widget.get('w1'))!;

		await expect(
			// @ts-expect-error — see above.
			c.db.widget.mutate.patch('w1', row),
		).rejects.toThrow(/use update\(key, draft .*, or put\(\) to replace it/);
		await c.close();
	});

	it('accepts a message instance under an explicit mask', async () => {
		// Copying named fields off another row is safe — the mask stays narrow —
		// so the explicit-paths overload still takes one.
		const kv = new MemoryKVStore();
		const c = newClient(kv);
		await c.db.widget.mutate.put({ id: 'w1', label: 'one', count: 1 });
		await c.db.widget.mutate.put({ id: 'w2', label: 'two', count: 2 });
		const source = (await c.db.widget.get('w2'))!;

		await c.db.widget.mutate.patch('w1', source, { paths: [ 'label' ] });

		const w1 = await c.db.widget.get('w1');
		expect(w1?.label).toBe('two');
		expect(w1?.count).toBe(1); // not copied — the mask named only label
		await c.close();
	});

	it('still accepts a plain partial init', async () => {
		const kv = new MemoryKVStore();
		const c = newClient(kv);
		await c.db.widget.mutate.put({ id: 'w1', label: 'a', count: 1 });
		await c.db.widget.mutate.patch('w1', { label: 'b' });
		expect(await lastMask(kv)).toEqual([ 'label' ]);
		expect((await c.db.widget.get('w1'))?.count).toBe(1);
		await c.close();
	});
});
