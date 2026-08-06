// Keyset paging on the client: the cursor codec, the bounded match set, and
// the index path agreeing with the scan.
//
// The cross-language half of the contract — a cursor minted here accepted by
// the Go server and the other way round — lives in test/integration; nothing
// in this file can prove it, because both sides of every assertion here are
// the same implementation.

import { describe, expect, it, vi } from 'vitest';

import { SyncClient } from '../../src/client.js';
import type { RowSet } from '../../src/coverage.js';
import { type Widget, WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import {
	CursorShapeError,
	OrderCursors,
	type Query,
	type QueryCursor,
} from '../../src/query/index.js';

function newClient() {
	return new SyncClient({
		workspaceId: 'ws1',
		kvStore: new MemoryKVStore(),
		entities: [ WidgetSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
	});
}

type WidgetQuery = Query<Widget>;

/** Seven widgets with distinct counts, so the ordering is total before the
 * row-key tiebreak ever runs. */
async function seeded(extra: Partial<Widget>[] = []) {
	const client = newClient();
	for (let i = 1; i <= 7; i++) {
		await client.db.widget.mutate.put({ id: `w${i}`, count: i });
	}
	for (const row of extra) await client.db.widget.mutate.put(row as never);
	return client;
}

const byCount: WidgetQuery = { orderBy: (w) => [ w.count.asc() ] };

const keysOf = (rows: RowSet<Widget>) => rows.map((r) => r.key);

describe('query cursors', () => {
	it('round-trips a position through the wire form', async () => {
		const cursors = new OrderCursors(WidgetSchema, [ { path: [ 'count' ], dir: 'asc' } ]);
		const row = { id: 'w1', count: 3, label: 'x' };
		const token = cursors.encode('widget', row, 'w1');
		expect(token).toMatch(/^[A-Za-z0-9_-]+$/);
		expect(cursors.decode('widget', token)).toEqual(cursors.entryOf(row, 'w1'));
	});

	it('rejects a cursor minted under a different ordering or entity', async () => {
		const asc = new OrderCursors(WidgetSchema, [ { path: [ 'count' ], dir: 'asc' } ]);
		const desc = new OrderCursors(WidgetSchema, [ { path: [ 'count' ], dir: 'desc' } ]);
		const token = asc.encode('widget', { id: 'w1', count: 3 }, 'w1');
		expect(() => desc.decode('widget', token)).toThrow(CursorShapeError);
		expect(() => asc.decode('gadget', token)).toThrow(CursorShapeError);
	});

	it('rejects bytes that are not a cursor', async () => {
		const cursors = new OrderCursors(WidgetSchema, [ { path: [ 'count' ], dir: 'asc' } ]);
		expect(() => cursors.decode('widget', 'AAAA' as QueryCursor)).toThrow(/too short/);
		expect(() => cursors.decode('widget', 'not base64url!' as QueryCursor)).toThrow(/base64url/);
	});

	it('orders by a collaborative-text key, which an index may not carry', async () => {
		// The index rejection is a policy about write cost; a cursor is computed
		// and discarded, so the same component is legal here.
		const cursors = new OrderCursors(WidgetSchema, [ { path: [ 'body' ], dir: 'asc' } ]);
		const plain = new OrderCursors(WidgetSchema, [ { path: [ 'label' ], dir: 'asc' } ]);
		expect(cursors.entryOf({ id: 'w', body: 'hello' }, 'w'))
			.toEqual(plain.entryOf({ id: 'w', label: 'hello' }, 'w'));
	});

	it('pages forward to the same rows the offset path returns', async () => {
		const client = await seeded();
		try {
			const byOffset: string[] = [];
			for (let off = 0; off < 7; off += 2) {
				byOffset.push(
					...keysOf(await client.query(WidgetSchema, { ...byCount, offset: off, limit: 2 })),
				);
			}

			const byCursor: string[] = [];
			let after: QueryCursor | undefined;
			for (;;) {
				const page = await client.query(WidgetSchema, { ...byCount, limit: 2, after });
				byCursor.push(...keysOf(page));
				if (!page.hasMore) break;
				after = page.endCursor;
			}
			expect(byCursor).toEqual(byOffset);
			expect(byCursor).toEqual([ 'w1', 'w2', 'w3', 'w4', 'w5', 'w6', 'w7' ]);
		} finally {
			await client.close();
		}
	});

	it('takes the limit from the before end, so before alone is the previous page', async () => {
		const client = await seeded();
		try {
			const last = await client.query(WidgetSchema, { ...byCount, offset: 5, limit: 2 });
			expect(keysOf(last)).toEqual([ 'w6', 'w7' ]);

			const prev = await client.query(WidgetSchema, {
				...byCount,
				limit: 2,
				before: last.startCursor,
			});
			expect(keysOf(prev)).toEqual([ 'w4', 'w5' ]);
			expect(prev.hasMore).toBe(true);

			const first = await client.query(WidgetSchema, {
				...byCount,
				limit: 4,
				before: prev.startCursor,
			});
			expect(keysOf(first)).toEqual([ 'w1', 'w2', 'w3' ]);
			expect(first.hasMore).toBe(false);
		} finally {
			await client.close();
		}
	});

	it('holds the next page steady when a row is inserted ahead of the cursor', async () => {
		const client = await seeded();
		try {
			const first = await client.query(WidgetSchema, { ...byCount, limit: 3 });
			expect(keysOf(first)).toEqual([ 'w1', 'w2', 'w3' ]);

			await client.db.widget.mutate.put({ id: 'w0', count: -1 });

			expect(keysOf(
				await client.query(WidgetSchema, {
					...byCount,
					limit: 3,
					after: first.endCursor,
				}),
			)).toEqual([ 'w4', 'w5', 'w6' ]);

			// The same continuation by rank re-serves w3: the insert pushed every
			// row one place along.
			expect(keysOf(await client.query(WidgetSchema, { ...byCount, limit: 3, offset: 3 })))
				.toEqual([ 'w3', 'w4', 'w5' ]);
		} finally {
			await client.close();
		}
	});

	it('bounds total to the range, and reads it when both cursors are set', async () => {
		const client = await seeded();
		try {
			const head = await client.query(WidgetSchema, { ...byCount, limit: 2 });
			expect(head.total).toBe(7);
			const tail = await client.query(WidgetSchema, { ...byCount, offset: 5, limit: 1 });

			const between = await client.query(WidgetSchema, {
				...byCount,
				after: head.endCursor,
				before: tail.startCursor,
			});
			// Both bounds are exclusive, so neither w2 nor w6 is in the range —
			// and `total` counts what is inside it, not the whole entity.
			expect(keysOf(between)).toEqual([ 'w3', 'w4', 'w5' ]);
			expect(between.total).toBe(3);
			expect(between.hasMore).toBe(false);
		} finally {
			await client.close();
		}
	});

	it('leaves the cursors undefined for an empty page', async () => {
		const client = await seeded();
		try {
			const empty = await client.query(WidgetSchema, {
				...byCount,
				where: (w) => w.count.gt(1000),
			});
			expect(empty).toHaveLength(0);
			expect(empty.startCursor).toBeUndefined();
			expect(empty.endCursor).toBeUndefined();
			expect(empty.hasMore).toBe(false);
		} finally {
			await client.close();
		}
	});

	it('refuses an offset alongside a cursor', async () => {
		const client = await seeded();
		try {
			const first = await client.query(WidgetSchema, { ...byCount, limit: 1 });
			await expect(
				client.query(WidgetSchema, { ...byCount, offset: 1, limit: 1, after: first.endCursor }),
			).rejects.toThrow(/cannot be combined/);
		} finally {
			await client.close();
		}
	});

	it('refuses a cursor on a cross-workspace query, which has no single ordering', async () => {
		const client = await seeded();
		try {
			const first = await client.query(WidgetSchema, { ...byCount, limit: 1 });
			await expect(
				client.queryAll(WidgetSchema, { ...byCount, limit: 1, after: first.endCursor }),
			).rejects.toThrow(/cross-workspace/);
		} finally {
			await client.close();
		}
	});

	it('keeps the cursors non-enumerable, like coverage and total', async () => {
		const client = await seeded();
		try {
			const page = await client.query(WidgetSchema, { ...byCount, limit: 2 });
			expect(page.endCursor).toBeDefined();
			// Enumerating a RowSet must still see an array and nothing else, or
			// spreads, structural equality and serialization would all change.
			expect(Object.keys(page)).toEqual([ '0', '1' ]);
		} finally {
			await client.close();
		}
	});

	it('bounds a live query before its window slices it', async () => {
		const client = await seeded();
		try {
			const head = await client.query(WidgetSchema, { ...byCount, limit: 2 });
			const seen: RowSet<Widget>[] = [];
			const handle = client.watchQuery(
				WidgetSchema,
				{ ...byCount, after: head.endCursor, limit: 2 },
				(rows) => void seen.push(rows as RowSet<Widget>),
			);
			await vi.waitFor(() => expect(seen).not.toHaveLength(0));
			handle.close();
			const last = seen[seen.length - 1]!;
			expect(keysOf(last)).toEqual([ 'w3', 'w4' ]);
			// The bound cut the match set down to w3..w7 before the window took
			// its two rows, so `total` is 5 rather than 7.
			expect(last.total).toBe(5);
			expect(last.hasMore).toBe(true);
		} finally {
			await client.close();
		}
	});
});
