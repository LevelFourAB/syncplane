// A local write already holds the row it produced, so the watcher hub takes it
// from the commit instead of reading it back: a watched patch, put or delete
// opens no read transaction at all (see WatcherHub.notifyWithRows). These specs
// pin both halves —
//
//   - the read is gone wherever the writer can state the whole post-commit row,
//     and the answer is still the one the read would have given;
//   - every shape that CANNOT be served that way — a text log folded back into
//     the row, a query crossing references, an `include` to hydrate, a custom
//     mutation's mirror — falls back to the read path and still delivers.
//
// The handed-over rows are compared against a full scan after every commit by
// the `debugVerifyTouches` hub, which is what proves the two paths agree row by
// row rather than only on the keys asserted here.

import type { DescMessage, Message } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import {
	IssueSchema,
	PairSchema,
	ProjectSchema,
	UserSchema,
	type Widget,
	WidgetSchema,
} from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { KVRead, KVStore, KVWrite } from '../../src/kv/store.js';
import type { MirrorTx, MutatorMirror } from '../../src/optimistic/rebase.js';
import type { Row } from '../../src/read.js';

const flush = (): Promise<unknown> => new Promise((r) => setTimeout(r, 0));

/** A store counting the read transactions opened against it. */
class CountingStore implements KVStore {
	reads = 0;

	constructor(private inner: KVStore = new MemoryKVStore()) {}

	read(): Promise<KVRead> {
		this.reads++;
		return this.inner.read();
	}

	write(): Promise<KVWrite> {
		return this.inner.write();
	}

	close(): Promise<void> {
		return this.inner.close();
	}
}

/**
 * A client with the divergence check OFF, for the specs that count reads: the
 * check re-scans the entity after every commit by design, which is exactly the
 * read they measure. The specs that assert answers leave it on.
 */
function countingClient(store: CountingStore): SyncClient<[typeof WidgetSchema]> {
	return new SyncClient({
		workspaceId: 'ws1',
		kvStore: store,
		entities: [ WidgetSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
		debugVerifyTouches: false,
	});
}

function verifyingClient(
	opts?: { mutators?: Record<string, MutatorMirror<DescMessage>> },
): SyncClient<[typeof WidgetSchema]> {
	return new SyncClient({
		workspaceId: 'ws1',
		kvStore: new MemoryKVStore(),
		entities: [ WidgetSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
		debugVerifyTouches: true,
		...opts,
	});
}

/** Issue/User/Project, for the reference shapes Widget has no counterpart for. */
function refClient(): SyncClient<
	[typeof IssueSchema, typeof ProjectSchema, typeof UserSchema, typeof PairSchema]
> {
	return new SyncClient({
		workspaceId: 'ws1',
		kvStore: new MemoryKVStore(),
		entities: [ IssueSchema, ProjectSchema, UserSchema, PairSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
		debugVerifyTouches: true,
	});
}

const keysOf = (rows: readonly Row<Widget>[]): string[] => rows.map((r) => r.key);
const countsOf = (rows: readonly Row<Widget>[]): string[] =>
	rows.map((r) => `${r.key}:${r.value.count}`);

describe('watched writes served from the commit', () => {
	it('opens no read transaction for a patch, a put or a delete', async () => {
		// Widget declares a collaborative-text field, which is what makes its put
		// read the row up front and so know the confirmed version the merged view
		// carries; the spec below pins what happens when nothing does.
		const store = new CountingStore();
		const client = countingClient(store);
		await client.db.widget.mutate.put({ id: 'w1', label: 'a', count: 1 });
		const seen: string[][] = [];
		const handle = client.db.widget.watchQuery(
			{ orderBy: (w) => [ w.count.asc() ] },
			(rows) => seen.push(countsOf(rows as Row<Widget>[])),
		);
		await flush();
		expect(seen.at(-1)).toEqual([ 'w1:1' ]);

		store.reads = 0;
		await client.db.widget.mutate.patch('w1', { count: 5 });
		await flush();
		expect(seen.at(-1)).toEqual([ 'w1:5' ]);
		expect(store.reads).toBe(0);

		await client.db.widget.mutate.put({ id: 'w2', label: 'b', count: 2 });
		await flush();
		expect(seen.at(-1)).toEqual([ 'w2:2', 'w1:5' ]);
		expect(store.reads).toBe(0);

		await client.db.widget.mutate.del('w1');
		await flush();
		expect(seen.at(-1)).toEqual([ 'w2:2' ]);
		expect(store.reads).toBe(0);

		handle.close();
		await client.close();
	});

	it('gives a keyed watch the same rows without a read', async () => {
		const store = new CountingStore();
		const client = countingClient(store);
		await client.db.widget.mutate.put({ id: 'w1', label: 'a', count: 1 });
		const seen: (string | undefined)[] = [];
		const handle = client.watchRows(
			WidgetSchema,
			[ 'w1' ],
			(rows) => seen.push(rows.get('w1')?.value.label),
		);
		await flush();
		expect(seen.at(-1)).toBe('a');

		store.reads = 0;
		await client.db.widget.mutate.patch('w1', { label: 'b' });
		await flush();
		expect(seen.at(-1)).toBe('b');
		expect(store.reads).toBe(0);

		await client.db.widget.mutate.del('w1');
		await flush();
		expect(seen.at(-1)).toBeUndefined();
		expect(store.reads).toBe(0);

		handle.close();
		await client.close();
	});

	it('keeps the page a read would have produced through every transition', async () => {
		const client = verifyingClient();
		const seen: string[][] = [];
		const handle = client.db.widget.watchQuery(
			{ where: (w) => w.count.gt(0), orderBy: (w) => [ w.count.asc() ] },
			(rows) => seen.push(countsOf(rows as Row<Widget>[])),
		);
		await flush();

		for (const [ id, count ] of [ [ 'c', 30 ], [ 'a', 10 ], [ 'b', 20 ] ] as const) {
			await client.db.widget.mutate.put({ id, label: id, count });
		}
		await flush();
		expect(seen.at(-1)).toEqual([ 'a:10', 'b:20', 'c:30' ]);

		// Move: same row, new order key.
		await client.db.widget.mutate.patch('a', { count: 25 });
		await flush();
		expect(seen.at(-1)).toEqual([ 'b:20', 'a:25', 'c:30' ]);

		// Leave by failing the predicate rather than by being deleted.
		await client.db.widget.mutate.patch('b', { count: 0 });
		await flush();
		expect(seen.at(-1)).toEqual([ 'a:25', 'c:30' ]);

		// Re-enter a result that has no history of the row.
		await client.db.widget.mutate.patch('b', { count: 5 });
		await flush();
		expect(seen.at(-1)).toEqual([ 'b:5', 'a:25', 'c:30' ]);

		await client.db.widget.mutate.del('a');
		await flush();
		expect(seen.at(-1)).toEqual([ 'b:5', 'c:30' ]);

		handle.close();
		await client.close();
	});

	it('splices a row out of and back into a windowed, ordered page', async () => {
		// Recency ordering with a short page: the rows the window admits change
		// without the commit ever naming them, which is what a keyed delta has to
		// get right when nothing re-reads the scope.
		const client = verifyingClient();
		for (const [ id, count ] of [ [ 'a', 10 ], [ 'b', 20 ], [ 'c', 30 ], [ 'd', 40 ] ] as const) {
			await client.db.widget.mutate.put({ id, label: id, count });
		}
		const seen: string[][] = [];
		const handle = client.db.widget.watchQuery(
			{ orderBy: (w) => [ w.count.desc() ], limit: 2 },
			(rows) => seen.push(countsOf(rows as Row<Widget>[])),
		);
		await flush();
		expect(seen.at(-1)).toEqual([ 'd:40', 'c:30' ]);

		// The page's own row falls out of it: the next one has to take its place.
		await client.db.widget.mutate.patch('d', { count: 5 });
		await flush();
		expect(seen.at(-1)).toEqual([ 'c:30', 'b:20' ]);

		// A row from behind the page jumps to the front of it.
		await client.db.widget.mutate.patch('a', { count: 99 });
		await flush();
		expect(seen.at(-1)).toEqual([ 'a:99', 'c:30' ]);

		// And the row that left comes back to the top.
		await client.db.widget.mutate.patch('d', { count: 100 });
		await flush();
		expect(seen.at(-1)).toEqual([ 'd:100', 'a:99' ]);

		handle.close();
		await client.close();
	});

	it('delivers two back-to-back writes in order, ending on the later one', async () => {
		// Both commits update the hub inside their own write section, so the
		// sections order the deltas; the callbacks race behind them and the read
		// stamp is what stops the earlier view from landing last.
		const client = verifyingClient();
		await client.db.widget.mutate.put({ id: 'w1', label: 'a', count: 0 });
		const seen: string[][] = [];
		const handle = client.db.widget.watchQuery(
			{ orderBy: (w) => [ w.count.asc() ] },
			(rows) => seen.push(countsOf(rows as Row<Widget>[])),
		);
		await flush();
		seen.length = 0;

		await Promise.all([
			client.db.widget.mutate.patch('w1', { count: 1 }),
			client.db.widget.mutate.patch('w1', { count: 2 }),
		]);
		await flush();
		// Whatever was delivered on the way, the final state is delivered once and
		// nothing after it goes back to the earlier view.
		const settled = seen.findIndex((page) => page[0] === 'w1:2');
		expect(settled).toBeGreaterThanOrEqual(0);
		expect(seen.slice(settled)).toEqual([ [ 'w1:2' ] ]);

		handle.close();
		await client.close();
	});
});

describe('watched writes that still have to read', () => {
	it('re-reads a put whose confirmed version the write never had to look at', async () => {
		// A merged view carries the confirmed base version, and the up-front read
		// only covers a row something in the write already wants — an undo
		// capture, a PATCH, a text field. A plain put of a text-free entity with
		// undo off is none of those, so it reports the key alone.
		const store = new CountingStore();
		const client = new SyncClient({
			workspaceId: 'ws1',
			kvStore: store,
			entities: [ ProjectSchema ],
			streamEnabled: false,
			pollIntervalMs: 0,
			debugVerifyTouches: false,
		});
		const seen: string[][] = [];
		const handle = client.db.project.watchQuery({}, (rows) => seen.push(rows.map((r) => r.key)));
		await flush();

		store.reads = 0;
		await client.db.project.mutate.put({ id: 'p1', name: 'A' });
		await flush();
		expect(seen.at(-1)).toEqual([ 'p1' ]);
		expect(store.reads).toBeGreaterThan(0);

		handle.close();
		await client.close();
	});

	it('re-reads a row whose text log the commit appended to', async () => {
		// A keystroke writes a fragment, not a row (see appendTextLog): what a
		// reader sees is the fragment folded back in, which the writer never
		// built, so no view is attached and the hub reads.
		const store = new CountingStore();
		const client = countingClient(store);
		await client.db.widget.mutate.put({ id: 'w1', label: 'a', body: 'hello' });
		const seen: string[][] = [];
		const handle = client.db.widget.watchQuery(
			{},
			(rows) => seen.push((rows as Row<Widget>[]).map((r) => r.value.body.length.toString())),
		);
		await flush();
		const before = seen.at(-1)!;

		store.reads = 0;
		const text = await client.db.widget.text('w1', 'body');
		store.reads = 0;
		await text.insert(5, ' world');
		await flush();
		expect(store.reads).toBeGreaterThan(0);
		// The folded document reached the watcher, which is the answer the
		// handed-over overlay bytes alone could not have given.
		expect(seen.at(-1)).not.toEqual(before);
		const row = await client.db.widget.text('w1', 'body');
		expect(row.value).toBe('hello world');
		row.close();
		text.close();
		handle.close();
		await client.close();
	});

	it('still delivers to a watcher hydrating an include', async () => {
		const client = refClient();
		await client.db.project.mutate.put({ id: 'p1', name: 'A' });
		await client.db.issue.mutate.put({ id: 'i1', title: 't', project: { id: 'p1' } });
		const names: (string | undefined)[] = [];
		const handle = client.db.issue.watchQuery(
			{},
			(rows) => {
				const r = rows[0] as
					| (Row<Message> & { refs: Record<string, Row<{ name: string }> | undefined> })
					| undefined;
				names.push(r?.refs.project?.value.name);
			},
			{ include: { project: true } },
		);
		await flush();
		expect(names.at(-1)).toBe('A');

		// The target row moves without the base row moving: only a hydration
		// read can see it.
		await client.db.project.mutate.patch('p1', { name: 'B' });
		await flush();
		expect(names.at(-1)).toBe('B');

		handle.close();
		await client.close();
	});

	it('still delivers to a watcher whose query crosses a reference', async () => {
		const client = refClient();
		await client.db.user.mutate.put({ id: 'u1', name: 'Maya' });
		await client.db.user.mutate.put({ id: 'u2', name: 'Noor' });
		await client.db.issue.mutate.put({ id: 'i1', state: 'open', assignee: { id: 'u1' } });
		await client.db.issue.mutate.put({ id: 'i2', state: 'open', assignee: { id: 'u2' } });
		const seen: string[][] = [];
		const handle = client.db.issue.watchQuery(
			{ where: (i) => i.assignee.matches((a) => a.name.eq('Maya')) },
			(rows) => seen.push(rows.map((r) => r.key)),
		);
		await flush();
		expect(seen.at(-1)).toEqual([ 'i1' ]);

		// A commit to the TARGET entity moves the membership; the pre-pass that
		// notices it reads, so this cannot be served from the commit.
		await client.db.user.mutate.patch('u2', { name: 'Maya' });
		await flush();
		expect(seen.at(-1)).toEqual([ 'i1', 'i2' ]);

		// A base-row commit on the same watcher still has to read too.
		await client.db.issue.mutate.put({ id: 'i3', state: 'open', assignee: { id: 'u1' } });
		await flush();
		expect(seen.at(-1)).toEqual([ 'i1', 'i2', 'i3' ]);

		handle.close();
		await client.close();
	});

	it('still delivers what a custom mutation mirror wrote', async () => {
		// A mirror writes through MirrorTx, which reports the rows it touched
		// without saying what they became — the hub reads them back.
		const client = verifyingClient({
			mutators: {
				relabel: {
					args: ProjectSchema,
					async apply(tx: MirrorTx, args) {
						const held = await tx.get(WidgetSchema, args!.id);
						if (held) await tx.put(WidgetSchema, { ...held, label: args!.name });
					},
				} satisfies MutatorMirror<typeof ProjectSchema> as MutatorMirror<DescMessage>,
			},
		});
		await client.db.widget.mutate.put({ id: 'w1', label: 'a', count: 1 });
		const seen: (string | undefined)[] = [];
		const handle = client.db.widget.watchQuery(
			{},
			(rows) => seen.push((rows as Row<Widget>[])[0]?.value.label),
		);
		await flush();
		expect(seen.at(-1)).toBe('a');

		await client.mutate.custom('relabel', ProjectSchema, { id: 'w1', name: 'renamed' });
		await flush();
		expect(seen.at(-1)).toBe('renamed');
		expect(keysOf((await client.db.widget.query({})) as Row<Widget>[])).toEqual([ 'w1' ]);

		handle.close();
		await client.close();
	});
});
