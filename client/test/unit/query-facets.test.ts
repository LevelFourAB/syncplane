// Facets on a row query: per-value counts of the match set, folded from the
// rows the query already walks. The specs pin three things —
//
//   - a facet describes the MATCH SET, not the page: whatever offset, limit or
//     cursor cuts the rows, the counts sum to `total`, and the index arm
//     agrees with the scan arm that defines them;
//   - a read spanning workspaces keys its groups by workspace unless the axis
//     opts out, because an id means nothing outside the workspace holding it;
//   - a maintained result keeps its facets from the rows a commit moved, and
//     re-emits when only they changed.

import { create, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it, vi } from 'vitest';

import { SyncClient } from '../../src/client.js';
import type { RowSet } from '../../src/coverage.js';
import { RowChangeSchema } from '../../src/gen/syncplane/v1/patch_pb.js';
import { metaCursor } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { KVStore } from '../../src/kv/store.js';
import { EntityRegistry } from '../../src/mutations.js';
import { ChainMutex } from '../../src/mutex.js';
import { buildIndexes } from '../../src/optimistic/index-build.js';
import { IndexCache } from '../../src/optimistic/index-maintain.js';
import { applyServerChanges } from '../../src/optimistic/rebase.js';
import {
	compileQuery,
	type FacetGroup,
	type FacetTable,
	type Query,
} from '../../src/query/index.js';
import { prepareQuery } from '../../src/query/wire.js';
import {
	defineSchema,
	entity,
	type Infer,
	int32,
	list,
	message,
	text,
} from '../../src/schema/builder.js';
import type { QueryEvent, SyncEvent } from '../../src/sync/events.js';
import { TouchSet } from '../../src/touch.js';

const task = entity('task', {
	id: text().key(),
	title: text(),
	state: text().indexed(),
	area: text(),
	rank: int32(),
	labels: list(text()),
	meta: message({ due: int32() }),
}, {
	indexes: [ { name: 'by_state_rank', key: [ { path: 'state' }, { path: 'rank', desc: true } ] } ],
});

type Task = Infer<typeof task>;
const bundle = defineSchema(task);
const TaskSchema = bundle.descMessage('task');
const registry = new EntityRegistry([ TaskSchema ]);

const A = 'ws-a';
const B = 'ws-b';

const row = (
	id: string,
	state: string,
	area: string,
	rank: number,
	labels: string[] = [],
): Task => ({ id, title: id, state, area, rank, labels, meta: { due: 0 } });

const SEED: Record<string, Task[]> = {
	[A]: [
		row('a1', 'open', 'home', 9, [ 'ui' ]),
		row('a2', 'open', 'work', 7, [ 'ui', 'bug' ]),
		row('a3', 'open', 'home', 5),
		row('a4', 'done', 'home', 3, [ 'bug' ]),
		row('a5', 'open', '', 1),
	],
	[B]: [
		row('b1', 'open', 'home', 8, [ 'perf' ]),
		row('b2', 'done', 'work', 2),
	],
};

async function seedConfirmed(kv: KVStore, indexes: IndexCache): Promise<void> {
	for (const [ workspaceId, rows ] of Object.entries(SEED)) {
		const tx = await kv.write();
		await tx.put(metaCursor(workspaceId), new Uint8Array([ 1 ]));
		await applyServerChanges(
			tx,
			registry,
			workspaceId,
			rows.map((r, i) =>
				create(RowChangeSchema, {
					entity: 'task',
					key: r.id,
					version: BigInt(i + 1),
					body: { case: 'value', value: toBinary(TaskSchema, create(TaskSchema, r)) },
				})
			),
			new TouchSet(),
			indexes,
		);
		await tx.commit();
	}
}

/** A two-workspace client over the seed; `built` is the only difference. */
async function clientWith(built: boolean, onEvent?: (event: SyncEvent) => void) {
	const kv = new MemoryKVStore();
	const indexes = new IndexCache(registry);
	await seedConfirmed(kv, indexes);
	if (built) {
		const mutex = new ChainMutex();
		for (const workspaceId of Object.keys(SEED)) {
			await buildIndexes(kv, (fn) => mutex.run(fn), registry, indexes, workspaceId);
		}
	}
	return new SyncClient({
		accountId: 'acct',
		workspaces: [ A, B ],
		defaultWorkspace: A,
		kvStore: kv,
		entities: [ TaskSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
		debugVerifyTouches: true,
		onSyncEvent: onEvent,
	}) as unknown as SyncClient<never>;
}

type Facetted = RowSet<Task, number | undefined, FacetTable>;

const read = (client: SyncClient<never>, q: Query<Task>): Promise<Facetted> =>
	client.query(TaskSchema, q as never) as unknown as Promise<Facetted>;

const readAll = (
	client: SyncClient<never>,
	q: Query<Task>,
	workspaces?: readonly string[],
): Promise<Facetted> =>
	client.queryAll(TaskSchema, q as never, { workspaces }) as unknown as Promise<Facetted>;

const put = (client: SyncClient<never>, workspaceId: string, r: Task): Promise<void> =>
	client.ws(workspaceId).mutate.put(TaskSchema, r as never);

/** Runs a read through both arms, asserting they agree on rows and facets. */
async function both(
	fn: (client: SyncClient<never>) => Promise<Facetted>,
): Promise<Facetted> {
	const scanned = await clientWith(false);
	const indexed = await clientWith(true);
	try {
		const s = await fn(scanned);
		const i = await fn(indexed);
		expect(i.map((r) => `${r.workspaceId}/${r.key}`)).toEqual(
			s.map((r) => `${r.workspaceId}/${r.key}`),
		);
		expect(i.total).toEqual(s.total);
		expect(i.facets).toEqual(s.facets);
		return s;
	} finally {
		await scanned.close();
		await indexed.close();
	}
}

const g = (value: string | undefined, count: number, workspaceId?: string): FacetGroup =>
	workspaceId === undefined ? { value, count } : { value, count, workspaceId };

describe('facets on a one-shot query', () => {
	it('counts the match set by each axis, keyed under the names the query gave', async () => {
		const rows = await both((c) =>
			read(c, {
				where: (t) => t.state.eq('open'),
				facets: { area: (t) => t.area, label: (t) => t.labels },
			})
		);
		expect(rows.total).toBe(4);
		expect(rows.facets).toEqual({
			area: [ g('', 1, A), g('home', 2, A), g('work', 1, A) ],
			// Per element on a repeated path, and the rows holding none as a
			// group of their own, last.
			label: [ g('bug', 1, A), g('ui', 2, A), g(undefined, 2, A) ],
		});
	});

	it('describes the rows a page was cut from, not the page', async () => {
		const q: Query<Task> = {
			where: (t) => t.state.eq('open'),
			orderBy: (t) => [ t.rank.desc() ],
			facets: { area: (t) => t.area },
		};
		const whole = await both((c) => read(c, q));
		const paged = await both((c) => read(c, { ...q, limit: 1, offset: 1 }));
		expect(paged.map((r) => r.key)).toEqual([ 'a2' ]);
		expect(paged.total).toBe(4);
		expect(paged.facets).toEqual(whole.facets);
		expect(paged.facets.area!.reduce((n, x) => n + x.count, 0)).toBe(paged.total);

		// A cursor bounds the match set itself, so the counts move with it.
		const bounded = await both((c) => read(c, { ...q, after: whole.startCursor }));
		expect(bounded.map((r) => r.key)).toEqual([ 'a2', 'a3', 'a5' ]);
		expect(bounded.facets).toEqual({ area: [ g('', 1, A), g('home', 1, A), g('work', 1, A) ] });
	});

	it('is reported even where the total is opted out', async () => {
		const rows = await both((c) =>
			read(c, { where: (t) => t.state.eq('open'), total: false, facets: { area: (t) => t.area } })
		);
		expect(rows.total).toBeUndefined();
		expect(rows.facets.area).toHaveLength(3);
	});

	it('is undefined for a query naming none, and empty for one naming an empty set', async () => {
		const none = await both((c) => read(c, { where: (t) => t.state.eq('open') }));
		expect(none.facets).toBeUndefined();
		const empty = await both((c) => read(c, { where: (t) => t.state.eq('open'), facets: {} }));
		expect(empty.facets).toEqual({});
	});

	it('rejects an axis a grouped read would reject', () => {
		expect(() =>
			prepareQuery(
				TaskSchema,
				compileQuery<Task>({ facets: { meta: (t) => t.meta as never } }),
			)
		).toThrow(/meta/);
	});

	it('reads every matching row on the index arm rather than the page', async () => {
		const events: SyncEvent[] = [];
		const client = await clientWith(true, (e) => events.push(e));
		try {
			await read(client, {
				where: (t) => t.state.eq('open'),
				orderBy: (t) => [ t.rank.desc() ],
				limit: 1,
			});
			await read(client, {
				where: (t) => t.state.eq('open'),
				orderBy: (t) => [ t.rank.desc() ],
				limit: 1,
				facets: { area: (t) => t.area },
			});
			const reads = events.filter((e): e is QueryEvent => e.type === 'query');
			expect(reads).toHaveLength(2);
			expect(reads[0]!.index).toBeDefined();
			expect(reads[1]!.index).toBe(reads[0]!.index);
			// The page alone settles after its rows; the facet walks the range.
			expect(reads[1]!.candidates!).toBeGreaterThan(reads[0]!.candidates!);
		} finally {
			await client.close();
		}
	});
});

describe('facets across workspaces', () => {
	const open: Query<Task> = {
		where: (t) => t.state.eq('open'),
		facets: {
			area: (t) => t.area,
			state: { by: (t) => t.state, perWorkspace: false },
		},
	};

	it('keys a group by workspace and value by default, and by value alone on request', async () => {
		const rows = await both((c) => readAll(c, open));
		expect(rows.total).toBe(5);
		expect(rows.facets).toEqual({
			// Ascending value, ties between workspaces broken by workspace id.
			area: [ g('', 1, A), g('home', 2, A), g('home', 1, B), g('work', 1, A) ],
			state: [ g('open', 5) ],
		});
	});

	it('counts only the workspaces the read names', async () => {
		const rows = await both((c) => readAll(c, open, [ B ]));
		expect(rows.facets).toEqual({ area: [ g('home', 1, B) ], state: [ g('open', 1) ] });
		const nothing = await both((c) => readAll(c, open, []));
		expect(nothing.facets).toEqual({ area: [], state: [] });
	});
});

describe('facets on a maintained result', () => {
	async function watching(
		client: SyncClient<never>,
		q: Query<Task>,
		workspaces?: readonly string[],
	) {
		const seen: Facetted[] = [];
		const handle = client.watchQueryAll(
			TaskSchema,
			q as never,
			(rows) => seen.push(rows as unknown as Facetted),
			{ workspaces, window: { limit: 1 } },
		);
		await vi.waitFor(() => expect(seen.length).toBeGreaterThan(0));
		return { seen, close: () => handle.close() };
	}

	it('follows the rows a commit moved, on both arms', async () => {
		for (const built of [ false, true ]) {
			const client = await clientWith(built);
			const q: Query<Task> = {
				where: (t) => t.state.eq('open'),
				orderBy: (t) => [ t.rank.desc() ],
				facets: { area: (t) => t.area, label: (t) => t.labels },
			};
			const w = await watching(client, q, [ A ]);
			try {
				expect(w.seen.at(-1)!.facets).toEqual((await readAll(client, q, [ A ])).facets);

				// A row entering the match set, one leaving it, and one changing
				// its value in place — each past the one-row page.
				await put(client, A, row('a6', 'open', 'work', 0, [ 'bug' ]));
				await put(client, A, row('a3', 'done', 'home', 5));
				await put(client, A, row('a5', 'open', 'work', 1));
				await vi.waitFor(() =>
					expect(w.seen.at(-1)!.facets).toEqual({
						area: [ g('home', 1, A), g('work', 3, A) ],
						label: [ g('bug', 2, A), g('ui', 2, A), g(undefined, 1, A) ],
					})
				);
				expect(w.seen.at(-1)!.facets).toEqual((await readAll(client, q, [ A ])).facets);
				expect(w.seen.at(-1)!.map((r) => r.key)).toEqual([ 'a1' ]);
			} finally {
				w.close();
				await client.close();
			}
		}
	});

	it('re-emits for a change only the facets can see', async () => {
		const client = await clientWith(true);
		const q: Query<Task> = {
			where: (t) => t.state.eq('open'),
			orderBy: (t) => [ t.rank.desc() ],
			facets: { area: (t) => t.area },
		};
		const w = await watching(client, q, [ A ]);
		try {
			const before = w.seen.length;
			// Same rows, same total, same page: only a value on the axis moved.
			await put(client, A, row('a3', 'open', 'work', 5));
			await vi.waitFor(() => expect(w.seen.length).toBe(before + 1));
			expect(w.seen.at(-1)!.facets).toEqual({
				area: [ g('', 1, A), g('home', 1, A), g('work', 2, A) ],
			});
			// And a commit that moves nothing on any axis stays silent.
			await put(client, A, row('a3', 'open', 'work', 5, [ 'x' ]));
			await new Promise((r) => setTimeout(r, 10));
			expect(w.seen.length).toBe(before + 1);
		} finally {
			w.close();
			await client.close();
		}
	});
});
