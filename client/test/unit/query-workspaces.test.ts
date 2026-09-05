// Cross-workspace reads bounded to a NAMED set of workspaces, and the
// per-workspace predicate built on top of it.
//
// Two things are being pinned. The first is that naming a set changes cost and
// exactness but never the answer: a set covering every workspace must return
// exactly what the unbounded read returns, and a set of one must return what
// the workspace-scoped read returns. The second is the tie-break — a merged
// result orders by the order keys, then the ROW KEY, then the workspace — which
// is the reason a cross-workspace index read was declined before this and the
// thing a per-workspace index walk has to reproduce.
//
// Every case therefore runs twice over identical data, once with the indexes
// built and once without, and compares the arms. The scan is the definition;
// the per-workspace index legs are an optimization over it.

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
import type { Query, WorkspaceQuery } from '../../src/query/index.js';
import type { Row } from '../../src/read.js';
import { defineSchema, entity, type Infer, int32, text } from '../../src/schema/builder.js';
import type { QueryEvent, SyncEvent } from '../../src/sync/events.js';
import { TouchSet } from '../../src/touch.js';

const issue = entity('issue', {
	id: text().key(),
	title: text(),
	state: text().indexed(),
	rank: int32(),
}, {
	indexes: [ { name: 'by_state_rank', key: [ { path: 'state' }, { path: 'rank', desc: true } ] } ],
});

type Issue = Infer<typeof issue>;
const bundle = defineSchema(issue);
const IssueSchema = bundle.descMessage('issue');
const registry = new EntityRegistry([ IssueSchema ]);

const A = 'ws-a';
const B = 'ws-b';

// `shared` exists in both workspaces under the same key AND the same rank, so
// nothing but the workspace separates the two rows — the only case where the
// final tie-break is observable. The rank-5 pair is the case one step before
// it: tied on the order key, separated by the row key, which sorts B's row
// ahead of A's and so cannot be reproduced by concatenating workspaces.
const SEED: Record<string, Issue[]> = {
	[A]: [
		{ id: 'i1', title: 'a-one', state: 'open', rank: 5 },
		{ id: 'i2', title: 'a-two', state: 'open', rank: 9 },
		{ id: 'shared', title: 'a-shared', state: 'open', rank: 7 },
	],
	[B]: [
		{ id: 'b1', title: 'b-one', state: 'open', rank: 5 },
		{ id: 'shared', title: 'b-shared', state: 'open', rank: 7 },
		{ id: 'b2', title: 'b-two', state: 'done', rank: 1 },
	],
};

async function seedConfirmed(
	kv: KVStore,
	indexes: IndexCache,
	seed: Record<string, Issue[]> = SEED,
): Promise<void> {
	for (const [ workspaceId, rows ] of Object.entries(seed)) {
		const tx = await kv.write();
		// A build walks only a workspace whose rows are a whole installed
		// snapshot, and the cursor is what says so.
		await tx.put(metaCursor(workspaceId), new Uint8Array([ 1 ]));
		await applyServerChanges(
			tx,
			registry,
			workspaceId,
			rows.map((row, i) =>
				create(RowChangeSchema, {
					entity: 'issue',
					key: row.id,
					version: BigInt(i + 1),
					body: { case: 'value', value: toBinary(IssueSchema, create(IssueSchema, row)) },
				})
			),
			new TouchSet(),
			indexes,
		);
		await tx.commit();
	}
}

/**
 * A two-workspace client over the seed, with its indexes built or not. `built`
 * is the only difference between the arms, which is what makes a divergence a
 * bug rather than a difference in setup. It may also name the workspaces to
 * build, leaving the rest of the seed unbuilt.
 */
async function clientWith(
	built: boolean | string[],
	onEvent?: (event: SyncEvent) => void,
	seed: Record<string, Issue[]> = SEED,
) {
	const kv = new MemoryKVStore();
	const indexes = new IndexCache(registry);
	await seedConfirmed(kv, indexes, seed);
	if (built !== false) {
		const mutex = new ChainMutex();
		for (const workspaceId of built === true ? Object.keys(seed) : built) {
			await buildIndexes(kv, (fn) => mutex.run(fn), registry, indexes, workspaceId);
		}
	}
	return new SyncClient({
		accountId: 'acct',
		workspaces: [ A, B ],
		defaultWorkspace: A,
		kvStore: kv,
		entities: [ IssueSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
		// The maintained watch result is checked against a fresh scan on every
		// read, which is the only thing that would notice a per-workspace
		// predicate drifting from the set it is supposed to describe.
		debugVerifyTouches: true,
		onSyncEvent: onEvent,
	}) as unknown as SyncClient<never>;
}

/** Rows as `<workspace>/<key>`, which is what an ordering assertion is about. */
const at = (rows: readonly Row<Issue>[]): string[] => rows.map((r) => `${r.workspaceId}/${r.key}`);

const openByRank: Query<Issue> = {
	where: (i) => i.state.eq('open'),
	orderBy: (i) => [ i.rank.desc() ],
};

/**
 * One cross-workspace read through the client's normal path.
 *
 * The casts are the price of a runtime-built schema: `Issue` is inferred from
 * the builder, while the descriptor the client holds is only statically a
 * `DescMessage`, so the two cannot be related without a nominal link the builder
 * does not create.
 */
const readAll = (
	client: SyncClient<never>,
	q: Query<Issue> | WorkspaceQuery<Issue>,
	workspaces?: readonly string[],
): Promise<RowSet<Issue>> =>
	client.queryAll(IssueSchema, q as never, { workspaces }) as unknown as Promise<RowSet<Issue>>;

/** A row written straight into one workspace, past the same cast. */
const put = (client: SyncClient<never>, workspaceId: string, row: Issue): Promise<void> =>
	client.ws(workspaceId).mutate.put(IssueSchema, row as never);

/**
 * Runs a read through both arms and hands back what each returned, so a case
 * asserts the answer once and gets the index/scan agreement for free.
 */
async function both(
	read: (client: SyncClient<never>) => Promise<RowSet<Issue>>,
): Promise<{ scanned: RowSet<Issue>; indexed: RowSet<Issue> }> {
	const scanned = await clientWith(false);
	const indexed = await clientWith(true);
	try {
		return { scanned: await read(scanned), indexed: await read(indexed) };
	} finally {
		await scanned.close();
		await indexed.close();
	}
}

/** Asserts both arms returned the same rows in the same order, and pins them. */
async function same(
	read: (client: SyncClient<never>) => Promise<RowSet<Issue>>,
	want: string[],
): Promise<void> {
	const { scanned, indexed } = await both(read);
	expect(at(scanned)).toEqual(want);
	expect(at(indexed)).toEqual(want);
	expect(indexed.total).toEqual(scanned.total);
}

// rank 9, then the rank-7 pair the workspace alone separates, then the rank-5
// pair the row key separates the other way round.
const ALL_OPEN = [ `${A}/i2`, `${A}/shared`, `${B}/shared`, `${B}/b1`, `${A}/i1` ];

describe('queryAll over a named workspace set', () => {
	it('returns what the unbounded read returns when the set covers everything', async () => {
		await same((c) => readAll(c, openByRank), ALL_OPEN);
		await same((c) => readAll(c, openByRank, [ A, B ]), ALL_OPEN);
		// Order of the list is not part of the question it asks.
		await same((c) => readAll(c, openByRank, [ B, A ]), ALL_OPEN);
	});

	it('reads only the workspaces it names, and counts only them', async () => {
		const { scanned, indexed } = await both((c) => readAll(c, openByRank, [ B ]));
		expect(at(scanned)).toEqual([ `${B}/shared`, `${B}/b1` ]);
		expect(at(indexed)).toEqual([ `${B}/shared`, `${B}/b1` ]);
		// The total is the point: reading every workspace and filtering afterwards
		// leaves a total describing a different question.
		expect(scanned.total).toBe(2);
		expect(indexed.total).toBe(2);
	});

	it('names no workspace and reads nothing, which is not "all"', async () => {
		await same((c) => readAll(c, openByRank, []), []);
	});

	it('contributes nothing for a workspace the client does not hold', async () => {
		await same((c) => readAll(c, openByRank, [ B, 'ws-none' ]), [ `${B}/shared`, `${B}/b1` ]);
	});

	// The unbounded read covers every workspace holding rows in the STORE — a
	// workspace removed without a purge, or synced by another tab — which is
	// not the list the client was configured with. Its index legs have to find
	// those workspaces by their rows, and decline for one whose index is not
	// built rather than leave its rows out.
	describe('over every workspace holding rows', () => {
		const C = 'ws-c';
		const WITH_C: Record<string, Issue[]> = {
			...SEED,
			[C]: [ { id: 'c1', title: 'c-one', state: 'open', rank: 8 } ],
		};
		const ALL_OPEN_WITH_C = [ ALL_OPEN[0]!, `${C}/c1`, ...ALL_OPEN.slice(1) ];

		it('reads a workspace the client does not hold, from its index', async () => {
			const events: SyncEvent[] = [];
			const client = await clientWith(true, (e) => events.push(e), WITH_C);
			try {
				expect(at(await readAll(client, openByRank))).toEqual(ALL_OPEN_WITH_C);
				expect(at(await readAll(client, openByRank, [ A, B ]))).toEqual(ALL_OPEN);
			} finally {
				await client.close();
			}
			const queries = events.filter((e): e is QueryEvent => e.type === 'query');
			expect(queries.map((e) => [ e.path, e.index ?? e.declined ])).toEqual([
				[ 'index', 'by_state_rank' ],
				[ 'index', 'by_state_rank' ],
			]);
		});

		it('declines whole while one workspace holding rows is unbuilt', async () => {
			const events: SyncEvent[] = [];
			const client = await clientWith([ A, B ], (e) => events.push(e), WITH_C);
			try {
				expect(at(await readAll(client, openByRank))).toEqual(ALL_OPEN_WITH_C);
			} finally {
				await client.close();
			}
			const queries = events.filter((e): e is QueryEvent => e.type === 'query');
			expect(queries.map((e) => [ e.path, e.declined ])).toEqual([ [ 'scan', 'not-built' ] ]);
		});

		it('reads a workspace whose only rows are pending', async () => {
			// A local create in a workspace with no confirmed rows sits in the
			// overlay alone, and the overlay is the second keyspace the
			// enumeration has to look in. B has a snapshot (an empty one, so its
			// index counts as built) and no confirmed rows.
			const events: SyncEvent[] = [];
			const client = await clientWith(true, (e) => events.push(e), { [A]: SEED[A]!, [B]: [] });
			try {
				await put(client, B, { id: 'b-new', title: 'b-new', state: 'open', rank: 6 });
				expect(at(await readAll(client, openByRank))).toEqual([
					`${A}/i2`,
					`${A}/shared`,
					`${B}/b-new`,
					`${A}/i1`,
				]);
			} finally {
				await client.close();
			}
			const queries = events.filter((e): e is QueryEvent => e.type === 'query');
			expect(queries.map((e) => [ e.path, e.index ?? e.declined ])).toEqual([
				[ 'index', 'by_state_rank' ],
			]);
		});
	});

	it('serves a set of any size from one index walk per workspace', async () => {
		const events: SyncEvent[] = [];
		const client = await clientWith(true, (e) => events.push(e));
		try {
			await readAll(client, openByRank, [ A, B ]);
			await readAll(client, openByRank, [ A ]);
			// The unbounded read: the workspaces holding rows are its set.
			await readAll(client, openByRank);
		} finally {
			await client.close();
		}
		const queries = events.filter((e): e is QueryEvent => e.type === 'query');
		expect(queries.map((e) => [ e.path, e.index ?? e.declined ])).toEqual([
			[ 'index', 'by_state_rank' ],
			[ 'index', 'by_state_rank' ],
			[ 'index', 'by_state_rank' ],
		]);
		// A set of one names the workspace it read; a wider one has none to name.
		expect(queries[0]!.workspaceId).toBeUndefined();
		expect(queries[1]!.workspaceId).toBe(A);
		expect(queries[2]!.workspaceId).toBeUndefined();
	});

	it('refuses a cursor, at any set size', async () => {
		const client = await clientWith(false);
		const page = await client.ws(A).query(IssueSchema, openByRank as never);
		try {
			for (const workspaces of [ [ A ], [ A, B ] ]) {
				await expect(
					readAll(client, { ...openByRank, after: page.endCursor }, workspaces),
				).rejects.toThrow(/cross-workspace/);
			}
		} finally {
			await client.close();
		}
	});
});

describe('a predicate chosen per workspace', () => {
	// The shape the option exists for: the literal a row is matched against is
	// workspace-scoped, so one `where` cannot express it.
	const byWorkspace: WorkspaceQuery<Issue> = {
		orderBy: (i) => [ i.rank.desc() ],
		byWorkspace: (workspaceId) => {
			const title = workspaceId === A ? 'a-two' : 'b-shared';
			return (i) => i.title.eq(title);
		},
	};

	it('matches each workspace on its own literal, under one merged result', async () => {
		await same((c) => readAll(c, byWorkspace, [ A, B ]), [ `${A}/i2`, `${B}/shared` ]);
	});

	it('leaves out a workspace whose predicate declines', async () => {
		const declining: WorkspaceQuery<Issue> = {
			orderBy: (i) => [ i.rank.desc() ],
			byWorkspace: (workspaceId) => workspaceId === A ? undefined : ((i) => i.state.eq('open')),
		};
		await same((c) => readAll(c, declining, [ A, B ]), [ `${B}/shared`, `${B}/b1` ]);
	});

	it('has nothing to apply itself to without a workspace set', async () => {
		const client = await clientWith(false);
		try {
			expect(() => readAll(client, byWorkspace)).toThrow(/byWorkspace has no workspaces/);
		} finally {
			await client.close();
		}
	});
});

describe('watchQueryAll over a named workspace set', () => {
	/** The rows a watch delivers once it has settled on its first read. */
	async function firstEmit(
		client: SyncClient<never>,
		q: Query<Issue> | WorkspaceQuery<Issue>,
		workspaces?: readonly string[],
	): Promise<{ rows: string[]; total: number | undefined }> {
		const seen: RowSet<Issue>[] = [];
		const handle = client.watchQueryAll(
			IssueSchema,
			q as never,
			(rows) => seen.push(rows as unknown as RowSet<Issue>),
			{ workspaces },
		);
		try {
			await vi.waitFor(() => expect(seen.length).toBeGreaterThan(0));
			return { rows: at(seen.at(-1)!), total: seen.at(-1)!.total };
		} finally {
			handle.close();
		}
	}

	it('delivers the same result the one-shot read returns', async () => {
		for (const built of [ false, true ]) {
			const client = await clientWith(built);
			try {
				expect((await firstEmit(client, openByRank)).rows).toEqual(ALL_OPEN);
				expect((await firstEmit(client, openByRank, [ A, B ])).rows).toEqual(ALL_OPEN);
				expect(await firstEmit(client, openByRank, [ B ])).toEqual({
					rows: [ `${B}/shared`, `${B}/b1` ],
					total: 2,
				});
				expect((await firstEmit(client, openByRank, [])).rows).toEqual([]);
			} finally {
				await client.close();
			}
		}
	});

	it('seeds a watcher spanning every workspace from one index walk per workspace', async () => {
		const events: SyncEvent[] = [];
		const client = await clientWith(true, (e) => events.push(e));
		try {
			expect((await firstEmit(client, openByRank)).rows).toEqual(ALL_OPEN);
		} finally {
			await client.close();
		}
		const seeds = events.filter((e): e is QueryEvent => e.type === 'query' && e.origin === 'watch');
		expect(seeds.map((e) => [ e.path, e.index ?? e.declined, e.workspaceId ])).toEqual([
			[ 'index', 'by_state_rank', undefined ],
		]);
	});

	it('maintains a per-workspace predicate across commits', async () => {
		const mine: WorkspaceQuery<Issue> = {
			orderBy: (i) => [ i.rank.desc() ],
			byWorkspace: (workspaceId) => (i) => i.title.eq(`${workspaceId}-mine`),
		};
		const client = await clientWith(true);
		const seen: RowSet<Issue>[] = [];
		const handle = client.watchQueryAll(
			IssueSchema,
			mine as never,
			(rows) => seen.push(rows as unknown as RowSet<Issue>),
			{ workspaces: [ A, B ] },
		);
		try {
			await vi.waitFor(() => expect(seen.length).toBeGreaterThan(0));
			expect(at(seen.at(-1)!)).toEqual([]);

			// A row matching B's literal, written to B: a match. The same title
			// written to A is not, because A's predicate names a different one.
			await put(client, B, { id: 'b3', title: `${B}-mine`, state: 'open', rank: 4 });
			await vi.waitFor(() => expect(at(seen.at(-1)!)).toEqual([ `${B}/b3` ]));

			await put(client, A, { id: 'a3', title: `${B}-mine`, state: 'open', rank: 8 });
			await put(client, A, { id: 'a4', title: `${A}-mine`, state: 'open', rank: 2 });
			await vi.waitFor(() => expect(at(seen.at(-1)!)).toEqual([ `${B}/b3`, `${A}/a4` ]));
		} finally {
			handle.close();
			await client.close();
		}
	});

	it('reports a cursor it cannot honour as a watch that never established', async () => {
		const client = await clientWith(false);
		const page = await client.ws(A).query(IssueSchema, openByRank as never);
		const errors: unknown[] = [];
		const handle = client.watchQueryAll(
			IssueSchema,
			{ ...openByRank, after: page.endCursor } as never,
			() => expect.unreachable('a watch that cannot be established delivers nothing'),
			{ workspaces: [ A, B ], onError: (err) => errors.push(err) },
		);
		try {
			await vi.waitFor(() => expect(errors).toHaveLength(1));
			expect(String(errors[0])).toMatch(/cross-workspace/);
		} finally {
			handle.close();
			await client.close();
		}
	});

	it('is not woken by a commit outside the workspaces it names', async () => {
		const client = await clientWith(false);
		const seen: RowSet<Issue>[] = [];
		const handle = client.watchQueryAll(
			IssueSchema,
			openByRank as never,
			(rows) => seen.push(rows as unknown as RowSet<Issue>),
			{ workspaces: [ A ] },
		);
		try {
			await vi.waitFor(() => expect(seen.length).toBeGreaterThan(0));
			const settled = seen.length;

			await put(client, B, { id: 'b9', title: 'b-nine', state: 'open', rank: 99 });
			await new Promise((r) => setTimeout(r, 10));
			expect(seen.length).toBe(settled);

			await put(client, A, { id: 'a9', title: 'a-nine', state: 'open', rank: 99 });
			await vi.waitFor(() => expect(seen.length).toBeGreaterThan(settled));
			expect(at(seen.at(-1)!)[0]).toBe(`${A}/a9`);
		} finally {
			handle.close();
			await client.close();
		}
	});
});
