// Relevance ordering over a cross-workspace read whose predicate is chosen per
// workspace.
//
// The shape being pinned: each leg carries its own `match`, and the merged
// result is ordered by the ranks those matches produced. That is sound because
// a rank reads the row alone — no corpus statistics, nothing about the
// partition it came from — so two legs' ranks are as comparable as two rows'
// within one leg.
//
// Every case runs twice over identical data, once with the search index built
// and once without, and compares the arms. The scan is the definition; the
// per-workspace index legs are an optimization over it.

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
import { and, type Query, type WorkspaceQuery } from '../../src/query/index.js';
import type { Row } from '../../src/read.js';
import { defineSchema, entity, type Infer, text } from '../../src/schema/builder.js';
import { TouchSet } from '../../src/touch.js';

const note = entity('note', {
	id: text().key(),
	title: text().searchable(),
	owner: text().indexed(),
});

type Note = Infer<typeof note>;
const bundle = defineSchema(note);
const NoteSchema = bundle.descMessage('note');
const registry = new EntityRegistry([ NoteSchema ]);

const A = 'ws-a';
const B = 'ws-b';

/** The member id standing for "me" in each workspace — a different literal per
 * partition, which is the whole reason `byWorkspace` exists. */
const ME: Record<string, string> = { [A]: 'member-7', [B]: 'member-3' };

// Ranked on the position of the matched term, which is the only component that
// separates these rows: `a1` carries "report" second, `b1` carries it first, so
// relevance puts B's row ahead of A's — the opposite of the row-key order a
// merged result falls back to. `a2` matches the term but belongs to someone
// else, and `b2` is the reverse, so each leg's non-search half has to survive
// the search-index path as well as the scan.
const SEED: Record<string, Note[]> = {
	[A]: [
		{ id: 'a1', title: 'Quarterly report', owner: ME[A]! },
		{ id: 'a2', title: 'Report on hiring', owner: 'member-9' },
		{ id: 'a3', title: 'Kitchen roster', owner: ME[A]! },
	],
	[B]: [
		{ id: 'b1', title: 'Report of the year', owner: ME[B]! },
		{ id: 'b2', title: 'Nothing to see', owner: ME[B]! },
	],
};

async function seedConfirmed(kv: KVStore, indexes: IndexCache): Promise<void> {
	for (const [ workspaceId, rows ] of Object.entries(SEED)) {
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
					entity: 'note',
					key: row.id,
					version: BigInt(i + 1),
					body: { case: 'value', value: toBinary(NoteSchema, create(NoteSchema, row)) },
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
 * bug rather than a difference in setup.
 */
async function clientWith(built: boolean) {
	const kv = new MemoryKVStore();
	const indexes = new IndexCache(registry);
	await seedConfirmed(kv, indexes);
	if (built) {
		const mutex = new ChainMutex();
		for (const workspaceId of [ A, B ]) {
			await buildIndexes(kv, (fn) => mutex.run(fn), registry, indexes, workspaceId);
		}
	}
	return new SyncClient({
		accountId: 'acct',
		workspaces: [ A, B ],
		defaultWorkspace: A,
		kvStore: kv,
		entities: [ NoteSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
		// The maintained watch result is checked against a fresh scan on every
		// read, which is the only thing that would notice a per-workspace
		// ranking drifting from the set it is supposed to describe.
		debugVerifyTouches: true,
	}) as unknown as SyncClient<never>;
}

/** Rows as `<workspace>/<key>`, which is what an ordering assertion is about. */
const at = (rows: readonly Row<Note>[]): string[] => rows.map((r) => `${r.workspaceId}/${r.key}`);

/**
 * One cross-workspace read through the client's normal path.
 *
 * The casts are the price of a runtime-built schema: `Note` is inferred from the
 * builder, while the descriptor the client holds is only statically a
 * `DescMessage`, so the two cannot be related without a nominal link the builder
 * does not create.
 */
const readAll = (
	client: SyncClient<never>,
	q: Query<Note> | WorkspaceQuery<Note>,
	workspaces?: readonly string[],
): Promise<RowSet<Note>> =>
	client.queryAll(NoteSchema, q as never, { workspaces }) as unknown as Promise<RowSet<Note>>;

/** Asserts both arms returned the same rows in the same order, and pins them. */
async function same(
	read: (client: SyncClient<never>) => Promise<RowSet<Note>>,
	want: string[],
): Promise<void> {
	const scanned = await clientWith(false);
	const indexed = await clientWith(true);
	try {
		const a = await read(scanned);
		const b = await read(indexed);
		expect(at(a)).toEqual(want);
		expect(at(b)).toEqual(want);
		expect(b.total).toEqual(a.total);
	} finally {
		await scanned.close();
		await indexed.close();
	}
}

/** "My notes about reports", where "mine" is a different id in each workspace. */
const mineAboutReports: WorkspaceQuery<Note> = {
	orderBy: (n) => [ n.relevance() ],
	byWorkspace: (workspaceId) => {
		const me = ME[workspaceId];
		return me === undefined ? undefined : ((n) => and(n.match('report'), n.owner.eq(me)));
	},
};

describe('relevance over a per-workspace predicate', () => {
	it('ranks the merged result by the match each leg carried', async () => {
		// b1 carries the term first and so outranks a1, which the row-key
		// fallback would have put ahead of it; a2 and b2 are filtered out by the
		// non-search half of their leg's predicate.
		await same((c) => readAll(c, mineAboutReports, [ A, B ]), [ `${B}/b1`, `${A}/a1` ]);
	});

	it('reads one workspace of the same query unchanged', async () => {
		await same((c) => readAll(c, mineAboutReports, [ A ]), [ `${A}/a1` ]);
		await same((c) => readAll(c, mineAboutReports, [ B ]), [ `${B}/b1` ]);
	});

	it('lets each workspace search for something different', async () => {
		// Two legs, two queries, one order: b2 carries its term first and a3
		// carries its second, and the ranks say so across the legs exactly as
		// they would within one.
		const perTerm: WorkspaceQuery<Note> = {
			orderBy: (n) => [ n.relevance() ],
			byWorkspace: (workspaceId) => (n) => n.match(workspaceId === A ? 'roster' : 'nothing'),
		};
		await same((c) => readAll(c, perTerm, [ A, B ]), [ `${B}/b2`, `${A}/a3` ]);
	});

	it('leaves out a workspace whose predicate declines, match and all', async () => {
		const onlyB: WorkspaceQuery<Note> = {
			orderBy: (n) => [ n.relevance() ],
			byWorkspace: (workspaceId) => workspaceId === B ? ((n) => n.match('report')) : undefined,
		};
		await same((c) => readAll(c, onlyB, [ A, B ]), [ `${B}/b1` ]);
	});

	it('names the workspace whose predicate it cannot rank', async () => {
		const client = await clientWith(false);
		const halfMatched: WorkspaceQuery<Note> = {
			orderBy: (n) => [ n.relevance() ],
			byWorkspace: (workspaceId) =>
				workspaceId === A ? ((n) => n.match('report')) : ((n) => n.owner.eq(ME[B]!)),
		};
		try {
			await expect(readAll(client, halfMatched, [ A, B ])).rejects.toThrow(
				/exactly one match condition in every workspace's predicate, found 0 for workspace "ws-b"/,
			);
		} finally {
			await client.close();
		}
	});

	it('keeps ranking a shared predicate across a named set', async () => {
		// The uniform form of the same query: one `where` for every workspace,
		// which binds the relevance key the way a single-workspace query does.
		const shared: Query<Note> = {
			where: (n) => n.match('report'),
			orderBy: (n) => [ n.relevance() ],
		};
		// a2 and b1 both carry the term first, so nothing in the rank separates
		// them and the row key does.
		await same((c) => readAll(c, shared, [ A, B ]), [ `${A}/a2`, `${B}/b1`, `${A}/a1` ]);
	});
});

describe('watchQueryAll ranking a per-workspace predicate', () => {
	it('delivers the ranked result and maintains it across commits', async () => {
		const client = await clientWith(true);
		const seen: RowSet<Note>[] = [];
		const handle = client.watchQueryAll(
			NoteSchema,
			mineAboutReports as never,
			(rows) => seen.push(rows as unknown as RowSet<Note>),
			{ workspaces: [ A, B ] },
		);
		try {
			await vi.waitFor(() => expect(seen.length).toBeGreaterThan(0));
			expect(at(seen.at(-1)!)).toEqual([ `${B}/b1`, `${A}/a1` ]);

			// A worse match than either, written into A: the term is further into
			// the title than in any row already held, so it lands last — which is
			// what the maintained set has to work out without re-ranking the rows
			// it already holds.
			await client.ws(A).mutate.put(
				NoteSchema,
				{ id: 'a4', title: 'Draft of the report', owner: ME[A]! } as never,
			);
			await vi.waitFor(() => expect(at(seen.at(-1)!)).toEqual([ `${B}/b1`, `${A}/a1`, `${A}/a4` ]));

			// Owned by someone else in B, so B's leg never sees it however well
			// it matches.
			await client.ws(B).mutate.put(
				NoteSchema,
				{ id: 'b3', title: 'Report', owner: 'member-9' } as never,
			);
			await new Promise((r) => setTimeout(r, 10));
			expect(at(seen.at(-1)!)).toEqual([ `${B}/b1`, `${A}/a1`, `${A}/a4` ]);
		} finally {
			handle.close();
			await client.close();
		}
	});
});
