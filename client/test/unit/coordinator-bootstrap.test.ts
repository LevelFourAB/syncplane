import { create, toBinary } from '@bufbuild/protobuf';
import { createRouterTransport, type Transport } from '@connectrpc/connect';
import { describe, expect, it } from 'vitest';

import { CoverageResolver } from '../../src/coverage.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { type Mutation, MutationSchema } from '../../src/gen/syncplane/v1/mutation_pb.js';
import { BootstrapResponseSchema, SyncService } from '../../src/gen/syncplane/v1/service_pb.js';
import {
	confirmedKey,
	confirmedWsPrefix,
	metaBootstrapGen,
	metaCursor,
	metaIndexBuilt,
	metaLastMutationId,
	metaSnapshotGen,
	stagingWsRoot,
} from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { KVRead } from '../../src/kv/store.js';
import { buildPut, EntityRegistry } from '../../src/mutations.js';
import { createKvMutex, type Mutex } from '../../src/mutex.js';
import { buildIndexes } from '../../src/optimistic/index-build.js';
import { IndexCache } from '../../src/optimistic/index-maintain.js';
import { enqueuePending } from '../../src/optimistic/queue.js';
import { applyMutationLocally } from '../../src/optimistic/rebase.js';
import { scanMergedRows } from '../../src/read.js';
import { Coordinator } from '../../src/sync/coordinator.js';
import { StatusTracker } from '../../src/sync/status.js';
import { TouchSet } from '../../src/touch.js';
import { WatcherHub } from '../../src/watch.js';
import { pushApplied, pushedMutations, pushFailed, pushResults } from './pushfake.js';

/** The workspace these specs drive. */
const WS = 'ws';

/**
 * The row keys the todo index holds positions for, in index order — read
 * through the store's index, which is what a query reads.
 */
async function indexed(tx: KVRead): Promise<string[]> {
	const prefix = confirmedWsPrefix('todo', WS);
	const keys: string[] = [];
	for await (const cKey of tx.indexScan()) keys.push(cKey.slice(prefix.length));
	return keys;
}

// Exercises the streaming bootstrap end-to-end against a fake server, with the
// emphasis on the property issue 011 is about: local writes (which share the
// kv mutex) must NOT block while the snapshot streams.

const registry = new EntityRegistry([ TodoSchema ]);

function serverRowBody(id: string, title: string, version: bigint) {
	return {
		entity: 'todo',
		key: id,
		version,
		body: { case: 'value' as const, value: toBinary(TodoSchema, create(TodoSchema, { id, title })) },
	};
}

interface Gates {
	/** Resolves once the stream has emitted its chunk and is parked before End. */
	parked: Promise<void>;
	/** Call to let the stream emit End and finish. */
	releaseEnd: () => void;
}

/**
 * A transport whose bootstrap streams Begin → chunk per `chunks` → End, parking
 * after the chunk at index `parkAfter` until the test releases it. push/pull are
 * benign no-ops so the post-bootstrap cycle completes without touching state,
 * unless `resetFirstPull` is set — the way to make a workspace that already
 * holds a snapshot re-bootstrap.
 */
function streamingTransport(
	chunks: ReturnType<typeof serverRowBody>[][],
	parkAfter: number,
	resetFirstPull = false,
): { transport: Transport; gates: Gates } {
	let signalParked!: () => void;
	const parked = new Promise<void>((r) => (signalParked = r));
	let releaseEnd!: () => void;
	const endGate = new Promise<void>((r) => (releaseEnd = r));
	let pulls = 0;

	const transport = createRouterTransport(({ service }) => {
		service(SyncService, {
			async *bootstrap() {
				yield create(BootstrapResponseSchema, {
					body: {
						case: 'begin',
						value: { cursor: new Uint8Array([ 9 ]), lastAckedMutationId: 0n, seq: 7n },
					},
				});
				for (const [ i, rows ] of chunks.entries()) {
					yield create(BootstrapResponseSchema, { body: { case: 'chunk', value: { rows } } });
					if (i === parkAfter) {
						signalParked();
						await endGate;
					}
				}
				yield create(BootstrapResponseSchema, { body: { case: 'end', value: {} } });
			},
			push(req) {
				return pushResults(req, []);
			},
			pull() {
				return {
					patch: { changes: [], dropEntities: [] },
					nextCursor: new Uint8Array([ 9 ]),
					lastAckedMutationId: 0n,
					// A server that has lost the client's position answers reset, which
					// is what sends a workspace holding a cursor back through bootstrap.
					reset: resetFirstPull && pulls++ === 0,
					hasMore: false,
					seq: 7n,
				};
			},
			query() {
				return { rows: [] };
			},
		});
	});

	return { transport, gates: { parked, releaseEnd } };
}

/** One chunk, then a park before End. */
function gatedTransport(
	rows: ReturnType<typeof serverRowBody>[],
	resetFirstPull = false,
): { transport: Transport; gates: Gates } {
	return streamingTransport([ rows ], 0, resetFirstPull);
}

/**
 * Two chunks with the park BETWEEN them — the window in which another tab can
 * take the workspace away from a bootstrap that is already writing rows.
 */
function twoChunkTransport(
	first: ReturnType<typeof serverRowBody>[],
	second: ReturnType<typeof serverRowBody>[],
): { transport: Transport; gates: Gates } {
	return streamingTransport([ first, second ], 0);
}

function newCoordinator(
	kv: MemoryKVStore,
	kvMutex: Mutex,
	transport: Transport,
	indexes?: IndexCache,
	onIndexesStale?: (workspaceId: string) => void,
): Coordinator {
	const hub = new WatcherHub(kv, registry, kvMutex);
	return new Coordinator({
		transport,
		kv,
		kvMutex,
		accountId: 'acct',
		scopes: new Map([ [ WS, [] ] ]),
		clientId: 'c1',
		registry,
		indexes,
		onIndexesStale,
		mirrors: {},
		notify: (touched) => hub.notify(touched),
		status: new StatusTracker(),
		streamEnabled: false,
		pollIntervalMs: 0,
	});
}

/**
 * An IndexCache that gives `todo` one index. Todo declares none in the shared
 * schema, and injecting one here is cheaper than moving a schema several other
 * suites pin.
 */
function indexedRegistry(): EntityRegistry {
	const def = registry.get('todo')!;
	return {
		get: () => ({
			...def,
			indexes: [ { name: 'by_title', key: [ { path: [ 'title' ], desc: false } ] } ],
		}),
		names: () => [ 'todo' ],
	} as unknown as EntityRegistry;
}

function indexedTodo(): IndexCache {
	return new IndexCache(indexedRegistry());
}

/**
 * Waits for a condition that only the parked stream's chunk can satisfy.
 *
 * The park says the SERVER has emitted its chunk, which is one hop short of the
 * client having written it; polling closes that gap without weakening what is
 * being asserted, since nothing else is in flight to make the answer true.
 */
async function until(probe: () => Promise<boolean>, what: string): Promise<void> {
	for (let i = 0; i < 200; i++) {
		if (await probe()) return;
		await new Promise((r) => setTimeout(r, 5));
	}
	throw new Error(`timed out waiting for ${what}`);
}

async function stagedKeys(kv: MemoryKVStore): Promise<string[]> {
	const tx = await kv.read();
	try {
		const out: string[] = [];
		for await (const [ k ] of tx.scan(stagingWsRoot(WS))) out.push(k);
		return out;
	} finally {
		tx.release();
	}
}

async function keysOf(kv: MemoryKVStore): Promise<string[]> {
	const tx = await kv.read();
	try {
		return (await scanMergedRows(tx, registry, 'todo', WS)).map((r) => r.key);
	} finally {
		tx.release();
	}
}

describe('streaming bootstrap', () => {
	it('does not hold the kv mutex while the snapshot streams', async () => {
		const kv = new MemoryKVStore();
		const kvMutex = createKvMutex('bootstrap-test-1');
		const { transport, gates } = gatedTransport([ serverRowBody('t1', 'from server', 5n) ]);
		const coord = newCoordinator(kv, kvMutex, transport);

		const syncP = coord.syncNow();
		await gates.parked; // stream emitted its chunk and is parked before End

		// A local write through the SAME mutex while the stream is parked. Under
		// the old design (mutex held for the whole stream) this would deadlock;
		// here it must complete promptly.
		const writeP = kvMutex.run(async () => {
			const tx = await kv.write();
			const def = registry.bySchema(TodoSchema);
			const crud = buildPut(
				def,
				TodoSchema,
				create(TodoSchema, { id: 'local', title: 'written mid-stream' }),
			);
			const mutation: Mutation = create(MutationSchema, {
				clientId: 'c1',
				mutationId: 1n,
				payload: { case: 'crud', value: crud },
			});
			await enqueuePending(tx, WS, mutation);
			await applyMutationLocally(tx, registry, {}, WS, mutation, new TouchSet());
			await tx.put(metaLastMutationId(WS), '1');
			await tx.commit();
		});

		await expect(
			Promise.race([
				writeP.then(() => 'written'),
				new Promise((_, reject) =>
					setTimeout(() => reject(new Error('local write blocked during bootstrap')), 1000)
				),
			]),
		).resolves.toBe('written');

		gates.releaseEnd();
		await syncP;

		// The streamed snapshot and the mid-stream local write both survive.
		expect((await keysOf(kv)).sort()).toEqual([ 'local', 't1' ]);
		const tx = await kv.read();
		expect(await tx.get(metaCursor(WS))).toEqual(new Uint8Array([ 9 ]));
		tx.release();

		await coord.stop();
	});

	it('leaves no staging rows behind after a clean bootstrap', async () => {
		const kv = new MemoryKVStore();
		const kvMutex = createKvMutex('bootstrap-test-2');
		const { transport, gates } = gatedTransport([
			serverRowBody('t1', 'a', 1n),
			serverRowBody('t2', 'b', 2n),
		]);
		const coord = newCoordinator(kv, kvMutex, transport);

		const syncP = coord.syncNow();
		await gates.parked;
		gates.releaseEnd();
		await syncP;

		const tx = await kv.read();
		const staging: string[] = [];
		for await (const [ k ] of tx.scan(stagingWsRoot(WS))) staging.push(k);
		// The generation marker persists (monotonic), but no staged rows linger.
		expect(await tx.get(metaBootstrapGen(WS))).toBe(1);
		tx.release();
		expect(staging).toEqual([]);
		expect((await keysOf(kv)).sort()).toEqual([ 't1', 't2' ]);

		await coord.stop();
	});

	it('leaves the indexes unbuilt and asks for a rebuild', async () => {
		// The snapshot is promoted WITHOUT index positions — computing them means
		// decoding every row of the snapshot inside the one transaction the swap
		// has to commit before anything is readable. So the swap must leave the
		// workspace in the state a query already handles (no positions, no marker
		// claiming there are) and hand the rebuild to someone who can do it off
		// the critical path. Unindexed rows under a surviving marker would be the
		// one combination that silently returns wrong answers.
		const kv = new MemoryKVStore();
		const kvMutex = createKvMutex('bootstrap-test-3');
		const { transport, gates } = gatedTransport([ serverRowBody('t1', 'a', 1n) ]);
		const indexes = indexedTodo();
		const marker = metaIndexBuilt(WS, 'todo', 'by_title', indexes.for('todo')[0]!.specHash);
		const stale: string[] = [];
		const coord = newCoordinator(kv, kvMutex, transport, indexes, (ws) => stale.push(ws));

		// The state an earlier build left behind. It described rows this swap is
		// about to replace, so keeping it would be a lie.
		const seed = await kv.write();
		await seed.put(marker, true);
		await seed.commit();

		const syncP = coord.syncNow();
		await gates.parked;
		gates.releaseEnd();
		await syncP;

		const tx = await kv.read();
		try {
			expect(await tx.has(marker)).toBe(false);
			expect(await indexed(tx)).toEqual([]);
			// The generation guard the build pass checks itself against.
			expect(await tx.get(metaSnapshotGen(WS))).toBe(1);
		} finally {
			tx.release();
		}
		// And the rebuild was actually requested — without this the workspace
		// scans for the rest of the session.
		expect(stale).toEqual([ WS ]);

		await coord.stop();
	});

	it('builds the indexes the swap left unbuilt', async () => {
		// The other half of the contract above: once the build pass runs, the
		// entity is indexed and the marker is back. Together these two say the
		// deferral costs a delay and not the index.
		const kv = new MemoryKVStore();
		const kvMutex = createKvMutex('bootstrap-test-4');
		const { transport, gates } = gatedTransport([
			serverRowBody('t1', 'b', 1n),
			serverRowBody('t2', 'a', 1n),
		]);
		const indexes = indexedTodo();
		const marker = metaIndexBuilt(WS, 'todo', 'by_title', indexes.for('todo')[0]!.specHash);
		const coord = newCoordinator(kv, kvMutex, transport, indexes);

		const syncP = coord.syncNow();
		await gates.parked;
		gates.releaseEnd();
		await syncP;

		await buildIndexes(kv, (fn) => kvMutex.run(fn), indexedRegistry(), indexes, WS);

		const tx = await kv.read();
		try {
			expect(await tx.has(marker)).toBe(true);
			// Index order is the indexed value's, not the row key's: 'a' before
			// 'b' means the positions really encode the title.
			expect(await indexed(tx)).toEqual([ 't2', 't1' ]);
		} finally {
			tx.release();
		}

		await coord.stop();
	});

	it('does not claim markers for a snapshot that was replaced under it', async () => {
		// A build walks outside the swap's transaction, so a bootstrap can land
		// mid-walk and replace every row it has visited. Marking the index built
		// then would claim coverage of rows that no longer exist — the one way
		// deferring the build could return wrong answers rather than slow ones.
		const kv = new MemoryKVStore();
		const kvMutex = createKvMutex('bootstrap-test-5');
		const indexes = indexedTodo();
		const marker = metaIndexBuilt(WS, 'todo', 'by_title', indexes.for('todo')[0]!.specHash);

		// A store holding one confirmed row under snapshot generation 1. The
		// cursor is what makes those rows an installed snapshot; without it the
		// build declines to walk at all and this would pass for the wrong reason.
		const seed = await kv.write();
		await seed.put(metaSnapshotGen(WS), 1);
		await seed.put(metaCursor(WS), new Uint8Array([ 9 ]));
		await seed.put(
			confirmedKey('todo', WS, 't1'),
			{ v: toBinary(TodoSchema, create(TodoSchema, { id: 't1', title: 'a' })), ver: '1' },
		);
		await seed.commit();

		// A swap lands between the walk and the marker write, exactly where the
		// guard has to catch it: runLocked is what the build takes for each, so
		// bumping the generation from inside one puts it in the gap.
		let batches = 0;
		await buildIndexes(
			kv,
			async (fn) => {
				const result = await kvMutex.run(fn);
				if (++batches === 1) {
					const tx = await kv.write();
					await tx.put(metaSnapshotGen(WS), 2);
					await tx.commit();
				}
				return result;
			},
			indexedRegistry(),
			indexes,
			WS,
		);

		const tx = await kv.read();
		try {
			expect(await tx.has(marker)).toBe(false);
		} finally {
			tx.release();
		}
	});

	it('does not claim markers when the workspace is cleared mid-walk', async () => {
		// The gap the generation check alone cannot cover: a bootstrap that clears
		// the workspace during the walk has not installed a new generation yet, so
		// on a store that never had one the guard compares undefined to undefined
		// and passes. Losing the cursor is what says the rows underneath are being
		// replaced.
		const kv = new MemoryKVStore();
		const kvMutex = createKvMutex('bootstrap-test-7');
		const indexes = indexedTodo();
		const marker = metaIndexBuilt(WS, 'todo', 'by_title', indexes.for('todo')[0]!.specHash);

		const seed = await kv.write();
		await seed.put(metaCursor(WS), new Uint8Array([ 9 ]));
		await seed.put(
			confirmedKey('todo', WS, 't1'),
			{ v: toBinary(TodoSchema, create(TodoSchema, { id: 't1', title: 'a' })), ver: '1' },
		);
		await seed.commit();

		let batches = 0;
		await buildIndexes(
			kv,
			async (fn) => {
				const result = await kvMutex.run(fn);
				if (++batches === 1) {
					const tx = await kv.write();
					await tx.del(metaCursor(WS));
					await tx.commit();
				}
				return result;
			},
			indexedRegistry(),
			indexes,
			WS,
		);

		const tx = await kv.read();
		try {
			expect(await tx.has(marker)).toBe(false);
		} finally {
			tx.release();
		}
	});

	it('does not claim markers over a workspace that is still filling', async () => {
		// The same hazard from the other side, and the one the generation cannot
		// see: a cold bootstrap writes rows into c/ as they arrive and installs no
		// new generation until it finishes, so a build racing it would walk part of
		// a snapshot and claim to cover all of it. No cursor is what says the rows
		// are not (yet) a snapshot.
		const kv = new MemoryKVStore();
		const kvMutex = createKvMutex('bootstrap-test-6');
		const indexes = indexedTodo();
		const marker = metaIndexBuilt(WS, 'todo', 'by_title', indexes.for('todo')[0]!.specHash);

		const seed = await kv.write();
		await seed.put(
			confirmedKey('todo', WS, 't1'),
			{ v: toBinary(TodoSchema, create(TodoSchema, { id: 't1', title: 'a' })), ver: '1' },
		);
		await seed.commit();

		await buildIndexes(kv, (fn) => kvMutex.run(fn), indexedRegistry(), indexes, WS);

		const tx = await kv.read();
		try {
			expect(await tx.has(marker)).toBe(false);
			expect(await indexed(tx)).toEqual([]);
		} finally {
			tx.release();
		}
	});
});

// A COLD bootstrap — no cursor, so no installed snapshot — writes its rows
// straight into confirmed state instead of staging them. Staging exists to keep
// readers on the old snapshot until the new one is whole, and a workspace that
// has none has nothing to keep them on; paying a second put and a delete per row
// for that is the cost issue 118 is about.

describe('cold bootstrap', () => {
	it('writes the snapshot straight into confirmed state', async () => {
		const kv = new MemoryKVStore();
		const kvMutex = createKvMutex('cold-test-1');
		const { transport, gates } = gatedTransport([
			serverRowBody('t1', 'a', 1n),
			serverRowBody('t2', 'b', 2n),
		]);
		const coord = newCoordinator(kv, kvMutex, transport);

		const syncP = coord.syncNow();
		await gates.parked; // the chunk has been written, End has not arrived

		// Mid-stream: the rows are readable and nothing was staged. Under the
		// staged path both of these would be the other way round.
		await until(async () => (await keysOf(kv)).length === 2, 'the chunk to land in c/');
		expect((await keysOf(kv)).sort()).toEqual([ 't1', 't2' ]);
		expect(await stagedKeys(kv)).toEqual([]);
		// Not durable yet, though: the cursor lands with the final transaction, so
		// a crash here re-bootstraps rather than trusting a partial snapshot.
		const mid = await kv.read();
		try {
			expect(await mid.has(metaCursor(WS))).toBe(false);
		} finally {
			mid.release();
		}

		gates.releaseEnd();
		await syncP;

		const tx = await kv.read();
		try {
			expect(await tx.get(metaCursor(WS))).toEqual(new Uint8Array([ 9 ]));
			expect(await tx.get(metaSnapshotGen(WS))).toBe(1);
		} finally {
			tx.release();
		}
		expect((await keysOf(kv)).sort()).toEqual([ 't1', 't2' ]);

		await coord.stop();
	});

	it('stages a re-bootstrap, so readers never see half a snapshot', async () => {
		// The other half of the rule: once a cursor exists the workspace holds a
		// whole snapshot, and replacing it in place would show readers a mixture of
		// two. This is the case staging is for, and it must not have been traded
		// away for the cold path's speed.
		const kv = new MemoryKVStore();
		const kvMutex = createKvMutex('cold-test-2');
		const { transport, gates } = gatedTransport([ serverRowBody('t2', 'new', 5n) ], true);
		const coord = newCoordinator(kv, kvMutex, transport);

		const seed = await kv.write();
		await seed.put(metaCursor(WS), new Uint8Array([ 1 ]));
		await seed.put(
			confirmedKey('todo', WS, 't1'),
			{ v: toBinary(TodoSchema, create(TodoSchema, { id: 't1', title: 'old' })), ver: '1' },
		);
		await seed.commit();

		const syncP = coord.syncNow();
		await gates.parked;

		// Mid-stream the pre-bootstrap snapshot is intact and the incoming one is
		// invisible, sitting in staging.
		await until(async () => (await stagedKeys(kv)).length === 1, 'the chunk to land in b/');
		expect(await keysOf(kv)).toEqual([ 't1' ]);

		gates.releaseEnd();
		await syncP;

		// And then the flip, whole: the old row is gone, the new one is in.
		expect(await keysOf(kv)).toEqual([ 't2' ]);

		await coord.stop();
	});

	it('clears what an interrupted attempt left behind', async () => {
		// Rows with no cursor are the residue of a bootstrap that never finished,
		// never a snapshot — so the claim wipes them. Keeping them would merge two
		// snapshots under one cursor: a row the server deleted in between would
		// survive locally forever, since the pull that follows only carries changes
		// after the new snapshot's seq.
		const kv = new MemoryKVStore();
		const kvMutex = createKvMutex('cold-test-3');
		const { transport, gates } = gatedTransport([ serverRowBody('t2', 'b', 2n) ]);
		const coord = newCoordinator(kv, kvMutex, transport);

		const seed = await kv.write();
		await seed.put(
			confirmedKey('todo', WS, 'gone'),
			{ v: toBinary(TodoSchema, create(TodoSchema, { id: 'gone', title: 'x' })), ver: '1' },
		);
		await seed.commit();

		const syncP = coord.syncNow();
		await gates.parked;
		gates.releaseEnd();
		await syncP;

		expect(await keysOf(kv)).toEqual([ 't2' ]);

		await coord.stop();
	});

	it('stops writing when another tab claims the workspace mid-stream', async () => {
		// A cold bootstrap cannot discard rows it has already made visible, so
		// instead of detecting supersession at the end it re-checks the generation
		// inside every chunk transaction. The tab that claimed the workspace wiped
		// what came before at its own claim, so nothing of this one's snapshot may
		// land after that point.
		const kv = new MemoryKVStore();
		const kvMutex = createKvMutex('cold-test-4');
		const { transport, gates } = twoChunkTransport(
			[ serverRowBody('first', 'a', 1n) ],
			[ serverRowBody('second', 'b', 2n) ],
		);
		const coord = newCoordinator(kv, kvMutex, transport);

		const syncP = coord.syncNow();
		await gates.parked; // chunk one is in; chunk two has not been sent

		// Another tab claims the workspace, exactly as doBootstrap's own claim
		// does — bumping the generation and wiping what is there.
		const claim = await kv.write();
		await claim.put(metaBootstrapGen(WS), 99);
		await claim.del(confirmedKey('todo', WS, 'first'));
		await claim.commit();

		gates.releaseEnd();
		await syncP;

		const tx = await kv.read();
		try {
			// Chunk two never landed, and the losing bootstrap installed no cursor:
			// the workspace belongs to generation 99 alone.
			expect(await tx.has(confirmedKey('todo', WS, 'second'))).toBe(false);
			expect(await tx.has(metaCursor(WS))).toBe(false);
			expect(await tx.get(metaBootstrapGen(WS))).toBe(99);
		} finally {
			tx.release();
		}
		expect(await keysOf(kv)).toEqual([]);

		await coord.stop();
	});
});

// A cold snapshot lands entity by entity and is readable as it goes, so
// "this entity is empty" and "this entity has not arrived" are the same `[]`
// until something says otherwise. These specs pin what says otherwise: the
// coverage bound readers see, the workspace status a progress UI renders, and
// the watcher notification that makes an entity appear the moment it is whole.
describe('cold bootstrap hydration', () => {
	const twoEntities = new EntityRegistry([ TodoSchema, WidgetSchema ]);

	function widgetRowBody(id: string, version: bigint) {
		return {
			entity: 'widget',
			key: id,
			version,
			body: {
				case: 'value' as const,
				value: toBinary(WidgetSchema, create(WidgetSchema, { id })),
			},
		};
	}

	/**
	 * Streams Begin (announcing todo before widget) → the todo chunk → a park →
	 * the widget chunk → End, which is the shape a two-entity snapshot has.
	 * `announce` false is the older server that names no order.
	 */
	function orderedTransport(
		announce = true,
		seedCursor = false,
	): { transport: Transport; gates: Gates } {
		let signalParked!: () => void;
		const parked = new Promise<void>((r) => (signalParked = r));
		let releaseEnd!: () => void;
		const endGate = new Promise<void>((r) => (releaseEnd = r));
		let pulls = 0;
		const transport = createRouterTransport(({ service }) => {
			service(SyncService, {
				async *bootstrap() {
					yield create(BootstrapResponseSchema, {
						body: {
							case: 'begin',
							value: {
								cursor: new Uint8Array([ 9 ]),
								seq: 7n,
								entities: announce ? [ 'todo', 'widget' ] : [],
							},
						},
					});
					yield create(BootstrapResponseSchema, {
						body: { case: 'chunk', value: { rows: [ serverRowBody('t1', 'a', 1n) ] } },
					});
					signalParked();
					await endGate;
					yield create(BootstrapResponseSchema, {
						body: { case: 'chunk', value: { rows: [ widgetRowBody('w1', 2n) ] } },
					});
					yield create(BootstrapResponseSchema, { body: { case: 'end', value: {} } });
				},
				push(req) {
					return pushResults(req, []);
				},
				pull() {
					return {
						patch: { changes: [], dropEntities: [] },
						nextCursor: new Uint8Array([ 9 ]),
						lastAckedMutationId: 0n,
						reset: seedCursor && pulls++ === 0,
						hasMore: false,
						seq: 7n,
					};
				},
				query() {
					return { rows: [] };
				},
			});
		});
		return { transport, gates: { parked, releaseEnd } };
	}

	function hydratingCoordinator(
		kv: MemoryKVStore,
		kvMutex: Mutex,
		transport: Transport,
	): { coord: Coordinator; status: StatusTracker; notified: string[][] } {
		const status = new StatusTracker();
		const notified: string[][] = [];
		const coord = new Coordinator({
			transport,
			kv,
			kvMutex,
			accountId: 'acct',
			scopes: new Map([ [ WS, [] ] ]),
			clientId: 'c1',
			registry: twoEntities,
			mirrors: {},
			notify: (touched) => {
				notified.push([ ...touched.pairs() ].map((p) => p.entity).sort());
				return Promise.resolve();
			},
			status,
			streamEnabled: false,
			pollIntervalMs: 0,
		});
		return { coord, status, notified };
	}

	it('reports the entities still arriving, and clears them as they land', async () => {
		const kv = new MemoryKVStore();
		const kvMutex = createKvMutex('hydration-test-1');
		const { transport, gates } = orderedTransport();
		const { coord, status, notified } = hydratingCoordinator(kv, kvMutex, transport);

		const syncP = coord.syncNow();
		await gates.parked;
		// Parked after the todo chunk: `todo` has shipped everything it had, but
		// only the widget rows starting prove it, so both are still outstanding.
		await until(
			async () => status.current().workspaces[WS]?.hydration !== undefined,
			'hydration to be reported',
		);
		expect(status.current().workspaces[WS]?.hydration).toEqual({
			pending: [ 'todo', 'widget' ],
			total: 2,
			done: 0,
		});
		expect([ ...coord.hydrationOf(WS)! ].sort()).toEqual([ 'todo', 'widget' ]);

		gates.releaseEnd();
		await syncP;

		// Snapshot installed: nothing is outstanding and nothing is reported.
		expect(status.current().workspaces[WS]?.hydration).toBeUndefined();
		expect(coord.hydrationOf(WS)).toBeUndefined();
		// The widget chunk completed `todo` before the install did anything, so
		// its watchers heard about it while the widget rows were still streaming.
		expect(notified.some((entities) => entities.join() === 'todo')).toBe(true);

		await coord.stop();
	});

	it('feeds the coverage a reader sees', async () => {
		const kv = new MemoryKVStore();
		const kvMutex = createKvMutex('hydration-test-2');
		const { transport, gates } = orderedTransport();
		const { coord } = hydratingCoordinator(kv, kvMutex, transport);
		const coverage = new CoverageResolver(
			() => new Map([ [ WS, [] ] ]),
			(ws) => coord.hydrationOf(ws),
		);

		const syncP = coord.syncNow();
		await gates.parked;
		await until(
			async () => !coverage.coverageOf('widget', WS).complete,
			'the workspace to report as hydrating',
		);
		// Full sync, so the only thing that can bound either entity is that its
		// rows have not arrived yet.
		expect(coverage.coverageOf('widget', WS)).toEqual({
			complete: false,
			bounds: [ 'hydrating' ],
		});

		gates.releaseEnd();
		await syncP;

		expect(coverage.coverageOf('widget', WS)).toEqual({ complete: true, bounds: [] });
		await coord.stop();
	});

	it('holds every entity pending when the server announces no order', async () => {
		// An older server names no delivery order, so nothing proves an entity is
		// finished until the snapshot ends. Claiming otherwise would be a guess in
		// the unsafe direction.
		const kv = new MemoryKVStore();
		const kvMutex = createKvMutex('hydration-test-3');
		const { transport, gates } = orderedTransport(false);
		const { coord, status } = hydratingCoordinator(kv, kvMutex, transport);

		const syncP = coord.syncNow();
		await gates.parked;
		await until(
			async () => status.current().workspaces[WS]?.hydration !== undefined,
			'hydration to be reported',
		);
		expect(status.current().workspaces[WS]?.hydration?.done).toBe(0);
		expect([ ...coord.hydrationOf(WS)! ].sort()).toEqual([ 'todo', 'widget' ]);

		gates.releaseEnd();
		await syncP;
		expect(coord.hydrationOf(WS)).toBeUndefined();

		await coord.stop();
	});

	it('says nothing about a re-bootstrap, which readers never see mid-flight', async () => {
		// The staged path swaps atomically: the previous snapshot is intact and
		// readable throughout, so there is no partial state to warn about.
		const kv = new MemoryKVStore();
		const kvMutex = createKvMutex('hydration-test-4');
		const { transport, gates } = orderedTransport(true, true);
		const { coord, status } = hydratingCoordinator(kv, kvMutex, transport);

		const seed = await kv.write();
		await seed.put(metaCursor(WS), new Uint8Array([ 1 ]));
		await seed.commit();

		const syncP = coord.syncNow();
		await gates.parked;
		expect(status.current().workspaces[WS]?.hydration).toBeUndefined();
		expect(coord.hydrationOf(WS)).toBeUndefined();

		gates.releaseEnd();
		await syncP;
		await coord.stop();
	});
});
