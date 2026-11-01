import { create } from '@bufbuild/protobuf';
import { createRouterTransport, type Transport } from '@connectrpc/connect';
import { beforeEach, describe, expect, it } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import {
	CrudMutationSchema,
	CrudOp,
	type Mutation,
	MutationSchema,
} from '../../src/gen/syncplane/v1/mutation_pb.js';
import {
	BootstrapResponseSchema,
	MutationStatus,
	type PullResponse,
	PullResponseSchema,
	SyncService,
} from '../../src/gen/syncplane/v1/service_pb.js';
import { metaCursor, metaLastMutationId, metaUnresolvedPush } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { EntityRegistry } from '../../src/mutations.js';
import { createKvMutex } from '../../src/mutex.js';
import { enqueuePending, readPending } from '../../src/optimistic/queue.js';
import { DEFAULT_STORAGE_PREFIX, storageNames } from '../../src/storagenames.js';
import {
	Coordinator,
	type CoordinatorOptions,
	type RejectionInfo,
} from '../../src/sync/coordinator.js';
import { StatusTracker } from '../../src/sync/status.js';
import { WatcherHub } from '../../src/watch.js';
import { pushApplied, pushedMutations, pushFailed, pushResults } from './pushfake.js';

// Exercises the push path's two offline-burst defenses: compacting the
// never-pushed tail before pushing, and splitting an over-limit queue into
// within-limit push RPCs.

function patch(id: bigint, key: string, paths: string[]): Mutation {
	return create(MutationSchema, {
		clientId: 'c1',
		mutationId: id,
		payload: {
			case: 'crud',
			value: create(CrudMutationSchema, {
				entity: 'todo',
				key,
				op: CrudOp.PATCH,
				fieldMask: { paths },
			}),
		},
	});
}

function put(id: bigint, key: string): Mutation {
	return create(MutationSchema, {
		clientId: 'c1',
		mutationId: id,
		payload: {
			case: 'crud',
			value: create(CrudMutationSchema, { entity: 'todo', key, op: CrudOp.PUT }),
		},
	});
}

/** The workspace these specs drive. */
const WS = 'ws';

describe('coordinator push: compaction + chunking', () => {
	let kv: MemoryKVStore;
	let registry: EntityRegistry;
	let pushes: Mutation[][];

	beforeEach(() => {
		kv = new MemoryKVStore();
		registry = new EntityRegistry([ TodoSchema ]);
		pushes = [];
	});

	/** Records each push batch and acks every mutation as applied. */
	function transport(maxPerPush?: number): Transport {
		return createRouterTransport(({ service }) => {
			service(SyncService, {
				push(req) {
					pushes.push(pushedMutations(req));
					if (maxPerPush !== undefined && pushedMutations(req).length > maxPerPush) {
						// Match the engine: an over-limit push is an RPC error, not an ack.
						throw new Error(`push of ${pushedMutations(req).length} exceeds ${maxPerPush}`);
					}
					return pushResults(
						req,
						pushedMutations(req).map((m) => ({
							mutationId: m.mutationId,
							status: MutationStatus.APPLIED,
							seq: m.mutationId,
						})),
					);
				},
				pull() {
					return {
						patch: { changes: [], dropEntities: [] },
						nextCursor: new Uint8Array([ 1 ]),
						lastAckedMutationId: 0n,
						reset: false,
						hasMore: false,
						seq: 0n,
					};
				},
				// eslint-disable-next-line require-yield
				async *bootstrap() {
					throw new Error('bootstrap should not run with a seeded cursor');
				},
				query() {
					return { rows: [] };
				},
			});
		});
	}

	async function seed(mutations: Mutation[]): Promise<void> {
		const tx = await kv.write();
		for (const m of mutations) await enqueuePending(tx, WS, m);
		await tx.put(metaCursor(WS), new Uint8Array([ 1 ]));
		await tx.put(metaLastMutationId(WS), mutations[mutations.length - 1]!.mutationId.toString());
		await tx.commit();
	}

	function newCoordinator(t: Transport, opts: Partial<CoordinatorOptions> = {}): Coordinator {
		const kvMutex = opts.kvMutex ?? createKvMutex('test');
		const hub = new WatcherHub(kv, registry, kvMutex);
		const coord = new Coordinator({
			transport: t,
			kv,
			kvMutex,
			storageNames: storageNames(DEFAULT_STORAGE_PREFIX, 'acct'),
			clientId: 'c1',
			registry,
			mirrors: {},
			notify: (touched) => hub.notify(touched),
			status: new StatusTracker(),
			scopes: new Map([ [ WS, [] ] ]),
			streamEnabled: false,
			pollIntervalMs: 0,
			...opts,
		});
		return coord;
	}

	async function pendingIds(): Promise<bigint[]> {
		const tx = await kv.read();
		try {
			return (await readPending(tx, WS)).map((m) => m.mutationId);
		} finally {
			tx.release();
		}
	}

	it('compacts the never-pushed tail before pushing', async () => {
		// Five PATCHes to the same field of one key collapse to one mutation.
		await seed([
			patch(1n, 't1', [ 'title' ]),
			patch(2n, 't1', [ 'title' ]),
			patch(3n, 't1', [ 'title' ]),
			patch(4n, 't1', [ 'title' ]),
			patch(5n, 't1', [ 'title' ]),
		]);
		const coord = newCoordinator(transport());

		await coord.syncNow();

		// One push carrying a single compacted mutation; the queue ends at one id.
		expect(pushes).toHaveLength(1);
		expect(pushes[0]).toHaveLength(1);
		expect(await pendingIds()).toEqual([ 1n ]);

		await coord.stop();
	});

	it('splits an over-limit queue into within-limit push RPCs', async () => {
		// Five distinct keys → nothing compacts; a limit of 2 forces 3 pushes.
		await seed([ put(1n, 't1'), put(2n, 't2'), put(3n, 't3'), put(4n, 't4'), put(5n, 't5') ]);
		const coord = newCoordinator(transport(2), { maxMutationsPerPush: 2 });

		await coord.syncNow();

		expect(pushes.map((p) => p.length)).toEqual([ 2, 2, 1 ]);
		expect(pushes.flat().map((m) => m.mutationId)).toEqual([ 1n, 2n, 3n, 4n, 5n ]);

		await coord.stop();
	});

	/** Appends one mutation to the shared queue and bumps the id counter. */
	async function enqueueRaw(m: Mutation): Promise<void> {
		const tx = await kv.write();
		await enqueuePending(tx, WS, m);
		await tx.put(metaLastMutationId(WS), m.mutationId.toString());
		await tx.commit();
	}

	/** Acks every mutation; fails the first `failures` push RPCs after recording. */
	function flakyTransport(failures: number): Transport {
		let seen = 0;
		return createRouterTransport(({ service }) => {
			service(SyncService, {
				push(req) {
					pushes.push(pushedMutations(req));
					if (seen++ < failures) throw new Error('push response lost');
					return pushResults(
						req,
						pushedMutations(req).map((m) => ({
							mutationId: m.mutationId,
							status: MutationStatus.APPLIED,
							seq: m.mutationId,
						})),
					);
				},
				pull() {
					return {
						patch: { changes: [], dropEntities: [] },
						nextCursor: new Uint8Array([ 1 ]),
						lastAckedMutationId: 0n,
						reset: false,
						hasMore: false,
						seq: 0n,
					};
				},
				// eslint-disable-next-line require-yield
				async *bootstrap() {
					throw new Error('bootstrap should not run with a seeded cursor');
				},
				query() {
					return { rows: [] };
				},
			});
		});
	}

	it('a lost push response protects already-shipped ids from later compaction', async () => {
		// A push reaches the server and commits, but the response is lost. The id
		// must NOT become compactable again: a new same-key edit enqueued
		// afterwards must not be folded into a slot renumbered at/below the
		// server's watermark (which would vanish as ALREADY_APPLIED).
		await seed([ put(1n, 't1') ]);
		const coord = newCoordinator(flakyTransport(1));

		// First cycle: the push commits server-side but the reply is lost (throws).
		// The durable pushed-through watermark is advanced BEFORE the RPC, so id 1
		// stays protected even though the cycle ends in error.
		await expect(coord.syncNow()).rejects.toThrow();

		// A new edit to the same key lands in the queue.
		await enqueueRaw(patch(2n, 't1', [ 'title' ]));

		// Second cycle (transport now healthy): compaction must keep id 2 distinct
		// from the already-shipped id 1 — no fold, no renumber below the watermark.
		await coord.syncNow();

		expect(await pendingIds()).toEqual([ 1n, 2n ]);
		// The new edit actually reached the server on its own id; it was not lost.
		expect(pushes.at(-1)!.map((m) => m.mutationId)).toEqual([ 1n, 2n ]);

		await coord.stop();
	});

	it('keeps an unanswered push queued past a pull that acks it', async () => {
		// The push commits and the response is lost. The server's watermark now
		// covers the mutation, so the pull that follows would rebase it away as
		// acked — taking with it a rejection this client never saw. It stays
		// queued until a re-push says what became of it.
		await seed([ put(1n, 't1') ]);
		let pulls = 0;
		let seen = 0;
		const transport = createRouterTransport(({ service }) => {
			service(SyncService, {
				push(req) {
					pushes.push(pushedMutations(req));
					if (seen++ === 0) throw new Error('push response lost');
					return pushResults(
						req,
						pushedMutations(req).map((m) => ({
							mutationId: m.mutationId,
							status: MutationStatus.REJECTED,
							errorCode: 'conflict',
							errorMessage: 'stale version',
						})),
					);
				},
				pull() {
					pulls++;
					return {
						patch: { changes: [], dropEntities: [] },
						nextCursor: new Uint8Array([ 1 ]),
						lastAckedMutationId: 1n,
						reset: false,
						hasMore: false,
						seq: 0n,
					};
				},
				// eslint-disable-next-line require-yield
				async *bootstrap() {
					throw new Error('bootstrap should not run with a seeded cursor');
				},
				query() {
					return { rows: [] };
				},
			});
		});
		const rejections: RejectionInfo[] = [];
		const coord = newCoordinator(transport, {
			onMutationRejected: (info) => rejections.push(info),
		});

		// The cycle pulls even though the push failed, and the pull acks id 1 —
		// which must not retire it.
		await expect(coord.syncNow()).rejects.toThrow();
		expect(pulls).toBe(1);
		expect(await pendingIds()).toEqual([ 1n ]);

		// The re-push collects the verdict, and only then is it dropped.
		await coord.syncNow();
		expect(pushes).toHaveLength(2);
		expect(rejections.map((r) => r.errorCode)).toEqual([ 'conflict' ]);
		expect(await pendingIds()).toEqual([]);

		await coord.stop();
	});

	it('keeps an unanswered push queued past a reset that re-bootstraps the workspace', async () => {
		// The same lost response, but the pull that follows answers reset (a
		// cursor behind the GC horizon, say), so the cycle re-bootstraps. The
		// snapshot's watermark covers the mutation just as the pull's did, and
		// installing it must not retire the mutation either.
		await seed([ put(1n, 't1') ]);
		let pulls = 0;
		let bootstraps = 0;
		let seen = 0;
		const transport = createRouterTransport(({ service }) => {
			service(SyncService, {
				push(req) {
					pushes.push(pushedMutations(req));
					if (seen++ === 0) throw new Error('push response lost');
					return pushResults(
						req,
						pushedMutations(req).map((m) => ({
							mutationId: m.mutationId,
							status: MutationStatus.REJECTED,
							errorCode: 'conflict',
							errorMessage: 'stale version',
						})),
					);
				},
				pull() {
					pulls++;
					return {
						patch: { changes: [], dropEntities: [] },
						nextCursor: new Uint8Array([ 1 ]),
						lastAckedMutationId: 1n,
						reset: pulls === 1,
						hasMore: false,
						seq: 5n,
					};
				},
				async *bootstrap() {
					bootstraps++;
					yield create(BootstrapResponseSchema, {
						body: {
							case: 'begin',
							value: { cursor: new Uint8Array([ 2 ]), lastAckedMutationId: 1n, seq: 5n },
						},
					});
					yield create(BootstrapResponseSchema, { body: { case: 'end', value: {} } });
				},
				query() {
					return { rows: [] };
				},
			});
		});
		const rejections: RejectionInfo[] = [];
		const coord = newCoordinator(transport, {
			onMutationRejected: (info) => rejections.push(info),
		});

		await expect(coord.syncNow()).rejects.toThrow();
		expect(pulls).toBe(1);
		expect(bootstraps).toBe(1);
		expect(await pendingIds()).toEqual([ 1n ]);

		// The re-push collects the verdict, and only then is it dropped.
		await coord.syncNow();
		expect(pushes).toHaveLength(2);
		expect(rejections.map((r) => r.errorCode)).toEqual([ 'conflict' ]);
		expect(await pendingIds()).toEqual([]);

		await coord.stop();
	});

	/**
	 * Answers the first push with a lost response and every later one with
	 * `status`; every pull reports the watermark past the pushed ids.
	 */
	function lostOnceTransport(status: MutationStatus): Transport {
		let watermark = 0n;
		let seen = 0;
		return createRouterTransport(({ service }) => {
			service(SyncService, {
				push(req) {
					pushes.push(pushedMutations(req));
					const results = pushedMutations(req).map((m) => {
						if (m.mutationId > watermark) watermark = m.mutationId;
						return status === MutationStatus.REJECTED
							? { mutationId: m.mutationId, status, errorCode: 'conflict', errorMessage: 'stale version' }
							: { mutationId: m.mutationId, status, seq: m.mutationId };
					});
					if (seen++ === 0) throw new Error('push response lost');
					return pushResults(req, results);
				},
				pull() {
					return {
						patch: { changes: [], dropEntities: [] },
						nextCursor: new Uint8Array([ 1 ]),
						lastAckedMutationId: watermark,
						reset: false,
						hasMore: false,
						seq: 0n,
					};
				},
				// eslint-disable-next-line require-yield
				async *bootstrap() {
					throw new Error('bootstrap should not run with a seeded cursor');
				},
				query() {
					return { rows: [] };
				},
			});
		});
	}

	it("keeps an unanswered push queued past a peer tab's page that acks it", async () => {
		// Tab A's push response is lost. Tab B — the stream leader, which applies
		// live pages through applyServerPage — rebases a page whose watermark
		// covers the mutation before tab A re-pushes. Tab B never pushed, so only
		// the store can tell it to keep the mutation for tab A's re-push.
		await seed([ put(1n, 't1') ]);
		const kvMutex = createKvMutex('test');
		const transport = lostOnceTransport(MutationStatus.REJECTED);
		const rejections: RejectionInfo[] = [];
		const onMutationRejected = (info: RejectionInfo) => rejections.push(info);
		const tabA = newCoordinator(transport, { kvMutex, onMutationRejected });
		const tabB = newCoordinator(transport, { kvMutex, onMutationRejected });

		await expect(tabA.syncNow()).rejects.toThrow();
		expect(await pendingIds()).toEqual([ 1n ]);

		// Node declines stream leadership, so the page goes in through the
		// method the stream loop calls.
		const page = create(PullResponseSchema, {
			patch: { changes: [] },
			nextCursor: new Uint8Array([ 2 ]),
			lastAckedMutationId: 1n,
			seq: 5n,
		});
		await (tabB as unknown as {
			applyServerPage(ws: string, p: PullResponse, subs: never[]): Promise<unknown>;
		}).applyServerPage(WS, page, []);
		expect(await pendingIds()).toEqual([ 1n ]);

		await tabA.syncNow();
		expect(rejections.map((r) => r.mutationId)).toEqual([ 1n ]);
		expect(await pendingIds()).toEqual([]);

		await tabA.stop();
		await tabB.stop();
	});

	it('drops the mutations of an unanswered push once a re-push answers for them', async () => {
		// The hold must end: once the re-push collects the verdicts, the next
		// rebase drops what the watermark covers, and the mark is gone.
		await seed([ put(1n, 't1'), put(2n, 't2') ]);
		const coord = newCoordinator(lostOnceTransport(MutationStatus.APPLIED));

		await expect(coord.syncNow()).rejects.toThrow();
		expect(await pendingIds()).toEqual([ 1n, 2n ]);

		await coord.syncNow();
		expect(pushes.map((p) => p.map((m) => m.mutationId))).toEqual([ [ 1n, 2n ], [ 1n, 2n ] ]);
		expect(await pendingIds()).toEqual([]);
		const tx = await kv.read();
		try {
			expect(await tx.get(metaUnresolvedPush(WS))).toBeUndefined();
		} finally {
			tx.release();
		}

		await coord.stop();
	});

	it("a peer tab's push protects ids from this tab's compaction (shared watermark)", async () => {
		// Two tabs share the KV store (and the kv mutex). Tab B pushes id 1, which
		// advances the durable, shared watermark. Tab A — a separate Coordinator
		// with its own in-memory state — must observe that watermark and refuse to
		// fold a later same-key edit into the id the server already saw.
		await seed([ put(1n, 't1') ]);
		const mutex = createKvMutex('test');

		const tabB = newCoordinator(transport(), { kvMutex: mutex });
		await tabB.syncNow(); // pushes id 1, raising the workspace's pushed-through watermark
		await tabB.stop();

		await enqueueRaw(patch(2n, 't1', [ 'title' ]));

		const tabA = newCoordinator(transport(), { kvMutex: mutex });
		await tabA.syncNow();
		expect(await pendingIds()).toEqual([ 1n, 2n ]);

		await tabA.stop();
	});

	it('seeds the pushed-through watermark of a workspace added after start()', async () => {
		// A store written before the watermark key existed carries queues that may
		// already have reached the server. start() seeds the mark for the
		// workspaces it finds; one added mid-session must get the same seed, or
		// compaction reads the absent mark as 0 and renumbers ids the server would
		// then drop as ALREADY_APPLIED.
		await seed([ patch(1n, 't1', [ 'title' ]), patch(2n, 't1', [ 'completed' ]) ]);
		const coord = newCoordinator(transport(), { scopes: new Map() });
		await coord.start();

		coord.addWorkspace(WS, []);
		await coord.syncNow();

		// Neither folded nor renumbered: both ids ship as they stand.
		expect(await pendingIds()).toEqual([ 1n, 2n ]);
		expect(pushes.at(-1)!.map((m) => m.mutationId)).toEqual([ 1n, 2n ]);

		await coord.stop();
	});

	it('does not recompact mutations once pushed', async () => {
		// The pull acks nothing, so the compacted mutation stays queued. A second
		// cycle must NOT compact it again (it has now been pushed).
		await seed([ patch(1n, 't1', [ 'title' ]), patch(2n, 't1', [ 'title' ]) ]);
		const coord = newCoordinator(transport());

		await coord.syncNow(); // compacts 2 → 1, pushes it, advances pushedThrough
		await coord.syncNow(); // pushedThrough now covers id 1: nothing to compact

		// Both cycles re-push the same single (idempotent) mutation; never the
		// original two.
		expect(pushes.map((p) => p.length)).toEqual([ 1, 1 ]);
		expect(await pendingIds()).toEqual([ 1n ]);

		await coord.stop();
	});
});
