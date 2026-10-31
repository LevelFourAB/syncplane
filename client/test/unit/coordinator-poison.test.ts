import { create } from '@bufbuild/protobuf';
import { Code, ConnectError, createRouterTransport, type Transport } from '@connectrpc/connect';
import { beforeEach, describe, expect, it } from 'vitest';

import { CodeInternal, CodeMutationFailed, CodePoisonDropped } from '../../src/codes.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { type Mutation, MutationSchema } from '../../src/gen/syncplane/v1/mutation_pb.js';
import {
	MutationStatus,
	type PushRequest,
	SyncService,
} from '../../src/gen/syncplane/v1/service_pb.js';
import { metaCursor, metaLastMutationId, metaPushedThrough, pendingKey } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { IndexEntry, KVWrite } from '../../src/kv/store.js';
import { buildPut, EntityRegistry } from '../../src/mutations.js';
import { ChainMutex, createKvMutex } from '../../src/mutex.js';
import { enqueuePending, readPending } from '../../src/optimistic/queue.js';
import { applyMutationLocally, dropAndResequencePending } from '../../src/optimistic/rebase.js';
import { DEFAULT_STORAGE_PREFIX, storageNames } from '../../src/storagenames.js';
import { Coordinator, type RejectionInfo } from '../../src/sync/coordinator.js';
import { StatusTracker } from '../../src/sync/status.js';
import { TouchSet } from '../../src/touch.js';
import { WatcherHub } from '../../src/watch.js';
import { pushApplied, pushedMutations, pushFailed, pushResults } from './pushfake.js';

/** The workspace these specs drive. */
const WS = 'ws';

/** The batch verdict the engine sends when a custom mutator returns a plain error. */
const MUTATOR_FAILED = `workspace "${WS}": mutator "explode" failed on mutation 1`;

// Drives the Coordinator's push path against a fake server to exercise the
// poison-queue backstop: a mutation the server keeps failing — as an RPC error
// or as a workspace-scoped batch failure — must not wedge the queue forever.

/** A SyncService transport with the given push(); pull/bootstrap are no-ops so
 * a seeded cursor keeps sync from bootstrapping. */
function syncTransport(
	push: (
		req: PushRequest,
	) => ReturnType<typeof pushResults> | Promise<ReturnType<typeof pushResults>>,
): Transport {
	return createRouterTransport(({ service }) => {
		service(SyncService, {
			push,
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
				throw new Error('bootstrap should not be called with a seeded cursor');
			},
			query() {
				return { rows: [] };
			},
		});
	});
}

/** A transport whose push() runs the given hook and then answers emptily. */
function fakeTransport(onPush: () => void): Transport {
	return syncTransport((req) => {
		onPush();
		return pushResults(req, []);
	});
}

/** A transport whose push() fails the whole batch with a workspace-scoped code. */
function batchFailTransport(errorCode: string, errorMessage: () => string): Transport {
	return syncTransport((req) => pushFailed(req, errorCode, errorMessage()));
}

/** A transaction that calls `onDel` before each delete and `onCommit` after a commit. */
class HookedWrite implements KVWrite {
	constructor(
		private readonly inner: KVWrite,
		private readonly onDel: (key: string) => void,
		private readonly onCommit: () => void,
	) {}

	get(key: string): Promise<unknown | undefined> {
		return this.inner.get(key);
	}

	getMany(keys: readonly string[]): Promise<(unknown | undefined)[]> {
		return this.inner.getMany(keys);
	}

	has(key: string): Promise<boolean> {
		return this.inner.has(key);
	}

	scan(prefix: string): AsyncIterable<[key: string, value: unknown]> {
		return this.inner.scan(prefix);
	}

	scanRange(lo?: string, hi?: string): AsyncIterable<[key: string, value: unknown]> {
		return this.inner.scanRange(lo, hi);
	}

	indexScan(lo?: string, hi?: string, direction?: 'asc' | 'desc'): AsyncIterable<string> {
		return this.inner.indexScan(lo, hi, direction);
	}

	indexScanRows(lo?: string, hi?: string, direction?: 'asc' | 'desc'): AsyncIterable<IndexEntry> {
		return this.inner.indexScanRows(lo, hi, direction);
	}

	indexCount(lo?: string, hi?: string): Promise<number> {
		return this.inner.indexCount(lo, hi);
	}

	indexSeek(
		lo?: string,
		hi?: string,
		direction?: 'asc' | 'desc',
	): Promise<{ position: string; key: string } | undefined> {
		return this.inner.indexSeek(lo, hi, direction);
	}

	seek(lo?: string, hi?: string): Promise<string | undefined> {
		return this.inner.seek(lo, hi);
	}

	put(key: string, value: unknown): Promise<void> {
		return this.inner.put(key, value);
	}

	async del(key: string): Promise<void> {
		this.onDel(key);
		await this.inner.del(key);
	}

	async commit(): Promise<void> {
		await this.inner.commit();
		this.onCommit();
	}

	release(): void {
		this.inner.release();
	}
}

/**
 * A store whose transaction opened straight AFTER the one that deleted `key`
 * committed fails once — the window a dead-letter would leave open if it
 * dropped the mutation in one transaction and renumbered the survivors in the
 * next.
 */
class FailAfterDelete extends MemoryKVStore {
	private armed = false;

	constructor(private readonly key: string) {
		super();
	}

	override async write(): Promise<KVWrite> {
		if (this.armed) {
			this.armed = false;
			throw new Error('kv unavailable');
		}
		let deleted = false;
		return new HookedWrite(
			await super.write(),
			(k) => {
				if (k === this.key) deleted = true;
			},
			() => {
				if (deleted) this.armed = true;
			},
		);
	}
}

/** A store whose first delete of `key` throws, failing the dead-letter's own write. */
class FailOnDelete extends MemoryKVStore {
	private failed = false;

	constructor(private readonly key: string) {
		super();
	}

	override async write(): Promise<KVWrite> {
		return new HookedWrite(
			await super.write(),
			(k) => {
				if (k !== this.key || this.failed) return;
				this.failed = true;
				throw new Error('kv unavailable');
			},
			() => {},
		);
	}
}

describe('coordinator poison backstop', () => {
	let kv: MemoryKVStore;
	let registry: EntityRegistry;
	let rejections: RejectionInfo[];

	beforeEach(async () => {
		kv = new MemoryKVStore();
		registry = new EntityRegistry([ TodoSchema ]);
		rejections = [];
		await seed();
	});

	/**
	 * Seeds `kv` with one pending mutation plus its optimistic overlay, a cursor
	 * (so the first sync skips bootstrap), and the id counter. A spec that needs
	 * a different store assigns `kv` and calls this again.
	 */
	async function seed(): Promise<void> {
		const def = registry.bySchema(TodoSchema);
		const crud = buildPut(def, TodoSchema, create(TodoSchema, { id: 't1', title: 'poison' }));
		const mutation = create(MutationSchema, {
			clientId: 'c1',
			mutationId: 1n,
			payload: { case: 'crud', value: crud },
		});
		const tx = await kv.write();
		await enqueuePending(tx, WS, mutation);
		await applyMutationLocally(tx, registry, {}, WS, mutation, new TouchSet());
		await tx.put(metaCursor(WS), new Uint8Array([ 1 ]));
		await tx.put(metaLastMutationId(WS), '1');
		await tx.commit();
	}

	/** Queues further pending mutations behind the seeded head. */
	async function seedTail(ids: bigint[]): Promise<void> {
		const def = registry.bySchema(TodoSchema);
		const tx = await kv.write();
		for (const id of ids) {
			const crud = buildPut(def, TodoSchema, create(TodoSchema, { id: `t${id}`, title: 'keep' }));
			const m = create(MutationSchema, {
				clientId: 'c1',
				mutationId: id,
				payload: { case: 'crud', value: crud },
			});
			await enqueuePending(tx, WS, m);
			await applyMutationLocally(tx, registry, {}, WS, m, new TouchSet());
		}
		await tx.put(metaLastMutationId(WS), String(ids[ids.length - 1]));
		await tx.commit();
	}

	function newCoordinator(transport: Transport, poisonThreshold: number): Coordinator {
		const kvMutex = createKvMutex('test');
		const hub = new WatcherHub(kv, registry, kvMutex);
		return new Coordinator({
			transport,
			kv,
			kvMutex,
			storageNames: storageNames(DEFAULT_STORAGE_PREFIX, 'acct'),
			scopes: new Map([ [ WS, [] ] ]),
			clientId: 'c1',
			registry,
			mirrors: {},
			notify: (touched) => hub.notify(touched),
			status: new StatusTracker(),
			onMutationRejected: (info) => rejections.push(info),
			poisonThreshold,
			streamEnabled: false,
			pollIntervalMs: 0,
		});
	}

	/** Runs syncNow, swallowing the expected push error so we can step cycles. */
	async function cycle(coord: Coordinator): Promise<void> {
		await coord.syncNow().catch(() => undefined);
	}

	it('drops a head the server keeps deterministically rejecting', async () => {
		const coord = newCoordinator(
			fakeTransport(() => {
				throw new ConnectError('unknown entity', Code.InvalidArgument);
			}),
			3,
		);

		// First two cycles only count failures; the head is not dropped yet.
		await cycle(coord);
		await cycle(coord);
		expect(rejections).toHaveLength(0);
		expect(await pendingIds()).toEqual([ 1n ]);

		// The third deterministic failure crosses the threshold: dead-letter it.
		await cycle(coord);
		expect(rejections).toEqual([
			expect.objectContaining({ mutationId: 1n, errorCode: CodePoisonDropped }),
		]);
		expect(await pendingIds()).toEqual([]);
		// Nothing is left to renumber, but the counter still named the dropped id.
		// Left there, the next local write would mint an id past the watermark the
		// server stopped at and gap the workspace all over again.
		const tx = await kv.read();
		try {
			expect(await tx.get(metaLastMutationId(WS))).toBe('0');
			expect(await tx.get(metaPushedThrough(WS))).toBe('0');
		} finally {
			tx.release();
		}

		await coord.stop();
	});

	it('drops a head whose batch the server keeps failing the same way', async () => {
		// mutation_failed is what a custom mutator returning a plain error
		// instead of Reject(...) produces. It is not an RPC error, so nothing
		// about it is deterministic on its face — but a head that fails alone
		// with the same verdict every time is one nothing can drain.
		const coord = newCoordinator(
			batchFailTransport(CodeMutationFailed, () => MUTATOR_FAILED),
			3,
		);

		await cycle(coord);
		await cycle(coord);
		expect(rejections).toHaveLength(0);
		expect(await pendingIds()).toEqual([ 1n ]);

		await cycle(coord);
		expect(rejections).toEqual([
			expect.objectContaining({
				mutationId: 1n,
				errorCode: CodePoisonDropped,
				errorMessage: MUTATOR_FAILED,
			}),
		]);
		expect(await pendingIds()).toEqual([]);

		await coord.stop();
	});

	it('restarts the count when the batch failure changes', async () => {
		// A mutator that fails differently each time may still succeed: only a
		// repeat of the same verdict is evidence that retrying cannot help.
		let attempt = 0;
		const coord = newCoordinator(
			batchFailTransport(CodeMutationFailed, () => `attempt ${++attempt} lost its connection`),
			3,
		);

		for (let i = 0; i < 6; i++) await cycle(coord);

		expect(rejections).toHaveLength(0);
		expect(await pendingIds()).toEqual([ 1n ]);

		await coord.stop();
	});

	describe('during a server outage', () => {
		// The engine words every infrastructure failure as a fixed phase and logs
		// the cause, so an outage repeats one verdict for as long as it lasts.
		for (
			const [ name, message ] of [
				[ 'storage', `workspace "${WS}": applying the batch failed` ],
				[ 'permission hook', `workspace "${WS}": resolving access failed` ],
			] as const
		) {
			it(`keeps the head through a ${name} outage and pushes it once the server recovers`, async () => {
				let pushes = 0;
				let watermark = 0n;
				const applied: bigint[] = [];
				const coord = newCoordinator(
					createRouterTransport(({ service }) => {
						service(SyncService, {
							push(req) {
								if (++pushes <= 5) return pushFailed(req, CodeInternal, message);
								for (const m of pushedMutations(req)) {
									applied.push(m.mutationId);
									if (m.mutationId > watermark) watermark = m.mutationId;
								}
								return pushApplied(req);
							},
							pull() {
								return {
									patch: { changes: [], dropEntities: [] },
									nextCursor: new Uint8Array([ 1 ]),
									lastAckedMutationId: watermark,
								};
							},
						});
					}),
					5,
				);

				for (let i = 0; i < 6; i++) await cycle(coord);

				expect(rejections).toEqual([]);
				expect(applied).toEqual([ 1n ]);
				expect(await pendingIds()).toEqual([]);

				await coord.stop();
			});
		}

		it('restarts the count of a failing mutator', async () => {
			// Two mutator failures, an outage, then two more: four in all, but
			// never three in a row.
			const verdicts = [ CodeMutationFailed, CodeMutationFailed, CodeInternal ];
			let attempt = 0;
			const coord = newCoordinator(
				syncTransport((req) => {
					const code = verdicts[attempt++] ?? CodeMutationFailed;
					return pushFailed(req, code, code === CodeInternal ? 'outage' : MUTATOR_FAILED);
				}),
				3,
			);

			for (let i = 0; i < 5; i++) await cycle(coord);
			expect(rejections).toHaveLength(0);
			expect(await pendingIds()).toEqual([ 1n ]);

			await cycle(coord);
			expect(rejections).toEqual([
				expect.objectContaining({ mutationId: 1n, errorCode: CodePoisonDropped }),
			]);

			await coord.stop();
		});
	});

	it('never drops on transient (non-deterministic) errors', async () => {
		const coord = newCoordinator(
			fakeTransport(() => {
				throw new ConnectError('server down', Code.Unavailable);
			}),
			3,
		);

		for (let i = 0; i < 6; i++) await cycle(coord);

		// The queue retries indefinitely on transient errors; it never gives up.
		expect(rejections).toHaveLength(0);
		expect(await pendingIds()).toEqual([ 1n ]);

		await coord.stop();
	});

	it('honors poisonThreshold <= 0 by retrying forever', async () => {
		const coord = newCoordinator(
			fakeTransport(() => {
				throw new ConnectError('nope', Code.InvalidArgument);
			}),
			0,
		);

		for (let i = 0; i < 6; i++) await cycle(coord);

		expect(rejections).toHaveLength(0);
		expect(await pendingIds()).toEqual([ 1n ]);

		await coord.stop();
	});

	it('keeps rejections from an answered chunk when a later chunk RPC fails', async () => {
		// Add a second pending mutation so the queue splits into two single-mutation
		// chunks (maxMutationsPerPush=1 below). Chunk 1 (id 1) is rejected by the
		// server; chunk 2 (id 2) then dies on a network error.
		const def = registry.bySchema(TodoSchema);
		const crud = buildPut(def, TodoSchema, create(TodoSchema, { id: 't2', title: 'second' }));
		const m2 = create(MutationSchema, {
			clientId: 'c1',
			mutationId: 2n,
			payload: { case: 'crud', value: crud },
		});
		const seedTx = await kv.write();
		await enqueuePending(seedTx, WS, m2);
		await applyMutationLocally(seedTx, registry, {}, WS, m2, new TouchSet());
		await seedTx.put(metaLastMutationId(WS), '2');
		await seedTx.commit();

		let calls = 0;
		const transport = createRouterTransport(({ service }) => {
			service(SyncService, {
				push(req) {
					calls += 1;
					if (calls === 1) {
						// Chunk 1: the server rejects mutation 1 (and bumps its
						// watermark, so a retry would ack it as ALREADY_APPLIED).
						// A rejection is per-MUTATION — the batch itself succeeded,
						// so it rides inside the workspace's results, not on it.
						return pushResults(req, [ {
							mutationId: 1n,
							status: MutationStatus.REJECTED,
							errorCode: 'BAD',
							errorMessage: 'no good',
						} ]);
					}
					// Chunk 2: a flaky connection drops the RPC mid-cycle.
					throw new ConnectError('connection reset', Code.Unavailable);
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
					throw new Error('bootstrap should not be called with a seeded cursor');
				},
				query() {
					return { rows: [] };
				},
			});
		});

		const kvMutex = createKvMutex('test');
		const hub = new WatcherHub(kv, registry, kvMutex);
		const coord = new Coordinator({
			transport,
			kv,
			kvMutex,
			storageNames: storageNames(DEFAULT_STORAGE_PREFIX, 'acct'),
			scopes: new Map([ [ WS, [] ] ]),
			clientId: 'c1',
			registry,
			mirrors: {},
			notify: (touched) => hub.notify(touched),
			status: new StatusTracker(),
			onMutationRejected: (info) => rejections.push(info),
			poisonThreshold: 3,
			streamEnabled: false,
			pollIntervalMs: 0,
			maxMutationsPerPush: 1,
		});

		await cycle(coord);

		// The rejection chunk 1 reported must NOT be dropped by chunk 2's throw:
		// it fired onMutationRejected and rolled the doomed mutation out of the
		// queue. Mutation 2 is untouched and will retry next cycle.
		expect(calls).toBe(2);
		expect(rejections).toEqual([
			expect.objectContaining({ mutationId: 1n, errorCode: 'BAD', errorMessage: 'no good' }),
		]);
		expect(await pendingIds()).toEqual([ 2n ]);

		await coord.stop();
	});

	it('renumbers the tail down over a dropped head', async () => {
		// A dead-letter discards an id the server never acked, so its watermark
		// stays below the tail. Left at their old ids, the survivors would gap
		// past it and the workspace would never push again.
		await seedTail([ 2n, 3n ]);

		const coord = newCoordinator(
			fakeTransport(() => {
				throw new ConnectError('unknown entity', Code.InvalidArgument);
			}),
			3,
		);

		// Cycle 1 pushes the whole queue, 2 and 3 isolate the head, and the third
		// confirmed failure dead-letters it.
		await cycle(coord);
		await cycle(coord);
		await cycle(coord);

		expect(rejections).toEqual([
			expect.objectContaining({ mutationId: 1n, errorCode: CodePoisonDropped }),
		]);
		// The survivors close the hole rather than keeping ids 2 and 3.
		expect(await pendingIds()).toEqual([ 1n, 2n ]);
		const tx = await kv.read();
		try {
			expect(await tx.get(metaLastMutationId(WS))).toBe('2');
			expect(await tx.get(metaPushedThrough(WS))).toBe('0');
		} finally {
			tx.release();
		}

		await coord.stop();
	});

	it('drops and renumbers in one transaction', async () => {
		// The drop is what makes the hole and the renumber is what closes it. A
		// store that dies between them would leave the queue numbered above a
		// watermark no ack can ever reach: every push refused for the gap, and the
		// gap recovery is this same renumber. One transaction is the only
		// arrangement in which the hole cannot outlive the drop.
		kv = new FailAfterDelete(pendingKey(WS, 1n));
		await seed();
		await seedTail([ 2n, 3n ]);

		const coord = newCoordinator(
			fakeTransport(() => {
				throw new ConnectError('unknown entity', Code.InvalidArgument);
			}),
			3,
		);

		await cycle(coord);
		await cycle(coord);
		await cycle(coord);

		expect(await pendingIds()).toEqual([ 1n, 2n ]);
		expect(rejections).toEqual([
			expect.objectContaining({ mutationId: 1n, errorCode: CodePoisonDropped }),
		]);

		await coord.stop();
	});

	it('keeps the mutation queued when the dead-letter cannot commit', async () => {
		// The other half of the same atomicity: a drop that fails writes nothing,
		// so the head stays queued with the survivors behind it and the backstop
		// simply counts up to the threshold again.
		kv = new FailOnDelete(pendingKey(WS, 1n));
		await seed();
		await seedTail([ 2n, 3n ]);

		const coord = newCoordinator(
			fakeTransport(() => {
				throw new ConnectError('unknown entity', Code.InvalidArgument);
			}),
			3,
		);

		await cycle(coord);
		await cycle(coord);
		await cycle(coord);

		expect(rejections).toHaveLength(0);
		expect(await pendingIds()).toEqual([ 1n, 2n, 3n ]);

		// The failure was the store's, not the mutation's: the next run of the
		// threshold dead-letters it for real.
		await cycle(coord);
		await cycle(coord);
		await cycle(coord);

		expect(rejections).toEqual([
			expect.objectContaining({ mutationId: 1n, errorCode: CodePoisonDropped }),
		]);
		expect(await pendingIds()).toEqual([ 1n, 2n ]);

		await coord.stop();
	});

	describe('with a peer tab pushing the same queue', () => {
		// Every tab that holds a workspace pushes the one shared queue, so a
		// failed push in this tab proves nothing about a peer's push of the same
		// head. Node has no shared lock manager: two coordinators over one store
		// and one mutex stand in for the two tabs.
		const kvMutex = new ChainMutex();

		function tab(transport: Transport, onRejected: (info: RejectionInfo) => void): Coordinator {
			return new Coordinator({
				transport,
				kv,
				kvMutex,
				storageNames: storageNames(DEFAULT_STORAGE_PREFIX, 'acct'),
				scopes: new Map([ [ WS, [] ] ]),
				clientId: 'c1',
				registry,
				mirrors: {},
				notify: async () => {},
				status: new StatusTracker(),
				onMutationRejected: onRejected,
				poisonThreshold: 2,
				streamEnabled: false,
				pollIntervalMs: 0,
			});
		}

		/** A local write as SyncClient makes one: mint counter+1, enqueue, advance the counter. */
		async function localWrite(title: string): Promise<bigint> {
			const def = registry.bySchema(TodoSchema);
			const tx = await kv.write();
			const id = BigInt(((await tx.get(metaLastMutationId(WS))) as string | undefined) ?? '0') + 1n;
			await enqueuePending(
				tx,
				WS,
				create(MutationSchema, {
					clientId: 'c1',
					mutationId: id,
					payload: {
						case: 'crud',
						value: buildPut(def, TodoSchema, create(TodoSchema, { id: title, title })),
					},
				}),
			);
			await tx.put(metaLastMutationId(WS), id.toString());
			await tx.commit();
			return id;
		}

		/**
		 * A server with a per-client watermark that applies in order and records
		 * what it applied. Its first two pushes fail like a failing mutator; the
		 * second runs `meanwhile` first, the window in which a peer's push lands.
		 */
		function watermarkServer(meanwhile: (server: { landHead(): Promise<void> }) => Promise<void>) {
			let watermark = 0n;
			const applied = new Map<bigint, string>();
			let pushNo = 0;
			const apply = (m: Mutation) => {
				if (m.mutationId <= watermark) {
					return { mutationId: m.mutationId, status: MutationStatus.ALREADY_APPLIED };
				}
				watermark = m.mutationId;
				applied.set(m.mutationId, (m.payload.value as { key: string }).key);
				return { mutationId: m.mutationId, status: MutationStatus.APPLIED, seq: m.mutationId };
			};
			const server = {
				/** Applies the queue head as a peer's request would. */
				async landHead() {
					apply((await readQueue())[0]!);
				},
			};
			const transport = createRouterTransport(({ service }) => {
				service(SyncService, {
					async push(req) {
						pushNo++;
						if (pushNo === 1) return pushFailed(req, CodeMutationFailed, MUTATOR_FAILED);
						if (pushNo === 2) {
							await meanwhile(server);
							return pushFailed(req, CodeMutationFailed, MUTATOR_FAILED);
						}
						return pushResults(req, pushedMutations(req).map(apply));
					},
					pull() {
						return {
							patch: { changes: [], dropEntities: [] },
							nextCursor: new Uint8Array([ 1 ]),
							lastAckedMutationId: watermark,
						};
					},
				});
			});
			return { transport, applied };
		}

		it('keeps a head a peer tab pushed before the drop, and mints the next id above it', async () => {
			let peer: Coordinator | undefined;
			// The failure ends while this tab's isolated retry is in flight: the
			// peer's whole cycle lands first, then the retry is answered with the
			// failure it hit.
			const { transport, applied } = watermarkServer(() => peer!.syncNow());
			const coord = tab(transport, (info) => rejections.push(info));
			peer = tab(transport, () => {});

			await cycle(coord); // push 1 fails and counts
			await cycle(coord); // push 2: the peer lands the head, then this tab's fails

			expect(applied.get(1n)).toBe('t1');
			expect(rejections).toHaveLength(0);

			expect(await localWrite('second')).toBe(2n);
			await cycle(coord);
			expect(applied.get(2n)).toBe('second');

			await coord.stop();
			await peer.stop();
		});

		it('keeps a head a peer tab pushed but never pulled for', async () => {
			// The queue still holds the head when this tab's push fails: only this
			// tab's own pull can learn that the server took it.
			const { transport, applied } = watermarkServer((server) => server.landHead());
			const coord = tab(transport, (info) => rejections.push(info));

			await cycle(coord);
			await cycle(coord);

			expect(applied.get(1n)).toBe('t1');
			expect(rejections).toHaveLength(0);
			expect(await readQueue()).toHaveLength(0);
			expect(await localWrite('second')).toBe(2n);

			await coord.stop();
		});

		it('leaves the queue alone when a peer tab already dropped the head and renumbered its tail', async () => {
			await seedTail([ 2n ]);
			let pushNo = 0;
			const coord = tab(
				syncTransport(async (req) => {
					pushNo++;
					if (pushNo === 2) {
						// The peer dead-letters the same head first: id 1 now names
						// the mutation that was queued at 2.
						const head = (await readQueue())[0]!;
						await kvMutex.run(() => dropAndResequencePending(kv, registry, {}, WS, head));
					}
					return pushFailed(req, CodeMutationFailed, MUTATOR_FAILED);
				}),
				(info) => rejections.push(info),
			);

			await cycle(coord); // push 1 fails and counts
			await cycle(coord); // push 2 fails again; the peer dropped the head meanwhile

			expect(rejections).toHaveLength(0);
			const queue = await readQueue();
			expect(queue.map((m) => [ m.mutationId, (m.payload.value as { key: string }).key ])).toEqual([
				[ 1n, 't2' ],
			]);
			const tx = await kv.read();
			try {
				expect(await tx.get(metaLastMutationId(WS))).toBe('1');
			} finally {
				tx.release();
			}

			await coord.stop();
		});
	});

	async function readQueue(): Promise<Mutation[]> {
		const tx = await kv.read();
		try {
			return await readPending(tx, WS);
		} finally {
			tx.release();
		}
	}

	async function pendingIds(): Promise<bigint[]> {
		const tx = await kv.read();
		try {
			return (await readPending(tx, WS)).map((m) => m.mutationId);
		} finally {
			tx.release();
		}
	}
});
