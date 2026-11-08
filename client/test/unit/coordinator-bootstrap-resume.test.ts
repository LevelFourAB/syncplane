import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { Code, ConnectError, createRouterTransport, type Transport } from '@connectrpc/connect';
import { describe, expect, it } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { MutationSchema } from '../../src/gen/syncplane/v1/mutation_pb.js';
import {
	type BootstrapRequest,
	BootstrapResponseSchema,
	MutationStatus,
	SyncService,
} from '../../src/gen/syncplane/v1/service_pb.js';
import {
	confirmedKey,
	metaBootstrapGen,
	metaBootstrapResume,
	metaCursor,
	metaLastMutationId,
	stagingPrefix,
	stagingWsRoot,
} from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { buildPatch, EntityRegistry } from '../../src/mutations.js';
import { ChainMutex, createKvMutex, type Mutex } from '../../src/mutex.js';
import { enqueuePending, readPending } from '../../src/optimistic/queue.js';
import { applyMutationLocally } from '../../src/optimistic/rebase.js';
import { readMergedRow, scanMergedRows } from '../../src/read.js';
import { DEFAULT_STORAGE_PREFIX, storageNames } from '../../src/storagenames.js';
import { Coordinator } from '../../src/sync/coordinator.js';
import { StatusTracker } from '../../src/sync/status.js';
import { TouchSet } from '../../src/touch.js';
import { WatcherHub } from '../../src/watch.js';
import { pushedMutations, pushResults } from './pushfake.js';

// Resuming an interrupted bootstrap. What every spec here turns on is that the
// client keeps what an attempt delivered — staged rows and the tokens that
// describe them — and hands the tokens back, so the second attempt downloads
// only the rest. The server's half is covered by the Go suite; this drives the
// client against a server that says what those specs prove it says.

const WS = 'ws';
const registry = new EntityRegistry([ TodoSchema ]);

function row(id: string, version: bigint) {
	return {
		entity: 'todo',
		key: id,
		version,
		body: { case: 'value' as const, value: toBinary(TodoSchema, create(TodoSchema, { id })) },
	};
}

interface Fake {
	transport: Transport;
	/** Every Bootstrap request the client made, in order. */
	requests: BootstrapRequest[];
}

/**
 * A server whose first Bootstrap attempt drops after `dropAfter` chunks and
 * whose second serves the rest. Chunks carry a resume token naming their index,
 * so a resumed attempt can pick up from the token the client hands back.
 *
 * `insteadServe` makes it answer a resume with a FRESH snapshot instead — the
 * server-side refusal (changed scope, GC'd cursor, other store) — which is the
 * case the client must not merge into what it already staged.
 */
function resumableTransport(opts: {
	chunks: ReturnType<typeof row>[][];
	dropAfter: number;
	/** Serve these instead, as a fresh snapshot, rather than honouring a resume. */
	insteadServe?: ReturnType<typeof row>[][];
	/** Pulls answered with reset, to send a workspace holding a cursor back through bootstrap. */
	resetPulls?: number;
}): Fake {
	const requests: BootstrapRequest[] = [];
	let pulls = 0;
	const transport = createRouterTransport(({ service }) => {
		service(SyncService, {
			async *bootstrap(req) {
				requests.push(req);
				const resume = opts.insteadServe === undefined ? req.resume : undefined;
				const chunks = req.resume !== undefined && opts.insteadServe !== undefined
					? opts.insteadServe
					: opts.chunks;
				const from = resume === undefined ? 0 : resume.token[0]!;
				yield create(BootstrapResponseSchema, {
					body: {
						case: 'begin',
						value: {
							cursor: resume?.cursor ?? new Uint8Array([ 9 ]),
							lastAckedMutationId: 0n,
							seq: 7n,
							resumed: resume !== undefined,
						},
					},
				});
				for (let i = from; i < chunks.length; i++) {
					// The first attempt hangs up mid-snapshot; a resumed one runs to End.
					if (requests.length === 1 && i === opts.dropAfter) {
						throw new ConnectError('connection lost', Code.Unavailable);
					}
					yield create(BootstrapResponseSchema, {
						body: {
							case: 'chunk',
							value: { rows: chunks[i]!, resumeToken: new Uint8Array([ i + 1 ]) },
						},
					});
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
					reset: pulls++ < (opts.resetPulls ?? 0),
					hasMore: false,
					seq: 7n,
				};
			},
			query() {
				return { rows: [] };
			},
		});
	});
	return { transport, requests };
}

function newCoordinator(kv: MemoryKVStore, kvMutex: Mutex, transport: Transport): Coordinator {
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
		streamEnabled: false,
		pollIntervalMs: 0,
	});
}

async function keysOf(kv: MemoryKVStore): Promise<string[]> {
	const tx = await kv.read();
	try {
		return (await scanMergedRows(tx, registry, 'todo', WS)).map((r) => r.key);
	} finally {
		tx.release();
	}
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

async function read<T>(kv: MemoryKVStore, key: string): Promise<T | undefined> {
	const tx = await kv.read();
	try {
		return (await tx.get(key)) as T | undefined;
	} finally {
		tx.release();
	}
}

describe('bootstrap resume', () => {
	it('continues a cold snapshot instead of downloading it again', async () => {
		const kv = new MemoryKVStore();
		const kvMutex = createKvMutex('resume-cold');
		const fake = resumableTransport({
			chunks: [ [ row('t1', 1n) ], [ row('t2', 2n) ], [ row('t3', 3n) ] ],
			dropAfter: 2,
		});
		const coord = newCoordinator(kv, kvMutex, fake.transport);

		await expect(coord.syncNow()).rejects.toThrow(/connection lost/);

		// The interrupted attempt's rows stay: they are a prefix of a snapshot the
		// next attempt can finish, not residue to be swept.
		expect((await keysOf(kv)).sort()).toEqual([ 't1', 't2' ]);
		expect(await read(kv, metaBootstrapResume(WS))).toMatchObject({
			gen: 1,
			snapshotGen: 1,
			direct: true,
			cursor: new Uint8Array([ 9 ]),
			token: new Uint8Array([ 2 ]),
		});

		await coord.syncNow();

		// Only the third chunk was downloaded, and the whole snapshot is installed.
		expect(fake.requests).toHaveLength(2);
		expect(fake.requests[1]!.resume?.token).toEqual(new Uint8Array([ 2 ]));
		expect((await keysOf(kv)).sort()).toEqual([ 't1', 't2', 't3' ]);
		expect(await read(kv, metaCursor(WS))).toEqual(new Uint8Array([ 9 ]));
		// Installed, so there is nothing left to resume.
		expect(await read(kv, metaBootstrapResume(WS))).toBeUndefined();
		expect(await stagedKeys(kv)).toEqual([]);

		await coord.stop();
	});

	it('keeps a re-bootstrap staged under the generation that started it', async () => {
		// The staged path's version: the partial snapshot is invisible in b/, and
		// the attempt that adopts it stages into the SAME prefix, so the swap
		// promotes both attempts' rows as one snapshot.
		const kv = new MemoryKVStore();
		const kvMutex = createKvMutex('resume-staged');
		const fake = resumableTransport({
			chunks: [ [ row('t1', 1n) ], [ row('t2', 2n) ] ],
			dropAfter: 1,
			resetPulls: 2,
		});
		const coord = newCoordinator(kv, kvMutex, fake.transport);

		const seed = await kv.write();
		await seed.put(metaCursor(WS), new Uint8Array([ 1 ]));
		await seed.put(
			confirmedKey('todo', WS, 'old'),
			{ v: toBinary(TodoSchema, create(TodoSchema, { id: 'old' })), ver: '1' },
		);
		await seed.commit();

		await expect(coord.syncNow()).rejects.toThrow(/connection lost/);

		// Mid-resume the reader still sees the intact pre-bootstrap workspace, and
		// the partial snapshot waits in staging.
		expect(await keysOf(kv)).toEqual([ 'old' ]);
		expect(await stagedKeys(kv)).toHaveLength(1);
		const record = await read<{ gen: number; snapshotGen: number }>(kv, metaBootstrapResume(WS));
		expect(record).toMatchObject({ gen: 1, snapshotGen: 1, direct: false });

		await coord.syncNow();

		// The adopting attempt claimed generation 2 but staged under 1, so the swap
		// promoted the rows of both.
		expect(await read(kv, metaBootstrapGen(WS))).toBe(2);
		expect((await keysOf(kv)).sort()).toEqual([ 't1', 't2' ]);
		expect(await stagedKeys(kv)).toEqual([]);
		expect(await read(kv, metaBootstrapResume(WS))).toBeUndefined();

		await coord.stop();
	});

	it('discards the partial snapshot when the server refuses to resume', async () => {
		// A refusal means the chunks that follow belong to a DIFFERENT snapshot.
		// Merging them into what the earlier attempt left would strand a row the
		// new snapshot no longer has under a cursor that cannot heal it.
		const kv = new MemoryKVStore();
		const kvMutex = createKvMutex('resume-refused');
		const fake = resumableTransport({
			chunks: [ [ row('gone', 1n) ], [ row('unreached', 2n) ] ],
			dropAfter: 1,
			// The fresh snapshot the refusal serves: the row deleted since the
			// interrupted attempt shipped it is simply not in it.
			insteadServe: [ [ row('t2', 2n) ] ],
		});
		const coord = newCoordinator(kv, kvMutex, fake.transport);

		await expect(coord.syncNow()).rejects.toThrow(/connection lost/);
		expect(await keysOf(kv)).toEqual([ 'gone' ]);

		await coord.syncNow();

		// The client asked to resume, was refused, and threw away what it held
		// rather than leaving a row no cursor could ever remove.
		expect(fake.requests[1]!.resume).toBeDefined();
		expect(await keysOf(kv)).toEqual([ 't2' ]);
		expect(await read(kv, metaCursor(WS))).toEqual(new Uint8Array([ 9 ]));

		await coord.stop();
	});

	it('forgets the refused position even if the fresh snapshot also drops', async () => {
		// The refusal wipes the rows the record described, so the record cannot
		// outlive it: offered back to a server that this time DID honour it, it
		// would install a snapshot missing everything the first attempt delivered.
		const kv = new MemoryKVStore();
		const kvMutex = createKvMutex('resume-refused-twice');
		const requests: BootstrapRequest[] = [];
		const transport = createRouterTransport(({ service }) => {
			service(SyncService, {
				async *bootstrap(req) {
					const attempt = requests.push(req);
					yield create(BootstrapResponseSchema, {
						body: {
							case: 'begin',
							value: {
								cursor: new Uint8Array([ attempt ]),
								seq: 7n,
								// Attempt 2 refuses the resume, then drops before it can
								// deliver (and record) anything of the snapshot it started.
								resumed: false,
							},
						},
					});
					if (attempt <= 2) throw new ConnectError('connection lost', Code.Unavailable);
					yield create(BootstrapResponseSchema, {
						body: {
							case: 'chunk',
							value: { rows: [ row('t1', 1n) ], resumeToken: new Uint8Array([ 1 ]) },
						},
					});
					yield create(BootstrapResponseSchema, { body: { case: 'end', value: {} } });
				},
				push(req) {
					return pushResults(req, []);
				},
				pull() {
					return {
						patch: { changes: [], dropEntities: [] },
						nextCursor: new Uint8Array([ 3 ]),
						lastAckedMutationId: 0n,
						reset: false,
						hasMore: false,
						seq: 7n,
					};
				},
				query() {
					return { rows: [] };
				},
			});
		});
		const coord = newCoordinator(kv, kvMutex, transport);

		// Attempt 1 records a position; attempt 2 is refused and then drops.
		await expect(coord.syncNow()).rejects.toThrow(/connection lost/);
		await expect(coord.syncNow()).rejects.toThrow(/connection lost/);
		expect(await read(kv, metaBootstrapResume(WS))).toBeUndefined();

		await coord.syncNow();
		expect(requests[2]!.resume).toBeUndefined();
		expect(await keysOf(kv)).toEqual([ 't1' ]);

		await coord.stop();
	});

	describe('a write acked between the attempts', () => {
		const todo = (title: string) => toBinary(TodoSchema, create(TodoSchema, { id: 'r', title }));

		/**
		 * A server where row r is 'A' until mutation 1 sets it to 'B' at seq 11.
		 * It refuses the client's cursor, and its re-bootstrap snapshot is taken
		 * at seq 10: the first attempt drops after the chunk holding r. A resumed
		 * attempt keeps that snapshot's cursor and seq and reports the watermark
		 * as it is when the attempt begins. `refuseResume` answers the resume
		 * with a fresh snapshot instead.
		 */
		function ackBetweenAttemptsTransport(opts: { refuseResume: boolean }): Transport {
			let watermark = 0n;
			let title = 'A';
			let installed = false;
			return createRouterTransport(({ service }) => {
				service(SyncService, {
					async *bootstrap(req) {
						const resumed = req.resume !== undefined && !opts.refuseResume;
						if (req.resume !== undefined && opts.refuseResume) {
							yield create(BootstrapResponseSchema, {
								body: {
									case: 'begin',
									value: { cursor: new Uint8Array([ 11 ]), seq: 11n, lastAckedMutationId: watermark },
								},
							});
							yield create(BootstrapResponseSchema, {
								body: {
									case: 'chunk',
									value: {
										rows: [ {
											entity: 'todo',
											key: 'r',
											version: 11n,
											body: { case: 'value', value: todo(title) },
										} ],
									},
								},
							});
							yield create(BootstrapResponseSchema, { body: { case: 'end', value: {} } });
							installed = true;
							return;
						}
						yield create(BootstrapResponseSchema, {
							body: {
								case: 'begin',
								value: {
									cursor: new Uint8Array([ 10 ]),
									seq: 10n,
									resumed,
									lastAckedMutationId: watermark,
								},
							},
						});
						if (!resumed) {
							yield create(BootstrapResponseSchema, {
								body: {
									case: 'chunk',
									value: {
										rows: [ {
											entity: 'todo',
											key: 'r',
											version: 5n,
											body: { case: 'value', value: todo('A') },
										} ],
										resumeToken: new Uint8Array([ 1 ]),
									},
								},
							});
							throw new ConnectError('connection lost', Code.Unavailable);
						}
						yield create(BootstrapResponseSchema, { body: { case: 'end', value: {} } });
						installed = true;
					},
					push(req) {
						const ms = pushedMutations(req);
						for (const m of ms) {
							watermark = m.mutationId;
							title = 'B';
						}
						return pushResults(
							req,
							ms.map((m) => ({ mutationId: m.mutationId, status: MutationStatus.APPLIED, seq: 11n })),
						);
					},
					pull(req) {
						if (!installed) {
							return {
								patch: { changes: [], dropEntities: [] },
								nextCursor: req.cursor,
								lastAckedMutationId: watermark,
								reset: true,
								hasMore: false,
								seq: 0n,
							};
						}
						return {
							patch: {
								changes: [ {
									entity: 'todo',
									key: 'r',
									version: 11n,
									body: { case: 'value', value: todo(title) },
								} ],
								dropEntities: [],
							},
							nextCursor: new Uint8Array([ 11 ]),
							lastAckedMutationId: watermark,
							reset: false,
							hasMore: false,
							seq: 11n,
						};
					},
					query() {
						return { rows: [] };
					},
				});
			});
		}

		/** Given r = 'A' installed, a re-bootstrap that dropped after r, then a local edit of r to 'B'. */
		async function editDuringReBootstrap(opts: { refuseResume: boolean }) {
			const kv = new MemoryKVStore();
			const seed = await kv.write();
			await seed.put(metaCursor(WS), new Uint8Array([ 5 ]));
			await seed.put(confirmedKey('todo', WS, 'r'), { v: todo('A'), ver: '5' });
			await seed.commit();
			const coord = newCoordinator(
				kv,
				createKvMutex('resume-acked'),
				ackBetweenAttemptsTransport(opts),
			);

			await expect(coord.syncNow()).rejects.toThrow(/connection lost/);

			const def = registry.bySchema(TodoSchema);
			const edit = create(MutationSchema, {
				clientId: 'c1',
				mutationId: 1n,
				payload: { case: 'crud', value: buildPatch(def, TodoSchema, 'r', { title: 'B' }) },
			});
			const tx = await kv.write();
			await enqueuePending(tx, WS, edit);
			await applyMutationLocally(tx, registry, {}, WS, edit, new TouchSet());
			await tx.put(metaLastMutationId(WS), '1');
			await tx.commit();

			return { kv, coord };
		}

		async function titleOf(kv: MemoryKVStore): Promise<string | undefined> {
			const tx = await kv.read();
			try {
				const bytes = await readMergedRow(tx, registry, 'todo', WS, 'r');
				return bytes === undefined ? undefined : fromBinary(TodoSchema, bytes).title;
			} finally {
				tx.release();
			}
		}

		async function pendingOf(kv: MemoryKVStore): Promise<number> {
			const tx = await kv.read();
			try {
				return (await readPending(tx, WS)).length;
			} finally {
				tx.release();
			}
		}

		it('keeps the edit visible when the resumed snapshot installs', async () => {
			const { kv, coord } = await editDuringReBootstrap({ refuseResume: false });
			expect(await titleOf(kv)).toBe('B');

			// When one cycle pushes the edit (acked at seq 11), and the resumed
			// snapshot (seq 10) installs
			await coord.syncNow();
			expect(await read(kv, metaCursor(WS))).toEqual(new Uint8Array([ 10 ]));

			// Then the row shows the acked edit
			expect(await titleOf(kv)).toBe('B');

			await coord.stop();
		});

		it('takes the edit from a fresh snapshot when the server refuses the resume', async () => {
			const { kv, coord } = await editDuringReBootstrap({ refuseResume: true });

			await coord.syncNow();

			expect(await read(kv, metaCursor(WS))).toEqual(new Uint8Array([ 11 ]));
			expect(await titleOf(kv)).toBe('B');
			expect(await pendingOf(kv)).toBe(0);

			await coord.stop();
		});
	});

	it('starts over when another bootstrap claimed the workspace in between', async () => {
		// The record is adoptable only while the generation it was written under is
		// still current. Another tab bootstrapping bumps that marker — and wipes the
		// staging the record points at — so the record has to be ignored.
		const kv = new MemoryKVStore();
		const kvMutex = createKvMutex('resume-superseded');
		const fake = resumableTransport({
			chunks: [ [ row('t1', 1n) ], [ row('t2', 2n) ] ],
			dropAfter: 1,
		});
		const coord = newCoordinator(kv, kvMutex, fake.transport);

		await expect(coord.syncNow()).rejects.toThrow(/connection lost/);

		const claim = await kv.write();
		await claim.put(metaBootstrapGen(WS), 99);
		await claim.commit();

		await coord.syncNow();

		expect(fake.requests[1]!.resume).toBeUndefined();
		expect((await keysOf(kv)).sort()).toEqual([ 't1', 't2' ]);

		await coord.stop();
	});

	// Two coordinators over one store and one mutex stand in for two tabs that
	// re-bootstrap the same workspace at the same time.
	describe('two tabs', () => {
		function deferred() {
			let resolve!: () => void;
			const promise = new Promise<void>((r) => (resolve = r));
			return { promise, resolve };
		}

		/**
		 * A server holding a two-chunk snapshot that stops before frame `pauseAt`
		 * (0 and 1 are the chunks, 2 is End) until `resume` is called. `paused`
		 * resolves once it is waiting there.
		 */
		function pausingTransport(pauseAt: number) {
			const chunks = [ [ row('t1', 1n) ], [ row('t2', 2n) ] ];
			const requests: BootstrapRequest[] = [];
			const paused = deferred();
			const held = deferred();
			const transport = createRouterTransport(({ service }) => {
				service(SyncService, {
					async *bootstrap(req) {
						requests.push(req);
						const from = req.resume === undefined ? 0 : req.resume.token[0]!;
						yield create(BootstrapResponseSchema, {
							body: {
								case: 'begin',
								value: {
									cursor: req.resume?.cursor ?? new Uint8Array([ 9 ]),
									seq: 7n,
									resumed: req.resume !== undefined,
								},
							},
						});
						for (let i = from; i <= chunks.length; i++) {
							if (i === pauseAt) {
								paused.resolve();
								await held.promise;
							}
							if (i === chunks.length) break;
							yield create(BootstrapResponseSchema, {
								body: {
									case: 'chunk',
									value: { rows: chunks[i]!, resumeToken: new Uint8Array([ i + 1 ]) },
								},
							});
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
							reset: true,
							hasMore: false,
							seq: 7n,
						};
					},
					query() {
						return { rows: [] };
					},
				});
			});
			return { transport, requests, paused: paused.promise, resume: held.resolve };
		}

		/** A synced workspace, so the next bootstrap stages rather than writing directly. */
		async function seedSynced(kv: MemoryKVStore): Promise<void> {
			const seed = await kv.write();
			await seed.put(metaCursor(WS), new Uint8Array([ 1 ]));
			await seed.put(
				confirmedKey('todo', WS, 'old'),
				{ v: toBinary(TodoSchema, create(TodoSchema, { id: 'old' })), ver: '1' },
			);
			await seed.commit();
		}

		/**
		 * Tab A commits chunk 0 of a staged re-bootstrap and pauses before chunk 1;
		 * tab B then claims the workspace, adopts A's record and pauses before End.
		 */
		async function adoptMidSnapshot() {
			const kv = new MemoryKVStore();
			const kvMutex = new ChainMutex();
			await seedSynced(kv);
			const serverA = pausingTransport(1);
			const serverB = pausingTransport(2);
			const a = newCoordinator(kv, kvMutex, serverA.transport);
			const b = newCoordinator(kv, kvMutex, serverB.transport);

			const syncA = a.syncNow();
			await serverA.paused;
			await expect.poll(() => read(kv, metaBootstrapResume(WS))).toMatchObject({
				gen: 1,
				snapshotGen: 1,
			});

			const syncB = b.syncNow();
			await serverB.paused;
			expect(serverB.requests[0]!.resume?.token).toEqual(new Uint8Array([ 1 ]));
			expect(await read(kv, metaBootstrapGen(WS))).toBe(2);

			return { kv, a, b, serverA, serverB, syncA, syncB };
		}

		it('keeps the rows of the attempt the other tab adopted', async () => {
			// Given tab B resuming the snapshot tab A started
			const t = await adoptMidSnapshot();

			// When A finishes first, superseded, and then B installs
			t.serverA.resume();
			await t.syncA;
			expect.soft(await stagedKeys(t.kv)).not.toEqual([]);
			t.serverB.resume();
			await t.syncB;

			// Then the installed workspace is the whole snapshot
			expect((await keysOf(t.kv)).sort()).toEqual([ 't1', 't2' ]);
			expect(await read(t.kv, metaCursor(WS))).toEqual(new Uint8Array([ 9 ]));

			await t.a.stop();
			await t.b.stop();
		});

		it('stops a superseded attempt from writing to the staging or the resume record', async () => {
			// Given tab B resuming the snapshot tab A started
			const t = await adoptMidSnapshot();
			const before = await stagedKeys(t.kv);
			const record = await read(t.kv, metaBootstrapResume(WS));

			// When A receives chunk 1 after B's claim
			t.serverA.resume();
			await t.syncA;

			// Then A wrote nothing: the record and the staging are B's
			expect(record).toMatchObject({ gen: 2 });
			expect(await read(t.kv, metaBootstrapResume(WS))).toEqual(record);
			expect(await stagedKeys(t.kv)).toEqual(before);

			t.serverB.resume();
			await t.syncB;
			await t.a.stop();
			await t.b.stop();
		});

		it('keeps the adopted rows when the other tab finds out at its install', async () => {
			// Given tab A with every chunk staged, and tab B adopting its record
			// before A installs
			const kv = new MemoryKVStore();
			const kvMutex = new ChainMutex();
			await seedSynced(kv);
			const serverA = pausingTransport(2);
			const serverB = pausingTransport(2);
			const a = newCoordinator(kv, kvMutex, serverA.transport);
			const b = newCoordinator(kv, kvMutex, serverB.transport);
			const syncA = a.syncNow();
			await serverA.paused;
			await expect.poll(() => read(kv, metaBootstrapResume(WS))).toMatchObject({ gen: 1 });
			const syncB = b.syncNow();
			await serverB.paused;
			expect(serverB.requests[0]!.resume?.token).toEqual(new Uint8Array([ 2 ]));

			// When A ends, superseded at its install, and then B installs
			serverA.resume();
			await syncA;
			serverB.resume();
			await syncB;

			// Then the installed workspace is the whole snapshot
			expect((await keysOf(kv)).sort()).toEqual([ 't1', 't2' ]);
			expect(await stagedKeys(kv)).toEqual([]);

			await a.stop();
			await b.stop();
		});

		it('installs its own snapshot when the other tab staged nothing before the claim', async () => {
			// Given tab A paused before its first chunk, so there is no record to adopt
			const kv = new MemoryKVStore();
			const kvMutex = new ChainMutex();
			await seedSynced(kv);
			const serverA = pausingTransport(0);
			const serverB = pausingTransport(2);
			const a = newCoordinator(kv, kvMutex, serverA.transport);
			const b = newCoordinator(kv, kvMutex, serverB.transport);
			const syncA = a.syncNow();
			await serverA.paused;
			const syncB = b.syncNow();
			await serverB.paused;
			expect(serverB.requests[0]!.resume).toBeUndefined();

			// When A finishes superseded, then B installs
			serverA.resume();
			await syncA;
			const ownStaging: string[] = [];
			for (const k of await stagedKeys(kv)) {
				if (k.startsWith(stagingPrefix(WS, 2))) ownStaging.push(k);
			}
			expect(ownStaging).toHaveLength(2);
			serverB.resume();
			await syncB;

			// Then B's snapshot is installed unchanged
			expect((await keysOf(kv)).sort()).toEqual([ 't1', 't2' ]);
			expect(await stagedKeys(kv)).toEqual([]);

			await a.stop();
			await b.stop();
		});
	});
});
