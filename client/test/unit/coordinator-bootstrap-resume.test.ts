import { create, toBinary } from '@bufbuild/protobuf';
import { Code, ConnectError, createRouterTransport, type Transport } from '@connectrpc/connect';
import { describe, expect, it } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import {
	type BootstrapRequest,
	BootstrapResponseSchema,
	SyncService,
} from '../../src/gen/syncplane/v1/service_pb.js';
import {
	confirmedKey,
	metaBootstrapGen,
	metaBootstrapResume,
	metaCursor,
	stagingWsRoot,
} from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { EntityRegistry } from '../../src/mutations.js';
import { createKvMutex, type Mutex } from '../../src/mutex.js';
import { scanMergedRows } from '../../src/read.js';
import { DEFAULT_STORAGE_PREFIX, storageNames } from '../../src/storagenames.js';
import { Coordinator } from '../../src/sync/coordinator.js';
import { StatusTracker } from '../../src/sync/status.js';
import { WatcherHub } from '../../src/watch.js';
import { pushResults } from './pushfake.js';

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
});
