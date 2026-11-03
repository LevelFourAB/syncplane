import { create, toBinary } from '@bufbuild/protobuf';
import { Code, ConnectError, createRouterTransport, type Transport } from '@connectrpc/connect';
import { describe, expect, it } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { BootstrapResponseSchema, SyncService } from '../../src/gen/syncplane/v1/service_pb.js';
import { confirmedKey, metaBootstrapResume, metaCursor } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { EntityRegistry } from '../../src/mutations.js';
import { createKvMutex } from '../../src/mutex.js';
import { DEFAULT_STORAGE_PREFIX, storageNames } from '../../src/storagenames.js';
import { Coordinator } from '../../src/sync/coordinator.js';
import { type BootstrapProgressInfo, StatusTracker } from '../../src/sync/status.js';
import { WatcherHub } from '../../src/watch.js';
import { pushResults } from './pushfake.js';

// WorkspaceStatus.bootstrap: the download progress an app shows while a
// snapshot arrives. The tracker's arithmetic is covered in
// bootstrap-progress.test.ts; this drives the coordinator against a server that
// sends (or does not send) row estimates.

const WS = 'ws';
const registry = new EntityRegistry([ TodoSchema ]);

function row(id: string) {
	return {
		entity: 'todo',
		key: id,
		version: 1n,
		body: { case: 'value' as const, value: toBinary(TodoSchema, create(TodoSchema, { id })) },
	};
}

/**
 * A server shipping `chunks`, with `estimates` on Begin (none when absent). The
 * first attempt drops after `dropAfter` chunks when that is set; a resumed one
 * continues from the chunk its token names.
 */
function transport(opts: {
	chunks: ReturnType<typeof row>[][];
	estimates?: bigint[];
	dropAfter?: number;
	resetPulls?: number;
}): Transport {
	let attempts = 0;
	let pulls = 0;
	return createRouterTransport(({ service }) => {
		service(SyncService, {
			async *bootstrap(req) {
				attempts++;
				const from = req.resume === undefined ? 0 : req.resume.token[0]!;
				yield create(BootstrapResponseSchema, {
					body: {
						case: 'begin',
						value: {
							cursor: new Uint8Array([ 9 ]),
							seq: 7n,
							entities: [ 'todo' ],
							rowEstimates: opts.estimates ?? [],
							resumed: req.resume !== undefined,
						},
					},
				});
				for (let i = from; i < opts.chunks.length; i++) {
					if (attempts === 1 && i === opts.dropAfter) {
						throw new ConnectError('connection lost', Code.Unavailable);
					}
					yield create(BootstrapResponseSchema, {
						body: {
							case: 'chunk',
							value: { rows: opts.chunks[i]!, resumeToken: new Uint8Array([ i + 1 ]) },
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
}

function setup(t: Transport, name: string) {
	const kv = new MemoryKVStore();
	const kvMutex = createKvMutex(name);
	const status = new StatusTracker();
	const seen: (BootstrapProgressInfo | undefined)[] = [];
	status.onChange((s) => seen.push(s.workspaces[WS]?.bootstrap));
	const hub = new WatcherHub(kv, registry, kvMutex);
	const coord = new Coordinator({
		transport: t,
		kv,
		kvMutex,
		storageNames: storageNames(DEFAULT_STORAGE_PREFIX, 'acct'),
		scopes: new Map([ [ WS, [] ] ]),
		clientId: 'c1',
		registry,
		mirrors: {},
		notify: (touched) => hub.notify(touched),
		status,
		streamEnabled: false,
		pollIntervalMs: 0,
	});
	return { kv, coord, status, seen };
}

function defined(seen: (BootstrapProgressInfo | undefined)[]): BootstrapProgressInfo[] {
	return seen.filter((p): p is BootstrapProgressInfo => p !== undefined);
}

describe('bootstrap progress in the status', () => {
	it('reports rows against the server estimate and clears once the snapshot is in', async () => {
		const { coord, status, seen } = setup(
			transport({ chunks: [ [ row('t1'), row('t2') ], [ row('t3') ] ], estimates: [ 4n ] }),
			'progress-estimate',
		);

		await coord.syncNow();

		const ps = defined(seen);
		expect(ps[0]).toEqual({ rowsApplied: 0, rowsEstimated: 0, entitiesDone: 0, entitiesTotal: 0 });
		expect(ps).toContainEqual({
			rowsApplied: 0,
			rowsEstimated: 4,
			entitiesDone: 0,
			entitiesTotal: 1,
		});
		expect(ps.at(-1)).toEqual({
			rowsApplied: 3,
			rowsEstimated: 4,
			entitiesDone: 1,
			entitiesTotal: 1,
		});
		expect(seen.at(-1)).toBeUndefined();
		expect(status.current().workspaces[WS]?.bootstrap).toBeUndefined();
		await coord.stop();
	});

	it('shows an unknown size from a server that sends no estimate, and still installs', async () => {
		const { kv, coord, seen } = setup(
			transport({ chunks: [ [ row('t1') ], [ row('t2') ] ] }),
			'progress-old-server',
		);

		await coord.syncNow();

		const ps = defined(seen);
		expect(ps.every((p) => p.rowsEstimated === 0)).toBe(true);
		expect(ps.at(-1)).toMatchObject({ rowsApplied: 2, entitiesDone: 1, entitiesTotal: 1 });
		const tx = await kv.read();
		try {
			expect(await tx.get(metaCursor(WS))).toEqual(new Uint8Array([ 9 ]));
		} finally {
			tx.release();
		}
		await coord.stop();
	});

	it('reports a staged re-bootstrap too', async () => {
		const { kv, coord, seen } = setup(
			transport({ chunks: [ [ row('t1') ], [ row('t2') ] ], estimates: [ 2n ], resetPulls: 1 }),
			'progress-staged',
		);
		const seed = await kv.write();
		await seed.put(metaCursor(WS), new Uint8Array([ 1 ]));
		await seed.put(
			confirmedKey('todo', WS, 'old'),
			{ v: toBinary(TodoSchema, create(TodoSchema, { id: 'old' })), ver: '1' },
		);
		await seed.commit();

		await coord.syncNow();

		expect(defined(seen).at(-1)).toEqual({
			rowsApplied: 2,
			rowsEstimated: 2,
			entitiesDone: 1,
			entitiesTotal: 1,
		});
		expect(seen.at(-1)).toBeUndefined();
		await coord.stop();
	});

	it('continues from the rows an interrupted attempt stored, without going back', async () => {
		const { kv, coord, seen } = setup(
			transport({
				chunks: [ [ row('t1'), row('t2') ], [ row('t3') ], [ row('t4') ] ],
				estimates: [ 4n ],
				dropAfter: 2,
			}),
			'progress-resume',
		);

		await expect(coord.syncNow()).rejects.toThrow(/connection lost/);
		expect(seen.at(-1), 'a failed attempt clears the progress').toBeUndefined();
		const tx = await kv.read();
		try {
			expect(await tx.get(metaBootstrapResume(WS))).toMatchObject({
				progress: { rows: 3, estimate: 4, entitiesDone: 0, entitiesTotal: 1 },
			});
		} finally {
			tx.release();
		}
		const firstAttempt = seen.length;

		await coord.syncNow();

		const second = defined(seen.slice(firstAttempt));
		expect(second[0]).toMatchObject({ rowsApplied: 3, rowsEstimated: 4 });
		for (let i = 1; i < second.length; i++) {
			expect(second[i]!.rowsApplied).toBeGreaterThanOrEqual(second[i - 1]!.rowsApplied);
		}
		expect(second.at(-1)).toEqual({
			rowsApplied: 4,
			rowsEstimated: 4,
			entitiesDone: 1,
			entitiesTotal: 1,
		});
		await coord.stop();
	});
});
