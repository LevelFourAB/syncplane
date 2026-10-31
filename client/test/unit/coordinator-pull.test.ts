import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { Code, ConnectError, createRouterTransport, type Transport } from '@connectrpc/connect';
import { beforeEach, describe, expect, it } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { type RowChange, RowChangeSchema } from '../../src/gen/syncplane/v1/patch_pb.js';
import { BootstrapResponseSchema, SyncService } from '../../src/gen/syncplane/v1/service_pb.js';
import { metaCursor, metaCursorSeq, metaServerSeq, rowTouchKey } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { EntityRegistry } from '../../src/mutations.js';
import { createKvMutex } from '../../src/mutex.js';
import { readMergedRow } from '../../src/read.js';
import { DEFAULT_STORAGE_PREFIX, storageNames } from '../../src/storagenames.js';
import { Coordinator } from '../../src/sync/coordinator.js';
import { StatusTracker } from '../../src/sync/status.js';
import { pushApplied, pushedMutations, pushFailed, pushResults } from './pushfake.js';

/** The workspace these specs drive. */
const WS = 'ws';

// Drives the Coordinator's paged pull (doPull) against a fake server whose
// second page fails, to pin down that already-committed earlier pages still
// notify watchers.

function serverRow(key: string, version: bigint, title: string): RowChange {
	return create(RowChangeSchema, {
		entity: 'todo',
		key,
		version,
		body: { case: 'value', value: toBinary(TodoSchema, create(TodoSchema, { id: key, title })) },
	});
}

describe('coordinator paged pull', () => {
	let kv: MemoryKVStore;
	let registry: EntityRegistry;

	beforeEach(async () => {
		kv = new MemoryKVStore();
		registry = new EntityRegistry([ TodoSchema ]);
		// A cursor already present makes sync run a pull, not the initial bootstrap.
		const tx = await kv.write();
		await tx.put(metaCursor(WS), new Uint8Array([ 0 ]));
		await tx.commit();
	});

	function newCoordinator(transport: Transport, notified: Set<string>): Coordinator {
		const kvMutex = createKvMutex('test');
		return new Coordinator({
			transport,
			kv,
			kvMutex,
			storageNames: storageNames(DEFAULT_STORAGE_PREFIX, 'acct'),
			scopes: new Map([ [ WS, [] ] ]),
			clientId: 'c1',
			registry,
			mirrors: {},
			notify: async (touched) => {
				for (const e of touched.tokens()) notified.add(e);
			},
			status: new StatusTracker(),
			streamEnabled: false,
			pollIntervalMs: 0,
		});
	}

	it('notifies for a committed page even when a later page fails mid-drain', async () => {
		let pulls = 0;
		const transport = createRouterTransport(({ service }) => {
			service(SyncService, {
				push(req) {
					return pushResults(req, []);
				},
				pull() {
					pulls++;
					if (pulls === 1) {
						// Page 1: commits a row for 'todo', then signals more pages.
						return {
							patch: { changes: [ serverRow('t1', 1n, 'page one') ], dropEntities: [] },
							nextCursor: new Uint8Array([ 1 ]),
							lastAckedMutationId: 0n,
							reset: false,
							hasMore: true,
							seq: 0n,
							schemaRevision: '',
						};
					}
					// Page 2: the drain fails before completing.
					throw new Error('pull page 2 dropped');
				},
				// eslint-disable-next-line require-yield
				async *bootstrap() {
					throw new Error('bootstrap not used');
				},
				query() {
					return { rows: [] };
				},
			});
		});

		const notified = new Set<string>();
		const coord = newCoordinator(transport, notified);

		// The cycle fails on page 2...
		await expect(coord.syncNow()).rejects.toThrow();

		// ...but page 1 was committed AND its touched row was notified, so
		// watchers (and other tabs) are not stranded on the stale row.
		expect(notified.has(rowTouchKey(WS, 'todo', 't1'))).toBe(true);
		const read = await kv.read();
		const bytes = await readMergedRow(read, registry, 'todo', WS, 't1');
		read.release();
		expect(bytes).toBeDefined();

		await coord.stop();
	});

	/** Puts the given meta values into the store. */
	async function seedMeta(values: Record<string, unknown>): Promise<void> {
		const tx = await kv.write();
		for (const [ k, v ] of Object.entries(values)) await tx.put(k, v);
		await tx.commit();
	}

	async function title(key: string): Promise<string | undefined> {
		const read = await kv.read();
		try {
			const bytes = await readMergedRow(read, registry, 'todo', WS, key);
			return bytes === undefined ? undefined : fromBinary(TodoSchema, bytes).title;
		} finally {
			read.release();
		}
	}

	async function storedCursor(): Promise<unknown> {
		const read = await kv.read();
		try {
			return await read.get(metaCursor(WS));
		} finally {
			read.release();
		}
	}

	// The state a cursorless Subscribe frame leaves behind: rows and serverSeq at
	// 61, the cursor at 60. A scope change then pulls a resync, and every resync
	// page reports the cursor's own seq, 60. The store must take those pages, or
	// each pull asks for the same page again and the drain never ends.
	it('resyncs from a cursor behind the stored seq instead of pulling the same page forever', async () => {
		await seedMeta({ [metaCursor(WS)]: new Uint8Array([ 60 ]), [metaServerSeq(WS)]: '61' });
		let pulls = 0;
		const transport = createRouterTransport(({ service }) => {
			service(SyncService, {
				push: (req) => pushResults(req, []),
				pull(req) {
					if (++pulls > 20) throw new ConnectError('pull loop', Code.Aborted);
					const page = (changes: RowChange[], cursor: number, seq: bigint, hasMore: boolean) => ({
						patch: { changes, dropEntities: [] },
						nextCursor: new Uint8Array([ cursor ]),
						lastAckedMutationId: 0n,
						reset: false,
						hasMore,
						seq,
						schemaRevision: '',
					});
					switch (req.cursor[0]) {
						case 60: // a resync step: the cursor's own seq
							return page([ serverRow('t3', 40n, 'now visible') ], 160, 60n, true);
						case 160: // the resync completes, still at the cursor's seq
							return page([], 161, 60n, true);
						case 161: // the catch-up under the new scope
							return page([ serverRow('t4', 62n, 'caught up') ], 62, 62n, false);
						default:
							return page([], req.cursor[0]!, 62n, false);
					}
				},
				// eslint-disable-next-line require-yield
				async *bootstrap() {
					throw new Error('bootstrap not used');
				},
				query: () => ({ rows: [] }),
			});
		});
		const coord = newCoordinator(transport, new Set());

		await coord.syncNow();

		expect(pulls).toBe(3);
		expect(await title('t3')).toBe('now visible');
		expect(await title('t4')).toBe('caught up');
		expect(await storedCursor()).toEqual(new Uint8Array([ 62 ]));
		await coord.stop();
	});

	// The backstop for a gate and cursor that disagree in a way the gate does not
	// expect: the server answers the stored cursor with a page the gate keeps
	// dropping. Pulling again from the same cursor cannot change that, so the
	// drain gives up on the cursor and re-bootstraps.
	it('re-bootstraps when the pages for the stored cursor are dropped again and again', async () => {
		await seedMeta({
			[metaCursor(WS)]: new Uint8Array([ 60 ]),
			[metaServerSeq(WS)]: '61',
			[metaCursorSeq(WS)]: '61',
		});
		let pulls = 0;
		let bootstraps = 0;
		const transport = createRouterTransport(({ service }) => {
			service(SyncService, {
				push: (req) => pushResults(req, []),
				pull() {
					if (++pulls > 20) throw new ConnectError('pull loop', Code.Aborted);
					return {
						patch: { changes: [ serverRow('t1', 59n, 'dropped') ], dropEntities: [] },
						nextCursor: new Uint8Array([ 59 ]),
						lastAckedMutationId: 0n,
						reset: false,
						hasMore: true,
						seq: 59n,
						schemaRevision: '',
					};
				},
				async *bootstrap() {
					bootstraps++;
					yield create(BootstrapResponseSchema, {
						body: {
							case: 'begin',
							value: {
								cursor: new Uint8Array([ 9 ]),
								lastAckedMutationId: 0n,
								seq: 70n,
								schemaRevision: '',
							},
						},
					});
					yield create(BootstrapResponseSchema, { body: { case: 'end', value: {} } });
				},
				query: () => ({ rows: [] }),
			});
		});
		const coord = newCoordinator(transport, new Set());

		await coord.syncNow();

		expect(bootstraps).toBe(1);
		expect(pulls).toBeLessThanOrEqual(5);
		expect(await storedCursor()).toEqual(new Uint8Array([ 9 ]));
		await coord.stop();
	});
});
