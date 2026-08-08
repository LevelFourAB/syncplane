import { create, toBinary } from '@bufbuild/protobuf';
import { createRouterTransport, type Transport } from '@connectrpc/connect';
import { beforeEach, describe, expect, it } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { type RowChange, RowChangeSchema } from '../../src/gen/syncplane/v1/patch_pb.js';
import { SyncService } from '../../src/gen/syncplane/v1/service_pb.js';
import { metaCursor, rowTouchKey } from '../../src/keys.js';
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
});
