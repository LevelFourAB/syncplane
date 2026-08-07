import { create, toBinary } from '@bufbuild/protobuf';
import { createRouterTransport, type Transport } from '@connectrpc/connect';
import { beforeEach, describe, expect, it } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { type RowChange, RowChangeSchema } from '../../src/gen/syncplane/v1/patch_pb.js';
import { BootstrapResponseSchema, SyncService } from '../../src/gen/syncplane/v1/service_pb.js';
import { metaCursor, metaServerSeq } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { EntityRegistry } from '../../src/mutations.js';
import { createKvMutex } from '../../src/mutex.js';
import { scanMergedRows } from '../../src/read.js';
import { Coordinator } from '../../src/sync/coordinator.js';
import { StatusTracker } from '../../src/sync/status.js';
import { pushApplied, pushedMutations, pushFailed, pushResults } from './pushfake.js';

/** The workspace these specs drive. */
const WS = 'ws';

// Store swap end-to-end at the client: a client synced against a store at a
// high seq, then the same URL begins serving a fresh, empty store.
// The server answers reset (its cursor-provenance guard fired); the client must
// re-bootstrap, purge the phantom rows, AND reset its monotonic stale-page gate
// DOWN to the new store's baseline — otherwise every subsequent page from the
// new store (whose seq sits below the old high-water mark) is dropped forever.

function serverRow(key: string, version: bigint, title: string): RowChange {
	return create(RowChangeSchema, {
		entity: 'todo',
		key,
		version,
		body: { case: 'value', value: toBinary(TodoSchema, create(TodoSchema, { id: key, title })) },
	});
}

describe('coordinator store swap', () => {
	let kv: MemoryKVStore;
	let registry: EntityRegistry;

	beforeEach(async () => {
		kv = new MemoryKVStore();
		registry = new EntityRegistry([ TodoSchema ]);
		// A cursor already present makes the first sync run a pull, not the
		// initial bootstrap — the client believes it has prior state.
		const tx = await kv.write();
		await tx.put(metaCursor(WS), new Uint8Array([ 1 ]));
		await tx.commit();
	});

	function newCoordinator(transport: Transport): Coordinator {
		return new Coordinator({
			transport,
			kv,
			kvMutex: createKvMutex('swap-test'),
			accountId: 'acct',
			scopes: new Map([ [ WS, [] ] ]),
			clientId: 'c1',
			registry,
			mirrors: {},
			notify: async () => {},
			status: new StatusTracker(),
			streamEnabled: false,
			pollIntervalMs: 0,
		});
	}

	async function todoKeys(): Promise<string[]> {
		const tx = await kv.read();
		try {
			return (await scanMergedRows(tx, registry, 'todo', WS)).map((r) => r.key).sort();
		} finally {
			tx.release();
		}
	}

	async function meta(key: string): Promise<unknown> {
		const tx = await kv.read();
		try {
			return await tx.get(key);
		} finally {
			tx.release();
		}
	}

	it('heals after the store is recreated empty: purges phantoms and re-accepts low-seq pages', async () => {
		// phase 0: original store delivers a row at seq 86.
		// phase 1: store swapped — pull answers reset.
		// phase 2+: fresh empty store at seq 0, then a first write at seq 1.
		let phase = 0;
		const transport = createRouterTransport(({ service }) => {
			service(SyncService, {
				push(req) {
					return pushResults(req, []);
				},
				pull() {
					if (phase === 0) {
						return {
							patch: { changes: [ serverRow('old1', 86n, 'phantom') ], dropEntities: [] },
							nextCursor: new Uint8Array([ 86 ]),
							lastAckedMutationId: 0n,
							reset: false,
							hasMore: false,
							seq: 86n,
							schemaRevision: '',
						};
					}
					if (phase === 1) {
						// The new store never issued seq 86: provenance guard resets.
						return {
							patch: { changes: [], dropEntities: [] },
							nextCursor: new Uint8Array([ 86 ]),
							lastAckedMutationId: 0n,
							reset: true,
							hasMore: false,
							seq: 0n,
							schemaRevision: '',
						};
					}
					// phase 2: the fresh store's first write, at seq 1 — far below
					// the old high-water mark of 86. The healed gate must apply it.
					return {
						patch: { changes: [ serverRow('new1', 1n, 'on the new store') ], dropEntities: [] },
						nextCursor: new Uint8Array([ 1 ]),
						lastAckedMutationId: 0n,
						reset: false,
						hasMore: false,
						seq: 1n,
						schemaRevision: '',
					};
				},
				// The fresh store is empty: Begin at seq 0, no chunks, End.
				async *bootstrap() {
					yield create(BootstrapResponseSchema, {
						body: {
							case: 'begin',
							value: { cursor: new Uint8Array([ 0 ]), lastAckedMutationId: 0n, seq: 0n },
						},
					});
					yield create(BootstrapResponseSchema, { body: { case: 'end', value: {} } });
				},
				query() {
					return { rows: [] };
				},
			});
		});

		const coord = newCoordinator(transport);

		// Phase 0: confirm the phantom row and a high stale-page watermark.
		await coord.syncNow();
		expect(await todoKeys()).toEqual([ 'old1' ]);
		expect(await meta(metaServerSeq(WS))).toBe('86');

		// Phase 1: the swap. Pull resets, the client re-bootstraps against the
		// empty store, purging the phantom and resetting the gate to 0 — NOT
		// clamping it to the stale 86 (the empty-store seq is 0, the bug's
		// hardest case for the gate-reset).
		phase = 1;
		await coord.syncNow();
		expect(await todoKeys()).toEqual([]);
		expect(await meta(metaServerSeq(WS))).toBe('0');

		// Phase 2: a write on the new store at seq 1 must survive the gate that
		// previously sat at 86. Without the baseline reset this page is dropped
		// and the client never sees new1.
		phase = 2;
		await coord.syncNow();
		expect(await todoKeys()).toEqual([ 'new1' ]);
		expect(await meta(metaServerSeq(WS))).toBe('1');

		await coord.stop();
	});
});
