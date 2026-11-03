import { create, toBinary } from '@bufbuild/protobuf';
import { createRouterTransport, type Transport } from '@connectrpc/connect';
import { describe, expect, it } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { BootstrapResponseSchema, SyncService } from '../../src/gen/syncplane/v1/service_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { EntityRegistry } from '../../src/mutations.js';
import { createKvMutex } from '../../src/mutex.js';
import { DEFAULT_STORAGE_PREFIX, storageNames } from '../../src/storagenames.js';
import { Coordinator } from '../../src/sync/coordinator.js';
import { StatusTracker } from '../../src/sync/status.js';
import { WatcherHub } from '../../src/watch.js';
import { pushResults } from './pushfake.js';

// WorkspaceStatus.behind: the server's head (PullResponse.head_seq) minus the
// seq this client has pulled through.

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

/**
 * A server whose snapshot is at seq 2, with pages at seqs 4, 6 and 8 after it.
 * `head` is the head_seq every page reports; 0n is a server older than it.
 */
function transport(head: bigint): Transport {
	const pages = [ 4n, 6n, 8n ];
	return createRouterTransport(({ service }) => {
		service(SyncService, {
			async *bootstrap() {
				yield create(BootstrapResponseSchema, {
					body: {
						case: 'begin',
						value: { cursor: new Uint8Array([ 2 ]), seq: 2n, entities: [ 'todo' ] },
					},
				});
				yield create(BootstrapResponseSchema, { body: { case: 'end', value: {} } });
			},
			push(req) {
				return pushResults(req, []);
			},
			pull(req) {
				const at = req.cursor[0] ?? 2;
				const i = pages.findIndex((p) => p > BigInt(at));
				const seq = i < 0 ? BigInt(at) : pages[i]!;
				return {
					patch: { changes: i < 0 ? [] : [ row(`t${seq}`, seq) ], dropEntities: [] },
					nextCursor: new Uint8Array([ Number(seq) ]),
					lastAckedMutationId: 0n,
					reset: false,
					hasMore: i >= 0 && i < pages.length - 1,
					seq,
					headSeq: head,
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
	const seen: (number | undefined)[] = [];
	status.onChange((s) => {
		const behind = s.workspaces[WS]?.behind;
		if (seen.at(-1) !== behind) seen.push(behind);
	});
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
	return { coord, status, seen };
}

describe('changes behind the server', () => {
	it('shrinks as pages drain and reaches 0 once caught up', async () => {
		const { coord, status, seen } = setup(transport(8n), 'behind-drain');

		await coord.syncNow();

		// 0 right after the snapshot, whose seq was the head when it was taken.
		// The first page reports the newer head 8 at seq 4, and the distance
		// shrinks from there.
		expect(seen).toEqual([ 0, 4, 2, 0 ]);
		expect(status.current().workspaces[WS]?.behind).toBe(0);
		await coord.stop();
	});

	it('is unknown once a page from a server that reports no head arrives', async () => {
		// The snapshot's seq is the head when it was taken, so the client is 0
		// behind right after it. A page with no head makes it unknown again.
		const { coord, status, seen } = setup(transport(0n), 'behind-old-server');

		await coord.syncNow();

		expect(seen).toEqual([ 0, undefined ]);
		expect(status.current().workspaces[WS]?.behind).toBeUndefined();
		await coord.stop();
	});
});
