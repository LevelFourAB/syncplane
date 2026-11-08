import { create, toBinary } from '@bufbuild/protobuf';
import { createRouterTransport, type Transport } from '@connectrpc/connect';
import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { BootstrapResponseSchema, SyncService } from '../../src/gen/syncplane/v1/service_pb.js';
import { confirmedWsPrefix, metaCursor } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { pushResults } from './pushfake.js';

// A purge while sync runs. The pull or stream page the purge overtakes is
// answered for a workspace the store no longer holds, and landing it would
// leave a cursor over a partial row set: the next add of the workspace would
// pull from that cursor instead of bootstrapping.

const A = 'ws-a';
const B = 'ws-b';

function row(id: string, version: bigint) {
	return {
		entity: 'todo',
		key: id,
		version,
		body: {
			case: 'value' as const,
			value: toBinary(TodoSchema, create(TodoSchema, { id, title: id })),
		},
	};
}

/**
 * A server holding a1 and a2 in workspace A, which commits a3 while it holds
 * the next pull of A it is asked to hold. Every row's version is its seq, and a
 * cursor is the one-byte seq it was read at.
 */
function heldPullServer() {
	const rows = [ row('a1', 1n), row('a2', 2n) ];
	let seq = 2n;
	let bootstraps = 0;
	let hold: { reached: () => void; release: Promise<void> } | undefined;

	const transport: Transport = createRouterTransport(({ service }) => {
		service(SyncService, {
			async *bootstrap(req) {
				bootstraps++;
				const snapshot = req.workspaceId === A ? [ ...rows ] : [];
				yield create(BootstrapResponseSchema, {
					body: { case: 'begin', value: { cursor: new Uint8Array([ Number(seq) ]), seq } },
				});
				yield create(BootstrapResponseSchema, { body: { case: 'chunk', value: { rows: snapshot } } });
				yield create(BootstrapResponseSchema, { body: { case: 'end', value: {} } });
			},
			push(req) {
				return pushResults(req, []);
			},
			async pull(req) {
				if (req.workspaceId === A && hold !== undefined) {
					const h = hold;
					hold = undefined;
					rows.push(row('a3', 3n));
					seq = 3n;
					h.reached();
					await h.release;
				}
				const from = BigInt(req.cursor[0] ?? 0);
				const changes = req.workspaceId === A ? rows.filter((r) => r.version > from) : [];
				return {
					patch: { changes, dropEntities: [] },
					nextCursor: new Uint8Array([ Number(seq) ]),
					lastAckedMutationId: 0n,
					reset: false,
					hasMore: false,
					seq,
					headSeq: seq,
				};
			},
			query() {
				return { rows: [] };
			},
		});
	});

	/** Holds the next pull of A; resolves `reached` once it is held. */
	function holdNextPull() {
		let reached!: () => void;
		const isHeld = new Promise<void>((r) => (reached = r));
		let release!: () => void;
		hold = { reached, release: new Promise<void>((r) => (release = r)) };
		return { isHeld, release };
	}

	return { transport, holdNextPull, bootstraps: () => bootstraps };
}

function newClient(kvStore: MemoryKVStore, transport: Transport) {
	return new SyncClient({
		accountId: 'acct',
		workspaces: [ A, B ],
		defaultWorkspace: B,
		kvStore,
		entities: [ TodoSchema ],
		transport,
		streamEnabled: false,
		pollIntervalMs: 0,
	});
}

async function storedRows(kv: MemoryKVStore): Promise<string[]> {
	const tx = await kv.read();
	try {
		const keys: string[] = [];
		for await (const [ k ] of tx.scan(confirmedWsPrefix('todo', A))) keys.push(k);
		return keys;
	} finally {
		tx.release();
	}
}

async function storedCursor(kv: MemoryKVStore): Promise<unknown> {
	const tx = await kv.read();
	try {
		return await tx.get(metaCursor(A));
	} finally {
		tx.release();
	}
}

async function keysIn(client: SyncClient): Promise<string[]> {
	return (await client.ws(A).scan(TodoSchema)).map((r) => r.key).sort();
}

describe('removeWorkspace purge while syncing', () => {
	/** Given workspace A synced with a1 and a2, and a pull of A held at the server. */
	async function purgeDuringPull() {
		const kv = new MemoryKVStore();
		const server = heldPullServer();
		const client = newClient(kv, server.transport);
		await client.start();
		await client.syncNow();
		expect(await keysIn(client)).toEqual([ 'a1', 'a2' ]);

		const held = server.holdNextPull();
		const sync = client.syncNow().catch(() => {});
		await held.isHeld;

		// When the purge commits before the held pull answers
		expect((await client.removeWorkspace(A, { purge: true })).purged).toBe(true);
		held.release();
		await sync;

		return { kv, server, client };
	}

	it('writes nothing of a pull page that lands after the purge', async () => {
		const { kv, client } = await purgeDuringPull();

		// Then the store holds no row and no cursor of A
		expect(await storedRows(kv)).toEqual([]);
		expect(await storedCursor(kv)).toBeUndefined();

		await client.close();
	});

	it('bootstraps a purged workspace when it is added again', async () => {
		const { server, client } = await purgeDuringPull();
		const before = server.bootstraps();

		client.addWorkspace(A);
		await client.syncNow();

		expect(server.bootstraps()).toBeGreaterThan(before);
		expect(await keysIn(client)).toEqual([ 'a1', 'a2', 'a3' ]);

		await client.close();
	});
});

describe('purgeLocalData while another tab syncs', () => {
	it('leaves no cursor behind for the pull the other tab had in flight', async () => {
		// Given two tabs on one store, with tab 2's pull of A held at the server
		const kv = new MemoryKVStore();
		const server = heldPullServer();
		const tab1 = newClient(kv, server.transport);
		const tab2 = newClient(kv, server.transport);
		await tab1.start();
		await tab1.syncNow();
		await tab2.start();
		await tab2.syncNow();
		const held = server.holdNextPull();
		const sync = tab2.syncNow().catch(() => {});
		await held.isHeld;

		// When tab 1 purges and then tab 2's page lands
		await tab1.purgeLocalData();
		held.release();
		await sync;

		// Then the store holds no cursor of A, and tab 2 bootstraps on its next cycle
		expect(await storedCursor(kv)).toBeUndefined();
		const before = server.bootstraps();
		await tab2.syncNow();
		expect(server.bootstraps()).toBeGreaterThan(before);
		expect(await keysIn(tab2)).toEqual([ 'a1', 'a2', 'a3' ]);

		await tab2.close();
	});
});
