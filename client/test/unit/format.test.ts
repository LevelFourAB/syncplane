import { create, type MessageInitShape, toBinary } from '@bufbuild/protobuf';
import { createRouterTransport } from '@connectrpc/connect';
import { describe, expect, it } from 'vitest';

import { SyncClient, type SyncStatus } from '../../src/client.js';
import { CodeFormatUpgraded } from '../../src/codes.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { CrudOp, type Mutation, MutationSchema } from '../../src/gen/syncplane/v1/mutation_pb.js';
import {
	BootstrapResponseSchema,
	MutationStatus,
	SyncService,
} from '../../src/gen/syncplane/v1/service_pb.js';
import {
	confirmedKey,
	FORMAT_VERSION,
	META_CLIENT_ID,
	META_FORMAT_VERSION,
	metaCursor,
	metaLastMutationId,
	metaPushedThrough,
	pendingKey,
} from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { RejectionInfo } from '../../src/sync/coordinator.js';
import { pushApplied, pushedMutations, pushFailed, pushResults } from './pushfake.js';

/** The single workspace the upgrading clients in this suite hold. */
const WS = 'ws1';

function pendingPut(id: bigint, todoId: string): Mutation {
	return create(MutationSchema, {
		clientId: 'seeded-client',
		mutationId: id,
		payload: {
			case: 'crud',
			value: {
				entity: 'todo',
				key: todoId,
				op: CrudOp.PUT,
				value: toBinary(TodoSchema, create(TodoSchema, { id: todoId, title: `todo ${todoId}` })),
			},
		},
	});
}

/**
 * A store carrying data written by an (imaginary) older format version, with a
 * pending queue of `count` mutations (ids 1..count).
 */
async function seedOldFormatStore(count = 2): Promise<MemoryKVStore> {
	const kv = new MemoryKVStore();
	const tx = await kv.write();
	await tx.put(META_FORMAT_VERSION, FORMAT_VERSION - 1);
	await tx.put(META_CLIENT_ID, 'seeded-client');
	await tx.put(metaLastMutationId(WS), String(count));
	await tx.put(metaCursor(WS), new Uint8Array([ 9 ]));
	await tx.put(confirmedKey('todo', WS, 'old'), { v: new Uint8Array([ 1 ]), ver: '1' });
	for (let i = 1; i <= count; i++) {
		await tx.put(
			pendingKey(WS, BigInt(i)),
			toBinary(MutationSchema, pendingPut(BigInt(i), `p${i}`)),
		);
	}
	await tx.commit();
	return kv;
}

interface StubServer {
	transport: ReturnType<typeof createRouterTransport>;
	pushedBatches: Mutation[][];
}

function stubServer(
	{
		failPush = false,
		failFirstPush = false,
		failPushAttempt = 0,
		watermark = 0n,
		onPush,
	}: {
		failPush?: boolean;
		failFirstPush?: boolean;
		failPushAttempt?: number;
		watermark?: bigint;
		/** Runs before the push is answered, to interleave another tab's work. */
		onPush?: () => Promise<void> | void;
	} = {},
): StubServer {
	const pushedBatches: Mutation[][] = [];
	// Per-client watermarks, like the real server. The seeded store's client
	// starts at `watermark` (mutations acked in an earlier session).
	const acked = new Map<string, bigint>([ [ 'seeded-client', watermark ] ]);
	let pushAttempts = 0;
	const transport = createRouterTransport(({ service }) => {
		service(SyncService, {
			async push(req) {
				pushAttempts++;
				await onPush?.();
				if (failPush || (failFirstPush && pushAttempts === 1) || pushAttempts === failPushAttempt) {
					throw new Error('server unreachable during upgrade');
				}
				pushedBatches.push(pushedMutations(req));
				return pushResults(
					req,
					pushedMutations(req).map((m) => {
						const last = acked.get(m.clientId) ?? 0n;
						if (m.mutationId <= last) {
							return { mutationId: m.mutationId, status: MutationStatus.ALREADY_APPLIED };
						}
						acked.set(m.clientId, m.mutationId);
						return { mutationId: m.mutationId, status: MutationStatus.APPLIED, seq: m.mutationId };
					}),
				);
			},
			pull(req) {
				return {
					patch: { changes: [] },
					nextCursor: new Uint8Array([ 1 ]),
					lastAckedMutationId: acked.get(req.clientId) ?? 0n,
				};
			},
			async *bootstrap(req): AsyncIterable<MessageInitShape<typeof BootstrapResponseSchema>> {
				yield {
					body: {
						case: 'begin',
						value: { cursor: new Uint8Array([ 1 ]), lastAckedMutationId: acked.get(req.clientId) ?? 0n },
					},
				};
				yield { body: { case: 'end', value: {} } };
			},
		});
	});
	return { transport, pushedBatches };
}

function newUpgradingClient(
	kv: MemoryKVStore,
	transport: StubServer['transport'],
	statuses: SyncStatus[],
	maxMutationsPerPush?: number,
	rejections?: RejectionInfo[],
) {
	return new SyncClient({
		transport,
		workspaceId: 'ws1',
		kvStore: kv,
		entities: [ TodoSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
		maxMutationsPerPush,
		onStatusChange: (s) => statuses.push(s),
		onMutationRejected: (info) => rejections?.push(info),
	});
}

/**
 * The wipe another tab's upgrade commits when its own salvage dropped
 * something: the whole store gone, identity included, and the new version
 * stamped.
 */
async function otherTabUpgrade(kv: MemoryKVStore): Promise<void> {
	const tx = await kv.write();
	const keys: string[] = [];
	for await (const [ k ] of tx.scan('')) keys.push(k);
	for (const k of keys) await tx.del(k);
	await tx.put(META_FORMAT_VERSION, FORMAT_VERSION);
	await tx.commit();
}

async function dump(kv: MemoryKVStore): Promise<Map<string, unknown>> {
	const tx = await kv.read();
	const all = new Map<string, unknown>();
	for await (const [ k, v ] of tx.scan('')) all.set(k, v);
	tx.release();
	return all;
}

describe('format version upgrade', () => {
	it('stamps fresh stores without wiping', async () => {
		const kv = new MemoryKVStore();
		const { transport } = stubServer();
		const client = newUpgradingClient(kv, transport, []);
		await client.mutate.put(TodoSchema, { id: 't1', title: 'pre-start' });
		await client.start();

		const all = await dump(kv);
		expect(all.get(META_FORMAT_VERSION)).toBe(FORMAT_VERSION);
		expect(await client.pendingCount()).toBeGreaterThanOrEqual(0); // queue untouched by stamping
		await client.close();
	});

	it('pushes the pending queue once, then wipes everything but the clientId', async () => {
		const kv = await seedOldFormatStore();
		const { transport, pushedBatches } = stubServer();
		const statuses: SyncStatus[] = [];
		const client = newUpgradingClient(kv, transport, statuses);
		await client.start();
		await client.syncNow();

		// The salvage push carried both seeded mutations.
		expect(pushedBatches[0]?.map((m) => m.mutationId)).toEqual([ 1n, 2n ]);

		const all = await dump(kv);
		expect(all.get(META_CLIENT_ID)).toBe('seeded-client');
		expect(all.get(metaLastMutationId(WS))).toBe('2');
		// The counter's pushed-through mark comes with it: the salvage put every
		// id the counter has minted on the server.
		expect(all.get(metaPushedThrough(WS))).toBe('2');
		expect(all.get(META_FORMAT_VERSION)).toBe(FORMAT_VERSION);
		expect(all.has(confirmedKey('todo', WS, 'old'))).toBe(false);
		expect(await client.pendingCount()).toBe(0);
		// Everything was salvaged: no format_upgraded error surfaced.
		expect(statuses.some((s) => s.lastError?.code === CodeFormatUpgraded)).toBe(false);
		await client.close();
	});

	it('keeps minting mutation ids above the server watermark after the wipe', async () => {
		const kv = await seedOldFormatStore();
		// The server already acked ids 1-2 from "seeded-client" in an earlier
		// session. If the wipe lost the counter, the post-upgrade mutation
		// would reuse id 1, come back ALREADY_APPLIED and be silently dropped.
		const { transport, pushedBatches } = stubServer({ watermark: 2n });
		const client = newUpgradingClient(kv, transport, []);
		await client.start();
		await client.syncNow();

		await client.mutate.put(TodoSchema, { id: 't3', title: 'post-upgrade' });
		await client.syncNow();

		const postUpgrade = pushedBatches.at(-1);
		expect(postUpgrade?.map((m) => m.mutationId)).toEqual([ 3n ]);
		await client.close();
	});

	it('keeps compaction off the ids the salvage push already delivered', async () => {
		const kv = await seedOldFormatStore();
		const { transport, pushedBatches } = stubServer();
		const client = newUpgradingClient(kv, transport, []);
		await client.start();
		await client.syncNow();
		// The salvage push carried ids 1-2, so the server's watermark is 2.
		expect(pushedBatches[0]?.map((m) => m.mutationId)).toEqual([ 1n, 2n ]);

		// A burst on one key folds to a single slot, and compaction renumbers the
		// folded tail from the pushed-through mark. A mark the wipe left for
		// initPushedThrough to re-seed from the (now empty) queue reads as 0, which
		// puts that slot back at id 1 — already applied, and silently dropped along
		// with every id up to the watermark that followed it.
		await client.mutate.put(TodoSchema, { id: 't3', title: 'first' });
		await client.mutate.put(TodoSchema, { id: 't3', title: 'second' });
		await client.syncNow();

		expect(pushedBatches.at(-1)?.map((m) => m.mutationId)).toEqual([ 3n ]);
		expect(await client.pendingCount()).toBe(0);
		await client.close();
	});

	it('reports dropped mutations and rotates the clientId when the salvage push fails', async () => {
		const kv = await seedOldFormatStore();
		const { transport } = stubServer({ failPush: true });
		const statuses: SyncStatus[] = [];
		const client = newUpgradingClient(kv, transport, statuses);
		await client.start();
		await client.syncNow();

		const all = await dump(kv);
		// Dropped mutations leave the server watermark behind the local
		// counter, so the identity cannot be reused.
		expect(all.get(META_CLIENT_ID)).not.toBe('seeded-client');
		expect(all.get(META_FORMAT_VERSION)).toBe(FORMAT_VERSION);
		expect(await client.pendingCount()).toBe(0);

		const reported = statuses.find((s) => s.lastError?.code === CodeFormatUpgraded);
		expect(reported?.lastError?.message).toContain('2');
		await client.close();
	});

	it('starts over as a fresh client when the salvage fails, so new mutations do not gap', async () => {
		const kv = await seedOldFormatStore();
		// Server acked ids 1-2 earlier; the salvage push of the seeded queue
		// fails (e.g. the upgrade happens offline). Keeping clientId + counter
		// here would mint id 3 against a watermark of 2 *minus* the dropped
		// mutations the server never saw — a permanent mutation-id gap.
		const { transport, pushedBatches } = stubServer({ failFirstPush: true, watermark: 2n });
		const client = newUpgradingClient(kv, transport, []);
		await client.start();
		await client.syncNow();

		await client.mutate.put(TodoSchema, { id: 't3', title: 'post-upgrade' });
		await client.syncNow();

		const batch = pushedBatches.at(-1);
		expect(batch?.map((m) => m.mutationId)).toEqual([ 1n ]);
		expect(batch?.[0]?.clientId).not.toBe('seeded-client');
		expect(await client.pendingCount()).toBe(0);
		await client.close();
	});

	it('rotates the identity a pre-start mutation already took', async () => {
		const kv = await seedOldFormatStore();
		// The pre-start mutate reads the seeded clientId (and caches it) before
		// the upgrade discards it. Carrying that cache past the wipe would put
		// the rest of the session back on the identity whose watermark the
		// dropped salvage left behind.
		const { transport, pushedBatches } = stubServer({ failFirstPush: true, watermark: 2n });
		const client = newUpgradingClient(kv, transport, []);
		await client.mutate.put(TodoSchema, { id: 't3', title: 'pre-start' });
		const before = await client.clientId();
		expect(before).toBe('seeded-client');

		await client.start();
		await client.syncNow();
		expect(await client.clientId()).not.toBe('seeded-client');

		await client.mutate.put(TodoSchema, { id: 't4', title: 'post-upgrade' });
		await client.syncNow();
		const batch = pushedBatches.at(-1);
		expect(batch?.[0]?.clientId).not.toBe('seeded-client');
		expect(batch?.map((m) => m.mutationId)).toEqual([ 1n ]);
		await client.close();
	});

	it('rotates the identity when another tab wiped it before this one looked', async () => {
		const kv = await seedOldFormatStore();
		const { transport, pushedBatches } = stubServer();
		const client = newUpgradingClient(kv, transport, []);
		await client.mutate.put(TodoSchema, { id: 't3', title: 'pre-start' });
		expect(await client.clientId()).toBe('seeded-client');

		// The other tab finishes the upgrade, identity and all, before this one
		// reaches the version check — which then finds nothing to do.
		await otherTabUpgrade(kv);
		await client.start();
		expect(await client.clientId()).not.toBe('seeded-client');

		await client.mutate.put(TodoSchema, { id: 't4', title: 'post-upgrade' });
		await client.syncNow();
		expect(pushedBatches.at(-1)?.[0]?.clientId).not.toBe('seeded-client');
		await client.close();
	});

	it('rotates the identity when another tab wiped it during the salvage push', async () => {
		const kv = await seedOldFormatStore();
		// The other tab wins the race while this one's salvage is in flight: by
		// the time this tab takes the lock back the version is already current, so
		// it leaves the wipe alone — but the identity it memoized before start()
		// went with it, and minting under it would put every later mutation on
		// watermarks the wipe existed to escape.
		let wiped = false;
		const { transport, pushedBatches } = stubServer({
			onPush: async () => {
				if (wiped) return;
				wiped = true;
				await otherTabUpgrade(kv);
			},
		});
		const client = newUpgradingClient(kv, transport, []);
		await client.mutate.put(TodoSchema, { id: 't3', title: 'pre-start' });
		expect(await client.clientId()).toBe('seeded-client');

		await client.start();
		expect(await client.clientId()).not.toBe('seeded-client');

		await client.mutate.put(TodoSchema, { id: 't4', title: 'post-upgrade' });
		await client.syncNow();
		expect(pushedBatches.at(-1)?.[0]?.clientId).not.toBe('seeded-client');
		await client.close();
	});

	it('chunks a salvage queue larger than maxMutationsPerPush into within-limit pushes', async () => {
		const kv = await seedOldFormatStore(5);
		const { transport, pushedBatches } = stubServer();
		const statuses: SyncStatus[] = [];
		const client = newUpgradingClient(kv, transport, statuses, 2);
		await client.start();
		await client.syncNow();

		// 5 mutations, cap 2 → three within-limit chunks, every mutation salvaged.
		expect(pushedBatches.map((b) => b.map((m) => m.mutationId))).toEqual([
			[ 1n, 2n ],
			[ 3n, 4n ],
			[ 5n ],
		]);
		const all = await dump(kv);
		expect(all.get(META_CLIENT_ID)).toBe('seeded-client'); // clean salvage keeps identity
		expect(await client.pendingCount()).toBe(0);
		expect(statuses.some((s) => s.lastError?.code === CodeFormatUpgraded)).toBe(false);
		await client.close();
	});

	it('salvages the chunks that landed and drops the remainder when a later chunk fails', async () => {
		const kv = await seedOldFormatStore(5);
		// Cap 2 → chunks [1,2],[3,4],[5]; the second push fails, so [1,2] are
		// salvaged and the unsent remainder ([3,4] and [5]) is dropped.
		const { transport, pushedBatches } = stubServer({ failPushAttempt: 2 });
		const statuses: SyncStatus[] = [];
		const rejections: RejectionInfo[] = [];
		const client = newUpgradingClient(kv, transport, statuses, 2, rejections);
		await client.start();
		await client.syncNow();

		expect(pushedBatches.map((b) => b.map((m) => m.mutationId))).toEqual([ [ 1n, 2n ] ]);
		const all = await dump(kv);
		// A dropped mutation rotates the identity to avoid a mutation-id gap.
		expect(all.get(META_CLIENT_ID)).not.toBe('seeded-client');
		expect(await client.pendingCount()).toBe(0);
		const reported = statuses.find((s) => s.lastError?.code === CodeFormatUpgraded);
		expect(reported?.lastError?.message).toContain('3'); // 3 unsent mutations dropped
		// The status line carries the count, the dead-letter surface names them.
		expect(rejections.map((r) => r.mutationId)).toEqual([ 3n, 4n, 5n ]);
		expect(rejections.every((r) => r.errorCode === CodeFormatUpgraded)).toBe(true);
		await client.close();
	});

	it('ignores undecodable pending entries (counted as dropped)', async () => {
		const kv = await seedOldFormatStore();
		const tx = await kv.write();
		await tx.put(pendingKey(WS, 3n), new Uint8Array([ 0xff, 0xff, 0xff, 0xff, 0xff ]));
		await tx.commit();

		const { transport, pushedBatches } = stubServer();
		const statuses: SyncStatus[] = [];
		const rejections: RejectionInfo[] = [];
		const client = newUpgradingClient(kv, transport, statuses, undefined, rejections);
		await client.start();
		await client.syncNow();

		expect(pushedBatches[0]?.map((m) => m.mutationId)).toEqual([ 1n, 2n ]);
		const reported = statuses.find((s) => s.lastError?.code === CodeFormatUpgraded);
		expect(reported?.lastError?.message).toContain('1');
		// Nothing decoded the entry, so there is no mutation to name — the count on
		// the status line is all this one can be reported as.
		expect(rejections).toEqual([]);
		await client.close();
	});
});
