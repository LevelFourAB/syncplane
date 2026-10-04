/**
 * What the stored CallerAccess costs local writes through the real client, for
 * a caller with many grants.
 *
 * access.bench.ts measures the steps of a CallerAccess change: decode,
 * summary, lookups. This file measures the time between two changes, when the
 * CallerAccess stays the same and the app continues to write:
 *
 *   - "local write": a patch of a task. The floor for the next arm.
 *   - "local write, then hasPermission": the same patch, then one prediction
 *     that walks one row up to a project the caller holds the permission at.
 *     The difference from the floor is what the first prediction after a write
 *     costs.
 *   - "create with a creator field": a put of a new post, which stores the
 *     caller's primary subject in its author field.
 *
 * Each arm drives a SyncClient with no server, over MemoryKVStore and over
 * IDBKVStore on fake-indexeddb. MemoryKVStore gives back the value it holds,
 * so a read of a large value costs it nothing. fake-indexeddb structured-clones
 * each value a read gives back, so its arms include the CPU cost of the copy
 * that a browser also makes. Neither says anything about the disk of a
 * browser; the bytes below are the measure for that. The CallerAccess has the
 * shape of access.bench.ts: four permissions, each held at every one of N
 * rows, with UUID-shaped keys.
 *
 * Each arm also prints the bytes that the KV store gave back per iteration,
 * counted over the values of get, getMany, scan and scanRange: the size of
 * each byte array and string in them.
 *
 * Nothing acks, so the pending queue grows during a run, and a prediction
 * reads the queue when a write changes it. Each arm runs a fixed number of
 * iterations, so the queue gets to the same length in each run and two runs
 * compare fairly.
 *
 * Run with: client/node_modules/.bin/vitest bench --project bench test/bench/access-client.bench.ts
 */

import 'fake-indexeddb/auto';

import { create, toBinary } from '@bufbuild/protobuf';
import { bench, describe } from 'vitest';

import { SyncClient } from '../../src/client.js';
import {
	PostSchema,
	ProjectMemberSchema,
	ProjectSchema,
	RoomMemberSchema,
	RoomSchema,
	TaskSchema,
} from '../../src/gen/syncplane/testdata/access/v1/access_pb.js';
import { CallerAccessSchema } from '../../src/gen/syncplane/v1/service_pb.js';
import { metaCallerAccess } from '../../src/keys.js';
import { IDBKVStore } from '../../src/kv/idb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type {
	GenerationOf,
	GenerationTracking,
	KVRead,
	KVStore,
	KVWrite,
} from '../../src/kv/store.js';

const WS = 'ws';
const SIZES = [ 50, 1000, 5000 ];
const BACKENDS = [ 'memory', 'fake-indexeddb' ] as const;
type Backend = (typeof BACKENDS)[number];

/** A fixed count of iterations; see the file comment. */
const FIXED = { time: 0, iterations: 200, warmupTime: 0, warmupIterations: 10 };

/** The key of row r, as access.bench.ts writes it. */
function rowKey(r: number): string {
	return `${r.toString(16).padStart(8, '0')}-0000-4000-8000-${r.toString(16).padStart(12, '0')}`;
}

/** Encodes a CallerAccess that holds each of `permissions` at `rows` rows of `entity`. */
function accessBytes(entity: string, permissions: readonly string[], rows: number): Uint8Array {
	const keys = Array.from({ length: rows }, (_, r) => rowKey(r)).sort();
	return toBinary(
		CallerAccessSchema,
		create(CallerAccessSchema, {
			subjects: [ 'me' ],
			seq: 1n,
			permissions: permissions.map((permission) => ({
				permission,
				anchors: [ { entity, keys } ],
			})),
		}),
	);
}

/** The size of the byte arrays and strings in `value`. */
function sizeOf(value: unknown): number {
	if (value instanceof Uint8Array) return value.byteLength;
	if (typeof value === 'string') return value.length;
	if (typeof value !== 'object' || value === null) return 0;
	let n = 0;
	for (const v of Object.values(value)) n += sizeOf(v);
	return n;
}

/** A store that counts the bytes its reads give back. */
class ByteCountingStore implements KVStore {
	bytes = 0;

	constructor(private inner: KVStore & GenerationTracking) {}

	async read(): Promise<KVRead> {
		return this.counting(await this.inner.read());
	}

	async write(): Promise<KVWrite> {
		return this.counting(await this.inner.write());
	}

	close(): Promise<void> {
		return this.inner.close();
	}

	trackGenerations(generationOf: GenerationOf): void {
		this.inner.trackGenerations(generationOf);
	}

	private counting<T extends KVRead>(tx: T): T {
		const store = this;
		return new Proxy(tx, {
			get(target, prop) {
				const value = Reflect.get(target, prop) as unknown;
				if (typeof value !== 'function') return value;
				const fn = value as (...args: unknown[]) => unknown;
				switch (prop) {
					case 'get':
						return async (key: string) => {
							const got = await fn.call(target, key);
							store.bytes += sizeOf(got);
							return got;
						};
					case 'getMany':
						return async (keys: string[]) => {
							const got = (await fn.call(target, keys)) as unknown[];
							for (const v of got) store.bytes += sizeOf(v);
							return got;
						};
					case 'scan':
					case 'scanRange':
						return async function*(...args: unknown[]) {
							for await (const entry of fn.apply(target, args) as AsyncIterable<[string, unknown]>) {
								store.bytes += sizeOf(entry[1]);
								yield entry;
							}
						};
					default:
						return fn.bind(target);
				}
			},
		});
	}
}

let dbCounter = 0;

async function storeOf(backend: Backend): Promise<ByteCountingStore> {
	const inner = backend === 'memory'
		? new MemoryKVStore()
		: await IDBKVStore.open(`access-client-bench-${dbCounter++}`);
	return new ByteCountingStore(inner);
}

/** Stores `raw` as the workspace's CallerAccess, as a pull does. */
async function storeAccess(kv: KVStore, raw: Uint8Array): Promise<void> {
	const tx = await kv.write();
	await tx.put(metaCallerAccess(WS), raw);
	await tx.commit();
}

interface Arm {
	client: SyncClient;
	kv: ByteCountingStore;
	/** Iterations run so far. */
	ops: number;
}

/**
 * A client that holds every task permission at `rows` projects and has a
 * queued task in the first one, with the summary decided once.
 */
async function writer(backend: Backend, rows: number): Promise<Arm> {
	const kv = await storeOf(backend);
	const client = new SyncClient({
		workspaceId: WS,
		kvStore: kv,
		entities: [ ProjectSchema, ProjectMemberSchema, TaskSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
	});
	// The first answer opens the store, so the CallerAccess stored next is
	// stamped like one a pull stores.
	await client.hasPermission(TaskSchema, 't1', 'edit_tasks');
	await storeAccess(
		kv,
		accessBytes('project', [ 'edit_tasks', 'view_members', 'view_projects', 'view_tasks' ], rows),
	);
	await client.mutate.put(TaskSchema, { id: 't1', project: { id: rowKey(0) } });
	if (await client.hasPermission(TaskSchema, 't1', 'edit_tasks') !== 'yes') {
		throw new Error('the setup does not predict yes');
	}
	return { client, kv, ops: 0 };
}

/** A client that may post in `rows` rooms, as the subject `me`. */
async function poster(backend: Backend, rows: number): Promise<Arm> {
	const kv = await storeOf(backend);
	const client = new SyncClient({
		workspaceId: WS,
		kvStore: kv,
		entities: [ RoomSchema, RoomMemberSchema, PostSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
	});
	await client.hasPermission(RoomSchema, rowKey(0), 'post');
	await storeAccess(
		kv,
		accessBytes('room', [ 'post', 'view_posts', 'view_room_members', 'view_rooms' ], rows),
	);
	await client.mutate.put(PostSchema, { id: 'po-setup', room: { id: rowKey(0) } });
	if ((await client.get(PostSchema, 'po-setup'))?.authorId !== 'me') {
		throw new Error('the setup does not store the primary subject');
	}
	return { client, kv, ops: 0 };
}

/**
 * Bench options that print the bytes the store gave back per iteration of
 * the timed run.
 */
function reporting(label: string, arm: () => Arm | undefined) {
	let bytes = 0;
	let ops = 0;
	return {
		...FIXED,
		setup: (_task: unknown, mode?: string) => {
			const a = arm();
			if (mode !== 'run' || a === undefined) return;
			bytes = a.kv.bytes;
			ops = a.ops;
		},
		teardown: (_task: unknown, mode?: string) => {
			const a = arm();
			if (mode !== 'run' || a === undefined) return;
			const per = Math.round((a.kv.bytes - bytes) / (a.ops - ops));
			console.log(`${label}: ${per} bytes read per iteration`);
		},
	};
}

for (const n of SIZES) {
	describe(`${n} rows`, () => {
		for (const backend of BACKENDS) {
			let floor: Arm | undefined;
			bench(`${backend}: local write`, async () => {
				floor ??= await writer(backend, n);
				await floor.client.mutate.patch(TaskSchema, 't1', { title: `edit ${floor.ops++}` });
			}, reporting(`${n} rows, ${backend}: local write`, () => floor));

			let asking: Arm | undefined;
			bench(`${backend}: local write, then hasPermission`, async () => {
				asking ??= await writer(backend, n);
				await asking.client.mutate.patch(TaskSchema, 't1', { title: `edit ${asking.ops++}` });
				if (await asking.client.hasPermission(TaskSchema, 't1', 'edit_tasks') !== 'yes') {
					throw new Error('the prediction changed');
				}
			}, reporting(`${n} rows, ${backend}: local write, then hasPermission`, () => asking));

			let creating: Arm | undefined;
			bench(`${backend}: create with a creator field`, async () => {
				creating ??= await poster(backend, n);
				await creating.client.mutate.put(PostSchema, {
					id: `po-${creating.ops++}`,
					room: { id: rowKey(0) },
				});
			}, reporting(`${n} rows, ${backend}: create with a creator field`, () => creating));
		}
	});
}
