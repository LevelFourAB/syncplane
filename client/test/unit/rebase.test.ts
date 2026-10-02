import { create, fromBinary, type MessageInitShape, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it, vi } from 'vitest';

import { ACCESS_TOUCH_ENTITY } from '../../src/access.js';
import { type Todo, TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { CrudOp, type Mutation, MutationSchema } from '../../src/gen/syncplane/v1/mutation_pb.js';
import {
	PatchSchema,
	type RowChange,
	RowChangeSchema,
} from '../../src/gen/syncplane/v1/patch_pb.js';
import { CallerAccessSchema, PullResponseSchema } from '../../src/gen/syncplane/v1/service_pb.js';
import {
	confirmedKey,
	metaBootstrapGen,
	metaCallerAccess,
	metaCursor,
	overlayKey,
	STAGING_ROOT,
	stagingKey,
	stagingPrefix,
	stagingWsRoot,
	touchKey,
} from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { EntityRegistry } from '../../src/mutations.js';
import { enqueuePending, readPending } from '../../src/optimistic/queue.js';
import {
	applyMutationLocally,
	applyServerChanges,
	clearStaging,
	finalizeRebase,
	type MirrorMap,
	promoteStaged,
	rebase,
	stageServerChanges,
	wipeConfirmedState,
} from '../../src/optimistic/rebase.js';
import { readMergedRow, scanMergedRows } from '../../src/read.js';
import { TouchSet } from '../../src/touch.js';

const registry = new EntityRegistry([ TodoSchema ]);
/** Every spec here drives one workspace; the multi-workspace specs live below. */
const WS = 'ws1';
const noMirrors: MirrorMap = {};
const cursor = (n: number) => new Uint8Array([ n ]);

function putMutation(
	id: bigint,
	todo: MessageInitShape<typeof TodoSchema> & { id: string },
): Mutation {
	return create(MutationSchema, {
		clientId: 'c1',
		mutationId: id,
		payload: {
			case: 'crud',
			value: {
				entity: 'todo',
				key: todo.id,
				op: CrudOp.PUT,
				value: toBinary(TodoSchema, create(TodoSchema, todo)),
			},
		},
	});
}

function patchMutation(
	id: bigint,
	key: string,
	init: MessageInitShape<typeof TodoSchema>,
	paths: string[],
): Mutation {
	return create(MutationSchema, {
		clientId: 'c1',
		mutationId: id,
		payload: {
			case: 'crud',
			value: {
				entity: 'todo',
				key,
				op: CrudOp.PATCH,
				value: toBinary(TodoSchema, create(TodoSchema, init)),
				fieldMask: { paths },
			},
		},
	});
}

function serverRow(
	key: string,
	version: bigint,
	todo: MessageInitShape<typeof TodoSchema> & { id: string },
): RowChange {
	return create(RowChangeSchema, {
		entity: 'todo',
		key,
		version,
		body: { case: 'value', value: toBinary(TodoSchema, create(TodoSchema, todo)) },
	});
}

function tombstone(key: string, version: bigint): RowChange {
	return create(RowChangeSchema, {
		entity: 'todo',
		key,
		version,
		body: { case: 'tombstone', value: true },
	});
}

/** Enqueues mutations and applies them to the overlay, like SyncClient does. */
async function enqueueLocally(
	kv: MemoryKVStore,
	...mutations: Mutation[]
): Promise<void> {
	await enqueueLocallyIn(kv, WS, ...mutations);
}

async function enqueueLocallyIn(
	kv: MemoryKVStore,
	workspaceId: string,
	...mutations: Mutation[]
): Promise<void> {
	const tx = await kv.write();
	for (const m of mutations) {
		await enqueuePending(tx, workspaceId, m);
		await applyMutationLocally(tx, registry, noMirrors, workspaceId, m, new TouchSet());
	}
	await tx.commit();
}

async function mergedTodos(
	kv: MemoryKVStore,
	workspaceId: string = WS,
): Promise<Map<string, Todo>> {
	const tx = await kv.read();
	const rows = await scanMergedRows(tx, registry, 'todo', workspaceId);
	tx.release();
	return new Map(rows.map((row) => [ row.key, fromBinary(TodoSchema, row.v) ]));
}

describe('rebase', () => {
	it('drops acked pending mutations and keeps the rest', async () => {
		const kv = new MemoryKVStore();
		await enqueueLocally(
			kv,
			putMutation(1n, { id: 't1', title: 'one' }),
			putMutation(2n, { id: 't2', title: 'two' }),
		);

		// Server acked #1 and shipped its confirmed row.
		await rebase(kv, registry, noMirrors, WS, {
			changes: [ serverRow('t1', 1n, { id: 't1', title: 'one' }) ],
			nextCursor: cursor(1),
			lastAckedMutationId: 1n,
		});

		const tx = await kv.read();
		const pending = await readPending(tx, WS);
		expect(await tx.get(metaCursor(WS))).toEqual(cursor(1));
		tx.release();
		expect(pending.map((m) => m.mutationId)).toEqual([ 2n ]);

		const todos = await mergedTodos(kv);
		expect(todos.get('t1')?.title).toBe('one');
		expect(todos.get('t2')?.title).toBe('two');
	});

	// A Subscribe frame the server chose not to checkpoint carries no cursor
	// (SubscribeRequest.sparse_cursor). Storing the empty value would read back
	// as "never synced" and re-bootstrap the workspace on the next cycle, so the
	// rows have to land while the stored position stays put.
	it('applies a page that carries no cursor without disturbing the stored one', async () => {
		const kv = new MemoryKVStore();
		await rebase(kv, registry, noMirrors, WS, {
			changes: [ serverRow('t1', 1n, { id: 't1', title: 'one' }) ],
			nextCursor: cursor(7),
			lastAckedMutationId: 0n,
			serverSeq: 1n,
		});

		await rebase(kv, registry, noMirrors, WS, {
			changes: [ serverRow('t2', 2n, { id: 't2', title: 'two' }) ],
			nextCursor: new Uint8Array(),
			lastAckedMutationId: 0n,
			serverSeq: 2n,
		});

		const tx = await kv.read();
		expect(await tx.get(metaCursor(WS))).toEqual(cursor(7));
		tx.release();

		// The seq still advances, so the gate keeps dropping stale pages from a
		// concurrent tab even while the cursor sits behind what has been applied.
		const stale = await rebase(kv, registry, noMirrors, WS, {
			changes: [ serverRow('t2', 1n, { id: 't2', title: 'stale' }) ],
			nextCursor: new Uint8Array(),
			lastAckedMutationId: 0n,
			serverSeq: 1n,
		});
		expect(stale.skipped).toBe(true);

		const todos = await mergedTodos(kv);
		expect(todos.get('t1')?.title).toBe('one');
		expect(todos.get('t2')?.title).toBe('two');
	});

	it('with deferAcked, keeps acked mutations queued so a row does not regress mid-drain', async () => {
		const kv = new MemoryKVStore();
		// Confirmed base: t1 = "old".
		await rebase(kv, registry, noMirrors, WS, {
			changes: [ serverRow('t1', 1n, { id: 't1', title: 'old' }) ],
			nextCursor: cursor(1),
			lastAckedMutationId: 0n,
		});
		// User edits t1 -> "new" optimistically.
		await enqueueLocally(kv, putMutation(1n, { id: 't1', title: 'new' }));

		// Page 1 of a paged pull: the server has acked #1, but its confirmed row
		// ("new") is seq-ordered into a later page and hasn't arrived yet. has_more
		// is true, so deferAcked holds the drop.
		await rebase(kv, registry, noMirrors, WS, {
			changes: [],
			nextCursor: cursor(2),
			lastAckedMutationId: 1n,
			deferAcked: true,
		});

		// The mutation stays queued and its optimistic value still shadows the row:
		// no regression to the old confirmed value mid-drain.
		let tx = await kv.read();
		expect((await readPending(tx, WS)).map((m) => m.mutationId)).toEqual([ 1n ]);
		tx.release();
		expect((await mergedTodos(kv)).get('t1')?.title).toBe('new');

		// Final page (!has_more): t1's confirmed "new" row arrives and the drop runs.
		await rebase(kv, registry, noMirrors, WS, {
			changes: [ serverRow('t1', 2n, { id: 't1', title: 'new' }) ],
			nextCursor: cursor(3),
			lastAckedMutationId: 1n,
			deferAcked: false,
		});

		tx = await kv.read();
		expect(await readPending(tx, WS)).toEqual([]);
		expect(await tx.get(metaCursor(WS))).toEqual(cursor(3));
		tx.release();
		expect((await mergedTodos(kv)).get('t1')?.title).toBe('new');
	});

	it('replays surviving patches on top of a changed server base', async () => {
		const kv = new MemoryKVStore();
		// Confirmed base arrives first.
		await rebase(kv, registry, noMirrors, WS, {
			changes: [ serverRow('t1', 1n, { id: 't1', title: 'original', completed: false }) ],
			nextCursor: cursor(1),
			lastAckedMutationId: 0n,
		});

		// Local pending patch renames the todo.
		await enqueueLocally(kv, patchMutation(1n, 't1', { title: 'local rename' }, [ 'title' ]));

		// Meanwhile the server completed it (another client); our patch is not
		// acked yet, so it must replay ON TOP of the new base.
		await rebase(kv, registry, noMirrors, WS, {
			changes: [ serverRow('t1', 2n, { id: 't1', title: 'original', completed: true }) ],
			nextCursor: cursor(2),
			lastAckedMutationId: 0n,
		});

		const todos = await mergedTodos(kv);
		expect(todos.get('t1')?.title).toBe('local rename');
		expect(todos.get('t1')?.completed).toBe(true);
	});

	it('clears optimistic state once the server acks everything', async () => {
		const kv = new MemoryKVStore();
		await enqueueLocally(kv, putMutation(1n, { id: 't1', title: 'local' }));

		// Server applied the mutation but its authoritative row differs
		// (e.g. a server mutator normalized the title): server must win.
		await rebase(kv, registry, noMirrors, WS, {
			changes: [ serverRow('t1', 1n, { id: 't1', title: 'server version' }) ],
			nextCursor: cursor(1),
			lastAckedMutationId: 1n,
		});

		const todos = await mergedTodos(kv);
		expect(todos.get('t1')?.title).toBe('server version');
		const tx = await kv.read();
		expect(await readPending(tx, WS)).toEqual([]);
		tx.release();
	});

	it('applies tombstones from the server', async () => {
		const kv = new MemoryKVStore();
		await rebase(kv, registry, noMirrors, WS, {
			changes: [ serverRow('t1', 1n, { id: 't1', title: 'doomed' }) ],
			nextCursor: cursor(1),
			lastAckedMutationId: 0n,
		});
		await rebase(kv, registry, noMirrors, WS, {
			changes: [ tombstone('t1', 2n) ],
			nextCursor: cursor(2),
			lastAckedMutationId: 0n,
		});
		expect((await mergedTodos(kv)).size).toBe(0);
	});

	it('drops whole entities before applying changes (delta resync)', async () => {
		const kv = new MemoryKVStore();
		await rebase(kv, registry, noMirrors, WS, {
			changes: [
				serverRow('t1', 1n, { id: 't1', title: 'one' }),
				serverRow('t2', 2n, { id: 't2', title: 'two' }),
			],
			nextCursor: cursor(1),
			lastAckedMutationId: 0n,
		});
		// A pending local mutation must survive the drop: it replays on the
		// emptied base.
		await enqueueLocally(kv, putMutation(1n, { id: 't3', title: 'local' }));

		const { touched } = await rebase(kv, registry, noMirrors, WS, {
			changes: [],
			dropEntities: [ 'todo' ],
			nextCursor: cursor(2),
			lastAckedMutationId: 0n,
		});
		expect(touched.tokens().has(touchKey(WS, 'todo'))).toBe(true);
		expect([ ...(await mergedTodos(kv)).keys() ]).toEqual([ 't3' ]);
	});

	it('names the overlay rows it cleared rather than the whole entity', async () => {
		const kv = new MemoryKVStore();
		await enqueueLocally(
			kv,
			putMutation(1n, { id: 't1', title: 'one' }),
			putMutation(2n, { id: 'nested/t2', title: 'two' }),
		);

		// #1 is acked and its confirmed row arrives; #2 replays onto the cleared
		// overlay. Both rows sat in the overlay, so both are named — a wildcard
		// here would put every watcher of the entity back on a full scan.
		const { touched } = await rebase(kv, registry, noMirrors, WS, {
			changes: [ serverRow('t1', 1n, { id: 't1', title: 'one' }) ],
			nextCursor: cursor(1),
			lastAckedMutationId: 1n,
		});

		expect([ ...touched.pairs() ]).toEqual([
			{ workspaceId: WS, entity: 'todo', keys: new Set([ 't1', 'nested/t2' ]) },
		]);
	});

	it('reports the surviving pending count', async () => {
		const kv = new MemoryKVStore();
		await enqueueLocally(
			kv,
			putMutation(1n, { id: 't1', title: 'one' }),
			putMutation(2n, { id: 't2', title: 'two' }),
			putMutation(3n, { id: 't3', title: 'three' }),
		);
		const { pendingCount } = await rebase(kv, registry, noMirrors, WS, {
			changes: [],
			nextCursor: cursor(1),
			lastAckedMutationId: 2n,
		});
		expect(pendingCount).toBe(1);
	});

	it('wipes and rebuilds confirmed state on reset (bootstrap sequence), replaying pending', async () => {
		const kv = new MemoryKVStore();
		await rebase(kv, registry, noMirrors, WS, {
			changes: [
				serverRow('stale', 1n, { id: 'stale', title: 'should disappear' }),
				serverRow('t1', 2n, { id: 't1', title: 'old' }),
			],
			nextCursor: cursor(2),
			lastAckedMutationId: 0n,
		});
		await enqueueLocally(kv, patchMutation(1n, 't1', { title: 'local edit' }, [ 'title' ]));

		// Reset path, as the coordinator drives it: wipe tx, chunk tx(s),
		// finalize tx. "stale" is gone on the server, t1 changed, and the local
		// patch must still replay.
		const touched = new TouchSet();
		let tx = await kv.write();
		await wipeConfirmedState(tx, registry, WS, touched);
		await tx.commit();

		tx = await kv.write();
		await applyServerChanges(
			tx,
			registry,
			WS,
			[ serverRow('t1', 5n, { id: 't1', title: 'rebootstrapped', completed: true }) ],
			touched,
		);
		await tx.commit();

		tx = await kv.write();
		const pendingCount = await finalizeRebase(
			tx,
			registry,
			noMirrors,
			WS,
			{ nextCursor: cursor(5), lastAckedMutationId: 0n },
			touched,
		);
		await tx.commit();

		const todos = await mergedTodos(kv);
		expect(todos.has('stale')).toBe(false);
		expect(todos.get('t1')?.title).toBe('local edit');
		expect(todos.get('t1')?.completed).toBe(true);
		expect(touched.tokens().has(touchKey(WS, 'todo'))).toBe(true);
		expect(pendingCount).toBe(1);
	});

	it('announces the caller-access summary a wipe removes', async () => {
		const kv = new MemoryKVStore();
		let tx = await kv.write();
		await tx.put(metaCallerAccess(WS), toBinary(CallerAccessSchema, create(CallerAccessSchema, {})));
		await tx.commit();

		// can() answers from the summary, so its listeners must learn that it
		// is gone, or useCan keeps an answer the store no longer backs.
		const touched = new TouchSet();
		tx = await kv.write();
		await wipeConfirmedState(tx, registry, WS, touched);
		await tx.commit();

		expect(touched.tokens().has(touchKey(WS, ACCESS_TOUCH_ENTITY))).toBe(true);
	});

	it('split functions compose to the same result as rebase', async () => {
		const input = {
			changes: [
				serverRow('t1', 3n, { id: 't1', title: 'from server' }),
				tombstone('t2', 4n),
			],
			nextCursor: cursor(4),
			lastAckedMutationId: 1n,
		};
		const seed = async (kv: MemoryKVStore) => {
			await rebase(kv, registry, noMirrors, WS, {
				changes: [
					serverRow('t1', 1n, { id: 't1', title: 'old' }),
					serverRow('t2', 2n, { id: 't2', title: 'doomed' }),
				],
				nextCursor: cursor(2),
				lastAckedMutationId: 0n,
			});
			await enqueueLocally(
				kv,
				putMutation(1n, { id: 't3', title: 'acked' }),
				patchMutation(2n, 't1', { title: 'pending edit' }, [ 'title' ]),
			);
		};

		const composed = new MemoryKVStore();
		await seed(composed);
		const touched = new TouchSet();
		const tx = await composed.write();
		await applyServerChanges(tx, registry, WS, input.changes, touched);
		const pendingCount = await finalizeRebase(tx, registry, noMirrors, WS, input, touched);
		await tx.commit();

		const whole = new MemoryKVStore();
		await seed(whole);
		const result = await rebase(whole, registry, noMirrors, WS, input);

		expect(await mergedTodos(composed)).toEqual(await mergedTodos(whole));
		expect(touched).toEqual(result.touched);
		expect(pendingCount).toBe(result.pendingCount);
	});

	it('a crash before the final bootstrap tx leaves no cursor and an intact overlay', async () => {
		const kv = new MemoryKVStore();
		await rebase(kv, registry, noMirrors, WS, {
			changes: [ serverRow('t1', 1n, { id: 't1', title: 'confirmed' }) ],
			nextCursor: cursor(1),
			lastAckedMutationId: 0n,
		});
		await enqueueLocally(kv, putMutation(1n, { id: 't2', title: 'optimistic' }));

		// Bootstrap starts: wipe commits, a chunk commits — then the process
		// dies (no finalize tx).
		const touched = new TouchSet();
		let tx = await kv.write();
		await wipeConfirmedState(tx, registry, WS, touched);
		await tx.commit();
		tx = await kv.write();
		await applyServerChanges(
			tx,
			registry,
			WS,
			[ serverRow('t1', 5n, { id: 't1', title: 'new snapshot' }) ],
			touched,
		);
		await tx.commit();

		const read = await kv.read();
		expect(await read.get(metaCursor(WS))).toBeUndefined();
		const pending = await readPending(read, WS);
		read.release();
		expect(pending.map((m) => m.mutationId)).toEqual([ 1n ]);
		// The optimistic write still shadows reads mid-bootstrap.
		expect((await mergedTodos(kv)).get('t2')?.title).toBe('optimistic');
	});

	it('local delete survives rebase until acked', async () => {
		const kv = new MemoryKVStore();
		await rebase(kv, registry, noMirrors, WS, {
			changes: [ serverRow('t1', 1n, { id: 't1', title: 'server' }) ],
			nextCursor: cursor(1),
			lastAckedMutationId: 0n,
		});
		await enqueueLocally(kv, {
			...create(MutationSchema, {
				clientId: 'c1',
				mutationId: 1n,
				payload: {
					case: 'crud',
					value: { entity: 'todo', key: 't1', op: CrudOp.DELETE },
				},
			}),
		});

		await rebase(kv, registry, noMirrors, WS, {
			changes: [ serverRow('t1', 2n, { id: 't1', title: 'server again' }) ],
			nextCursor: cursor(2),
			lastAckedMutationId: 0n,
		});
		expect((await mergedTodos(kv)).size).toBe(0);
	});
});

/**
 * Tabs of different builds share one store, and therefore one pending queue per
 * workspace — so a tab routinely rebases over mutations naming entities its own
 * registry has never heard of. `registry` here is the older build (todo only);
 * "gadget" is what the newer build registers.
 */
describe('replay across builds', () => {
	const foreignPut = (id: bigint, key: string): Mutation =>
		create(MutationSchema, {
			clientId: 'c2',
			mutationId: id,
			payload: {
				case: 'crud',
				value: {
					entity: 'gadget',
					key,
					op: CrudOp.PUT,
					// Opaque here by construction: this build has no schema to decode it.
					value: new Uint8Array([ 1, 2, 3 ]),
				},
			},
		});

	/** Queues without applying — the other tab applied it to its own overlay. */
	async function enqueueForeign(kv: MemoryKVStore, ...mutations: Mutation[]): Promise<void> {
		const tx = await kv.write();
		for (const m of mutations) await enqueuePending(tx, WS, m);
		await tx.commit();
	}

	it('steps over a queued mutation for an unregistered entity and replays the rest', async () => {
		const kv = new MemoryKVStore();
		await enqueueForeign(kv, foreignPut(1n, 'g1'));
		await enqueueLocally(kv, putMutation(2n, { id: 't1', title: 'mine' }));

		await rebase(kv, registry, noMirrors, WS, {
			changes: [],
			nextCursor: cursor(1),
			lastAckedMutationId: 0n,
		});

		// The rebase committed, and the foreign mutation is still queued for the
		// push that will drain it.
		const tx = await kv.read();
		expect((await readPending(tx, WS)).map((m) => m.mutationId)).toEqual([ 1n, 2n ]);
		expect(await tx.get(metaCursor(WS))).toEqual(cursor(1));
		tx.release();
		expect((await mergedTodos(kv)).get('t1')?.title).toBe('mine');
	});

	it('applies the registered sub-mutations of a mixed batch', async () => {
		const kv = new MemoryKVStore();
		await enqueueForeign(
			kv,
			create(MutationSchema, {
				clientId: 'c2',
				mutationId: 1n,
				payload: {
					case: 'batch',
					value: {
						mutations: [
							{ entity: 'gadget', key: 'g1', op: CrudOp.PUT, value: new Uint8Array([ 1 ]) },
							{
								entity: 'todo',
								key: 't1',
								op: CrudOp.PUT,
								value: toBinary(TodoSchema, create(TodoSchema, { id: 't1', title: 'mine' })),
							},
						],
					},
				},
			}),
		);

		await rebase(kv, registry, noMirrors, WS, {
			changes: [],
			nextCursor: cursor(1),
			lastAckedMutationId: 0n,
		});

		expect((await mergedTodos(kv)).get('t1')?.title).toBe('mine');
	});

	it("leaves the other build's overlay rows alone", async () => {
		const kv = new MemoryKVStore();
		await enqueueForeign(kv, foreignPut(1n, 'g1'));
		// The overlay the other tab built for its own entity, in the shared store.
		const write = await kv.write();
		await write.put(overlayKey('gadget', WS, 'g1'), { v: new Uint8Array([ 1, 2, 3 ]) });
		await write.commit();

		await rebase(kv, registry, noMirrors, WS, {
			changes: [],
			nextCursor: cursor(1),
			lastAckedMutationId: 0n,
		});

		// Not cleared: this rebase does not replay the mutation that would rebuild
		// it, so dropping it would strand the tab that can read it.
		const tx = await kv.read();
		expect(await tx.get(overlayKey('gadget', WS, 'g1'))).toEqual({ v: new Uint8Array([ 1, 2, 3 ]) });
		tx.release();
	});

	it('a bootstrap swap replays past it instead of wedging the workspace', async () => {
		const kv = new MemoryKVStore();
		await enqueueForeign(kv, foreignPut(1n, 'g1'));
		await enqueueLocally(kv, putMutation(2n, { id: 't1', title: 'mine' }));

		// The path that cannot self-heal: a bootstrap runs before the push, so a
		// throw here leaves the mutation queued forever with nothing to drain it.
		const gen = await claimGen(kv);
		await stageChunk(kv, gen, serverRow('t2', 1n, { id: 't2', title: 'from server' }));
		const { committed, pendingCount } = await swap(kv, gen, {
			nextCursor: cursor(1),
			lastAckedMutationId: 0n,
		});

		expect(committed).toBe(true);
		expect(pendingCount).toBe(2);
		const todos = await mergedTodos(kv);
		expect(todos.get('t2')?.title).toBe('from server');
		expect(todos.get('t1')?.title).toBe('mine');
	});

	/**
	 * A PATCH of a REGISTERED entity naming a field only the newer build's schema
	 * has — additive schema evolution as documented, seen from the older tab. The
	 * mask cannot be applied here at all: mergeMask throws on the unknown path.
	 */
	const foreignFieldPatch = (id: bigint, key: string): Mutation =>
		create(MutationSchema, {
			clientId: 'c2',
			mutationId: id,
			payload: {
				case: 'crud',
				value: {
					entity: 'todo',
					key,
					op: CrudOp.PATCH,
					value: toBinary(TodoSchema, create(TodoSchema, {})),
					fieldMask: { paths: [ 'priority' ] },
				},
			},
		});

	/** Confirmed base every unapplicable-patch spec below patches against. */
	async function seedBase(kv: MemoryKVStore): Promise<void> {
		await rebase(kv, registry, noMirrors, WS, {
			changes: [ serverRow('t1', 1n, { id: 't1', title: 'confirmed' }) ],
			nextCursor: cursor(1),
			lastAckedMutationId: 0n,
		});
	}

	it('steps over a queued PATCH whose mask names a field it cannot resolve', async () => {
		const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
		const kv = new MemoryKVStore();
		await seedBase(kv);
		await enqueueForeign(kv, foreignFieldPatch(1n, 't1'));
		await enqueueLocally(kv, putMutation(2n, { id: 't2', title: 'mine' }));

		await rebase(kv, registry, noMirrors, WS, {
			changes: [],
			nextCursor: cursor(2),
			lastAckedMutationId: 0n,
		});

		// The page applied and the cursor advanced: one unapplicable queue entry
		// does not stop this workspace from pulling.
		const tx = await kv.read();
		expect(await tx.get(metaCursor(WS))).toEqual(cursor(2));
		expect((await readPending(tx, WS)).map((m) => m.mutationId)).toEqual([ 1n, 2n ]);
		tx.release();
		const todos = await mergedTodos(kv);
		expect(todos.get('t1')?.title).toBe('confirmed');
		expect(todos.get('t2')?.title).toBe('mine');
		expect(errSpy).toHaveBeenCalled();
		errSpy.mockRestore();
	});

	it('steps over a queued crud whose value will not decode', async () => {
		const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
		const kv = new MemoryKVStore();
		await seedBase(kv);
		await enqueueForeign(
			kv,
			create(MutationSchema, {
				clientId: 'c2',
				mutationId: 1n,
				payload: {
					case: 'crud',
					value: {
						entity: 'todo',
						key: 't1',
						op: CrudOp.PATCH,
						// Not a Todo — an incompatible message type reaching a shared queue.
						value: new Uint8Array([ 0xff, 0xff, 0xff, 0xff ]),
						fieldMask: { paths: [ 'title' ] },
					},
				},
			}),
		);
		await enqueueLocally(kv, putMutation(2n, { id: 't2', title: 'mine' }));

		await rebase(kv, registry, noMirrors, WS, {
			changes: [],
			nextCursor: cursor(2),
			lastAckedMutationId: 0n,
		});

		const todos = await mergedTodos(kv);
		expect(todos.get('t1')?.title).toBe('confirmed');
		expect(todos.get('t2')?.title).toBe('mine');
		expect(errSpy).toHaveBeenCalled();
		errSpy.mockRestore();
	});

	it('applies the other sub-mutations of a batch holding an unapplicable PATCH', async () => {
		const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
		const kv = new MemoryKVStore();
		await seedBase(kv);
		await enqueueForeign(
			kv,
			create(MutationSchema, {
				clientId: 'c2',
				mutationId: 1n,
				payload: {
					case: 'batch',
					value: {
						mutations: [
							{
								entity: 'todo',
								key: 't1',
								op: CrudOp.PATCH,
								value: toBinary(TodoSchema, create(TodoSchema, {})),
								fieldMask: { paths: [ 'priority' ] },
							},
							{
								entity: 'todo',
								key: 't2',
								op: CrudOp.PUT,
								value: toBinary(TodoSchema, create(TodoSchema, { id: 't2', title: 'sibling' })),
							},
						],
					},
				},
			}),
		);

		await rebase(kv, registry, noMirrors, WS, {
			changes: [],
			nextCursor: cursor(2),
			lastAckedMutationId: 0n,
		});

		// Only the op that could not be applied is skipped.
		const todos = await mergedTodos(kv);
		expect(todos.get('t2')?.title).toBe('sibling');
		expect(todos.get('t1')?.title).toBe('confirmed');
		expect(errSpy).toHaveBeenCalled();
		errSpy.mockRestore();
	});

	it('bootstraps past an unapplicable PATCH instead of wedging the workspace', async () => {
		const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
		const kv = new MemoryKVStore();
		await seedBase(kv);
		await enqueueForeign(kv, foreignFieldPatch(1n, 't1'));
		await enqueueLocally(kv, putMutation(2n, { id: 't2', title: 'mine' }));

		// The path that cannot self-heal: the swap runs before the push, so a throw
		// here leaves the workspace unable to bootstrap out of it.
		const gen = await claimGen(kv);
		await stageChunk(kv, gen, serverRow('t1', 5n, { id: 't1', title: 'rebootstrapped' }));
		const { committed, pendingCount } = await swap(kv, gen, {
			nextCursor: cursor(5),
			lastAckedMutationId: 0n,
		});

		expect(committed).toBe(true);
		expect(pendingCount).toBe(2);
		const todos = await mergedTodos(kv);
		expect(todos.get('t1')?.title).toBe('rebootstrapped');
		expect(todos.get('t2')?.title).toBe('mine');
		expect(errSpy).toHaveBeenCalled();
		errSpy.mockRestore();
	});

	it('steps over a custom mutation whose mirror throws', async () => {
		const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
		const kv = new MemoryKVStore();
		const mirrors: MirrorMap = {
			// App code runs inside the rebase transaction; a bug in it must not take
			// the workspace with it.
			broken: { apply: () => Promise.reject(new Error('mirror bug')) },
		};
		await enqueueForeign(
			kv,
			create(MutationSchema, {
				clientId: 'c1',
				mutationId: 1n,
				payload: { case: 'custom', value: { name: 'broken' } },
			}),
		);
		await enqueueLocally(kv, putMutation(2n, { id: 't2', title: 'mine' }));

		const { pendingCount } = await rebase(kv, registry, mirrors, WS, {
			changes: [],
			nextCursor: cursor(1),
			lastAckedMutationId: 0n,
		});

		expect(pendingCount).toBe(2);
		expect((await mergedTodos(kv)).get('t2')?.title).toBe('mine');
		expect(errSpy).toHaveBeenCalled();
		errSpy.mockRestore();
	});
});

/**
 * Claims a generation the way Coordinator.doBootstrap does: bump the marker and
 * GC the staging of every now-superseded earlier generation.
 */
async function claimGen(kv: MemoryKVStore, workspaceId: string = WS): Promise<number> {
	const tx = await kv.write();
	const next = (((await tx.get(metaBootstrapGen(workspaceId))) as number | undefined) ?? 0) + 1;
	await tx.put(metaBootstrapGen(workspaceId), next);
	await clearStaging(tx, stagingWsRoot(workspaceId));
	await tx.commit();
	return next;
}

/** Stages one chunk of a snapshot under `gen` (a streamed bootstrap chunk). */
async function stageChunk(kv: MemoryKVStore, gen: number, ...rows: RowChange[]): Promise<void> {
	await stageChunkIn(kv, WS, gen, ...rows);
}

async function stageChunkIn(
	kv: MemoryKVStore,
	workspaceId: string,
	gen: number,
	...rows: RowChange[]
): Promise<void> {
	const tx = await kv.write();
	await stageServerChanges(tx, registry, workspaceId, gen, rows);
	await tx.commit();
}

/**
 * The swap step of Coordinator.doBootstrap as one transaction: discard if
 * superseded, otherwise wipe + promote + GC + rebase. Returns committed.
 */
async function swap(
	kv: MemoryKVStore,
	gen: number,
	input: { nextCursor: Uint8Array; lastAckedMutationId: bigint },
	workspaceId: string = WS,
): Promise<{ committed: boolean; touched: TouchSet; pendingCount: number }> {
	const touched = new TouchSet();
	const tx = await kv.write();
	const active = ((await tx.get(metaBootstrapGen(workspaceId))) as number | undefined) ?? 0;
	if (active !== gen) {
		await clearStaging(tx, stagingPrefix(workspaceId, gen));
		await tx.commit();
		return { committed: false, touched, pendingCount: 0 };
	}
	await wipeConfirmedState(tx, registry, workspaceId, touched);
	await promoteStaged(tx, workspaceId, gen, touched);
	await clearStaging(tx, stagingWsRoot(workspaceId));
	const pendingCount = await finalizeRebase(
		tx,
		registry,
		noMirrors,
		workspaceId,
		input,
		touched,
	);
	await tx.commit();
	return { committed: true, touched, pendingCount };
}

describe('bootstrap staging', () => {
	it('keeps the old snapshot visible while streaming, then swaps atomically', async () => {
		const kv = new MemoryKVStore();
		// Existing confirmed state (a reset re-bootstrap, not a first load).
		await rebase(kv, registry, noMirrors, WS, {
			changes: [ serverRow('old', 1n, { id: 'old', title: 'old row' }) ],
			nextCursor: cursor(1),
			lastAckedMutationId: 0n,
		});

		const gen = await claimGen(kv);
		await stageChunk(kv, gen, serverRow('t1', 5n, { id: 't1', title: 'fresh' }));
		await stageChunk(kv, gen, serverRow('t2', 6n, { id: 't2', title: 'fresh two' }));

		// Mid-stream: staged rows are invisible; the OLD snapshot still reads.
		let todos = await mergedTodos(kv);
		expect([ ...todos.keys() ]).toEqual([ 'old' ]);

		const { committed, pendingCount } = await swap(kv, gen, {
			nextCursor: cursor(6),
			lastAckedMutationId: 0n,
		});
		expect(committed).toBe(true);
		expect(pendingCount).toBe(0);

		// After the swap the old row is gone and the snapshot is live.
		todos = await mergedTodos(kv);
		expect([ ...todos.keys() ]).toEqual([ 't1', 't2' ]);
		const read = await kv.read();
		expect(await read.get(metaCursor(WS))).toEqual(cursor(6));
		read.release();
	});

	it('replays a pending mutation issued DURING the stream onto the fresh base', async () => {
		const kv = new MemoryKVStore();
		const gen = await claimGen(kv);
		await stageChunk(kv, gen, serverRow('t1', 5n, { id: 't1', title: 'from server' }));

		// A local write lands while the snapshot is still staging (the whole point:
		// the mutex is free). It must survive the swap and shadow the server row.
		await enqueueLocally(kv, patchMutation(1n, 't1', { title: 'local edit' }, [ 'title' ]));

		const { committed, pendingCount } = await swap(kv, gen, {
			nextCursor: cursor(5),
			lastAckedMutationId: 0n,
		});
		expect(committed).toBe(true);
		expect(pendingCount).toBe(1);
		expect((await mergedTodos(kv)).get('t1')?.title).toBe('local edit');
	});

	it('discards a superseded generation and keeps the newer one intact', async () => {
		const kv = new MemoryKVStore();
		const genA = await claimGen(kv);
		await stageChunk(kv, genA, serverRow('a', 1n, { id: 'a', title: 'from A' }));

		// A second tab starts its own bootstrap mid-stream: newer generation wins.
		const genB = await claimGen(kv);
		await stageChunk(kv, genB, serverRow('b', 2n, { id: 'b', title: 'from B' }));

		// A reaches its swap first and finds itself superseded — it must not
		// install its half-snapshot, and must not disturb B's staging.
		const a = await swap(kv, genA, { nextCursor: cursor(1), lastAckedMutationId: 0n });
		expect(a.committed).toBe(false);
		const read = await kv.read();
		expect(await read.get(metaCursor(WS))).toBeUndefined();
		expect([ ...(await mergedTodos(kv)).keys() ]).toEqual([]);
		// B's staging survived A's bail.
		const bStaged: string[] = [];
		for await (const [ k ] of read.scan(stagingPrefix(WS, genB))) bStaged.push(k);
		read.release();
		expect(bStaged).toHaveLength(1);

		// B swaps and wins.
		const b = await swap(kv, genB, { nextCursor: cursor(2), lastAckedMutationId: 0n });
		expect(b.committed).toBe(true);
		expect([ ...(await mergedTodos(kv)).keys() ]).toEqual([ 'b' ]);
	});

	it('garbage-collects staging from a crashed earlier generation at swap', async () => {
		const kv = new MemoryKVStore();
		// A prior bootstrap claimed gen 1, staged, then crashed before swapping.
		const crashed = await claimGen(kv);
		await stageChunk(kv, crashed, serverRow('ghost', 1n, { id: 'ghost', title: 'leaked' }));

		// The next bootstrap streams and swaps cleanly.
		const gen = await claimGen(kv);
		await stageChunk(kv, gen, serverRow('t1', 5n, { id: 't1', title: 'fresh' }));
		await swap(kv, gen, { nextCursor: cursor(5), lastAckedMutationId: 0n });

		// No staging rows linger from either generation.
		const read = await kv.read();
		const leftover: string[] = [];
		for await (const [ k ] of read.scan(STAGING_ROOT)) leftover.push(k);
		read.release();
		expect(leftover).toEqual([]);
		expect([ ...(await mergedTodos(kv)).keys() ]).toEqual([ 't1' ]);
	});

	it('garbage-collects a crashed generation at the next claim, before any swap', async () => {
		const kv = new MemoryKVStore();
		// Several bootstrap attempts each crash mid-stream, leaving partial staging.
		const crashedA = await claimGen(kv);
		await stageChunk(kv, crashedA, serverRow('a', 1n, { id: 'a', title: 'leaked A' }));
		const crashedB = await claimGen(kv);
		await stageChunk(kv, crashedB, serverRow('b', 1n, { id: 'b', title: 'leaked B' }));
		// A third attempt claims; its claim alone must sweep both crashed predecessors.
		await claimGen(kv);

		// Accumulation cannot wait for a successful swap that may never come: the
		// crashed generations' staging is already gone, with no swap in between.
		const read = await kv.read();
		const leftover: string[] = [];
		for await (const [ k ] of read.scan(STAGING_ROOT)) leftover.push(k);
		read.release();
		expect(leftover).toEqual([]);
	});

	it('promotes staged rows whose keys contain slashes', async () => {
		const kv = new MemoryKVStore();
		const gen = await claimGen(kv);
		await stageChunk(kv, gen, serverRow('a/b/c', 1n, { id: 'a/b/c', title: 'nested' }));
		await swap(kv, gen, { nextCursor: cursor(1), lastAckedMutationId: 0n });

		const read = await kv.read();
		const bytes = await readMergedRow(read, registry, 'todo', WS, 'a/b/c');
		read.release();
		expect(bytes).toBeDefined();
		expect(fromBinary(TodoSchema, bytes!).title).toBe('nested');
	});
});

/**
 * Rows arriving over the binary codec are VIEWS into the response buffer —
 * protobuf-es never copies a `bytes` field on read — and IndexedDB stores a
 * view by structured-cloning its whole backing ArrayBuffer. A row written
 * straight off the wire therefore costs the size of the CHUNK it came in, not
 * its own, and pins that chunk on the heap for as long as the row is held.
 *
 * The bug is invisible over the JSON codec, which base64-decodes into
 * exact-size arrays, and invisible in any test that builds its rows with
 * `toBinary` per row. Both of these decode a whole chunk first, the way the
 * transport does, so the stored bytes are genuinely aliased before the assert.
 */
describe('wire bytes are detached before storage', () => {
	/** A chunk of `n` rows, decoded as one message the way the wire delivers it. */
	function decodedChunk(n: number): RowChange[] {
		const rows = Array.from(
			{ length: n },
			(_, i) => serverRow(`t${i}`, BigInt(i + 1), { id: `t${i}`, title: `title number ${i}` }),
		);
		const patch = create(PatchSchema, { changes: rows });
		return fromBinary(PatchSchema, toBinary(PatchSchema, patch)).changes;
	}

	it('confirms a pull page without retaining the page buffer', async () => {
		const changes = decodedChunk(64);
		// Precondition: the decode really did alias, or the test proves nothing.
		const wire = changes[0]!.body.value as Uint8Array;
		expect(wire.buffer.byteLength).toBeGreaterThan(wire.byteLength);

		const kv = new MemoryKVStore();
		const tx = await kv.write();
		await applyServerChanges(tx, registry, WS, changes, new TouchSet());
		await tx.commit();

		const read = await kv.read();
		const stored = await read.get(confirmedKey('todo', WS, 't0')) as { v: Uint8Array };
		read.release();
		expect(stored.v.byteLength).toBe(wire.byteLength);
		expect(stored.v.buffer.byteLength).toBe(stored.v.byteLength);
		expect(fromBinary(TodoSchema, stored.v).title).toBe('title number 0');
	});

	it('stages a bootstrap chunk without retaining the chunk buffer', async () => {
		const changes = decodedChunk(64);
		const kv = new MemoryKVStore();
		const tx = await kv.write();
		await stageServerChanges(tx, registry, WS, 1, changes);
		await tx.commit();

		const read = await kv.read();
		const staged = await read.get(stagingKey(WS, 1, 'todo', 't0')) as { v: Uint8Array };
		read.release();
		expect(staged.v.buffer.byteLength).toBe(staged.v.byteLength);
		expect(fromBinary(TodoSchema, staged.v).title).toBe('title number 0');
	});
	it('persists a pull cursor without retaining the page it arrived on', async () => {
		// A cursor is a few bytes viewing a whole page, so it has the worst
		// ratio of anything stored off the wire — and it is written on every
		// pull.
		const page = create(PullResponseSchema, {
			patch: { changes: decodedChunk(64) },
			nextCursor: new Uint8Array([ 1, 2, 3, 4 ]),
			lastAckedMutationId: 0n,
		});
		const decoded = fromBinary(PullResponseSchema, toBinary(PullResponseSchema, page));
		expect(decoded.nextCursor.buffer.byteLength).toBeGreaterThan(decoded.nextCursor.byteLength);

		const kv = new MemoryKVStore();
		await rebase(kv, registry, noMirrors, WS, {
			changes: decoded.patch!.changes,
			nextCursor: decoded.nextCursor,
			lastAckedMutationId: 0n,
		});

		const read = await kv.read();
		const stored = await read.get(metaCursor(WS)) as Uint8Array;
		read.release();
		expect(stored.buffer.byteLength).toBe(stored.byteLength);
		expect(Array.from(stored)).toEqual([ 1, 2, 3, 4 ]);
	});
});
