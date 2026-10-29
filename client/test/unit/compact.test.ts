import { create, fromBinary, type MessageInitShape, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import {
	type CrudMutation,
	CrudMutationSchema,
	CrudOp,
	type Mutation,
	MutationSchema,
} from '../../src/gen/syncplane/v1/mutation_pb.js';
import {
	confirmedKey,
	type ConfirmedRecord,
	metaLastMutationId,
	overlayKey,
} from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { EntityRegistry } from '../../src/mutations.js';
import { compactPending, planCompaction } from '../../src/optimistic/compact.js';
import { enqueuePending, readPending } from '../../src/optimistic/queue.js';
import { applyMutationLocally } from '../../src/optimistic/rebase.js';
import {
	decodeTextDoc,
	encodeFragmentFrom,
	encodeTextDoc,
	isFragment,
	mergeText,
	plainTextOf,
	TextDoc,
} from '../../src/text/index.js';
import { TouchSet } from '../../src/touch.js';

const registry = new EntityRegistry([ TodoSchema ]);
const NO_PROTECTED: ReadonlySet<bigint> = new Set();

function mutation(id: bigint, crud: CrudMutation): Mutation {
	return create(MutationSchema, {
		clientId: 'c1',
		mutationId: id,
		payload: { case: 'crud', value: crud },
	});
}

function put(
	id: bigint,
	todo: MessageInitShape<typeof TodoSchema> & { id: string },
	expectedVersion?: bigint,
): Mutation {
	return mutation(
		id,
		create(CrudMutationSchema, {
			entity: 'todo',
			key: todo.id,
			op: CrudOp.PUT,
			value: toBinary(TodoSchema, create(TodoSchema, todo)),
			expectedVersion,
		}),
	);
}

function patch(
	id: bigint,
	key: string,
	init: MessageInitShape<typeof TodoSchema>,
	paths: string[],
	expectedVersion?: bigint,
): Mutation {
	return mutation(
		id,
		create(CrudMutationSchema, {
			entity: 'todo',
			key,
			op: CrudOp.PATCH,
			value: toBinary(TodoSchema, create(TodoSchema, init)),
			fieldMask: { paths },
			expectedVersion,
		}),
	);
}

function del(id: bigint, key: string, expectedVersion?: bigint): Mutation {
	return mutation(
		id,
		create(CrudMutationSchema, { entity: 'todo', key, op: CrudOp.DELETE, expectedVersion }),
	);
}

function custom(id: bigint): Mutation {
	return create(MutationSchema, {
		clientId: 'c1',
		mutationId: id,
		payload: { case: 'custom', value: { name: 'complete_all' } },
	});
}

/**
 * Replays a queue onto a fresh overlay (optionally over seeded confirmed rows)
 * and returns the resulting overlay records. The core compaction invariant is
 * that this is identical before and after compaction.
 */
/** The workspace every spec here compacts. */
const WS = 'ws1';

async function overlayAfterReplay(
	reg: EntityRegistry,
	mutations: Mutation[],
	confirmed: Record<string, ConfirmedRecord> = {},
): Promise<Map<string, unknown>> {
	const kv = new MemoryKVStore();
	const tx = await kv.write();
	for (const [ k, v ] of Object.entries(confirmed)) await tx.put(k, v);
	for (const m of mutations) await applyMutationLocally(tx, reg, {}, WS, m, new TouchSet());
	await tx.commit();
	const rtx = await kv.read();
	const overlay = new Map<string, unknown>();
	try {
		for await (const [ k, v ] of rtx.scan('o/')) overlay.set(k, v);
	} finally {
		rtx.release();
	}
	return overlay;
}

/** Asserts the compacted plan replays to the same overlay as the original. */
async function expectSameOverlay(
	reg: EntityRegistry,
	original: Mutation[],
	compacted: Mutation[],
	confirmed: Record<string, ConfirmedRecord> = {},
): Promise<void> {
	const before = await overlayAfterReplay(reg, original, confirmed);
	const after = await overlayAfterReplay(reg, compacted, confirmed);
	expect(after).toEqual(before);
}

function ids(mutations: Mutation[]): bigint[] {
	return mutations.map((m) => m.mutationId);
}

describe('planCompaction', () => {
	it('keeps unguarded PATCHes of different fields separate', () => {
		// The server can accept one and refuse the other — a permission hook, a
		// field protected for this caller only — and it gives a folded write one
		// verdict, so a refusal would take the accepted edit with it.
		const original = [
			patch(1n, 't1', { title: 'a' }, [ 'title' ]),
			patch(2n, 't1', { completed: true }, [ 'completed' ]),
		];
		const plan = planCompaction(registry, original, 0n, NO_PROTECTED);
		expect(plan.changed).toBe(false);
		expect(ids(plan.mutations)).toEqual([ 1n, 2n ]);
	});

	it('folds unguarded PATCHes whose masks name the same fields in another order', async () => {
		const original = [
			patch(1n, 't1', { title: 'a', completed: true }, [ 'title', 'completed' ]),
			patch(2n, 't1', { title: 'b', completed: false }, [ 'completed', 'title' ]),
		];
		const plan = planCompaction(registry, original, 0n, NO_PROTECTED);

		expect(plan.mutations).toHaveLength(1);
		// Both originals renumber onto the single surviving id 1.
		expect(plan.remap.get(1n)).toBe(1n);
		expect(plan.remap.get(2n)).toBe(1n);
		await expectSameOverlay(registry, original, plan.mutations, {
			[confirmedKey('todo', WS, 't1')]: {
				v: toBinary(TodoSchema, create(TodoSchema, { id: 't1' })),
				ver: '1',
			},
		});
	});

	it('last write wins when two PATCHes touch the same field', async () => {
		const original = [
			patch(1n, 't1', { title: 'a' }, [ 'title' ]),
			patch(2n, 't1', { title: 'b' }, [ 'title' ]),
		];
		const plan = planCompaction(registry, original, 0n, NO_PROTECTED);
		const crud = plan.mutations[0]!.payload.value as CrudMutation;
		expect(fromBinary(TodoSchema, crud.value).title).toBe('b');
		await expectSameOverlay(registry, original, plan.mutations, {
			[confirmedKey('todo', WS, 't1')]: {
				v: toBinary(TodoSchema, create(TodoSchema, { id: 't1' })),
				ver: '1',
			},
		});
	});

	it('keeps unguarded writes of different ops separate', () => {
		// A refused PATCH must not cancel the PUT that created the row, and a
		// refused DELETE must not take the edits queued before it.
		const pairs = [
			[ put(1n, { id: 't1', title: 'orig' }), patch(2n, 't1', { title: 'x' }, [ 'title' ]) ],
			[ patch(1n, 't1', { title: 'x' }, [ 'title' ]), put(2n, { id: 't1', title: 'y' }) ],
			[ put(1n, { id: 't1', title: 'orig' }), del(2n, 't1') ],
			[ patch(1n, 't1', { title: 'x' }, [ 'title' ]), del(2n, 't1') ],
			[ del(1n, 't1'), put(2n, { id: 't1', title: 'back' }) ],
		];
		for (const original of pairs) {
			expect(planCompaction(registry, original, 0n, NO_PROTECTED).changed).toBe(false);
		}
	});

	it('folds repeated unguarded PUTs, and repeated DELETEs, of one row', async () => {
		const puts = [ put(1n, { id: 't1', title: 'a' }), put(2n, { id: 't1', title: 'b' }) ];
		const plan = planCompaction(registry, puts, 0n, NO_PROTECTED);
		expect(plan.mutations).toHaveLength(1);
		const crud = plan.mutations[0]!.payload.value as CrudMutation;
		expect(fromBinary(TodoSchema, crud.value).title).toBe('b');
		await expectSameOverlay(registry, puts, plan.mutations);

		const dels = [ del(1n, 't1'), del(2n, 't1') ];
		expect(planCompaction(registry, dels, 0n, NO_PROTECTED).mutations).toHaveLength(1);
	});

	it('collapses a guarded PUT and the PATCHes after it into one PUT', async () => {
		const original = [
			put(1n, { id: 't1', title: 'orig' }, 5n),
			patch(2n, 't1', { title: 'edited' }, [ 'title' ], 5n),
			patch(3n, 't1', { completed: true }, [ 'completed' ], 5n),
		];
		const plan = planCompaction(registry, original, 0n, NO_PROTECTED);
		expect(plan.mutations).toHaveLength(1);
		const crud = plan.mutations[0]!.payload.value as CrudMutation;
		expect(crud.op).toBe(CrudOp.PUT);
		const todo = fromBinary(TodoSchema, crud.value);
		expect(todo.title).toBe('edited');
		expect(todo.completed).toBe(true);
		await expectSameOverlay(registry, original, plan.mutations);
	});

	it('collapses a guarded PUT/PATCH then DELETE into one DELETE', async () => {
		const original = [
			put(1n, { id: 't1', title: 'orig' }, 5n),
			patch(2n, 't1', { title: 'x' }, [ 'title' ], 5n),
			del(3n, 't1', 5n),
		];
		const plan = planCompaction(registry, original, 0n, NO_PROTECTED);
		expect(plan.mutations).toHaveLength(1);
		expect((plan.mutations[0]!.payload.value as CrudMutation).op).toBe(CrudOp.DELETE);
		await expectSameOverlay(registry, original, plan.mutations);
	});

	it('keeps a PUT after a DELETE separate, even under one guard', async () => {
		// The PUT re-creates the row, and the server treats a PUT on a tombstone
		// differently from one on a live row: text does not union with the deleted
		// document, and protected fields do not keep their stored values. One PUT
		// would land on the live row and get the second treatment.
		const original = [ del(1n, 't1', 5n), put(2n, { id: 't1', title: 'back' }, 5n) ];
		const plan = planCompaction(registry, original, 0n, NO_PROTECTED);
		expect(plan.changed).toBe(false);
		expect(ids(plan.mutations)).toEqual([ 1n, 2n ]);
	});

	it('keeps a PATCH after a DELETE separate (delete-wins)', () => {
		const original = [ del(1n, 't1'), patch(2n, 't1', { title: 'x' }, [ 'title' ]) ];
		const plan = planCompaction(registry, original, 0n, NO_PROTECTED);
		expect(plan.mutations).toHaveLength(2);
		expect(ids(plan.mutations)).toEqual([ 1n, 2n ]);
	});

	it('compacts distinct keys independently across an intervening op', async () => {
		const original = [
			patch(1n, 't1', { title: 'a' }, [ 'title' ]),
			patch(2n, 't2', { title: 'b' }, [ 'title' ]),
			patch(3n, 't1', { title: 'c' }, [ 'title' ]),
		];
		const plan = planCompaction(registry, original, 0n, NO_PROTECTED);
		// t1's two patches merge; t2's stays. Two survivors, renumbered 1,2.
		expect(plan.mutations).toHaveLength(2);
		expect(ids(plan.mutations)).toEqual([ 1n, 2n ]);
		await expectSameOverlay(registry, original, plan.mutations, {
			[confirmedKey('todo', WS, 't1')]: {
				v: toBinary(TodoSchema, create(TodoSchema, { id: 't1' })),
				ver: '1',
			},
			[confirmedKey('todo', WS, 't2')]: {
				v: toBinary(TodoSchema, create(TodoSchema, { id: 't2' })),
				ver: '1',
			},
		});
	});

	it('never touches ids at or below pushedThrough', () => {
		const original = [
			patch(1n, 't1', { title: 'a' }, [ 'title' ]),
			patch(2n, 't1', { title: 'b' }, [ 'title' ]),
			patch(3n, 't1', { completed: true }, [ 'completed' ]),
		];
		// ids 1,2 already pushed: only id 3 is compactable (a lone survivor → no change).
		const plan = planCompaction(registry, original, 2n, NO_PROTECTED);
		expect(plan.changed).toBe(false);
		expect(ids(plan.mutations)).toEqual([ 1n, 2n, 3n ]);
	});

	it('merges only the never-pushed tail and renumbers it contiguously', () => {
		const original = [
			patch(1n, 't1', { title: 'a' }, [ 'title' ]), // pushed
			patch(2n, 't1', { title: 'b' }, [ 'title' ]), // tail
			patch(3n, 't1', { title: 'c' }, [ 'title' ]), // tail
			patch(4n, 't2', { title: 'd' }, [ 'title' ]), // tail
		];
		const plan = planCompaction(registry, original, 1n, NO_PROTECTED);
		// id 1 stays; ids 2,3 (same key) merge → new id 2; id 4 → new id 3.
		expect(ids(plan.mutations)).toEqual([ 1n, 2n, 3n ]);
		expect(plan.remap.get(2n)).toBe(2n);
		expect(plan.remap.get(3n)).toBe(2n);
		expect(plan.remap.get(4n)).toBe(3n);
	});

	it('treats a custom mutation as a hard barrier across all keys', () => {
		const original = [
			patch(1n, 't1', { title: 'a' }, [ 'title' ]),
			custom(2n),
			patch(3n, 't1', { completed: true }, [ 'completed' ]),
		];
		const plan = planCompaction(registry, original, 0n, NO_PROTECTED);
		expect(plan.changed).toBe(false);
		expect(ids(plan.mutations)).toEqual([ 1n, 2n, 3n ]);
	});

	it('folds same-key mutations that carry the SAME compare-and-swap guard', async () => {
		// The typing case: every edit is cut against the same confirmed version,
		// because only a pull can move it. Unfolded, the second would reach the
		// server after the first had already bumped the row past the guard.
		const original = [
			patch(1n, 't1', { title: 'a' }, [ 'title' ], 5n),
			patch(2n, 't1', { completed: true }, [ 'completed' ], 5n),
			patch(3n, 't1', { title: 'c' }, [ 'title' ], 5n),
		];
		const plan = planCompaction(registry, original, 0n, NO_PROTECTED);
		expect(plan.mutations).toHaveLength(1);
		const folded = plan.mutations[0]!.payload.value as CrudMutation;
		expect(folded.expectedVersion).toBe(5n);
		expect(folded.fieldMask?.paths.sort()).toEqual([ 'completed', 'title' ]);
		await expectSameOverlay(registry, original, plan.mutations);
	});

	it('keeps same-key mutations with DIFFERENT guards separate', () => {
		// Distinct guards are distinct intents — the caller learned a new version
		// between the two — and only one of them can be the folded slot's.
		const original = [
			patch(1n, 't1', { title: 'a' }, [ 'title' ], 5n),
			patch(2n, 't1', { completed: true }, [ 'completed' ], 6n),
		];
		const plan = planCompaction(registry, original, 0n, NO_PROTECTED);
		expect(plan.changed).toBe(false);
		expect(ids(plan.mutations)).toEqual([ 1n, 2n ]);
	});

	it('keeps a guarded and an unguarded mutation on one key separate', () => {
		// Folding either way rewrites what the caller asked for: onto the guard,
		// and the unguarded write gains a rejection it never had; off it, and an
		// explicit guard is silently dropped.
		const guardedFirst = [
			patch(1n, 't1', { title: 'a' }, [ 'title' ], 5n),
			patch(2n, 't1', { completed: true }, [ 'completed' ]),
		];
		expect(planCompaction(registry, guardedFirst, 0n, NO_PROTECTED).changed).toBe(false);

		const unguardedFirst = [
			patch(1n, 't1', { title: 'a' }, [ 'title' ]),
			patch(2n, 't1', { completed: true }, [ 'completed' ], 5n),
		];
		expect(planCompaction(registry, unguardedFirst, 0n, NO_PROTECTED).changed).toBe(false);
	});

	it('carries the guard through a fold that changes op', async () => {
		const original = [
			put(1n, { id: 't1', title: 'x' }, 5n),
			patch(2n, 't1', { title: 'y' }, [ 'title' ], 5n),
		];
		const plan = planCompaction(registry, original, 0n, NO_PROTECTED);
		expect(plan.mutations).toHaveLength(1);
		expect((plan.mutations[0]!.payload.value as CrudMutation).expectedVersion).toBe(5n);
		await expectSameOverlay(registry, original, plan.mutations);

		const toDelete = [
			patch(1n, 't1', { title: 'x' }, [ 'title' ], 5n),
			del(2n, 't1', 5n),
		];
		const deletePlan = planCompaction(registry, toDelete, 0n, NO_PROTECTED);
		expect(deletePlan.mutations).toHaveLength(1);
		expect((deletePlan.mutations[0]!.payload.value as CrudMutation).expectedVersion).toBe(5n);
	});

	it('treats undo-referenced mutations as barriers (but still renumbers them)', () => {
		const original = [
			patch(1n, 't1', { title: 'a' }, [ 'title' ]),
			patch(2n, 't1', { completed: true }, [ 'completed' ]), // protected
			patch(3n, 't1', { title: 'c' }, [ 'title' ]),
		];
		// id 2 protected → cannot merge into id 1; id 3 cannot merge into id 2.
		// Nothing merges, so nothing changes.
		const plan = planCompaction(registry, original, 0n, new Set([ 2n ]));
		expect(plan.changed).toBe(false);

		// With a drop earlier in the tail, the protected id is renumbered and the
		// remap reflects it.
		const withDrop = [
			put(1n, { id: 't2', title: 'x' }),
			put(2n, { id: 't2', title: 'y' }), // merges with id 1 → one PUT
			patch(3n, 't1', { title: 'c' }, [ 'title' ]), // protected, survives, renumbered
		];
		const plan2 = planCompaction(registry, withDrop, 0n, new Set([ 3n ]));
		expect(plan2.mutations).toHaveLength(2);
		expect(ids(plan2.mutations)).toEqual([ 1n, 2n ]);
		expect(plan2.remap.get(3n)).toBe(2n);
	});

	it('does not merge a PATCH that touches a key field into a PUT', () => {
		const original = [
			put(1n, { id: 't1', title: 'x' }, 5n),
			patch(2n, 't1', { id: 't1' }, [ 'id' ], 5n), // mask touches the key field
		];
		const plan = planCompaction(registry, original, 0n, NO_PROTECTED);
		expect(plan.mutations).toHaveLength(2);
	});

	it('reports no change when the tail cannot be compacted', () => {
		const original = [ put(1n, { id: 't1' }), put(2n, { id: 't2' }) ];
		const plan = planCompaction(registry, original, 0n, NO_PROTECTED);
		expect(plan.changed).toBe(false);
	});

	it('leaves the queue it planned from untouched', () => {
		// compactPending deletes the old pending keys through the very array it
		// handed over, so a plan that renumbered in place would strand whatever
		// it moved — under an id nothing ever deletes.
		const original = [
			put(1n, { id: 't1', title: 'a' }),
			put(2n, { id: 't1', title: 'b' }),
			custom(3n),
		];
		const plan = planCompaction(registry, original, 0n, NO_PROTECTED);

		expect(plan.changed).toBe(true);
		expect(ids(plan.mutations)).toEqual([ 1n, 2n ]);
		expect(ids(original)).toEqual([ 1n, 2n, 3n ]);
	});
});

describe('planCompaction with mergers', () => {
	const widgetRegistry = (): EntityRegistry => {
		const reg = new EntityRegistry([ WidgetSchema ]);
		// Last-writer-wins byte merger on an opaque field.
		reg.setMergers([ { schema: WidgetSchema, field: 'doc', merge: (_a, b) => b } ]);
		return reg;
	};

	function widgetPut(
		id: bigint,
		init: MessageInitShape<typeof WidgetSchema> & { id: string },
		expectedVersion?: bigint,
	): Mutation {
		return mutation(
			id,
			create(CrudMutationSchema, {
				entity: 'widget',
				key: init.id,
				op: CrudOp.PUT,
				value: toBinary(WidgetSchema, create(WidgetSchema, init)),
				expectedVersion,
			}),
		);
	}

	function widgetPatch(
		id: bigint,
		key: string,
		init: MessageInitShape<typeof WidgetSchema>,
		paths: string[],
		expectedVersion?: bigint,
	): Mutation {
		return mutation(
			id,
			create(CrudMutationSchema, {
				entity: 'widget',
				key,
				op: CrudOp.PATCH,
				value: toBinary(WidgetSchema, create(WidgetSchema, init)),
				fieldMask: { paths },
				expectedVersion,
			}),
		);
	}

	it('does not merge PATCH into PATCH on a merger entity (associativity hazard)', () => {
		const reg = widgetRegistry();
		const original = [
			widgetPatch(1n, 'w1', { label: 'a' }, [ 'label' ]),
			widgetPatch(2n, 'w1', { label: 'b' }, [ 'label' ]),
		];
		const plan = planCompaction(reg, original, 0n, NO_PROTECTED);
		expect(plan.changed).toBe(false);
		expect(plan.mutations).toHaveLength(2);
	});

	it('still absorbs a PATCH into a PUT on a merger entity (single merge is safe)', async () => {
		const reg = widgetRegistry();
		const original = [
			widgetPut(1n, { id: 'w1', label: 'orig', count: 1 }, 5n),
			widgetPatch(2n, 'w1', { count: 9 }, [ 'count' ], 5n),
		];
		const plan = planCompaction(reg, original, 0n, NO_PROTECTED);
		expect(plan.mutations).toHaveLength(1);
		expect((plan.mutations[0]!.payload.value as CrudMutation).op).toBe(CrudOp.PUT);
		await expectSameOverlay(reg, original, plan.mutations);
	});
});

describe('compactPending', () => {
	async function seedQueue(mutations: Mutation[]): Promise<MemoryKVStore> {
		const kv = new MemoryKVStore();
		const tx = await kv.write();
		for (const m of mutations) await enqueuePending(tx, WS, m);
		await tx.put(metaLastMutationId(WS), mutations[mutations.length - 1]!.mutationId.toString());
		await tx.commit();
		return kv;
	}

	it('rewrites the queue and lowers the mutation-id counter', async () => {
		const kv = await seedQueue([
			patch(1n, 't1', { title: 'a' }, [ 'title' ]),
			patch(2n, 't1', { title: 'b' }, [ 'title' ]),
			patch(3n, 't1', { title: 'c' }, [ 'title' ]),
		]);
		const result = await compactPending(kv, registry, {}, WS, () => NO_PROTECTED);
		expect(result.changed).toBe(true);
		expect(result.pendingCount).toBe(1);

		const tx = await kv.read();
		try {
			const pending = await readPending(tx, WS);
			expect(ids(pending)).toEqual([ 1n ]);
			expect(await tx.get(metaLastMutationId(WS))).toBe('1');
		} finally {
			tx.release();
		}
	});

	it('deletes the old key of a barrier the renumbering moved', async () => {
		const kv = await seedQueue([
			put(1n, { id: 't1', title: 'a' }),
			put(2n, { id: 't1', title: 'b' }),
			custom(3n),
		]);
		const result = await compactPending(kv, registry, {}, WS, () => NO_PROTECTED);
		expect(result.changed).toBe(true);
		expect(result.pendingCount).toBe(2);

		const tx = await kv.read();
		try {
			// An entry left behind at id 3 is the custom mutation a SECOND time,
			// under an id the server has never seen — so the watermark cannot
			// dedupe it and the mutator runs twice.
			expect(ids(await readPending(tx, WS))).toEqual([ 1n, 2n ]);
			expect(await tx.get(metaLastMutationId(WS))).toBe('2');
		} finally {
			tx.release();
		}
	});

	it('is a no-op (no write) when nothing compacts', async () => {
		const kv = await seedQueue([ put(1n, { id: 't1' }), put(2n, { id: 't2' }) ]);
		const result = await compactPending(kv, registry, {}, WS, () => NO_PROTECTED);
		expect(result.changed).toBe(false);
		expect(result.pendingCount).toBe(2);

		const tx = await kv.read();
		try {
			expect(ids(await readPending(tx, WS))).toEqual([ 1n, 2n ]);
			expect(await tx.get(metaLastMutationId(WS))).toBe('2');
		} finally {
			tx.release();
		}
	});
});

describe('text field compaction', () => {
	const widgetRegistry = new EntityRegistry([ WidgetSchema ]);

	function widgetBodyPatch(
		id: bigint,
		key: string,
		body: Uint8Array,
		expectedVersion?: bigint,
	): Mutation {
		return mutation(
			id,
			create(CrudMutationSchema, {
				entity: 'widget',
				key,
				op: CrudOp.PATCH,
				value: toBinary(WidgetSchema, create(WidgetSchema, { body })),
				fieldMask: { paths: [ 'body' ] },
				expectedVersion,
			}),
		);
	}

	it('folds successive text PATCHes through the auto-installed merger', async () => {
		// Two whole-document states from one typing session: the second is a
		// superset of the first, so the fold must merge (union) rather than
		// last-write-replace — either way here the union IS the later state,
		// and the overlay-equivalence check proves the folded queue replays
		// to the same result.
		const d = new TextDoc('c1');
		d.insert(0, 'hello');
		const s1 = encodeTextDoc(d);
		d.insert(5, ' world');
		const s2 = encodeTextDoc(d);

		const original = [ widgetBodyPatch(1n, 'w1', s1), widgetBodyPatch(2n, 'w1', s2) ];
		const plan = planCompaction(widgetRegistry, original, 0n, NO_PROTECTED);

		expect(plan.changed).toBe(true);
		expect(plan.mutations).toHaveLength(1);
		const crud = plan.mutations[0]!.payload.value as CrudMutation;
		expect(crud.op).toBe(CrudOp.PATCH);
		expect(crud.fieldMask?.paths).toEqual([ 'body' ]);
		const folded = fromBinary(WidgetSchema, crud.value);
		expect(plainTextOf(folded.body)).toBe('hello world');
		// The folded state is the canonical merge of both inputs.
		expect(folded.body).toEqual(mergeText(s1, s2));

		await expectSameOverlay(widgetRegistry, original, plan.mutations, {
			[confirmedKey('widget', WS, 'w1')]: {
				v: toBinary(WidgetSchema, create(WidgetSchema, { id: 'w1' })),
				ver: '1',
			},
		});
	});

	// A live text handle queues the fragment for each edit, so a typing burst
	// reaches compaction as a run of deltas rather than a run of documents.
	// Folding them has to union the deltas — the folded slot stands in for every
	// edit it absorbed, against the same confirmed row they were all cut from.
	describe('queued delta state', () => {
		/** Confirmed 'hello', then two keystrokes queued as fragments from `firstId`. */
		function burst(
			firstId = 1n,
			expectedVersion?: bigint,
		): { confirmed: Uint8Array; queue: Mutation[]; final: Uint8Array } {
			const base = new TextDoc('c1');
			base.insert(0, 'hello');
			const confirmed = encodeTextDoc(base);

			const doc = decodeTextDoc(confirmed, 'c1');
			const first = doc.insert(5, '!');
			const f1 = encodeFragmentFrom(doc, [ { peer: first.peer, counter: first.start } ], []);
			const second = doc.insert(6, '?');
			const f2 = encodeFragmentFrom(doc, [ { peer: second.peer, counter: second.start } ], []);

			return {
				confirmed,
				queue: [
					widgetBodyPatch(firstId, 'w1', f1, expectedVersion),
					widgetBodyPatch(firstId + 1n, 'w1', f2, expectedVersion),
				],
				final: encodeTextDoc(doc),
			};
		}

		it('folds a typing burst into one mutation carrying the union of its deltas', async () => {
			const { confirmed, queue, final } = burst();
			const plan = planCompaction(widgetRegistry, queue, 0n, NO_PROTECTED);

			expect(plan.changed).toBe(true);
			expect(plan.mutations).toHaveLength(1);
			const crud = plan.mutations[0]!.payload.value as CrudMutation;
			const folded = fromBinary(WidgetSchema, crud.value).body;
			// Still delta state, and applying it to the confirmed row reaches
			// exactly where applying both keystrokes in turn would have.
			expect(isFragment(folded)).toBe(true);
			expect(mergeText(confirmed, folded)).toEqual(final);

			await expectSameOverlay(widgetRegistry, queue, plan.mutations, {
				[confirmedKey('widget', WS, 'w1')]: {
					v: toBinary(WidgetSchema, create(WidgetSchema, { id: 'w1', body: confirmed })),
					ver: '1',
				},
			});
		});

		it('absorbs a delta into a PUT queued before it under the same guard', async () => {
			const { confirmed, queue, final } = burst(2n, 5n);
			const put = mutation(
				1n,
				create(CrudMutationSchema, {
					entity: 'widget',
					key: 'w1',
					op: CrudOp.PUT,
					value: toBinary(WidgetSchema, create(WidgetSchema, { id: 'w1', body: confirmed })),
					expectedVersion: 5n,
				}),
			);
			const plan = planCompaction(widgetRegistry, [ put, ...queue ], 0n, NO_PROTECTED);

			expect(plan.mutations).toHaveLength(1);
			const crud = plan.mutations[0]!.payload.value as CrudMutation;
			expect(crud.op).toBe(CrudOp.PUT);
			// A PUT carries a whole row, so the deltas expand into it.
			const body = fromBinary(WidgetSchema, crud.value).body;
			expect(isFragment(body)).toBe(false);
			expect(body).toEqual(final);
		});

		it('keeps a whole document queued after a delta as its own mutation', async () => {
			// Nothing here knows the base the fragment was cut against, so the two
			// cannot be folded — one unfolded slot, never a wrong merge.
			const { queue } = burst();
			const whole = new TextDoc('c1');
			whole.insert(0, 'rewritten');
			const original = [ queue[0]!, widgetBodyPatch(2n, 'w1', encodeTextDoc(whole)) ];
			const plan = planCompaction(widgetRegistry, original, 0n, NO_PROTECTED);
			expect(plan.mutations).toHaveLength(2);
		});
	});

	// A PUT unions each text document with the stored one and keeps a stored one
	// it carries no bytes for, on the server and in the overlay. A fold that
	// ends in a PUT has to reach the same row as the sequential apply.
	describe('folds that end in a PUT', () => {
		const confirmedDoc = new TextDoc('c1');
		confirmedDoc.insert(0, 'base');
		const baseBody = encodeTextDoc(confirmedDoc);
		const typedDoc = decodeTextDoc(baseBody, 'c1');
		typedDoc.insert(4, ' typed offline');
		const typedBody = encodeTextDoc(typedDoc);

		const confirmed = {
			[confirmedKey('widget', WS, 'w1')]: {
				v: toBinary(WidgetSchema, create(WidgetSchema, { id: 'w1', label: 'base', body: baseBody })),
				ver: '3',
			},
		};

		function widgetPut(
			id: bigint,
			init: MessageInitShape<typeof WidgetSchema>,
			expectedVersion?: bigint,
		): Mutation {
			return mutation(
				id,
				create(CrudMutationSchema, {
					entity: 'widget',
					key: 'w1',
					op: CrudOp.PUT,
					value: toBinary(WidgetSchema, create(WidgetSchema, { id: 'w1', ...init })),
					expectedVersion,
				}),
			);
		}

		async function overlayBody(mutations: Mutation[]): Promise<string> {
			const overlay = await overlayAfterReplay(widgetRegistry, mutations, confirmed);
			const record = overlay.get(overlayKey('widget', WS, 'w1')) as { v: Uint8Array } | undefined;
			expect(record).toBeDefined();
			return plainTextOf(fromBinary(WidgetSchema, record!.v).body);
		}

		it('keeps the typed text when the later PUT carries no document', async () => {
			const original = [
				widgetPut(1n, { label: 'a', body: typedBody }),
				widgetPut(2n, { label: 'b' }),
			];
			const plan = planCompaction(widgetRegistry, original, 0n, NO_PROTECTED);
			expect(plan.mutations).toHaveLength(1);
			expect(await overlayBody(original)).toBe('base typed offline');
			await expectSameOverlay(widgetRegistry, original, plan.mutations, confirmed);
		});

		it('keeps the typed text when the later PUT carries a stale document', async () => {
			// `put({ ...row, label })` from a row read before the first PUT.
			const original = [
				widgetPut(1n, { label: 'a', body: typedBody }),
				widgetPut(2n, { label: 'b', body: baseBody }),
			];
			const plan = planCompaction(widgetRegistry, original, 0n, NO_PROTECTED);
			expect(plan.mutations).toHaveLength(1);
			expect(await overlayBody(original)).toBe('base typed offline');
			await expectSameOverlay(widgetRegistry, original, plan.mutations, confirmed);
		});

		it('keeps a text PATCH apart from a PUT queued after it under the same guard', async () => {
			const original = [
				widgetBodyPatch(1n, 'w1', typedBody, 3n),
				widgetPut(2n, { label: 'b', body: baseBody }, 3n),
			];
			const plan = planCompaction(widgetRegistry, original, 0n, NO_PROTECTED);
			expect(plan.mutations).toHaveLength(2);
			expect(await overlayBody(plan.mutations)).toBe('base typed offline');
		});

		it('keeps a PATCH that clears the document apart from the PUT before it', async () => {
			// Folded, the clear would be a PUT without a document, which keeps
			// the confirmed one.
			const original = [
				widgetPut(1n, { label: 'a', body: typedBody }, 3n),
				widgetBodyPatch(2n, 'w1', new Uint8Array(), 3n),
			];
			const plan = planCompaction(widgetRegistry, original, 0n, NO_PROTECTED);
			expect(plan.mutations).toHaveLength(2);
			expect(await overlayBody(plan.mutations)).toBe('');
		});
	});

	// A PATCH that clears a text document replaces it, while a PATCH that
	// carries text unions with what is stored. The overlay keeps the cleared
	// document's version vector, as the server does, so text typed afterwards
	// gets ids the cleared text did not use.
	describe('a PATCH that clears the document', () => {
		const confirmedDoc = new TextDoc('c1');
		confirmedDoc.insert(0, 'old');
		const confirmed = {
			[confirmedKey('widget', WS, 'w1')]: {
				v: toBinary(
					WidgetSchema,
					create(WidgetSchema, { id: 'w1', body: encodeTextDoc(confirmedDoc) }),
				),
				ver: '3',
			},
		};
		const clear = widgetBodyPatch(1n, 'w1', new Uint8Array());

		async function overlayBody(mutations: Mutation[]): Promise<Uint8Array | undefined> {
			const overlay = await overlayAfterReplay(widgetRegistry, mutations, confirmed);
			const record = overlay.get(overlayKey('widget', WS, 'w1')) as { v: Uint8Array } | undefined;
			expect(record).toBeDefined();
			return fromBinary(WidgetSchema, record!.v).body;
		}

		it('keeps the version vector of the text it cleared', async () => {
			const body = await overlayBody([ clear ]);
			expect(plainTextOf(body)).toBe('');
			expect(decodeTextDoc(body, '').versionVector()).toEqual(confirmedDoc.versionVector());
		});

		it('stays apart from a text PATCH queued after it', async () => {
			const typed = decodeTextDoc(await overlayBody([ clear ]), 'c1');
			typed.insert(0, 'new');
			const original = [ clear, widgetBodyPatch(2n, 'w1', encodeTextDoc(typed)) ];

			const plan = planCompaction(widgetRegistry, original, 0n, NO_PROTECTED);
			expect(plan.mutations).toHaveLength(2);
			expect(plainTextOf(await overlayBody(plan.mutations))).toBe('new');
		});
	});

	it('folds divergent text states into their union', async () => {
		// A remote state merged in between two local patches: the fold must
		// keep both sides' items.
		const base = new TextDoc('c1');
		base.insert(0, 'abc');
		const s1 = encodeTextDoc(base);
		const remote = decodeTextDoc(s1, 'other');
		remote.insert(3, 'X');
		const local = decodeTextDoc(s1, 'c1');
		local.insert(0, 'Y');
		const merged = mergeText(encodeTextDoc(local), encodeTextDoc(remote));

		const original = [
			widgetBodyPatch(1n, 'w1', encodeTextDoc(local)),
			widgetBodyPatch(2n, 'w1', merged),
		];
		const plan = planCompaction(widgetRegistry, original, 0n, NO_PROTECTED);
		expect(plan.mutations).toHaveLength(1);
		const crud = plan.mutations[0]!.payload.value as CrudMutation;
		expect(plainTextOf(fromBinary(WidgetSchema, crud.value).body)).toBe('YabcX');
	});
});
