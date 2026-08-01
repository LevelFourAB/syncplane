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
import { confirmedKey, type ConfirmedRecord, metaLastMutationId } from '../../src/keys.js';
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
	it('merges successive PATCHes to the same key into one, unioning masks', async () => {
		const original = [
			patch(1n, 't1', { title: 'a' }, [ 'title' ]),
			patch(2n, 't1', { completed: true }, [ 'completed' ]),
		];
		const plan = planCompaction(registry, original, 0n, NO_PROTECTED);

		expect(plan.changed).toBe(true);
		expect(plan.mutations).toHaveLength(1);
		const crud = plan.mutations[0]!.payload.value as CrudMutation;
		expect(crud.op).toBe(CrudOp.PATCH);
		expect(new Set(crud.fieldMask?.paths)).toEqual(new Set([ 'title', 'completed' ]));
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

	it('collapses PUT followed by PATCHes into one PUT', async () => {
		const original = [
			put(1n, { id: 't1', title: 'orig' }),
			patch(2n, 't1', { title: 'edited' }, [ 'title' ]),
			patch(3n, 't1', { completed: true }, [ 'completed' ]),
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

	it('collapses PUT/PATCH then DELETE into one DELETE', async () => {
		const original = [
			put(1n, { id: 't1', title: 'orig' }),
			patch(2n, 't1', { title: 'x' }, [ 'title' ]),
			del(3n, 't1'),
		];
		const plan = planCompaction(registry, original, 0n, NO_PROTECTED);
		expect(plan.mutations).toHaveLength(1);
		expect((plan.mutations[0]!.payload.value as CrudMutation).op).toBe(CrudOp.DELETE);
		await expectSameOverlay(registry, original, plan.mutations);
	});

	it('collapses DELETE then PUT into one resurrecting PUT', async () => {
		const original = [ del(1n, 't1'), put(2n, { id: 't1', title: 'back' }) ];
		const plan = planCompaction(registry, original, 0n, NO_PROTECTED);
		expect(plan.mutations).toHaveLength(1);
		expect((plan.mutations[0]!.payload.value as CrudMutation).op).toBe(CrudOp.PUT);
		await expectSameOverlay(registry, original, plan.mutations, {
			[confirmedKey('todo', WS, 't1')]: {
				v: toBinary(TodoSchema, create(TodoSchema, { id: 't1', title: 'old' })),
				ver: '1',
			},
		});
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
			patch(3n, 't1', { completed: true }, [ 'completed' ]),
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
			patch(3n, 't1', { completed: true }, [ 'completed' ]), // tail
			patch(4n, 't2', { title: 'c' }, [ 'title' ]), // tail
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

	it('treats a compare-and-swap guard as a barrier', () => {
		const original = [
			patch(1n, 't1', { title: 'a' }, [ 'title' ]),
			patch(2n, 't1', { completed: true }, [ 'completed' ], 5n), // expectedVersion set
			patch(3n, 't1', { title: 'c' }, [ 'title' ]),
		];
		const plan = planCompaction(registry, original, 0n, NO_PROTECTED);
		expect(plan.changed).toBe(false);
		expect(ids(plan.mutations)).toEqual([ 1n, 2n, 3n ]);
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
			del(2n, 't2'), // merges with id 1 → one DELETE
			patch(3n, 't1', { title: 'c' }, [ 'title' ]), // protected, survives, renumbered
		];
		const plan2 = planCompaction(registry, withDrop, 0n, new Set([ 3n ]));
		expect(plan2.mutations).toHaveLength(2);
		expect(ids(plan2.mutations)).toEqual([ 1n, 2n ]);
		expect(plan2.remap.get(3n)).toBe(2n);
	});

	it('does not merge a PATCH that touches a key field into a PUT', () => {
		const original = [
			put(1n, { id: 't1', title: 'x' }),
			patch(2n, 't1', { id: 't1' }, [ 'id' ]), // mask touches the key field
		];
		const plan = planCompaction(registry, original, 0n, NO_PROTECTED);
		expect(plan.mutations).toHaveLength(2);
	});

	it('reports no change when the tail cannot be compacted', () => {
		const original = [ put(1n, { id: 't1' }), put(2n, { id: 't2' }) ];
		const plan = planCompaction(registry, original, 0n, NO_PROTECTED);
		expect(plan.changed).toBe(false);
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
	): Mutation {
		return mutation(
			id,
			create(CrudMutationSchema, {
				entity: 'widget',
				key: init.id,
				op: CrudOp.PUT,
				value: toBinary(WidgetSchema, create(WidgetSchema, init)),
			}),
		);
	}

	function widgetPatch(
		id: bigint,
		key: string,
		init: MessageInitShape<typeof WidgetSchema>,
		paths: string[],
	): Mutation {
		return mutation(
			id,
			create(CrudMutationSchema, {
				entity: 'widget',
				key,
				op: CrudOp.PATCH,
				value: toBinary(WidgetSchema, create(WidgetSchema, init)),
				fieldMask: { paths },
			}),
		);
	}

	it('does not merge PATCH into PATCH on a merger entity (associativity hazard)', () => {
		const reg = widgetRegistry();
		const original = [
			widgetPatch(1n, 'w1', { label: 'a' }, [ 'label' ]),
			widgetPatch(2n, 'w1', { count: 7 }, [ 'count' ]),
		];
		const plan = planCompaction(reg, original, 0n, NO_PROTECTED);
		expect(plan.changed).toBe(false);
		expect(plan.mutations).toHaveLength(2);
	});

	it('still absorbs a PATCH into a PUT on a merger entity (single merge is safe)', async () => {
		const reg = widgetRegistry();
		const original = [
			widgetPut(1n, { id: 'w1', label: 'orig', count: 1 }),
			widgetPatch(2n, 'w1', { count: 9 }, [ 'count' ]),
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
			patch(2n, 't1', { completed: true }, [ 'completed' ]),
			patch(3n, 't1', { title: 'c' }, [ 'title' ]),
		]);
		const result = await compactPending(kv, registry, WS, () => NO_PROTECTED);
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

	it('is a no-op (no write) when nothing compacts', async () => {
		const kv = await seedQueue([ put(1n, { id: 't1' }), put(2n, { id: 't2' }) ]);
		const result = await compactPending(kv, registry, WS, () => NO_PROTECTED);
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

	function widgetBodyPatch(id: bigint, key: string, body: Uint8Array): Mutation {
		return mutation(
			id,
			create(CrudMutationSchema, {
				entity: 'widget',
				key,
				op: CrudOp.PATCH,
				value: toBinary(WidgetSchema, create(WidgetSchema, { body })),
				fieldMask: { paths: [ 'body' ] },
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
					widgetBodyPatch(firstId, 'w1', f1),
					widgetBodyPatch(firstId + 1n, 'w1', f2),
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

		it('absorbs a delta into a PUT queued before it', async () => {
			const { confirmed, queue, final } = burst(2n);
			const put = mutation(
				1n,
				create(CrudMutationSchema, {
					entity: 'widget',
					key: 'w1',
					op: CrudOp.PUT,
					value: toBinary(WidgetSchema, create(WidgetSchema, { id: 'w1', body: confirmed })),
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
