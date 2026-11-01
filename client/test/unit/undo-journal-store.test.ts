// The undo journal (optimistic/undojournal.ts) is how one tab's stacks learn
// what another tab handled. Every transaction that drops a dead mutation must
// write its tokens there, and the record must stay bounded.

import { create } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import {
	CrudMutationSchema,
	CrudOp,
	type Mutation,
	MutationSchema,
} from '../../src/gen/syncplane/v1/mutation_pb.js';
import { metaLastMutationId } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { EntityRegistry } from '../../src/mutations.js';
import { enqueuePending, readPending, writeUndoTokens } from '../../src/optimistic/queue.js';
import { dropAndResequencePending, rollbackRejected } from '../../src/optimistic/rebase.js';
import {
	DEAD_TOKEN_TTL_MS,
	MAX_DEAD_TOKENS,
	noteDeadUndoTokens,
	readUndoJournal,
	type UndoJournal,
	voidsToken,
	writeUndoBarrier,
} from '../../src/optimistic/undojournal.js';

const WS = 'ws';
const registry = new EntityRegistry([ TodoSchema ]);

function patch(id: bigint, key: string): Mutation {
	return create(MutationSchema, {
		clientId: 'c1',
		mutationId: id,
		payload: {
			case: 'crud',
			value: create(CrudMutationSchema, {
				entity: 'todo',
				key,
				op: CrudOp.PATCH,
				fieldMask: { paths: [ 'title' ] },
			}),
		},
	});
}

async function seed(entries: [Mutation, bigint[]][]): Promise<MemoryKVStore> {
	const kv = new MemoryKVStore();
	const tx = await kv.write();
	for (const [ m, tokens ] of entries) {
		await enqueuePending(tx, WS, m);
		await writeUndoTokens(tx, WS, m.mutationId, tokens);
	}
	await tx.put(metaLastMutationId(WS), entries[entries.length - 1]![0].mutationId.toString());
	await tx.commit();
	return kv;
}

async function journal(kv: MemoryKVStore): Promise<UndoJournal> {
	const tx = await kv.read();
	try {
		return await readUndoJournal(tx, WS);
	} finally {
		tx.release();
	}
}

async function write(
	kv: MemoryKVStore,
	fn: (tx: Awaited<ReturnType<MemoryKVStore['write']>>) => Promise<void>,
) {
	const tx = await kv.write();
	await fn(tx);
	await tx.commit();
}

describe('the undo journal', () => {
	it('records the tokens of a rejected mutation', async () => {
		const kv = await seed([ [ patch(1n, 't1'), [ 10n, 11n ] ], [ patch(2n, 't2'), [ 20n ] ] ]);
		await rollbackRejected(kv, registry, {}, WS, [ 1n ]);
		expect((await journal(kv)).dead).toEqual(new Set([ 10n, 11n ]));
	});

	it('records the tokens of a mutation dropped as poison', async () => {
		const kv = await seed([ [ patch(1n, 't1'), [ 10n ] ], [ patch(2n, 't2'), [ 20n ] ] ]);
		const [ head ] = await (async () => {
			const tx = await kv.read();
			try {
				return await readPending(tx, WS);
			} finally {
				tx.release();
			}
		})();
		expect((await dropAndResequencePending(kv, registry, {}, WS, head!)).dropped).toBe(true);
		expect((await journal(kv)).dead).toEqual(new Set([ 10n ]));
	});

	it('writes nothing for a mutation without tokens', async () => {
		const kv = await seed([ [ patch(1n, 't1'), [] ] ]);
		await rollbackRejected(kv, registry, {}, WS, [ 1n ]);
		expect(await journal(kv)).toEqual({ barrier: 0n, dead: new Set() });
	});

	it('voids tokens below the barrier and dead tokens, and nothing else', async () => {
		const kv = new MemoryKVStore();
		await write(kv, async (tx) => {
			await writeUndoBarrier(tx, WS, 100n);
			await noteDeadUndoTokens(tx, WS, [ 150n ]);
		});
		const j = await journal(kv);
		expect(voidsToken(j, 99n)).toBe(true);
		expect(voidsToken(j, 100n)).toBe(false);
		expect(voidsToken(j, 150n)).toBe(true);
		expect(voidsToken(j, 151n)).toBe(false);
	});

	it('prunes dead tokens below the barrier', async () => {
		const kv = new MemoryKVStore();
		await write(kv, async (tx) => {
			await noteDeadUndoTokens(tx, WS, [ 5n ]);
			await writeUndoBarrier(tx, WS, 10n);
			await noteDeadUndoTokens(tx, WS, [ 20n ]);
		});
		expect((await journal(kv)).dead).toEqual(new Set([ 20n ]));
	});

	it('prunes dead tokens older than the TTL', async () => {
		const kv = new MemoryKVStore();
		const t0 = 1_000_000;
		await write(kv, async (tx) => {
			await noteDeadUndoTokens(tx, WS, [ 1n ], t0);
			await noteDeadUndoTokens(tx, WS, [ 2n ], t0 + DEAD_TOKEN_TTL_MS);
			await noteDeadUndoTokens(tx, WS, [ 3n ], t0 + DEAD_TOKEN_TTL_MS + 1);
		});
		expect((await journal(kv)).dead).toEqual(new Set([ 2n, 3n ]));
	});

	it('keeps at most MAX_DEAD_TOKENS, and still voids the tokens it drops', async () => {
		const kv = new MemoryKVStore();
		await write(kv, async (tx) => {
			await noteDeadUndoTokens(tx, WS, [ 1n ], 1);
			const many = Array.from({ length: MAX_DEAD_TOKENS }, (_, i) => BigInt(i + 2));
			await noteDeadUndoTokens(tx, WS, many, 2);
		});
		const j = await journal(kv);
		expect(j.dead.size).toBe(MAX_DEAD_TOKENS);
		expect(j.dead.has(1n)).toBe(false);
		// The barrier moved above the dropped token, so its group stays void, and a
		// later group does not.
		expect(voidsToken(j, 1n)).toBe(true);
		expect(voidsToken(j, BigInt(MAX_DEAD_TOKENS + 2))).toBe(false);
	});
});
