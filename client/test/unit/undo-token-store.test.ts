// Undo tokens ride beside pending mutations (metaUndoTokens). Every
// transaction that folds, renumbers or deletes a pending entry must carry its
// tokens along, or an undo stack loses the link to its mutation.

import { create } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import {
	CrudMutationSchema,
	CrudOp,
	type Mutation,
	MutationSchema,
} from '../../src/gen/syncplane/v1/mutation_pb.js';
import { metaLastMutationId, metaUndoTokenCounter, metaUndoTokens } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { EntityRegistry } from '../../src/mutations.js';
import { compactPending } from '../../src/optimistic/compact.js';
import {
	dropAcked,
	enqueuePending,
	mintUndoToken,
	readPending,
	readUndoTokens,
	writeUndoTokens,
} from '../../src/optimistic/queue.js';
import { resequencePending, rollbackRejected } from '../../src/optimistic/rebase.js';

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

/** Queues `entries`; each mutation carries the given tokens. */
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

async function tokens(kv: MemoryKVStore): Promise<Map<bigint, bigint[]>> {
	const tx = await kv.read();
	try {
		return await readUndoTokens(tx, WS);
	} finally {
		tx.release();
	}
}

describe('undo tokens beside the pending queue', () => {
	it('gives a folded slot the tokens of every mutation folded into it', async () => {
		const kv = await seed([
			[ patch(1n, 't1'), [ 10n ] ],
			[ patch(2n, 't1'), [ 20n ] ],
			[ patch(3n, 't2'), [ 30n ] ],
		]);
		const result = await compactPending(kv, registry, {}, WS, () => new Set());
		expect(result.changed).toBe(true);
		expect(await tokens(kv)).toEqual(new Map([ [ 1n, [ 10n, 20n ] ], [ 2n, [ 30n ] ] ]));
	});

	it('keeps a mutation that carries a protected token as its own slot', async () => {
		const kv = await seed([
			[ patch(1n, 't9'), [ 5n ] ],
			[ patch(2n, 't1'), [ 10n ] ],
			[ patch(3n, 't1'), [ 20n ] ],
		]);
		await compactPending(kv, registry, {}, WS, () => new Set([ 20n ]));
		const tx = await kv.read();
		try {
			expect((await readPending(tx, WS)).map((m) => m.mutationId)).toEqual([ 1n, 2n, 3n ]);
		} finally {
			tx.release();
		}
		expect(await tokens(kv)).toEqual(new Map([ [ 1n, [ 5n ] ], [ 2n, [ 10n ] ], [ 3n, [ 20n ] ] ]));
	});

	it('moves the tokens of a renumbered queue to the new ids', async () => {
		const kv = await seed([
			[ patch(4n, 't1'), [ 40n ] ],
			[ patch(5n, 't2'), [ 50n ] ],
		]);
		const result = await resequencePending(kv, registry, {}, WS, 1n);
		expect(result.changed).toBe(true);
		expect(await tokens(kv)).toEqual(new Map([ [ 1n, [ 40n ] ], [ 2n, [ 50n ] ] ]));
	});

	it('hands back and deletes the tokens of a rejected mutation', async () => {
		const kv = await seed([
			[ patch(1n, 't1'), [ 10n, 11n ] ],
			[ patch(2n, 't2'), [ 20n ] ],
		]);
		const result = await rollbackRejected(kv, registry, {}, WS, [ 1n ]);
		expect(result.undoTokens).toEqual(new Map([ [ 1n, [ 10n, 11n ] ] ]));
		expect(await tokens(kv)).toEqual(new Map([ [ 2n, [ 20n ] ] ]));
	});

	it('deletes the tokens of acked mutations', async () => {
		const kv = await seed([
			[ patch(1n, 't1'), [ 10n ] ],
			[ patch(2n, 't2'), [ 20n ] ],
		]);
		const tx = await kv.write();
		await dropAcked(tx, WS, 1n);
		await tx.commit();
		expect(await tokens(kv)).toEqual(new Map([ [ 2n, [ 20n ] ] ]));
	});

	it('mints tokens that rise, also after the counter was deleted', async () => {
		const kv = new MemoryKVStore();
		const mint = async (now: number) => {
			const tx = await kv.write();
			const token = await mintUndoToken(tx, WS, await tx.get(metaUndoTokenCounter(WS)), now);
			await tx.commit();
			return token;
		};
		const first = await mint(1000);
		expect(first).toBe(1000n * 1024n);
		// Same millisecond: the counter, not the clock, makes it unique.
		expect(await mint(1000)).toBe(first + 1n);

		// A purge deletes the counter; the clock floor keeps the next token above
		// every earlier one.
		const tx = await kv.write();
		await tx.del(metaUndoTokenCounter(WS));
		await tx.commit();
		expect(await mint(1001)).toBeGreaterThan(first + 1n);
	});
});

describe('stored token values that do not decode', () => {
	it('reads a malformed token list as no tokens, and keeps the good ones', async () => {
		const kv = await seed([ [ patch(1n, 'a'), [ 7n ] ], [ patch(2n, 'b'), [ 8n ] ] ]);
		const tx = await kv.write();
		await tx.put(metaUndoTokens(WS, 1n), [ 'not-a-number', '9' ]);
		await tx.put(metaUndoTokens(WS, 2n), 'not-a-list');
		await tx.commit();

		const read = await kv.read();
		const tokens = await readUndoTokens(read, WS);
		read.release();
		expect(tokens.get(1n)).toEqual([ 9n ]);
		expect(tokens.get(2n)).toEqual([]);
	});

	it('mints above the clock floor when the counter does not decode', async () => {
		const kv = new MemoryKVStore();
		const tx = await kv.write();
		const token = await mintUndoToken(tx, WS, 'not-a-number', 1000);
		await tx.commit();
		expect(token).toBe(1000n * 1024n);
	});
});
