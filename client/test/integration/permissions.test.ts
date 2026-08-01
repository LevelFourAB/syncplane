import { describe, expect, it } from 'vitest';

import type { RejectionInfo } from '../../src/client.js';
import { CodeConflict, CodePermissionDenied } from '../../src/codes.js';
import { CompleteAllArgsSchema, TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import type { MirrorTx } from '../../src/optimistic/rebase.js';
import { freshWorkspace, newClient } from './helpers.js';

describe('permissions', () => {
	it('rejected mutations fire the callback and drop from the queue', async () => {
		const ws = freshWorkspace();
		const rejections: RejectionInfo[] = [];
		const a = newClient(ws, { onMutationRejected: (info) => rejections.push(info) });
		await a.start();

		await a.mutate.put(TodoSchema, { id: 'bad', title: 'forbidden fruit' });
		// Optimistic state shows it until the server says no.
		expect(await a.get(TodoSchema, 'bad')).toBeDefined();

		await a.syncNow();

		expect(rejections).toHaveLength(1);
		expect(rejections[0]?.errorCode).toBe(CodePermissionDenied);
		expect(await a.pendingCount()).toBe(0);
		expect(await a.get(TodoSchema, 'bad')).toBeUndefined();

		// The queue keeps flowing after a rejection.
		await a.mutate.put(TodoSchema, { id: 'good', title: 'allowed' });
		await a.syncNow();
		expect(await a.pendingCount()).toBe(0);
		expect(await a.get(TodoSchema, 'good')).toBeDefined();

		await a.close();
	});

	it('rolls rejected mutations back before the callback fires (no pull needed)', async () => {
		const ws = freshWorkspace();
		let observed: Promise<string | undefined> | undefined;
		const a = newClient(ws, {
			onMutationRejected: () => {
				// Capture what the app sees AT callback time — the rollback must
				// already have happened, without waiting for any pull.
				observed = a.get(TodoSchema, 't1').then((t) => t?.title);
			},
		});
		await a.start();

		await a.mutate.put(TodoSchema, { id: 't1', title: 'ok' });
		await a.syncNow();

		await a.mutate.patch(TodoSchema, 't1', { title: 'forbidden edit' });
		expect((await a.get(TodoSchema, 't1'))?.title).toBe('forbidden edit');
		await a.syncNow();

		expect(observed).toBeDefined();
		expect(await observed).toBe('ok');
		expect(await a.pendingCount()).toBe(0);
		await a.close();
	});

	it('read filters hide rows the server will not serve', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		await a.start();

		await a.mutate.put(TodoSchema, { id: 'p', title: 'public' });
		await a.mutate.put(TodoSchema, { id: 's', title: 'my secret diary' });
		// Optimistically both exist.
		expect((await a.scan(TodoSchema)).map((r) => r.key)).toEqual([ 'p', 's' ]);

		await a.syncNow();

		// The write was accepted, but pull filters the secret row, and the
		// overlay cleared once acked — so it vanishes locally too.
		expect((await a.scan(TodoSchema)).map((r) => r.key)).toEqual([ 'p' ]);

		const b = newClient(ws);
		await b.start();
		await b.syncNow();
		expect((await b.scan(TodoSchema)).map((r) => r.key)).toEqual([ 'p' ]);

		await a.close();
		await b.close();
	});
});

describe('filter epoch', () => {
	it('bump_epoch invalidates the cursor and the client re-bootstraps cleanly', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		await a.start();
		await a.mutate.put(TodoSchema, { id: 't1', title: 'survives' });
		await a.syncNow();

		// The example policy does not implement EpochReadFilters, so the epoch
		// bump makes the server reject a's cursor on the next pull (reset=true),
		// forcing a re-bootstrap under the new epoch. The Go suite pins the
		// reset; here we verify the client rides through it.
		await a.mutate.custom('bump_epoch');
		await a.syncNow();
		await a.syncNow();
		expect(await a.pendingCount()).toBe(0);
		expect((await a.get(TodoSchema, 't1'))?.title).toBe('survives');

		await a.close();
	});
});

describe('custom mutations', () => {
	const completeAllMirror = {
		args: CompleteAllArgsSchema,
		async apply(tx: MirrorTx) {
			for (const todo of await tx.scan(TodoSchema)) {
				if (!todo.completed) {
					todo.completed = true;
					await tx.put(TodoSchema, todo);
				}
			}
		},
	};

	it('applies optimistically via the mirror and is confirmed by the server', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws, { mutators: { complete_all: completeAllMirror } });
		await a.start();

		await a.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		await a.mutate.put(TodoSchema, { id: 't2', title: 'two' });
		await a.mutate.custom('complete_all', CompleteAllArgsSchema, {});

		// Mirror applied optimistically, before any network round-trip.
		for (const row of await a.scan(TodoSchema)) {
			expect(row.value.completed).toBe(true);
		}

		await a.syncNow();
		expect(await a.pendingCount()).toBe(0);
		for (const row of await a.scan(TodoSchema)) {
			expect(row.value.completed).toBe(true);
		}
		await a.close();
	});

	it('unmirrored custom mutations are optimistic no-ops but apply on the server', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws); // no mirror registered
		await a.start();

		await a.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		await a.mutate.custom('complete_all', CompleteAllArgsSchema, {});
		expect((await a.get(TodoSchema, 't1'))?.completed).toBe(false);

		await a.syncNow();
		expect((await a.get(TodoSchema, 't1'))?.completed).toBe(true);
		await a.close();
	});

	it("a rejected mutator's writes roll back on the server (savepoints)", async () => {
		const ws = freshWorkspace();
		const rejections: RejectionInfo[] = [];
		const a = newClient(ws, { onMutationRejected: (info) => rejections.push(info) });
		await a.start();

		await a.mutate.put(TodoSchema, { id: 'keep', title: 'stays' });
		// write_then_reject Puts the given todo server-side, then rejects: the
		// savepoint must roll the write back while still acking the mutation.
		await a.mutate.custom('write_then_reject', TodoSchema, { id: 'ghost', title: 'never' });
		await a.syncNow();

		expect(rejections).toHaveLength(1);
		expect(rejections[0]?.errorCode).toBe(CodeConflict);
		expect(await a.pendingCount()).toBe(0);

		const b = newClient(ws);
		await b.start();
		await b.syncNow();
		expect((await b.scan(TodoSchema)).map((r) => r.key)).toEqual([ 'keep' ]);

		await a.close();
		await b.close();
	});

	it('mirror divergence self-heals: the server wins after rebase', async () => {
		const ws = freshWorkspace();
		// This mirror is WRONG on purpose: besides completing todos it also
		// rewrites titles, which the Go mutator does not do.
		const divergent = {
			args: CompleteAllArgsSchema,
			async apply(tx: MirrorTx) {
				for (const todo of await tx.scan(TodoSchema)) {
					todo.completed = true;
					todo.title = `${todo.title} (done locally)`;
					await tx.put(TodoSchema, todo);
				}
			},
		};
		const a = newClient(ws, { mutators: { complete_all: divergent } });
		await a.start();

		await a.mutate.put(TodoSchema, { id: 't1', title: 'truth' });
		await a.mutate.custom('complete_all', CompleteAllArgsSchema, {});

		// Divergent optimistic state is visible locally…
		expect((await a.get(TodoSchema, 't1'))?.title).toBe('truth (done locally)');

		await a.syncNow();

		// …but the server's authoritative result replaces it after rebase.
		const todo = await a.get(TodoSchema, 't1');
		expect(todo?.title).toBe('truth');
		expect(todo?.completed).toBe(true);
		expect(await a.pendingCount()).toBe(0);
		await a.close();
	});
});
