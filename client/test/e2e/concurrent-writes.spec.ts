import { expect, test } from '@playwright/test';

import { freshWorkspace, Tab, todoIds } from './fixtures.js';

/**
 * Two tabs writing at the same moment through one store. This is what
 * WebLockMutex exists for: mutation ids are minted from the shared store and
 * the server requires them contiguous per client and workspace, so a mutex that
 * only excludes within a tab (ChainMutex, which is what every Node test tier
 * gets) would let two tabs mint the same id and lose a mutation. Nothing below
 * mentions locking — it just asserts that no write goes missing.
 */
test.describe('concurrent writers', () => {
	test('interleaved writes from two tabs all reach the server', async ({ browser, context }) => {
		const ws = freshWorkspace();
		const a = await Tab.open(context, { workspaceId: ws });
		const b = await Tab.open(context, { workspaceId: ws });

		const fromA = todoIds('a', 25);
		const fromB = todoIds('b', 25);
		// Both tabs enqueue concurrently, and each fires its own writes without
		// awaiting between them, so the interleaving is as bad as it can get.
		await Promise.all([ a.putTodos(fromA), b.putTodos(fromB) ]);

		const all = [ ...fromA, ...fromB ].sort();
		// One shared queue: either tab reports every pending mutation.
		expect(await a.pendingCount()).toBe(all.length);
		expect(await b.pendingCount()).toBe(all.length);
		expect(await a.todoIds()).toEqual(all);

		await a.start();
		await a.syncNow();
		expect(await a.pendingCount()).toBe(0);
		expect(await a.rejections()).toEqual([]);
		expect(await b.rejections()).toEqual([]);

		// Read back through a context of its own — separate origin storage, so
		// separate IndexedDB — to see what the server actually stored.
		const verifierContext = await browser.newContext();
		try {
			const verifier = await Tab.open(verifierContext, {
				workspaceId: ws,
				accountId: `verify-${ws}`,
				start: true,
			});
			await verifier.syncNow();
			expect(await verifier.todoIds()).toEqual(all);
		} finally {
			await verifierContext.close();
		}
	});

	test('concurrent edits to one row from two tabs keep both fields', async ({ context }) => {
		const ws = freshWorkspace();
		const a = await Tab.open(context, { workspaceId: ws });
		const b = await Tab.open(context, { workspaceId: ws });

		await a.putTodo('t1', 'original');
		await expect.poll(() => b.getTodo('t1')).not.toBeNull();

		// Two tabs patching disjoint fields of the same row at once: the overlay
		// and the pending queue they share must merge, not clobber.
		await Promise.all([
			a.patchTodo('t1', { title: 'renamed' }),
			b.patchTodo('t1', { completed: true }),
		]);

		await a.start();
		await a.syncNow();
		expect(await a.pendingCount()).toBe(0);
		expect(await a.getTodo('t1')).toEqual({ id: 't1', title: 'renamed', completed: true });
	});
});
