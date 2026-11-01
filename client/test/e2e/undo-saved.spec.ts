import { expect, test } from '@playwright/test';

import { freshWorkspace, Tab } from './fixtures.js';

/**
 * Saved undo scopes across real tabs. Two things only a browser can show: that
 * a Web Lock gives one tab at a time the saved stack of a key, and that the
 * stack survives the page going away. Node falls back to an in-process lock
 * registry, so the unit suite cannot see either.
 *
 * The save after a change is debounced; SAVE_SETTLE_MS waits it out before a
 * page goes away without closing its scope.
 */
const SAVE_SETTLE_MS = 500;

/** Restores `key` until this tab holds its saved stack, closing each scope it did not get. */
async function claimSaved(tab: Tab, key: string): Promise<number> {
	let handle = 0;
	await expect.poll(async () => {
		if (handle !== 0) await tab.closeUndoScope(handle);
		handle = await tab.restoreUndoScope(key);
		return (await tab.undoScope(handle)).saved;
	}, { timeout: 10_000 }).toBe(true);
	return handle;
}

test.describe('saved undo scopes', () => {
	test('a scope saved before a reload undoes the write after it', async ({ context }) => {
		const ws = freshWorkspace();
		const tab = await Tab.open(context, { workspaceId: ws, undo: true });
		const scope = await tab.restoreUndoScope('page:1');
		expect((await tab.undoScope(scope)).saved).toBe(true);
		await tab.putTodoIn(scope, 't1', 'before reload');
		await tab.page.waitForTimeout(SAVE_SETTLE_MS);

		// No close(): the page goes away the way a refresh takes it.
		await tab.reload({ workspaceId: ws, undo: true });

		const restored = await tab.restoreUndoScope('page:1');
		expect(await tab.undoScope(restored)).toMatchObject({ saved: true, canUndo: true });
		expect(await tab.undoIn(restored)).toBe(true);
		expect(await tab.getTodo('t1')).toBeNull();
		expect(await tab.undoScope(restored)).toMatchObject({ canUndo: false, canRedo: true });
		await tab.closeTab();
	});

	test('a scope closed before the tab closes keeps its history for the next tab', async ({ context }) => {
		const ws = freshWorkspace();
		const first = await Tab.open(context, { workspaceId: ws, undo: true });
		const scope = await first.restoreUndoScope('page:1');
		await first.putTodoIn(scope, 't1', 'closed cleanly');
		await first.closeTab();

		const next = await Tab.open(context, { workspaceId: ws, undo: true });
		const restored = await next.restoreUndoScope('page:1');
		expect(await next.undoScope(restored)).toMatchObject({ saved: true, canUndo: true });
		expect(await next.undoIn(restored)).toBe(true);
		expect(await next.getTodo('t1')).toBeNull();
		await next.closeTab();
	});

	test('only one tab at a time holds the saved stack of a key', async ({ context }) => {
		const ws = freshWorkspace();
		const a = await Tab.open(context, { workspaceId: ws, undo: true });
		const b = await Tab.open(context, { workspaceId: ws, undo: true });

		const [ inA, inB ] = await Promise.all([
			a.restoreUndoScope('page:1'),
			b.restoreUndoScope('page:1'),
		]);
		const [ viewA, viewB ] = await Promise.all([ a.undoScope(inA), b.undoScope(inB) ]);
		expect([ viewA.saved, viewB.saved ].filter(Boolean)).toHaveLength(1);

		const [ holder, holderScope, other, otherScope ] = viewA.saved
			? [ a, inA, b, inB ] as const
			: [ b, inB, a, inA ] as const;
		await holder.putTodoIn(holderScope, 't1', 'held');
		// The tab without the claim records nothing that reaches the saved stack.
		await other.putTodoIn(otherScope, 't2', 'not saved');
		await other.closeUndoScope(otherScope);

		await holder.closeUndoScope(holderScope);
		const reclaimed = await claimSaved(other, 'page:1');
		expect(await other.undoScope(reclaimed)).toMatchObject({ saved: true, canUndo: true });
		expect(await other.undoIn(reclaimed)).toBe(true);
		expect(await other.getTodo('t1')).toBeNull();
		// Only the holder's step was saved, so nothing is left to undo.
		expect(await other.undoIn(reclaimed)).toBe(false);
		expect(await other.getTodo('t2')).not.toBeNull();

		await a.closeTab();
		await b.closeTab();
	});

	test('closing the holding tab hands the key to another tab', async ({ context }) => {
		const ws = freshWorkspace();
		const a = await Tab.open(context, { workspaceId: ws, undo: true });
		const b = await Tab.open(context, { workspaceId: ws, undo: true });
		const inA = await a.restoreUndoScope('page:1');
		expect((await a.undoScope(inA)).saved).toBe(true);
		await a.putTodoIn(inA, 't1', 'from A');

		const busy = await b.restoreUndoScope('page:1');
		expect((await b.undoScope(busy)).saved).toBe(false);
		await b.closeUndoScope(busy);

		await a.closeTab();
		const inB = await claimSaved(b, 'page:1');
		expect((await b.undoScope(inB)).canUndo).toBe(true);
		expect(await b.undoIn(inB)).toBe(true);
		expect(await b.getTodo('t1')).toBeNull();
		await b.closeTab();
	});

	test('a tab that dies without closing frees the key and leaves its last save', async ({ context }) => {
		const ws = freshWorkspace();
		const a = await Tab.open(context, { workspaceId: ws, undo: true });
		const b = await Tab.open(context, { workspaceId: ws, undo: true });
		const inA = await a.restoreUndoScope('page:1');
		await a.putTodoIn(inA, 't1', 'from A');
		await a.page.waitForTimeout(SAVE_SETTLE_MS);

		// No harness close, no unload handlers: the page is simply gone.
		await a.page.close({ runBeforeUnload: false });

		const inB = await claimSaved(b, 'page:1');
		expect((await b.undoScope(inB)).canUndo).toBe(true);
		expect(await b.undoIn(inB)).toBe(true);
		expect(await b.getTodo('t1')).toBeNull();
		await b.closeTab();
	});

	test("a custom mutation in another tab voids the holder's history", async ({ context }) => {
		const ws = freshWorkspace();
		const a = await Tab.open(context, { workspaceId: ws, undo: true });
		const b = await Tab.open(context, { workspaceId: ws, undo: true });
		const inA = await a.restoreUndoScope('page:1');
		await a.putTodoIn(inA, 't1', 'before the barrier');
		expect((await a.undoScope(inA)).canUndo).toBe(true);

		await b.customMutation('complete_all', 't1');

		// Tab A hears about the barrier without trying an undo first.
		await expect.poll(async () => (await a.undoScope(inA)).canUndo).toBe(false);
		expect(await a.undoIn(inA)).toBe(false);
		expect(await a.getTodo('t1')).not.toBeNull();

		// The saved copy is void as well: a fresh claim after a reload has nothing.
		await a.closeUndoScope(inA);
		await a.reload({ workspaceId: ws, undo: true });
		const again = await a.restoreUndoScope('page:1');
		expect(await a.undoScope(again)).toMatchObject({ saved: true, canUndo: false });
		await a.closeTab();
		await b.closeTab();
	});

	test("a rejection another tab receives drops the holder's step", async ({ context }) => {
		const ws = freshWorkspace();
		// Tab A never starts, so it never pushes: the rejection reaches only B.
		const a = await Tab.open(context, { workspaceId: ws, undo: true });
		const b = await Tab.open(context, { workspaceId: ws, undo: true, start: true });
		const inA = await a.restoreUndoScope('page:1');
		await a.putTodoIn(inA, 't1', 'accepted');
		await b.syncNow();
		expect(await b.rejections()).toEqual([]);

		// The testserver refuses a todo whose title contains "forbidden".
		await a.patchTodoIn(inA, 't1', { title: 'forbidden title' });
		await b.syncNow();
		expect(await b.rejections()).toHaveLength(1);
		await expect.poll(async () => (await a.getTodo('t1'))?.title).toBe('accepted');

		// One undo: the voided patch is skipped and the put below it replays. Had
		// the patch replayed, t1 would still exist with canUndo still true.
		expect(await a.undoIn(inA)).toBe(true);
		expect(await a.getTodo('t1')).toBeNull();
		expect((await a.undoScope(inA)).canUndo).toBe(false);
		await a.closeTab();
		await b.closeTab();
	});

	test('a discarded scope starts empty on the next restore', async ({ context }) => {
		const ws = freshWorkspace();
		const tab = await Tab.open(context, { workspaceId: ws, undo: true });
		const scope = await tab.restoreUndoScope('page:1');
		await tab.putTodoIn(scope, 't1', 'discarded');
		await tab.discardUndoScope(scope);
		expect((await tab.undoScope(scope)).closed).toBe(true);

		const fresh = await tab.restoreUndoScope('page:1');
		expect(await tab.undoScope(fresh)).toMatchObject({ saved: true, canUndo: false, canRedo: false });
		await tab.closeUndoScope(fresh);

		// Discarding by key, with no tab holding it, also clears the saved stack.
		const kept = await tab.restoreUndoScope('page:2');
		await tab.putTodoIn(kept, 't2', 'saved then discarded by key');
		await tab.closeUndoScope(kept);
		expect(await tab.discardSavedUndo('page:2')).toBe(true);
		const after = await tab.restoreUndoScope('page:2');
		expect((await tab.undoScope(after)).canUndo).toBe(false);
		await tab.closeTab();
	});
});
