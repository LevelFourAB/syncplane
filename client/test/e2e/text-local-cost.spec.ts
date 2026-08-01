import type { BrowserContext } from '@playwright/test';
import { expect, test } from '@playwright/test';

import { freshWorkspace, pushBytes, Tab } from './fixtures.js';

/**
 * What a text edit costs on disk, measured from the browser.
 *
 * The sibling of text-delta-wire.spec.ts: that one pins what an edit costs on
 * the wire, this one what it costs in IndexedDB. A live text handle queues the
 * delta for its one edit rather than the whole document, so a keystroke queues
 * bytes proportional to the keystroke — while everything a reader touches, the
 * stored row included, still holds a whole document.
 *
 * That leaves the two halves of a keystroke's local cost pulling in opposite
 * directions, which is why both are measured here: the pending queue by scanning
 * `p/` (a level — an entry stays until it is acked), and the optimistic overlay
 * by metering the store as writes go past (a flow — each write to a row
 * overwrites the last, so nothing about it is visible afterwards). The last
 * assertion below is what says which of the two dominates.
 *
 * It belongs here rather than in the unit tier because the cost being measured
 * is structured-clone and disk, which only a real IndexedDB has.
 */

/** A document big enough that a delta and a whole document cannot be confused. */
const PARAGRAPH = 'lorem ipsum dolor sit amet consectetur adipiscing elit ';
const BIG = PARAGRAPH.repeat(900);

/**
 * Average bytes one keystroke commits to storage, per keyspace, typing into a
 * document that is already synced and sitting in confirmed state.
 *
 * Typed OFFLINE — a tab reopened without start() — so the numbers are the local
 * write path alone. A started tab pushes and pulls under the burst, and the
 * confirmed rows its pulls fold in would otherwise be counted as part of what a
 * keystroke costs.
 */
async function keystrokeCost(context: BrowserContext, doc: string) {
	const ws = freshWorkspace();
	const first = await Tab.open(context, { workspaceId: ws, accountId: `w-${ws}`, start: true });
	await first.putTextString('d1', doc);
	await first.syncNow();
	await first.closeTab();

	const tab = await Tab.open(context, {
		workspaceId: ws,
		accountId: `w-${ws}`,
		measureWrites: true,
	});
	const handle = await tab.openText('d1');
	// After the handle is open: leasing its writer identity writes too, and that
	// is a per-document cost, not a per-keystroke one.
	await tab.resetWriteBytes();

	const typed = 'hello world';
	for (const [ i, ch ] of [ ...typed ].entries()) await tab.insertText(handle, i, ch);
	const total = await tab.writeBytes();
	// The edits landed where the caller thinks they did, whatever the bytes say.
	const value = await tab.textValue(handle);
	if (value !== `${typed}${doc}`) throw new Error('the burst did not produce the expected text');
	await tab.closeText(handle);
	await tab.closeTab();

	return {
		overlay: total.overlay / typed.length,
		overlayText: total.overlayText / typed.length,
		pending: total.pending / typed.length,
		total: total.total / typed.length,
		commits: total.commits / typed.length,
	};
}

test.describe('what a text edit costs locally', () => {
	test('a keystroke queues bytes proportional to the keystroke', async ({ context }) => {
		const ws = freshWorkspace();
		const tab = await Tab.open(context, { workspaceId: ws, accountId: `w-${ws}`, start: true });

		await tab.putTextString('d1', BIG);
		await tab.syncNow();
		expect(await tab.pendingCount()).toBe(0);

		const handle = await tab.openText('d1');
		const before = await tab.pendingBytes();
		await tab.insertText(handle, 0, 'X');
		const afterOne = await tab.pendingBytes();

		expect(afterOne).toBeGreaterThan(before);
		expect(afterOne - before, 'one keystroke wrote a fraction of the document').toBeLessThan(
			BIG.length * 0.05,
		);

		// Nothing about the row itself changed shape: a reader sees a document.
		expect(await tab.textValue(handle)).toBe(`X${BIG}`);
		expect(await tab.storedText('d1')).toBe(`X${BIG}`);
		await tab.closeText(handle);
	});

	test('an offline burst grows with the text typed, not with the document', async ({ context }) => {
		const ws = freshWorkspace();
		const first = await Tab.open(context, { workspaceId: ws, accountId: `w-${ws}`, start: true });
		await first.putTextString('d1', BIG);
		await first.syncNow();
		await first.closeTab();

		// Reopened without start(): nothing pushes, so the queue only grows — the
		// case that used to be bounded by nothing but the document.
		const tab = await Tab.open(context, { workspaceId: ws, accountId: `w-${ws}` });

		const handle = await tab.openText('d1');
		const before = await tab.pendingBytes();
		const typed = 'hello world';
		for (const [ i, ch ] of [ ...typed ].entries()) {
			await tab.insertText(handle, i, ch);
		}
		const growth = await tab.pendingBytes() - before;

		expect(await tab.pendingCount()).toBe(typed.length);
		// Every entry holding the document would be ~11 × BIG. The bound that
		// matters is that the burst does not scale with the document at all.
		expect(growth, 'the burst queued deltas, not documents').toBeLessThan(BIG.length * 0.05);
		expect(await tab.textValue(handle)).toBe(`${typed}${BIG}`);
		await tab.closeText(handle);
	});

	test('a keystroke costs the same whatever the document weighs', async ({ context }) => {
		// 27× apart, so anything that still scales with the document cannot hide in
		// the constants.
		const small = await keystrokeCost(context, PARAGRAPH.repeat(100));
		const large = await keystrokeCost(context, PARAGRAPH.repeat(2700));

		// A keystroke is one transaction: the queue entry, the optimistic edit and
		// the undo record commit together. That is the crash-safety this path is
		// built around, and no amount of shrinking the write may cost it.
		expect(small.commits).toBe(1);
		expect(large.commits).toBe(1);

		// Nothing rewrites the row. A text edit appends its fragment to the row's
		// text log (keys.ts) and the read path folds it back, so the overlay — the
		// one place a whole document used to be rewritten per edit — is untouched.
		expect(small.overlay).toBe(0);
		expect(large.overlay).toBe(0);

		// What is left is two copies of the same edit-sized fragment: one queued to
		// push, one logged to read back locally. Both flat in the document.
		for (const cost of [ small, large ]) {
			expect(cost.pending).toBeLessThan(1024);
			expect(cost.overlayText).toBeLessThan(1024);
		}
		expect(large.total).toBeLessThan(small.total * 1.5);
		// The bound that matters, stated against the document rather than against
		// the previous design: a keystroke into a 148 KB document writes well under
		// a kilobyte, where rewriting the row cost ~149,000 bytes.
		expect(large.total).toBeLessThan(1024);
	});

	test('the queued burst reaches the server without carrying the document', async ({ context }) => {
		const ws = freshWorkspace();
		const first = await Tab.open(context, { workspaceId: ws, accountId: `w-${ws}`, start: true });
		await first.putTextString('d1', BIG);
		await first.syncNow();
		await first.closeTab();

		const tab = await Tab.open(context, { workspaceId: ws, accountId: `w-${ws}` });
		const handle = await tab.openText('d1');
		const typed = 'hello world';
		for (const [ i, ch ] of [ ...typed ].entries()) {
			await tab.insertText(handle, i, ch);
		}
		expect(await tab.pendingCount()).toBe(typed.length);

		// Compaction folds the burst before it is pushed — a text edit's inverse is
		// symbolic, so it does not pin its mutation as a queue slot the way a
		// snapshot inverse does — and what goes out is one delta, not eleven
		// documents.
		const pushedBefore = await pushBytes(ws);
		await tab.start();
		await tab.syncNow();

		expect(await tab.pendingCount()).toBe(0);
		expect(await pushBytes(ws) - pushedBefore, 'the burst pushed a delta').toBeLessThan(
			BIG.length * 0.05,
		);
		expect(await tab.confirmedText('d1')).toBe(`${typed}${BIG}`);
		expect(await tab.rejections()).toEqual([]);
		await tab.closeText(handle);
	});
});
