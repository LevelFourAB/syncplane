import { expect, test } from '@playwright/test';

import { freshWorkspace, pullBytes, pushBytes, Tab } from './fixtures.js';

/**
 * What a text edit costs on the wire, measured from the browser.
 *
 * Both directions replicate as delta-state fragments: a push rewrites a queued
 * text PATCH into the fragment between the confirmed row and the new value, and
 * a pull folds the per-field fragment log over the span above the reader's
 * cursor. Neither is visible from inside a tab — the queue, the overlay and the
 * stored rows all keep whole documents on purpose — so the claim is asserted
 * against the server's own byte counters (/debug/rpc, keyed by workspace).
 *
 * Bootstrap is deliberately exempt: a client with no prior state has nothing for
 * a fragment to be relative to, so a cold device pays for the whole document.
 */

/** A document big enough that a delta and a full state cannot be confused. */
const PARAGRAPH = 'lorem ipsum dolor sit amet consectetur adipiscing elit ';
const BIG = PARAGRAPH.repeat(900);

test.describe('text delta state on the wire', () => {
	test('an edit to a large document costs a fraction of it in both directions', async ({ browser, context }) => {
		const ws = freshWorkspace();
		const writer = await Tab.open(context, {
			workspaceId: ws,
			accountId: `w-${ws}`,
			start: true,
		});

		// The reader takes the document once, the expensive way, and stops there.
		const readerContext = await browser.newContext();
		try {
			const reader = await Tab.open(readerContext, {
				workspaceId: ws,
				accountId: `r-${ws}`,
				start: true,
			});

			await writer.putTextString('d1', BIG);
			await writer.syncNow();
			await reader.syncNow();
			expect(await reader.storedText('d1')).toBe(BIG);

			const pushedBefore = await pushBytes(ws);
			const pulledBefore = await pullBytes(ws);

			// One word changed in a ~50KB document.
			await writer.patchTextString('d1', BIG.replace('lorem ipsum', 'LOREM IPSUM'));
			await writer.syncNow();
			await reader.syncNow();

			const pushed = await pushBytes(ws) - pushedBefore;
			const pulled = await pullBytes(ws) - pulledBefore;
			expect(pushed).toBeGreaterThan(0);
			expect(pushed, 'the push carried a fragment, not the document').toBeLessThan(
				BIG.length * 0.2,
			);
			expect(pulled).toBeGreaterThan(0);
			expect(pulled, 'the pull carried a fragment, not the document').toBeLessThan(
				BIG.length * 0.2,
			);

			// The fold reconstructed a whole document in the CONFIRMED row, not
			// just something the overlay covered for: everything downstream of the
			// store — reads, watches, replay, the next fragment's base — sees a
			// document either way.
			const expected = BIG.replace('lorem ipsum', 'LOREM IPSUM');
			expect(await reader.confirmedText('d1')).toBe(expected);
			expect(await reader.storedText('d1')).toBe(expected);
			expect(await reader.rejections()).toEqual([]);
		} finally {
			await readerContext.close();
		}
	});

	test('a change to another field ships no document at all', async ({ browser, context }) => {
		const ws = freshWorkspace();
		const writer = await Tab.open(context, {
			workspaceId: ws,
			accountId: `w-${ws}`,
			start: true,
		});
		const readerContext = await browser.newContext();
		try {
			const reader = await Tab.open(readerContext, {
				workspaceId: ws,
				accountId: `r-${ws}`,
				start: true,
			});

			await writer.putTextString('d1', BIG);
			await writer.syncNow();
			await reader.syncNow();

			const pulledBefore = await pullBytes(ws);
			// The row's label changes; its text field does not. An empty fragment
			// says "unchanged over this span, keep what you hold", which is what
			// stops the untouched document from riding along with the label.
			await writer.patchWidgetLabel('d1', 'renamed');
			await writer.syncNow();
			await reader.syncNow();

			expect(await pullBytes(ws) - pulledBefore).toBeLessThan(BIG.length * 0.2);
			expect(await reader.storedText('d1')).toBe(BIG);
		} finally {
			await readerContext.close();
		}
	});

	test('a cold device bootstraps the whole document', async ({ browser, context }) => {
		const ws = freshWorkspace();
		const writer = await Tab.open(context, {
			workspaceId: ws,
			accountId: `w-${ws}`,
			start: true,
		});
		await writer.putTextString('d1', BIG);
		await writer.syncNow();

		const coldContext = await browser.newContext();
		try {
			const cold = await Tab.open(coldContext, {
				workspaceId: ws,
				accountId: `cold-${ws}`,
				start: true,
			});
			await cold.syncNow();
			expect(await cold.storedText('d1')).toBe(BIG);
			expect(await cold.confirmedText('d1')).toBe(BIG);
		} finally {
			await coldContext.close();
		}
	});
});
