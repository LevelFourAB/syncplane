import type { BrowserContext } from '@playwright/test';
import { expect, test } from '@playwright/test';

import { freshWorkspace, Tab } from './fixtures.js';

/**
 * Strings at the write boundary, between two devices rather than two tabs:
 * `mutate.put`/`mutate.patch` take a plain string for a text field and diff it
 * into CRDT edits under a leased writer identity. The point of running it here
 * is that the diffing writer and the typing writer are separate browsers with
 * separate stores, so nothing but the merge can reconcile them.
 *
 * Each tab gets its own accountId — same origin, but `sync:<accountId>` names
 * both the IndexedDB database and the lock, so they are two devices that happen
 * to share a browser.
 */
test.describe('string writes to collaborative text', () => {
	async function twoDevices(context: BrowserContext, ws: string) {
		const a = await Tab.open(context, { workspaceId: ws, accountId: `a-${ws}`, start: true });
		const b = await Tab.open(context, { workspaceId: ws, accountId: `b-${ws}`, start: true });
		return { a, b };
	}

	test('a whole-value writer and a typist keep both edits', async ({ context }) => {
		const ws = freshWorkspace();
		const { a, b } = await twoDevices(context, ws);

		await a.putTextString('d1', 'shopping: milk');
		await a.syncNow();
		await b.syncNow();
		expect(await b.storedText('d1')).toBe('shopping: milk');

		// b rewrites the whole value from a form it rendered a moment ago, while
		// a types into a live handle. A replace would drop one of the two; the
		// diff makes b's write an insert that merges with a's.
		const ha = await a.openText('d1');
		await Promise.all([
			a.insertText(ha, 'shopping: milk'.length, ', eggs'),
			b.patchTextString('d1', 'shopping list: milk'),
		]);
		await a.syncNow();
		await b.syncNow();
		await a.syncNow();

		const expected = 'shopping list: milk, eggs';
		expect(await a.storedText('d1')).toBe(expected);
		await expect.poll(() => a.textValue(ha)).toBe(expected);
		await expect.poll(async () => {
			await b.syncNow();
			return await b.storedText('d1');
		}).toBe(expected);
		expect(await a.rejections()).toEqual([]);
		expect(await b.rejections()).toEqual([]);
	});

	test('the empty string clears the field without breaking it', async ({ context }) => {
		const ws = freshWorkspace();
		const { a, b } = await twoDevices(context, ws);

		await a.putTextString('d1', 'delete me');
		await a.syncNow();
		await b.syncNow();

		// Select-all-delete: every character becomes a tombstone, and the field
		// still accepts writes afterwards.
		await b.patchTextString('d1', '');
		await b.syncNow();
		await a.syncNow();
		expect(await a.storedText('d1')).toBe('');

		await a.putTextString('d1', 'written again');
		await a.syncNow();
		await b.syncNow();
		expect(await b.storedText('d1')).toBe('written again');
		expect(await b.rejections()).toEqual([]);
	});

	test('PUT with a string creates the row', async ({ context }) => {
		const ws = freshWorkspace();
		const { a, b } = await twoDevices(context, ws);

		// No prior row: PATCH would have nothing to attach to, PUT creates one
		// whose text field is already a document.
		await a.putTextString('fresh', 'created by a string');
		await a.syncNow();
		await b.syncNow();
		expect(await b.storedText('fresh')).toBe('created by a string');

		// And a PUT over an existing row unions rather than replacing the field,
		// so a concurrent edit it never saw survives it.
		const hb = await b.openText('fresh');
		await b.insertText(hb, 0, '! ');
		await b.syncNow();
		await a.putTextString('fresh', 'created by a string, then rewritten');
		await a.syncNow();
		await b.syncNow();
		expect(await b.storedText('fresh')).toBe('! created by a string, then rewritten');
	});
});
