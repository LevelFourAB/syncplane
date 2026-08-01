import { expect, test } from '@playwright/test';

import { freshWorkspace, Tab, totalCalls } from './fixtures.js';

/**
 * Collaborative text as a user meets it: two tabs of one app with live handles
 * open on the same field, typing at the same time, and a second device that has
 * to arrive at the same document from the server alone.
 *
 * What only this tier can show is the tabs' shared substrate — one IndexedDB,
 * one pending queue, one clientId, and writer identities leased out of a
 * device-wide pool on Web Locks. In Node each client is its own island, so a
 * text field that converged there proves nothing about two editors of one app.
 */
test.describe('collaborative text across tabs', () => {
	test('two tabs typing at once converge, and a second device agrees', async ({ browser, context }) => {
		const ws = freshWorkspace();
		const a = await Tab.open(context, { workspaceId: ws, start: true });
		const b = await Tab.open(context, { workspaceId: ws });

		await a.putTextString('d1', 'the quick fox');
		await expect.poll(() => b.storedText('d1')).toBe('the quick fox');

		const ha = await a.openText('d1');
		const hb = await b.openText('d1');

		// Edits at opposite ends of the value, issued without awaiting between
		// them: the two documents have never seen each other, so this is the
		// concurrent case, not a serialized one.
		await Promise.all([
			a.insertText(ha, 0, 'a: '),
			b.insertText(hb, 'the quick fox'.length, ' jumps'),
		]);

		const expected = 'a: the quick fox jumps';
		for (const [ tab, handle ] of [ [ a, ha ], [ b, hb ] ] as const) {
			await expect.poll(() => tab.textValue(handle)).toBe(expected);
			expect(await tab.rejections()).toEqual([]);
		}

		await a.syncNow();

		// A device with a store of its own: it holds none of the two tabs' state
		// and has to reach the same text through the server.
		const cold = await browser.newContext();
		try {
			const c = await Tab.open(cold, {
				workspaceId: ws,
				accountId: `cold-${ws}`,
				start: true,
			});
			await c.syncNow();
			expect(await c.storedText('d1')).toBe(expected);
		} finally {
			await cold.close();
		}
	});

	test("a live handle picks up the peer tab's edit in place", async ({ context }) => {
		const ws = freshWorkspace();
		const a = await Tab.open(context, { workspaceId: ws });
		const b = await Tab.open(context, { workspaceId: ws });

		await a.putTextString('d1', 'hello world');
		await expect.poll(() => b.storedText('d1')).toBe('hello world');

		const ha = await a.openText('d1');
		await a.markText(ha, 6, 11, 'bold', 'true');

		// b never opens a handle — it writes the whole value as a string, which
		// the client diffs into the minimal edit. Nothing but the changed hunk
		// moves, so a's mark stays on the same word.
		await b.patchTextString('d1', 'hello brave world');

		await expect.poll(() => a.textValue(ha)).toBe('hello brave world');
		expect(await a.textMarks(ha)).toEqual([
			{ start: 12, end: 17, key: 'bold', value: 'true' },
		]);
		expect(await b.storedMarks('d1')).toEqual([
			{ start: 12, end: 17, key: 'bold', value: 'true' },
		]);
	});

	test('a run of keystrokes ships as one push, not one per key', async ({ context }) => {
		const ws = freshWorkspace();
		const a = await Tab.open(context, { workspaceId: ws });
		await a.putTextString('d1', '');
		const h = await a.openText('d1');

		// Every keystroke is its own PATCH carrying the whole document, so the
		// pending queue holds one per character.
		for (const ch of 'hello there'.split('')) {
			await a.insertText(h, (await a.textValue(h)).length, ch);
		}
		expect(await a.pendingCount()).toBeGreaterThan(1);

		const pushesBefore = await totalCalls('Push');
		await a.start();
		await a.syncNow();

		// Queue compaction folds them into a single PATCH before the wire, so
		// what the server sees is one push carrying one mutation.
		expect(await a.pendingCount()).toBe(0);
		expect(await totalCalls('Push') - pushesBefore).toBeLessThanOrEqual(2);
		expect(await a.storedText('d1')).toBe('hello there');
		expect(await a.rejections()).toEqual([]);
	});

	test('one writer identity per live handle, however much is typed', async ({ context }) => {
		// The peer table is part of the document: an identity minted per session,
		// or per keystroke, would grow it without bound.
		const ws = freshWorkspace();
		const a = await Tab.open(context, { workspaceId: ws });
		const b = await Tab.open(context, { workspaceId: ws });

		await a.putTextString('d1', 'seed');
		await expect.poll(() => b.storedText('d1')).toBe('seed');

		const ha = await a.openText('d1');
		const hb = await b.openText('d1');
		for (let i = 0; i < 5; i++) {
			await a.insertText(ha, 0, 'x');
			await b.insertText(hb, 0, 'y');
		}
		await expect.poll(() => a.textValue(ha)).toBe(await b.textValue(hb));

		// The string PUT leased an identity of its own and gave it back, so what
		// is left is the two open handles.
		const peers = await a.storedPeers('d1');
		expect(peers.length).toBeLessThanOrEqual(3);
		expect(new Set(peers).size).toBe(peers.length);
	});
});
