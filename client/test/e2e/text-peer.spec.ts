import { expect, test } from '@playwright/test';

import { freshWorkspace, Tab } from './fixtures.js';

/**
 * Text writer identities across real tabs.
 *
 * A text item's id is (peer, counter), and counters come from the document's
 * own version vector — so two documents that have not seen each other's writes
 * and share a peer mint the same id for different content. The sync clientId is
 * exactly such a shared value: it is a property of the STORE, so every tab on
 * the account reads the same one. Handles therefore lease `<clientId>:<slot>`
 * out of a device-wide pool held on Web Locks.
 *
 * Node has no shared lock manager and falls back to an in-process registry, so
 * the cross-tab half of that mechanism only actually runs here.
 */
test.describe('text peer leases', () => {
	test('two tabs sharing a clientId get different writer identities', async ({ context }) => {
		const ws = freshWorkspace();
		const a = await Tab.open(context, { workspaceId: ws });
		const b = await Tab.open(context, { workspaceId: ws });

		// The precondition that makes this a real hazard: one store, one clientId.
		const clientId = await a.clientId();
		expect(await b.clientId()).toBe(clientId);

		await a.createTextRow('d1');
		await expect.poll(() => b.storedText('d1')).toBe('');

		const ha = await a.openText('d1');
		const hb = await b.openText('d1');
		const peerA = await a.textPeer(ha);
		const peerB = await b.textPeer(hb);

		expect(peerA).not.toBe(peerB);
		expect(peerA.startsWith(`${clientId}:`)).toBe(true);
		expect(peerB.startsWith(`${clientId}:`)).toBe(true);
	});

	test('a second handle in the SAME tab gets its own identity too', async ({ context }) => {
		// React StrictMode double-mounts, and two editor bindings on one field are
		// ordinary: each builds its own document, so each needs its own peer.
		const ws = freshWorkspace();
		const a = await Tab.open(context, { workspaceId: ws });
		await a.createTextRow('d1');

		const one = await a.openText('d1');
		const two = await a.openText('d1');
		expect(await a.textPeer(one)).not.toBe(await a.textPeer(two));
	});

	test('concurrent typing in two tabs converges instead of wedging the field', async ({ context }) => {
		const ws = freshWorkspace();
		const a = await Tab.open(context, { workspaceId: ws });
		const b = await Tab.open(context, { workspaceId: ws });

		await a.createTextRow('d1');
		await expect.poll(() => b.storedText('d1')).toBe('');
		const ha = await a.openText('d1');
		const hb = await b.openText('d1');

		// Both tabs type into documents that have not seen each other. Under one
		// shared peer these allocate the SAME item id: identical content merges
		// into a text neither tab typed, different content is rejected by the
		// merge and breaks the field for good.
		await Promise.all([ a.insertText(ha, 0, 'A'), b.insertText(hb, 0, 'B') ]);

		for (const tab of [ a, b ]) {
			await expect.poll(() => tab.storedText('d1')).toMatch(/^(AB|BA)$/);
			expect(await tab.rejections()).toEqual([]);
		}
		// Both tabs' live handles agree with the store, and with each other.
		await expect.poll(() => a.textValue(ha)).toBe(await b.storedText('d1'));
		await expect.poll(() => b.textValue(hb)).toBe(await a.storedText('d1'));

		// One entry per concurrent editor, not per keystroke or per session.
		expect(await a.storedPeers('d1')).toHaveLength(2);
	});

	test('closing a handle returns its slot to the pool', async ({ context }) => {
		const ws = freshWorkspace();
		const a = await Tab.open(context, { workspaceId: ws });
		const b = await Tab.open(context, { workspaceId: ws });
		await a.createTextRow('d1');
		await expect.poll(() => b.storedText('d1')).toBe('');

		const ha = await a.openText('d1');
		const peerA = await a.textPeer(ha);
		await a.closeText(ha);

		// b takes the slot a just released — the recycling that keeps a
		// document's peer table from growing once per editor session, forever.
		const hb = await b.openText('d1');
		expect(await b.textPeer(hb)).toBe(peerA);
	});
});
