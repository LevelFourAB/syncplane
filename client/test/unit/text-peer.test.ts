import { afterEach, describe, expect, it, vi } from 'vitest';

import { createPeerLeases } from '../../src/text/peer.js';

// A minimal Web Locks fake covering the one mode peer leases use: ifAvailable,
// held for as long as the callback's promise is pending. Grants to at most one
// holder and hands `null` to everyone else, exactly as the real API does.
function fakeLocks() {
	const held = new Set<string>();
	const request = async (
		name: string,
		options: { ifAvailable?: boolean },
		cb: (lock: unknown) => unknown,
	): Promise<unknown> => {
		if (held.has(name)) {
			if (options.ifAvailable) return cb(null);
			throw new Error('fakeLocks: only ifAvailable is modelled');
		}
		held.add(name);
		try {
			return await cb({ name });
		} finally {
			held.delete(name);
		}
	};
	return { request, heldCount: () => held.size };
}

/** Lets the lock fake's release path (an await inside request) run. */
const flush = (): Promise<void> => new Promise((r) => setTimeout(r, 0));

// Node reads as a server runtime, so this is the in-process registry unless a
// test stubs a browser-shaped navigator.
describe('peer leases', () => {
	afterEach(() => {
		vi.unstubAllGlobals();
	});

	it('never hands the same peer to two live holders', async () => {
		const leases = createPeerLeases('c1');
		const held = await Promise.all([ leases.acquire(), leases.acquire(), leases.acquire() ]);
		const peers = held.map((l) => l.peer);
		expect(new Set(peers).size).toBe(3);
		for (const p of peers) expect(p.startsWith('c1:')).toBe(true);
		for (const l of held) l.release();
	});

	it('keeps slots dense and recycles them', async () => {
		const leases = createPeerLeases('c2');
		const first = await leases.acquire();
		expect(first.peer).toBe('c2:0');
		const second = await leases.acquire();
		expect(second.peer).toBe('c2:1');

		// A released slot is the lowest free one again — this is what keeps a
		// document's peer table from growing with the number of sessions.
		first.release();
		const third = await leases.acquire();
		expect(third.peer).toBe('c2:0');

		second.release();
		third.release();
	});

	it('is idempotent on release', async () => {
		const leases = createPeerLeases('c3');
		const lease = await leases.acquire();
		lease.release();
		lease.release();
		// The double release must not have freed the slot a second time, which
		// would let the NEXT holder be handed out twice.
		const a = await leases.acquire();
		const b = await leases.acquire();
		expect(a.peer).not.toBe(b.peer);
		a.release();
		b.release();
	});

	it('separates two clientIds', async () => {
		const one = await createPeerLeases('cA').acquire();
		const two = await createPeerLeases('cB').acquire();
		expect(one.peer).not.toBe(two.peer);
		one.release();
		two.release();
	});

	describe('with Web Locks (the cross-tab path)', () => {
		it('holds one lock per live lease and drops it on release', async () => {
			const locks = fakeLocks();
			vi.stubGlobal('navigator', { locks });
			const leases = createPeerLeases('c4');

			const a = await leases.acquire();
			const b = await leases.acquire();
			expect([ a.peer, b.peer ]).toEqual([ 'c4:0', 'c4:1' ]);
			expect(locks.heldCount()).toBe(2);

			a.release();
			await flush();
			expect(locks.heldCount()).toBe(1);

			// Slot 0 is free again, so the next lease takes it back.
			const c = await leases.acquire();
			expect(c.peer).toBe('c4:0');
			b.release();
			c.release();
		});

		it('gives two clients sharing a clientId disjoint peers', async () => {
			// The multi-tab case: same device, same clientId, one lock manager.
			vi.stubGlobal('navigator', { locks: fakeLocks() });
			const tabA = createPeerLeases('shared');
			const tabB = createPeerLeases('shared');
			const a = await tabA.acquire();
			const b = await tabB.acquire();
			expect(a.peer).not.toBe(b.peer);
			a.release();
			b.release();
		});
	});
});
