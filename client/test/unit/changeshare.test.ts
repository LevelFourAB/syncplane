import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { startChangeShare } from '../../src/sync/changeshare.js';

const flush = () => new Promise((r) => setTimeout(r, 10));

describe('startChangeShare', () => {
	// The cross-tab channel only engages in a browser context; under the test's
	// own Node runtime it would otherwise no-op (see env.isServerRuntime). Fake
	// a browser navigator so the fan-out mechanism is exercised, while the real
	// in-process BroadcastChannel still carries the messages.
	beforeEach(() => {
		vi.stubGlobal('navigator', { userAgent: 'Mozilla/5.0' });
	});
	afterEach(() => {
		vi.unstubAllGlobals();
	});

	it('fans a touched set out to other tabs but not itself', async () => {
		const recv1 = vi.fn();
		const recv2 = vi.fn();
		const share1 = startChangeShare('ws', recv1);
		const share2 = startChangeShare('ws', recv2);

		share1.broadcast(new Set([ 'todo', 'note' ]));
		await flush();

		expect(recv1).not.toHaveBeenCalled();
		expect(recv2).toHaveBeenCalledTimes(1);
		expect(recv2.mock.calls[0]?.[0]).toEqual(new Set([ 'todo', 'note' ]));

		share1.stop();
		share2.stop();
	});

	it('does not broadcast an empty set', async () => {
		const recv = vi.fn();
		const sender = startChangeShare('ws', () => {});
		const receiver = startChangeShare('ws', recv);

		sender.broadcast(new Set());
		await flush();

		expect(recv).not.toHaveBeenCalled();
		sender.stop();
		receiver.stop();
	});

	it('isolates tabs on different scopes', async () => {
		const recv = vi.fn();
		const sender = startChangeShare('ws-a', () => {});
		const other = startChangeShare('ws-b', recv);

		sender.broadcast(new Set([ 'todo' ]));
		await flush();

		expect(recv).not.toHaveBeenCalled();
		sender.stop();
		other.stop();
	});

	it('degrades to a no-op without BroadcastChannel', () => {
		vi.stubGlobal('BroadcastChannel', undefined);
		const share = startChangeShare('ws', () => {
			throw new Error('must not fire');
		});
		// Both methods are safe to call and do nothing.
		expect(() => share.broadcast(new Set([ 'todo' ]))).not.toThrow();
		expect(() => share.stop()).not.toThrow();
	});

	it('degrades to a no-op on a server runtime', () => {
		// Node/Deno/Bun expose BroadcastChannel but their clients hold private
		// stores, so a peer's touched set is meaningless — the channel must stay
		// closed rather than cross-fire watchers between unrelated stores.
		vi.stubGlobal('navigator', { userAgent: 'Node.js/26' });
		const recv = vi.fn();
		const sender = startChangeShare('ws', () => {});
		const receiver = startChangeShare('ws', recv);

		sender.broadcast(new Set([ 'todo' ]));
		expect(recv).not.toHaveBeenCalled();
		sender.stop();
		receiver.stop();
	});
});
