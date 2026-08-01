import { afterEach, describe, expect, it, vi } from 'vitest';

import { startReopenSignal, startStreamLeadership } from '../../src/sync/leadership.js';

// Grace before a hidden tab releases leadership (mirrors HIDE_GRACE_MS).
const HIDE_GRACE_MS = 1_500;

// A minimal Web Locks fake: grants each named lock to one holder at a time,
// queueing the rest, like the real API. The abort signal cancels a queued wait
// (not a held callback — the held loop watches the signal itself).
function fakeLocks() {
	const queues = new Map<string, Array<() => void>>();
	const held = new Set<string>();
	const request = async (
		name: string,
		options: { signal?: AbortSignal },
		cb: () => Promise<unknown>,
	): Promise<unknown> => {
		if (held.has(name)) {
			await new Promise<void>((resolve, reject) => {
				const queue = queues.get(name) ?? [];
				queue.push(resolve);
				queues.set(name, queue);
				options.signal?.addEventListener('abort', () => reject(new Error('AbortError')));
			});
		}
		held.add(name);
		try {
			return await cb();
		} finally {
			held.delete(name);
			queues.get(name)?.shift()?.();
		}
	};
	return { request };
}

// A minimal EventTarget-ish document/window with a mutable visibilityState.
function fakeDoc(initial: 'visible' | 'hidden' = 'visible') {
	const listeners = new Map<string, Set<() => void>>();
	const target = {
		visibilityState: initial,
		addEventListener: (type: string, handler: () => void) => {
			const set = listeners.get(type) ?? new Set();
			set.add(handler);
			listeners.set(type, set);
		},
		removeEventListener: (type: string, handler: () => void) => {
			listeners.get(type)?.delete(handler);
		},
	};
	const fire = (type: string): void => {
		for (const handler of [ ...(listeners.get(type) ?? []) ]) handler();
	};
	const setVisibility = (v: 'visible' | 'hidden'): void => {
		target.visibilityState = v;
		fire('visibilitychange');
	};
	return { target, fire, setVisibility };
}

// runLeaderLoop that resolves when its term aborts; counts runs and releases.
function leaderHooks() {
	const runs = vi.fn();
	let released = 0;
	const hooks = {
		runLeaderLoop: (signal: AbortSignal) => {
			runs();
			return new Promise<void>((resolve) => {
				signal.addEventListener('abort', () => {
					released++;
					resolve();
				});
			});
		},
	};
	return { hooks, runs, released: () => released };
}

const flush = () => new Promise((r) => setTimeout(r, 10));

describe('startStreamLeadership', () => {
	afterEach(() => {
		vi.useRealTimers();
		vi.unstubAllGlobals();
	});

	it('returns undefined without Web Locks (per-instance fallback)', () => {
		vi.stubGlobal('navigator', {});
		const share = startStreamLeadership('ws', { runLeaderLoop: () => new Promise(() => {}) });
		expect(share).toBeUndefined();
	});

	it('returns undefined on a server runtime even with Web Locks', () => {
		vi.stubGlobal('navigator', { locks: fakeLocks(), userAgent: 'Node.js/26' });
		const share = startStreamLeadership('ws', { runLeaderLoop: () => new Promise(() => {}) });
		expect(share).toBeUndefined();
	});

	it("elects one leader and hands over when the leader's term ends", async () => {
		vi.stubGlobal('navigator', { locks: fakeLocks() });
		// No document: a worker-like context is always "visible" and leads.

		const a = leaderHooks();
		const share1 = startStreamLeadership('ws', a.hooks);
		expect(share1).toBeDefined();
		await flush();
		expect(a.runs).toHaveBeenCalledTimes(1);

		const b = leaderHooks();
		const share2 = startStreamLeadership('ws', b.hooks);
		await flush();
		expect(b.runs).not.toHaveBeenCalled();

		// Leader 1 stops (releases its term): tab 2 takes over.
		share1?.stop();
		await flush();
		expect(b.runs).toHaveBeenCalledTimes(1);

		share2?.stop();
	});

	it('releases leadership when hidden (after a grace) and re-acquires when visible', async () => {
		vi.useFakeTimers();
		const { target, setVisibility } = fakeDoc('visible');
		vi.stubGlobal('navigator', { locks: fakeLocks() });
		vi.stubGlobal('document', target);

		const a = leaderHooks();
		const share = startStreamLeadership('ws', a.hooks);
		await vi.advanceTimersByTimeAsync(0);
		expect(a.runs).toHaveBeenCalledTimes(1);

		// Hidden: leadership survives the grace window, then releases.
		setVisibility('hidden');
		await vi.advanceTimersByTimeAsync(HIDE_GRACE_MS - 100);
		expect(a.released()).toBe(0);
		await vi.advanceTimersByTimeAsync(200);
		expect(a.released()).toBe(1);

		// Visible again: re-contends and leads.
		setVisibility('visible');
		await vi.advanceTimersByTimeAsync(0);
		expect(a.runs).toHaveBeenCalledTimes(2);

		share?.stop();
	});

	it('keeps leadership across a brief hide within the grace window', async () => {
		vi.useFakeTimers();
		const { target, setVisibility } = fakeDoc('visible');
		vi.stubGlobal('navigator', { locks: fakeLocks() });
		vi.stubGlobal('document', target);

		const a = leaderHooks();
		const share = startStreamLeadership('ws', a.hooks);
		await vi.advanceTimersByTimeAsync(0);

		setVisibility('hidden');
		await vi.advanceTimersByTimeAsync(HIDE_GRACE_MS - 200);
		setVisibility('visible'); // back before the grace elapses
		await vi.advanceTimersByTimeAsync(HIDE_GRACE_MS);
		// Never released, never re-ran: the same term kept running throughout.
		expect(a.released()).toBe(0);
		expect(a.runs).toHaveBeenCalledTimes(1);

		share?.stop();
	});

	it('releases immediately on pagehide', async () => {
		const { target } = fakeDoc('visible');
		const win = fakeDoc('visible');
		vi.stubGlobal('navigator', { locks: fakeLocks() });
		vi.stubGlobal('document', target);
		vi.stubGlobal('window', win.target);

		const a = leaderHooks();
		const share = startStreamLeadership('ws', a.hooks);
		await flush();
		expect(a.runs).toHaveBeenCalledTimes(1);

		win.fire('pagehide');
		await flush();
		expect(a.released()).toBe(1);

		share?.stop();
	});

	it('reports a Web Locks failure and hands the stream back to the caller', async () => {
		const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
		try {
			// Web Locks present but unusable (a sandboxed frame, a partitioned embed):
			// the request rejects with something that is not our own AbortError.
			const request = vi.fn(() => Promise.reject(new Error('locks denied')));
			const { target, setVisibility } = fakeDoc('visible');
			vi.stubGlobal('navigator', { locks: { request } });
			vi.stubGlobal('document', target);

			const onUnavailable = vi.fn();
			const share = startStreamLeadership('ws', {
				runLeaderLoop: () => new Promise(() => {}),
				onUnavailable,
			});
			await flush();

			// The caller is told once, so it can run its own stream instead of
			// silently riding out the session on the poll interval.
			expect(onUnavailable).toHaveBeenCalledTimes(1);
			expect(errSpy).toHaveBeenCalled();

			// And this tab stops contending: re-showing it must not queue another
			// doomed request (nor a second onUnavailable).
			setVisibility('hidden');
			setVisibility('visible');
			await flush();
			expect(request).toHaveBeenCalledTimes(1);
			expect(onUnavailable).toHaveBeenCalledTimes(1);

			share?.stop();
		} finally {
			errSpy.mockRestore();
		}
	});

	it('stops cleanly while waiting for leadership', async () => {
		vi.stubGlobal('navigator', { locks: fakeLocks() });

		const a = leaderHooks();
		const share1 = startStreamLeadership('ws', a.hooks);
		const b = leaderHooks();
		const share2 = startStreamLeadership('ws', b.hooks);
		await flush();
		share2?.stop(); // still queued — must abort without becoming leader
		await flush();
		expect(b.runs).not.toHaveBeenCalled();
		share1?.stop();
	});
});

describe('startReopenSignal', () => {
	afterEach(() => {
		vi.useRealTimers();
		vi.unstubAllGlobals();
	});

	it('delivers reopen pings and departure notices to peer tabs, not the sender', async () => {
		// Browser-like: userAgent undefined ⇒ not a server runtime, so the real
		// BroadcastChannel (a Node global) wires the two "tabs" together.
		vi.stubGlobal('navigator', {});
		const a = { onReopen: vi.fn(), onDeparted: vi.fn() };
		const b = { onReopen: vi.fn(), onDeparted: vi.fn() };
		const tabA = startReopenSignal('ws-reopen', a);
		const tabB = startReopenSignal('ws-reopen', b);

		tabA.signal();
		tabA.announceDeparture('tab-x');
		await flush();

		// BroadcastChannel never echoes to the sender; the peer sees both.
		expect(a.onReopen).not.toHaveBeenCalled();
		expect(a.onDeparted).not.toHaveBeenCalled();
		expect(b.onReopen).toHaveBeenCalledTimes(1);
		expect(b.onDeparted).toHaveBeenCalledWith('tab-x');

		tabA.stop();
		tabB.stop();
	});

	it('is an inert no-op on a server runtime', () => {
		vi.stubGlobal('navigator', { userAgent: 'Node.js/26' });
		const h = { onReopen: vi.fn(), onDeparted: vi.fn() };
		const s = startReopenSignal('ws-reopen-srv', h);
		s.signal();
		s.announceDeparture('x');
		s.stop();
		expect(h.onReopen).not.toHaveBeenCalled();
		expect(h.onDeparted).not.toHaveBeenCalled();
	});
});
