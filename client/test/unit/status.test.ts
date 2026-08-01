import { describe, expect, it, vi } from 'vitest';

import { StatusTracker, type SyncStatus } from '../../src/sync/status.js';

/** The workspace these specs count pending mutations for. */
const WS = 'ws1';

describe('StatusTracker', () => {
	it('starts idle, offline and clean', () => {
		const tracker = new StatusTracker();
		expect(tracker.current()).toEqual({
			state: 'idle',
			online: false,
			dirty: false,
			pendingMutations: 0,
			workspaces: {},
			scope: undefined,
			workspaceAccess: undefined,
		});
	});

	it('emits on change and passes the new snapshot', () => {
		const tracker = new StatusTracker();
		const seen: SyncStatus[] = [];
		tracker.onChange((s) => seen.push(s));

		tracker.update({ state: 'syncing' });
		tracker.update({ state: 'idle', online: true });
		expect(seen.map((s) => s.state)).toEqual([ 'syncing', 'idle' ]);
		expect(seen[1]?.online).toBe(true);
	});

	it('suppresses no-op updates', () => {
		const tracker = new StatusTracker();
		tracker.addWorkspace(WS);
		let emits = 0;
		tracker.onChange(() => emits++);

		tracker.update({ state: 'idle', online: false });
		tracker.setPendingCount(WS, 0);
		expect(emits).toBe(0);

		tracker.update({ lastError: { message: 'boom' } });
		tracker.update({ lastError: { message: 'boom' } });
		expect(emits).toBe(1);

		const at = new Date();
		tracker.update({ lastSyncedAt: at });
		tracker.update({ lastSyncedAt: new Date(at.getTime()) });
		expect(emits).toBe(2);
	});

	it('derives dirty and the total from the per-workspace pending counts', () => {
		const tracker = new StatusTracker();
		tracker.incrementPending(WS);
		expect(tracker.current()).toMatchObject({ dirty: true, pendingMutations: 1 });
		tracker.incrementPending(WS);
		expect(tracker.current().pendingMutations).toBe(2);
		// A second workspace adds to the same total; dirty stays true until both
		// queues drain.
		tracker.incrementPending('ws2');
		expect(tracker.current().pendingMutations).toBe(3);
		tracker.setPendingCount(WS, 0);
		expect(tracker.current()).toMatchObject({ dirty: true, pendingMutations: 1 });
		tracker.setPendingCount('ws2', 0);
		expect(tracker.current()).toMatchObject({ dirty: false, pendingMutations: 0 });
		expect(tracker.current().workspaces[WS]).toEqual({
			workspaceId: WS,
			pendingMutations: 0,
		});
	});

	it('mirrors the default workspace scope and access onto the top level', () => {
		const tracker = new StatusTracker();
		tracker.addWorkspace(WS);
		tracker.addWorkspace('ws2');
		tracker.setDefaultWorkspace(WS);
		tracker.updateWorkspace(WS, { scope: { own: 1, effective: 0, widened: true } });
		tracker.updateWorkspace('ws2', { access: 'revoked' });

		expect(tracker.current().scope).toEqual({ own: 1, effective: 0, widened: true });
		// The mirror follows the DEFAULT workspace only: ws2's revocation is
		// visible per workspace, not as an account-wide verdict.
		expect(tracker.current().workspaceAccess).toBeUndefined();
		expect(tracker.current().workspaces['ws2']?.access).toBe('revoked');

		tracker.removeWorkspace('ws2');
		expect(tracker.current().workspaces['ws2']).toBeUndefined();
	});

	it('unsubscribes cleanly', () => {
		const tracker = new StatusTracker();
		let emits = 0;
		const unsub = tracker.onChange(() => emits++);
		tracker.update({ state: 'syncing' });
		unsub();
		tracker.update({ state: 'error' });
		expect(emits).toBe(1);
	});

	it('isolates a throwing listener from update() and from other listeners', () => {
		const tracker = new StatusTracker();
		const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
		try {
			const seen: SyncStatus[] = [];
			tracker.onChange(() => {
				throw new Error('boom');
			});
			tracker.onChange((s) => seen.push(s));

			// A throwing listener must not propagate out of update() (the caller's
			// write is already committed) and must not skip later listeners.
			expect(() => tracker.update({ state: 'syncing' })).not.toThrow();
			expect(seen.map((s) => s.state)).toEqual([ 'syncing' ]);
			expect(errSpy).toHaveBeenCalled();
		} finally {
			errSpy.mockRestore();
		}
	});
});
