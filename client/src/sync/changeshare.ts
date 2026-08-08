// Cross-tab data-change sharing. Watcher notification is per-tab in-memory
// (WatcherHub) while the data lives in a shared IndexedDB, so a commit in one
// tab leaves other tabs' watchers stale until something else happens to nudge
// them. After every committed write the writing tab broadcasts what it touched
// over a dedicated BroadcastChannel; receiving tabs bring their own watchers up
// to date against the (already committed) shared store.
//
// The tokens on the wire are TouchSet.broadcastTokens() — per-row tokens plus a
// wildcard for each pair they cover — so tabs of different builds can share one
// channel and one store: an older tab sees only wildcards it understands and
// full-scans, while a current one drops the duplicate wildcards and patches its
// snapshots row by row. That is why no format version rides along.
//
// This is a SECOND channel, separate from leadership's: stream leadership carries no payload
// and only ask the leader-elected tab to sync, whereas these messages carry
// the touched entity set and every tab acts on them. On a server runtime
// (clients hold private stores, so another client's touched set means nothing
// to this one) or without BroadcastChannel (old browsers) it degrades to a
// no-op — each tab still notifies its own watchers locally.

import { isServerRuntime } from '../env.js';

export interface ChangeShare {
	/** Fans a committed touch set out to the other tabs. No-op when empty. */
	broadcast(tokens: ReadonlySet<string>): void;
	stop(): void;
}

/**
 * Starts cross-tab change sharing over the BroadcastChannel named `channelName`
 * (StorageNames.changeChannel), which is derived from the store partition so
 * exactly the tabs sharing a store share the channel. `onRemoteChange` fires
 * with the raw tokens (see keys.touchKey / keys.rowTouchKey) each time another
 * tab broadcasts a commit; TouchSet.fromTokens turns them back into a touch set.
 */
export function startChangeShare(
	channelName: string,
	onRemoteChange: (tokens: Set<string>) => void,
): ChangeShare {
	if (isServerRuntime() || typeof BroadcastChannel === 'undefined') {
		return { broadcast: () => {}, stop: () => {} };
	}
	const channel = new BroadcastChannel(channelName);
	channel.onmessage = (ev) => {
		const tokens = ev.data as unknown;
		if (Array.isArray(tokens) && tokens.length > 0) {
			onRemoteChange(new Set(tokens as string[]));
		}
	};
	return {
		broadcast: (tokens) => {
			if (tokens.size === 0) return;
			try {
				channel.postMessage([ ...tokens ]);
			} catch {
				// closed channel — racing stop(); local watchers already fired
			}
		},
		stop: () => channel.close(),
	};
}
