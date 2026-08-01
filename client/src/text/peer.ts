// Peer leases: the unique writer identity every editable TextDoc needs.
//
// A text item's identity is (peer, counter), and counters are allocated
// Lamport-style out of the document's own version vector — one past everything
// that document has seen. Uniqueness therefore rests on a precondition the CRDT
// itself cannot check: no two documents that can diverge may allocate under the
// same peer. Two that do mint the same id for different content, which merge
// rejects outright, or the same id at different tree positions, which merges
// without complaint into two different texts.
//
// The sync `clientId` does not satisfy that precondition. It is per-device and
// account-scoped, so every tab shares it, and even inside one tab two open
// handles on a field hold two independent TextDocs behind it. A lease narrows
// it to `<clientId>:<slot>`, held for the life of one document instance:
//
//   - Across contexts a slot is a Web Lock. The lock manager is per-origin, so
//     probing slots in ascending order hands every live document instance in
//     every tab of the device a distinct number, and the browser drops the lock
//     when a tab dies — there is no lease record to expire or reap.
//   - Across time a slot is reused the moment it is free. That is safe because
//     a new holder only re-speaks the peer through documents loaded from the
//     same local store that persisted the previous holder's writes, so the
//     version vector it allocates past already covers them. The slot pool and
//     the clientId also live and die together: wiping the store mints a fresh
//     clientId, and the retired peers are never spoken again.
//   - Without Web Locks (Node, SSR, older browsers) the registry is
//     in-process. That is the same degradation the rest of the client accepts
//     there, and it is sound for the same reason: those runtimes hold private
//     stores instead of sharing one across contexts.
//
// Slots stay dense, so a device that edits one document at a time always speaks
// `<clientId>:0` and a document's peer table does not grow with the number of
// sessions that have touched it.

import { isServerRuntime } from '../env.js';

/** A held writer identity, released when its document instance goes away. */
export interface PeerLease {
	/** The writer identity to hand the TextDoc. */
	readonly peer: string;
	/** Frees the slot for reuse. Idempotent. */
	release(): void;
}

/** Hands out unique writer identities for one device (one clientId). */
export interface PeerLeases {
	/** Takes the lowest free slot. Never returns a peer another live lease holds. */
	acquire(): Promise<PeerLease>;
}

/**
 * Probe ceiling. Reaching it means thousands of text documents are open at once
 * on one device; the fallback past it is correct but stops recycling slots.
 */
const MAX_SLOTS = 4096;

/**
 * In-process slot registry (the no-Web-Locks path), keyed by clientId. Module
 * scope, not per-pool: a runtime without Web Locks still has to keep two pools
 * over one clientId apart, and a server process may hold many clientIds — hence
 * the entry going away with its last lease.
 */
const localSlots = new Map<string, Set<number>>();

function holdLocal(clientId: string, slot: number): (() => void) | undefined {
	let taken = localSlots.get(clientId);
	if (taken === undefined) {
		taken = new Set<number>();
		localSlots.set(clientId, taken);
	}
	if (taken.has(slot)) return undefined;
	taken.add(slot);
	const held = taken;
	return () => {
		held.delete(slot);
		// The last release drops the entry; nothing can hold a stale reference to
		// it, because every lease releases exactly once (release is idempotent).
		if (held.size === 0 && localSlots.get(clientId) === held) localSlots.delete(clientId);
	};
}

/**
 * Takes the named Web Lock if it is free right now, resolving with its release
 * function, or with undefined when someone else holds it. It cannot be a plain
 * `await navigator.locks.request(...)`: the lock is held for exactly as long as
 * the callback's promise is pending, so grant and release have to be separated.
 */
function holdWebLock(name: string): Promise<(() => void) | undefined> {
	return new Promise((resolve, reject) => {
		let release!: () => void;
		const held = new Promise<void>((r) => (release = r));
		let granted = false;
		navigator.locks
			.request(name, { ifAvailable: true }, (lock) => {
				if (lock === null) return undefined;
				granted = true;
				resolve(release);
				return held;
			})
			.then(
				// Settles on release once granted; otherwise the lock was busy.
				() => {
					if (!granted) resolve(undefined);
				},
				(err) => {
					if (!granted) reject(err);
				},
			);
	});
}

/** Slot pool for one clientId. One per SyncClient; peers are `<clientId>:<slot>`. */
export function createPeerLeases(clientId: string): PeerLeases {
	const webLocks = typeof navigator !== 'undefined' && navigator.locks !== undefined
		&& !isServerRuntime();
	const hold = (slot: number): Promise<(() => void) | undefined> =>
		webLocks
			? holdWebLock(`syncplane:textpeer:${clientId}:${slot}`)
			: Promise.resolve(holdLocal(clientId, slot));

	return {
		async acquire(): Promise<PeerLease> {
			for (let slot = 0; slot < MAX_SLOTS; slot++) {
				const release = await hold(slot);
				if (release === undefined) continue;
				let released = false;
				return {
					peer: `${clientId}:${slot}`,
					release: () => {
						if (released) return;
						released = true;
						release();
					},
				};
			}
			// Every slot is live. Still unique, but unleased: nothing recycles it,
			// so it costs each document it writes to one permanent peer-table entry.
			return { peer: `${clientId}:u${crypto.randomUUID()}`, release: () => {} };
		},
	};
}
