/**
 * The names one client claims in the browser's per-origin namespaces: the
 * IndexedDB database its store lives in, the Web Locks that serialize the
 * store's readers and writers and elect a stream leader, and the
 * BroadcastChannels its tabs talk over.
 *
 * Sharing state and sharing a name are the same thing here — two tabs are one
 * client's tabs precisely when they derive the same names — so every name below
 * carries the account partition, under a prefix the app may choose
 * (SyncClientOptions.storagePrefix). The prefix is what keeps two unrelated
 * clients on one origin apart when they cannot agree on distinct account ids,
 * and what an app changes to abandon a store wholesale.
 *
 * A name is never parsed back, so the parts only have to be unambiguous enough
 * that two configurations cannot spell the same one; see validateStoragePrefix.
 */

/** The prefix a client uses when SyncClientOptions.storagePrefix is unset. */
export const DEFAULT_STORAGE_PREFIX = 'sync';

/**
 * Caps a prefix. It is repeated in every name and buys nothing past a short
 * label, while an unbounded one reaches an IndexedDB database name.
 */
const MAX_STORAGE_PREFIX_LENGTH = 64;

const STORAGE_PREFIX = /^[A-Za-z0-9][A-Za-z0-9_-]*$/;

/**
 * Throws unless `prefix` starts with an ASCII letter or digit and continues
 * with letters, digits, `_` or `-`, within MAX_STORAGE_PREFIX_LENGTH
 * characters.
 *
 * `:` is the one exclusion that matters: it separates the parts of every name
 * (`<prefix>:stream:<account>` and friends), so a prefix carrying one could
 * spell a name another configuration also spells — `sync:stream` with account
 * `a` naming the same Web Lock as `sync` with account `stream:a`. Two clients
 * that believe they are apart would then share a lock.
 */
export function validateStoragePrefix(prefix: string): void {
	if (prefix === '') throw new Error('storagePrefix is empty');
	if (prefix.length > MAX_STORAGE_PREFIX_LENGTH) {
		throw new Error(
			`storagePrefix "${prefix}" is ${prefix.length} characters, the limit is ${MAX_STORAGE_PREFIX_LENGTH}`,
		);
	}
	if (!STORAGE_PREFIX.test(prefix)) {
		throw new Error(
			`storagePrefix "${prefix}" is not valid: use letters, digits, "_" and "-", `
				+ 'starting with a letter or digit',
		);
	}
}

/** The per-origin names one account's storage and cross-tab coordination use. */
export interface StorageNames {
	/** IndexedDB database holding the KV store. */
	readonly store: string;
	/**
	 * Web Lock serializing the store's readers and writers across tabs. Equal to
	 * `store`: the tabs that must not tear each other's reads are exactly those
	 * sharing a database.
	 */
	readonly kvLock: string;
	/** Web Lock whose holder runs the account's change stream. */
	readonly streamLock: string;
	/** BroadcastChannel carrying committed touch sets between tabs. */
	readonly changeChannel: string;
	/** BroadcastChannel carrying stream-reopen pings and tab departures. */
	readonly reopenChannel: string;
	/**
	 * Web Lock whose holder runs the account's room Watch stream.
	 *
	 * Separate from `streamLock` because the two planes are elected
	 * independently: rooms have to work on a client running no data stream at
	 * all, and the tab holding one need not be the tab holding the other.
	 */
	readonly presenceLock: string;
	/**
	 * BroadcastChannel carrying room frames from the Watch leader to the other
	 * tabs, and their room sets back to it.
	 */
	readonly presenceChannel: string;
	/**
	 * Stem of the per-slot Web Locks that keep text writer identities unique
	 * (see text/peer.ts). Scoped by clientId rather than by account, since that
	 * is what a text peer is derived from; the account never enters the name.
	 */
	readonly textPeerLocks: string;
	/**
	 * IndexedDB database holding the blob upload queue and download cache. Apart
	 * from `store`, so blob bytes never share a transaction with the rows.
	 */
	readonly blobStore: string;
	/** Web Lock whose holder uploads the account's queued blobs. */
	readonly blobLock: string;
	/** BroadcastChannel carrying blob upload status between tabs. */
	readonly blobChannel: string;
}

/** Derives every name of one account's storage from the configured prefix. */
export function storageNames(prefix: string, accountId: string): StorageNames {
	return {
		store: `${prefix}:${accountId}`,
		kvLock: `${prefix}:${accountId}`,
		streamLock: `${prefix}:stream:${accountId}`,
		changeChannel: `${prefix}:change:${accountId}`,
		reopenChannel: `${prefix}:reopen:${accountId}`,
		presenceLock: `${prefix}:presencelock:${accountId}`,
		presenceChannel: `${prefix}:presence:${accountId}`,
		textPeerLocks: `${prefix}:textpeer`,
		blobStore: `${prefix}:blobs:${accountId}`,
		blobLock: `${prefix}:bloblock:${accountId}`,
		blobChannel: `${prefix}:blob:${accountId}`,
	};
}
