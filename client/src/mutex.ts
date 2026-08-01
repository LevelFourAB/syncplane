/**
 * Reader/writer lock over KV transactions. `run` takes the lock EXCLUSIVELY
 * (writers never interleave — the IDB adapter buffers writes outside a real
 * IDB transaction, so two concurrent logical writers could otherwise race
 * read-modify-write). `runShared` takes it in SHARED mode (readers run
 * concurrently with each other but never alongside a writer), which gives a
 * multi-key logical read a consistent view: a writer's commit cannot land
 * between the separate transactions the read straddles.
 *
 * Both implementations are NON-REENTRANT for exclusive access: calling `run`
 * from inside a running `fn` deadlocks under WebLockMutex (and silently
 * interleaves under ChainMutex). Never nest `run` calls, and never await a
 * `run`/`runShared` from inside a held section (a shared holder awaiting a
 * writer deadlocks). Shared-in-shared is safe (both grant concurrently).
 */

import { isServerRuntime } from './env.js';

export interface Mutex {
	/** Run `fn` with exclusive (writer) access. */
	run<T>(fn: () => Promise<T>): Promise<T>;
	/** Run `fn` with shared (reader) access, excluded from writers. */
	runShared<T>(fn: () => Promise<T>): Promise<T>;
}

/**
 * In-process reader/writer lock chaining async operations. Sufficient for
 * Node, tests and SSR, but provides no cross-tab exclusion.
 *
 * Consecutive shared acquisitions join one batch and run concurrently; an
 * exclusive acquisition closes the batch so later readers form a fresh one
 * that waits behind the writer (writers are not starved by a steady reader
 * stream). Errors thrown by `fn` never poison the chain — the internal
 * release promises always resolve.
 */
export class ChainMutex implements Mutex {
	/** Promise the next acquirer waits on; resolves when the lock frees for it. */
	private tail: Promise<void> = Promise.resolve();
	/** The reader batch currently accepting new shared holders, or null. */
	private openBatch: { prev: Promise<void>; release: () => void; active: number } | null = null;

	run<T>(fn: () => Promise<T>): Promise<T> {
		// Closing the open batch makes following readers wait behind this writer.
		this.openBatch = null;
		const prev = this.tail;
		let release!: () => void;
		this.tail = new Promise<void>((resolve) => (release = resolve));
		return (async () => {
			await prev;
			try {
				return await fn();
			} finally {
				release();
			}
		})();
	}

	runShared<T>(fn: () => Promise<T>): Promise<T> {
		let batch = this.openBatch;
		if (batch === null) {
			// First reader after a writer (or at rest): open a batch whose drain
			// becomes the tail, so a following writer waits for all its readers.
			const prev = this.tail;
			let release!: () => void;
			this.tail = new Promise<void>((resolve) => (release = resolve));
			batch = { prev, release, active: 0 };
			this.openBatch = batch;
		}
		batch.active++;
		const b = batch;
		return (async () => {
			await b.prev;
			try {
				return await fn();
			} finally {
				if (--b.active === 0) {
					if (this.openBatch === b) this.openBatch = null;
					b.release();
				}
			}
		})();
	}
}

/**
 * Cross-tab reader/writer lock backed by the Web Locks API. Two tabs sharing
 * one IndexedDB database (and thus one clientId) coordinate on the same named
 * lock: writers take it exclusively so mutation-id minting never races across
 * tabs, and readers take it shared so a peer tab's rebase/bootstrap commit
 * cannot tear a multi-key read.
 */
export class WebLockMutex implements Mutex {
	constructor(private name: string) {}

	run<T>(fn: () => Promise<T>): Promise<T> {
		// DOM lib types infer the callback's return as the resolution type, so
		// an async fn would nest the promise type; the runtime flattens it.
		return navigator.locks.request(this.name, { mode: 'exclusive' }, fn) as Promise<T>;
	}

	runShared<T>(fn: () => Promise<T>): Promise<T> {
		return navigator.locks.request(this.name, { mode: 'shared' }, fn) as Promise<T>;
	}
}

/**
 * Picks the strongest available mutex for the given lock name: Web Locks in
 * browsers (cross-tab safe), in-process chaining elsewhere. A server runtime is
 * "elsewhere" even though Node >= 26 exposes navigator.locks — its clients hold
 * private stores, so a workspace-named Web Lock would needlessly serialize (and
 * risk deadlocking) two unrelated stores in one process. See env.ts.
 */
export function createKvMutex(name: string): Mutex {
	if (typeof navigator !== 'undefined' && navigator.locks !== undefined && !isServerRuntime()) {
		return new WebLockMutex(name);
	}
	return new ChainMutex();
}
