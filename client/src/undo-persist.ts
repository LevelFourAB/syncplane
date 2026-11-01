// Claim and save of saved undo scopes (see restoreUndoScope in client.ts and
// optimistic/undosaved.ts for the record).
//
// Claim. At most one open scope may hold a saved key: two that both restored
// the same stacks would each replay the same steps, and a text re-insert
// replayed twice puts the text back twice. A claim is a Web Lock per (store,
// workspace, key), taken only if free and held until the scope closes; the
// browser drops it when the tab dies, so there is no claim record to expire.
// Node and SSR keep the claims in memory instead, per store object: there the
// clients that share a store are clients in this process that were handed the
// same store, and clients on different stores must not block each other. A browser without a lock manager cannot see its other tabs' claims,
// so it claims nothing and every scope it opens is unsaved.
//
// Save. Every stack change schedules a save; a save writes the whole record in
// its own transaction after SAVE_DEBOUNCE_MS, so a typing burst costs one write.
// A save first reconciles the stacks with the undo journal, so it never writes
// back a group another tab voided. Closing the scope, or the client, writes
// what is not saved yet.

import { isServerRuntime } from './env.js';
import type { KVStore } from './kv/store.js';
import type { Mutex } from './mutex.js';
import { readUndoJournal } from './optimistic/undojournal.js';
import { encodeSavedUndo, writeSavedUndo } from './optimistic/undosaved.js';
import { reportError } from './report.js';
import { UndoManager } from './undo.js';

/** Delay between a stack change and the save that writes it. */
export const SAVE_DEBOUNCE_MS = 100;

/** Claims held in this process on a server runtime, by store and lock name. */
const localClaims = new WeakMap<KVStore, Set<string>>();

/**
 * Claims the saved key named `name` in `store` if no other open scope holds it.
 * Resolves with the release function, or undefined when the key is held or this
 * runtime cannot claim at all.
 */
export function claimUndoKey(store: KVStore, name: string): Promise<(() => void) | undefined> {
	if (isServerRuntime()) {
		let held = localClaims.get(store);
		if (held === undefined) localClaims.set(store, held = new Set());
		if (held.has(name)) return Promise.resolve(undefined);
		held.add(name);
		let released = false;
		return Promise.resolve(() => {
			if (released) return;
			released = true;
			held.delete(name);
		});
	}
	if (typeof navigator === 'undefined' || navigator.locks === undefined) {
		return Promise.resolve(undefined);
	}
	return new Promise((resolve, reject) => {
		let release!: () => void;
		const held = new Promise<void>((r) => (release = r));
		let granted = false;
		// Held for exactly as long as the callback's promise is pending, so grant
		// and release are separated through `held` (as in text/peer.ts).
		navigator.locks
			.request(name, { ifAvailable: true }, (lock) => {
				if (lock === null) return undefined;
				granted = true;
				resolve(release);
				return held;
			})
			.then(
				() => {
					if (!granted) resolve(undefined);
				},
				(err) => {
					if (!granted) reject(err);
				},
			);
	});
}

/** Where and how a saved scope writes its record. */
export interface UndoSaverOptions {
	kv: KVStore;
	mutex: Mutex;
	workspaceId: string;
	key: string;
	mgr: UndoManager;
	maxScopes: number;
	maxAgeMs: number;
}

/**
 * Keeps one saved scope's record in step with its manager. Construct it after
 * the manager holds the loaded stacks; it saves on every change from then on.
 */
export class UndoSaver {
	private timer: ReturnType<typeof setTimeout> | undefined;
	private dirty = false;
	/** The last save started; every save chains onto it, so they never overlap. */
	private running: Promise<void> | undefined;
	private detached = false;
	/**
	 * What a save writes: the live manager, or after close() a copy of its stacks
	 * as they were at the call. A save reads it only once it holds the write
	 * transaction, so a save that was waiting when close() cleared the manager
	 * writes the copy, not the empty stacks.
	 */
	private source: UndoManager;
	private readonly unsubscribe: () => void;

	constructor(private readonly opts: UndoSaverOptions) {
		this.source = opts.mgr;
		this.unsubscribe = opts.mgr.onChange(() => this.schedule());
	}

	/** Marks the record stale and starts a save after SAVE_DEBOUNCE_MS. */
	schedule(): void {
		if (this.detached) return;
		this.dirty = true;
		if (this.timer !== undefined) return;
		this.timer = setTimeout(() => {
			this.timer = undefined;
			void this.drain();
		}, SAVE_DEBOUNCE_MS);
	}

	/**
	 * Stops saving and writes the stacks as they are now. Call it before the
	 * manager is cleared: the stacks are copied at the call, so the clear is not
	 * saved. The record is written even when nothing changed since the last save,
	 * because another scope's save may have evicted it (see writeSavedUndo).
	 * idle() waits for this write. Never rejects.
	 */
	close(): Promise<void> {
		const { undo, redo } = this.opts.mgr.snapshot();
		this.detach();
		const copy = new UndoManager(Number.MAX_SAFE_INTEGER);
		copy.load(undo, redo);
		this.source = copy;
		return this.chain();
	}

	/** Settles once no save is running or queued. Never rejects. */
	async idle(): Promise<void> {
		while (this.running !== undefined) await this.running;
	}

	/**
	 * Stops saving: later stack changes are not written. A save already started
	 * still runs; idle() waits for it.
	 */
	detach(): void {
		this.detached = true;
		this.dirty = false;
		if (this.timer !== undefined) {
			clearTimeout(this.timer);
			this.timer = undefined;
		}
		this.unsubscribe();
	}

	private async drain(): Promise<void> {
		if (!this.dirty || this.detached) return;
		this.dirty = false;
		await this.chain();
	}

	/** Queues a save after the running one and tracks it as `running`. */
	private chain(): Promise<void> {
		const before = this.running ?? Promise.resolve();
		const run: Promise<void> = before.then(() => this.save()).finally(() => {
			if (this.running === run) this.running = undefined;
		});
		this.running = run;
		return run;
	}

	/** Writes the source's stacks as the record. Never rejects; a failure is reported. */
	private async save(): Promise<void> {
		const { kv, mutex, workspaceId, key, maxScopes, maxAgeMs } = this.opts;
		try {
			await mutex.run(async () => {
				const tx = await kv.write();
				try {
					const mgr = this.source;
					const now = Date.now();
					const cutoff = now - maxAgeMs;
					// Reconciling emits when it drops a group, which marks the record
					// stale again; the encode below already leaves that group out, so
					// the follow-up save writes the same record and stops there.
					mgr.reconcile(await readUndoJournal(tx, workspaceId));
					mgr.dropRecordedBefore(cutoff);
					const record = encodeSavedUndo(mgr.snapshot(), now);
					await writeSavedUndo(tx, workspaceId, key, record, { maxScopes, cutoff });
					await tx.commit();
				} catch (err) {
					tx.release();
					throw err;
				}
			});
		} catch (err) {
			reportError(`syncplane: saving undo scope "${key}" failed`, err);
		}
	}
}
