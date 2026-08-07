import { fromBinary, toBinary } from '@bufbuild/protobuf';

import { type Subscription, SubscriptionSchema } from '../gen/syncplane/v1/patch_pb.js';
import { INTENT_PREFIX, intentKey } from '../keys.js';
import type { KVRead, KVStore, KVWrite } from '../kv/store.js';
import type { Mutex } from '../mutex.js';

/**
 * Cross-tab sync intent. Every tab sharing a KV store writes a small record at
 * `intent/<tabId>` describing what it needs from sync — which workspaces it
 * holds, each one's subscription set, and the mutation ids its undo/redo stacks
 * still reference — plus a heartbeat timestamp. Each tab then syncs with the
 * UNION of all live tabs' intent:
 *
 *  - Union scopes: every tab syncs the same workspaces with the same
 *    per-workspace subscription set, so the server never sees two tabs
 *    alternating scopes against the one shared cursor (the delta-resync-forever
 *    subscription thrash), and the single leader stream carries every workspace
 *    any tab needs rather than only the leader's own.
 *  - Union undo refs: compaction — whichever tab runs it — protects every
 *    tab's undo-referenced mutations, not just its own, read atomically
 *    inside the compaction transaction. A peer's referenced ids are simply
 *    never renumbered, so no cross-tab remap protocol is needed. Ids are
 *    counted per workspace, so the union is taken per workspace too.
 *
 * Liveness is heartbeat-TTL based — no locks, no channels, no leader — so
 * this works identically in browsers and Node. A stale record only ever
 * over-pulls or over-protects (never incorrect), bounded by the TTLs below;
 * a crashed tab's record ages out, a closed tab deletes its own. A client
 * with a private store sees only its own record: union = self = single-tab
 * behavior.
 */

/**
 * Refresh the own record's heartbeat during a sync cycle when it is older
 * than this. Any record write (refs, subs) counts as a heartbeat.
 */
export const HEARTBEAT_MS = 5 * 60_000;

/**
 * Records older than this drop out of the scope union. An idle but open tab
 * stays fresh through its own poll/stream sync cycles, which heartbeat its
 * record; only a tab gone for this long stops widening everyone's sync scope.
 */
export const SUBS_TTL_MS = 30 * 60_000;

/**
 * Records older than this stop protecting undo refs and are deleted.
 * Deliberately generous: over-protection only costs compaction efficiency,
 * and a long-suspended tab keeps a usable undo stack when it wakes. Only the
 * never-pushed tail is at stake — pushed mutations are already shielded by
 * the per-workspace pushed-through watermark.
 */
export const GC_TTL_MS = 7 * 24 * 60 * 60_000;

/**
 * Per-workspace subscription sets a tab is syncing. A workspace present with an
 * empty array means "full sync of that workspace"; a workspace absent means the
 * tab does not hold it at all.
 */
export type ScopeMap = ReadonlyMap<string, Subscription[]>;

/** Shape stored at `intent/<tabId>`. All fields structured-clone safe (IDB). */
export interface IntentRecord {
	/** Workspace id → wire-encoded Subscriptions (toBinary each). Empty = full sync. */
	subs: Record<string, Uint8Array[]>;
	/** Workspace id → undo/redo-referenced mutation ids as decimal strings. */
	undoRefIds: Record<string, string[]>;
	/** Heartbeat (epoch ms); drives the TTLs above. */
	updatedAt: number;
}

export interface IntentRegistryOptions {
	heartbeatMs?: number;
	subsTtlMs?: number;
	gcTtlMs?: number;
	/** Clock, injectable for tests. Default Date.now. */
	now?: () => number;
}

export class IntentRegistry {
	/** Ephemeral per-instance id; never persisted across reloads. */
	readonly tabId: string = crypto.randomUUID();
	private readonly kv: KVStore;
	private readonly kvMutex: Mutex;
	private readonly heartbeatMs: number;
	private readonly subsTtlMs: number;
	private readonly gcTtlMs: number;
	private readonly now: () => number;
	/**
	 * In-memory master copy of the own record's payload. Every write persists
	 * the whole record from these, so a record another tab wiped (e.g. a format
	 * upgrade) is recreated intact by the next write instead of degrading to a
	 * partial read-modify-write against a missing base.
	 */
	private ownSubs: Record<string, Uint8Array[]> = {};
	private ownRefs: Record<string, string[]> = {};
	/** When the own record was last written (any field); heartbeat gate. */
	private lastWriteAt = 0;
	private seeded = false;
	private deleted = false;

	constructor(kv: KVStore, kvMutex: Mutex, opts?: IntentRegistryOptions) {
		this.kv = kv;
		this.kvMutex = kvMutex;
		this.heartbeatMs = opts?.heartbeatMs ?? HEARTBEAT_MS;
		this.subsTtlMs = opts?.subsTtlMs ?? SUBS_TTL_MS;
		this.gcTtlMs = opts?.gcTtlMs ?? GC_TTL_MS;
		this.now = opts?.now ?? Date.now;
	}

	/** Writes the initial record. Called once from Coordinator.start(). */
	async seed(scopes: ScopeMap, refs: ReadonlyMap<string, ReadonlySet<bigint>>): Promise<void> {
		this.ownSubs = encodeScopes(scopes);
		this.ownRefs = encodeAllRefs(refs);
		this.seeded = true;
		await this.writeOwn();
	}

	/**
	 * Persists a workspace's new undo-ref set inside a caller-owned open
	 * transaction (the enqueue path), so the record commits atomically with the
	 * mutation whose undo bookkeeping it predicts. No-op before seed() or after
	 * delete().
	 */
	async writeRefs(
		tx: KVWrite,
		workspaceId: string,
		refs: ReadonlySet<bigint>,
	): Promise<void> {
		if (!this.seeded || this.deleted) return;
		this.setRefs(workspaceId, refs);
		await this.putRecord(tx);
	}

	/** Persists a new per-workspace scope map in its own transaction. */
	async updateScopes(scopes: ScopeMap): Promise<void> {
		if (!this.seeded || this.deleted) return;
		this.ownSubs = encodeScopes(scopes);
		// A workspace this tab dropped keeps no undo refs: its stacks went with it.
		for (const ws of Object.keys(this.ownRefs)) {
			if (!scopes.has(ws)) delete this.ownRefs[ws];
		}
		await this.writeOwn();
	}

	/**
	 * Persists a workspace's new undo-ref set in its own transaction — for stack
	 * changes outside an enqueue (a rejection dropping groups, a compaction
	 * remap).
	 */
	async updateRefs(workspaceId: string, refs: ReadonlySet<bigint>): Promise<void> {
		if (!this.seeded || this.deleted) return;
		this.setRefs(workspaceId, refs);
		await this.writeOwn();
	}

	/**
	 * Refreshes the own record's updatedAt when older than the heartbeat
	 * interval; called once per sync cycle. Tracks the last write in memory
	 * (every record write goes through this instance), so the common fresh
	 * case costs no transaction at all.
	 */
	async heartbeatIfStale(): Promise<void> {
		if (!this.seeded || this.deleted) return;
		if (this.now() - this.lastWriteAt < this.heartbeatMs) return;
		await this.writeOwn();
	}

	/**
	 * The scope map this tab should sync with: the given own in-memory map
	 * (always included, covering the window before the own record write lands)
	 * unioned with every live record's. A workspace present in any live record is
	 * in the union; within a workspace, its subscription sets are unioned and
	 * deduplicated by wire encoding, with full sync absorbing (any participant
	 * asking for everything makes the union everything). Records past GC_TTL_MS
	 * are deleted via a fire-and-forget write transaction (acquired sequentially
	 * after the shared scan — the mutex is non-reentrant).
	 */
	async unionScopes(own: ScopeMap): Promise<Map<string, Subscription[]>> {
		const now = this.now();
		const ownKey = intentKey(this.tabId);
		const live: IntentRecord[] = [];
		const doomed: string[] = [];
		await this.kvMutex.runShared(async () => {
			const tx = await this.kv.read();
			try {
				for await (const [ k, v ] of tx.scan(INTENT_PREFIX)) {
					const rec = asIntentRecord(v);
					if (!rec) continue;
					if (now - rec.updatedAt > this.gcTtlMs) {
						// Own record is never GC'd here: heartbeats keep it fresh, and a
						// suspended-then-resumed tab must not delete itself mid-cycle.
						if (k !== ownKey) doomed.push(k);
						continue;
					}
					if (now - rec.updatedAt <= this.subsTtlMs) live.push(rec);
				}
			} finally {
				tx.release();
			}
		});
		if (doomed.length > 0) void this.gc(doomed, now);

		// Own workspaces first so the union's iteration order is stable and starts
		// with what this tab actually asked for.
		const out = new Map<string, Subscription[]>();
		const seen = new Map<string, Set<string>>();
		/** Workspaces whose union is already full sync; further sets cannot narrow it. */
		const full = new Set<string>();
		const add = (ws: string, subs: readonly Uint8Array[]): void => {
			if (!out.has(ws)) {
				out.set(ws, []);
				seen.set(ws, new Set());
			}
			if (full.has(ws)) return;
			if (subs.length === 0) {
				// A full-sync participant widens the workspace for everyone.
				full.add(ws);
				out.set(ws, []);
				return;
			}
			const dedup = seen.get(ws)!;
			const list = out.get(ws)!;
			for (const bytes of subs) {
				const key = dedupKey(bytes);
				if (dedup.has(key)) continue;
				dedup.add(key);
				try {
					list.push(fromBinary(SubscriptionSchema, bytes));
				} catch {
					// malformed peer record entry: skip, never wedge the cycle
				}
			}
		};
		for (const [ ws, subs ] of own) add(ws, subs.map((s) => toBinary(SubscriptionSchema, s)));
		for (const rec of live) {
			for (const [ ws, subs ] of Object.entries(rec.subs)) add(ws, subs);
		}
		return out;
	}

	/**
	 * The undo-ref union compaction of `workspaceId` must protect: the given own
	 * in-memory set unioned with every record younger than GC_TTL_MS. MUST run
	 * inside the caller's open compaction transaction so the union is atomic with
	 * the renumber — a peer's reference written after this scan only concerns
	 * mutations enqueued after it, which get ids above the renumbered range.
	 */
	async unionUndoRefIds(
		tx: KVRead,
		workspaceId: string,
		own: ReadonlySet<bigint>,
	): Promise<Set<bigint>> {
		const now = this.now();
		const ids = new Set(own);
		for await (const [ , v ] of tx.scan(INTENT_PREFIX)) {
			const rec = asIntentRecord(v);
			if (!rec || now - rec.updatedAt > this.gcTtlMs) continue;
			for (const raw of rec.undoRefIds[workspaceId] ?? []) {
				try {
					ids.add(BigInt(raw));
				} catch {
					// malformed id in a peer record: skip
				}
			}
		}
		return ids;
	}

	/**
	 * Removes the own record (clean shutdown). Crash/close-without-stop relies
	 * on the TTLs instead. Later writes through this instance become no-ops.
	 */
	async delete(): Promise<void> {
		this.deleted = true;
		await this.kvMutex.run(async () => {
			const tx = await this.kv.write();
			try {
				await tx.del(intentKey(this.tabId));
				await tx.commit();
			} catch (err) {
				tx.release();
				throw err;
			}
		});
	}

	/**
	 * Removes a PEER tab's record on a departure signal from that tab, so it
	 * stops widening the scope union immediately instead of lingering for up to
	 * SUBS_TTL_MS. Idempotent (the record may already be gone) and a no-op for
	 * the own tabId — the own record is only ever removed by delete(), which
	 * also disarms future writes. Best-effort: a failed reap just leaves the
	 * record for the TTL to collect.
	 */
	async reap(tabId: string): Promise<void> {
		if (tabId === this.tabId) return;
		await this.kvMutex.run(async () => {
			const tx = await this.kv.write();
			try {
				await tx.del(intentKey(tabId));
				await tx.commit();
			} catch (err) {
				tx.release();
				throw err;
			}
		});
	}

	private setRefs(workspaceId: string, refs: ReadonlySet<bigint>): void {
		if (refs.size === 0) delete this.ownRefs[workspaceId];
		else this.ownRefs[workspaceId] = encodeRefs(refs);
	}

	/** Writes the own record in its own exclusive transaction. */
	private async writeOwn(): Promise<void> {
		await this.kvMutex.run(async () => {
			// A delete() that landed while this write was queued must win.
			if (this.deleted) return;
			const tx = await this.kv.write();
			try {
				await this.putRecord(tx);
				await tx.commit();
			} catch (err) {
				tx.release();
				throw err;
			}
		});
	}

	private async putRecord(tx: KVWrite): Promise<void> {
		const record: IntentRecord = {
			subs: this.ownSubs,
			undoRefIds: this.ownRefs,
			updatedAt: this.now(),
		};
		this.lastWriteAt = record.updatedAt;
		await tx.put(intentKey(this.tabId), record);
	}

	/** Best-effort delete of expired peer records found during a union scan. */
	private async gc(keys: string[], now: number): Promise<void> {
		try {
			await this.kvMutex.run(async () => {
				const tx = await this.kv.write();
				try {
					for (const k of keys) {
						// Re-check inside the write transaction: the owner may have
						// heartbeated between the shared scan and this delete.
						const rec = asIntentRecord(await tx.get(k));
						if (rec && now - rec.updatedAt <= this.gcTtlMs) continue;
						await tx.del(k);
					}
					await tx.commit();
				} catch (err) {
					tx.release();
					throw err;
				}
			});
		} catch {
			// Best-effort: a failed GC just leaves stale records for a later pass.
		}
	}
}

function encodeScopes(scopes: ScopeMap): Record<string, Uint8Array[]> {
	const out: Record<string, Uint8Array[]> = {};
	for (const [ ws, subs ] of scopes) {
		out[ws] = subs.map((s) => toBinary(SubscriptionSchema, s));
	}
	return out;
}

function encodeRefs(refs: ReadonlySet<bigint>): string[] {
	return [ ...refs ].map((id) => id.toString());
}

function encodeAllRefs(
	refs: ReadonlyMap<string, ReadonlySet<bigint>>,
): Record<string, string[]> {
	const out: Record<string, string[]> = {};
	for (const [ ws, ids ] of refs) {
		if (ids.size > 0) out[ws] = encodeRefs(ids);
	}
	return out;
}

/**
 * Dedup key for a wire-encoded Subscription: base64 of its bytes. create()-
 * built messages encode deterministically, so equal subscriptions collide.
 */
function dedupKey(bytes: Uint8Array): string {
	let bin = '';
	for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]!);
	return btoa(bin);
}

/** Validates a scanned value as an IntentRecord; undefined for foreign shapes. */
function asIntentRecord(v: unknown): IntentRecord | undefined {
	if (typeof v !== 'object' || v === null) return undefined;
	const rec = v as Partial<IntentRecord>;
	if (
		typeof rec.subs !== 'object' || rec.subs === null || Array.isArray(rec.subs)
		|| typeof rec.undoRefIds !== 'object' || rec.undoRefIds === null
		|| Array.isArray(rec.undoRefIds)
		|| typeof rec.updatedAt !== 'number'
	) {
		return undefined;
	}
	return rec as IntentRecord;
}
