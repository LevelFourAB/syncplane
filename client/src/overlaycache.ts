import type { DescMessage } from '@bufbuild/protobuf';

import type { EntityRegistry } from './mutations.js';

/**
 * One row of an overlay as a local read sees it, ready to decode.
 *
 * `bytes` is the merged view of the row — the optimistic record with its
 * unacked text fragments already folded in — and is undefined for a local
 * tombstone, which shadows a confirmed row without being one.
 */
export interface OverlayEntry {
	readonly key: string;
	readonly bytes: Uint8Array | undefined;
	/**
	 * Values decoded from `bytes`, by the descriptor they were decoded through.
	 * Filled by {@link overlayEntryValue}; never handed to a caller of the
	 * query, which decodes a returned row from `bytes` again.
	 */
	projections?: WeakMap<DescMessage, unknown>;
}

/**
 * How many bytes of overlay rows one client keeps decoded-ready. The pending
 * queue bounds the overlay in practice; this is the ceiling for the case where
 * it does not, and a pair over it is read from the store every time instead.
 *
 * Counted over `bytes` alone, while an entry also holds whatever values have
 * been decoded through it — a few times its own size for a wide projection.
 */
const OVERLAY_CACHE_BYTES = 2 * 1024 * 1024;

interface CachedOverlay {
	generation: unknown;
	rows: readonly OverlayEntry[];
	bytes: number;
}

/**
 * The overlay of each (entity, workspace) pair a client has read, against the
 * generation token the store held when it was taken.
 *
 * Nothing here decides staleness on its own: a caller hands in the token it
 * just read from the store, and a copy taken under any other token is dropped
 * rather than returned. That is what makes the cache correct across tabs —
 * another tab's write restamps the token inside its own transaction.
 *
 * Least-recently-used, bounded by the total size of the rows held.
 */
export class OverlayCache {
	private entries = new Map<string, CachedOverlay>();
	private used = 0;

	constructor(private budget: number) {}

	/**
	 * The rows cached for `at` when they were taken under `generation`, else
	 * undefined. A copy taken under a different token is dropped: the overlay
	 * has moved on and nothing will ask for that token again.
	 */
	get(at: string, generation: unknown): readonly OverlayEntry[] | undefined {
		const entry = this.entries.get(at);
		if (entry === undefined) return undefined;
		this.entries.delete(at);
		if (entry.generation !== generation) {
			this.used -= entry.bytes;
			return undefined;
		}
		this.entries.set(at, entry);
		return entry.rows;
	}

	/** Records an overlay read under the token the store held when it was taken.
	 * Its rows must not be mutated afterwards. */
	put(at: string, generation: unknown, rows: readonly OverlayEntry[], bytes: number): void {
		// One pair larger than the whole budget would evict everything else and
		// then itself; leave it to the store rather than thrash.
		if (bytes > this.budget) return;
		const existing = this.entries.get(at);
		if (existing !== undefined) {
			this.entries.delete(at);
			this.used -= existing.bytes;
		}
		this.entries.set(at, { generation, rows, bytes });
		this.used += bytes;
		for (const oldest of this.entries.keys()) {
			if (this.used <= this.budget) break;
			this.used -= this.entries.get(oldest)!.bytes;
			this.entries.delete(oldest);
		}
	}
}

/**
 * The overlay cache of one entity registry, created on first use.
 *
 * A registry is built once per client and reaches every read path already, so
 * this scopes the cache to the client without threading it through the read
 * functions and their callers. Weak, so a closed or dropped client takes its
 * cached rows with it.
 */
const caches = new WeakMap<EntityRegistry, OverlayCache>();

export function overlayCacheOf(registry: EntityRegistry): OverlayCache {
	let cache = caches.get(registry);
	if (cache === undefined) caches.set(registry, cache = new OverlayCache(OVERLAY_CACHE_BYTES));
	return cache;
}
