/**
 * A KVStore decorator that tallies what the client commits to storage, split by
 * keyspace. It exists for one question the e2e tier cannot otherwise ask: what
 * does a single text keystroke actually write to disk?
 *
 * The pending queue can be measured after the fact by scanning `p/` (see the
 * harness's pendingQueueBytes), because a queue entry stays until it is acked.
 * The optimistic overlay cannot: every write to `o/<entity>/<ws>/<key>`
 * overwrites the last one, so its cost is a flow, not a level, and only the
 * write path itself can report it. Metering the store is the one place both
 * numbers are visible in the same units.
 *
 * It wraps a real IDBKVStore rather than replacing it, so a metered tab still
 * exercises the same storage, locking and structured-clone behavior as every
 * other spec in this tier — the meter only counts what goes past.
 */

import type { KVRead, KVStore, KVWrite } from '../../../src/kv/store.js';

/** Bytes committed per keyspace since the last reset. See keys.ts for the layout. */
export interface WriteTally {
	/** Optimistic overlay rows (`o/`) — one whole row per local mutation. */
	overlay: number;
	/** Optimistic text log (`ot/`) — one fragment per local text edit. */
	overlayText: number;
	/** Pending mutation queue (`p/`). */
	pending: number;
	/** Server-confirmed rows (`c/`). */
	confirmed: number;
	/** Bootstrap staging (`b/`). */
	staging: number;
	/** Cursors, watermarks, format version, per-tab intent. */
	meta: number;
	/** Everything above, plus anything unclassified. */
	total: number;
	/** Committed transactions — the count `total` is spread across. */
	commits: number;
}

const emptyTally = (): WriteTally => ({
	overlay: 0,
	overlayText: 0,
	pending: 0,
	confirmed: 0,
	staging: 0,
	meta: 0,
	total: 0,
	commits: 0,
});

/**
 * Size of a structured-cloneable value, in bytes, counted the way the storage
 * cost actually scales: byte arrays at their true length, strings at UTF-16, and
 * a flat per-property allowance for the record wrappers.
 *
 * Deliberately approximate — no engine exposes its real structured-clone
 * footprint, and the numbers this feeds are ratios (delta vs document), where a
 * constant per-record fudge cancels out. What it must not do is undercount the
 * payload, which is why Uint8Array and string are exact.
 */
function sizeOf(value: unknown): number {
	if (value === null || value === undefined) return 0;
	if (value instanceof Uint8Array) return value.byteLength;
	if (ArrayBuffer.isView(value)) return value.byteLength;
	if (typeof value === 'string') return value.length * 2;
	if (typeof value === 'number') return 8;
	if (typeof value === 'bigint') return 8;
	if (typeof value === 'boolean') return 4;
	if (Array.isArray(value)) return value.reduce<number>((n, v) => n + sizeOf(v), 0);
	if (typeof value === 'object') {
		let total = 0;
		for (const [ k, v ] of Object.entries(value)) total += k.length * 2 + sizeOf(v);
		return total;
	}
	return 0;
}

/** Which counter a key belongs to. Mirrors the prefixes in src/keys.ts. */
function bucketOf(key: string): keyof WriteTally {
	if (key.startsWith('ot/')) return 'overlayText';
	if (key.startsWith('o/')) return 'overlay';
	if (key.startsWith('p/')) return 'pending';
	if (key.startsWith('c/')) return 'confirmed';
	if (key.startsWith('b/')) return 'staging';
	if (key.startsWith('meta/') || key.startsWith('intent/')) return 'meta';
	return 'total';
}

export class WriteMeter implements KVStore {
	private tally = emptyTally();

	constructor(private inner: KVStore) {}

	/** The tally since the last reset. */
	tallied(): WriteTally {
		return { ...this.tally };
	}

	reset(): void {
		this.tally = emptyTally();
	}

	/**
	 * Called by a metered transaction at commit. Deletes are counted as their key
	 * only: the row goes away, and what a delete costs is the tombstone bookkeeping,
	 * not the value that left.
	 */
	private record(puts: Map<string, unknown>, deletes: Set<string>): void {
		this.tally.commits++;
		for (const [ k, v ] of puts) {
			const n = k.length * 2 + sizeOf(v);
			this.tally.total += n;
			const bucket = bucketOf(k);
			if (bucket !== 'total') this.tally[bucket] += n;
		}
		for (const k of deletes) {
			const n = k.length * 2;
			this.tally.total += n;
			const bucket = bucketOf(k);
			if (bucket !== 'total') this.tally[bucket] += n;
		}
	}

	async read(): Promise<KVRead> {
		return await this.inner.read();
	}

	async write(): Promise<KVWrite> {
		const tx = await this.inner.write();
		return new MeteredWrite(tx, (puts, deletes) => this.record(puts, deletes));
	}

	async close(): Promise<void> {
		await this.inner.close();
	}
}

/**
 * Buffers what the transaction was told to write so the meter can tally it at
 * commit. The buffer mirrors IDBKVStore's own — last write per key wins, a
 * delete cancels a pending put — so the tally counts what reaches disk rather
 * than how many times the client changed its mind on the way there.
 */
class MeteredWrite implements KVWrite {
	private puts = new Map<string, unknown>();
	private deletes = new Set<string>();

	constructor(
		private inner: KVWrite,
		private onCommit: (puts: Map<string, unknown>, deletes: Set<string>) => void,
	) {}

	get(key: string): Promise<unknown | undefined> {
		return this.inner.get(key);
	}

	getMany(keys: readonly string[]): Promise<(unknown | undefined)[]> {
		return this.inner.getMany(keys);
	}

	has(key: string): Promise<boolean> {
		return this.inner.has(key);
	}

	scan(prefix: string): AsyncIterable<[key: string, value: unknown]> {
		return this.inner.scan(prefix);
	}

	scanRange(lo?: string, hi?: string): AsyncIterable<[key: string, value: unknown]> {
		return this.inner.scanRange(lo, hi);
	}

	indexScan(lo?: string, hi?: string): AsyncIterable<string> {
		return this.inner.indexScan(lo, hi);
	}

	indexCount(lo?: string, hi?: string): Promise<number> {
		return this.inner.indexCount(lo, hi);
	}

	async put(key: string, value: unknown): Promise<void> {
		this.deletes.delete(key);
		this.puts.set(key, value);
		await this.inner.put(key, value);
	}

	async del(key: string): Promise<void> {
		this.puts.delete(key);
		this.deletes.add(key);
		await this.inner.del(key);
	}

	async commit(): Promise<void> {
		await this.inner.commit();
		// Only after the commit resolves: a transaction that threw wrote nothing.
		this.onCommit(this.puts, this.deletes);
	}

	release(): void {
		this.inner.release();
	}
}
