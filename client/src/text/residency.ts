// Keeping decoded text documents around, addressed by the bytes they encode to.
//
// Every path that changes a text field already materialises a document: the
// optimistic fold decodes the row, applies the `ot/` fragments and re-encodes
// (read.ts), and an arriving pull page does the same against the confirmed row
// (sync/textdelta.ts). Microseconds later a reader that renders the field
// decodes those very bytes back into the very same document — `textOf` in
// project.ts, `plainTextOf` in query evaluation. At 50 000 code points that
// second decode is ~13 ms and is the largest single term left on the client's
// read path.
//
// This is where a producer leaves the document it built so a reader can use it
// instead of rebuilding it. It is also where the pull path picks the confirmed
// row's document back up, which is the client's counterpart of the server's
// `engine/server/textcache.go`: applying a remote fragment then costs an encode
// rather than a decode and an encode, and it does so inside the KV write
// transaction, which is held exclusively across tabs.
//
// # Why a hit is always correct
//
// The encoding is canonical and injective — two documents encode to the same
// bytes exactly when they are the same document — so "the document for these
// bytes" is well defined without knowing which row, field, workspace or client
// the bytes came from. Entries are therefore keyed by a CONTENT fingerprint and
// confirmed by comparing the bytes themselves. The fingerprint decides only
// where to look; the comparison decides whether the answer is right, exactly as
// in textfold.ts. A collision, a stale entry, a document left by a mutation
// that was since rejected, or bytes from an entirely different client sharing
// this process cannot return a wrong document — none of them can pass the
// comparison.
//
// That is what lets one cache serve every producer and every reader with no
// invalidation and no identity plumbed through the read path. `textOf` takes
// bytes and nothing else; it could not be handed a row key to look up even if
// there were one to give.
//
// # Ownership: take, lend, put
//
// Merging mutates a document in place, so the hazard is aliasing rather than
// staleness:
//
//   - `take` REMOVES the entry and transfers ownership. A caller that is going
//     to mutate must take, never lend, and must `put` whatever the document
//     became — under its new bytes. A caller that fails part-way simply puts
//     nothing back, and the cost of that is a miss.
//   - `lend` hands out a document that stays registered, for read-only use
//     within the synchronous call. A borrower must not mutate it and must not
//     retain it: the next `take` may hand it to a writer. Projecting it to a
//     string or to mark spans — which is all any reader wants — is safe, and
//     what comes back is immutable.
//   - `put` registers a document the caller will not touch again.
//
// Because a mutation is only reachable through `take`, and `take` removes the
// entry, every registered document encodes to exactly the bytes beside it.
//
// One invariant the callers keep rather than this file: what is registered is
// always a document decoded for reading, with no local peer. A live TextHandle's
// document is never handed over, so a taker can merge into what it gets and
// re-encode without an editing identity leaking into the bytes.

import type { TextDoc } from './doc.js';

/**
 * How much decoded text state the process keeps resident, counted in ENCODED
 * bytes.
 *
 * Deliberately small next to the fold cache's budget, because what is retained
 * here is far heavier than what is counted: a decoded document costs a node, a
 * child list and an id per code point, which is an order of magnitude more live
 * heap than its encoding. The value is concentrated in the one or two documents
 * currently being changed — by the local typist or by a peer — so a budget that
 * holds a handful of large documents captures nearly all of it.
 */
const RESIDENT_BYTES = 1 << 20;

/**
 * Documents below this encoded size are not kept.
 *
 * Decoding a kilobyte is tens of microseconds, so there is nothing to save, and
 * an app that renders hundreds of small text fields at once — cards on a canvas,
 * titles in a list — would otherwise fill the budget with entries whose hits are
 * worth nothing and evict the large document that is worth everything.
 */
const MIN_RESIDENT_BYTES = 1024;

interface Resident {
	/** The bytes this document encodes to. Retained for the comparison. */
	readonly bytes: Uint8Array;
	readonly doc: TextDoc;
}

/**
 * How many bytes are sampled to build a fingerprint. The fingerprint only
 * chooses a bucket, so the cost of sampling too little is an occasional miss;
 * a fixed budget keeps it O(1) in the document instead of O(size).
 */
const SAMPLES = 64;

/**
 * A content fingerprint over the encoded bytes: the length, a strided sample,
 * and the last few bytes (an append lands there and a stride can step over it).
 */
function fingerprint(b: Uint8Array): string {
	let h = 0x811c9dc5;
	const mix = (x: number): void => {
		h = Math.imul(h ^ x, 0x01000193) >>> 0;
	};
	const step = Math.max(1, Math.floor(b.length / SAMPLES));
	for (let i = 0; i < b.length; i += step) mix(b[i]!);
	for (let i = Math.max(0, b.length - 8); i < b.length; i++) mix(b[i]!);
	return `${b.length}:${h}`;
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
	if (a === b) return true;
	if (a.length !== b.length) return false;
	for (let i = 0; i < a.length; i++) {
		if (a[i] !== b[i]) return false;
	}
	return true;
}

/**
 * A bounded, least-recently-used set of decoded documents, addressed by the
 * bytes they encode to. Not a correctness mechanism: dropping every entry only
 * costs the decodes they would have saved.
 */
export class DocResidency {
	// Map iteration is insertion order, and every read re-inserts, so the head
	// is always the least recently used entry.
	private entries = new Map<string, Resident>();
	private used = 0;

	/** Hits and misses over `lend` and `take`, for tests and benchmarks. */
	readonly stats = { hits: 0, misses: 0 };

	constructor(private readonly budget: number) {}

	/**
	 * The document `bytes` encode to, if it is resident — for reading only,
	 * within the caller's synchronous scope. See the file comment: the entry
	 * stays registered, so the document must be neither mutated nor retained.
	 */
	lend(bytes: Uint8Array): TextDoc | undefined {
		const entry = this.lookup(bytes);
		if (entry === undefined) return undefined;
		// Refresh recency: this document is being read, so it is part of the
		// working set even if nothing is writing to it.
		this.entries.delete(entry.key);
		this.entries.set(entry.key, entry.resident);
		return entry.resident.doc;
	}

	/**
	 * The document `bytes` encode to, removed from the cache and handed over.
	 * The caller owns it and may mutate it, and should `put` what it becomes.
	 */
	take(bytes: Uint8Array): TextDoc | undefined {
		const entry = this.lookup(bytes);
		if (entry === undefined) return undefined;
		this.drop(entry.key);
		return entry.resident.doc;
	}

	/**
	 * Registers `doc` as the document for `bytes`. Neither may be mutated
	 * afterwards without taking the entry back out first.
	 */
	put(bytes: Uint8Array, doc: TextDoc): void {
		if (bytes.length < MIN_RESIDENT_BYTES || bytes.length > this.budget) return;
		const key = fingerprint(bytes);
		this.drop(key);
		this.entries.set(key, { bytes, doc });
		this.used += bytes.length;
		for (const oldest of this.entries.keys()) {
			if (this.used <= this.budget) break;
			this.drop(oldest);
		}
	}

	/** Drops everything. Only affects how much work the next read does. */
	clear(): void {
		this.entries.clear();
		this.used = 0;
	}

	/** Resident documents. For tests. */
	get count(): number {
		return this.entries.size;
	}

	private lookup(bytes: Uint8Array): { key: string; resident: Resident } | undefined {
		if (bytes.length < MIN_RESIDENT_BYTES) return undefined;
		const key = fingerprint(bytes);
		const resident = this.entries.get(key);
		if (resident === undefined || !bytesEqual(resident.bytes, bytes)) {
			// A fingerprint collision leaves an entry that will never be asked
			// for under this key again; the LRU walk reclaims it in time.
			this.stats.misses++;
			return undefined;
		}
		this.stats.hits++;
		return { key, resident };
	}

	private drop(key: string): void {
		const existing = this.entries.get(key);
		if (existing === undefined) return;
		this.entries.delete(key);
		this.used -= existing.bytes.length;
	}
}

/**
 * The documents this process is holding decoded.
 *
 * Process-wide rather than per client, because the reader that needs it —
 * `textOf`, a free function taking bytes — has no client to reach through. That
 * is sound for the same reason the cache needs no invalidation: entries are
 * addressed and confirmed by content, so two clients, two workspaces or two
 * tenants sharing this map can only ever hit their own bytes, and a hit is the
 * document those bytes encode whoever produced it.
 */
export const textResidency = new DocResidency(RESIDENT_BYTES);
