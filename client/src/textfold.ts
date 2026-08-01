// Keeping the optimistic text log's fold result between reads.
//
// A read of a text row resolves its `ot/` fragments into a whole document
// (read.ts): decode the stored document, apply the fragments, re-encode. Only
// the apply is O(edit); the decode and the encode are both O(document), and the
// decode is the larger of the two — at 50 000 code points it is roughly four
// times the encode. That runs on every read that resolves the row, and the row
// watch runs one per notification, so a typist pays it per keystroke on top of
// every other read of the same row in the same cycle.
//
// Two shapes make it avoidable, and they are the two a typist produces:
//
//   - the SAME read again — several paths resolve one row within a keystroke's
//     cycle (the watcher's re-read, the push path, the next enqueue's prior-row
//     read), and nothing changed between them;
//   - one more fragment on the end — the log is append-only between rebases, so
//     the next keystroke's fold is the last one plus an edit.
//
// An entry therefore keeps the folded bytes, which answer the first outright,
// and per field the bytes that fold produced — which answer the second, because
// they are the address the folded DOCUMENT is registered under in
// text/residency.ts. The next keystroke takes that document back out, applies
// the tail and encodes, with no decode. Documents are kept there rather than
// here so that one budget covers every document this process holds decoded, and
// so that a reader rendering the row gets the same one (residency.ts).
//
// # Why a hit is always correct
//
// The encoding is canonical and injective — two documents encode to the same
// bytes exactly when they are the same document — and a fold is a pure function
// of the row bytes it starts from and the fragments it applies. An entry is
// therefore valid if and only if BOTH still compare equal, which is a memcmp
// over the row plus one per fragment (fragments are keystroke-sized). At 50 KB
// that is tens of microseconds against the ~14 ms of decode and encode it
// replaces.
//
// The row identity in the key decides only WHERE to look and what to evict; it
// never decides whether a hit is right. A stale entry, an entry left by a
// mutation that was since rejected, even a key collision between two rows,
// cannot return a wrong document — none of them can pass the comparison. That
// is what lets this sit under a read path with no invalidation hooks at all: no
// producer has to remember to report a change, and another tab writing the
// shared store cannot make an entry lie.
//
// # Why entries are taken rather than borrowed
//
// `take` REMOVES the entry and hands it over; `put` registers whatever the fold
// ended up producing. A fold that throws part-way never puts anything back,
// which costs a miss rather than a wrong answer. The same discipline governs
// the documents themselves, where it is load-bearing rather than tidy — a fold
// mutates the document it extends, so it must own it (residency.ts).

import { overlayTextRowPrefix } from './keys.js';
import type { EntityRegistry } from './mutations.js';

/**
 * How much folded text state one registry keeps cached.
 *
 * The budget counts ENCODED bytes — the row it started from plus the row it
 * produced. A decoded document costs several times that in live heap (a node, a
 * child list and an id per code point), so the real ceiling is a multiple of
 * this number, which is why it is deliberately small: the value is almost
 * entirely in the rows currently being typed into, and a client rarely has more
 * than one of those.
 */
const TEXT_FOLD_CACHE_BYTES = 4 << 20;

/** One row's fold, kept against the inputs that produced it. */
export interface FoldEntry {
	/** Row bytes the fold started from. Retained for the validity comparison. */
	readonly base: Uint8Array;
	/**
	 * Fragments applied, per field, in the order they were scanned — the full
	 * scanned list, including any that failed to apply. A fold is deterministic
	 * given the base and this list, so it is the list a later fold has to match
	 * (or extend) for the result below to stand.
	 */
	readonly frags: ReadonlyMap<string, readonly Uint8Array[]>;
	/**
	 * What the fold produced for each text field — the bytes now sitting in
	 * `result`, and therefore the address to ask text/residency.ts for the
	 * document behind them. That document is what makes appending one more
	 * fragment cost an encode instead of a decode and an encode; if it has been
	 * evicted, the fold falls back to decoding the base.
	 */
	readonly fields: ReadonlyMap<string, Uint8Array>;
	/** The folded row bytes — the answer, when the comparison holds. */
	readonly result: Uint8Array;
}

/**
 * Identifies one row: the prefix its `ot/` log lives under, which is exactly
 * "the row this fold belongs to" and is already unambiguous by construction —
 * the workspace segment is percent-encoded, so no row key, whatever it contains,
 * can forge another row's key.
 */
export const foldCacheKey = overlayTextRowPrefix;

const sizeOf = (entry: FoldEntry): number => entry.base.length + entry.result.length;

/**
 * A bounded, least-recently-used map of row folds. Not a correctness
 * mechanism — see the file comment; dropping every entry only costs work.
 */
export class TextFoldCache {
	// Map iteration is insertion order, and `take` removes before `put`
	// re-inserts, so the head is always the least recently used entry.
	private entries = new Map<string, FoldEntry>();
	private used = 0;

	constructor(private readonly budget: number) {}

	/**
	 * Removes the entry for `key` and returns it, but only when it was folded
	 * from exactly `base`. The caller owns what comes back and must either put
	 * back what it becomes or drop it.
	 */
	take(key: string, base: Uint8Array): FoldEntry | undefined {
		const entry = this.entries.get(key);
		if (entry === undefined) return undefined;
		// Removed whether or not the bytes match: a mismatch means this row moved
		// on without going through here, so the entry is dead weight.
		this.entries.delete(key);
		this.used -= sizeOf(entry);
		return bytesEqual(entry.base, base) ? entry : undefined;
	}

	/** Records a fold. Its `base` and `result` must not be mutated afterwards. */
	put(key: string, entry: FoldEntry): void {
		const size = sizeOf(entry);
		// One row larger than the whole budget would evict everything else and
		// then itself; skip it rather than thrash.
		if (size > this.budget) return;
		const existing = this.entries.get(key);
		if (existing !== undefined) {
			this.entries.delete(key);
			this.used -= sizeOf(existing);
		}
		this.entries.set(key, entry);
		this.used += size;
		for (const oldest of this.entries.keys()) {
			if (this.used <= this.budget) break;
			this.used -= sizeOf(this.entries.get(oldest)!);
			this.entries.delete(oldest);
		}
	}
}

/**
 * The fold cache of one entity registry, created on first use.
 *
 * A registry is built once per client and reaches every read path already, so
 * this scopes the cache to the client without threading it through five read
 * functions and their callers. Weak, so a closed or dropped client takes its
 * cached documents with it.
 */
const caches = new WeakMap<EntityRegistry, TextFoldCache>();

export function foldCacheOf(registry: EntityRegistry): TextFoldCache {
	let cache = caches.get(registry);
	if (cache === undefined) caches.set(registry, cache = new TextFoldCache(TEXT_FOLD_CACHE_BYTES));
	return cache;
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
 * Whether `prior` is `list` with zero or more fragments removed from the END —
 * the shape an append-only log produces between rebases, and the one an entry's
 * documents can be extended from rather than rebuilt.
 */
export function isFragmentPrefix(
	prior: readonly Uint8Array[],
	list: readonly Uint8Array[],
): boolean {
	if (prior.length > list.length) return false;
	for (let i = 0; i < prior.length; i++) {
		if (!bytesEqual(prior[i]!, list[i]!)) return false;
	}
	return true;
}

/**
 * Whether an entry's fragments are exactly the ones just scanned — same fields,
 * same fragments, same order. Its result then stands as it is.
 */
export function sameFragments(
	prior: ReadonlyMap<string, readonly Uint8Array[]>,
	list: ReadonlyMap<string, readonly Uint8Array[]>,
): boolean {
	if (prior.size !== list.size) return false;
	for (const [ field, mine ] of prior) {
		const theirs = list.get(field);
		if (theirs === undefined || theirs.length !== mine.length) return false;
		if (!isFragmentPrefix(mine, theirs)) return false;
	}
	return true;
}
