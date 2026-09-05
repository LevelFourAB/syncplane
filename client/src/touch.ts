import { parseTouchKey, rowTouchKey, touchKey } from './keys.js';

/**
 * What a commit touched, at row granularity where the writer knows the row and
 * at entity granularity where it honestly does not.
 *
 * Reactivity is scan-based: a commit reports what changed, and every watcher
 * that reaches it re-reads the merged view. Reporting only the ENTITY forces
 * that re-read to be a full prefix scan, which makes a keystroke-sized mutation
 * cost the whole entity (see WatcherHub). Reporting ROWS lets the hub patch its
 * snapshot instead, so a commit costs O(changed rows).
 *
 * The invariant this class exists to hold: within one commit a given
 * (workspace, entity) pair is EITHER fully keyed OR wildcard, never both.
 * `entity()` marks the pair wildcard and discards whatever keys it had
 * collected; `row()` on an already-wildcard pair is a no-op. Consumers can
 * therefore read a wildcard as "this pair's keys are unknown" rather than as
 * "these keys, plus possibly others" — which is what makes the cross-tab compat
 * encoding below sound.
 */
export class TouchSet {
	/** Pair tokens (see keys.touchKey) whose changed rows are unknown. */
	private wildcards = new Set<string>();
	/** Pair token → the exact row keys that changed. Disjoint from wildcards. */
	private keyed = new Map<string, Set<string>>();

	/** Records that one row of an entity changed. */
	row(workspaceId: string, entity: string, key: string): void {
		const pair = touchKey(workspaceId, entity);
		if (this.wildcards.has(pair)) return;
		let keys = this.keyed.get(pair);
		if (!keys) {
			keys = new Set<string>();
			this.keyed.set(pair, keys);
		}
		keys.add(key);
	}

	/**
	 * Records that an entity changed without saying which rows — a
	 * confirmed-state wipe, a bootstrap swap, a workspace purge. Honest rather
	 * than a fallback: those paths replace or discard the whole entity and have
	 * no cheap key list, and a wildcard is exactly the statement "re-scan this
	 * one".
	 */
	entity(workspaceId: string, entity: string): void {
		const pair = touchKey(workspaceId, entity);
		this.wildcards.add(pair);
		this.keyed.delete(pair);
	}

	/** True when nothing was touched — a commit that changed no merged view. */
	isEmpty(): boolean {
		return this.wildcards.size === 0 && this.keyed.size === 0;
	}

	/** The touched pairs, each with its exact keys or `undefined` for wildcard. */
	*pairs(): IterableIterator<{ workspaceId: string; entity: string; keys?: ReadonlySet<string> }> {
		for (const pair of this.wildcards) {
			yield parseTouchKey(pair) as { workspaceId: string; entity: string };
		}
		for (const [ pair, keys ] of this.keyed) {
			const { workspaceId, entity } = parseTouchKey(pair);
			yield { workspaceId, entity, keys };
		}
	}

	/**
	 * The canonical token set: one keyed token per changed row, or one wildcard
	 * token per wildcard pair — never both for the same pair.
	 */
	tokens(): Set<string> {
		const out = new Set<string>(this.wildcards);
		for (const [ pair, keys ] of this.keyed) {
			const { workspaceId, entity } = parseTouchKey(pair);
			for (const key of keys) out.add(rowTouchKey(workspaceId, entity, key));
		}
		return out;
	}

	/**
	 * The token set to put on the cross-tab channel: canonical tokens PLUS a
	 * wildcard for every keyed pair.
	 *
	 * Tabs of different builds share one BroadcastChannel and one store, so the
	 * message has to mean the right thing to both. An older tab matches the
	 * wildcard and full-scans exactly as it always did, while the keyed tokens
	 * parse into an entity name no watcher holds and are ignored. A newer tab
	 * (see fromTokens) drops a wildcard that co-occurs with keyed tokens for the
	 * same pair — sound precisely because of this class's invariant, which says
	 * such a wildcard can only be the compat duplicate. No format version, no
	 * negotiation.
	 */
	broadcastTokens(): Set<string> {
		const out = this.tokens();
		for (const pair of this.keyed.keys()) out.add(pair);
		return out;
	}

	/**
	 * Rebuilds a TouchSet from tokens received over the cross-tab channel,
	 * applying the compat rule: a wildcard for a pair that also has keyed tokens
	 * in the same message is the duplicate emitted by broadcastTokens and is
	 * dropped. A message from an older tab carries wildcards only, so it stays a
	 * wildcard and scans.
	 */
	static fromTokens(tokens: Iterable<string>): TouchSet {
		const set = new TouchSet();
		const wildcards: string[] = [];
		for (const token of tokens) {
			const { workspaceId, entity, key } = parseTouchKey(token);
			if (key === undefined) {
				wildcards.push(token);
			} else {
				set.row(workspaceId, entity, key);
			}
		}
		// Second pass: row() above cannot have been suppressed by a wildcard that
		// had not been seen yet, and a wildcard only survives if no keyed token
		// claimed its pair.
		for (const token of wildcards) {
			const { workspaceId, entity } = parseTouchKey(token);
			if (!set.keyed.has(token)) set.entity(workspaceId, entity);
		}
		return set;
	}
}
