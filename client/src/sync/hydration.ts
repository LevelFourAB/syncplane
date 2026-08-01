// Which entities of a cold-starting workspace have not finished downloading.
//
// A cold bootstrap writes its rows straight into confirmed state, so they are
// readable as each chunk commits rather than after one atomic swap (see
// Coordinator.doBootstrap). That is what makes a first paint fast, and it is
// also what makes an entity that has not started indistinguishable from an
// empty one: a reader sees `[]` either way. This is the record that tells them
// apart — the client half of the delivery order the server announces on Begin.
//
// Only the COLD path has the problem. A re-bootstrap stages its rows and swaps
// atomically, so readers see the intact previous snapshot the whole time and
// nothing is ever half-arrived.

import type { HydrationInfo } from './status.js';

/** One workspace's snapshot progress. */
interface WorkspaceHydration {
	/**
	 * Entities the snapshot ships, in delivery order. Seeded with the client's
	 * own scope (order unknown, so nothing can be declared complete early) and
	 * replaced by the server's announced order on Begin.
	 */
	order: string[];
	/**
	 * How many entries of `order` have landed in full. Entities ship
	 * contiguously and in order, so a row of `order[i]` proves `order[0..i)`
	 * shipped everything they had.
	 */
	done: number;
	/** `order.slice(done)`, kept as a set because readers ask per entity. */
	pending: Set<string>;
}

/**
 * Per-workspace snapshot progress for the coverage layer.
 *
 * In memory and per tab, like every other coverage input: it describes what
 * THIS tab knows about the store's completeness, and a tab that is not the one
 * streaming the snapshot learns of the rows through the shared store and the
 * cross-tab change channel instead.
 */
export class HydrationTracker {
	private byWorkspace = new Map<string, WorkspaceHydration>();

	/**
	 * Starts tracking a cold bootstrap. `entities` is the best guess available
	 * before the server has spoken — everything in scope — so every entity reads
	 * as pending until Begin narrows it and rows start completing it.
	 */
	begin(workspaceId: string, entities: readonly string[]): void {
		this.byWorkspace.set(workspaceId, {
			order: [ ...entities ],
			done: 0,
			pending: new Set(entities),
		});
	}

	/**
	 * Installs the delivery order the server announced on Begin. An empty list —
	 * a server predating the announcement — leaves the seeded guess in place, so
	 * such a client reports every entity pending until the snapshot ends rather
	 * than reporting a completeness it cannot prove.
	 */
	order(workspaceId: string, entities: readonly string[]): void {
		const state = this.byWorkspace.get(workspaceId);
		if (!state || entities.length === 0) return;
		state.order = [ ...entities ];
		state.done = 0;
		state.pending = new Set(entities);
	}

	/**
	 * Records that a committed chunk carried rows of `entities`, completing
	 * every entity the server has already moved past. Returns the entities that
	 * just completed, in delivery order — the ones whose watchers now have their
	 * whole set and can stop saying "loading".
	 */
	advance(workspaceId: string, entities: Iterable<string>): string[] {
		const state = this.byWorkspace.get(workspaceId);
		if (!state) return [];
		let furthest = state.done;
		for (const entity of entities) {
			// A row of an entity the announcement did not name (a server shipping
			// more than it said) proves nothing about the order; ignore it rather
			// than let it complete entities that may still be coming.
			const at = state.order.indexOf(entity);
			if (at > furthest) furthest = at;
		}
		return this.completeThrough(state, furthest);
	}

	/**
	 * Ends tracking — the snapshot installed, or the workspace turned out to
	 * hold one already. Returns the entities that were still pending, which are
	 * exactly the ones whose coverage just changed.
	 */
	finish(workspaceId: string): string[] {
		const state = this.byWorkspace.get(workspaceId);
		if (!state) return [];
		const completed = this.completeThrough(state, state.order.length);
		this.byWorkspace.delete(workspaceId);
		return completed;
	}

	/** Entities of a workspace whose rows are still arriving; absent when none are. */
	pendingOf(workspaceId: string): ReadonlySet<string> | undefined {
		return this.byWorkspace.get(workspaceId)?.pending;
	}

	/** The status surface's view of a workspace's progress; absent when not hydrating. */
	infoOf(workspaceId: string): HydrationInfo | undefined {
		const state = this.byWorkspace.get(workspaceId);
		if (!state) return undefined;
		return {
			pending: state.order.slice(state.done),
			total: state.order.length,
			done: state.done,
		};
	}

	private completeThrough(state: WorkspaceHydration, through: number): string[] {
		if (through <= state.done) return [];
		const completed = state.order.slice(state.done, through);
		for (const entity of completed) state.pending.delete(entity);
		state.done = through;
		return completed;
	}
}
