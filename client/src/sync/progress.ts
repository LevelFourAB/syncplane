// How far a running bootstrap has got, in rows and entities, for the status
// surface (WorkspaceStatus.bootstrap).
//
// The coordinator writes a chunk's rows in one transaction and lets the store
// commit it while it decodes the next chunk, so there are two counts. The
// WRITTEN count includes chunks still committing; it goes into the resume
// record in the same transaction as the rows, which keeps the record from ever
// claiming more than the store holds. The COMMITTED count is what the status
// shows, so a reader never sees rows counted that it cannot read yet.

import type { BootstrapProgressInfo } from './status.js';

/**
 * Shortest time between two status updates for progress alone. A fast server
 * streams many chunks a second, and an update per chunk would wake every
 * status listener as often.
 */
export const BOOTSTRAP_PROGRESS_INTERVAL_MS = 250;

/**
 * What an attempt of a snapshot applied, as stored in the resume record so
 * that the next attempt of the same snapshot continues from it.
 */
export interface StoredProgress {
	rows: number;
	/** Smallest whole-snapshot estimate a Begin gave, 0 when none gave one. */
	estimate: number;
	entitiesDone: number;
	entitiesTotal: number;
}

interface Counts {
	rows: number;
	entitiesDone: number;
}

interface WorkspaceProgress {
	/** Index of each entity in the delivery order Begin announced. */
	position: Map<string, number>;
	entitiesTotal: number;
	estimate: number;
	written: Counts;
	committed: Counts;
	/** When the status last showed this workspace's progress. */
	publishedAt: number;
	/** Cancels the pending trailing update, when one waits. */
	cancelTrailing?: () => void;
}

/** How a tracker reaches the status surface and the clock. */
export interface ProgressTrackerOptions {
	/**
	 * Shows a workspace's progress on the status surface. The tracker calls it
	 * for a committed chunk, at once or after the throttle interval.
	 */
	publish: (workspaceId: string) => void;
	/** Milliseconds, monotonic. @defaultValue `Date.now` */
	now?: () => number;
	/** @defaultValue BOOTSTRAP_PROGRESS_INTERVAL_MS */
	intervalMs?: number;
	/** Runs `fn` after `ms`, and returns what cancels it. @defaultValue `setTimeout` */
	schedule?: (fn: () => void, ms: number) => () => void;
}

/** Per-workspace progress of the bootstraps this tab runs. */
export class BootstrapProgressTracker {
	private byWorkspace = new Map<string, WorkspaceProgress>();
	private readonly publish: (workspaceId: string) => void;
	private readonly now: () => number;
	private readonly intervalMs: number;
	private readonly schedule: (fn: () => void, ms: number) => () => void;

	constructor(opts: ProgressTrackerOptions) {
		this.publish = opts.publish;
		this.now = opts.now ?? (() => Date.now());
		this.intervalMs = opts.intervalMs ?? BOOTSTRAP_PROGRESS_INTERVAL_MS;
		this.schedule = opts.schedule ?? ((fn, ms) => {
			const timer = setTimeout(fn, ms);
			return () => clearTimeout(timer);
		});
	}

	/**
	 * Starts tracking an attempt. `resumed` is what earlier attempts of the
	 * same snapshot applied, from the resume record the attempt adopts. The
	 * caller publishes after it.
	 */
	start(workspaceId: string, resumed?: StoredProgress): void {
		this.byWorkspace.get(workspaceId)?.cancelTrailing?.();
		const counts = { rows: resumed?.rows ?? 0, entitiesDone: resumed?.entitiesDone ?? 0 };
		this.byWorkspace.set(workspaceId, {
			position: new Map(),
			entitiesTotal: resumed?.entitiesTotal ?? 0,
			estimate: resumed?.estimate ?? 0,
			written: { ...counts },
			committed: { ...counts },
			publishedAt: this.now(),
		});
	}

	/**
	 * Takes in the Begin frame. `resumed` false is a fresh snapshot, so what an
	 * earlier attempt applied no longer counts. An estimate list that does not
	 * match `entities` is no estimate (see BootstrapResponse.Begin). The
	 * caller publishes after it.
	 */
	begin(
		workspaceId: string,
		entities: readonly string[],
		estimates: readonly bigint[],
		resumed: boolean,
	): void {
		const state = this.byWorkspace.get(workspaceId);
		if (!state) return;
		if (!resumed) {
			state.estimate = 0;
			state.written = { rows: 0, entitiesDone: 0 };
			state.committed = { rows: 0, entitiesDone: 0 };
		}
		state.position = new Map(entities.map((e, i) => [ e, i ]));
		state.entitiesTotal = entities.length;
		let total = 0;
		if (estimates.length === entities.length) {
			for (const n of estimates) total += Number(n);
		}
		state.estimate = smallerEstimate(state.estimate, total);
		markPublished(state, this.now());
	}

	/**
	 * Counts a chunk handed to the store, and returns what the resume record
	 * written with it stores.
	 */
	written(workspaceId: string, entities: readonly string[]): StoredProgress | undefined {
		const state = this.byWorkspace.get(workspaceId);
		if (!state) return undefined;
		advance(state, state.written, entities);
		return {
			rows: state.written.rows,
			estimate: state.estimate,
			entitiesDone: state.written.entitiesDone,
			entitiesTotal: state.entitiesTotal,
		};
	}

	/**
	 * Counts a chunk whose transaction committed, and publishes it: at once
	 * when the interval has passed since the last update, and otherwise when it
	 * passes.
	 */
	committed(workspaceId: string, entities: readonly string[]): void {
		const state = this.byWorkspace.get(workspaceId);
		if (!state) return;
		advance(state, state.committed, entities);
		const wait = this.intervalMs - (this.now() - state.publishedAt);
		if (wait <= 0) {
			markPublished(state, this.now());
			this.publish(workspaceId);
			return;
		}
		// The trailing update: without it, a server that stops sending chunks
		// for a while leaves the last ones unshown, which looks like a stall.
		state.cancelTrailing ??= this.schedule(() => {
			state.cancelTrailing = undefined;
			// A finished or restarted attempt holds another state object.
			if (this.byWorkspace.get(workspaceId) !== state) return;
			state.publishedAt = this.now();
			this.publish(workspaceId);
		}, wait);
	}

	/**
	 * Marks every entity complete: the server sent End and every chunk
	 * committed. The caller publishes after it.
	 */
	ended(workspaceId: string): void {
		const state = this.byWorkspace.get(workspaceId);
		if (!state) return;
		state.committed.entitiesDone = state.entitiesTotal;
		state.written.entitiesDone = state.entitiesTotal;
		markPublished(state, this.now());
	}

	/** Ends tracking: the attempt installed its snapshot, lost it, or failed. */
	finish(workspaceId: string): void {
		this.byWorkspace.get(workspaceId)?.cancelTrailing?.();
		this.byWorkspace.delete(workspaceId);
	}

	/** The status surface's view of a workspace's progress; absent when no bootstrap runs. */
	infoOf(workspaceId: string): BootstrapProgressInfo | undefined {
		const state = this.byWorkspace.get(workspaceId);
		if (!state) return undefined;
		const { rows, entitiesDone } = state.committed;
		return {
			rowsApplied: rows,
			// Raised to the rows applied, so the fraction never passes 1. The
			// estimate itself only goes down during a snapshot, so the fraction
			// never goes down either.
			rowsEstimated: state.estimate > 0 ? Math.max(state.estimate, rows) : 0,
			entitiesDone,
			entitiesTotal: state.entitiesTotal,
		};
	}
}

/**
 * Records an update the caller makes itself, which shows everything counted so
 * far, so a pending trailing update has nothing left to show.
 */
function markPublished(state: WorkspaceProgress, now: number): void {
	state.cancelTrailing?.();
	state.cancelTrailing = undefined;
	state.publishedAt = now;
}

/**
 * Adds one chunk's rows to `counts`. Rows of one entity arrive contiguously
 * and in the announced order, so a row of entity i proves the i entities
 * before it complete.
 */
function advance(state: WorkspaceProgress, counts: Counts, entities: readonly string[]): void {
	counts.rows += entities.length;
	for (const entity of entities) {
		// An entity the announcement did not name proves nothing about the order.
		const at = state.position.get(entity);
		if (at !== undefined && at > counts.entitiesDone) counts.entitiesDone = at;
	}
}

/** The smaller of two estimates, where 0 is "unknown" and loses to any known one. */
function smallerEstimate(a: number, b: number): number {
	if (a === 0) return b;
	if (b === 0) return a;
	return Math.min(a, b);
}
