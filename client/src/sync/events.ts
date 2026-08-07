/**
 * The client's observability seam: a typed stream of what the background
 * coordinator did, for apps that want to measure sync rather than only react
 * to its status.
 *
 * This is deliberately NOT an OpenTelemetry integration. `syncplane` ships to
 * browsers, where every dependency is bytes the app pays for on first load, and
 * an observability vendor is a choice that belongs to the app rather than to
 * its sync library. So the client emits plain structured events and an adopter
 * maps them to whatever they already run — OpenTelemetry, a product analytics
 * pipe, a debug overlay, or nothing at all, which is the default and costs one
 * undefined check per event.
 *
 * # Events vs. status
 *
 * `SyncStatus` (onStatusChange) is the CURRENT state, meant to be rendered: is
 * it online, is it pushing, did the last cycle fail. These events are what
 * HAPPENED, meant to be aggregated: how long the push took, how many changes
 * that pull carried, how often clients re-bootstrap. A status listener that
 * tried to measure latency would be sampling a level and hoping; this reports
 * each occurrence with its own duration.
 *
 * # Delivery
 *
 * Best-effort and synchronous, on whatever task the coordinator was already
 * running. The handler is invoked in isolation — a throw is reported and
 * swallowed, never mistaken for a failed sync cycle — but a SLOW handler
 * delays the sync cycle it fired from. Aggregate in the handler; do not await
 * anything in it.
 *
 * Events are diagnostics, not a ledger: they are dropped on reload, are not
 * persisted, and carry no ordering guarantee beyond the order they occurred in
 * this tab. Nothing about correctness depends on anyone listening.
 */

/** Common shape of every event. */
interface EventBase {
	/** `Date.now()` when the event was emitted. */
	at: number;
}

/**
 * How an operation ended. `error` is a genuine failure; `offline` is the
 * expected shape of a client that cannot reach the server, separated so an
 * error rate does not simply track connectivity.
 */
export type SyncEventOutcome = 'ok' | 'error' | 'offline';

/**
 * One push RPC completed (or failed). Fires once per request, which may carry
 * several workspaces' mutations.
 */
export interface PushEvent extends EventBase {
	type: 'push';
	/** Workspaces whose mutations this request carried. */
	workspaceIds: string[];
	/** Mutations submitted. */
	mutations: number;
	/**
	 * Mutations the server applied. Together with `rejected` this is the
	 * client-side half of the server's accept ratio — and the number that
	 * exposes a queue looping without making progress, which a status of
	 * "syncing" cannot.
	 */
	applied: number;
	/**
	 * Mutations the server refused. A rejection is still an ack, so these are
	 * NOT retried; each also surfaces individually through onMutationRejected.
	 */
	rejected: number;
	/** Wall time of the RPC, in milliseconds. */
	durationMs: number;
	outcome: SyncEventOutcome;
}

/** One pull page was applied to the local store. */
export interface PullEvent extends EventBase {
	type: 'pull';
	workspaceId: string;
	/** Row changes the page carried. */
	changes: number;
	/** The server has more waiting; another page follows immediately. */
	hasMore: boolean;
	/**
	 * The server refused to serve this cursor incrementally and the client must
	 * re-bootstrap — it fell behind the server's GC horizon, or the store's
	 * lineage did not match. The expensive outcome, and the one worth alerting
	 * on: a bootstrap re-downloads the workspace.
	 */
	reset: boolean;
	/** Wall time from issuing the pull to the page being committed locally. */
	durationMs: number;
	outcome: SyncEventOutcome;
}

/**
 * A full bootstrap of one workspace finished.
 *
 * `durationMs` is the whole thing; `streamMs`, `stageMs` and `swapMs` are the
 * three phases it is made of, and they are reported separately because their
 * costs move independently. A bootstrap that is slow because the snapshot took
 * a long time to arrive and one that is slow because the store took a long time
 * to write it are the same number and different problems — "the download was
 * fast and the store was slow" is a different support conversation from the
 * reverse — and with one number a change that halves one phase while doubling
 * another looks like no change at all.
 *
 * The three very nearly account for `durationMs`; the remainder is bookkeeping
 * between them, so treat a gap of a few milliseconds as noise rather than a
 * missing phase.
 */
export interface BootstrapEvent extends EventBase {
	type: 'bootstrap';
	workspaceId: string;
	/** Rows written into the local store. */
	rows: number;
	/**
	 * The snapshot was continued from where an interrupted attempt stopped, so
	 * `rows` counts what THIS attempt downloaded rather than the whole workspace.
	 * A run of these is the signal that snapshots are not surviving the network
	 * they are being served over — the reason resume exists, not a fault of it.
	 */
	resumed: boolean;
	/** Wall time of the whole bootstrap: stream, staging and swap together. */
	durationMs: number;
	/**
	 * Waiting on the server's snapshot stream — everything in the stream phase
	 * that was not staging, so it also covers decoding each frame. This is the
	 * part a slower link makes worse and a faster store does not.
	 */
	streamMs: number;
	/**
	 * Writing the arriving chunks into the store, summed over every chunk — into
	 * the staging keyspace when the workspace already holds a snapshot, straight
	 * into confirmed state when it does not. Local reads and writes run
	 * throughout, so this is store throughput rather than a stall.
	 */
	stageMs: number;
	/**
	 * The swap: one transaction that replaces the workspace's confirmed state
	 * with the staged snapshot. Includes waiting for the store mutex, because a
	 * swap queued behind another tab's write is time the bootstrap really spent.
	 * This phase is the one that blocks other writers — and the one a cold
	 * bootstrap barely has, since its rows are already in place and all that is
	 * left is the cursor.
	 */
	swapMs: number;
	/**
	 * KV entries the swap transaction wrote — puts and deletes alike, since both
	 * are work for the store. Row count alone hides write amplification: the same
	 * snapshot written at one entry per row and at six is the same `rows` and a
	 * very different transaction, and without this the difference only surfaces
	 * later as `swapMs`. A cold bootstrap writes its rows during the stream
	 * instead, so a handful of entries here is the swap it does not do rather
	 * than a snapshot that cost nothing.
	 */
	swapWrites: number;
	outcome: SyncEventOutcome;
}

/**
 * A server page was rebased over local state: server patch applied, acked
 * mutations dropped, the optimistic overlay cleared and what remains replayed.
 */
export interface RebaseEvent extends EventBase {
	type: 'rebase';
	workspaceId: string;
	/** Pending mutations dropped because the server acked them. */
	acked: number;
	/** Pending mutations replayed back over the new server state. */
	replayed: number;
	/** Wall time of the rebase. */
	durationMs: number;
}

/**
 * A workspace's changes arrived over the live stream rather than being found
 * by a pull. Counting these against `pull` events is what separates changes
 * that took the realtime path from ones that waited for the poll floor — the
 * ratio a realtime deployment is actually trying to move.
 *
 * Only the stream produces these. A poll is not a poke: it is a scheduled
 * check that finds whatever is there, and it is already reported as a `pull`.
 */
export interface PokeEvent extends EventBase {
	type: 'poke';
	workspaceId: string;
	/** Row changes the frame carried. */
	changes: number;
}

/** The live change stream opened or closed. */
export interface StreamEvent extends EventBase {
	type: 'stream';
	state: 'open' | 'closed';
	/** Why it closed, when known. Absent on open and on a clean shutdown. */
	reason?: string;
}

/**
 * This tab took or lost stream leadership. Exactly one tab per account holds
 * the stream; the rest read its results through the shared store. A tab that
 * never becomes leader is normal, not a fault.
 */
export interface LeadershipEvent extends EventBase {
	type: 'leadership';
	leader: boolean;
}

/**
 * A workspace's pending queue was renumbered so it resumes at the server's
 * mutation-id watermark. Always a recovery, and rare: something discarded a
 * queued mutation the server never saw, and the ids above the hole it left
 * could not be pushed until they moved down onto it. Every still-queued
 * mutation has a new id afterwards.
 */
export interface ResequenceEvent extends EventBase {
	type: 'resequence';
	workspaceId: string;
	/** The id the queue now starts at: the server's watermark plus one. */
	firstId: bigint;
	/** Mutations renumbered. */
	mutations: number;
}

/** Everything the coordinator reports. Switch on `type`. */
export type SyncEvent =
	| PushEvent
	| PullEvent
	| BootstrapEvent
	| RebaseEvent
	| PokeEvent
	| StreamEvent
	| LeadershipEvent
	| ResequenceEvent;

/** Handler passed as `onSyncEvent`. */
export type SyncEventHandler = (event: SyncEvent) => void;

/**
 * A SyncEvent without the `at` stamp the emitter fills in, so no call site has
 * to reach for the clock.
 *
 * The conditional over a naked type PARAMETER is what makes Omit distribute
 * across the union. Applied to the union directly — `Omit<SyncEvent, 'at'>` —
 * it would keep only the keys every member shares, which is just `type`, and
 * every event body would fail to typecheck. Internal; the emitted type is
 * SyncEvent.
 */
type Unstamped<T> = T extends unknown ? Omit<T, 'at'> : never;

export type UnstampedSyncEvent = Unstamped<SyncEvent>;
