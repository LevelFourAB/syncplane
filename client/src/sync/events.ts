/**
 * The client's observability seam: a typed stream of what the client did — the
 * background coordinator's sync cycle, and the local reads its queries are
 * answered from — for apps that want to measure it rather than only react to
 * its status.
 *
 * This is deliberately NOT an OpenTelemetry integration. `syncplane` ships to
 * browsers, where every dependency is bytes the app pays for on first load, and
 * an observability vendor is a choice that belongs to the app rather than to
 * its sync library. So the client emits plain structured events and an adopter
 * maps them to whatever they already run — OpenTelemetry, a product analytics
 * pipe, a debug overlay, or nothing at all, which is the default and costs no
 * more than building the dropped event.
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
 * Best-effort and synchronous, on whatever task already produced the event — a
 * sync cycle, or the read a query was answered from. The handler is invoked in
 * isolation — a throw is reported and swallowed, never mistaken for a failed
 * sync cycle or a failed read — but a SLOW handler delays the work it fired
 * from. Aggregate in the handler; do not await anything in it.
 *
 * Events are diagnostics, not a ledger: they are dropped on reload, are not
 * persisted, and carry no ordering guarantee beyond the order they occurred in
 * this tab. Nothing about correctness depends on anyone listening.
 */

import { safeInvoke } from '../report.js';

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
	/**
	 * `durationMs` split in two: waiting on the server's response, and applying
	 * it locally (rebase included). They move independently — a slow link and a
	 * slow store are the same total and different problems — and are absent on
	 * a pull that errored before either could be measured.
	 */
	requestMs?: number;
	applyMs?: number;
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

/**
 * The background index build finished for a workspace: its `not-built` scan
 * declines end, and a query issued after this event is answered from the
 * indexes that cover it.
 *
 * Fires on the tab that ran the build, once per build. A build overtaken by a
 * bootstrap — which replaces the rows being indexed — starts over and still
 * reports once, at the end. A build that found everything already indexed
 * reports `rows: 0`, the normal shape at every start() after the first, so
 * "indexes are ready" is the first event per workspace rather than the first
 * with rows. A client that closes mid-build, or has never synced the
 * workspace, reports nothing.
 */
export interface IndexBuildEvent extends EventBase {
	type: 'indexing';
	workspaceId: string;
	/** Rows this build indexed; 0 when every index was already built. */
	rows: number;
	/** Wall time of the build, queueing behind other writers included. */
	durationMs: number;
}

/**
 * Why a query read rows from the entity rather than from a secondary index.
 *
 * A decline is never a wrong answer: an index that cannot reproduce the scan's
 * result exactly declines, and the scan is the definition.
 *
 * - `cross-workspace` — the read spans every workspace, whose combined ordering
 *   no single workspace's index reproduces.
 * - `no-query` — a `watch()` with no query, which is the whole entity by
 *   definition.
 * - `no-index` — the entity declares no index.
 * - `not-built` — an index is declared but not built for this workspace yet.
 *   Builds run in the background, so a burst of these after a bootstrap or a
 *   schema change ends on its own.
 * - `no-plan` — no declared index matches this query's filter and ordering.
 * - `budget` — the walk had to READ more rows than its ceiling allows and fell
 *   back. A filter the index range could not express spends that budget, as
 *   does an unordered plan, which reads every match to re-create the scan's
 *   order; positions an ordered plan merely counts toward `total` do not.
 */
export type QueryDeclineReason =
	| 'cross-workspace'
	| 'no-query'
	| 'no-index'
	| 'not-built'
	| 'no-plan'
	| 'budget';

/**
 * One local query was answered from the store, by an index or by a scan.
 *
 * The read-path counterpart to the events above: those measure what the server
 * cost, this measures what the store cost. `path` is the field to aggregate on
 * — an index reads rows in proportion to the RESULT, a scan in proportion to
 * the ENTITY — and nothing in a result says which of the two happened, since
 * both return the same rows.
 */
export interface QueryEvent extends EventBase {
	type: 'query';
	/**
	 * What issued the read: a one-shot `query()`/`queryAll()`, or a watcher
	 * taking its first result. One subscription reports exactly one `watch`
	 * event — what follows is maintained per commit rather than re-read — so a
	 * stream of them means watchers are being re-registered.
	 */
	origin: 'query' | 'watch';
	entity: string;
	/** The workspace read, or undefined for a cross-workspace read. */
	workspaceId?: string;
	path: 'index' | 'scan';
	/** The index that served the read. Present exactly when `path` is `index`. */
	index?: string;
	/** Present exactly when `path` is `scan`. */
	declined?: QueryDeclineReason;
	/**
	 * Index positions the walk visited. Present exactly when `path` is `index`.
	 *
	 * Read against `rows` it is how much of the walk the result did not use — a
	 * filter the range could not express, or a repeated field putting one
	 * position per element on a row, both widen the gap. A ratio drifting upward
	 * is a query on its way to the candidate budget, and past it to a scan.
	 */
	candidates?: number;
	/**
	 * Rows the read produced — the page for a `query`, and for a `watch` the
	 * whole match set it goes on to maintain, which its window is a slice of.
	 */
	rows: number;
	/** Matches before the page was cut; see RowSet.total. Absent when the
	 * query opted out with `total: false`. */
	total?: number;
	/**
	 * Wall time from the call to the answer, `waitMs` included.
	 *
	 * Concurrent reads OVERLAP: several issued in the same tick each span most of
	 * the burst, so durations across one sum to more than the burst took and the
	 * largest may be reporting its neighbours' work. Aggregate rather than
	 * threshold, and subtract `waitMs` to drop the part contention explains.
	 *
	 * Reference hydration (`include`) is not part of it; the reference queries it
	 * runs report their own events.
	 */
	durationMs: number;
	/**
	 * Wall time the read spent waiting for a concurrent commit before it could
	 * begin. Reads never wait on each other, only on writes — a rebase, a
	 * bootstrap swap, an index build — so this rises under write load while the
	 * read itself is unchanged.
	 */
	waitMs: number;
}

/** Everything the client reports. Switch on `type`. */
export type SyncEvent =
	| PushEvent
	| PullEvent
	| BootstrapEvent
	| RebaseEvent
	| PokeEvent
	| StreamEvent
	| LeadershipEvent
	| ResequenceEvent
	| IndexBuildEvent
	| QueryEvent;

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

/** A QueryEvent as its producers build it; see UnstampedSyncEvent. */
export type UnstampedQueryEvent = Omit<QueryEvent, 'at'>;

/**
 * Stamps and delivers one event, in isolation: a handler that throws is
 * reported and swallowed rather than failing the work that emitted it. A no-op
 * when no handler is installed.
 *
 * The `at` stamp is filled in here so every event agrees on one clock and no
 * call site has to reach for it.
 */
export function emitSyncEvent(
	handler: SyncEventHandler | undefined,
	event: UnstampedSyncEvent,
): void {
	if (!handler) return;
	safeInvoke(handler, { ...event, at: Date.now() } as SyncEvent, 'onSyncEvent');
}

/**
 * Builds a query event from what the read reported about itself, resolving
 * `path` and the fields that ride on it. A `report.declined` means the rows
 * came from a scan; the index fields are then absent rather than undefined.
 */
export function queryEventOf(
	base: {
		origin: QueryEvent['origin'];
		entity: string;
		workspaceId?: string;
		rows: number;
		total: number | undefined;
		durationMs: number;
		waitMs: number;
	},
	report: { index?: string; declined?: QueryDeclineReason; candidates: number },
): UnstampedQueryEvent {
	return {
		type: 'query',
		...base,
		...(report.declined === undefined
			? { path: 'index', index: report.index, candidates: report.candidates }
			: { path: 'scan', declined: report.declined }),
	};
}
