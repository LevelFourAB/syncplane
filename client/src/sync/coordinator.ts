import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { type Client, Code, ConnectError, createClient, type Transport } from '@connectrpc/connect';

import {
	CodeInternal,
	CodeMutationIdGap,
	CodePoisonDropped,
	CodeTextFullStateRequired,
	CodeTextStateStale,
	CodeUnavailable,
	CodeWorkspaceAccessRevoked,
	ErrorCodeMetadataKey,
	type RejectionCode,
	type SyncErrorCode,
} from '../codes.js';
import { opaqueEntities, syncableEntities } from '../entities.js';
import {
	CrudMutationSchema,
	CrudOp,
	type Mutation,
	MutationSchema,
} from '../gen/syncplane/v1/mutation_pb.js';
import type { Subscription } from '../gen/syncplane/v1/patch_pb.js';
import {
	MutationStatus,
	type PullResponse,
	SyncService,
	WorkspaceDropped_Reason,
} from '../gen/syncplane/v1/service_pb.js';
import {
	metaAckedSeq,
	metaBootstrapGen,
	metaBootstrapResume,
	metaCursor,
	metaLastMutationId,
	metaPushedThrough,
	metaSnapshotGen,
	stagingPrefix,
	stagingWsRoot,
} from '../keys.js';
import { CountingWrite, type KVRead, type KVStore } from '../kv/store.js';
import { type EntityRegistry, setTextFieldBytes, textFieldBytes } from '../mutations.js';
import type { Mutex } from '../mutex.js';
import { compactPending } from '../optimistic/compact.js';
import { markIndexesBuilt } from '../optimistic/index-build.js';
import type { IndexCache } from '../optimistic/index-maintain.js';
import {
	enqueuePending,
	readPending,
	readPushedThrough,
	writePushedThrough,
} from '../optimistic/queue.js';
import {
	applyBootstrapRows,
	applyCrudLocally,
	clearStaging,
	dropAndResequencePending,
	finalizeRebase,
	type MirrorMap,
	promoteStaged,
	rebase,
	type RebaseResult,
	resequencePending,
	type ResequenceResult,
	rollbackRejected,
	stageServerChanges,
	wipeConfirmedState,
} from '../optimistic/rebase.js';
import { readMergedRow } from '../read.js';
import { reportError, safeCall } from '../report.js';
import type { StorageNames } from '../storagenames.js';
import { decodeTextDoc, encodeTextDoc, textOf } from '../text/index.js';
import { createPeerLeases, type PeerLeases } from '../text/peer.js';
import { TouchSet } from '../touch.js';
import { Backoff, sleep, STREAM_STABLE_MS } from './backoff.js';
import type { SyncEventHandler, SyncEventOutcome, UnstampedSyncEvent } from './events.js';
import { emitSyncEvent } from './events.js';
import { HydrationTracker } from './hydration.js';
import type { IntentRegistry, ScopeMap } from './intent.js';
import {
	type ReopenSignal,
	startReopenSignal,
	startStreamLeadership,
	type StreamLeadership,
} from './leadership.js';
import type { SchemaSkewAction, StatusTracker, SyncErrorInfo } from './status.js';
import { fullStateKey, TextDeltaUnusableError, toWireMutations } from './textdelta.js';

/** One row's text documents, captured for a full-state re-send. */
interface TextResendRow {
	entity: string;
	key: string;
	fields: { path: string; value: Uint8Array }[];
}

/**
 * One row's text fields captured as VISIBLE TEXT, to be re-applied after a
 * re-bootstrap. Used for `text_state_stale`, where the document bytes are
 * exactly what the server refused: only the text survives the round trip, and
 * it has to be re-diffed against whatever the server now holds.
 */
interface TextRestoreRow {
	entity: string;
	key: string;
	fields: { path: string; text: string }[];
}

/** One row a rejected mutation would have written. */
export interface RejectionTarget {
	entity: string;
	key: string;
}

export interface RejectionInfo {
	/** Workspace the rejected mutation was queued for. */
	workspaceId: string;
	mutationId: bigint;
	/**
	 * Why it died. One of the known RejectionCodes, or whatever code a custom
	 * server-side mutator returned — the union autocompletes the former without
	 * rejecting the latter.
	 */
	errorCode: RejectionCode | (string & {});
	errorMessage: string;
	/**
	 * What the mutation would have written, in payload order: one entry for a
	 * CRUD mutation, one per member for a batch, none for a custom mutator (see
	 * `mutator`). This is what lets an app name the thing that failed — "Todo
	 * 'buy milk' was rejected" rather than "a mutation was rejected" — without
	 * keeping its own id→row ledger.
	 */
	targets: readonly RejectionTarget[];
	/** Name of the custom mutator, when the rejected mutation was one. */
	mutator?: string;
}

/** Reads the dead-letter description off a queued mutation (see RejectionInfo). */
export function describeMutation(
	m: Mutation,
): { targets: RejectionTarget[]; mutator?: string } {
	switch (m.payload.case) {
		case 'crud':
			return { targets: [ { entity: m.payload.value.entity, key: m.payload.value.key } ] };
		case 'batch':
			return {
				targets: m.payload.value.mutations.map((c) => ({ entity: c.entity, key: c.key })),
			};
		case 'custom':
			return { targets: [], mutator: m.payload.value.name };
		default:
			return { targets: [] };
	}
}

/** Default deterministic-failure count before a head mutation is dead-lettered. */
const DEFAULT_POISON_THRESHOLD = 5;

/**
 * Default write-to-push coalescing window, and the ceiling on it under an
 * unbroken run of writes. Deliberately short: they exist to keep a burst
 * (typing, a drag) from spending one push per event, not to batch on a human
 * timescale. At the defaults an isolated write leaves 50ms after it lands —
 * under the round trip that follows it — and a sustained burst still ships four
 * times a second.
 */
const DEFAULT_PUSH_DEBOUNCE_MS = 50;
const DEFAULT_PUSH_MAX_DELAY_MS = 250;

/**
 * Default per-push mutation cap. Matches the engine's default
 * Limits.MaxMutationsPerPush so a long offline burst is shipped in several
 * within-limit RPCs instead of one oversized push the server rejects. The
 * engine counts this TOTAL across the batches of one request, so the packer
 * below budgets across workspaces rather than per workspace. A server
 * configured with a lower limit needs maxMutationsPerPush set to match.
 */
export const DEFAULT_MAX_MUTATIONS_PER_PUSH = 256;

/**
 * Default cap on how many workspaces one request may name. Matches the engine's
 * Limits.MaxWorkspacesPerRequest; a client holding more workspaces than this
 * splits its push across requests and its Subscribe across streams.
 */
export const DEFAULT_MAX_WORKSPACES_PER_REQUEST = 64;

/**
 * How long the leader waits before re-checking for work when it holds no
 * syncable workspace at all (every one removed or revoked). Interrupted the
 * moment a workspace is added, so this is only the ceiling on noticing an
 * external change.
 */
const IDLE_STREAM_RECHECK_MS = 30_000;

const EMPTY_IDS: ReadonlySet<bigint> = new Set();

export interface CoordinatorOptions {
	transport: Transport;
	kv: KVStore;
	kvMutex: Mutex;
	/**
	 * The origin-scoped names of the store this coordinator drives (see
	 * storagenames.ts). They carry the store partition, so exactly the tabs
	 * sharing a store coordinate with each other — regardless of which
	 * workspaces each of them holds.
	 */
	storageNames: StorageNames;
	clientId: string;
	registry: EntityRegistry;
	mirrors: MirrorMap;
	/**
	 * Compiled secondary indexes, shared with the read path so both sides agree
	 * on which index is which. Absent leaves every index unmaintained and hence
	 * unbuilt, which every query answers by scanning — correct, just not fast.
	 */
	indexes?: IndexCache;
	/**
	 * Called after every bootstrap swap commits, naming the workspace to run a
	 * build pass over. A swap that installed its rows unindexed needs the walk to
	 * get them back; one that installed them indexed needs the pass anyway, both
	 * to check the markers it claimed and to report the workspace as indexed. A
	 * client that leaves this unset gets correct answers from the scan fallback
	 * and never the index over a re-bootstrapped workspace.
	 *
	 * Fire-and-forget by contract — the caller must not make the sync cycle wait
	 * on a walk of the whole store.
	 */
	onIndexesStale?: (workspaceId: string) => void;
	/**
	 * Notifies local watchers of a committed touched set AND fans it out to
	 * other tabs sharing the store (the writing tab's WatcherHub is in-memory,
	 * the data is shared). Always called after a commit, never with stale data.
	 * The set carries the touched rows, or whole entities where the writer had
	 * no cheap key list; see TouchSet.
	 */
	notify: (touched: TouchSet) => Promise<void>;
	status: StatusTracker;
	onMutationRejected?: (info: RejectionInfo) => void;
	/** Observability seam; see sync/events.ts. */
	onSyncEvent?: SyncEventHandler;
	/** Fallback poll interval; 0 disables polling. Default 60s. */
	pollIntervalMs?: number;
	/**
	 * How long a local write waits before it triggers a sync cycle, restarted by
	 * each further write. Only writes are held; every other trigger (poke, poll,
	 * reconnect, scope change) still cycles at once, so this never delays
	 * INBOUND changes. Default 50ms; 0 pushes on every write.
	 *
	 * The point is not saving round trips so much as compaction: doPush squashes
	 * the never-pushed tail first, so edits that land inside one window fold into
	 * a single mutation instead of shipping one per keystroke.
	 */
	pushDebounceMs?: number;
	/**
	 * Ceiling on that wait, measured from the first write of a burst and NOT
	 * restarted by later ones. Without it, an unbroken stream of writes (someone
	 * typing) would defer the push indefinitely. Default 250ms; 0 removes the
	 * ceiling.
	 */
	pushMaxDelayMs?: number;
	/** Hold the live change stream open. Default true. */
	streamEnabled?: boolean;
	/** Max changes per pull page; 0 = server default. Pages loop until drained. */
	pullLimit?: number;
	/**
	 * Initial workspaces and their subscription sets (already compiled to wire
	 * form). A workspace mapped to an empty array is full sync.
	 */
	scopes: ScopeMap;
	/**
	 * Entities a bootstrap should ship first, in this order. A cold snapshot is
	 * readable entity by entity as it lands, so this is what the user looks at
	 * while the rest downloads. Unset leaves the order to the server, which
	 * serves a subscribed client in subscription order.
	 */
	priorityEntities?: string[];
	/**
	 * How many times the head mutation may fail the same way — an
	 * invalid-argument push error, or a batch the server refuses as a whole with
	 * an internal or unrecognized code — before the client gives up and drops it
	 * to the dead-letter surface (onMutationRejected, code "poison_dropped").
	 * Counted only while the verdict repeats unchanged, so an outage does not
	 * accumulate toward it. A higher value tolerates longer windows in which the
	 * failure could still turn out to be transient (a rolling deploy, a failing
	 * dependency) at the cost of staying wedged longer. Default 5; <= 0 disables
	 * dropping (retry forever).
	 */
	poisonThreshold?: number;
	/**
	 * Largest number of mutations one push RPC may carry, counted across every
	 * workspace batch in it. A bigger backlog is split across several sequential
	 * pushes. Default 256 (the engine's default limit); lower it to match a
	 * server with a tighter limit.
	 */
	maxMutationsPerPush?: number;
	/**
	 * Largest number of workspaces one request may name (push batches, subscribe
	 * scopes). Default 64 (the engine's default limit).
	 */
	maxWorkspacesPerRequest?: number;
	/**
	 * This client build's schema revision. When set, the coordinator compares it
	 * against the revision the server advertises (on every stream frame, pull, and
	 * bootstrap) and runs onSchemaSkew on a mismatch. Unset disables the
	 * handshake entirely.
	 */
	clientRevision?: string;
	/**
	 * Decides how to react when the server's advertised revision differs from
	 * clientRevision. Called once per distinct mismatching server revision.
	 * Defaults to "warn" when omitted. See SchemaSkewAction.
	 */
	onSchemaSkew?: (info: { client: string; server: string }) => SchemaSkewAction;
	/**
	 * Mutation ids the undo/redo stacks of one workspace still reference.
	 * Compaction treats them as barriers so the inverses stay valid. Omitted when
	 * undo is disabled.
	 */
	undoRefIds?: (workspaceId: string) => ReadonlySet<bigint>;
	/**
	 * Notified whenever a workspace's queued mutation ids are renumbered — by
	 * compaction folding its never-pushed tail, or by a resequence closing a
	 * mutation-id hole — so undo bookkeeping can remap the ids it references.
	 */
	onCompacted?: (workspaceId: string, remap: ReadonlyMap<bigint, bigint>) => void;
	/**
	 * Cross-tab intent registry (see sync/intent.ts). When present, sync
	 * cycles run with the union of all live tabs' workspaces and subscriptions,
	 * and compaction protects the union of their undo refs. Absent (unit tests,
	 * bespoke embedders) the coordinator uses only its own scopes and undo refs.
	 */
	intent?: IntentRegistry;
	/**
	 * Re-bootstrap every workspace on the first cycle. Set when start-up found
	 * entities in this build's registry that the store had never synced, whose
	 * rows can only arrive as a snapshot (see entities.ts) — passing it in rather
	 * than relying on the dropped cursors closes the window where another tab's
	 * in-flight pull commits a fresh cursor over them.
	 */
	forceResync?: boolean;
}

/** Per-workspace sync state that lives only in memory. */
interface WorkspaceSync {
	/** This tab's own subscription request; empty = full sync. */
	subscriptions: Subscription[];
	/**
	 * Workspace seq this client has pulled through, from PullResponse.seq /
	 * Begin.seq (the cursor itself is an opaque token). Judges streamed page
	 * positions: a page at or below it is redundant. In-memory only — start()
	 * schedules an unconditional first sync, which re-learns it.
	 */
	serverSeq: bigint;
	/**
	 * Tracks consecutive push failures of this workspace's queue head for the
	 * poison backstop, and what it failed with — a different verdict restarts
	 * the count (see countHeadFailure). Reset whenever the head changes or a
	 * push succeeds.
	 */
	poison?: { id: bigint; failures: number; signature: string };
	/**
	 * The server said this client may no longer access the workspace. Permanent:
	 * it is dropped from pushes, pulls, bootstraps and the stream scope set, and
	 * nothing retries it. The local rows stay put — purging is the app's call
	 * (SyncClient.removeWorkspace with purge).
	 */
	revoked: boolean;
	/**
	 * The server refused this workspace's queue for a mutation-id gap and has
	 * not taken it since. Cleared once a push is accepted or the queue is
	 * resequenced onto the server's watermark; see recoverGap.
	 */
	gapped?: boolean;
}

/**
 * What a failed bootstrap leaves behind so the next one can continue its
 * snapshot instead of downloading it again. Stored under metaBootstrapResume,
 * where it is shared with every tab on the store, and written in the same
 * transaction as the rows it accounts for.
 */
interface BootstrapResumeRecord {
	/**
	 * Bootstrap generation of the attempt that wrote this record. A claim adopts
	 * the record only when the generation marker still stands here: that is the
	 * proof no other bootstrap has run since, so the staged rows are still the
	 * partial snapshot the tokens below describe.
	 */
	gen: number;
	/** The generation whose staging prefix holds the partial snapshot. */
	snapshotGen: number;
	/**
	 * Whether the partial snapshot went straight into confirmed state. A record
	 * is adoptable only by an attempt taking the same path — the rows sit in a
	 * different keyspace on each, so mixing them would install half a snapshot.
	 */
	direct: boolean;
	/** The Begin cursor of the snapshot being continued (opaque). */
	cursor: Uint8Array;
	/** Scan position within it, from the last chunk stored (opaque). */
	token: Uint8Array;
}

/**
 * One bootstrap chunk whose rows are written and whose transaction the store has
 * yet to report on. The stream loop decodes the next chunk while these commit,
 * so what a chunk counts and completes lands only once ITS own commit came back
 * — in commit order, which is the order the transactions were handed over.
 */
interface BootstrapChunkCommit {
	/**
	 * Settles with the transaction's failure, or undefined once it committed.
	 * Never rejects: it is awaited some way behind the loop that made it, and a
	 * bare rejection would be an unhandled one until then.
	 */
	settled: Promise<{ err: unknown } | undefined>;
	/** Rows the chunk wrote. */
	rows: number;
	/** Entities the chunk carried rows for, for the hydration coverage. */
	entities: string[];
}

/**
 * Bootstrap chunks whose commit may be outstanding while the loop stages the
 * next one.
 *
 * A store commits its write transactions one at a time, so depth here buys the
 * overlap of one chunk's decoding with one commit and no more; the second slot
 * is what keeps the loop from stalling on the bookkeeping of a chunk that has
 * already committed.
 */
const MAX_INFLIGHT_BOOTSTRAP_COMMITS = 2;

/**
 * Drives background sync for every workspace the client holds: drains their
 * pending queues (one Push carrying a batch per workspace), pulls + rebases each
 * of them, keeps ONE multiplexed change stream alive with backoff, and falls
 * back to polling.
 */
export class Coordinator {
	private rpc: Client<typeof SyncService>;
	private opts: CoordinatorOptions;
	private chain: Promise<unknown> = Promise.resolve();
	private abort = new AbortController();
	private stopped = false;
	private syncing = false;
	private syncAgain = false;
	/** Armed by syncAfterWrite; see there for why there are two of them. */
	private writeDebounceTimer?: ReturnType<typeof setTimeout>;
	private writeMaxDelayTimer?: ReturnType<typeof setTimeout>;
	private retryBackoff = new Backoff();
	/** Per-workspace state, in the order the workspaces were added. */
	private workspaces = new Map<string, WorkspaceSync>();
	/**
	 * Workspaces added since start() whose durable pushed-through watermark has
	 * not been seeded yet; see initPushedThrough.
	 */
	private unseeded = new Set<string>();
	/**
	 * Which entities of a cold-starting workspace are still downloading. Read by
	 * the coverage layer so a reader can tell a not-yet-arrived entity from an
	 * empty one; see hydration.ts.
	 */
	private readonly hydration = new HydrationTracker();
	private leadership?: StreamLeadership;
	/** Whether this tab currently holds a leadership term; see isLeader(). */
	private leaderNow = false;
	/**
	 * Cross-tab "reopen the stream" channel: a scope change on any tab reaches
	 * whichever tab holds the leader stream so it re-scopes. Present only while
	 * sync is running.
	 */
	private reopenSignal?: ReopenSignal;
	/**
	 * Window 'pagehide' handler that announces this tab's departure so a peer
	 * reaps its intent record promptly. Registered (in browsers) by start(),
	 * removed by stop(). Held so the listener can be torn down.
	 */
	private onPageHide?: (e: PageTransitionEvent) => void;
	/**
	 * Aborts the leader's currently-open Subscribe stream so subscribeLoop
	 * reopens it — used to re-scope on a scope change (the browser stream is
	 * unidirectional, so a scope change is a close + reopen). Present only while
	 * a stream is open.
	 */
	private subscribeStreamAbort?: AbortController;
	/**
	 * Interrupts the leader's idle wait (it holds no syncable workspace) so an
	 * added workspace starts streaming at once instead of after the recheck delay.
	 */
	private streamIdleAbort?: AbortController;
	/**
	 * Set when subscribeStreamAbort was fired deliberately (to reopen), so the
	 * resulting stream cancellation is treated as an immediate reopen rather than
	 * an error that backs off.
	 */
	private subscribeReopen = false;
	private readonly poisonThreshold: number;
	private readonly maxMutationsPerPush: number;
	private readonly maxWorkspacesPerRequest: number;
	/**
	 * Armed after an InvalidArgument push whose request named several workspaces:
	 * the RPC error is request-level, so it says nothing about WHICH workspace is
	 * at fault. The next push sends one workspace per request, which makes the
	 * next failure attributable and lets the poison backstop count it. Cleared by
	 * the first push the server answers.
	 */
	private splitPush = false;
	/**
	 * Workspaces whose last push shipped mutations the server never answered for
	 * — a response lost after the write, so what became of them is unknown here.
	 * Their queues hold on to acked mutations (rebase's deferAcked) until the
	 * next push says what happened: the server's watermark already covers those
	 * ids, so dropping them on that alone would retire a rejection nobody ever
	 * saw. Cleared when that push starts.
	 */
	private unresolvedPush = new Set<string>();
	/**
	 * Rows (fullStateKey) whose collaborative-text writes must ship whole
	 * documents because the server refused a delta-state fragment for them. In
	 * memory only and never cleared: a session that hit the fallback once has
	 * nothing to gain from probing again, and the next tab load starts fresh.
	 */
	private textFullState = new Set<string>();
	/**
	 * Text captured from `text_state_stale` rejections, waiting for the
	 * re-bootstrap that makes it applicable again (see applyTextRestores). Keyed
	 * by workspace. Re-applying before the bootstrap would diff against the same
	 * stale row and be refused all over again.
	 */
	private textRestores = new Map<string, TextRestoreRow[]>();
	/**
	 * Writer identities for the restore path, created on first use. Slots are
	 * held for the length of one restore batch and coordinate with the client's
	 * own pool (same clientId, same lock names), so a restore never shares a peer
	 * with a live handle.
	 */
	private peerLeases?: PeerLeases;
	/**
	 * While true, doPush is suspended: the server advertised a schema revision
	 * that this client's skew policy mapped to "block" (read-only / update
	 * required). Pull keeps running so reads stay fresh. Cleared when the server
	 * advertises a matching revision (or none) again.
	 */
	private pushBlocked = false;
	/**
	 * While true, the next sync cycle re-bootstraps every workspace before
	 * pushing/pulling, then clears it. Account-wide, because both things that arm
	 * it are: the "resync" skew action (a property of the server build) and a
	 * registry that gained an entity at start-up (a property of this build).
	 */
	private forceResync: boolean;
	/**
	 * The last server revision we ran the skew policy against (or cleared on).
	 * Lets us act once per distinct revision instead of on every heartbeat.
	 */
	private lastSkewRevision?: string;

	constructor(opts: CoordinatorOptions) {
		this.opts = opts;
		this.rpc = createClient(SyncService, opts.transport);
		for (const [ ws, subs ] of opts.scopes) {
			this.workspaces.set(ws, { subscriptions: subs, serverSeq: 0n, revoked: false });
		}
		this.forceResync = opts.forceResync ?? false;
		this.poisonThreshold = opts.poisonThreshold ?? DEFAULT_POISON_THRESHOLD;
		this.maxMutationsPerPush = Math.max(
			1,
			opts.maxMutationsPerPush ?? DEFAULT_MAX_MUTATIONS_PER_PUSH,
		);
		this.maxWorkspacesPerRequest = Math.max(
			1,
			opts.maxWorkspacesPerRequest ?? DEFAULT_MAX_WORKSPACES_PER_REQUEST,
		);
	}

	/** The workspaces this tab itself holds, with its own subscription sets. */
	private ownScopes(): Map<string, Subscription[]> {
		const out = new Map<string, Subscription[]>();
		for (const [ ws, state ] of this.workspaces) out.set(ws, state.subscriptions);
		return out;
	}

	/**
	 * Swaps one workspace's subscription set and schedules a sync. No local
	 * hashing or persistence: the server compares the set against the cursor and
	 * delta-resyncs only the difference through ordinary pulls (rows entering
	 * scope as values, rows leaving as tombstones or entity drops). When the
	 * server cannot reconstruct the old scope it replies reset=true (or drops the
	 * scope from the stream), which re-bootstraps into the new scope.
	 */
	setSubscriptions(workspaceId: string, subs: Subscription[]): void {
		const state = this.workspaces.get(workspaceId);
		if (!state) return;
		state.subscriptions = subs;
		this.publishScopes();
	}

	/** Begins syncing a workspace that was not in the initial set. */
	addWorkspace(workspaceId: string, subs: Subscription[]): void {
		if (this.workspaces.has(workspaceId)) {
			this.setSubscriptions(workspaceId, subs);
			return;
		}
		this.workspaces.set(workspaceId, { subscriptions: subs, serverSeq: 0n, revoked: false });
		// Its store-resident queue is inherited exactly like those start() found,
		// so it needs the same watermark seed before anything compacts it. Deferred
		// to the push path (this method is synchronous) — see initPushedThrough.
		this.unseeded.add(workspaceId);
		this.publishScopes();
	}

	/** Stops syncing a workspace. Its local data is untouched. */
	removeWorkspace(workspaceId: string): void {
		if (!this.workspaces.delete(workspaceId)) return;
		// Nobody is going to finish that snapshot now. Dropped rather than left
		// standing: the status entry goes with the workspace, and a re-added
		// workspace bootstraps (and re-seeds) from scratch.
		this.hydration.finish(workspaceId);
		this.publishScopes();
	}

	/** Whether this coordinator still syncs the workspace (not removed, not revoked). */
	isSyncing(workspaceId: string): boolean {
		const state = this.workspaces.get(workspaceId);
		return state !== undefined && !state.revoked;
	}

	/**
	 * Publishes a changed scope set: persist it to the intent record, re-scope our
	 * own leader stream, tell peer tabs (theirs may be the leader), and sync.
	 */
	private publishScopes(): void {
		// The union always includes the in-memory set, so the cycle scheduled below
		// and our OWN leader reopen are correct even before this write lands.
		const written = this.opts.intent?.updateScopes(this.ownScopes());
		// Re-scope a live leader stream: it is unidirectional, so a scope change is
		// a close + reopen with the new union (subscribeLoop recomputes it). No-op
		// when this tab holds no stream.
		this.reopenSubscribe();
		// The leader may be ANOTHER tab. Signal it cross-tab to reopen — but only
		// after our intent record write lands, so the leader's union scan sees the
		// new set. (When the leader is this very tab, reopenSubscribe above already
		// handled it against our in-memory set.)
		if (written) void written.then(() => this.reopenSignal?.signal(), () => {});
		else this.reopenSignal?.signal();
		this.syncSoon();
	}

	/**
	 * Aborts the leader's open Subscribe stream so subscribeLoop reopens it with
	 * a freshly computed union — without the backoff an error would incur — and
	 * wakes it from an idle wait. No-op when this tab holds no stream (a
	 * follower, or stream not yet open).
	 */
	private reopenSubscribe(): void {
		if (this.subscribeStreamAbort) {
			this.subscribeReopen = true;
			this.subscribeStreamAbort.abort();
		}
		this.streamIdleAbort?.abort();
	}

	/** Serializes sync operations (push/pull/bootstrap order matters). */
	private run<T>(fn: () => Promise<T>): Promise<T> {
		const result = this.chain.then(fn, fn);
		this.chain = result.catch(() => undefined);
		return result;
	}

	/**
	 * Begins background sync. Does not wait for the first cycle (which
	 * bootstraps workspaces that have no cursor): a dead server surfaces through
	 * the status tracker, not as a start() rejection.
	 */
	async start(): Promise<void> {
		await this.initPushedThrough([ ...this.workspaces.keys() ]);
		const refs = new Map<string, ReadonlySet<bigint>>();
		for (const ws of this.workspaces.keys()) {
			refs.set(ws, this.opts.undoRefIds?.(ws) ?? EMPTY_IDS);
		}
		await this.opts.intent?.seed(this.ownScopes(), refs);
		this.syncSoon();
		// Listen for a peer tab's scope change so a leader stream held by THIS tab
		// re-scopes even when the change originated elsewhere, and for a peer tab's
		// departure so we reap its record and re-scope. Started regardless of
		// leadership: any tab may be (or become) the leader.
		this.reopenSignal = startReopenSignal(this.opts.storageNames.reopenChannel, {
			onReopen: () => this.reopenSubscribe(),
			onDeparted: (tabId) => this.onPeerDeparted(tabId),
		});
		// On tab close, tell a surviving peer to reap our intent record now: a
		// departing tab cannot reliably finish its own async IDB delete during
		// unload, so a closed full-sync tab would otherwise widen the union for
		// up to SUBS_TTL_MS. persisted=true is a bfcache freeze (the tab may
		// return), so leave the record in place then.
		if (typeof window !== 'undefined') {
			this.onPageHide = (e: PageTransitionEvent): void => {
				if (!e.persisted) this.announceDeparture();
			};
			window.addEventListener('pagehide', this.onPageHide);
		}
		if (this.opts.streamEnabled !== false) {
			// In browsers, one VISIBLE tab per ACCOUNT holds the Subscribe stream
			// (Web Locks leader election) and applies the rows it pushes down to the
			// shared store; other tabs see those writes via the shared KV +
			// changeshare, exactly as stream followers do. One stream carries every
			// workspace, so the connection count does not grow with workspace count.
			// Elsewhere (server runtime / no Web Locks) every instance runs its own
			// session-long stream, bounded only by the global stop signal.
			this.leadership = startStreamLeadership(this.opts.storageNames.streamLock, {
				// Bracketed around the leader loop rather than hooked into the lock
				// itself: holding the lock IS the term, so the loop's lifetime is
				// exactly the leadership. A tab that never leads emits nothing,
				// which is the normal case for all but one tab.
				runLeaderLoop: async (termSignal) => {
					this.leaderNow = true;
					this.emit({ type: 'leadership', leader: true });
					try {
						await this.subscribeLoop(termSignal);
					} finally {
						this.leaderNow = false;
						this.emit({ type: 'leadership', leader: false });
					}
				},
				// Web Locks failed us, so this tab will never lead. Run our own stream
				// for the session, exactly as a browser without Web Locks does: a
				// second stream costs a connection, no stream costs realtime.
				onUnavailable: () => {
					if (!this.stopped) void this.subscribeLoop(this.abort.signal);
				},
			});
			if (!this.leadership) {
				void this.subscribeLoop(this.abort.signal);
			}
		}
		void this.pollLoop();
	}

	async stop(): Promise<void> {
		this.stopped = true;
		this.clearWriteWindow();
		this.abort.abort();
		this.leadership?.stop();
		if (this.onPageHide && typeof window !== 'undefined') {
			window.removeEventListener('pagehide', this.onPageHide);
			this.onPageHide = undefined;
		}
		// Tell peers we are leaving so they re-scope at once, BEFORE closing the
		// channel. We also delete our own record below, but the announcement lets
		// a peer narrow its union without waiting for its next sync cycle.
		this.announceDeparture();
		this.reopenSignal?.stop();
		// Clean shutdown removes the intent record; a crash/close-without-stop
		// relies on the registry's TTLs instead. Best-effort: a failed delete
		// only leaves a record that ages out.
		try {
			await this.opts.intent?.delete();
		} catch {
			// ignore
		}
		await this.chain.catch(() => undefined);
	}

	/** Broadcasts this tab's intent-record id so a peer can reap it. No-op when
	 *  there is no intent registry (single-client runtime) or channel. */
	private announceDeparture(): void {
		const tabId = this.opts.intent?.tabId;
		if (tabId) this.reopenSignal?.announceDeparture(tabId);
	}

	/**
	 * A peer tab announced it is gone: reap its intent record so it stops
	 * widening the union, then re-scope. The leader reopens its Subscribe stream
	 * onto the narrowed union (followers no-op); the scheduled cycle pulls the
	 * difference (rows that left scope tombstone). Reaping is idempotent, so
	 * every receiving tab doing it is harmless.
	 */
	private onPeerDeparted(tabId: string): void {
		void (async () => {
			try {
				await this.opts.intent?.reap(tabId);
			} catch {
				// best-effort; the TTL collects it otherwise
			}
			this.reopenSubscribe();
			this.syncSoon();
		})();
	}

	/**
	 * Publishes each workspace's effective scope (this tab's own request vs. the
	 * union it actually syncs) to the status tracker, so a UI can show when a peer
	 * tab — or a lingering record from a closed one — has widened it. Full sync
	 * (own empty) is never "widened": nothing is broader than everything.
	 */
	private reportScopes(effective: ScopeMap): void {
		for (const [ ws, state ] of this.workspaces) {
			const own = state.subscriptions.length;
			const eff = effective.get(ws)?.length ?? own;
			const widened = own > 0 && (eff === 0 || eff > own);
			this.opts.status.updateWorkspace(ws, { scope: { own, effective: eff, widened } });
		}
	}

	/** Schedules a push+pull cycle; safe to call from anywhere, any time. */
	syncSoon(): void {
		void this.syncCycle();
	}

	/**
	 * Schedules a cycle for a local write, coalescing a burst of writes into one
	 * push (pushDebounceMs, restarted per write) while still shipping within a
	 * bound (pushMaxDelayMs, armed once per burst). Two timers rather than one
	 * because neither alone is enough: the restarting one alone never fires
	 * under continuous typing, and the bounding one alone makes an isolated
	 * write wait out the whole ceiling.
	 *
	 * Only local writes come through here. Deferring one is safe because it is
	 * already durable in the pending queue before this is called — what the
	 * delay costs is how soon a PEER sees the edit, never the edit itself. That
	 * is also why stop() may simply drop an armed window: the queue outlives the
	 * session and the next start() pushes it.
	 */
	syncAfterWrite(): void {
		const debounce = this.opts.pushDebounceMs ?? DEFAULT_PUSH_DEBOUNCE_MS;
		if (debounce <= 0) {
			this.syncSoon();
			return;
		}
		clearTimeout(this.writeDebounceTimer);
		this.writeDebounceTimer = setTimeout(() => this.flushWriteWindow(), debounce);
		const cap = this.opts.pushMaxDelayMs ?? DEFAULT_PUSH_MAX_DELAY_MS;
		if (cap > 0 && this.writeMaxDelayTimer === undefined) {
			this.writeMaxDelayTimer = setTimeout(() => this.flushWriteWindow(), cap);
		}
	}

	private flushWriteWindow(): void {
		this.clearWriteWindow();
		if (!this.stopped) this.syncSoon();
	}

	/**
	 * Disarms the coalescing window. Called both when it fires and whenever a
	 * cycle starts for any other reason: that cycle pushes the same queue the
	 * window was waiting to push, so leaving it armed would only buy a second,
	 * redundant cycle right behind it.
	 */
	private clearWriteWindow(): void {
		clearTimeout(this.writeDebounceTimer);
		clearTimeout(this.writeMaxDelayTimer);
		this.writeDebounceTimer = undefined;
		this.writeMaxDelayTimer = undefined;
	}

	/** Runs one push+pull cycle and resolves when it completes (or fails). */
	syncNow(): Promise<void> {
		// Synchronously, before the chain: a write landing while this cycle runs
		// must arm a FRESH window (this cycle may already have snapshotted the
		// queue past it), whereas one that arrived before it is subsumed.
		this.clearWriteWindow();
		return this.run(async () => {
			const intent = this.opts.intent;
			void intent?.heartbeatIfStale();
			// Snapshot the scope map for the whole cycle so a concurrent
			// setSubscriptions/addWorkspace cannot mix scopes between bootstrap and
			// pull. Recomputed every cycle — one shared-lock KV scan is negligible
			// next to the RPCs.
			const own = this.ownScopes();
			const union = intent ? await intent.unionScopes(own) : own;
			this.reportScopes(union);
			// Push/pull/bootstrap only the workspaces THIS tab holds — a peer's are
			// its own cycle's business, and only a tab that holds a workspace has the
			// in-memory state (poison isolation, seq, access verdict) those paths
			// need. What the union does supply is each workspace's SUBSCRIPTION set:
			// every tab presents the same scope against the one shared cursor, so the
			// server never delta-resyncs A→B→A between tabs (subscription thrash).
			// (The leader's stream is the opposite: it carries the whole union, so a
			// workspace only a peer holds still gets its live tail — see planStream.)
			const active: [string, Subscription[]][] = [];
			for (const [ ws, state ] of this.workspaces) {
				if (state.revoked) continue;
				active.push([ ws, union.get(ws) ?? state.subscriptions ]);
			}
			if (active.length === 0) return;

			// Bootstrap first, per workspace: a workspace with no cursor (or one the
			// skew policy asked to resync) has nothing to push or pull against.
			// Failures are collected rather than thrown so one unreachable or
			// misbehaving workspace cannot starve the others of a cycle; the first is
			// rethrown at the end to drive the usual backoff.
			const errors: unknown[] = [];
			const syncable: [string, Subscription[]][] = [];
			for (const [ ws, subs ] of active) {
				try {
					if (await this.ensureBase(ws, subs)) syncable.push([ ws, subs ]);
				} catch (err) {
					if (!this.noteRevoked(ws, err)) errors.push(err);
				}
			}
			// Keep a "resync" decision armed when a bootstrap failed, so the next
			// cycle retries it rather than silently falling back to a cursor pull.
			if (errors.length === 0) this.forceResync = false;
			// Text saved from a compaction rejection can be re-applied now that the
			// workspace has re-read the row, and before the push below so it ships
			// in this same cycle. A failure here must not cost the cycle: the
			// restore stays dropped rather than replaying against a moving row.
			for (const [ ws ] of syncable) {
				if (this.textRestores.has(ws)) {
					try {
						await this.applyTextRestores(ws);
					} catch (err) {
						errors.push(err);
					}
				}
			}

			if (syncable.length > 0) {
				try {
					await this.doPush(syncable.map(([ ws ]) => ws));
				} catch (err) {
					errors.push(err);
				}
				// Snapshotted BEFORE the pulls, which clear the flag: the queue did
				// not move this cycle whether or not the recovery below repairs it
				// for the next one, and a cycle that shipped nothing must not report
				// success (the failure is otherwise invisible — a refusal rides as a
				// code on an RPC that succeeded).
				const gapped = syncable
					.map(([ ws ]) => ws)
					.filter((ws) => this.workspaces.get(ws)?.gapped);
				for (const [ ws, subs ] of syncable) {
					if (this.workspaces.get(ws)?.revoked) continue;
					try {
						await this.doPull(ws, subs);
					} catch (err) {
						if (!this.noteRevoked(ws, err)) errors.push(err);
					}
				}
				for (const ws of gapped) errors.push(gapError(ws));
			}
			if (errors.length > 0) throw errors[0];
		});
	}

	/**
	 * Makes sure a workspace has a usable local base before push/pull: it
	 * re-bootstraps on a "resync" skew decision, or when there is no cursor at
	 * all. Returns false when the workspace must sit this cycle out — a
	 * superseded bootstrap installs no cursor, so another tab's bootstrap owns the
	 * result and pushing/pulling against a missing cursor would be wrong.
	 */
	private async ensureBase(workspaceId: string, subs: Subscription[]): Promise<boolean> {
		if (this.forceResync) {
			// A "resync" skew decision: re-bootstrap onto the current server's state
			// before doing anything else. The flag stays armed across the bootstrap
			// and is cleared by the caller only once every workspace has been
			// handled. A throw (e.g. a transient network error mid-stream) leaves it
			// armed so the next cycle retries rather than silently falling back to a
			// normal cursor pull — observeServerRevision won't re-arm it, since the
			// mismatching revision was already handled.
			return this.doBootstrap(workspaceId, subs);
		}
		if (await this.cursor(workspaceId)) {
			// A cursor means a whole snapshot is installed — this tab's, or the one
			// that superseded it while it was streaming. Either way nothing is
			// half-arrived any more, so a cold bootstrap that never got to say so
			// itself (superseded, or interrupted before another tab finished the
			// job) stops being reported as hydrating here.
			await this.finishHydration(workspaceId);
			return true;
		}
		return this.doBootstrap(workspaceId, subs);
	}

	private async syncCycle(): Promise<void> {
		if (this.syncing) {
			this.syncAgain = true;
			return;
		}
		this.syncing = true;
		try {
			do {
				this.syncAgain = false;
				this.opts.status.update({ state: 'syncing' });
				try {
					await this.syncNow();
					this.retryBackoff.reset();
					this.opts.status.update({
						state: 'idle',
						online: true,
						lastSyncedAt: new Date(),
						lastError: undefined,
					});
				} catch (err) {
					// The error lands in the status surface; the cycle still retries
					// with backoff exactly as before.
					this.opts.status.update({ state: 'error', online: false, lastError: errorInfo(err) });
					if (this.stopped) return;
					await sleep(this.retryBackoff.next(), this.abort.signal);
					this.syncAgain = true;
				}
			}
			while (this.syncAgain && !this.stopped);
		} finally {
			this.syncing = false;
		}
	}

	/** Reports one observability event; see emitSyncEvent. */
	private emit(event: UnstampedSyncEvent): void {
		emitSyncEvent(this.opts.onSyncEvent, event);
	}

	/** Classifies a caught error for an event's `outcome`. */
	private static outcomeOf(err: unknown): SyncEventOutcome {
		// Unavailable is the shape of "the server is not reachable", which is the
		// normal condition of an offline-capable client rather than a fault.
		// Keeping it out of `error` is what lets an error rate mean something.
		return err instanceof ConnectError && err.code === Code.Unavailable ? 'offline' : 'error';
	}

	private async cursor(workspaceId: string): Promise<Uint8Array | undefined> {
		return this.opts.kvMutex.runShared(async () => {
			const tx = await this.opts.kv.read();
			try {
				return (await tx.get(metaCursor(workspaceId))) as Uint8Array | undefined;
			} finally {
				tx.release();
			}
		});
	}

	/**
	 * One workspace's resume position: the stored cursor and the acked-seq
	 * freshness floor that rides beside it as PullRequest.min_seq /
	 * WorkspaceScope.min_seq. Read together so a pull opened between a push's
	 * ack and its record cannot pair a fresh cursor with a stale floor.
	 */
	private async pullPosition(
		workspaceId: string,
	): Promise<{ cursor: Uint8Array | undefined; minSeq: bigint }> {
		return this.opts.kvMutex.runShared(async () => {
			const tx = await this.opts.kv.read();
			try {
				const cursor = (await tx.get(metaCursor(workspaceId))) as Uint8Array | undefined;
				const acked = (await tx.get(metaAckedSeq(workspaceId))) as string | undefined;
				return { cursor, minSeq: acked === undefined ? 0n : BigInt(acked) };
			} finally {
				tx.release();
			}
		});
	}

	/**
	 * Records the highest workspace seq the server acked one of this store's
	 * pushes at (see metaAckedSeq). Monotonic max inside the transaction: tabs
	 * push independently, and a lower ack landing later must not regress a
	 * floor another tab already earned.
	 */
	private async recordAckedSeq(workspaceId: string, seq: bigint): Promise<void> {
		await this.opts.kvMutex.run(async () => {
			const tx = await this.opts.kv.write();
			try {
				const stored = (await tx.get(metaAckedSeq(workspaceId))) as string | undefined;
				if (stored !== undefined && BigInt(stored) >= seq) {
					tx.release();
					return;
				}
				await tx.put(metaAckedSeq(workspaceId), seq.toString());
				await tx.commit();
			} catch (err) {
				tx.release();
				throw err;
			}
		});
	}

	/**
	 * The entity names to send as no_delta_entities.
	 *
	 * Re-read per pull cycle and per stream open rather than cached: the set
	 * grows when a tab of a different build opens the shared store, and this tab
	 * has no signal for that beyond the reset it eventually causes. One point
	 * read costs less than one stale answer.
	 */
	private async noDeltaEntities(): Promise<string[]> {
		return this.opts.kvMutex.runShared(async () => {
			const tx = await this.opts.kv.read();
			try {
				return await opaqueEntities(tx, this.opts.registry);
			} finally {
				tx.release();
			}
		});
	}

	/**
	 * Marks a workspace permanently revoked when an error carries the revocation
	 * code, and reports it. Returns true when it did — the caller then treats the
	 * failure as handled rather than as a reason to back off and retry, because
	 * retrying can never change the answer.
	 *
	 * Pull, Bootstrap and Query are single-workspace RPCs, so the server denies
	 * them as an RPC error with the code in metadata rather than as in-band data
	 * (see codes.ts); this is where that arrives.
	 */
	private noteRevoked(workspaceId: string, err: unknown): boolean {
		if (
			!(err instanceof ConnectError)
			|| err.metadata.get(ErrorCodeMetadataKey) !== CodeWorkspaceAccessRevoked
		) {
			return false;
		}
		this.markRevoked(workspaceId);
		return true;
	}

	/** Records a permanent access revocation and re-scopes the stream off it. */
	private markRevoked(workspaceId: string): void {
		const state = this.workspaces.get(workspaceId);
		if (!state || state.revoked) return;
		state.revoked = true;
		this.opts.status.updateWorkspace(workspaceId, { access: 'revoked' });
	}

	/**
	 * Pushes every syncable workspace's queue. The wire carries one batch per
	 * workspace in a single request, each committed in its own server-side
	 * transaction, so a client holding many workspaces spends one round trip
	 * rather than N. The engine counts its mutation limit ACROSS the batches of a
	 * request, so the packing below budgets across workspaces.
	 */
	private async doPush(workspaceIds: string[]): Promise<void> {
		// Read-only mode: the server's schema revision is one this client cannot
		// safely write against, so hold the queues (they keep growing locally) until
		// the revisions line up and the skew clears.
		if (this.pushBlocked) return;
		// Whatever the last cycle could not resolve, this one is about to re-ship.
		this.unresolvedPush.clear();
		// Seed the watermark of any workspace added since start() before reading it
		// below: absent parses as 0, and compaction against 0 would renumber an
		// inherited queue the server may already have applied.
		await this.initPushedThrough(workspaceIds.filter((ws) => this.unseeded.has(ws)));
		// Squash the never-pushed tails first: shrinks both this push and the
		// rebase the following pull will run. Skipped for a workspace isolating a
		// poison head — there we are deliberately draining one mutation at a time.
		for (const ws of workspaceIds) {
			if (!this.workspaces.get(ws)?.poison) await this.compact(ws);
		}

		// Snapshot the queues AND raise the durable, cross-tab pushed-through
		// watermarks to cover what this cycle will ship — atomically, under the kv
		// mutex, BEFORE any RPC leaves. From here those ids are off-limits to
		// compaction (this tab's or any peer's), so a lost push response or a
		// concurrent tab cannot leave compaction renumbering/folding a mutation the
		// server already applied. Marking before the RPC is deliberately
		// conservative: a push that never lands just forfeits some later compaction
		// of those ids, never correctness.
		// Every lookup below tolerates a missing state: removeWorkspace can land
		// during any of this method's awaits.
		const queues = await this.markPushThrough(workspaceIds);
		for (const ws of workspaceIds) {
			const state = this.workspaces.get(ws);
			if (state && !queues.has(ws)) state.poison = undefined;
		}
		if (queues.size === 0) return;

		// Normalize text PATCHes to the delta state that goes on the wire, on
		// copies: what ships is cut against the confirmed row rather than against
		// whatever each queue entry happens to hold, and the queue itself must
		// keep the values rebase replays.
		const wire = await this.toWireQueues(queues);

		// A workspace whose head has already failed a push is pushed ALONE, in its
		// own request, so the next failure (or success) is unambiguously
		// attributable to that one mutation. Other workspaces wait a cycle; the
		// isolation runs at most poisonThreshold times.
		for (const [ ws, pending ] of wire) {
			if (this.workspaces.get(ws)?.poison?.id === pending[0]!.mutationId) {
				await this.pushRequests(new Map([ [ ws, [ pending[0]! ] ] ]), 1);
				return;
			}
		}
		await this.pushRequests(wire, this.splitPush ? 1 : this.maxWorkspacesPerRequest);
		// Acked/rejected mutations leave the queues during each pull's rebase via
		// last_acked_mutation_id.
	}

	/**
	 * Rewrites each workspace's queue into the form that goes on the wire,
	 * substituting delta-state fragments for collaborative-text PATCH values
	 * where the confirmed row makes that both possible and worthwhile. Ids,
	 * order and everything else are untouched, so every caller downstream still
	 * matches results back by mutation id.
	 *
	 * One read transaction covers all workspaces; a failure here is never worth
	 * failing a push over, so it degrades to shipping full state.
	 */
	private async toWireQueues(
		queues: Map<string, Mutation[]>,
	): Promise<Map<string, Mutation[]>> {
		if (![ ...queues.keys() ].some((ws) => this.hasTextWork(queues.get(ws)!))) return queues;
		try {
			return await this.opts.kvMutex.runShared(async () => {
				const tx = await this.opts.kv.read();
				try {
					const out = new Map<string, Mutation[]>();
					for (const [ ws, pending ] of queues) {
						out.set(
							ws,
							await toWireMutations(tx, this.opts.registry, ws, pending, this.textFullState),
						);
					}
					return out;
				} finally {
					tx.release();
				}
			});
		} catch {
			return queues;
		}
	}

	/** Whether any queued mutation touches an entity that has text fields. */
	private hasTextWork(pending: readonly Mutation[]): boolean {
		return pending.some((m) => {
			const cruds = m.payload.case === 'crud'
				? [ m.payload.value ]
				: m.payload.case === 'batch'
				? m.payload.value.mutations
				: [];
			return cruds.some((c) => this.opts.registry.textPathsOf(c.entity).length > 0);
		});
	}

	/**
	 * Ships the given per-workspace queues in as few requests as the limits allow,
	 * handling rejections, per-workspace batch failures and the poison backstop.
	 *
	 * A workspace's mutations must reach the server in id order and gap-free, so a
	 * workspace whose batch failed (or whose prefix was refused) is dropped from
	 * the remaining rounds rather than having its next slice sent past the gap.
	 */
	private async pushRequests(
		queues: Map<string, Mutation[]>,
		maxWorkspaces: number,
	): Promise<void> {
		/** What is left to ship per workspace, and whether its next slice is the queue head. */
		const remaining = new Map<string, { pending: Mutation[]; atHead: boolean }>();
		for (const [ ws, pending ] of queues) remaining.set(ws, { pending, atHead: true });
		const rejections: RejectionInfo[] = [];
		try {
			while (remaining.size > 0) {
				const batches: { workspaceId: string; mutations: Mutation[] }[] = [];
				const shipped: { workspaceId: string; count: number; atHead: boolean }[] = [];
				// What was sent this round, so a rejection can name the rows it would
				// have written; the server answers with ids alone.
				const inFlight = new Map<string, Mutation>();
				let budget = this.maxMutationsPerPush;
				for (const [ ws, entry ] of remaining) {
					if (batches.length >= maxWorkspaces || budget <= 0) break;
					const take = Math.min(budget, entry.pending.length);
					const slice = entry.pending.slice(0, take);
					for (const m of slice) inFlight.set(`${ws}\u0000${m.mutationId}`, m);
					batches.push({ workspaceId: ws, mutations: slice });
					shipped.push({ workspaceId: ws, count: take, atHead: entry.atHead });
					budget -= take;
				}

				let resp;
				const pushStarted = performance.now();
				const submitted = batches.reduce((n, b) => n + b.mutations.length, 0);
				try {
					resp = await this.rpc.push({ batches }, { signal: this.abort.signal });
				} catch (err) {
					this.emit({
						type: 'push',
						workspaceIds: batches.map((b) => b.workspaceId),
						mutations: submitted,
						applied: 0,
						rejected: 0,
						durationMs: performance.now() - pushStarted,
						outcome: Coordinator.outcomeOf(err),
					});
					if (err instanceof ConnectError && err.code === Code.InvalidArgument) {
						const lone = shipped.length === 1 ? shipped[0]! : undefined;
						if (lone?.atHead && this.poisonThreshold > 0) {
							// Deterministic head failure of a single workspace. Normally such
							// failures self-heal because the server acks them as rejections
							// (see the engine push handler); this is the backstop for an older
							// server (version skew) that still RPC-errors. Count failures of
							// the head, and once it has been isolated and confirmed to fail on
							// its own, dead-letter it so the queue stops wedging forever.
							const state = this.workspaces.get(lone.workspaceId);
							const head = queues.get(lone.workspaceId)?.[0];
							if (
								state && head
								&& this.countHeadFailure(state, head, `rpc ${err.code}`, lone.count === 1)
							) {
								await this.deadLetter(lone.workspaceId, head, err.rawMessage);
								return;
							}
							throw err; // retry; the next cycle isolates the head.
						}
						if (shipped.length > 1) {
							// Request-level error over several workspaces says nothing about
							// which one is at fault. Fall back to one workspace per request
							// next time so the failure becomes attributable.
							this.splitPush = true;
							throw err;
						}
						// A later slice is malformed but the good prefix already applied.
						// Stop pushing and let the pull ack that prefix, which moves the bad
						// mutation toward the head where isolation can dead-letter it.
						return;
					}
					// The batch may well have committed — a response can be lost after
					// the write — so these mutations must be re-pushed to learn what
					// became of them. Until then their queues keep acked ids rather
					// than rebasing them away against a watermark that has already
					// moved past a verdict this client never received.
					for (const s of shipped) this.unresolvedPush.add(s.workspaceId);
					throw err; // network / unknown: retry the whole cycle with backoff.
				}

				// The server answered, so mixing workspaces is not what broke.
				this.splitPush = false;
				this.observeServerRevision(resp.schemaRevision);
				// Counted before the per-workspace handling below, which may throw
				// or return early: the RPC succeeded, and what the server decided
				// about these mutations is exactly what this event is for. A
				// rejection is an ack, so it is neither an error nor a retry — and
				// without this it is invisible on both sides of the wire.
				let applied = 0;
				let rejected = 0;
				for (const r of resp.results) {
					for (const m of r.results) {
						if (m.status === MutationStatus.REJECTED) rejected++;
						else if (m.status === MutationStatus.APPLIED) applied++;
					}
				}
				this.emit({
					type: 'push',
					workspaceIds: batches.map((b) => b.workspaceId),
					mutations: submitted,
					applied,
					rejected,
					durationMs: performance.now() - pushStarted,
					outcome: 'ok',
				});
				const byWorkspace = new Map(resp.results.map((r) => [ r.workspaceId, r ]));
				for (const sent of shipped) {
					const wsResult = byWorkspace.get(sent.workspaceId);
					// One batch out, one result in. Anything else is a server answering
					// a request we did not make.
					if (!wsResult) {
						throw new Error(`push response carried no result for workspace ${sent.workspaceId}`);
					}
					const state = this.workspaces.get(sent.workspaceId);
					const entry = remaining.get(sent.workspaceId)!;

					if (wsResult.errorCode) {
						// The batch failed as a whole, so nothing applied and nothing was
						// acked: the queue stays put and the code decides what happens next.
						// Either way this workspace ships nothing more this cycle — its
						// remaining mutations would land past the gap.
						remaining.delete(sent.workspaceId);
						if (wsResult.errorCode === CodeMutationIdGap) {
							// The server is ahead of or behind our queue (e.g. a restored
							// backup, or a locally dropped mutation it never saw). Stop
							// pushing and let this cycle's pull settle which: ids at or
							// below the watermark are dropped by rebase, and a queue
							// stranded above it is renumbered onto the watermark. Recorded
							// rather than retried, because a re-push of the same ids can
							// only be refused the same way.
							if (state) {
								state.poison = undefined;
								state.gapped = true;
							}
							continue;
						}
						if (wsResult.errorCode === CodeWorkspaceAccessRevoked) {
							// Permanent: this client may no longer write here. Retrying can
							// never succeed, so stop and let the status carry it.
							if (state) state.poison = undefined;
							this.markRevoked(sent.workspaceId);
							continue;
						}
						// Internal or unrecognized: presumed transient, so the cycle retries
						// with backoff rather than draining a queue the server never took.
						// A deterministic one would retry forever though — a custom mutator
						// returning a plain error instead of Reject(...) fails its batch this
						// way on every attempt — so the head runs the same isolation and
						// dead-lettering as a deterministic RPC failure. Only a repeat of the
						// identical verdict counts, which is what keeps an outage (whose
						// batches fail for changing reasons) out of the backstop.
						const failure = wsResult.errorMessage || wsResult.errorCode;
						const head = queues.get(sent.workspaceId)?.[0];
						if (
							state && head && sent.atHead && this.poisonThreshold > 0
							&& this.countHeadFailure(
								state,
								head,
								`batch ${wsResult.errorCode} ${wsResult.errorMessage}`,
								sent.count === 1,
							)
						) {
							await this.deadLetter(sent.workspaceId, head, failure);
							continue;
						}
						throw new Error(`push failed for workspace ${sent.workspaceId}: ${failure}`);
					}

					// A clean (or rejecting) response means the head is not poison, and
					// that the server took the ids as numbered.
					if (state) {
						state.poison = undefined;
						state.gapped = false;
					}
					let ackedSeq = 0n;
					for (const result of wsResult.results) {
						if (result.status === MutationStatus.APPLIED && result.seq > ackedSeq) {
							ackedSeq = result.seq;
						}
						if (result.status === MutationStatus.REJECTED) {
							const m = inFlight.get(`${sent.workspaceId}\u0000${result.mutationId}`);
							rejections.push({
								workspaceId: sent.workspaceId,
								mutationId: result.mutationId,
								errorCode: result.errorCode,
								errorMessage: result.errorMessage,
								...(m ? describeMutation(m) : { targets: [] }),
							});
						}
					}
					// Rejections carry seq 0, so a rejected-only batch records nothing.
					if (ackedSeq > 0n) await this.recordAckedSeq(sent.workspaceId, ackedSeq);
					entry.pending = entry.pending.slice(sent.count);
					entry.atHead = false;
					if (entry.pending.length === 0) remaining.delete(sent.workspaceId);
				}
			}
		} finally {
			// Process every rejection we actually received, even if a *later*
			// request then threw and is about to propagate out. The server already
			// bumped its watermark for these ids, so a retried push answers
			// ALREADY_APPLIED and the REJECTED status never reappears — dropping them
			// here would silently strand the doomed optimistic edits until a later
			// pull's dropAcked. Running in `finally` covers the rethrow path without
			// disturbing the clean fall-through.
			if (rejections.length > 0) {
				// A refused fragment is not a refused edit: capture the affected rows'
				// full local state BEFORE the rollback discards it, so the same edit can
				// be re-sent as a full document. Recovered rejections are handled here
				// and never reach the app.
				const recovered = await this.captureTextFullState(rejections, queues)
					.catch(() => ({ rows: new Map<string, TextResendRow[]>(), handled: new Set<string>() }));
				// Text refused for predating a compaction cannot be re-sent as-is; its
				// visible text is captured here and re-applied once the workspace has
				// re-bootstrapped onto the state the server actually holds.
				const restored = await this.captureTextStale(rejections, queues)
					.catch(() => new Set<string>());
				for (const key of restored) recovered.handled.add(key);
				// Roll the rejected mutations back immediately — a rejection is an ack,
				// so waiting for the next pull would leave doomed optimistic state
				// visible in the meantime. Callbacks fire after the rollback so apps
				// observe post-rollback state.
				const byWorkspace = new Map<string, bigint[]>();
				for (const r of rejections) {
					const list = byWorkspace.get(r.workspaceId) ?? [];
					list.push(r.mutationId);
					byWorkspace.set(r.workspaceId, list);
				}
				for (const [ ws, ids ] of byWorkspace) await this.rollbackPending(ws, ids);
				for (const [ ws, rows ] of recovered.rows) {
					try {
						await this.resendTextFullState(ws, rows);
					} catch {
						// The edit could not be re-queued after all, so it really is
						// gone: hand those rejections back to the app rather than
						// swallowing a loss the recovery promised to undo.
						for (const key of recovered.handled) {
							if (key.startsWith(`${ws} `)) recovered.handled.delete(key);
						}
					}
				}
				for (const info of rejections) {
					if (recovered.handled.has(`${info.workspaceId} ${info.mutationId}`)) continue;
					this.opts.onMutationRejected?.(info);
				}
			}
		}
	}

	/**
	 * One-time seed of each workspace's durable pushed-through watermark for a
	 * store written by a build that predates it (the key is absent). Such a queue
	 * may already have been pushed and we cannot know how much, so conservatively
	 * mark the whole inherited queue as pushed: compaction leaves it alone and
	 * only squashes mutations enqueued from here on (which get higher ids). A
	 * workspace that already has the key — including 0 for a genuinely
	 * never-pushed queue written by this build — is left untouched. Idempotent and
	 * cross-tab safe.
	 *
	 * Called for the workspaces held at start(), and again from the push path for
	 * any workspace added after it: compaction reads an absent mark as 0, which
	 * would let it renumber ids the server has already applied. A seed that fails
	 * leaves the workspace marked unseeded, so the next cycle retries it before
	 * compacting.
	 */
	private async initPushedThrough(workspaceIds: string[]): Promise<void> {
		if (workspaceIds.length === 0) return;
		await this.opts.kvMutex.run(async () => {
			const tx = await this.opts.kv.write();
			try {
				let dirty = false;
				for (const ws of workspaceIds) {
					if ((await tx.get(metaPushedThrough(ws))) !== undefined) continue;
					const pending = await readPending(tx, ws);
					const through = pending.length ? pending[pending.length - 1]!.mutationId : 0n;
					await writePushedThrough(tx, ws, through);
					dirty = true;
				}
				if (!dirty) {
					tx.release();
					return;
				}
				await tx.commit();
			} catch (err) {
				tx.release();
				throw err;
			}
		});
		for (const ws of workspaceIds) this.unseeded.delete(ws);
	}

	/**
	 * Compacts one workspace's never-pushed tail and keeps undo bookkeeping in
	 * step with the renumbering it does.
	 */
	private async compact(workspaceId: string): Promise<void> {
		// Read before the mutex, so an enqueue that wins it leaves this snapshot
		// short of the id its undo group just started referencing. Harmless: the
		// enqueue persisted that id to the intent record in its own transaction,
		// and the union below picks it up. The snapshot only ever ADDS ids the
		// record may not carry yet, which over-protects at worst.
		const own = this.opts.undoRefIds?.(workspaceId) ?? EMPTY_IDS;
		const intent = this.opts.intent;
		const result = await this.opts.kvMutex.run(() =>
			compactPending(this.opts.kv, this.opts.registry, this.opts.mirrors, workspaceId, (tx) =>
				// The undo-ref union — every live tab's record, this one included — is
				// read INSIDE the compaction transaction: atomic with the renumber, so
				// referenced mutations are never folded or renumbered out from under
				// any tab's stacks.
				intent ? intent.unionUndoRefIds(tx, workspaceId, own) : own)
		);
		if (result.changed) {
			this.opts.onCompacted?.(workspaceId, result.remap);
			this.opts.status.setPendingCount(workspaceId, result.pendingCount);
			// The own record's ref ids are stale after the renumber; onCompacted
			// above already remapped the in-memory stacks, so refresh from those.
			void intent?.updateRefs(workspaceId, this.opts.undoRefIds?.(workspaceId) ?? EMPTY_IDS);
		}
	}

	/**
	 * Renumbers a workspace's queue to resume at `firstId` and carries the
	 * bookkeeping that names mutation ids across the move: the undo stacks (via
	 * onCompacted, which exists for exactly this remap) and this tab's intent
	 * record. Returns whether anything moved.
	 *
	 * Only ever called with an id the server has provably not taken — the id a
	 * failed push dropped, or the watermark a pull just reported — because
	 * renumbering onto an applied id would re-send it as a new mutation.
	 */
	private async resequence(workspaceId: string, firstId: bigint): Promise<boolean> {
		const result = await this.opts.kvMutex.run(() =>
			resequencePending(
				this.opts.kv,
				this.opts.registry,
				this.opts.mirrors,
				workspaceId,
				firstId,
			)
		);
		if (!result.changed) return false;
		this.afterResequence(workspaceId, firstId, result);
		await this.opts.notify(result.touched);
		return true;
	}

	/**
	 * The in-memory half of a renumber, once its transaction has committed: the
	 * status count, and — when ids actually moved — the bookkeeping that names
	 * them (the undo stacks via onCompacted, this tab's intent record) plus the
	 * event.
	 */
	private afterResequence(workspaceId: string, firstId: bigint, result: ResequenceResult): void {
		this.opts.status.setPendingCount(workspaceId, result.pendingCount);
		if (!result.changed) return;
		this.opts.onCompacted?.(workspaceId, result.remap);
		// The record's ref ids name the old numbering; onCompacted above already
		// remapped the in-memory stacks, so refresh from those.
		void this.opts.intent?.updateRefs(workspaceId, this.opts.undoRefIds?.(workspaceId) ?? EMPTY_IDS);
		this.emit({
			type: 'resequence',
			workspaceId,
			firstId,
			mutations: result.pendingCount,
		});
	}

	/**
	 * Settles a queue the server refused for a mutation-id gap, using the
	 * watermark the just-finished pull reported.
	 *
	 * A queue stranded ABOVE the watermark is the unrecoverable half of the
	 * refusal: the pull's rebase can only retire ids at or below it, so a hole
	 * below the head closes only by renumbering. The pull is the arbiter either
	 * way — once it has run, whatever it left is contiguous with the watermark
	 * or is about to be made so — so the flag clears here regardless.
	 */
	private async recoverGap(workspaceId: string, lastAcked: bigint): Promise<void> {
		const state = this.workspaces.get(workspaceId);
		if (!state?.gapped) return;
		state.gapped = false;
		await this.resequence(workspaceId, lastAcked + 1n);
	}

	/**
	 * Reads the pending queues and, in the same transaction, raises each
	 * workspace's durable pushed-through watermark to the highest id this cycle is
	 * about to ship: the whole queue normally, or just the isolated head while
	 * draining a poison mutation one at a time. Returns the non-empty queues to
	 * push. Run before the push RPC so the marks are durable and visible to peers
	 * the moment a mutation could reach the server.
	 */
	private async markPushThrough(workspaceIds: string[]): Promise<Map<string, Mutation[]>> {
		return this.opts.kvMutex.run(async () => {
			const queues = new Map<string, Mutation[]>();
			const tx = await this.opts.kv.write();
			try {
				let dirty = false;
				for (const ws of workspaceIds) {
					const pending = await readPending(tx, ws);
					const head = pending[0];
					if (!head) continue;
					queues.set(ws, pending);
					const isolating = this.workspaces.get(ws)?.poison?.id === head.mutationId;
					const through = isolating ? head.mutationId : pending[pending.length - 1]!.mutationId;
					if (through > (await readPushedThrough(tx, ws))) {
						await writePushedThrough(tx, ws, through);
						dirty = true;
					}
				}
				if (dirty) await tx.commit();
				else tx.release();
				return queues;
			} catch (err) {
				tx.release();
				throw err;
			}
		});
	}

	/**
	 * Reads back the live local state of every text field a `text_full_state_required`
	 * rejection touched, and pins those rows to full-state writes for the rest of
	 * the session. Runs BEFORE the rollback, which is the only moment the edit
	 * still exists locally.
	 *
	 * Returns the captured rows per workspace plus the rejections it took
	 * ownership of. Ownership is all-or-nothing per rejection: unless every text
	 * field the mutation wrote yields state to re-send, nothing of it is re-sent
	 * and the rejection is left to the app. A batch whose first crud captures and
	 * whose second does not (the entity is gone, the row was deleted meanwhile)
	 * would otherwise look recovered while half its edits were silently dropped —
	 * better a visible rejection than a silent loss.
	 */
	private async captureTextFullState(
		rejections: readonly RejectionInfo[],
		queues: Map<string, Mutation[]>,
	): Promise<{ rows: Map<string, TextResendRow[]>; handled: Set<string> }> {
		const rows = new Map<string, TextResendRow[]>();
		const handled = new Set<string>();
		const targets = rejections.filter((r) => r.errorCode === CodeTextFullStateRequired);
		if (targets.length === 0) return { rows, handled };
		for (const r of targets) {
			const m = queues.get(r.workspaceId)?.find((q) => q.mutationId === r.mutationId);
			const cruds = m?.payload.case === 'crud'
				? [ m.payload.value ]
				: m?.payload.case === 'batch'
				? m.payload.value.mutations
				: [];
			// Staged per mutation, published only once every text-bearing crud in it
			// has been captured — see the all-or-nothing rule above.
			const staged: TextResendRow[] = [];
			let complete = true;
			for (const crud of cruds) {
				const mask = crud.fieldMask?.paths ?? [];
				const paths = this.opts.registry.textPathsOf(crud.entity)
					.filter((p) => mask.includes(p));
				if (paths.length === 0) continue;
				// Pin before anything else: even if the capture below finds nothing,
				// this row must stop shipping delta state.
				this.textFullState.add(fullStateKey(r.workspaceId, crud.entity, crud.key));
				const captured = await this.readTextFields(r.workspaceId, crud.entity, crud.key, paths);
				// Capture runs before the rollback, so local state still holds the
				// rejected edit: a path that reads back nothing means the document is
				// really gone, not merely unchanged.
				if (captured.length < paths.length) {
					complete = false;
					continue;
				}
				staged.push({ entity: crud.entity, key: crud.key, fields: captured });
			}
			if (!complete || staged.length === 0) continue;
			const list = rows.get(r.workspaceId) ?? [];
			list.push(...staged);
			rows.set(r.workspaceId, list);
			handled.add(`${r.workspaceId} ${r.mutationId}`);
		}
		return { rows, handled };
	}

	/**
	 * Reads back the VISIBLE TEXT of every field a `text_state_stale` rejection
	 * touched and stages it for re-application after a re-bootstrap, which it
	 * arms. Runs before the rollback, the only moment the edit still exists.
	 *
	 * The bytes cannot be re-sent — they are precisely what the server refused —
	 * so only the text is carried over, to be re-diffed against whatever the row
	 * holds once this client has read it again. Formatting the stale side applied
	 * on top of characters the server dropped does not survive that.
	 *
	 * Returns the rejections it took ownership of, all-or-nothing per mutation
	 * for the same reason captureTextFullState is: half a recovered batch reads
	 * as recovered while silently losing the rest.
	 */
	private async captureTextStale(
		rejections: readonly RejectionInfo[],
		queues: Map<string, Mutation[]>,
	): Promise<Set<string>> {
		const handled = new Set<string>();
		const targets = rejections.filter((r) => r.errorCode === CodeTextStateStale);
		if (targets.length === 0) return handled;
		for (const r of targets) {
			const m = queues.get(r.workspaceId)?.find((q) => q.mutationId === r.mutationId);
			const cruds = m?.payload.case === 'crud'
				? [ m.payload.value ]
				: m?.payload.case === 'batch'
				? m.payload.value.mutations
				: [];
			const staged: TextRestoreRow[] = [];
			let complete = true;
			for (const crud of cruds) {
				const all = this.opts.registry.textPathsOf(crud.entity);
				// A PUT carries no mask and writes every field it holds; a PATCH is
				// limited to what its mask named.
				const mask = crud.fieldMask?.paths;
				const paths = mask && mask.length > 0 ? all.filter((p) => mask.includes(p)) : all;
				if (paths.length === 0) continue;
				const captured = await this.readTextFields(r.workspaceId, crud.entity, crud.key, paths);
				if (captured.length < paths.length) {
					complete = false;
					continue;
				}
				staged.push({
					entity: crud.entity,
					key: crud.key,
					fields: captured.map((f) => ({ path: f.path, text: textOf(f.value) })),
				});
			}
			if (!complete || staged.length === 0) continue;
			const list = this.textRestores.get(r.workspaceId) ?? [];
			list.push(...staged);
			this.textRestores.set(r.workspaceId, list);
			handled.add(`${r.workspaceId} ${r.mutationId}`);
		}
		if (handled.size > 0) {
			// Nothing can be re-applied against the row this client currently
			// holds — that row is what the server refused. Re-read first.
			this.forceResync = true;
		}
		return handled;
	}

	/**
	 * Re-applies text captured from a `text_state_stale` rejection, now that the
	 * workspace has re-bootstrapped: each field's saved text is diffed against
	 * the document the server actually holds and queued as an ordinary PATCH.
	 *
	 * Minted here rather than through the client's enqueue, like
	 * resendTextFullState: this restores an edit the user already made, so it
	 * pushes nothing onto an undo stack. A restore that cannot be applied (the
	 * row is gone) is dropped rather than retried forever — the row it belonged
	 * to no longer exists.
	 */
	private async applyTextRestores(workspaceId: string): Promise<void> {
		const rows = this.textRestores.get(workspaceId);
		if (rows === undefined || rows.length === 0) return;
		this.textRestores.delete(workspaceId);
		// One lease for the whole batch: every document is read and rewritten
		// inside the transaction below, which is exactly the serialized
		// read-then-edit that lets a writer keep one peer (see text/peer.ts).
		const lease = await (this.peerLeases ??= createPeerLeases(
			this.opts.clientId,
			this.opts.storageNames.textPeerLocks,
		)).acquire();
		const touched = new TouchSet();
		try {
			await this.opts.kvMutex.run(async () => {
				const tx = await this.opts.kv.write();
				try {
					const lastRaw = (await tx.get(metaLastMutationId(workspaceId))) as string | undefined;
					let mutationId = lastRaw ? BigInt(lastRaw) : 0n;
					for (const row of rows) {
						const def = this.opts.registry.get(row.entity);
						if (!def) continue;
						const current = await readMergedRow(
							tx,
							this.opts.registry,
							def.name,
							workspaceId,
							row.key,
						);
						if (current === undefined) continue;
						const merged = fromBinary(def.schema, current);
						const value = create(def.schema);
						const paths: string[] = [];
						for (const field of row.fields) {
							const doc = decodeTextDoc(
								textFieldBytes(def.schema, field.path, merged),
								lease.peer,
							);
							doc.setString(field.text);
							setTextFieldBytes(def.schema, field.path, value, encodeTextDoc(doc));
							paths.push(field.path);
						}
						const crud = create(CrudMutationSchema, {
							entity: row.entity,
							key: row.key,
							op: CrudOp.PATCH,
							value: toBinary(def.schema, value),
							fieldMask: { paths },
						});
						mutationId += 1n;
						await enqueuePending(
							tx,
							workspaceId,
							create(MutationSchema, {
								clientId: this.opts.clientId,
								mutationId,
								payload: { case: 'crud', value: crud },
							}),
						);
						await applyCrudLocally(tx, this.opts.registry, workspaceId, crud, touched);
					}
					await tx.put(metaLastMutationId(workspaceId), mutationId.toString());
					await tx.commit();
				} catch (err) {
					tx.release();
					throw err;
				}
			});
		} finally {
			lease.release();
		}
		await this.opts.notify(touched);
	}

	/** Reads the named text fields out of a row's current merged local state. */
	private async readTextFields(
		workspaceId: string,
		entity: string,
		key: string,
		paths: readonly string[],
	): Promise<{ path: string; value: Uint8Array }[]> {
		const def = this.opts.registry.get(entity);
		if (!def) return [];
		return this.opts.kvMutex.runShared(async () => {
			const tx = await this.opts.kv.read();
			try {
				const bytes = await readMergedRow(tx, this.opts.registry, def.name, workspaceId, key);
				if (bytes === undefined) return [];
				const msg = fromBinary(def.schema, bytes);
				const out: { path: string; value: Uint8Array }[] = [];
				for (const path of paths) {
					const value = textFieldBytes(def.schema, path, msg);
					if (value !== undefined) out.push({ path, value });
				}
				return out;
			} finally {
				tx.release();
			}
		});
	}

	/**
	 * Re-queues captured text documents as ordinary full-state PATCHes. The
	 * mutations are minted here rather than through the client's enqueue on
	 * purpose: they restore an edit the user already made, so they must not push
	 * an undo entry onto any stack, and their inverse would be the one the
	 * rolled-back mutation already carried.
	 */
	private async resendTextFullState(
		workspaceId: string,
		rows: readonly TextResendRow[],
	): Promise<void> {
		const touched = new TouchSet();
		await this.opts.kvMutex.run(async () => {
			const tx = await this.opts.kv.write();
			try {
				const lastRaw = (await tx.get(metaLastMutationId(workspaceId))) as string | undefined;
				let mutationId = lastRaw ? BigInt(lastRaw) : 0n;
				for (const row of rows) {
					const def = this.opts.registry.get(row.entity);
					if (!def) continue;
					const value = create(def.schema);
					for (const field of row.fields) {
						setTextFieldBytes(def.schema, field.path, value, field.value);
					}
					const crud = create(CrudMutationSchema, {
						entity: row.entity,
						key: row.key,
						op: CrudOp.PATCH,
						value: toBinary(def.schema, value),
						fieldMask: { paths: row.fields.map((f) => f.path) },
					});
					mutationId += 1n;
					const mutation = create(MutationSchema, {
						clientId: this.opts.clientId,
						mutationId,
						payload: { case: 'crud', value: crud },
					});
					await enqueuePending(tx, workspaceId, mutation);
					await applyCrudLocally(tx, this.opts.registry, workspaceId, crud, touched);
				}
				await tx.put(metaLastMutationId(workspaceId), mutationId.toString());
				await tx.commit();
			} catch (err) {
				tx.release();
				throw err;
			}
		});
		await this.opts.notify(touched);
		this.syncSoon();
	}

	/** Drops the given pending mutations of a workspace and replays the rest. */
	private async rollbackPending(workspaceId: string, ids: bigint[]): Promise<void> {
		const { touched, pendingCount } = await this.opts.kvMutex.run(() =>
			rollbackRejected(this.opts.kv, this.opts.registry, this.opts.mirrors, workspaceId, ids)
		);
		this.opts.status.setPendingCount(workspaceId, pendingCount);
		await this.opts.notify(touched);
	}

	/**
	 * Counts one failed push of a workspace's queue head and reports whether the
	 * head has now earned a dead-letter — in which case the count is cleared,
	 * because the mutation it named is about to leave the queue.
	 *
	 * signature says what the head failed with; a different one restarts the
	 * count, so only a verdict that repeats unchanged is taken as evidence that
	 * retrying cannot help. isolated says the failed push carried the head alone,
	 * which is the only arrangement in which the failure is attributable to it —
	 * failures counted before isolation still lead up to the threshold, but the
	 * drop itself never fires on one.
	 */
	private countHeadFailure(
		state: WorkspaceSync,
		head: Mutation,
		signature: string,
		isolated: boolean,
	): boolean {
		const prev = state.poison;
		const repeat = isolated && prev?.id === head.mutationId && prev.signature === signature;
		const failures = repeat ? prev.failures + 1 : 1;
		if (repeat && failures >= this.poisonThreshold) {
			state.poison = undefined;
			return true;
		}
		state.poison = { id: head.mutationId, failures, signature };
		return false;
	}

	/**
	 * Drops a mutation whose push the server keeps failing the same way: rolls it
	 * back like a rejection, renumbers the queue onto the id it vacated, and
	 * reports it through onMutationRejected with the client-side poison code, so
	 * the app can observe (and re-issue) it instead of the queue staying wedged
	 * forever.
	 *
	 * All of it or none of it: a store that fails leaves the mutation queued and
	 * the app unnotified, and the backstop counts up to the threshold again.
	 */
	private async deadLetter(
		workspaceId: string,
		m: Mutation,
		failure: string,
	): Promise<void> {
		// Nothing was acked for this id — its push died as an RPC error, or the
		// server refused the whole batch — so the watermark stops one below. Left
		// where they are, the survivors would gap past that watermark and the
		// workspace could never push again, which is why the drop and the renumber
		// share one transaction: a failure between them would leave exactly that
		// queue behind, and the renumber is also the only way back out of it.
		const result = await this.opts.kvMutex.run(() =>
			dropAndResequencePending(
				this.opts.kv,
				this.opts.registry,
				this.opts.mirrors,
				workspaceId,
				m.mutationId,
			)
		);
		this.afterResequence(workspaceId, m.mutationId, result);
		await this.opts.notify(result.touched);
		this.opts.onMutationRejected?.({
			workspaceId,
			mutationId: m.mutationId,
			errorCode: CodePoisonDropped,
			errorMessage: failure,
			...describeMutation(m),
		});
	}

	private async doPull(workspaceId: string, subs: Subscription[]): Promise<void> {
		// The watermark the last page reported, which is what a queue refused for
		// a mutation-id gap has to be renumbered onto once paging is done.
		let lastAcked = 0n;
		const noDeltaEntities = await this.noDeltaEntities();
		for (;;) {
			const { cursor, minSeq } = await this.pullPosition(workspaceId);
			// Timed from issuing the request to the page being committed locally,
			// not just the RPC: what a user waits for is the data being readable,
			// and the rebase is part of that.
			const started = performance.now();
			let resp;
			try {
				resp = await this.rpc.pull(
					{
						workspaceId,
						clientId: this.opts.clientId,
						cursor: cursor ?? new Uint8Array(),
						limit: this.opts.pullLimit ?? 0,
						subscriptions: subs,
						noDeltaEntities,
						minSeq,
					},
					{ signal: this.abort.signal },
				);
			} catch (err) {
				this.emit({
					type: 'pull',
					workspaceId,
					changes: 0,
					hasMore: false,
					reset: false,
					durationMs: performance.now() - started,
					outcome: Coordinator.outcomeOf(err),
				});
				throw err;
			}
			lastAcked = resp.lastAckedMutationId;
			const responded = performance.now();
			const { reset } = await this.applyServerPage(workspaceId, resp, subs);
			this.emit({
				type: 'pull',
				workspaceId,
				changes: resp.patch?.changes.length ?? 0,
				hasMore: resp.hasMore,
				reset,
				durationMs: performance.now() - started,
				requestMs: responded - started,
				applyMs: performance.now() - responded,
				outcome: 'ok',
			});
			// A reset re-bootstrapped instead of paging, and a bootstrap leaves the
			// queue alone: the next cycle's push re-reports any gap.
			if (reset) return;
			if (!resp.hasMore) break;
		}
		await this.recoverGap(workspaceId, lastAcked);
	}

	/**
	 * Applies one forward page of a workspace (from a Pull response or a Subscribe
	 * stream — the shapes are identical) onto the local store: observe the schema
	 * revision, re-bootstrap on reset, otherwise rebase under the stored-seq guard
	 * and notify watchers. Returns `reset` (the caller must stop / reopen) and, on
	 * a reset, whether the re-bootstrap actually `committed` — a superseded
	 * bootstrap left the cursor untouched, so a stream reopening on it would just
	 * reset again. Shared so the stream applies pages through exactly the path
	 * Pull does.
	 */
	private async applyServerPage(
		workspaceId: string,
		// The generated PullResponse, NOT a hand-written subset: a field added to
		// the proto then flows into the stream-apply path automatically, instead of
		// being silently dropped by a structural type that still matches the old
		// shape. Pull and Subscribe stay identical by construction.
		page: PullResponse,
		subs: Subscription[],
	): Promise<{ reset: boolean; committed: boolean }> {
		this.observeServerRevision(page.schemaRevision);
		if (page.reset) {
			const committed = await this.doBootstrap(workspaceId, subs);
			return { reset: true, committed };
		}
		const state = this.workspaces.get(workspaceId);
		let applied;
		try {
			applied = await this.rebasePage(workspaceId, page);
		} catch (err) {
			if (!(err instanceof TextDeltaUnusableError)) throw err;
			// The page's text delta state does not fit this client's confirmed
			// rows, so the page cannot be trusted at all — and neither can the
			// cursor it would have advanced. Nothing was written (the rebase
			// transaction rolled back); a full snapshot is the way back to a
			// state both sides agree on. Treated exactly like a server-sent
			// reset, because it is the same verdict reached one layer later.
			const committed = await this.doBootstrap(workspaceId, subs);
			return { reset: true, committed };
		}
		const { touched, pendingCount, skipped } = applied;
		if (state && page.seq > state.serverSeq) state.serverSeq = page.seq;
		if (!skipped) {
			this.opts.status.setPendingCount(workspaceId, pendingCount);
			// Notify per page rather than once after the final page: each page's
			// rebase is already committed above, so a later page's rebase throwing
			// must not strand watchers (and other tabs) on stale rows for the
			// entities this page changed.
			await this.opts.notify(touched);
		}
		return { reset: false, committed: false };
	}

	/** One page's rebase transaction; see applyServerPage. */
	private async rebasePage(workspaceId: string, page: PullResponse): Promise<RebaseResult> {
		const started = performance.now();
		const before = this.opts.status.pendingCount(workspaceId);
		const result = await this.rebaseTx(workspaceId, page);
		if (!result.skipped) {
			// acked is what left the queue: everything the server confirmed since
			// the last page. replayed is what is still queued and therefore still
			// re-applied over the new server state on every page — the cost that
			// grows when a client writes faster than it can push.
			this.emit({
				type: 'rebase',
				workspaceId,
				acked: Math.max(0, before - result.pendingCount),
				replayed: result.pendingCount,
				durationMs: performance.now() - started,
			});
		}
		return result;
	}

	private rebaseTx(workspaceId: string, page: PullResponse): Promise<RebaseResult> {
		return this.opts.kvMutex.run(() =>
			rebase(this.opts.kv, this.opts.registry, this.opts.mirrors, workspaceId, {
				changes: page.patch?.changes ?? [],
				dropEntities: page.patch?.dropEntities ?? [],
				nextCursor: page.nextCursor,
				lastAckedMutationId: page.lastAckedMutationId,
				// Hold acked mutations queued until the last page: a dropped
				// mutation's confirmed change may land in a later page, and dropping
				// mid-drain would transiently regress its row for direct get/scan
				// calls (and persist that regression across a crash).
				//
				// Held for the same reason while a push of this workspace went
				// unanswered — this path also runs off the live stream, which does
				// not wait for the cycle — because the server's watermark covers
				// mutations whose verdicts the re-push has yet to collect, and a
				// rejection among them would leave with them.
				deferAcked: page.hasMore || this.unresolvedPush.has(workspaceId),
				// Gates the page against the workspace's stored seq inside the rebase
				// transaction: a stale page (a concurrent tab already pulled past
				// it) is dropped whole — rows AND cursor — instead of regressing
				// the shared store.
				serverSeq: page.seq,
			}, this.opts.indexes)
		);
	}

	/**
	 * Reacts to a workspace the server dropped from the stream. It sends nothing
	 * further for THAT workspace — the others keep streaming — so the client
	 * decides per workspace what, if anything, is worth reopening for.
	 *
	 * `reopen` asks the caller to close and reopen the stream, which is the only
	 * way to change a scope set (the browser stream is unidirectional). A
	 * revocation needs no reopen: the scope is simply gone, and reopening on a
	 * revoked answer would be an infinite reconnect. Local data for a revoked
	 * workspace is NOT purged here — deleting a user's rows on one server verdict
	 * is the app's decision (`removeWorkspace(id, { purge: true })`), not a side
	 * effect of a stream frame.
	 */
	private async onWorkspaceDropped(
		workspaceId: string,
		reason: WorkspaceDropped_Reason,
		subs: Subscription[],
	): Promise<{ reopen: boolean; resetCommitted: boolean }> {
		switch (reason) {
			case WorkspaceDropped_Reason.RESET: {
				// The cursor is unusable. Re-bootstrap onto the server's current
				// state, then reopen from the fresh cursor — but only treat the
				// reopen as productive if the bootstrap actually committed, or we
				// would spin open→reset→reopen against the same stale cursor.
				const committed = await this.doBootstrap(workspaceId, subs);
				return { reopen: true, resetCommitted: committed };
			}
			case WorkspaceDropped_Reason.ACCESS_REVOKED:
				this.markRevoked(workspaceId);
				return { reopen: false, resetCommitted: false };
			default:
				// Transient, or a reason this build does not know. Reopen with
				// backoff; the poll loop keeps the store converging meanwhile.
				return { reopen: true, resetCommitted: false };
		}
	}

	/**
	 * Streaming bootstrap of one workspace that keeps local writes flowing. Rows
	 * arrive WITHOUT the kv mutex being held, so user writes and reads interleave
	 * freely; in browsers that means other tabs no longer freeze for the whole
	 * stream. The mutex is taken only twice, each time briefly: once to claim a
	 * generation, once for the final transaction.
	 *
	 * Where those rows land depends on what the workspace already holds, because
	 * the two cases want opposite things:
	 *
	 * - **Re-bootstrap** (a cursor exists, so a whole snapshot is installed):
	 *   rows STAGE under b/<ws>/<gen>/, a keyspace reads never resolve, and one
	 *   swap transaction wipes the confirmed rows and promotes the staged ones.
	 *   Readers see the intact pre-bootstrap state (shadowed by the overlay)
	 *   throughout, then flip atomically — never a half-replaced workspace.
	 * - **Cold bootstrap** (no cursor, so no snapshot is installed): rows go
	 *   STRAIGHT into c/ and become readable as they land. Atomicity here would
	 *   protect nothing — the state it would hold readers on is an empty
	 *   keyspace — while costing a second put and a delete for every row of the
	 *   slowest, most visible case there is. The wipe moves to the claim
	 *   instead: rows without a cursor are the residue of an interrupted
	 *   attempt, never a snapshot, and mixing two snapshots under one cursor
	 *   would be silent divergence rather than a slow start. Each chunk is a
	 *   transaction of its own outside the mutex, so these rows can afford to be
	 *   written INDEXED, and the install claims the index-built markers over a
	 *   snapshot it wrote entirely itself.
	 *
	 * Only this workspace's rows are touched either way; the other workspaces in
	 * the store are untouched throughout.
	 *
	 * Concurrency: two tabs sharing the store can bootstrap the same workspace at
	 * once. Each claims a distinct generation, and the final transaction commits
	 * only if its generation is still current, so a superseded staged bootstrap
	 * discards its staging instead of installing a half-snapshot. A cold
	 * bootstrap cannot discard rows it has already written into c/, so it
	 * re-checks the generation INSIDE every chunk transaction and stops writing
	 * the moment it is superseded. The two transactions serialize, so each of its
	 * chunks either lands before the winner's claim — which wipes it — or sees
	 * the bumped generation and never lands at all.
	 *
	 * A failed attempt is not thrown away. What it staged stays where it is, and
	 * the resume record it left (metaBootstrapResume) lets the next attempt ask
	 * the server to continue that same snapshot — which is what keeps a large
	 * workspace on a flaky link from starting over every time the connection
	 * drops. The record is only adoptable while the workspace has not bootstrapped
	 * since, so an adopted snapshot is always one this store still holds intact.
	 *
	 * Returns true if this bootstrap committed, false if it was superseded.
	 */
	private async doBootstrap(workspaceId: string, subs: Subscription[]): Promise<boolean> {
		// What to re-scan once this is over. A cold bootstrap fills it as it goes,
		// since its rows are visible before the final transaction; the staged path
		// fills it in the swap.
		const touched = new TouchSet();
		// 1. Claim a generation. Bumping the marker under the mutex makes a later
		//    bootstrap supersede this one, which the chunk loop and the final
		//    transaction both detect. Bumping it also supersedes every EARLIER
		//    generation of THIS workspace, so this is the moment to GC their
		//    staging: a mid-stream crash or abort leaves a partial snapshot behind,
		//    and the only other cleanup is a *successful* swap — so a large
		//    workspace on a flaky connection that fails attempt after attempt would
		//    otherwise pile one partial snapshot per try until one finally lands.
		//    Sweeping this workspace's staging root here is safe: the generation we
		//    just claimed has nothing staged yet, and every existing generation of
		//    it is now superseded and will discard itself at its own swap. A
		//    concurrent tab still streaming an older generation simply re-creates
		//    and re-discards its rows, so the garbage stays bounded to the streams
		//    in flight. Staging is workspace-partitioned, so a concurrent bootstrap
		//    of a DIFFERENT workspace is never swept.
		//
		//    Unless there is an interrupted snapshot to ADOPT: a resume record left
		//    by an earlier attempt whose generation is still the current one, which
		//    is exactly the proof that nothing has bootstrapped this workspace
		//    since — so what it staged is still the partial snapshot it describes.
		//    That attempt's rows are then kept (no sweep, no wipe) and this one
		//    asks the server to continue where it stopped. Adoption re-stamps the
		//    record with the generation claimed here, so it stays adoptable if this
		//    attempt also fails, and so the NEXT claim can tell "still the same
		//    lineage" from "someone else bootstrapped in between". Two tabs cannot
		//    adopt the same record and both install: adoption bumps the marker
		//    inside this transaction, and every install is gated on it.
		const claim = await this.opts.kvMutex.run(
			async () => {
				const tx = await this.opts.kv.write();
				try {
					const current = ((await tx.get(metaBootstrapGen(workspaceId))) as number | undefined) ?? 0;
					const next = current + 1;
					await tx.put(metaBootstrapGen(workspaceId), next);
					// No cursor means no installed snapshot, which is what makes writing
					// into c/ legal (see the contract above). Decided under the mutex,
					// in the transaction that claims the generation, so two tabs cannot
					// both answer "cold" and both start writing: the loser's next chunk
					// sees the generation this one bumped.
					const direct = !(await tx.has(metaCursor(workspaceId)));
					// The entity set every chunk of this snapshot is filtered against,
					// fixed here instead of re-read per chunk: a chunk that read it from
					// its own transaction could not decode a row until the PREVIOUS
					// chunk's commit had finished, which is exactly the serialization
					// the loop below is shaped to avoid. Reading it once loses nothing a
					// per-chunk read would keep — an entity registered mid-snapshot is
					// already missing from the chunks that arrived before it, and that
					// registration invalidates this workspace's cursor either way.
					const syncable = await syncableEntities(tx, this.opts.registry);
					const record = (await tx.get(metaBootstrapResume(workspaceId))) as
						| BootstrapResumeRecord
						| undefined;
					// The cold/staged decision has to still hold as well: a record from
					// a cold attempt describes rows in c/, and adopting it onto the
					// staged path would install a snapshot that is half in each place.
					const adopt = record !== undefined && record.gen === current
						&& record.direct === direct;
					if (adopt) {
						await tx.put(metaBootstrapResume(workspaceId), { ...record, gen: next });
					} else {
						// The wipe a bootstrap would otherwise do at the end, done before
						// this one writes anything instead. It clears the rows, index
						// entries and built markers of an interrupted earlier attempt —
						// all of which describe a snapshot that was never installed — so
						// what streams in now cannot inherit a row the server has since
						// deleted, and no marker outlives the rows it described. It is
						// also what leaves every c/ row of this workspace one this
						// attempt wrote, which the marker claim at the install rests on.
						if (direct) await wipeConfirmedState(tx, this.opts.registry, workspaceId, touched);
						await clearStaging(tx, stagingWsRoot(workspaceId));
						await tx.del(metaBootstrapResume(workspaceId));
					}
					await tx.commit();
					return {
						gen: next,
						snapshotGen: adopt ? record.snapshotGen : next,
						direct,
						syncable,
						resume: adopt ? { cursor: record.cursor, token: record.token } : undefined,
					};
				} catch (err) {
					tx.release();
					throw err;
				}
			},
		);
		const { gen, snapshotGen: claimedSnapshotGen, direct, resume, syncable } = claim;
		// The generation whose staging prefix holds this snapshot. It trails `gen`
		// on an adopted snapshot — the staged rows keep the prefix they were
		// written under, since moving them would cost the copy the adoption exists
		// to avoid — and is reset to `gen` if the server refuses to resume.
		let snapshotGen = claimedSnapshotGen;
		let resuming = resume !== undefined;
		// From here on the cold path's rows are visible as they land, so readers
		// need to know which entities have not arrived. Started before the stream
		// opens — the workspace is at its emptiest right now — with every entity
		// in scope pending until Begin says what is actually coming.
		if (direct) this.beginHydration(workspaceId, subs);

		// 2. Stream the snapshot in outside the mutex, one transaction per chunk.
		//    A chunk's transaction is handed to the store and the NEXT chunk is
		//    decoded while it commits, so the two halves of the work — this
		//    thread's decoding and the store's writing — overlap instead of taking
		//    turns. On the staged path the cursor and confirmed state stay
		//    untouched, so a crash here just leaves stale staging the next claim
		//    garbage-collects; on the cold path it leaves rows with no cursor,
		//    which is the state that asks for a bootstrap in the first place.
		let cursor: Uint8Array | undefined;
		let lastAcked = 0n;
		let snapshotSeq = 0n;
		let ended = false;
		let superseded = false;
		// A bootstrap re-downloads the whole workspace, so its size and duration
		// are what a re-bootstrap actually costs a user — the number that says
		// whether the server's GC retention is set correctly for how long these
		// clients stay offline. The phases are timed separately (see
		// BootstrapEvent): a slow link and a slow store both show up here, and
		// only split apart do they say which one it was.
		const bootStarted = performance.now();
		let rows = 0;
		// Time the loop spent waiting for the server's next frame; what it spent on
		// the frames instead is taken by subtraction once the stream is done.
		let streamMs = 0;
		// Chunks handed to the store, oldest first.
		const inflight: BootstrapChunkCommit[] = [];
		// Applies what the oldest chunks carried — their rows and the entities they
		// completed — until at most `keep` are still outstanding, waiting for each
		// commit in turn. Every one of them is awaited even after one has failed,
		// since leaving a transaction unheard-of would strand its result; the first
		// failure is the one reported.
		const settleChunks = async (keep: number): Promise<void> => {
			let failure: { err: unknown } | undefined;
			while (inflight.length > keep) {
				const chunk = inflight.shift()!;
				const failed = await chunk.settled;
				if (failed !== undefined) {
					failure ??= failed;
					continue;
				}
				if (failure !== undefined) continue;
				rows += chunk.rows;
				// Committed, so whatever this chunk finished is now readable.
				if (direct) {
					try {
						await this.completeHydrated(workspaceId, chunk.entities);
					} catch (err) {
						failure = { err };
					}
				}
			}
			if (failure !== undefined) throw failure.err;
		};
		// Waits out every transaction still in flight without letting any of them
		// speak: this runs on the way out with a failure already in hand.
		const abandonChunks = async (): Promise<void> => {
			await Promise.all(inflight.splice(0).map((chunk) => chunk.settled));
		};
		let frameStarted = performance.now();
		try {
			for await (
				const msg of this.rpc.bootstrap(
					{
						workspaceId,
						clientId: this.opts.clientId,
						subscriptions: subs,
						priorityEntities: this.opts.priorityEntities,
						resume,
					},
					{ signal: this.abort.signal },
				)
			) {
				streamMs += performance.now() - frameStarted;
				switch (msg.body.case) {
					case 'begin':
						cursor = msg.body.value.cursor;
						lastAcked = msg.body.value.lastAckedMutationId;
						snapshotSeq = msg.body.value.seq;
						this.observeServerRevision(msg.body.value.schemaRevision);
						if (direct) {
							this.hydration.order(workspaceId, msg.body.value.entities);
							this.publishHydration(workspaceId);
						}
						// A resume the server would not honour (the scope changed, the
						// cursor fell behind its GC horizon, the store is a different
						// one): what follows is a NEW snapshot, so the rows the adopted
						// attempt left are not part of it and must not be mixed in.
						// This attempt stages under its own generation from here.
						if (resuming && !msg.body.value.resumed) {
							resuming = false;
							snapshotGen = gen;
							if (!(await this.discardResumedSnapshot(workspaceId, gen, direct, touched))) {
								superseded = true;
							}
						}
						break;
					case 'chunk': {
						if (cursor === undefined) throw new Error('bootstrap chunk before Begin');
						const tx = await this.opts.kv.write();
						try {
							// The generation check the staged path can leave to the swap:
							// these rows are visible the moment they commit, so a
							// superseded cold bootstrap has to stop writing rather than
							// discard afterwards. Read from the transaction that writes,
							// which is what makes the two orderings against the winner's
							// claim exhaustive (see the contract above) — and asked for
							// here, before the rows below are decoded, rather than awaited
							// here: the store answers it behind the previous chunk's
							// commit, so a check that waited for its answer first would
							// serialize this chunk's decoding against that commit. The
							// verdict still gates every write, since the transaction is
							// handed over only after it comes back.
							const current = direct ? this.stillCurrent(tx, workspaceId, gen) : undefined;
							// Awaited below, which is where a failure is reported. This
							// only keeps one that never gets there — the staging under it
							// threw first — from surfacing as an unhandled rejection.
							void current?.catch(() => {});
							if (direct) {
								// Indexed as they land. The chunk holds a transaction of
								// its own and no store mutex, so the decode this costs is
								// off the swap's critical path — which is what lets the
								// install claim the markers rather than leaving the
								// workspace scanning until a background walk finishes.
								await applyBootstrapRows(
									tx,
									this.opts.registry,
									workspaceId,
									msg.body.value.rows,
									touched,
									this.opts.indexes,
									syncable,
								);
							} else {
								await stageServerChanges(
									tx,
									this.opts.registry,
									workspaceId,
									snapshotGen,
									msg.body.value.rows,
									syncable,
								);
							}
							// Durable in the same transaction as the rows it accounts
							// for, so the record can never claim more than the store
							// holds. An empty token is a server that cannot resume:
							// nothing is recorded and the next attempt starts over.
							if (msg.body.value.resumeToken.length > 0) {
								await tx.put(
									metaBootstrapResume(workspaceId),
									{
										gen,
										snapshotGen,
										direct,
										cursor,
										token: msg.body.value.resumeToken,
									} satisfies BootstrapResumeRecord,
								);
							}
							superseded = current !== undefined && !(await current);
							if (superseded) {
								tx.release();
							} else {
								// Room for this one first: the queue is what bounds how far
								// ahead of the store the loop may run.
								await settleChunks(MAX_INFLIGHT_BOOTSTRAP_COMMITS - 1);
								inflight.push({
									settled: tx.commit().then(
										() => undefined,
										(err: unknown) => ({ err }),
									),
									rows: msg.body.value.rows.length,
									entities: msg.body.value.rows.map((r) => r.entity),
								});
							}
						} catch (err) {
							tx.release();
							throw err;
						}
						break;
					}
					case 'end':
						ended = true;
						break;
				}
				frameStarted = performance.now();
				// Leaving the loop closes the stream: there is nothing left to write,
				// and the rows already downloaded belong to a snapshot that lost.
				if (superseded) break;
			}
			streamMs += performance.now() - frameStarted;
			// Nothing may still be in flight past here: the install below has to
			// observe every row this snapshot wrote.
			await settleChunks(0);
		} catch (err) {
			// Whatever is still with the store is left to finish on its own — the
			// failure being reported is the one that got here first, and an
			// abandoned transaction would otherwise report a second time with
			// nobody listening.
			await abandonChunks();
			// A cold bootstrap has already made rows visible and wiped what was
			// there before, so watchers have to hear about it even though the
			// snapshot never completed. The store is consistent either way — rows
			// with no cursor re-bootstrap — but a watcher left on its pre-wipe
			// snapshot would paint rows the store no longer holds until the retry
			// lands, which may be a long time on the connection that just failed.
			if (direct) await this.notifyAfterFailure(touched);
			throw err;
		}
		// A bootstrap that stopped early because it lost the workspace is allowed
		// to have neither frame; anything else that ends without them is a server
		// this client cannot install a snapshot from.
		if (!superseded && (cursor === undefined || !ended)) {
			throw new Error('bootstrap stream ended without Begin/End');
		}
		// The check above is what makes this defined for every bootstrap that gets
		// as far as installing; a const copy is what carries that into the closure.
		const snapshotCursor = cursor;
		// Whatever the stream loop spent NOT waiting for the server: decoding the
		// frames into transactions, and blocking on a commit when it had run as far
		// ahead of the store as it may. Chunks are written while the ones before
		// them are still committing, so this is not a phase that follows streamMs —
		// the two split the loop's wall time between them.
		const stageMs = performance.now() - bootStarted - streamMs;

		// 3. Install. One transaction under the mutex: on the staged path wipe this
		//    workspace's confirmed rows + cursor and promote the staged snapshot;
		//    on the cold path the rows are already where they belong and the wipe
		//    happened at the claim. Either way it writes the cursor, GCs staging and
		//    rebases the surviving pending mutations onto the fresh base. Only this
		//    commit makes the bootstrap durable, and a crash before it leaves a
		//    workspace that re-bootstraps: the old cursor and state intact on the
		//    staged path, no cursor at all on the cold one. If a newer generation
		//    was claimed meanwhile, discard ours and let it win.
		let pendingCount = 0;
		let swapWrites = 0;
		// Whether the install may claim the index-built markers, which is only true
		// when this attempt wrote every row of the snapshot itself under the current
		// specs. A RESUMED snapshot fails that: the rows the adopted attempt left
		// are part of what installs here, and this one cannot say what specs — or
		// what client build — wrote them. It leaves the markers to the build pass,
		// which walks the whole entity and earns them.
		const claimIndexes = direct && this.opts.indexes !== undefined && !resuming;
		// From before the mutex, not from inside it: a swap queued behind another
		// tab's write is time this bootstrap spent, and hiding it would report a
		// contended store as a fast one.
		const swapStarted = performance.now();
		// A bootstrap the chunk loop already found superseded has nothing to
		// install and takes no lock; the generation that displaced it owns the
		// workspace, including the rows this one had written.
		const committed = superseded || snapshotCursor === undefined
			? false
			: await this.opts.kvMutex.run(async () => {
				const inner = await this.opts.kv.write();
				// Counted so the event can report what the snapshot cost in KV entries
				// rather than only in rows — the two differ by whatever this
				// transaction writes per row, which is the thing worth watching.
				const tx = new CountingWrite(inner);
				try {
					const active = ((await tx.get(metaBootstrapGen(workspaceId))) as number | undefined) ?? 0;
					if (active !== gen) {
						// Only a prefix nothing else can be writing is swept here. An
						// adopted snapshot's prefix is shared with whoever adopted it
						// next, and clearing it would throw away the winner's rows;
						// leaving it costs one stale prefix, which the next claim's
						// sweep — or the next successful swap — takes.
						if (snapshotGen === gen) await clearStaging(tx, stagingPrefix(workspaceId, gen));
						await tx.commit();
						swapWrites = tx.writes;
						return false;
					}
					if (!direct) {
						await wipeConfirmedState(tx, this.opts.registry, workspaceId, touched);
						// Rows only: the staged snapshot is NOT indexed inside this
						// transaction. A staged record carries no positions, so giving it
						// some means decoding every row of the snapshot here, in the one
						// transaction that must commit before anything is readable. The
						// wipe already took the built markers, so promoting them
						// positionless is a state readers already handle: every query
						// scans until a marker reappears, and onIndexesStale below is
						// what makes one reappear.
						await promoteStaged(tx, workspaceId, snapshotGen, touched);
					} else if (claimIndexes) {
						// Every c/ row of this workspace was written by THIS attempt,
						// carrying the positions its current specs encode to: the claim
						// at the top wiped what was there, the chunk loop stops the
						// moment another generation takes the workspace, and nothing else
						// writes c/ during a cold bootstrap (pulls need a cursor, local
						// writes go to the overlay). That makes the markers true here,
						// which spares the workspace a walk of its whole snapshot before
						// its first indexed query.
						await markIndexesBuilt(
							tx,
							this.opts.indexes!,
							workspaceId,
							this.opts.registry.names(),
						);
					}
					// The generation this snapshot installs, for the build pass to
					// guard its walk against (see keys.metaSnapshotGen). Written even
					// when there are no indexes: a later build must not read a stale
					// value left by an earlier layout.
					await tx.put(metaSnapshotGen(workspaceId), gen);
					await clearStaging(tx, stagingWsRoot(workspaceId));
					// The snapshot is installed, so there is nothing left to resume;
					// leaving the record would offer a later attempt a position inside
					// a snapshot whose staging this transaction just swept.
					await tx.del(metaBootstrapResume(workspaceId));
					pendingCount = await finalizeRebase(
						tx,
						this.opts.registry,
						this.opts.mirrors,
						workspaceId,
						{
							nextCursor: snapshotCursor,
							lastAckedMutationId: lastAcked,
							serverSeq: snapshotSeq,
							baseline: true,
						},
						touched,
					);
					await tx.commit();
					swapWrites = tx.writes;
					return true;
				} catch (err) {
					tx.release();
					throw err;
				}
			});
		const swapMs = performance.now() - swapStarted;

		// Reported for the superseded case too, with outcome "ok": the rows really
		// were downloaded and the time really was spent, and hiding that would
		// understate what concurrent bootstraps cost.
		this.emit({
			type: 'bootstrap',
			workspaceId,
			rows,
			resumed: resuming,
			durationMs: performance.now() - bootStarted,
			streamMs,
			stageMs,
			swapMs,
			swapWrites,
			outcome: 'ok',
		});
		if (!committed) {
			// The rows a superseded cold bootstrap wrote were wiped by the claim that
			// displaced it, and the wipe at its OWN claim removed whatever was there
			// before — both invisible to watchers until someone says so. The winner
			// notifies as well; one redundant re-scan is the cheaper mistake.
			if (direct) await this.notifyAfterFailure(touched);
			return false;
		}
		// A bootstrap establishes a fresh baseline, authoritative even when it
		// moves the in-memory seq BACKWARDS: after a store swap/recreate the new
		// snapshot seq sits below the stale one, and clamping to the max would
		// keep the stream loop (which skips positions <= serverSeq) ignoring every
		// page from the new store until it climbed past the old high-water mark.
		const state = this.workspaces.get(workspaceId);
		if (state) state.serverSeq = snapshotSeq;
		this.opts.status.setPendingCount(workspaceId, pendingCount);
		// Nothing is outstanding any more, and the notify below re-emits every
		// watcher this bootstrap touched — which is where they pick up the
		// coverage this drops. Done before it, so no watcher fires on a snapshot
		// that is complete while its coverage still says it is arriving.
		this.hydration.finish(workspaceId);
		this.publishHydration(workspaceId);
		await this.opts.notify(touched);
		// Asked for unconditionally, including after an install that claimed the
		// markers itself. Over a claimed workspace the pass walks nothing — one key
		// lookup per index — and it is also what verifies the claim and emits the
		// `indexing` event readers wait on. Where the markers were NOT claimed
		// (a staged swap, a resumed snapshot) it is the walk that ends the
		// scanning, and it has to happen here rather than only at start(): a
		// mid-session re-bootstrap would otherwise leave the workspace scanning for
		// the rest of the session.
		this.opts.onIndexesStale?.(workspaceId);
		return true;
	}

	/**
	 * Entities of a workspace whose first-snapshot rows are still arriving, for
	 * the coverage layer. Absent for every workspace holding a snapshot, which
	 * after start-up is all of them.
	 */
	hydrationOf(workspaceId: string): ReadonlySet<string> | undefined {
		return this.hydration.pendingOf(workspaceId);
	}

	/**
	 * Whether this tab currently holds the account's stream leadership — the
	 * current-state counterpart of the LeadershipEvent stream. False wherever
	 * elections do not run (server runtimes, no Web Locks), including for a tab
	 * that holds a session-long stream of its own.
	 */
	isLeader(): boolean {
		return this.leaderNow;
	}

	/**
	 * Starts reporting a workspace as hydrating, before its first row lands.
	 *
	 * The seed is everything this tab's own scope names that can sync at all —
	 * Begin replaces it with what the server will actually ship. A full-sync
	 * client cannot narrow that to the entities the SERVER holds, and must not:
	 * an entity it does not know about cannot be read locally anyway, and one the
	 * server drops from the snapshot completes when the next entity's rows arrive.
	 */
	private beginHydration(workspaceId: string, subs: Subscription[]): void {
		const scoped = subs.length === 0
			? this.opts.registry.syncableNames()
			: [ ...new Set(subs.map((s) => s.entity)) ];
		this.hydration.begin(workspaceId, scoped);
		this.publishHydration(workspaceId);
	}

	/**
	 * Records the entities a committed bootstrap chunk finished, and re-emits
	 * their watchers.
	 *
	 * The re-emit is what turns "the rows are in the store" into "the UI shows
	 * them": a cold bootstrap otherwise notifies only once, at the end, which
	 * would leave every watcher on the empty view for the whole download and
	 * make progressive delivery invisible to anything reactive. Entity
	 * completion is the granularity because a watcher re-reads its whole scan
	 * scope per notification — per chunk that would re-scan a growing entity
	 * once per chunk, which costs more than the snapshot itself.
	 */
	private async completeHydrated(workspaceId: string, entities: readonly string[]): Promise<void> {
		const completed = this.hydration.advance(workspaceId, entities);
		if (completed.length === 0) return;
		this.publishHydration(workspaceId);
		const touched = new TouchSet();
		// Whole entities: a snapshot just wrote all of one, so every watcher on it
		// re-scans regardless of which rows are named.
		for (const entity of completed) touched.entity(workspaceId, entity);
		await this.opts.notify(touched);
	}

	/**
	 * Stops reporting a workspace as hydrating and re-emits whatever was still
	 * outstanding — the workspace turned out to hold a snapshot after all (another
	 * tab's bootstrap installed one, or this one never needed a cold start).
	 */
	private async finishHydration(workspaceId: string): Promise<void> {
		const completed = this.hydration.finish(workspaceId);
		if (completed.length === 0) return;
		this.publishHydration(workspaceId);
		const touched = new TouchSet();
		for (const entity of completed) touched.entity(workspaceId, entity);
		await this.opts.notify(touched);
	}

	/** Mirrors the tracker's view of one workspace onto the status surface. */
	private publishHydration(workspaceId: string): void {
		this.opts.status.updateWorkspace(workspaceId, {
			hydration: this.hydration.infoOf(workspaceId),
		});
	}

	/**
	 * Whether `gen` is still the workspace's current bootstrap generation, read
	 * inside a caller's transaction. False means a later bootstrap claimed the
	 * workspace and this one no longer owns what it writes.
	 */
	private async stillCurrent(tx: KVRead, workspaceId: string, gen: number): Promise<boolean> {
		const active = ((await tx.get(metaBootstrapGen(workspaceId))) as number | undefined) ?? 0;
		return active === gen;
	}

	/**
	 * Throws away an adopted partial snapshot mid-stream, for the one case the
	 * claim cannot decide: the server refused to resume, so the chunks that
	 * follow belong to a different snapshot. This is the claim's own cleanup,
	 * deferred to the moment the answer arrived — the staged rows go, a cold
	 * attempt's visible rows go with them, and the resume record goes because it
	 * describes a snapshot that no longer exists anywhere. Leaving the record
	 * would offer the next attempt a position into rows this transaction swept.
	 *
	 * Under the mutex and gated on the generation, like the claim. Returns false
	 * if a later bootstrap has taken the workspace, in which case nothing is
	 * touched — that state is the winner's business now.
	 */
	private async discardResumedSnapshot(
		workspaceId: string,
		gen: number,
		direct: boolean,
		touched: TouchSet,
	): Promise<boolean> {
		return this.opts.kvMutex.run(async () => {
			const tx = await this.opts.kv.write();
			try {
				if (!(await this.stillCurrent(tx, workspaceId, gen))) {
					tx.release();
					return false;
				}
				if (direct) await wipeConfirmedState(tx, this.opts.registry, workspaceId, touched);
				await clearStaging(tx, stagingWsRoot(workspaceId));
				await tx.del(metaBootstrapResume(workspaceId));
				await tx.commit();
				return true;
			} catch (err) {
				tx.release();
				throw err;
			}
		});
	}

	/**
	 * Notifies watchers about writes that are already committed, on a path that is
	 * on its way out with a more interesting failure. A broken watcher must not
	 * replace the error that got us here, so it is reported rather than thrown.
	 */
	private async notifyAfterFailure(touched: TouchSet): Promise<void> {
		if (touched.isEmpty()) return;
		try {
			await this.opts.notify(touched);
		} catch (err) {
			reportError('bootstrap could not notify watchers about a snapshot it abandoned', err);
		}
	}

	/**
	 * The leader's live change stream (one leadership term). ONE stream carries
	 * every workspace: it catches each scope up from that workspace's cursor, then
	 * applies the rows the server pushes down directly — eliminating the
	 * notify→pull round trip — so a client's connection count is independent of
	 * how many workspaces it holds. Each page goes through the SAME path Pull does
	 * (applyServerPage: rebase under the stored-seq guard + notify + changeshare),
	 * so followers converge through the shared store with no stream of their own.
	 * The stream reopens on a scope (union) change — the browser stream is
	 * unidirectional — and on error/close with backoff. The term signal ends the
	 * loop when the tab hides or the coordinator stops.
	 */
	private async subscribeLoop(termSignal: AbortSignal): Promise<void> {
		const backoff = new Backoff();
		while (!this.stopped && !termSignal.aborted) {
			// Snapshot the cursors + scope union ON the chain. The union mirrors the
			// per-cycle sync (every tab presents the same scope); the chain ordering
			// matters because a forceResync bootstrap queued ahead of us rewrites the
			// cursors — reading them off the chain could open the stream from a stale
			// position the shared store already moved past (wasted catch-up the
			// server-seq guard then discards). Only this brief read is chained; page
			// application below is not.
			let planned;
			try {
				planned = await this.run(() => this.planStream());
			} catch (err) {
				// Reading the cursors/union failed (a broken store, a peer's record we
				// could not scan). Nothing to open a stream from, but the loop must
				// survive it: dying here leaves the tab on the poll floor for the rest
				// of the session — and, when this tab holds leadership, every other tab
				// with it.
				reportError('subscribe stream could not read its scope; retrying', err);
				this.opts.status.update({ online: false });
				await sleep(backoff.next(), termSignal);
				continue;
			}
			if (planned.scopes.length === 0) {
				// Nothing to stream (every workspace removed or revoked). Idle until a
				// workspace shows up; reopenSubscribe interrupts the wait.
				const idle = new AbortController();
				this.streamIdleAbort = idle;
				const onIdleStop = (): void => idle.abort();
				termSignal.addEventListener('abort', onIdleStop);
				try {
					await sleep(IDLE_STREAM_RECHECK_MS, idle.signal);
				} finally {
					termSignal.removeEventListener('abort', onIdleStop);
					this.streamIdleAbort = undefined;
				}
				continue;
			}

			const streamAbort = new AbortController();
			this.subscribeStreamAbort = streamAbort;
			const onStop = (): void => streamAbort.abort();
			termSignal.addEventListener('abort', onStop);
			this.abort.signal.addEventListener('abort', onStop);
			let immediate = false;
			// Flipped once the stream proves healthy — a caught_up marker or a real
			// applied page. Gates the on-connect push flush, and (with the open
			// duration below) the backoff reset.
			let healthy = false;
			const openedAt = Date.now();
			// Set when a dropped scope re-bootstrapped AND that bootstrap committed a
			// fresh cursor: the reopen is then productive and may skip backoff. A
			// superseded bootstrap leaves it false, so the reopen backs off instead of
			// spinning open→reset→reopen against the same stale cursor.
			let resetCommitted = false;
			// Reported on the transition to healthy rather than on the attempt: a
			// stream that never connects would otherwise emit an open/closed pair
			// per retry, making a flapping connection look like a working one.
			let reportedOpen = false;
			let streamError: unknown;
			try {
				const markHealthy = (): void => {
					if (healthy) return;
					healthy = true;
					reportedOpen = true;
					this.emit({ type: 'stream', state: 'open' });
					this.opts.status.update({ online: true });
					// The stream is read-only, so flush the pending queues on every
					// (re)connect: a leader that reconnected with queued local writes
					// (server had been unreachable) would otherwise never push them
					// until an unrelated syncSoon trigger.
					this.syncSoon();
				};
				for await (
					const msg of this.rpc.subscribe(
						{
							clientId: this.opts.clientId,
							scopes: planned.scopes,
							limit: this.opts.pullLimit ?? 0,
							noDeltaEntities: planned.noDeltaEntities,
							// A frame without a cursor leaves the stored one in
							// place (see FinalizeInput.nextCursor), so the server
							// can skip restating a membership-carrying cursor on
							// every frame and send it as a checkpoint instead.
							sparseCursor: true,
						},
						{ signal: streamAbort.signal },
					)
				) {
					// Carried on every frame, heartbeats included, so a connected
					// client learns of a rolling deploy without waiting for a page.
					this.observeServerRevision(msg.schemaRevision);
					const body = msg.body;
					const ws = msg.workspaceId;
					if (body.case === 'page') {
						// Apply OFF the chain: the rebase already serializes through the
						// kv mutex under the stored-seq guard, so it needs no
						// operation-level ordering. Routing it through this.run would
						// couple the realtime consumer to a long bootstrap/compaction —
						// server-side Send blocks on backpressure and the live tail
						// stalls behind that op (head-of-line blocking).
						this.emit({
							type: 'poke',
							workspaceId: ws,
							changes: body.value.patch?.changes.length ?? 0,
						});
						const { reset, committed } = await this.applyServerPage(
							ws,
							body.value,
							planned.subs.get(ws) ?? [],
						);
						if (reset) {
							// The stream never sets reset (a reset arrives as a dropped
							// frame); handled defensively so a server that does send one
							// still recovers rather than looping on a dead cursor.
							resetCommitted = committed;
							break;
						}
						markHealthy();
					} else if (body.case === 'caughtUp') {
						// Backlog drained for this workspace: it is current and its live
						// tail follows. Other scopes may still be catching up.
						markHealthy();
					} else if (body.case === 'heartbeat') {
						// Content-free liveness keepalive: prove the connection is warm
						// without a rebase/notify.
						this.opts.status.update({ online: true });
					} else if (body.case === 'dropped') {
						// The server has stopped streaming THIS workspace and will send
						// nothing further for it; the rest of the stream lives on. The
						// reason decides whether reopening buys anything.
						const dropped = await this.onWorkspaceDropped(
							ws,
							body.value.reason,
							planned.subs.get(ws) ?? [],
						);
						if (dropped.reopen) {
							resetCommitted = dropped.resetCommitted;
							break;
						}
						// Revoked: keep consuming the remaining scopes. If that was the
						// last one, fall out and let the loop idle or end.
						planned.scopes = planned.scopes.filter((s) => s.workspaceId !== ws);
						if (planned.scopes.length === 0) break;
					}
				}
			} catch (err) {
				// A dropped connection (ConnectError) or our own cancellation is the
				// expected way out, and the reconnect below handles it. Anything else
				// came from OUR side of the loop — a rebase that threw, an unusable
				// frame — and reconnecting cannot fix it: report it rather than let the
				// retry disguise a client defect as a flaky server.
				if (!streamAbort.signal.aborted && !(err instanceof ConnectError)) {
					reportError('subscribe stream failed', err);
				}
				streamError = err;
			} finally {
				// Tear the connection down promptly however the loop exited (clean
				// end, reset break, or throw); the next iteration opens a fresh one.
				streamAbort.abort();
				termSignal.removeEventListener('abort', onStop);
				this.abort.signal.removeEventListener('abort', onStop);
				this.subscribeStreamAbort = undefined;
				// Paired with the open above, so a listener can measure how long
				// streams survive — which is what separates "connected" from
				// "reconnecting every few seconds".
				if (reportedOpen) {
					this.emit({
						type: 'stream',
						state: 'closed',
						reason: streamError instanceof Error ? streamError.message : undefined,
					});
				}
			}
			// Only a stream that both worked AND lasted clears the backoff: see
			// STREAM_STABLE_MS. A short-lived one leaves the attempt counter climbing,
			// so a flapping peer is reconnected to ever less often instead of forever
			// at the floor.
			if (healthy && Date.now() - openedAt >= STREAM_STABLE_MS) backoff.reset();
			// Read-and-clear the deliberate-reopen flag BEFORE the early-out, so a
			// term ending (tab hidden / stop) while it is latched cannot leak it into
			// a later term, where a benign first close would wrongly skip backoff.
			const deliberate = this.subscribeReopen;
			this.subscribeReopen = false;
			if (this.stopped || termSignal.aborted) return;
			// Reopen at once on a deliberate re-scope (setSubscriptions, add/remove
			// workspace), or on a dropped scope whose re-bootstrap committed a fresh
			// cursor — productive, not a spin. A superseded/failed bootstrap, or any
			// error/close, falls through to backoff, so a persistently-resetting
			// server cannot spin open→reset→reopen with zero delay.
			if (deliberate || resetCommitted) {
				immediate = true;
			}
			if (!immediate) {
				this.opts.status.update({ online: false });
				await sleep(backoff.next(), termSignal);
			}
		}
	}

	/**
	 * Builds the scope set for one Subscribe stream: every syncable workspace in
	 * the union, each with its shared cursor and the union subscription set. Capped
	 * at maxWorkspacesPerRequest — a client past the server's limit streams the
	 * first N and leaves the rest to the poll loop, which is slower but correct.
	 */
	private async planStream(): Promise<{
		scopes: {
			workspaceId: string;
			cursor: Uint8Array;
			subscriptions: Subscription[];
			minSeq: bigint;
		}[];
		subs: Map<string, Subscription[]>;
		noDeltaEntities: string[];
	}> {
		const own = this.ownScopes();
		const union = this.opts.intent ? await this.opts.intent.unionScopes(own) : own;
		this.reportScopes(union);
		const scopes: {
			workspaceId: string;
			cursor: Uint8Array;
			subscriptions: Subscription[];
			minSeq: bigint;
		}[] = [];
		for (const [ ws, subs ] of union) {
			if (this.workspaces.get(ws)?.revoked) continue;
			if (scopes.length >= this.maxWorkspacesPerRequest) break;
			const { cursor, minSeq } = await this.pullPosition(ws);
			scopes.push({
				workspaceId: ws,
				cursor: cursor ?? new Uint8Array(),
				subscriptions: subs,
				minSeq,
			});
		}
		return { scopes, subs: union, noDeltaEntities: await this.noDeltaEntities() };
	}

	private async pollLoop(): Promise<void> {
		const interval = this.opts.pollIntervalMs ?? 60_000;
		if (interval <= 0) return;
		while (!this.stopped) {
			await sleep(interval, this.abort.signal);
			if (!this.stopped) this.syncSoon();
		}
	}

	/**
	 * Reacts to the schema revision the server advertised on a stream frame,
	 * push, pull, or bootstrap. A no-op unless this client was configured with
	 * its own revision. An empty server revision means the server advertises
	 * none (an older build) — no conclusion, treated like a match. A genuine
	 * mismatch runs the skew policy exactly once per distinct server revision; a
	 * match (or empty) clears any standing skew reaction.
	 *
	 * Public because queryServer sees a revision too, on an RPC the coordinator
	 * does not issue: the same deployment answers it, so a skew it reveals is
	 * the same skew, and a client that reads mostly through server queries would
	 * otherwise not hear of it until its next pull.
	 */
	observeServerRevision(serverRev: string): void {
		const clientRev = this.opts.clientRevision;
		if (!clientRev) return;
		if (serverRev === '' || serverRev === clientRev) {
			if (this.lastSkewRevision !== serverRev) {
				this.lastSkewRevision = serverRev;
				this.clearSkew();
			}
			return;
		}
		if (this.lastSkewRevision === serverRev) return; // already handled
		this.lastSkewRevision = serverRev;
		// The policy is app code called from whichever path saw the revision (push,
		// pull, bootstrap, or a stream frame). A throw there must not surface as a
		// failure of that path — in the poke loop it would be indistinguishable
		// from a dropped stream — so a broken handler falls back to the same
		// 'warn' an absent one gets.
		const policy = this.opts.onSchemaSkew;
		const action = policy
			? safeCall(() => policy({ client: clientRev, server: serverRev }), 'onSchemaSkew policy', 'warn')
			: 'warn';
		this.applySkewAction(action, clientRev, serverRev);
	}

	/** Applies one skew-policy decision; see SchemaSkewAction. */
	private applySkewAction(action: SchemaSkewAction, client: string, server: string): void {
		if (action === 'ignore') {
			this.clearSkew();
			return;
		}
		this.pushBlocked = action === 'block';
		this.opts.status.update({ schemaSkew: { client, server, action } });
		if (action === 'resync') {
			// Re-bootstrap every workspace on the next cycle. The block stays off: a
			// resync, not read-only, is the chosen recovery.
			this.forceResync = true;
			this.syncSoon();
		}
	}

	/** Drops any standing skew reaction (unblocks pushes, clears the status). */
	private clearSkew(): void {
		this.pushBlocked = false;
		if (this.opts.status.current().schemaSkew) {
			this.opts.status.update({ schemaSkew: undefined });
		}
	}
}

/** Splits a list into consecutive chunks of at most `size` items. */
export function chunkBySize<T>(items: T[], size: number): T[][] {
	const chunks: T[][] = [];
	for (let i = 0; i < items.length; i += size) chunks.push(items.slice(i, i + size));
	return chunks;
}

/**
 * Classifies a failed sync cycle for the status surface.
 *
 * The code stays in the library's own namespace (see codes.ts) rather than
 * carrying Connect's: a server that typed its failure gets that code through
 * verbatim, and anything else is reduced to "retry" vs "the server broke". The
 * exact Connect code is kept alongside it for callers that want to tell a 404
 * from a 500 — but reading it means opting into a second vocabulary, so nothing
 * on the main path needs it.
 */
function errorInfo(err: unknown): SyncErrorInfo {
	if (err instanceof ConnectError) {
		const tagged = err.metadata.get(ErrorCodeMetadataKey) as SyncErrorCode | null;
		return {
			message: err.rawMessage,
			code: tagged ?? (unreachable(err.code) ? CodeUnavailable : CodeInternal),
			transportCode: Code[err.code],
		};
	}
	return { message: err instanceof Error ? err.message : String(err) };
}

/**
 * The failure a mutation-id gap is, in the shape the status surface reads. The
 * server reports it as a code on a workspace's batch result rather than as an
 * RPC error, so nothing else in the cycle would carry it.
 */
function gapError(workspaceId: string): ConnectError {
	const err = new ConnectError(
		`workspace "${workspaceId}": the server refused this client's queue for a mutation-id gap`,
		Code.FailedPrecondition,
	);
	err.metadata.set(ErrorCodeMetadataKey, CodeMutationIdGap);
	return err;
}

/**
 * Whether a Connect code means "the server was not reached" rather than "the
 * server said no".
 *
 * Unknown counts because that is what a failed fetch becomes in the browser:
 * connect-web has no response to read a code from, and the platform
 * deliberately hides whether it was DNS, a refused connection or CORS. A server
 * that DID answer carries its own code — and, for anything the engine typed, an
 * ErrorCodeMetadataKey that is preferred over this classification entirely.
 */
function unreachable(code: Code): boolean {
	return code === Code.Unavailable || code === Code.DeadlineExceeded || code === Code.Unknown;
}
