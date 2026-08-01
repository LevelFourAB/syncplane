/**
 * The contract between the benchmark specs (Node) and the harness page
 * (browser). Everything crossing it is JSON: a spec drives the page through
 * `page.evaluate`, so a SyncClient, a Row or a TextHandle cannot be returned —
 * only numbers about them.
 *
 * The e2e tier's harness-api.ts is the sibling of this file. That one exposes
 * what a spec must ASSERT on; this one exposes what a spec must MEASURE. Kept
 * apart because a benchmark wants a different page: bench entities, a scope it
 * can swap at runtime, and a recorded event stream rather than a row count.
 */

/**
 * The subscription sets the benchmarks run under. Each is a shape a real app
 * has, and the difference between them is what the partial-sync numbers mean.
 *
 * - `full` — no subscriptions. The whole workspace, which is what makes the
 *   other three interpretable.
 * - `window` — the N most recently written issues and comments, plus the
 *   dimension entities. The bounded working set: its cost should track the
 *   window, not the corpus.
 * - `project` — one project's issues by predicate, following to the rows a
 *   board needs. Unbounded in principle (a busy project is large), selective
 *   in practice.
 * - `thread` — one issue, its comments and its dependency chain. The deep
 *   follow: small in rows, several hops to compute.
 */
export type ScopeName = 'full' | 'window' | 'project' | 'thread';

export interface ScopeOptions {
	/** Rows a windowed scope caps at. Bounded by the server's MaxWindowSize. */
	windowLimit?: number;
	/** Project the `project` scope selects. Defaults to the corpus's first. */
	projectId?: string;
	/** Issue the `thread` scope selects. Defaults to the corpus's first. */
	issueId?: string;
}

export interface OpenOptions extends ScopeOptions {
	workspaceId: string;
	/** Defaults to workspaceId. A distinct account is a distinct KV database. */
	accountId?: string;
	scope: ScopeName;
	/** Live change stream. Off unless a scenario is measuring realtime latency. */
	streamEnabled?: boolean;
	/** Fallback poll; 0 disables. Off by default so nothing syncs unasked. */
	pollIntervalMs?: number;
	/** Wrap the KV store in a byte meter (see the e2e tier's write-meter). */
	measureWrites?: boolean;
	/** Rows per pull page; 0 leaves the server's default. */
	pullLimit?: number;
}

/**
 * One recorded SyncEvent, flattened to JSON. `at` is page-relative
 * (performance.now()), so events from one measurement are comparable to each
 * other without a wall clock.
 */
export interface EventRecord {
	type: string;
	at: number;
	durationMs?: number;
	outcome?: string;
	/** bootstrap */
	rows?: number;
	/** bootstrap: the three phases durationMs is made of. */
	streamMs?: number;
	stageMs?: number;
	swapMs?: number;
	/** bootstrap: KV entries the swap transaction wrote. */
	swapWrites?: number;
	/** pull, poke */
	changes?: number;
	hasMore?: boolean;
	/** pull: the client fell behind the horizon and must re-bootstrap. */
	reset?: boolean;
	/** push */
	mutations?: number;
	applied?: number;
	rejected?: number;
	/** rebase */
	acked?: number;
	replayed?: number;
	/** stream, leadership */
	state?: string;
	leader?: boolean;
}

/**
 * What one measured operation cost. `wallMs` is the only number measured from
 * outside; the rest are summed from the events the operation emitted, which is
 * what separates "the client was busy" from "the client was waiting".
 */
export interface SyncSummary {
	wallMs: number;
	bootstraps: number;
	bootstrapRows: number;
	bootstrapMs: number;
	/**
	 * `bootstrapMs` split by phase: waiting on the snapshot stream, staging the
	 * chunks it delivered, and the swap transaction that installs them. They move
	 * independently — a change that shifts time from one to another is invisible
	 * in the total — so a bootstrap regression is attributable only here.
	 */
	bootstrapStreamMs: number;
	bootstrapStageMs: number;
	bootstrapSwapMs: number;
	/** KV entries the swaps wrote. Against `bootstrapRows`: write amplification. */
	bootstrapSwapWrites: number;
	pulls: number;
	pullChanges: number;
	pullMs: number;
	/** Pulls that forced a re-bootstrap. Non-zero is the expensive path. */
	resets: number;
	pushes: number;
	pushMutations: number;
	pushMs: number;
	rebases: number;
	rebaseAcked: number;
	rebaseReplayed: number;
	rebaseMs: number;
	pokes: number;
	pokeChanges: number;
	events: EventRecord[];
}

/** A bootstrap measured from the reader's side rather than the client's. */
export interface FirstReadSummary {
	/** Until a query first returned rows — when the app could paint. */
	firstReadMs: number;
	firstReadRows: number;
	/** Until start() resolved — when the local store was provably complete. */
	completeMs: number;
	summary: SyncSummary;
}

/** Local read cost. The query shapes a real UI issues. */
export type ReadOp =
	| { kind: 'scanIssues' }
	| { kind: 'scanFlat' }
	/** One project's issues, newest first — the board. Served by an index. */
	| { kind: 'board'; projectId: string; limit: number }
	/** Filter on state, order by priority — the composite index's shape. */
	| { kind: 'triage'; limit: number }
	/** Membership in a repeated field — the fan-out index's shape. */
	| { kind: 'byLabel'; label: string; limit: number }
	/** The board again, with its refs hydrated: adds a lookup per row per ref. */
	| { kind: 'boardHydrated'; projectId: string; limit: number }
	/** One issue's comments — a reverse relation, pushed down to a ref leaf. */
	| { kind: 'thread'; issueId: string }
	/** A predicate no index serves, so the evaluator walks the entity. */
	| { kind: 'unindexed'; limit: number }
	| { kind: 'get'; issueId: string };

export interface ReadResult {
	ms: number;
	rows: number;
	/** Whether the local store provably held every matching row. */
	complete: boolean;
	/** Why not, when it did not: 'unsubscribed' | 'predicate' | 'window'. */
	bounds: string[];
}

/** Rows the local store holds, per entity. */
export type RowCounts = Record<string, number>;

/** Bytes the origin occupies, as the browser reports them. */
export interface StorageView {
	usage: number;
	quota: number;
}

/** Bytes written to the KV store, per keyspace; see the e2e write-meter. */
export interface WriteBytesView {
	total: number;
	overlay: number;
	overlayText: number;
	pending: number;
	commits: number;
}

export interface BenchHarnessApi {
	open(opts: OpenOptions): Promise<void>;
	/** Times client.start() — bootstrap on a cold store, nothing on a warm one. */
	start(): Promise<SyncSummary>;
	/**
	 * Starts without awaiting, polling a small query until it answers. The gap
	 * between the two numbers it returns is the difference between "usable" and
	 * "complete", which a single duration cannot express.
	 */
	startWithFirstRead(probeLimit: number): Promise<FirstReadSummary>;
	/** One push+pull cycle. */
	syncNow(): Promise<SyncSummary>;
	/** Swaps scope and syncs the difference. */
	setScope(scope: ScopeName, opts?: ScopeOptions): Promise<SyncSummary>;
	read(op: ReadOp): Promise<ReadResult>;
	counts(): Promise<RowCounts>;
	storage(): Promise<StorageView>;
	writeBytes(): Promise<WriteBytesView>;
	resetWriteBytes(): Promise<void>;
	/**
	 * Queues `count` local PATCHes against issues starting at `firstIndex`,
	 * without syncing. Returns ms to queue them.
	 *
	 * The index is the caller's because which rows are written matters: a patch
	 * to a row outside the client's scope is a cheaper, less interesting write
	 * than one to a row it is holding, and only the spec knows where its scope
	 * currently sits.
	 */
	queueWrites(count: number, firstIndex: number): Promise<number>;
	pendingCount(): Promise<number>;
	/**
	 * Waits until `issueId`'s title starts with `prefix`, polling the local
	 * store. Returns ms waited — with a live stream, that is poke latency.
	 */
	waitForTitle(issueId: string, prefix: string, timeoutMs: number): Promise<number>;
	/** Recorded events since the last reset. */
	events(): Promise<EventRecord[]>;
	resetEvents(): Promise<void>;
	/** JS heap, where the browser exposes it (Chromium only; 0 elsewhere). */
	heapBytes(): Promise<number>;
	close(): Promise<void>;
}

declare global {
	interface Window {
		/**
		 * Deliberately not `harness`: the e2e tier already owns that name, and
		 * both tiers compile in one TypeScript program, so a second declaration
		 * of it would be a conflicting global rather than a second harness.
		 */
		benchHarness: BenchHarnessApi;
	}
}
