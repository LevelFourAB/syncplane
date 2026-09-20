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
	/** pull: durationMs split into waiting on the server and applying locally. */
	requestMs?: number;
	applyMs?: number;
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
	/** query: the index that answered, or why the read scanned instead. */
	index?: string;
	declined?: string;
	/** query: index positions the walk visited. */
	candidates?: number;
	/** query: wall time spent queued behind a concurrent commit. */
	waitMs?: number;
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
	/** `pullMs` split: waiting on the server, and applying its pages locally. */
	pullRequestMs: number;
	pullApplyMs: number;
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
	/**
	 * Full-text search over the issue bodies, served by the term-postings
	 * index. `terms` is the query as a user typed it, so a trailing space is
	 * what separates an exact final term from a prefix one, and `ranked` adds
	 * the relevance ordering — which ranks every matching row, whatever the
	 * limit.
	 *
	 * The corpus plants terms of known selectivity (benchapp.SearchRareTerm and
	 * its siblings), and that is the axis these reads vary: what a search costs
	 * is set by how many rows carry its terms, not by how many words it has.
	 */
	| { kind: 'search'; terms: string; limit: number; ranked?: true }
	| { kind: 'get'; issueId: string }
	/**
	 * The triage page with the counts a filter sidebar shows beside it:
	 * project, priority and label. The plan is the same index as `triage`, but
	 * a facet folds from every matching row, so the walk reads the whole match
	 * set rather than stopping at the page.
	 */
	| { kind: 'triageFacets'; limit: number }
	/** The board with state, assignee and label counts. */
	| { kind: 'boardFacets'; projectId: string; limit: number }
	/** No predicate at all: the sidebar over the whole entity. */
	| { kind: 'allFacets'; limit: number };

/**
 * One live query a bench holds open. The query shapes mirror ReadOp's, because
 * the interesting comparison is one-shot cost against maintained cost of the
 * same read. `flat` watches an entity no write scenario touches — the control
 * that prices what a commit costs a watcher it cannot affect.
 */
export type WatchSpec =
	| { kind: 'board'; projectId: string; limit: number }
	| { kind: 'triage'; limit: number }
	| { kind: 'byLabel'; label: string; limit: number }
	/** Entity watch over every issue — fires on ANY issue change, which is what
	 * makes it the waiter for change latency without knowing the row. */
	| { kind: 'issues' }
	/**
	 * Comments whose issue matches a predicate over the ISSUE — a one-hop
	 * semi-join, so this watch is maintained against two entities at once. A
	 * commit to the target entity moves which comments match without touching
	 * any of them, which is the case no keyed delta can carry.
	 */
	| { kind: 'relComments'; state: string; priority: number; limit: number }
	/**
	 * The same comments, ordered and paged the same way, with the semi-join
	 * replaced by a row-local predicate. The control that separates what a
	 * commit costs a comment watch from what crossing a reference adds.
	 */
	| { kind: 'comments'; limit: number }
	| { kind: 'flat' }
	/** The `triage` watch carrying the sidebar's facets, kept in step per commit. */
	| { kind: 'triageFacets'; limit: number };

/**
 * One open watch's deliveries. A callback fires only when the delivered page
 * MOVED, so `callbacks` counts visible updates — maintenance work that ended in
 * "nothing changed" is invisible here and shows up as sync wall time instead.
 */
export interface WatchView {
	/** Deliveries since the watch opened or the last resetWatchStats. */
	callbacks: number;
	/** Rows the latest delivery carried. */
	rows: number;
	/**
	 * Deliveries of those that carried a `reset` delta rather than a splice.
	 * A reset says the result was re-derived rather than maintained, so a
	 * consumer holding rendered rows rebuilds all of them; the split is what
	 * separates maintenance that stayed incremental from maintenance that gave
	 * up.
	 */
	resets: number;
}

/**
 * Latency distribution of `count` awaited local writes. Percentiles rather
 * than a mean: one slow write among two hundred is a stall a user feels, and a
 * mean buries it.
 */
export interface LocalWriteStats {
	totalMs: number;
	p50Ms: number;
	p95Ms: number;
	maxMs: number;
}

/**
 * A page walk over one ordered, indexed query. `total: false` runs every page
 * with the match count opted out, which is the shape an app that only follows
 * `hasMore` pays.
 */
export type PageWalkOp =
	| { kind: 'boardPages'; projectId: string; limit: number; pages: number; total?: false }
	| { kind: 'triagePages'; limit: number; pages: number; total?: false };

export interface PageWalkResult {
	/** ms per page, in walk order — the shape says whether depth costs. */
	pageMs: number[];
	/** Rows delivered across the whole walk. */
	rows: number;
	/** The match total the first page reported; 0 when the walk opted out. */
	total: number;
	/** ms to reach the final page's rank in one query via `offset` instead. */
	offsetMs: number;
	/** How the store answered, as in ReadResult.path (first page's event). */
	path?: string;
}

export interface ReadResult {
	ms: number;
	rows: number;
	/** Whether the local store provably held every matching row. */
	complete: boolean;
	/** Why not, when it did not: 'unsubscribed' | 'predicate' | 'window'. */
	bounds: string[];
	/**
	 * How the store answered: the index's name, or `scan:<reason>` for a read
	 * that fell back (see QueryDeclineReason). Absent for operations that do
	 * not go through the query path at all (get, scan). An indexed shape
	 * reporting `scan:not-built` is measuring the fallback, not the index —
	 * the number is real either way, but it prices a different plan.
	 */
	path?: string;
	/**
	 * Index positions the walk visited, when an index answered. Against `ms` it
	 * says whether a slow read walked too far or paid elsewhere.
	 */
	candidates?: number;
}

/** What waiting on the background index build cost, and what the build did. */
export interface IndexBuildView {
	/** From the call until the build's completion event had been recorded. */
	waitedMs: number;
	/** The build's own wall time, from its `indexing` event. */
	buildMs: number;
	/** Rows the build rewrote; 0 when every index was already built. */
	rows: number;
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
	/**
	 * Resolves when the workspace's background index build has completed —
	 * the `indexing` event the client emits — so a read after this prices the
	 * index, not the scan it stands in for while the build runs. Rejects after
	 * `timeoutMs`: a spec that meant to measure the built state must not
	 * silently measure the other one.
	 */
	waitForIndexes(timeoutMs: number): Promise<IndexBuildView>;
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
	/**
	 * `count` sequential local writes, each awaited — the latency an event
	 * handler sees. `put` writes whole issues at `firstIndex` on (a fresh index
	 * range inserts, a seeded one replaces); `patch` masks title+updatedAtMs.
	 * Unlike queueWrites this prices the write itself, so run it online or
	 * offline according to what the scenario is measuring.
	 */
	localWrites(op: 'put' | 'patch', count: number, firstIndex: number): Promise<LocalWriteStats>;
	/**
	 * Opens one watch per spec, in order — WatchView indexes match. Resolves
	 * after every watch has delivered its first page, so measurements that
	 * follow start from a settled hub.
	 */
	openWatches(specs: WatchSpec[]): Promise<void>;
	watchStats(): Promise<WatchView[]>;
	resetWatchStats(): Promise<void>;
	/**
	 * Resolves when watch `index` has delivered at least `minCallbacks` since
	 * the last reset. Returns ms waited — with a live stream, watch-observed
	 * change latency.
	 */
	waitForWatch(index: number, minCallbacks: number, timeoutMs: number): Promise<number>;
	closeWatches(): Promise<void>;
	/**
	 * Walks `op.pages` pages of an ordered query by feeding each page's
	 * endCursor back as `after`, then jumps to the final page's rank once more
	 * via `offset` — the two ways an app reaches a deep rank, priced together.
	 */
	paginate(op: PageWalkOp): Promise<PageWalkResult>;
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
