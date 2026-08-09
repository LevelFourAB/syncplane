/**
 * The page half of the benchmark tier: one SyncClient per tab against the
 * seeded bench corpus, wrapped in the JSON façade the specs drive over
 * `page.evaluate`. See ../harness-api.ts for the contract.
 *
 * Nothing here configures storage or locking. A browser SyncClient given only a
 * url picks IDBKVStore, WebLockMutex, changeshare and stream leadership on its
 * own, and those defaults are what the numbers are supposed to describe: a
 * benchmark run against an in-memory store would measure a client nobody ships.
 *
 * Every measured operation returns a SyncSummary built from the events the
 * client emitted while it ran. Wall time alone cannot say whether a slow
 * bootstrap was one long stream or forty pages, and the events can.
 */

import {
	CommentSchema,
	FlatSchema,
	IssueSchema,
	LabelSchema,
	NoteSchema,
	ProjectSchema,
	UserSchema,
} from '../../../src/gen/syncplane/bench/v1/bench_pb.js';
import { subscription, type SubscriptionSpec, SyncClient } from '../../../src/index.js';
import { IDBKVStore } from '../../../src/kv/idb.js';
import { WriteMeter } from '../../e2e/app/write-meter.js';
import {
	type BenchHarnessApi,
	type EventRecord,
	type FirstReadSummary,
	type OpenOptions,
	type ReadOp,
	type ReadResult,
	type RowCounts,
	type ScopeName,
	type ScopeOptions,
	type StorageView,
	type SyncSummary,
} from '../harness-api.js';

/** The spawned benchserver's address, handed over as ?server=... by BenchTab. */
const serverUrl = new URLSearchParams(location.search).get('server');

/**
 * Every entity in the corpus. A client holds only the entities it declares, so
 * this tuple — not the subscription set — is what decides that a full-sync
 * client is measured against the whole workspace.
 */
const ENTITIES = [
	ProjectSchema,
	UserSchema,
	LabelSchema,
	IssueSchema,
	CommentSchema,
	NoteSchema,
	FlatSchema,
] as const;

let client: SyncClient | undefined;
let meter: WriteMeter | undefined;
let scopeOpts: ScopeOptions = {};
/** Every event the open client has emitted since the last reset. */
let recorded: EventRecord[] = [];
/** Serial number for locally queued writes, so each one is a distinct value. */
let writeSeq = 0;

function current(): SyncClient {
	if (client === undefined) throw new Error('bench harness: no client open');
	return client;
}

/** The corpus's first row of an entity — the default target of a scoped run. */
function firstKey(prefix: string): string {
	return `${prefix}-${'0'.repeat(8)}`;
}

/**
 * The subscription set for a named scope. Kept here rather than in the specs
 * because a spec cannot hold a schema: the boundary is JSON, and a
 * SubscriptionSpec closes over descriptors and predicate lambdas.
 */
function scopeSpecs(scope: ScopeName, opts: ScopeOptions): SubscriptionSpec[] {
	const limit = opts.windowLimit ?? 500;
	const projectId = opts.projectId ?? firstKey('project');
	const issueId = opts.issueId ?? firstKey('issue');
	switch (scope) {
		case 'full':
			// An empty set IS full sync — not "no sync".
			return [];
		case 'window':
			return [
				// byRecency ranks by write sequence, so maintaining this window
				// costs the server no field comparison at all. The comparable
				// ordering is priced by the `project` scope below.
				subscription(IssueSchema, undefined, {
					limit,
					byRecency: true,
					include: { project: true, assignee: true },
				}),
				subscription(CommentSchema, undefined, { limit, byRecency: true }),
				// Dimension entities whole: they are small, and a board that has
				// to fetch a project name over the network is not a local-first
				// board.
				subscription(ProjectSchema),
				subscription(LabelSchema),
			];
		case 'project':
			return [
				subscription(IssueSchema, (i) => i.project.id.eq(projectId), {
					include: { project: true, assignee: true, reporter: true },
				}),
				subscription(ProjectSchema),
				subscription(LabelSchema),
			];
		case 'thread':
			return [
				// The deep follow: one row, then its comments (a reverse hop),
				// each comment's author, and two hops down the dependency chain.
				//
				// The chain is spelled out by NESTING rather than by `depth`.
				// Depth re-applies the whole path set each round, so it only
				// works when every segment is walkable from every target it
				// reaches — with a mixed set, round two asks a project for its
				// `project` and the subscription is refused. Nesting says the
				// same thing about the one relation that is self-referential
				// and leaves the rest at one hop.
				subscription(IssueSchema, (i) => i.id.eq(issueId), {
					include: {
						project: true,
						assignee: true,
						comments: { author: true },
						blockedBy: { blockedBy: { assignee: true } },
					},
				}),
			];
	}
}

/**
 * Flattens a SyncEvent to the JSON the boundary carries.
 *
 * The event's own `at` is a Date, which is both unserializable across
 * page.evaluate and the wrong clock: two events are compared to each other, not
 * to wall time. Overwriting it with performance.now() is the point, so it has
 * to be assigned AFTER the spread.
 */
function record(event: Record<string, unknown>): void {
	recorded.push({ ...event, type: String(event.type), at: performance.now() } as EventRecord);
}

/**
 * Sums the events recorded from `from` onward into what the operation cost.
 *
 * Every total is per event type. A single "total duration" would double-count:
 * a rebase runs inside the pull that delivered the page it rebases over, so
 * their durations overlap in time rather than adding up.
 */
function summarize(from: number, wallMs: number): SyncSummary {
	const events = recorded.slice(from);
	const count = (type: string) => events.filter((e) => e.type === type).length;
	const per = (type: string, of: (e: EventRecord) => number | undefined) =>
		events.filter((e) => e.type === type).reduce((total, e) => total + (of(e) ?? 0), 0);
	return {
		wallMs,
		bootstraps: count('bootstrap'),
		bootstrapRows: per('bootstrap', (e) => e.rows),
		bootstrapMs: per('bootstrap', (e) => e.durationMs),
		bootstrapStreamMs: per('bootstrap', (e) => e.streamMs),
		bootstrapStageMs: per('bootstrap', (e) => e.stageMs),
		bootstrapSwapMs: per('bootstrap', (e) => e.swapMs),
		bootstrapSwapWrites: per('bootstrap', (e) => e.swapWrites),
		pulls: count('pull'),
		pullChanges: per('pull', (e) => e.changes),
		pullMs: per('pull', (e) => e.durationMs),
		resets: events.filter((e) => e.type === 'pull' && e.reset === true).length,
		pushes: count('push'),
		pushMutations: per('push', (e) => e.mutations),
		pushMs: per('push', (e) => e.durationMs),
		rebases: count('rebase'),
		rebaseAcked: per('rebase', (e) => e.acked),
		rebaseReplayed: per('rebase', (e) => e.replayed),
		rebaseMs: per('rebase', (e) => e.durationMs),
		pokes: count('poke'),
		pokeChanges: per('poke', (e) => e.changes),
		events,
	};
}

/**
 * Resolves when the client's first sync cycle after `start()` has completed.
 *
 * `start()` deliberately does NOT wait for it — a dead server surfaces through
 * the status tracker rather than as a start() rejection — so timing start()
 * alone would report a few milliseconds of setup and call it a bootstrap. The
 * status transition to idle with a `lastSyncedAt` is the first moment the local
 * store is what the scope says it should be.
 *
 * The listener is attached BEFORE start() is called, so the transition cannot
 * be missed. An error state rejects rather than retrying quietly: the
 * coordinator would back off and try again, and a benchmark that silently
 * measured the second attempt would be reporting a number nobody could explain.
 */
function settled(c: SyncClient): Promise<void> {
	return new Promise((resolve, reject) => {
		const off = c.onStatusChange((status) => {
			if (status.state === 'idle' && status.lastSyncedAt !== undefined) {
				off();
				resolve();
			} else if (status.state === 'error') {
				off();
				reject(new Error(`sync failed: ${status.lastError?.message ?? 'unknown'}`));
			}
		});
	});
}

/** Runs `fn`, timing it and summarizing the events it caused. */
async function measured(fn: () => Promise<void>): Promise<SyncSummary> {
	const from = recorded.length;
	const started = performance.now();
	await fn();
	return summarize(from, performance.now() - started);
}

/**
 * How the store answered the reads since `from`: the serving index's name, or
 * `scan:<reason>` for a query that fell back. Undefined when nothing emitted a
 * query event (get and scan do not). One read issues one query, so the first
 * event is the read's own.
 */
function pathOf(from: number): string | undefined {
	const event = recorded.slice(from).find((e) => e.type === 'query');
	if (event === undefined) return undefined;
	return event.index ?? `scan:${event.declined ?? 'unknown'}`;
}

async function runRead(op: ReadOp): Promise<ReadResult> {
	const c = current();
	const from = recorded.length;
	const started = performance.now();
	let rows: { length: number; coverage: { complete: boolean; bounds: string[] } };
	switch (op.kind) {
		case 'scanIssues':
			rows = await c.scan(IssueSchema) as typeof rows;
			break;
		case 'scanFlat':
			rows = await c.scan(FlatSchema) as typeof rows;
			break;
		case 'board':
			rows = await c.query(IssueSchema, {
				where: (i) => i.project.id.eq(op.projectId),
				orderBy: (i) => [ i.updatedAtMs.desc() ],
				limit: op.limit,
			}) as typeof rows;
			break;
		case 'triage':
			rows = await c.query(IssueSchema, {
				where: (i) => i.state.eq('open'),
				orderBy: (i) => [ i.priority.desc() ],
				limit: op.limit,
			}) as typeof rows;
			break;
		case 'byLabel':
			rows = await c.query(IssueSchema, {
				where: (i) => i.labels.has(op.label),
				orderBy: (i) => [ i.updatedAtMs.desc() ],
				limit: op.limit,
			}) as typeof rows;
			break;
		case 'boardHydrated':
			rows = await c.query(IssueSchema, {
				where: (i) => i.project.id.eq(op.projectId),
				orderBy: (i) => [ i.updatedAtMs.desc() ],
				limit: op.limit,
			}, { include: { project: true, assignee: true, reporter: true } }) as typeof rows;
			break;
		case 'thread':
			rows = await c.query(CommentSchema, {
				where: (m) => m.issue.id.eq(op.issueId),
				orderBy: (m) => [ m.createdAtMs.desc() ],
			}) as typeof rows;
			break;
		case 'unindexed':
			// `contains` is not a prefix, so nothing brackets it: the evaluator
			// walks the entity. The counterweight to every indexed read above.
			rows = await c.query(IssueSchema, {
				where: (i) => i.body.contains('consectetur'),
				limit: op.limit,
			}) as typeof rows;
			break;
		case 'get': {
			const row = await c.get(IssueSchema, op.issueId);
			return {
				ms: performance.now() - started,
				rows: row === undefined ? 0 : 1,
				complete: true,
				bounds: [],
			};
		}
	}
	return {
		ms: performance.now() - started,
		rows: rows.length,
		complete: rows.coverage.complete,
		bounds: rows.coverage.bounds,
		path: pathOf(from),
	};
}

const harness: BenchHarnessApi = {
	async open(opts: OpenOptions) {
		if (client !== undefined) throw new Error('bench harness: client already open');
		if (serverUrl === null) throw new Error('bench harness: page opened without ?server=');
		recorded = [];
		writeSeq = 0;
		scopeOpts = opts;

		const accountId = opts.accountId ?? opts.workspaceId;
		meter = opts.measureWrites === true
			? new WriteMeter(await IDBKVStore.open(`sync:${accountId}`))
			: undefined;

		client = new SyncClient({
			url: serverUrl,
			workspaceId: opts.workspaceId,
			accountId,
			kvStore: meter,
			entities: ENTITIES,
			subscriptions: scopeSpecs(opts.scope, opts),
			// Both default off: a benchmark measures the cycle it asks for, and a
			// poll firing mid-measurement is another client's traffic as far as
			// the numbers are concerned.
			streamEnabled: opts.streamEnabled ?? false,
			pollIntervalMs: opts.pollIntervalMs ?? 0,
			pullLimit: opts.pullLimit ?? 0,
			onSyncEvent: (event) => record(event as unknown as Record<string, unknown>),
		});
	},

	start() {
		return measured(async () => {
			const c = current();
			const done = settled(c);
			await c.start();
			await done;
		});
	},

	async startWithFirstRead(probeLimit: number): Promise<FirstReadSummary> {
		const c = current();
		const from = recorded.length;
		const started = performance.now();
		let firstReadMs = -1;
		let firstReadRows = 0;
		let complete = false;

		// The first cycle, awaited through the status tracker rather than
		// through start(): see settled().
		const running = settled(c).then(() => {
			complete = true;
		});
		// Held rather than discarded: start() resolving early is the whole
		// reason this waits on the status tracker, but start() FAILING still
		// has to surface instead of becoming an unhandled rejection.
		const starting = c.start();
		// Probing is not free — each round is a real query against a store that
		// is being written to — so the loop yields between attempts rather than
		// spinning, and the cost of the probe itself lands in the number the
		// same way a real app's first render would.
		const probe = (async () => {
			for (;;) {
				const rows = await c.query(IssueSchema, { limit: probeLimit });
				if (rows.length > 0) {
					firstReadMs = performance.now() - started;
					firstReadRows = rows.length;
					return;
				}
				// Checked AFTER the query, so a scope that legitimately selects
				// no issues ends the loop instead of spinning to the timeout.
				if (complete) return;
				await new Promise((resolve) => setTimeout(resolve, 5));
			}
		})();
		await Promise.all([ running, starting ]);
		const completeMs = performance.now() - started;
		await probe;

		return {
			// A corpus can be small enough that start() wins the race; reporting
			// the completion time then is honest rather than reporting -1.
			firstReadMs: firstReadMs < 0 ? completeMs : firstReadMs,
			firstReadRows,
			completeMs,
			summary: summarize(from, completeMs),
		};
	},

	syncNow() {
		return measured(async () => {
			await current().syncNow();
		});
	},

	setScope(scope: ScopeName, opts?: ScopeOptions) {
		const merged = { ...scopeOpts, ...opts };
		scopeOpts = merged;
		return measured(async () => {
			const c = current();
			c.setSubscriptions(scopeSpecs(scope, merged));
			// The swap only records intent; the resync happens on the next
			// cycle, so the cost belongs to this measurement.
			await c.syncNow();
		});
	},

	read(op: ReadOp) {
		return runRead(op);
	},

	async waitForIndexes(timeoutMs: number) {
		const started = performance.now();
		for (;;) {
			const done = recorded.find((e) => e.type === 'indexing');
			if (done !== undefined) {
				return {
					waitedMs: performance.now() - started,
					buildMs: done.durationMs ?? 0,
					rows: done.rows ?? 0,
				};
			}
			if (performance.now() - started > timeoutMs) {
				throw new Error(`bench harness: indexes not built within ${timeoutMs}ms`);
			}
			await new Promise((resolve) => setTimeout(resolve, 50));
		}
	},

	async counts(): Promise<RowCounts> {
		const c = current();
		const entries = await Promise.all([
			[ 'project', ProjectSchema ] as const,
			[ 'user', UserSchema ] as const,
			[ 'label', LabelSchema ] as const,
			[ 'issue', IssueSchema ] as const,
			[ 'comment', CommentSchema ] as const,
			[ 'note', NoteSchema ] as const,
			[ 'flat', FlatSchema ] as const,
		].map(async ([ name, schema ]) => [ name, (await c.scan(schema)).length ] as const));
		return Object.fromEntries(entries);
	},

	async storage(): Promise<StorageView> {
		const estimate = await navigator.storage?.estimate?.() ?? {};
		return { usage: estimate.usage ?? 0, quota: estimate.quota ?? 0 };
	},

	async writeBytes() {
		if (meter === undefined) {
			throw new Error('bench harness: open with measureWrites to count writes');
		}
		return meter.tallied();
	},

	async resetWriteBytes() {
		if (meter === undefined) {
			throw new Error('bench harness: open with measureWrites to count writes');
		}
		meter.reset();
	},

	async queueWrites(count: number, firstIndex: number): Promise<number> {
		const c = current();
		const started = performance.now();
		for (let i = firstIndex; i < firstIndex + count; i++) {
			const id = `issue-${String(i).padStart(8, '0')}`;
			await c.mutate.patch(IssueSchema, id, { title: `local ${writeSeq++}` });
		}
		return performance.now() - started;
	},

	pendingCount() {
		return current().pendingCount();
	},

	async waitForTitle(issueId: string, prefix: string, timeoutMs: number): Promise<number> {
		const c = current();
		const started = performance.now();
		for (;;) {
			const row = await c.get(IssueSchema, issueId);
			if (row?.title.startsWith(prefix) === true) return performance.now() - started;
			if (performance.now() - started > timeoutMs) {
				throw new Error(`bench harness: ${issueId} did not reach "${prefix}" in ${timeoutMs}ms`);
			}
			await new Promise((resolve) => setTimeout(resolve, 2));
		}
	},

	async events() {
		return recorded;
	},

	async resetEvents() {
		recorded = [];
	},

	async heapBytes(): Promise<number> {
		// Chromium-only and approximate, which is why nothing asserts on it: it
		// is here to make a run that quietly doubled its retained heap visible.
		const perf = performance as unknown as { memory?: { usedJSHeapSize?: number } };
		return perf.memory?.usedJSHeapSize ?? 0;
	},

	async close() {
		await client?.close();
		client = undefined;
		meter = undefined;
	},
};

window.benchHarness = harness;
