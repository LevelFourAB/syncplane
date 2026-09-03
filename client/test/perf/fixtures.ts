import type { BrowserContext, Page } from '@playwright/test';
import { test } from '@playwright/test';

import { SERVER_URL_ENV, TIERS_ENV } from './global-setup.js';
import type {
	EventRecord,
	FirstReadSummary,
	IndexBuildView,
	LocalWriteStats,
	OpenOptions,
	PageWalkOp,
	PageWalkResult,
	ReadOp,
	ReadResult,
	RowCounts,
	ScopeName,
	ScopeOptions,
	StorageView,
	SyncSummary,
	WatchSpec,
	WatchView,
	WriteBytesView,
} from './harness-api.js';
import { reportBrowser, type Wire } from './report.js';

/** Address of the benchserver global setup spawned for this run. */
export function serverUrl(): string {
	const url = process.env[SERVER_URL_ENV];
	if (url === undefined) throw new Error(`${SERVER_URL_ENV} unset — global setup did not run`);
	return url;
}

/**
 * One corpus size, as the server reports it. Read from /debug/tiers rather than
 * restated here: a spec that scaled its expectations to a stale copy of the row
 * counts would look like it passed while measuring a different corpus.
 */
export interface Tier {
	name: string;
	approx: number;
	total: number;
	workspace: string;
	projects: number;
	users: number;
	labels: number;
	issues: number;
	comments: number;
	notes: number;
	flats: number;
	noteChars: number;
	issueBodyChars: number;
}

export function tiers(): Tier[] {
	const raw = process.env[TIERS_ENV];
	if (raw === undefined) throw new Error(`${TIERS_ENV} unset — global setup did not run`);
	return JSON.parse(raw) as Tier[];
}

export function tier(name: string): Tier {
	const found = tiers().find((t) => t.name === name);
	if (found === undefined) throw new Error(`no tier ${name} in this run's corpus`);
	return found;
}

/**
 * The biggest corpus this run seeded. Scenarios that sweep a parameter (how far
 * behind, how many pending) run against one tier rather than all of them, and
 * the interesting one is always the largest — that is where a cost that scales
 * with the corpus instead of with the change set becomes visible. Picking it at
 * runtime keeps `TIERS=s,m` runs working instead of failing on a hardcoded name.
 */
export function largestTier(): Tier {
	const all = tiers();
	const last = all[all.length - 1];
	if (last === undefined) throw new Error('this run seeded no tiers');
	return last;
}

/**
 * A workspace nobody has synced yet, derived from a seeded one.
 *
 * The corpus lives in one workspace per tier, and a client that has bootstrapped
 * it leaves an IndexedDB database behind. Cold-start numbers therefore need a
 * fresh ACCOUNT, not a fresh workspace — the account is what names the local
 * database. Every benchmark below that says "cold" opens against one of these.
 */
export function freshAccount(label: string): string {
	return `${label}-${crypto.randomUUID().slice(0, 8)}`;
}

/**
 * The key of the i-th seeded issue, mirroring benchapp.IssueKey. Zero-padded,
 * because row keys order the store and an unpadded id would sort issue-9
 * between issue-89 and issue-90.
 */
export function issueKey(index: number): string {
	return `issue-${String(index).padStart(8, '0')}`;
}

/**
 * The terms the corpus plants in its issue bodies, mirroring
 * benchapp.SearchRareTerm and its siblings. What a search costs is set by how
 * many rows carry its terms, so these — and the counts below — are the axis
 * every search benchmark varies.
 */
export const searchTerms = {
	rare: 'zygote',
	mid: 'quasar',
	common: 'telemetry',
} as const;

/** Issues carrying `searchTerms.rare`; fixed across tiers (benchapp.RareTermRows). */
export const rareTermRows = 10;

/** Issues carrying `searchTerms.mid` (benchapp.MidTermPer). */
export function midTermRows(t: Tier): number {
	return Math.ceil(t.issues / 100);
}

export interface WireStats {
	total: Record<string, number>;
	bytesIn: Record<string, number>;
	bytesOut: Record<string, number>;
	gzipIn: Record<string, number>;
	gzipOut: Record<string, number>;
	encodedOut: Record<string, number>;
	wireIn: Record<string, number>;
	wireOut: Record<string, number>;
	serverMs: Record<string, number>;
	serverMaxMs: Record<string, number>;
}

/** The server's view of what crossed the wire (benchserver's /debug/stats). */
export async function wireStats(): Promise<WireStats> {
	const res = await fetch(new URL('/debug/stats', serverUrl()));
	if (!res.ok) throw new Error(`/debug/stats responded ${res.status}`);
	return await res.json() as WireStats;
}

/**
 * What crossed the wire between two snapshots, from the server's side.
 *
 * The counters are process-wide, which is only enough because this tier runs
 * single-worker and serially: nothing else is talking to the server while a
 * scenario is bracketed.
 *
 * `roundTrips` counts CALLS, not pages. A server-streaming method is one call
 * however many frames it carries, which is the distinction that matters for
 * estimating a real link: forty pull pages cost forty round trips, while a
 * forty-chunk bootstrap stream costs one.
 */
export function wireDelta(before: WireStats, after: WireStats): Wire {
	const sum = (of: Record<string, number>) => Object.values(of).reduce((a, b) => a + b, 0);
	return {
		roundTrips: sum(after.total) - sum(before.total),
		bytesUp: sum(after.bytesIn) - sum(before.bytesIn),
		bytesDown: sum(after.bytesOut) - sum(before.bytesOut),
		gzipUp: sum(after.gzipIn) - sum(before.gzipIn),
		gzipDown: sum(after.gzipOut) - sum(before.gzipOut),
		wireUp: sum(after.wireIn) - sum(before.wireIn),
		wireDown: sum(after.wireOut) - sum(before.wireOut),
		serverMs: sum(after.serverMs) - sum(before.serverMs),
	};
}

/** Brackets `fn` with wire snapshots, returning its result and what it cost. */
export async function withWire<T>(fn: () => Promise<T>): Promise<{ result: T; wire: Wire }> {
	const before = await wireStats();
	const result = await fn();
	return { result, wire: wireDelta(before, await wireStats()) };
}

/**
 * The issue state and priority the `flip` write moves rows onto, and therefore
 * the pair a semi-join has to name for a flip to move its membership. Mirrors
 * relFlipState/relFlipPriority in cmd/benchserver/debug.go.
 */
export const REL_FLIP = { state: 'review', priority: 4 } as const;

/**
 * Server-side writes against a seeded corpus; see benchserver's writeHandler.
 * Returns the index of the first issue touched, so a later call can target the
 * same rows — deleting exactly what an earlier insert created, rather than the
 * seeded range every op defaults to.
 */
export async function serverWrite(
	tierName: string,
	op: 'patch' | 'insert' | 'delete' | 'flip',
	count: number,
	/** Title prefix for a patch, so a waiter can distinguish this write. */
	marker?: string,
	/** Index of the first issue to touch, overriding the op's default range. */
	first?: number,
): Promise<{ first: number }> {
	const res = await fetch(new URL('/debug/write', serverUrl()), {
		method: 'POST',
		body: JSON.stringify({
			workspace: tier(tierName).workspace,
			tier: tierName,
			op,
			count,
			marker,
			first,
		}),
	});
	if (!res.ok) throw new Error(`/debug/write responded ${res.status}: ${await res.text()}`);
	return await res.json() as { first: number };
}

/** One GC pass, which is how the tombstone horizon is made to overtake a client. */
export async function collectGarbage(tierName: string): Promise<void> {
	const res = await fetch(new URL('/debug/gc', serverUrl()), {
		method: 'POST',
		body: JSON.stringify({ workspace: tier(tierName).workspace }),
	});
	if (!res.ok) throw new Error(`/debug/gc responded ${res.status}: ${await res.text()}`);
}

/**
 * Records which browser is taking this run's numbers, from inside a worker —
 * the only place that knows, since the project a worker runs is the one the
 * command line selected while the config lists every project regardless.
 *
 * Once per worker process is enough: a run measures one project, and every
 * worker in it launches the same build.
 */
let browserRecorded = false;
function recordBrowser(context: BrowserContext): void {
	if (browserRecorded) return;
	browserRecorded = true;
	const browser = context.browser();
	const project = test.info().project;
	reportBrowser({
		project: project.name,
		engine: browser?.browserType().name() ?? project.use.browserName ?? 'chromium',
		channel: project.use.channel,
		version: browser?.version(),
	});
}

/**
 * A browser tab holding one SyncClient against the bench corpus. Every method
 * is a typed round-trip into the page harness; see harness-api.ts.
 */
export class BenchTab {
	private constructor(readonly page: Page) {}

	static async open(context: BrowserContext, opts: OpenOptions): Promise<BenchTab> {
		recordBrowser(context);
		const page = await context.newPage();
		await page.goto(`/?server=${encodeURIComponent(serverUrl())}`);
		await page.waitForFunction(() => window.benchHarness !== undefined);
		await page.evaluate((o) => window.benchHarness.open(o), opts);
		return new BenchTab(page);
	}

	start(): Promise<SyncSummary> {
		return this.page.evaluate(() => window.benchHarness.start());
	}

	startWithFirstRead(probeLimit = 20): Promise<FirstReadSummary> {
		return this.page.evaluate((n) => window.benchHarness.startWithFirstRead(n), probeLimit);
	}

	syncNow(): Promise<SyncSummary> {
		return this.page.evaluate(() => window.benchHarness.syncNow());
	}

	setScope(scope: ScopeName, opts?: ScopeOptions): Promise<SyncSummary> {
		return this.page.evaluate(
			(a) => window.benchHarness.setScope(a.scope, a.opts),
			{ scope, opts },
		);
	}

	read(op: ReadOp): Promise<ReadResult> {
		return this.page.evaluate((o) => window.benchHarness.read(o), op);
	}

	waitForIndexes(timeoutMs = 600_000): Promise<IndexBuildView> {
		return this.page.evaluate((t) => window.benchHarness.waitForIndexes(t), timeoutMs);
	}

	counts(): Promise<RowCounts> {
		return this.page.evaluate(() => window.benchHarness.counts());
	}

	storage(): Promise<StorageView> {
		return this.page.evaluate(() => window.benchHarness.storage());
	}

	writeBytes(): Promise<WriteBytesView> {
		return this.page.evaluate(() => window.benchHarness.writeBytes());
	}

	resetWriteBytes(): Promise<void> {
		return this.page.evaluate(() => window.benchHarness.resetWriteBytes());
	}

	queueWrites(count: number, firstIndex: number): Promise<number> {
		return this.page.evaluate(
			(a) => window.benchHarness.queueWrites(a.count, a.firstIndex),
			{ count, firstIndex },
		);
	}

	localWrites(op: 'put' | 'patch', count: number, firstIndex: number): Promise<LocalWriteStats> {
		return this.page.evaluate(
			(a) => window.benchHarness.localWrites(a.op, a.count, a.firstIndex),
			{ op, count, firstIndex },
		);
	}

	openWatches(specs: WatchSpec[]): Promise<void> {
		return this.page.evaluate((s) => window.benchHarness.openWatches(s), specs);
	}

	watchStats(): Promise<WatchView[]> {
		return this.page.evaluate(() => window.benchHarness.watchStats());
	}

	resetWatchStats(): Promise<void> {
		return this.page.evaluate(() => window.benchHarness.resetWatchStats());
	}

	waitForWatch(index: number, minCallbacks: number, timeoutMs = 30_000): Promise<number> {
		return this.page.evaluate(
			(a) => window.benchHarness.waitForWatch(a.index, a.minCallbacks, a.timeoutMs),
			{ index, minCallbacks, timeoutMs },
		);
	}

	closeWatches(): Promise<void> {
		return this.page.evaluate(() => window.benchHarness.closeWatches());
	}

	paginate(op: PageWalkOp): Promise<PageWalkResult> {
		return this.page.evaluate((o) => window.benchHarness.paginate(o), op);
	}

	pendingCount(): Promise<number> {
		return this.page.evaluate(() => window.benchHarness.pendingCount());
	}

	waitForTitle(issueId: string, prefix: string, timeoutMs = 30_000): Promise<number> {
		return this.page.evaluate(
			(a) => window.benchHarness.waitForTitle(a.issueId, a.prefix, a.timeoutMs),
			{ issueId, prefix, timeoutMs },
		);
	}

	events(): Promise<EventRecord[]> {
		return this.page.evaluate(() => window.benchHarness.events());
	}

	resetEvents(): Promise<void> {
		return this.page.evaluate(() => window.benchHarness.resetEvents());
	}

	heapBytes(): Promise<number> {
		return this.page.evaluate(() => window.benchHarness.heapBytes());
	}

	/**
	 * Closes the client, then the tab — how a user closes a tab, in order.
	 *
	 * A page that has gone away takes the client with it, so a missing harness
	 * is nothing left to close rather than a failure. Throwing here would run
	 * inside the caller's `finally` and replace whatever error sent it there,
	 * which is how a crashed tab comes back reported as a missing property.
	 */
	async closeTab(): Promise<void> {
		await this.page.evaluate(async () => {
			await window.benchHarness?.close();
		}).catch(() => {});
		await this.page.close();
	}
}
