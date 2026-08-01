import type { BrowserContext, Page } from '@playwright/test';

import { SERVER_URL_ENV, TIERS_ENV } from './global-setup.js';
import type {
	EventRecord,
	FirstReadSummary,
	OpenOptions,
	ReadOp,
	ReadResult,
	RowCounts,
	ScopeName,
	ScopeOptions,
	StorageView,
	SyncSummary,
	WriteBytesView,
} from './harness-api.js';
import type { Wire } from './report.js';

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

export interface WireStats {
	total: Record<string, number>;
	bytesIn: Record<string, number>;
	bytesOut: Record<string, number>;
	gzipIn: Record<string, number>;
	gzipOut: Record<string, number>;
	encodedOut: Record<string, number>;
	wireIn: Record<string, number>;
	wireOut: Record<string, number>;
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
	};
}

/** Brackets `fn` with wire snapshots, returning its result and what it cost. */
export async function withWire<T>(fn: () => Promise<T>): Promise<{ result: T; wire: Wire }> {
	const before = await wireStats();
	const result = await fn();
	return { result, wire: wireDelta(before, await wireStats()) };
}

/** Server-side writes against a seeded corpus; see benchserver's writeHandler. */
export async function serverWrite(
	tierName: string,
	op: 'patch' | 'insert' | 'delete',
	count: number,
	/** Title prefix for a patch, so a waiter can distinguish this write. */
	marker?: string,
): Promise<void> {
	const res = await fetch(new URL('/debug/write', serverUrl()), {
		method: 'POST',
		body: JSON.stringify({
			workspace: tier(tierName).workspace,
			tier: tierName,
			op,
			count,
			marker,
		}),
	});
	if (!res.ok) throw new Error(`/debug/write responded ${res.status}: ${await res.text()}`);
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
 * A browser tab holding one SyncClient against the bench corpus. Every method
 * is a typed round-trip into the page harness; see harness-api.ts.
 */
export class BenchTab {
	private constructor(readonly page: Page) {}

	static async open(context: BrowserContext, opts: OpenOptions): Promise<BenchTab> {
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

	/** Closes the client, then the tab — how a user closes a tab, in order. */
	async closeTab(): Promise<void> {
		await this.page.evaluate(() => window.benchHarness.close());
		await this.page.close();
	}
}
