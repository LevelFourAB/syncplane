import { expect, test } from '@playwright/test';

import { BenchTab, freshAccount, type Tier, tiers, wireDelta, wireStats } from './fixtures.js';
import type { WatchSpec } from './harness-api.js';
import { report } from './report.js';

/**
 * What one local write costs, at the size where it starts to matter.
 *
 * Every keystroke-adjacent interaction is a put or a patch: the row is written
 * to the overlay, every declared index is maintained, and every open watch is
 * notified — synchronously, before the await returns, which is why this is a
 * latency a user can feel. Bootstrap's `swapWritesPerRow` prices write
 * amplification once per sync; nothing else prices it per interaction, or says
 * how it grows with the store.
 *
 * Four rounds per tier. Three of them run OFFLINE, so the timed loop prices the
 * local pipeline and not whichever push cycle happened to overlap it: patches
 * (mask of title+updatedAtMs — the recency indexes move), puts (whole fresh
 * rows with two labels — every index gains positions), and patches again with
 * twenty watches open (the same write with a board on screen). Percentiles
 * rather than means: one stalled write among two hundred is a jank frame.
 *
 * Going online afterwards and draining the queue prices the push separately,
 * and puts the corpus in a state later specs can account for (the patched rows
 * carry markers, the puts live far past the seeded key range).
 *
 * The fourth round is the same twenty-watch patch burst ONLINE, and it answers
 * a different question: what a rebase costs the watches that are open while it
 * runs. Each pushed batch comes back as a rebase — clear the overlay, replay
 * what is still queued — and every open watch has to bring its result up to
 * date behind it. `drainMs` is the whole round, from the first write until the
 * queue is empty, against `totalMs` for the writes alone; `rebases` and
 * `deliveries` say how much of the gap was per-rebase watch maintenance.
 */

const WRITES = 200;
/** Key range for puts: far past anything seeded or inserted, so a put is a
 * fresh row and two runs cannot collide. */
const PUT_BASE = 90_000_000;

/**
 * The watches a watched round runs under: the three cross-cutting board queries
 * a screen holds, an entity watch over every issue, and one board per project
 * to make up `count`. Shared by the offline and online rounds so the two price
 * the same open results.
 */
function watchSpecs(t: Tier, count: number): WatchSpec[] {
	return [
		{ kind: 'board', projectId: 'project-00000000', limit: 50 },
		{ kind: 'triage', limit: 50 },
		{ kind: 'byLabel', label: 'label-00000001', limit: 50 },
		{ kind: 'issues' },
		...Array.from(
			{ length: count - 4 },
			(_, i): WatchSpec => ({
				kind: 'board',
				projectId: `project-${String(i % t.projects).padStart(8, '0')}`,
				limit: 50,
			}),
		),
	];
}

/**
 * Blocks until the client's pending queue is empty.
 *
 * Throws past `timeoutMs` rather than returning: a drain time that did not
 * include the drain is a number nobody can read.
 */
async function awaitDrained(tab: BenchTab, timeoutMs = 180_000): Promise<void> {
	const started = Date.now();
	for (;;) {
		const pending = await tab.pendingCount();
		if (pending === 0) return;
		if (Date.now() - started > timeoutMs) {
			throw new Error(
				`pending queue still holds ${pending} mutations after ${timeoutMs}ms — the online `
					+ 'round never drained, so its drainMs would describe the timeout instead',
			);
		}
		await new Promise((resolve) => setTimeout(resolve, 10));
	}
}

test.describe('local write cost', () => {
	for (const t of tiers()) {
		test(`tier ${t.name} (${t.total} rows)`, async ({ context }) => {
			const scope = t.name === 'xl' ? 'window' : 'full';
			const tab = await BenchTab.open(context, {
				workspaceId: t.workspace,
				accountId: freshAccount(`writes-${t.name}`),
				scope,
				windowLimit: 500,
				measureWrites: true,
			});
			try {
				await tab.start();
				await tab.waitForIndexes();
				await context.setOffline(true);

				const round = async (
					name: string,
					op: 'put' | 'patch',
					firstIndex: number,
					watches: number,
				) => {
					if (watches > 0) await tab.openWatches(watchSpecs(t, watches));
					await tab.resetWriteBytes();
					const stats = await tab.localWrites(op, WRITES, firstIndex);
					const bytes = await tab.writeBytes();
					if (watches > 0) await tab.closeWatches();
					report({
						scenario: 'local write',
						tier: t.name,
						scope,
						metrics: {
							round: name,
							writes: WRITES,
							p50Ms: stats.p50Ms,
							p95Ms: stats.p95Ms,
							maxMs: stats.maxMs,
							totalMs: stats.totalMs,
							bytesPerWrite: Math.round(bytes.total / WRITES),
						},
					});
				};

				// Hot rows: the newest issues are what a recency window holds and
				// what the ordered indexes rank first, so a patch there does the
				// most representative index maintenance.
				await round('patch', 'patch', t.issues - WRITES, 0);
				await round('put', 'put', PUT_BASE, 0);
				await round('patch+20w', 'patch', t.issues - WRITES, 20);

				// Drain online: the push is its own number, and later specs must
				// not inherit an unpushed queue.
				await context.setOffline(false);
				const before = await wireStats();
				const drain = await tab.syncNow();
				const wire = wireDelta(before, await wireStats());
				expect(await tab.pendingCount()).toBe(0);
				report({
					scenario: 'push drain',
					tier: t.name,
					scope,
					metrics: {
						mutations: drain.pushMutations,
						pushes: drain.pushes,
						pushMs: drain.pushMs,
						wallMs: drain.wallMs,
					},
					wire: { ...wire, localMs: drain.wallMs, of: 'wallMs' },
				});

				// The same burst with the same watches open, but ONLINE: the
				// coordinator pushes and rebases underneath the writes, and every
				// open watch has to be brought up to date behind each rebase. A
				// fresh key range, so this prices rows the rounds above left alone.
				await tab.openWatches(watchSpecs(t, 20));
				const watchedBefore = await tab.watchStats();
				await tab.resetEvents();
				const startedAt = performance.now();
				const online = await tab.localWrites('patch', WRITES, t.issues - 2 * WRITES);
				await awaitDrained(tab);
				const drainMs = performance.now() - startedAt;
				const events = await tab.events();
				const watchedAfter = await tab.watchStats();
				await tab.closeWatches();
				report({
					scenario: 'local write',
					tier: t.name,
					scope,
					metrics: {
						round: 'patch+20w online',
						writes: WRITES,
						p50Ms: online.p50Ms,
						p95Ms: online.p95Ms,
						maxMs: online.maxMs,
						totalMs: online.totalMs,
						drainMs,
						pushes: events.filter((e) => e.type === 'push').length,
						rebases: events.filter((e) => e.type === 'rebase').length,
						deliveries: watchedAfter.reduce(
							(sum, w, i) => sum + w.callbacks - (watchedBefore[i]?.callbacks ?? 0),
							0,
						),
					},
				});
			} finally {
				await tab.closeTab();
			}
		});
	}
});
