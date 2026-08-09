import { expect, test } from '@playwright/test';

import { BenchTab, freshAccount, tiers, wireDelta, wireStats } from './fixtures.js';
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
 * Three rounds per tier: patches (mask of title+updatedAtMs — the recency
 * indexes move), puts (whole fresh rows with two labels — every index gains
 * positions), and patches again with twenty watches open (the same write with
 * a board on screen). Percentiles rather than means: one stalled write among
 * two hundred is a jank frame.
 *
 * The writes happen OFFLINE so the timed loop prices the local pipeline, not
 * whichever push cycles happened to overlap it; going online afterwards and
 * draining the queue prices the push separately, and puts the corpus in a
 * state later specs can account for (the patched rows carry markers, the puts
 * live far past the seeded key range).
 */

const WRITES = 200;
/** Key range for puts: far past anything seeded or inserted, so a put is a
 * fresh row and two runs cannot collide. */
const PUT_BASE = 90_000_000;

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
					if (watches > 0) {
						await tab.openWatches([
							{ kind: 'board', projectId: 'project-00000000', limit: 50 },
							{ kind: 'triage', limit: 50 },
							{ kind: 'byLabel', label: 'label-00000001', limit: 50 },
							{ kind: 'issues' },
							...Array.from(
								{ length: watches - 4 },
								(_, i): { kind: 'board'; projectId: string; limit: number } => ({
									kind: 'board',
									projectId: `project-${String(i % t.projects).padStart(8, '0')}`,
									limit: 50,
								}),
							),
						]);
					}
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
			} finally {
				await tab.closeTab();
			}
		});
	}
});
