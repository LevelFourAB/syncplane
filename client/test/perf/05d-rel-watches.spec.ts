import { expect, test } from '@playwright/test';

import { BenchTab, freshAccount, REL_FLIP, serverWrite, tiers } from './fixtures.js';
import type { WatchSpec } from './harness-api.js';
import { report } from './report.js';

/**
 * What holding a live query that CROSSES A REFERENCE costs while changes
 * arrive.
 *
 * A `matches` is answered in two phases (docs/explanation/relational-queries.md):
 * a pre-pass runs the inner condition over the target entity and collects the
 * matching keys K, then the node is rewritten to a membership test on the
 * stored stubs and the query runs row-local. A one-shot query pays that once. A
 * WATCH pays it again on every commit that touches the target entity — and a
 * commit to the target moves which base rows match without touching any of
 * them, which no keyed delta can carry.
 *
 * So the cost splits in two, and the two grow for different reasons:
 *
 *   - **K did not move.** The `patch` arm: titles change, the inner predicate
 *     is over state and priority, so the pre-pass returns what it returned
 *     before. The watch delivers NOTHING — asserted below — and the whole cost
 *     is invisible maintenance, which is why the wall time is the number.
 *   - **K moved.** The `flip` arm: state and priority change, so keys enter and
 *     leave. The base entity's match set has to be re-derived from rows the
 *     commit never named.
 *
 * Each arm is measured at 1 and 8 open watches against a no-watch round taken
 * immediately before it, and `perWatchMs` is the difference over the count —
 * what one watch costs a catch-up cycle. The `comments` control watches the
 * same entity in the same order with the semi-join removed, so the columns
 * separate what a commit costs a comment watch from what crossing a reference
 * adds to it. `flat` is the second control: an entity no write here touches,
 * asserted to zero deliveries.
 *
 * `resets` counts deliveries that carried no edit script. These watches hold a
 * page, and a windowed watcher reports reset whatever moved its result — its
 * edit indices are into the match set, not the page — so the column is a
 * constant here rather than a measure of how the result was arrived at. It is
 * kept because a change in it means a change in how often these deliver at all.
 *
 * A caveat on `perWatchMs`: a no-watch cycle keeps getting cheaper over the
 * first dozen rounds of a run — past the warm-up above, the later arm's
 * baselines still land several times lower than the earlier arm's — so the
 * subtraction is only trustworthy within one round pair. Compare two RUNS of
 * this spec by `wallMs` at the same row instead, where whatever causes the
 * slope is the same on both sides.
 *
 * Scope `full`, so the whole corpus is local and a re-derivation is priced at
 * the entity's real size rather than a window's. Writes to the corpus, hence
 * the position: after the read-only specs. The flipped range walks forward from
 * a quarter into the id space, away from the newest end the other write specs
 * and 07's deletion churn share.
 */

const LIMIT = 50;
const CHANGES = 200;
const WATCH_COUNTS = [ 1, 8 ];
const WARMUP_ROUNDS = 8;

/** The semi-join watch, and the row-local control it is read against. */
function relWatches(count: number): WatchSpec[] {
	return Array.from({ length: count }, (): WatchSpec => ({
		kind: 'relComments',
		state: REL_FLIP.state,
		priority: REL_FLIP.priority,
		limit: LIMIT,
	}));
}

function plainWatches(count: number): WatchSpec[] {
	return Array.from({ length: count }, (): WatchSpec => ({ kind: 'comments', limit: LIMIT }));
}

test.describe('rel watches', () => {
	for (const t of tiers()) {
		test(`catch-up under semi-join watches — tier ${t.name} (${t.total} rows)`, async ({ context }) => {
			test.skip(t.name === 's', 'too few rows for a re-derivation to be distinguishable');
			test.skip(t.name === 'xl', 'a full local store at 500k rows is priced by the windowed tiers');
			const tab = await BenchTab.open(context, {
				workspaceId: t.workspace,
				accountId: freshAccount(`rel-watches-${t.name}`),
				scope: 'full',
			});
			try {
				await tab.start();
				await tab.waitForIndexes();
				// Changes an earlier spec left behind would land in the first
				// round's pull and break the rounds' comparability.
				await tab.syncNow();
				// Away from the newest end that the other write specs share, with
				// room for every round to walk fresh rows: each one advances by
				// CHANGES, and a round past the seeded range would patch keys no
				// row has.
				let first = Math.floor(t.issues / 4);
				// Discarded rounds. A catch-up taken straight after a bootstrap
				// competes with the store still settling that bootstrap's writes,
				// and prices it: the no-watch baseline falls by most of an order
				// of magnitude over the first few cycles before it holds steady.
				// Measuring inside that slope is how a round WITH watches open
				// comes out faster than one without.
				for (let i = 0; i < WARMUP_ROUNDS; i++) {
					await serverWrite(t.name, 'patch', CHANGES, `rel-warmup${i}`, first);
					first += CHANGES;
					await tab.syncNow();
				}
				/** One catch-up cycle of CHANGES writes, with `specs` held open. */
				const round = async (arm: 'patch' | 'flip', specs: WatchSpec[]) => {
					if (specs.length > 0) await tab.openWatches(specs);
					await tab.resetWatchStats();
					await tab.resetEvents();
					await serverWrite(t.name, arm, CHANGES, `rel-${arm}${specs.length}`, first);
					first += CHANGES;
					const sync = await tab.syncNow();
					const stats = await tab.watchStats();
					await tab.closeWatches();
					return {
						wallMs: sync.wallMs,
						changes: sync.pullChanges,
						sum: (kind: WatchSpec['kind'], of: 'callbacks' | 'resets') =>
							stats.reduce((n, s, i) => specs[i]?.kind === kind ? n + s[of] : n, 0),
					};
				};

				for (const arm of [ 'patch', 'flip' ] as const) {
					for (const count of WATCH_COUNTS) {
						// The baseline is taken in the round immediately before the
						// one it is subtracted from, not once per arm: the store
						// keeps warming over a run, and a baseline taken six rounds
						// earlier prices a colder store than the round it explains.
						const base = await round(arm, []);
						const held = await round(arm, [
							...relWatches(count),
							...plainWatches(count),
							{ kind: 'flat' },
						]);
						expect(held.sum('flat', 'callbacks'), 'a watch on an untouched entity must not deliver')
							.toBe(0);
						if (arm === 'patch') {
							// The receipt that the memo works: a commit that moved no
							// membership key must be invisible to a semi-join watch,
							// however much it cost to find that out.
							expect(
								held.sum('relComments', 'callbacks'),
								'a commit that moved no membership key must not deliver',
							).toBe(0);
						}
						report({
							scenario: `rel watch catch-up (${arm})`,
							tier: t.name,
							scope: 'full',
							metrics: {
								watches: count,
								changes: held.changes,
								baseMs: base.wallMs,
								wallMs: held.wallMs,
								perWatchMs: (held.wallMs - base.wallMs) / count,
								relDeliveries: held.sum('relComments', 'callbacks'),
								relResets: held.sum('relComments', 'resets'),
								plainDeliveries: held.sum('comments', 'callbacks'),
								plainResets: held.sum('comments', 'resets'),
							},
						});
					}
				}
			} finally {
				await tab.closeTab();
			}
		});
	}
});
