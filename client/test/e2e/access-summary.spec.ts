import { expect, test } from '@playwright/test';

import {
	accessServerUrl,
	activeCalls,
	freshWorkspace,
	Tab,
	totalCalls,
	type TrustedWrite,
	trustedWrite,
} from './fixtures.js';
import type { AccessWireView } from './harness-api.js';

/**
 * The CallerAccess summary across tabs that share one store. The leader tab
 * applies each change of the caller's grants from its Subscribe stream, and
 * the other tabs pull; both name the summary version the shared store holds,
 * and the server answers with a delta from that version. When the other
 * channel moved the store on while a delta was in flight, the delta applies
 * to a version the store no longer holds. The client must then keep the
 * summary it has and pull again, so every tab ends on the newest grants and no
 * tab ever predicts without a summary.
 *
 * Only this tier has tabs that share an IndexedDB store, so it is the only
 * tier where a stream and a pull race on one stored summary. The race depends
 * on timing, so the spec changes the grants in bursts until the tabs report
 * that it ran (see AccessWireView), and fails when it never does.
 */

const USER = 'mia';
/** Grant changes in one burst, each its own transaction and summary version. */
const ROUNDS_PER_BURST = 10;
/** Bursts to run before the spec gives up on seeing the race. */
const MAX_BURSTS = 8;
/**
 * Pause between two rounds of a burst, in ms, by burst. Which spacing makes
 * the race likely depends on how fast the browser runs a sync cycle, so the
 * bursts try several.
 */
const SPACING_MS = [ 0, 15, 30 ];
/**
 * Projects the caller holds a grant at: p0, which keeps its grant, and one
 * per round. Enough rows that the server sends deltas, not whole summaries.
 */
const PROJECTS = 1 + ROUNDS_PER_BURST * MAX_BURSTS;

/** The caller's grant at project `p<i>`, with `role`. */
function grant(i: number, role: string): TrustedWrite {
	return {
		entity: 'project_member',
		value: { id: `pm-${USER}-p${i}`, project: { id: `p${i}` }, userId: USER, role },
	};
}

/**
 * How many times the tabs saw a delta that applies to another summary than
 * the stored one, since `before`: a follower pulls once more in its sync
 * cycle, and the leader, which has no other reason to pull, pulls from the
 * background.
 */
function racesSince(before: AccessWireView[], now: AccessWireView[]): number {
	const [ leaderBefore, ...followersBefore ] = before;
	const [ leaderNow, ...followersNow ] = now;
	let races = leaderNow!.backgroundPulls - leaderBefore!.backgroundPulls;
	for (const [ i, follower ] of followersNow.entries()) {
		races += follower.repulls - followersBefore[i]!.repulls;
	}
	return races;
}

test.describe('caller access across tabs', () => {
	test.beforeEach(async () => {
		// Streams from a previous spec's context are torn down asynchronously.
		await expect.poll(() => activeCalls('Subscribe', accessServerUrl())).toBe(0);
	});

	test('grant changes in quick succession reach every tab, which never lose the summary', async ({ context }) => {
		const ws = freshWorkspace();
		await trustedWrite(ws, [
			...Array.from({ length: PROJECTS }, (_, i) => ({
				entity: 'project',
				value: { id: `p${i}`, name: `project ${i}` },
			})),
			...Array.from({ length: PROJECTS }, (_, i) => grant(i, 'project_reader')),
		]);

		// The leader is alone when it wins the lock, so the other two are the
		// tabs that pull. No tab polls or writes: the stream and the pulls the
		// spec asks for are the only traffic.
		const opts = {
			workspaceId: ws,
			user: USER,
			streamEnabled: true,
			pollIntervalMs: 0,
			start: true,
		};
		const leader = await Tab.open(context, opts);
		await expect.poll(() => activeCalls('Subscribe', accessServerUrl())).toBe(1);
		const followers = [ await Tab.open(context, opts), await Tab.open(context, opts) ];
		const tabs = [ leader, ...followers ];
		for (const tab of tabs) {
			await expect.poll(() => tab.hasPermission('project', 'p0', 'view_members')).toBe('yes');
		}
		const streams = await totalCalls('Subscribe', accessServerUrl());
		const before = await Promise.all(tabs.map((tab) => tab.accessWire()));

		// p0 keeps its grant through every round, so its prediction must stay
		// 'yes' in every tab. A tab without a summary says 'unknown:no_summary'.
		for (const tab of tabs) await tab.startSampling('project', 'p0', 'view_members');

		let round = 0;
		let races = 0;
		for (let burst = 0; burst < MAX_BURSTS && races === 0; burst++) {
			// The followers pull without pause while the grants change, so their
			// pulls overlap the leader's stream pages. Each round makes the caller
			// admin of one more project and takes admin away at the one before.
			let pulling = true;
			const pulls = followers.map(async (tab) => {
				while (pulling) await tab.syncNow();
			});
			const spacing = SPACING_MS[burst % SPACING_MS.length]!;
			try {
				for (let i = 0; i < ROUNDS_PER_BURST; i++) {
					round++;
					await trustedWrite(ws, [
						grant(round, 'project_admin'),
						...(round > 1 ? [ grant(round - 1, 'project_reader') ] : []),
					]);
					if (spacing > 0) await leader.page.waitForTimeout(spacing);
				}
			} finally {
				pulling = false;
				await Promise.all(pulls);
			}

			// No tab syncs from here on: what is left to arrive comes on the
			// stream, and from the pull a stale delta asks for.
			for (const [ i, tab ] of tabs.entries()) {
				await expect.poll(
					() => tab.hasPermission('project', `p${round}`, 'manage_members'),
					{ message: `tab ${i}: admin at p${round}`, timeout: 10_000 },
				).toBe('yes');
				expect(
					await tab.hasPermission('project', `p${round - 1}`, 'manage_members'),
					`tab ${i}: admin taken away at p${round - 1}`,
				).toBe('no');
			}
			races = racesSince(before, await Promise.all(tabs.map((tab) => tab.accessWire())));
		}

		const wire = await Promise.all(tabs.map((tab) => tab.accessWire()));
		test.info().annotations.push({
			type: 'access wire',
			description: JSON.stringify({ rounds: round, races, before, wire }),
		});
		expect(races, `a stale delta in ${round} grant changes`).toBeGreaterThan(0);
		// The summary changes travelled as deltas both ways.
		expect(wire[0]!.streamDelta, 'leader: deltas on the stream').toBeGreaterThan(0);
		expect(
			wire.slice(1).reduce((n, w) => n + w.pullDelta, 0),
			'followers: deltas in pull responses',
		).toBeGreaterThan(0);

		for (const [ i, tab ] of tabs.entries()) {
			for (let revoked = 1; revoked < round; revoked++) {
				expect(
					await tab.hasPermission('project', `p${revoked}`, 'manage_members'),
					`tab ${i}: admin taken away at p${revoked}`,
				).toBe('no');
			}
			const seen = await tab.stopSampling();
			expect(seen.samples, `tab ${i}: predictions made`).toBeGreaterThan(0);
			expect(seen.answers, `tab ${i}: predictions at p0`).toEqual({ yes: seen.samples });
			expect(await tab.rejections()).toEqual([]);
		}

		// A stale delta on the stream asks for a pull, not for a new stream.
		expect(await activeCalls('Subscribe', accessServerUrl())).toBe(1);
		expect(await totalCalls('Subscribe', accessServerUrl())).toBe(streams);
	});
});
