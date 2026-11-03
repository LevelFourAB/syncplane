import { describe, expect, it } from 'vitest';

import { BootstrapProgressTracker } from '../../src/sync/progress.js';

/** A tracker on a fake clock and timer, recording every publish. */
function tracked() {
	let t = 0;
	const timers: { at: number; fn: () => void; live: boolean }[] = [];
	const published: number[] = [];
	const p = new BootstrapProgressTracker({
		publish: (ws) => published.push(p.infoOf(ws)!.rowsApplied),
		now: () => t,
		intervalMs: 250,
		schedule: (fn, ms) => {
			const timer = { at: t + ms, fn, live: true };
			timers.push(timer);
			return () => {
				timer.live = false;
			};
		},
	});
	const advance = (ms: number) => {
		t += ms;
		for (const timer of timers) {
			if (timer.live && timer.at <= t) {
				timer.live = false;
				timer.fn();
			}
		}
	};
	const live = () => timers.filter((x) => x.live).length;
	return { p, advance, published, live };
}

describe('BootstrapProgressTracker', () => {
	it('counts committed rows against the summed estimate', () => {
		const { p, advance } = tracked();
		p.start('ws');
		p.begin('ws', [ 'a', 'b' ], [ 3n, 2n ], false);
		expect(p.infoOf('ws')).toEqual({ rowsApplied: 0, rowsEstimated: 5, entitiesDone: 0, entitiesTotal: 2 });

		p.written('ws', [ 'a', 'a' ]);
		expect(p.infoOf('ws')?.rowsApplied, 'a written chunk is not shown before it commits').toBe(0);
		advance(300);
		p.committed('ws', [ 'a', 'a' ]);
		p.written('ws', [ 'a', 'b' ]);
		advance(300);
		p.committed('ws', [ 'a', 'b' ]);
		expect(p.infoOf('ws')).toEqual({ rowsApplied: 4, rowsEstimated: 5, entitiesDone: 1, entitiesTotal: 2 });

		p.ended('ws');
		expect(p.infoOf('ws')?.entitiesDone).toBe(2);
		p.finish('ws');
		expect(p.infoOf('ws')).toBeUndefined();
	});

	it('publishes at most once per interval, and the newest count once the interval passes', () => {
		const { p, advance, published } = tracked();
		p.start('ws');
		p.begin('ws', [ 'a' ], [ 100n ], false);
		for (let i = 0; i < 10; i++) {
			advance(50);
			p.committed('ws', [ 'a' ]);
		}
		// The timer of each interval fires at its end, before the chunk that
		// lands at the same moment.
		expect(published).toEqual([ 4, 9 ]);
		expect(p.infoOf('ws')?.rowsApplied, 'the count is current between updates').toBe(10);

		// One more chunk, then the server sends nothing for a while.
		advance(10);
		p.committed('ws', [ 'a' ]);
		expect(published).toEqual([ 4, 9 ]);
		advance(1000);
		expect(published, 'the trailing update shows the last chunk, once').toEqual([ 4, 9, 11 ]);
	});

	it('drops a pending trailing update when the attempt ends', () => {
		const { p, advance, published, live } = tracked();
		p.start('ws');
		p.begin('ws', [ 'a' ], [ 100n ], false);
		p.committed('ws', [ 'a' ]);
		expect(live()).toBe(1);
		p.finish('ws');
		expect(live()).toBe(0);
		advance(1000);
		expect(published).toEqual([]);
	});

	it('lets an update the caller makes replace the pending trailing one', () => {
		const { p, advance, published, live } = tracked();
		p.start('ws');
		p.begin('ws', [ 'a' ], [ 100n ], false);
		p.committed('ws', [ 'a' ]);
		p.ended('ws');
		expect(live()).toBe(0);
		advance(1000);
		expect(published).toEqual([]);
	});

	it('reports an unknown size when the server sends no estimate', () => {
		const { p } = tracked();
		p.start('ws');
		// A server older than row_estimates.
		p.begin('ws', [ 'a', 'b' ], [], false);
		p.committed('ws', [ 'a' ]);
		expect(p.infoOf('ws')).toEqual({ rowsApplied: 1, rowsEstimated: 0, entitiesDone: 0, entitiesTotal: 2 });
	});

	it('treats an estimate list that does not match the entities as no estimate', () => {
		const { p } = tracked();
		p.start('ws');
		p.begin('ws', [ 'a', 'b' ], [ 7n ], false);
		expect(p.infoOf('ws')?.rowsEstimated).toBe(0);
	});

	it('never shows more rows applied than estimated', () => {
		const { p } = tracked();
		p.start('ws');
		p.begin('ws', [ 'a' ], [ 1n ], false);
		p.committed('ws', [ 'a', 'a', 'a' ]);
		expect(p.infoOf('ws')).toMatchObject({ rowsApplied: 3, rowsEstimated: 3 });
	});

	it('continues a resumed snapshot and keeps the smaller estimate', () => {
		const { p } = tracked();
		p.start('ws', { rows: 40, estimate: 100, entitiesDone: 1, entitiesTotal: 2 });
		expect(p.infoOf('ws')).toEqual({ rowsApplied: 40, rowsEstimated: 100, entitiesDone: 1, entitiesTotal: 2 });
		// The resumed Begin counts the whole snapshot again, with rows written
		// since the first attempt.
		p.begin('ws', [ 'a', 'b' ], [ 60n, 50n ], true);
		expect(p.infoOf('ws')?.rowsEstimated).toBe(100);
		expect(p.written('ws', [ 'b' ])).toEqual({ rows: 41, estimate: 100, entitiesDone: 1, entitiesTotal: 2 });
		p.committed('ws', [ 'b' ]);
		expect(p.infoOf('ws')?.rowsApplied).toBe(41);
	});

	it('starts over when the server refuses to resume', () => {
		const { p } = tracked();
		p.start('ws', { rows: 40, estimate: 100, entitiesDone: 1, entitiesTotal: 2 });
		p.begin('ws', [ 'a', 'b' ], [ 60n, 70n ], false);
		expect(p.infoOf('ws')).toEqual({ rowsApplied: 0, rowsEstimated: 130, entitiesDone: 0, entitiesTotal: 2 });
	});
});
