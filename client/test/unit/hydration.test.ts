import { describe, expect, it } from 'vitest';

import { HydrationTracker } from '../../src/sync/hydration.js';

describe('HydrationTracker', () => {
	it('reports everything pending until rows arrive', () => {
		const h = new HydrationTracker();
		h.begin('ws', [ 'todo', 'widget' ]);
		expect([ ...h.pendingOf('ws')! ]).toEqual([ 'todo', 'widget' ]);
		expect(h.infoOf('ws')).toEqual({ pending: [ 'todo', 'widget' ], total: 2, done: 0 });
	});

	it('knows nothing about a workspace that is not cold-starting', () => {
		const h = new HydrationTracker();
		expect(h.pendingOf('ws')).toBeUndefined();
		expect(h.infoOf('ws')).toBeUndefined();
		expect(h.advance('ws', [ 'todo' ])).toEqual([]);
		expect(h.finish('ws')).toEqual([]);
	});

	it('replaces the seeded guess with the announced delivery order', () => {
		const h = new HydrationTracker();
		h.begin('ws', [ 'todo', 'widget' ]);
		h.order('ws', [ 'widget', 'todo', 'issue' ]);
		expect(h.infoOf('ws')).toEqual({
			pending: [ 'widget', 'todo', 'issue' ],
			total: 3,
			done: 0,
		});
	});

	it('keeps the seeded guess when the server announces nothing', () => {
		// An older server sends no order. Reporting everything pending until the
		// snapshot ends is the only honest answer: nothing says what has shipped.
		const h = new HydrationTracker();
		h.begin('ws', [ 'todo', 'widget' ]);
		h.order('ws', []);
		expect(h.advance('ws', [ 'todo' ])).toEqual([]);
		expect(h.infoOf('ws')?.done).toBe(0);
		expect(h.finish('ws')).toEqual([ 'todo', 'widget' ]);
	});

	it('completes every entity the server has moved past', () => {
		const h = new HydrationTracker();
		h.begin('ws', [ 'a', 'b', 'c' ]);
		h.order('ws', [ 'a', 'b', 'c' ]);
		// Rows of `a` prove nothing about `a` itself — more may follow.
		expect(h.advance('ws', [ 'a' ])).toEqual([]);
		// A chunk spanning the a→b boundary completes `a` and only `a`.
		expect(h.advance('ws', [ 'a', 'b' ])).toEqual([ 'a' ]);
		expect(h.infoOf('ws')).toEqual({ pending: [ 'b', 'c' ], total: 3, done: 1 });
		// `b` shipped nothing more and `c` started: `b` completes silently.
		expect(h.advance('ws', [ 'c' ])).toEqual([ 'b' ]);
		expect(h.finish('ws')).toEqual([ 'c' ]);
		expect(h.pendingOf('ws')).toBeUndefined();
	});

	it('never goes backwards on a late chunk', () => {
		const h = new HydrationTracker();
		h.begin('ws', [ 'a', 'b', 'c' ]);
		h.order('ws', [ 'a', 'b', 'c' ]);
		expect(h.advance('ws', [ 'c' ])).toEqual([ 'a', 'b' ]);
		expect(h.advance('ws', [ 'a' ])).toEqual([]);
		expect(h.infoOf('ws')?.done).toBe(2);
	});

	it('ignores rows of an entity the announcement did not name', () => {
		const h = new HydrationTracker();
		h.begin('ws', [ 'a', 'b' ]);
		h.order('ws', [ 'a', 'b' ]);
		expect(h.advance('ws', [ 'surprise' ])).toEqual([]);
		expect(h.infoOf('ws')?.done).toBe(0);
	});

	it('tracks workspaces independently', () => {
		const h = new HydrationTracker();
		h.begin('one', [ 'a', 'b' ]);
		h.begin('two', [ 'a', 'b' ]);
		h.order('one', [ 'a', 'b' ]);
		h.order('two', [ 'a', 'b' ]);
		expect(h.advance('one', [ 'b' ])).toEqual([ 'a' ]);
		expect(h.infoOf('two')?.done).toBe(0);
		h.finish('one');
		expect(h.pendingOf('two')).toBeDefined();
	});
});
