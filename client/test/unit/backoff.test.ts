import { afterEach, describe, expect, it, vi } from 'vitest';

import { Backoff } from '../../src/sync/backoff.js';

afterEach(() => {
	vi.restoreAllMocks();
});

/** Pins Math.random so a delay is a function of the attempt alone. */
function draw(at: number): void {
	vi.spyOn(Math, 'random').mockReturnValue(at);
}

describe('Backoff', () => {
	it('draws the first delay from anywhere in the base window', () => {
		draw(0);
		expect(new Backoff(1_000, 30_000).next()).toBe(0);
		draw(0.5);
		expect(new Backoff(1_000, 30_000).next()).toBe(500);
		draw(0.999);
		expect(new Backoff(1_000, 30_000).next()).toBeCloseTo(999, 0);
	});

	// The whole point of the first delay reaching down to zero: every client a
	// lost node drops retries within one base window of the same instant, so a
	// floor at half the window would halve the spread of the reconnect surge.
	it('spreads a fleet of clients across the whole base window', () => {
		vi.spyOn(Math, 'random').mockRestore();
		const delays = Array.from({ length: 500 }, () => new Backoff(1_000, 30_000).next());
		expect(Math.min(...delays)).toBeLessThan(100);
		expect(Math.max(...delays)).toBeGreaterThan(900);
	});

	it('doubles the window per attempt and holds at the ceiling', () => {
		draw(1);
		const b = new Backoff(1_000, 8_000);
		expect([ b.next(), b.next(), b.next(), b.next(), b.next() ])
			.toEqual([ 1_000, 2_000, 4_000, 8_000, 8_000 ]);
	});

	it('returns to the base window on reset', () => {
		draw(1);
		const b = new Backoff(1_000, 30_000);
		b.next();
		b.next();
		b.reset();
		expect(b.next()).toBe(1_000);
	});
});
