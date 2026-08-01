import { describe, expect, it } from 'vitest';

import { generateKeyBetween, generateNKeysBetween } from '../../src/fractional.js';

/** Deterministic PRNG (mulberry32) so failures reproduce. */
function prng(seed: number): () => number {
	let a = seed;
	return () => {
		a |= 0;
		a = (a + 0x6d2b79f5) | 0;
		let t = Math.imul(a ^ (a >>> 15), 1 | a);
		t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
}

describe('generateKeyBetween', () => {
	it('creates a first key and inserts before/after/between', () => {
		const first = generateKeyBetween(null, null);
		expect(first).toBe('a0');
		const after = generateKeyBetween(first, null);
		expect(after > first).toBe(true);
		const before = generateKeyBetween(null, first);
		expect(before < first).toBe(true);
		const mid = generateKeyBetween(before, first);
		expect(mid > before && mid < first).toBe(true);
	});

	it('rejects a >= b', () => {
		expect(() => generateKeyBetween('a1', 'a1')).toThrow();
		expect(() => generateKeyBetween('a2', 'a1')).toThrow();
	});

	it('rejects keys with trailing zeros', () => {
		expect(() => generateKeyBetween('a00', null)).toThrow();
	});

	it('property: random insertions stay ordered and unique', () => {
		const rand = prng(42);
		const keys: string[] = [ generateKeyBetween(null, null) ];
		for (let i = 0; i < 2000; i++) {
			// Pick a random gap, including before-first and after-last.
			const gap = Math.floor(rand() * (keys.length + 1));
			const a = gap === 0 ? null : (keys[gap - 1] as string);
			const b = gap === keys.length ? null : (keys[gap] as string);
			const key = generateKeyBetween(a, b);
			if (a !== null) expect(key > a).toBe(true);
			if (b !== null) expect(key < b).toBe(true);
			keys.splice(gap, 0, key);
		}
		const sorted = [ ...keys ].sort();
		expect(keys).toEqual(sorted);
		expect(new Set(keys).size).toBe(keys.length);
	});

	it('property: repeated append and prepend stay bounded and ordered', () => {
		let hi: string | null = null;
		let lo: string | null = null;
		let prevHi = '';
		let prevLo: string | undefined;
		for (let i = 0; i < 500; i++) {
			hi = generateKeyBetween(hi, null);
			lo = generateKeyBetween(null, lo);
			expect(hi > prevHi).toBe(true);
			if (prevLo !== undefined) expect(lo < prevLo).toBe(true);
			prevHi = hi;
			prevLo = lo;
			expect(hi.length).toBeLessThan(20);
			expect(lo.length).toBeLessThan(20);
		}
	});
});

describe('generateNKeysBetween', () => {
	it('returns n ordered keys in an open range', () => {
		const keys = generateNKeysBetween(null, null, 10);
		expect(keys).toHaveLength(10);
		expect([ ...keys ].sort()).toEqual(keys);
		expect(new Set(keys).size).toBe(10);
	});

	it('fits n keys between two adjacent keys', () => {
		const [ a, b ] = generateNKeysBetween(null, null, 2) as [string, string];
		const between = generateNKeysBetween(a, b, 25);
		expect(between).toHaveLength(25);
		const all = [ a, ...between, b ];
		expect([ ...all ].sort()).toEqual(all);
	});

	it('returns an empty array for n = 0', () => {
		expect(generateNKeysBetween(null, null, 0)).toEqual([]);
	});
});
