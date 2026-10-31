// Conformance vectors shared with the Go engine
// (engine/fractional/vectors_test.go): both sides must make the same order key
// from the same bounds, and refuse the same bounds.

import { readFileSync } from 'node:fs';

import { describe, expect, it } from 'vitest';

import { generateKeyBetween, generateNKeysBetween } from '../../src/fractional.js';
import { generateKeyBetween as generateKeyBetweenFor } from '../../src/ordering.js';

type Bound = string | null;

interface Vectors {
	between: { name: string; a: Bound; b: Bound; want: string }[];
	nBetween: { name: string; a: Bound; b: Bound; n: number; want: string[] }[];
	actorBetween: { name: string; a: Bound; b: Bound; actor: string; want: string }[];
	rejects: { name: string; a: Bound; b: Bound; error: 'invalidKey' | 'outOfOrder' }[];
	walks: { name: string; actor?: string; gaps: number[]; want: string[] }[];
}

const vectors = JSON.parse(
	readFileSync(new URL('../../../testdata/fractional/vectors.json', import.meta.url), 'utf-8'),
) as Vectors;

describe('fractional conformance vectors', () => {
	it('has cases', () => {
		expect(vectors.between.length).toBeGreaterThan(0);
		expect(vectors.rejects.length).toBeGreaterThan(0);
		expect(vectors.walks.length).toBeGreaterThan(0);
	});

	describe('between', () => {
		for (const c of vectors.between) {
			it(c.name, () => {
				const got = generateKeyBetween(c.a, c.b);
				expect(got).toBe(c.want);
				if (c.a !== null) expect(got > c.a).toBe(true);
				if (c.b !== null) expect(got < c.b).toBe(true);
			});
		}
	});

	describe('nBetween', () => {
		for (const c of vectors.nBetween) {
			it(c.name, () => {
				expect(generateNKeysBetween(c.a, c.b, c.n)).toEqual(c.want);
			});
		}
	});

	describe('actorBetween', () => {
		for (const c of vectors.actorBetween) {
			it(c.name, () => {
				const got = generateKeyBetweenFor(c.a, c.b, c.actor);
				expect(got).toBe(c.want);
				if (c.a !== null) expect(got > c.a).toBe(true);
				if (c.b !== null) expect(got < c.b).toBe(true);
			});
		}
	});

	describe('rejects', () => {
		for (const c of vectors.rejects) {
			it(c.name, () => {
				expect(() => generateKeyBetween(c.a, c.b)).toThrow();
				expect(() => generateKeyBetweenFor(c.a, c.b, 'actor')).toThrow();
				expect(() => generateNKeysBetween(c.a, c.b, 2)).toThrow();
			});
		}
	});

	describe('walks', () => {
		for (const w of vectors.walks) {
			it(w.name, () => {
				const list: string[] = [];
				for (const gap of w.gaps) {
					const i = Math.min(gap, list.length);
					const a = i === 0 ? null : (list[i - 1] as string);
					const b = i === list.length ? null : (list[i] as string);
					const key = w.actor === undefined
						? generateKeyBetween(a, b)
						: generateKeyBetweenFor(a, b, w.actor);
					list.splice(i, 0, key);
				}
				expect(list).toEqual(w.want);
				for (let i = 1; i < list.length; i++) {
					expect((list[i - 1] as string) < (list[i] as string)).toBe(true);
				}
			});
		}
	});
});
