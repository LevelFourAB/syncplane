// Conformance vectors shared with the Go engine (engine/blob): both sides must
// name the same bytes with the same digest and the same chunk hashes.

import { readFileSync } from 'node:fs';

import { describe, expect, it } from 'vitest';

import {
	BLOB_CHUNK_SIZE,
	chunkCount,
	chunkLength,
	digestOf,
	hashBlob,
	isBlobDigest,
	toHex,
} from '../../src/blob/digest.js';
import { mod251 } from '../support/blobserver.js';

interface Vectors {
	chunkSize: number;
	cases: {
		name: string;
		size: number;
		pattern: 'mod251' | 'text';
		text?: string;
		digest: string;
		chunkHashes: string[];
	}[];
}

const vectors = JSON.parse(
	readFileSync(new URL('../../../testdata/blob/vectors.json', import.meta.url), 'utf-8'),
) as Vectors;

function bytesOf(c: Vectors['cases'][number]): Uint8Array {
	return c.pattern === 'text' ? new TextEncoder().encode(c.text ?? '') : mod251(c.size);
}

describe('blob digest vectors', () => {
	it('agrees on the chunk size', () => {
		expect(vectors.chunkSize).toBe(BLOB_CHUNK_SIZE);
	});

	it('has cases', () => {
		expect(vectors.cases.length).toBeGreaterThan(0);
	});

	it.each(vectors.cases.map((c) => [ c.name, c ] as const))('%s', async (_name, c) => {
		const data = bytesOf(c);
		expect(data.length).toBe(c.size);
		const seen: number[] = [];
		const got = await hashBlob(
			new Blob([ data as Uint8Array<ArrayBuffer> ]),
			async (index, chunk) => {
				seen.push(index);
				expect(chunk.length).toBe(chunkLength(c.size, index));
			},
		);
		expect(got.digest).toBe(c.digest);
		expect(got.chunkHashes.map(toHex)).toEqual(c.chunkHashes);
		expect(seen).toEqual([ ...Array(chunkCount(c.size)).keys() ]);
		expect(isBlobDigest(got.digest)).toBe(true);
	});
});

describe('digestOf', () => {
	it('commits to the size: an empty blob hashes eight zero bytes', async () => {
		expect(await digestOf(0, [])).toBe(
			'sp1:af5570f5a1810b7af78caf4bc70a660f0df51e42baf91d4de5b2328de0e83dfc',
		);
	});

	it('gives the same chunk hashes under another size another digest', async () => {
		const hashes = [ new Uint8Array(32) ];
		expect(await digestOf(1, hashes)).not.toBe(await digestOf(2, hashes));
	});
});

describe('isBlobDigest', () => {
	it.each([
		[ 'sp1:' + 'a'.repeat(64), true ],
		[ 'sp1:' + 'A'.repeat(64), false ],
		[ 'sp1:' + 'a'.repeat(63), false ],
		[ 'sp2:' + 'a'.repeat(64), false ],
		[ '', false ],
	])('%s → %s', (s, want) => {
		expect(isBlobDigest(s)).toBe(want);
	});
});
