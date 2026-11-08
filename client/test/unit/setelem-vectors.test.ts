// Conformance vectors shared with the Go engine
// (engine/internal/naming/setelem_vectors_test.go): both sides must render a
// scalar set element to the same mask segment text.

import { readFileSync } from 'node:fs';

import { ScalarType } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { scalarKeyString } from '../../src/naming.js';

interface Vectors {
	cases: {
		name: string;
		kind: 'double' | 'float' | 'bytes';
		bits?: string;
		hex?: string;
		want: string;
	}[];
}

const vectors = JSON.parse(
	readFileSync(new URL('../../../testdata/setelem/vectors.json', import.meta.url), 'utf-8'),
) as Vectors;

function fromHex(hex: string): Uint8Array {
	const out = new Uint8Array(hex.length / 2);
	for (let i = 0; i < out.length; i++) out[i] = parseInt(hex.slice(2 * i, 2 * i + 2), 16);
	return out;
}

/** The value as protobuf-es holds it after a decode: a float32 widened to a number. */
function valueOf(c: Vectors['cases'][number]): { value: unknown; scalar: ScalarType } {
	switch (c.kind) {
		case 'double':
			return { value: new DataView(fromHex(c.bits!).buffer).getFloat64(0), scalar: ScalarType.DOUBLE };
		case 'float':
			return { value: new DataView(fromHex(c.bits!).buffer).getFloat32(0), scalar: ScalarType.FLOAT };
		case 'bytes':
			return { value: fromHex(c.hex!), scalar: ScalarType.BYTES };
	}
}

describe('set element conformance vectors', () => {
	it('has cases', () => {
		expect(vectors.cases.length).toBeGreaterThan(0);
	});

	for (const c of vectors.cases) {
		it(`${c.kind}: ${c.name}`, () => {
			const { value, scalar } = valueOf(c);
			expect(scalarKeyString(value, scalar, false)).toBe(c.want);
		});
	}
});
