import { describe, expect, it } from 'vitest';

import { newId } from '../../src/id.js';

describe('newId', () => {
	it('produces 26-char Crockford base32', () => {
		const id = newId();
		expect(id).toHaveLength(26);
		expect(id).toMatch(/^[0-9A-HJKMNP-TV-Z]{26}$/);
	});

	it('encodes the UUIDv7 version and variant bits', () => {
		// Decode back to bytes and check byte 6 (version 7) and byte 8 (variant 10).
		const ALPHABET = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
		const id = newId();
		const bytes = new Uint8Array(16);
		let value = 0;
		let bits = 0;
		let bi = 0;
		for (const ch of id) {
			value = (value << 5) | ALPHABET.indexOf(ch);
			bits += 5;
			if (bits >= 8) {
				bits -= 8;
				bytes[bi++] = (value >>> bits) & 0xff;
			}
		}
		expect(bytes[6]! & 0xf0).toBe(0x70);
		expect(bytes[8]! & 0xc0).toBe(0x80);
	});

	it('is unique and sorts by creation time across calls', async () => {
		const a = newId();
		await new Promise((r) => setTimeout(r, 2));
		const b = newId();
		expect(a).not.toBe(b);
		// b created in a later millisecond, so it sorts after a as a plain string.
		expect(b > a).toBe(true);
	});

	it('has no collisions within a tight loop', () => {
		const seen = new Set<string>();
		for (let i = 0; i < 10000; i++) seen.add(newId());
		expect(seen.size).toBe(10000);
	});
});
