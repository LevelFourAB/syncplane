// Time-ordered entity identifiers: a UUIDv7 (RFC 9562) rendered as 26-char
// Crockford base32. The leading 48 bits are a big-endian Unix-millisecond
// timestamp, so the encoded string sorts lexicographically by creation time
// (at millisecond granularity) — keeping server-side index inserts local and
// giving rows a rough creation order for free. Dependency-free; the Go engine
// (`engine/id`) emits the identical format so IDs are uniform across the system.
//
// This is for entity identity (key fields). For user-defined list ordering use
// `generateKeyBetween` in `fractional.ts` instead.

// Crockford base32, minus the ambiguous I/L/O/U. The alphabet is in ASCII
// order, so plain string comparison preserves the underlying byte order.
const CROCKFORD = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';

/** A random UUIDv7 as 16 bytes, big-endian millisecond timestamp first. */
function uuidv7Bytes(): Uint8Array {
	const bytes = new Uint8Array(16);
	crypto.getRandomValues(bytes);

	// 48-bit timestamp. Division (not `>>>`) because the value exceeds 32 bits.
	const ms = Date.now();
	bytes[0] = Math.floor(ms / 2 ** 40) & 0xff;
	bytes[1] = Math.floor(ms / 2 ** 32) & 0xff;
	bytes[2] = Math.floor(ms / 2 ** 24) & 0xff;
	bytes[3] = Math.floor(ms / 2 ** 16) & 0xff;
	bytes[4] = Math.floor(ms / 2 ** 8) & 0xff;
	bytes[5] = ms & 0xff;

	bytes[6] = (bytes[6]! & 0x0f) | 0x70; // version 7
	bytes[8] = (bytes[8]! & 0x3f) | 0x80; // variant 10
	return bytes;
}

/** MSB-first base32 over the Crockford alphabet, no padding. */
function base32(bytes: Uint8Array): string {
	let out = '';
	let value = 0;
	let bits = 0;
	for (let i = 0; i < bytes.length; i++) {
		value = (value << 8) | bytes[i]!;
		bits += 8;
		while (bits >= 5) {
			bits -= 5;
			out += CROCKFORD[(value >>> bits) & 31];
		}
		value &= (1 << bits) - 1; // drop already-emitted high bits so value stays small
	}
	if (bits > 0) {
		out += CROCKFORD[(value << (5 - bits)) & 31];
	}
	return out;
}

/**
 * Generate a new entity id: a UUIDv7 encoded as 26-character Crockford base32.
 * Suitable for any string key field; lexicographically sortable by creation time.
 */
export function newId(): string {
	return base32(uuidv7Bytes());
}
