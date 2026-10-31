// Blob naming: the chunking and digest that engine/blob defines on the Go side.
// testdata/blob/vectors.json pins both implementations to the same digests.

/** Size in bytes of every chunk of a blob except the last. Part of the digest. */
export const BLOB_CHUNK_SIZE = 8 * 1024 * 1024;

const DIGEST_PREFIX = 'sp1:';
const DIGEST_PATTERN = /^sp1:[0-9a-f]{64}$/;

/** Whether `s` has the form `sp1:<64 lowercase hex digits>`. */
export function isBlobDigest(s: string): boolean {
	return DIGEST_PATTERN.test(s);
}

/** The number of chunks a blob of `size` bytes has. An empty blob has none. */
export function chunkCount(size: number): number {
	return size <= 0 ? 0 : Math.ceil(size / BLOB_CHUNK_SIZE);
}

/** The length in bytes of chunk `index` of a blob of `size` bytes. */
export function chunkLength(size: number, index: number): number {
	return Math.max(0, Math.min(BLOB_CHUNK_SIZE, size - index * BLOB_CHUNK_SIZE));
}

/** The SHA-256 of one chunk. */
export async function hashChunk(data: Uint8Array): Promise<Uint8Array> {
	return new Uint8Array(await crypto.subtle.digest('SHA-256', data as Uint8Array<ArrayBuffer>));
}

/**
 * The digest of a blob of `size` bytes with these chunk hashes, in order: the
 * SHA-256 of the size as an 8-byte unsigned big-endian integer followed by
 * the chunk hashes.
 */
export async function digestOf(size: number, chunkHashes: readonly Uint8Array[]): Promise<string> {
	const joined = new Uint8Array(8 + chunkHashes.length * 32);
	new DataView(joined.buffer).setBigUint64(0, BigInt(size));
	chunkHashes.forEach((h, i) => joined.set(h, 8 + i * 32));
	return DIGEST_PREFIX + toHex(await hashChunk(joined));
}

/** Lowercase hex of `bytes`. */
export function toHex(bytes: Uint8Array): string {
	let out = '';
	for (const b of bytes) out += b.toString(16).padStart(2, '0');
	return out;
}

/** Whether two byte arrays hold the same bytes. */
export function sameBytes(a: Uint8Array, b: Uint8Array): boolean {
	if (a.length !== b.length) return false;
	for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
	return true;
}

/**
 * Reads bytes `[start, end)` of `blob` into a fresh array of exactly that
 * length.
 *
 * Falls back to FileReader where Blob has no arrayBuffer() (jsdom, older
 * WebViews).
 */
export async function readBlobRange(blob: Blob, start: number, end: number): Promise<Uint8Array> {
	const part = blob.slice(start, end);
	if (typeof part.arrayBuffer === 'function') return new Uint8Array(await part.arrayBuffer());
	return new Promise<Uint8Array>((resolve, reject) => {
		const reader = new FileReader();
		reader.onload = () => resolve(new Uint8Array(reader.result as ArrayBuffer));
		reader.onerror = () => reject(reader.error ?? new Error('FileReader failed'));
		reader.readAsArrayBuffer(part);
	});
}

/**
 * Hashes `blob` chunk by chunk, handing each chunk to `onChunk` as it is read,
 * and returns the digest and the chunk hashes. Holds one chunk in memory at a
 * time.
 */
export async function hashBlob(
	blob: Blob,
	onChunk?: (index: number, data: Uint8Array) => Promise<void>,
): Promise<{ digest: string; chunkHashes: Uint8Array[] }> {
	const chunkHashes: Uint8Array[] = [];
	const count = chunkCount(blob.size);
	for (let i = 0; i < count; i++) {
		const start = i * BLOB_CHUNK_SIZE;
		const data = await readBlobRange(blob, start, Math.min(blob.size, start + BLOB_CHUNK_SIZE));
		chunkHashes.push(await hashChunk(data));
		if (onChunk) await onChunk(i, data);
	}
	return { digest: await digestOf(blob.size, chunkHashes), chunkHashes };
}
