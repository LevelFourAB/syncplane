import { createHash } from 'node:crypto';

import { expect, test } from '@playwright/test';

import { freshWorkspace, Tab } from './fixtures.js';
import { blobBytes } from './harness-api.js';

const CHUNK = 8 * 1024 * 1024;

/**
 * Whether this project's IndexedDB stores Blob values. WebKit refuses them in
 * an ephemeral session, which every Playwright WebKit context is, and the
 * blob store then keeps chunk bytes instead.
 */
function blobValues(): boolean {
	return test.info().project.name !== 'webkit';
}

function sha256(bytes: Uint8Array): string {
	return createHash('sha256').update(bytes).digest('hex');
}

/**
 * Blobs in a real browser: chunks stored as IndexedDB Blob values, which
 * Node's fake IndexedDB cannot tell apart from bytes, and a download read back
 * from them. The writer and the reader use separate accounts, so the reader
 * has nothing local and must download.
 */
test.describe('blobs in a browser', () => {
	test('downloads a multi-chunk blob into IndexedDB and reads it back', async ({ context }) => {
		const ws = freshWorkspace();
		const size = 2 * CHUNK + 1234;
		const seed = 7;
		const want = sha256(blobBytes(size, seed));

		const writer = await Tab.open(context, {
			workspaceId: ws,
			accountId: `writer-${ws}`,
			entities: [ 'blob' ],
			start: true,
		});
		const ref = await writer.putBlob(size, seed, 'application/x-test');
		await writer.putBlobHolder('h1', ref);
		await writer.syncNow();
		await writer.blobUploaded(ref);
		// The queued chunks are Blob values, not byte arrays, where IndexedDB
		// takes them.
		expect((await writer.storedBlob(ref)).allBlobs).toBe(blobValues());

		const reader = await Tab.open(context, {
			workspaceId: ws,
			accountId: `reader-${ws}`,
			entities: [ 'blob' ],
			start: true,
		});
		await reader.syncNow();
		expect(await reader.storedBlob(ref)).toEqual({ cached: false, chunks: 0, allBlobs: false });

		const got = await reader.getBlob(ref);
		expect(got).toMatchObject({ size, type: 'application/x-test', sha256: want });
		// The download went into the cache chunk by chunk; get() composes its
		// result from those.
		expect(await reader.storedBlob(ref)).toEqual({ cached: true, chunks: 3, allBlobs: blobValues() });

		const streamed = await reader.streamBlob(ref);
		expect(streamed.sha256).toBe(want);
		expect(streamed.chunks).toEqual([ CHUNK, CHUNK, 1234 ]);
	});

	test('streams a blob it has not downloaded yet', async ({ context }) => {
		const ws = freshWorkspace();
		const size = CHUNK + 5;
		const writer = await Tab.open(context, {
			workspaceId: ws,
			accountId: `writer-${ws}`,
			entities: [ 'blob' ],
			start: true,
		});
		const ref = await writer.putBlob(size, 3, '');
		await writer.putBlobHolder('h1', ref);
		await writer.syncNow();
		await writer.blobUploaded(ref);

		const reader = await Tab.open(context, {
			workspaceId: ws,
			accountId: `reader-${ws}`,
			entities: [ 'blob' ],
			start: true,
		});
		const streamed = await reader.streamBlob(ref);
		expect(streamed.sha256).toBe(sha256(blobBytes(size, 3)));
		expect(streamed.chunks).toEqual([ CHUNK, 5 ]);
		expect((await reader.storedBlob(ref)).cached).toBe(true);
	});
});
