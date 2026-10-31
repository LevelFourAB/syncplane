import type { TestProject } from 'vitest/node';

import { startTestServer } from '../support/testserver.js';

declare module 'vitest' {
	export interface ProvidedContext {
		serverUrl: string;
		tunnelAddr: string;
		maxBlobBytes: number;
	}
}

/**
 * The testserver's blob size limit: above every blob the suite uploads, and
 * low enough that a test can go past it.
 */
const MAX_BLOB_BYTES = 2 * 8 * 1024 * 1024;

/**
 * Spawns the Go testserver and provides its URL to the tests. The short
 * heartbeat and chunk size keep stream keepalives and bootstrap paging
 * observable inside a single test, and the low blob limit lets a test provoke
 * a size refusal.
 */
export default async function setup(project: TestProject): Promise<() => void> {
	const server = await startTestServer([
		'-heartbeat=250ms',
		'-chunk-size=8',
		`-max-blob-bytes=${MAX_BLOB_BYTES}`,
	]);
	project.provide('serverUrl', server.url);
	project.provide('tunnelAddr', server.tunnelAddr);
	project.provide('maxBlobBytes', MAX_BLOB_BYTES);
	return () => server.stop();
}
