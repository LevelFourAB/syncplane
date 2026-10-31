import type { TestProject } from 'vitest/node';

import { startTestServer } from '../support/testserver.js';

declare module 'vitest' {
	export interface ProvidedContext {
		serverUrl: string;
		tunnelAddr: string;
	}
}

/**
 * Spawns the Go testserver and provides its URL to the tests. The short
 * heartbeat and chunk size keep stream keepalives and bootstrap paging
 * observable inside a single test.
 */
export default async function setup(project: TestProject): Promise<() => void> {
	const server = await startTestServer([ '-heartbeat=250ms', '-chunk-size=8' ]);
	project.provide('serverUrl', server.url);
	project.provide('tunnelAddr', server.tunnelAddr);
	return () => server.stop();
}
