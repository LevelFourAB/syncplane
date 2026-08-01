import { startTestServer, type TestServer } from '../support/testserver.js';

/**
 * Environment variable carrying the spawned server's address to the specs.
 * Playwright forks its workers after global setup has run, so they inherit it.
 */
export const SERVER_URL_ENV = 'SYNC_E2E_SERVER_URL';

/** Held for global teardown, which runs in this same process. */
let server: TestServer | undefined;

/**
 * Spawns the Go testserver for the whole e2e run. One server serves every spec;
 * isolation comes from a fresh workspace id per test, as in the integration
 * tier. The short heartbeat keeps a stalled Subscribe stream from looking alive.
 */
export default async function globalSetup(): Promise<void> {
	server = await startTestServer([ '-heartbeat=250ms' ]);
	process.env[SERVER_URL_ENV] = server.url;
}

export function stopServer(): void {
	server?.stop();
	server = undefined;
}
