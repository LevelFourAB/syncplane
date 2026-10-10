import { startTestServer, type TestServer } from '../support/testserver.js';

/**
 * Environment variable carrying the spawned server's address to the specs.
 * Playwright forks its workers after global setup has run, so they inherit it.
 */
export const SERVER_URL_ENV = 'SYNC_E2E_SERVER_URL';

/**
 * Environment variable carrying the address of the access-model server, which
 * serves the access-model test schema and takes a bearer token as the caller.
 */
export const ACCESS_SERVER_URL_ENV = 'SYNC_E2E_ACCESS_SERVER_URL';

/** Held for global teardown, which runs in this same process. */
const servers: TestServer[] = [];

/**
 * Spawns the Go testservers for the whole e2e run. One server serves every
 * spec on the default schema; isolation comes from a fresh workspace id per
 * test, as in the integration tier. The access-model specs get a server of
 * their own, because a schema that uses the access model admits no caller
 * without grant rows. The short heartbeat keeps a stalled Subscribe stream from
 * looking alive.
 */
export default async function globalSetup(): Promise<void> {
	// One after the other, and each held as soon as it runs, so a server that
	// fails to start does not leave the other one behind for teardown to miss.
	const server = await startTestServer([ '-heartbeat=250ms' ]);
	servers.push(server);
	const access = await startTestServer([ '-access', '-heartbeat=250ms' ]);
	servers.push(access);
	process.env[SERVER_URL_ENV] = server.url;
	process.env[ACCESS_SERVER_URL_ENV] = access.url;
}

export function stopServer(): void {
	for (const server of servers.splice(0)) server.stop();
}
