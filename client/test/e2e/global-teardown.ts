import { stopServer } from './global-setup.js';

/**
 * Kills the testserver global setup spawned. Setup and teardown share this
 * process, so importing the setup module here reaches the same handle rather
 * than a second copy of it.
 */
export default function globalTeardown(): void {
	stopServer();
}
