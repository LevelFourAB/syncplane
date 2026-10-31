import { stopServer } from './global-setup.js';

/** Kills the benchserver global setup spawned, in the same process. */
export default function globalTeardown(): void {
	stopServer();
}
