import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vite';

/** client/ — the harness imports ../../../src/**, which sits outside the root. */
const clientDir = fileURLToPath(new URL('../..', import.meta.url));

/**
 * Serves the e2e harness page straight from client/src, with no build step, so
 * the browser runs exactly the sources the unit and integration tiers do rather
 * than a `dist` that may be stale. Playwright starts this as its webServer; see
 * client/playwright.config.ts.
 */
export default defineConfig({
	root: fileURLToPath(new URL('./app', import.meta.url)),
	server: {
		host: '127.0.0.1',
		fs: { allow: [ clientDir ] },
	},
});
