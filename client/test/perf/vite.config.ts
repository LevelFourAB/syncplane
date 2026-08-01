import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vite';

/** client/ — the harness imports ../../../src/**, which sits outside the root. */
const clientDir = fileURLToPath(new URL('../..', import.meta.url));

/**
 * Serves the benchmark harness page straight from client/src, with no build
 * step, so the numbers describe the sources every other tier runs rather than a
 * `dist` that may be stale.
 *
 * Vite serves modules unbundled and unminified in dev, which is a real cost the
 * benchmarks pay: absolute wall times here are not what a production build
 * would post. That is fine, and deliberate — these numbers exist to be compared
 * against each other and against their own history, and a build step between
 * the source and the measurement would only add a way for the two to drift.
 */
export default defineConfig({
	root: fileURLToPath(new URL('./app', import.meta.url)),
	server: {
		host: '127.0.0.1',
		fs: { allow: [ clientDir ] },
	},
});
