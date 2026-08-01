import { defineConfig, devices } from '@playwright/test';

/**
 * The benchmark tier: a real browser, a real IndexedDB and a seeded corpus of
 * up to half a million rows, against the Go benchserver.
 *
 * Separate from playwright.config.ts rather than a project inside it because
 * the two need different global setup — this one seeds and caches a corpus,
 * and the e2e tier must not pay for that — and because they answer different
 * questions. Nothing here asserts a behaviour; a spec that "passes" only means
 * its numbers were taken.
 *
 * Run it with `task bench:browser`. Never part of `task ci`: it needs a
 * downloaded browser, it takes minutes, and its output is a table rather than
 * a verdict.
 */

const PORT = 5179;

export default defineConfig({
	testDir: './test/perf',
	// Every number here is a wall-clock measurement of one process's work.
	// Anything running beside it — a second worker, a retried spec, a parallel
	// file — competes for the same CPU and disk and lands in the measurement.
	workers: 1,
	fullyParallel: false,
	retries: 0,
	forbidOnly: !!process.env.CI,
	// The xl tier bootstraps half a million rows into IndexedDB, which is
	// minutes rather than seconds by design; the point of the tier is finding
	// out how many.
	timeout: 20 * 60_000,
	expect: { timeout: 60_000 },
	reporter: [ [ 'list' ] ],
	globalSetup: './test/perf/global-setup.ts',
	globalTeardown: './test/perf/global-teardown.ts',
	use: {
		baseURL: `http://127.0.0.1:${PORT}`,
		// No tracing: it instruments the very page whose cost is being measured.
		trace: 'off',
		video: 'off',
	},
	projects: [
		{ name: 'chromium', use: { ...devices['Desktop Chrome'] } },
		{ name: 'firefox', use: { ...devices['Desktop Firefox'] } },
		{ name: 'webkit', use: { ...devices['Desktop Safari'] } },
	],
	webServer: {
		// The installed binary directly rather than `pnpm exec`: pnpm re-checks
		// dependency status before running anything, which is a slow no-op at
		// best and a reinstall at worst. Nothing here needs resolution — vite is
		// already in node_modules or this config could not have loaded.
		command: `node_modules/.bin/vite --config test/perf/vite.config.ts --port ${PORT} --strictPort`,
		url: `http://127.0.0.1:${PORT}/`,
		reuseExistingServer: !process.env.CI,
		stdout: 'ignore',
		stderr: 'pipe',
	},
});
