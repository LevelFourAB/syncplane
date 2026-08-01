import { defineConfig, devices } from '@playwright/test';

/**
 * The browser tier: two real tabs, one real IndexedDB, real Web Locks and a
 * real BroadcastChannel, against the spawned Go testserver. Node cannot stand
 * in for any of that — `createKvMutex` falls back to ChainMutex and stream
 * leadership declines to run without `navigator.locks` — so the multi-tab
 * guarantees the README makes are only actually checked here.
 *
 * Deliberately NOT part of `task ci`: it needs `playwright install` to have
 * downloaded browsers. Run it with `task test:e2e` (chromium), or
 * `pnpm exec playwright test` for every installed engine.
 */

const PORT = 5178;

export default defineConfig({
	testDir: './test/e2e',
	// Specs assert process-wide server state (streams in flight, bootstrap
	// counts), so they must not overlap.
	workers: 1,
	fullyParallel: false,
	forbidOnly: !!process.env.CI,
	// Leadership handover waits out a 1.5s hide grace, and a cold first run pays
	// for Vite's dependency pre-bundling.
	timeout: 60_000,
	expect: { timeout: 15_000 },
	reporter: process.env.CI ? 'list' : [ [ 'list' ], [ 'html', { open: 'never' } ] ],
	globalSetup: './test/e2e/global-setup.ts',
	globalTeardown: './test/e2e/global-teardown.ts',
	use: {
		baseURL: `http://127.0.0.1:${PORT}`,
		trace: 'retain-on-failure',
	},
	projects: [
		{ name: 'chromium', use: { ...devices['Desktop Chrome'] } },
		// Both ship Web Locks, IndexedDB and BroadcastChannel, and the suite uses
		// no Chromium-only APIs — but they are opt-in because each is another
		// browser to download.
		{ name: 'firefox', use: { ...devices['Desktop Firefox'] } },
		{ name: 'webkit', use: { ...devices['Desktop Safari'] } },
	],
	webServer: {
		command: `pnpm exec vite --config test/e2e/vite.config.ts --port ${PORT} --strictPort`,
		url: `http://127.0.0.1:${PORT}/`,
		reuseExistingServer: !process.env.CI,
		stdout: 'ignore',
		stderr: 'pipe',
	},
});
