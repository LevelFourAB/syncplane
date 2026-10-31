import { defineConfig, devices } from '@playwright/test';

/**
 * The client storage size benchmark: how many bytes IndexedDB keeps on disk
 * for a synced text-heavy workspace, and how many of them are index data. See
 * test/idbsize/idb-size.spec.ts for what it measures and how to read it.
 *
 * Run it with
 *
 *   node_modules/.bin/playwright test --config playwright.idbsize.config.ts
 *
 * from client/, or name one engine with `--project=webkit`, `--project=chrome`
 * (the installed Google Chrome) or `--project=chromium` (the bundled build).
 *
 * A config of its own, not a project of playwright.bench.config.ts, because
 * it seeds a different corpus and measures a browser profile on disk rather
 * than a time. Never part of `task ci` or of the e2e run.
 */

const PORT = 5180;

export default defineConfig({
	testDir: './test/idbsize',
	workers: 1,
	fullyParallel: false,
	retries: 0,
	forbidOnly: !!process.env.CI,
	timeout: 15 * 60_000,
	expect: { timeout: 60_000 },
	reporter: [ [ 'list' ] ],
	globalSetup: './test/idbsize/global-setup.ts',
	globalTeardown: './test/idbsize/global-teardown.ts',
	use: {
		baseURL: `http://127.0.0.1:${PORT}`,
		trace: 'off',
		video: 'off',
	},
	projects: [
		{ name: 'webkit', use: { ...devices['Desktop Safari'], browserName: 'webkit' } },
		{
			name: 'chrome',
			use: { ...devices['Desktop Chrome'], browserName: 'chromium', channel: 'chrome' },
		},
		{ name: 'chromium', use: { ...devices['Desktop Chrome'], browserName: 'chromium' } },
	],
	webServer: {
		// The perf tier's harness page, on a port of its own. The installed
		// binary directly: `pnpm exec` re-checks dependencies first.
		command: `node_modules/.bin/vite --config test/perf/vite.config.ts --port ${PORT} --strictPort`,
		url: `http://127.0.0.1:${PORT}/`,
		reuseExistingServer: !process.env.CI,
		stdout: 'ignore',
		stderr: 'pipe',
	},
});
