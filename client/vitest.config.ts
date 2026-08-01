import { defineConfig } from 'vitest/config';

export default defineConfig({
	test: {
		projects: [
			{
				test: {
					name: 'unit',
					environment: 'node',
					include: [ 'test/unit/**/*.test.ts' ],
				},
			},
			{
				test: {
					name: 'react',
					environment: 'jsdom',
					include: [ 'test/react/**/*.test.ts' ],
					setupFiles: [ './test/react/setup.ts' ],
				},
			},
			{
				// Performance measurement, not correctness: runs only under
				// `vitest bench`, never as part of `task ci`. The empty test
				// include is what keeps `vitest run` out of it.
				test: {
					name: 'bench',
					environment: 'node',
					include: [],
					benchmark: { include: [ 'test/bench/**/*.bench.ts' ] },
				},
			},
			{
				test: {
					name: 'integration',
					environment: 'node',
					include: [ 'test/integration/**/*.test.ts' ],
					globalSetup: [ './test/integration/global-setup.ts' ],
					testTimeout: 30_000,
					hookTimeout: 30_000,
				},
			},
		],
	},
});
