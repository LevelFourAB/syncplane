import { afterEach, describe, expect, it, vi } from 'vitest';

import { isServerRuntime } from '../../src/env.js';

describe('isServerRuntime', () => {
	afterEach(() => {
		vi.unstubAllGlobals();
	});

	it('detects Node, Deno and Bun from navigator.userAgent', () => {
		for (const ua of [ 'Node.js/26', 'Node.js/20.11.0', 'Deno/1.45.0', 'Bun/1.1.0' ]) {
			vi.stubGlobal('navigator', { userAgent: ua });
			expect(isServerRuntime(), ua).toBe(true);
		}
	});

	it('treats a browser userAgent as not a server runtime', () => {
		vi.stubGlobal('navigator', { userAgent: 'Mozilla/5.0 (Macintosh)' });
		expect(isServerRuntime()).toBe(false);
	});

	it('treats a faked browser (locks only, no userAgent) as not a server runtime', () => {
		// The cross-tab unit tests stub navigator with just a locks shim; that
		// absence of userAgent must read as a browser so the mechanisms engage.
		vi.stubGlobal('navigator', { locks: {} });
		expect(isServerRuntime()).toBe(false);
	});

	it('is false when navigator is absent (old runtime)', () => {
		vi.stubGlobal('navigator', undefined);
		expect(isServerRuntime()).toBe(false);
	});
});
