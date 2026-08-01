import { cleanup } from '@testing-library/react';
import { afterEach } from 'vitest';

// React Testing Library does not auto-cleanup under vitest; unmount every
// rendered tree after each test so watchers/cache entries do not leak across
// cases (several tests assert on hub.watchers being empty).
afterEach(() => {
	cleanup();
});
