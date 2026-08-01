import { useCallback, useSyncExternalStore } from 'react';

import type { SyncStatus } from '../../client.js';
import { useSyncClient } from '../context.js';

/**
 * The live sync status. `client.status()` is synchronous, works before
 * `start()`, and keeps a stable reference between changes, so it doubles as a
 * tearing-safe snapshot and an SSR snapshot.
 */
export function useSyncStatus(): SyncStatus {
	const client = useSyncClient();
	const subscribe = useCallback((cb: () => void) => client.onStatusChange(cb), [ client ]);
	const getSnapshot = useCallback(() => client.status(), [ client ]);
	return useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
}
