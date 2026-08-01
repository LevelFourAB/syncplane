// Runtime detection for the cross-tab seams (leadership, changeshare,
// WebLockMutex). Those features coordinate several browser contexts that share
// ONE IndexedDB store; they key on the workspace id, so two contexts in the
// same workspace are assumed to share storage and a clientId.
//
// That assumption only holds in a browser. On a server runtime every
// SyncClient owns a private store, so two clients that merely share a process
// and workspace id must NOT be coordinated onto one change stream / one lock /
// one change channel — doing so drops one client's updates (it never holds the
// stream leadership lock), serializes unrelated stores, or cross-fires watchers
// against the wrong store.
//
// The features used to detect "browser" by the mere presence of Web Locks and
// BroadcastChannel, both historically browser-only. That proxy broke when Node
// grew global `navigator` (v21), `BroadcastChannel` (v18) and `navigator.locks`
// (v26): the APIs are now present on a runtime where the shared-store premise
// is false, so a Node integration test running two clients in one workspace
// began electing a single stream leader and starving the other client.

/**
 * True on a server JS runtime (Node, Deno, Bun) rather than a browser/worker
 * context. Such runtimes set `navigator.userAgent` to "Node.js/<v>",
 * "Deno/<v>" or "Bun/<v>"; browsers report a "Mozilla/..." string and tests
 * that fake a browser by stubbing only `navigator.locks` leave userAgent
 * undefined — both correctly read as NOT a server runtime, so the cross-tab
 * mechanisms (and their unit tests) keep working there.
 */
export function isServerRuntime(): boolean {
	const ua = typeof navigator !== 'undefined' ? navigator.userAgent : undefined;
	return typeof ua === 'string' && /^(?:Node\.js|Deno|Bun)\//.test(ua);
}

/**
 * Process-wide default for WatcherHubOptions.debugVerifyTouches: set
 * `SYNC_DEBUG_VERIFY_TOUCHES=1` to put every watcher hub in the process under
 * the check without threading the option through each client construction.
 * Reads nothing (and so stays off) on a runtime with no environment, which is
 * every browser.
 */
export function debugVerifyTouchesEnv(): boolean {
	const proc = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process;
	return proc?.env?.SYNC_DEBUG_VERIFY_TOUCHES === '1';
}
