import type { BlobRefLike, ClientBlobs, WorkspaceBlobs } from './api.js';
import type { BlobManager } from './manager.js';

/**
 * The WorkspaceBlobs of the workspace `workspaceId()` names. Both arguments are
 * read on every call: the manager is created on first use, and the client's
 * default workspace can change.
 */
export function workspaceBlobs(
	manager: () => BlobManager,
	workspaceId: () => string,
): WorkspaceBlobs {
	return {
		put: (data, opts) => call(() => manager().put(workspaceId(), data, opts)),
		status: (ref: BlobRefLike) => call(() => manager().status(workspaceId(), ref)),
		watch: (ref, cb) => manager().watch(workspaceId(), ref, cb),
		uploaded: (ref) => call(() => manager().uploaded(workspaceId(), ref)),
		get: (ref) => call(() => manager().get(workspaceId(), ref)),
		stream: (ref) => manager().stream(workspaceId(), ref),
		discard: (ref) => call(() => manager().discard(workspaceId(), ref)),
	};
}

/** The ClientBlobs over the default workspace's WorkspaceBlobs. */
export function clientBlobs(manager: () => BlobManager, workspaceId: () => string): ClientBlobs {
	return {
		...workspaceBlobs(manager, workspaceId),
		idle: () => call(() => manager().idle()),
		pendingCount: () => call(() => manager().pendingCount()),
	};
}

/** Turns a synchronous throw (closed client, no default workspace) into a rejection. */
function call<T>(fn: () => Promise<T>): Promise<T> {
	try {
		return fn();
	} catch (err) {
		return Promise.reject(err);
	}
}
