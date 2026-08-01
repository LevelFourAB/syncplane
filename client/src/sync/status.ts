import type { SyncErrorCode } from '../codes.js';
import { safeInvoke } from '../report.js';

/** Last sync failure, kept until the next successful cycle. */
export interface SyncErrorInfo {
	message: string;
	/**
	 * Machine-readable code when known, in the library's namespace (see
	 * codes.ts). A server that typed its failure passes its code through here;
	 * otherwise a transport failure is classified as `unavailable` or `internal`.
	 */
	code?: SyncErrorCode | (string & {});
	/**
	 * The underlying Connect code (e.g. "NotFound", "ResourceExhausted") when the
	 * failure came from an RPC. A second, coarser vocabulary — present only for
	 * callers that need to tell apart failures `code` deliberately collapses.
	 */
	transportCode?: string;
}

/** How the client reacts to a server/client schema-revision mismatch. */
export type SchemaSkewAction = 'ignore' | 'warn' | 'block' | 'resync';

/**
 * The sync scope actually in effect for one workspace, vs. what this tab asked
 * for. All tabs in a browser share one store and one cursor per workspace, so
 * they sync the UNION of every live tab's subscription set — and full sync is
 * the absorbing element of that union (one full-sync participant pulls the whole
 * workspace for everyone). This lets a UI explain why a "sync only X" scope is
 * not taking effect: a peer tab, or a not-yet-expired record from a
 * recently-closed tab, has widened it.
 */
export interface SyncScopeInfo {
	/** Subscriptions this tab itself requested (0 = this tab asked for full sync). */
	own: number;
	/** Subscriptions in the effective union actually synced (0 = full sync). */
	effective: number;
	/**
	 * The effective scope is broader than this tab requested — another tab (or a
	 * lingering record from a closed one) widened it. Always false when this tab
	 * asked for full sync (own === 0): nothing is broader than everything.
	 */
	widened: boolean;
}

/**
 * Progress of a workspace's FIRST snapshot, while it is still arriving.
 *
 * A cold client applies the snapshot entity by entity and can read each one as
 * it lands, so this is what a progress indicator renders and what tells an
 * entity that has not started from one that is genuinely empty. Per-read, the
 * same fact arrives as the `hydrating` coverage bound on the result array;
 * this is the workspace-wide view of it.
 *
 * Only a COLD start reports it. A re-bootstrap stages its rows and swaps
 * atomically, so nothing is ever partly visible and there is no progress a
 * reader could act on.
 */
export interface HydrationInfo {
	/** Entities whose rows are still arriving, in delivery order. */
	pending: readonly string[];
	/** Entities the snapshot ships in total. */
	total: number;
	/** How many of them have landed in full (`total - pending.length`). */
	done: number;
}

/** Details of an active schema-revision skew (see SyncStatus.schemaSkew). */
export interface SchemaSkewInfo {
	/** This client build's revision (the configured schemaRevision). */
	client: string;
	/** The revision the server is advertising. */
	server: string;
	/** The policy decision in effect for this mismatch. */
	action: SchemaSkewAction;
}

/**
 * Whether the server still grants this client access to a workspace.
 *
 * "revoked" is PERMANENT — the server said so explicitly (a WorkspaceDropped
 * frame, or a workspace-scoped denial on push), not inferred from a failure —
 * so a client seeing it must stop expecting that workspace to resume syncing
 * rather than treat it as another outage to back off from. The other workspaces
 * on the same stream are unaffected.
 *
 * The client does not purge the local copy on its own: deleting a user's rows
 * off one server verdict is a decision an app has to make, not a side effect of
 * reading a frame (issue 079). What it does do is stop syncing that workspace,
 * so the app can act on this status — `client.removeWorkspace(id, { purge:
 * true })` is the purge when it decides to.
 */
export type WorkspaceAccess = 'granted' | 'revoked';

/** Sync health of one workspace the client holds. */
export interface WorkspaceStatus {
	workspaceId: string;
	/** Local mutations queued for this workspace that the server has not acked. */
	pendingMutations: number;
	/**
	 * Whether the workspace is still accessible. Unset means "no verdict yet" —
	 * it only becomes "revoked" when the server says so. Like schemaSkew and
	 * unlike lastError, this survives successful cycles: it is a standing
	 * condition, not a transient failure.
	 */
	access?: WorkspaceAccess;
	/** The effective sync scope for this workspace. Unset until the first cycle. */
	scope?: SyncScopeInfo;
	/**
	 * Set while this workspace's first snapshot is still downloading, naming the
	 * entities that have not fully arrived. Unset the moment the client holds a
	 * complete snapshot — which is the normal state, so a UI keyed off it shows
	 * nothing for the rest of the session. See HydrationInfo.
	 */
	hydration?: HydrationInfo;
}

/** Snapshot of the client's sync health, for UI indicators. */
export interface SyncStatus {
	/** What the sync loop is doing right now. */
	state: 'idle' | 'syncing' | 'error';
	/** Live stream connected, or the last sync cycle succeeded. */
	online: boolean;
	/** Derived: there are local mutations the server has not acked, anywhere. */
	dirty: boolean;
	/** Total queued mutations across every workspace. */
	pendingMutations: number;
	/** Completion time of the last successful sync cycle. */
	lastSyncedAt?: Date;
	lastError?: SyncErrorInfo;
	/**
	 * Set while the server's advertised schema revision differs from this
	 * client's and the skew policy is reacting (warn/block/resync). Unset when
	 * the server advertises a matching revision or none, and when no
	 * schemaRevision is configured. A "block" action means pushes are suspended
	 * (read-only / update-required) until the revisions line up again. Unlike
	 * lastError, this survives successful sync cycles — it reflects a standing
	 * condition, not a transient failure.
	 *
	 * Account-wide: the revision is a property of the server build, not of a
	 * workspace.
	 */
	schemaSkew?: SchemaSkewInfo;
	/**
	 * Per-workspace health, keyed by workspace id — one entry per workspace this
	 * client holds. Also reachable as `client.ws(id).status()`.
	 */
	workspaces: Record<string, WorkspaceStatus>;
	/**
	 * The default workspace's scope, mirrored here so a single-workspace app can
	 * read `status().scope` without going through `workspaces`. Unset when there
	 * is no default workspace or it has not synced yet.
	 */
	scope?: SyncScopeInfo;
	/**
	 * The default workspace's access verdict, mirrored here for the same reason
	 * as `scope`. See WorkspaceAccess.
	 */
	workspaceAccess?: WorkspaceAccess;
	/**
	 * The default workspace's snapshot progress, mirrored here for the same
	 * reason as `scope`. See HydrationInfo.
	 */
	hydration?: HydrationInfo;
}

export type StatusListener = (status: SyncStatus) => void;

/**
 * Mutable sync-status cell. `update` shallow-compares and only notifies
 * listeners when something actually changed; `dirty` and the total
 * `pendingMutations` are always derived from the per-workspace entries, as are
 * the default-workspace mirrors.
 */
export class StatusTracker {
	private status: SyncStatus = {
		state: 'idle',
		online: false,
		dirty: false,
		pendingMutations: 0,
		workspaces: {},
	};
	private listeners = new Set<StatusListener>();
	private defaultWorkspace?: string;

	current(): SyncStatus {
		return this.status;
	}

	onChange(cb: StatusListener): () => void {
		this.listeners.add(cb);
		return () => {
			this.listeners.delete(cb);
		};
	}

	/** Names the workspace whose scope/access mirror onto the top level. */
	setDefaultWorkspace(workspaceId: string | undefined): void {
		this.defaultWorkspace = workspaceId;
		this.commit({ ...this.status });
	}

	/** Registers a workspace so it appears in the status before its first sync. */
	addWorkspace(workspaceId: string): void {
		if (this.status.workspaces[workspaceId]) return;
		this.updateWorkspace(workspaceId, {});
	}

	/** Drops a workspace's entry (it was removed from the client). */
	removeWorkspace(workspaceId: string): void {
		if (!this.status.workspaces[workspaceId]) return;
		const workspaces = { ...this.status.workspaces };
		delete workspaces[workspaceId];
		this.commit({ ...this.status, workspaces });
	}

	update(patch: Partial<Omit<SyncStatus, 'dirty' | 'pendingMutations' | 'workspaces'>>): void {
		this.commit({ ...this.status, ...patch });
	}

	updateWorkspace(
		workspaceId: string,
		patch: Partial<Omit<WorkspaceStatus, 'workspaceId'>>,
	): void {
		const prev = this.status.workspaces[workspaceId]
			?? { workspaceId, pendingMutations: 0 };
		const next: WorkspaceStatus = { ...prev, ...patch };
		this.commit({
			...this.status,
			workspaces: { ...this.status.workspaces, [workspaceId]: next },
		});
	}

	setPendingCount(workspaceId: string, count: number): void {
		this.updateWorkspace(workspaceId, { pendingMutations: count });
	}

	/**
	 * Queue depth currently reported for a workspace, 0 for one that has none.
	 * Read by the coordinator to work out how many mutations a rebase acked —
	 * the rebase reports what is left, not what left.
	 */
	pendingCount(workspaceId: string): number {
		return this.status.workspaces[workspaceId]?.pendingMutations ?? 0;
	}

	incrementPending(workspaceId: string): void {
		const prev = this.status.workspaces[workspaceId]?.pendingMutations ?? 0;
		this.updateWorkspace(workspaceId, { pendingMutations: prev + 1 });
	}

	/** Derives the computed fields, compares, and notifies only on a real change. */
	private commit(next: SyncStatus): void {
		let total = 0;
		for (const ws of Object.values(next.workspaces)) total += ws.pendingMutations;
		next.pendingMutations = total;
		next.dirty = total > 0;
		const def = this.defaultWorkspace ? next.workspaces[this.defaultWorkspace] : undefined;
		next.scope = def?.scope;
		next.workspaceAccess = def?.access;
		next.hydration = def?.hydration;
		if (sameStatus(this.status, next)) return;
		this.status = next;
		for (const cb of [ ...this.listeners ]) safeInvoke(cb, next, 'onStatusChange listener');
	}
}

function sameStatus(a: SyncStatus, b: SyncStatus): boolean {
	return (
		a.state === b.state
		&& a.online === b.online
		&& a.dirty === b.dirty
		&& a.pendingMutations === b.pendingMutations
		&& a.lastSyncedAt?.getTime() === b.lastSyncedAt?.getTime()
		&& a.lastError?.message === b.lastError?.message
		&& a.lastError?.code === b.lastError?.code
		&& a.lastError?.transportCode === b.lastError?.transportCode
		&& a.schemaSkew?.client === b.schemaSkew?.client
		&& a.schemaSkew?.server === b.schemaSkew?.server
		&& a.schemaSkew?.action === b.schemaSkew?.action
		&& sameWorkspaces(a.workspaces, b.workspaces)
	);
}

function sameWorkspaces(
	a: Record<string, WorkspaceStatus>,
	b: Record<string, WorkspaceStatus>,
): boolean {
	const aKeys = Object.keys(a);
	if (aKeys.length !== Object.keys(b).length) return false;
	for (const k of aKeys) {
		const x = a[k]!;
		const y = b[k];
		if (
			y === undefined
			|| x.pendingMutations !== y.pendingMutations
			|| x.access !== y.access
			|| x.scope?.own !== y.scope?.own
			|| x.scope?.effective !== y.scope?.effective
			|| x.scope?.widened !== y.scope?.widened
			|| !sameHydration(x.hydration, y.hydration)
		) {
			return false;
		}
	}
	return true;
}

/**
 * Compares progress by the outstanding list, not just its length: the delivery
 * order is replaced wholesale when the server announces it, which can leave the
 * counts alone while changing what is actually still coming.
 */
function sameHydration(a?: HydrationInfo, b?: HydrationInfo): boolean {
	if (a === undefined || b === undefined) return a === b;
	if (a.total !== b.total || a.pending.length !== b.pending.length) return false;
	return a.pending.every((entity, i) => entity === b.pending[i]);
}
