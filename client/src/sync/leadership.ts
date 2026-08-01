// Cross-tab stream leadership: one tab per ACCOUNT holds the Subscribe stream
// open (leader, elected via Web Locks) and applies the changed rows it pushes
// down to the shared KV store; the other tabs see those writes through the
// shared store and the changeshare BroadcastChannel, exactly as today's stream
// followers do. On a server runtime, or without Web Locks (old browsers), the
// caller falls back to a single session-long stream of its own.
//
// The scope is the account, not the workspace, because the stream itself is:
// Subscribe multiplexes every workspace the account holds onto one connection,
// so one leader per account is one connection per account regardless of how
// many workspaces are open.
//
// Leadership is VISIBILITY-AWARE: a leader holds the lock only while its tab is
// visible. On `visibilitychange→hidden` (after a short grace), `pagehide` or
// `freeze` it aborts its term — closing the stream and releasing the lock — so
// a visible peer takes over; on `visible`/`resume` it re-contends. `hidden`
// fires before a browser suspends a background tab, so a suspended tab never
// strands the lock; leadership always lives in a visible tab, or nowhere (all
// hidden ⇒ every tab degrades to the per-tab poll floor, identical to a frozen
// leader today).
//
// The lock name is dedicated to stream leadership and is NEVER nested with the
// kv mutex lock — the leader loop only drives the stream + rebase, which take
// the kv mutex independently.

import { isServerRuntime } from '../env.js';
import { reportError } from '../report.js';

/** Grace after a tab hides before it releases leadership (debounces tab flips). */
const HIDE_GRACE_MS = 1_500;

export interface StreamLeadershipHooks {
	/**
	 * Runs the Subscribe stream while this tab is the leader, for ONE term. The
	 * term signal aborts when the tab loses visibility or the coordinator stops;
	 * the loop must close its stream and resolve when it fires. Resolving
	 * releases leadership to another tab.
	 */
	runLeaderLoop(termSignal: AbortSignal): Promise<void>;
	/**
	 * Leadership can never be held by this tab: the Web Locks request failed for
	 * a reason that is not a term ending (the API is unavailable to this context
	 * — a sandboxed frame, a storage-partitioned embed — or the leader loop threw
	 * its way out). Called at most once, after which the tab stops contending, so
	 * the caller can do what the no-Web-Locks path does and run its own
	 * session-long stream instead of silently falling back to polling alone.
	 */
	onUnavailable?(): void;
}

export interface StreamLeadership {
	stop(): void;
}

export interface ReopenSignal {
	/**
	 * Asks every tab sharing the account to re-scope its live Subscribe stream
	 * (the leader reopens; followers, holding no stream, no-op). Fired by a tab
	 * whose setSubscriptions widened/narrowed the union, so the leader — which
	 * may be a DIFFERENT tab and fixes the subscription set for the life of its
	 * stream — picks up the new union instead of pushing the stale one forever.
	 */
	signal(): void;
	/**
	 * Announces that the tab `tabId` is going away (pagehide / clean stop), so a
	 * surviving tab can reap its intent record NOW instead of waiting out the
	 * subscription TTL. A departing tab cannot reliably finish its own async IDB
	 * delete during unload, but a peer — which has all the time it needs —
	 * can; this is what keeps a closed full-sync tab from widening the union for
	 * up to SUBS_TTL_MS after it is gone.
	 */
	announceDeparture(tabId: string): void;
	stop(): void;
}

/** Handlers for messages another tab broadcasts on the reopen channel. */
export interface ReopenHandlers {
	/** A peer changed the union; re-scope our own leader stream (no-op if none). */
	onReopen(): void;
	/** A peer tab `tabId` is gone; reap its record and re-scope. */
	onDeparted(tabId: string): void;
}

/**
 * Cross-tab coordination channel for partial sync. Carries two messages: a
 * "reopen the stream" ping (the browser Subscribe stream is unidirectional and
 * its scope is fixed for its life, so a subscription change on any tab must
 * reach whichever tab holds the leader stream) and a "tab departed" notice (so
 * a surviving tab reaps the departed tab's intent record promptly). A no-op on
 * a server runtime (private stores, no shared leader) or without
 * BroadcastChannel (every tab then runs its own stream and re-scopes itself
 * directly, and orphan records age out by TTL).
 */
export function startReopenSignal(
	accountId: string,
	handlers: ReopenHandlers,
): ReopenSignal {
	if (isServerRuntime() || typeof BroadcastChannel === 'undefined') {
		return { signal: () => {}, announceDeparture: () => {}, stop: () => {} };
	}
	const channel = new BroadcastChannel(`sync:reopen:${accountId}`);
	const post = (msg: unknown): void => {
		try {
			channel.postMessage(msg);
		} catch {
			// closed channel — racing stop(); harmless
		}
	};
	channel.onmessage = (e: MessageEvent) => {
		const m = e.data as { r?: 1; gone?: string } | 1 | undefined;
		// `1` is the legacy reopen ping; accept it so mixed-version tabs interop.
		if (m === 1 || (typeof m === 'object' && m?.r === 1)) handlers.onReopen();
		else if (typeof m === 'object' && typeof m?.gone === 'string') {
			handlers.onDeparted(m.gone);
		}
	};
	return {
		signal: () => post({ r: 1 }),
		announceDeparture: (tabId) => post({ gone: tabId }),
		stop: () => channel.close(),
	};
}

/**
 * Starts visibility-aware cross-tab stream leadership. Returns undefined on a
 * server runtime (where clients do not share a store, so leadership would
 * starve all but one) or when the platform lacks Web Locks; callers then run
 * their own session-long stream loop.
 */
export function startStreamLeadership(
	accountId: string,
	hooks: StreamLeadershipHooks,
): StreamLeadership | undefined {
	if (
		isServerRuntime()
		|| typeof navigator === 'undefined'
		|| navigator.locks === undefined
		// Without BroadcastChannel the changeshare fanout is a no-op, so a leader's
		// applied rows would never nudge follower tabs' watchers — they would show
		// stale data until their own poll. Decline leadership so every tab runs its
		// own stream loop and refreshes itself, exactly as the no-Web-Locks path.
		|| typeof BroadcastChannel === 'undefined'
	) {
		return undefined;
	}
	const name = `sync:stream:${accountId}`;
	const hasDoc = typeof document !== 'undefined';
	// Treat a context without a Document (a worker) as always visible: it never
	// suspends on visibility and should hold a session-long term.
	const hidden = (): boolean => hasDoc && (document as Document).visibilityState === 'hidden';

	let stopped = false;
	/** Web Locks let us down; stop contending (see StreamLeadershipHooks.onUnavailable). */
	let unavailable = false;
	let term: AbortController | undefined;
	let hideTimer: ReturnType<typeof setTimeout> | undefined;

	const clearHideTimer = (): void => {
		if (hideTimer !== undefined) {
			clearTimeout(hideTimer);
			hideTimer = undefined;
		}
	};

	// Begins (or re-contends for) a leadership term. The lock request blocks
	// (possibly forever) until this tab wins; the term signal cancels the WAIT,
	// and once granted the leader loop watches the same signal to end the term.
	const startTerm = (): void => {
		if (stopped || unavailable || term || hidden()) return;
		const t = new AbortController();
		term = t;
		void navigator.locks
			.request(name, { signal: t.signal }, async () => {
				if (stopped || t.signal.aborted) return;
				await hooks.runLeaderLoop(t.signal);
			})
			.catch((err: unknown) => {
				// An AbortError is how a term ENDS (or a queued wait is cancelled) —
				// routine, and our own doing. A rejection with the term still live is
				// not: the tab holds no stream and, unless the caller is told, has no
				// fallback either, so it would ride out the session on the poll floor
				// with nothing logged. Stop contending and hand the job back.
				if (stopped || t.signal.aborted) return;
				unavailable = true;
				reportError('stream leadership unavailable; running a per-tab stream', err);
				hooks.onUnavailable?.();
			})
			.finally(() => {
				if (term === t) term = undefined;
			});
	};

	const endTerm = (): void => {
		clearHideTimer();
		term?.abort();
		term = undefined;
	};

	// hidden: release after a grace period (a quick tab flip keeps leadership).
	// pagehide/freeze: release immediately — the tab is about to be frozen.
	const onVisibilityChange = (): void => {
		if (hidden()) {
			if (hideTimer === undefined) {
				hideTimer = setTimeout(() => {
					hideTimer = undefined;
					// Re-check visibility: a setTimeout callback already dequeued when
					// clearHideTimer() runs cannot be cancelled, so the tab may have
					// become visible (and startTerm() may have begun a fresh term)
					// before this fires. Aborting unconditionally would tear down that
					// new term, stranding leadership at the poll floor. Only release if
					// the tab is still hidden.
					if (!hidden()) return;
					term?.abort();
					term = undefined;
				}, HIDE_GRACE_MS);
			}
		} else {
			clearHideTimer();
			startTerm();
		}
	};
	const onHideNow = (): void => endTerm();
	const onResume = (): void => {
		clearHideTimer();
		startTerm();
	};

	const doc: EventTarget | undefined = hasDoc ? (document as unknown as EventTarget) : undefined;
	const win: EventTarget | undefined = typeof window !== 'undefined'
		? (window as unknown as EventTarget)
		: undefined;
	doc?.addEventListener('visibilitychange', onVisibilityChange);
	doc?.addEventListener('freeze', onHideNow);
	doc?.addEventListener('resume', onResume);
	win?.addEventListener('pagehide', onHideNow);

	// Contend immediately when visible (or when there is no Document to gate on).
	startTerm();

	return {
		stop: () => {
			stopped = true;
			endTerm();
			doc?.removeEventListener('visibilitychange', onVisibilityChange);
			doc?.removeEventListener('freeze', onHideNow);
			doc?.removeEventListener('resume', onResume);
			win?.removeEventListener('pagehide', onHideNow);
		},
	};
}
