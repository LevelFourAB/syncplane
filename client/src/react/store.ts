// The bridge between the client's async watch callbacks and React's
// useSyncExternalStore. Two flavors share one underlying watch:
//
//   - default: a per-hook cache cell starts LOADING and is filled on the first
//     async callback. getSnapshot is synchronous and referentially stable
//     (WatcherHub hands us a NEW array only on a real change, so caching by
//     reference cannot loop useSyncExternalStore).
//
//   - suspense: a ref-counted, client-scoped cache keyed by the structural
//     identity string. A cache miss starts the watch and throws a promise that
//     resolves on the first fire, bridging the one-microtask gap before data
//     exists; once ready, rows are returned synchronously.
//
// The two differ in how they report a watch that could NOT be established: the
// default flavor puts the error on the result (there is a render to put it on),
// while the suspense flavor throws it, because a component that suspended has
// no rendered output to attach an error state to — that is what the error
// boundary beside the Suspense boundary is for.
//
// Neither flavor mutates rows — they are exposed readonly and React relies on
// that (no tearing) just as much as on the snapshot being stable.

import { useCallback, useRef, useSyncExternalStore } from 'react';

import type { SyncClient } from '../client.js';
import type { ReadonlyRowSet } from '../coverage.js';
import { asError, emptyResult, type WatchResult, withState } from './result.js';

/** Shared frozen sentinel returned before the first callback (and on the server). */
const LOADING = emptyResult<unknown>(true, undefined) as WatchResult<never>;

function loadingFor<T>(): WatchResult<T> {
	return LOADING as unknown as WatchResult<T>;
}

/**
 * Factory that wires the underlying watch and returns its sync unsubscribe.
 * `onError` reports a watch that will never fire (see WatchOptions.onError).
 */
export type WatchFactory<T> = (
	onRows: (rows: ReadonlyRowSet<T>) => void,
	onError: (error: unknown) => void,
) => () => void;

export interface WatchStoreParams<T> {
	/** Identity used to scope the suspense cache; also the resubscribe key. */
	identity: string;
	/** Subscribes the underlying watch; called once per (re)subscription. */
	subscribe: WatchFactory<T>;
	/** The client the watch belongs to; scopes the suspense cache. */
	client: SyncClient;
	/** Must be stable for the lifetime of a call site (do not toggle per render). */
	suspense?: boolean;
}

/**
 * Subscribes a watch and returns its latest rows with their load state,
 * suspending on first render when `suspense` is set. `suspense` must not change
 * between renders of a given component (it selects a fixed hook path), which is
 * why the public API exposes it as a separate hook rather than an option.
 */
export function useWatchStore<T>(params: WatchStoreParams<T>): WatchResult<T> {
	return params.suspense ? useSuspenseWatch(params) : useDefaultWatch(params);
}

// --- default (empty-then-fill) -------------------------------------------------

function useDefaultWatch<T>(params: WatchStoreParams<T>): WatchResult<T> {
	const { identity } = params;
	// Latest result for this hook; reset to LOADING when the subscription
	// identity changes so a stale snapshot is never shown for a different query.
	const cellRef = useRef<WatchResult<T>>(loadingFor<T>());
	const factoryRef = useRef(params.subscribe);
	factoryRef.current = params.subscribe;
	const identityRef = useRef(identity);
	if (identityRef.current !== identity) {
		identityRef.current = identity;
		cellRef.current = loadingFor<T>();
	}

	// Re-created only when identity changes, so React resubscribes exactly then.
	const subscribe = useCallback(
		(notify: () => void) => {
			const fail = (err: unknown) => {
				// Keep whatever rows already arrived: an established watch that later
				// dies leaves the last view on screen, with the error beside it.
				const rows = cellRef.current;
				cellRef.current = rows.length > 0
					? withState(rows, false, asError(err))
					: emptyResult<T>(false, asError(err), rows.coverage);
				notify();
			};
			let unsub: () => void;
			try {
				unsub = factoryRef.current((rows) => {
					cellRef.current = withState(rows, false, undefined);
					notify();
				}, fail);
			} catch (err) {
				// A synchronous throw — an unregistered entity, a bad include spec —
				// is the same "no rows will ever arrive" outcome as onError, and
				// notifying here is safe: React tolerates a store that changes
				// between render and subscribe.
				cellRef.current = emptyResult<T>(false, asError(err));
				notify();
				return () => {};
			}
			return () => {
				unsub();
				cellRef.current = loadingFor<T>();
			};
		},
		// eslint-disable-next-line react-hooks/exhaustive-deps
		[ identity ],
	);

	const getSnapshot = useCallback(() => cellRef.current, []);
	const getServerSnapshot = useCallback(() => loadingFor<T>(), []);
	return useSyncExternalStore(subscribe, getSnapshot, getServerSnapshot);
}

// --- suspense (throw-until-first-fire) ----------------------------------------

interface SuspenseEntry<T> {
	status: 'pending' | 'ready';
	promise: Promise<void>;
	rows: WatchResult<T>;
	/** Set when the watch could not be established; thrown on the retry render. */
	error: Error | undefined;
	unsub: () => void;
	/** useSyncExternalStore notifiers for post-first-fire updates. */
	notify: Set<() => void>;
	/** Active subscribers; the watch is torn down when this returns to 0. */
	refs: number;
	/** Pending GC timer when refs is 0 (cancelled if a subscriber returns). */
	gc: ReturnType<typeof setTimeout> | undefined;
}

// Client-scoped so two clients sharing an identity never alias each other's
// rows. WeakMap lets a closed/GC'd client drop its cache automatically.
const caches = new WeakMap<SyncClient, Map<string, SuspenseEntry<unknown>>>();

function cacheFor(client: SyncClient): Map<string, SuspenseEntry<unknown>> {
	let m = caches.get(client);
	if (m === undefined) {
		m = new Map();
		caches.set(client, m);
	}
	return m;
}

function createEntry<T>(
	client: SyncClient,
	identity: string,
	factory: WatchFactory<T>,
): SuspenseEntry<T> {
	const map = cacheFor(client);
	let resolve!: () => void;
	const entry: SuspenseEntry<T> = {
		status: 'pending',
		promise: new Promise<void>((r) => {
			resolve = r;
		}),
		rows: loadingFor<T>(),
		error: undefined,
		unsub: () => {},
		notify: new Set(),
		refs: 0,
		gc: undefined,
	};
	const settle = () => {
		const wasPending = entry.status === 'pending';
		entry.status = 'ready';
		if (wasPending) {
			resolve();
			// Resolved while nobody is (yet) subscribed — e.g. the component
			// unmounted during the fallback. GC unless a subscriber arrives.
			if (entry.refs === 0) scheduleGc(client, identity, entry);
		}
		for (const n of [ ...entry.notify ]) n();
	};
	const fail = (err: unknown) => {
		// Resolving (not rejecting) is deliberate: React retries the render, and
		// the retry throws entry.error synchronously. A rejected suspense promise
		// is handled inconsistently across React versions; a plain throw is not.
		entry.error = asError(err);
		settle();
	};
	try {
		entry.unsub = factory((rows) => {
			entry.rows = withState(rows, false, undefined);
			settle();
		}, fail);
	} catch (err) {
		fail(err);
	}
	map.set(identity, entry as SuspenseEntry<unknown>);
	return entry;
}

function scheduleGc<T>(client: SyncClient, identity: string, entry: SuspenseEntry<T>): void {
	if (entry.gc !== undefined) return;
	entry.gc = setTimeout(() => {
		if (entry.refs > 0) return;
		entry.unsub();
		const map = caches.get(client);
		if (map?.get(identity) === (entry as SuspenseEntry<unknown>)) map.delete(identity);
	}, 0);
}

function useSuspenseWatch<T>(params: WatchStoreParams<T>): WatchResult<T> {
	const { client, identity } = params;
	const factoryRef = useRef(params.subscribe);
	factoryRef.current = params.subscribe;

	const map = cacheFor(client);
	let entry = map.get(identity) as SuspenseEntry<T> | undefined;
	if (entry === undefined) {
		entry = createEntry(client, identity, factoryRef.current);
	}
	if (entry.status === 'pending') {
		// Suspend: the render is discarded, hooks below are not committed. React
		// retries (and runs the full hook list) once the promise resolves.
		throw entry.promise;
	}
	if (entry.error !== undefined) {
		// Drop the dead entry so a remount (after the boundary resets) retries
		// rather than replaying the same failure from cache forever.
		if (map.get(identity) === (entry as SuspenseEntry<unknown>)) map.delete(identity);
		throw entry.error;
	}

	const subscribe = useCallback(
		(notify: () => void) => {
			// Re-create if a prior GC tore the entry down between renders.
			let e = map.get(identity) as SuspenseEntry<T> | undefined;
			if (e === undefined) e = createEntry(client, identity, factoryRef.current);
			if (e.gc !== undefined) {
				clearTimeout(e.gc);
				e.gc = undefined;
			}
			e.refs++;
			e.notify.add(notify);
			return () => {
				e!.notify.delete(notify);
				e!.refs--;
				if (e!.refs === 0) scheduleGc(client, identity, e!);
			};
		},
		// eslint-disable-next-line react-hooks/exhaustive-deps
		[ client, identity ],
	);

	const getSnapshot = useCallback(() => {
		const e = map.get(identity) as SuspenseEntry<T> | undefined;
		return e?.rows ?? loadingFor<T>();
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [ identity ]);

	return useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
}
