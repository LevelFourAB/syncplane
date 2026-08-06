// One watch per (client, workspace, entity, include) — shared by every useRow
// on it — with a key → row index in front of it.
//
// The hook used to watch the whole entity per call site and `find` its key in
// the callback, so a list of N rows was N hub watchers, each re-scanning the
// entity on every commit, and N linear scans on top. Here the entity is watched
// ONCE and indexed once per commit; a hook costs one Map lookup and re-renders
// only when its own row moves. That is the difference between a row hook you
// can put in a list and one you cannot.
//
// The refcount/GC shape deliberately mirrors the suspense cache in store.ts:
// entries are client-scoped, torn down a macrotask after the last subscriber
// leaves, and revived if one returns first.

import { useCallback, useRef, useSyncExternalStore } from 'react';

import type { SyncClient } from '../client.js';
import type { ReadonlyRowSet } from '../coverage.js';
import type { Row } from '../read.js';
import type { WatchHandle } from '../watch.js';
import { asError } from './result.js';
import type { WatchFactory } from './store.js';

/** What one useRow call site observes: its row, and how the watch behind it is. */
export interface RowCell<T> {
	row: Row<T> | undefined;
	/** True until the entity's first callback; see WatchState.loading. */
	loading: boolean;
	/**
	 * True while the entity's first snapshot is still arriving; see
	 * WatchState.hydrating. An absent `row` means "not here yet" rather than "no
	 * such row" while this is set — the distinction a key lookup cannot make on
	 * its own.
	 */
	hydrating: boolean;
	/** Set when the watch could not be established; see WatchState.error. */
	error: Error | undefined;
}

/**
 * Shared "nothing has arrived yet" cell. Identical for every key, so one frozen
 * object serves them all and getSnapshot stays referentially stable.
 */
const LOADING: RowCell<never> = Object.freeze({
	row: undefined,
	loading: true,
	hydrating: false,
	error: undefined,
});

function loadingCell<T>(): RowCell<T> {
	return LOADING as unknown as RowCell<T>;
}

interface RowEntry<T> {
	status: 'pending' | 'ready';
	promise: Promise<void>;
	/** Resolves the promise above on the first callback (or on failure). */
	settle: () => void;
	byKey: Map<string, Row<T>>;
	/** Memoized snapshots, one per key any hook has asked about. */
	cells: Map<string, RowCell<T>>;
	/** Notifiers per key, so an unrelated row's change re-renders nothing. */
	listeners: Map<string, Set<() => void>>;
	/** Coverage state of the last delivered result; see WatchState.hydrating. */
	hydrating: boolean;
	error: Error | undefined;
	handle: WatchHandle;
	refs: number;
	gc: ReturnType<typeof setTimeout> | undefined;
}

const caches = new WeakMap<SyncClient, Map<string, RowEntry<unknown>>>();

function cacheFor(client: SyncClient): Map<string, RowEntry<unknown>> {
	let m = caches.get(client);
	if (m === undefined) {
		m = new Map();
		caches.set(client, m);
	}
	return m;
}

/** The cell for `key`, computed and memoized on first ask. */
function cellFor<T>(entry: RowEntry<T>, key: string): RowCell<T> {
	const existing = entry.cells.get(key);
	if (existing !== undefined) return existing;
	if (entry.status === 'pending' && entry.error === undefined) return loadingCell<T>();
	const cell: RowCell<T> = {
		row: entry.byKey.get(key),
		loading: false,
		hydrating: entry.hydrating,
		error: entry.error,
	};
	entry.cells.set(key, cell);
	return cell;
}

/**
 * Re-indexes after a callback and notifies only the keys that actually moved.
 *
 * Row objects are reused across commits by WatcherHub (it patches a snapshot in
 * place rather than re-decoding), so identity comparison is a sound change
 * test — and the reason a commit touching one row does not re-render the other
 * hooks on the same entity.
 */
function publish<T>(entry: RowEntry<T>, rows: ReadonlyRowSet<T>): void {
	const next = new Map<string, Row<T>>();
	for (const row of rows) next.set(row.key, row);
	const wasPending = entry.status === 'pending';
	entry.byKey = next;
	entry.status = 'ready';
	// The entity finishing its first snapshot moves every cell, including the
	// ones whose row did not arrive: "missing" stops meaning "still coming".
	const hydratingChanged = entry.hydrating !== rows.coverage.bounds.includes('hydrating');
	entry.hydrating = rows.coverage.bounds.includes('hydrating');
	for (const [ key, listeners ] of entry.listeners) {
		const prev = entry.cells.get(key);
		const row = next.get(key);
		if (
			!wasPending && !hydratingChanged && prev !== undefined && prev.row === row
			&& prev.error === entry.error
		) {
			continue;
		}
		entry.cells.set(key, { row, loading: false, hydrating: entry.hydrating, error: entry.error });
		for (const notify of [ ...listeners ]) notify();
	}
	// Keys nobody is subscribed to (looked at once, then unmounted) must not keep
	// serving a snapshot from before this commit.
	for (const key of entry.cells.keys()) {
		if (!entry.listeners.has(key)) entry.cells.delete(key);
	}
	if (wasPending) entry.settle();
}

function fail<T>(entry: RowEntry<T>, err: unknown): void {
	entry.error = asError(err);
	const wasPending = entry.status === 'pending';
	entry.status = 'ready';
	for (const [ key, listeners ] of entry.listeners) {
		// Rows already delivered stay: the view stops tracking the store rather
		// than emptying out under the error.
		entry.cells.set(key, {
			row: entry.byKey.get(key),
			loading: false,
			hydrating: entry.hydrating,
			error: entry.error,
		});
		for (const notify of [ ...listeners ]) notify();
	}
	if (wasPending) entry.settle();
}

function createEntry<T>(
	client: SyncClient,
	identity: string,
	factory: WatchFactory<T>,
): RowEntry<T> {
	let resolve!: () => void;
	const entry: RowEntry<T> = {
		status: 'pending',
		promise: new Promise<void>((r) => {
			resolve = r;
		}),
		settle: () => {
			resolve();
			if (entry.refs === 0) scheduleGc(client, identity, entry);
		},
		byKey: new Map(),
		cells: new Map(),
		listeners: new Map(),
		hydrating: false,
		error: undefined,
		handle: { close: () => {}, setWindow: () => {} },
		refs: 0,
		gc: undefined,
	};
	cacheFor(client).set(identity, entry as RowEntry<unknown>);
	try {
		entry.handle = factory((rows) => publish(entry, rows), (err) => fail(entry, err));
	} catch (err) {
		fail(entry, err);
	}
	return entry;
}

function scheduleGc<T>(client: SyncClient, identity: string, entry: RowEntry<T>): void {
	if (entry.gc !== undefined) return;
	entry.gc = setTimeout(() => {
		if (entry.refs > 0) return;
		entry.handle.close();
		const map = caches.get(client);
		if (map?.get(identity) === (entry as RowEntry<unknown>)) map.delete(identity);
	}, 0);
}

function entryFor<T>(
	client: SyncClient,
	identity: string,
	factory: WatchFactory<T>,
): RowEntry<T> {
	const map = cacheFor(client);
	const existing = map.get(identity) as RowEntry<T> | undefined;
	return existing ?? createEntry(client, identity, factory);
}

export interface RowStoreParams<T> {
	/** Identity of the shared watch: workspace + entity + include. */
	identity: string;
	/** Subscribes the underlying whole-entity watch. */
	subscribe: WatchFactory<T>;
	client: SyncClient;
	key: string;
	/** Suspend until the entity's first callback instead of reporting loading. */
	suspense?: boolean;
}

/**
 * Subscribes the shared entity watch and returns the cell for one key.
 * `suspense` must be stable for a given call site (it selects a hook path),
 * which is why the public API exposes it as a separate hook.
 */
export function useRowStore<T>(params: RowStoreParams<T>): RowCell<T> {
	const { client, identity, key } = params;
	const factoryRef = useRef(params.subscribe);
	factoryRef.current = params.subscribe;

	if (params.suspense) {
		const entry = entryFor<T>(client, identity, factoryRef.current);
		if (entry.status === 'pending') throw entry.promise;
		if (entry.error !== undefined) {
			// Drop the dead entry so a boundary reset retries instead of replaying
			// the same failure from cache forever.
			const map = cacheFor(client);
			if (map.get(identity) === (entry as RowEntry<unknown>)) map.delete(identity);
			throw entry.error;
		}
	}

	const subscribe = useCallback(
		(notify: () => void) => {
			const entry = entryFor<T>(client, identity, factoryRef.current);
			if (entry.gc !== undefined) {
				clearTimeout(entry.gc);
				entry.gc = undefined;
			}
			entry.refs++;
			let listeners = entry.listeners.get(key);
			if (listeners === undefined) {
				listeners = new Set();
				entry.listeners.set(key, listeners);
			}
			listeners.add(notify);
			return () => {
				listeners!.delete(notify);
				if (listeners!.size === 0) {
					entry.listeners.delete(key);
					entry.cells.delete(key);
				}
				entry.refs--;
				if (entry.refs === 0) scheduleGc(client, identity, entry);
			};
		},
		[ client, identity, key ],
	);

	const getSnapshot = useCallback(() => {
		const entry = cacheFor(client).get(identity) as RowEntry<T> | undefined;
		return entry === undefined ? loadingCell<T>() : cellFor(entry, key);
	}, [ client, identity, key ]);

	const getServerSnapshot = useCallback(() => loadingCell<T>(), []);
	return useSyncExternalStore(subscribe, getSnapshot, getServerSnapshot);
}
