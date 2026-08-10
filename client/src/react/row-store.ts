// One keyed watch per (client, workspace, entity, include) — shared by every
// useRow and useRows on it — over the union of the keys those call sites hold.
//
// A row hook backed by an entity watch derives the whole entity to find one row
// in it: a full scan and a decode per row at mount, that much memory for as
// long as any hook is up, and a pass over all of it on every commit. This store
// owns a key set instead and the hub reads exactly those keys (see
// WatcherHub.watchKeys), so a screen of row views costs its own rows and
// nothing for the entity around them.
//
// Keys are refcounted individually and synced to the watch in one batch per
// turn, which is what keeps a list of a hundred row hooks mounting together to
// a single read. The entry-level GC mirrors the suspense cache in store.ts:
// torn down a macrotask after the last subscriber leaves, and kept if one
// returns first.

import type { DescMessage, MessageShape } from '@bufbuild/protobuf';
import { useCallback, useRef, useSyncExternalStore } from 'react';

import type { SyncClient, WorkspaceClient } from '../client.js';
import type { Coverage } from '../coverage.js';
import type { Row } from '../read.js';
import type { IncludeSpec } from '../refs.js';
import type { KeyedRows, KeyedWatchHandle } from '../watch.js';
import { asError, type KeyedRowsResult, withKeyedState } from './result.js';
import { stableHash } from './stable-hash.js';

/** What one useRow call site observes: its row, and how the watch behind it is. */
export interface RowCell<T> {
	row: Row<T> | undefined;
	/** True until this key's first callback; see WatchState.loading. */
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
 * Factory that wires the underlying keyed watch and returns its handle. It
 * subscribes no keys — the store sets them — so one factory serves every call
 * site sharing the entry. `onError` reports a watch that will never fire (see
 * WatchOptions.onError).
 */
export type KeyedWatchFactory<T> = (
	onRows: (rows: KeyedRows<T>, coverage: Coverage) => void,
	onError: (error: unknown) => void,
) => KeyedWatchHandle;

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

/** One watched key: who is holding it, what it holds, and whether it has landed. */
interface KeyState<T> {
	/** Live subscribers. The key leaves the watch when this returns to 0. */
	refs: number;
	listeners: Set<() => void>;
	/** Memoized snapshot, dropped whenever anything it reports moves. */
	cell: RowCell<T> | undefined;
	row: Row<T> | undefined;
	/** False until the first callback carrying this key — including "no such row". */
	answered: boolean;
	/** Resolves on that first callback (or on failure); what suspense throws. */
	promise: Promise<void>;
	settle: () => void;
}

interface RowEntry<T> {
	client: SyncClient;
	identity: string;
	keys: Map<string, KeyState<T>>;
	hydrating: boolean;
	error: Error | undefined;
	handle: KeyedWatchHandle;
	/** Whether a key sync is scheduled; see scheduleSync. */
	syncing: boolean;
	/** Pending sweep of zero-ref keys (and of the entry, once none are left). */
	gc: ReturnType<typeof setTimeout> | undefined;
}

// Client-scoped so two clients sharing an identity never alias each other's
// rows. WeakMap lets a closed/GC'd client drop its cache automatically.
const caches = new WeakMap<SyncClient, Map<string, RowEntry<unknown>>>();

function cacheFor(client: SyncClient): Map<string, RowEntry<unknown>> {
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
	factory: KeyedWatchFactory<T>,
): RowEntry<T> {
	const entry: RowEntry<T> = {
		client,
		identity,
		keys: new Map(),
		hydrating: false,
		error: undefined,
		handle: { close: () => {}, setKeys: () => {} },
		syncing: false,
		gc: undefined,
	};
	cacheFor(client).set(identity, entry as RowEntry<unknown>);
	try {
		entry.handle = factory(
			(rows, coverage) => publish(entry, rows, coverage),
			(err) => fail(entry, err),
		);
	} catch (err) {
		fail(entry, err);
	}
	return entry;
}

function entryFor<T>(
	client: SyncClient,
	identity: string,
	factory: KeyedWatchFactory<T>,
): RowEntry<T> {
	const map = cacheFor(client);
	const existing = map.get(identity) as RowEntry<T> | undefined;
	return existing ?? createEntry(client, identity, factory);
}

/**
 * The state for `key`, registering it with the watch on first ask. Called from
 * render on the suspense path, so a key whose component never commits is left
 * at zero refs — publish and release both sweep those.
 */
function keyState<T>(entry: RowEntry<T>, key: string): KeyState<T> {
	const existing = entry.keys.get(key);
	if (existing !== undefined) return existing;
	let resolve!: () => void;
	const state: KeyState<T> = {
		refs: 0,
		listeners: new Set(),
		cell: undefined,
		row: undefined,
		// A dead watch answers nothing, so a key joining one is answered on
		// arrival rather than left suspended on a callback that cannot come.
		answered: entry.error !== undefined,
		promise: new Promise<void>((r) => {
			resolve = r;
		}),
		settle: () => resolve(),
	};
	if (entry.error !== undefined) resolve();
	entry.keys.set(key, state);
	scheduleSync(entry);
	return state;
}

/**
 * Pushes the key set to the watch, coalescing the calls made within one turn.
 * A list of row hooks mounting together registers one key each; without this
 * each would re-diff the whole set.
 */
function scheduleSync<T>(entry: RowEntry<T>): void {
	if (entry.syncing) return;
	entry.syncing = true;
	queueMicrotask(() => {
		entry.syncing = false;
		if (cacheFor(entry.client).get(entry.identity) !== (entry as RowEntry<unknown>)) return;
		entry.handle.setKeys([ ...entry.keys.keys() ]);
	});
}

/**
 * Schedules the drop of keys nobody holds, and of the entry once it holds none.
 *
 * Deliberately not cancelled when a subscriber arrives first: the sweep keeps
 * whatever has refs by then, so a remount inside the same tick — StrictMode, a
 * list reordering — finds its key still watched and its row still there.
 */
function scheduleSweep<T>(entry: RowEntry<T>): void {
	if (entry.gc !== undefined) return;
	entry.gc = setTimeout(() => {
		entry.gc = undefined;
		const map = caches.get(entry.client);
		if (map?.get(entry.identity) !== (entry as RowEntry<unknown>)) return;
		for (const [ key, state ] of entry.keys) {
			if (state.refs === 0) entry.keys.delete(key);
		}
		if (entry.keys.size === 0) {
			entry.handle.close();
			map.delete(entry.identity);
			return;
		}
		entry.handle.setKeys([ ...entry.keys.keys() ]);
	}, 0);
}

function notify<T>(state: KeyState<T>): void {
	for (const listener of [ ...state.listeners ]) listener();
}

/**
 * Records a callback and notifies only the keys it moved.
 *
 * Row objects are reused across commits by WatcherHub (it patches a snapshot in
 * place rather than re-decoding), so identity comparison is a sound change
 * test — and the reason a commit touching one row does not re-render the hooks
 * holding the others.
 */
function publish<T>(entry: RowEntry<T>, rows: KeyedRows<T>, coverage: Coverage): void {
	const hydrating = coverage.bounds.includes('hydrating');
	const hydratingMoved = hydrating !== entry.hydrating;
	entry.hydrating = hydrating;
	for (const [ key, row ] of rows) {
		const state = entry.keys.get(key);
		if (state === undefined) continue;
		if (state.answered && state.row === row && !hydratingMoved) continue;
		const wasAnswered = state.answered;
		state.answered = true;
		state.row = row;
		state.cell = undefined;
		if (!wasAnswered) state.settle();
		notify(state);
		// Answered while nobody is (yet) subscribed — a component that unmounted
		// during its own suspense fallback, say.
		if (state.refs === 0) scheduleSweep(entry);
	}
	if (!hydratingMoved) return;
	// The entity finishing its first snapshot moves every cell, including the
	// keys this callback did not name: "missing" stops meaning "still coming".
	for (const [ key, state ] of entry.keys) {
		if (rows.has(key) || !state.answered) continue;
		state.cell = undefined;
		notify(state);
	}
}

function fail<T>(entry: RowEntry<T>, err: unknown): void {
	entry.error = asError(err);
	for (const state of entry.keys.values()) {
		// Rows already delivered stay: the view stops tracking the store rather
		// than emptying out under the error.
		const wasAnswered = state.answered;
		state.answered = true;
		state.cell = undefined;
		if (!wasAnswered) state.settle();
		notify(state);
	}
}

/** The cell for `key`, computed and memoized on first ask. */
function cellFor<T>(entry: RowEntry<T>, key: string): RowCell<T> {
	const state = entry.keys.get(key);
	if (state === undefined) return loadingCell<T>();
	if (state.cell !== undefined) return state.cell;
	if (!state.answered) return loadingCell<T>();
	const cell: RowCell<T> = {
		row: state.row,
		loading: false,
		hydrating: entry.hydrating,
		error: entry.error,
	};
	state.cell = cell;
	return cell;
}

/**
 * Drops an entry whose watch died, so a boundary reset retries instead of
 * replaying the same failure from cache forever.
 */
function discard<T>(entry: RowEntry<T>): void {
	const map = cacheFor(entry.client);
	if (map.get(entry.identity) === (entry as RowEntry<unknown>)) map.delete(entry.identity);
}

/**
 * Identity of the watch every row hook on one workspace and entity shares. The
 * row key is deliberately absent — every key on this entity is served from one
 * subscription — and the `row` namespace keeps it from ever aliasing a useQuery
 * subscription on the same entity.
 *
 * `/` separates unambiguously: it is legal in neither a workspace id nor a proto
 * type name (see naming.ts), so no two triples can spell the same string.
 */
export function rowWatchIdentity(
	workspaceId: string,
	typeName: string,
	include?: IncludeSpec,
): string {
	return stableHash(`row/${workspaceId}/${typeName}`, undefined, include);
}

/** Subscribes the shared keyed watch behind rowWatchIdentity's identity. */
export function rowWatchOf<D extends DescMessage>(
	scope: WorkspaceClient,
	schema: D,
	include?: IncludeSpec,
): KeyedWatchFactory<MessageShape<D>> {
	return (onRows, onError) => scope.watchRows(schema, [], onRows, { include, onError });
}

export interface RowStoreParams<T> {
	/** Identity of the shared watch: workspace + entity + include. */
	identity: string;
	/** Subscribes the underlying keyed watch, with no keys of its own. */
	subscribe: KeyedWatchFactory<T>;
	client: SyncClient;
	key: string;
	/** Suspend until this key's first callback instead of reporting loading. */
	suspense?: boolean;
}

/**
 * Subscribes the shared keyed watch and returns the cell for one key.
 * `suspense` must be stable for a given call site (it selects a hook path),
 * which is why the public API exposes it as a separate hook.
 */
export function useRowStore<T>(params: RowStoreParams<T>): RowCell<T> {
	const { client, identity, key } = params;
	const factoryRef = useRef(params.subscribe);
	factoryRef.current = params.subscribe;

	if (params.suspense) {
		const entry = entryFor<T>(client, identity, factoryRef.current);
		const state = keyState(entry, key);
		if (entry.error !== undefined) {
			discard(entry);
			throw entry.error;
		}
		if (!state.answered) throw state.promise;
	}

	const subscribe = useCallback(
		(onStoreChange: () => void) => {
			const entry = entryFor<T>(client, identity, factoryRef.current);
			const state = keyState(entry, key);
			state.refs++;
			state.listeners.add(onStoreChange);
			return () => {
				state.listeners.delete(onStoreChange);
				state.refs--;
				if (state.refs === 0) scheduleSweep(entry);
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

export interface RowsStoreParams<T> {
	/** Identity of the shared watch; the same one useRowStore uses. */
	identity: string;
	subscribe: KeyedWatchFactory<T>;
	client: SyncClient;
	/** Keys to read, in the order the result is returned in. */
	keys: readonly string[];
	/** Suspend until every key has been answered. */
	suspense?: boolean;
}

/**
 * Subscribes the shared keyed watch for several keys and returns one row per
 * key, in the order asked for.
 *
 * The result array is rebuilt only when one of these keys moves — a commit
 * elsewhere in the entity notifies nothing here — so it is stable enough to
 * pass straight into a memoized child.
 */
export function useRowsStore<T>(params: RowsStoreParams<T>): KeyedRowsResult<T> {
	const { client, identity, keys } = params;
	const factoryRef = useRef(params.subscribe);
	factoryRef.current = params.subscribe;
	const keysRef = useRef(keys);
	keysRef.current = keys;
	// Identity of the key LIST: a caller passes a fresh array every render, and
	// only its contents decide what this call site holds.
	const listId = JSON.stringify(keys);

	const cellRef = useRef<KeyedRowsResult<T> | undefined>(undefined);
	const serverRef = useRef<KeyedRowsResult<T> | undefined>(undefined);
	const dirtyRef = useRef(true);
	const listIdRef = useRef(listId);
	if (listIdRef.current !== listId) {
		listIdRef.current = listId;
		dirtyRef.current = true;
		serverRef.current = undefined;
	}

	if (params.suspense) {
		const entry = entryFor<T>(client, identity, factoryRef.current);
		if (entry.error !== undefined) {
			discard(entry);
			throw entry.error;
		}
		const waiting = keys
			.map((key) => keyState(entry, key))
			.filter((state) => !state.answered)
			.map((state) => state.promise);
		if (waiting.length > 0) throw Promise.all(waiting);
	}

	const subscribe = useCallback(
		(onStoreChange: () => void) => {
			const entry = entryFor<T>(client, identity, factoryRef.current);
			const mark = () => {
				dirtyRef.current = true;
				onStoreChange();
			};
			const held = keysRef.current.map((key) => {
				const state = keyState(entry, key);
				state.refs++;
				state.listeners.add(mark);
				return state;
			});
			dirtyRef.current = true;
			return () => {
				for (const state of held) {
					state.listeners.delete(mark);
					state.refs--;
				}
				scheduleSweep(entry);
			};
		},
		[ client, identity, listId ],
	);

	const getSnapshot = useCallback(() => {
		if (!dirtyRef.current && cellRef.current !== undefined) return cellRef.current;
		const entry = cacheFor(client).get(identity) as RowEntry<T> | undefined;
		const rows: (Row<T> | undefined)[] = [];
		let loading = false;
		// One slot per key whether or not it has been answered, so `rows[i]` names
		// `keys[i]` from the very first render.
		for (const key of keysRef.current) {
			const state = entry?.keys.get(key);
			rows.push(state?.row);
			if (state?.answered !== true) loading = true;
		}
		const cell = withKeyedState(rows, loading, entry?.hydrating ?? false, entry?.error);
		dirtyRef.current = false;
		cellRef.current = cell;
		return cell;
	}, [ client, identity, listId ]);

	const getServerSnapshot = useCallback(() => {
		serverRef.current ??= withKeyedState<T>(
			keysRef.current.map(() => undefined),
			true,
			false,
			undefined,
		);
		return serverRef.current;
	}, [ listId ]);
	return useSyncExternalStore(subscribe, getSnapshot, getServerSnapshot);
}
