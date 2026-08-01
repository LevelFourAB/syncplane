import {
	createElement,
	type ReactElement,
	type ReactNode,
	useEffect,
	useRef,
	useState,
} from 'react';

import { SyncClient, type SyncClientOptions } from '../client.js';
import { SyncContext } from './context.js';

export interface SyncProviderProps {
	/** A caller-constructed client, whose lifecycle the caller keeps. */
	client?: SyncClient;
	/**
	 * Construct and own the client instead: the provider builds it on mount,
	 * `start()`s it, and `close()`s it when the tree unmounts. Read ONCE — a
	 * later change is ignored, because swapping the store under a mounted tree is
	 * not something a re-render can do safely. To move to another account,
	 * remount the provider with a different React `key`.
	 *
	 * Give exactly one of `client` and `options`.
	 */
	options?: SyncClientOptions;
	/**
	 * Called when the owned client's `start()` rejects — a local failure such as
	 * bad configuration or a KV format mismatch, not an unreachable server (that
	 * shows up on `useSyncStatus()` and retries itself). Without a handler the
	 * error is thrown during the next render so the nearest error boundary sees
	 * it, rather than becoming an unhandled rejection nobody notices. Ignored
	 * when the caller owns the client.
	 */
	onStartError?: (error: unknown) => void;
	children?: ReactNode;
}

/**
 * Pending closes of owned clients, keyed by the client itself.
 *
 * `close()` is one-shot and a closed client cannot be reopened, so an effect
 * that closed on cleanup would break under StrictMode's mount → unmount →
 * remount, which reuses the SAME client: the second mount would start a corpse.
 * Deferring the close by a macrotask and cancelling it when the effect runs
 * again makes a remount a no-op while a real unmount still closes.
 */
const pendingCloses = new WeakMap<SyncClient, ReturnType<typeof setTimeout>>();

/**
 * Makes a client available to the hooks below it — either one the caller built
 * (`client`) or one the provider constructs and owns (`options`). Written with
 * React.createElement (no JSX) so this file stays a plain `.ts` and the build's
 * tsconfig needs no JSX option.
 */
export function SyncProvider(props: SyncProviderProps): ReactElement {
	const { client, options, onStartError } = props;
	const ownsClient = client === undefined && options !== undefined;

	// Constructed once per mount, lazily. The constructor touches no storage (the
	// KV store opens on first use), so the copy StrictMode's double-invoked
	// initializer discards costs nothing and leaves nothing to clean up.
	const [ owned ] = useState<SyncClient | undefined>(() =>
		ownsClient ? new SyncClient(options!) : undefined
	);
	const [ startError, setStartError ] = useState<unknown>(undefined);
	// Held in a ref so an inline callback's identity cannot re-run the effect
	// below — which would close and restart the client on every render.
	const onStartErrorRef = useRef(onStartError);
	onStartErrorRef.current = onStartError;

	useEffect(() => {
		if (!owned) return;
		const pending = pendingCloses.get(owned);
		if (pending !== undefined) {
			// A StrictMode remount (or a fast unmount/remount) came back for this
			// client before the deferred close ran.
			clearTimeout(pending);
			pendingCloses.delete(owned);
		}
		// start() memoizes its in-flight promise, so the remount above rejoins the
		// first start instead of running a second one.
		owned.start().catch((err: unknown) => {
			const handler = onStartErrorRef.current;
			if (handler) {
				handler(err);
				return;
			}
			setStartError(err);
		});
		return () => {
			pendingCloses.set(
				owned,
				setTimeout(() => {
					pendingCloses.delete(owned);
					void owned.close();
				}, 0),
			);
		};
	}, [ owned ]);

	// Hooks first, then the shape check, so a component that starts rendering
	// with bad props fails on the props rather than on a changed hook count.
	if ((client === undefined) === (options === undefined)) {
		throw new Error(
			'SyncProvider: give exactly one of `client` (you own its lifecycle) or '
				+ '`options` (the provider constructs, starts and closes it).',
		);
	}
	// A local start failure leaves nothing worth rendering underneath: the client
	// never synced and will not without intervention.
	if (startError !== undefined) throw startError;

	return createElement(SyncContext.Provider, { value: client ?? owned! }, props.children);
}
