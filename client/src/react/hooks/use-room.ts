import type { DescMessage } from '@bufbuild/protobuf';
import { useEffect, useMemo, useState } from 'react';

import type { Peer, Room, RoomMessage, RoomOptions } from '../../room.js';
import { useWorkspaceScope } from '../context.js';
import type { UseWorkspaceOptions } from '../types.js';

/** What useRoom returns: the live roster plus the handle for publishing. */
export interface UseRoomResult<S, M = S> {
	/**
	 * Everyone in the room, this client included, ordered by session id.
	 *
	 * The array keeps its reference until the roster actually changes, and an
	 * unchanged peer keeps its object identity across changes — so a memoized
	 * child re-renders when its own peer moved and not when somebody else's did.
	 */
	peers: readonly Peer<S>[];
	/**
	 * The open room — `setPresence`, `send`, `onMessage` — or undefined while it
	 * is joining. Referentially stable for as long as the identity holds.
	 */
	room: Room<S, M> | undefined;
	/** True until the first roster arrives. */
	loading: boolean;
	/**
	 * Set when the server withdrew the room: access was revoked, the anchor row
	 * went away, or the app's room hook refused. Terminal — reopening meets the
	 * same answer — so render it rather than retrying.
	 */
	error: Error | undefined;
}

/** Shared empty roster, so "nobody here" is one stable reference. */
const NO_PEERS: readonly Peer<never>[] = Object.freeze([]);

/**
 * What the hook reports, tagged with the room it describes so a changed key
 * never shows the previous room's peers while the new one joins.
 */
interface RoomState<S, M> {
	identity: string;
	peers: readonly Peer<S>[];
	room: Room<S, M> | undefined;
	loading: boolean;
	error: Error | undefined;
}

function joining<S, M>(identity: string): RoomState<S, M> {
	return {
		identity,
		peers: NO_PEERS as readonly Peer<S>[],
		room: undefined,
		loading: true,
		error: undefined,
	};
}

/** Options for useRoom, on top of which workspace it resolves against. */
export interface UseRoomOptions<S, M = S> extends UseWorkspaceOptions, RoomOptions<S, M> {
	/**
	 * Called for each broadcast message. Held in a ref, so it may change between
	 * renders without reopening the room — pass an inline closure freely.
	 */
	onMessage?: (msg: RoomMessage<M>) => void;
}

/**
 * Live view of one room in the resolved workspace: opens it, subscribes to the
 * roster, and re-renders as people come, go and move. The room closes — which
 * withdraws this session's record — when the component unmounts or the
 * workspace/anchor/name identity changes.
 *
 * Pass a schema and key to anchor the room to a row, which makes membership
 * inherit that row's read permission.
 */
export function useRoomHook<S, M = S>(
	name: string,
	opts?: UseRoomOptions<S, M>,
): UseRoomResult<S, M>;
export function useRoomHook<S, M = S, D extends DescMessage = DescMessage>(
	schema: D,
	key: string,
	name: string,
	opts?: UseRoomOptions<S, M>,
): UseRoomResult<S, M>;
export function useRoomHook<S, M = S>(
	a: string | DescMessage,
	b?: string | UseRoomOptions<S, M>,
	c?: string,
	d?: UseRoomOptions<S, M>,
): UseRoomResult<S, M> {
	return useRoomImpl(a, b, c, d);
}

/**
 * Both public hooks' body, resolving the two call shapes: `(name, opts)` for an
 * unanchored room, `(schema, key, name, opts)` for one anchored to a row.
 */
function useRoomImpl<S, M>(
	a: string | DescMessage,
	b: string | UseRoomOptions<S, M> | undefined,
	c: string | undefined,
	d: UseRoomOptions<S, M> | undefined,
): UseRoomResult<S, M> {
	const anchored = typeof a !== 'string';
	const schema = anchored ? a : undefined;
	const rowKey = anchored ? (b as string) : undefined;
	const roomName = anchored ? c! : a;
	const opts = anchored ? d : (b as UseRoomOptions<S, M> | undefined);

	const scope = useWorkspaceScope(opts?.workspaceId, opts?.undo);
	const identity = [
		scope.workspaceId,
		schema?.typeName ?? '',
		rowKey ?? '',
		roomName,
	].join('\u0000');

	const [ state, setState ] = useState<RoomState<S, M>>(() => joining(identity));

	// The message callback and the codec are read at open time only. Holding the
	// callback in state would reopen the room on every render that passes an
	// inline closure, which is every render.
	const [ latest ] = useState(() => ({ onMessage: opts?.onMessage }));
	latest.onMessage = opts?.onMessage;
	const [ openOpts ] = useState(() => ({ codec: opts?.codec, throttleMs: opts?.throttleMs }));

	useEffect(() => {
		let cancelled = false;
		const room = anchored
			? scope.room<S, M>(schema!, rowKey!, roomName, openOpts)
			: scope.room<S, M>(roomName, openOpts);

		const publish = (): void => {
			// A frame can land after the effect was cleaned up; setState then
			// would be an update on a component that no longer holds this room.
			if (cancelled) return;
			const status = room.status();
			setState({
				identity,
				peers: room.peers(),
				room,
				loading: status.state === 'joining',
				error: status.error === undefined
					? undefined
					: new Error(status.error.message, { cause: status.error.cause }),
			});
		};

		const unsubs = [
			room.subscribe(publish),
			room.onError(publish),
			room.onMessage((msg) => latest.onMessage?.(msg)),
		];
		publish();

		return () => {
			cancelled = true;
			for (const unsub of unsubs) unsub();
			// close() publishes the departure so the other participants see it go
			// at once; the round trip it owes must not hold up the unmount.
			void room.close();
		};
	}, [ scope, anchored, schema, rowKey, roomName, identity, openOpts, latest ]);

	// A state carrying a different identity belongs to the previous room:
	// report loading until the new one publishes.
	const pending = useMemo(() => joining<S, M>(identity), [ identity ]);
	const current = state.identity === identity ? state : pending;
	return useMemo<UseRoomResult<S, M>>(() => ({
		peers: current.peers,
		room: current.room,
		loading: current.loading,
		error: current.error,
	}), [ current ]);
}

/**
 * The roster of one room, for a component that renders who is here and never
 * publishes.
 *
 * Equivalent to `useRoom(...).peers`: the room is opened, subscribed to and
 * closed on exactly the same terms, and this session appears in the roster it
 * returns.
 */
export function usePresenceHook<S>(
	name: string,
	opts?: UseRoomOptions<S, S>,
): readonly Peer<S>[];
export function usePresenceHook<S, D extends DescMessage = DescMessage>(
	schema: D,
	key: string,
	name: string,
	opts?: UseRoomOptions<S, S>,
): readonly Peer<S>[];
export function usePresenceHook<S>(
	a: string | DescMessage,
	b?: string | UseRoomOptions<S, S>,
	c?: string,
	d?: UseRoomOptions<S, S>,
): readonly Peer<S>[] {
	return useRoomImpl<S, S>(a, b, c, d).peers;
}
