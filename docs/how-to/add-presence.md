# Adding presence to an app

This guide shows you how to track and display ephemeral presence state—such as
user rosters, active selections, and cursors—across connected clients in your
application. Use presence when you need real-time awareness that only matters
while clients are connected and does not need to be stored or replayed. For more
details on the room model, see [Rooms](../explanation/rooms.md).

## Prerequisites

Before adding presence to your application, make sure you have:

- A running Syncplane server. The presence service is mounted by
  `HTTPHandler()` and `Handlers()` alongside the sync service, with no extra
  server configuration or bus required for single-node deployments.
- An established client WebSocket connection (`ws`).
- An authenticator configured with
  [`WithAuthenticator`](authentication-and-permissions.md) if you want to
  display verified user identities from `Peer.subject`.

## Adding presence

Follow these steps to configure and use rooms for presence in your client
application.

### 1. Join a room

To join a room by name, call `ws.room()` with the state type:

```ts
const room = ws.room<{ name: string; color: string }>('lobby');
```

If you want membership to inherit read permissions from a data row, pass the
schema and row key:

```ts
const room = ws.room(DocumentSchema, docId, 'cursors');
```

If the row is hidden by your `ReadFilter`, deleted, or was never created, the
room is dropped with `room_dropped` and `Room.status()` reports `dropped`. This
status is terminal, and reopening returns the same result.

If you send raw bytes rather than JSON (for example, with text anchors), provide
custom codec functions in the room options:

```ts
import { decodeAnchor, encodeAnchor } from 'syncplane';

const text = await ws.text(DocumentSchema, docId, 'body');
const room = ws.room<Uint8Array>(DocumentSchema, docId, 'cursors', {
	// The state is already bytes, so hand them through instead of JSON.
	codec: {
		encodeState: (s) => s,
		decodeState: (b) => b,
		encodeMessage: (m) => m,
		decodeMessage: (b) => b,
	},
});
```

### 2. Publish your presence state

To announce your state to other peers in the room, call `setPresence()`:

```ts
room.setPresence({ name: 'Ada', color: '#c0f' });
```

Calls to `setPresence` are throttled (60 ms by default, configured by
`throttleMs`). You can call it on every mouse movement; intermediate positions
are dropped and the latest position is sent.

If you publish collaborative text cursors, publish
[text anchors](../explanation/collaborative-text.md) instead of integer
offsets:

```ts
editor.onSelectionChange((index) => {
	room.setPresence(encodeAnchor(text.anchorAt(index)), { seq: lastSyncedSeq });
});
```

If you need to indicate that an asynchronous operation is in progress (such as
AI generation), publish status updates using presence:

```ts
stream.setPresence({ status: 'thinking' });
// …
stream.setPresence({ status: 'idle' });
```

### 3. Subscribe to peer updates

To receive updates whenever peers join, leave, or change state, call
`subscribe()`:

```ts
room.subscribe((peers) => {
	render(peers.map((p) => ({ ...p.state, self: p.self })));
});
```

The `peers` array contains all active peer records including your own, ordered
by session ID. The array is frozen and keeps its reference until data changes,
making it safe to pass to `useSyncExternalStore`. `Peer.subject` is stamped by
the server from the request principal and cannot be forged by the client.

If you resolve collaborative text anchors, wrap resolution in a `try`/`catch`
block to handle anchors that point to pending or compacted text:

```ts
room.subscribe((peers) => {
	for (const p of peers) {
		if (p.self || p.state === undefined) continue;
		try {
			drawCaret(p.sessionId, text.resolve(decodeAnchor(p.state)));
		} catch {
			// The item is gone or has not arrived. Skip this peer's caret this
			// frame; the next announcement brings a fresh position.
		}
	}
});
```

If you use React, you can use the `usePresence` hook to manage subscription
and state:

```tsx
function Presence() {
	const peers = usePresence<{ name: string; color: string }>('lobby');
	return <AvatarStrip people={ peers.filter((p) => !p.self) } />;
}
```

### 4. Broadcast ephemeral messages (optional)

To send transient messages to whoever is currently connected, call `send()` and
listen with `onMessage()`:

```ts
room.onMessage((m) => {
	if (!m.self) flashReaction(m.senderSubject, m.message);
});
await room.send({ kind: 'wave' });
```

Messages are delivered at most once. The `send()` call resolves when the server
accepts the message, not when peers receive it. If the client cannot reach the
server, `send()` rejects.

If you implement features like agent chat where transcript data must persist
while intermediate tokens are transient, write the durable data to a row and
broadcast the transient hints:

```ts
// The transcript is data. It survives a reload and a new reader gets it.
await ws.mutate.put(MessageSchema, { id, threadId, author, body });

// The token stream is not. It exists to make the wait bearable.
const stream = ws.room<string>(ThreadSchema, threadId, 'tokens');
for await (const token of completion) await stream.send(token);
```

### 5. Close the room

When the view or component is unmounted, close the room to withdraw your record
immediately:

```ts
await room.close();
```

If you use the React `usePresence` hook, the hook closes the room
automatically on unmount.

## Confirming the result

To confirm that presence is functioning:

1. Open your application in two separate browser windows or tabs.
2. Verify that each window displays the other user in the peer roster.
3. Trigger a state change in one window (such as updating presence or moving a
   cursor) and verify that the other window receives and renders the update.
4. Close one window and verify that the corresponding peer is removed from the
   active roster.

## Filtering participant visibility

If you need to control which participants can see each other in a room,
implement `RoomPermissions` on the server:

```go
type policy struct {
	server.AllowAll
	server.AllowAllRooms
}

func (policy) VisibleParticipants(
	_ context.Context, p server.Principal, _ string, _ presence.Topic,
) (server.ParticipantFilter, error) {
	if p.IsAnonymous() {
		return func(string) bool { return false }, nil // browse without seeing who else is
	}
	return nil, nil
}
```

Both hooks run when a client joins, and `CanJoinRoom` runs again on the
stream heartbeat. Access revocations take effect within one heartbeat interval.

## Error behavior

The following table describes how the client and server handle failure
scenarios:

| Situation                                       | What happens                                                                  |
| ----------------------------------------------- | ----------------------------------------------------------------------------- |
| Offline when `setPresence` is called            | Remembered and re-sent; the record is re-published when the stream reopens    |
| Offline when `send` is called                   | Rejects. A broadcast is not queued — one that arrives late is worse than none |
| Access revoked while the room is open           | `room_dropped` within one heartbeat; terminal                                 |
| A peer's tab is killed                          | Disappears within three announce intervals (~15 seconds by default)           |
| A peer publishes state your build cannot decode | That peer's `state` is `undefined`; the room carries on                       |

## Sizing limits

The default limits suit cursors and short status objects:

- `MaxPresenceStateBytes`: 4 KiB
- `MaxBroadcastBytes`: 4 KiB
- `MaxParticipantsPerRoom`: 128
- `MaxRoomsPerStream`: 64

For more information, see the [Limits table](../reference/server.md#limits).
