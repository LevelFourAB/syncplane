# Add presence to an app

Rooms carry the state that only matters while somebody is connected: cursors,
selections, "who is here", typing indicators. None of it is stored or replayed
— see [rooms](../explanation/rooms.md) for the model and the dividing test.

Nothing to configure on the server: the presence service is mounted by
`HTTPHandler()` and `Handlers()` alongside the sync service, and a single-node
deployment needs no bus.

## Who's here

```ts
const room = ws.room<{ name: string; color: string }>('lobby');

room.setPresence({ name: 'Ada', color: '#c0f' });

room.subscribe((peers) => {
	render(peers.map((p) => ({ ...p.state, self: p.self })));
});
```

`peers()` is the roster including your own record, ordered by session id. The
array is frozen and keeps its reference until something actually changes, so it
is safe to hand to `useSyncExternalStore`.

Close the room when the view goes away — that withdraws your record at once
rather than leaving it to expire:

```ts
await room.close();
```

In React the hook does both for you:

```tsx
function Presence() {
	const peers = usePresence<{ name: string; color: string }>('lobby');
	return <AvatarStrip people={ peers.filter((p) => !p.self) } />;
}
```

## Anchor a room to a row

Passing a schema and key makes membership inherit that row's read permission —
a caller who could not read the document cannot be in its cursor room, and you
write no policy for it:

```ts
const room = ws.room(DocumentSchema, docId, 'cursors');
```

If the row is hidden by your `ReadFilter`, deleted, or was never created, the
room is dropped with `room_dropped` and `Room.status()` reports `dropped`. That
is terminal — reopening meets the same answer.

## Live cursors in collaborative text

Publish [text anchors](../explanation/collaborative-text.md), not integer
offsets. An offset means something different the moment anyone types above it;
an anchor stays attached to the character it was placed at.

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

editor.onSelectionChange((index) => {
	room.setPresence(encodeAnchor(text.anchorAt(index)), { seq: lastSyncedSeq });
});
```

Reading a peer's cursor back needs a `try`/`catch`. Rooms are not synchronized
with the data stream, so a peer may be pointing at a character your copy has not
pulled yet — and an anchor can outlive a compaction that removed the item it
named:

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

`setPresence` is throttled (60ms by default, `throttleMs`), so calling it per
mouse-move is the intended usage: intermediate positions are dropped and the
last one always goes out.

## Ephemeral messages

`send` broadcasts to whoever is watching at that moment:

```ts
room.onMessage((m) => {
	if (!m.self) flashReaction(m.senderSubject, m.message);
});
await room.send({ kind: 'wave' });
```

**At most once.** If nobody is watching, or the connection is between
reconnects, the message is gone — `send` resolves when the server accepted it,
not when anyone received it. It rejects when the client could not reach the
server, so a failure you care about is visible rather than swallowed.

## Agent chat: durable messages plus a live overlay

The pattern for anything that must not be lost but should still feel live:
**write the row, broadcast the hint.**

```ts
// The transcript is data. It survives a reload and a new reader gets it.
await ws.mutate.put(MessageSchema, { id, threadId, author, body });

// The token stream is not. It exists to make the wait bearable.
const stream = ws.room<string>(ThreadSchema, threadId, 'tokens');
for await (const token of completion) await stream.send(token);
```

A reader who joins mid-generation sees no tokens and the finished message when
it lands, which is correct: they missed the typing, not the answer.

Use presence rather than broadcast for the fact that generation is _happening_ —
`{ status: 'thinking' }` is state, and a reader arriving mid-run should see it:

```ts
stream.setPresence({ status: 'thinking' });
// …
stream.setPresence({ status: 'idle' });
```

## Identity

`Peer.subject` is stamped by the server from the request principal, so it is
safe to render as an identity. Nothing in the payload names the publisher —
presence cannot be used to impersonate.

It is empty when the caller is unauthenticated. Install a
[`WithAuthenticator`](authentication-and-permissions.md) if you want names.

## Hiding participants from each other

Being able to read a row is not the same as being allowed to know who else is
looking at it. Implement `RoomPermissions` when they differ:

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

Both hooks run at join and `CanJoinRoom` again on the stream's heartbeat, so a
revocation takes effect within one heartbeat interval.

## What to expect when things go wrong

| Situation                                       | What happens                                                                  |
| ----------------------------------------------- | ----------------------------------------------------------------------------- |
| Offline when `setPresence` is called            | Remembered and re-sent; the record is re-published when the stream reopens    |
| Offline when `send` is called                   | Rejects. A broadcast is not queued — one that arrives late is worse than none |
| Access revoked while the room is open           | `room_dropped` within one heartbeat; terminal                                 |
| A peer's tab is killed                          | It disappears within three announce intervals (~15s by default)               |
| A peer publishes state your build cannot decode | That peer's `state` is `undefined`; the room carries on                       |

## Sizing

The defaults suit cursors and short status objects, not documents:
`MaxPresenceStateBytes` and `MaxBroadcastBytes` are 4 KiB,
`MaxParticipantsPerRoom` is 128, and `MaxRoomsPerStream` is 64. See the
[`Limits` table](../reference/server.md#limits).
