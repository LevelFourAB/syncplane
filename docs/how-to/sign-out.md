# Wipe local data on sign-out

A client keeps the workspace on the device: rows, the optimistic overlay, the
pending queue, cursors and the `clientId`, all in an IndexedDB database named
after the account. `close()` leaves every byte of it there — which is what lets
the next launch read the workspace instead of bootstrapping it, and what leaves
the previous user's data readable on a shared browser.

`purgeLocalData()` is the sign-out wipe.

```ts
async function signOut(client: SyncClient) {
	// Push what is still queued while the credential is still good; anything
	// left over is discarded by the wipe, not sent.
	await client.syncNow().catch(() => {});

	const unsent = await client.purgeLocalData();
	if (unsent.length > 0) {
		console.warn(`${unsent.length} unsent change(s) were discarded`);
	}

	dropCredential(); // your auth layer
}
```

It stops sync, deletes everything the store holds, wakes the watchers so a
mounted UI empties, and closes the client. The client cannot be reopened — build
a new `SyncClient` for whoever signs in next.

The delete is one transaction, so a rejection leaves the store as it was and the
client open but no longer syncing. Retry the call; while it keeps failing the
device still holds the account's data, which is the thing a shared-browser
sign-out has to account for.

## What it does and does not remove

| What                                                          | Happens to it                                             |
| ------------------------------------------------------------- | --------------------------------------------------------- |
| Rows, overlay, bootstrap staging, cursors, queues, `clientId` | deleted                                                   |
| The IndexedDB database itself                                 | kept, empty                                               |
| Your own tokens, caches and app state                         | yours to clear                                            |
| Anything on the server                                        | untouched: the wipe is local and tells the server nothing |

The `clientId` goes with the rest, so the next client on the account mints a new
one and starts with an empty queue and no watermarks of its own. The server's
idempotency watermarks are keyed by that id, so the old ones are never consulted
again.

The wipe reaches the database the client is on. An app that has moved between
`storagePrefix` values deletes what it left behind itself:
`indexedDB.deleteDatabase(storageNames('old-prefix', accountId).store)`.

## Unsent work

Mutations still queued can never be acked once their queue is gone, so they come
back from the call — read inside the transaction that deleted them, so the list
is exactly what was lost. Each also reaches `onMutationRejected` with code
`workspace_purged`, which is the channel to use if the app already has a
dead-letter path. Push first (as above) if the work matters; the wipe itself
never sends anything.

## Sign every tab out

Tabs share one store. A tab whose credential still works will see the rows
disappear, treat the empty store as a fresh install, and bootstrap them straight
back. Tell the other tabs before wiping:

```ts
const auth = new BroadcastChannel('app:auth');
auth.onmessage = (e) => {
	if (e.data === 'signed-out') void client.close(); // then re-render as signed out
};

// in signOut(), before purgeLocalData():
auth.postMessage('signed-out');
```

`close()` in the peers is enough — one wipe empties the store they all share.

## With React

Let the app own the client rather than the provider, so the wipe outlives the
tree:

```tsx
function App({ accountId, signedIn }: { accountId: string; signedIn: boolean }) {
	const client = useMemo(() => new SyncClient({ ... }), [ accountId ]);
	return signedIn
		? <SyncProvider client={ client }><Todos /></SyncProvider>
		: <SignedOut />;
}
```

Render the signed-out view first so the provider unmounts, then
`await client.purgeLocalData()`. A provider given `options` instead owns its
client and closes it on unmount, which leaves the data behind.

## Switching accounts

Give each account its own `accountId`: it then gets its own store and its own
`SyncClient`. Signing in as someone else therefore needs no wipe on a personal
device — the first account's data waits under its own database. Purge when the
device is shared, or when a user asks to be forgotten.
