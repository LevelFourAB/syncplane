# Wiping local data on sign-out

When a user signs out on a shared device or asks to be forgotten, you must wipe their local workspace data from IndexedDB. Calling `close()` leaves rows, the optimistic overlay, the pending queue, cursors, and the `clientId` on the device. Calling `purgeLocalData()` removes this data, empties the store, and closes the client.

## Prerequisites

Before wiping local data, ensure that you have:

- An active `SyncClient` instance.
- An authentication layer to clear stored credentials.

## Steps

1. Notify other open tabs:

   If your application runs across multiple tabs, notify other tabs to close their clients before wiping the store. Tabs share one store. If another tab with a valid credential detects an empty store, it treats the store as a fresh install and bootstraps the data again.

   ```ts
   const auth = new BroadcastChannel('app:auth');
   auth.onmessage = (e) => {
   	if (e.data === 'signed-out') void client.close(); // then re-render as signed out
   };

   // in signOut(), before purgeLocalData():
   auth.postMessage('signed-out');
   ```

   Calling `close()` in peer tabs is sufficient because one wipe empties the shared store.

2. Push pending mutations to the server:

   If you have queued changes that you want to preserve, push them before dropping credentials. The wipe does not send unsent changes to the server.

   ```ts
   await client.syncNow().catch(() => {});
   ```

3. Call `purgeLocalData()`:

   Call `purgeLocalData()` to wipe the store and close the client. The client cannot be reopened after the wipe; create a new `SyncClient` for subsequent sign-ins.

   ```ts
   const unsent = await client.purgeLocalData();
   if (unsent.length > 0) {
   	console.warn(`${unsent.length} unsent change(s) were discarded`);
   }
   ```

   The deletion runs in a single transaction. If the transaction rejects, the store remains unchanged and the client stays open but stops syncing. If the call fails, retry the call until it succeeds.

   The method returns any mutations that were still queued when the store was deleted. Each discarded mutation also triggers `onMutationRejected` with the code `workspace_purged`.

4. Clear credentials and application state:

   Drop your authentication credentials and clear any remaining application state, tokens, or caches:

   ```ts
   dropCredential(); // your auth layer
   ```

5. Delete older storage prefix databases (if applicable):

   If your application previously used different `storagePrefix` values, delete leftover databases manually:

   ```ts
   indexedDB.deleteDatabase(storageNames('old-prefix', accountId).store);
   ```

## Complete sign-out example

The following example combines the sign-out steps into a single function:

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

## Handling React lifecycles

If you use React, let the application own the client rather than the provider so that the wipe outlives the component tree:

```tsx
function App({ accountId, signedIn }: { accountId: string; signedIn: boolean }) {
	const client = useMemo(() => new SyncClient({ ... }), [ accountId ]);
	return signedIn
		? <SyncProvider client={ client }><Todos /></SyncProvider>
		: <SignedOut />;
}
```

Render the signed-out view first so that the provider unmounts, then call `await client.purgeLocalData()`. A provider initialized with `options` owns its client and closes it on unmount without wiping local data.

## Switching accounts without wiping

If you switch accounts on a personal device, assign each account its own `accountId`. Each account receives its own IndexedDB store and `SyncClient`. The first account's data remains isolated in its own database without needing a wipe. Only purge data when the device is shared or when a user requests data deletion.

## Confirming the result

To verify that the local data was wiped:

1. Check that the UI empties when the watchers wake.
2. Inspect the IndexedDB database in browser developer tools to verify that rows, overlay, bootstrap staging, cursors, queues, and `clientId` are deleted.
3. Verify that the `SyncClient` instance is closed and cannot be reused.

## Reference: data removed during purge

The following table lists what `purgeLocalData()` removes:

| Item                                                          | Result                                                      |
| ------------------------------------------------------------- | ----------------------------------------------------------- |
| Rows, overlay, bootstrap staging, cursors, queues, `clientId` | Deleted                                                     |
| The IndexedDB database itself                                 | Kept, empty                                                 |
| Tokens, caches, and application state                         | Kept; clear these in your application                       |
| Data on the server                                            | Untouched; the wipe is local and does not notify the server |

The `clientId` is deleted with the rest of the store. When the account next creates a client, it mints a new `clientId` with an empty queue and no watermarks. The server keys idempotency watermarks by `clientId`, so it does not consult old watermarks again.
