# Blobs: bytes beside the rows

A row can refer to an immutable blob of bytes, such as an image, a PDF or a video, with a field of type `syncplane.v1.BlobRef`. The row syncs through the mutation log as usual. The bytes do not: they move in chunks through a separate Connect service, `syncplane.v1.BlobService`, and a device fetches them only when it reads them.

This page explains why blobs work this way and what the design costs. To add attachments to an app, see [Attach files to rows](../how-to/attach-files-to-rows.md). For the API, see the [server reference](../reference/server.md#blobs) and the [client reference](../reference/typescript-client.md#blobs).

## Why bytes stay out of the mutation log

Every row write goes through the [mutation log](convergence-model.md). It takes a workspace sequence number, reaches every subscriber, takes part in rebase, stays in the local KV store of every device, and can stay in tombstones and field history. These costs are correct for a title or a checkbox. For a 50 MiB photo they are not:

- Every device that bootstraps the workspace downloads every photo, also the ones it never shows.
- Every `PUT` of the row sends the photo again.
- A row value is limited to `MaxValueBytes` (1 MiB by default), and a push carries many rows in one request.

A blob needs none of what the log gives. It never changes, so it needs no merge, no ordering and no rebase. It needs three other things: proof that the bytes are the right bytes, a transfer that resumes after a failure, and a read only when a reader wants it.

So the row holds a small `BlobRef`: the digest, the size and the media type. The row merges per field like every other row. To change the file, an app uploads a new blob and writes a new ref; the old blob stays until [garbage collection](#garbage-collection) deletes it.

## Content addressing, per workspace

A blob is named by its workspace and its digest. When two uploads of the same bytes go into one workspace, the server stores one blob: the second `BeginUpload` answers `BLOB_STATE_COMPLETE`, and the second device sends no bytes.

Deduplication stops at the workspace. The same bytes in two workspaces are two blobs. The reasons are the same reasons that make `workspace_id` the hard partition key everywhere in the engine:

- A store that deduplicates across workspaces tells a caller in one workspace that some other workspace holds the same bytes, because `BeginUpload` answers `COMPLETE` without the bytes.
- A digest would become a key into data of other tenants.
- Deleting a workspace removes its blobs. No count of references across workspaces must be kept correct.

The cost is storage: a file attached in two workspaces is stored twice. A `blob.Store` can keep one copy of the bytes for two workspaces if it wants to. The contract permits it, and no caller can see the difference.

A `Manager` that gives one store to every app it hosts keeps the apps apart in the same way. Each app names its workspaces in the store with its own prefix.

## The digest and fixed chunks

A digest has the form `sp1:` followed by 64 lowercase hex digits. The hex digits are the SHA-256 of:

1. the size of the blob, as an 8-byte unsigned big-endian integer, then
2. the SHA-256 of each chunk, in order.

Every chunk except the last is exactly 8 MiB (8388608 bytes). The last chunk holds the remaining bytes. An empty blob has no chunks, so its digest is the SHA-256 of eight zero bytes. The shared vectors in `testdata/blob/` make the Go and TypeScript implementations agree byte for byte.

Fixed chunks give these properties:

- **The same bytes always give the same digest.** Every client cuts a file at the same places, which deduplication needs.
- **Hashing uses memory for one chunk.** A client hashes a file of any size one chunk at a time.
- **Each chunk is checked alone.** The server checks each `PutChunk` against its hash before it stores it. The client checks each `GetChunk` before it keeps it. A bad chunk costs one chunk, not the whole transfer.
- **An upload resumes by chunk index.** `BeginUpload` for a part-way upload returns the indexes of the missing chunks. The server needs no session for this.

The chunk size is part of the digest definition. The `sp1` prefix names this definition, so a different chunk size needs a different prefix.

### Why the size is in the digest

The size is the first thing the digest hashes, so the digest proves the size before any bytes move:

- The server checks the size against `MaxBlobBytes` at `BeginUpload`. The chunk hashes must match a chunk count that follows from the size, and together they must produce the digest. A manifest that claims a small size for large bytes cannot pass this check.
- A downloader reads the size and the chunk hashes from `StatBlobs` and checks them against the digest and against the ref before it requests the first chunk. A server or a row that reports a wrong size fails at once with `blob_corrupt`, not after the last chunk.

The media type is not part of the digest. The same bytes with two media types are one blob. The type travels in the ref, and the engine does not read or check it.

## Rows never wait for uploads

`blobs.put(file)` hashes the file, stores the bytes and the chunk hashes in the upload queue of the device, and returns the ref. The app writes the row at once, also when the device is offline. The row and the upload then travel separately, and the row can reach other devices before the bytes reach the server.

The server accepts a row that names a digest it does not have. A reader then sees the blob as not available yet: `StatBlobs` reports `MISSING` or `UPLOADING`, and the client reports the status `pending`.

If a row write waited for the upload, a save would wait for the network, and an offline device could not save at all. The server would also have to refuse a row that names a missing blob, which refuses the offline write that the rest of the engine accepts.

The cost is that a ref is a promise. When the device that holds the bytes never comes back (its storage was cleared, or the app called `discard`), the row names a blob that no device has. Show a placeholder for a blob that is not available.

### How a client knows that an upload is done

The status of a blob belongs to one device. `pending` means that this device has no bytes to send, and that the server has not reported the blob complete.

The device that uploads knows at once. The `PutChunk` that stores the last chunk answers `COMPLETE`, and the status changes to `uploaded`. Other tabs of the account get the change over a `BroadcastChannel`.

Other devices learn it from `WatchBlobs`, a server stream. While a tab watches blobs that are `pending` (through `watch`, `uploaded` or `useBlob`), it holds one `WatchBlobs` stream per workspace for up to 256 of them. The first message reports every digest. The server checks the digests again each time the workspace is notified, which a commit and a completed upload both do, and sends only the digests whose state changed. It ends the stream when it has reported every digest complete. When the set of pending blobs changes, the tab closes the stream and opens a new one, after a short pause that collects changes made together. The tab shares each completion it hears with the other tabs.

The server sends an empty message at the `Subscribe` heartbeat interval, and runs the workspace gate again at the same time. When the caller loses access to the workspace, the stream ends with `permission_denied`, and the tab stops asking about blobs of that workspace. With a shared notifier, a completion on one node reaches streams on every node.

`Subscribe` does not carry this: it sends a frame only when rows change. The client falls back to asking the server with `StatBlobs` for the pending blobs that no working stream covers: after each pull and poke, and on a timer whose random delay has an upper bound that starts at 2 seconds and doubles to 60 seconds. This applies to a server without `WatchBlobs` (the client then stops opening streams), to a stream that waits out a failure before it opens again, and to blobs past the 256th in one workspace.

## Read authorization goes through rows

A blob has no access rules of its own. A caller can read a blob when at least one live row that the caller can read names the digest in a `BlobRef` field. The engine applies the field guards of the caller first, so a ref in a field that a guard hides from the caller does not count. A row of a [server-only entity](../reference/proto-options.md#server-only-entities) does not count either, because no client can read it. A `Trusted` caller can read every blob that a live row names, also in server-only rows.

A blob that the caller cannot read looks the same as a blob that the server does not have: `StatBlobs` reports `MISSING`, and `GetChunk` fails with `not_found`.

An upload needs admission to the workspace. A `Permissions` that implements `BlobPermissions` can also refuse an upload in `CanUploadBlob`, for example by the caller's role or by the size. The engine decides which rows may name a blob when a row is written, and who may read the blob when it is read.

This has consequences:

- **Access follows the row.** When a caller loses read access to every row that names a blob, or when those rows stop naming it, the next request for the blob fails. Bytes that a device downloaded earlier stay in its cache.
- **The client does not need the row.** The check runs on the server against the rows the caller can read. A device with [partial sync](partial-sync-and-coverage.md) can read a blob whose row it does not hold.
- **A digest is a read capability inside the workspace.** A caller that knows a digest, and that can write a row it can read, can name the digest in that row and then read the blob. A field guard hides a digest from the callers who cannot read the field, but a caller that saw the digest earlier can still use it.
- **Uploads tell a little.** `BeginUpload` answers `COMPLETE` for a stored blob, but only to a caller that sends every chunk hash, which only a holder of the bytes or a reader of the blob has. `PutChunk` for a stored blob with nothing staged answers `COMPLETE` only when the chunk matches the blob, and otherwise fails as for a digest the server does not have. While an upload of a digest is staged, a wrong chunk for it fails with `invalid_argument`, not `failed_precondition`. So a member of the workspace who knows a digest can learn that someone is uploading it, but not that the server stores it from the answer alone. The response time can still tell: `StatBlobs` and `WatchBlobs` run the read check only for a digest the store holds, so a stored digest takes longer to answer than an absent one.
- **Each read runs the check.** `StatBlobs` runs it for the digests the store holds, `GetChunk` runs it for every chunk, and a `WatchBlobs` stream runs it for its stored, incomplete digests each time the workspace is notified. A download of a blob with N chunks runs it N + 1 times.

The engine derives a sparse index for each `BlobRef` path, named `__blob_<path>`, so the check is one index probe per entity. The check reads every row of the entity instead when no index can serve it:

- while the index builds, after a schema adds a `BlobRef` field;
- when `Limits.DisableIndexes` is set;
- when a `BlobRef` sits in a list inside a list element, or inside a message that contains itself;
- when the probe finds more than `MaxIndexScanCandidates` rows.

The index entries count toward `MaxIndexEntriesPerRow`. A repeated `BlobRef` field adds one entry per element.

## Garbage collection

The server keeps a committed blob while a live row names it. This check reads no access rules: a row that no caller can read keeps the blob for every caller, and so does a row of a server-only entity. A deleted row does not keep a blob, and neither does a field-history record.

When a garbage collection pass finds no live row that names a blob, it marks the blob with the time (`UnreferencedSince`). A later pass deletes the blob when the mark is older than the blob retention, which is 30 days by default (`WithBlobRetention`). These events remove the mark and restart the clock:

- A pass finds a live row that names the blob again.
- A `BeginUpload` names the blob. An uploader that deduplicates against an old blob does not lose it to the next pass.

Before it deletes, the pass checks the references again in a read transaction, which can read every row of an entity and so holds no write lock. It then deletes in a write transaction, and only when the workspace sequence has not moved since that read and the mark is still old. A row that names the blob and commits before the delete therefore keeps the blob, and a `BeginUpload` cannot report `COMPLETE` for a blob that the pass then deletes. When the workspace sequence moved, the blob waits for the next pass. In a workspace with steady writes, a delete can wait for a pass that finds a quiet moment.

The pass deletes nothing in a workspace that holds a live row of an entity the schema no longer declares, because such a row can name any blob. It still sets and removes marks there, and it logs one warning per pass. To find such rows, the pass reads every row of the workspace, tombstones included, before each batch of deletes.

The same pass aborts a staged upload when its first `BeginUpload` is older than the retention. The retention therefore also limits how long one upload can take. After an abort, the client stages the upload again and sends every chunk again.

Blob collection runs only with `WithGC`, inside its pass, on its interval. Each workspace gets its own lease (`blobs`), so in a multi-node deployment one node at a time collects the blobs of a workspace. `CollectGarbage` runs it for one workspace on demand. Deleting a workspace deletes its blobs. If that delete fails, the next pass deletes them.

The mark gives time between the upload and the row write in either order. It also needs no reference count: a count would have to change with every write to every `BlobRef` field, and the mark lives in the blob store itself.

The trade-offs:

- **The window is not exact.** A pass sets the mark when it first sees the blob unreferenced, not when the last reference went away. An unreferenced blob lives for the retention plus up to two GC intervals.
- **A restore can come too late.** [Recovering a deleted row](../how-to/recover-a-deleted-row.md) or [restoring a field](../how-to/restore-a-field.md) after the retention can bring back a ref to a blob that GC deleted. The blob then stays unavailable until a device uploads the same bytes again. The same bytes give the same digest, so the old ref works again.
- **Protection reads the current schema.** A removed entity keeps its blobs, because the pass stops deleting in a workspace that still holds its rows. A removed `BlobRef` field of an entity that stays does not: the next pass sees its blobs as unreferenced, and they are deleted after the retention. Schema evolution is additive, so removing such a field is outside the contract.

## More than one node

The blob store holds state, the same as the storage adapter. Every node must use the same store, or a blob that one node stored is missing on the others.

`engine/blob/fsstore` keeps blobs in a local directory. Several stores, in one process or in several processes, can share one directory. The directory must be on a local file system that renames atomically, supports hard links and keeps an open file readable after it is removed, such as ext4, XFS, APFS or ZFS. NFS and SMB do not give these guarantees. A deployment that uses `fsstore` therefore runs on one host.

`fsstore` writes each chunk of an upload at its place in one data file. The commit adds a short trailer and renames the file, so it does not copy or hash the bytes again: its cost does not depend on the size of the blob, and an upload needs disk space for one copy.

A deployment across hosts needs a `blob.Store` over shared storage, such as an object store. The engine does not include one. The `blob.Store` contract (stage an upload, store its chunks, commit it atomically, abort it) has the shape of the multipart upload that object stores offer.

The completion poke goes through the notifier, so with a shared notifier such as `pgnotify` it reaches clients on every node.

## The transport

`BlobService` is a Connect service. It uses the transport of the client, the same interceptors (and so the same authentication) and the same workspace gate as the sync service, and it goes through a [tunnel](tunnels.md) the same way.

A tab that waits for blobs of another device holds a `WatchBlobs` stream per workspace. Over HTTP/1.1 this counts toward the [browser connection limit](limits-and-trade-offs.md#browser-connection-limits).

A chunk is a request of up to 8 MiB. On a tunnel channel that many calls share, the protocol has no flow control, so a chunk in flight delays the calls behind it. The uploader of an account sends one chunk at a time, which limits this delay to one chunk.

## On the device

- **The queue survives reloads.** In a browser, the upload queue and the download cache are in an IndexedDB database of their own, beside the KV store (`<prefix>:blobs:<account>`). When the client uses a memory KV store, or runs where there is no IndexedDB, the queue is in memory and a reload loses it.
- **One tab uploads.** The tab that holds a Web Lock sends the queue of the account. A hidden tab keeps uploading, because an upload in a hidden tab is still progress. A frozen page releases the lock to the tabs that still run. Where Web Locks are not available, each client sends its own queue.
- **Status reaches every tab.** Progress, completion and removal go to the other tabs over a `BroadcastChannel`.
- **The cache has a budget.** Downloads go into a cache that drops the least recently read blobs first when it is over `blobCacheBytes` (256 MiB by default). Queued uploads do not count against the budget, and the cache never deletes them. When an upload completes, its bytes move into the cache and can then be dropped.
- **`get()` returns a `Blob` made of the stored chunks.** It reads the queue, then the cache, then the server. A download goes into the cache chunk by chunk as it arrives, whatever its size, and the returned `Blob` is made of the stored chunks. In a browser it is backed by IndexedDB on disk, so a video of 2 GiB does not need 2 GiB of memory. `stream()` gives the same bytes as a `ReadableStream`, at most one chunk ahead of the reader.
- **Recent reads stay.** Eviction keeps a blob that a tab read in the last 10 minutes. In the tab that holds it, eviction also keeps a blob whose `get()` result is still reachable, and a download in progress. The cache can therefore stay above its budget by what those hold, and a blob larger than the budget is cached and leaves in a later eviction.
- **`get()` checks what it reads.** A browser can return other bytes from IndexedDB than it stored: WebKit can link a stored `Blob` to the file of a different `Blob`, and the record keeps the correct size. The hash check of a download cannot see this, because it runs before the chunk is stored. So the first `get()` of a stored blob in a tab reads every byte once and checks it against the digest. A cached blob that fails the check is dropped and downloaded again. A queued blob that fails cannot be downloaded, because its bytes are only on this device, so `get()` rejects with `blob_corrupt`. `stream()` does not make this check.
- **WebKit can lose an old `Blob`.** Chromium and Firefox keep the bytes behind a `Blob` from IndexedDB while the `Blob` exists. WebKit gives no such promise. A `Blob` that tab A holds for more than 10 minutes without a new read can lose its bytes when tab B evicts them. Call `get()` again before a late read.

## Related

- [Attach files to rows](../how-to/attach-files-to-rows.md)
- [Server reference: blobs](../reference/server.md#blobs)
- [TypeScript client reference: blobs](../reference/typescript-client.md#blobs)
- [Limits and trade-offs](limits-and-trade-offs.md#blob-bytes-travel-beside-the-log)
