# Attach files to rows

This guide shows you how to attach images, PDFs and other files to rows. The row holds a `syncplane.v1.BlobRef`, and the bytes upload beside it. The upload is resumable, survives a reload, and does not delay the row write. For the model behind this, see [Blobs](../explanation/blobs.md).

## Prerequisites

- A server built with `server.New` and an entity defined in a `.proto` file (see [Define entities in your own module](define-entities.md)). The [TypeScript schema builder](define-schemas-in-typescript.md) has no `BlobRef` field.
- A `SyncClient`, or a React app that uses `syncplane/react`.

## 1. Give the server a blob store

Open an `fsstore` over a local directory and pass it with `WithBlobStore`. Blob garbage collection runs inside the GC pass, so also set `WithGC`:

```go
import (
	"github.com/levelfourab/syncplane/engine/blob/fsstore"
	"github.com/levelfourab/syncplane/engine/server"
)

blobs, err := fsstore.Open("/var/lib/myapp/blobs")
if err != nil {
	log.Fatal(err)
}
defer blobs.Close() // the server does not close the blob store

srv, err := server.New(store,
	server.WithEntities(&appv1.Attachment{}),
	server.WithBlobStore(blobs),
	server.WithGC(30*24*time.Hour, time.Hour),
	server.WithBlobRetention(7*24*time.Hour),             // optional; default 30 days
	server.WithLimits(server.Limits{MaxBlobBytes: 1 << 30}), // optional; default 4 GiB
)
if err != nil {
	log.Fatal(err)
}
http.ListenAndServe(addr, srv.HTTPHandler())
```

`HTTPHandler()` and `Handlers()` mount `syncplane.v1.BlobService` beside the sync service. If you mount services by path, or forward them through a proxy, add `/syncplane.v1.BlobService/`.

Put the directory on a local file system. To run more than one host, see [More than one node](../explanation/blobs.md#more-than-one-node).

Every caller admitted to the workspace can upload. To limit uploads, for example to editors or by size, implement `server.BlobPermissions` on your `Permissions`:

```go
func (p appPermissions) CanUploadBlob(ctx context.Context, pr server.Principal, workspaceID string, size int64) error {
	if size > 20<<20 && !p.isEditor(ctx, pr, workspaceID) {
		return server.Reject("upload_too_large", "only editors upload files above 20 MiB")
	}
	return nil
}
```

`BeginUpload` calls it after the workspace gate, also for a file the server already stores. A refusal reaches the client as a `failed` upload with `transportCode` `PermissionDenied`.

If you run `syncplaned`, pass `-blob-dir /var/lib/syncplane/blobs` instead. `syncplaned` has no GC flags, so it never deletes the blobs it stores.

## 2. Add a `BlobRef` field to the entity

Import `syncplane/v1/blob.proto` and give the entity a field of type `syncplane.v1.BlobRef`:

```proto
syntax = "proto3";

package myapp.v1;

import "syncplane/v1/blob.proto";
import "syncplane/v1/options.proto";

message Attachment {
	option (syncplane.v1.entity) = { name: "attachment" key_fields: [ "id" ] };

	string id = 1;
	string name = 2;
	syncplane.v1.BlobRef file = 3;
}
```

`blob.proto` ships in the same Buf module as `options.proto`. Configure code generation for it the same way: an import, not a target. With `protoc`, also pass `Msyncplane/v1/blob.proto=github.com/levelfourab/syncplane/engine/gen/syncplane/v1`.

A `BlobRef` can also sit in a nested message, a repeated field, a keyed list or a map value. See [Blob references](../reference/proto-options.md#blob-references).

## 3. Upload the file and write the row

Call `blobs.put` with the file, then write the row with the ref it returns:

```ts
async function attach(file: File): Promise<string> {
	const ref = await client.blobs.put(file); // hashes and queues; works offline
	return client.db.attachment.mutate.create({ name: file.name, file: ref });
}
```

`put` does not wait for the upload. The client sends the queue in the background while it is started, also after a reload. The ref carries `file.type` as its media type. To set another one, pass `{ mediaType: 'image/png' }`.

`put` rejects when the device cannot store the bytes, for example with the browser's `QuotaExceededError`.

To use a workspace other than the default one, use `client.ws(id).blobs`.

## 4. Show the file

In React, use `useBlob`. It reads the bytes as soon as this device can read them, and returns an object URL:

```tsx
import { useBlob } from 'syncplane/react';

function AttachmentPreview({ attachment }: { attachment: Attachment }) {
	const { status, url } = useBlob(attachment.file);
	if (status?.state === 'failed') {
		return <p>The server refused the upload: { status.error.message }</p>;
	}
	if (url === undefined) {
		return <p>Waiting for { attachment.name }</p>;
	}
	return (
		<figure>
			<img src={ url } alt={ attachment.name } />
			{ status?.state === 'uploading' && (
				<progress
					value={ status.progress.sentBytes }
					max={ status.progress.totalBytes }
				/>
			) }
		</figure>
	);
}
```

On the device that uploads, `url` is set at once from the local queue. On other devices, it is set when the upload is complete. `useBlob` revokes the URL when the component unmounts or the ref changes. Pass `undefined` for a row without a file.

Without React, call `get`:

```ts
import { BlobError, CodeBlobUnavailable } from 'syncplane';

try {
	const blob = await client.blobs.get(row.file!);
	image.src = URL.createObjectURL(blob);
} catch (err) {
	if (err instanceof BlobError && err.code === CodeBlobUnavailable) {
		showPlaceholder(); // not uploaded yet, or not readable by this caller
	} else {
		throw err;
	}
}
```

`get` costs one request for each 8 MiB chunk the first time, and reads the device's cache after that. In a browser the returned `Blob` is backed by IndexedDB, not by memory. To process the bytes as they arrive, read `client.blobs.stream(ref)` instead:

```ts
const reader = client.blobs.stream(row.file!).getReader();
for (let next = await reader.read(); !next.done; next = await reader.read()) {
	await sink.write(next.value); // one chunk of up to 8 MiB
}
```

## 5. Show upload progress and wait for the upload

`watch` calls back with each status change. `uploaded` resolves when the server holds every chunk:

```ts
const ref = await client.blobs.put(file);
const stop = client.blobs.watch(ref, (status) => {
	if (status.state === 'uploading') {
		bar.value = status.progress.sentBytes / status.progress.totalBytes;
	}
});
try {
	await client.blobs.uploaded(ref);
	toast('Uploaded');
} finally {
	stop();
}
```

`uploaded` rejects with a `BlobError` when the server refuses the upload. It waits while the device is offline, and it does not settle while the client is not started.

## 6. Warn before the user closes the page

The queue survives a reload, so closing the page does not lose an upload. Other devices do not get the file until the app runs again on this device. To warn the user, track `idle()`:

```ts
let uploading = false;

function trackUploads(): void {
	if (uploading) return;
	uploading = true;
	client.blobs.idle()
		.catch(() => {}) // the client closed
		.finally(() => {
			uploading = false;
		});
}

window.addEventListener('beforeunload', (event) => {
	if (uploading) event.preventDefault();
});
```

Call `trackUploads()` after each `put`, and once at start. `client.blobs.pendingCount()` returns the number of uploads, across workspaces and tabs, that are not uploaded or failed.

## 7. Handle a refused upload

A status of `failed` means the server refused the upload, and sending the same bytes again gets the same answer. `status.error.transportCode` names the reason:

| `transportCode`     | Cause                                                                                    |
| ------------------- | ---------------------------------------------------------------------------------------- |
| `ResourceExhausted` | The file is larger than the server's `MaxBlobBytes`.                                     |
| `PermissionDenied`  | `CanUploadBlob` refused the upload, or the caller's access to the workspace was revoked. |
| `Unimplemented`     | The server has no blob store.                                                            |
| `InvalidArgument`   | The manifest does not match the digest.                                                  |

A caller that holds no grant in the workspace yet does not get `failed`. The upload stays `queued`, and the client tries it again when the caller's access changes, and with backoff from 5 seconds to 5 minutes.

The client does not retry a failed upload. The bytes stay in the queue, and `get` still reads them. To try again after the cause is fixed, call `put` with the same file: it sets the upload back to `queued`. To give up, call `discard(ref)`, then write the row without the ref.

## Confirming the result

1. Attach a file in one browser. The status changes from `queued` to `uploading` to `uploaded`.
2. Open the app in a second browser as a user who can read the row. The file shows when the upload completes.
3. Open it as a user who cannot read the row. `get` rejects with `blob_unavailable`.

## Related

- [Blobs](../explanation/blobs.md)
- [TypeScript client reference: blobs](../reference/typescript-client.md#blobs)
- [Server reference: blobs](../reference/server.md#blobs)
- [Handle failures](handle-failures.md)
