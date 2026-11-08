// Package blobbridge moves blob bytes between a local engine and the origin
// server it syncs with.
//
// Rows that hold a syncplane.v1.BlobRef sync through the mutation log, but the
// bytes they name do not. A Bridge closes that gap in both directions:
//
//   - Uploads: a blob committed in the local engine and named by a row of a
//     synced entity is uploaded to the origin, in the same workspace. The
//     row can leave the local engine before the upload, as a row outside the
//     link's scope does once the origin confirms it.
//   - Read-through: a read of a blob that the local engine does not hold, but
//     that a live local row names, fetches it from the origin and commits it
//     in the local engine.
//   - Prefetch (optional): blobs that newly written rows name are fetched
//     before a reader asks for them.
//
// A Bridge is a blob.Store that wraps the local engine's store. Pass it to
// server.WithBlobStore, then call Run with the server:
//
//	br := blobbridge.New(fsStore, originClient.BlobService())
//	srv, err := server.New(store, server.WithBlobStore(br))
//	...
//	go br.Run(ctx, srv)
//
// The local workspace id is the origin workspace id. The bridge authenticates
// to the origin with the BlobServiceClient it is given, so the origin applies
// its own upload policy and read rule to that identity.
package blobbridge
