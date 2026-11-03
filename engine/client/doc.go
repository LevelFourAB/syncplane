// Package client is a Go client for a remote syncplane server, for code that
// keeps no local copy of the data: backend jobs, tools and tests.
//
// A Client wraps the generated Connect clients of SyncService, BlobService and
// PresenceService. Workspace handles add typed calls on top:
//
//   - Writes: Workspace.Put, Patch, Delete, Apply (several operations as one
//     atomic batch) and Invoke (a custom mutator). Each returns when the
//     server has applied or rejected the mutation. WithExpectedVersion makes
//     a write a compare-and-swap, and WithText sets a collaborative-text field
//     from a plain string.
//   - Reads: Get and Query (generic functions over the entity message type),
//     Workspace.Bootstrap and Pull for the change feed, and Client.Subscribe
//     for a live stream.
//   - Blobs: Workspace.UploadBlob and DownloadBlob.
//   - Presence: Workspace.Room.
//
// Entity names and row keys come from the (syncplane.v1.entity) options of
// the message, derived as the server derives them; EntityKey exposes the
// derivation.
//
// Every mutation carries the Client's client id and a mutation id from a
// counter the Client keeps per workspace in memory. A push that fails in
// transit is sent again under the same id, so the server applies it at most
// once. A rejected mutation reports a *RejectedError, and a push the server
// refuses for the whole workspace reports a *WorkspaceError; CodeOf reads the
// code of either. The raw services remain available through
// Client.SyncService, BlobService and PresenceService.
package client
