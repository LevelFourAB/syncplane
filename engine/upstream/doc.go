// Package upstream keeps a local syncplane engine converged with an origin
// syncplane server, so that a Go process can serve, read and write synced
// data while offline: a desktop app whose webview syncs against the local
// engine, a server that syncs with another, or a local engine that forwards
// an origin to browsers.
//
// A Link binds one local *server.Server to one origin. Per linked workspace
// it pulls the origin's changes into the local engine and pushes local writes
// to the origin. The origin orders every write; a local write is optimistic
// until the origin confirms it. Local rows always hold the rows the origin
// confirmed with the writes it has not confirmed yet replayed on top, with
// the semantics the origin applies them with: per-field last-writer-wins for
// PATCH, a union for collaborative text, and the origin's arrival order
// deciding between concurrent writes. A write the origin rejects is reverted
// locally and reported through Config.OnRejected, and, when a downstream
// client pushed it, to that client as a late rejection on its next pull.
//
// Every local write to a bound entity is captured, whichever path made it: a
// downstream client's push, server.Apply, Mutate and Invoke, mutators, effects
// and connectors. Config.Bindings decides which entities sync and in which
// direction; an entity no binding names stays local. The link's state (the
// confirmed rows, the pending writes, the origin cursor) lives in the local
// store, in the same transactions as the rows, so a restart resumes where the
// process stopped.
//
// Workspace is a typed handle for a Go app that reads and writes the local
// engine itself: Get, Query, Put, Patch, Delete, Invoke, Watch and Flush,
// with a Receipt per write to await the origin's verdict.
//
// # Shutdown
//
// Stop the parts in the reverse order of their dependencies: cancel the
// context of blobbridge's Run and wait for it to return, then Close the
// Link, then Close the local engine (server.Server.Close), then Close the
// presencebridge and blobbridge stores the engine was built with, then
// Close the storage adapter. Link.Close tolerates an engine or store that is
// already closed: its in-flight state writes fail, and it returns once its
// goroutines have stopped.
package upstream
