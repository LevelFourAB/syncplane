# Chained sync: a local engine linked to an origin

A Go process can run its own sync engine and keep it converged with a remote one. Package `engine/upstream` does this. A `Link` connects one local `*server.Server` to one **origin** server. For each linked workspace, the link pulls the origin's changes into the local engine and pushes local writes to the origin.

This setup has these uses:

- **Desktop apps.** The Go side of a Wails or similar app runs the local engine and the link. The webview runs the TypeScript client against the local engine through a tunnel. The app works offline, and the data survives a restart.
- **Forwarded sync.** A local server sits between the origin and a group of browsers, for example on a site network. The browsers sync with the local server, and the local server syncs with the origin.
- **Server-to-server sync.** One server keeps a copy of some workspaces of another.
- **Local actions.** Effects and connectors run on the local engine and see the origin's data, for example a mirror of rows to Markdown files on the user's disk.
- **Go apps without a TypeScript client.** Go code reads and writes the local engine directly through `upstream.Workspace`.

This page explains the model and its costs. For the steps, see [Embed sync in a Go desktop app](../how-to/embed-sync-in-a-go-desktop-app.md) and [Forward sync to browsers](../how-to/forward-sync-to-browsers.md). For every type and option, see the [upstream reference](../reference/upstream.md).

## The origin is the only authority

The [convergence model](convergence-model.md) depends on one fact: per workspace, one server puts every write in one order. Per-field last-writer-wins, rejections and compare-and-swap all follow from that order.

A local engine also gives its writes an order. It must, because its own clients sync against it with the usual protocol. If the local order were final too, the same workspace would have two orders, and two replicas could keep different values for the same field with no rule to choose. So the local order is provisional. The origin's order is the only one that decides. A local write is optimistic until the origin confirms it, exactly as a write in a browser tab is optimistic until the server confirms it.

The result is that the link is a client of the origin with the same guarantees as the TypeScript client: per-field last-writer-wins by arrival at the origin, idempotent pushes under one client id and its mutation-id watermark, and a fresh snapshot when the origin's tombstone horizon passes the link's cursor.

## Confirmed rows with pending writes on top

For each row of a bound entity, the link keeps two things in the local store:

- The **confirmed row**: the row as the origin last sent it, with its origin version.
- The **pending writes**: the local writes that touched the row and that the origin has not confirmed yet, in local order.

The local row is always the confirmed row with the pending writes replayed on top. The replay uses the semantics the origin applies the writes with: a PATCH merges only the fields of its mask, a PUT replaces the row, a collaborative-text field takes the union of documents, and a `WithMerger` hook merges its field. When a page of origin changes arrives, the link updates the confirmed rows it touches and replays the pending writes again. When the origin confirms a write, the link drops it from the pending writes.

This is the same rebase the TypeScript client does with its overlay. The difference is where the result lives: the link writes it into real rows of the local engine. Downstream clients, effects, connectors, queries and the change feed of the local engine all see these rows as ordinary rows.

The link writes the rows through `server.Replicate`. Such a write keeps every derived structure current (indexes, search, the text delta log, field history, the access model and the change feed), but runs no permission hook and no compare-and-swap. The change feed attributes the rows to `Actor{System: "upstream"}`.

The link keeps its own state (confirmed rows, pending writes, cursor) in the local store, in the same transactions as the rows. A local write and the pending entry that records it commit together. A crash cannot leave a written row that the link does not know about, or a pending entry for a row that did not commit. After a restart, `Start` continues where the process stopped.

### Local versions are not origin versions

The local engine gives each write its own sequence. A row's version in the local engine is a local sequence. It is not the version the same row has at the origin. Downstream clients see only local versions, and a compare-and-swap uses local versions. The link translates between the two (see [Compare-and-swap across the hop](#compare-and-swap-across-the-hop)).

## A rejection becomes a revert

The default write mode is **write-behind**. A local write commits at once in the local engine, and the link pushes it to the origin later. When the origin rejects it, the write has already committed locally, and downstream clients may have seen it.

The link then drops the write from the pending writes and replays the rest. Each row the write touched goes back to "confirmed plus the remaining pending writes". The link writes that result as a new local change. A downstream client sees this change in its next pull, as any other change from the server.

The client got `APPLIED` for its push from the local engine, so the local engine reports the verdict separately. In the transaction that reverts the rows, the link records a late rejection for the client and the mutation id that pushed the write (`ReplicaTx.RejectLate`). The page that carries the revert, or a later page, carries it as a `LateRejection`. The TypeScript client then calls `onMutationRejected` with the origin's code and `late: true`.

Some limits apply to late rejections:

- They reach only the client id and the caller that pushed the write. A write that no client pushed, such as one from `Apply` or an effect, is reported only through `OnRejected`.
- The local engine keeps them with the client's other rejection records, and expires them with those records. A client that pulls after that, or bootstraps again, sees only the revert. The size bound on those records does not drop a late rejection.
- The local engine cuts the message of a late rejection to 1 KiB.
- A client that pulls again from an earlier cursor receives them again.
- The TypeScript client fires the callback in the tab that applies the page, which is not always the tab that made the write.
- A late rejection that the local engine itself receives from its origin, in a chain of more than two servers, is not passed on.

The Go app learns about the rejection through `Config.OnRejected`. The `Rejection` names the workspace, the origin's code and message, the mutator for a custom mutation, and the rows the link reverted. Use it to tell the user, to log, or to make a corrected write.

Delivery of `OnRejected` is at least once. The link removes a rejection from its store only after the callback returns. If the process crashes while a callback runs, the link reports the same rejection again after the next `Start`, with the same `MutationID`. `Close` waits for a running callback and then removes its rejection, so an orderly stop does not repeat it. Deduplicate on `(Workspace, MutationID)` if a repeat matters.

A write that the origin keeps failing in the same way (`mutation_failed`, `invalid_argument`, or an unknown code) would block every write behind it. After `Config.PoisonThreshold` failures in a row, the link drops it and reports it as rejected with that code.

## Write-through trades availability for a final answer

In **write-through** mode, a local write goes to the origin before it commits locally. The local write fails with the origin's rejection, or with an error that wraps `upstream.ErrOriginUnavailable` when the origin does not answer. Nothing commits locally in either case. A downstream client that pushed the write gets `REJECTED` with the origin's code, as if it had pushed to the origin. A Go caller gets the error from the write call.

The cost is in three parts:

- **No offline writes.** A write fails when the origin is not reachable.
- **Latency.** Each write waits for a round trip to the origin.
- **Blocked writers.** The call to the origin runs while the local store's write lock is held. Every other write to the workspace waits, the link's own writes too, so pulls and the live stream stall for the round trip. On a store with one writer, such as `sqlitestore`, every write to every workspace waits.

Use write-through for single writes whose result the user must know before going on, for example a payment or a name that must be unique. Pass `upstream.WriteThrough(ctx)` for that call, and keep the default write-behind for the rest. `Config.WriteMode` sets the mode for writes whose context names none, which includes every push from a downstream client.

When the origin answers neither yes nor no (a timeout, a lost connection), the write fails locally, but the origin can have applied it. The row then arrives back through a pull.

## Entity bindings and local-only entities

`Config.Bindings` names the entities that sync with the origin. Each binding has a direction:

- **`Both`** (the default): the link pulls the entity and pushes local writes to it.
- **`PullOnly`**: the link pulls the entity. A local write to it fails with `permission_denied`. A custom mutation is the exception, because the origin runs it (see below).

An entity that no binding names is **local-only**. The link never pulls it and never pushes a write to it. It lives only in the local engine: per-device settings, drafts, a cache of computed values, the state of a local connector. A mutation that writes bound and local-only entities together commits both locally, and the link forwards only the bound part.

The local engine must register every bound entity. `Start` fails when one is missing. A local engine with an older schema keeps the fields that only the origin's newer schema knows: they stay in the stored row, and a local PATCH leaves them as they are. A local PUT keeps them too, because every server keeps the stored top-level fields that its schema does not declare (see [Schema evolution is additive](convergence-model.md#schema-evolution-is-additive)). The link forwards the PUT with those fields, so the origin keeps them as well.

The two schemas can differ in the other direction too: a local engine with a newer schema can write a field the origin does not know. The origin keeps its stored value of that field on a PUT, and refuses a PATCH whose mask names it. `Config.OnSchemaSkew` decides what the link does while the two schema revisions differ. It can report the skew, ignore it, or stop forwarding local writes until the revisions agree (see [Schema skew](../reference/upstream.md#schema-skew)). Downstream clients compare their revision with the revision of the local engine, because the local engine is the server they write to.

The link holds the workspace for the origin. When the first snapshot of a workspace arrives, the link deletes every local row of a bound entity that the origin does not hold and that no pending write touches. Such rows exist only when something wrote to the local engine without the link installed. To push them to the origin instead, link the workspace with `AddWorkspaceWith` and `AddOptions.AdoptLocalRows`.

## Custom mutations: mirror or forward

A custom mutation (`Invoke`, or a client's custom push) can go two ways.

**The local engine declares the mutator.** The local mutator runs and writes its rows locally. These rows are a **prediction**. The link pushes the mutation to the origin by name and arguments, not the rows it wrote. The origin runs its own mutator, and its result replaces the prediction when it arrives through a pull. This is the Go counterpart of a TypeScript mutation mirror. A local mutator can write a `PullOnly` entity, because the origin decides the real result. A local mutator that writes only local-only entities stays local, and the link forwards nothing.

**The local engine does not declare the mutator.** `link.ServerOptions()` includes `server.WithUnknownMutatorForwarding()`. With it, the local engine accepts a mutation of any name as a mutator that writes nothing, and the link forwards it. Its arguments are not decoded locally. The local engine answers a downstream push with `APPLIED` at once, so the client drops its optimistic state. The rows the mutation changes arrive later, from the origin through the link. Between the two, the client can show the rows without the mutation. Declare a local mutator when this gap is visible to users.

In write-through mode both kinds wait for the origin's verdict before they commit.

## Compare-and-swap across the hop

A compare-and-swap (`expected` on `Patch`, or `WithExpectedVersion`) runs twice:

1. **Locally**, against the local version of the row. A stale guard fails at once with `conflict`, as on any server.
2. **At the origin**, against a version the link translates. When no pending write touches the row, the link sends the origin version of the confirmed row. When a pending write touches it, the link sends the origin sequence that write was applied at. When the origin rejected that earlier write, the guarded write fails with `conflict` too. When the origin applied that earlier write but its answer does not give the sequence, the guarded write also fails with `conflict`. This occurs when an origin that does not return the sequence on `ALREADY_APPLIED` answers a resend. The link does not use the confirmed version of the row in place of the sequence, because that version can include a later write by another client, and the guarded write would then overwrite it.

The result is the same as a compare-and-swap made directly against the origin at the moment the write arrives there. A guard that passes locally can still fail at the origin, because another writer changed the row there first. In write-behind mode, that failure is a revert with code `conflict`.

One case cannot be translated: a compare-and-swap on a row that an earlier operation of the same mutation already wrote. The local engine refuses it with `invalid_argument`.

## Partial sync across the hop

The link's subscriptions (`AddWorkspace`, `SetSubscriptions`) decide which rows the local engine holds. With none, it holds every row of every bound entity that the origin lets the link's principal read. A subscription can name only a bound entity.

Downstream clients subscribe against the local engine, so they can only get rows that the link's scope holds. A browser that subscribes to a wider scope than the link gets a complete answer for the local engine, and that answer is missing the rows outside the link's scope. Make the link's scope the union of what its downstream clients need.

A write to a row outside the link's scope still goes to the origin. The local engine holds the row only while the write is pending. When the origin confirms the write, it does not send the row back, and the row leaves the local engine.

When the scope changes, the origin sends the rows that enter it, and the link removes the rows that leave it.

## Collaborative text across the hop

A collaborative-text field takes the union of documents at every step, so a text edit converges across the hop in the same way as between two clients:

- **Upward**, the link sends the full document the local row holds after the write, never a fragment. The origin takes the union with its document, so edits from both sides stay.
- **Downward**, the link asks the origin for text deltas when it can, and merges each fragment into the confirmed document it holds. The local engine then serves its own clients with its own text delta log.

Each writer keeps its own peer id in the document. A browser edit through the local engine keeps the browser's peer id at the origin. A Go write through `upstream.Workspace` uses the peer id that `server.WithTextPeer` gives it.

A peer mints each new item id past the ids the stored row holds. A local engine that syncs only part of the origin's rows does not hold the ids a peer minted in a row outside its scope, so a write to such a row can mint ids the origin already holds with other content. The origin refuses that write with `invalid_argument`. The link then writes the text again under a peer unique to the link and the write, as plain text without formatting marks, and queues it behind the writes already pending, so the edit is kept rather than reported.

An edit can still fail. When the origin compacted its document past the state the local edit was made against, the origin refuses the edit with `text_state_stale`. The link then reports it through `OnRejected` and reverts it.

## Identity collapses at the origin

The link has one identity at the origin: the principal that `Config.Interceptors` or `Config.HTTPClient` establish. Every workspace syncs as that principal, and every write the link pushes is that principal's write. As a result:

- The origin's permissions and read rules apply to the link's principal, not to the user behind a downstream client. The local engine must enforce its own permissions for its own clients.
- At the origin, the change feed, field history and `CanMutate` see the link's principal as the writer of every forwarded write.
- The origin sees one client id, the link's. Downstream client ids stay in the local engine, where they make pushes idempotent on the first hop.

Presence crosses the hop through `engine/upstream/presencebridge` and collapses in the same way. Upward, the origin stamps every forwarded presence record and broadcast with the bridge's principal. Participants from the local engine appear at the origin with the bridge's subject, each under its own session id. Downward, origin participants appear locally with the subject the origin reports and the issuer `presencebridge.OriginIssuer`. The bridge leaves out the sessions the local engine published, so a local participant never comes back as a second participant. Presence stays soft state on both hops: when the origin is unreachable, local presence keeps working, and origin participants lapse locally after the local expiry window.

## Blobs across the hop

Rows that hold a `BlobRef` sync through the link, but the bytes do not. `engine/upstream/blobbridge` moves them. It wraps the local engine's blob store. It uploads a blob to the origin when a row of a synced entity names it, also when that row leaves the local engine before the upload, as a row outside the link's scope does once the origin confirms the write. It fetches a blob from the origin when a local reader asks for one that the local store does not hold. It uses its own identity at the origin, so the origin's upload policy and read rule apply to that identity. See [Blobs](blobs.md) for the model of blobs on one server.

## What the confirmed copy costs

The link needs the confirmed row to replay pending writes on. For a row that no pending write touches, the local row is the confirmed row, so the link keeps only a small record with the origin version. When a local write to the row is captured, the link copies the row as it was before the write into that record, and it removes the copy again when the origin decides the last pending write on the row. A bound row is therefore stored twice only while it has pending writes. Each bound row also costs one small record, and each pending write costs one entry. Local-only entities cost nothing extra.

The small record depends on one rule: only the link and captured writes change a bound row. The local engine does not compact the text of bound entities, because the origin compacts its own rows and the link receives the result as an ordinary change (see `server.WithoutTextCompaction`).

Each origin page costs one local write transaction that updates the confirmed rows, replays the pending writes on the rows it touched, and writes the rows that changed. The work for each page is proportional to the rows in it and the pending writes on those rows, not to the size of the workspace.

## Failures the link recovers from

- **The origin is unreachable.** The workspace goes to `StateOffline`. Local writes queue. The link retries with backoff.
- **The origin grants no access yet.** The workspace goes to `StateNoAccess`. Local writes queue. The link asks again with backoff.
- **The origin collected tombstones past the link's cursor.** The link replaces the workspace's rows with a fresh snapshot, keeps the pending writes on top, and calls `OnReset`.
- **The origin lost the link's watermark** (`mutation_id_gap`). The link learns the origin's watermark again and renumbers its pending writes after it.
- **The origin revoked the workspace.** The workspace goes to `StateRevoked`. The link stops syncing it, calls `OnAccessRevoked`, and refuses local writes to its bound entities. With `Config.DeleteOnRevoke`, it also deletes the local rows of bound entities.
- **A synced row changes outside the link.** A write straight to the storage adapter changes a row without a capture. The link finds this the next time it reads the confirmed state of that row, because the local row has a different version. It uses the changed row as the base for the moment, and takes a fresh snapshot to get the confirmed row back.
- **The local engine refuses a row.** For example, an origin change and a pending write together close a cycle in an access chain. The link keeps the row at its confirmed state, keeps the pending writes, and lets the origin decide them.

## What is not supported

- **More than one identity at the origin per link.** A link syncs every workspace as one principal. To sync as different users, run one link and one local engine per user.
- **More than one engine per link, or one engine per two links.** A link installs on one engine, and an engine has one write capture.
- **A local order that wins.** Two local engines linked to the same origin converge only through the origin.
- **Bridges on a `server.Manager`.** `presencebridge` and `blobbridge` read workspace ids as the local engine names them, and a `Manager` adds its app prefix to them.
- **A compare-and-swap after another write to the same row in one mutation.** See above.
- **Rows written to the local engine before linking**, without `AddOptions.AdoptLocalRows`. The first snapshot deletes them.

## Related

- [The convergence model](convergence-model.md): the order, last-writer-wins and rebase that the link reproduces.
- [Partial sync and coverage](partial-sync-and-coverage.md): what a subscription holds.
- [Rooms](rooms.md): presence and broadcast on one server.
- [Tunnels](tunnels.md): how a webview reaches a local engine.
- [Limits and trade-offs](limits-and-trade-offs.md).
