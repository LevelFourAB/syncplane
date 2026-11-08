# How connector convergence works

This document explains how connectors synchronize data between an external data store and a workspace, resolve concurrent edits, and recover from failures without data loss.

For instructions on configuring and running a connector, see [Sync an external store](../how-to/sync-an-external-store.md). For field and type specifications, see [Connectors reference](../reference/connectors.md).

## The per-row shadow and three-way merging

A connector tracks state changes using a per-row shadow that records three values:

- The row fingerprint
- The engine version
- The last-agreed bytes

The shadow serves two purposes in the sync loop:

1. **Echo breaking:** It prevents changes originating from the workspace or the external store from reflecting back to their source.
2. **Three-way merge base:** It acts as the common ancestor for incoming changes.

Because the shadow provides a base state, an edit to one field in the external source merges with a concurrent client edit to a different field in the workspace without overwriting the client update.

## Conflict resolution

When concurrent edits target the same field, arrival order in the workspace sequence resolves the conflict. The workspace processes connector updates using the same sequence ordering rules that apply to standard clients.

## Failure recovery and idempotent repair

Connector operations use an at-least-once model with idempotent repair. A failure costs a reconcile pass and does not lose data. Potential disruptions include:

- Losing the connector state database
- Crashing during an egress operation
- Pausing or sleeping past the garbage collection (GC) retention window

When any of these failures occur, a reconcile pass restores convergence between the source and the workspace.

Losing the state database also loses the shadows, so for a row that both sides hold nothing records which side changed it. A connector with a fixed `WriterID` keeps a copy of its egress position in the engine's store, as the consumer position `connect:<WriterID>`. That copy is stored with the rows, so it survives the loss. A row that the workspace changed after the copied position holds a client edit that the source has not received: the connector sends it to the source and does not take the source's value. Every other row takes the source's value. A row that both sides changed after the last egress keeps the client edit, and the source's edit to that row is lost. A connector without a `WriterID` has no name that stays the same across restarts, so it keeps no copy: after a lost state database every row takes the source's value, and client edits that the connector had not sent yet are lost.

When the engine store is swapped or restored from a backup, or the workspace is deleted and restored, the egress cursor names a history that the engine does not hold. The connector then deletes every shadow of the workspace and heals as an initial import: source rows come into the workspace, and workspace rows go out to the source. The shadows of the old history do not decide anything, so a source row that the engine does not hold is imported and not deleted at the source.

## Collaborative text handling

Connectors exchange text content as plain strings using `Item.TextFields`.

When synchronizing text, the connector diffs the incoming string against the shadow base. The shadow base represents the last state observed by the external source. The engine then unions this diff with the current document state. Both an external file edit and a concurrent client edit survive in the merged document.

For details on text CRDT operations, see [Collaborative text](collaborative-text.md).

If a writer has no stored ancestor in the shadow, it diffs against the current document and adopts the entire text of the field.

### Text constraints

Collaborative text synchronization in connectors relies on two constraints:

- **Stable writer identity:** A connector holds a single writer identity for its entire lifetime. You must pin `WriterID` to preserve this identity across connector restarts.
- **Text encoding boundary:** The connector processes data at the write boundary as text, not raw bytes. If an external string cannot be represented as valid code points, the connector logs an error for that item and skips it without changing the shadow. This prevents a single mis-encoded file from blocking ingress for the rest of the data source. The item converges on the first sync pass after you correct its encoding.
