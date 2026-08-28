# How connector convergence works

This document explains how connectors synchronize data between an external data store and a workspace, resolve concurrent edits, and recover from failures without data loss.

For instructions on configuring and running a connector, see [Sync an external store](../how-to/sync-an-external-store.md). For field and type specifications, see [Connectors reference](../reference/connectors.md).

## The per-row shadow and three-way merging

A connector tracks state changes using a per-row shadow that records three values:

- The row fingerprint
- The engine version
- The last-agreed bytes

The shadow serves two distinct purposes in the sync loop:

1. **Echo breaking:** It breaks echo loops in both directions by preventing changes originating from the workspace or the external store from reflecting back to their source.
2. **Three-way merge base:** It acts as the common ancestor for incoming changes.

Because the shadow provides a base state, an edit to one field in the external source merges cleanly with a concurrent client edit to a different field in the workspace, rather than clobbering the client update.

## Conflict resolution

When concurrent edits target the same field, conflicts resolve by arrival order at the workspace sequence. The workspace processes connector updates using the same sequence ordering rules that apply between two standard clients.

## Failure recovery and idempotent repair

Connector operations use an at-least-once model with idempotent repair. Failures never cause data loss. Potential disruptions include:

- Losing the connector state database
- Crashing during an egress operation
- Pausing or sleeping past the garbage collection (GC) retention window

When any of these failures occur, the recovery cost is a reconcile pass rather than lost data. The system uses the next pass to restore convergence between the source and the workspace.

## Collaborative text handling

Connectors exchange text content as plain strings using `Item.TextFields`.

When synchronizing text, the connector diffs the incoming string against the shadow base, which represents the last state observed by the external source. The engine then unions this diff with the current document state. This process ensures that both an external file edit and a concurrent client edit survive in the merged document.

For more details on text CRDT operations, see [Collaborative text](collaborative-text.md).

If a writer has no stored ancestor in the shadow, it diffs against the current document instead and adopts the text of the field in its entirety.

### Text constraints

Collaborative text synchronization in connectors relies on two constraints:

- **Stable writer identity:** A connector holds a single writer identity for its entire lifetime. You must pin `WriterID` to preserve this identity across connector restarts.
- **Text encoding boundary:** Data at the write boundary is processed as text, not raw bytes. If an external string cannot be represented as valid code points, the connector logs the error per item and skips that item while leaving the shadow untouched. Skipping invalid items ensures that a single mis-encoded file does not block ingress for the rest of the data source. The item converges on the first sync pass after you correct its encoding.
