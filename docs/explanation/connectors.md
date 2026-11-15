# How connector convergence works

This document explains how connectors synchronize data between an external data store and a workspace, resolve concurrent edits, and recover from failures without data loss. The sections before [Record mode](#record-mode) describe row mode, where one external item is one row. Record mode, where one external record is a group of rows, has a section of its own.

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

- **Stable writer identity:** A connector holds a single writer identity for its entire lifetime: `TextPeer`, or `connector:<WriterID>` when `TextPeer` is empty. Pin one of them to keep the identity across restarts. Two connectors that can write the same rows at the same time need different identities (see [TextPeer and WriterID](#textpeer-and-writerid)).
- **Text encoding boundary:** The connector processes data at the write boundary as text, not raw bytes. If an external string cannot be represented as valid code points, the connector logs an error for that item and skips it without changing the shadow. This prevents a single mis-encoded file from blocking ingress for the rest of the data source. The item converges on the first sync pass after you correct its encoding.

## Record mode

Many stores do not hold one item for each row. A Markdown file holds a page and its blocks, and a JSON document holds an order and its lines. The store reads and writes the whole file, and an editor saves the whole file. Record mode syncs such a **record** with a group of rows: one **root row** and the **member rows** that name the root row in a field (`Member.RecordField`).

The adopter writes two parts. A `RecordStore` does the native I/O: list, read, write and delete records. A `Codec` maps a record to rows and rows to a record. The connector does everything between them: scheduling, bookkeeping, the quiet period before a write, the delete grace, rename detection and the first pass. `RowCodec` is a `Codec` that also does the per-row merge, so that the adopter only decodes content to rows and encodes rows to content.

### Key and locator

A record has two names, and they change for different reasons.

- The **key** is the key of the root row. It is the identity of the record in the engine, and it does not change. Clients and queries refer to it.
- The **locator** is the address of the record in the store, such as a file path. A user renames a file, or a client changes the title that the file name comes from. Both sides can change it.

The connector keeps an index from locator to key. A rename changes the locator and keeps the key, so the root row, the member rows and their keys stay. A store that keeps an ID in each record can report it as `RecordRef.Key`: a record that moves to a locator the connector does not know then keeps its key without a guess.

### Bookkeeping in the engine store

Row mode keeps its shadows in the connector's own `State`. Record mode keeps all its bookkeeping in the engine's store, as the position and the shadows of the consumer `connect:<WriterID>`. This placement gives three properties:

- **One transaction for rows and bookkeeping.** An ingest writes the rows and the new agreed state of the record in one `Mutate` (see the consumer-shadow methods of `EntityTx`). A crash cannot keep one and lose the other. A replay finds the agreed state with the new fingerprint and writes nothing.
- **Backup and restore with the rows.** A restored engine store has the bookkeeping that agrees with its rows. A separate state file could be older or newer than the rows, and every difference would look like an edit.
- **No rerun of the first pass after garbage collection.** The consumer is retained (`server.ConsumerUpdate.Retain`), so garbage collection never removes it. The agreed states stay correct below the tombstone horizon. When the feed cannot resume from the stored position (`server.ErrCursorTooOld`), the connector marks every record for egress, renders each one, and writes only those that differ from the agreed content. A collected consumer would run the first pass again, and with `SourceWins` that deletes every row that clients created while the connector did not run.

The cost is space in the engine store: an agreed state for each record, an index entry for each member row, and, with `RowCodec`, a copy of each row as the store holds it.

### Echo detection

A connector writes to both sides, and each write comes back to it as a change. The agreed state of a record stops both echoes:

- **From the store:** the agreed state holds the fingerprint of the content that the two sides agreed on. When the store lists a record with that locator and fingerprint, the content is unchanged, or it is the connector's own write. The connector does not read it. This is why equal content must give an equal fingerprint in `List`, `Read` and `Write`.
- **From the engine:** the agreed state holds the engine sequence that its content reflects. A feed change at or below that sequence is already in the content, so the drain does not mark the record. The rows of the connector's own merge take a sequence at or below the agreed state, so they do not come back as engine edits.

A third check stops writes that change nothing. Before a write, the connector compares the render with a hash of the agreed content. An equal render updates the agreed state and writes nothing to the store.

### Quiet period and maximum delay

A user who types into a note makes many engine changes in a few seconds. A store write for each change costs I/O, and an editor that has the file open reloads it each time. The drain therefore only marks the record dirty. The mark commits with the feed position, so the position moves past the change while the record waits, and a crash does not lose the mark. Egress writes the record when it has had no engine change for `Quiet`.

A user who never stops typing would keep the record waiting. `MaxDelay` sets a limit: the record is written when its first unsent change is `MaxDelay` old, also while changes keep coming. `Flush`, `Close` and `Forget` write every waiting record at once.

### Check before egress, and write-back

Between two passes, the store can change a record that also waits for egress. A write of the render would replace that store edit. Before each write, the connector reads the store record. When its fingerprint moved since the agreed state, the connector ingests the record first. The merge keeps both edits, and the render after it holds both.

An ingest can keep engine edits that the store does not have. After each merge of a `Bidirectional` record, the connector renders the record in the same transaction. When the render differs from the store content, the record is written back. The dirty mark that orders the write-back commits in the same transaction as the merge, so a crash before the write-back does not lose the engine edits: the next pass writes them.

Egress never writes over a store record that the connector does not know. When a new record wants a locator that holds unknown content, the connector ingests that content as a new record, and the engine record waits until the locator is free. The only exceptions are the cases in which the adopter gets the old content first through `Records.Overwrite`.

### Delete grace and rename detection

Many editors save a file by deleting it and writing a new one, or by writing a temporary file and moving it over the old one. A store watcher can report the delete before the new file exists. A connector that deleted the rows at once would delete every member row and create new rows with new keys a moment later, and client edits to those rows would be lost.

A record that is missing from the store therefore gets a missing mark first. Its rows are deleted only when it is still missing after `DeleteGrace`. A record that comes back in that time keeps its rows.

A rename has the same shape: the old locator is missing, and a new locator appears. When the new locator has the fingerprint of a missing record, the connector takes it as a rename of that record. The record keeps its key and its row keys. With `Touch`, the two locators can arrive in either order and in different calls: an index from agreed fingerprint to record finds a record whose old locator the store no longer holds. A copy has the fingerprint of a record whose locator is still there, so it becomes a new record. Rename detection needs a fingerprint that depends on the content only, such as a content hash. A fingerprint from the modification time cannot match two locators.

### Pending writes and crash replay

A store write and the engine store cannot commit in one transaction. A crash between the write and the new agreed state would leave the connector with an agreed state that describes the old content, and the next pass would take its own write for a store edit.

Before each store write or delete, the connector commits a **pending write** that names the content that the store holds when the write is done. Each pass starts by settling the pending writes from what the store holds now:

- **The written content:** the write is done, and its agreed state is saved.
- **The content that the write replaced, or nothing:** the write was not done. The dirty mark of the record makes a later pass write it.
- **Other content:** the store changed the record after the crash, and the write may or may not be done. The old agreed state stays the base, moved to the written locator, so that the next ingest merges the store content with it. A record with no agreed state drops the write, and the next list ingests the store content as a new record.

For a delete, a missing record completes the delete, and a present record drops it.

### The first-pass policy

The first pass runs when a workspace has no record-mode bookkeeping: on the first sync, and after `server.ResetConsumer`. Then there is no agreed state, and for a record that one side holds and the other does not, nothing tells which side changed it. A root row with no file is a row that a user created, or a file that a user deleted. A file with no row is a new file, or a row that a client deleted.

`SourceWins` takes the store as the truth: it imports every record and deletes the rows that no record holds. `EngineWins` takes the rows as the truth: it writes every root row to the store, and imports only the records that no root row renders to. It deletes nothing.

The connector does not infer the policy from the rows or the store, because the same state has opposite meanings. An empty folder can be a new folder that must receive the rows, or a folder whose files a user deleted on purpose. A restored engine store can be older than the folder or newer. A wrong guess deletes data on one side. The host knows which case it has, for example a first connection of a folder or a restore from a backup, so the host decides: `Records.FirstPass` for every workspace, or `Records.ChooseFirstPass` for each workspace.

For the same reason, a first pass fails when the codec refuses a record, instead of skipping it as a later pass does. With `SourceWins`, the rows of a refused record look like rows that no record holds, and the pass would delete them. With `EngineWins`, the locator of a root that cannot be rendered is unknown, and the pass would import the store record there as a new record.

### Refused writes on a replica

On a replica linked to an origin with `engine/upstream`, an ingest writes rows on the replica first. The origin can refuse the write later, for example for a permission. The link then reverts the rows, and the revert is an engine change. Egress renders the reverted rows and writes them over the store content. The store edit of the user is gone, with no trace.

`Connector.Refused` stops that. The host calls it from `upstream.Config.OnRejected` with the rows of the refused write. The connector marks their records. The next egress of a marked record gives the store content that it replaces to `Records.Overwrite` with `OverwriteRefused`, so the adopter can keep a copy or show it to the user.

The revert and the call do not arrive at the same time. The link reverts the rows before it calls `OnRejected`, and egress can run in between. Then the store content is already replaced when `Refused` marks the record. `Records.KeepReplaced` covers this case: the connector keeps the content that an egress write replaced when the record had an ingest a short time before. `Refused` then gives that kept content to `Overwrite`.

### TextPeer and WriterID

A connector has two identities, and they must follow opposite rules across an origin and its replicas:

- **`WriterID` names the connector.** It labels its writes as `connect:<WriterID>` and names the consumer that holds its bookkeeping. An effect or another connector skips the writes of a connector by this label (`effects.SkipWriters`). Give every instance of one connector the same `WriterID`, also an origin and its replicas, so that one constant skips the writes of all of them.
- **`TextPeer` is the collaborative-text writer identity.** Text items carry the peer that minted them. Two live writers that share a peer mint items with the same IDs, and the merge refuses the documents (see [Writer identities](collaborative-text.md#writer-identities)). An origin connector and a replica connector can write the same rows at the same time, so each needs its own `TextPeer`. Keep each value the same across restarts: a document keeps an entry for each peer that wrote to it, and a new peer on each run adds a dead entry each time.

`TextPeer` defaults to `connector:<WriterID>`. That default is correct for one instance, and wrong for an origin and its replicas, which share a `WriterID`.

### Trade-offs and limits

- **A moved part is a delete and a create.** `MatchKeys` matches parts by a longest common subsequence. A line that moves past others does not match, so it gets a new key: its old row is deleted and a new row is created. Fields that the record does not own, such as a client-only flag, do not move with it. A format that stores an ID for each part avoids this: `Decode` can then give each part the key that the content names.
- **A replay with an old agreed state can double a text edit.** When the connector merges a record against an agreed state that is older than the rows, for example after a pending write with an unknown result, the merge applies the store's changes from that older state again. A collaborative-text field that both sides changed since then can hold the replayed edit twice. A field that only one side changed, or that both sides changed to the same text, is not affected.
- **One instance for each workspace on each server.** Nothing leases a workspace to one connector. Two instances on one server share the bookkeeping, and both write the store. Run exactly one instance for each workspace on each server. An origin and a replica are different servers, so each runs its own instance over its own store.
- **The whole record is rendered on each write.** A record with many rows costs a render and a store write of all of them for each egress, and the codec runs under the workspace write lock. Split a large document into several records when its size makes writes slow.
- **The codec runs inside the transaction.** `Merge`, `Render`, `Decode` and `Encode` must do no I/O and must give the same result for the same input, because the transaction can be retried.
