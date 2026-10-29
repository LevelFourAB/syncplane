# Collaborative text

This document explains why the system uses a convergent conflict-free replicated data type (CRDT) for collaborative text, how concurrent edits merge without data loss, how delta synchronization keeps editing responsive, and how the engine maintains consistency across storage and clients.

Per-field last-writer-wins is the wrong tool for prose: when two people edit the same description concurrently, one person loses their entire field. A `bytes` field marked `text: true` instead holds a **convergent text document**. This document is a first-class CRDT implemented in Go (`engine/text`) and TypeScript (`client/src/text`) with byte-identical canonical encodings, verified by shared test vectors in `testdata/text/`.

Registration automatically installs the merge hook on both sides. Text requires no `WithMerger` or `MergerSpec` configuration. PATCH operations carrying document states merge as a **pure union**: idempotent, commutative, and associative. This property makes rebase replay and repeated pulls harmless by construction.

## Ordering is Fugue

Concurrent insertions at the same position never interleave. You receive `alicebob` or `bobalice`, never `abolibce`. The ordering algorithm is [Fugue](https://arxiv.org/abs/2305.00583).

## Replication is state-based

A shipped state is causally closed. Replication has no causal-delivery requirement, and offline splits converge on reconnect.

Stored rows, bootstrap chunks, and the `Changes` feed always carry the full encoded document. Replication payloads can instead carry a **delta-state fragment** (format version 2). A fragment contains the items, tombstone flips, and mark operations that one state contains beyond a base that the receiver is known to hold, plus the writer's full version vector.

Applying a fragment uses the same pure union and produces byte-identical results to a full merge. The engine rejects a fragment **whole** if the fragment cannot apply because of an unknown parent or anchor. The engine answers with full state as the universal fallback, never with a partial write.

## Deltas in three places

### Upstream: text PATCHes ship deltas

A pushed text PATCH whose mask names the field carries the fragment between the row that the client last confirmed from the server and the value being written. The server state always covers that confirmed row. Typing into a large document costs a payload proportional to the edit rather than to the full document.

If the server cannot apply a fragment, it rejects the edit with `text_full_state_required`. This acknowledgment consumes no sequence number and leaves the row untouched. The client re-sends the same edit as a whole document and stops using deltas for that row. Applications never observe the rejection. Nothing negotiates, and a client that never sends fragments works against the same server.

### Downstream: pulls ship deltas

Downstream synchronization mirrors the upstream design. Every write of a text-bearing row appends its added fragment to a per-(row, field) log keyed by the sequence number of the write. A pull operation folds the entries above the client's cursor into one fragment carried beside the row (`RowChange.text_deltas`, with those fields cleared from `value`).

Catching up on another peer's typing costs a payload proportional to what they typed. A field that remained untouched over the span ships as an empty fragment that means "keep what you hold". A row whose other fields changed stops re-sending its document entirely. The client folds the fragments back into its confirmed row, so IndexedDB, reads, watches, and rebase replay continue to observe whole documents.

The server sends full state instead whenever a fold cannot be trusted:

- The log no longer covers the span because garbage collection (GC) pruned it (see `WithTextDeltaRetention`).
- A write in the log cannot be expressed as a delta, such as a cleared or deleted document.
- The fragment saves too little data.
- The client visibility of the entity might have changed since its cursor due to an application `ReadFilter`, a predicate, a window, or a follow closure.
- A write in the span changed the row's access parent, or a field that a read rule compares with the caller's subjects. Such a write can show the row to a caller for the first time, and the log cannot tell that caller apart from a caller that held the row. Every reader whose span covers the write therefore receives the documents whole once.
- The pull is the catch-up after a delta resync. The resync sends the rows that the new scope or access shows as of the cursor only, so every page of the catch-up sends full state until the client is current.

Bootstrap chunks, delta resyncs, membership diffs, query results, and the `Changes` feed always send full state.

The access model does not stop deltas. A caller below the workspace root receives fragments, as a caller at the root does. A change of the caller's grants, or a move above a row that changes what the caller reads, turns the pull into a delta resync. A change of the row's own access parent or subject field records a gap in the log. Each case sends full state to a reader that did not hold the row (see [Access model](access-model.md)).

The server can make each of these decisions independently. Holding the row is necessary but not sufficient: folding a fragment also requires a schema, and a build routinely stores rows for an entity that only a neighbouring tab declares (see [A client holds only the entities it declares](convergence-model.md#a-client-holds-only-the-entities-it-declares)). Only the client knows this information, so it names those entities in `PullRequest.no_delta_entities` and their rows arrive as full state.

### Locally: edits are deltas in storage too

A live text handle queues the fragment for the edit that it applied. The engine appends that same fragment to a per-(row, field) **optimistic text log** (`ot/`) instead of rewriting the optimistic overlay row around it.

A keystroke writes bytes proportional to the keystroke rather than to the document. On a 148 KB document, a keystroke commits under a kilobyte where rewriting the row cost ~149,000 bytes. An offline typing burst grows with the text typed rather than with the document size multiplied by keystrokes. Both writes commit in the single transaction that makes a local edit crash-safe.

Reads fold the log back, so stored rows, rebase replay, and the `Changes` feed continue to observe whole documents without reader-side coordination. The log is derived state with the lifetime of the overlay. Any event that renumbers the queue — such as a rebase, a resequence, or a compaction — clears both the overlay and the log, and replay rebuilds them from the queue. This rebuild is necessary because each log entry is keyed by the mutation id that wrote it.

Only a pure text PATCH whose fields all carry fragments takes this path. A mask that also names a scalar, or a whole-document write, writes the row and drops the log it supersedes. Queue compaction folds deltas from an edit burst into one mutation before pushing. String-valued text writes queue whole documents, and the push path normalizes both writes to the same cumulative delta against the confirmed row.

## Caches validated by comparison, not invalidation

Three caches make this model efficient. All three rely on the same property: the encoding is **canonical**. An entry is usable when the bytes it was built from equal the bytes being evaluated (a `memcmp`). A cache hit cannot be invalid, no producer must report a change, and nothing requires invalidation:

- **The optimistic fold** is served from a per-row cache validated by comparing the row bytes and fragments from which it was built. A concurrent tab writing to the shared store cannot invalidate the cache data. Reading an unchanged row costs comparison checks instead of decoding the document (~0.065 ms compared to ~17 ms at 50,000 code points). The next keystroke extends the cached document with the appended fragment rather than decoding again.
- **Written documents stay decoded between writes** on the server (`server.WithTextCache`, enabled by default). The write closure decodes the stored document, merges changes, and re-encodes. Decoding is the largest of these three operations. A rejected or retried write leaves a cache miss rather than stale data. At 50,000 code points, 8.4 ms of workspace-exclusive time per keystroke drops to 2.5 ms. `WithTextCache(0)` trades memory for decode execution time.
- **Read documents stay decoded on the client.** Every client path that modifies a text field materializes a document, including the optimistic fold or an arriving pull page folded into the confirmed row. A moment later, a reader rendering the field decodes those same bytes back into the same document, borrowing it from whichever component built it. Merging takes a document _out_ rather than borrowing it, which prevents aliasing during active writes. At 50,000 code points, a remote keystroke applied to the confirmed row costs 6.5 ms instead of 23.7 ms inside the key-value (KV) write transaction, which is held exclusively across tabs. Rendering a row while another peer types costs 8.6 ms instead of 50.9 ms. A local keystroke rendered by a watcher other than the typist costs 12.6 ms instead of 32.8 ms. Documents under one kilobyte are not retained in this cache. Decoding a small document takes tens of microseconds, and caching hundreds of small text fields displaces larger documents where caching provides substantial gains.

## Writer identities

An item id is `(peer, counter)` and counters derive from the document's version vector. Therefore, **two documents that can diverge must not share a peer**.

Each open handle leases an identity (`<client_id>:<slot>`) for its lifetime and releases it on `close()`. Two tabs — or two editor bindings in one tab — never collide, and slots recycle instead of growing the document peer table.

Recycling is protected by the ordering around a lease. A handle leases an identity before reading its row and returns it only after queued edits enter the store. The next holder of the slot builds on state that covers previous writes, allocating past earlier counters. The same rule applies to non-handle writers: undo replay and string-valued writes each hold a lease across the transaction that persists them.

A slot retains a consistent meaning across tabs by using a Web Lock. When running in a browser without a lock manager that shares an IndexedDB database across tabs, each handle takes an unrecycled `<client_id>:u<id>` identity instead. This identity is unique without cross-tab coordination, at the cost of one peer-table entry per written document.

Server-side writers maintain the same invariant and bound. A connector holds one identity for its lifetime (set `WriterID` to keep it across restarts), and the admin user interface (UI) uses one identity per handler. They can reuse identities because they diff against the stored document read inside the write transaction, which contains their earlier writes.

The merge operation rejects identity violations rather than converging on corrupted text.

## Marks and anchors

**Marks** (Peritext-style) attach key-value ranges to characters so that the ranges move with the text and survive concurrent edits. Resolution occurs per-key-per-character by highest operation id. Removal is an operation rather than a deletion. The `expand` setting controls whether typing at boundaries expands the mark.

**Anchors** are stable positions (`(item, before|after)`), encodable to bytes for storage or presence. A deleted target degrades to the nearest live neighbour.

A mark can also be **collapsed** (`start === end`), covering no characters. Both boundaries sit at one caret, and their anchor configuration determines subsequent behavior:

- `ExpandBoth` straddles the caret. The first character typed at the caret falls inside, converting the mark into a standard span. This is a **pending format**: you press Cmd-B, then type. It lives in the document rather than in editor memory, surviving page reloads and remaining visible to other peers.
- `ExpandNone` pins both boundaries to the same edge. The mark stays zero-width for the life of the document while drifting with surrounding text. This represents a **note attached to a position** rather than a span.

Cancelling a pending format requires no new primitive. An unmark operation at the same caret resolves to the same anchors and outranks the pending mark, both while covering nothing and after typing expands it. Because a collapsed mark covers no characters, it is invisible to the span resolver and read through two alternative calls: `marksAt(index)` for styles that typed text inherits, and `points()` for zero-width marks.

Annotations — such as comments on a range — combine marks, anchors, and an entity row. They are an architectural pattern rather than an engine feature. The range lives inside the document as a mark keyed by `comment:<id>`. The comment body, author, and resolved status live in a standard entity row keyed by that ID. This entity structure provides last-writer-wins merging, permissions, queries, and undo support. To make edits land atomically and undo in a single step, write both operations in one `client.transact()` call. Per-key resolution allows overlapping comments to coexist, and resolution costs O(document + marked text) rather than O(keys × document).

## Both decoders accept exactly the same documents

Varints are capped at 2^53−1 (the JavaScript safe-integer ceiling). Strings must be valid UTF-8. Float mark values must be finite. A mark anchored to an item that the document does not contain is an error rather than a guessed range. Counter 0 belongs to the root sentinel, so an item or a mark operation that holds it is refused. Without that rule, one write could store a document that no reader can decode. A run whose last item counter passes 2^53−1 is refused for the same reason: the encoder writes that counter as a varint in the version vector.

Shared test vectors pin all refusals by payload and exact error message. This verification prevents the engine from storing a document that fails only in the browser.

## The counter ceiling

Every edit takes a counter above every counter the document has seen, from any peer. An insert takes one counter for each code point, and a mark or an unmark takes one counter. When the next counter would pass 2^53−1, an edit fails and changes nothing: Go returns `text.ErrCounterCeiling`, and TypeScript throws `text: no counters left below the ceiling`. A union never lowers the version vector, so a document whose counters reach the ceiling stays readable but can never be edited again.

Honest edits cannot reach this ceiling. The danger is one write that puts a very high counter in the version vector. For this reason, the server refuses a write that moves the highest counter of a text document more than 2^32 above the stored document (with error `invalid_argument`). For a new document, the server measures the increase from 0. The rule covers client pushes and trusted writes. A step of 2^32 is far more than any honest write needs, and reaching the ceiling takes millions of writes to one row instead of one.

## The write boundary takes text, not bytes

Items are code points. The engine rejects a string that cannot be represented as code points (invalid UTF-8 in Go, or an unpaired surrogate in JavaScript) rather than storing it with replacement character U+FFFD.

A substituted document would be well-formed and pass subsequent checks, so a writer would discover data loss only when reading the field back. A connector would not detect the failure, because source comparisons would mismatch and rewrite the row on every pass. Connectors skip invalid items (logged per item, leaving shadow state untouched) so that one mis-encoded file does not block ingress for the rest of the source. Connectors converge on the first pass after you fix the encoding.

String writes use a multi-hunk Myers diff over code points, preserving untouched regions, their items, and attached marks. Strings that diverge beyond the shared `maxDiffCost` bound (1024) fall back to a single replacement hunk. This bound is pinned by shared vectors because it determines which items survive.

## A field can carry text instead of a document

Encoded forms are tagged by their first byte: `0x01` for a document, `0x02` for a delta-state fragment, and `0x03` for a **plain-string value** (the tag followed by UTF-8 bytes). The plain-string value allows values to represent what a field should read as before assigning a writer identity. Field history is the primary use case: the server captures what a field held, and the identity that restores it belongs to whoever performs the restore, potentially days later.

Recording the document directly fails for two reasons:

1. It cannot be restored as itself. Writing an old document back merges it with the stored document, where deletion markers take precedence, so the restore leaves the text unchanged.
2. It costs several times the size of the rendered text, recorded repeatedly for every write.

The tag makes the recorded value a **legal value of the field** rather than an informal convention. A record decodes as a partial message of its source entity, restores by being applied with a PATCH under the record mask without special handling for the text field, and `plainTextOf` or `text.PlainText` reads either form for display.

Resolving a plain-string value requires a diff against the stored document, minting changes under a writer identity. This resolution occurs where an identity exists: the client enqueue path under its leased peer, and `EntityTx.Put` or `Patch` under `WithTextPeer`.

Other components reject plain-string values directly. The document decoder rejects them as it does fragments, and the merger rejects them on both sides. The server ingress guard rejects plain strings in any position, masked or unmasked. The wire protocol carries documents, and the client resolves strings before pushing.

Where a value can appear is therefore a matter of **direction, not storage**. A field-history record carries a plain string at rest, and that record is stored and served like any other row. The engine refuses plain-string values arriving as the incoming payload of a write. Only an active text field is guaranteed to hold a document.

## The PUT exception

Every PUT path (`EntityTx.Put` for trusted server-side writes and connectors, client push PUT, and the client local overlay) **merges** text fields with the stored row instead of replacing them. A PUT operation built from a stale snapshot must not discard edits that its writer never observed, and a PUT racing an in-flight PATCH must not duplicate the document. All other fields maintain replace semantics.

A PUT that carries no document for a text field keeps the stored document. Its writer never observed one, and the union of the stored document with nothing is the stored document. To clear the text, write an empty string, which deletes the text through the diff, or send a PATCH whose mask names the field and that carries no bytes. Removing a keyed list element or map entry still removes its document.

The server also validates text bytes on ingress (PUT and PATCH). The server rejects data that decoders cannot read as `invalid_argument` instead of corrupting the row for readers. A PATCH can carry a delta-state fragment in a text field that its mask names directly, validated and unfolded by the merger. A PUT operation, or a field reached only through an ancestor mask, must carry a full document because nothing merges it there.

## One document per element

A text field path can cross a keyed message list or a message-valued map. In this case, the field holds one document **per element**, and all references address the document by its **instance path** — the mask form of its position, including the element key (`comments.<id>.body`). The instance path serves as the document identity everywhere per-document state lives: the PATCH mask routing an edit through the merger, the server delta log and compaction floors, decoded-document caches, the client optimistic text log, and `FieldDelta.field` on the wire.

This design works because the element key provides a **stable identity**. The merge descends into a keyed element and resolves mergers by schema path (`comments.body` serves every element). Concurrent edits to an element document merge into a union like top-level fields, and edits to different elements do not interfere. Other container shapes remain unsupported: an unkeyed list element lacks an addressable identity, and a field inside a `oneof` disappears when an alternate case is chosen. In both situations, a masked write replaces the enclosing unit entirely, preventing the merger from running.

The element lifecycle controls the document lifecycle. Removing an element removes its document. The write records a gap in the delta log at that position, ensuring readers covering removal and re-creation receive full state rather than a delta that resurrects old text. It also clears the compaction floor, allowing a re-created element under the same key to start a fresh document rather than measuring against earlier purges. A mask naming a whole element (`comments.<id>` without a sub-path) replaces the element and its document. Client diffs avoid emitting whole-element masks for text-bearing elements, narrowing instead to per-unit paths so documents merge while scalar fields maintain last-writer-wins semantics.

## Undo is op-level and local-only

A snapshot inverse on a state CRDT merges into a no-op. Handle edits therefore record **symbolic inverses**: inserted identifier ranges, deleted content with surviving-neighbour anchors, and marks with prior resolved values. These inverses materialize against the document state present at undo time. Undo removes your specific edits while preserving concurrent edits from peers.

Typing coalesces into word-sized undo groups based on idle timeouts and whitespace boundaries. Selection restoration uses the standard delta subscription mechanism.

Undoing a deletion restores text content and formatting, but not original item identities. Tombstones are monotone, so restored content receives new identifiers. The inverse carries marks resolved over the text at deletion time to re-apply them. Formatting and comment ranges survive deletion, undo, and repeated undo/redo cycles. Anchors held outside the document do not follow restored text, so applications must read positions back from marks rather than retaining external anchors across an undo. Restoring through re-marking has two side effects: a concurrent unmark operation can be undone with the deletion, and the engine does not retain the original `expand` policy, defaulting restored marks to `ExpandNone`.

## Compaction

Without compaction, a document retains one item per typed character and one mark operation per formatting action. Setting `server.WithTextCompaction(minPurged)` runs a background sweep that purges unneeded document state and records the version vector as the field **compaction floor**.

The engine purges two types of state:

- **Tombstones** that are deleted, have no children, and are not anchored by a surviving mark. A run that was typed and deleted collapses completely. The engine preserves a tombstone that positions active text because sibling ordering relies on identifier order, and re-parenting children would shift text positions. Compaction bounds a document by live text **plus the tombstones that position it**, rather than live text alone.
- **Mark operations that can never win again**. An operation is dead when a higher-id operation on the same key shares identical anchors (producing identical coverage under future insertions and permanently shadowing the lower operation), or when it is a removal operation with no remaining targets on its key. Operations cannot be judged dead based on current shadowing alone: two operations covering the same characters with different expand policies diverge when characters are typed at boundaries. Similarly, zero width does not indicate a dead operation, because point annotations are zero-width by design. A mark and its cancelling unmark operation are reclaimed as a pair, preventing abandoned pending formats from retaining permanent operations and pinned tombstones.

The sweep purges mark operations first because they often represent the only reference keeping boundary tombstones alive. Reclaiming mark operations makes those tombstones eligible for removal in the same pass.

The sweep processes only **quiet** rows whose last write is at or below the sequence through which the delta log has been pruned (`WithTextDeltaRetention`). At this point, the server can no longer send cheap deltas to readers. The rewrite commits at a new sequence, causing clients to re-fetch the field once, and records a gap in the delta log to prevent folding across a purge.

The prune horizon paces the sweep. Each pass inspects the window between where the previous pass stopped and the current prune sequence, matching rows that became quiet in that interval. Sweep cost scales with edit activity rather than total storage, and workspaces with idle documents are skipped. Compaction requires `WithGC` because the sweep only touches rows quietened by delta pruning. Compaction runs separately from garbage collection because GC deletes indexed ranges, whereas compaction scans, decodes, and rewrites rows. Combining them in one transaction would force writers to wait on row processing.

Passes within a workspace are bounded by document size. A single transaction processes at most `batch` rows or a fixed byte threshold, committing rewrites alongside progress markers. Pages of large documents complete early, and subsequent transactions resume at the marker. Rows are decoded only when stored tombstones and mark operations reach `minPurged`, which the encoding indicates without full decoding. Fields without deletions are skipped with minimal overhead.

The compaction floor ensures safety. An incoming write containing an identifier absent from the row, at a counter covered by the floor, represents pre-purge state. Because items and mark operations share a counter space, one floor handles both:

- If received as a **tombstone** or referencing a purged **mark operation**, the write applies with that identifier skipped, allowing replicas with older reads to continue functioning. Without this rule, the union merge would reintroduce reclaimed operations on subsequent writes.
- If received **live**, or if the write attaches new text to a purged item, applying it would restore purged state. The server rejects the write with `text_state_stale` without consuming a sequence number or modifying the row.

The client recovers by re-reading the row and re-applying only its own edits that the row has not seen: deletions of characters the row still shows, and text it typed that the row's version vector does not cover. Edits other peers made in the meantime stay as they are, including text they typed or deleted while the stale client was away. A live `TextHandle` performs this recovery automatically upon detecting compacted state. An edit still queued from a closed handle recovers the same way: when a pull or snapshot replaces the row with compacted state the queued edit no longer fits, the client keeps the edit as a whole document, and the `text_state_stale` refusal that document gets starts the recovery. **This recovery discards formatting**: mark operations the stale client applied that the row has not seen do not survive, and re-applied text gets new identifiers.

Because compaction can discard formatting during stale recoveries, it is disabled by default. Enable compaction for long-lived documents edited over months, and leave it disabled for short-lived documents.

## What is out of scope

Collaborative text provides no presence or awareness channel, and rich text formatting is out of scope. Marks are intended for annotations; store formatted prose using markup formats such as Markdown.

A client that does not implement the CRDT cannot set a text field directly. The wire protocol rejects plain-string values across all positions, requiring strings to resolve in-process using the TypeScript client enqueue path with a leased peer or Go server code using `EntityTx.Put` or `Patch` with `WithTextPeer`.

This limitation is not inherent to the CRDT. A server-minted peer could function similarly to connector peer reuse: resolution executes inside the serialized write transaction and inspects the stored document version before allocating counters. However, the engine requires a reliable identity and an ancestor state. The `client_id` is an unauthenticated idempotency token, and ancestor state cannot be reconstructed from current documents. A three-way diff requires the document the writer previously observed rather than plain text, because diff hunks map to positions anchored to specific items, and items store single deletion flags without operation provenance. Version vector states cannot be rebuilt from current documents, making stored ancestors (such as a client confirmed row or connector shadow) necessary for three-way merges.

A stateless caller is limited to the two-way merge used by `EntityTx.Put`, `EntityTx.Patch`, and ingress-only connectors: text present on the server but absent in the string is deleted, while subsequent concurrent edits merge normally. This behavior is suitable when a caller provides its own identity and acts as the source of truth, but not as a general string API. Field history reflects this design: recorded values are readable by any client, but can be restored only by clients capable of resolving them.

## Related

- [Use collaborative text](../how-to/use-collaborative-text.md) — How-to guide for reading and writing text fields.
- [Adding collaborative text](../tutorials/adding-collaborative-text.md) — Tutorial for adding collaborative text to an application.
