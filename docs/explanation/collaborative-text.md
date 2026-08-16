# Collaborative text

Per-field last-writer-wins is the wrong tool for prose: two people editing the
same description concurrently means one of them loses the whole field. A `bytes`
field marked `text: true` instead holds a **convergent text document** — a
first-class CRDT implemented twice (Go `engine/text`, TypeScript
`client/src/text`) with byte-identical canonical encodings, verified by shared
vectors in `testdata/text/`.

Registration auto-installs the merge hook on both sides — there is no
`WithMerger` or `MergerSpec` for text — and PATCHes carrying document states
merge as a **pure union**: idempotent, commutative, associative. That is what
makes rebase replay and repeated pulls harmless by construction.

## Ordering is Fugue

Concurrent insertions at the same position never interleave — you get
`alicebob` or `bobalice`, never `abolibce`. The ordering algorithm is
[Fugue](https://arxiv.org/abs/2305.00583).

## Replication is state-based

A shipped state is causally closed, so there is no causal-delivery requirement
and offline splits converge on reconnect.

Stored rows, bootstrap chunks and the `Changes` feed always carry the whole
encoded document. Replication payloads may instead carry a **delta-state
fragment** (format version 2) — the items, tombstone flips and mark ops one state
has beyond a base the receiver is known to hold, plus the writer's full version
vector.

Applying a fragment is the same pure union and produces byte-identical results
to the full merge. A fragment that cannot apply (unknown parent or anchor) is
rejected **whole** and answered with full state — the universal fallback — never
a partial write.

## Deltas in three places

### Upstream: text PATCHes ship deltas

A pushed text PATCH whose mask names the field carries the fragment between the
row the client last confirmed from the server (which the server's state
therefore always covers) and the value being written. Typing into a large
document costs a payload proportional to the edit rather than to the document.

If the server cannot apply a fragment it rejects with
`text_full_state_required` — an ack that consumes no sequence and leaves the row
untouched — and the client re-sends the same edit as a whole document and stops
using deltas for that row. Apps never see the rejection; nothing negotiates, and
a client that never sends fragments works against the same server.

### Downstream: pulls ship deltas

The mirror image. Every write of a text-bearing row appends the fragment it
added to a small per-(row, field) log keyed by the write's sequence, and a pull
folds the entries above the client's cursor into one fragment carried beside the
row (`RowChange.text_deltas`, with those fields cleared from `value`).

Catching up on someone else's typing then costs a payload proportional to what
they typed. A field nothing touched over the span ships as an empty fragment
meaning "keep what you hold", so a row whose _other_ fields changed stops
re-sending its document at all. The client folds the fragments back into its
confirmed row, so IndexedDB, reads, watches and rebase replay keep seeing whole
documents.

Full state is sent instead wherever the fold could not be trusted: the log no
longer covers the span (it is pruned on the GC pass — see
`WithTextDeltaRetention`), a write in it was not expressible as a delta (a
cleared or deleted document), the fragment saves too little, or the client's
visibility of the entity could have changed since its cursor (a read filter, a
predicate, a window or a follow closure). Bootstrap chunks, delta resyncs,
membership diffs, query results and the `Changes` feed are always full state.

Every one of those is a judgement the server can make alone. Holding the row is
necessary and not sufficient — folding a fragment also takes a schema, and a
build routinely stores rows for an entity only a neighbouring tab declares (see
[A client holds only the entities it declares](convergence-model.md#a-client-holds-only-the-entities-it-declares)).
That half only the client knows, so it names those entities in
`PullRequest.no_delta_entities` and their rows arrive as full state.

### Locally: edits are deltas in storage too

A live text handle queues the fragment for the one edit it just applied, and
that same fragment is appended to a per-(row, field) **optimistic text log**
(`ot/`) instead of the optimistic overlay row being rewritten around it.

A keystroke therefore writes bytes proportional to the keystroke rather than to
the document — on a 148 KB document it commits under a kilobyte where rewriting
the row cost ~149,000 bytes — and an offline typing burst grows with the text
typed rather than with the document times the keystrokes. Both writes still land
in the one transaction that makes a local edit crash-safe.

Reads fold the log back, so stored rows, rebase replay and the `Changes` feed
all still see whole documents and nothing that reads a row has to know. The log
is derived state with the overlay's lifetime: anything that renumbers the queue
— a rebase, a resequence, a compaction — clears both, and replay rebuilds them
from the queue. It has to, because a log entry is keyed by the mutation id that
wrote it.

Only a pure text PATCH whose fields all carry fragments takes that path — a mask
that also names a scalar, or a whole-document write, writes the row and drops
the log it supersedes. Queue compaction folds a burst's deltas into one mutation
before it is pushed; string-valued text writes still queue whole documents, and
the push path normalizes both to the same cumulative delta against the confirmed
row.

## Caches validated by comparison, not invalidation

Three caches make the above affordable. All three rest on the same argument: the
encoding is **canonical**, so an entry is usable exactly when the bytes it was
built from equal the bytes being used — a memcmp. A hit therefore cannot be
wrong, no producer has to report a change, and nothing needs invalidating.

- **The optimistic fold** is served from a per-row cache validated by comparing
  the row bytes and fragments it was built from. A concurrent tab writing the
  shared store cannot make an entry lie. Reading an unchanged row again costs
  those comparisons instead of the document (~0.065 ms against ~17 ms at 50,000
  code points), and the next keystroke extends the cached document with the
  fragment it appended rather than decoding again.
- **Written documents stay decoded between writes** on the server
  (`server.WithTextCache`, on by default). The write closure decodes the stored
  document, merges, and re-encodes, and the decode is the largest of the three.
  A rejected or retried write leaves a miss rather than a lie. Measured at
  50,000 code points, 8.4 ms of workspace-exclusive time per keystroke becomes
  2.5 ms. `WithTextCache(0)` trades the memory back for the decode.
- **Read documents stay decoded on the client.** Every client path that changes
  a text field already materialises a document — the optimistic fold, an arriving
  pull page folded into the confirmed row — and a moment later a reader rendering
  the field decodes those same bytes back into the same document, so it is
  borrowed from whoever built it. Merging takes a document _out_ rather than
  borrowing it, so nothing can alias one being written to. Measured at 50,000
  code points: a remote keystroke applied to the confirmed row costs 6.5 ms
  instead of 23.7 ms — inside the KV write transaction, which is held exclusively
  across tabs — and rendering the row someone else is typing into costs 8.6 ms
  instead of 50.9 ms. A local keystroke rendered by a watcher other than the
  typist costs 12.6 ms instead of 32.8 ms. Documents under a kilobyte are not
  kept: decoding one is tens of microseconds, and an app rendering hundreds of
  small text fields would otherwise crowd out the document the win is in.

## Writer identities

An item's id is `(peer, counter)` and counters come from the document's own
version vector, so **two documents that can diverge must not share a peer**.

Each open handle leases one (`<client_id>:<slot>`) for as long as it lives and
returns it on `close()`, so two tabs — or two editor bindings in one tab — never
collide, and slots recycle instead of growing the document's peer table.

Recycling is what the ordering around a lease protects. A handle takes its lease
before it reads its row and returns it only once the edits it queued are in the
store, so whoever takes the slot next always builds on state that already covers
what the previous holder wrote — and therefore allocates past it. The same
discipline governs the two writers that are not handles: an undo replay and a
string-valued write each hold a lease across the transaction that persists them.

A slot only means the same thing in every tab because it is a Web Lock. A
browser without a lock manager still shares one IndexedDB across its tabs, so
there each handle takes an unrecycled `<client_id>:u<id>` identity instead:
unique with nothing to coordinate against, at one peer-table entry per document
it writes to.

Server-side writers keep the same invariant and the same bound: a connector
holds one identity for its lifetime (pin `WriterID` to keep it across restarts),
the admin UI one per handler. They may reuse it because they diff against the
stored document read inside the write transaction, which already carries their
earlier writes.

Merge rejects a violation rather than converging on a text nobody typed.

## Marks and anchors

**Marks** (Peritext-style) attach key/value ranges to the characters themselves,
so they move with the text and survive concurrent edits. Resolution is
per-key-per-character by highest op id; removal is an op, not a delete. `expand`
controls whether boundary typing joins the mark.

**Anchors** are stable positions (`(item, before|after)`), encodable to bytes for
storage or presence; a deleted target degrades to the nearest live neighbour.

A mark may also be **collapsed** — `start === end`, covering no character. Both
of its boundaries sit at one caret, and which pair of anchors they are decides
what happens next:

- `ExpandBoth` straddles the caret, so the first character typed there falls
  inside and the mark becomes an ordinary span. That is a **pending format**:
  press Cmd-B, then type. It lives in the document rather than in editor memory,
  so it survives a reload and other peers can see it.
- `ExpandNone` pins both boundaries to the same edge, so the mark stays
  zero-width for the life of the document while drifting with the text around
  it. That is a **note attached to a position** rather than to a span.

Cancelling a pending format needs no new primitive: an unmark at the same caret
resolves to the same anchors and outranks it, both while they cover nothing and
after typing makes them cover something. Since a collapsed mark covers nothing,
it is invisible to the span resolver and read back through two other calls —
`marksAt(index)` for what typed text would take on, `points()` for the
zero-width marks themselves.

Annotations — comments on a range — are built from these two plus an ordinary
entity row, and are a pattern rather than an engine feature. Per-key resolution
means overlapping comments coexist, and resolution costs O(document + marked
text) rather than O(keys × document).

## Both decoders accept exactly the same documents

Varints are capped at 2^53−1 (JavaScript's safe-integer ceiling), strings must
be valid UTF-8, float mark values must be finite, and a mark anchored to an item
the document does not hold is an error rather than a guessed range.

The shared vectors pin the refusals — payload plus the exact message — so the
engine cannot store a document that bricks for the browser only.

## The write boundary takes text, not bytes

Items are code points, so a string that cannot be represented as one is
rejected — invalid UTF-8 on the Go side, an unpaired surrogate on the JS side —
rather than stored with U+FFFD in its place.

A substituted document is well-formed and passes every later check, so the
writer would only discover the loss by reading its own field back; a connector
never would, because its source comparison then never agrees and it rewrites the
row on every pass. Connectors skip such an item (logged, per item, shadow
untouched) so one mis-encoded file does not stall ingress for the rest of the
source, and converge on the first pass after the encoding is fixed.

String writes use a multi-hunk Myers diff over code points, so untouched regions
keep their items and the marks attached to them. Strings further apart than the
shared `maxDiffCost` bound (1024) fall back to one replacement hunk; the bound is
pinned by the shared vectors because it changes which items survive.

## A field can carry text instead of a document

The encoded forms are tagged by their first byte: `0x01` a document, `0x02` a
delta-state fragment, `0x03` a **plain-string value** — the tag followed by the
text's UTF-8 bytes. The third exists because some values have to say what a field
should read as before anyone has decided who is writing it. A field-history
record's before-state is the case that forced it: the server captures what a
field held, and the identity that will put it back belongs to whoever restores,
possibly days later.

Recording the document instead fails twice over. It cannot be restored as
itself — writing an old document back merges it with the stored one, where the
delete markers win, so the restore silently changes nothing — and it costs
several times the text it renders, recorded again for every write.

The tag is what makes the recorded value a **legal value of the field** rather
than a convention two sides have to remember. A record decodes as an ordinary
partial message of its source entity, restores by being PATCHed back under the
record's mask with nothing said about the text field, and `plainTextOf` /
`text.PlainText` read either form for display.

Resolving one is a diff against the stored document, minting the difference
under a writer identity — so it happens where an identity exists: the client's
enqueue path under its leased peer, and `EntityTx.Put`/`Patch` under
`WithTextPeer`.

Everywhere else refuses it by name. The document decoder rejects it the way it
already rejects a fragment, and the merger rejects it on either side. The
server's ingress guard rejects it in any position, masked or not — the wire
carries documents, and a client resolves before it pushes.

Where a value may appear is therefore a matter of **direction, not storage**. A
field-history record carries one at rest, and that record is stored and served
like any other row; what is refused is a plain-string value arriving as the
incoming side of a write. Only a live text field is guaranteed to hold a
document.

## The PUT exception

Every PUT path — `EntityTx.Put` (trusted server-side writes, connectors), client
push PUT, and the client's local overlay — **merges** text fields with the stored
row instead of replacing them. A Put built from a stale snapshot must not drop
edits its writer never saw, and a Put racing an in-flight PATCH must not
duplicate the document. Every other field keeps replace semantics.

The server also validates text bytes on ingress (PUT and PATCH): junk no decoder
could read is rejected as `invalid_argument` instead of bricking the row for
every reader. A PATCH may carry a delta-state fragment in a text field its mask
names outright, validated the same way and unfolded by the merger; a PUT, or a
field the mask only reaches through an ancestor, must carry a whole document,
because nothing would merge it there.

## One document per element

A text field's path may cross a keyed message list or a message-valued map. The
field then holds one document **per element**, and everything that names a
document names it by **instance path** — the mask form of its position, element
key included: `comments.<id>.body`. The instance path is the document's
identity everywhere per-document state lives: the PATCH mask that routes an
edit through the merger, the server's delta log and compaction floors, the
decoded-document caches on both sides, the client's optimistic text log, and
`FieldDelta.field` on the wire.

What makes this sound is that the element key is a **stable identity**. The
merge already descends into a keyed element and looks its mergers up by schema
path (`comments.body` serves every element), so concurrent edits to the same
element's document union exactly as edits to a top-level field do, and edits to
two different elements never touch each other. The same reasoning is why the
other container shapes stay refused: an unkeyed list element has no identity a
path could address, and a field inside a oneof vanishes whenever a sibling case
wins — in both, a masked write replaces the enclosing unit whole and the merger
could never run.

The element's lifecycle is the document's. Removing the element removes its
document: the write gaps the delta log at that position (a reader whose span
covers a removal and a re-creation is served full state rather than a fold that
would resurrect the old text) and clears its compaction floor, so an element
re-created later under the same key starts a fresh document rather than being
measured against the old one's purges. A mask that names a whole element
(`comments.<id>` with no sub-path) replaces it, document included — the same
contract as a mask naming a singular message above a fixed text field — which
is why the client's diff never emits one for a text-bearing element: a changed
element narrows to per-unit paths, so its document goes through the merger and
its other fields keep last-writer-wins.

## Undo is op-level and local-only

A snapshot inverse would merge to a silent no-op on a state CRDT, so handle
edits record **symbolic inverses** — inserted id ranges; deleted content plus
surviving-neighbour anchors; marks with their prior resolved values — that
materialise against the _then-current_ document at undo time. Undo removes
exactly your edits and leaves concurrent peers' work intact.

Typing coalesces into word-sized undo groups (idle timeout plus whitespace
boundary), and selection restore rides the same delta subscription editors
already use.

Undoing a delete restores the text and its formatting but not its item
identities: tombstones are monotone, so the content necessarily comes back under
new ids, and the inverse carries the marks resolved over it at delete time to
re-apply. Formatting and comment ranges therefore survive delete → undo and
repeated undo/redo cycles; anchors held _outside_ the document do not follow the
restored text, so read positions back from the marks rather than keeping your
own anchors across an undo. Two consequences of restoring by re-marking: a
concurrent unmark can be undone along with the delete, and the original `expand`
policy is not recorded, so restored marks come back `ExpandNone`.

## Compaction

A document otherwise keeps one item per character ever typed, and one mark
operation per formatting action ever taken. `server.WithTextCompaction(minPurged)`
runs a background sweep that purges what the document can no longer need and
records the document's version vector as that field's **compaction floor**.

Two kinds of state go. **Tombstones** that are deleted, childless and not
anchored by a surviving mark: a run that was typed and then deleted collapses
entirely, while a tombstone that still positions live text stays, because
sibling order is by id and re-parenting its children would move them in the
text. So compaction bounds a document by the live text **plus the tombstones
that place it**, not by the live text alone.

**Mark operations that can never win again**, which is a narrower test than it
sounds. An op is dead when a higher-id op on the same key has identical anchors
— identical anchors mean identical coverage under every future insert, so the
lower one is shadowed for good — or when it is a removal left with nothing on
its key to remove. "Shadowed right now" would not be safe: two ops covering the
same characters with different expand policies diverge at their boundaries, and
one character typed there reveals the lower one. Nor is "covers nothing" a proxy
for dead, since a point annotation covers nothing by design. A mark and the
unmark that turned it off reclaim as a pair, which is what keeps an abandoned
pending format from costing a permanent op and a pinned tombstone.

Mark ops are dropped first, because an op is often the only thing keeping its
boundary tombstones alive, and reclaiming it is what makes them reclaimable in
the same pass.

Only **quiet** rows are touched — those whose last write is at or below the
sequence the delta log has been pruned through
(`WithTextDeltaRetention`), the point where the server has already stopped being
able to send such a reader a cheap delta. The rewrite lands at a fresh sequence,
so every client re-fetches the field once, and the delta log is gap-marked there
so nobody folds across a purge.

That horizon is also what the sweep is paced by. Each pass takes the window
between where the last one stopped and where the prune has since reached, which
is exactly the set of rows that became quiet in between — so the sweep costs
what has been edited rather than what is stored, and a workspace whose documents
have gone quiet stops being visited. It runs apart from garbage collection for
the same reason it is bounded at all: GC's collectors are deletes over indexed
ranges, while this scans, decodes and rewrites rows, and one transaction holding
both would make every writer wait on the expensive half.

A pass is bounded inside a workspace too, because a text row is as large as the
document in it. One transaction takes at most `batch` rows or a fixed number of
bytes of them, whichever comes first, and commits its rewrites with the mark
recording how far it got — so a page of large documents ends early and the next
transaction resumes where it stopped. And a row is only decoded when the
tombstones and mark ops its stored bytes carry could reach `minPurged`, which
the encoding answers without rebuilding the document: a field nothing has been
deleted from costs almost nothing to skip.

The floor is what makes the purge safe. An incoming write carrying an id the row
lacks, at an id the floor covers, can only be state from before the purge —
items and mark ops share one counter space, so one floor decides both:

- If it arrives as a **tombstone**, or names a purged **mark operation**, the
  write applies with that id skipped, so a replica that has not re-read the row
  keeps working. Without this the union would put every reclaimed op back on the
  next write and the reclaim would never stick.
- If it arrives **live**, or the write hangs new text off one (a character typed
  next to purged text), applying it would put back what the document dropped. It
  is rejected with `text_state_stale` — no sequence consumed, row untouched.

The client recovers by re-reading the row and re-applying the edit's visible
text; a live `TextHandle` does the same on its own the moment it sees the
compacted state. **What that recovery loses is formatting**: mark operations the
stale side applied on top of dropped characters do not survive the re-diff.

This is the one place the engine can drop a user's work, which is why compaction
is off by default. Enable it for documents edited over months; leave it off for
short-lived ones.

## What is out of scope

There is no presence/awareness channel, and formatting is out of scope — marks
are for annotations, so keep rich text in the source format (e.g. Markdown).

A client that does not implement the CRDT cannot set a text field: the wire
refuses a plain-string value in every position, and strings resolve in-process
instead — the TypeScript client's enqueue path under its leased peer, and
`EntityTx.Put`/`Patch` under `WithTextPeer`. A string API in front of the engine
is therefore server-side Go code.

Nothing in the CRDT forces that. A server-minted peer would be sound on the
argument that already lets a connector reuse one: resolution runs inside the
serialized write transaction and observes the stored document's version before
allocating counters. What is missing is an identity the engine is willing to
invent — `client_id` is an unauthenticated idempotency handle — and an ancestor.
The ancestor is the part that cannot be recovered. A three-way diff needs the
document the writer last saw rather than its text, because hunks are positions
and positions have to land on items, and an item carries one deleted flag with
no record of which operation set it. The state a version vector describes cannot
be reconstructed from the current document, so the only sound ancestors are the
ones somebody stored: the client's confirmed row, a connector's shadow.

A stateless caller would therefore get the two-way form `EntityTx.Put`/`Patch`
and ingress-only connectors already take — text the server holds at that moment
and the string does not contain is deleted, while edits arriving afterwards
still merge. That is worth having where a caller names its own identity and
knows it is the source of truth for the field, and not worth having as the
shortest path to writing text. The cost of holding that line falls on field
history, where a recorded value is readable by any client and restorable only by
one that can resolve it.

## Related

- [Use collaborative text](../how-to/use-collaborative-text.md) — the API.
- [Adding collaborative text](../tutorials/adding-collaborative-text.md) — the
  tutorial.
