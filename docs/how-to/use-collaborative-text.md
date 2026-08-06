# Use collaborative text

A `bytes` field marked `text: true` holds a convergent text document instead of
raw bytes: concurrent edits merge without interleaving, and formatting or
comment ranges travel with the characters they cover.

```proto
message Doc {
	option (syncplane.v1.entity) = { name: "doc" key_fields: [ "id" ] };
	string id = 1;
	bytes body = 2 [(syncplane.v1.field_behavior) = { text: true }];
}
```

Nothing else to wire: registration auto-installs the merge hook on both sides
(no `WithMerger` / `MergerSpec`).

## Edit through a live handle

```ts
const text = await client.db.doc.text(id, 'body'); // row must exist
text.value; // current string
await text.insert(5, ', world'); // ships a PATCH with the new state
await text.delete(0, 2);
await text.setString(whole); // multi-hunk-diff whole-value write
await text.mark(0, 5, 'comment:c1', true); // ExpandNone by default
text.marks(); // resolved spans
text.marksAt(5); // what text typed at a caret would be marked with
text.points(); // resolved zero-width marks
const a = text.anchorAt(3); // survives remote edits
text.resolve(a);
text.subscribe((delta, value) => editor.applyDelta(delta)); // in-place diffs
await text.close(); // stops watching, returns the peer, drains queued PATCHes
```

Positions are UTF-16 string indices; conversion to code points happens inside.

Remote changes arrive as item-graph deltas (retain/insert/delete), so an editor
patches in place and carets transform through the delta instead of jumping. A
delta's `marksChanged` says whether the binding must re-read `marks()`: spans are
resolved against the item order rather than stored, so it is set both when mark
ops arrive and when an edit could have moved a span. A marked document
re-resolves on ordinary typing; an unmarked one never does.

### Close handles you are done with

`close()` stops the watch and returns the writer identity the handle leased
(identities are leased per handle and must never be shared — see
[Collaborative text](../explanation/collaborative-text.md#writer-identities)).
It returns a promise that resolves once the PATCHes queued before it have been
enqueued; awaiting is optional and it never rejects.

## Read without a handle

Rendering a text field you merely read needs no handle — no peer lease, no
watch:

```ts
const rows = await client.db.doc.query({});
rows.map((r) => textOf(r.value.body)); // no handle, no lease
```

`textOf(bytes)` is the visible string and `textMarksOf(bytes)` its resolved
spans, both memoized per byte instance. `text.PlainText` is the Go equivalent.

## Write a plain string

`mutate.put` / `mutate.patch` (and `create`) accept a plain string for a text
field. The client diffs it against the current merged row under a leased writer
identity — a multi-hunk Myers diff over code points, so untouched regions keep
their items and the marks attached to them — and ships ordinary full-state
operations.

- An **empty string is select-all-delete**: concurrent inserts survive the union.
- Strings further apart than the shared `maxDiffCost` bound (1024) fall back to
  one replacement hunk.
- The boundary takes **text, not bytes**: a string that cannot be represented as
  code points is rejected — invalid UTF-8 on the Go side, an unpaired surrogate
  on the JS side — rather than stored with U+FFFD in its place.

Server-side, `EntityTx.Put` / `Patch` take `WithTextString(field, s)` plus
`WithTextPeer(peer)`. The diff base there is the **stored** document rather than
a state the writer saw earlier, so text the row already holds and the string does
not contain is deleted — see
[Write from server-side code](server-side-writes.md#collaborative-text).

## In React

```tsx
const { value, marks, handle, error, setText } = db.doc.useText(id, 'body');
```

It opens a handle in the hook's [resolved workspace](react.md#choosing-a-workspace)
(add `{ workspaceId }` as a third argument to override it), re-renders on every
local and remote delta, and closes the handle on unmount or when the
workspace/row/field identity changes (the previous row's text is never shown
under the new key). `marks` keeps its array reference until
a delta reports the spans may have moved, and `setText(s)` is the whole-value
write a controlled `<textarea>` needs.

## Annotations (comments on a range)

Annotations are a pattern, not an engine feature. The **range** lives inside the
document as a mark keyed `comment:<id>` (value `true`, `ExpandNone`) so it
follows the text; the **body, author and resolved state** live in an ordinary
entity row keyed by that id, getting last-writer-wins merge, permissions,
queries and CRUD undo for free.

Write the mark PATCH and the row in one `transact()` so they land atomically and
undo as one step, and join them for rendering:

```ts
joinAnnotations(rows, idOf, text.marks());
```

Per-key resolution means overlapping comments coexist, and resolution costs
O(document + marked text) rather than O(keys × document), so a document carrying
hundreds of comments resolves in about the same time as one carrying a single
mark. The todo demo's notes panel is the worked example.

## Undo

Undo is op-level and local-only: it removes exactly your edits and leaves
concurrent peers' work intact. Typing coalesces into word-sized undo groups
(idle timeout plus whitespace boundary), and selection restore rides the same
delta subscription editors already use.

Undoing a **delete** restores the text and its formatting but not its item
identities. Two consequences: anchors you held _outside_ the document — a cached
cursor, a saved position — do not follow the restored text, so read positions
back from the marks rather than keeping your own anchors across an undo; and
restored marks come back `ExpandNone`, because the original `expand` policy is
not recorded.

## When a handle gives up

Decoding, merging and diffing all run on state the handle did not produce, and
each refuses input it cannot reconcile. When that happens the handle **stops
merging and refuses edits**, and reports itself on `status()`
(`{ state: 'failed', error }`, code `text_merge_failed`) and to `onError`
listeners, plus `console.error` for the app that registered neither.

Reads keep working, so what the user was looking at can be salvaged, but it is
stale from then on. **Recovery is to close the handle and open a new one**,
which rebuilds from the stored row. `useText` surfaces the same condition as its
`error` while still rendering the last value it had.

## Pending formats and notes on a position

`mark(i, i, …)` — start equal to end — is a **collapsed** mark, covering no
character:

```ts
await text.mark(i, i, 'bold', true, ExpandBoth); // pending: the next character typed at i is bold
await text.unmark(i, i, 'bold', ExpandBoth); // changed your mind
await text.mark(i, i, 'note:n1', 'look here', ExpandNone); // a note at a position, never widens
```

`ExpandBoth` gives the pending-format state a rich-text editor needs between
Cmd-B and the first keystroke — kept in the document, so it survives a reload
and other peers can see it. `ExpandNone` gives a zero-width annotation that
drifts with the surrounding text without ever covering any of it. The one-sided
policies are rejected here: there is no side to expand on.

Neither shows up in `marks()`, which reports spans over characters that exist.
Read them back with `marksAt(index)` — the active formatting for a caret,
including ordinary marks whose expand policy would take in text typed there —
and `points()`, the zero-width marks with their current positions.

## Bounding growth on the server

A document otherwise keeps one item per character ever typed and one mark
operation per formatting action. `server.WithTextCompaction(minPurged)` makes
the GC pass purge the tombstones and mark operations a document can no longer
need — including a pending format and the unmark that cancelled it, which
reclaim as a pair.

It is **off by default because it is the one place the engine can drop a user's
work**: a client editing on top of state the purge dropped is asked to redo its
edit, and mark operations it applied over dropped characters do not survive the
re-diff. Enable it for documents edited over months; leave it off for
short-lived ones. [Collaborative text](../explanation/collaborative-text.md#compaction) explains the mechanism.

## What is out of scope

- There is no presence/awareness channel.
- Formatting is out of scope — marks are for annotations; keep rich text in the
  source format (e.g. Markdown).
