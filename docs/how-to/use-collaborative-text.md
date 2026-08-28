# Using collaborative text

This guide shows you how to define collaborative text fields in your schema and edit them concurrently across clients. Use this guide when your application needs real-time collaborative text editing with convergent merging and range annotations.

## Prerequisites

Before you begin, ensure you have:

- A Syncplane schema and database client.
- An existing entity row to edit.

## 1. Define the collaborative text field

Add a `bytes` field with `[(syncplane.v1.field_behavior) = { text: true }]` to your protobuf message definition. This field stores a convergent text document where concurrent edits merge without interleaving:

```proto
message Doc {
	option (syncplane.v1.entity) = { name: "doc" key_fields: [ "id" ] };
	string id = 1;
	bytes body = 2 [(syncplane.v1.field_behavior) = { text: true }];
}
```

Registration automatically installs the merge hook on both sides without requiring `WithMerger` or `MergerSpec`.

If you need collaborative text inside repeated elements or maps, place the text field inside a keyed message list or a message-valued map:

```proto
message Doc {
	option (syncplane.v1.entity) = { name: "doc" key_fields: [ "id" ] };
	string id = 1;
	repeated Comment comments = 2
		[(syncplane.v1.field_behavior) = { key_field: [ "id" ] }];
}

message Comment {
	string id = 1;
	string author = 2;
	bytes body = 3 [(syncplane.v1.field_behavior) = { text: true }];
}
```

Each element holds its own document addressed by an instance path, which is the dotted proto path containing the element key (for example, `text(id, 'comments.<commentId>.body')` or `WithTextString("comments.<commentId>.body", s)`). The element lifecycle controls the document: removing the element removes its document. Unkeyed lists cannot contain collaborative text fields.

## 2. Edit text with a live handle

To edit a collaborative text field interactively, open a live handle:

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

Positions use UTF-16 string indices. The handle converts indices to code points internally.

Remote changes arrive as item-graph deltas containing retain, insert, and delete operations. Editors patch in place and transform carets through deltas without jumping. When a delta sets `marksChanged` to `true`, re-read `marks()`.

When you finish editing, call `text.close()` to stop watching and return the leased writer identity. Identities are leased per handle and must never be shared. For more information, see [Collaborative text](../explanation/collaborative-text.md#writer-identities).

### Recovering from handle failures

If a handle fails to decode, merge, or diff state, it stops merging and refuses edits. The handle sets its status to `{ state: 'failed', error }` with code `text_merge_failed` and notifies `onError` listeners.

Reads continue to return the last known value. To recover, close the handle and open a new handle to rebuild state from the stored row.

## 3. Bind text to a textarea

A handle streams changes so that an editor can patch text in place instead of re-rendering from a string. Bind the handle to a `<textarea>` element:

```ts
const area = document.querySelector('textarea')!;
const text = await client.db.scratch.text('notes', 'body');

area.value = text.value;
text.subscribe((delta, value) => {
	area.value = value; // a real editor would apply `delta` in place
});

area.addEventListener('input', () => {
	void text.setString(area.value);
});
```

The `setString` method diffs the string against the current merged document and sends operations. Untouched regions retain their identity, which protects concurrent remote edits from being overwritten by the `<textarea>` contents.

Release the handle when you finish editing. Closing the handle stops the watch, releases the leased writer identity, and drains queued edits:

```ts
await text.close();
```

## 4. Use collaborative text in React

In React applications, bind text fields with the `useText` hook:

```tsx
const { value, setText } = db.scratch.useText('notes', 'body');
<textarea value={ value } onChange={ (e) => setText(e.target.value) } />;
```

In React, `useText` automatically closes the handle on unmount.

The hook opens a handle in the [resolved workspace](react.md#integrating-syncplane-into-your-application). Pass `{ workspaceId }` as the third argument to override the workspace.

The hook re-renders on local and remote deltas. It closes the handle on unmount or when the workspace, row, or field changes. The `marks` array keeps its reference until spans move, and `setText(s)` performs whole-value writes for controlled `<textarea>` components:

```tsx
const { value, marks, handle, error, setText } = db.doc.useText(id, 'body');
```

If the handle fails, `useText` surfaces the failure in `error` while rendering the last value.

## 5. Read and write text without a handle

If you only need to read a text field without watching for live updates, read the bytes directly:

```ts
const rows = await client.db.doc.query({});
rows.map((r) => textOf(r.value.body)); // no handle, no lease
```

Use `textOf(bytes)` to get the visible string and `textMarksOf(bytes)` to get resolved spans. Both functions are memoized per byte instance. In Go, use `text.PlainText`.

To update a text field without opening a live handle, pass a plain string to `mutate.put`, `mutate.patch`, or `create`. The client computes a multi-hunk Myers diff over code points against the current merged row and sends full-state operations:

- Passing an empty string performs a select-all-delete. Concurrent inserts from other clients survive the union.
- Strings with a difference exceeding the `maxDiffCost` bound (1024) fall back to a single replacement hunk.
- The input must be valid code points. The client rejects invalid UTF-8 in Go or unpaired surrogates in JavaScript.

If you write from the server side, pass `WithTextString(field, s)` and `WithTextPeer(peer)` to `EntityTx.Put` or `EntityTx.Patch`. The diff base is the stored document. Any text in the row that is missing from the string is deleted. For more information, see [Write from server-side code](server-side-writes.md#writing-collaborative-text).

## 6. Add annotations to text ranges

To attach comments or annotations to text ranges, store the mark range in the document and the comment metadata in a separate entity:

1. Store the range as a mark keyed `comment:<id>` with value `true` and policy `ExpandNone`.
2. Store the comment body, author, and resolved status in an entity row keyed by that ID.
3. Write the mark `PATCH` and the comment row inside a single `transact()` call to apply them atomically.
4. Join the rows with marks for rendering:

```ts
joinAnnotations(rows, idOf, text.marks());
```

Per-key resolution allows overlapping comments to coexist. Resolution scales at O(document + marked text).

## 7. Apply collapsed marks for pending formats and position notes

Use collapsed marks (where start index equals end index) to set pending formatting or position notes:

```ts
await text.mark(i, i, 'bold', true, ExpandBoth); // pending: the next character typed at i is bold
await text.unmark(i, i, 'bold', ExpandBoth); // changed your mind
await text.mark(i, i, 'note:n1', 'look here', ExpandNone); // a note at a position, never widens
```

Use `ExpandBoth` for pending formats at a caret position. Use `ExpandNone` for zero-width annotations that drift with surrounding text without covering characters. One-sided expand policies are not supported for collapsed marks.

To read collapsed marks, call `marksAt(index)` for active formatting at a caret or `points()` for zero-width marks and their positions. Collapsed marks do not appear in `marks()`.

## Confirming the result

To verify that collaborative text editing works:

1. Open handles from two client sessions on the same row.
2. Type into both tabs or client sessions at the same time and verify that both streams of characters appear in a stable order and merge without interleaving.
3. Apply a mark with `text.mark()` and verify that `text.marks()` reports the updated spans after concurrent edits.

## Related

- [The text CRDT](../explanation/collaborative-text.md)
