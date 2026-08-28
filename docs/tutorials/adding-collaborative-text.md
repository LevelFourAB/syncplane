# Adding collaborative text

Per-field last-writer-wins causes data loss when multiple users edit the same
text at the same time. In this tutorial, you add a collaborative text field to a
schema, bind it to an input element, and edit it concurrently from two clients.
By the end of this tutorial, you can create collaborative text fields and merge
concurrent edits across clients.

## Prerequisites

Before you start this tutorial, ensure you have:

- Completed [Your first sync app](your-first-sync-app.md).
- Running instances of the demo server and web app.

## 1. Declare a text field

A collaborative text field is a singular `bytes` field with one annotation. Add
an entity to `proto/examples/example/todo/v1/todo.proto`:

```proto
message Scratch {
	option (syncplane.v1.entity) = {
		name: "scratch"
		key_fields: [ "id" ]
	};

	string id = 1;
	bytes body = 2 [(syncplane.v1.field_behavior) = { text: true }];
}
```

The field holds an encoded convergent text document rather than a UTF-8
string. Both the Go implementation in `engine/text` and the TypeScript
implementation in `client/src/text` produce byte-identical encodings, pinned by
shared vectors in `testdata/text/`.

Regenerate the code and declare the entity:

```sh
task generate
```

Register the entity in the Go server in `internal/exampleapp`:

```go
server.WithEntities(/* ... */ &todov1.Scratch{})   // internal/exampleapp
```

Register the entity schema in TypeScript in `examples/todo/src/main.ts`:

```ts
entities: [ /* ... */ ScratchSchema ]; // examples/todo/src/main.ts
```

Registration installs the merge hook on both sides. You do not need to declare
a merger.

## 2. Edit through a handle

A row must exist before you can open a handle on one of its text fields. Create
a row and open a handle:

```ts
await client.db.scratch.mutate.put({ id: 'notes' });

const text = await client.db.scratch.text('notes', 'body');
text.value; // '' — the current string
await text.insert(0, 'hello');
await text.insert(5, ', world');
text.value; // 'hello, world'
```

Positions are UTF-16 string indices, the same indices used by
`String.prototype.slice`. The handle converts indices to code points
internally.

Open the app in two browser tabs and run the script in the developer console of
both tabs, typing into the same position. Neither edit is lost and characters do
not interleave. You see one insertion followed by the other. This ordering
guarantee comes from [Fugue](https://arxiv.org/abs/2305.00583).

## 3. Bind it to a textarea

A handle streams changes so that an editor can patch text in place instead of
re-rendering from a string. Bind the handle to a `<textarea>` element:

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

The `setString` method diffs the string against the current merged document and
sends operations. Untouched regions retain their identity, which protects
concurrent remote edits from being overwritten by the `<textarea>` contents.

Type into both tabs at the same time. Both streams of characters appear in a
stable order in both tabs.

Release the handle when you finish editing. Closing the handle stops the watch,
releases the leased writer identity, and drains queued edits:

```ts
await text.close();
```

In React, the `useText` hook does this for you. See
[Use collaborative text](../how-to/use-collaborative-text.md#4-use-collaborative-text-in-react).

## 4. Read without a handle

Rendering text that you do not edit requires no handle, lease, or watch. Query
the rows directly:

```ts
const rows = await client.db.scratch.query({});
rows.map((r) => textOf(r.value.body)); // the visible string
```

Use a handle when you edit text. Use `textOf` when you only read text.

## 5. Attach a comment to a range

Marks are key-value ranges attached to characters. Marks move with the text
during edits. Add a mark to a range:

```ts
await text.mark(0, 5, 'comment:c1', true);
text.marks(); // resolved spans, re-resolved after every edit
```

The range lives inside the document as a mark keyed by `comment:<id>`. The
comment body, author, and resolved status live in a standard entity row keyed
by that ID. This entity structure provides last-writer-wins merging,
permissions, queries, and undo support.

To make edits land atomically and undo in a single step, write both operations
in one `client.transact()` call.

Open the notes panel in the demo app and add a comment. The comment range
follows the text as you type in front of it.

## What you can rely on

- **Concurrent insertions never interleave**, whether they happen a millisecond
  or a week apart.
- **Offline splits converge on reconnect.** Replication is state-based, so there
  is no causal-delivery requirement — a client can be gone for days.
- **Merging is a pure union** — idempotent, commutative, associative — which is
  why rebase replay and repeated pulls are harmless.
- **Typing costs bytes proportional to the edit**, not to the document, in both
  directions and in local storage.

## Where to go next

You now have a collaborative text field that merges concurrent edits across
multiple clients.

To learn more about collaborative text, see the following guides:

- [Use collaborative text](../how-to/use-collaborative-text.md) — Learn about
  anchors, `expand` policies, undo, and error handling.
- [Collaborative text](../explanation/collaborative-text.md) — Learn how the
  CRDT works, why deltas are safe, and compaction behavior.
