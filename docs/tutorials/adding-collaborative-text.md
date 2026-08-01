# Adding collaborative text

Per-field last-writer-wins is the wrong tool for prose: if two people edit the
same description at once, one of them loses the whole field. In this tutorial
you will add a field that two people can type into simultaneously, watch both
sets of keystrokes survive, and see what makes that possible.

It continues from [Your first sync app](your-first-sync-app.md) — you should
have the demo server and web app running.

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

The field holds an encoded convergent text document rather than a UTF-8 string.
Both implementations — Go in `engine/text`, TypeScript in `client/src/text` —
produce byte-identical encodings, pinned by shared vectors in `testdata/text/`.

Regenerate and declare it exactly as you did for `Tag`:

```sh
task generate
```

```go
server.WithEntities(/* ... */ &todov1.Scratch{})   // internal/exampleapp
```

```ts
entities: [ /* ... */ ScratchSchema ]; // examples/todo/src/main.ts
```

There is nothing else to wire. Registration installs the merge hook on both
sides; you do not declare a merger.

## 2. Edit through a handle

A row has to exist before you can open a handle on one of its text fields:

```ts
await client.db.scratch.mutate.put({ id: 'notes' });

const text = await client.db.scratch.text('notes', 'body');
text.value; // '' — the current string
await text.insert(0, 'hello');
await text.insert(5, ', world');
text.value; // 'hello, world'
```

Positions are UTF-16 string indices, the same ones `String.prototype.slice`
uses; the conversion to code points happens inside the handle.

Open the app in two tabs and run that in both consoles, typing into the _same_
position. Neither edit is lost and the characters do not interleave: you get
one insertion then the other, never `hbeollolo`. That ordering guarantee is
[Fugue](https://arxiv.org/abs/2305.00583).

## 3. Bind it to a textarea

A handle streams changes, so an editor can patch in place instead of being
re-rendered from a string:

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

`setString` diffs the string you hand it against the current merged document —
a multi-hunk diff, so untouched regions keep their identity — and ships ordinary
operations. That is what makes it safe for a controlled `<textarea>`: a
concurrent remote edit survives instead of being clobbered by whatever string
the textarea happened to hold.

Now type into both tabs at once. Both streams of characters land, in a stable
order, in both tabs.

In React the same binding is one hook:

```tsx
const { value, setText } = db.scratch.useText('notes', 'body');
<textarea value={ value } onChange={ (e) => setText(e.target.value) } />;
```

Remember to release the handle when you are done with it — `await text.close()`
stops the watch, returns the writer identity it leased, and drains the edits
still queued behind it. `useText` does this for you on unmount.

## 4. Read without a handle

Rendering text you are not editing needs no handle, no lease and no watch:

```ts
const rows = await client.db.scratch.query({});
rows.map((r) => textOf(r.value.body)); // the visible string
```

Use a handle when the user is editing; use `textOf` everywhere else.

## 5. Attach a comment to a range

Marks are key/value ranges attached to the characters themselves, so they move
with the text as it is edited:

```ts
await text.mark(0, 5, 'comment:c1', true);
text.marks(); // resolved spans, re-resolved after every edit
```

Annotations in the demo are built on exactly this: the _range_ lives inside the
document as a mark keyed `comment:<id>`, while the comment's body, author and
resolved flag live in an ordinary entity row keyed by that id — so the comment
text gets last-writer-wins merge, permissions, queries and undo for free. Write
both in one `client.transact()` so they land atomically and undo as one step.

The demo's notes panel is the worked example; open it and add a comment to see
the range follow the text as you type in front of it.

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

- [Use collaborative text](../how-to/use-collaborative-text.md) — the full
  working surface: anchors, `expand` policies, undo, error handling.
- [Collaborative text](../explanation/collaborative-text.md) — how the CRDT
  works, why deltas are safe, and what compaction can cost you.
