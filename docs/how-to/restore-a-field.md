# Restore a field to a previous value

Someone overwrites a description by mistake and it has to come back. Opt the
entity into field history, read the log when a history panel opens, and put the
old value back with an ordinary `patch`.

## Restore is a PATCH, and that matters

The restore is a **`patch` with the recorded mask**, never a `put` of the
decoded prior value. The recorded prior carries only the fields the mask names
(plus the row's key fields), and a `put` replaces whole rows — so it would blank
every field the record never recorded.

A collaborative-text field needs no handling of its own. A record holds its
**text** rather than its document, in a form the field itself can carry, and the
write path resolves it: the text is diffed against the stored document and the
difference applied as **real edits**. The characters come back as new
insertions, untouched regions keep their items, and a peer typing concurrently
merges instead of losing their work. See
[Collaborative text](../explanation/collaborative-text.md) for why the text is
what gets recorded.

Marks do not survive that round trip. The restored span is re-inserted text, so
formatting that covered it is gone even though formatting on untouched regions
stays. Deep history restores a value, not a rendering.

The restore is itself an ordinary optimistic mutation — offline-queueable,
undoable, and itself captured, so undoing a restore is another restore.

## 1. Opt the entity in

```proto
message Issue {
	option (syncplane.v1.entity) = {
		name: "issue"
		key_fields: [ "id" ]
		history: {}
	};
	string id = 1;
	string title = 2 [(syncplane.v1.field_behavior) = { history: true }];
	bytes description = 3 [(syncplane.v1.field_behavior) = { text: true history: true }];
	int64 sort_key = 4; // no mark, so reordering never touches the log
}
```

Capture is **explicit**: the fields you mark are recorded and nothing else, and
an entity that opts in without marking a field is rejected at registration.
Leave a field like `sort_key` unmarked and a drag never touches the log.

Nothing else is needed on the server. The engine registers the history entity
(`syncplane.v1.FieldHistory`, wire name `syncplane_field_history`) the first time
some entity opts in, and captures on every write path: a client push, a mutator,
a connector, a trusted write. See
[Proto options → Field history](../reference/proto-options.md#field-history) for
what a record contains and
[Go server API → Field history](../reference/server.md#field-history) for the
retention and admission-hook options.

## 2. Register the history entity on the client

History never syncs — it is a `server_only` entity — so it is read on demand
through `queryServer`, which resolves the entity by name from the client's
registry. Add the schema there; it costs a name and nothing else.

```ts
import { FieldHistorySchema } from 'syncplane/gen/syncplane/v1/history_pb.js';

const client = new SyncClient({
	entities: [ IssueSchema, FieldHistorySchema ],
	// …
});
```

Registering the entity does not sync it: it holds no local rows, and adding it to
a build costs no re-bootstrap. Keep it out of any subscription set — naming an
entity that never syncs throws — which is worth a check if the app builds that
set from its entity list.

## 3. Read the log when the panel opens

```ts
const log = await client.queryServer(FieldHistorySchema, {
	where: (h) => and(h.sourceEntity.eq('issue'), h.sourceKey.eq(issueId)),
	orderBy: (h) => [ h.seq.desc() ],
	limit: 50,
});
```

`queryServer` needs connectivity: this tier is deep history, not a synced
previous-value cache, so a restore is unavailable offline.

**Permissions come along for free.** A record is served under the read filter of
the row it describes — a caller who cannot read `issue/i-42` gets nothing back
for it, prior values, masks and actors included. There is nothing to register:
the rule is the engine's, not a filter you write for the history entity. Tighten
the source entity's filter and the log follows — deny someone a row and its
history goes with it.

Each row renders straightforwardly: `recordedAt` for the timestamp,
`actorSubject` for who (and `actorSystem` when the engine or a connector wrote
it rather than a person), `mask.paths` for which fields — the
server records changes rather than writes, so a mask names the fields that
actually moved and an entry never says "nothing happened here".

**An entry is an editing session, not a write.** A run of writes to one row by
one actor folds into the record the first of them opened, so `prior` is what the
fields held before that whole run and `seq`…`throughSeq` is the span it covers;
`writes` says how many folded in. Render `writes` when it is worth showing ("12
changes"), and expect one entry per session rather than per write — the server
keeps the log proportional to editing sessions rather than to keystrokes. The
window is a server option,
[`WithHistorySession`](../reference/server.md#field-history).

## 4. Restore

Decode `prior` as the source entity type and hand `patch` the recorded mask.
That is the whole thing, text field or not:

```ts
import { fromBinary } from '@bufbuild/protobuf';

const rec = log[0]!.value;
const prior = fromBinary(IssueSchema, rec.prior);
await client.mutate.patch(IssueSchema, issueId, prior, {
	paths: rec.mask!.paths,
});
```

From Go it is `tx.Patch(prior, rec.GetMask().GetPaths(), nil)`, plus
`server.WithTextPeer(…)` when the entity has a text field: that names the
identity the restored edits are minted under, and the engine invents none.

Restoring a **text** field needs an implementation that can resolve the recorded
value into edits, which the TypeScript client and server-side Go both are. The
wire refuses the value in every position, so a third-party client speaking the
RPCs directly can restore a record's other fields — clearing the text field and
dropping its path from the mask — and has to leave that one to a server-side
handler ([Write from server-side code](server-side-writes.md#collaborative-text)).

To **display** a recorded value rather than restore it, read text fields with
`plainTextOf` (Go: `text.PlainText`), which takes either form. The document
decoder refuses a recorded value by name rather than returning something wrong.

## 5. Show before and after, not only before

A record holds the value a field had **before** its write. The value it changed
_to_ is the next newer recorded value of that field, or the live row when no
newer record names it — so one descending query plus the current row is enough
to render a full timeline. Walk newest to oldest, folding each record's `prior`
back over a cursor:

```ts
import { mergeMask } from 'syncplane';

const log = await client.queryServer(FieldHistorySchema, logOf(issueId));

// The cursor starts at the row as it is now and steps back one write per entry.
let after = (await client.get(IssueSchema, issueId))!;
const timeline = log.map(({ value: rec }) => {
	const before = fromBinary(IssueSchema, rec.prior);
	const paths = rec.mask?.paths ?? [];
	const entry = {
		seq: rec.seq,
		at: rec.recordedAt,
		by: rec.actorSubject,
		paths,
		before,
		after,
	};
	after = { ...after };
	// The same merge the server applied, so nested and keyed paths land the
	// same way here as they did there.
	mergeMask(IssueSchema, after, before, paths);
	return entry;
});
```

Each entry now has both sides for every field its mask names: `entry.before` for
what the field held, `entry.after` for what the write made of it. Diff the two
under `paths` and you have the highlight.

Read text on both sides with `plainTextOf`, which takes the recorded form and
the live row's document alike — the fold puts recorded values into the cursor,
so a text field's `after` is a document only for the newest entry.

One thing the fold does not do for you: **the oldest entry is not necessarily
the row's creation.** It is only that when `rowCreated` is set. Otherwise
retention or your page limit truncated the log, and the entry to render is
"nothing recorded before this".

## Edge cases worth handling in the UI

- **`rowCreated`** means the row did not exist before that write — the record
  marks its creation. Its `mask` and `prior` are empty and restoring from it
  patches nothing; render it as "created" rather than as a restorable state.
  Test the flag, not the mask: a [history filter](../reference/server.md#the-admission-hook)
  that redacts every path leaves an empty mask on a record that is an ordinary
  change.
- **A `DELETE` is not captured, and it purges the row's log.** A row's
  disappearance is not a field change, nothing can restore into a tombstone, and
  a row re-created under the same key starts clean rather than inheriting the
  previous one's history. This tier restores fields, not rows — to make a
  removal recoverable and attributable, model it as a field instead
  ([Recover a deleted row](recover-a-deleted-row.md)).
- **Key fields are never in the mask**, so a restore can never move a row's
  identity. `prior` still carries them, which is how a decoded record says which
  row it belongs to.
- **Records expire.** The default retention is 90 days
  (`WithHistoryRetention`); a panel that pages deep enough will run out. To tie
  a record's lifetime to its row's alone — kept until the row is deleted, however
  old — configure `server.WithHistoryRetention(server.HistoryForever)`.
