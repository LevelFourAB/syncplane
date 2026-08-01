# Sync an external store (connectors)

The `connect` package layers the engine over a store it does not own — an
existing database, a folder of files — and keeps the two converged
bidirectionally.

The external store stays the source of truth for its own tooling; the engine
holds a sync-optimized mirror and contributes ordering, per-field conflict
resolution, delta sync and offline clients. External edits enter through
`Mutate` on a `server.Trusted` context — a connector has no caller to authorize,
so it says so rather than being denied as anonymous; committed engine changes
flow back out through the change feed.

## Implement one port

```go
type Source interface {
    List(ctx, workspaceID, fn func(connect.Item) error) error   // scan everything
}

// Implement this too unless every entity is bound IngressOnly.
type Writer interface {
    Write(ctx, workspaceID, msg) (fingerprint string, err error) // idempotent upsert
    Delete(ctx, workspaceID, prototype, key string) error        // missing = no-op
}
```

## Wire it up

```go
state, _ := sqlitestate.Open("connector-state.db") // bookkeeping, reconstructible
conn, _ := connect.New(connect.Config{
    Server:   srv,
    Source:   mySource,
    State:    state,
    Entities: []proto.Message{&todov1.Todo{}},
})
conn.Start(ctx)        // drains on commit, syncs on an interval
conn.Kick(workspaceID) // wire fsnotify / DB triggers / webhooks here
```

That is the whole setup for a small, local, uniformly reachable store. The
sections after "How conflicts resolve" are for the ones that are not.

## How conflicts resolve

A per-row shadow (fingerprint + engine version + last-agreed bytes) breaks echo
loops in both directions and serves as the three-way base, so a source edit to
one field merges with a concurrent client edit to another instead of clobbering
it. Conflicts resolve by arrival order at the workspace sequence — exactly as
between two clients.

Everything is at-least-once with idempotent repair: losing the state database,
crashing mid-egress, or sleeping past GC retention costs a reconcile pass, never
data.

## Collaborative text in a connector

Connectors report text as plain strings (`Item.TextFields`). The connector diffs
against the shadow base and unions with the engine's current document — via the
same `server.ApplyTextStrings` three-way that backs `WithTextString` — so an
external file edit and a concurrent client edit both survive.

Two constraints:

- A connector holds **one writer identity for its lifetime**. Pin `WriterID` to
  keep it across restarts.
- Text at the write boundary is **text, not bytes**: a string that cannot be
  represented as code points is skipped (logged, per item, shadow untouched) so
  one mis-encoded file does not stall ingress for the rest of the source. It
  converges on the first pass after its encoding is fixed.

## Make ingestion incremental

`List` enumerates the whole store, and on the default interval that is once a
minute per workspace. Against a mailbox or a calendar that is unaffordable —
and unnecessary, because those stores have a change cursor of their own (Gmail
`historyId`, IMAP `HIGHESTMODSEQ`, CalDAV sync-token). Implement
`connect.Incremental` and the connector rides it instead:

```go
func (s *mySource) Checkpoint(ctx context.Context, ws string) (string, error) {
    // The source's position NOW. Taken before a full List, so a change racing
    // that list is redelivered rather than skipped.
    return s.api.CurrentHistoryID(ctx, ws)
}

func (s *mySource) Changes(ctx context.Context, ws, cursor string) (connect.Delta, error) {
    page, err := s.api.History(ctx, ws, cursor)
    if errors.Is(err, api.ErrHistoryExpired) {
        // Forces a fresh Checkpoint and a full List, inside the same pass.
        return connect.Delta{}, fmt.Errorf("history expired: %w", connect.ErrCursorInvalid)
    }
    if err != nil {
        return connect.Delta{}, err
    }
    return connect.Delta{
        Items:      page.Items,      // decoded as for List
        Deletions:  page.Deletions,  // applied after Items
        NextCursor: page.NextID,
        More:       page.HasMore,    // loop now; do not wait for the tick
    }, nil
}
```

`Connector.Ingress` then decides per pass: full enumeration when there is no
cursor, when the source rejects the one it has, or when
`FullReconcileInterval` (default 1h) is due — and `Changes` otherwise. Set
`ReconcileInterval` to how often you want the feed polled and
`FullReconcileInterval` to how often you want the safety net; a negative one
means "only when the cursor breaks".

Two consequences worth knowing:

- The cursor is persisted **after** its page is applied, so a crash replays
  that page. Redelivery is a no-op — an unchanged fingerprint short-circuits,
  and a deletion checks the row before writing a tombstone — but your `Write`
  must stay idempotent for the same reason.
- The deletion **sweep** ("a shadow exists but the source did not list it")
  belongs to the full pass. An incremental pass only knows what `Deletions`
  told it, so a source that deletes silently needs the safety net left on.

## Give each workspace its own credentials

`Config.Source` is one instance for every workspace. A per-user mailbox needs
one per workspace instead:

```go
conn, _ := connect.New(connect.Config{
    Server: srv,
    OpenSource: func(ctx context.Context, ws string) (connect.Source, error) {
        tok, err := tokens.For(ctx, ws)
        if errors.Is(err, tokens.ErrRevoked) {
            return nil, fmt.Errorf("reconnect required: %w", connect.ErrNeedsUserAction)
        }
        if err != nil {
            return nil, err
        }
        return newMailSource(tok), nil
    },
    State:    state,
    Entities: []proto.Message{&mailv1.Message{}},
})
```

The source is built on the workspace's first pass and reused after that, so an
OAuth handshake is not re-run every minute. Implement `io.Closer` on it to
release a connection when the connector is done with it. It is dropped, and
rebuilt on the next pass, in three cases: `Close`, `Forget(workspaceID)`, and
any pass failing with an error that wraps `connect.ErrNeedsUserAction` — which
is the seam a re-consent flow hangs off. Store the new token, and the next
pass picks it up without restarting the process.

`Forget` is also the teardown call for "the user disconnected this account":
it drops the workspace and releases the source, leaving shadows and cursors in
place so reconnecting resumes instead of re-importing. Exclude the workspace
from `Config.Workspaces` to keep it from being rediscovered — the sweep re-reads
that list and drops what it no longer names, so for a workspace that also
disappears from the engine, excluding it is all that is needed.

## Read-only sources

A source the app never writes back to declares its entities `IngressOnly` and
may then omit `Writer` entirely:

```go
Bindings: []connect.Binding{
    {Prototype: &mailv1.Message{}, Direction: connect.IngressOnly},
},
```

Every egress path skips those entities. The three-way merge narrows to match:
a field the source did not change is left alone (so a client-side flag on the
row survives), and a field it did change is applied over whatever the engine
holds. Collaborative text follows the same rule — unchanged means untouched,
changed means the source wins, applied as a minimal diff onto the live
document.

## Surface health, and back off

Loop failures are logged, never returned: convergence work must not take the
process down. That is the wrong amount of silence for "this user's token
expired two days ago", so every pass also records:

```go
h, ok := conn.Health(workspaceID)
// h.LastSuccess, h.LastError, h.ConsecutiveFailures, h.NextAttempt
// h.Kind: FailureNone | FailureTransient | FailureUserAction

conn.HealthAll() // every workspace, ordered
```

Set `Config.OnHealth` to persist or alert on it; it fires after every pass,
including passes you drive by hand.

Classification is the source's job, and there is exactly one distinction to
make: wrap `connect.ErrNeedsUserAction` when retrying cannot help. Everything
else is transient and is retried with exponential backoff between
`Config.RetryMin` and `RetryMax`. When the API tells you how long to wait, say
so and the connector obeys it verbatim:

```go
return connect.RetryAfter(fmt.Errorf("rate limited: %w", err), resetIn)
```

While a workspace is backing off, commit signals, kicks and ticks fold into
one deferred pass instead of hammering a rate-limited API. Direct calls to
`Ingress`, `Reconcile` and `Drain` always attempt — the caller asked.

## Keep a cold start off the interactive path

The first sync of a large account is one long run of writes into a single
workspace, each taking that workspace's write lock. Pace it:

```go
IngestChunk: 200,                   // after every 200 engine writes…
IngestPause: 50 * time.Millisecond, // …yield for this long
```

Only actual writes count; items that turn out unchanged are free.

## A worked example

`cmd/mdtodo` is a complete connector syncing the todo app with a folder
of Markdown files, including `Note` rows as Markdown documents.

## Related

- [Connectors reference](../reference/connectors.md) — every interface,
  `Config` field, sentinel and health value.
- [Follow committed changes](change-feed.md) — the egress half, on its own.
- [Collaborative text](../explanation/collaborative-text.md) — why writer
  identities may never be shared.
