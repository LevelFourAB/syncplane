# Write from server-side code

`Apply` writes from your own Go code — an HTTP handler, an agent, a cron job —
through the same storage path clients use. The closure reads from a snapshot and
records the writes it wants; the engine authorizes them and applies them as one
atomic mutation.

```go
err := srv.Apply(ctx, workspaceID, func(tx *server.ApplyTx) error {
    var todo todov1.Todo
    ok, version, err := tx.Get(&todo, "t1")
    if err != nil || !ok {
        return err
    }
    todo.Completed = true
    return tx.Patch(&todo, []string{"completed"}, &version)
})

err = srv.Invoke(ctx, workspaceID, "complete_all", &todov1.CompleteAllArgs{})
```

All writes in one call share a single workspace sequence (allocated lazily — a
call that records nothing costs nothing), connected clients are notified on
commit, and `server.Reject(...)` aborts with a typed error.

Three things to keep in mind:

- There is **no client watermark** involved, so calling `Apply` twice applies
  twice. If the caller can retry, use `ApplyIdempotent` below.
- Writes are **authorized against the ctx principal** — see
  [Acting as a caller](#acting-as-a-caller).
- Reads come from the snapshot taken **before** the closure ran, so they do not
  see the closure's own writes. `Get` on a key you have already written returns
  an error rather than a stale row.

For large entities, page with `ApplyTx.ScanPage(prototype, afterKey, limit, fn)`,
which returns the last visited key and whether more rows follow.

## Acting as a caller

`Apply`, `Mutate`, `Invoke` and `View` run the same permission hooks the sync
surface does, against the principal on the context. Put the identity you
authenticated on the ctx and the engine holds your handler to the policy your
clients are held to:

```go
ctx = server.WithPrincipal(ctx, server.Principal{Subject: userID})
err := srv.Apply(ctx, workspaceID, func(tx *server.ApplyTx) error { … })
```

A ctx with no principal authorizes as **anonymous**, which a deny-by-default
policy refuses — so forgetting to authenticate fails loudly rather than writing
unchecked.

Code with no caller to authorize — a connector, a migration, a nightly job —
says so explicitly:

```go
err := srv.Mutate(server.Trusted(ctx), workspaceID, func(tx *server.EntityTx) error { … })
```

`Trusted` skips both hooks for that one call. Forgetting it is safe: the call
authorizes as anonymous and a real policy denies it, so a job fails on its first
pass instead of writing unauthorized.

`server.TrustedAs(ctx, "connect:markdown")` is the same bypass with a name
attached. The name is recorded on every row the call writes and shows up on the
[change feed](change-feed.md#who-wrote-it) as the write's actor, so a consumer
can skip its own writes instead of reacting to them. Prefer it for anything that
writes on a schedule or in a loop; nothing verifies the name, and no policy runs
against it. A bare `Trusted` records the name `"system"`.

Both bypasses compose with a principal rather than replacing it: a request ctx
that opts one call out of enforcement records the label **and** the caller whose
request it ran under, which is what tells a user-triggered restore from a
scheduled pass.

`Invoke` authorizes **once**, on the verb — a `Write{Custom}` carrying the
mutator name and args. The rows the mutator body writes are not authorized
individually, exactly as on the client push path, so do not register a mutator
as a way to launder writes a policy would refuse.

## When to use `Mutate` instead

`Mutate` runs its closure inside the write transaction, which means it holds the
workspace's write lock for everything the closure does — and a storage adapter
may hold a wider lock than that, as the SQLite one does — and calls `CanMutate`
from inside the transaction. It also
**may run the closure more than once** if the storage adapter retries, so keep
side effects out of it.

That cost buys two things `Apply` does not offer:

- a genuinely **serializable** read-then-write, where nothing can move between
  the read and the write;
- writes that **stream** rather than buffer in memory, which is what a bulk
  import of many thousands of rows needs.

Everything else — and every request handler — should use `Apply`. For a
read-modify-write under `Apply`, pass the version you read as `Patch`'s
`expected` argument, which turns a concurrent edit into a `conflict` rejection
instead of a silent merge.

## Collaborative text

A collaborative-text field holds a CRDT document, so server-side code sets it
from a plain string and lets the engine mint the difference. This is how you put
a string-shaped API — an HTTP handler, an importer, an admin form — in front of a
text field:

```go
srv.Apply(ctx, workspaceID, func(tx *server.ApplyTx) error {
    return tx.Patch(&Issue{Id: key}, nil, nil,
        server.WithTextString("description", body),
        server.WithTextPeer("importer:text"))
})
```

The string is diffed against the **stored** document — a multi-hunk diff over
code points, so untouched regions keep their items and the marks attached to
them — and the difference is applied as edits. Text the row already holds and the
string does not contain is deleted; edits arriving after the write merge
normally. An empty string deletes all visible text, and on `Patch` the field is
added to the mask.

`WithTextPeer` is required: the engine invents no writer identity. Two writers
that can produce diverging documents must never share a peer, and a server-side
string write is not one of them — it resolves inside the write transaction,
reading the stored row and observing its version before it allocates counters,
so one name is safe across calls, across a retried closure, and across processes
(per-workspace write exclusion is the storage contract). Keep that name away from
anything that writes outside such a transaction — a client, a connector — and
prefix it per service.

A text field of the written message that already holds a plain-string value
resolves the same way, which is what makes restoring a field-history record an
ordinary `Patch`. See [Restore a field](restore-a-field.md).

## Writes that may be retried

`ApplyIdempotent` is the same write guarded by a caller-supplied key, for
anything that retries: an HTTP handler behind a load balancer, an at-least-once
change-feed handler writing back, an integration with its own backoff. The first
call for a key runs the closure and records what it did; every later call with
that key skips the closure and replays the record — including a result payload,
so a retried request answers with the first attempt's response instead of an
opaque "already applied":

```go
out, err := srv.ApplyIdempotent(ctx, workspaceID, r.Header.Get("Idempotency-Key"),
    func(tx *server.ApplyTx) ([]byte, error) {
        if err := tx.Put(&issue); err != nil {
            return nil, err
        }
        return json.Marshal(response{ID: issue.GetId()})
    })
// out.Applied — did this call do the work? out.Seq — the sequence it took.
w.Write(out.Result)
```

`MutateIdempotent` is the `Mutate` equivalent, and `InvokeIdempotent` does the
same for a registered mutator (no result payload — mutators have no channel for
one).

### What makes it correct

The ledger read, the writes and the key claim all commit in **one transaction**
under the workspace's write exclusion, so a concurrent retry can never see
"unclaimed" while the first call is mid-write, and no outcome is recorded that
the store did not keep.

That coupling also decides what happens on failure: an error or `Reject` rolls
the key back with the writes, leaving the operation retryable — only committed
outcomes replay.

### Choosing keys

Keys are **per workspace**, so derive them from something the caller owns and
prefix per source when several systems write to one workspace.

Two more things to know: a successful call claims the key even when the closure
wrote nothing (so "nothing to do" replays too, which means that unlike `Mutate`
it registers the workspace), and keys expire on `WithIdempotencyRetention`
(default 24 hours) — **set it above the longest retry window your callers use**.
This is the one retention where too short is a correctness problem, since a
retry arriving after its key expired applies again.

This is a separate ledger from the client watermarks, deliberately: those are
contiguity-checked per client and pruned once a client goes idle, neither of
which an API caller's retry key can satisfy or survive.

## Related

- [Query from server-side code](server-side-queries.md) — reads inside and
  outside a mutation.
- [React to changes](react-to-changes.md) — running effects when rows are
  written, and [the change feed](change-feed.md) they read from.
- [Go server API](../reference/server.md) — signatures and options.
