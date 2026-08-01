# Query from server-side code

`ViewTx.Query` answers "find everything matching X" from Go — the read shape
every list or search endpoint is built on. `EntityTx.Query` is the same read
inside a mutation, for "find the rows matching X and update them".

Build the predicate with the `query` package rather than by hand:

```go
err := srv.View(ctx, workspaceID, func(tx *server.ViewTx) error {
    return tx.Query(&todov1.Todo{}, query.Spec{
        Where:   query.And(query.Eq("completed", false), query.Contains("title", "milk")),
        OrderBy: []*syncplanev1.OrderKey{query.Desc("created_at_ms")},
        Limit:   20,
    }, func(msg proto.Message, version uint64) error {
        todo := msg.(*todov1.Todo)
        // version round-trips into a compare-and-swap Patch
        return nil
    })
})
```

It is the same compiled predicate the `Query` RPC and the TypeScript client
use — one implementation, kept honest across both languages by the shared
vectors in `testdata/queryeval/` — so a predicate selects the same rows wherever
it is evaluated.

Paths are proto field names, dotted for nesting (`"settings.color"`), and a path
naming no field is rejected at compile time rather than matching nothing.

## Two costs, stated plainly

Nothing in the API hints at either:

- **There is no secondary index.** Every query scans and decodes every live row
  of the entity, so it is O(entity) in time however selective the predicate is
  and however small the limit. Fine for a background job; think twice in a hot
  handler over a large entity.
- **No engine limit caps the result**, unlike the RPC, so a `Spec` with no
  `Limit` holds every match in memory. Inside `EntityTx` the scan also runs
  while the workspace write lock is held.

`ViewTx` and `ApplyTx` apply the caller's `ReadFilter` — a filtered row is
simply absent from the result, and `Offset`/`Limit` count only rows the caller
may see. `EntityTx` does not: its reads are server-authoritative, like a
mutator's. A read marked `server.Trusted(ctx)` skips the filter everywhere.

## Related

- [Query data](query-data.md) — the client-side equivalents.
- [Limits and trade-offs](../explanation/limits-and-trade-offs.md) — where the
  missing index bites.
