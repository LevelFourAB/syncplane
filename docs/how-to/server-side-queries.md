# Querying from server-side code

Use this guide to query entity data from Go server-side code. Use `ViewTx.Query` for read endpoints such as list or search operations. Use `EntityTx.Query` inside mutations to find and update matching rows.

## Prerequisites

Before querying entities, ensure that you have:

- A Go server instance (`srv`) configured for your workspace.
- Generated protocol buffer message types for your entities.

## Running a query

To query entities from server-side code:

1. Build a `query.Spec` with your filter predicate, sort order, and limit using the `query` package. Use proto field names for paths. Use dot notation for nested fields such as `"settings.color"`.

   The compiler rejects field paths that do not match a proto field. Predicates use the same compiled implementation as the `Query` RPC and the TypeScript client, validated against shared test vectors in `testdata/queryeval/`.

2. Range over `server.Query` inside a `srv.View` read transaction, an `Apply` closure or an `EntityTx` mutation transaction:

   ```go
   err := srv.View(ctx, workspaceID, func(tx *server.ViewTx) error {
       for todo, err := range server.Query[*todov1.Todo](tx, query.Spec{
           Where:   query.And(query.Eq("completed", false), query.Contains("title", "milk")),
           OrderBy: []*syncplanev1.OrderKey{query.Desc("created_at_ms")},
           Limit:   20,
       }) {
           if err != nil {
               return err
           }
           // todo is a *todov1.Todo
       }
       return nil
   })
   ```

3. Process each matching row in the loop body. To get the row version for a compare-and-swap `Patch`, range over `server.QueryRows` instead and read `row.Msg` and `row.Version`. The methods `tx.Query` on each transaction type take a callback instead of returning an iterator.

## Paging

To page through query results:

1. Obtain a cursor from a previous `QueryResponse` or create one with `Order.EncodeCursor`.
2. Assign the cursor to `Spec.After` or `Spec.Before` in your `query.Spec`:

   ```go
   spec := query.Spec{OrderBy: []*syncplanev1.OrderKey{query.Desc("created_at_ms")}, Limit: 50}
   spec.After = cursorFromTheLastPage // an earlier response's end_cursor
   ```

3. Set `Limit` to specify the page size. `Limit` counts forward from `After`. When `Before` is the only bound, `Limit` counts backward from `Before`.

Cursors represent a position in the sort order rather than a row count to skip. Page with cursors instead of `Offset`. Pages stay stable when rows are inserted ahead of the cursor, and an index can seek to the position.

**Note:** A cursor is tied to the `(entity, order_by)` pair it was issued under. Replaying a cursor against a different pair fails with `query.ErrCursorShape`. You cannot set `Offset` alongside `After` or `Before`.

`ViewTx` and `ApplyTx` apply the caller's `ReadFilter`. The result omits filtered rows, and the page counts only rows the caller can see. `EntityTx` reads are server-authoritative and do not apply `ReadFilter`. To skip the filter across all transaction types, mark the context with `server.Trusted(ctx)`.

## Confirming the result

To verify that your query works as expected:

1. Check that the transaction returns `nil` for error.
2. Confirm that the callback receives the expected proto messages for matching entities.

## Related

- [Query data](query-data.md) — client-side equivalents.
- [Limits and trade-offs](../explanation/limits-and-trade-offs.md) — performance considerations for unindexed queries.
