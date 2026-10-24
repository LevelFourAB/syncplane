# Writing from server-side code

This guide shows you how to write data from server-side Go code—such as HTTP
handlers, agents, or cron jobs—through the storage path used by clients. Use
this guide when you need to apply atomic mutations directly on the server.

## Prerequisites

Before you begin, verify that you have:

- A configured server instance (`srv`).
- A workspace ID (`workspaceID`).
- A `context.Context` instance (`ctx`).

## Acting as a caller

`Apply`, `Mutate`, `Invoke`, and `View` run the same permission hooks as the sync
surface against the principal on the context.

To configure caller authorization, choose one of the following methods:

1. **Authenticate as a specific user:** Attach the authenticated identity to
   `ctx` with `server.WithPrincipal`. The engine evaluates your handler
   against the client policy:

   ```go
   ctx = server.WithPrincipal(ctx, server.Principal{Subject: userID})
   err := srv.Apply(ctx, workspaceID, func(tx *server.ApplyTx) error { … })
   ```

   A context with no principal authorizes as anonymous. A deny-by-default policy
   rejects anonymous requests.

2. **Bypass authorization for system tasks:** For operations with no caller
   identity, such as migrations or nightly jobs, pass `server.Trusted(ctx)`:

   ```go
   err := srv.Mutate(server.Trusted(ctx), workspaceID, func(tx *server.EntityTx) error { … })
   ```

   `Trusted` skips permission hooks for that single call. A bare `Trusted`
   records `"system"` as the actor.

3. **Bypass authorization with a named actor:** If you write on a schedule or in
   a loop, call `server.TrustedAs(ctx, name)`. The engine records the name on
   every written row. The name appears on the
   [change feed](../reference/server.md#change-feed) as the actor of the write:

   ```go
   ctx = server.TrustedAs(ctx, "connect:markdown")
   ```

   Consumers can use this actor name to skip their own writes. Both `Trusted` and
   `TrustedAs` compose with an existing principal.

**Note:** `Invoke` authorizes once on the verb (`Write{Custom}` carrying the
mutator name and arguments). The engine does not authorize the rows written by
the mutator body individually.

## Applying mutations

The engine authorizes writes against the principal on the context. For details,
see [Acting as a caller](#acting-as-a-caller).

To write data with `Apply`:

1. Execute writes within an `Apply` closure:

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

   - To read existing rows, call `tx.Get`. Reads come from the snapshot taken
     before the closure runs. Calling `tx.Get` on a key already written in the
     closure returns an error.
   - To record updates, call `tx.Patch` or `tx.Put`. For read-modify-write
     operations, pass the version returned by `tx.Get` as the `expected`
     argument of `tx.Patch` to convert concurrent edits into conflicts.
   - To abort the mutation, return an error or call `server.Reject(...)` with a
     typed error.
   - To execute a registered mutator instead of an inline closure, call
     `srv.Invoke`.

2. To visit every row of an entity, range over `server.Scan`. It reads the
   entity in pages, so it does not hold the whole entity in memory:

   ```go
   err := srv.Apply(ctx, workspaceID, func(tx *server.ApplyTx) error {
       for todo, err := range server.Scan[*todov1.Todo](tx) {
           if err != nil {
               return err
           }
           if todo.GetCompleted() {
               if err := server.Delete[*todov1.Todo](tx, todo.GetId()); err != nil {
                   return err
               }
           }
       }
       return nil
   })
   ```

   To also get each row's version for a compare-and-swap, range over
   `server.ScanRows` and read `row.Version`. To stop at a page boundary
   yourself, call `ApplyTx.ScanPage(prototype, afterKey, limit, fn)`. This
   method returns the last visited key and a boolean value that indicates
   whether more rows follow.

**Note:** `Apply` does not use client watermarks. Calling `Apply` twice applies
the mutation twice. If callers can retry, use `ApplyIdempotent`.

## Writing collaborative text

A collaborative-text field holds a conflict-free replicated data type (CRDT)
document. To update a collaborative-text field from a plain string:

1. Call `tx.Patch` inside an `Apply` closure with `server.WithTextString` and
   `server.WithTextPeer`:

   ```go
   srv.Apply(ctx, workspaceID, func(tx *server.ApplyTx) error {
       return tx.Patch(&Issue{Id: key}, nil, nil,
           server.WithTextString("description", body),
           server.WithTextPeer("importer:text"))
   })
   ```

   The engine diffs the string against the stored document and applies the
   difference as edits. The engine deletes text omitted from the string. An
   empty string deletes all visible text. On `tx.Patch`, the engine adds the
   field to the mask.

2. Specify a unique peer name with `server.WithTextPeer`. Prefix the peer name
   per service. Writers that can produce diverging documents outside
   transactions must not share a peer name.

3. If the document resides inside a keyed list or map element, specify its
   instance path, such as `WithTextString("comments.<id>.body", s)`. The written
   message must contain the addressed element.

**Note:** If a text field on the written message already holds a plain-string
value, it resolves the same way. For more information, see
[Restore a field](restore-a-field.md).

## Retrying writes idempotently

If callers can retry requests (such as HTTP handlers behind load balancers or
change-feed handlers), use `ApplyIdempotent` with a caller-supplied key.

1. Call `srv.ApplyIdempotent` with the idempotency key:

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

   The first call executes the closure and records the result. Subsequent calls
   with the same key replay the recorded result without re-executing the
   closure.

2. If you use `Mutate` or `Invoke`, call `MutateIdempotent` or
   `InvokeIdempotent`. `InvokeIdempotent` does not return a result payload.

## Verifying the write

To verify that the write succeeded:

1. Check the returned error from `srv.Apply`, `srv.Invoke`, or
   `srv.ApplyIdempotent`. A `nil` error indicates that all writes committed
   atomically under a single sequence number.
2. For `srv.ApplyIdempotent`, inspect `out.Applied` to confirm whether the
   closure executed (`true`) or replayed a prior result (`false`). Inspect
   `out.Seq` for the assigned sequence number.
3. Confirm that connected clients received the commit notification dispatched by
   the engine.

## Related

- [In-process writes](../reference/server.md#in-process-writes)
- [Atomic batches](../explanation/convergence-model.md#atomic-batches)
- [Query from server-side code](server-side-queries.md) — reads inside and
  outside a mutation.
- [React to changes](react-to-changes.md) — running effects when rows are
  written, and [the change feed](change-feed.md) they read from.
- [Go server API](../reference/server.md) — signatures and options.
