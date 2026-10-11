# Authentication and permissions

Syncplane separates identity from access control: an `Authenticator` establishes who is calling, and `Permissions` decides what the caller can do. Neither is installed by default. The engine's default configuration assumes the server runs behind your own authentication layer, which is safe only when untrusted traffic cannot reach the sync endpoint.

**Note:** By default, `server.AllowAll` admits every caller into every workspace. This configuration is suitable for single-tenant deployments, but multi-tenant deployments must override `CanAccessWorkspace`.

## Authenticating callers

An `Authenticator` converts request headers into a verified `server.Principal`. When you configure an authenticator, the engine runs it before every sync RPC, including stream initialization, and passes the principal to every permission hook.

To implement an authenticator, define a type with an `Authenticate` method and pass it to `server.New`:

```go
type myAuth struct{ verifier *oidc.Verifier }

func (a myAuth) Authenticate(ctx context.Context, h http.Header) (server.Principal, error) {
    tok, ok := strings.CutPrefix(h.Get("Authorization"), "Bearer ")
    if !ok {
        return server.Principal{}, fmt.Errorf("%w: no bearer token", server.ErrUnauthenticated)
    }
    claims, err := a.verifier.Verify(ctx, tok)   // your credential check
    if err != nil {
        return server.Principal{}, fmt.Errorf("%w: %v", server.ErrUnauthenticated, err)
    }
    return server.Principal{Subject: claims.Sub, Issuer: claims.Iss, Claims: claims.All}, nil
}

srv, err := server.New(store, server.WithAuthenticator(myAuth{verifier: v}))
```

Return `server.ErrUnauthenticated`, or an error wrapping it, to reject the request with HTTP status code 401. The engine treats any other returned error as an infrastructure fault (HTTP 500). This prevents an identity-provider outage from appearing to a client as an invalid credential.

In both cases, the server logs the error message but does not send it to the client. The wire protocol carries a fixed message, so you can wrap errors freely. To admit an anonymous caller, return a zero `server.Principal` with a `nil` error.

For fixed API keys, you can use `server.StaticTokenAuthenticator`. It is not a login system.

If you authenticate requests in HTTP middleware, call `server.WithPrincipal(ctx, p)` to attach the principal to the context so permission hooks can access it.

## Configuring permissions

For access that follows rows in the workspace — project members who see their projects, org admins who see everything — declare it in the schema instead of writing hooks. See [Declare roles and permissions](declare-roles-and-permissions.md). The hooks below still run with the access model, and can narrow what it allows.

Permission hooks receive the `server.Principal` as a direct argument rather than reading it from `ctx`. This prevents a policy from silently authorizing an anonymous caller. Embed `server.AllowAll` to implement only the hooks you need:

```go
type myPermissions struct{ server.AllowAll }

// The tenant boundary: workspace_id is self-asserted, so this is what stops
// an authenticated caller naming someone else's workspace.
func (myPermissions) CanAccessWorkspace(ctx context.Context, p server.Principal, ws string) error {
    ok, err := members.Contains(ctx, ws, p.Subject)
    if err != nil {
        return err                                    // transient -> 500, client retries
    }
    if !ok {
        return server.DenyWorkspace("not a member")    // permanent -> 403, client stops
    }
    return nil
}

func (myPermissions) CanMutate(_ context.Context, p server.Principal, ws string, w server.Write) error {
    if p.IsAnonymous() {
        return server.Reject(server.CodePermissionDenied, "sign in to make changes")
    }
    return nil
}

srv, err := server.New(store, server.WithPermissions(myPermissions{}))
```

### Authorizing workspace access with CanAccessWorkspace

`CanAccessWorkspace` runs first for every workspace named in a request:

- For `Push`, once per batch.
- For `Subscribe`, once per scope.
- For `Pull`, `Bootstrap`, and `Query`, once for their target workspace.

The engine executes `CanAccessWorkspace` before opening a read snapshot, subscribing to the notifier, resolving `FilterEpoch`, or writing a client watermark. A rejected caller leaves no trace in the denied workspace.

For `Subscribe`, `CanAccessWorkspace` also runs on every heartbeat. A revocation drops the revoked scope from an active stream without waiting for the client to reconnect.

Return the appropriate error type based on the failure condition:

| Return value                | Meaning              | Client response                                                                 |
| --------------------------- | -------------------- | ------------------------------------------------------------------------------- |
| `server.DenyWorkspace(...)` | Settled denial       | Receives `permission_denied` and `workspace_access_revoked`, then stops syncing |
| Any other error             | Policy cannot decide | Receives an infrastructure error, then retries                                  |

In a schema that uses the access model, a caller that `CanAccessWorkspace` admits but that holds no permission in the workspace gets `permission_denied` with `workspace_no_access`. In-process calls return `server.ErrNoAccess`. This denial is not permanent: the client keeps its queue and syncs again once a grant exists. A caller that loses its last permission first receives a pull that removes the rows it can no longer read. See [Create a workspace and invite people](create-a-workspace-and-invite-people.md).

**Note:** Do not return a denial when a membership store is unreachable. Return the transient error so clients retry instead of discarding valid local data.

### Filtering reads and writes with CanMutate and ReadFilter

`CanMutate` gates write operations. When `CanMutate` rejects a mutation, the engine returns the rejection as an operation result rather than an RPC error. The client rolls back the mutation locally and reports it through `onMutationRejected`.

`ReadFilter` returns a per-row predicate. `FilterEpoch` versions the visibility inputs behind `ReadFilter` so that a permission change triggers a delta resync instead of requiring a full re-bootstrap. For more information, see [Partial sync and coverage](../explanation/partial-sync-and-coverage.md).

`ReadFilter` also gates writes. The engine refuses a `PUT`, `PATCH`, or `DELETE` of a stored row that the filter hides with `permission_denied`. A caller therefore cannot change, delete, or reveal a row that it cannot read. To decide writes with a different rule, see [Check writes against the stored row](#check-writes-against-the-stored-row).

Keep membership checks out of `ReadFilter`. Handle membership in `CanAccessWorkspace`. Combining the two can cause a `nil` return to leak entities across tenants.

Keep secrets out of keys. A filter hides row values, but the entity, key, and version can reach subscribed clients as tombstones upon deletion or when an edit hides a row the client previously held. Key rows by surrogate ID and place sensitive data such as email addresses, slugs, or invite codes in filtered fields. For more information, see [Read filter trade-offs](../explanation/limits-and-trade-offs.md#a-read-filter-hides-values-not-existence).

### Performance considerations

Hooks run outside storage transactions, so a slow hook does not hold the workspace write lock. However, a slow hook stalls its caller. Verify credentials and cache membership lookups to avoid I/O operations on every call.

As an exception, `Server.Mutate` executes `CanMutate` inside its write transaction because writes in the closure are not known until it runs. `Apply` executes outside the write transaction.

The row check is the one part of a policy that always runs inside the write transaction, because it needs the stored row. The function that `WriteFilter` returns runs there; `WriteFilter` itself runs before the transaction opens, so do lookups in `WriteFilter`.

## Authorizing in-process API calls

`Apply`, `Mutate`, `Invoke`, and `View` evaluate permission hooks against the principal stored on the context. An HTTP handler that calls the engine directly is held to the same policy as sync clients, and `CanMutate` receives the same `server.Write` value.

To set the identity, call `server.WithPrincipal` on the context:

```go
ctx = server.WithPrincipal(ctx, principal)
```

A context without a principal authorizes as anonymous.

To bypass authorization for internal server operations, specify `server.Trusted(ctx)` explicitly:

```go
err := srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error { ... })
```

`server.Trusted` skips every permission hook, including `CanAccessWorkspace`, `CanMutate`, and the row check. Omitting `server.Trusted` fails safe: the engine authorizes the call as anonymous, and the policy rejects it. To record a specific actor name on written rows during a trusted operation, use `server.TrustedAs(ctx, name)`.

The engine uses the principal to attribute writes. Every row records its last writer, which appears in the change feed. For more information, see [Change feed attribution](../reference/server.md#change-feed). Because attribution derives from the context that the hooks read, attribution cannot disagree with authorization.

Note the following behavior regarding stored write attribution:

- **Authentication is required for write attribution:** Rows record only the subject. On a server that admits all requests as anonymous, every push records an empty actor. Features such as echo suppression and actor-filtered notifications do not operate without an authenticated subject.
- **Multi-issuer authenticators must namespace subjects:** The engine stores the subject but not the issuer. If two callers from different issuers share the same subject, they collapse into the same actor in the change feed and field history. Prefix the subject (for example, `google:1234` or `saml:1234`) when accepting principals from multiple identity providers.

`Invoke` authorizes once on the verb (`Write{Custom}`). The mutator body's individual row writes are not authorized separately.

## Reserve an operation for trusted writes

Some writes must come only from your own server code. For example, only the server records activity rows, or clients archive projects but never delete them. Trusted writes skip the access rules and the permission hooks, so a rule that refuses the operation to every other caller reserves it for trusted writes.

In a schema that uses the access model, leave the operation out of every access rule of the entity. The server refuses the write before `CanMutate` runs, and the TypeScript client rejects it at the call site. See [Allow some operations and not others](declare-roles-and-permissions.md#allow-some-operations-and-not-others).

The access model does not need roles for this. When no entity declares `grant`, the schema declares no role, and `CanAccessWorkspace` alone decides who enters a workspace. Give the entity rules with `admitted: {}` as the source. The other entities need no `access`: an entity without one is open to every caller that enters the workspace, as before.

```proto
// Clients read activity rows. Only the server records them.
message Activity {
	option (syncplane.v1.entity) = {
		name: "activity"
		key_fields: [ "id" ]
		access: { allow: [ { read: true admitted: {} } ] }
	};
	string id = 1;
	string text = 2;
}

// Clients create, read and archive projects, but never delete them.
message Project {
	option (syncplane.v1.entity) = {
		name: "project"
		key_fields: [ "id" ]
		access: { allow: [ { read: true create: true update: true admitted: {} } ] }
	};
	string id = 1;
	bool archived = 2;
}
```

In a schema that uses the access model, a client can invoke only a mutator that declares a `MutatorAccess`. Without roles, declare `server.MutatorAccess{Admitted: true}` to let every caller that enters the workspace invoke it:

```go
server.WithMutator("send_feedback", &appv1.SendFeedbackArgs{}, sendFeedback,
	server.MutatorAccess{Admitted: true})
```

If you later add a grant entity, the schema declares its first role, and the workspace gate then refuses every caller that holds no permission. Registration refuses that change unless you allow it; see [Changing the access model](../reference/proto-options.md#changing-the-access-model).

In a schema that does not use the access model, refuse the operation in `CanMutate`. The hook receives each CRUD write of a batch on its own, with its entity and its operation:

```go
func (myPermissions) CanMutate(_ context.Context, p server.Principal, ws string, w server.Write) error {
	if c := w.Crud; c != nil && c.GetEntity() == "project" && c.GetOp() == syncplanev1.CrudOp_CRUD_OP_DELETE {
		return server.Reject(server.CodePermissionDenied, "projects are archived, not deleted")
	}
	return nil
}
```

- **Writes under `server.Trusted` skip the hook,** so a connector, an effect or a retention job can still delete projects.
- **A custom mutator reaches the hook as one `Write` with `Custom` set,** and the rows that its body writes do not. A mutator that deletes projects therefore needs its own check.
- **The client cannot predict this refusal.** It queues the delete, and reverts it when the server rejects it.

`CanMutate` cannot enforce a rule that depends on the stored row, such as "only edit your own comment". An access rule can when a field of the row names the caller: set `caller_is` on the rule to that field. For other rules, use a write filter if the rule depends on the stored row's fields, or a custom mutator if it depends on who last wrote the row. For more information, see [Check writes against the stored row](#check-writes-against-the-stored-row) and [Check ownership in a mutator](#check-ownership-in-a-mutator).

To keep individual fields of a writable row from clients, give the entity a field guard whose only rule is the read rule. A `PATCH` whose mask names a guarded field is rejected with `access_denied`. A `PUT` keeps the stored value of the guarded field and applies the rest of the row:

```proto
// Clients edit the title. Only the server sets the total.
message Order {
	option (syncplane.v1.entity) = {
		name: "order"
		key_fields: [ "id" ]
		access: {
			allow: [ { read: true write: true admitted: {} } ]
			guard: [ { fields: [ "total" ] allow: [ { read: true admitted: {} } ] } ]
		}
	};
	string id = 1;
	string title = 2;
	int64 total = 3;
}
```

A guard can also give a field back to some callers, such as a status that only managers set, and keep a field from some readers, such as a salary that only HR reads. For more information, see [Field guards](../reference/proto-options.md#field-guards).

## Check writes against the stored row

`CanMutate` sees only the write. To decide based on the row that a write changes, implement `RowWritePermissions` on your `Permissions` struct. `WriteFilter` returns a function that the engine calls with the write and the stored row:

```go
func (p *policy) WriteFilter(
	_ context.Context, caller server.Principal, workspaceID, entity string,
) (server.RowWriteFilter, error) {
	if entity != "task" {
		return nil, nil // writable when readable
	}
	member := p.engagementsOf(workspaceID, caller) // cached lookup
	return func(w server.Write, stored proto.Message) error {
		task := stored.(*taskv1.Task)
		if !member[task.GetEngagementId()] {
			return server.Reject(server.CodePermissionDenied, "no access to this task")
		}
		if w.Crud.GetOp() == syncplanev1.CrudOp_CRUD_OP_DELETE && task.GetOwner() != caller.Subject {
			return server.Reject(server.CodePermissionDenied, "only the owner may delete this task")
		}
		return nil
	}, nil
}
```

Follow these rules:

- **Return `nil` to keep the default.** Without a write filter, the engine refuses a write to a row that `ReadFilter` hides. A caller can then change a row exactly when it can read the row.
- **Return a filter that admits every write to allow writes to hidden rows.** A filter replaces the `ReadFilter` check for its entity. Do this only when callers must change rows that they cannot read.
- **Check incoming values in `CanMutate`.** The filter receives the stored row as it is before the write, and the engine does not call it for creates. To stop a caller from moving a row to a place where the caller may not put it, check the new value in `CanMutate`.
- **Do lookups in `WriteFilter`.** The engine calls `WriteFilter` at most once per entity per request, before the write transaction opens. The returned filter runs inside the transaction while the workspace write lock is held, and runs again if the storage adapter retries. Keep the filter a fast, pure function of its arguments.

The client cannot predict a row-check refusal. It enqueues the write, and reverts the write when the server rejects it.

A write filter sees only the fields of the row. To decide based on who last wrote the row, check ownership in a custom mutator.

## Check ownership in a mutator

When authorization depends on who last wrote a row, implement a custom mutator. Custom mutators are authorized once by name, and the mutator function evaluates access. `EntityTx.GetMeta` returns the actor stamped on the row:

```go
server.WithMutator("editComment", &appv1.EditComment{},
	func(ctx context.Context, tx *server.EntityTx, args proto.Message) error {
		in := args.(*appv1.EditComment)
		var comment appv1.Comment
		found, meta, err := tx.GetMeta(&comment, in.GetId())
		if err != nil {
			return err
		}
		if !found {
			return server.Reject(server.CodeConflict, "comment is gone")
		}
		caller, _ := server.PrincipalFrom(ctx)
		if caller.IsAnonymous() || meta.Actor.Subject != caller.Subject {
			return server.Reject(server.CodePermissionDenied, "not your comment")
		}
		comment.Body = in.GetBody()
		return tx.Put(&comment)
	})
```

Check and reject anonymous callers before comparing subjects. Unauthenticated callers and rows written without an authenticated actor both have an empty `Actor.Subject`, which evaluates as a match if unchecked.

`meta.Actor` reflects the last writer of the row. If a moderator edits a comment created by an author, `meta.Actor` reports the moderator. To restrict edits to the original creator, mark a field of the row `(syncplane.v1.field_behavior).creator`: the server stores the caller's subject in it on the create, and no client write changes it later. An access rule with `caller_is` on that field then admits only the creator, and needs no mutator. See [Access model](../reference/proto-options.md#access-model).

`meta.Version` contains the row version. Because a mutator holds the workspace write lock, read-then-write operations within a mutator are serializable. When performing the same check in an `Apply` closure, pass the version to `server.WithExpectedVersion` so the write fails if the row changes concurrently. To read metadata in a read-only transaction, call `ViewTx.GetMeta` inside a `View`.

## Protecting fields per caller

A [field guard](../reference/proto-options.md#field-guards) restricts a field to the callers that hold a permission where the row sits, or that a field of the row names. The client predicts a guard with `can()`. When the rule depends on facts outside the workspace's rows, such as a billing plan, implement `FieldPermissions` on your `Permissions` struct:

```go
func (p *policy) ProtectedFields(
	ctx context.Context, caller server.Principal, workspaceID, entity string,
) ([]string, error) {
	if entity != "task" || p.roleOf(ctx, workspaceID, caller) == roleOwner {
		return nil, nil
	}
	return []string{"assignee", "due_date"}, nil
}
```

The hook only takes fields away: a field is writable when the schema's guards admit the caller and the hook does not return it. A `PATCH` targeting a field the hook returns is rejected with `permission_denied`, and a `PUT` preserves the stored field value.

Return names of top-level fields or oneof group names. Returning an invalid field name or a key field causes the request to fail rather than silently permitting access.

`ProtectedFields` must execute quickly and without side effects. The engine calls `ProtectedFields` at most once per entity per request before opening the write transaction and reuses the result during application. Place expensive lookups, such as membership checks, in middleware that attaches results to the context.

Clients handle guards and the hook differently:

- The TypeScript client refuses a `patch()` of a field whose guard admits no update to any caller before it sends the request, and `can()` predicts the other guard rules.
- Fields returned by `ProtectedFields` are evaluated on the server. The client enqueues the write and reverts it when receiving the server rejection.

Prefer a guard when the rule depends only on permissions and the row.

## Unauthenticated services

The change feed and admin service do not authenticate callers or evaluate `Permissions` because they run in-process. The admin service provides its own authorization hook through `RegisterAuthorizer`.

## Authorizing room access

A room anchored to a row admits a caller only if the entity's `ReadFilter` permits reading that row. Presence on an entity follows the entity's existing read permissions. For more information, see [Rooms](../explanation/rooms.md).

Unanchored rooms check `CanAccessWorkspace`. To customize access control for rooms, implement `RoomPermissions`:

```go
type policy struct {
	server.AllowAll
	server.AllowAllRooms
}

func (policy) CanJoinRoom(
	_ context.Context, p server.Principal, ws string, t presence.Topic,
) error {
	if t.Name == "admin" && !directory.IsAdmin(p.Subject, ws) {
		return server.DenyRoom("not an administrator")
	}
	return nil
}
```

`server.DenyRoom` rejects access to a single topic while preserving access to other topics. Any other error is treated as transient and retried.

Room permission hooks execute upon joining, and `CanJoinRoom` runs again on stream heartbeats to enforce revocations.

## Handling revocation

When a server returns a settled denial (`server.DenyWorkspace`), the client stops syncing the workspace and sets `status().workspaces[id].access`.

The client does not automatically delete local data upon denial. To remove cached data, call `removeWorkspace(id, { purge: true })`. For more information, see [Handle failures](handle-failures.md).

**Note:** Active streams verify workspace access on heartbeats, but do not re-evaluate credentials with `Authenticate`. A credential that expires during an open stream remains valid until the stream closes. Configure stream lifetime limits if credentials must be refreshed periodically.

## Configuring multiple apps on one host

`server.Manager` supports a shared authenticator with per-app permissions. For more information, see [Host multiple apps](host-multiple-apps.md#steps).
