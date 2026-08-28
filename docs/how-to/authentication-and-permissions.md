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

**Note:** Do not return a denial when a membership store is unreachable. Return the transient error so clients retry instead of discarding valid local data.

### Filtering reads and writes with CanMutate and ReadFilter

`CanMutate` gates write operations. When `CanMutate` rejects a mutation, the engine returns the rejection as an operation result rather than an RPC error. The client rolls back the mutation locally and reports it through `onMutationRejected`.

`ReadFilter` returns a per-row predicate. `FilterEpoch` versions the visibility inputs behind `ReadFilter` so that a permission change triggers a delta resync instead of requiring a full re-bootstrap. For more information, see [Partial sync and coverage](../explanation/partial-sync-and-coverage.md).

Keep membership checks out of `ReadFilter`. Handle membership in `CanAccessWorkspace`. Combining the two can cause a `nil` return to leak entities across tenants.

Keep secrets out of keys. A filter hides row values, but the entity, key, and version can reach subscribed clients as tombstones upon deletion or when an edit hides a row the client previously held. Key rows by surrogate ID and place sensitive data such as email addresses, slugs, or invite codes in filtered fields. For more information, see [Read filter trade-offs](../explanation/limits-and-trade-offs.md#a-read-filter-hides-values-not-existence).

### Performance considerations

Hooks run outside storage transactions, so a slow hook does not hold the workspace write lock. However, a slow hook stalls its caller. Verify credentials and cache membership lookups to avoid I/O operations on every call.

As an exception, `Server.Mutate` executes `CanMutate` inside its write transaction because writes in the closure are not known until it runs. `Apply` executes outside the write transaction.

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

`server.Trusted` skips both `CanAccessWorkspace` and `CanMutate`. Omitting `server.Trusted` fails safe: the engine authorizes the call as anonymous, and the policy rejects it. To record a specific actor name on written rows during a trusted operation, use `server.TrustedAs(ctx, name)`.

The engine uses the principal to attribute writes. Every row records its last writer, which appears in the change feed. For more information, see [Change feed attribution](../reference/server.md#change-feed). Because attribution derives from the context that the hooks read, attribution cannot disagree with authorization.

Note the following behavior regarding stored write attribution:

- **Authentication is required for write attribution:** Rows record only the subject. On a server that admits all requests as anonymous, every push records an empty actor. Features such as echo suppression and actor-filtered notifications do not operate without an authenticated subject.
- **Multi-issuer authenticators must namespace subjects:** The engine stores the subject but not the issuer. If two callers from different issuers share the same subject, they collapse into the same actor in the change feed and field history. Prefix the subject (for example, `google:1234` or `saml:1234`) when accepting principals from multiple identity providers.

`Invoke` authorizes once on the verb (`Write{Custom}`). The mutator body's individual row writes are not authorized separately.

## Enforcing write policies in the schema

`(syncplane.v1.entity)` defines three write policies: `create_policy`, `update_policy`, and `delete_policy`. The engine enforces these policies before `CanMutate` runs on every write path.

Setting a policy to `*_TRUSTED_ONLY` restricts that write operation to trusted server writes and custom mutators, blocking sync clients and direct in-process calls. Common use cases include:

- Server-owned entities: restrict all three operations to `*_TRUSTED_ONLY`.
- Append-only entities: restrict update and delete operations while permitting creation.

Use schema write policies for rules that apply to all deployments. Defining the rule in the schema ensures it is enforced consistently, rather than relying on policy code in each deployment where a missing check fails open.

Schema write policies apply to caller classes rather than specific rows. To enforce row-level rules such as "only edit your own comment", use `CanMutate` if ownership can be derived from the key, or use a custom mutator if the check requires reading the stored row. For more information, see [Check ownership in a mutator](#check-ownership-in-a-mutator) and [Write policies](../reference/proto-options.md#write-policies).

To protect individual fields on a writable row, use `(syncplane.v1.field_behavior).write_policy`. A `PATCH` request that includes a protected field in its field mask is rejected. A `PUT` request preserves the existing stored value of the protected field and applies the remaining updates. For more information, see [Protected fields](../reference/proto-options.md#protected-fields).

## Check ownership in a mutator

When authorization requires inspecting stored row data, implement a custom mutator. Custom mutators are authorized once by name, and the mutator function evaluates access. `EntityTx.GetMeta` returns the actor stamped on the row:

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

`meta.Actor` reflects the last writer of the row. If a moderator edits a comment created by an author, `meta.Actor` reports the moderator. To restrict edits to the original creator, store the creator's identity in a field on creation and set `write_policy: FIELD_WRITE_POLICY_TRUSTED_ONLY` to prevent subsequent client modifications.

`meta.Version` contains the row version. Because a mutator holds the workspace write lock, read-then-write operations within a mutator are serializable. When performing the same check in an `Apply` closure, pass the version to `server.WithExpectedVersion` so the write fails if the row changes concurrently. To read metadata in a read-only transaction, call `ViewTx.GetMeta` inside a `View`.

## Protecting fields per caller

To restrict field writes based on the identity of the caller, implement `FieldPermissions` on your `Permissions` struct:

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

The effective protected set combines the schema definitions with the fields returned by `ProtectedFields`. This hook widens restrictions and cannot remove protections defined in the schema. Both schema rules and hook returns are enforced identically: a `PATCH` targeting a protected field is rejected, and a `PUT` preserves the stored field value.

Return names of top-level fields or oneof group names. Returning an invalid field name or a key field causes the request to fail rather than silently permitting access.

`ProtectedFields` must execute quickly and without side effects. The engine calls `ProtectedFields` at most once per entity per request before opening the write transaction and reuses the result during application. Place expensive lookups, such as membership checks, in middleware that attaches results to the context.

Clients handle schema protections and dynamic protections differently:

- The TypeScript client checks schema annotations locally and prevents protected writes before sending requests.
- Protections returned by `ProtectedFields` are evaluated on the server. The client enqueues the write and reverts it when receiving the server rejection.

Prefer schema definitions when a field protection applies to all callers.

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
