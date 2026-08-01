# Authentication and permissions

Two separate seams: an `Authenticator` establishes **who** is calling, and
`Permissions` decides **what** they may do. Neither is installed by default —
the engine's baseline posture is "embedded behind your own auth", which is safe
only when the sync surface is unreachable by untrusted traffic.

> **The default is open.** `AllowAll` admits every caller into every workspace.
> That is right for single-tenant and wrong for shared deployments, and nothing
> will tell you at compile time. A multi-tenant deployment must override
> `CanAccessWorkspace`.

## Establish identity

`Authenticator` turns request headers into a verified `Principal`. Install one
and the engine runs it ahead of every sync RPC (streams included, at open) and
hands the result to every permission hook:

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

Return `ErrUnauthenticated` (or an error wrapping it) to reject the request as
401; **any other error is treated as an infrastructure fault (500)**, so an
identity-provider outage never reads to a client as "your credential is bad".
Returning the zero `Principal` with a nil error deliberately admits an anonymous
caller.

`server.StaticTokenAuthenticator` is a ready-made implementation for fixed API
keys; it is not a login system. Authenticating in your own HTTP middleware works
too — call `server.WithPrincipal(ctx, p)` so the hooks see the identity.

## Decide what it may do

Permission hooks receive the principal as an argument rather than fishing it out
of `ctx`, so a policy cannot silently authorize as nobody. Embed
`server.AllowAll` to implement only the hooks you need:

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

### `CanAccessWorkspace` — the tenant boundary

It runs first for every workspace a request names (`Push` per batch, `Subscribe`
per scope, and `Pull`/`Bootstrap`/`Query` for their one workspace) — before the
engine opens a read snapshot, subscribes to the notifier, resolves `FilterEpoch`
or writes a client watermark, so a refused caller leaves no trace in the
workspace they were denied. On `Subscribe` it also runs again on every heartbeat,
so a revocation drops that scope from a live stream rather than waiting for the
client to reconnect.

**Return the right kind of error.** The split is load-bearing:

| Return                    | Meaning                     | Client sees                                             |
| ------------------------- | --------------------------- | ------------------------------------------------------- |
| `server.DenyWorkspace(…)` | a settled answer            | `permission_denied` + `workspace_access_revoked` — stop |
| any other error           | the policy could not decide | an infrastructure error — retry                         |

Returning a denial when your membership store is merely down would have clients
discard good data over an outage.

### `CanMutate` and `ReadFilter`

`CanMutate` gates writes; a rejection comes back as a result, not an RPC error,
and the client rolls the mutation back locally and reports it on
`onMutationRejected`.

`ReadFilter` returns a per-row predicate, and `FilterEpoch` versions the
visibility inputs behind it so that a permission change [delta-resyncs the difference](../explanation/partial-sync-and-coverage.md) instead of forcing a
full re-bootstrap.

**Keep membership out of `ReadFilter`** — that is what `CanAccessWorkspace` is
for, and folding the two together is how a single `nil` return leaks an entity
across tenants.

### Performance

Hooks run outside storage transactions, so a slow one does not hold the
workspace write lock — but it still stalls its caller. Verify credentials and
cache membership lookups rather than doing I/O per call. The one exception is
`Server.Mutate`, which calls `CanMutate` from inside its write transaction
because the closure's writes are not known until it runs; `Apply` does not.

## The in-process API is gated too

`Apply`, `Mutate`, `Invoke` and `View` consult these same hooks, against the
principal on the **context** — so an HTTP handler that drives the engine
directly is held to the policy your sync clients are, and `CanMutate` sees the
same `Write` either way. Put the identity you authenticated on the ctx with
`server.WithPrincipal`. A ctx with no principal authorizes as anonymous.

Code with no caller to authorize says so explicitly, per call:

```go
err := srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error { … })
```

`Trusted` skips both hooks. Forgetting it fails safe — the call authorizes as
anonymous and this policy denies it — which is why the bypass is opt-in rather
than the default. `server.TrustedAs(ctx, name)` is the same bypass with a name
recorded on the rows it writes.

The principal is also what the engine attributes writes to: every row records
its last writer, and the [change feed](change-feed.md#who-wrote-it) reports it.
Attribution is derived from the same ctx the hooks read, so it cannot disagree
with what was authorized.

`Invoke` authorizes once on the verb (`Write{Custom}`); the rows the mutator
body writes are not authorized individually, exactly as on the push path.

## What is not gated

The change feed and the admin service do not authenticate and do not consult
`Permissions`; reaching them means you are already inside the process. The admin
service has its own gate (`RegisterAuthorizer`).

## Revocation

A settled denial is permanent and reaches the client as such: it stops syncing
that workspace and reports it on `status().workspaces[id].access`. The local
copy is **not** deleted automatically — discarding a user's rows on one server
verdict is an app decision, made with
`removeWorkspace(id, { purge: true })`. See [Handle failures](handle-failures.md).

Note also that a live stream re-checks _access_, not _credentials_:
`Authenticate` runs at stream open, so a credential that expires mid-stream
rides to the end of the stream. Bound stream lifetime if that matters.

## Several apps on one host

`server.Manager` splits its options along the same line — one shared
authenticator, per-app permissions. See [Host multiple apps](host-multiple-apps.md#authenticating-a-collection-of-apps).
