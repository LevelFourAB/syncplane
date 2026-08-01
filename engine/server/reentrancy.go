package server

import "context"

// writeTxGuardKey marks a context as running inside a server-managed write
// transaction. Mutators receive such a context (see applyCustom and Invoke),
// so any write call they make back into the server carries the mark.
type writeTxGuardKey struct{}

// markWriteTx returns ctx tagged as executing inside a write transaction. It
// is applied to the context handed to mutator code so that re-entrant write
// calls can be detected.
func markWriteTx(ctx context.Context) context.Context {
	return context.WithValue(ctx, writeTxGuardKey{}, struct{}{})
}

// inWriteTx reports whether ctx is already inside a server-managed write
// transaction. A nested write transaction deadlocks waiting for the write lock
// the outer one holds; callers use this to fail fast with a clear error
// instead. An adapter may hold a lock wider than one workspace — the SQLite
// store serializes every write on one connection — so nesting across
// workspaces is no safer than nesting within one.
//
// Detection relies on the offending call threading the context the mutator was
// given — which mutators do idiomatically. A trusted caller that re-enters
// Mutate from inside its own Mutate closure with a freshly minted context is
// not caught (the closure gets no context to carry the mark) and will still
// deadlock; do not nest write transactions.
func inWriteTx(ctx context.Context) bool {
	return ctx.Value(writeTxGuardKey{}) != nil
}
