package server

import (
	"context"
	"errors"

	"google.golang.org/protobuf/proto"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
)

// Rejection is a business-level "no" to a mutation. It becomes a
// MUTATION_STATUS_REJECTED result (an ack the client drops the mutation
// on), never an RPC error.
type Rejection struct {
	Code    string
	Message string
}

func (r *Rejection) Error() string { return r.Code + ": " + r.Message }

// Reject builds a Rejection error for permission hooks and mutators.
func Reject(code, message string) error {
	return &Rejection{Code: code, Message: message}
}

// AsRejection extracts a Rejection from an error chain.
func AsRejection(err error) (*Rejection, bool) {
	var r *Rejection
	if errors.As(err, &r) {
		return r, true
	}
	return nil, false
}

// WorkspaceDenial is a PERMANENT "no" from CanAccessWorkspace: this caller may
// not access this workspace, and retrying will not change that. It becomes an
// RPC error with Connect code permission_denied carrying
// CodeWorkspaceAccessRevoked, so a client can tell "I was removed from this
// project" from "the membership service is having a bad day" and act on it —
// stop syncing the workspace instead of backing off forever.
//
// It is deliberately distinct from Rejection: a Rejection is an ack for one
// mutation (the client drops it and moves on), while this denies the entire
// request, including reads.
type WorkspaceDenial struct {
	// Message explains the denial. It reaches the client, so keep it free of
	// detail the caller has not earned — "not a member of this workspace" is
	// better than naming who is.
	Message string
}

func (d *WorkspaceDenial) Error() string { return "workspace access denied: " + d.Message }

// DenyWorkspace builds the permanent denial CanAccessWorkspace returns when the
// caller is not (or is no longer) entitled to the workspace.
//
// Return it only when the answer is settled. A membership lookup that FAILED
// must return its underlying error instead: that surfaces as an infrastructure
// error the client retries, where a denial would have it discard real data over
// a transient outage.
func DenyWorkspace(message string) error {
	return &WorkspaceDenial{Message: message}
}

// AsWorkspaceDenial extracts a WorkspaceDenial from an error chain.
func AsWorkspaceDenial(err error) (*WorkspaceDenial, bool) {
	var d *WorkspaceDenial
	if errors.As(err, &d) {
		return d, true
	}
	return nil, false
}

// RowFilter decides per row whether a client may read it. The message is the
// decoded entity row. A nil RowFilter allows all rows.
type RowFilter func(key string, row proto.Message) bool

// Write is a single authorizable unit handed to Permissions.CanMutate. The
// engine flattens every mutation envelope into Writes before authorizing, so a
// policy always sees exactly one write and can never forget to unwrap a batch:
//
//   - a CRUD mutation   → one Write with Crud set
//   - a custom mutation → one Write with Custom set
//   - a batch           → one Write per sub-mutation, each with Crud set, all
//     sharing the envelope's ClientId/MutationId
//
// Exactly one of Crud or Custom is non-nil. Rejecting any Write flattened from
// a batch rejects (and rolls back) the entire batch.
type Write struct {
	// ClientId is the client-asserted origin of the envelope this Write came
	// from. It is an idempotency handle, NOT an identity: it is whatever the
	// client sent, unverified. Authorize against the Principal CanMutate is
	// handed instead; never derive authorization from ClientId.
	ClientId string
	// MutationId is the envelope's per-client id. Writes flattened from one
	// batch share it.
	MutationId uint64
	// Crud is set for a CRUD write, including each sub-mutation of a batch.
	Crud *syncplanev1.CrudMutation
	// Custom is set for a custom-mutator invocation. A policy may authorize on
	// its Name/Args but not on the individual rows it writes — those happen
	// inside the registered mutator, after this hook.
	Custom *syncplanev1.CustomMutation
}

// Permissions hooks the server consults on every write (per Write) and every
// read (pull/bootstrap row filtering).
//
// # The caller's identity
//
// Every hook is handed the request's verified Principal explicitly, rather than
// leaving it to be fished out of ctx, so a policy cannot silently authorize as
// nobody. It is the zero (anonymous) Principal when no Authenticator is
// installed or when one admitted an unauthenticated request — a policy guarding
// untrusted traffic should lead with Principal.IsAnonymous.
//
// The in-process API consults these hooks too. Server.Apply, Server.Mutate,
// Server.Invoke and Server.View authorize against the ctx principal, so an app
// handler that drives the engine directly is held to the same policy as a sync
// client — a write through Apply reaches CanMutate carrying the very
// CrudMutation the wire path would have carried. Code with no caller to
// authorize (a connector, a migration, a cron job) marks its ctx with Trusted,
// which skips these hooks entirely; see Trusted for why that is opt-in.
//
// # Partial implementations
//
// Embed AllowAll to implement only the hooks you care about — see its doc.
//
// # Execution context: these hooks run outside transactions, with one exception
//
// The server schedules every method here outside transaction scope: CanMutate
// runs before the push WRITE transaction opens (and, on Apply, between the read
// snapshot and the write transaction), and ReadFilter/FilterEpoch are resolved
// before the pull/query/bootstrap/View READ snapshot opens (their results
// memoized so the in-snapshot row scan only applies the pure predicate). A slow
// hook therefore does not hold the workspace write lock or pin a read snapshot
// — but it does stall the RPC it belongs to, so implementations must still be
// fast and should avoid blocking I/O. Do such work — token introspection, a
// membership lookup against another service — before the RPC (e.g. in
// middleware that stashes the result on ctx) and only consult the cached value
// here.
//
// The exception is Server.Mutate, whose closure discovers its writes as it runs:
// there CanMutate is called from inside the write transaction, so a slow policy
// does hold the workspace write lock. That is the price of a serializable
// read-then-write under policy, it is confined to that one entry point, and
// Apply — which every app handler should prefer — does not pay it. A policy that
// cannot be fast should keep callers off Mutate.
//
// CanMutate must also be a pure function of its arguments — free of observable
// side effects. It runs once per authorized write, but the engine relies on its
// verdict being reproducible: on the Mutate path the storage adapter may re-run
// the write closure and re-invoke the hook, and a side-effectful hook (audit
// logging, metering) would double-count. Such effects belong after the call
// commits, with knowledge of the committed result.
type Permissions interface {
	// CanAccessWorkspace decides whether the caller may touch this workspace at
	// all. It is the tenant boundary: workspace_id arrives in the request and is
	// otherwise self-asserted, so this hook is the only thing standing between an
	// authenticated caller and any workspace id they care to name.
	//
	// The engine runs it for every workspace a request names — per batch on
	// Push, per scope on Subscribe, and for the one workspace of Pull, Bootstrap
	// and Query — before it opens a read snapshot, subscribes to the notifier,
	// resolves FilterEpoch or writes a client watermark, so a denied request
	// costs no storage work and leaves no trace in the workspace it was
	// refused.
	//
	// Return nil to admit. Return DenyWorkspace(...) for a settled "no", which
	// reaches the client as a permanent, actionable denial. Return any other
	// error for a failure to DECIDE (the membership store is down); that
	// surfaces as an infrastructure error the client retries, which is the
	// outcome you want when the alternative is a client discarding good data
	// over an outage.
	//
	// It runs on the hot path of every RPC, so keep it to a cached lookup —
	// see the interface doc on execution context.
	//
	// This is workspace-level access. Row-level visibility WITHIN a workspace
	// stays with ReadFilter; do not fold membership into per-row filters, which
	// is how a single nil return leaks an entity across tenants.
	CanAccessWorkspace(ctx context.Context, p Principal, workspaceID string) error
	// CanMutate is called before a write is applied, once per Write. The engine
	// flattens batches for you (see Write), so an implementation authorizing per
	// CRUD never unwraps a batch itself. Return a Rejection (via Reject) for a
	// business "no"; any other error aborts the RPC.
	//
	// It sees the same Write whether the write arrived over the wire or through
	// the in-process API, which is what keeps an app's own handlers held to the
	// policy its sync clients are. A mutator invocation authorizes ONCE as a
	// Write{Custom} on its name and args; the rows its body writes are not
	// authorized individually, on either path.
	//
	// Runs before the push write transaction opens, and between Apply's read and
	// write transactions — but INSIDE the transaction on Server.Mutate. Keep it
	// fast and side-effect free; see the interface doc for details.
	CanMutate(ctx context.Context, p Principal, workspaceID string, w Write) error
	// ReadFilter returns the row filter for an entity, or nil to allow all.
	// Resolved before the read snapshot opens and memoized for the row scan, so
	// keep it fast (see the interface doc).
	//
	// It gates the in-process reads too — Server.View and the reads an Apply
	// closure makes — where a filtered row reads as absent rather than as a
	// tombstone. Those callers can touch any entity, so they resolve every
	// registered entity's filter up front rather than one per request.
	//
	// The returned filter is applied to rows of one request and must close over
	// p rather than consult ctx: it runs during the row scan, after this hook
	// returned.
	ReadFilter(ctx context.Context, p Principal, workspaceID, entity string) (RowFilter, error)
	// FilterEpoch returns the workspace's visibility epoch (0 if unused). It
	// is stamped into cursors; a later mismatch forces clients to
	// re-bootstrap, dropping rows that became invisible. Resolved before the
	// read snapshot opens — keep it fast (see the interface doc).
	//
	// Contract: row visibility normally propagates through row versions —
	// editing a row re-evaluates its filter on the next pull and hides it
	// with a tombstone if needed. ReadFilter predicates are pure functions
	// of the row, so when a visibility INPUT changes outside a row edit
	// (e.g. a membership table the filter consults), the app must either
	// touch the affected rows or bump this epoch.
	//
	// An epoch bump alone forces every client into a full re-bootstrap on
	// its next pull. Apps that also implement EpochReadFilters get a delta
	// resync instead: only rows whose visibility actually changed ship.
	FilterEpoch(ctx context.Context, p Principal, workspaceID string) (uint64, error)
}

// ErrEpochUnavailable is returned by ReadFilterAt when the filter at the
// requested epoch can no longer be reconstructed. The affected client falls
// back to a full reset (re-bootstrap), exactly as if the app did not
// implement EpochReadFilters.
var ErrEpochUnavailable = errors.New("server: filter epoch unavailable")

// EpochReadFilters is an optional Permissions extension. Apps that can
// evaluate read filters as of an EARLIER FilterEpoch opt into delta resync
// for permission changes: instead of resetting every client after an epoch
// bump, the server diffs each client's old visible set (filters at the
// cursor's epoch) against the new one and sends only the difference — rows
// that became visible as values, rows that became hidden as tombstones.
//
// ReadFilterAt carries the same purity contract as ReadFilter, evaluated as
// of the given epoch. Return ErrEpochUnavailable for epochs too old to
// reconstruct (e.g. beyond retained history); those clients reset as before.
type EpochReadFilters interface {
	ReadFilterAt(ctx context.Context, p Principal, workspaceID, entity string, epoch uint64) (RowFilter, error)
}

// AllowAll is the default Permissions implementation: every caller may do
// everything. It is the right default for the engine's original posture —
// embedded behind an app that already authenticated and authorized the request
// — and the wrong one for anything reachable by untrusted traffic.
//
// Embed it to implement only the hooks you need; the promoted methods satisfy
// the rest, so a policy that only rejects writes needs one method:
//
//	type policy struct{ server.AllowAll }
//
//	func (policy) CanMutate(_ context.Context, p server.Principal, ws string, w server.Write) error {
//		if p.IsAnonymous() {
//			return server.Reject(server.CodePermissionDenied, "sign in to make changes")
//		}
//		return nil
//	}
//
// That inherits allow-all reads and a constant zero epoch, so an embedding
// policy never meets the epoch machinery until it needs it. It also means new
// hooks added to Permissions arrive as allow-all rather than as a compile
// error — convenient, but check the release notes when upgrading, since a
// silently-inherited default on a new hook is an authorization decision you did
// not make.
//
// # The workspace gate is the one to think twice about
//
// AllowAll.CanAccessWorkspace admits every caller into every workspace. That is
// correct for a single-tenant deployment and WRONG for a shared one: since
// workspace_id is self-asserted, inheriting this hook means any caller who
// reaches the server can name any workspace and be served it. A multi-tenant
// deployment must override CanAccessWorkspace — embedding AllowAll will not
// tell you that you forgot.
//
// It is also what makes AllowAll unsafe behind an app's own API: the in-process
// paths authorize like the sync surface does, so a handler that forgot to
// authenticate is caught by a real policy (it authorizes as anonymous) and
// waved through by this one.
type AllowAll struct{}

func (AllowAll) CanAccessWorkspace(context.Context, Principal, string) error { return nil }
func (AllowAll) CanMutate(context.Context, Principal, string, Write) error   { return nil }
func (AllowAll) ReadFilter(context.Context, Principal, string, string) (RowFilter, error) {
	return nil, nil
}
func (AllowAll) FilterEpoch(context.Context, Principal, string) (uint64, error) { return 0, nil }
