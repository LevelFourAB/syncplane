package server

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/presence"
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
	// It is not the only thing that can refuse a write. The engine enforces the
	// schema's own rules first — in a schema that uses the access model, a
	// write that can only be an operation no (syncplane.v1.entity).access rule
	// admits — so a write this hook never sees has already been rejected. A policy therefore cannot widen what a schema
	// forbids, only narrow it further. A write this hook admits must then pass
	// the row check against the row it targets, which by default refuses a
	// write to a row ReadFilter hides; see RowWritePermissions.
	//
	// Runs before the push write transaction opens, and between Apply's read and
	// write transactions — but INSIDE the transaction on Server.Mutate. Keep it
	// fast and side-effect free; see the interface doc for details.
	CanMutate(ctx context.Context, p Principal, workspaceID string, w Write) error
	// ReadFilter returns the row filter for an entity, or nil to allow all.
	// Resolved before the read snapshot opens and memoized for the row scan, so
	// keep it fast (see the interface doc).
	//
	// A filter hides row VALUES, not row existence. A subscribed client can
	// still learn a denied row's entity, key and version: a deletion reaches
	// every subscribed client as a tombstone, and a write that leaves a row
	// denied may reach clients as one too — that tombstone is what revokes the
	// row from a client that could read it before the write (see FilterEpoch).
	// The server withholds it wherever the cursor proves the client never held
	// the row — rows created past the cursor's position, non-members of a
	// windowed or followed scope — but cannot prove that for older rows, so
	// keys must not themselves be secrets: key rows by surrogate id and put
	// the email, slug or invite code in a field the filter covers. See
	// docs/explanation/limits-and-trade-offs.md.
	//
	// It gates the in-process reads too — Server.View and the reads an Apply
	// closure makes — where a filtered row reads as absent rather than as a
	// tombstone. Those callers can touch any entity, so they resolve every
	// registered entity's filter up front rather than one per request.
	//
	// Unless RowWritePermissions supplies a write filter for the entity, it
	// also gates writes: a PUT, PATCH or DELETE of a stored row it hides is
	// refused with CodePermissionDenied, on the wire and in-process alike.
	//
	// The returned filter is applied to rows of one request and must close over
	// p rather than consult ctx: it runs during the row scan, after this hook
	// returned.
	ReadFilter(ctx context.Context, p Principal, workspaceID, entity string) (RowFilter, error)
	// FilterEpoch returns the workspace's visibility epoch (0 if unused). It
	// is stamped into cursors; a later mismatch forces clients to
	// re-bootstrap, dropping rows that became invisible. Resolved before the
	// read snapshot opens — keep it fast (see the interface doc). Pull calls
	// it once per request, and an open Subscribe stream once per page it
	// sends, live-tail pages included.
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

// FieldPermissions is an optional Permissions extension covering per-caller
// field protection. A schema says which fields NO authorized caller may write
// (see the FieldWritePolicy proto option); this says which further fields THIS
// caller may not, so the two together answer "the owner may reassign a task, a
// viewer may not" — a rule a schema enum cannot state.
//
// It widens, never narrows. The effective set for a write is the schema's marks
// plus whatever this returns, so a policy cannot hand a caller a field the
// schema reserved for trusted writes.
//
// Both halves are enforced identically once resolved: a PATCH whose mask reaches
// a protected field is rejected, and a PUT keeps the stored value for one. What
// differs is what a client can predict — the TypeScript client mirrors the
// schema's marks and refuses such a PATCH at the call site, while a field this
// hook adds is only known to the server, so the client learns of it as a
// REJECTED ack.
//
// A Permissions that does not implement it protects exactly what the schemas
// mark, which for almost every entity is nothing.
type FieldPermissions interface {
	// ProtectedFields returns the top-level field names this caller may not
	// write in the named entity, beyond the ones its schema marks. Return nil
	// for "nothing further", which is the answer for most entities.
	//
	// Names are proto field names, and a oneof is named by its GROUP — the only
	// unit a mask may name it in — though naming a member resolves to the same
	// thing. A name that matches no field, or matches a key field, aborts the
	// RPC: the hook states a restriction, and one that silently protects nothing
	// is the wrong way for a permission control to fail.
	//
	// The engine resolves it at most once per entity per request, before the
	// write transaction opens, and reads the memoized answer again while
	// applying — so it carries the same cost contract as the rest of Permissions
	// (see the interface doc on execution context) and the same purity
	// requirement as CanMutate.
	ProtectedFields(ctx context.Context, p Principal, workspaceID, entity string) ([]string, error)
}

// RowWriteFilter decides whether one write may change an existing row. stored
// is the row the write replaces, patches or deletes, decoded as it stands inside
// the write transaction, and w.Crud is always set. Return nil to admit the
// write, or a Rejection (via Reject) to refuse it; any other error aborts the
// RPC.
//
// It runs while the workspace write lock is held and may run again when the
// storage adapter retries the transaction, so it must be fast and a pure
// function of its arguments and of what it closed over when WriteFilter
// returned it. It must not retain or modify stored.
type RowWriteFilter func(w Write, stored proto.Message) error

// RowWritePermissions is an optional Permissions extension for write rules that
// depend on the row a write targets, such as "only the owner may delete a
// task". CanMutate sees only the write, so it cannot state such a rule.
//
// Every write that is not Trusted and that targets a live row is checked
// against the stored row before it is applied: a PUT that replaces the row, a
// PATCH, and a DELETE. A create, which is a write to a key with no live row, is
// not checked and is left to CanMutate, which sees the new value. The rows a
// custom mutator's body writes are not checked either; the verdict on the
// invocation covers them.
//
// The check for an entity is the RowWriteFilter that WriteFilter returns. When
// it returns nil, or the Permissions does not implement this interface, the
// check is the entity's ReadFilter: a caller may change a row exactly when they
// may read it, and a write to a row the ReadFilter hides is refused with
// CodePermissionDenied. An entity with neither filter is not checked and costs
// no extra read.
//
// A stored row that does not decode cannot be checked, so a write to it is
// refused with CodePermissionDenied whenever the entity has a check; a Trusted
// write repairs it. A refused write does not disclose the row's version.
//
//	func (p *policy) WriteFilter(
//		_ context.Context, who server.Principal, _, entity string,
//	) (server.RowWriteFilter, error) {
//		if entity != "task" {
//			return nil, nil // writable exactly when readable
//		}
//		member := p.engagementsOf(who) // cached lookup, resolved once per request
//		return func(w server.Write, stored proto.Message) error {
//			task := stored.(*taskv1.Task)
//			if !member[task.GetEngagementId()] {
//				return server.Reject(server.CodePermissionDenied, "no access to this task")
//			}
//			if w.Crud.GetOp() == syncplanev1.CrudOp_CRUD_OP_DELETE && task.GetOwner() != who.Subject {
//				return server.Reject(server.CodePermissionDenied, "only the owner may delete this task")
//			}
//			return nil
//		}, nil
//	}
type RowWritePermissions interface {
	// WriteFilter returns the check for writes to existing rows of the named
	// entity, or nil to check them with the entity's ReadFilter.
	//
	// The engine resolves it at most once per entity per request, before the
	// write transaction opens — except on Server.Mutate, which discovers its
	// writes inside the transaction (see the Permissions doc on execution
	// context). It carries the cost contract of ReadFilter, and the returned
	// filter must close over p rather than consult ctx.
	WriteFilter(ctx context.Context, p Principal, workspaceID, entity string) (RowWriteFilter, error)
}

// RoomDenial is a PERMANENT "no" from CanJoinRoom: this caller may not be in
// this room, and retrying will not change that. It drops the topic from the
// caller's Watch stream with reason ACCESS_REVOKED, so a client stops showing
// the room instead of reconnecting forever.
//
// It is distinct from WorkspaceDenial because it is narrower: the caller keeps
// the workspace and everything else in it, and loses one room.
type RoomDenial struct {
	// Message explains the denial. It reaches the client, so keep it free of
	// detail the caller has not earned.
	Message string
}

func (d *RoomDenial) Error() string { return "room access denied: " + d.Message }

// DenyRoom builds the permanent denial CanJoinRoom returns when the caller may
// not be in the room.
//
// Return it only when the answer is settled. A lookup that FAILED must return
// its underlying error instead: that drops the topic as a retryable error,
// where a denial tells the client to stop asking.
func DenyRoom(message string) error { return &RoomDenial{Message: message} }

// AsRoomDenial extracts a RoomDenial from an error chain.
func AsRoomDenial(err error) (*RoomDenial, bool) {
	var d *RoomDenial
	if errors.As(err, &d) {
		return d, true
	}
	return nil, false
}

// ParticipantFilter decides per participant whether a caller may see it. The
// argument is the server-stamped subject of the publishing session, empty for
// an unauthenticated one. A nil ParticipantFilter shows every participant.
//
// It is resolved once when a topic is joined and applied to that topic's
// snapshot and to every later frame, so it must be a pure function of the
// subject — the same reasoning RowFilter is warmed once per stream under.
type ParticipantFilter func(subject string) bool

// RoomPermissions is an optional Permissions extension covering the ephemeral
// plane. A Permissions that does not implement it admits every caller who
// cleared the room's own gate — workspace access, and the anchor row's read
// filter for an anchored topic — and shows them every participant.
//
// Reach for it when those two are not the whole answer. Being able to READ a
// document is not the same as being allowed to know who else is looking at it:
// an app where a reviewer browses anonymously, or where an admin's attention is
// not something the author should see, needs the roster filtered even though
// every one of those callers may read the row the room is anchored to.
//
// Both hooks run on the join gate, and CanJoinRoom runs again on the stream's
// heartbeat, so a settled denial takes effect within one heartbeat interval
// rather than whenever the client next reconnects. Neither runs per event.
//
// Embed AllowAllRooms to implement one hook and inherit the other.
type RoomPermissions interface {
	// CanJoinRoom admits or refuses one topic. Return DenyRoom for a settled
	// no; any other error drops the topic as retryable, so an outage never
	// reads as a revocation.
	CanJoinRoom(ctx context.Context, p Principal, workspaceID string, topic presence.Topic) error
	// VisibleParticipants returns the filter applied to this caller's view of
	// the room, or nil to show everyone.
	VisibleParticipants(ctx context.Context, p Principal, workspaceID string, topic presence.Topic) (ParticipantFilter, error)
}

// AllowAllRooms is the permissive RoomPermissions: every caller who cleared the
// room's own gate may join, and sees everyone. Embed it to implement only the
// hook you need:
//
//	type policy struct {
//		server.AllowAll
//		server.AllowAllRooms
//	}
//
//	func (policy) VisibleParticipants(
//		_ context.Context, p server.Principal, _ string, _ presence.Topic,
//	) (server.ParticipantFilter, error) {
//		if p.IsAnonymous() {
//			return func(string) bool { return false }, nil
//		}
//		return nil, nil
//	}
type AllowAllRooms struct{}

func (AllowAllRooms) CanJoinRoom(context.Context, Principal, string, presence.Topic) error {
	return nil
}

func (AllowAllRooms) VisibleParticipants(
	context.Context, Principal, string, presence.Topic,
) (ParticipantFilter, error) {
	return nil, nil
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

// authorizeWrite is the single gate an enforced write passes through: the
// engine's own schema-level rules first, then the app's policy. Push, Apply,
// Mutate and the mutator API call this instead of Permissions.CanMutate
// directly, so a rule a schema states cannot end up enforced on some write
// paths and not others.
//
// Trusted callers never reach it — every call site sits behind the same
// `enforced` check that skips CanMutate — which is what leaves an operation no
// access rule admits to trusted writes.
//
// g is the request's write gate, non-nil for every enforced write. Resolving it
// here rather than at apply time is what keeps FieldPermissions, WriteFilter and
// the ReadFilter a row check falls back to off the write lock: the answers this
// call memoizes are the ones the PUT carry-over and the row check read inside
// the transaction.
//
// The row check itself cannot run here — it needs the stored row, which only
// the write transaction can read without racing a concurrent write — so this
// only resolves it, and only once the write has cleared CanMutate.
func (s *Server) authorizeWrite(
	ctx context.Context,
	p Principal,
	workspaceID string,
	g *writeGate,
	w Write,
) error {
	if err := s.checkWritePolicy(w); err != nil {
		return err
	}
	if err := s.checkFieldPolicy(g, w); err != nil {
		return err
	}
	if err := s.perms.CanMutate(ctx, p, workspaceID, w); err != nil {
		return err
	}
	return s.warmRowCheck(g, w)
}

// checkFieldPolicy enforces the per-caller half of field protection — the
// fields FieldPermissions adds to the ones the schema marks — and warms the
// gate's memo for the entity so the write path can carry protected fields over
// without consulting the hook under the write lock.
//
// The schema's own marks are already refused by checkWritePolicy, which needs no
// hook and no principal; this re-checks them only because the gate answers with
// the union, and reaching the same verdict twice costs one scan of a slice that
// is empty for almost every entity.
//
// A custom mutation has no fields to protect: its rows are covered by the
// verdict on the invocation, exactly as the entity policies are.
func (s *Server) checkFieldPolicy(g *writeGate, w Write) error {
	if g == nil || w.Crud == nil {
		return nil
	}
	info, ok := s.catalogPtr().entities[w.Crud.GetEntity()]
	if !ok {
		return nil
	}
	protected, err := g.protected(info)
	if err != nil {
		return err
	}
	return refuseProtectedMask(info, w.Crud, protected)
}

// checkWritePolicy enforces the schema's own write rules that need no row on
// a CRUD write: the operations that no (syncplane.v1.entity).access rule
// admits, which only trusted writes may perform, and the
// (syncplane.v1.field_behavior) write_policy marks that put a field out of a
// client's reach.
//
// The field rule refuses PATCH and nothing else. A mask naming a protected field
// asks outright to change it; a PUT names no field, so it is not refused — the
// write path substitutes the stored value instead (see carryProtectedFields),
// which is a decision that needs the row and so cannot be made here.
//
// It runs BEFORE the app's policy so a schema-level "no" does not depend on
// that policy having an opinion, and it refuses with a Rejection because the
// answer is deterministic: the client should ack and drop the mutation rather
// than back off and retry a write that can never land. Whether a rule admits
// this caller at this row needs the row, so the write transaction decides
// that (see beginAccessWrite).
//
// Which operation a write can be is decided from the write alone, so this
// stays a pure function that needs no storage read and can run before the write
// transaction opens:
//
//   - PATCH is an update, always: it never upserts. Its mask names what it
//     changes, so it is refused when every update rule has a fields list
//     and no list names every field of the mask.
//   - DELETE is a delete.
//   - PUT asserting expected_version 0 is a create — that version means "only
//     if the row does not exist".
//   - PUT asserting any other version is an update — a row at that version
//     exists.
//   - PUT asserting nothing is a create or an update, depending on the stored
//     row, and is refused here only when no rule admits either. The write
//     transaction checks the one it turns out to be.
//
// Two things deliberately pass. An unknown entity does — applyCrud rejects it
// with a message about the entity, and re-deciding that here would only give
// the two paths a chance to disagree; an unspecified op passes for the same
// reason. An entity registered after this check and before the write applies
// is known by then, so applyCrud runs writePolicyOf again against the entity it
// applies with. So does a custom mutation: a mutator body's rows are covered by
// the Write{Custom} verdict on its invocation rather than authorized one by
// one.
func (s *Server) checkWritePolicy(w Write) error {
	crud := w.Crud
	if crud == nil {
		return nil
	}
	info, ok := s.catalogPtr().entities[crud.GetEntity()]
	if !ok {
		return nil
	}
	return writePolicyOf(info, crud)
}

// writePolicyOf is checkWritePolicy for one entity, as info declares it.
func writePolicyOf(info *entityInfo, crud *syncplanev1.CrudMutation) error {
	if err := refuseProtectedMask(info, crud, info.protectedFields); err != nil {
		return err
	}
	a := info.access
	if a == nil {
		return nil
	}
	refuse := func(what string) error {
		return Reject(CodePermissionDenied, fmt.Sprintf(
			"entity %q has no access rule that admits %s, so only a trusted write may %s its rows",
			info.name, what, what))
	}
	switch crud.GetOp() {
	case syncplanev1.CrudOp_CRUD_OP_DELETE:
		if !a.names(opDelete) {
			return refuse("delete")
		}
	case syncplanev1.CrudOp_CRUD_OP_PATCH:
		if !a.names(opUpdate) {
			return refuse("update")
		}
		if heads, ok := a.maskCovered(info, crud.GetFieldMask().GetPaths()); !ok {
			quoted := make([]string, len(heads))
			for i, h := range heads {
				quoted[i] = fmt.Sprintf("%q", h)
			}
			return Reject(CodePermissionDenied, fmt.Sprintf(
				"entity %q has no access rule that admits an update of %s together, so a PATCH masking them is refused",
				info.name, strings.Join(quoted, ", ")))
		}
	case syncplanev1.CrudOp_CRUD_OP_PUT:
		switch {
		case crud.ExpectedVersion == nil:
			if !a.names(opCreate) && !a.names(opUpdate) {
				return refuse("create or update")
			}
		case crud.GetExpectedVersion() == 0:
			if !a.names(opCreate) {
				return refuse("create")
			}
		default:
			if !a.names(opUpdate) {
				return refuse("update")
			}
		}
	}
	return nil
}
