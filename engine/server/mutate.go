package server

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/storage"
)

// Mutate runs fn inside a write transaction against the workspace and notifies
// connected clients if fn wrote anything.
//
// Prefer Apply. Mutate holds the workspace's write lock for the whole of fn, so
// any work fn does serializes every other writer to that workspace — and an
// adapter may hold something wider than one workspace, as the shipped SQLite
// store does; and because fn's writes are only discovered as it runs, the
// permission hook has to run inside the transaction (see below). Apply has
// neither property. What Mutate offers that Apply does not is a genuinely
// serializable read-then-write and writes that stream rather than buffer, which
// is what bulk imports and scan-driven work need.
//
// # Permissions
//
// Each Put/Patch/Delete is authorized with CanMutate against the ctx principal,
// carrying the same CrudMutation a client push would have — so a policy cannot
// tell the two apart — and a write to an existing row then passes the row check
// (see RowWritePermissions). The workspace gate (CanAccessWorkspace) runs first,
// before the transaction opens. A ctx with no principal authorizes as
// anonymous, which a deny-by-default policy refuses.
//
// System work that has no caller to authorize — connectors, migrations, cron —
// marks its ctx with Trusted, which skips both hooks. See Trusted for why the
// bypass is opt-in.
//
// This is the ONE place the engine consults a permission hook while holding a
// lock, and it is confined here because nothing else can know fn's writes in
// advance. The Permissions contract already requires CanMutate to be fast, free
// of blocking I/O and side-effect free, which is what keeps that survivable; a
// policy that does I/O should use Apply, where the hook runs between
// transactions.
//
// Differences from a client push:
//
//   - No client_id/mutation_id: client watermarks are untouched and there is
//     no idempotency — calling Mutate twice applies twice. Callers that retry
//     (an HTTP handler, an at-least-once effects handler) want
//     MutateIdempotent, which guards the same closure with a key.
//   - A mutator invoked through Invoke has its rows authorized by the verdict
//     on the invocation, not per row — exactly as on push.
//
// All writes through the tx share ONE workspace sequence, allocated lazily
// on the first Put/Delete — a call that only reads consumes no sequence,
// notifies nobody, and leaves a workspace that did not exist unregistered
// (see the storage.Adapter WithWriteTx contract), so probing an arbitrary
// workspace id is free of lasting effects.
//
// fn may be invoked multiple times (the storage adapter may retry the
// transaction): reset any captured state at the top of fn and keep side
// effects out of it until Mutate returns nil. Returning an error (including
// Reject, which AsRejection recovers on the caller's side) rolls the
// transaction back and surfaces the error.
//
// Do not call Mutate (or Invoke/CollectGarbage) from inside a mutator or
// another write closure: the nested transaction waits for the write lock the
// enclosing one already holds, so it deadlocks. When the re-entry threads the
// mutator's context such calls fail fast with an error instead.
func (s *Server) Mutate(ctx context.Context, workspaceID string, fn func(tx *EntityTx) error) error {
	if workspaceID == "" {
		return fmt.Errorf("server: workspace id is required")
	}
	if inWriteTx(ctx) {
		return fmt.Errorf("server: Mutate called re-entrantly inside a write transaction (workspace %q): a nested write transaction waits for the write lock the enclosing one holds and would deadlock; perform writes through the enclosing EntityTx instead", workspaceID)
	}
	adm, enforced, err := s.gate(ctx, workspaceID)
	if err != nil {
		return err
	}
	authz, gate := s.writeAuthorizer(ctx, adm, workspaceID, enforced)
	wrote := false
	err = s.store.WithWriteTx(ctx, workspaceID, func(tx storage.WriteTx) error {
		actor, err := actorOf(ctx)
		if err != nil {
			return err
		}
		// A fresh EntityTx per attempt keeps the closure re-runnable: a
		// retried attempt re-allocates its sequence instead of stamping rows
		// with one a rolled-back attempt consumed.
		if err := gate.accessOf().begin(tx); err != nil {
			return err
		}
		etx := &EntityTx{srv: s, ctx: ctx, ws: workspaceID, tx: tx, authz: authz, gate: gate, actor: actor,
			access: gate.accessOf()}
		if err := fn(etx); err != nil {
			return err
		}
		wrote = etx.wrote
		return nil
	})
	if err != nil {
		return err
	}
	if wrote {
		s.notifier.Notify(workspaceID)
	}
	return nil
}

// Invoke runs a registered mutator by name through Mutate, reusing
// client-triggered mutator logic for server-side triggers. args must match the
// registered prototype's message type, or be nil for the prototype's zero
// value; a mutator registered without a prototype takes nil and rejects
// anything else. The body sees what a push would have handed it (see
// MutatorFunc).
//
// # Permissions
//
// The workspace gate runs, then CanMutate is called ONCE with a Write{Custom}
// naming the mutator and its args — the rows the mutator body writes are not
// authorized individually. That is exactly what a custom mutation gets on the
// push path, and deliberately so: a mutator is server-authoritative logic, and
// the decision a policy makes is whether this caller may invoke this verb. Do
// not register a mutator as a way to launder writes a policy would refuse.
//
// In a schema that uses the access model, the invocation is also checked
// against the mutator's MutatorAccess inside the write transaction, before the
// body runs, and refused when the mutator declares none.
//
// Mark the ctx with Trusted for system triggers that have no caller.
//
// It inherits Mutate's lack of idempotency: a trigger that may fire twice wants
// InvokeIdempotent.
func (s *Server) Invoke(ctx context.Context, workspaceID, mutatorName string, args proto.Message) error {
	if inWriteTx(ctx) {
		return fmt.Errorf("server: Invoke(%q) called re-entrantly inside a write transaction (workspace %q): a nested write transaction waits for the write lock the enclosing one holds and would deadlock; perform writes through the enclosing EntityTx instead", mutatorName, workspaceID)
	}
	mi, bodyArgs, err := s.mutatorFor(mutatorName, args)
	if err != nil {
		return err
	}
	access, err := s.authorizeInvoke(ctx, workspaceID, mutatorName, args)
	if err != nil {
		return err
	}
	// The invocation is authorized; the body writes as server-authoritative
	// logic, so the inner Mutate must not re-gate or authorize per row.
	return s.Mutate(Trusted(ctx), workspaceID, func(tx *EntityTx) error {
		if err := s.beginInvocation(ctx, workspaceID, tx, access, mi, bodyArgs); err != nil {
			return err
		}
		return mi.fn(markWriteTx(ctx), tx, bodyArgs)
	})
}

// beginInvocation checks an authorized invocation against the access model
// inside its write transaction, and gives the body's EntityTx the invoking
// caller's access for Can. Shared by Invoke and InvokeIdempotent.
func (s *Server) beginInvocation(
	ctx context.Context,
	workspaceID string,
	tx *EntityTx,
	access *txAccess,
	mi *mutatorInfo,
	args proto.Message,
) error {
	// Per attempt: the adapter may run the closure again on a new transaction.
	if err := access.begin(tx.tx); err != nil {
		return err
	}
	if err := s.checkInvocation(ctx, workspaceID, tx.tx, access, mi, args); err != nil {
		return err
	}
	tx.access = access
	return nil
}

// authorizeInvoke runs the workspace gate and the single Write{Custom} verdict
// for a mutator invocation, before any transaction opens. Shared by Invoke and
// InvokeIdempotent. It returns the caller's access in the access model, which
// the invocation is checked against inside the transaction; nil for a Trusted
// call and in a schema without the model.
func (s *Server) authorizeInvoke(ctx context.Context, workspaceID, mutatorName string, args proto.Message) (*txAccess, error) {
	adm, enforced, err := s.gate(ctx, workspaceID)
	p := adm.principal
	if err != nil {
		return nil, err
	}
	if !enforced {
		return nil, nil
	}
	custom := &syncplanev1.CustomMutation{Name: mutatorName}
	if args != nil {
		packed, err := anypb.New(args)
		if err != nil {
			return nil, fmt.Errorf("server: mutator %q: packing args for authorization: %w", mutatorName, err)
		}
		custom.Args = packed
	}
	// No field gate: a custom mutation has no fields of its own to protect, and
	// the rows its body writes are covered by this one verdict.
	if err := s.authorizeWrite(ctx, p, workspaceID, nil, Write{Custom: custom}); err != nil {
		return nil, err
	}
	return newTxAccess(adm.caller, adm.access, s.limits, func(row storage.Row, err error) {
		s.reportPoison(ctx, workspaceID, row, err)
	}), nil
}

// mutatorFor resolves a registered mutator and returns the args its body is to
// be called with — the lookup both Invoke and InvokeIdempotent run before
// opening a transaction, so a name or type mistake fails without touching
// storage.
//
// The returned args carry the same nil-contract the push path gives a body, so
// one written against push (asserting its prototype's type unconditionally)
// behaves identically when a server-side trigger invokes it: a mutator
// registered with a prototype always gets a non-nil message of that type —
// zeroed when the caller passed none, as applyCustom does for a wire mutation
// with no args — and one registered without a prototype always gets nil, so
// passing args to it is a caller mistake rather than something the body could
// have read on push.
//
// Only the body sees the substitute. Authorization still runs on what the
// caller actually passed (see authorizeInvoke), matching push, where an
// args-less custom mutation reaches the policy with no Any attached.
func (s *Server) mutatorFor(mutatorName string, args proto.Message) (*mutatorInfo, proto.Message, error) {
	mi, ok := s.mutators[mutatorName]
	if !ok {
		return nil, nil, fmt.Errorf("server: unknown mutator %q", mutatorName)
	}
	if mi.args == nil {
		if args != nil {
			return nil, nil, fmt.Errorf("server: mutator %q takes no args, got %s",
				mutatorName, args.ProtoReflect().Descriptor().FullName())
		}
		return mi, nil, nil
	}
	if args == nil {
		zeroed := proto.Clone(mi.args)
		proto.Reset(zeroed)
		return mi, zeroed, nil
	}
	want := mi.args.ProtoReflect().Descriptor().FullName()
	got := args.ProtoReflect().Descriptor().FullName()
	if want != got {
		return nil, nil, fmt.Errorf("server: mutator %q expects args of type %s, got %s", mutatorName, want, got)
	}
	return mi, args, nil
}
