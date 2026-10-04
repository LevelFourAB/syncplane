package server

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/levelfourab/syncplane/engine/storage"
)

// writeGate carries one enforced request's per-entity write rules into its
// writes: the fields the caller may not change (see protected) and the check a
// write to an existing row must pass (see rowCheck and RowWritePermissions).
//
// A nil gate enforces neither, which is what two kinds of write get: a Trusted
// one, and the rows a custom mutator's body writes (covered by the verdict on
// its invocation, exactly as the entity policies are). Every enforced write
// path builds one before it opens a transaction.
//
// Both rules are memoized per entity because each is asked twice: once while
// authorizing, outside the transaction, and again while applying, where the
// answer needs the stored row. Resolving once per entity per request is what
// keeps the hooks off the write lock — warmed while authorizing (see
// authorizeWrite), read while applying. The exception is Server.Mutate, which
// authorizes inside the transaction for the reason its doc gives, so warms the
// gate there too.
type writeGate struct {
	srv *Server
	ctx context.Context
	p   Principal
	ws  string
	// fieldHook is the policy's FieldPermissions, nil when it implements none.
	fieldHook FieldPermissions
	fieldMemo map[string][]string
	// rowMemo holds each resolved entity's row check. A present nil entry is a
	// resolved "no check", so the hooks are not asked again.
	rowMemo map[string]RowWriteFilter
	// access is what the caller holds in the access model, resolved inside
	// each write transaction; nil for a schema without the model.
	access *txAccess
	// creator is what an enforced create stores in each creator field, set
	// once creatorKnown is.
	creator      string
	creatorKnown bool
}

// newWriteGate builds the gate an enforced request carries into its writes, or
// nil when nothing is enforced.
func (s *Server) newWriteGate(ctx context.Context, adm admission, workspaceID string, enforced bool) *writeGate {
	if !enforced {
		return nil
	}
	hook, _ := s.perms.(FieldPermissions)
	return &writeGate{
		srv: s, ctx: ctx, p: adm.principal, ws: workspaceID, fieldHook: hook,
		access: newTxAccess(adm.caller, adm.access, s.limits, func(row storage.Row, err error) {
			s.reportPoison(ctx, workspaceID, row, err)
		}),
	}
}

// creatorSubject returns the caller's primary subject, which a create stores
// in each creator field. In a schema with the access model it is the one the
// workspace gate resolved, so the hook runs once per request.
func (g *writeGate) creatorSubject() string {
	if !g.creatorKnown {
		if g.access != nil {
			g.creator = g.access.caller.primary
		} else {
			g.creator = g.srv.primarySubjectOf(g.p)
		}
		g.creatorKnown = true
	}
	return g.creator
}

// accessOf returns the gate's access, nil for a nil gate.
func (g *writeGate) accessOf() *txAccess {
	if g == nil {
		return nil
	}
	return g.access
}

// rowCheck returns the check this caller's writes to existing rows of the
// entity must pass, or nil when they need none. A nil gate checks nothing. See
// RowWritePermissions for how the check is chosen.
func (g *writeGate) rowCheck(info *entityInfo) (RowWriteFilter, error) {
	if g == nil {
		return nil, nil
	}
	if check, ok := g.rowMemo[info.name]; ok {
		return check, nil
	}
	check, err := g.resolveRowCheck(info)
	if err != nil {
		return nil, err
	}
	if g.rowMemo == nil {
		g.rowMemo = map[string]RowWriteFilter{}
	}
	g.rowMemo[info.name] = check
	return check, nil
}

func (g *writeGate) resolveRowCheck(info *entityInfo) (RowWriteFilter, error) {
	if hook, ok := g.srv.perms.(RowWritePermissions); ok {
		check, err := hook.WriteFilter(g.ctx, g.p, g.ws, info.name)
		if err != nil {
			return nil, fmt.Errorf("server: entity %q: resolving write filter: %w", info.name, err)
		}
		if check != nil {
			return check, nil
		}
	}
	filter, err := g.srv.perms.ReadFilter(g.ctx, g.p, g.ws, info.name)
	if err != nil {
		return nil, fmt.Errorf("server: entity %q: resolving read filter: %w", info.name, err)
	}
	if filter == nil {
		return nil, nil
	}
	return func(w Write, stored proto.Message) error {
		if filter(w.Crud.GetKey(), stored) {
			return nil
		}
		return refuseRow(info, w.Crud.GetKey())
	}, nil
}

// guardRow refuses a write to an existing row when the request's row check
// denies it. load returns the stored row and whether it is live; write builds
// the Write the check is handed. Neither is called when the entity has no
// check, so an unchecked write costs no read, and write is not called for an
// absent row or a tombstone, which have nothing to check.
//
// Callers run it before anything else that reads the row can answer the
// caller: a compare-and-swap conflict names the row's version, which a caller
// refused here must not learn.
func (g *writeGate) guardRow(
	info *entityInfo,
	key string,
	load func() (storage.Row, bool, error),
	write func() (Write, error),
) error {
	check, err := g.rowCheck(info)
	if err != nil || check == nil {
		return err
	}
	row, live, err := load()
	if err != nil || !live {
		return err
	}
	stored, err := info.decode(row.Value)
	if err != nil {
		// The check cannot run on a row nobody can read, and admitting the write
		// would let any caller overwrite or delete it. A Trusted write carries no
		// gate, so it remains the repair.
		g.srv.reportPoison(g.ctx, g.ws, row, err)
		return refuseRow(info, key)
	}
	w, err := write()
	if err != nil {
		return err
	}
	return check(w, stored)
}

// refuseRow is the verdict on a write to a row the caller may not change. It
// names only what the caller sent, and reads the same whether the row was
// hidden or would not decode.
func refuseRow(info *entityInfo, key string) error {
	return Reject(CodePermissionDenied,
		fmt.Sprintf("entity %q: row %q may not be changed by this caller", info.name, key))
}

// warmRowCheck resolves the row check for a CRUD write's entity into the gate,
// so the write path reads it from the memo instead of calling a hook under the
// write lock. A custom mutation has none: its body's rows are covered by the
// verdict on the invocation.
func (s *Server) warmRowCheck(g *writeGate, w Write) error {
	if g == nil || w.Crud == nil {
		return nil
	}
	info, ok := s.catalogPtr().entities[w.Crud.GetEntity()]
	if !ok {
		return nil
	}
	_, err := g.rowCheck(info)
	return err
}
