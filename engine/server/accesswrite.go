package server

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/storage"
)

// txAccess resolves what one caller holds inside a write transaction. The
// writes of one request see each other, so a grant row written early in a
// batch counts for the writes after it: the set is resolved from the
// transaction itself, and resolved again after a write that can change it.
type txAccess struct {
	caller *accessCaller
	limits Limits
	tx     storage.ReadTx
	set    *accessSet
}

// newTxAccess returns the access of caller for its write transactions, or nil
// for a caller outside the access model.
func newTxAccess(caller *accessCaller, limits Limits) *txAccess {
	if caller == nil {
		return nil
	}
	return &txAccess{caller: caller, limits: limits}
}

// get returns what the caller holds as of tx.
func (a *txAccess) get(tx storage.ReadTx) (*accessSet, error) {
	if a.set != nil && a.tx == tx {
		return a.set, nil
	}
	set, err := resolveAccess(tx, a.caller, a.limits)
	if err != nil {
		return nil, err
	}
	a.tx, a.set = tx, set
	return set, nil
}

// invalidate drops the resolved set. A write path calls it when writes it
// made are rolled back, since the set may have been read after them.
func (a *txAccess) invalidate() {
	if a != nil {
		a.set = nil
	}
}

// noteWrite drops the resolved set after a write to info that can change what
// the caller holds: a grant row, or a row that can be an anchor.
func (a *txAccess) noteWrite(info *entityInfo) {
	if a != nil && (info.grant != nil || a.caller.model.anchors[info.name]) {
		a.set = nil
	}
}

// model returns the access model the caller was resolved against.
func (a *txAccess) model() *accessModel {
	return a.caller.model
}

// holdsAt reports whether the set holds perm at the row of info with key: at
// the workspace root, at the row itself, or at the row its access parent
// points at. row is the row's decoded value, or nil for a row that does not
// exist, which has no parent.
func (s *accessSet) holdsAt(model *accessModel, perm string, info *entityInfo, key string, row protoreflect.Message) bool {
	var parent *anchorRow
	if row != nil && info.access != nil && info.access.parent != nil {
		if parentKey, ok := parentKeyOf(row, info.access, model.parents[info.name]); ok {
			parent = &anchorRow{entity: info.access.parentEntity, key: parentKey}
		}
	}
	return s.holdsUnder(perm, anchorRow{entity: info.name, key: key}, parent)
}

// holdsUnder reports whether the set holds perm at row: at the workspace
// root, at the row itself, or at parent when it is non-nil. It is the rule
// the client's canAt mirrors; testdata/access/can.json pins both.
func (s *accessSet) holdsUnder(perm string, row anchorRow, parent *anchorRow) bool {
	anchors := s.perms[perm]
	if anchors.holds(row.entity, row.key) {
		return true
	}
	return parent != nil && anchors.holds(parent.entity, parent.key)
}

// heldAt reports whether the set holds perm at one anchor; a nil anchor is the
// workspace root.
func (s *accessSet) heldAt(perm string, at *anchorRow) bool {
	a := s.perms[perm]
	if a == nil {
		return false
	}
	if a.root {
		return true
	}
	if at == nil {
		return false
	}
	_, ok := a.rows[*at]
	return ok
}

// grantAnchorOf returns the anchor a grant row applies at, nil for the
// workspace root. ok is false when the row anchors at nothing.
func grantAnchorOf(model *accessModel, info *entityInfo, row protoreflect.Message) (*anchorRow, bool) {
	if info.access.parent == nil {
		return nil, true
	}
	parentKey, ok := parentKeyOf(row, info.access, model.parents[info.name])
	if !ok {
		return nil, false
	}
	return &anchorRow{entity: info.access.parentEntity, key: parentKey}, true
}

// touchesGrant reports whether a write to a grant entity can change what one
// of its rows grants: a PUT or a DELETE always can, a PATCH when its mask
// reaches the subject, the role or the access parent.
func touchesGrant(info *entityInfo, op syncplanev1.CrudOp, mask []string) bool {
	if info.grant == nil {
		return false
	}
	if op != syncplanev1.CrudOp_CRUD_OP_PATCH {
		return true
	}
	fields := []string{string(info.grant.subject.Name()), string(info.grant.role.Name())}
	if info.access.parent != nil {
		fields = append(fields, string(info.access.parent.fd.Name()))
	}
	for _, path := range mask {
		head, _, _ := strings.Cut(path, ".")
		for _, f := range fields {
			if head == f {
				return true
			}
		}
	}
	return false
}

// checkGrantSubset refuses a grant row whose role grants a permission the
// writer does not hold where the row applies. A writer can therefore give and
// take away only what it holds itself: it cannot raise a grant to a stronger
// role, move a grant to an anchor it does not manage, or change a grant that
// holds more than it does.
//
// A row whose role is undeclared, or whose anchor is missing, grants nothing,
// and so passes.
func checkGrantSubset(set *accessSet, model *accessModel, info *entityInfo, key string, row protoreflect.Message) error {
	role := grantRoleOf(info.grant, row)
	perms := info.grant.roles[role]
	if len(perms) == 0 {
		return nil
	}
	at, ok := grantAnchorOf(model, info, row)
	if !ok {
		return nil
	}
	for _, p := range perms {
		if !set.heldAt(p, at) {
			return Reject(CodePermissionDenied, fmt.Sprintf(
				"entity %q: row %q grants role %q, which holds permissions this caller does not hold where the row applies",
				info.name, key, role))
		}
	}
	return nil
}

// checkGrantRow refuses a grant row that could never grant anything: an empty
// subject, a role the entity does not declare, or, on an entity with an access
// parent, no parent. Every write path runs it, Trusted ones included, on a PUT
// and on a PATCH that reaches one of those fields.
func (s *Server) checkGrantRow(info *entityInfo, key string, op syncplanev1.CrudOp, mask []string, msg proto.Message) error {
	if !touchesGrant(info, op, mask) {
		return nil
	}
	g := info.grant
	row := msg.ProtoReflect()
	if row.Get(g.subject).String() == "" {
		return Reject(CodeInvalidArgument, fmt.Sprintf(
			"entity %q: row %q has an empty %q; a grant row must name its subject", info.name, key, g.subject.Name()))
	}
	if role := grantRoleOf(g, row); g.roles[role] == nil {
		return Reject(CodeInvalidArgument, fmt.Sprintf(
			"entity %q: row %q holds role %q, which the entity does not declare", info.name, key, role))
	}
	if a := info.access; a.parent != nil {
		named := row.Has(a.parent.fd)
		if model := s.catalogPtr().access; named && model != nil {
			_, named = parentKeyOf(row, a, model.parents[info.name])
		}
		if !named {
			return Reject(CodeInvalidArgument, fmt.Sprintf(
				"entity %q: row %q names no %q; a grant row must name the row it applies at", info.name, key, a.parent.fd.Name()))
		}
	}
	return nil
}

// accessWrite carries one enforced CRUD write of a model entity through the
// access model's two checks. A nil accessWrite checks nothing, which is what a
// write outside the model, an unenforced write and a Trusted one get.
type accessWrite struct {
	access *txAccess
	info   *entityInfo
	key    string
	op     syncplanev1.CrudOp
	mask   []string
	// live and storedParent describe the row before the write: whether one
	// existed, and the key its access parent named.
	live         bool
	storedParent string
}

// beginAccessWrite runs the access model's check against the stored row, and
// returns the write for finish. Callers run it before the version check, so a
// refused caller does not learn the row's version.
//
// An update or a delete needs the entity's write permission where the stored
// row sits. On a grant entity, a write that can change what the row grants
// also needs every permission of the stored role at the stored anchor, so a
// caller cannot demote or remove a grant stronger than its own.
func (g *writeGate) beginAccessWrite(
	tx storage.ReadTx,
	info *entityInfo,
	key string,
	op syncplanev1.CrudOp,
	mask []string,
	load func() (storage.Row, bool, error),
) (*accessWrite, error) {
	if g == nil || g.access == nil || info.access == nil || info.access.unrestricted {
		return nil, nil
	}
	w := &accessWrite{access: g.access, info: info, key: key, op: op, mask: mask}
	row, live, err := load()
	if err != nil || !live {
		return w, err
	}
	stored, err := info.decode(row.Value)
	if err != nil {
		// Nothing can show that the caller may change a row nobody can read. A
		// Trusted write repairs it.
		g.srv.reportPoison(g.ctx, g.ws, row, err)
		return nil, refuseRow(info, key)
	}
	set, err := g.access.get(tx)
	if err != nil {
		return nil, err
	}
	model := g.access.model()
	sr := stored.ProtoReflect()
	if !set.holdsAt(model, info.access.write, info, key, sr) {
		return nil, refuseRow(info, key)
	}
	if touchesGrant(info, op, mask) {
		if err := checkGrantSubset(set, model, info, key, sr); err != nil {
			return nil, err
		}
	}
	w.live = true
	w.storedParent, _ = parentKeyOf(sr, info.access, model.parents[info.name])
	return w, nil
}

// finish runs the access model's check against the row as it will be stored:
// after the mask merge, the protected-field carry-over and the reference
// projection, before the index update.
//
// A create needs the write permission where the new row sits, and so does an
// update that moves the row to another access parent. On a grant entity, a
// write that can change what the row grants also needs every permission of
// the new role at the new anchor, so a caller cannot grant more than it holds.
func (w *accessWrite) finish(tx storage.ReadTx, final proto.Message) error {
	if w == nil {
		return nil
	}
	set, err := w.access.get(tx)
	if err != nil {
		return err
	}
	model := w.access.model()
	fr := final.ProtoReflect()
	parent, _ := parentKeyOf(fr, w.info.access, model.parents[w.info.name])
	if !w.live || parent != w.storedParent {
		if !set.holdsAt(model, w.info.access.write, w.info, w.key, fr) {
			return refuseRow(w.info, w.key)
		}
	}
	if touchesGrant(w.info, w.op, w.mask) {
		return checkGrantSubset(set, model, w.info, w.key, fr)
	}
	return nil
}

// checkInvocation refuses a mutator invocation that the caller's access does
// not cover; see MutatorAccess. access is nil for a Trusted invocation and in
// a schema without the access model, and then nothing is checked.
func (s *Server) checkInvocation(
	ctx context.Context,
	ws string,
	tx storage.ReadTx,
	access *txAccess,
	mi *mutatorInfo,
	args proto.Message,
) error {
	if access == nil {
		return nil
	}
	ma := mi.access
	if ma == nil {
		return Reject(CodePermissionDenied, fmt.Sprintf(
			"mutator %q declares no MutatorAccess, so only a trusted caller may invoke it", mi.name))
	}
	set, err := access.get(tx)
	if err != nil {
		return err
	}
	if ma.Entity == "" {
		if set.heldAt(ma.Permission, nil) {
			return nil
		}
		return Reject(CodePermissionDenied, fmt.Sprintf("mutator %q may not be invoked by this caller", mi.name))
	}
	refuse := func(key string) error {
		return Reject(CodePermissionDenied, fmt.Sprintf(
			"mutator %q may not be invoked on %s %q by this caller", mi.name, ma.Entity, key))
	}
	info, ok := s.catalogPtr().entities[ma.Entity]
	if !ok {
		return refuse("")
	}
	key := args.ProtoReflect().Get(args.ProtoReflect().Descriptor().Fields().ByName(protoreflect.Name(ma.KeyField))).String()
	row, err := tx.GetRow(info.name, key)
	if err == storage.ErrNotFound || (err == nil && row.Deleted) {
		return refuse(key)
	}
	if err != nil {
		return err
	}
	msg, err := info.decode(row.Value)
	if err != nil {
		s.reportPoison(ctx, ws, row, err)
		return refuse(key)
	}
	if !set.holdsAt(access.model(), ma.Permission, info, key, msg.ProtoReflect()) {
		return refuse(key)
	}
	return nil
}

// Can reports whether the caller of this transaction holds permission at the
// row of the entity prototype names, with the given key: at the row itself,
// at the row its access parent points at, or at the workspace root. For a row
// that does not exist only the root counts.
//
// The caller is the one a mutator was invoked by, or the one Mutate, Apply or
// a push runs for. A Trusted transaction, and any transaction in a schema
// without the access model, holds every permission. A mutator body uses Can to
// apply the model to the rows it writes, which are not checked otherwise.
//
// The answer reflects the writes this transaction has made so far.
func (e *EntityTx) Can(permission string, prototype proto.Message, key string) (bool, error) {
	if e.access == nil {
		return true, nil
	}
	info, err := e.info(prototype)
	if err != nil {
		return false, err
	}
	set, err := e.access.get(e.tx)
	if err != nil {
		return false, err
	}
	row, live, err := e.liveRow(info, key)
	if err != nil {
		return false, err
	}
	var msg protoreflect.Message
	if live {
		decoded, err := info.decode(row.Value)
		if err != nil {
			e.srv.reportPoison(e.ctx, e.ws, row, err)
			return false, nil
		}
		msg = decoded.ProtoReflect()
	}
	return set.holdsAt(e.access.model(), permission, info, key, msg), nil
}
