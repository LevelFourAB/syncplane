package server

import (
	"context"
	"errors"
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
// The parent chains it walks are read the same way.
type txAccess struct {
	caller *accessCaller
	limits Limits
	report func(storage.Row, error)
	tx     storage.ReadTx
	set    *accessSet
	// chains walks parent chains over chainsTx, and is dropped after a write
	// to a row that can sit on a chain.
	chainsTx storage.ReadTx
	chains   *accessChains
}

// newTxAccess returns the access of caller for its write transactions, or nil
// for a caller outside the access model. report is told about a row on a
// parent chain that does not decode.
func newTxAccess(caller *accessCaller, limits Limits, report func(storage.Row, error)) *txAccess {
	if caller == nil {
		return nil
	}
	return &txAccess{caller: caller, limits: limits, report: report}
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

// chainsOf returns a parent-chain walker over tx.
func (a *txAccess) chainsOf(tx storage.ReadTx) *accessChains {
	if a.chains == nil || a.chainsTx != tx {
		a.chains = newAccessChains(a.caller.model, tx, a.limits.maxAccessDepth(), a.report)
		a.chainsTx = tx
	}
	return a.chains
}

// invalidate drops the resolved set and the chains read so far. A write path
// calls it when writes it made are rolled back, since both may have been read
// after them.
func (a *txAccess) invalidate() {
	if a != nil {
		a.set, a.chains = nil, nil
	}
}

// noteWrite drops what a write to info can change: the resolved set after a
// write to a grant row or to a row that can be an anchor, and the chains read
// so far after a write to a row that can sit on a chain.
func (a *txAccess) noteWrite(info *entityInfo) {
	if a == nil {
		return
	}
	anchor := a.caller.model.anchors[info.name]
	if info.grant != nil || anchor {
		a.set = nil
	}
	if anchor {
		a.chains = nil
	}
}

// model returns the access model the caller was resolved against.
func (a *txAccess) model() *accessModel {
	return a.caller.model
}

// holds reports whether the caller holds perm at the row of info with key, as
// of tx: at the workspace root, at the row itself, or at a row up its parent
// chain. row is the row's decoded value, or nil for a row that does not
// exist, which has no parent.
func (a *txAccess) holds(tx storage.ReadTx, perm string, info *entityInfo, key string, row protoreflect.Message) (bool, error) {
	set, err := a.get(tx)
	if err != nil {
		return false, err
	}
	return a.chainsOf(tx).holds(set.perms[perm], info, key, row)
}

// holdsAt reports whether the caller holds perm at one anchor, as of tx: at
// the workspace root, at the anchor, or up its parent chain. A nil anchor is
// the workspace root, where only the root counts.
func (a *txAccess) holdsAt(tx storage.ReadTx, perm string, at *anchorRow) (bool, error) {
	set, err := a.get(tx)
	if err != nil {
		return false, err
	}
	anchors := set.perms[perm]
	if at == nil {
		return anchors != nil && anchors.root, nil
	}
	return a.chainsOf(tx).holdsAt(anchors, *at)
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
// A row whose role is undeclared, or whose access parent is unset, grants
// nothing, and so passes.
func checkGrantSubset(tx storage.ReadTx, access *txAccess, info *entityInfo, key string, row protoreflect.Message) error {
	role := grantRoleOf(info.grant, row)
	perms := info.grant.roles[role]
	if len(perms) == 0 {
		return nil
	}
	at, ok := grantAnchorOf(access.model(), info, row)
	if !ok {
		return nil
	}
	for _, p := range perms {
		held, err := access.holdsAt(tx, p, at)
		if err != nil {
			return err
		}
		if !held {
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

// checkAccessChain refuses a write that would put a row on a parent chain the
// model cannot walk: a row below itself, or a row more than
// Limits.MaxAccessDepth hops below a row without an access parent. Every
// write path runs it, Trusted ones included, on the row as it will be stored.
// Only an entity whose chain of parent entities is recursive can do either,
// since registration bounds every other chain, and only a write that changes
// the row's access parent is walked.
func (s *Server) checkAccessChain(
	ctx context.Context,
	ws string,
	tx storage.ReadTx,
	info *entityInfo,
	key string,
	load func() (storage.Row, bool, error),
	final proto.Message,
) error {
	model := s.catalogPtr().access
	if model == nil || info.access == nil || info.access.parent == nil || model.depths[info.name] != depthUnbounded {
		return nil
	}
	report := func(row storage.Row, err error) { s.reportPoison(ctx, ws, row, err) }
	e := edgeOf(model, info, final.ProtoReflect())
	if !e.ok {
		return nil
	}
	row, live, err := load()
	if err != nil {
		return err
	}
	if live {
		if stored, err := info.decode(row.Value); err == nil && edgeOf(model, info, stored.ProtoReflect()) == e {
			return nil
		}
	}
	maxDepth := s.limits.maxAccessDepth()
	chains := newAccessChains(model, tx, maxDepth, report)
	self := anchorRow{entity: info.name, key: key}
	for hops := 1; e.ok; hops++ {
		if e.parent == self {
			return Reject(CodeInvalidArgument, fmt.Sprintf(
				"entity %q: row %q would sit below itself in the access model", info.name, key))
		}
		if hops > maxDepth {
			return Reject(CodeInvalidArgument, fmt.Sprintf(
				"entity %q: row %q would sit more than MaxAccessDepth (%d) hops below a row without an access parent",
				info.name, key, maxDepth))
		}
		if e, err = chains.edge(e.parent); err != nil {
			return err
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
	// live, stored and storedParent describe the row before the write:
	// whether one existed, its value, and the key its access parent named.
	live         bool
	stored       protoreflect.Message
	storedParent string
}

// beginAccessWrite runs the access model's check against the stored row, and
// returns the write for finish. Callers run it before the version check, so a
// refused caller does not learn the row's version.
//
// An update or a delete needs the entity's write permission where the stored
// row sits. On a grant entity, a write that can change what the row grants
// also needs every permission of the stored role at the stored anchor, so a
// caller cannot demote or remove a grant stronger than its own. A delete of a
// row that other rows name as their access parent is refused while one of
// them is live: deleting it would leave them anchored at nothing, and a row
// created later with the same key would take them over.
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
	model := g.access.model()
	sr := stored.ProtoReflect()
	ok, err := g.access.holds(tx, info.access.write, info, key, sr)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, refuseRow(info, key)
	}
	if touchesGrant(info, op, mask) {
		if err := checkGrantSubset(tx, g.access, info, key, sr); err != nil {
			return nil, err
		}
	}
	if op == syncplanev1.CrudOp_CRUD_OP_DELETE && model.anchors[info.name] {
		below, err := liveRowBelow(tx, model, info, key, g.srv.limits)
		if err != nil {
			return nil, err
		}
		if below != "" {
			return nil, Reject(CodeConflict, fmt.Sprintf(
				"entity %q: row %q is the access parent of a live %s row; delete or move the rows below it first",
				info.name, key, below))
		}
	}
	w.live, w.stored = true, sr
	w.storedParent, _ = parentKeyOf(sr, info.access, model.parents[info.name])
	return w, nil
}

// finish runs the access model's check against the row as it will be stored:
// after the mask merge, the protected-field carry-over and the reference
// projection, before the index update.
//
// A create needs the write permission where the new row sits, and so does an
// update that moves the row to another access parent. A move also needs, where
// the row sits before it, the read permission of every entity whose rows can
// sit below it: the rows below move with it, and the caller would otherwise
// show them to whoever reads at the new parent. On a grant entity, a write
// that can change what the row grants also needs every permission of the new
// role at the new anchor, so a caller cannot grant more than it holds.
func (w *accessWrite) finish(tx storage.ReadTx, final proto.Message) error {
	if w == nil {
		return nil
	}
	model := w.access.model()
	fr := final.ProtoReflect()
	parent, _ := parentKeyOf(fr, w.info.access, model.parents[w.info.name])
	moved := w.live && parent != w.storedParent
	if !w.live || moved {
		ok, err := w.access.holds(tx, w.info.access.write, w.info, w.key, fr)
		if err != nil {
			return err
		}
		if !ok {
			return refuseRow(w.info, w.key)
		}
	}
	if moved {
		for _, perm := range model.readPermsBelow(w.info.name) {
			ok, err := w.access.holds(tx, perm, w.info, w.key, w.stored)
			if err != nil {
				return err
			}
			if !ok {
				return Reject(CodePermissionDenied, fmt.Sprintf(
					"entity %q: moving row %q needs permission %q where it is now, to carry the rows below it",
					w.info.name, w.key, perm))
			}
		}
	}
	if touchesGrant(w.info, w.op, w.mask) {
		return checkGrantSubset(tx, w.access, w.info, w.key, fr)
	}
	return nil
}

// liveRowBelow returns the name of an entity with a live row whose access
// parent is the row of info with key, or "" when there is none. It probes the
// reference index of each child entity, and scans the entity while that index
// is not ready.
func liveRowBelow(tx storage.ReadTx, model *accessModel, info *entityInfo, key string, limits Limits) (string, error) {
	values, ok := keyValuesOf(info, key)
	if !ok {
		return "", nil
	}
	for _, child := range model.children[info.name] {
		found := false
		stop := func() error {
			found = true
			return errCandidateBound
		}
		ci := child.indexByName(refIndexName(child.access.parent.ref))
		ready := false
		if ci != nil && !limits.DisableIndexes {
			var err error
			if ready, err = ci.ready(tx, child.name); err != nil {
				return "", err
			}
		}
		var err error
		if ready {
			lo, hi, perr := ci.probeRefIndex(values)
			if perr != nil {
				return "", perr
			}
			err = tx.ScanIndex(child.name, ci.name, lo, hi, 1, storage.ScanAsc, func([]byte, string) error {
				return stop()
			})
		} else {
			err = tx.ScanEntity(child.name, func(row storage.Row) error {
				msg, derr := child.decode(row.Value)
				if derr != nil {
					return nil
				}
				if parentKey, ok := parentKeyOf(msg.ProtoReflect(), child.access, info); ok && parentKey == key {
					return stop()
				}
				return nil
			})
		}
		if err != nil && !errors.Is(err, errCandidateBound) {
			return "", err
		}
		if found {
			return child.name, nil
		}
	}
	return "", nil
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
	if ma.Entity == "" {
		held, err := access.holdsAt(tx, ma.Permission, nil)
		if err != nil {
			return err
		}
		if held {
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
	held, err := access.holds(tx, ma.Permission, info, key, msg.ProtoReflect())
	if err != nil {
		return err
	}
	if !held {
		return refuse(key)
	}
	return nil
}

// Can reports whether the caller of this transaction holds permission at the
// row of the entity prototype names, with the given key: at the row itself,
// at a row up its chain of access parents, or at the workspace root. For a
// row that does not exist only the root counts.
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
	return e.access.holds(e.tx, permission, info, key, msg)
}
