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
	// admitted is the set the workspace gate read, which begin takes as the
	// transaction's own where it is held (see heldAt). verdict is the grant
	// check the gate left to the write transaction, which begin reaches.
	admitted *accessSet
	verdict  *pendingVerdict
	tx       storage.ReadTx
	set      *accessSet
	// chains walks parent chains over chainsTx, and is dropped after a write
	// to a row that can sit on a chain.
	chainsTx storage.ReadTx
	chains   *accessChains
}

// newTxAccess returns the access of the caller adm admitted for its write
// transactions, or nil for a caller outside the access model. report is told
// about a row on a parent chain that does not decode.
func newTxAccess(adm admission, limits Limits, report func(storage.Row, error)) *txAccess {
	if adm.caller == nil {
		return nil
	}
	return &txAccess{caller: adm.caller, admitted: adm.access, verdict: adm.verdict, limits: limits, report: report}
}

// begin starts one attempt of a write transaction. It drops what an earlier
// attempt resolved and, when tx reads the workspace at the position the
// workspace gate read the caller's grants at, takes the gate's set as what the
// caller holds as of tx. A transaction that has taken a sequence is past that
// position (see workspacePos), so a write path calls it before its first
// write; after one it only costs the read of the position.
//
// When the workspace gate left its grant check to the write transaction, begin
// reaches that verdict in tx and returns the gate's error, a gateError, for a
// refused caller. A write path then calls it before it reads anything else of
// tx, and the refused transaction writes nothing.
func (a *txAccess) begin(tx storage.ReadTx) error {
	if a == nil {
		return nil
	}
	a.invalidate()
	if a.verdict != nil {
		set, err := a.verdict.reach(tx)
		if err != nil {
			return err
		}
		a.admitted = set
	}
	if a.admitted == nil || a.admitted.caller != a.caller {
		return nil
	}
	pos, err := workspacePosOf(tx)
	if err != nil {
		return err
	}
	if a.admitted.heldAt(a.caller, pos) {
		// The workspace lock keeps every other write out until the
		// transaction ends, so the set stays what tx holds until a write of
		// the transaction's own drops it (see noteWrite).
		a.tx, a.set = tx, a.admitted
	}
	return nil
}

// settle is pendingVerdict.settle for the grant check a carries. A nil a, or
// one the workspace gate decided, returns err.
func (a *txAccess) settle(ctx context.Context, err error) error {
	if a == nil {
		return err
	}
	return a.verdict.settle(ctx, err)
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

// admits reports whether rule admits the caller at the row of info with key,
// as of tx. A rule with a permission needs it at the workspace root, at the
// row itself, or at a row up its parent chain. A rule that admits every
// admitted caller holds for every caller that passed the workspace gate,
// which is every caller with an access. A rule with a subject check also
// needs the row's subject field to hold one of the caller's subjects. row is
// the row's decoded value, or nil for a row that does not exist, which no
// subject check admits.
func (a *txAccess) admits(tx storage.ReadTx, rule accessRule, info *entityInfo, key string, row protoreflect.Message) (bool, error) {
	if rule.subject != nil && (row == nil || !rule.subjectHolds(a.caller.subjects, row)) {
		return false, nil
	}
	if rule.admitted() {
		return true, nil
	}
	return a.holds(tx, rule.permission, info, key, row)
}

// admitting returns the rules of info that name op and admit the caller at
// the row of info with key, as of tx, in declaration order.
func (a *txAccess) admitting(tx storage.ReadTx, op accessOp, info *entityInfo, key string, row protoreflect.Message) ([]accessRule, error) {
	var out []accessRule
	for _, rule := range info.access.rules {
		if !rule.names(op) {
			continue
		}
		ok, err := a.admits(tx, rule, info, key, row)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, rule)
		}
	}
	return out, nil
}

// grantAnchorOf returns the anchor a grant row applies at, nil for the
// workspace root. ok is false when the row anchors at nothing.
func grantAnchorOf(model *accessModel, info *entityInfo, row protoreflect.Message) (*anchorRow, bool) {
	if !info.access.hasParent() {
		return nil, true
	}
	parent, ok := parentOf(row, model.parents[info.name])
	if !ok {
		return nil, false
	}
	return &parent, true
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
	fields := []string{info.grant.subjectName, string(info.grant.role.Name())}
	if info.access.hasParent() {
		fields = append(fields, info.access.parentName)
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

// checkGrantRow refuses a grant row that could never grant anything: no
// subject, a role the entity does not declare, or, on an entity with an access
// parent, no parent. Every write path runs it, Trusted ones included, on a PUT
// and on a PATCH that reaches one of those fields.
func (s *Server) checkGrantRow(info *entityInfo, key string, op syncplanev1.CrudOp, mask []string, msg proto.Message) error {
	if !touchesGrant(info, op, mask) {
		return nil
	}
	g := info.grant
	row := msg.ProtoReflect()
	if !g.subjectSet(row, s.catalogPtr().access, info.name) {
		return Reject(CodeInvalidArgument, fmt.Sprintf(
			"entity %q: row %q has an empty %q; a grant row must name its subject", info.name, key, g.subjectName))
	}
	if role := grantRoleOf(g, row); g.roles[role] == nil {
		return Reject(CodeInvalidArgument, fmt.Sprintf(
			"entity %q: row %q holds role %q, which the entity does not declare", info.name, key, role))
	}
	if a := info.access; a.hasParent() {
		named := a.parentFieldSet(row)
		if model := s.catalogPtr().access; named && model != nil {
			_, named = parentOf(row, model.parents[info.name])
		}
		if !named {
			return Reject(CodeInvalidArgument, fmt.Sprintf(
				"entity %q: row %q names no %q; a grant row must name the row it applies at", info.name, key, a.parentName))
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
	if model == nil || info.access == nil || !info.access.hasParent() || model.depths[info.name] != depthUnbounded {
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
	// whether one existed, its value, and the row its access parent named.
	live         bool
	stored       protoreflect.Message
	storedParent chainEdge
	// updating are the update rules that admit the caller at the stored row.
	// One of them must also admit the row as it will be stored, and the
	// fields the write changes.
	updating []accessRule
}

// beginAccessWrite runs the access model's check against the stored row, and
// returns the write for finish. Callers run it before the version check, so a
// refused caller does not learn the row's version.
//
// A write to a live row is an update, or a delete for a DELETE, and needs a
// rule of the entity that names the operation and admits the caller where the
// stored row sits. On a grant entity, a write that can change what the row
// grants also needs every permission of the stored role at the stored anchor,
// so a caller cannot demote or remove a grant stronger than its own. A delete
// of a row that other rows name as their access parent, or that grant rows
// name as their subject, is refused while one of them is live: deleting it
// would leave them pointing at nothing, and a row created later with the same
// key would take them over.
func (g *writeGate) beginAccessWrite(
	tx storage.ReadTx,
	info *entityInfo,
	key string,
	op syncplanev1.CrudOp,
	mask []string,
	load func() (storage.Row, bool, error),
) (*accessWrite, error) {
	if g == nil || g.access == nil || info.access == nil {
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
	want := opUpdate
	if op == syncplanev1.CrudOp_CRUD_OP_DELETE {
		want = opDelete
	}
	admitting, err := g.access.admitting(tx, want, info, key, sr)
	if err != nil {
		return nil, err
	}
	if len(admitting) == 0 {
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
	if op == syncplanev1.CrudOp_CRUD_OP_DELETE && len(model.subjectsOf[info.name]) > 0 {
		grant, err := liveRowNaming(tx, info, key, model.subjectsOf[info.name], model.teamSubjects, g.srv.limits)
		if err != nil {
			return nil, err
		}
		if grant != "" {
			return nil, Reject(CodeConflict, fmt.Sprintf(
				"entity %q: row %q is the subject of a live %s row; delete the grant rows that name it first",
				info.name, key, grant))
		}
	}
	w.live, w.stored = true, sr
	w.storedParent = edgeOf(model, info, sr)
	if want == opUpdate {
		w.updating = admitting
	}
	return w, nil
}

// finish runs the access model's check against the row as it will be stored:
// after the mask merge, the protected-field carry-over, the creator stamp and
// the reference projection, before the index update.
//
// A create needs a create rule that admits the caller and the new row. An
// update needs one of the update rules that admitted the caller at the stored
// row to admit the row as it will be stored as well, and the fields the write
// changes. A move also needs, where the row sits before it, the right to read
// every entity whose rows can sit below it: the rows below move with it, and
// the caller would otherwise show them to whoever reads at the new parent. On
// a grant entity, a write that can change what the row grants also needs
// every permission of the new role at the new anchor, so a caller cannot
// grant more than it holds.
func (w *accessWrite) finish(tx storage.ReadTx, final proto.Message) error {
	if w == nil {
		return nil
	}
	model := w.access.model()
	fr := final.ProtoReflect()
	// The parent's entity counts as well as its key: a polymorphic parent can
	// move between rows of two entities that share a key.
	moved := w.live && edgeOf(model, w.info, fr) != w.storedParent
	switch {
	case !w.live:
		admitting, err := w.access.admitting(tx, opCreate, w.info, w.key, fr)
		if err != nil {
			return err
		}
		if len(admitting) == 0 {
			return refuseRow(w.info, w.key)
		}
	default:
		admitted, err := admitsUpdate(w.updating,
			func() []string { return changedUnits(w.info.msgType.Descriptor(), w.stored, fr) },
			func(rule accessRule) (bool, error) {
				// Unmoved, the row sits on the chain the rule's source was
				// checked against, so only a subject check can answer
				// differently.
				if !moved && rule.subject == nil {
					return true, nil
				}
				return w.access.admits(tx, rule, w.info, w.key, fr)
			})
		if err != nil {
			return err
		}
		if !admitted {
			return refuseRow(w.info, w.key)
		}
		if !moved {
			break
		}
		set, err := w.access.get(tx)
		if err != nil {
			return err
		}
		chains := w.access.chainsOf(tx)
		for _, r := range model.readersBelow(w.info.name) {
			ok, err := chains.holds(set.readAnchors(r), w.info, w.key, w.stored)
			if err != nil {
				return err
			}
			if !ok {
				return Reject(CodePermissionDenied, fmt.Sprintf(
					"entity %q: moving row %q needs one of the permissions %s where it is now, to carry the rows below it",
					w.info.name, w.key, strings.Join(r.perms, ", ")))
			}
		}
	}
	if touchesGrant(w.info, w.op, w.mask) {
		return checkGrantSubset(tx, w.access, w.info, w.key, fr)
	}
	return nil
}

// admitsUpdate reports whether one of rules admits an update, given that each
// admitted the stored row: its fields check allows the units changed returns,
// and final reports that it admits the row as it will be stored. One rule
// must admit both sides, so two rules that each admit one side do not add up
// to a write neither admits. changed runs at most once, and only for a rule
// with a fields check. testdata/access/rules.json pins the decision.
func admitsUpdate(rules []accessRule, changed func() []string, final func(accessRule) (bool, error)) (bool, error) {
	var units []string
	read := false
	for _, rule := range rules {
		if rule.fields != nil {
			if !read {
				units, read = changed(), true
			}
			if !rule.fieldsAllow(units) {
				continue
			}
		}
		ok, err := final(rule)
		if err != nil || ok {
			return ok, err
		}
	}
	return false, nil
}

// liveRowBelow returns the name of an entity with a live row whose access
// parent is the row of info with key, or "" when there is none.
func liveRowBelow(tx storage.ReadTx, model *accessModel, info *entityInfo, key string, limits Limits) (string, error) {
	return liveRowNaming(tx, info, key, model.children[info.name], model.parents, limits)
}

// liveRowNaming returns the name of an entity with a live row whose reference
// along one of edges names the row of info with key, or "" when there is
// none. cases maps each entity to every edge of the same kind it has, which a
// scan compares a row with. It probes the reference index of each edge, and
// scans the edge's entity, once for all of its edges, while that index is not
// ready.
func liveRowNaming(
	tx storage.ReadTx,
	info *entityInfo,
	key string,
	edges []*accessEdge,
	cases map[string][]*accessEdge,
	limits Limits,
) (string, error) {
	values, ok := keyValuesOf(info, key)
	if !ok {
		return "", nil
	}
	self := anchorRow{entity: info.name, key: key}
	// A scan compares the reference of each row whatever case it holds, so
	// one scan answers for every case of the entity that points at info.
	scanned := map[string]bool{}
	for _, e := range edges {
		child := e.child
		found := false
		stop := func() error {
			found = true
			return errCandidateBound
		}
		ci := child.indexByName(refIndexName(e.ref.ref))
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
			if scanned[child.name] {
				continue
			}
			scanned[child.name] = true
			err = tx.ScanEntity(child.name, func(row storage.Row) error {
				msg, derr := child.decode(row.Value)
				if derr != nil {
					return nil
				}
				if named, ok := parentOf(msg.ProtoReflect(), cases[child.name]); ok && named == self {
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
