package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
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
// as of tx. A rule with permissions needs each of them at the workspace root,
// at the row itself, or at a row up its parent chain. A rule that admits every
// admitted caller holds for every caller that passed the workspace gate,
// which is every caller with an access. A rule with a caller_is check also
// needs the row's subject field to hold one of the caller's subjects, and a
// rule with a when check needs the row to match its condition, and a rule
// with within entries needs the row to sit within a row that passes each. row
// is the row's decoded value, or nil for a row that does not exist, which no
// check on the row and no within entry admits.
func (a *txAccess) admits(tx storage.ReadTx, rule accessRule, info *entityInfo, key string, row protoreflect.Message) (bool, error) {
	if rule.checksRow() && (row == nil || !rule.rowHolds(a.caller.subjects, row)) {
		return false, nil
	}
	for _, p := range rule.requires {
		if ok, err := a.holds(tx, p, info, key, row); err != nil || !ok {
			return false, err
		}
	}
	if rule.checksAbove() {
		return a.chainsOf(tx).withinAll(rule, a.caller.subjects, info, key, row)
	}
	return true, nil
}

// admitsThrough reports whether rule's source holds for the caller through
// each of parents on its own, as of tx: each permission it needs at the
// workspace root, at the row of info with key, or at that parent or a row
// above it. It reports true for no parents and for a rule that admits every
// admitted caller. A write that adds a row below a parent shows the row to
// the readers of that parent, and one that takes it away hides the row from
// them, so the rule must hold there and not only through another parent.
func (a *txAccess) admitsThrough(tx storage.ReadTx, rule accessRule, info *entityInfo, key string, parents []anchorRow) (bool, error) {
	if len(parents) == 0 || len(rule.requires) == 0 {
		return true, nil
	}
	set, err := a.get(tx)
	if err != nil {
		return false, err
	}
	chains := a.chainsOf(tx)
	for _, p := range rule.requires {
		anchors := set.perms[p]
		if anchors.holds(info.name, key) {
			continue
		}
		if ok, err := chains.holdsAtEach(anchors, parents); err != nil || !ok {
			return false, err
		}
	}
	return true, nil
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

// accessRefusal is the verdict on a write the access model refuses. Its
// message names only what the caller sent, and reads the same whether the row
// is hidden, would not decode, or no rule admits the write.
func accessRefusal(info *entityInfo, key string) error {
	return Reject(CodeAccessDenied,
		fmt.Sprintf("entity %q: row %q may not be changed by this caller", info.name, key))
}

// opName is the name of one operation, as a rule's spec writes it.
func opName(op accessOp) string {
	for _, o := range accessOpNames {
		if o.op == op {
			return o.name
		}
	}
	return "unknown"
}

// explainRules renders, for the refusal log, each rule of info that names op
// and what it does not admit at the row of info with key, as of tx. row is
// nil for a row that does not exist.
func (a *txAccess) explainRules(tx storage.ReadTx, op accessOp, info *entityInfo, key string, row protoreflect.Message) []string {
	var out []string
	for i, rule := range info.access.rules {
		if rule.names(op) {
			out = append(out, explained(i, rule, a.ruleOutcome(tx, rule, info, key, row)))
		}
	}
	return out
}

// explained is one entry of the refusal log: the rule, by its place in allow,
// and why it does not admit the write.
func explained(i int, rule accessRule, why string) string {
	return fmt.Sprintf("allow[%d] %s: %s", i, rule.spec(), why)
}

// ruleOutcome renders why rule does not admit the caller at the row of info
// with key, as of tx, or "admits" when it does. row is nil for a row that does
// not exist.
func (a *txAccess) ruleOutcome(tx storage.ReadTx, rule accessRule, info *entityInfo, key string, row protoreflect.Message) string {
	var why []string
	if row == nil && (rule.checksRow() || rule.checksAbove()) {
		why = append(why, "the row does not exist, and the rule checks it")
	}
	if row != nil && rule.callerIs != nil && !subjectMatches(a.caller.subjects, row.Get(rule.callerIs).String()) {
		why = append(why, fmt.Sprintf("%s does not hold a subject of the caller", rule.callerIs.Name()))
	}
	if row != nil && rule.when != nil && !rule.when.Eval(row) {
		why = append(why, "the row does not match when")
	}
	for _, p := range rule.requires {
		held, err := a.holds(tx, p, info, key, row)
		if err != nil {
			return "not decided: " + err.Error()
		}
		if !held {
			why = append(why, p+" is not held at the root, the row or a row above it")
		}
	}
	if row != nil && rule.checksAbove() {
		ok, err := a.chainsOf(tx).withinAll(rule, a.caller.subjects, info, key, row)
		if err != nil {
			return "not decided: " + err.Error()
		}
		if !ok {
			why = append(why, "no row at or above the row passes within")
		}
	}
	if len(why) == 0 {
		return "admits"
	}
	return strings.Join(why, "; ")
}

// logRefusal records at debug level why the access model refused a write of
// op to the row of info with key. explain runs only when the record is kept.
func (g *writeGate) logRefusal(op accessOp, info *entityInfo, key string, explain func() []string) {
	l := g.srv.log()
	if !l.Enabled(g.ctx, slog.LevelDebug) {
		return
	}
	id := info.name + "\x00" + key + "\x00" + opName(op)
	if g.refusalsLogged[id] {
		return
	}
	if g.refusalsLogged == nil {
		g.refusalsLogged = map[string]bool{}
	}
	g.refusalsLogged[id] = true
	l.LogAttrs(g.ctx, slog.LevelDebug, "access refused",
		slog.String("component", "access"),
		slog.String("workspace", g.ws),
		slog.String("entity", info.name),
		slog.String("key", key),
		slog.String("op", opName(op)),
		slog.Any("subjects", g.access.caller.subjects),
		slog.Any("rules", explain()))
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
	fields = append(fields, info.access.parentUnits...)
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
			return Reject(CodeAccessDenied, fmt.Sprintf(
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
				"entity %q: row %q names no %q; a grant row must name the row it applies at", info.name, key, a.parentUnits[0]))
		}
	}
	return nil
}

// checkAccessChain refuses a write that would put a row on a parent chain the
// model cannot walk: a row below itself, a row more than
// Limits.MaxAccessDepth hops below a row without an access parent on any
// path, or a row whose access parents name more than Limits.MaxAccessParents
// rows. Every write path runs it, Trusted ones included, on the row as it will
// be stored. Only an entity whose chain of parent entities is recursive can
// put a row below itself or too deep, since registration bounds every other
// chain, and only a write that changes the row's access parents is checked.
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
	if model == nil || info.access == nil || !info.access.hasParent() {
		return nil
	}
	e := edgeOf(model, info, final.ProtoReflect())
	maxParents := s.limits.maxAccessParents()
	recursive := model.depths[info.name] == depthUnbounded
	if !e.ok() || (len(e.parents) <= maxParents && !recursive) {
		return nil
	}
	row, live, err := load()
	if err != nil {
		return err
	}
	if live {
		if stored, err := info.decode(row.Value); err == nil && edgeOf(model, info, stored.ProtoReflect()).equal(e) {
			return nil
		}
	}
	if len(e.parents) > maxParents {
		return Reject(CodeInvalidArgument, fmt.Sprintf(
			"entity %q: row %q names %d access parents, more than MaxAccessParents (%d) allows",
			info.name, key, len(e.parents), maxParents))
	}
	if !recursive {
		return nil
	}
	report := func(row storage.Row, err error) { s.reportPoison(ctx, ws, row, err) }
	maxDepth := s.limits.maxAccessDepth()
	chains := newAccessChains(model, tx, maxDepth, report)
	self := anchorRow{entity: info.name, key: key}
	tooDeep := Reject(CodeInvalidArgument, fmt.Sprintf(
		"entity %q: row %q would sit more than MaxAccessDepth (%d) hops below a row without an access parent",
		info.name, key, maxDepth))
	// A depth-first walk for the longest path, since a read finds an anchor on
	// any path and so each must be walkable. height memoizes the most hops
	// from a row up to a row without a parent, and onPath finds a cycle, which
	// has no end.
	height := map[anchorRow]int{}
	onPath := map[anchorRow]bool{self: true}
	var walk func(at anchorRow, hops int) (int, error)
	walk = func(at anchorRow, hops int) (int, error) {
		if at == self {
			return 0, Reject(CodeInvalidArgument, fmt.Sprintf(
				"entity %q: row %q would sit below itself in the access model", info.name, key))
		}
		if hops > maxDepth || onPath[at] {
			return 0, tooDeep
		}
		if h, ok := height[at]; ok {
			if hops+h > maxDepth {
				return 0, tooDeep
			}
			return h, nil
		}
		up, err := chains.edge(at)
		if err != nil {
			return 0, err
		}
		onPath[at] = true
		h := 0
		for _, p := range up.parents {
			ph, err := walk(p, hops+1)
			if err != nil {
				return 0, err
			}
			h = max(h, ph+1)
		}
		delete(onPath, at)
		height[at] = h
		return h, nil
	}
	for _, p := range e.parents {
		if _, err := walk(p, 1); err != nil {
			return err
		}
	}
	return nil
}

// accessWrite carries one enforced CRUD write of a model entity through the
// access model's two checks. A nil accessWrite checks nothing, which is what a
// write outside the model, an unenforced write and a Trusted one get.
type accessWrite struct {
	gate   *writeGate
	access *txAccess
	info   *entityInfo
	key    string
	op     syncplanev1.CrudOp
	mask   []string
	// live, stored and storedParents describe the row before the write:
	// whether one existed, its value, and the rows its access parents named.
	live          bool
	stored        protoreflect.Message
	storedParents chainEdge
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
	w := &accessWrite{gate: g, access: g.access, info: info, key: key, op: op, mask: mask}
	row, live, err := load()
	if err != nil || !live {
		return w, err
	}
	stored, err := info.decode(row.Value)
	if err != nil {
		// Nothing can show that the caller may change a row nobody can read. A
		// Trusted write repairs it.
		g.srv.reportPoison(g.ctx, g.ws, row, err)
		return nil, accessRefusal(info, key)
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
		g.logRefusal(want, info, key, func() []string { return g.access.explainRules(tx, want, info, key, sr) })
		return nil, accessRefusal(info, key)
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
	w.storedParents = edgeOf(model, info, sr)
	if want == opUpdate {
		w.updating = admitting
	}
	return w, nil
}

// finish runs the access model's check against the row as it will be stored:
// after the mask merge, the protected-field carry-over, the creator stamp and
// the reference projection, before the index update.
//
// A create needs a create rule that admits the caller and the new row, with
// its source held through each parent the row names. An update needs one of
// the update rules that admitted the caller at the stored row to admit the
// row as it will be stored as well, and the fields the write changes. When
// the update changes the row's access parents, that rule's source must also
// hold through each parent the write adds and each one it removes. A move
// also needs, where the row sits before it, the right to read every entity
// whose rows can sit below it: the rows below move with it, and the caller
// would otherwise show them to whoever reads at the new parent. An update
// that changes what a within entry reads of the row needs the same, since it
// changes what the rows below show to whoever reads them. On
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
	parents := edgeOf(model, w.info, fr)
	moved := w.live && !parents.equal(w.storedParents)
	switch {
	case !w.live:
		admitting, err := w.access.admitting(tx, opCreate, w.info, w.key, fr)
		if err != nil {
			return err
		}
		admitted := false
		for _, rule := range admitting {
			if admitted, err = w.access.admitsThrough(tx, rule, w.info, w.key, parents.parents); err != nil {
				return err
			}
			if admitted {
				break
			}
		}
		if !admitted {
			w.gate.logRefusal(opCreate, w.info, w.key, func() []string { return w.explainCreate(tx, fr, parents.parents) })
			return accessRefusal(w.info, w.key)
		}
	default:
		var changed []anchorRow
		if moved {
			changed = symmetricDifference(w.storedParents.parents, parents.parents)
		}
		admitted, err := admitsUpdate(w.updating,
			func() []string { return changedUnits(w.info.msgType.Descriptor(), w.stored, fr) },
			func(rule accessRule) (bool, error) {
				// Unmoved, the row sits on the chain the rule's source was
				// checked against, so only a check on the row can answer
				// differently.
				if !moved && !rule.checksRow() && !rule.checksAbove() {
					return true, nil
				}
				ok, err := w.access.admits(tx, rule, w.info, w.key, fr)
				if err != nil || !ok || !moved {
					return ok, err
				}
				return w.access.admitsThrough(tx, rule, w.info, w.key, changed)
			})
		if err != nil {
			return err
		}
		if !admitted {
			w.gate.logRefusal(opUpdate, w.info, w.key, func() []string { return w.explainUpdate(tx, fr, moved, changed) })
			return accessRefusal(w.info, w.key)
		}
		// A change of what a within entry reads of the row changes what the rows
		// below it show, as a move does.
		rechecked := !moved && len(model.withinOn[w.info.name]) > 0 &&
			!withinStateOf(model, w.info, w.stored).equal(withinStateOf(model, w.info, fr))
		if !moved && !rechecked {
			break
		}
		set, err := w.access.get(tx)
		if err != nil {
			return err
		}
		chains := w.access.chainsOf(tx)
		subjects := w.access.caller.subjects
		for _, r := range model.readersBelow(w.info.name) {
			ok, err := chains.holds(set.readAnchors(r), w.info, w.key, w.stored)
			for _, o := range r.checked {
				if err != nil || ok {
					break
				}
				ok, err = chains.holdsAll(set.anchorsOf(o.requires), w.info, w.key, w.stored)
				if err == nil && ok && o.checksAbove() {
					ok, err = chains.withinAllOn(o, subjects, w.info, w.key, w.stored)
				}
			}
			if err != nil {
				return err
			}
			if !ok {
				what := "moving"
				if rechecked {
					what = "changing"
				}
				return Reject(CodeAccessDenied, fmt.Sprintf(
					"entity %q: %s row %q needs one of the permissions %s where it is now, to change what the rows below it show",
					w.info.name, what, w.key, strings.Join(r.sources(), ", ")))
			}
		}
	}
	if touchesGrant(w.info, w.op, w.mask) {
		return checkGrantSubset(tx, w.access, w.info, w.key, fr)
	}
	return nil
}

// explainCreate renders, for the refusal log, why no create rule admits row,
// as of tx: what a rule does not admit, or that its source does not hold
// through each of parents on its own.
func (w *accessWrite) explainCreate(tx storage.ReadTx, row protoreflect.Message, parents []anchorRow) []string {
	var out []string
	for i, rule := range w.info.access.rules {
		if !rule.names(opCreate) {
			continue
		}
		why := w.access.ruleOutcome(tx, rule, w.info, w.key, row)
		if why == "admits" {
			why = "the source does not hold through each parent the row names"
		}
		out = append(out, explained(i, rule, why))
	}
	return out
}

// explainUpdate renders, for the refusal log, why no update rule admits both
// sides of an update to final, as of tx: a rule that does not admit the
// stored row, does not list a field the update changes, or does not admit the
// row as it will be stored or the parents a move adds or removes.
func (w *accessWrite) explainUpdate(tx storage.ReadTx, final protoreflect.Message, moved bool, changed []anchorRow) []string {
	units := changedUnits(w.info.msgType.Descriptor(), w.stored, final)
	var out []string
	for i, rule := range w.info.access.rules {
		if !rule.names(opUpdate) {
			continue
		}
		why := w.access.ruleOutcome(tx, rule, w.info, w.key, w.stored)
		switch {
		case why != "admits":
		case !rule.fieldsAllow(units):
			why = fmt.Sprintf("the update changes %s, and fields lists %s",
				strings.Join(units, ", "), strings.Join(rule.fields, ", "))
		default:
			why = w.access.ruleOutcome(tx, rule, w.info, w.key, final)
			if why != "admits" {
				why = "the row as it will be stored: " + why
			} else if moved {
				ok, err := w.access.admitsThrough(tx, rule, w.info, w.key, changed)
				switch {
				case err != nil:
					why = "not decided: " + err.Error()
				case !ok:
					why = "the source does not hold through each parent the move adds or removes"
				}
			}
		}
		out = append(out, explained(i, rule, why))
	}
	return out
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

// symmetricDifference returns the rows in exactly one of a and b, which are
// sorted by entity and key and hold no repeats, in the same order.
func symmetricDifference(a, b []anchorRow) []anchorRow {
	var out []anchorRow
	for _, r := range a {
		if !slices.Contains(b, r) {
			out = append(out, r)
		}
	}
	for _, r := range b {
		if !slices.Contains(a, r) {
			out = append(out, r)
		}
	}
	return compactRows(out)
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
				if slices.Contains(parentsOf(msg.ProtoReflect(), cases[child.name]), self) {
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
		return Reject(CodeAccessDenied, fmt.Sprintf(
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
		return Reject(CodeAccessDenied, fmt.Sprintf("mutator %q may not be invoked by this caller", mi.name))
	}
	refuse := func(key string) error {
		return Reject(CodeAccessDenied, fmt.Sprintf(
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

// ErrUnknownPermission is returned by EntityTx.Can for a permission that no
// role of the schema grants, which no caller can hold.
var ErrUnknownPermission = errors.New("server: no role grants the permission")

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
//
// In a schema with the access model, Can returns an error that wraps
// ErrUnknownPermission for a permission no role grants, in a Trusted
// transaction too.
func (e *EntityTx) Can(permission string, prototype proto.Message, key string) (bool, error) {
	model := e.srv.catalogPtr().access
	if e.access != nil {
		model = e.access.caller.model
	}
	if model != nil && model.granted[permission] == nil {
		return false, fmt.Errorf("%w: %q", ErrUnknownPermission, permission)
	}
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
