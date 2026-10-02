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
	// wrap maps the gate's error onto what the entry point reports.
	admitted *accessSet
	verdict  *pendingVerdict
	wrap     func(error) error
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
	return &txAccess{caller: adm.caller, admitted: adm.access, verdict: adm.verdict, wrap: adm.wrap, limits: limits, report: report}
}

// begin starts one attempt of a write transaction. It drops what an earlier
// attempt resolved and, when tx reads the workspace at the position the
// workspace gate read the caller's grants at, takes the gate's set as what the
// caller holds as of tx. A transaction that has taken a sequence is past that
// position (see workspacePos), so a write path calls it before its first
// write; after one it only costs the read of the position.
//
// When the workspace gate left its grant check to the write transaction, begin
// reaches that verdict in tx. When tx reads the workspace at another position
// than the one the verdict was reached at, begin reads the caller's grants
// from tx and decides the verdict again. Either way it returns the gate's
// error, a gateError, for a refused caller. A write path then calls it before
// it reads anything else of tx, and the refused transaction writes nothing.
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
		return nil
	}
	// The grants may have changed since the verdict: on a rerun under
	// savepoints, an adapter retry, or after a gate that read them in a
	// transaction of its own. A rule with permissions reads them again where
	// it is checked, but one that admits every admitted caller reads nothing,
	// so a caller whose last grant went in between would write through it.
	set, err := accessIn(tx, a.caller, a.limits, nil)
	if err != nil {
		return err
	}
	if !set.any() {
		return &gateError{a.wrap(ErrNoAccess)}
	}
	a.tx, a.set = tx, set
	return nil
}

// settle is pendingVerdict.settle for the grant check a carries. A nil a
// returns err.
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

// admitsThrough reports whether rule's source and its within entries hold
// for the caller through each of parents on its own, as of tx: each
// permission it needs at the workspace root, at the row of info with key, or
// at that parent or a row above it, and each within entry at the row, whose
// value is row, or at that parent or a row above it. It reports true for no
// parents and for a rule that admits every admitted caller and sets no within
// entry. A write that adds a row below a parent shows the row to the readers
// of that parent, and one that takes it away hides the row from them, so the
// rule must hold there and not only through another parent.
func (a *txAccess) admitsThrough(
	tx storage.ReadTx,
	rule accessRule,
	info *entityInfo,
	key string,
	row protoreflect.Message,
	parents []anchorRow,
) (bool, error) {
	if len(parents) == 0 || (len(rule.requires) == 0 && !rule.checksAbove()) {
		return true, nil
	}
	if rule.checksAbove() {
		chains := a.chainsOf(tx)
		for _, p := range parents {
			if ok, err := withinThrough(chains, rule, a.caller.subjects, info, key, row, p); err != nil || !ok {
				return false, err
			}
		}
	}
	if len(rule.requires) == 0 {
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

// withinThrough reports whether each within entry of rule holds for a caller
// with subjects at the row of info with key, whose value is row, through the
// parent at on its own: the row passes the entry, or at or a row above it
// does, on any path of at most the walker's depth.
func withinThrough(
	chains *accessChains,
	rule accessRule,
	subjects []string,
	info *entityInfo,
	key string,
	row protoreflect.Message,
	at anchorRow,
) (bool, error) {
	for _, entry := range rule.within {
		w := chains.model.withins[entry.spec()]
		if w == nil {
			return false, nil
		}
		if w.target.name == info.name && chains.firstWithin(info, key, row).passes(w, subjects) {
			continue
		}
		if ok, err := chains.withinAbove(w, subjects, edgeTo(at)); err != nil || !ok {
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
// rows. A row below the written row counts as well, since it moves with it.
// Every write path runs it, Trusted ones included, on the row as it will be
// stored. Only an entity whose chain of parent entities is recursive can put a
// row below itself or too deep, since registration bounds every other chain,
// and only a write that changes the row's access parents is checked.
func (s *Server) checkAccessChain(
	ctx context.Context,
	ws string,
	tx storage.ReadTx,
	info *entityInfo,
	key string,
	load func() (storage.Row, bool, error),
	final proto.Message,
) error {
	model, info, fr, err := s.trackedAccess(info, final)
	if err != nil || model == nil || info.access == nil || !info.access.hasParent() {
		return err
	}
	e := edgeOf(model, info, fr)
	maxParents := s.limits.maxAccessParents()
	recursive := model.depths[info.name] == depthUnbounded
	if !e.ok() || (len(e.parents) <= maxParents && !recursive) {
		return nil
	}
	row, live, err := load()
	if err != nil {
		return err
	}
	var stored chainEdge
	if live {
		if msg, err := info.decode(row.Value); err == nil {
			if stored = edgeOf(model, info, msg.ProtoReflect()); stored.equal(e) {
				return nil
			}
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
	above := 0
	for _, p := range e.parents {
		h, err := walk(p, 1)
		if err != nil {
			return err
		}
		above = max(above, h+1)
	}
	// A write that does not lengthen the longest path above the row puts no
	// row below it deeper than it already was, which spares the walk down. A
	// path above the stored row that does not fit the limit any more, which a
	// lowered limit leaves, does not spare it.
	if stored.ok() {
		was, fits := 0, true
		for _, p := range stored.parents {
			h, err := walk(p, 1)
			if err != nil {
				fits = false
				break
			}
			was = max(was, h+1)
		}
		if fits && above <= was {
			return nil
		}
	}
	// The rows below the row move with it, so the longest path down from it
	// must fit in what the path up leaves.
	deeper, err := rowsBelowDeeper(tx, model, self, maxDepth-above, s.limits, report)
	if err != nil {
		return err
	}
	if deeper {
		return Reject(CodeInvalidArgument, fmt.Sprintf(
			"entity %q: row %q would put a row below it more than MaxAccessDepth (%d) hops below a row without an access parent",
			info.name, key, maxDepth))
	}
	return nil
}

// checkNewParentsRead refuses a write in a recursive chain that adds an
// access parent the caller does not read, as of tx: a row that does not exist
// or that no read rule of its entity admits the caller at. Such a write gets
// the refusal a write the rules refuse gets. Callers run it before
// checkAccessChain, whose depth and cycle checks read the rows above the new
// parents, so their answer does not tell the caller where a row it cannot
// read sits.
func (w *accessWrite) checkNewParentsRead(tx storage.ReadTx, final proto.Message) error {
	if w == nil {
		return nil
	}
	model := w.access.model()
	if !w.info.access.hasParent() || model.depths[w.info.name] != depthUnbounded {
		return nil
	}
	for _, p := range edgeOf(model, w.info, final.ProtoReflect()).parents {
		if slices.Contains(w.storedParents.parents, p) {
			continue
		}
		ok, err := w.readsRow(tx, model, p)
		if err != nil {
			return err
		}
		if !ok {
			w.gate.logRefusal(opUpdate, w.info, w.key, func() []string {
				return []string{fmt.Sprintf("the caller does not read %s %q, which the write names as an access parent", p.entity, p.key)}
			})
			return accessRefusal(w.info, w.key)
		}
	}
	return nil
}

// readsRow reports whether a read rule admits the caller at the row at, as of
// tx. A row that does not exist, or does not decode, is not read.
func (w *accessWrite) readsRow(tx storage.ReadTx, model *accessModel, at anchorRow) (bool, error) {
	info := model.entities[at.entity]
	if info == nil || info.access == nil {
		return info != nil, nil
	}
	row, err := tx.GetRow(at.entity, at.key)
	if errors.Is(err, storage.ErrNotFound) || (err == nil && row.Deleted) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	msg, err := info.decode(row.Value)
	if err != nil {
		w.gate.srv.reportPoison(w.gate.ctx, w.gate.ws, row, err)
		return false, nil
	}
	reading, err := w.access.admitting(tx, opRead, info, at.key, msg.ProtoReflect())
	return len(reading) > 0, err
}

// trackedAccess returns the access model that the engine's own bookkeeping of
// a write to info runs against, the chain checks and the move log, with info
// and final, a row of info or nil, as that model knows them. That is the live
// model or, while a registration leaves the model incomplete, the last model
// that compiled. model is nil when there is neither, or when that model does
// not know the entity.
func (s *Server) trackedAccess(info *entityInfo, final proto.Message) (
	model *accessModel,
	tracked *entityInfo,
	row protoreflect.Message,
	err error,
) {
	c := s.catalogPtr()
	model, tracked = c.access, info
	if model == nil && c.accessErr != nil && c.lastAccess != nil {
		model, tracked = c.lastAccess, c.lastAccess.entities[info.name]
	}
	if model == nil || tracked == nil {
		return nil, nil, nil, nil
	}
	if final == nil {
		return model, tracked, nil, nil
	}
	row = final.ProtoReflect()
	if tracked.msgType.Descriptor() != info.msgType.Descriptor() {
		// A registration since that model replaced the entity's message, and
		// the model reads fields by its own descriptors.
		b, err := proto.Marshal(final)
		if err != nil {
			return nil, nil, nil, err
		}
		msg, err := tracked.decode(b)
		if err != nil {
			return nil, nil, nil, err
		}
		row = msg.ProtoReflect()
	}
	return model, tracked, row, nil
}

// rowsBelowDeeper reports whether a live row sits more than budget hops below
// the row at, on some path down the access parent edges, as of tx. A row that
// does not decode names no parent, and report is told about it.
func rowsBelowDeeper(
	tx storage.ReadTx,
	model *accessModel,
	at anchorRow,
	budget int,
	limits Limits,
	report func(storage.Row, error),
) (bool, error) {
	// Level by level, keeping each row once per level and not once overall: a
	// row on two paths of different lengths sits as deep as the longer one.
	// The walk ends after budget+1 levels, so a stored cycle cannot hold it.
	frontier := []anchorRow{at}
	for level := 1; len(frontier) > 0; level++ {
		next, err := rowsOneBelow(tx, model, frontier, limits, report)
		if err != nil {
			return false, err
		}
		if len(next) > 0 && level > budget {
			return true, nil
		}
		frontier = next
	}
	return false, nil
}

// rowsOneBelow returns the live rows whose access parents name one of
// frontier, as of tx, without repeats. It probes the reference index of each
// edge into a frontier entity, and scans the edge's entity, once for all of
// frontier, while that index is not ready. A hit in the index is read back,
// since only the row says what it names now.
func rowsOneBelow(
	tx storage.ReadTx,
	model *accessModel,
	frontier []anchorRow,
	limits Limits,
	report func(storage.Row, error),
) ([]anchorRow, error) {
	inFrontier := make(map[anchorRow]bool, len(frontier))
	var entities []string
	byEntity := map[string][]anchorRow{}
	for _, at := range frontier {
		inFrontier[at] = true
		if _, ok := byEntity[at.entity]; !ok {
			entities = append(entities, at.entity)
		}
		byEntity[at.entity] = append(byEntity[at.entity], at)
	}
	seen := map[anchorRow]bool{}
	var out []anchorRow
	// keep adds the row of child if it names one of frontier.
	keep := func(child *entityInfo, row storage.Row) {
		if row.Deleted {
			return
		}
		msg, err := child.decode(row.Value)
		if err != nil {
			report(row, err)
			return
		}
		r := anchorRow{entity: child.name, key: row.Key}
		if seen[r] {
			return
		}
		for _, p := range parentsOf(msg.ProtoReflect(), model.parents[child.name]) {
			if inFrontier[p] {
				seen[r] = true
				out = append(out, r)
				return
			}
		}
	}
	scanned := map[string]bool{}
	for _, entity := range entities {
		parent := model.entities[entity]
		for _, e := range model.children[entity] {
			child := e.child
			ci := child.indexByName(refIndexName(e.ref.ref))
			ready := false
			if ci != nil && !limits.DisableIndexes {
				var err error
				if ready, err = ci.ready(tx, child.name); err != nil {
					return nil, err
				}
			}
			if !ready {
				if scanned[child.name] {
					continue
				}
				scanned[child.name] = true
				if err := tx.ScanEntity(child.name, func(row storage.Row) error {
					keep(child, row)
					return nil
				}); err != nil {
					return nil, err
				}
				continue
			}
			for _, at := range byEntity[entity] {
				values, ok := keyValuesOf(parent, at.key)
				if !ok {
					continue
				}
				lo, hi, err := ci.probeRefIndex(values)
				if err != nil {
					return nil, err
				}
				err = tx.ScanIndex(child.name, ci.name, lo, hi, 0, storage.ScanAsc, func(_ []byte, rowKey string) error {
					row, err := tx.GetRow(child.name, rowKey)
					if errors.Is(err, storage.ErrNotFound) {
						return nil
					}
					if err != nil {
						return err
					}
					keep(child, row)
					return nil
				})
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return out, nil
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
// key would take them over. A PATCH or a DELETE of a row that does not exist,
// or is a tombstone, gets the refusal a row the caller may not change gets,
// whatever the caller holds.
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
	if err != nil {
		return nil, err
	}
	if !live {
		if op == syncplanev1.CrudOp_CRUD_OP_PUT {
			return w, nil
		}
		// A missing row gets the answer of a row the caller may not change, so
		// the answer does not tell which keys exist. A DELETE also writes no
		// tombstone, which would reach every reader.
		want := opUpdate
		if op == syncplanev1.CrudOp_CRUD_OP_DELETE {
			want = opDelete
		}
		g.logRefusal(want, info, key, func() []string { return g.access.explainRules(tx, want, info, key, nil) })
		return nil, accessRefusal(info, key)
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
			// The message leaves out the entity below, which the caller may
			// not read; the debug log names it.
			g.logRefusal(opDelete, info, key, func() []string {
				return []string{fmt.Sprintf("a live %s row names the row as its access parent", below)}
			})
			return nil, Reject(CodeConflict, fmt.Sprintf(
				"entity %q: row %q is the access parent of a live row; delete or move the rows below it first",
				info.name, key))
		}
	}
	if op == syncplanev1.CrudOp_CRUD_OP_DELETE && len(model.subjectsOf[info.name]) > 0 {
		grant, err := liveRowNaming(tx, info, key, model.subjectsOf[info.name], model.teamSubjects, g.srv.limits)
		if err != nil {
			return nil, err
		}
		if grant != "" {
			g.logRefusal(opDelete, info, key, func() []string {
				return []string{fmt.Sprintf("a live %s row names the row as its subject", grant)}
			})
			return nil, Reject(CodeConflict, fmt.Sprintf(
				"entity %q: row %q is the subject of a live grant row; delete the grant rows that name it first",
				info.name, key))
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
// its source held through each parent the row names. A create is refused with
// CodeConflict while a live row names the new row's key as its access parent,
// or a live grant row names it as its subject.
//
// An update needs one of the update rules that admitted the caller at the
// stored row to admit the row as it will be stored as well, and the fields
// the write changes. When the update changes the row's access parents, that
// rule's source must also hold through each parent the write adds and each
// one it removes. An update that changes a field that a read rule of a hiding
// guard checks with caller_is or when needs the right to read the guard's
// fields before and after it.
//
// A move also needs, where the row sits before it, the right to read the row
// and each unit of it that a guard can hide, and to read every row of each
// entity whose rows can sit below it: the row and the rows below move
// together, and the caller would otherwise show them to whoever reads at the
// new parent, itself included. A read rule with a caller_is or a when check
// does not give the right to read every row. An update that changes what a
// within entry reads of the row needs the same right for the rows below,
// since it changes what they show to whoever reads them.
//
// On a grant entity, a write that can change what the row grants also needs
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
			if admitted, err = w.access.admitsThrough(tx, rule, w.info, w.key, fr, parents.parents); err != nil {
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
		if err := w.checkNothingNames(tx); err != nil {
			return err
		}
	default:
		if w.op == syncplanev1.CrudOp_CRUD_OP_PATCH {
			if err := w.checkGuardedMask(tx, fr); err != nil {
				return err
			}
		}
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
				return w.access.admitsThrough(tx, rule, w.info, w.key, fr, changed)
			})
		if err != nil {
			return err
		}
		if !admitted {
			w.gate.logRefusal(opUpdate, w.info, w.key, func() []string { return w.explainUpdate(tx, fr, moved, changed) })
			return accessRefusal(w.info, w.key)
		}
		if err := w.checkGuardInputs(tx, fr); err != nil {
			return err
		}
		// A change of what a within entry reads of the row changes what the rows
		// below it show, as a move does.
		rechecked := !moved && len(model.withinOn[w.info.name]) > 0 &&
			!withinStateOf(model, w.info, w.stored).equal(withinStateOf(model, w.info, fr))
		if !moved && !rechecked {
			break
		}
		if moved {
			if err := w.checkReadsStored(tx); err != nil {
				return err
			}
		}
		set, err := w.access.get(tx)
		if err != nil {
			return err
		}
		chains := w.access.chainsOf(tx)
		subjects := w.access.caller.subjects
		for _, r := range model.moveReadersBelow(w.info.name) {
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
				if sources := r.sources(); len(sources) > 0 {
					return Reject(CodeAccessDenied, fmt.Sprintf(
						"entity %q: %s row %q needs one of the permissions %s where it is now, to change what the rows below it show",
						w.info.name, what, w.key, strings.Join(sources, ", ")))
				}
				return Reject(CodeAccessDenied, fmt.Sprintf(
					"entity %q: %s row %q needs the right to read every row below it, which only a read rule without caller_is or when gives",
					w.info.name, what, w.key))
			}
		}
	}
	if touchesGrant(w.info, w.op, w.mask) {
		return checkGrantSubset(tx, w.access, w.info, w.key, fr)
	}
	return nil
}

// checkNothingNames refuses a create while a live row names the new row's key
// as its access parent, or a live grant row names it as its subject, as of
// tx. Those rows were left by an earlier row with the key, or written before
// it, and the create would put them below the new row: the caller would show
// them to whoever reads the new row, and a grant row would hold its role
// again.
func (w *accessWrite) checkNothingNames(tx storage.ReadTx) error {
	model := w.access.model()
	limits := w.gate.srv.limits
	if model.anchors[w.info.name] {
		below, err := liveRowBelow(tx, model, w.info, w.key, limits)
		if err != nil {
			return err
		}
		if below != "" {
			// The message leaves out the entity below, which the caller may
			// not read; the debug log names it.
			w.gate.logRefusal(opCreate, w.info, w.key, func() []string {
				return []string{fmt.Sprintf("a live %s row names the key as its access parent", below)}
			})
			return Reject(CodeConflict, fmt.Sprintf(
				"entity %q: row %q would become the access parent of a live row that names its key; delete or move that row first",
				w.info.name, w.key))
		}
	}
	if len(model.subjectsOf[w.info.name]) > 0 {
		grant, err := liveRowNaming(tx, w.info, w.key, model.subjectsOf[w.info.name], model.teamSubjects, limits)
		if err != nil {
			return err
		}
		if grant != "" {
			w.gate.logRefusal(opCreate, w.info, w.key, func() []string {
				return []string{fmt.Sprintf("a live %s row names the key as its subject", grant)}
			})
			return Reject(CodeConflict, fmt.Sprintf(
				"entity %q: row %q would become the subject of a live grant row that names its key; delete that grant row first",
				w.info.name, w.key))
		}
	}
	return nil
}

// checkReadsStored refuses the write unless the caller reads the stored row
// and each unit of it that a guard can hide where the row sits, as of tx. A
// move shows the row to whoever reads at its new parents, the caller
// included, so it must not carry what the caller could not read where the
// row was.
func (w *accessWrite) checkReadsStored(tx storage.ReadTx) error {
	reading, err := w.access.admitting(tx, opRead, w.info, w.key, w.stored)
	if err != nil {
		return err
	}
	ok := len(reading) > 0
	for i := range w.info.access.guards {
		// A guard whose readers do not depend on where the row sits shows its
		// units to the same callers after the move. A rule of it that checks
		// a unit the move changes, such as the access parent, is
		// checkGuardInputs' to refuse.
		if g := &w.info.access.guards[i]; ok && g.hides() && g.read.dependsOnChain() {
			if ok, err = w.guardReads(tx, g); err != nil {
				return err
			}
		}
	}
	if !ok {
		return Reject(CodeAccessDenied, fmt.Sprintf(
			"entity %q: moving row %q needs the right to read it, and each field of it a guard can hide, where it is now",
			w.info.name, w.key))
	}
	return nil
}

// guardAdmits reports whether a rule of g admits op, opCreate or opUpdate, to
// the caller at the row of the write, as of tx: on final, the row as it will
// be stored, and for an update on the stored row as well.
func (w *accessWrite) guardAdmits(tx storage.ReadTx, g *fieldGuard, op accessOp, final protoreflect.Message) (bool, error) {
	for _, rule := range g.rules {
		if !rule.names(op) {
			continue
		}
		if op == opUpdate {
			ok, err := w.access.admits(tx, rule, w.info, w.key, w.stored)
			if err != nil {
				return false, err
			}
			if !ok {
				continue
			}
		}
		ok, err := w.access.admits(tx, rule, w.info, w.key, final)
		if err != nil || ok {
			return ok, err
		}
	}
	return false, nil
}

// unitSet reports whether msg, a row of info, sets unit: a oneof holds a case,
// or a field holds a value other than its default.
func unitSet(info *entityInfo, msg protoreflect.Message, unit string) bool {
	desc := info.msgType.Descriptor()
	if od := desc.Oneofs().ByName(protoreflect.Name(unit)); od != nil {
		return msg.WhichOneof(od) != nil
	}
	fd := desc.Fields().ByName(protoreflect.Name(unit))
	return fd != nil && msg.Has(fd)
}

// guardReads reports whether a read rule of g admits the caller on the stored
// row, as of tx.
func (w *accessWrite) guardReads(tx storage.ReadTx, g *fieldGuard) (bool, error) {
	return w.guardReadsOn(tx, g, w.stored)
}

// guardReadsOn reports whether a read rule of g admits the caller on row, as
// of tx.
func (w *accessWrite) guardReadsOn(tx storage.ReadTx, g *fieldGuard, row protoreflect.Message) (bool, error) {
	for _, rule := range g.reads {
		ok, err := w.access.admits(tx, rule, w.info, w.key, row)
		if err != nil || ok {
			return ok, err
		}
	}
	return false, nil
}

// checkGuardInputs refuses an update that changes a unit that a read rule of a
// hiding guard checks on the row, with caller_is or when, unless the caller
// reads the guard's units on the stored row and on final, the row as it will
// be stored, as of tx. Such a change decides anew who reads the units, so a
// caller that did not read them could otherwise point the row at itself, or
// at another reader, and show them.
func (w *accessWrite) checkGuardInputs(tx storage.ReadTx, final protoreflect.Message) error {
	var changed []string
	read := false
	for i := range w.info.access.guards {
		g := &w.info.access.guards[i]
		if !g.hides() {
			continue
		}
		inputs := guardRowInputs(g)
		if len(inputs) == 0 {
			continue
		}
		if !read {
			changed, read = changedUnits(w.info.msgType.Descriptor(), w.stored, final), true
		}
		var hit []string
		for _, unit := range inputs {
			if slices.Contains(changed, unit) {
				hit = append(hit, unit)
			}
		}
		if len(hit) == 0 {
			continue
		}
		ok, err := w.guardReadsOn(tx, g, w.stored)
		if err == nil && ok {
			ok, err = w.guardReadsOn(tx, g, final)
		}
		if err != nil {
			return err
		}
		if !ok {
			w.gate.logRefusal(opUpdate, w.info, w.key, func() []string {
				return []string{fmt.Sprintf("the update changes %s, which decides who reads %s, and the caller does not read %s before and after it",
					strings.Join(hit, ", "), strings.Join(g.units, ", "), strings.Join(g.units, ", "))}
			})
			return Reject(CodeAccessDenied, fmt.Sprintf(
				"entity %q: changing %s of row %q needs the right to read %s before and after the change",
				w.info.name, strings.Join(hit, ", "), w.key, strings.Join(g.units, ", ")))
		}
	}
	return nil
}

// guardRowInputs returns the units that the read rules of g check on the row,
// with caller_is or when, without repeats.
func guardRowInputs(g *fieldGuard) []string {
	var out []string
	add := func(unit string) {
		if !slices.Contains(out, unit) {
			out = append(out, unit)
		}
	}
	for _, rule := range g.reads {
		if rule.callerIs != nil {
			add(unitOf(rule.callerIs))
		}
		for _, unit := range rule.whenReads {
			add(unit)
		}
	}
	return out
}

// guardRefusal is the verdict on a write that names a guarded field its guard
// does not admit for the caller.
func guardRefusal(info *entityInfo, key, unit string) error {
	return Reject(CodeAccessDenied, fmt.Sprintf(
		"entity %q: field %q of row %q may not be changed by this caller", info.name, unit, key))
}

// carryGuarded makes a PUT land for the fields the caller may write and change
// nothing else. It runs on msg, the row the PUT will store, after the
// creator stamp and before the text handling and finish.
//
// On an update, when every update rule that admitted the caller at the stored
// row has a fields list, each unit that none of those lists names keeps its
// stored value. Then, on a create or an update, each guarded unit whose guard
// does not admit the caller keeps its stored value, or gets the zero value on
// a create. On an update, each guarded unit that the caller may not read on
// the stored row keeps its stored value too, unless the write names it or sets
// it: the caller received the unit cleared, so a PUT that leaves it unset does
// not mean to clear it. A guard decides on msg after the first step: its checks read
// only fields that no guard restricts.
//
// named are units the write names outright, as a PATCH mask does; a guarded
// one the caller may not write is refused rather than carried over.
func (w *accessWrite) carryGuarded(tx storage.ReadTx, msg proto.Message, named []string) error {
	if w == nil {
		return nil
	}
	var base proto.Message
	if w.live {
		base = w.stored.Interface()
		if units := w.unlistedUnits(); len(units) > 0 {
			if err := carryProtectedFields(w.info, units, base, msg); err != nil {
				return err
			}
		}
	}
	if len(w.info.access.guards) == 0 {
		return nil
	}
	op := opCreate
	if w.live {
		op = opUpdate
	}
	fr := msg.ProtoReflect()
	for i := range w.info.access.guards {
		g := &w.info.access.guards[i]
		ok, err := w.guardAdmits(tx, g, op, fr)
		if err != nil {
			return err
		}
		if ok {
			if !w.live || !g.hides() {
				continue
			}
			reads, err := w.guardReads(tx, g)
			if err != nil {
				return err
			}
			if reads {
				continue
			}
			// The caller received each unit cleared, so a PUT that leaves one
			// unset sends back what it was given, and a set one is a value the
			// caller wrote.
			var carried []string
			for _, unit := range g.units {
				if !slices.Contains(named, unit) && !unitSet(w.info, fr, unit) {
					carried = append(carried, unit)
				}
			}
			if err := carryProtectedFields(w.info, carried, base, msg); err != nil {
				return err
			}
			continue
		}
		for _, unit := range named {
			if slices.Contains(g.units, unit) {
				w.gate.logRefusal(op, w.info, w.key, func() []string {
					return []string{fmt.Sprintf("guard of %q admits no %s of the field by this caller", unit, opName(op))}
				})
				return guardRefusal(w.info, w.key, unit)
			}
		}
		if err := carryProtectedFields(w.info, g.units, base, msg); err != nil {
			return err
		}
	}
	return nil
}

// unlistedUnits returns the units an update may not change because no update
// rule that admitted the caller at the stored row lists them, or nil when one
// of those rules has no fields list.
func (w *accessWrite) unlistedUnits() []string {
	var listed []string
	for _, rule := range w.updating {
		if rule.fields == nil {
			return nil
		}
		listed = append(listed, rule.fields...)
	}
	if len(w.updating) == 0 {
		return nil
	}
	var out []string
	for _, unit := range entityUnits(w.info) {
		if !slices.Contains(listed, unit) {
			out = append(out, unit)
		}
	}
	return out
}

// entityUnits returns the units of an entity's rows other than its key
// fields, in field order: the top-level fields outside a oneof, and the
// oneofs, each by name.
func entityUnits(info *entityInfo) []string {
	desc := info.msgType.Descriptor()
	var out []string
	fields := desc.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if slices.Contains(info.keyFields, fd) {
			continue
		}
		if unit := unitOf(fd); !slices.Contains(out, unit) {
			out = append(out, unit)
		}
	}
	return out
}

// checkGuardedMask refuses a PATCH whose mask writes into a guarded unit that
// its guard does not admit the caller to update, at the stored row and at
// final, the row as it will be stored.
func (w *accessWrite) checkGuardedMask(tx storage.ReadTx, final protoreflect.Message) error {
	if len(w.info.access.guards) == 0 {
		return nil
	}
	var checked []string
	for _, path := range w.mask {
		unit := protectedHead(path)
		g := w.info.access.guardFor(unit)
		if g == nil || slices.Contains(checked, unit) {
			continue
		}
		checked = append(checked, unit)
		ok, err := w.guardAdmits(tx, g, opUpdate, final)
		if err != nil {
			return err
		}
		if !ok {
			w.gate.logRefusal(opUpdate, w.info, w.key, func() []string {
				return []string{fmt.Sprintf("guard of %q admits no update of the field by this caller", unit)}
			})
			return guardRefusal(w.info, w.key, unit)
		}
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
				ok, err := w.access.admitsThrough(tx, rule, w.info, w.key, final, changed)
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
	if ma.TrustedOnly {
		return Reject(CodeAccessDenied, fmt.Sprintf("mutator %q is open only to trusted callers", mi.name))
	}
	if ma.Admitted {
		return nil
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
