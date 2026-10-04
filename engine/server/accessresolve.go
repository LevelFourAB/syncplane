package server

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/levelfourab/syncplane/engine/storage"
)

// SubjectsFunc returns the subject strings that the grant rows of a caller
// may name. See WithSubjects.
type SubjectsFunc func(p Principal) []string

// GlobalRolesFunc returns the roles a caller holds at the workspace root of
// every workspace. See WithGlobalRoles.
type GlobalRolesFunc func(ctx context.Context, p Principal) ([]string, error)

// WithSubjects sets the subjects a caller's grant rows may name in the access
// model: a grant row applies to a caller when its subject field equals one of
// them. The default is the caller's Principal.Subject, and none for an
// anonymous caller.
//
// Empty strings in the result are ignored, so an empty subject never matches a
// grant row. A deployment that accepts more than one issuer should return
// subjects that name the issuer, so that two issuers' callers with the same
// Subject do not share grants.
//
// The first non-empty subject is the caller's primary subject: an enforced
// create stores it in each field marked (syncplane.v1.field_behavior).creator.
//
// fn runs once per request, before any read transaction opens, and must be
// safe for concurrent use. A long-lived stream calls it again on every
// heartbeat. In a schema that does not use the access model, only the primary
// subject has an effect.
func WithSubjects(fn SubjectsFunc) Option {
	return func(s *Server) error {
		s.subjects = fn
		return nil
	}
}

// WithGlobalRoles sets the roles a caller holds at the workspace root of every
// workspace, in addition to what its grant rows give. Use it for access that
// comes from outside the data, such as staff roles in an identity-provider
// claim:
//
//	server.WithGlobalRoles(func(_ context.Context, p server.Principal) ([]string, error) {
//		if staff, _ := p.StringClaim("staff"); staff == "admin" {
//			return []string{"org_admin"}, nil
//		}
//		return nil, nil
//	})
//
// Each role must be declared in the grant option of an entity in the schema.
// A role that is not fails the request with an internal error, rather than
// granting nothing in silence. Any other error from fn fails the request as
// retryable; it is never read as a denial.
//
// fn runs once per request, before any read transaction opens, and must be
// safe for concurrent use. A long-lived stream calls it again on every
// heartbeat. It has no effect on a schema that does not use the access model.
func WithGlobalRoles(fn GlobalRolesFunc) Option {
	return func(s *Server) error {
		s.globalRoles = fn
		return nil
	}
}

// accessCaller is a caller's identity in the access model, resolved once per
// request by the workspace gate: the subjects its grant rows may name and the
// roles it holds at the root of every workspace.
type accessCaller struct {
	model *accessModel
	// subjects are sorted and without repeats; primary is the first one the
	// hook returned, "" when there is none.
	subjects    []string
	primary     string
	globalRoles []string
}

// resolveCaller runs the WithSubjects and WithGlobalRoles hooks for p against
// model. It returns nil for a schema that does not use the model.
func (s *Server) resolveCaller(ctx context.Context, p Principal, model *accessModel) (*accessCaller, error) {
	if model == nil {
		return nil, nil
	}
	c := &accessCaller{model: model}
	for _, subject := range s.subjectsOf(p) {
		if subject != "" && !slices.Contains(c.subjects, subject) {
			c.subjects = append(c.subjects, subject)
		}
	}
	if len(c.subjects) > 0 {
		c.primary = c.subjects[0]
	}
	sort.Strings(c.subjects)
	if s.globalRoles != nil {
		roles, err := s.globalRoles(ctx, p)
		if err != nil {
			return nil, fmt.Errorf("server: resolving global roles: %w", err)
		}
		for _, role := range roles {
			if model.roles[role] == nil {
				return nil, connect.NewError(connect.CodeInternal, fmt.Errorf(
					"server: global role %q is not declared by any grant entity of the schema", role))
			}
			if !slices.Contains(c.globalRoles, role) {
				c.globalRoles = append(c.globalRoles, role)
			}
		}
		sort.Strings(c.globalRoles)
	}
	return c, nil
}

// subjectsOf returns the subjects of p as WithSubjects maps them, in the
// order the hook returns them: Principal.Subject without the hook.
func (s *Server) subjectsOf(p Principal) []string {
	if s.subjects != nil {
		return s.subjects(p)
	}
	if p.Subject != "" {
		return []string{p.Subject}
	}
	return nil
}

// primarySubjectOf returns the primary subject of p: the first non-empty
// subject WithSubjects returns, "" for an anonymous caller.
func (s *Server) primarySubjectOf(p Principal) string {
	for _, subject := range s.subjectsOf(p) {
		if subject != "" {
			return subject
		}
	}
	return ""
}

// callerSubjects returns the subjects of c with the primary subject first,
// the rest sorted, as a client receives them.
func callerSubjects(c *accessCaller) []string {
	if c.primary == "" {
		return nil
	}
	out := make([]string, 0, len(c.subjects))
	out = append(out, c.primary)
	for _, subject := range c.subjects {
		if subject != c.primary {
			out = append(out, subject)
		}
	}
	return out
}

// accessSet is what one caller holds in one workspace, as of one snapshot:
// for each permission, the anchors the caller holds it at. A permission
// absent from perms is held nowhere.
type accessSet struct {
	perms map[string]*permAnchors
}

// permAnchors are the anchors one permission is held at: the workspace root,
// or individual rows.
type permAnchors struct {
	root bool
	rows map[anchorRow]struct{}
}

// anchorRow names one row of a workspace.
type anchorRow struct {
	entity, key string
}

func (s *accessSet) anchors(perm string) *permAnchors {
	a := s.perms[perm]
	if a == nil {
		a = &permAnchors{}
		s.perms[perm] = a
	}
	return a
}

func (s *accessSet) grantRoot(perms []string) {
	for _, p := range perms {
		s.anchors(p).root = true
	}
}

func (s *accessSet) grantAt(perms []string, at anchorRow) {
	for _, p := range perms {
		a := s.anchors(p)
		if a.rows == nil {
			a.rows = map[anchorRow]struct{}{}
		}
		a.rows[at] = struct{}{}
	}
}

// any reports whether the caller holds at least one permission anywhere,
// which is what admits it to the workspace.
func (s *accessSet) any() bool {
	for _, a := range s.perms {
		if a.root || len(a.rows) > 0 {
			return true
		}
	}
	return false
}

// rootAnchors holds a permission at the workspace root. It is shared, so
// nothing may change it.
var rootAnchors = &permAnchors{root: true}

// readAnchors returns where the set lets the caller read the rows that r
// governs: the union of the anchors of r's permissions, or the workspace root
// when r admits every admitted caller. It returns nil when the caller holds
// none of r's permissions. The result can be shared with the set, so nothing
// may change it.
func (s *accessSet) readAnchors(r readers) *permAnchors {
	if r.all {
		return rootAnchors
	}
	if len(r.perms) == 1 {
		return s.perms[r.perms[0]]
	}
	var union *permAnchors
	for _, p := range r.perms {
		a := s.perms[p]
		switch {
		case a == nil:
		case a.root:
			return rootAnchors
		case len(a.rows) > 0:
			if union == nil {
				union = &permAnchors{rows: make(map[anchorRow]struct{}, len(a.rows))}
			}
			for at := range a.rows {
				union.rows[at] = struct{}{}
			}
		}
	}
	return union
}

func (a *permAnchors) holds(entity, key string) bool {
	if a == nil {
		return false
	}
	if a.root {
		return true
	}
	_, ok := a.rows[anchorRow{entity: entity, key: key}]
	return ok
}

// resolveAccess reads what the caller holds from the grant rows in tx, and
// adds its global roles at the root.
//
// A grant row counts only when its subject is one of the caller's, its role is
// declared, and its anchor exists: a grant entity with an access parent
// anchors at the parent row, which must be live in tx. The read is one subject
// probe per grant entity and subject while the subject index is ready, and a
// scan of the grant entity otherwise.
func resolveAccess(tx storage.ReadTx, c *accessCaller, limits Limits) (*accessSet, error) {
	set := &accessSet{perms: map[string]*permAnchors{}}
	for _, role := range c.globalRoles {
		set.grantRoot(c.model.roles[role].perms)
	}
	if len(c.subjects) == 0 {
		return set, nil
	}
	for _, g := range c.model.grantEntities {
		type pending struct {
			perms []string
			at    anchorRow
		}
		var parented []pending
		err := grantRowsOf(tx, g, c.subjects, limits, func(msg protoreflect.Message) {
			if !slices.Contains(c.subjects, msg.Get(g.grant.subject).String()) {
				return
			}
			perms := g.grant.roles[grantRoleOf(g.grant, msg)]
			if perms == nil {
				return
			}
			if !g.access.hasParent() {
				set.grantRoot(perms)
				return
			}
			at, ok := parentOf(msg, c.model.parents[g.name])
			if !ok {
				return
			}
			parented = append(parented, pending{perms: perms, at: at})
		})
		if err != nil {
			return nil, err
		}
		if len(parented) == 0 {
			continue
		}
		// A grant anchored at a row that does not exist grants nothing, so a
		// grant row cannot reserve access to a key before the row is created.
		keys := map[string][]string{}
		for _, p := range parented {
			keys[p.at.entity] = append(keys[p.at.entity], p.at.key)
		}
		entities := make([]string, 0, len(keys))
		for entity := range keys {
			entities = append(entities, entity)
		}
		sort.Strings(entities)
		live := make(map[anchorRow]bool, len(parented))
		for _, entity := range entities {
			ks := keys[entity]
			for start := 0; start < len(ks); start += maxRowBatch {
				batch := ks[start:min(start+maxRowBatch, len(ks))]
				if err := tx.GetRows(entity, batch, func(row storage.Row) error {
					if !row.Deleted {
						live[anchorRow{entity: entity, key: row.Key}] = true
					}
					return nil
				}); err != nil {
					return nil, err
				}
			}
		}
		for _, p := range parented {
			if live[p.at] {
				set.grantAt(p.perms, p.at)
			}
		}
	}
	return set, nil
}

// grantRowsOf calls fn with every live, decodable row of the grant entity g
// whose subject may be one of subjects. fn still has to compare the subject:
// the scan fallback yields every row.
func grantRowsOf(tx storage.ReadTx, g *entityInfo, subjects []string, limits Limits, fn func(protoreflect.Message)) error {
	visit := func(row storage.Row) error {
		if row.Deleted {
			return nil
		}
		msg, err := g.decode(row.Value)
		if err != nil {
			// A grant row that does not decode grants nothing. The read paths
			// quarantine and report it when they meet it.
			return nil
		}
		fn(msg.ProtoReflect())
		return nil
	}
	ci := g.indexByName(grantSubjectIndexName(g.grant))
	ready := false
	if ci != nil && !limits.DisableIndexes {
		var err error
		if ready, err = ci.ready(tx, g.name); err != nil {
			return err
		}
	}
	if !ready {
		return tx.ScanEntity(g.name, visit)
	}
	var keys []string
	for _, subject := range subjects {
		lo, hi, err := ci.probeRefIndex([]protoreflect.Value{protoreflect.ValueOfString(subject)})
		if err != nil {
			return err
		}
		if err := tx.ScanIndex(g.name, ci.name, lo, hi, 0, storage.ScanAsc, func(_ []byte, rowKey string) error {
			keys = append(keys, rowKey)
			return nil
		}); err != nil {
			return err
		}
	}
	for start := 0; start < len(keys); start += maxRowBatch {
		if err := tx.GetRows(g.name, keys[start:min(start+maxRowBatch, len(keys))], visit); err != nil {
			return err
		}
	}
	return nil
}

// grantRoleOf returns the role name a grant row holds, or "" when its role
// field holds an enum number the enum does not declare.
func grantRoleOf(g *grantSpec, msg protoreflect.Message) string {
	v := msg.Get(g.role)
	if g.role.Kind() != protoreflect.EnumKind {
		return v.String()
	}
	if ev := g.role.Enum().Values().ByNumber(v.Enum()); ev != nil {
		return string(ev.Name())
	}
	return ""
}

// modelReadFilter returns the filter the access model puts on reads of info
// for the bound caller, or nil when the model hides no row of it: the entity
// is outside the model, or a read rule without a subject check admits the
// caller at the root. A row is readable when the caller holds the permission
// of one of the read rules at the row or at a row up its parent chain, and
// the row passes that rule's subject check.
func (e *rowEmitter) modelReadFilter(info *entityInfo) readFilter {
	a := info.access
	if a == nil {
		return nil
	}
	anchors := e.access.readAnchors(a.read)
	if anchors != nil && anchors.root {
		return nil
	}
	if anchors != nil && len(anchors.rows) == 0 {
		anchors = nil
	}
	owned := ownedReadsOf(a.read, e.caller, e.access)
	if anchors == nil && len(owned) == 0 {
		return denyAll
	}
	chains := e.chains
	subjects := e.caller.subjects
	return func(key string, row proto.Message) (bool, error) {
		r := row.ProtoReflect()
		if anchors != nil {
			if ok, err := chains.holds(anchors, info, key, r); err != nil || ok {
				return ok, err
			}
		}
		for _, o := range owned {
			if !o.rule.subjectHolds(subjects, r) {
				continue
			}
			if ok, err := chains.holds(o.anchors, info, key, r); err != nil || ok {
				return ok, err
			}
		}
		return false, nil
	}
}

// ownedRead is a read rule with a subject check, and where the caller holds
// its source: the workspace root for an admitted rule.
type ownedRead struct {
	rule    accessRule
	anchors *permAnchors
}

// ownedReadsOf returns the read rules of r with a subject check that can
// admit caller under set: the caller has a subject, and holds the rule's
// source somewhere.
func ownedReadsOf(r readers, caller *accessCaller, set *accessSet) []ownedRead {
	if len(r.owned) == 0 || len(caller.subjects) == 0 {
		return nil
	}
	var out []ownedRead
	for _, rule := range r.owned {
		anchors := rootAnchors
		if !rule.admitted() {
			anchors = set.perms[rule.permission]
		}
		if anchors != nil && (anchors.root || len(anchors.rows) > 0) {
			out = append(out, ownedRead{rule: rule, anchors: anchors})
		}
	}
	return out
}

// appReadFilter adapts an application's RowFilter to a readFilter.
func appReadFilter(f RowFilter) readFilter {
	if f == nil {
		return nil
	}
	return func(key string, row proto.Message) (bool, error) {
		return f(key, row), nil
	}
}

// andFilters returns a filter that admits a row only when both a and b do. A
// nil filter admits every row.
func andFilters(a, b readFilter) readFilter {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	}
	return func(key string, row proto.Message) (bool, error) {
		ok, err := a(key, row)
		if err != nil || !ok {
			return false, err
		}
		return b(key, row)
	}
}

// errNoAccess is the settled denial for a caller who holds no permission in a
// workspace whose schema uses the access model.
var errNoAccess = DenyWorkspace("no access to this workspace")

// admitAccess resolves what the caller holds from the primary and refuses a
// caller that holds nothing. The primary is read so that a replica that has
// not yet seen a new grant cannot turn into a permanent denial.
func (s *Server) admitAccess(ctx context.Context, workspaceID string, c *accessCaller) (*accessSet, error) {
	var set *accessSet
	err := s.store.WithReadTx(ctx, workspaceID, func(tx storage.ReadTx) error {
		var err error
		set, err = resolveAccess(tx, c, s.limits)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("server: resolving access: %w", err)
	}
	if !set.any() {
		return nil, errNoAccess
	}
	return set, nil
}

// errAccessUnbound reports a read path that consulted a read filter of a model
// entity before binding the caller's access. It is a defect in the engine,
// never a decision about the caller, and it fails closed.
var errAccessUnbound = errors.New("server: access model read before the caller's access was resolved")

// accessNarrows reports whether the bound access set can narrow the rows of
// info: the entity is in the model and no read rule admits the caller at the
// root. It reads nothing.
func (e *rowEmitter) accessNarrows(info *entityInfo) bool {
	a := info.access
	if e.caller == nil || e.access == nil || a == nil {
		return false
	}
	anchors := e.access.readAnchors(a.read)
	return anchors == nil || !anchors.root
}

// accessCandidates returns, in key order, the keys of the rows of info that the
// bound access set can let the caller read: the anchors of its read
// permissions that are rows of info, and the rows of info below such an
// anchor (see rowsBelow). For a read rule with a subject check, the rows whose
// subject field names the caller stand in when its subject index is ready. It
// is a superset of the rows the model's filter admits, so reading these keys
// and classifying each row gives what a scan would. ok is false when the set
// does not narrow the entity, or when narrowing would cost more than
// maxCandidates keys or needs an index that is not ready; the caller scans.
func (e *rowEmitter) accessCandidates(tx storage.ReadTx, info *entityInfo, maxCandidates int) (keys []string, ok bool, err error) {
	if !e.accessNarrows(info) {
		return nil, false, nil
	}
	keys, ok, err = rowsBelow(tx, e.caller.model, info, anchorRowsOf(e.access.readAnchors(info.access.read)), maxCandidates, e.srv.limits)
	if err != nil || !ok {
		return nil, false, err
	}
	for _, o := range ownedReadsOf(info.access.read, e.caller, e.access) {
		owned, ok, err := e.ownedCandidates(tx, info, o, maxCandidates)
		if err != nil || !ok {
			return nil, false, err
		}
		if keys, ok = mergeKeys(keys, owned, maxCandidates); !ok {
			return nil, false, nil
		}
	}
	return keys, true, nil
}

// ownedCandidates returns, in key order, a superset of the keys of the rows of
// info that the read rule o admits the bound caller to: the rows whose
// subject field names one of the caller's subjects when the field's index is
// ready, and the rows below the rule's anchors otherwise. ok is false when
// neither can be read within maxCandidates keys.
func (e *rowEmitter) ownedCandidates(tx storage.ReadTx, info *entityInfo, o ownedRead, maxCandidates int) ([]string, bool, error) {
	keys, ok, err := subjectRows(tx, info, o.rule.subject, e.caller.subjects, maxCandidates, e.srv.limits)
	if err != nil || ok {
		return keys, ok, err
	}
	if o.anchors.root {
		return nil, false, nil
	}
	return rowsBelow(tx, e.caller.model, info, anchorRowsOf(o.anchors), maxCandidates, e.srv.limits)
}

// subjectRows returns, in key order, the keys of the rows of info whose field
// fd holds one of subjects, read from the field's index. ok is false when the
// index is not ready or holds more than maxCandidates such rows.
func subjectRows(tx storage.ReadTx, info *entityInfo, fd protoreflect.FieldDescriptor, subjects []string, maxCandidates int, limits Limits) ([]string, bool, error) {
	ci := info.indexByName(fieldIndexName(fd))
	if ci == nil || limits.DisableIndexes {
		return nil, false, nil
	}
	if ready, err := ci.ready(tx, info.name); err != nil || !ready {
		return nil, false, err
	}
	var keys []string
	for _, subject := range subjects {
		lo, hi, err := ci.probeRefIndex([]protoreflect.Value{protoreflect.ValueOfString(subject)})
		if err != nil {
			return nil, false, err
		}
		err = tx.ScanIndex(info.name, ci.name, lo, hi, 0, storage.ScanAsc, func(_ []byte, rowKey string) error {
			keys = append(keys, rowKey)
			if len(keys) > maxCandidates {
				return errCandidateBound
			}
			return nil
		})
		if errors.Is(err, errCandidateBound) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
	}
	sort.Strings(keys)
	return slices.Compact(keys), true, nil
}

// anchorRowsOf returns the rows a is held at, in no order; none for nil.
func anchorRowsOf(a *permAnchors) []anchorRow {
	if a == nil {
		return nil
	}
	out := make([]anchorRow, 0, len(a.rows))
	for at := range a.rows {
		out = append(out, at)
	}
	return out
}

// mergeKeys returns the union of two key lists in key order. ok is false when
// it holds more than maxCandidates keys.
func mergeKeys(a, b []string, maxCandidates int) ([]string, bool) {
	out := make([]string, 0, len(a)+len(b))
	out = append(out, a...)
	out = append(out, b...)
	sort.Strings(out)
	out = slices.Compact(out)
	return out, len(out) <= maxCandidates
}

// errCandidateBound stops an index walk that has found more rows than its
// caller would read by key.
var errCandidateBound = errors.New("server: access candidate bound")

// rowsBelow returns, in key order, the keys of the rows of target that are
// one of from or sit at most Limits.MaxAccessDepth hops below one of them. It
// walks down from each row through the reference index of each case of the
// access parents that point at its entity, entering only the entities that
// lead to target, so it reads index entries and no rows.
//
// The walk follows the parents rows hold now. It is a superset of the rows a
// caller holding a permission at from can read, as long as the caller
// classifies each row it reads. ok is false when the walk would visit more
// than maxCandidates rows, or needs an index that is not ready.
func rowsBelow(tx storage.ReadTx, model *accessModel, target *entityInfo, from []anchorRow, maxCandidates int, limits Limits) (keys []string, ok bool, err error) {
	leads := func(entity string) bool {
		return entity == target.name || slices.Contains(model.below[entity], target.name)
	}
	found := map[string]struct{}{}
	visited := map[anchorRow]struct{}{}
	var frontier []anchorRow
	for _, at := range from {
		if _, dup := visited[at]; !dup && leads(at.entity) {
			visited[at] = struct{}{}
			frontier = append(frontier, at)
		}
	}
	readyIndex := map[*accessEdge]*compiledIndex{}
	indexOf := func(e *accessEdge) (*compiledIndex, bool, error) {
		if ci, known := readyIndex[e]; known {
			return ci, ci != nil, nil
		}
		ci := e.child.indexByName(refIndexName(e.ref.ref))
		if ci == nil || limits.DisableIndexes {
			ci = nil
		} else if ready, err := ci.ready(tx, e.child.name); err != nil {
			return nil, false, err
		} else if !ready {
			ci = nil
		}
		readyIndex[e] = ci
		return ci, ci != nil, nil
	}
	maxDepth := limits.maxAccessDepth()
	for level := 0; len(frontier) > 0; level++ {
		var next []anchorRow
		for _, at := range frontier {
			if at.entity == target.name {
				found[at.key] = struct{}{}
			}
			if level == maxDepth {
				continue
			}
			for _, e := range model.children[at.entity] {
				child := e.child
				if !leads(child.name) {
					continue
				}
				values, parsed := keyValuesOf(model.entities[at.entity], at.key)
				if !parsed {
					return nil, false, nil
				}
				ci, ready, err := indexOf(e)
				if err != nil || !ready {
					return nil, false, err
				}
				lo, hi, err := ci.probeRefIndex(values)
				if err != nil {
					return nil, false, err
				}
				// Stops inside the scan, so one row with many rows below it costs
				// the bound rather than every key it holds.
				err = tx.ScanIndex(child.name, ci.name, lo, hi, 0, storage.ScanAsc, func(_ []byte, rowKey string) error {
					below := anchorRow{entity: child.name, key: rowKey}
					if _, dup := visited[below]; dup {
						return nil
					}
					visited[below] = struct{}{}
					if len(visited) > maxCandidates {
						return errCandidateBound
					}
					next = append(next, below)
					return nil
				})
				if errors.Is(err, errCandidateBound) {
					return nil, false, nil
				}
				if err != nil {
					return nil, false, err
				}
			}
		}
		frontier = next
	}
	if len(found) > maxCandidates {
		return nil, false, nil
	}
	keys = make([]string, 0, len(found))
	for key := range found {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, true, nil
}

// accessRows is the rows of one entity that a query read by a narrowed caller
// has to consider: the keys accessCandidates found. A key outside the set
// names a row the caller cannot read, so a read can drop it without reading
// the row. A nil *accessRows narrows nothing.
type accessRows struct {
	keys []string // ascending
	set  map[string]struct{}
}

// accessRowsOf returns the rows of info a query read by e has to consider, or
// nil when the read must consider every row: e is nil (a trusted read), the
// bound access set does not narrow the entity, or the candidates do not fit in
// MaxIndexScanCandidates. The candidate budget is the index walk's, because
// the two answer the same question: how many rows a read may name before a
// scan is the cheaper way.
func (e *rowEmitter) accessRowsOf(tx storage.ReadTx, info *entityInfo) (*accessRows, error) {
	if e == nil {
		return nil, nil
	}
	keys, ok, err := e.accessCandidates(tx, info, e.srv.limits.maxIndexScanCandidates())
	if err != nil || !ok {
		return nil, err
	}
	set := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		set[key] = struct{}{}
	}
	return &accessRows{keys: keys, set: set}, nil
}

// admits reports whether key may name a row the caller can read.
func (a *accessRows) admits(key string) bool {
	if a == nil {
		return true
	}
	_, ok := a.set[key]
	return ok
}

// scan yields the live rows of the set in key order, or every live row of
// entity when a is nil: what tx.ScanEntity yields, less the rows the caller
// cannot read.
func (a *accessRows) scan(tx storage.ReadTx, entity string, fn func(storage.Row) error) error {
	if a == nil {
		return tx.ScanEntity(entity, fn)
	}
	batch := &rowBatch{tx: tx, entity: entity, size: maxRowBatch, emit: func(row storage.Row) error {
		if row.Deleted {
			return nil
		}
		return fn(row)
	}}
	for _, key := range a.keys {
		if err := batch.add(key); err != nil {
			return err
		}
	}
	return batch.flush()
}

// admitSink drops the keys a rejects before they reach the wrapped sink, so
// an index walk reads no row the caller cannot see. The walk still counts the
// dropped entries against its budget.
type admitSink struct {
	candidateSink
	a *accessRows
}

func (s admitSink) add(rowKey string) error {
	if !s.a.admits(rowKey) {
		return nil
	}
	return s.candidateSink.add(rowKey)
}
