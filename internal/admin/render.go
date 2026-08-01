package admin

import (
	"context"
	"hash/fnv"
	"strings"
	"unicode"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/internal/admin/ui"
)

// render.go turns registered proto descriptors and stored rows into the UI view
// models — purely from the schema, with no per-app configuration. Enums become
// colored pills, references resolve to labelled chips/initials avatars, repeated
// scalars become chips; a value's color is a stable hash of its text, so the
// same value is the same color everywhere.

// paletteSize must match the c0..cN-1 slots defined in admin.css / ui.model.
const paletteSize = 8

// countCap bounds the per-entity sidebar count scan; past it the count renders
// as "N+".
const countCap = 500

// colorSlot maps an arbitrary string to a stable palette slot.
func colorSlot(s string) int {
	if s == "" {
		return -1
	}
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() % paletteSize)
}

// buildNav assembles the sidebar for app/ws with active entity highlighted.
// Counts are scanned for the chosen workspace; with no workspace they are
// omitted. active may be "" (no entity selected).
func (h *handler) buildNav(ctx context.Context, srv *server.Server, app, ws, active string) (ui.Nav, error) {
	nav := ui.Nav{
		Brand:    "Syncplane",
		Tagline:  "admin",
		AppID:    app,
		AppLabel: titleCase(app),
		AppSub:   "sync app",
		Status:   "",
	}

	schemas, err := h.mgr.ListAppSchemas(ctx)
	if err != nil {
		return ui.Nav{}, err
	}
	nav.AppCount = len(schemas)
	for _, sc := range schemas {
		nav.AppOptions = append(nav.AppOptions, ui.NavOption{
			Label:    sc.AppID,
			URL:      "/apps/" + sc.AppID,
			Selected: sc.AppID == app,
		})
		if sc.AppID == app && sc.Revision != "" {
			nav.AppSub = "rev " + shortRev(sc.Revision)
		}
	}

	workspaces, err := srv.WorkspaceIDs(ctx)
	if err != nil {
		return ui.Nav{}, err
	}
	nav.Workspace = ws
	entities := srv.Entities()
	target := active
	if target == "" && len(entities) > 0 {
		target = entities[0].Name
	}
	for _, w := range workspaces {
		nav.WSOptions = append(nav.WSOptions, ui.NavOption{
			Label:    w,
			URL:      entityURL(app, w, target),
			Selected: w == ws,
		})
	}

	counts := map[string]countResult{}
	if ws != "" {
		counts, err = h.countEntities(ctx, srv, ws, entities)
		if err != nil {
			return ui.Nav{}, err
		}
	}
	for _, e := range entities {
		c := counts[e.Name]
		nav.Entities = append(nav.Entities, ui.NavEntity{
			Name:   e.Name,
			Label:  titleCase(e.Name),
			URL:    entityURL(app, ws, e.Name),
			Count:  c.n,
			More:   c.more,
			Color:  colorSlot(e.Name),
			Active: e.Name == active,
		})
	}
	return nav, nil
}

type countResult struct {
	n    int
	more bool
}

// countEntities counts live rows per entity in one read snapshot, bounded by
// countCap.
func (h *handler) countEntities(ctx context.Context, srv *server.Server, ws string, entities []server.EntityMeta) (map[string]countResult, error) {
	out := map[string]countResult{}
	err := srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
		for _, e := range entities {
			n := 0
			prototype := dynamicpb.NewMessage(e.Descriptor)
			_, more, err := tx.ScanPage(prototype, "", countCap, func(proto.Message, uint64) error {
				n++
				return nil
			})
			if err != nil {
				return err
			}
			out[e.Name] = countResult{n: n, more: more}
		}
		return nil
	})
	return out, err
}

// displayField picks the field whose value labels a row: the first non-key
// string field, else the first key field, else nil.
func displayField(meta server.EntityMeta) protoreflect.FieldDescriptor {
	keySet := map[protoreflect.Name]bool{}
	for _, kf := range meta.KeyFields {
		keySet[kf.Name()] = true
	}
	fields := meta.Descriptor.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if !keySet[fd.Name()] && !fd.IsList() && fd.Kind() == protoreflect.StringKind {
			return fd
		}
	}
	if len(meta.KeyFields) > 0 {
		return meta.KeyFields[0]
	}
	return nil
}

// displayLabel is a row's primary-column text: its display-field value, or key.
func displayLabel(meta server.EntityMeta, msg proto.Message, key string) string {
	fd := displayField(meta)
	if fd != nil {
		if s := scalarString(fd, msg.ProtoReflect().Get(fd)); s != "" {
			return s
		}
	}
	return key
}

// columnFields returns the entity's table columns (in proto order): every
// non-key field that renders inline (scalars, enums, bools, references, repeated
// scalars), excluding the primary display field. Sub-messages, maps, keyed
// message lists and opaque fields are omitted — open the row to see them.
func columnFields(meta server.EntityMeta) []protoreflect.FieldDescriptor {
	keySet := map[protoreflect.Name]bool{}
	for _, kf := range meta.KeyFields {
		keySet[kf.Name()] = true
	}
	primary := displayField(meta)
	var cols []protoreflect.FieldDescriptor
	fields := meta.Descriptor.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if keySet[fd.Name()] || fd == primary {
			continue
		}
		if _, ok := cellKindOf(fd); ok {
			cols = append(cols, fd)
		}
	}
	return cols
}

// cellKindOf classifies how a field renders as a table cell, and whether it is
// shown inline at all.
func cellKindOf(fd protoreflect.FieldDescriptor) (ui.CellKind, bool) {
	switch {
	case server.RefKeyFields(fd) != nil:
		return ui.CellChips, true // singular or repeated reference
	case isText(fd):
		return ui.CellText, true // collaborative text: shown as its plain string
	case isOpaque(fd), fd.IsMap():
		return 0, false
	case fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind:
		return 0, false // sub-message or keyed list
	case fd.IsList():
		return ui.CellChips, true // repeated scalar
	case fd.Kind() == protoreflect.BoolKind:
		return ui.CellBool, true
	case fd.Kind() == protoreflect.EnumKind:
		return ui.CellPill, true
	default:
		return ui.CellText, true
	}
}

// buildColumns renders the column headers for cols.
func buildColumns(cols []protoreflect.FieldDescriptor) []ui.Column {
	out := make([]ui.Column, len(cols))
	for i, fd := range cols {
		out[i] = ui.Column{Label: humanLabel(fd.Name()), Numeric: isNumeric(fd.Kind())}
	}
	return out
}

// humanLabel renders a field name for display, turning snake_case into spaced
// words (e.g. "due_date" → "due date"). Table headers upper-case via CSS; panel
// labels stay lower-case, matching the single-token entity labels.
func humanLabel(name protoreflect.Name) string {
	return strings.ReplaceAll(string(name), "_", " ")
}

// buildRow renders one stored row into the browse model, resolving reference
// labels through tx (cached across the page).
func (h *handler) buildRow(tx *server.ViewTx, srv *server.Server, meta server.EntityMeta, cols []protoreflect.FieldDescriptor, key string, version uint64, msg proto.Message, cache labelCache) ui.Row {
	cells := make([]ui.Cell, len(cols))
	for i, fd := range cols {
		cells[i] = h.buildCell(tx, srv, fd, msg, cache)
	}
	return ui.Row{Key: key, Label: displayLabel(meta, msg, key), Version: version, Cells: cells}
}

func (h *handler) buildCell(tx *server.ViewTx, srv *server.Server, fd protoreflect.FieldDescriptor, msg proto.Message, cache labelCache) ui.Cell {
	m := msg.ProtoReflect()
	kind, _ := cellKindOf(fd)
	switch kind {
	case ui.CellBool:
		return ui.Cell{Kind: ui.CellBool, Bool: m.Get(fd).Bool()}
	case ui.CellPill:
		return ui.Cell{Kind: ui.CellPill, Pill: pillFor(fd, m.Get(fd))}
	case ui.CellChips:
		if server.RefKeyFields(fd) != nil { // reference: labelled chip(s) with avatar
			var refs []protoreflect.Message
			if fd.IsList() {
				list := m.Get(fd).List()
				for i := 0; i < list.Len(); i++ {
					refs = append(refs, list.Get(i).Message())
				}
			} else if m.Has(fd) {
				refs = append(refs, m.Get(fd).Message())
			}
			if len(refs) == 0 {
				return ui.Cell{Kind: ui.CellText, Muted: true, Text: "—"}
			}
			chips := make([]ui.Pill, 0, len(refs))
			for _, r := range refs {
				label := h.refLabel(tx, srv, fd, r, cache)
				chips = append(chips, ui.Pill{Label: label, Color: colorSlot(label), Initials: initials(label)})
			}
			return ui.Cell{Kind: ui.CellChips, Chips: chips}
		}
		list := m.Get(fd).List() // repeated scalar
		chips := make([]ui.Pill, 0, list.Len())
		for i := 0; i < list.Len(); i++ {
			s := scalarString(fd, list.Get(i))
			chips = append(chips, ui.Pill{Label: s, Color: colorSlot(s)})
		}
		return ui.Cell{Kind: ui.CellChips, Chips: chips}
	default:
		var s string
		if isText(fd) {
			s = textString(fd, m)
		} else {
			s = scalarString(fd, m.Get(fd))
		}
		return ui.Cell{Kind: ui.CellText, Text: s, Muted: s == "", Numeric: isNumeric(fd.Kind())}
	}
}

// pillFor renders an enum value as a colored pill.
func pillFor(fd protoreflect.FieldDescriptor, v protoreflect.Value) ui.Pill {
	name := ""
	if ev := fd.Enum().Values().ByNumber(v.Enum()); ev != nil {
		name = string(ev.Name())
	}
	return ui.Pill{Label: enumLabel(name), Color: colorSlot(name)}
}

// labelCache memoizes resolved reference labels by "entity\x00key".
type labelCache map[string]string

// refLabel resolves a reference value (a message holding the target's key
// fields) to the target row's display label, loading the row through tx. Falls
// back to the key, then to the raw key-field text.
func (h *handler) refLabel(tx *server.ViewTx, srv *server.Server, fd protoreflect.FieldDescriptor, ref protoreflect.Message, cache labelCache) string {
	return h.refLabelByTarget(tx, srv, entityName(fd.Message()), ref, cache)
}

// refLabelByTarget resolves a reference to the named target entity to its
// display label, loading the row through tx. Falls back to the row key, then to
// the joined key-field text. It backs both the browse table cells and the
// edit-form ref chips.
func (h *handler) refLabelByTarget(tx *server.ViewTx, srv *server.Server, target string, ref protoreflect.Message, cache labelCache) string {
	meta, ok := srv.Entity(target)
	if !ok {
		return ""
	}
	// The reference message shares the target entity's descriptor, so EntityKey
	// reads its key directly.
	_, key, err := srv.EntityKey(ref.Interface())
	if err != nil || key == "" {
		return joinKeyText(meta.KeyFields, ref)
	}
	return h.refLabelByKey(tx, srv, target, key, cache)
}

// refLabelByKey resolves one row key of the named target entity to its display
// label, loading the row through tx and falling back to the key itself. The
// filter bar reaches for this one directly: a reference operand carries the
// target's key as its literal, with no stub message around it.
func (h *handler) refLabelByKey(tx *server.ViewTx, srv *server.Server, target, key string, cache labelCache) string {
	if cached, hit := cache[target+"\x00"+key]; hit {
		return cached
	}
	meta, ok := srv.Entity(target)
	if !ok {
		return key
	}
	row := dynamicpb.NewMessage(meta.Descriptor)
	found, _, err := tx.Get(row, key)
	label := key
	if err == nil && found {
		label = displayLabel(meta, row, key)
	}
	cache[target+"\x00"+key] = label
	return label
}

// refKeyText joins a reference's key-field values, for when the target row can't
// be resolved.
func refKeyText(fd protoreflect.FieldDescriptor, ref protoreflect.Message) string {
	return joinKeyText(server.RefKeyFields(fd), ref)
}

// joinKeyText joins the given key-field values of m with "/".
func joinKeyText(keyFields []protoreflect.FieldDescriptor, m protoreflect.Message) string {
	parts := make([]string, 0, len(keyFields))
	for _, kf := range keyFields {
		parts = append(parts, scalarString(kf, m.Get(kf)))
	}
	return strings.Join(parts, "/")
}

// initials returns up to two upper-case initials from a label, for an avatar.
func initials(s string) string {
	fields := strings.Fields(s)
	if len(fields) == 0 {
		return "?"
	}
	first := []rune(fields[0])
	if len(fields) == 1 {
		if len(first) >= 2 {
			return strings.ToUpper(string(first[:2]))
		}
		return strings.ToUpper(string(first[:1]))
	}
	last := []rune(fields[len(fields)-1])
	return strings.ToUpper(string(first[0]) + string(last[0]))
}

// enumLabel humanizes an enum value name: SCREAMING_SNAKE -> "screaming snake".
func enumLabel(name string) string {
	if name == "" {
		return ""
	}
	return strings.ToLower(strings.ReplaceAll(name, "_", " "))
}

// shortRev abbreviates a revision hash for the switcher sub-line.
func shortRev(rev string) string {
	if len(rev) > 10 {
		return rev[:10]
	}
	return rev
}

// titleCase upper-cases the first letter of s (entity/app labels are single
// tokens like "todo").
func titleCase(s string) string {
	if s == "" {
		return s
	}
	r := []rune(s)
	r[0] = unicode.ToUpper(r[0])
	return string(r)
}
