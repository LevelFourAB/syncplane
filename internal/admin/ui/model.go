// Package ui holds the admin UI's templ templates and the view-model types
// they render. The view models are built in the parent admin package (form.go)
// from registered proto descriptors and handed here for rendering, so the
// dependency runs admin -> ui only.
package ui

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/a-h/templ"
)

// paletteSize is the number of color slots the CSS defines (c0..cN-1). Keep in
// sync with admin.css.
const paletteSize = 8

// joinStrings renders a string slice as a comma-separated list for templates.
func joinStrings(ss []string) string { return strings.Join(ss, ", ") }

// countLabel renders a nav count, suffixing "+" when it is a lower bound.
func countLabel(n int, more bool) string {
	if more {
		return fmt.Sprintf("%d+", n)
	}
	return fmt.Sprintf("%d", n)
}

// recordsLabel names what the browse header's count counts: every row of the
// entity, or only the rows the active filter matches.
func recordsLabel(filtered bool) string {
	if filtered {
		return "matching"
	}
	return "records"
}

// slotClass maps a color slot to its CSS class; a negative slot is neutral.
// Components (.pill, .chip, .avatar, .dot) read --slot from this class.
func slotClass(slot int) string {
	if slot < 0 {
		return "neutral"
	}
	return fmt.Sprintf("c%d", ((slot%paletteSize)+paletteSize)%paletteSize)
}

// indexedName renders an indexed input name, e.g. tags[0].
func indexedName(name string, i int) string { return fmt.Sprintf("%s[%d]", name, i) }

// initialOf returns the leading letter of s, upper-cased, for a switcher badge.
func initialOf(s string) string {
	for _, r := range s {
		return strings.ToUpper(string(r))
	}
	return "?"
}

// rowID is a DOM-safe id for an entity row, the htmx delete swap target.
func rowID(entity, key string) string { return "row-" + entity + "-" + domSafe(key) }

// enumID is a DOM-safe id linking an enum radio to its label.
func enumID(name string, i int) string { return "enum-" + domSafe(name) + "-" + fmt.Sprint(i) }

// rowsURL is the browse/route base for an entity in an app + workspace. It
// mirrors the admin package's entityURL so templates can build sub-routes.
func rowsURL(app, ws, entity string) string {
	return "/apps/" + url.PathEscape(app) + "/ws/" + url.PathEscape(ws) + "/e/" + url.PathEscape(entity)
}

// primaryText is the primary-column text for a row: its display label, falling
// back to the key.
func primaryText(r Row) string {
	if r.Label != "" {
		return r.Label
	}
	return r.Key
}

// panelTitle labels the detail slide-over: the row's display label on edit, a
// "New …" heading on create.
func panelTitle(d Detail) string {
	if d.Edit {
		if d.Title != "" {
			return d.Title
		}
		return d.Key
	}
	return "New " + d.Entity
}

// domSafe replaces characters that are invalid in an HTML id with '-'.
func domSafe(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		default:
			return '-'
		}
	}, s)
}

// FieldKind classifies how a field renders.
type FieldKind int

const (
	KindScalar         FieldKind = iota // text/number input
	KindBool                            // checkbox
	KindEnum                            // <select> of enum values
	KindOpaque                          // replaced-whole bytes/message (base64 textarea)
	KindText                            // collaborative text document (plain textarea)
	KindMessage                         // embedded sub-message -> nested fieldset
	KindRef                             // typed reference -> key picker
	KindRepeatedScalar                  // add/remove scalar rows
	KindKeyedList                       // repeated embedded message (keyed list)
	KindMap                             // key/value pairs
)

// FieldView is the rendered model of one field; templates walk it, recursing
// through Children / ListItems.
type FieldView struct {
	Label     string
	TypeLabel string // type chip text, e.g. "string", "enum Status", "repeated User"
	Name      string // input-name base (dotted/indexed)
	Kind      FieldKind
	ReadOnly  bool
	Template  []FieldView // keyed list: blank field set cloned for a new item

	// Scalar / bool / enum / opaque.
	Value   string
	Numeric bool // render <input type=number>

	// Enum.
	EnumOptions []EnumOption

	// Reference.
	Ref RefControl

	// Embedded sub-message.
	Children []FieldView

	// Repeated scalar.
	ScalarItems []string

	// Keyed message list.
	ListItems [][]FieldView

	// Map.
	MapItems []MapItem
}

// InputType is the HTML input type for a scalar field.
func (f FieldView) InputType() string {
	if f.Numeric {
		return "number"
	}
	return "text"
}

// EnumOption is one option of an enum <select>.
type EnumOption struct {
	Value    string // numeric value, submitted
	Label    string // enum value name
	Selected bool
}

// RefControl is the reference picker component: the selected value(s) as
// chips, and a combobox popover listing the target entity's rows. It is shared
// by the row editor's reference field and the filter bar's reference operand,
// so both are the same control and admin.js drives them with one set of hooks.
type RefControl struct {
	Name      string   // input-name base the selection binds to
	Entity    string   // target entity wire name (picker route, aria labels)
	Keys      []string // target key field names
	PickerURL string   // picker endpoint listing target rows in this workspace
	Items     []RefItem
	// Multi allows several targets to be selected: the chips accumulate and
	// the popover stays open, rather than one selection replacing the last.
	Multi    bool
	ReadOnly bool
	// Inline is the filter bar's compact form. It also changes what the
	// selection submits: one input named Name per chip, carrying the target
	// key alone (a filter operand is a literal), rather than the editor's
	// Name[.field] / Name[i].field stubs. Only single-key targets use it.
	Inline bool
}

// RefItem is one selected reference value: the target key field values, keyed
// by field name, rendered as hidden inputs named Name[.field]. Label is the
// resolved display label of the target row (its key, when unresolved), shown on
// the chip so the user sees a name rather than an opaque id.
type RefItem struct {
	Name     string            // input-name base for this element
	Values   map[string]string // key field name -> value
	Label    string            // resolved target display label
	Color    int               // chip color slot (from Label)
	Initials string            // avatar initials (from Label)
}

// Key is the item's value for the target's single key field, which is what an
// inline (filter-operand) chip submits. Empty when the target has no single
// key field, in which case Inline does not apply.
func (c RefControl) Key(it RefItem) string {
	if len(c.Keys) != 1 {
		return ""
	}
	return it.Values[c.Keys[0]]
}

// PickerRow is one selectable target row in the reference picker dropdown: its
// display label, a secondary line (the joined key), and the target key field
// values as a JSON object (field -> value) the client copies into the hidden
// ref inputs on selection.
type PickerRow struct {
	Label    string
	Sub      string // secondary text (the row key)
	Color    int
	Initials string
	KeysJSON string // {"<keyfield>":"<value>", ...}
}

// MapItem is one map entry's key/value pair.
type MapItem struct {
	Name  string // input-name base for this entry
	Key   string
	Value string
}

// Row is one entity row in the browse table: its key, the display label shown
// in the primary column, its version, plus ordered typed cells matching the
// page's Columns.
type Row struct {
	Key     string
	Label   string // primary-column display text (a chosen string field, or the key)
	Version uint64
	Cells   []Cell
}

// Column is one browse-table column header.
type Column struct {
	Label   string
	Numeric bool // right-align numeric/date columns
}

// EntityView is the browse page state: the header, the filter bar, the
// (non-primary) columns, and one page of rows. The primary column is rendered
// from Row.Label/Key.
type EntityView struct {
	AppID        string
	WS           string
	Entity       string // wire name (routes)
	Title        string // display label, e.g. "Todo"
	FullName     string // descriptor full name, e.g. taskflow.v1.Todo
	PrimaryLabel string // header text for the primary column
	Count        int
	More         bool // count is a lower bound
	Columns      []Column
	Rows         []Row
	PageMore     bool   // another page of rows follows
	NextURL      string // browse URL of the next page, carrying the filter

	Filter   FilterView
	Filtered bool // a filter is applied: Count is a match count, not the total
	// FilterError, when set, replaces the table: a condition could not be read,
	// and showing unfiltered rows under a filter the user wrote would mislead.
	FilterError string
}

// Match values for FilterView.Match: how the conditions combine.
const (
	MatchAll = "all"
	MatchAny = "any"
)

// FilterView is the browse page's filter bar — the condition builder above the
// table. It submits as a plain GET form to Action, so the filter lives in the
// URL; RowURL re-renders one condition row when its field or operator changes.
type FilterView struct {
	Action   string              // form target: the entity's browse URL
	RowURL   string              // single-condition-row endpoint (htmx)
	Fields   []FilterFieldOption // every filterable path, for the field select
	Conds    []FilterCondView
	Template FilterCondView // blank row cloned by "+ Condition"
	Match    string         // MatchAll / MatchAny
	Active   bool           // at least one condition is complete and applied
}

// ReloadAttrs are the htmx attributes that re-render one condition row from the
// server. They sit on the field and operator selects only: changing either one
// changes which operators apply and which operand control is needed, while
// typing an operand must not disturb the input being typed into.
func (v FilterView) ReloadAttrs() templ.Attributes {
	return templ.Attributes{
		"hx-get":     v.RowURL,
		"hx-trigger": "change",
		"hx-include": "closest .filter-cond",
		"hx-target":  "closest .filter-cond",
		"hx-swap":    "outerHTML",
	}
}

// FilterFieldOption is one selectable path in a condition's field select.
type FilterFieldOption struct {
	Path  string // dotted proto path, submitted as f[i].path
	Label string
}

// FilterOpOption is one operator in a condition's operator select.
type FilterOpOption struct {
	Value    string
	Label    string
	Selected bool
}

// FilterCondView is one condition row: field · operator · operand. Name is the
// input-name base ("f[0]", or "f[__i__]" in the blank template admin.js
// clones). Ops is empty until a field is chosen.
type FilterCondView struct {
	Name  string
	Path  string
	Ops   []FilterOpOption
	Value ValueControl
	Error string // why this row's operand could not be read

	// Group renders the row as an element-scoped block instead of a pill: the
	// field is a repeated message list and the operator asks whether SOME
	// element satisfies the sub-conditions in Subs. All of them are asked of
	// the same element, which is the whole reason the group exists — see
	// ExistsCond in syncplane/v1/query.proto.
	Group bool
	// Subs are the sub-conditions, whose paths name fields of the ELEMENT.
	// They render as ordinary condition rows, one scope down.
	Subs []FilterCondView
	// SubFields are the element's filterable paths, for a sub-row's field
	// select. SubTemplate is the blank sub-row the group's own "+ Condition"
	// clones; nil when the group is read-only.
	SubFields   []FilterFieldOption
	SubTemplate *FilterCondView
}

// SubName is the input-name base of this group's sub-conditions, which is also
// the repeat group admin.js adds a blank row to.
func (c FilterCondView) SubName() string { return c.Name + ".s" }

// ValueKind selects the control rendered for a condition's operand.
type ValueKind int

const (
	ValueNone   ValueKind = iota // the presence / emptiness operators take no operand
	ValueText                    // free text
	ValueNumber                  // free text, numeric input
	ValueBool                    // yes / no
	ValueEnum                    // one enum value
	ValueList                    // comma-separated operands (the multi-value operators)
	ValueRef                     // reference picker: the operand is a target row's key
)

// ValueControl is the operand control of one condition.
type ValueControl struct {
	Kind        ValueKind
	Value       string       // submitted as f[i].v
	Options     []EnumOption // ValueEnum
	Placeholder string
	Ref         RefControl // ValueRef
}

// InputType is the HTML input type for a free-text operand. A comma-separated
// list stays text however numeric its parts are.
func (v ValueControl) InputType() string {
	if v.Kind == ValueNumber {
		return "number"
	}
	return "text"
}

// CellKind selects how a table cell renders.
type CellKind int

const (
	CellText  CellKind = iota // plain text
	CellPill                  // single colored pill (enum)
	CellChips                 // a row of colored chips (repeated scalar / references)
	CellBool                  // a check (true) / muted dash (false)
)

// Cell is one typed table cell. Only the field matching Kind is populated.
type Cell struct {
	Kind    CellKind
	Text    string // CellText
	Muted   bool   // CellText: render dimmed (empty / secondary)
	Numeric bool   // CellText: right-align (matches the column)
	Bool    bool   // CellBool
	Pill    Pill   // CellPill
	Chips   []Pill // CellChips
}

// Pill is a labelled, color-slotted badge. Color is a palette slot (see
// slotClass); a negative slot renders neutral/gray. Initials, when set, render
// a small avatar bubble inside the chip (used for reference chips).
type Pill struct {
	Label    string
	Color    int
	Initials string
}

// Nav is the sidebar state shared by every shell page.
type Nav struct {
	Brand      string      // product name, e.g. "Syncplane"
	Tagline    string      // e.g. "sync engine · admin"
	AppID      string      // current app id ("" on the app index)
	AppLabel   string      // human label for the current app
	AppSub     string      // app secondary line (revision / description)
	AppOptions []NavOption // app switcher options (value = target URL)
	Workspace  string      // current workspace ("" when none chosen)
	WSOptions  []NavOption // workspace switcher options (value = target URL)
	Entities   []NavEntity // current app's entities with counts
	Status     string      // footer status line
	AppCount   int         // footer app count
}

// NavOption is one <select> option in a switcher: its label, the URL to
// navigate to when chosen, and whether it is the current selection.
type NavOption struct {
	Label    string
	URL      string
	Selected bool
}

// NavEntity is one entity row in the sidebar, with its live row count.
type NavEntity struct {
	Name   string // wire name (route)
	Label  string // display label
	URL    string // browse link for this entity in the current workspace
	Count  int
	More   bool // count hit the scan cap (render as "N+")
	Color  int  // dot color slot
	Active bool
}

// Detail is the slide-over panel state for one row (view/edit).
type Detail struct {
	AppID     string
	Workspace string
	Entity    string // wire name
	FullName  string // descriptor full name, e.g. taskflow.v1.Todo
	Title     string // row display label
	Key       string
	Version   uint64
	Fields    []FieldView
	Edit      bool
}

// EntityRef names an entity within an app, for navigation.
type EntityRef struct {
	Name      string
	KeyFields []string
}

// AppRow is one hosted app in the app-list page.
type AppRow struct {
	AppID    string
	Revision string
	Entities []string
}
