// Package admin is the built-in, read/write web UI for a server.Manager: it
// lists hosted apps, browses each app's entity data per workspace, and
// creates/edits/deletes rows through a generic, reflection-driven proto editor.
//
// It performs trusted, unauthenticated reads (Server.View) and writes
// (Server.Mutate → Put/Patch/Delete). Mount it ONLY on the private admin
// listener — the private-interface binding is the safety boundary while auth is
// out of scope.
package admin

import (
	"context"
	"embed"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/a-h/templ"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/id"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/internal/admin/ui"
)

//go:embed static
var staticFS embed.FS

// pageSize bounds one browse page; "Load more" pages keyset-style by last key.
const pageSize = 50

// Handler builds the admin UI over a Manager, on a stdlib ServeMux using
// method+wildcard patterns. Mount it on the trusted private listener.
func Handler(mgr *server.Manager) http.Handler {
	h := &handler{mgr: mgr, peer: "admin:" + id.New()}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", h.apps)
	mux.HandleFunc("GET /apps/{app}", h.app)
	mux.HandleFunc("GET /apps/{app}/ws/{ws}/e/{entity}", h.rows)
	mux.HandleFunc("GET /apps/{app}/ws/{ws}/e/{entity}/new", h.newForm)
	mux.HandleFunc("GET /apps/{app}/ws/{ws}/e/{entity}/filter", h.filterRow)
	mux.HandleFunc("GET /apps/{app}/ws/{ws}/e/{entity}/picker", h.picker)
	mux.HandleFunc("GET /apps/{app}/ws/{ws}/e/{entity}/rows/{key}", h.editForm)
	mux.HandleFunc("POST /apps/{app}/ws/{ws}/e/{entity}/rows", h.create)
	mux.HandleFunc("PUT /apps/{app}/ws/{ws}/e/{entity}/rows/{key}", h.update)
	mux.HandleFunc("DELETE /apps/{app}/ws/{ws}/e/{entity}/rows/{key}", h.delete)
	mux.Handle("GET /static/", http.FileServerFS(staticFS))
	return mux
}

type handler struct {
	mgr *server.Manager
	// peer is this handler's collaborative-text writer identity, minted once
	// per handler rather than per edit: see bindTextFields.
	peer string
}

// apps lists every hosted app with its schema revision and entity names.
func (h *handler) apps(w http.ResponseWriter, r *http.Request) {
	schemas, err := h.mgr.ListAppSchemas(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rows := make([]ui.AppRow, 0, len(schemas))
	for _, sc := range schemas {
		var ents []string
		if srv := h.mgr.App(sc.AppID); srv != nil {
			for _, e := range appEntities(srv) {
				ents = append(ents, e.Name)
			}
		}
		rows = append(rows, ui.AppRow{AppID: sc.AppID, Revision: sc.Revision, Entities: ents})
	}
	nav := ui.Nav{Brand: "Syncplane", Tagline: "sync engine · admin", Status: "Live · streaming", AppCount: len(schemas)}
	render(w, r, ui.AppsPage(nav, rows))
}

// app redirects into an app's first entity (in its first workspace), or shows
// an empty state when no workspace holds data yet.
func (h *handler) app(w http.ResponseWriter, r *http.Request) {
	srv, app, ok := h.resolveApp(w, r)
	if !ok {
		return
	}
	workspaces, err := srv.WorkspaceIDs(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	entities := appEntities(srv)
	if len(workspaces) > 0 && len(entities) > 0 {
		http.Redirect(w, r, entityURL(app, workspaces[0], entities[0].Name), http.StatusSeeOther)
		return
	}
	nav, err := h.buildNav(r.Context(), srv, app, "", "")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	render(w, r, ui.AppPage(nav, app))
}

// rows renders a paginated rich-cell table (full shell on first load, a
// RowsFragment for "Load more" — distinguished by the paging cursor), narrowed
// by whatever filter the query string carries.
func (h *handler) rows(w http.ResponseWriter, r *http.Request) {
	srv, app, meta, ok := h.resolveEntity(w, r)
	if !ok {
		return
	}
	ws := r.PathValue("ws")
	q := r.URL.Query()
	base := entityURL(app, ws, meta.Name)
	after := q.Get("after")
	offset := pageOffset(q)
	cols := columnFields(meta)
	fs := newFilterState(meta.Descriptor, q)
	where := fs.condition()

	primary := ""
	if pf := displayField(meta); pf != nil {
		primary = string(pf.Name())
	}
	view := ui.EntityView{
		AppID: app, WS: ws, Entity: meta.Name,
		Title: titleCase(meta.Name), FullName: string(meta.Descriptor.FullName()),
		PrimaryLabel: primary,
		Columns:      buildColumns(cols),
		Filter:       fs.view(base, base+"/filter", refContext{appID: app, ws: ws}),
	}
	h.fillRefLabels(r.Context(), srv, ws, &view.Filter)
	// The paging cursor is keyset when nothing is filtered and an offset when
	// something is: a predicate query has no last-key to resume from, and only
	// the first page needs a match count (later pages do not re-render the
	// header that shows it).
	count, countMore := 0, false
	var err error
	switch {
	case fs.err() != "":
		view.FilterError = "Fix the highlighted condition to see rows."
	case where != nil:
		view.Filtered = true
		view.Rows, view.PageMore, count, countMore, err = h.filteredRows(r.Context(), srv, meta, cols, ws, where, offset)
		if view.PageMore {
			view.NextURL = pageURL(base, q, "offset", strconv.Itoa(offset+pageSize))
		}
	default:
		var nextKey string
		view.Rows, view.PageMore, nextKey, err = h.scanRows(r.Context(), srv, meta, cols, ws, after)
		if view.PageMore {
			view.NextURL = pageURL(base, q, "after", nextKey)
		}
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if after != "" || q.Get("offset") != "" {
		render(w, r, ui.RowsFragment(view))
		return
	}
	nav, err := h.buildNav(r.Context(), srv, app, ws, meta.Name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if ne := navEntity(nav, meta.Name); ne != nil {
		view.Count, view.More = ne.Count, ne.More
	}
	if view.Filtered {
		view.Count, view.More = count, countMore
	}
	render(w, r, ui.EntityPage(nav, view))
}

// filterRow re-renders one condition row of the filter bar. Choosing a field or
// an operator reloads only that row, so its operator list and operand control
// follow the chosen path without disturbing the conditions around it. The row's
// index rides in its own input names, so the reload round-trips it.
func (h *handler) filterRow(w http.ResponseWriter, r *http.Request) {
	srv, app, meta, ok := h.resolveEntity(w, r)
	if !ok {
		return
	}
	ws := r.PathValue("ws")
	base := entityURL(app, ws, meta.Name)
	view := newFilterState(meta.Descriptor, r.URL.Query()).
		view(base, base+"/filter", refContext{appID: app, ws: ws})
	h.fillRefLabels(r.Context(), srv, ws, &view)
	cond := view.Template
	if len(view.Conds) > 0 {
		cond = view.Conds[0]
	}
	render(w, r, ui.FilterCond(view, cond))
}

// fillRefLabels resolves every reference operand in the bar to the display
// label of the row it names, in one read snapshot, so a chip reads as "Website
// redesign" rather than as the key the URL carries. A target that cannot be
// read keeps its key, which is still a true description of the condition — a
// filter that renders is worth more than an error page.
func (h *handler) fillRefLabels(ctx context.Context, srv *server.Server, ws string, v *ui.FilterView) {
	type pending struct {
		target string
		key    string
		item   *ui.RefItem
	}
	var items []pending
	collect := func(c *ui.FilterCondView) {
		ref := &c.Value.Ref
		for i := range ref.Items {
			if key := ref.Key(ref.Items[i]); key != "" {
				items = append(items, pending{target: ref.Entity, key: key, item: &ref.Items[i]})
			}
		}
	}
	for i := range v.Conds {
		collect(&v.Conds[i])
		for j := range v.Conds[i].Subs {
			collect(&v.Conds[i].Subs[j])
		}
	}
	if len(items) == 0 {
		return
	}
	cache := labelCache{}
	_ = srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
		for _, p := range items {
			label := h.refLabelByKey(tx, srv, p.target, p.key, cache)
			p.item.Label = label
			p.item.Color = colorSlot(label)
			p.item.Initials = initials(label)
		}
		return nil
	})
}

// rawRow is a row collected inside a read snapshot, rendered only once the scan
// is done so reference labels resolve through that same snapshot.
type rawRow struct {
	key     string
	version uint64
	msg     proto.Message
}

// collect appends one scanned row, resolving its key.
func collect(srv *server.Server, dst *[]rawRow, msg proto.Message, version uint64) error {
	_, key, err := srv.EntityKey(msg)
	if err != nil {
		return err
	}
	*dst = append(*dst, rawRow{key: key, version: version, msg: msg})
	return nil
}

// scanRows loads one unfiltered page in key order, returning the cursor the
// next page resumes from.
func (h *handler) scanRows(
	ctx context.Context,
	srv *server.Server,
	meta server.EntityMeta,
	cols []protoreflect.FieldDescriptor,
	ws, after string,
) (rows []ui.Row, more bool, nextKey string, err error) {
	prototype := dynamicpb.NewMessage(meta.Descriptor)
	err = srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
		var collected []rawRow
		last, m, serr := tx.ScanPage(prototype, after, pageSize, func(msg proto.Message, version uint64) error {
			return collect(srv, &collected, msg, version)
		})
		nextKey, more = last, m
		if serr != nil {
			return serr
		}
		rows = h.buildRows(tx, srv, meta, cols, collected)
		return nil
	})
	return rows, more, nextKey, err
}

// filteredRows loads one page of the rows matching where, plus — on the first
// page — how many rows match in total, bounded by countCap. One query answers
// both: the scan is O(entity) whatever the limit, so asking for the counting
// window costs no extra pass, and later pages do not re-render the header the
// count feeds.
//
// Paging is by offset because a predicate query has no last-key to resume
// from, which means a row written between two pages can shift the ones after
// it. That is the trade the engine's Query makes (see query.Spec) and it is
// the right one here: a browse table is a look at the data, not a cursor
// somebody holds open.
func (h *handler) filteredRows(
	ctx context.Context,
	srv *server.Server,
	meta server.EntityMeta,
	cols []protoreflect.FieldDescriptor,
	ws string,
	where *syncplanev1.Condition,
	offset int,
) (rows []ui.Row, more bool, count int, countMore bool, err error) {
	limit := pageSize + 1
	if offset == 0 {
		limit = countCap + 1
	}
	var collected []rawRow
	err = srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
		prototype := dynamicpb.NewMessage(meta.Descriptor)
		spec := query.Spec{Where: where, Offset: offset, Limit: limit}
		if qerr := tx.Query(prototype, spec, func(msg proto.Message, version uint64) error {
			return collect(srv, &collected, msg, version)
		}); qerr != nil {
			return qerr
		}
		page := collected
		if len(page) > pageSize {
			page = page[:pageSize]
		}
		rows = h.buildRows(tx, srv, meta, cols, page)
		return nil
	})
	if err != nil {
		return nil, false, 0, false, err
	}
	count = len(collected)
	if count > countCap {
		count, countMore = countCap, true
	}
	return rows, len(collected) > pageSize, count, countMore, nil
}

// buildRows renders collected rows, sharing one reference-label cache across
// the page.
func (h *handler) buildRows(
	tx *server.ViewTx,
	srv *server.Server,
	meta server.EntityMeta,
	cols []protoreflect.FieldDescriptor,
	collected []rawRow,
) []ui.Row {
	cache := labelCache{}
	rows := make([]ui.Row, 0, len(collected))
	for _, c := range collected {
		rows = append(rows, h.buildRow(tx, srv, meta, cols, c.key, c.version, c.msg, cache))
	}
	return rows
}

// pageOffset reads the offset cursor of a filtered page; anything unreadable is
// the first page.
func pageOffset(q url.Values) int {
	n, err := strconv.Atoi(q.Get("offset"))
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// pageURL is the browse URL of another page: the request's own query (so the
// filter carries) with the paging cursor replaced.
func pageURL(base string, q url.Values, cursor, value string) string {
	out := url.Values{}
	for k, vs := range q {
		if k != "after" && k != "offset" {
			out[k] = vs
		}
	}
	out.Set(cursor, value)
	return base + "?" + out.Encode()
}

// newForm renders the create slide-over for an entity (empty message).
func (h *handler) newForm(w http.ResponseWriter, r *http.Request) {
	_, app, meta, ok := h.resolveEntity(w, r)
	if !ok {
		return
	}
	ws := r.PathValue("ws")
	empty := dynamicpb.NewMessage(meta.Descriptor)
	// Pre-fill a generated id so the user need not invent one; the field stays
	// editable, so a deliberate id is still honoured.
	if kf := autoIDField(meta); kf != nil {
		empty.Set(kf, protoreflect.ValueOfString(id.New()))
	}
	fb := formBuilder{appID: app, ws: ws}
	fields := fb.form(meta.Descriptor, empty, meta.KeyFields, false)
	render(w, r, ui.DetailPanel(ui.Detail{
		AppID: app, Workspace: ws, Entity: meta.Name,
		FullName: string(meta.Descriptor.FullName()),
		Fields:   fields, Edit: false,
	}))
}

// editForm renders the edit slide-over for one row, round-tripping its version.
func (h *handler) editForm(w http.ResponseWriter, r *http.Request) {
	srv, app, meta, ok := h.resolveEntity(w, r)
	if !ok {
		return
	}
	ws := r.PathValue("ws")
	key := r.PathValue("key")
	msg := dynamicpb.NewMessage(meta.Descriptor)
	var version uint64
	var found bool
	var fields []ui.FieldView
	err := srv.View(server.Trusted(r.Context()), ws, func(tx *server.ViewTx) error {
		f, v, err := tx.Get(msg, key)
		found, version = f, v
		if err != nil || !f {
			return err
		}
		// Resolve selected reference labels through this same snapshot, so chips
		// show target names rather than opaque ids.
		cache := labelCache{}
		fb := formBuilder{appID: app, ws: ws, label: func(target string, ref protoreflect.Message) string {
			return h.refLabelByTarget(tx, srv, target, ref, cache)
		}}
		fields = fb.form(meta.Descriptor, msg, meta.KeyFields, true)
		return nil
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !found {
		http.NotFound(w, r)
		return
	}
	render(w, r, ui.DetailPanel(ui.Detail{
		AppID: app, Workspace: ws, Entity: meta.Name,
		FullName: string(meta.Descriptor.FullName()),
		Title:    displayLabel(meta, msg, key),
		Key:      key, Version: version,
		Fields: fields, Edit: true,
	}))
}

// navEntity finds the sidebar entry for entity name, for copying its count.
func navEntity(nav ui.Nav, name string) *ui.NavEntity {
	for i := range nav.Entities {
		if nav.Entities[i].Name == name {
			return &nav.Entities[i]
		}
	}
	return nil
}

// pickerLimit bounds how many matching rows the reference picker dropdown
// returns; the user narrows the set by typing rather than scrolling.
const pickerLimit = 20

// picker lists the target entity's rows (display label + key fields) for the
// reference picker dropdown, filtered by the ?q substring across label and key.
func (h *handler) picker(w http.ResponseWriter, r *http.Request) {
	srv, _, meta, ok := h.resolveEntity(w, r)
	if !ok {
		return
	}
	ws := r.PathValue("ws")
	q := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("__q")))
	var rows []ui.PickerRow
	prototype := dynamicpb.NewMessage(meta.Descriptor)
	err := srv.View(server.Trusted(r.Context()), ws, func(tx *server.ViewTx) error {
		_, _, err := tx.ScanPage(prototype, "", pageSize, func(msg proto.Message, _ uint64) error {
			if len(rows) >= pickerLimit {
				return nil
			}
			_, key, kerr := srv.EntityKey(msg)
			if kerr != nil {
				return kerr
			}
			label := displayLabel(meta, msg, key)
			if q != "" && !strings.Contains(strings.ToLower(label), q) && !strings.Contains(strings.ToLower(key), q) {
				return nil
			}
			rows = append(rows, ui.PickerRow{
				Label: label, Sub: key, Color: colorSlot(label), Initials: initials(label),
				KeysJSON: keyValuesJSON(meta.KeyFields, msg.ProtoReflect()),
			})
			return nil
		})
		return err
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	render(w, r, ui.PickerResults(rows))
}

// keyValuesJSON encodes a row's key-field values as a {field:value} JSON object
// the picker client copies into the hidden reference inputs on selection.
func keyValuesJSON(keyFields []protoreflect.FieldDescriptor, m protoreflect.Message) string {
	obj := make(map[string]string, len(keyFields))
	for _, kf := range keyFields {
		obj[string(kf.Name())] = scalarString(kf, m.Get(kf))
	}
	raw, err := json.Marshal(obj)
	if err != nil {
		return "{}"
	}
	return string(raw)
}

// create binds the submitted form and PUTs a whole new row.
func (h *handler) create(w http.ResponseWriter, r *http.Request) {
	srv, app, meta, ok := h.resolveEntity(w, r)
	if !ok {
		return
	}
	ws := r.PathValue("ws")
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	msg, err := bind(meta.Descriptor, r.Form)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Mint an id when the key was left blank (e.g. the prefilled value cleared),
	// so the admin never writes an empty-keyed row.
	if kf := autoIDField(meta); kf != nil && !msg.ProtoReflect().Has(kf) {
		msg.ProtoReflect().Set(kf, protoreflect.ValueOfString(id.New()))
	}
	_, key, err := srv.EntityKey(msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	err = srv.Mutate(server.Trusted(r.Context()), ws, func(tx *server.EntityTx) error {
		// Collaborative text fields submit plain strings. A create can still
		// land on a live row (a resubmitted form, a key typed over an existing
		// one) and Put merges text fields rather than replacing them, so bind
		// against the stored document from inside the write transaction, the
		// same way update does. Binding a fresh document instead would re-spend
		// counters this handler's peer already holds in that row.
		loaded := dynamicpb.NewMessage(meta.Descriptor)
		found, gerr := tx.Get(loaded, key)
		if gerr != nil {
			return gerr
		}
		var prev protoreflect.Message
		if found {
			prev = loaded
		}
		if terr := bindTextFields(meta.Descriptor, prev, msg.ProtoReflect(), r.Form, h.peer); terr != nil {
			return server.Reject(server.CodeInvalidArgument, terr.Error())
		}
		return tx.Put(msg)
	})
	if err != nil {
		writeMutateError(w, err)
		return
	}
	hxRedirect(w, r, entityURL(app, ws, meta.Name))
}

// update binds the submitted form, diffs it against the stored row, and PATCHes
// only the changed fields with optimistic compare-and-swap on _version.
func (h *handler) update(w http.ResponseWriter, r *http.Request) {
	srv, app, meta, ok := h.resolveEntity(w, r)
	if !ok {
		return
	}
	ws := r.PathValue("ws")
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	msg, err := bind(meta.Descriptor, r.Form)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var expected *uint64
	if v := r.FormValue("_version"); v != "" {
		n, perr := strconv.ParseUint(v, 10, 64)
		if perr != nil {
			http.Error(w, "bad _version", http.StatusBadRequest)
			return
		}
		expected = &n
	}
	key := r.PathValue("key")
	err = srv.Mutate(server.Trusted(r.Context()), ws, func(tx *server.EntityTx) error {
		loaded := dynamicpb.NewMessage(meta.Descriptor)
		found, gerr := tx.Get(loaded, key)
		if gerr != nil {
			return gerr
		}
		if !found {
			return server.Reject(server.CodeConflict, "row was deleted")
		}
		// Collaborative text fields submit plain strings; re-apply each onto
		// the STORED document (SetString = minimal diff), so the admin edit
		// merges with concurrent client edits. An unchanged value re-encodes
		// byte-identically and drops out of the mask below.
		if terr := bindTextFields(meta.Descriptor, loaded, msg.ProtoReflect(), r.Form, h.peer); terr != nil {
			return server.Reject(server.CodeInvalidArgument, terr.Error())
		}
		mask := changedMask(loaded, msg, meta.KeyFields)
		if len(mask) == 0 {
			return nil // nothing changed
		}
		return tx.Patch(msg, mask, expected)
	})
	if err != nil {
		writeMutateError(w, err)
		return
	}
	hxRedirect(w, r, entityURL(app, ws, meta.Name))
}

// delete tombstones one row; the htmx swap removes its table row.
func (h *handler) delete(w http.ResponseWriter, r *http.Request) {
	srv, _, meta, ok := h.resolveEntity(w, r)
	if !ok {
		return
	}
	ws := r.PathValue("ws")
	key := r.PathValue("key")
	prototype := dynamicpb.NewMessage(meta.Descriptor)
	err := srv.Mutate(server.Trusted(r.Context()), ws, func(tx *server.EntityTx) error {
		return tx.Delete(prototype, key)
	})
	if err != nil {
		writeMutateError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK) // empty body: the row is swapped out
}

// resolveApp looks up the app server from the {app} path value, 404 if unknown.
func (h *handler) resolveApp(w http.ResponseWriter, r *http.Request) (*server.Server, string, bool) {
	app := r.PathValue("app")
	srv := h.mgr.App(app)
	if srv == nil {
		http.NotFound(w, r)
		return nil, "", false
	}
	return srv, app, true
}

// resolveEntity additionally resolves the {entity} path value to its metadata.
// A managed entity resolves to 404 like an unregistered one: every route this
// serves leads into the generic editor, and appEntities keeps those entities
// out of the UI in the first place.
func (h *handler) resolveEntity(w http.ResponseWriter, r *http.Request) (*server.Server, string, server.EntityMeta, bool) {
	srv, app, ok := h.resolveApp(w, r)
	if !ok {
		return nil, "", server.EntityMeta{}, false
	}
	meta, ok := srv.Entity(r.PathValue("entity"))
	if !ok || meta.Managed {
		http.NotFound(w, r)
		return nil, "", server.EntityMeta{}, false
	}
	return srv, app, meta, true
}

// appEntities lists an app's entities as the UI offers them: everything
// registered except what the engine manages for itself. The admin browser and
// the editor are one surface — every row it lists is a row it will let someone
// edit or delete — and its writes go through Server.Mutate, which trusts the
// caller, so hiding those entities is what keeps a hand-written field-history
// record out of the log (see server.EntityMeta.Managed).
func appEntities(srv *server.Server) []server.EntityMeta {
	all := srv.Entities()
	out := make([]server.EntityMeta, 0, len(all))
	for _, e := range all {
		if e.Managed {
			continue
		}
		out = append(out, e)
	}
	return out
}

// autoIDField returns the entity's key field when it is keyed by a single
// string field — the only shape the admin can mint an id for. Otherwise nil
// (composite or non-string keys must be supplied by the user).
func autoIDField(meta server.EntityMeta) protoreflect.FieldDescriptor {
	if len(meta.KeyFields) != 1 {
		return nil
	}
	if kf := meta.KeyFields[0]; kf.Kind() == protoreflect.StringKind {
		return kf
	}
	return nil
}

// render writes a templ component as an HTML response.
func render(w http.ResponseWriter, r *http.Request, c templ.Component) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := c.Render(r.Context(), w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// hxRedirect redirects after a mutation: an HX-Redirect header for htmx
// requests, a plain 303 otherwise.
func hxRedirect(w http.ResponseWriter, r *http.Request, to string) {
	if r.Header.Get("HX-Request") != "" {
		w.Header().Set("HX-Redirect", to)
		w.WriteHeader(http.StatusOK)
		return
	}
	http.Redirect(w, r, to, http.StatusSeeOther)
}

// writeMutateError maps a trusted-write failure to a status: a Rejection
// (conflict / invalid) to 409/400, anything else to 500.
func writeMutateError(w http.ResponseWriter, err error) {
	if rej, ok := server.AsRejection(err); ok {
		status := http.StatusBadRequest
		if rej.Code == server.CodeConflict {
			status = http.StatusConflict
		}
		http.Error(w, rej.Message, status)
		return
	}
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func entityURL(app, ws, entity string) string {
	return "/apps/" + url.PathEscape(app) + "/ws/" + url.PathEscape(ws) + "/e/" + url.PathEscape(entity)
}
