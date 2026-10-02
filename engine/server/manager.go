package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/levelfourab/syncplane/engine/internal/naming"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/prefixstore"
	"github.com/levelfourab/syncplane/internal/protoopts"
)

// Manager hosts multiple apps on one engine, mapping each app to its own
// *Server over its own partition of the root adapter's workspaces. App
// identity is purely a routing + storage-partition concern (it is NOT encoded
// in the proto package), so two apps may both define sync.dynamic.Todo without
// colliding — they live in distinct *Server instances over distinct data
// partitions.
//
// The partition is a workspace-id prefix, "<appID>/", and it reaches every
// plane an app's server sits on: storage through prefixstore, the change
// notifier through the server itself, and the presence bus through its room
// registry. None of those knows what an app is, which is why one storage
// adapter, one notifier and one bus can be shared by every hosted app and
// still keep them apart. App ids are restricted (see naming.ValidateAppID) so
// that no two of them can produce the same prefixed id.
//
// An app's own code — its permission hooks, its effects, its clients — deals
// in bare workspace ids throughout.
//
// The public sync surface is routed by a leading "/<appID>" path segment (see
// SyncHandler); the trusted admin surface (AdminHandler) is mounted separately
// so it can bind to a private interface.
//
// # Authenticating a collection of apps
//
// Identity is normally shared across a collection and authorization is not.
// The two option seams follow that split:
//
//   - WithServerOptions carries what every app has in common — one
//     Authenticator, limits, the notifier, the presence bus, the logger. The authenticator runs
//     on the app's own handler, so a request is authenticated exactly once,
//     after routing, and its Principal reaches that app's permission hooks.
//   - WithAppOptions carries what differs per app, which in practice means
//     Permissions. A policy needs to know WHICH app it is answering for —
//     each app has its own workspace space, so "workspace team" in one app is
//     unrelated data from "workspace team" in another — and hooks are not
//     handed an app id. Closing over the app id at construction is how a
//     policy gets it.
//
// A single policy shared across every app via WithServerOptions is therefore
// only correct when the apps genuinely share a membership model. Otherwise
// build one per app:
//
//	mgr, err := server.NewManager(ctx, root,
//		server.WithServerOptions(server.WithAuthenticator(auth)),
//		server.WithAppOptions(func(appID string) []server.Option {
//			return []server.Option{server.WithPermissions(policyFor(appID))}
//		}))
//
// Nothing forces this: an app with no policy inherits AllowAll, which admits
// every caller into every workspace of that app (see AllowAll).
type Manager struct {
	root storage.Adapter
	// catalog is where the registered schemas persist, resolved from root at
	// construction; NewManager fails without one.
	catalog    storage.Catalog
	authorizer RegisterAuthorizer
	opts       []Option
	appOpts    func(appID string) []Option

	mu   sync.RWMutex
	apps map[string]*hostedApp
	// closed latches in Close. It exists so a registration racing shutdown
	// cannot start a fresh *Server — and with it a GC goroutine — that nothing
	// will ever close.
	closed bool
}

// hostedApp is one app: its *Server plus the mux routed traffic is served by.
// The mux carries every Connect service the server mounts — the sync plane and
// the ephemeral one — each at its own full method path, which is why routing
// can strip the "/<appID>" prefix and hand the rest over unexamined.
//
// It is built once, when the app is created, rather than per request: it owns
// Connect's route table and the app's interceptor chain (logging, then
// authentication), none of which change after New. Runtime schema registration
// swaps the server's catalog, not its handler.
type hostedApp struct {
	srv     *Server
	handler http.Handler

	// regMu serializes the registrations of the app, so that each one
	// merges over the schema the one before it stored.
	regMu sync.Mutex
	// schema is the descriptor set of the schema srv serves, as the catalog
	// stores it; nil before the app's first registration. regMu guards it.
	schema []byte
}

// ManagerOption configures a Manager.
type ManagerOption func(*Manager)

// WithRegisterAuthorizer installs the trust gate for the admin Register RPC.
// The default is DenyAll (fail closed).
func WithRegisterAuthorizer(a RegisterAuthorizer) ManagerOption {
	return func(m *Manager) { m.authorizer = a }
}

// WithServerOptions sets the server Options applied to every app's *Server
// (authenticator, limits, notifier, heartbeat, ...). Per-app schema and storage
// partition are managed by the Manager and must not be set here.
//
// Use it for what the collection shares. Anything that must differ per app —
// Permissions above all — belongs in WithAppOptions, which is applied after
// these and so wins on conflict.
func WithServerOptions(opts ...Option) ManagerOption {
	return func(m *Manager) { m.opts = append(m.opts, opts...) }
}

// WithAppOptions installs a per-app Option source: fn is called once per app,
// when that app's *Server is created (its first registration, or the boot
// reload of a persisted schema), and its Options are applied after the shared
// WithServerOptions ones — so a per-app Permissions overrides a shared one.
//
// This is the seam for policies that must be bound to an app: hooks receive a
// principal and a workspace id but no app id, and each app has its own
// workspace space, so a policy that cannot tell the apps apart cannot decide
// membership for them. Closing over appID here gives it that context once, rather than per
// request.
//
// fn must be safe for concurrent use — apps are created lazily, on whichever
// goroutine first routes to them — and should be cheap: it runs while the
// Manager's app map is locked, so a slow fn stalls other apps' first requests.
// Build the expensive parts (a membership client, a token cache) once outside
// and let fn hand out per-app views of them.
func WithAppOptions(fn func(appID string) []Option) ManagerOption {
	return func(m *Manager) { m.appOpts = fn }
}

// NewManager builds a Manager over a root adapter and reloads every persisted
// app schema, re-registering its entities BEFORE returning, so a restart
// leaves all previously-registered apps immediately syncable. A
// persisted schema that fails to re-register aborts startup (the stored bytes
// were valid when first accepted, so a failure signals real corruption or a
// binary downgrade) — and takes the apps reloaded before it down with it,
// because a caller that got an error has no Manager to Close.
//
// The Manager owns the app servers it creates; close it (and then the root
// adapter) at shutdown. See Close.
//
// root must implement storage.Catalog, which is where the registered schemas
// persist; an adapter without one can back a Server but not a Manager.
func NewManager(ctx context.Context, root storage.Adapter, opts ...ManagerOption) (*Manager, error) {
	catalog, ok := storage.As[storage.Catalog](root)
	if !ok {
		return nil, fmt.Errorf("server: manager: adapter cannot persist app schemas (no storage.Catalog)")
	}
	m := &Manager{
		root:       root,
		catalog:    catalog,
		authorizer: DenyAll{},
		apps:       map[string]*hostedApp{},
	}
	for _, opt := range opts {
		opt(m)
	}
	schemas, err := m.ListAppSchemas(ctx)
	if err != nil {
		return nil, fmt.Errorf("server: manager boot: list app schemas: %w", err)
	}
	for _, sc := range schemas {
		app, err := m.host(sc.AppID)
		if err != nil {
			_ = m.Close()
			return nil, fmt.Errorf("server: manager boot: create app %q: %w", sc.AppID, err)
		}
		srv := app.srv
		r, err := srv.registerDescriptorSet(sc.DescriptorSet)
		if err != nil {
			_ = m.Close()
			return nil, fmt.Errorf("server: manager boot: re-register app %q: %w", sc.AppID, err)
		}
		app.schema = sc.DescriptorSet
		srv.log().LogAttrs(ctx, slog.LevelInfo, "app schema reloaded on boot",
			slog.String("component", "schema"),
			slog.String("app", sc.AppID),
			slog.Int("entities", len(r.Entities)),
			slog.String("revision", r.Revision))
	}
	return m, nil
}

// appServer returns the app's *Server, creating it (over the app's storage
// partition, with the shared options plus this app's own) on first use. It
// fails when those options do not build a server — a bad per-app declaration
// from WithAppOptions is only discovered here, when that app is first hosted.
func (m *Manager) appServer(appID string) (*Server, error) {
	app, err := m.host(appID)
	if err != nil {
		return nil, err
	}
	return app.srv, nil
}

// host returns the app's hostedApp, creating it on first use.
func (m *Manager) host(appID string) (*hostedApp, error) {
	m.mu.RLock()
	app := m.apps[appID]
	m.mu.RUnlock()
	if app != nil {
		return app, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if app = m.apps[appID]; app != nil {
		return app, nil
	}
	if m.closed {
		return nil, fmt.Errorf("server: app %q: manager is closed", appID)
	}
	if err := naming.ValidateAppID(appID); err != nil {
		return nil, fmt.Errorf("server: %w", err)
	}
	prefix := appPrefix(appID)
	opts := append([]Option{withWorkspacePrefix(prefix)}, m.optionsFor(appID)...)
	srv, err := New(prefixstore.Wrap(m.root, prefix), opts...)
	if err != nil {
		return nil, fmt.Errorf("server: app %q: %w", appID, err)
	}
	mux := http.NewServeMux()
	for path, h := range srv.Handlers() {
		mux.Handle(path, h)
	}
	app = &hostedApp{srv: srv, handler: mux}
	m.apps[appID] = app
	return app, nil
}

// optionsFor returns the Options for one app: the shared set, then the per-app
// set, so the latter overrides on conflict. The shared slice is never appended
// to in place — that would leak one app's options into the next app's.
func (m *Manager) optionsFor(appID string) []Option {
	if m.appOpts == nil {
		return m.opts
	}
	own := m.appOpts(appID)
	opts := make([]Option, 0, len(m.opts)+len(own))
	opts = append(opts, m.opts...)
	return append(opts, own...)
}

// Close stops the background work of every hosted app — each *Server's GC
// pass, index backfill and notify-coalescing timers — and refuses to host new
// apps afterwards, so a registration racing shutdown cannot start a goroutine
// that outlives it. Every app is closed even if one fails; their errors come
// back joined.
//
// It does NOT close the root adapter, matching Server.Close: the caller owns
// the handle, and one root backs every app here. Nor does it stop serving —
// handlers keep answering from the still-open store, just without the
// background passes — so Close belongs at process shutdown, after the
// listeners are down:
//
//	root, err := sqlitestore.Open(path)
//	…
//	defer root.Close()
//	mgr, err := server.NewManager(ctx, root, …)
//	…
//	defer mgr.Close()
//
// Close is idempotent.
func (m *Manager) Close() error {
	m.mu.Lock()
	m.closed = true
	apps := make([]*hostedApp, 0, len(m.apps))
	for _, app := range m.apps {
		apps = append(apps, app)
	}
	m.mu.Unlock()

	// Outside the lock: Server.Close waits for the background loops to return,
	// and a pass in flight may still be routing requests through lookup.
	var errs []error
	for _, app := range apps {
		if err := app.srv.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// lookup returns an already-created app, or nil.
func (m *Manager) lookup(appID string) *hostedApp {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.apps[appID]
}

// AppIDs returns the ids of every live app server, sorted. An app becomes live
// once it is registered (or reloaded on boot); a never-registered app is not
// listed even if its schema is requested. Used by the admin UI to enumerate
// hosted apps.
func (m *Manager) AppIDs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ids := make([]string, 0, len(m.apps))
	for id := range m.apps {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// App returns the app's live *Server, or nil if the app is unknown (never
// registered or reloaded). It is the exported counterpart of lookup, for
// trusted introspection (the admin UI) that addresses an app by id.
func (m *Manager) App(appID string) *Server {
	app := m.lookup(appID)
	if app == nil {
		return nil
	}
	return app.srv
}

// AppSchema is one app's persisted registration: the serialized
// google.protobuf.FileDescriptorSet it registered, the revision the engine
// derived from it, and when it was last written.
type AppSchema struct {
	AppID         string
	DescriptorSet []byte
	Revision      string
	UpdatedAt     time.Time
}

// ListAppSchemas returns every persisted app schema (app id + revision +
// descriptor set), ordered by app id. It reads the adapter's catalog — the
// same call NewManager makes at boot — so the admin UI can list apps and their
// schema revision without re-deriving anything.
func (m *Manager) ListAppSchemas(ctx context.Context) ([]AppSchema, error) {
	entries, err := m.catalog.ListCatalogEntries(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]AppSchema, 0, len(entries))
	for _, e := range entries {
		out = append(out, AppSchema{
			AppID:         e.ID,
			DescriptorSet: e.Payload,
			Revision:      e.Revision,
			UpdatedAt:     e.UpdatedAt,
		})
	}
	return out, nil
}

// appPrefix is the workspace-id prefix an app's data lives under. The
// separator is outside the workspace-id and app-id alphabets, so no pair of
// ids can spell another pair's prefixed id.
func appPrefix(appID string) string { return appID + "/" }

// withWorkspacePrefix scopes a server's notifier and room registry to one
// prefix of the workspace-id space. Only a Manager sets it, together with the
// matching prefixstore view: a server built directly owns the whole space.
func withWorkspacePrefix(prefix string) Option {
	return func(s *Server) error { s.wsPrefix = prefix; return nil }
}

// RegisterStatus reports how a registration changed the app's schema.
type RegisterStatus int

const (
	// RegisterUnchanged: the submitted schema was identical; a no-op.
	RegisterUnchanged RegisterStatus = iota
	// RegisterCreated: a new app was registered.
	RegisterCreated
	// RegisterUpdated: an existing app's schema was additively evolved.
	RegisterUpdated
)

// String renders the status for logs and diagnostics.
func (s RegisterStatus) String() string {
	switch s {
	case RegisterCreated:
		return "created"
	case RegisterUpdated:
		return "updated"
	default:
		return "unchanged"
	}
}

// ManagerRegisterResult is the outcome of Manager.Register.
type ManagerRegisterResult struct {
	Revision string
	Status   RegisterStatus
	Entities []string
}

// Register validates and registers an app's descriptor set, then persists it
// for boot reload. Registration (which validates additive evolution and
// rejects conflicts) runs first, so a rejected set never reaches storage; an
// identical set is a no-op and is not re-persisted. It does NOT check the
// authorizer — that gate lives on the admin RPC, which calls this after
// admitting the caller. opts apply as they do to Server.RegisterDescriptorSet.
//
// A set adds and replaces entities and keeps the others the app registered
// before. The app's schema, which the catalog stores, is the merge of every
// set it registered: the definitions of set, and every definition of the
// schema before it that set does not replace. A restart serves that schema,
// and the revision is the revision of that schema. Entities in the result are
// the entities set declares. Registrations of one app run one at a time.
func (m *Manager) Register(ctx context.Context, appID string, set []byte, opts ...RegisterOption) (ManagerRegisterResult, error) {
	if appID == "" {
		return ManagerRegisterResult{}, fmt.Errorf("server: register needs an app_id")
	}
	app, err := m.host(appID)
	if err != nil {
		return ManagerRegisterResult{}, err
	}
	app.regMu.Lock()
	defer app.regMu.Unlock()
	srv := app.srv
	merged, entities, err := mergeDescriptorSets(app.schema, set)
	if err != nil {
		return ManagerRegisterResult{}, err
	}
	prevRev := srv.SchemaRevision()
	r, err := srv.registerDescriptorSet(merged, opts...)
	if err != nil {
		return ManagerRegisterResult{}, err
	}
	r.Entities = entities
	status := RegisterUnchanged
	if r.Changed {
		if prevRev == "" {
			status = RegisterCreated
		} else {
			status = RegisterUpdated
		}
		if err := m.catalog.PutCatalogEntry(ctx, appID, merged, r.Revision); err != nil {
			return ManagerRegisterResult{}, fmt.Errorf("server: persist app schema: %w", err)
		}
		app.schema = merged
		srv.log().LogAttrs(ctx, slog.LevelInfo, "app schema registered",
			slog.String("component", "schema"),
			slog.String("app", appID),
			slog.String("status", status.String()),
			slog.Int("entities", len(r.Entities)),
			slog.String("revision", r.Revision))
	}
	return ManagerRegisterResult{Revision: r.Revision, Status: status, Entities: r.Entities}, nil
}

// SyncHandler returns the routed public handler. It strips the leading
// "/<appID>" path segment, looks up that app's *Server, and delegates to that
// server's own mux, which carries every Connect service it mounts at the full
// method path each expects ("/syncplane.v1.SyncService/..." and
// "/syncplane.v1.PresenceService/..."). An unknown app is 404. App servers are
// created on demand by Register / boot reload, so a request for an unregistered
// app never auto-creates one.
func (m *Manager) SyncHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		appID, rest := splitAppPrefix(r.URL.Path)
		if appID == "" {
			http.NotFound(w, r)
			return
		}
		app := m.lookup(appID)
		if app == nil {
			http.NotFound(w, r)
			return
		}
		r2 := r.Clone(r.Context())
		r2.URL.Path = rest
		r2.URL.RawPath = "" // recomputed from Path; the appID prefix carried no escapes
		app.handler.ServeHTTP(w, r2)
	})
}

// AdminHandler returns the mount path and handler for the trusted AdminService
// (schema registration). Mount it on a separate, private listener — never on
// the public sync surface.
func (m *Manager) AdminHandler() (string, http.Handler) {
	return newAdminHandler(m)
}

// splitAppPrefix splits "/<appID>/rest..." into ("<appID>", "/rest..."). It
// returns ("", path) when there is no app segment.
func splitAppPrefix(path string) (appID, rest string) {
	trimmed := strings.TrimPrefix(path, "/")
	if trimmed == "" {
		return "", path
	}
	if i := strings.IndexByte(trimmed, '/'); i >= 0 {
		return trimmed[:i], trimmed[i:]
	}
	// "/app" with no trailing service path: nothing to route to.
	return trimmed, "/"
}

// mergedFilePrefix starts the path of a file that mergeDescriptorSets keeps
// from the registered schema under a new path, because the incoming set holds
// another file at its own path.
const mergedFilePrefix = "syncplane/registered/"

// mergeDescriptorSets returns the descriptor set of the schema an app serves
// once it registers next over prev, the descriptor set of the schema it
// serves now: every definition of next, and every top-level definition of
// prev that next does not replace. A definition of prev is replaced when next
// declares the same full name, or an entity of the same name. entities are the
// names of the entities next declares, sorted.
//
// A file of prev that next holds unchanged is dropped, and a file whose path
// next holds with other content keeps what next does not replace under a path
// below mergedFilePrefix. Each file kept from prev imports every file of
// next, so that a reference to a definition that next replaced resolves to
// the replacement. Merging the result with next again gives the result.
//
// prev nil returns next unchanged.
func mergeDescriptorSets(prev, next []byte) (merged []byte, entities []string, err error) {
	var nextSet descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(next, &nextSet); err != nil {
		return nil, nil, fmt.Errorf("server: decode FileDescriptorSet: %w", err)
	}
	nextFiles, err := protodesc.NewFiles(&nextSet)
	if err != nil {
		return nil, nil, fmt.Errorf("server: build descriptor files: %w", err)
	}
	entityNames := map[string]bool{}
	nextFiles.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		msgs := fd.Messages()
		for i := 0; i < msgs.Len(); i++ {
			if name := protoopts.Entity(msgs.Get(i)).GetName(); name != "" {
				entityNames[name] = true
			}
		}
		return true
	})
	for name := range entityNames {
		entities = append(entities, name)
	}
	sort.Strings(entities)
	if len(prev) == 0 {
		return next, entities, nil
	}

	var prevSet descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(prev, &prevSet); err != nil {
		return nil, nil, fmt.Errorf("server: decode registered FileDescriptorSet: %w", err)
	}
	prevFiles, err := protodesc.NewFiles(&prevSet)
	if err != nil {
		return nil, nil, fmt.Errorf("server: build registered descriptor files: %w", err)
	}

	nextByPath := map[string]*descriptorpb.FileDescriptorProto{}
	defined := map[string]bool{}
	for _, f := range nextSet.GetFile() {
		nextByPath[f.GetName()] = f
		for _, name := range topLevelNames(f) {
			defined[name] = true
		}
	}
	// replaced reports whether next replaces the top-level definition name of
	// a file of prev.
	replaced := func(name string) bool {
		if defined[name] {
			return true
		}
		md, err := prevFiles.FindDescriptorByName(protoreflect.FullName(name))
		if err != nil {
			return false
		}
		msg, ok := md.(protoreflect.MessageDescriptor)
		return ok && entityNames[protoopts.Entity(msg).GetName()]
	}

	renamed := map[string]string{}
	dropped := map[string]bool{}
	var kept []*descriptorpb.FileDescriptorProto
	for _, f := range prevSet.GetFile() {
		path := f.GetName()
		other, clash := nextByPath[path]
		if clash && proto.Equal(f, other) {
			continue
		}
		c := proto.Clone(f).(*descriptorpb.FileDescriptorProto)
		prefix := ""
		if pkg := c.GetPackage(); pkg != "" {
			prefix = pkg + "."
		}
		c.MessageType = slices.DeleteFunc(c.MessageType, func(m *descriptorpb.DescriptorProto) bool { return replaced(prefix + m.GetName()) })
		c.EnumType = slices.DeleteFunc(c.EnumType, func(e *descriptorpb.EnumDescriptorProto) bool { return replaced(prefix + e.GetName()) })
		c.Service = slices.DeleteFunc(c.Service, func(s *descriptorpb.ServiceDescriptorProto) bool { return replaced(prefix + s.GetName()) })
		c.Extension = slices.DeleteFunc(c.Extension, func(x *descriptorpb.FieldDescriptorProto) bool { return replaced(prefix + x.GetName()) })
		if len(c.MessageType)+len(c.EnumType)+len(c.Service)+len(c.Extension) == 0 {
			dropped[path] = true
			continue
		}
		if clash {
			raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(f)
			if err != nil {
				return nil, nil, err
			}
			sum := sha256.Sum256(raw)
			renamed[path] = mergedFilePrefix + hex.EncodeToString(sum[:8]) + "/" + path
			c.Name = proto.String(renamed[path])
		}
		kept = append(kept, c)
	}

	out := &descriptorpb.FileDescriptorSet{File: slices.Clone(nextSet.GetFile())}
	for _, c := range kept {
		rewireDependencies(c, renamed, dropped, nextSet.GetFile())
		out.File = append(out.File, c)
	}
	merged, err = proto.MarshalOptions{Deterministic: true}.Marshal(out)
	if err != nil {
		return nil, nil, err
	}
	return merged, entities, nil
}

// topLevelNames returns the full names of the top-level definitions of f.
func topLevelNames(f *descriptorpb.FileDescriptorProto) []string {
	prefix := ""
	if pkg := f.GetPackage(); pkg != "" {
		prefix = pkg + "."
	}
	var out []string
	for _, m := range f.GetMessageType() {
		out = append(out, prefix+m.GetName())
	}
	for _, e := range f.GetEnumType() {
		out = append(out, prefix+e.GetName())
	}
	for _, s := range f.GetService() {
		out = append(out, prefix+s.GetName())
	}
	for _, x := range f.GetExtension() {
		out = append(out, prefix+x.GetName())
	}
	return out
}

// rewireDependencies points the imports of c, a file kept from a registered
// schema, at the files of the merged schema: an import of a renamed file
// follows the rename, an import of a dropped file goes, and every file of
// next is imported. Public and weak imports keep their kind.
func rewireDependencies(c *descriptorpb.FileDescriptorProto, renamed map[string]string, dropped map[string]bool, next []*descriptorpb.FileDescriptorProto) {
	public := map[int32]bool{}
	for _, i := range c.GetPublicDependency() {
		public[i] = true
	}
	weak := map[int32]bool{}
	for _, i := range c.GetWeakDependency() {
		weak[i] = true
	}
	var deps []string
	var pub, wk []int32
	seen := map[string]bool{}
	add := func(dep string, isPublic, isWeak bool) {
		if seen[dep] || dep == c.GetName() {
			return
		}
		seen[dep] = true
		if isPublic {
			pub = append(pub, int32(len(deps)))
		}
		if isWeak {
			wk = append(wk, int32(len(deps)))
		}
		deps = append(deps, dep)
	}
	for i, dep := range c.GetDependency() {
		if dropped[dep] {
			continue
		}
		if to, ok := renamed[dep]; ok {
			dep = to
		}
		add(dep, public[int32(i)], weak[int32(i)])
	}
	for _, f := range next {
		add(f.GetName(), false, false)
	}
	c.Dependency, c.PublicDependency, c.WeakDependency = deps, pub, wk
}
