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
	"google.golang.org/protobuf/reflect/protoregistry"
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
//
// # Several nodes
//
// Several Managers may share one root adapter. A registration on one of them
// stores the schema only when the stored schema is still the one it merged
// over, so a node that had not seen a newer schema merges over that one
// instead of writing its own over it. The other nodes serve the stored schema
// once they learn of it: at once when their apps share a notifier with the
// node that registered (see WithNotifier), and otherwise on the next catalog
// poll (see WithCatalogPollInterval). A node also serves apps that another
// node registered from the poll after their first registration.
type Manager struct {
	root storage.Adapter
	// catalog is where the registered schemas persist, resolved from root at
	// construction; NewManager fails without one.
	catalog    storage.Catalog
	authorizer RegisterAuthorizer
	opts       []Option
	appOpts    func(appID string) []Option

	// pollInterval is how often the catalog is read for schemas other nodes
	// stored; 0 turns the poll off.
	pollInterval time.Duration

	mu   sync.RWMutex
	apps map[string]*hostedApp
	// closed latches in Close. It exists so a registration racing shutdown
	// cannot start a fresh *Server — and with it a GC goroutine — that nothing
	// will ever close.
	closed bool

	// ctx ends in Close, and stops the catalog poll and the poke watchers;
	// wg counts them, so Close can wait for a reload in flight before it
	// closes the servers.
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	stopOnce sync.Once
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

	// regMu serializes the registrations of the app and the reloads of its
	// stored schema, so that each one starts from the schema the one before
	// it left.
	regMu sync.Mutex
	// schema is the descriptor set of the schema srv serves, as the catalog
	// stores it, and revision is its catalog revision; both are empty before
	// the app's first registration. regMu guards them.
	schema   []byte
	revision string
	// stopPokes ends the subscription to the app's catalog pokes.
	stopPokes func()
}

// defaultCatalogPollInterval is the catalog poll interval of a Manager built
// without WithCatalogPollInterval.
const defaultCatalogPollInterval = 30 * time.Second

// maxRegisterAttempts bounds how often Register merges again after another
// node stored a schema between its read of the catalog and its write.
const maxRegisterAttempts = 5

// catalogPokeID is the id a Manager notifies when it stores a schema of
// appID. It has no "/", so it is outside the share of every app (see
// appPrefix) and reaches no app's subscribers or activity stream.
func catalogPokeID(appID string) string { return "syncplane:catalog:" + appID }

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

// WithCatalogPollInterval sets how often the Manager reads the catalog for
// schemas that other nodes over the same root adapter stored. The default is
// 30 seconds. A value of 0 or less turns the poll off; the Manager then learns
// of another node's registration only through a shared notifier, and serves
// an app that another node registered first only after a restart.
//
// A node that registers does not wait for the poll: it merges over the stored
// schema in either case.
func WithCatalogPollInterval(d time.Duration) ManagerOption {
	return func(m *Manager) { m.pollInterval = d }
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
		root:         root,
		catalog:      catalog,
		authorizer:   DenyAll{},
		apps:         map[string]*hostedApp{},
		pollInterval: defaultCatalogPollInterval,
	}
	for _, opt := range opts {
		opt(m)
	}
	m.ctx, m.cancel = context.WithCancel(context.Background())
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
		app.schema, app.revision = sc.DescriptorSet, sc.Revision
		srv.log().LogAttrs(ctx, slog.LevelInfo, "app schema reloaded on boot",
			slog.String("component", "schema"),
			slog.String("app", sc.AppID),
			slog.Int("entities", len(r.Entities)),
			slog.String("revision", r.Revision))
	}
	if m.pollInterval > 0 {
		m.wg.Add(1)
		go m.pollCatalog()
	}
	return m, nil
}

// pollCatalog reloads, every pollInterval until Close, each schema whose
// stored revision is not the one its app serves, and hosts each app it has
// not hosted yet.
func (m *Manager) pollCatalog() {
	defer m.wg.Done()
	t := time.NewTicker(m.pollInterval)
	defer t.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-t.C:
		}
		entries, err := m.catalog.ListCatalogEntries(m.ctx)
		if err != nil {
			if m.ctx.Err() == nil {
				slog.Default().LogAttrs(m.ctx, slog.LevelWarn, "catalog poll failed",
					slog.String("component", "schema"), slog.Any("error", err))
			}
			continue
		}
		for _, e := range entries {
			app, err := m.host(e.ID)
			if err != nil {
				if m.ctx.Err() == nil {
					slog.Default().LogAttrs(m.ctx, slog.LevelWarn, "catalog poll cannot host app",
						slog.String("component", "schema"), slog.String("app", e.ID), slog.Any("error", err))
				}
				continue
			}
			m.reload(app, e)
		}
	}
}

// watchPokes reloads the stored schema of app on each catalog poke, until
// pokes closes.
func (m *Manager) watchPokes(appID string, app *hostedApp, pokes <-chan struct{}) {
	defer m.wg.Done()
	for range pokes {
		e, ok, err := m.storedSchema(m.ctx, appID)
		if err != nil {
			if m.ctx.Err() == nil {
				app.srv.log().LogAttrs(m.ctx, slog.LevelWarn, "catalog read after a poke failed",
					slog.String("component", "schema"), slog.String("app", appID), slog.Any("error", err))
			}
			continue
		}
		if ok {
			m.reload(app, e)
		}
	}
}

// reload makes app serve the stored schema e, and logs when that fails.
func (m *Manager) reload(app *hostedApp, e storage.CatalogEntry) {
	app.regMu.Lock()
	defer app.regMu.Unlock()
	if err := m.adopt(m.ctx, app, e); err != nil {
		app.srv.log().LogAttrs(m.ctx, slog.LevelError, "stored app schema cannot be served",
			slog.String("component", "schema"),
			slog.String("app", e.ID),
			slog.String("revision", e.Revision),
			slog.Any("error", err))
	}
}

// adopt makes app serve e, the stored schema of the app, when it serves
// another revision. The caller holds app.regMu.
//
// The node that stored e checked its change of the access model against the
// schema it merged over, which is the stored schema before e or a later one.
// So e is served with AllowAccessChange: a node that served an older schema
// checks the same change again otherwise, and refuses a change the operator
// allowed.
func (m *Manager) adopt(ctx context.Context, app *hostedApp, e storage.CatalogEntry) error {
	if e.Revision == app.revision {
		return nil
	}
	r, err := app.srv.registerDescriptorSet(e.Payload, AllowAccessChange())
	if err != nil {
		return fmt.Errorf("server: app %q: serve stored schema: %w", e.ID, err)
	}
	app.schema, app.revision = e.Payload, e.Revision
	app.srv.log().LogAttrs(ctx, slog.LevelInfo, "app schema reloaded",
		slog.String("component", "schema"),
		slog.String("app", e.ID),
		slog.Int("entities", len(r.Entities)),
		slog.String("revision", r.Revision))
	return nil
}

// storedSchema returns the catalog entry of appID; ok is false when there is
// none.
func (m *Manager) storedSchema(ctx context.Context, appID string) (e storage.CatalogEntry, ok bool, err error) {
	entries, err := m.catalog.ListCatalogEntries(ctx)
	if err != nil {
		return storage.CatalogEntry{}, false, err
	}
	for _, e := range entries {
		if e.ID == appID {
			return e, true, nil
		}
	}
	return storage.CatalogEntry{}, false, nil
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
	pokes, stop := srv.sharedNotifier.Subscribe(catalogPokeID(appID))
	app.stopPokes = stop
	m.wg.Add(1)
	go m.watchPokes(appID, app, pokes)
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

	// The poll and the poke watchers go first, and Close waits for them: a
	// reload in flight registers on a server that is about to close.
	m.stopOnce.Do(func() {
		m.cancel()
		for _, app := range apps {
			app.stopPokes()
		}
	})
	m.wg.Wait()

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
// stored schema that set does not replace. A restart serves that schema, and
// the revision is the revision of that schema. Entities in the result are the
// entities set declares. Registrations of one app run one at a time.
//
// An entity cannot be renamed or removed: a set that declares the message of
// a registered entity as another entity, or with no entity option, is
// refused.
//
// The merge is over the schema the catalog stores, also when another node
// stored it, and set is checked against that schema. The app serves the
// merged schema only once the catalog stores it, so an error leaves the app
// on the stored schema, and a retry of the same set stores it. When other
// nodes store schemas of the app faster than Register can merge, it fails
// with an error that wraps storage.ErrCatalogConflict.
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
	for attempt := 1; ; attempt++ {
		stored, ok, err := m.storedSchema(ctx, appID)
		if err != nil {
			return ManagerRegisterResult{}, fmt.Errorf("server: read app schema: %w", err)
		}
		if ok {
			if err := m.adopt(ctx, app, stored); err != nil {
				return ManagerRegisterResult{}, err
			}
		}
		res, err := m.registerOver(ctx, appID, app, stored.Revision, set, opts)
		if errors.Is(err, storage.ErrCatalogConflict) && attempt < maxRegisterAttempts {
			continue
		}
		return res, err
	}
}

// registerOver merges set over the schema app serves, which the catalog
// stores at revision prev, and serves the result once the catalog stores it.
// The caller holds app.regMu.
func (m *Manager) registerOver(ctx context.Context, appID string, app *hostedApp, prev string, set []byte, opts []RegisterOption) (ManagerRegisterResult, error) {
	srv := app.srv
	merged, entities, err := mergeDescriptorSets(app.schema, set)
	if err != nil {
		return ManagerRegisterResult{}, err
	}
	prevRev := srv.SchemaRevision()
	persist := withPersist(func(revision string) error {
		if err := m.catalog.SwapCatalogEntry(ctx, appID, merged, revision, prev); err != nil {
			return fmt.Errorf("server: persist app schema: %w", err)
		}
		return nil
	})
	r, err := srv.registerDescriptorSet(merged, append(slices.Clone(opts), persist)...)
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
		app.schema, app.revision = merged, r.Revision
		srv.sharedNotifier.Notify(catalogPokeID(appID))
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
// method path each expects ("/syncplane.v1.SyncService/...",
// "/syncplane.v1.PresenceService/..." and "/syncplane.v1.BlobService/...").
// An unknown app is 404. App servers are created on demand by Register / boot
// reload, so a request for an unregistered app never auto-creates one.
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
// names of the entities next declares, sorted. A merge that would drop an
// entity of prev is an error (see refuseDroppedEntities).
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
	// A live server keeps every entity it registered, so a merge that drops
	// one would serve another set of entities after a restart than before it.
	if err := refuseDroppedEntities(prevFiles, nextFiles, defined, entityNames); err != nil {
		return nil, nil, err
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

// refuseDroppedEntities returns an error when next replaces the message of an
// entity of prev with a message that is another entity or none. defined holds
// the full names of the top-level definitions of next, and entities the names
// of the entities it declares.
func refuseDroppedEntities(prev, next *protoregistry.Files, defined, entities map[string]bool) error {
	var err error
	prev.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		msgs := fd.Messages()
		for i := 0; i < msgs.Len(); i++ {
			md := msgs.Get(i)
			name := protoopts.Entity(md).GetName()
			if name == "" || entities[name] || !defined[string(md.FullName())] {
				continue
			}
			as := "with no (syncplane.v1.entity) option"
			if d, ferr := next.FindDescriptorByName(md.FullName()); ferr == nil {
				if nmd, ok := d.(protoreflect.MessageDescriptor); ok {
					if other := protoopts.Entity(nmd).GetName(); other != "" {
						as = fmt.Sprintf("as entity %q", other)
					}
				}
			}
			err = fmt.Errorf("server: the set declares message %s, which the app registered as entity %q, %s; "+
				"an entity cannot be renamed or removed", md.FullName(), name, as)
			return false
		}
		return true
	})
	return err
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
