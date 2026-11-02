package upstream

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"

	"github.com/levelfourab/syncplane/engine/client"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/internal/lww"
	"github.com/levelfourab/syncplane/engine/internal/naming"
	"github.com/levelfourab/syncplane/engine/internal/telemetry"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/upstream/internal/rebase"
)

var (
	// ErrNotStarted is returned by a call that needs a started link, Workspace
	// calls included, and by a local write to a bound entity made before
	// Start.
	ErrNotStarted = errors.New("upstream: link not started")
	// ErrClosed is returned by a call made after Close.
	ErrClosed = errors.New("upstream: link closed")
	// ErrNotLinked is returned for a workspace the link does not sync.
	ErrNotLinked = errors.New("upstream: workspace not linked")
	// ErrRevoked is returned by Flush, Await and Receipt.Wait for a workspace
	// the origin refused for good.
	ErrRevoked = errors.New("upstream: workspace access revoked")
	// ErrUnknownWrite is returned by Await for a local sequence the link holds
	// no record of: a write that forwarded nothing, one confirmed before the
	// process started, or one confirmed long enough ago to be forgotten.
	ErrUnknownWrite = errors.New("upstream: no record of the write")
	// ErrSchemaSkew is returned by a write-through write to a workspace that
	// SkewBlock holds. The write did not commit locally and did not reach the
	// origin.
	ErrSchemaSkew = errors.New("upstream: schema revision differs from the origin's; forwarding is blocked")
)

// Link keeps the bound entities of a local engine converged with an origin
// server, per linked workspace: it pulls the origin's changes into the local
// engine and forwards local writes to the origin.
//
// A Link is built with New, installed on the local engine with ServerOptions,
// and run with Start; see the package example. It is safe for concurrent use.
type Link struct {
	cfg   Config
	bound map[string]Direction
	// names is the bound entity names, sorted.
	names []string
	rpc   syncplanev1connect.SyncServiceClient
	log   *slog.Logger
	mx    *metrics

	bind atomic.Pointer[bound]

	mu       sync.Mutex
	started  bool
	closed   bool
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	clientID string
	ws       map[string]*workspace
	// streamWake asks the stream manager to rebuild its scopes.
	streamWake chan struct{}
	// syncs holds a token per workspace bootstrapping or catching up.
	syncs chan struct{}
	// heartbeat is the longest gap seen between two stream heartbeats, in
	// nanoseconds; 0 until two were seen.
	heartbeat atomic.Int64
	// idleProbe is the silence allowed before the heartbeat is known, in
	// nanoseconds; 0 means streamIdleProbe.
	idleProbe atomic.Int64

	// wtMu guards wt, the write-through bookkeeping per workspace. Captures
	// take it while holding the store's write lock, so nothing holding it
	// may wait for that lock.
	wtMu sync.Mutex
	wt   map[string]*wtState
}

// bound is what Start learns from the local server.
type bound struct {
	srv    *server.Server
	models map[string]*rebase.Model
}

// New validates cfg and returns a Link that is not started. It does not
// connect to the origin.
//
// It returns an error when OriginURL is not an absolute URL, when no binding
// is given, when a binding names neither a prototype nor an entity, or names
// one entity twice, and when ClientID breaks the client-id rules.
func New(cfg Config) (*Link, error) {
	u, err := url.Parse(cfg.OriginURL)
	if err != nil || !u.IsAbs() || u.Host == "" {
		return nil, fmt.Errorf("upstream: origin URL %q is not absolute", cfg.OriginURL)
	}
	if len(cfg.Bindings) == 0 {
		return nil, errors.New("upstream: at least one binding is required")
	}
	l := &Link{
		bound:      make(map[string]Direction, len(cfg.Bindings)),
		ws:         make(map[string]*workspace),
		streamWake: make(chan struct{}, 1),
	}
	for _, b := range cfg.Bindings {
		name := b.Entity
		if b.Prototype != nil {
			n, err := client.EntityName(b.Prototype)
			if err != nil {
				return nil, fmt.Errorf("upstream: binding: %w", err)
			}
			if name != "" && name != n {
				return nil, fmt.Errorf("upstream: binding names entity %q and a %q prototype", name, n)
			}
			name = n
		}
		if name == "" {
			return nil, errors.New("upstream: binding names no entity")
		}
		if _, dup := l.bound[name]; dup {
			return nil, fmt.Errorf("upstream: entity %q is bound twice", name)
		}
		if b.Direction != Both && b.Direction != PullOnly {
			return nil, fmt.Errorf("upstream: entity %q: unknown direction %d", name, b.Direction)
		}
		l.bound[name] = b.Direction
		l.names = append(l.names, name)
	}
	sort.Strings(l.names)
	if cfg.ClientID != "" {
		if err := checkClientID(cfg.ClientID); err != nil {
			return nil, err
		}
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.RetryMin <= 0 {
		cfg.RetryMin = time.Second
	}
	if cfg.RetryMax <= 0 {
		cfg.RetryMax = 30 * time.Second
	}
	cfg.RetryMax = max(cfg.RetryMax, cfg.RetryMin)
	if cfg.MaxPushMutations <= 0 {
		cfg.MaxPushMutations = 256
	}
	if cfg.MaxPushBytes <= 0 {
		cfg.MaxPushBytes = 4 << 20
	}
	if cfg.WriteThroughTimeout <= 0 {
		cfg.WriteThroughTimeout = 10 * time.Second
	}
	if cfg.PoisonThreshold == 0 {
		cfg.PoisonThreshold = 5
	}
	if cfg.ConcurrentSyncs <= 0 {
		cfg.ConcurrentSyncs = 4
	}
	l.syncs = make(chan struct{}, cfg.ConcurrentSyncs)
	l.cfg = cfg
	l.log = cfg.Logger.With(slog.String("component", "upstream"))
	opts := append([]connect.ClientOption{connect.WithInterceptors(cfg.Interceptors...)}, cfg.ConnectOptions...)
	l.rpc = syncplanev1connect.NewSyncServiceClient(cfg.HTTPClient, cfg.OriginURL, opts...)
	mx, err := newMetrics(telemetry.Providers{TracerProvider: cfg.TracerProvider, MeterProvider: cfg.MeterProvider})
	if err != nil {
		return nil, fmt.Errorf("upstream: telemetry: %w", err)
	}
	l.mx = mx
	return l, nil
}

// ServerOptions returns the options that install the link on the local
// engine: the write capture that records local writes, and the forwarding of
// custom mutations the local engine declares no mutator for. Pass them to
// server.New of the engine later given to Start; a link installs on one
// engine only.
//
// With them installed, the engine refuses a local write to a bound entity
// made before Start (ErrNotStarted), and one to a workspace the link does not
// sync (a server Rejection with code permission_denied).
func (l *Link) ServerOptions() []server.Option {
	return []server.Option{
		server.WithWriteCapture(l.capture),
		server.WithUnknownMutatorForwarding(),
	}
}

// Start binds the link to srv, the engine built with ServerOptions, and
// starts syncing every workspace linked before, with the subscriptions it
// was linked with. It returns once the link state is loaded; syncing runs in
// the background until Close or until ctx ends. Loading reads each workspace
// of the local store once. A workspace whose link state does not load is
// reported as StateFailed, and the others sync.
//
// It returns an error when the link was started before, when srv is nil, and
// when a bound entity is not registered on srv or its schema cannot be
// replayed. A Start that returned an error can be called again.
func (l *Link) Start(ctx context.Context, srv *server.Server) (err error) {
	if srv == nil {
		return errors.New("upstream: Start needs a server")
	}
	l.mu.Lock()
	if l.started || l.closed {
		l.mu.Unlock()
		return errors.New("upstream: Start called twice")
	}
	l.started = true
	l.mu.Unlock()
	defer func() {
		if err != nil {
			l.bind.Store(nil)
			l.mu.Lock()
			l.started = false
			l.mu.Unlock()
		}
	}()

	b := &bound{srv: srv, models: make(map[string]*rebase.Model, len(l.names))}
	for _, name := range l.names {
		meta, ok := srv.Entity(name)
		if !ok {
			return fmt.Errorf("upstream: bound entity %q is not registered on the server", name)
		}
		var mergers lww.Mergers
		if len(meta.Mergers) > 0 {
			mergers = make(lww.Mergers, len(meta.Mergers))
			for path, fn := range meta.Mergers {
				mergers[path] = lww.Merger{Fn: lww.MergeFunc(fn)}
			}
		}
		m, err := rebase.NewModel(meta.Descriptor, mergers)
		if err != nil {
			return fmt.Errorf("upstream: %w", err)
		}
		b.models[name] = m
	}
	l.bind.Store(b)

	// The link's own list, read from the primary store, finds every
	// workspace linked through it; the store's workspace list, which may lag
	// on a replica, finds those linked before the list existed.
	ids, err := l.registered(ctx, b)
	if err != nil {
		return fmt.Errorf("upstream: read linked workspaces: %w", err)
	}
	listed, err := srv.WorkspaceIDs(ctx)
	if err != nil {
		return fmt.Errorf("upstream: list workspaces: %w", err)
	}
	for _, id := range listed {
		if !slices.Contains(ids, id) {
			ids = append(ids, id)
		}
	}
	var (
		loaded  []*workspace
		purging []string
	)
	for _, id := range ids {
		w, purge, err := l.load(ctx, id)
		switch {
		case err != nil:
			l.log.LogAttrs(ctx, slog.LevelError, "link state does not load",
				slog.String("workspace", id), slog.Any("error", err))
			loaded = append(loaded, l.failedWorkspace(id, err))
		case w != nil:
			loaded = append(loaded, w)
		}
		if purge {
			purging = append(purging, id)
		}
	}
	clientID := l.cfg.ClientID
	for _, w := range loaded {
		if clientID == "" {
			clientID = w.clientID
		}
	}
	if clientID == "" {
		clientID = randomID()
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	l.clientID = clientID
	l.ctx, l.cancel = context.WithCancel(context.WithoutCancel(ctx))
	go func() {
		select {
		case <-ctx.Done():
			l.cancel()
		case <-l.ctx.Done():
		}
	}()
	for _, w := range loaded {
		if w.clientID == "" {
			w.clientID = clientID
		}
		l.ws[w.id] = w
		l.runWorker(w)
	}
	for _, id := range purging {
		// A removal or a revocation that was deleting rows when the process
		// stopped.
		l.wg.Add(1)
		go func() {
			defer l.wg.Done()
			if err := l.purge(l.ctx, b, id, purgeResume); err != nil && l.ctx.Err() == nil {
				l.log.LogAttrs(l.ctx, slog.LevelError, "finish deleting rows", slog.String("workspace", id), slog.Any("error", err))
			}
		}()
	}
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		l.runStreams(l.ctx)
	}()
	l.mx.observe(l)
	return nil
}

// Close stops syncing: it ends the stream to the origin, waits for in-flight
// calls and state writes to finish, and returns. Local writes made after
// Close still queue for the next Start. Close is safe to call more than once.
func (l *Link) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	cancel := l.cancel
	l.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	l.wg.Wait()
	l.mx.close()
	return nil
}

// ClientID returns the client id the link writes to the origin under, ""
// before Start.
func (l *Link) ClientID() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.clientID
}

// AddWorkspace links a workspace and starts syncing it in the background,
// with the given subscriptions; none means every row of every bound entity.
// The link persists the workspace and its subscriptions, so a later Start
// resumes it. Adding a linked workspace sets its subscriptions; adding a
// revoked one asks the origin again.
//
// The origin decides what the workspace holds: when its first snapshot
// arrives, every local row of a bound entity that the snapshot does not carry
// and no pending local write touches is deleted. Such rows exist only when the
// local engine was written to without the link installed. To push them to the
// origin instead, link with AddWorkspaceWith and AddOptions.AdoptLocalRows.
//
// It returns ErrNotStarted before Start and ErrClosed after Close, and an
// error for a subscription to an entity that is not bound.
func (l *Link) AddWorkspace(ctx context.Context, workspaceID string, subs ...*syncplanev1.Subscription) error {
	return l.AddWorkspaceWith(ctx, workspaceID, AddOptions{Subscriptions: subs})
}

// AddOptions configures AddWorkspaceWith.
type AddOptions struct {
	// Subscriptions scope the workspace, as for AddWorkspace.
	Subscriptions []*syncplanev1.Subscription
	// AdoptLocalRows makes the workspace's first snapshot push every local row
	// of a bound entity that the origin does not hold, as a PUT, instead of
	// deleting it. A row the origin holds takes the origin's value either way.
	// It applies to a workspace that is not linked yet, and only until its
	// first snapshot is applied.
	AdoptLocalRows bool
}

// AddWorkspaceWith is AddWorkspace with options.
func (l *Link) AddWorkspaceWith(ctx context.Context, workspaceID string, opts AddOptions) error {
	subs := opts.Subscriptions
	if err := naming.ValidateWorkspaceID(workspaceID); err != nil {
		return fmt.Errorf("upstream: %w", err)
	}
	if err := l.checkSubs(subs); err != nil {
		return err
	}
	b, err := l.running()
	if err != nil {
		return err
	}
	l.mu.Lock()
	w, ok := l.ws[workspaceID]
	if ok {
		if st := w.snapshot().State; st == StateRevoked || st == StateFailed {
			// Adding a revoked or failed workspace again starts it over.
			delete(l.ws, workspaceID)
			ok = false
		}
	}
	l.mu.Unlock()
	if ok {
		return l.SetSubscriptions(ctx, workspaceID, subs...)
	}
	if w != nil {
		w.stop()
	}
	if err := l.register(ctx, b, workspaceID, true); err != nil {
		return fmt.Errorf("upstream: link workspace %q: %w", workspaceID, err)
	}
	var meta syncMeta
	var q queueMeta
	err = b.srv.Replicate(ctx, workspaceID, func(rtx *server.ReplicaTx) error {
		st := writeState(rtx.Tx())
		var found bool
		var err error
		q, found, err = st.queue()
		if err != nil {
			return err
		}
		if !found || !q.linked {
			if found && q.purgeRows {
				return fmt.Errorf("upstream: workspace %q is still being removed", workspaceID)
			}
			q = queueMeta{linked: true, lowID: 1, nextID: 1, nextRef: 1}
			meta = syncMeta{clientID: l.ClientID(), adopt: opts.AdoptLocalRows}
		} else if meta, err = st.sync(); err != nil {
			return err
		}
		q.revoked = false
		meta.subs = subs
		if err := st.putQueue(q); err != nil {
			return err
		}
		return st.putSync(meta)
	})
	if err != nil {
		return fmt.Errorf("upstream: link workspace %q: %w", workspaceID, err)
	}
	w = l.newWorkspace(workspaceID, meta, q)
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if _, dup := l.ws[workspaceID]; dup {
		return nil
	}
	l.ws[workspaceID] = w
	l.runWorker(w)
	return nil
}

// SetSubscriptions replaces the subscriptions of a linked workspace; none
// means every row of every bound entity. The origin sends what enters the new
// scope and removes what leaves it. A local write to a row outside the scope
// is forwarded as any other; the row shows locally until the origin confirms
// the write, and then leaves the local engine, as the origin does not send it. It returns ErrNotLinked for a workspace
// the link does not sync.
func (l *Link) SetSubscriptions(ctx context.Context, workspaceID string, subs ...*syncplanev1.Subscription) error {
	if err := l.checkSubs(subs); err != nil {
		return err
	}
	b, err := l.running()
	if err != nil {
		return err
	}
	w := l.workspace(workspaceID)
	if w == nil {
		return ErrNotLinked
	}
	w.mu.Lock()
	var meta syncMeta
	err = b.srv.Replicate(ctx, workspaceID, func(rtx *server.ReplicaTx) error {
		// From the store, not w.meta: a write-through capture records its
		// acked sequence there without passing through the view.
		st := writeState(rtx.Tx())
		var err error
		if meta, err = st.sync(); err != nil {
			return err
		}
		meta.subs = subs
		return st.putSync(meta)
	})
	if err != nil {
		w.mu.Unlock()
		return fmt.Errorf("upstream: workspace %q: set subscriptions: %w", workspaceID, err)
	}
	w.meta = meta
	w.subGen++
	// Live again only once the origin caught the new scope up. The status
	// changes under w.mu, so a catch-up of the new generation cannot come
	// first, and is reported outside it, so a status callback may call back
	// into the link.
	w.vmu.Lock()
	w.st.live = false
	w.vmu.Unlock()
	w.mu.Unlock()
	w.update(func(*status) {})
	l.kickStreams()
	return nil
}

// RemoveWorkspace stops syncing a workspace and deletes the link's state for
// it, its pending writes included: writes the origin has not received are
// lost, so call Flush first to keep them. Local writes to the workspace's
// bound entities are refused afterwards. Removing a workspace that is not
// linked does nothing, whatever opts says.
//
// With RemoveOptions.DeleteRows the rows are deleted in pages, one local
// transaction each; a removal the process stopped in the middle of finishes
// after the next Start.
func (l *Link) RemoveWorkspace(ctx context.Context, workspaceID string, opts RemoveOptions) error {
	b, err := l.running()
	if err != nil {
		return err
	}
	l.mu.Lock()
	w := l.ws[workspaceID]
	delete(l.ws, workspaceID)
	l.mu.Unlock()
	if w != nil {
		w.stop()
		l.kickStreams()
	}
	l.resetFloor(workspaceID)
	mode := purgeUnlink
	if opts.DeleteRows {
		mode = purgeUnlinkRows
	}
	return l.purge(ctx, b, workspaceID, mode)
}

// purgeMode is what purge removes.
type purgeMode int

const (
	// purgeUnlink deletes the link state.
	purgeUnlink purgeMode = iota
	// purgeUnlinkRows deletes the rows of bound entities, then the link
	// state.
	purgeUnlinkRows
	// purgeRevoked deletes the rows of bound entities of a revoked
	// workspace, and keeps its link state.
	purgeRevoked
	// purgeResume finishes a deletion the stored state records.
	purgeResume
)

// scanPage is how many rows one transaction of a long pass over an entity
// handles.
var scanPage = 512

// purge deletes what mode names. A workspace that is not linked, and has no
// deletion in progress, is left alone. Rows are deleted in pages, one
// transaction each, behind a mark in the queue state that a restart resumes
// from.
func (l *Link) purge(ctx context.Context, b *bound, workspaceID string, mode purgeMode) error {
	var unlink, rows, unlinked bool
	err := b.srv.Replicate(ctx, workspaceID, func(rtx *server.ReplicaTx) error {
		unlinked = false
		st := writeState(rtx.Tx())
		q, found, err := st.queue()
		unlink, rows = false, false
		switch {
		case err != nil:
			return err
		case !found:
			// Not linked; a stale entry in the link's list goes.
			unlinked = mode != purgeRevoked
			return nil
		case mode == purgeResume:
			unlink, rows = !q.linked, q.purgeRows
			return nil
		case !q.linked && !q.purgeRows:
			return nil
		}
		unlink = mode != purgeRevoked
		rows = mode != purgeUnlink || q.purgeRows
		if unlink && !rows {
			unlinked = true
			return rtx.Tx().DeleteConsumer(consumerName)
		}
		q.linked = q.linked && !unlink
		q.purgeRows = true
		return st.putQueue(q)
	})
	if err != nil {
		return fmt.Errorf("upstream: remove workspace %q: %w", workspaceID, err)
	}
	if unlinked {
		return l.register(ctx, b, workspaceID, false)
	}
	if !rows {
		return nil
	}
	for _, name := range l.names {
		after := ""
		for {
			var n int
			var last string
			err := b.srv.Replicate(ctx, workspaceID, func(rtx *server.ReplicaTx) error {
				var keys []string
				if err := rtx.Tx().ScanEntityAfter(name, after, scanPage, func(r storage.Row) error {
					keys = append(keys, r.Key)
					return nil
				}); err != nil {
					return err
				}
				st := writeState(rtx.Tx())
				for _, k := range keys {
					if err := rtx.Delete(name, k); err != nil {
						return err
					}
					if err := st.del(name, k); err != nil {
						return err
					}
				}
				n = len(keys)
				if n > 0 {
					last = keys[n-1]
				}
				return nil
			})
			if err != nil {
				return fmt.Errorf("upstream: workspace %q: delete rows: %w", workspaceID, err)
			}
			after = last
			if n < scanPage {
				break
			}
		}
	}
	err = b.srv.Replicate(ctx, workspaceID, func(rtx *server.ReplicaTx) error {
		if unlink {
			return rtx.Tx().DeleteConsumer(consumerName)
		}
		st := writeState(rtx.Tx())
		q, found, err := st.queue()
		if err != nil || !found {
			return err
		}
		q.purgeRows = false
		return st.putQueue(q)
	})
	if err != nil || !unlink {
		return err
	}
	return l.register(ctx, b, workspaceID, false)
}

// registryWS holds the link's list of the workspaces it links, which Start
// reads from the primary store.
var registryWS = server.ReservedWorkspacePrefix + "upstream"

const (
	registryEntity = "#linked"
	registryKey    = "all"
)

// registered returns the workspaces the link's list holds.
func (l *Link) registered(ctx context.Context, b *bound) ([]string, error) {
	var ids []string
	err := b.srv.ReadState(ctx, registryWS, func(tx storage.ReadTx) error {
		var err error
		ids, err = readState(tx).registry()
		return err
	})
	return ids, err
}

// register adds a workspace to the link's list, or removes it.
func (l *Link) register(ctx context.Context, b *bound, workspaceID string, add bool) error {
	return b.srv.Replicate(ctx, registryWS, func(rtx *server.ReplicaTx) error {
		st := writeState(rtx.Tx())
		ids, err := st.registry()
		if err != nil {
			return err
		}
		has := slices.Contains(ids, workspaceID)
		switch {
		case add && !has:
			ids = append(ids, workspaceID)
		case !add && has:
			ids = slices.DeleteFunc(ids, func(id string) bool { return id == workspaceID })
		default:
			return nil
		}
		return st.putRegistry(ids)
	})
}

// Workspaces returns the linked workspace ids, sorted.
func (l *Link) Workspaces() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, 0, len(l.ws))
	for id := range l.ws {
		out = append(out, id)
	}
	slices.Sort(out)
	return out
}

// Status returns the status of a linked workspace, and false when the link
// does not sync it.
func (l *Link) Status(workspaceID string) (Status, bool) {
	w := l.workspace(workspaceID)
	if w == nil {
		return Status{}, false
	}
	return w.snapshot(), true
}

func (l *Link) workspace(id string) *workspace {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.ws[id]
}

// running returns the binding of a started, open link.
func (l *Link) running() (*bound, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	switch {
	case l.closed:
		return nil, ErrClosed
	case l.ctx == nil:
		return nil, ErrNotStarted
	}
	return l.bind.Load(), nil
}

func (l *Link) checkSubs(subs []*syncplanev1.Subscription) error {
	for _, s := range subs {
		if _, ok := l.bound[s.GetEntity()]; !ok {
			return fmt.Errorf("upstream: subscription to entity %q, which is not bound", s.GetEntity())
		}
	}
	return nil
}

// effectiveSubs is what the link asks the origin for: the given
// subscriptions, or every bound entity in full.
func (l *Link) effectiveSubs(subs []*syncplanev1.Subscription) []*syncplanev1.Subscription {
	if len(subs) > 0 {
		return subs
	}
	out := make([]*syncplanev1.Subscription, len(l.names))
	for i, n := range l.names {
		out[i] = &syncplanev1.Subscription{Entity: n}
	}
	return out
}

// load reads the link state of one workspace. It returns a nil workspace for
// one that is not linked, and reports a deletion of rows to finish.
func (l *Link) load(ctx context.Context, id string) (w *workspace, purge bool, err error) {
	b := l.bind.Load()
	var (
		q      queueMeta
		meta   syncMeta
		linked bool
	)
	err = b.srv.ReadState(ctx, id, func(tx storage.ReadTx) error {
		st := readState(tx)
		var found bool
		var err error
		q, found, err = st.queue()
		if err != nil || !found {
			return err
		}
		purge = q.purgeRows
		if !q.linked {
			return nil
		}
		linked = true
		meta, err = st.sync()
		return err
	})
	if err != nil {
		return nil, false, err
	}
	if !linked {
		return nil, purge, nil
	}
	return l.newWorkspace(id, meta, q), purge, nil
}

// failedWorkspace is the runtime of a workspace whose link state does not
// load: it reports err and does not sync.
func (l *Link) failedWorkspace(id string, err error) *workspace {
	w := l.newWorkspace(id, syncMeta{}, queueMeta{})
	w.st.failed = true
	w.st.failErr = fmt.Errorf("upstream: load workspace %q: %w", id, err)
	w.last = w.statusLocked()
	return w
}

// acquireSync takes one of the slots that bound how many workspaces
// bootstrap or catch up at once.
func (l *Link) acquireSync(ctx context.Context) error {
	select {
	case l.syncs <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *Link) releaseSync() { <-l.syncs }

func (l *Link) kickStreams() {
	select {
	case l.streamWake <- struct{}{}:
	default:
	}
}

// runWorker starts w's worker. The caller holds l.mu.
func (l *Link) runWorker(w *workspace) {
	if w.snapshot().State == StateFailed {
		close(w.done)
		return
	}
	ctx, cancel := context.WithCancel(l.ctx)
	w.cancel = cancel
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		defer close(w.done)
		w.run(ctx)
	}()
}

func checkClientID(id string) error {
	if len(id) == 0 || len(id) > naming.MaxClientIDBytes {
		return fmt.Errorf("upstream: client id must be 1 to %d bytes", naming.MaxClientIDBytes)
	}
	for i := 0; i < len(id); i++ {
		if id[i] < 0x20 {
			return errors.New("upstream: client id must not contain control characters")
		}
	}
	return nil
}

// randomID returns 128 random bits, URL-safe base64 without padding.
func randomID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return base64.RawURLEncoding.EncodeToString(b[:])
}
