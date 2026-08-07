package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/naming"
	"github.com/levelfourab/syncplane/engine/storage"
)

// subScope is one workspace's state on a multiplexed Subscribe stream.
//
// Everything here is resolved once, at open, and then reused for the life of
// the stream: the emitter, the filter epoch and the warmed read filters must
// never be re-resolved per notification (a slow hook would then run on every
// change and, worse, inside a snapshot). The subscription set is fixed for the
// stream, so that warming stays valid.
//
// The workspace-access gate is the one exception, and it is re-run on the
// heartbeat rather than per notification — see regateScopes.
type subScope struct {
	ws string
	// clientID is the stream's idempotency handle. Watermarks are keyed
	// (workspace, client_id), so one handle serves every scope and each page
	// still reports last_acked_mutation_id for its own workspace.
	clientID string
	emitter  *rowEmitter
	epoch    uint64
	wantHash uint64
	subs     []*syncplanev1.Subscription
	// cur is the decoded pull position; rawCursor is the same position in
	// encoded form, which is what pullPage's reset/provenance checks compare
	// against. Both advance as pages are sent.
	cur       cursorState
	rawCursor []byte
	// sparseCursor is the client's opt-in to page frames that carry no cursor
	// (SubscribeRequest.sparse_cursor). cursorSent records whether the client
	// holds one at all — a scope opened without a cursor must be given one
	// before any can be withheld. owedBytes is the weight of the rows sent
	// since the last one. See checkpointDue.
	sparseCursor bool
	cursorSent   bool
	owedBytes    int
	// signals fires when this workspace changes; cancel releases the notifier
	// subscription.
	signals <-chan struct{}
	cancel  func()
	logger  *slog.Logger
	// live is false once the scope has been dropped (reset, revoked, failed).
	// A dropped scope is silently skipped by the tail loop; the client has
	// already been told and will reopen when it is ready.
	live bool
}

// Subscribe is the payload-carrying change stream: "a Pull that never closes",
// multiplexed over a set of workspaces.
//
// For each scope it catches the client up from that scope's cursor, marks the
// backlog drained with a caught_up frame, then pushes changed rows down the
// stream as the workspace changes — so an already-subscribed client applies
// them directly and skips a follow-up Pull RTT. Every frame names the workspace
// it concerns; only the heartbeat is stream-level.
//
// Multiplexing is the point: a client in ten workspaces holds ONE stream, not
// ten. Browsers cap concurrent connections per host, and a long-lived stream is
// what spends that budget, so tying stream count to workspace count is what
// makes a multi-workspace client unworkable. Each scope still does its own
// scan against its own partition — there is no cross-workspace transaction and
// no shared sequence — so the multiplexing is purely a transport concern.
//
// Failure is scoped too. A workspace whose cursor is unusable, whose access has
// been revoked, or whose scan fails is DROPPED from the stream with a typed
// reason; every other scope keeps streaming. Ending the whole stream because
// one partition had a bad day would take the other nine down with it.
//
// Access is re-checked while the stream runs, not only at open: every heartbeat
// re-runs the workspace gate for each live scope, so being removed from a
// project drops that scope within one heartbeat interval instead of whenever
// the client next reconnects. Authentication is NOT re-run — a credential that
// expires mid-stream still rides to the end of the stream (see authn.go).
//
// Each stream is its own goroutine doing its own scans, so a slow client blocks
// only its own Send, never another client. With the default in-process notifier
// the live tail only reaches clients connected to THIS process; multi-node
// realtime needs a shared content-free WithNotifier (clients still converge via
// the per-tab pull/poll floor either way — the stream is latency, not
// correctness).
func (s *Server) Subscribe(ctx context.Context, req *connect.Request[syncplanev1.SubscribeRequest], stream *connect.ServerStream[syncplanev1.SubscribeResponse]) error {
	msg := req.Msg
	if err := s.validateSubscribe(msg); err != nil {
		return err
	}

	// Streams held open, not streams opened: a long-lived channel's cost is the
	// level, and browsers cap concurrent connections per host, so how many are
	// currently held is what tells an operator whether clients have connection
	// slots left. Counted from after validation so a malformed request cannot
	// register a stream that never existed.
	s.tel().streams.Add(ctx, 1)
	defer s.tel().streams.Add(ctx, -1)

	limit := s.limits.MaxPullChanges
	if reqLimit := int(msg.GetLimit()); reqLimit > 0 && reqLimit < limit {
		limit = reqLimit
	}

	// Open every scope first. A scope that cannot be opened is reported as a
	// dropped frame rather than failing the stream, so one revoked workspace
	// does not deny the client the nine it still has.
	scopes := make([]*subScope, 0, len(msg.GetScopes()))
	defer func() {
		for _, sc := range scopes {
			sc.release()
		}
	}()
	noDeltas := noDeltaEntitySet(msg.GetNoDeltaEntities())
	for _, want := range msg.GetScopes() {
		sc, reason, err := s.openScope(ctx, want, msg.GetClientId(), noDeltas, msg.GetSparseCursor())
		if err != nil {
			if sendErr := s.sendDropped(stream, want.GetWorkspaceId(), reason, err); sendErr != nil {
				return sendErr
			}
			continue
		}
		scopes = append(scopes, sc)
	}
	if len(scopes) == 0 {
		// Every scope was refused. The client has the frames explaining why;
		// holding an inert stream open would just occupy a connection.
		return nil
	}

	heartbeat := time.NewTicker(s.heartbeat)
	defer heartbeat.Stop()

	// Catch-up, scope by scope. keepalive=true sends each scope's final page
	// even when empty, so the client always learns last_acked and the cursor
	// position before it is told it is current.
	for _, sc := range scopes {
		if err := s.drainScope(ctx, stream, sc, limit, true); err != nil {
			return err
		}
		if !sc.live {
			continue
		}
		if err := stream.Send(&syncplanev1.SubscribeResponse{
			WorkspaceId:    sc.ws,
			SchemaRevision: s.catalogPtr().schemaRev,
			Body:           &syncplanev1.SubscribeResponse_CaughtUp{CaughtUp: true},
		}); err != nil {
			return err
		}
		sc.logger.LogAttrs(ctx, slog.LevelDebug, "subscribe caught up")
	}

	// Merge every scope's notifications into one channel so the tail is a
	// single select regardless of workspace count. The forwarders carry only a
	// workspace id — the drain below rescans that partition — so a coalesced
	// or dropped duplicate signal costs nothing but a wasted scan.
	notes := s.mergeSignals(ctx, scopes)

	// Live tail. A notification drains the named scope; a heartbeat re-checks
	// access, drains every live scope (catching any signal lost to a full
	// buffer) and then sends a stream-level keepalive. The keepalive is
	// liveness-only, NOT a zero-row page: an idle client would otherwise run a
	// full empty rebase per tick across every workspace it holds.
	for {
		select {
		case <-ctx.Done():
			return nil
		case ws, ok := <-notes:
			if !ok {
				return nil
			}
			for _, sc := range scopes {
				if sc.ws != ws || !sc.live {
					continue
				}
				if err := s.drainScope(ctx, stream, sc, limit, false); err != nil {
					return err
				}
			}
		case <-heartbeat.C:
			if err := s.regateScopes(ctx, stream, scopes); err != nil {
				return err
			}
			anyLive := false
			for _, sc := range scopes {
				if !sc.live {
					continue
				}
				anyLive = true
				if err := s.drainScope(ctx, stream, sc, limit, false); err != nil {
					return err
				}
			}
			if !anyLive {
				// Every scope has been dropped; nothing left to stream.
				return nil
			}
			if err := stream.Send(&syncplanev1.SubscribeResponse{
				SchemaRevision: s.catalogPtr().schemaRev,
				Body:           &syncplanev1.SubscribeResponse_Heartbeat{Heartbeat: true},
			}); err != nil {
				return err
			}
		}
	}
}

// validateSubscribe enforces the request's shape before any workspace is
// touched. These are properties of the request, so they are RPC errors.
func (s *Server) validateSubscribe(msg *syncplanev1.SubscribeRequest) error {
	scopes := msg.GetScopes()
	if len(scopes) == 0 {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("subscribe needs at least one scope"))
	}
	if len(scopes) > s.limits.MaxWorkspacesPerRequest {
		return connect.NewError(connect.CodeInvalidArgument, errors.New(limitExceeded(
			fmt.Sprintf("subscribe spans %d workspaces", len(scopes)),
			"MaxWorkspacesPerRequest", s.limits.MaxWorkspacesPerRequest)))
	}
	seen := make(map[string]struct{}, len(scopes))
	for _, sc := range scopes {
		if err := naming.ValidateWorkspaceID(sc.GetWorkspaceId()); err != nil {
			return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("scope: %w", err))
		}
		// Two scopes for one workspace would interleave two cursors over one
		// partition and leave the client unable to tell the pages apart.
		if _, dup := seen[sc.GetWorkspaceId()]; dup {
			return connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("workspace %q appears in more than one scope", sc.GetWorkspaceId()))
		}
		seen[sc.GetWorkspaceId()] = struct{}{}
	}
	return nil
}

// openScope resolves one workspace's stream state. On failure it returns the
// drop reason the client should act on: a denial is permanent, a bad cursor is
// fixed by re-bootstrapping, everything else is worth a retry.
//
// Ordering is load-bearing and mirrors Pull. enterWorkspace runs FIRST, so a
// refused caller leaves no notifier subscription and no epoch lookup behind in
// a workspace they cannot see. Then every Permissions hook resolves ONCE,
// outside any read snapshot and outside the tail loop: FilterEpoch up front,
// then the emitter's per-entity read filters are warmed so classify() inside
// each per-notify scan is a pure cache hit.
func (s *Server) openScope(ctx context.Context, want *syncplanev1.WorkspaceScope, clientID string, noDeltas map[string]bool, sparseCursor bool) (*subScope, syncplanev1.WorkspaceDropped_Reason, error) {
	ws := want.GetWorkspaceId()

	principal, err := s.enterWorkspace(ctx, ws)
	if err != nil {
		if _, denied := AsWorkspaceDenial(err); denied {
			return nil, syncplanev1.WorkspaceDropped_REASON_ACCESS_REVOKED, err
		}
		return nil, syncplanev1.WorkspaceDropped_REASON_ERROR, err
	}
	cur, err := decodeCursor(want.GetCursor())
	if err != nil {
		// An undecodable cursor cannot be advanced from; a clean snapshot can.
		return nil, syncplanev1.WorkspaceDropped_REASON_RESET, err
	}
	wantHash, err := subscriptionHash(want.GetSubscriptions())
	if err != nil {
		return nil, syncplanev1.WorkspaceDropped_REASON_ERROR, err
	}
	emitter, err := newRowEmitter(ctx, s, ws, want.GetSubscriptions())
	if err != nil {
		return nil, syncplanev1.WorkspaceDropped_REASON_ERROR, err
	}
	emitter.noDeltaEntities = noDeltas
	epoch, err := s.perms.FilterEpoch(ctx, principal, ws)
	if err != nil {
		return nil, syncplanev1.WorkspaceDropped_REASON_ERROR, err
	}
	for _, e := range emitter.entities() {
		if _, err := emitter.filterOf(e); err != nil {
			return nil, syncplanev1.WorkspaceDropped_REASON_ERROR, err
		}
	}

	signals, cancel := s.notifier.Subscribe(ws)
	return &subScope{
		ws:           ws,
		clientID:     clientID,
		emitter:      emitter,
		epoch:        epoch,
		wantHash:     wantHash,
		subs:         want.GetSubscriptions(),
		cur:          cur,
		rawCursor:    want.GetCursor(),
		sparseCursor: sparseCursor,
		cursorSent:   len(want.GetCursor()) > 0,
		signals:      signals,
		cancel:       cancel,
		logger:       s.reqLog(ctx, "subscribe", ws, clientID),
		live:         true,
	}, syncplanev1.WorkspaceDropped_REASON_UNSPECIFIED, nil
}

// regateScopes re-runs the workspace-access gate for every live scope and drops
// the ones it no longer admits.
//
// A stream outlives the decision that admitted it. Gated only at open, a client
// removed from a project keeps receiving that workspace's rows for as long as
// its stream stays healthy — which, for an open tab, can be indefinitely. Doing
// this on the heartbeat bounds that window to one heartbeat interval
// (WithHeartbeat) and costs one gate call per live workspace per tick, on a
// timer the client cannot influence. It deliberately does NOT run per
// notification: a hook on that path would run per change, which is the cost the
// rest of the scope's state is resolved once at open to avoid.
//
// A settled denial drops the scope as ACCESS_REVOKED — the same permanent,
// actionable verdict openScope sends, so a client reacts to a revocation
// identically whenever it arrives. A gate that cannot DECIDE drops it as ERROR:
// an outage must never read as revocation (the client would discard good data),
// but continuing to stream a workspace whose access can no longer be affirmed
// is the wrong side to fail on. That scope's client reopens with backoff and
// meets the same gate at open, exactly as a Pull would during the same outage.
func (s *Server) regateScopes(ctx context.Context, stream *connect.ServerStream[syncplanev1.SubscribeResponse], scopes []*subScope) error {
	for _, sc := range scopes {
		if !sc.live {
			continue
		}
		_, err := s.enterWorkspace(ctx, sc.ws)
		if err == nil {
			continue
		}
		reason := syncplanev1.WorkspaceDropped_REASON_ERROR
		if _, denied := AsWorkspaceDenial(err); denied {
			reason = syncplanev1.WorkspaceDropped_REASON_ACCESS_REVOKED
		}
		sc.logger.LogAttrs(ctx, slog.LevelInfo, "subscribe scope dropped by access re-check",
			slog.String("reason", reason.String()),
			slog.String("cause", err.Error()))
		if err := s.dropScope(stream, sc, reason, err); err != nil {
			return err
		}
	}
	return nil
}

// mergeSignals fans every scope's notification channel into one, so the tail
// loop is a single select whatever the workspace count. The merged channel
// carries workspace ids; a full buffer drops the duplicate rather than blocking
// a notifier, which is safe because the drain rescans from the cursor and a
// heartbeat sweeps every scope anyway.
func (s *Server) mergeSignals(ctx context.Context, scopes []*subScope) <-chan string {
	merged := make(chan string, len(scopes))
	for _, sc := range scopes {
		go func(ws string, signals <-chan struct{}) {
			for {
				select {
				case <-ctx.Done():
					return
				case _, ok := <-signals:
					if !ok {
						return
					}
					select {
					case merged <- ws:
					default: // a wake for this workspace is already queued
					}
				}
			}
		}(sc.ws, sc.signals)
	}
	return merged
}

// checkpointDue reports whether the page being sent must carry next_cursor.
//
// Withholding one leaves the client resuming from an older position, so a
// reopened stream re-delivers every row sent since. That is a saving only
// while those rows weigh less than the cursor they displaced, which is what
// the byte debt tracks — and it needs no tuning to stay honest. The cursor of
// a subscription with no window and no follow is smaller than any page it
// rides on, so every frame checkpoints exactly as it did before sparse
// cursors existed; only a cursor carrying a whole membership set is large
// enough to be worth skipping, and even then never for longer than it takes
// the skipped rows to add up to it.
func (sc *subScope) checkpointDue(pageBytes, cursorBytes int) bool {
	if !sc.sparseCursor || !sc.cursorSent {
		return true
	}
	return sc.owedBytes+pageBytes >= cursorBytes
}

// drainScope pages one scope forward until its backlog (or burst) is
// exhausted, sending each page as it is produced. A page carrying rows is
// always sent; an empty page is sent only when keepalive is set, which is how
// catch-up reports last_acked and the cursor position on a quiet workspace.
//
// A reset verdict drops the scope instead of ending the stream: the client
// re-bootstraps that workspace and reopens. The scope's cursor advances as a
// side effect of every page sent; whether that cursor also goes out on the
// frame is checkpointDue's call.
//
// It returns an error only when the STREAM cannot continue (a failed Send, or
// a cancelled context). A failure scoped to the workspace drops the scope and
// returns nil.
func (s *Server) drainScope(ctx context.Context, stream *connect.ServerStream[syncplanev1.SubscribeResponse], sc *subScope, limit int, keepalive bool) error {
	for {
		var page *syncplanev1.PullResponse
		var next cursorState
		var reason resetReason
		if err := s.store.WithReadTx(ctx, sc.ws, func(tx storage.ReadTx) error {
			var e error
			page, next, reason, e = s.pullPage(tx, sc.cur, sc.emitter, sc.wantHash, sc.epoch, sc.subs, sc.clientID, sc.rawCursor, limit)
			return e
		}); err != nil {
			// A read failure is this workspace's problem, not the stream's.
			return s.dropScope(stream, sc, syncplanev1.WorkspaceDropped_REASON_ERROR, err)
		}
		if page.GetReset_() {
			logReset(ctx, sc.logger, reason)
			return s.dropScope(stream, sc, syncplanev1.WorkspaceDropped_REASON_RESET, fmt.Errorf("%s", reason))
		}
		sc.cur = next
		sc.rawCursor = page.GetNextCursor()
		last := !page.GetHasMore()
		hasRows := len(page.GetPatch().GetChanges()) > 0 || len(page.GetPatch().GetDropEntities()) > 0
		if hasRows || (keepalive && last) {
			// The scope carries its own position, so the cursor on the frame is
			// the client's resume point and nothing else — it can be left off
			// when it would cost more than the re-delivery it saves.
			pageBytes := proto.Size(page.GetPatch())
			if sc.checkpointDue(pageBytes, len(page.GetNextCursor())) {
				sc.owedBytes = 0
				sc.cursorSent = true
			} else {
				sc.owedBytes += pageBytes
				page.NextCursor = nil
			}
			if err := stream.Send(&syncplanev1.SubscribeResponse{
				WorkspaceId:    sc.ws,
				SchemaRevision: s.catalogPtr().schemaRev,
				Body:           &syncplanev1.SubscribeResponse_Page{Page: page},
			}); err != nil {
				return err
			}
			if hasRows {
				sc.logger.LogAttrs(ctx, slog.LevelDebug, "subscribe page sent",
					slog.Int("changes", len(page.GetPatch().GetChanges())),
					slog.Int("drop_entities", len(page.GetPatch().GetDropEntities())))
			}
		}
		if last {
			return nil
		}
	}
}

// dropScope marks a scope dead and tells the client why. It returns an error
// only if the frame could not be sent (the stream itself is gone).
func (s *Server) dropScope(stream *connect.ServerStream[syncplanev1.SubscribeResponse], sc *subScope, reason syncplanev1.WorkspaceDropped_Reason, cause error) error {
	sc.live = false
	sc.release()
	return s.sendDropped(stream, sc.ws, reason, cause)
}

// release drops the scope's notifier subscription. It is idempotent: a scope
// dropped mid-stream releases here, and Subscribe's deferred sweep releases
// whatever is left when the stream ends.
func (sc *subScope) release() {
	if sc.cancel == nil {
		return
	}
	sc.cancel()
	sc.cancel = nil
}

// sendDropped emits a WorkspaceDropped frame. The message is for logs only —
// clients branch on reason — so it carries the cause verbatim.
func (s *Server) sendDropped(stream *connect.ServerStream[syncplanev1.SubscribeResponse], ws string, reason syncplanev1.WorkspaceDropped_Reason, cause error) error {
	msg := ""
	if cause != nil {
		msg = cause.Error()
	}
	return stream.Send(&syncplanev1.SubscribeResponse{
		WorkspaceId:    ws,
		SchemaRevision: s.catalogPtr().schemaRev,
		Body: &syncplanev1.SubscribeResponse_Dropped{Dropped: &syncplanev1.WorkspaceDropped{
			Reason:  reason,
			Message: msg,
		}},
	})
}
