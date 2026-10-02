package server

import (
	"context"
	"log/slog"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/levelfourab/syncplane/engine/storage"
)

// Quarantine of undecodable rows.
//
// A stored row whose bytes will not unmarshal is poison for every read path
// that has to look inside it: a read filter, a subscription predicate, a query,
// a window, a follow hop. Surfacing the decode error fails the whole request,
// and because the failure is deterministic that leaves Pull, Bootstrap and
// Query broken for the ENTIRE workspace until somebody rewrites the row — one
// bad row bricking every client. Losing one row's visibility is the far
// cheaper outcome, so the client-facing read paths quarantine instead.
//
// Quarantine means the row is reported and then treated as if it were not
// there: hidden in pull (so a client that already holds it drops it, exactly as
// for a row a read filter denies), skipped in bootstrap, no query match, no
// window member, no follow seed or target. Nothing is deleted — the bytes stay
// as they are, and any later PUT of that key replaces them wholesale (see
// applyCrud), after which the row decodes and syncs normally again.
//
// Two neighbours deliberately do NOT quarantine:
//
//   - The write paths reject instead. A client value that does not decode is
//     refused at the door, and a PATCH cannot merge onto a base nobody can
//     decode, so it rejects — an ack the client drops, never an error that
//     wedges its queue.
//   - The trusted in-process reads (ViewTx, EntityTx) surface the decode error
//     to the Go caller, which can act on it. Silently short-changing a
//     server-side job's result set would be the worse failure there. A mutator
//     that hits one while serving a custom mutation should turn it into a
//     Reject, or the client retries a mutation nothing can make succeed.
//
// One asymmetry survives on purpose: a full sync with no read filter and no
// predicate never decodes a row at all — it ships the stored bytes verbatim on
// a zero-decode fast path. Decoding every row there to catch a corrupt one
// would tax every healthy workspace for a fault none of them have, so a corrupt
// row still reaches clients on that path, and the fault surfaces where it
// already did: at the reader that tries to parse it.

// maxPoisonReports bounds the set of distinct rows one process reports, so
// corruption can drive neither memory nor log volume. Past the cap rows are
// still quarantined, just silently: a store with this many undecodable rows has
// made its point.
const maxPoisonReports = 1024

// poisonKey identifies one undecodable row version. The version is part of the
// identity so a repaired row that goes bad again is reported again.
type poisonKey struct {
	workspace string
	entity    string
	key       string
	version   uint64
}

// poisonReports remembers which undecodable rows have been logged, so a scan
// that crosses the same row on every pull reports it once rather than on every
// request. It is consulted from inside read transactions, so it carries its own
// small lock rather than sharing any of the server's coarser ones.
type poisonReports struct {
	mu   sync.Mutex
	seen map[poisonKey]struct{}
}

// first reports whether k has not been logged before, recording it if so.
func (p *poisonReports) first(k poisonKey) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.seen[k]; ok || len(p.seen) >= maxPoisonReports {
		return false
	}
	if p.seen == nil {
		p.seen = make(map[poisonKey]struct{})
	}
	p.seen[k] = struct{}{}
	return true
}

// reportPoison logs one undecodable row, at most once per row version per
// process. It never fails and never decides anything: the caller has already
// chosen what to do with the row.
func (s *Server) reportPoison(ctx context.Context, workspace string, row storage.Row, err error) {
	if !s.poison.first(poisonKey{
		workspace: workspace,
		entity:    row.Entity,
		key:       row.Key,
		version:   row.Version,
	}) {
		return
	}
	s.log().LogAttrs(ctx, slog.LevelWarn, "undecodable row quarantined",
		slog.String("component", "quarantine"),
		slog.String("workspace", workspace),
		slog.String("entity", row.Entity),
		slog.String("key", row.Key),
		slog.Uint64("version", row.Version),
		slog.Any("err", err))
}

// decodeMatchable decodes a row a query scan is about to match against its
// predicate. A nil emitter marks the trusted in-process scan, whose Go caller
// gets the decode error; a client-facing scan quarantines instead, returning
// (nil, nil) for a row the scan must skip. A field-history record decodes as
// the caller receives it (see projectHistory).
func decodeMatchable(e *rowEmitter, info *entityInfo, row storage.Row) (proto.Message, error) {
	if e == nil {
		return info.decode(row.Value)
	}
	if rec, ok := e.historyView(row.Key); ok && info.name == historyEntityName {
		return rec, nil
	}
	msg, ok := e.decodeRow(info, row)
	if !ok {
		return nil, nil
	}
	return msg, nil
}

// decodeRow decodes a stored row for a client-facing read path. ok is false for
// a row that will not decode: it has been reported, and the caller treats it as
// absent rather than failing the request out from under every other row.
func (e *rowEmitter) decodeRow(info *entityInfo, row storage.Row) (msg proto.Message, ok bool) {
	msg, err := info.decode(row.Value)
	if err != nil {
		e.srv.reportPoison(e.ctx, e.ws, row, err)
		return nil, false
	}
	return msg, true
}
