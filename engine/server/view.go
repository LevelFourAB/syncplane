package server

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/storage"
)

// View runs fn inside a read-only snapshot of the workspace: it takes no write
// lock, notifies nobody, and never consumes a sequence. It is the read half of
// the in-process API — what an app's own GET handlers and background jobs read
// through.
//
// # Permissions
//
// The workspace gate (CanAccessWorkspace) runs before the snapshot opens, and
// every row the closure reads is passed through the entity's ReadFilter — the
// same filter, resolved by the same machinery, that decides what a sync client
// may pull. A filtered-out row reads as absent: Get reports not-found, scans and
// queries skip it. Authorization is against the ctx principal, so a ctx with no
// principal reads as anonymous.
//
// Both hooks are resolved BEFORE the snapshot opens, so neither can pin it.
// Because the closure may touch any entity, every registered entity's filter is
// resolved up front — see readGate for what that costs.
//
// System reads with no caller to authorize — the admin UI on its private
// listener, a connector, a migration — mark the ctx with Trusted, which skips
// both hooks and returns every live row.
//
// Unlike a write transaction the read closure runs AT MOST ONCE (storage
// adapters never retry a read tx), so fn may stream results out as it reads.
// Returning an error surfaces it; the snapshot is consistent for the call's
// duration.
func (s *Server) View(ctx context.Context, workspaceID string, fn func(tx *ViewTx) error) error {
	if workspaceID == "" {
		return fmt.Errorf("server: workspace id is required")
	}
	_, enforced, err := s.gate(ctx, workspaceID)
	if err != nil {
		return err
	}
	visible, err := s.readGate(ctx, workspaceID, enforced)
	if err != nil {
		return err
	}
	return s.store.WithReadTx(ctx, workspaceID, func(tx storage.ReadTx) error {
		return fn(&ViewTx{srv: s, tx: tx, visible: visible})
	})
}

// ViewTx is the read-only entity facade View runs against. Entity identity
// comes from the message type passed in, exactly like EntityTx; it surfaces the
// stored row version so an editor can round-trip it for compare-and-swap.
type ViewTx struct {
	srv *Server
	tx  storage.ReadTx
	// visible gates each row against the caller's read filter, or is nil for a
	// Trusted read (everything live is visible). Resolved before the snapshot
	// opened; see Server.readGate.
	visible func(storage.Row) (bool, error)
}

func (v *ViewTx) info(msg proto.Message) (*entityInfo, error) {
	return entityInfoFor(v.srv, msg)
}

// Get loads the row with the given key into dst and reports whether a live row
// was found and the sequence (version) it was last written at. A missing or
// deleted row returns (false, 0, nil).
func (v *ViewTx) Get(dst proto.Message, key string) (found bool, version uint64, err error) {
	info, err := v.info(dst)
	if err != nil {
		return false, 0, err
	}
	return readRowVisible(v.tx, info, dst, key, v.visible)
}

// ScanPage iterates up to limit live rows of the entity identified by prototype
// with key > afterKey, in key order, decoding each into a fresh message and
// passing it with its version. It returns the key of the last row visited and
// whether more rows follow; pass lastKey back as afterKey to continue. limit
// must be positive.
func (v *ViewTx) ScanPage(prototype proto.Message, afterKey string, limit int, fn func(msg proto.Message, version uint64) error) (lastKey string, more bool, err error) {
	info, err := v.info(prototype)
	if err != nil {
		return "", false, err
	}
	return scanPageRows(v.tx, info, afterKey, limit, v.visible, fn)
}

// Query iterates the live rows of the entity identified by prototype that
// match spec, in spec's order, decoding each into a fresh message and passing
// it with its version — so a result can be handed straight back to a
// compare-and-swap write. It is the "find everything matching X" read that
// list and search endpoints are built from.
//
// The predicate is the engine's own, compiled by the query package, so it
// means here exactly what it means to a sync client and to the Query RPC.
// Build it with the constructors rather than by hand:
//
//	tx.Query(&todov1.Todo{}, query.Spec{
//		Where:   query.And(query.Eq("done", false), query.Contains("title", "milk")),
//		OrderBy: []*syncplanev1.OrderKey{query.Desc("created_at_ms")},
//		Limit:   20,
//	}, func(msg proto.Message, version uint64) error { … })
//
// The caller's read filter applies (a Trusted view sees every live row), so the
// page counts only rows this caller may see. No engine limit caps the result,
// though, so unlike the RPC a spec with no Limit returns — and holds in memory —
// every match.
//
// Page with Spec.After/Before rather than Offset: a cursor names a position in
// the ordering, so the page is unaffected by rows inserted ahead of it and an
// index-served ordered query seeks straight to it. A cursor from a different
// entity or ordering fails with query.ErrCursorShape.
//
// A declared secondary index serves the query when one covers its predicate and
// ordering, making it O(matches). With none — or while an index is still
// building — every live row of the entity is read and decoded, so the query is
// O(entity) however selective the predicate is. Fine for a background job;
// think twice in a hot handler over a large entity.
//
// fn runs inside the read snapshot, so it must not start another transaction.
// An error from fn stops the iteration and surfaces.
func (v *ViewTx) Query(prototype proto.Message, spec query.Spec, fn func(msg proto.Message, version uint64) error) error {
	info, err := v.info(prototype)
	if err != nil {
		return err
	}
	rows, err := runQuerySpec(v.tx, info, spec, v.visible, v.srv.limits)
	if err != nil {
		return err
	}
	for _, r := range rows {
		if err := fn(r.msg, r.row.Version); err != nil {
			return err
		}
	}
	return nil
}

// runQuerySpec compiles a Spec against the entity and answers it over tx,
// keeping the rows visible admits. It is the in-process query path shared by
// ViewTx.Query, ApplyTx.Query and EntityTx.Query; no engine limit caps the
// result, unlike the Query RPC.
//
// visible is nil for a trusted read, in which case everything live matches.
// Because it is applied inside queryRows, the page counts only rows the caller
// may actually see — a filtered row consumes no page budget.
func runQuerySpec(tx storage.ReadTx, info *entityInfo, spec query.Spec, visible func(storage.Row) (bool, error), limits Limits) ([]qrow, error) {
	desc := info.msgType.Descriptor()
	// engine/query writes for the wire, where an unprefixed message is right;
	// on this path the same error lands in an app developer's Go call, so it
	// gets the package prefix and the entity it was compiled against.
	predicate, err := query.Compile(desc, spec.Where)
	if err != nil {
		return nil, fmt.Errorf("server: query %s: %w", info.name, err)
	}
	order, err := query.CompileOrder(desc, spec.OrderBy)
	if err != nil {
		return nil, fmt.Errorf("server: query %s order: %w", info.name, err)
	}
	if spec.Offset > 0 && (spec.After != nil || spec.Before != nil) {
		return nil, fmt.Errorf("server: query %s: Offset and After/Before mean different things by a page and cannot be combined", info.name)
	}
	after, err := decodeSpecCursor(order, info.name, "After", spec.After)
	if err != nil {
		return nil, err
	}
	before, err := decodeSpecCursor(order, info.name, "Before", spec.Before)
	if err != nil {
		return nil, err
	}
	if visible == nil {
		visible = func(storage.Row) (bool, error) { return true, nil }
	}
	// No emitter: this is an in-process read, so a row that will not decode
	// surfaces to the Go caller instead of being quarantined out of the result
	// set.
	rows, _, err := queryRows(tx, info, predicate, order, pageFor(after, before, spec.Offset, spec.Limit), visible, nil, limits)
	return rows, err
}

// decodeSpecCursor unwraps one of a Spec's cursors. Nothing bounds its length
// here: an in-process caller supplies a cursor the engine itself minted, unlike
// the RPC path, which caps it before decoding.
func decodeSpecCursor(order *query.Order, entity, field string, cursor []byte) ([]byte, error) {
	if len(cursor) == 0 {
		return nil, nil
	}
	entry, err := order.DecodeCursor(entity, cursor)
	if err != nil {
		return nil, fmt.Errorf("server: query %s %s: %w", entity, field, err)
	}
	return entry, nil
}

// entityInfoFor resolves the registered entity for msg's descriptor type. It is
// the shared lookup behind EntityTx.info and ViewTx.info.
func entityInfoFor(s *Server, msg proto.Message) (*entityInfo, error) {
	name := msg.ProtoReflect().Descriptor().FullName()
	info, ok := s.catalogPtr().byType[name]
	if !ok {
		return nil, errNotRegistered(name)
	}
	return info, nil
}

// readRowInto loads entity/key from tx into dst, returning whether a live row
// was found and its version. Shared by EntityTx.Get (write tx) and ViewTx.Get
// (read tx) — both run over a storage.ReadTx.
func readRowInto(tx storage.ReadTx, info *entityInfo, dst proto.Message, key string) (found bool, version uint64, err error) {
	return readRowVisible(tx, info, dst, key, nil)
}

// readRowVisible is readRowInto with an optional visibility gate applied before
// the row is decoded. A row the gate refuses reads exactly as an absent one, so
// a read filter never reveals that a row it hides exists.
func readRowVisible(
	tx storage.ReadTx,
	info *entityInfo,
	dst proto.Message,
	key string,
	visible func(storage.Row) (bool, error),
) (found bool, version uint64, err error) {
	row, err := tx.GetRow(info.name, key)
	if err == storage.ErrNotFound {
		return false, 0, nil
	}
	if err != nil {
		return false, 0, err
	}
	if row.Deleted {
		return false, 0, nil
	}
	if visible != nil {
		ok, err := visible(row)
		if err != nil {
			return false, 0, err
		}
		if !ok {
			return false, 0, nil
		}
	}
	proto.Reset(dst)
	if err := proto.Unmarshal(row.Value, dst); err != nil {
		return false, 0, err
	}
	return true, row.Version, nil
}

// scanPageRows iterates one page of live rows of info, decoding each into a
// fresh message and reporting its version. Shared by EntityTx.ScanPage,
// ViewTx.ScanPage and ApplyTx.ScanPage. It scans one row past limit to detect
// truncation without emitting it.
//
// visible is nil for a trusted read. When set, a row it refuses is skipped but
// still counts toward limit, so a filtered page can come back SHORTER than limit
// while more reports true — page on with lastKey rather than treating a short
// page as the end. Charging hidden rows to the budget is what keeps lastKey
// monotone and the paging terminating; the alternative (scan until limit visible
// rows are found) turns one storage call into an unbounded loop.
func scanPageRows(
	tx storage.ReadTx,
	info *entityInfo,
	afterKey string,
	limit int,
	visible func(storage.Row) (bool, error),
	fn func(msg proto.Message, version uint64) error,
) (lastKey string, more bool, err error) {
	if limit <= 0 {
		return "", false, fmt.Errorf("server: ScanPage limit must be positive, got %d", limit)
	}
	seen := 0
	err = tx.ScanEntityAfter(info.name, afterKey, limit+1, func(r storage.Row) error {
		seen++
		if seen > limit {
			more = true
			return nil
		}
		lastKey = r.Key
		if visible != nil {
			ok, err := visible(r)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
		}
		msg, err := info.decode(r.Value)
		if err != nil {
			return err
		}
		return fn(msg, r.Version)
	})
	if err != nil {
		return "", false, err
	}
	return lastKey, more, nil
}
