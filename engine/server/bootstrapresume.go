package server

import (
	"context"
	"fmt"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	syncplanecursorv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/cursor/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
)

// Resuming an interrupted Bootstrap. A dropped connection used to cost the
// whole snapshot; a client that kept what landed can instead hand back the
// cursor it was given and the position it got to, and be served the rest.
//
// Continuing the ORIGINAL snapshot — rather than starting a new one that skips
// what the client already holds — is what makes this safe. The rows delivered
// across attempts are a mixture of workspace states, exactly as they already
// are across the pages of one attempt, and the healing argument is the same
// one (see the Bootstrap doc comment): the client follows the snapshot with a
// Pull from the Begin cursor, and that cursor is re-issued unchanged on every
// resumed attempt. Everything written since it was minted is therefore
// re-delivered — no matter which attempt shipped the row, or whether any did.
//
// The position is recorded as the SET of entities already delivered plus the
// one that was mid-scan, never as an offset into the delivery order: that order
// depends on the request's priority entities and on the catalog, and neither is
// pinned across attempts.

// bootstrapResume is a resume request that decoded cleanly: the snapshot's
// Begin cursor (kept raw, since it is re-issued verbatim) and how far its scan
// got. Whether it can actually be served is decided by acceptable/servable.
type bootstrapResume struct {
	rawCursor []byte
	cur       cursorState
	done      map[string]bool
	entity    string
	afterKey  string
}

// decodeBootstrapResume parses the client's resume request. A token this
// server cannot parse is a client error rather than a reason to silently start
// over: it can only come from a client corrupting what it was handed, and
// failing loudly is what surfaces that.
func (s *Server) decodeBootstrapResume(ctx context.Context, ws string, r *syncplanev1.BootstrapResume) (*bootstrapResume, error) {
	if r == nil || len(r.GetCursor()) == 0 {
		return nil, nil
	}
	cur, err := s.decodeCursor(ctx, ws, r.GetCursor())
	if err != nil {
		return nil, err
	}
	var pos syncplanecursorv1.BootstrapPosition
	if err := proto.Unmarshal(r.GetToken(), &pos); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("bad resume token: %w", err))
	}
	res := &bootstrapResume{
		rawCursor: r.GetCursor(),
		cur:       cur,
		done:      make(map[string]bool, len(pos.GetDone())),
		entity:    pos.GetEntity(),
		afterKey:  pos.GetAfterKey(),
	}
	for _, e := range pos.GetDone() {
		res.done[e] = true
	}
	return res, nil
}

// acceptable reports whether the interrupted snapshot was taken under the scope
// this request asks for. A different subscription set or filter epoch means the
// rows already delivered were selected by a different rule, so continuing would
// leave the client holding a mixture no cursor describes — and the cursor is
// the only thing that could have healed it, since a pull from it reconciles
// against the sets IT records. Starting over is the recovery.
func (r *bootstrapResume) acceptable(wantHash, epoch uint64) bool {
	return r.cur.subHash == wantHash && r.cur.epoch == epoch
}

// servable reports whether the snapshot's cursor is still one this store can
// serve from, under exactly the rules a pull on it would apply (see
// pullPreamble): a cursor below the tombstone horizon would reset on the very
// pull that is meant to heal the snapshot, and one from a foreign store
// lineage describes rows this store never held. Either way the resumed
// snapshot could not be completed, so the client is better served by a fresh
// one it can finish.
func (r *bootstrapResume) servable(horizon, current, storeEpoch uint64) bool {
	if belowHorizon(r.cur.seq, r.cur.partialEntity, horizon) || r.cur.seq > current {
		return false
	}
	// The membership sets the snapshot was taken under are gone from the
	// cursor-state store, so the healing pull would reset on this cursor —
	// the same verdict the horizon check reaches for an over-aged one.
	if r.cur.stateEvicted {
		return false
	}
	return r.cur.storeEpoch == 0 || r.cur.storeEpoch == storeEpoch
}

// bootstrapPos tracks how far the snapshot scan has got, and renders it as the
// token that rides on each chunk. The token describes exactly the rows
// delivered through the chunk it accompanies: entities are marked done as their
// scans end, and the in-progress position advances per row.
type bootstrapPos struct {
	done     []string
	entity   string
	afterKey string
}

// resumedFrom seeds the position with what earlier attempts already delivered,
// so a second interruption does not forget the first one's work.
func (p *bootstrapPos) resumedFrom(r *bootstrapResume) {
	if r == nil {
		return
	}
	// Rebuilt from the map rather than kept in the decoded order: the set is
	// what matters, and the map is what the scan loop tests against.
	for e := range r.done {
		p.done = append(p.done, e)
	}
	p.entity, p.afterKey = r.entity, r.afterKey
}

// row records that a row of `entity` has been buffered for delivery.
func (p *bootstrapPos) row(entity, key string) {
	p.entity, p.afterKey = entity, key
}

// entityDone records that an entity's scan ran to completion. Nothing is left
// in progress at that moment: everything before it is in `done` too, so the
// position is the done set alone until the next row is buffered.
func (p *bootstrapPos) entityDone(entity string) {
	p.done = append(p.done, entity)
	p.entity, p.afterKey = "", ""
}

func (p *bootstrapPos) token() []byte {
	raw, err := proto.Marshal(&syncplanecursorv1.BootstrapPosition{
		Done:     p.done,
		Entity:   p.entity,
		AfterKey: p.afterKey,
	})
	if err != nil {
		panic(err) // cannot fail for a message of valid in-memory protos
	}
	return raw
}
