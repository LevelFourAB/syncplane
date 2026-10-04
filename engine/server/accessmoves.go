package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	syncplanecursorv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/cursor/v1"
	"github.com/levelfourab/syncplane/engine/internal/naming"
	"github.com/levelfourab/syncplane/engine/storage"
)

// The access move log.
//
// A row's visibility can depend on rows above it: a comment is readable where
// its task's project grants it. When a task moves to another project, every
// comment below it changes visibility, but no comment is written, so a pull
// that scans the changes after its cursor never meets them. The log records
// such a move in the transaction that makes it, and a pull reads the moves
// after its cursor to find the rows that moved without being written (see
// compareMoves).
//
// A move is recorded only for a row other rows can sit below, and only when
// one does at the time of the write: the change of its access parent, its
// creation below a parent (a row created with a key that rows below already
// name) and its deletion (a Trusted write may delete a row with rows below
// it). A row without rows below it moves nothing that the change scan does
// not already see. When one sequence writes a row more than once, the log
// keeps one record whose parent before the change is the one the row had at
// the start of the sequence.
//
// The records are rows of accessMovesEntity, which no schema declares, so the
// sync paths skip them as they skip any entity the catalog does not hold. Each
// is written at the sequence of its change, and keyed by that sequence first,
// so that key order is sequence order. GC removes the records at or below the
// tombstone horizon: a cursor below the horizon resets, so every cursor that
// is served incrementally still finds every move after it.

// accessMovesEntity is the storage entity of the access move log.
const accessMovesEntity = "syncplane_access_moves"

// accessMoveSeqWidth is the width of the sequence that opens a log row's key,
// enough for any uint64 in decimal.
const accessMoveSeqWidth = 20

// accessMoveKey returns the key of the log row that records a move of the row
// of entity with key at seq.
func accessMoveKey(seq uint64, entity, key string) string {
	return naming.RowKey([]string{fmt.Sprintf("%0*d", accessMoveSeqWidth, seq), entity, key})
}

// accessMovePrefix returns the prefix of the keys of every log row written at
// seq.
func accessMovePrefix(seq uint64) string {
	return fmt.Sprintf("%0*d/", accessMoveSeqWidth, seq)
}

// anchorRefOf renders an edge's parent for the log, nil when the edge is
// unset.
func anchorRefOf(e chainEdge) *syncplanecursorv1.AnchorRef {
	if !e.ok {
		return nil
	}
	return &syncplanecursorv1.AnchorRef{Entity: e.parent.entity, Key: e.parent.key}
}

// edgeOfRef reads an edge back from the log.
func edgeOfRef(r *syncplanecursorv1.AnchorRef) chainEdge {
	if r == nil {
		return chainEdge{}
	}
	return chainEdge{parent: anchorRow{entity: r.GetEntity(), key: r.GetKey()}, ok: true}
}

// recordAccessMove writes the log record of a write that changes the access
// parent of a row other rows can sit below, when rows sit below it. Every
// write path calls it, Trusted ones included, after every check that can
// refuse the write and before the row is written. final is the row as it will
// be stored, nil for a delete; load returns the row before the write.
func (s *Server) recordAccessMove(
	ctx context.Context,
	ws string,
	tx storage.WriteTx,
	info *entityInfo,
	key string,
	load func() (storage.Row, bool, error),
	final proto.Message,
	seq uint64,
	actor storage.Actor,
) error {
	model := s.catalogPtr().access
	if model == nil || info.access == nil || !info.access.hasParent() || !model.anchors[info.name] {
		return nil
	}
	var from, to chainEdge
	row, live, err := load()
	if err != nil {
		return err
	}
	if live {
		if stored, err := info.decode(row.Value); err != nil {
			s.reportPoison(ctx, ws, row, err)
		} else {
			from = edgeOf(model, info, stored.ProtoReflect())
		}
	}
	if final != nil {
		to = edgeOf(model, info, final.ProtoReflect())
	}
	if from == to {
		return nil
	}
	below, err := liveRowBelow(tx, model, info, key, s.limits)
	if err != nil || below == "" {
		return err
	}
	logKey := accessMoveKey(seq, info.name, key)
	// An earlier write of the same sequence moved the row already: its record
	// holds where the row was when the sequence began.
	prev, err := tx.GetRow(accessMovesEntity, logKey)
	switch {
	case errors.Is(err, storage.ErrNotFound):
	case err != nil:
		return err
	case !prev.Deleted:
		var m syncplanecursorv1.AccessMove
		if err := proto.Unmarshal(prev.Value, &m); err == nil {
			from = edgeOfRef(m.GetFrom())
		}
	}
	value, err := proto.MarshalOptions{Deterministic: true}.Marshal(&syncplanecursorv1.AccessMove{
		Entity: info.name,
		Key:    key,
		From:   anchorRefOf(from),
		To:     anchorRefOf(to),
	})
	if err != nil {
		return err
	}
	return tx.PutRow(accessMovesEntity, logKey, value, seq, actor)
}

// accessMove is one record of the log, read back.
type accessMove struct {
	version  uint64
	row      anchorRow
	from, to chainEdge
}

// errMovesRead stops a newest-first scan of the log.
var errMovesRead = errors.New("server: access moves read")

// readAccessMoves returns the moves recorded after floor, oldest first, and
// the version of the newest move recorded at all, 0 when there is none.
// overflow is set, and moves is nil, when more than limit moves are recorded
// after floor.
func readAccessMoves(tx storage.ReadTx, floor uint64, limit int) (moves []accessMove, newest uint64, overflow bool, err error) {
	err = tx.ScanEntityByVersionDesc(accessMovesEntity, 0, func(row storage.Row) error {
		if newest == 0 {
			newest = row.Version
		}
		if row.Version <= floor {
			return errMovesRead
		}
		if len(moves) == limit {
			overflow = true
			return errMovesRead
		}
		var m syncplanecursorv1.AccessMove
		if err := proto.Unmarshal(row.Value, &m); err != nil {
			return fmt.Errorf("server: access move %q does not decode: %w", row.Key, err)
		}
		moves = append(moves, accessMove{
			version: row.Version,
			row:     anchorRow{entity: m.GetEntity(), key: m.GetKey()},
			from:    edgeOfRef(m.GetFrom()),
			to:      edgeOfRef(m.GetTo()),
		})
		return nil
	})
	if err != nil && !errors.Is(err, errMovesRead) {
		return nil, 0, false, err
	}
	if overflow {
		return nil, newest, true, nil
	}
	for i, j := 0, len(moves)-1; i < j; i, j = i+1, j-1 {
		moves[i], moves[j] = moves[j], moves[i]
	}
	return moves, newest, false, nil
}

// accessMovesPurgeChunk bounds the log rows one GC pass reads to find what it
// may remove, so a backlog drains over several passes instead of holding one
// transaction.
const accessMovesPurgeChunk = 1024

// purgeAccessMoves removes the log records at or below the tombstone horizon,
// which no cursor that is served incrementally can need, and returns how many
// it removed.
func purgeAccessMoves(tx storage.WriteTx, horizon uint64) (int, error) {
	var seqs []uint64
	err := tx.ScanEntityAfter(accessMovesEntity, "", accessMovesPurgeChunk, func(row storage.Row) error {
		if row.Version > horizon {
			return errMovesRead
		}
		if len(seqs) == 0 || seqs[len(seqs)-1] != row.Version {
			seqs = append(seqs, row.Version)
		}
		return nil
	})
	if err != nil && !errors.Is(err, errMovesRead) {
		return 0, err
	}
	removed := 0
	for _, seq := range seqs {
		n, _, err := tx.PurgeEntityRows(accessMovesEntity, accessMovePrefix(seq), time.Time{}, 0)
		if err != nil {
			return removed, err
		}
		removed += n
	}
	return removed, nil
}
