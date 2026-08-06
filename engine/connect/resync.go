package connect

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/levelfourab/syncplane/engine/server"
)

// resyncSource is the egress half of the cursor-too-old fallback: when the
// change feed has a gap (tombstones below the horizon were collected before
// the connector saw them), the feed alone can no longer say what the source
// missed. This pass diffs current engine state against the shadows and
// repairs the source directly:
//
//   - engine row absent for a shadow → the delete the feed lost → delete at
//     the source, drop the shadow;
//   - engine row differing from its shadow base (or with no shadow) → an
//     update/create the cursor jumped over → write it out.
//
// Only bidirectional entities take part; an ingress-only entity has no egress
// to repair, and its shadows deliberately hold the source's view rather than
// the engine's, so diffing them against engine rows would report differences
// that are not drift.
//
// asOf is the engine sequence the caller will resume the feed from; swept
// shadows are stamped with it so the redelivered tail (rows written after
// asOf was read) flows through the normal drain path as no-ops or genuine
// changes. The scan runs inside one write transaction for a stable view —
// acceptable for a rare repair path, not for routine drains.
func (c *Connector) resyncSource(ctx context.Context, workspaceID string, p *pass, asOf uint64) error {
	if !c.needsWriter {
		// Nothing is bound bidirectionally, so there is no egress to repair —
		// and no Writer to demand of the source.
		return nil
	}
	w, err := p.writer()
	if err != nil {
		return err
	}
	// Snapshot the shadows first; State needs no reentrancy under scans.
	shadows := make(map[string]map[string]Shadow, len(c.bound))
	for entity, b := range c.bound {
		if b.direction != Bidirectional {
			continue
		}
		shadows[entity] = make(map[string]Shadow)
	}
	if err := c.state.ScanShadows(ctx, workspaceID, func(entity, key string, sh Shadow) error {
		if set, ok := shadows[entity]; ok {
			set[key] = sh
		}
		return nil
	}); err != nil {
		return err
	}

	type rowID struct{ entity, key string }
	var (
		writes  []proto.Message
		deletes []rowID
	)
	if err := c.srv.Mutate(server.TrustedAs(ctx, c.label), workspaceID, func(tx *server.EntityTx) error {
		// Reset on entry: the storage adapter may retry this closure. The
		// closure only reads; collected work applies after commit so source
		// I/O never runs under the workspace write lock.
		writes, deletes = nil, nil
		for entity, b := range c.bound {
			if b.direction != Bidirectional {
				continue
			}
			seen := make(map[string]bool)
			afterKey := ""
			for {
				lastKey, more, err := tx.ScanPage(b.prototype, afterKey, c.batch, func(msg proto.Message) error {
					_, key, err := c.srv.EntityKey(msg)
					if err != nil {
						return err
					}
					seen[key] = true
					sh, ok := shadows[entity][key]
					if ok && len(sh.Base) > 0 {
						base, err := c.decodeBase(b.prototype, sh.Base)
						if err != nil {
							return err
						}
						if proto.Equal(base, msg) {
							return nil
						}
					}
					writes = append(writes, msg)
					return nil
				})
				if err != nil {
					return err
				}
				if !more {
					break
				}
				afterKey = lastKey
			}
			for key := range shadows[entity] {
				if !seen[key] {
					deletes = append(deletes, rowID{entity, key})
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}

	for _, msg := range writes {
		entity, key, err := c.srv.EntityKey(msg)
		if err != nil {
			return err
		}
		fp, err := w.Write(ctx, workspaceID, msg)
		if err != nil {
			return fmt.Errorf("connect: resync write %s/%s: %w", entity, key, err)
		}
		if err := c.putSyncedShadow(ctx, workspaceID, entity, key, msg, fp, asOf); err != nil {
			return err
		}
	}
	for _, id := range deletes {
		if err := w.Delete(ctx, workspaceID, c.bound[id.entity].prototype, id.key); err != nil {
			return fmt.Errorf("connect: resync delete %s/%s: %w", id.entity, id.key, err)
		}
		if err := c.state.DeleteShadow(ctx, workspaceID, id.entity, id.key); err != nil {
			return err
		}
	}
	return nil
}
