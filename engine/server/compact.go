package server

import (
	"sort"

	"google.golang.org/protobuf/proto"

	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/text"
)

// Collaborative-text compaction: the GC pass that lets a long-lived document
// stop growing.
//
// A text field accumulates one item per character ever typed, because a delete
// only sets a tombstone (engine/text). Compaction drops the tombstones nothing
// can still refer to and records the document's version vector as that field's
// COMPACTION FLOOR, which is what stops a replica holding older state from
// merging them back in (see textfloor.go and the text package's compact.go).
//
// Two rules keep the disruption bounded:
//
//   - Only QUIET rows are compacted: those whose last write is at or below the
//     sequence the text delta log has been pruned through. That window is
//     WithTextDeltaRetention (an hour by default), and it is exactly the point
//     where the server has already stopped being able to send such a client a
//     cheap delta. A client that reads the row after this gets the compacted
//     document and carries on; one still holding pre-compaction state keeps
//     working too, until it tries to write text anchored to something purged —
//     then it is refused with CodeTextStateStale and re-applies its edit against
//     fresh state.
//   - The row is only rewritten when compaction frees at least minPurged items,
//     so a pass over documents with nothing to reclaim writes nothing and costs
//     one decode per quiet text row.
//
// The rewrite lands at a fresh sequence, so every client re-fetches the field
// once — the price of the reclaim. It deliberately does NOT notify: the visible
// text is unchanged, so there is nothing to poke anyone about, and the new row
// rides along on the next pull the client makes anyway.

// WithTextCompaction enables collaborative-text compaction in the garbage
// collection pass (it does nothing without WithGC). minPurged is how many
// tombstoned characters a document must give up before its row is rewritten;
// below that the reclaim does not pay for a pull of the whole field by every
// client. A value <= 0 leaves compaction off, which is the default.
//
// Compaction is not free of consequence, which is why it is opt-in: it is the
// one thing in the engine that can refuse a client's text write outright (see
// CodeTextStateStale), and the recovery costs that client the marks it had
// minted on top of state the server dropped. Enable it for workspaces whose
// documents are edited over months; leave it off if documents are short-lived.
func WithTextCompaction(minPurged int) Option {
	return func(s *Server) error {
		if s.gc == nil {
			s.gc = &gcLoop{}
		}
		s.gc.textCompaction = minPurged
		return nil
	}
}

// compactText runs one workspace's compaction pass inside the GC write
// transaction. throughSeq is the sequence the delta log has been pruned
// through: rows written after it are still within the window where clients
// catch up cheaply, and are left alone. Returns how many rows were rewritten.
func (s *Server) compactText(tx storage.WriteTx, throughSeq uint64) (int, error) {
	minPurged := s.gc.textCompaction
	if minPurged <= 0 || throughSeq == 0 {
		return 0, nil
	}
	entities := s.catalogPtr().entities
	names := make([]string, 0, len(entities))
	for name, info := range entities {
		if len(info.textFields) > 0 {
			names = append(names, name)
		}
	}
	sort.Strings(names)

	var seq uint64
	rewritten := 0
	for _, name := range names {
		info := entities[name]
		// Collect first: the scan and the rewrites share one transaction, and an
		// adapter is not required to tolerate writes during iteration.
		var quiet []storage.Row
		if err := tx.ScanEntity(info.name, func(r storage.Row) error {
			if r.Version <= throughSeq {
				quiet = append(quiet, r)
			}
			return nil
		}); err != nil {
			return rewritten, err
		}
		for _, row := range quiet {
			done, err := s.compactRow(tx, info, row, minPurged, &seq)
			if err != nil {
				return rewritten, err
			}
			if done {
				rewritten++
			}
		}
	}
	return rewritten, nil
}

// compactRow compacts every text field of one row, rewriting it when the
// reclaim clears the threshold. seq is allocated lazily and shared by every row
// the pass rewrites: they are one maintenance event, and one sequence means one
// page of changes rather than one per row.
func (s *Server) compactRow(
	tx storage.WriteTx,
	info *entityInfo,
	row storage.Row,
	minPurged int,
	seq *uint64,
) (bool, error) {
	msg, err := info.decode(row.Value)
	if err != nil {
		// A row that does not decode is not compaction's problem to report; the
		// write paths already reject anything that could have produced it.
		return false, nil
	}
	floors := make(map[string][]byte, len(info.textFields))
	for _, tf := range info.textFields {
		b := chainGetBytes(msg.ProtoReflect(), tf.chain)
		if len(b) == 0 {
			continue
		}
		d, err := text.Decode(b, "")
		if err != nil {
			continue
		}
		// Items and mark ops count the same towards the threshold: it asks
		// whether the reclaim earned a row rewrite and a sequence number, and
		// either kind shrinks the row.
		if items, marks := d.Compact(); items+marks < minPurged {
			continue
		}
		chainSetBytes(msg.ProtoReflect(), tf.chain, d.Encode())
		floors[tf.path] = text.EncodeVV(d.VersionVector())
	}
	if len(floors) == 0 {
		return false, nil
	}
	value, err := proto.Marshal(msg)
	if err != nil {
		return false, err
	}
	if *seq == 0 {
		if *seq, err = tx.NextSeq(); err != nil {
			return false, err
		}
	}
	for path, vv := range floors {
		if err := tx.PutTextFloor(info.name, row.Key, path, vv); err != nil {
			return false, err
		}
		// A purge removes items, which no fragment can express: a client folding
		// the delta log across this sequence would keep text the document no
		// longer has. Gap the compacted fields so any span covering this write is
		// served as full state. Fields left alone record nothing and still ship
		// as "unchanged".
		if err := info.putTextDelta(tx, row.Key, path, *seq, nil); err != nil {
			return false, err
		}
	}
	// Every site that writes a row refreshes its index entries, without
	// exception. Compaction rewrites only text fields, which can never BE
	// index components, so the entries it recomputes are byte-identical today
	// — the call is here so that the invariant is "a row write refreshes the
	// indexes" rather than "a row write refreshes the indexes unless someone
	// reasoned it need not", which is the version that rots when what
	// compaction rewrites changes.
	if err := info.updateRowIndexes(tx, s.limits, row.Key, msg); err != nil {
		return false, err
	}
	// Carry the row's recorded actor through unchanged. Compaction purges
	// tombstones from a document; it makes no semantic change to the row, so
	// stamping the maintenance pass here would erase who last wrote the row and
	// tell every feed consumer that the engine did.
	return true, tx.PutRow(info.name, row.Key, value, *seq, row.Actor)
}
