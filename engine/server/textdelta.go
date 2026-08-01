package server

import (
	"bytes"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/text"
)

// Downstream delta state: shipping a text edit to readers as the fragment it
// added rather than as the whole document.
//
// The engine cannot compute that fragment at read time — it would need the
// document as it stood at each reader's cursor, and only the current row is
// stored. So every write of a text-bearing row appends to a per-(row, field)
// delta log keyed by the write's sequence, and a pull folds the entries above
// the reader's cursor into one fragment. Storage, bootstrap chunks, the Changes
// feed and every read path keep seeing whole documents; the log is a wire-size
// optimization sitting beside them.
//
// The log is correct only if it accounts for EVERY write in a span, so a write
// whose effect is not expressible as a fragment records an empty gap marker
// instead of nothing (see storage.WriteTx.PutFieldDelta). A span containing one
// is served as full state, which is always right and never wrong — the whole
// mechanism degrades to Stage-4 behavior rather than to divergence.

// fragmentSizeRatio is how much smaller than the field's document the folded
// fragment must be before a pull ships delta state instead of the value. It
// matches the client's push-side ratio (client/src/sync/textdelta.ts) so both
// directions make the same call about when a delta stops paying for itself.
// Not byte-observable across languages: only the sender decides what to send.
const fragmentSizeRatio = 0.5

// textSnapshot captures the current bytes of every text field of msg, keyed by
// dotted path. Callers take it BEFORE merging into msg — the merge replaces
// field values rather than mutating them, so the captured slices stay valid as
// the pre-write state. A nil message (an absent row) snapshots to nil.
func (info *entityInfo) textSnapshot(msg proto.Message) map[string][]byte {
	if msg == nil || len(info.textFields) == 0 {
		return nil
	}
	out := make(map[string][]byte, len(info.textFields))
	for _, tf := range info.textFields {
		if b := chainGetBytes(msg.ProtoReflect(), tf.chain); len(b) > 0 {
			out[tf.path] = b
		}
	}
	return out
}

// recordTextDeltas logs how this write moved each text field, at the write's
// sequence. next is the message about to be stored; prev is the snapshot taken
// before the merge (nil for a row this write creates).
//
// A field whose bytes did not move records nothing: "no entry in the span" is
// how a pull learns the reader can keep what it holds. Anything else records a
// fragment, or a gap marker when one would not help — the new state is not
// built on the old (a cleared or replaced document), or the fragment is no
// smaller than the document it describes.
func (info *entityInfo) recordTextDeltas(
	tx storage.WriteTx,
	key string,
	prev map[string][]byte,
	next protoreflect.Message,
	seq uint64,
) error {
	for _, tf := range info.textFields {
		before, after := prev[tf.path], chainGetBytes(next, tf.chain)
		if bytes.Equal(before, after) {
			continue
		}
		frag := textFragmentOf(after, before)
		if err := info.putTextDelta(tx, key, tf.path, seq, frag); err != nil {
			return err
		}
	}
	return nil
}

// recordTextDeltaGaps marks every text field of a row as un-deltaable at seq.
// Used by DELETE: the row's documents go away wholesale, which no fragment can
// express. (The tombstone itself tells readers to drop the row, so the marker
// only matters for a reader whose span also covers a later re-creation.)
func (info *entityInfo) recordTextDeltaGaps(tx storage.WriteTx, key string, seq uint64) error {
	for _, tf := range info.textFields {
		if err := info.putTextDelta(tx, key, tf.path, seq, nil); err != nil {
			return err
		}
	}
	return nil
}

// putTextDelta writes one log entry, folding it into whatever is already logged
// at this sequence. A batch mutation shares ONE sequence across several cruds,
// so two writes to the same field can land at the same seq; the entry has to
// describe their combined effect or a reader folding the span would miss the
// first. Union is the right combiner here for the same reason it is everywhere
// else in this CRDT. An existing gap marker stays a gap marker.
func (info *entityInfo) putTextDelta(tx storage.WriteTx, key, field string, seq uint64, frag []byte) error {
	existing, complete, err := tx.FieldDeltas(info.name, key, field, seq-1)
	if err != nil {
		return err
	}
	switch {
	case !complete:
		// Something already logged at this sequence cannot be folded (a gap
		// marker from an earlier crud of the same batch). Stay a gap.
		frag = nil
	case len(existing) > 0 && len(frag) > 0:
		merged, mergeErr := text.MergeFragments(existing[0], frag)
		if mergeErr != nil {
			frag = nil
		} else {
			frag = merged
		}
	case len(existing) > 0:
		frag = nil
	}
	return tx.PutFieldDelta(info.name, key, field, seq, frag)
}

// textFragmentOf returns the fragment carrying what after holds beyond before,
// or nil (a gap marker) when delta state cannot stand in for the new document:
// the two states diverged rather than one extending the other, either side
// fails to decode, or the fragment is no smaller than the document itself
// (a wholesale rewrite, where full state is both simpler and smaller).
func textFragmentOf(after, before []byte) []byte {
	if len(after) == 0 {
		return nil // the field was cleared; a fragment can only add
	}
	next, err := text.Decode(after, "")
	if err != nil {
		return nil
	}
	var base *text.Doc
	if len(before) > 0 {
		if base, err = text.Decode(before, ""); err != nil {
			return nil
		}
	}
	frag, err := text.EncodeFragment(next, base)
	if err != nil || len(frag) >= len(after) {
		return nil
	}
	return frag
}
