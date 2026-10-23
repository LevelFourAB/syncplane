package server

import (
	"bytes"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

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

// textSnapshot captures the current bytes of every text document of msg, keyed
// by instance path — one entry per element for a field under a keyed list or
// map. Callers take it BEFORE merging into msg — the merge replaces field
// values rather than mutating them, so the captured slices stay valid as the
// pre-write state. A nil message (an absent row) snapshots to nil.
func (info *entityInfo) textSnapshot(msg proto.Message) map[string][]byte {
	if msg == nil || len(info.textFields) == 0 {
		return nil
	}
	out := make(map[string][]byte, len(info.textFields))
	for _, tf := range info.textFields {
		for _, ti := range tf.instances(msg.ProtoReflect()) {
			if b := ti.bytes(); len(b) > 0 {
				if _, ok := out[ti.path]; !ok {
					out[ti.path] = b
				}
			}
		}
	}
	return out
}

// recordTextDeltas logs how this write moved each text document, at the
// write's sequence. next is the message about to be stored; prev is the
// snapshot taken before the merge (nil for a row this write creates).
//
// A document whose bytes did not move records nothing: "no entry in the span"
// is how a pull learns the reader can keep what it holds. Anything else
// records a fragment, or a gap marker when one would not help — the new state
// is not built on the old (a cleared or replaced document), or the fragment is
// no smaller than the document it describes. A document prev holds and next
// does not (a cleared field, or a dropped element) records a gap: no fragment
// can express a removal, and a reader whose span also covers a later
// re-creation must be served full state rather than fold across it.
func (info *entityInfo) recordTextDeltas(
	tx storage.WriteTx,
	key string,
	prev map[string][]byte,
	next protoreflect.Message,
	seq uint64,
) error {
	seen := make(map[string]bool, len(info.textFields))
	for _, tf := range info.textFields {
		for _, ti := range tf.instances(next) {
			if seen[ti.path] {
				continue
			}
			seen[ti.path] = true
			before, after := prev[ti.path], ti.bytes()
			if bytes.Equal(before, after) {
				continue
			}
			frag := textFragmentOf(after, before)
			if err := info.putTextDelta(tx, key, ti.path, seq, frag); err != nil {
				return err
			}
		}
	}
	var dropped []string
	for path := range prev {
		if !seen[path] {
			dropped = append(dropped, path)
		}
	}
	sort.Strings(dropped)
	for _, path := range dropped {
		if err := info.putTextDelta(tx, key, path, seq, nil); err != nil {
			return err
		}
	}
	return nil
}

// maxTextCounterStep is how far one write may move the highest counter of a
// text document. Every edit takes a counter above every counter the document
// has seen, and none may pass 2^53−1, so a document whose counters reach that
// ceiling can never be edited again. The step is far above what honest edits
// move — one counter per inserted code point or mark — and makes reaching the
// ceiling take millions of writes rather than one.
const maxTextCounterStep = 1 << 32

// checkTextCounterSteps refuses a write that moves the highest counter of any
// text document of next more than maxTextCounterStep above the same document in
// prev (zero for a document prev does not hold). next holds the documents about
// to be stored, which every write path has decoded and encoded again, so their
// headers are exact.
func (info *entityInfo) checkTextCounterSteps(prev map[string][]byte, next protoreflect.Message) error {
	for _, tf := range info.textFields {
		for _, ti := range tf.instances(next) {
			after := ti.bytes()
			before := prev[ti.path]
			if len(after) == 0 || bytes.Equal(before, after) {
				continue
			}
			hi, err := text.MaxCounter(after)
			if err != nil {
				return fmt.Errorf("server: entity %q text field %q: %w", info.name, ti.path, err)
			}
			lo, err := text.MaxCounter(before)
			if err != nil {
				return fmt.Errorf("server: entity %q text field %q: stored document: %w", info.name, ti.path, err)
			}
			if hi > lo && hi-lo > maxTextCounterStep {
				// The step is left out: with the write's own counter it gives the
				// stored document's, which a caller that writes the field may not
				// read.
				return Reject(CodeInvalidArgument, fmt.Sprintf(
					"entity %q text field %q: the write moves the document's counters up by more than %d, the most one write may move them",
					info.name, ti.path, uint64(maxTextCounterStep)))
			}
		}
	}
	return nil
}

// recordTextDeltaGaps marks the named document paths as un-deltaable at seq.
// Used by DELETE with the row's textStatePaths: the documents go away
// wholesale, which no fragment can express. (The tombstone itself tells
// readers to drop the row, so the marker only matters for a reader whose span
// also covers a later re-creation.)
func (info *entityInfo) recordTextDeltaGaps(tx storage.WriteTx, key string, paths []string, seq uint64) error {
	for _, path := range paths {
		if err := info.putTextDelta(tx, key, path, seq, nil); err != nil {
			return err
		}
	}
	return nil
}

// readInputs is what a row's own fields decide about which callers may read it
// and its fields in the access model: its access parents, the values of the
// fields its read rules and the read rules of its guards check against the
// caller's subjects, and whether it matches each such rule's when condition.
// The zero value is a row whose fields decide nothing: one that does not
// exist, or one whose entity's read rules do not read its fields.
type readInputs struct {
	decides bool
	// parents is the key of the row's parent edge (see chainEdge.key), so
	// that two values compare with ==.
	parents string
	// checks holds, for each read rule with a check on the row in rule order,
	// the entity's and then each guard's, the value of its subject field
	// quoted and whether the row matches its when condition, then what within
	// entries on the row's own entity read of it.
	checks string
}

// readInputsOf returns what row's own fields decide about which callers may
// read it. model is nil for a schema without the access model, and row is nil
// for a row that does not exist.
func readInputsOf(model *accessModel, info *entityInfo, row proto.Message) readInputs {
	if model == nil || row == nil || info.access == nil {
		return readInputs{}
	}
	var checked []accessRule
	for _, r := range info.access.readerSets() {
		checked = append(checked, r.checked...)
	}
	if info.access.read.all && !info.access.hidesFields() {
		return readInputs{}
	}
	if len(model.parents[info.name]) == 0 && len(checked) == 0 {
		return readInputs{}
	}
	r := row.ProtoReflect()
	in := readInputs{decides: true, parents: edgeOf(model, info, r).key()}
	if len(checked) > 0 {
		var b strings.Builder
		self := false
		for _, rule := range checked {
			if rule.callerIs != nil {
				b.WriteString(strconv.Quote(r.Get(rule.callerIs).String()))
			}
			if rule.when != nil {
				b.WriteString(strconv.FormatBool(rule.when.Eval(r)))
			}
			self = self || slices.ContainsFunc(rule.within, func(w withinEntry) bool { return w.entity == info.name })
		}
		if self {
			b.WriteString(withinStateOf(model, info, r).key())
		}
		in.checks = b.String()
	}
	return in
}

// recordReadGaps marks every document of next as un-deltaable at seq when the
// write changes what the row's own fields decide about which callers may read
// it. prev is what they decided before the write, and next is the row about to
// be stored.
//
// A reader the write shows the row to for the first time cannot fold a
// fragment into anything, and the log cannot tell it apart from a reader that
// held the row, so every reader whose span covers the write receives the
// documents whole.
func (info *entityInfo) recordReadGaps(
	tx storage.WriteTx,
	model *accessModel,
	key string,
	prev readInputs,
	next proto.Message,
	seq uint64,
) error {
	if len(info.textFields) == 0 || !prev.decides || prev == readInputsOf(model, info, next) {
		return nil
	}
	return info.recordTextDeltaGaps(tx, key, info.textStatePaths(next.ProtoReflect()), seq)
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
