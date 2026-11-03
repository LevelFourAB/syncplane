package server

import (
	"fmt"
	"sort"

	"google.golang.org/protobuf/proto"

	"github.com/levelfourab/syncplane/engine/internal/textpath"
	"github.com/levelfourab/syncplane/engine/text"
)

// ApplyTextStrings rewrites each named collaborative-text field of dst from a
// plain string. texts maps dotted proto paths to the desired visible text; a
// document inside a keyed list or map element is named by its instance path
// ("items.<key>.body"), and dst must already carry the addressed element —
// the write cannot invent an element from its key alone.
//
// Each string is diffed (multi-hunk, see text.SetString) against the field's
// document in base — the last state the writer observed — falling back to cur
// and then to an empty document; the result is then unioned with cur's
// document, so concurrent edits the writer never saw survive. An empty string
// deletes all visible text (concurrent inserts still survive the union).
//
// peer is the writer identity the diff's operations are minted under, and the
// caller owes the text package's peer-uniqueness contract: two writers that
// can run concurrently must never share one. Reusing a peer across calls is
// sound exactly when cur is read inside the same serialized write transaction
// as the write — then cur always carries the peer's earlier operations, and
// observing cur's version before diffing keeps fresh counters past them. The
// connector's `connector:<writerID>` identity is the model.
func ApplyTextStrings(dst, base, cur proto.Message, texts map[string]string, peer string) error {
	if len(texts) == 0 {
		return nil
	}
	if peer == "" {
		return fmt.Errorf("server: text strings require a writer peer")
	}
	// Deterministic order so multi-field writes allocate counters (and fail)
	// identically across runs.
	paths := make([]string, 0, len(texts))
	for p := range texts {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, path := range paths {
		s := texts[path]
		ref, err := textpath.ParseRef(dst.ProtoReflect().Descriptor(), path)
		if err != nil {
			return fmt.Errorf("server: %w", err)
		}
		var curDoc *text.Doc
		if cur != nil {
			if curBytes := ref.Bytes(cur.ProtoReflect()); len(curBytes) > 0 {
				if curDoc, err = text.Decode(curBytes, ""); err != nil {
					return err
				}
			}
		}
		var ancestorBytes []byte
		if base != nil {
			ancestorBytes = ref.Bytes(base.ProtoReflect())
		} else if cur != nil {
			ancestorBytes = ref.Bytes(cur.ProtoReflect())
		}
		doc, err := text.Decode(ancestorBytes, peer)
		if err != nil {
			return err
		}
		if curDoc != nil {
			doc.ObserveVersion(curDoc.VersionVector())
		}
		if doc.PlainText() != s {
			if err := doc.SetString(s); err != nil {
				return err
			}
		}
		if curDoc != nil {
			if err := doc.MergeDocs(curDoc); err != nil {
				return err
			}
		}
		if err := ref.Set(dst.ProtoReflect(), doc.Encode()); err != nil {
			return fmt.Errorf("server: %w", err)
		}
	}
	return nil
}

// WriteOption customizes EntityTx.Put and EntityTx.Patch.
type WriteOption func(*writeOpts)

type writeOpts struct {
	texts    map[string]string
	peer     string
	expected *uint64
}

// WithTextString sets a collaborative-text field from a plain string instead
// of encoded document bytes: the string is diffed against the stored document
// under the WithTextPeer identity, so the write merges with concurrent edits
// instead of replacing them. field is the dotted proto path — for a document
// inside a keyed list or map element, the instance path with the element key
// ("notes.<key>.body"), and the written message must carry that element.
// Repeat the option for multiple fields. An empty string deletes all visible
// text. On Patch the field is added to the mask automatically.
//
// Requires WithTextPeer in the same call — the engine never invents a writer
// identity, because peer uniqueness is the caller's contract (see text.ID).
func WithTextString(field, value string) WriteOption {
	return func(o *writeOpts) {
		if o.texts == nil {
			o.texts = make(map[string]string)
		}
		o.texts[field] = value
	}
}

// WithTextPeer names the text writer identity WithTextString operations are
// minted under. Two writers that can run concurrently must never share a
// peer; a stable per-writer name like the connector's `connector:<writerID>`
// convention is the model. Reuse across calls is safe when every write of the
// peer to a row goes through a server that stores that row: Put/Patch read
// the stored row inside the serialized write transaction (see
// ApplyTextStrings) and mint past the ids it holds. A server that holds only
// part of another server's rows (a replica synced with subscriptions) does not
// see the ids the peer minted in a row it does not hold, so it can mint them
// again.
func WithTextPeer(peer string) WriteOption {
	return func(o *writeOpts) { o.peer = peer }
}

// WithExpectedVersion makes a Put a compare-and-swap: the stored row's version
// must equal version or the write is rejected as a conflict (CodeConflict, an
// *Rejection AsRejection recovers). An absent or deleted row reads as version
// 0, so WithExpectedVersion(0) means "only if the row does not exist".
//
// Beyond conflict detection it is how a write states its intent to an entity
// whose access rules admit creates and updates apart: version 0 is provably a
// create and a non-zero version provably an update, so a write the rules do
// not admit as that operation is refused before the write transaction opens.
// An unguarded PUT is a create or an update depending on the stored row.
//
// Ignored by Patch, which takes its expected version as an argument.
func WithExpectedVersion(version uint64) WriteOption {
	return func(o *writeOpts) { o.expected = &version }
}

func collectWriteOpts(opts []WriteOption) (writeOpts, error) {
	var o writeOpts
	for _, opt := range opts {
		opt(&o)
	}
	if len(o.texts) > 0 && o.peer == "" {
		return o, fmt.Errorf("server: WithTextString requires WithTextPeer")
	}
	return o, nil
}

// hasPlainString reports whether any text document of msg carries a
// plain-string value. Separate from takePlainStrings so Patch can skip cloning
// the message on the ordinary write, which carries none.
func hasPlainString(info *entityInfo, msg proto.Message) bool {
	refl := msg.ProtoReflect()
	for _, tf := range info.textFields {
		for _, ti := range tf.Instances(refl) {
			if text.IsPlainString(ti.Bytes()) {
				return true
			}
		}
	}
	return false
}

// takePlainStrings moves every plain-string value in msg's text fields into the
// map to hand ApplyTextStrings, keyed by instance path, clearing the field so
// the value can never reach a merge. It returns o.texts unchanged when msg
// carries none, and errors when it carries one and o names no peer — resolving
// mints edits, and the engine invents no identity to mint them under.
//
// An explicit WithTextString for the same path wins: the option states the
// intent of this write, while a value in the message came in with whatever the
// caller decoded.
//
// mask, when non-nil, limits it to the fields that write covers; a value
// outside is cleared rather than resolved, which keeps a PATCH writing what its
// mask names. Pass nil for a PUT, which carries no mask.
func takePlainStrings(
	info *entityInfo,
	msg proto.Message,
	mask []string,
	o writeOpts,
) (map[string]string, error) {
	var found map[string]string
	refl := msg.ProtoReflect()
	for _, tf := range info.textFields {
		for _, ti := range tf.Instances(refl) {
			value := ti.Bytes()
			if !text.IsPlainString(value) {
				continue
			}
			if mask != nil && !maskCovers(mask, ti.Path) {
				ti.Clear()
				continue
			}
			s, err := text.PlainString(value)
			if err != nil {
				return nil, fmt.Errorf("server: entity %q: text field %q: %w", info.name, ti.Path, err)
			}
			if found == nil {
				found = make(map[string]string, len(info.textFields))
			}
			found[ti.Path] = s
			ti.Clear()
		}
	}
	if found == nil {
		return o.texts, nil
	}
	if o.peer == "" {
		return nil, fmt.Errorf(
			"server: entity %q carries a plain-string text value, which requires WithTextPeer to resolve",
			info.name)
	}
	for path, s := range o.texts {
		found[path] = s
	}
	return found, nil
}
