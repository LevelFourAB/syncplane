package server

import (
	"fmt"
	"sort"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/levelfourab/syncplane/engine/internal/lww"
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
		ref, err := textFieldRefByPath(dst.ProtoReflect().Descriptor(), path)
		if err != nil {
			return err
		}
		var curDoc *text.Doc
		if cur != nil {
			if curBytes := ref.bytes(cur.ProtoReflect()); len(curBytes) > 0 {
				if curDoc, err = text.Decode(curBytes, ""); err != nil {
					return err
				}
			}
		}
		var ancestorBytes []byte
		if base != nil {
			ancestorBytes = ref.bytes(base.ProtoReflect())
		} else if cur != nil {
			ancestorBytes = ref.bytes(cur.ProtoReflect())
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
		if err := ref.set(dst.ProtoReflect(), doc.Encode()); err != nil {
			return err
		}
	}
	return nil
}

// textFieldRef addresses one text document within a row-shaped message: the
// resolved steps of an instance path, item keys decoded where a step crosses a
// keyed list or map.
type textFieldRef struct {
	path  string
	steps []textRefStep
}

type textRefStep struct {
	fd protoreflect.FieldDescriptor
	// mapKey addresses the entry of a map step.
	mapKey protoreflect.MapKey
	// itemKey (encoded form) and keyFields address the element of a keyed
	// list step.
	itemKey   string
	keyFields []protoreflect.FieldDescriptor
}

// textFieldRefByPath resolves a dotted path — instance form, with item keys
// for the keyed lists and maps it crosses — to a reference to the named text
// field, rejecting anything that does not land on a
// (syncplane.v1.field_behavior).text field.
func textFieldRefByPath(desc protoreflect.MessageDescriptor, path string) (textFieldRef, error) {
	badPath := func() (textFieldRef, error) {
		return textFieldRef{}, fmt.Errorf("server: %s has no text field %q", desc.FullName(), path)
	}
	segs := lww.SplitPath(path)
	ref := textFieldRef{path: path}
	cur := desc
	for i := 0; i < len(segs); i++ {
		fd := cur.Fields().ByName(protoreflect.Name(segs[i]))
		if fd == nil {
			return badPath()
		}
		last := i == len(segs)-1
		step := textRefStep{fd: fd}
		switch {
		case fd.IsMap():
			if last || fd.MapValue().Kind() != protoreflect.MessageKind {
				return badPath()
			}
			i++
			mk, err := lww.DecodeMapKey(fd, segs[i])
			if err != nil {
				return badPath()
			}
			step.mapKey = mk
			cur = fd.MapValue().Message()
			last = i == len(segs)-1
		case fd.IsList():
			step.keyFields = lww.KeyFieldsFor(fd)
			if last || len(step.keyFields) == 0 {
				return badPath()
			}
			i++
			step.itemKey = segs[i]
			cur = fd.Message()
			last = i == len(segs)-1
		case fd.Kind() == protoreflect.MessageKind:
			cur = fd.Message()
		default:
			if !last || !lww.IsText(fd) {
				return badPath()
			}
		}
		if last && !lww.IsText(fd) {
			return badPath()
		}
		if last && (fd.IsMap() || fd.IsList()) {
			return badPath()
		}
		ref.steps = append(ref.steps, step)
	}
	return ref, nil
}

// bytes returns the leaf document bytes in msg, or nil when any addressed step
// — an intermediate message, element or entry — is absent.
func (r textFieldRef) bytes(msg protoreflect.Message) []byte {
	cur := msg
	for i, st := range r.steps {
		if i == len(r.steps)-1 {
			return cur.Get(st.fd).Bytes()
		}
		if !cur.Has(st.fd) {
			return nil
		}
		switch {
		case st.fd.IsMap():
			mm := cur.Get(st.fd).Map()
			if !mm.Has(st.mapKey) {
				return nil
			}
			cur = mm.Get(st.mapKey).Message()
		case st.fd.IsList():
			elem, ok := findListElem(cur.Get(st.fd).List(), st.keyFields, st.itemKey)
			if !ok {
				return nil
			}
			cur = elem
		default:
			cur = cur.Get(st.fd).Message()
		}
	}
	return nil
}

// set writes the leaf document bytes in msg, materializing singular
// intermediates. A missing addressed element or entry is an error rather than
// created: an element is more than its key, and a caller that means to create
// one puts it in the row.
func (r textFieldRef) set(msg protoreflect.Message, value []byte) error {
	cur := msg
	for i, st := range r.steps {
		if i == len(r.steps)-1 {
			cur.Set(st.fd, protoreflect.ValueOfBytes(value))
			return nil
		}
		switch {
		case st.fd.IsMap():
			mm := cur.Mutable(st.fd).Map()
			if !mm.Has(st.mapKey) {
				return fmt.Errorf("server: text field %q addresses a map entry the value does not carry", r.path)
			}
			cur = mm.Mutable(st.mapKey).Message()
		case st.fd.IsList():
			elem, ok := findListElem(cur.Mutable(st.fd).List(), st.keyFields, st.itemKey)
			if !ok {
				return fmt.Errorf("server: text field %q addresses a list element the value does not carry", r.path)
			}
			cur = elem
		default:
			cur = cur.Mutable(st.fd).Message()
		}
	}
	return nil
}

// findListElem locates a keyed list element by encoded identity.
func findListElem(
	list protoreflect.List,
	keyFields []protoreflect.FieldDescriptor,
	itemKey string,
) (protoreflect.Message, bool) {
	for i := 0; i < list.Len(); i++ {
		elem := list.Get(i).Message()
		if lww.EncodeItemKey(elem, keyFields) == itemKey {
			return elem, true
		}
	}
	return nil, false
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
// convention is the model. Reuse across calls is safe because Put/Patch read
// the stored row inside the serialized write transaction (see
// ApplyTextStrings).
func WithTextPeer(peer string) WriteOption {
	return func(o *writeOpts) { o.peer = peer }
}

// WithExpectedVersion makes a Put a compare-and-swap: the stored row's version
// must equal version or the write is rejected as a conflict (CodeConflict, an
// *Rejection AsRejection recovers). An absent or deleted row reads as version
// 0, so WithExpectedVersion(0) means "only if the row does not exist".
//
// Beyond conflict detection it is how a server-side write states its intent to
// an entity that restricts updates or creates: an unguarded PUT could land
// either way, so it is refused where either policy is TRUSTED_ONLY, while
// version 0 is provably a create and a non-zero version provably an update (see
// CreatePolicy and UpdatePolicy).
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
		for _, ti := range tf.instances(refl) {
			if text.IsPlainString(ti.bytes()) {
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
		for _, ti := range tf.instances(refl) {
			value := ti.bytes()
			if !text.IsPlainString(value) {
				continue
			}
			if mask != nil && !maskCovers(mask, ti.path) {
				ti.clear()
				continue
			}
			s, err := text.PlainString(value)
			if err != nil {
				return nil, fmt.Errorf("server: entity %q: text field %q: %w", info.name, ti.path, err)
			}
			if found == nil {
				found = make(map[string]string, len(info.textFields))
			}
			found[ti.path] = s
			ti.clear()
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
