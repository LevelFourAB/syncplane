package server

import (
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/levelfourab/syncplane/engine/internal/lww"
	"github.com/levelfourab/syncplane/engine/text"
)

// ApplyTextStrings rewrites each named collaborative-text field of dst from a
// plain string. texts maps dotted proto paths to the desired visible text.
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
		chain, err := textFieldChainByPath(dst.ProtoReflect().Descriptor(), path)
		if err != nil {
			return err
		}
		var curDoc *text.Doc
		if cur != nil {
			if curBytes := chainGetBytes(cur.ProtoReflect(), chain); len(curBytes) > 0 {
				if curDoc, err = text.Decode(curBytes, ""); err != nil {
					return err
				}
			}
		}
		var ancestorBytes []byte
		if base != nil {
			ancestorBytes = chainGetBytes(base.ProtoReflect(), chain)
		} else if cur != nil {
			ancestorBytes = chainGetBytes(cur.ProtoReflect(), chain)
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
		chainSetBytes(dst.ProtoReflect(), chain, doc.Encode())
	}
	return nil
}

// textFieldChainByPath resolves a dotted path to a text field's descriptor
// chain, rejecting anything that is not a (syncplane.v1.field_behavior).text
// field.
func textFieldChainByPath(desc protoreflect.MessageDescriptor, path string) ([]protoreflect.FieldDescriptor, error) {
	segs := strings.Split(path, ".")
	chain := make([]protoreflect.FieldDescriptor, 0, len(segs))
	cur := desc
	for i, seg := range segs {
		fd := cur.Fields().ByName(protoreflect.Name(seg))
		if fd == nil {
			return nil, fmt.Errorf("server: %s has no text field %q", desc.FullName(), path)
		}
		chain = append(chain, fd)
		if i < len(segs)-1 {
			if fd.IsList() || fd.IsMap() || fd.Kind() != protoreflect.MessageKind {
				return nil, fmt.Errorf("server: %s has no text field %q", desc.FullName(), path)
			}
			cur = fd.Message()
			continue
		}
		if !lww.IsText(fd) {
			return nil, fmt.Errorf("server: %s has no text field %q", desc.FullName(), path)
		}
	}
	return chain, nil
}

// WriteOption customizes EntityTx.Put and EntityTx.Patch.
type WriteOption func(*writeOpts)

type writeOpts struct {
	texts map[string]string
	peer  string
}

// WithTextString sets a collaborative-text field from a plain string instead
// of encoded document bytes: the string is diffed against the stored document
// under the WithTextPeer identity, so the write merges with concurrent edits
// instead of replacing them. field is the dotted proto path; repeat the
// option for multiple fields. An empty string deletes all visible text. On
// Patch the field is added to the mask automatically.
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
