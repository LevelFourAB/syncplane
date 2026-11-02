package client

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"sort"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/levelfourab/syncplane/engine/internal/textpath"
	"github.com/levelfourab/syncplane/engine/text"
)

// applyTexts sets each collaborative-text field of dst that texts names from
// its string. Each string is diffed against the document cur holds at that
// path (nil cur, or an absent document, is an empty document) and dst gets the
// full resulting document, which the server unions with whatever is stored
// by the time the write lands.
//
// Every field gets a fresh peer. The base was read outside the write
// transaction and may be stale by the time the write lands — an earlier
// in-flight write of this process, a lagging read replica — and a peer reused
// over a base that lacks the peer's own earlier operations mints duplicate
// ids (see text.ID). A fresh peer costs one peer-table entry per write per
// document instead.
func applyTexts(dst, cur protoreflect.Message, texts map[string]string, clientID string) error {
	paths := make([]string, 0, len(texts))
	for p := range texts {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, path := range paths {
		ref, err := textpath.ParseRef(dst.Descriptor(), path)
		if err != nil {
			return fmt.Errorf("client: %w", err)
		}
		var base []byte
		if cur != nil {
			base = ref.Bytes(cur)
		}
		if text.IsPlainString(base) {
			return fmt.Errorf("client: text field %q holds a plain-string value, not a document", path)
		}
		doc, err := text.Decode(base, clientID+":"+randomPeerSuffix())
		if err != nil {
			return fmt.Errorf("client: text field %q: %w", path, err)
		}
		if doc.PlainText() != texts[path] {
			if err := doc.SetString(texts[path]); err != nil {
				return fmt.Errorf("client: text field %q: %w", path, err)
			}
		}
		if err := ref.Set(dst, doc.Encode()); err != nil {
			return fmt.Errorf("client: %w", err)
		}
	}
	return nil
}

func randomPeerSuffix() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return base64.RawURLEncoding.EncodeToString(b[:])
}
