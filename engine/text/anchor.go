package text

import (
	"encoding/binary"
	"fmt"
)

// EncodeAnchor serializes an anchor for storage outside its document (any
// bytes field, a presence broadcast, an undo entry). The peer is spelled out
// in full — standalone anchors cannot reference a document's peer table.
//
//	byte   kind (AnchorKind)
//	if AnchorBefore/AnchorAfter: varint peerLen, peer bytes, varint counter
func EncodeAnchor(a Anchor) []byte {
	buf := []byte{byte(a.Kind)}
	if a.Kind == AnchorBefore || a.Kind == AnchorAfter {
		buf = binary.AppendUvarint(buf, uint64(len(a.Item.Peer)))
		buf = append(buf, a.Item.Peer...)
		buf = binary.AppendUvarint(buf, a.Item.Counter)
	}
	return buf
}

// DecodeAnchor parses an EncodeAnchor payload. It reads through the document
// decoder's primitives, so a standalone anchor is held to the same limits as
// one inside a document — the varint ceiling and valid UTF-8 — and fails with
// the same messages as the TypeScript mirror.
func DecodeAnchor(data []byte) (Anchor, error) {
	if len(data) == 0 {
		return Anchor{}, fmt.Errorf("text: empty anchor")
	}
	r := &decoder{buf: data}
	k, err := r.byte()
	if err != nil {
		return Anchor{}, err
	}
	kind := AnchorKind(k)
	switch kind {
	case AnchorDocStart, AnchorDocEnd:
	case AnchorBefore, AnchorAfter:
		peer, err := r.str()
		if err != nil {
			return Anchor{}, err
		}
		c, err := r.uvarint()
		if err != nil {
			return Anchor{}, err
		}
		if r.off != len(r.buf) {
			return Anchor{}, fmt.Errorf("text: trailing bytes in anchor")
		}
		return Anchor{Kind: kind, Item: ID{Peer: peer, Counter: c}}, nil
	default:
		return Anchor{}, fmt.Errorf("text: invalid anchor kind %d", k)
	}
	if r.off != len(r.buf) {
		return Anchor{}, fmt.Errorf("text: trailing bytes in anchor")
	}
	return Anchor{Kind: kind}, nil
}
