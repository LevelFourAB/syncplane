package server

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"
	"sort"

	"google.golang.org/protobuf/proto"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
)

// subscriptionHash canonically hashes a subscription set: subs sort by
// (entity, deterministic-marshal bytes), then FNV-1a 64 runs over the
// length-prefixed marshaled messages. The empty set hashes to 0, which is
// what legacy cursors (no sub_hash field) decode to — full sync stays
// compatible. The hash only needs to be stable per server process: a
// spurious mismatch merely costs a re-bootstrap.
//
// The follow spec is DELIBERATELY excluded from the hash: a follow-spec change
// must not trigger a base-scope delta resync. It is reconciled instead by the
// closure diff against the cursor's stored Cursor.closure key-set, which is
// exact even across spec changes.
//
// The WINDOW is included, unlike follow — the asymmetry is deliberate. A
// membership diff can only ship rows the server can see are in or out of the
// new set at or below the cursor's seq; widening or dropping a window needs
// rows the client never held, which only a delta resync produces. Follow gets
// away without it because a widened follow spec reaches rows through holders
// the client already has.
//
// Existing clients send no window, so their subscriptions marshal exactly as
// before and every cursor issued so far still hashes identically: upgrading
// the server does not re-bootstrap anybody.
func subscriptionHash(subs []*syncplanev1.Subscription) (uint64, error) {
	if len(subs) == 0 {
		return 0, nil
	}
	type item struct {
		entity string
		raw    []byte
	}
	mo := proto.MarshalOptions{Deterministic: true}
	items := make([]item, 0, len(subs))
	for _, sub := range subs {
		// Hash the base scope plus the window; clone-and-clear follow so a
		// follow change leaves the hash stable.
		base := sub
		if sub.GetFollow() != nil {
			base = &syncplanev1.Subscription{
				Entity:    sub.GetEntity(),
				Predicate: sub.GetPredicate(),
				Window:    sub.GetWindow(),
			}
		}
		raw, err := mo.Marshal(base)
		if err != nil {
			return 0, err
		}
		items = append(items, item{entity: sub.GetEntity(), raw: raw})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].entity != items[j].entity {
			return items[i].entity < items[j].entity
		}
		return bytes.Compare(items[i].raw, items[j].raw) < 0
	})
	h := fnv.New64a()
	var lenBuf [binary.MaxVarintLen64]byte
	for _, it := range items {
		n := binary.PutUvarint(lenBuf[:], uint64(len(it.raw)))
		h.Write(lenBuf[:n])
		h.Write(it.raw)
	}
	return h.Sum64(), nil
}
