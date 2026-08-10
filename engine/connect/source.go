// Package connect binds the sync engine to an external source of truth — an
// adopter-owned store the engine does not manage (a folder of files, an
// existing database) — and keeps the two converged bidirectionally.
//
// The engine remains the ordering authority: every external change enters
// through the trusted write path (server.Mutate) where it is sequenced and
// conflict-resolved exactly like a client write, and every committed engine
// change flows back out through the change feed (server.Changes). The
// adopter implements one port (Source) over their store; the Connector owns
// the scheduling, the loop-prevention bookkeeping, and crash recovery.
//
// # Scale
//
// A connector is expected to run against many workspaces at once — one per
// connected account — so nothing here runs per workspace. Work is scheduled
// onto a fixed pool of workers (Config.Workers): a commit wakes its workspace
// through the notifier's activity stream, one subscription for the whole
// store, and one sweep discovers workspaces and runs the periodic pass. What
// the process holds is a small record per managed workspace, and what it does
// at once is the size of the pool — which is also what keeps a fleet of
// workspaces from arriving at one rate-limited API together.
//
// # Single instance
//
// Nothing leases a workspace to one connector, and the bookkeeping that keeps
// a write from echoing — cursors and shadows — lives in the connector's own
// State rather than the engine's store, where a second instance would not see
// it. Two processes converging the same workspace both egress to the source,
// so run exactly one instance. A rolling deploy counts: the outgoing process
// converges while the incoming one starts, and Close waiting on in-flight
// passes narrows that window without closing it.
package connect

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/levelfourab/syncplane/engine/text"
)

// Item is one record of the external store, decoded into a registered
// entity message.
type Item struct {
	Msg proto.Message
	// Fingerprint is a cheap stamp of the source's content for this item — a
	// content hash, mtime+size, a DB rowversion. Equal content MUST yield
	// equal fingerprints between List and Write: fingerprint equality is how
	// the connector tells its own echoed writes apart from real concurrent
	// edits.
	Fingerprint string
	// TextFields carries the plain-string values of the entity's
	// collaborative-text fields ((syncplane.v1.field_behavior).text), keyed by
	// top-level proto field name. A source cannot fabricate the encoded
	// document (it has no CRDT history), so it reports the text it holds and
	// the connector applies it as a minimal diff onto the engine's current
	// document — an external edit then MERGES with concurrent client edits
	// instead of clobbering them. Leave the field bytes unset on Msg; the
	// connector fills them. Egress renders the document back to a string via
	// PlainTextField.
	TextFields map[string]string
}

// PlainTextField decodes the collaborative-text field `field` (top-level
// proto name) of an engine message to its visible string — the egress
// rendering a Source.Write uses to serialize a text field natively.
func PlainTextField(msg proto.Message, field string) (string, error) {
	fd := msg.ProtoReflect().Descriptor().Fields().ByName(protoreflect.Name(field))
	if fd == nil || fd.Kind() != protoreflect.BytesKind {
		return "", fmt.Errorf("connect: %s has no bytes field %q", msg.ProtoReflect().Descriptor().FullName(), field)
	}
	return text.PlainText(msg.ProtoReflect().Get(fd).Bytes())
}

// Source is the minimum port an adopter implements over their store: read
// everything. A source that is also written back to implements Writer, and
// one with a native change cursor implements Incremental; the connector
// type-asserts for both.
//
// Identity mapping is the port's job: List decodes native records into
// entity messages (populating the key fields), and Writer's methods derive
// their native key from the message / engine key they are handed.
//
// Every method takes workspaceID even when Config.OpenSource already mints
// one instance per workspace, so a single implementation can serve both
// wirings.
type Source interface {
	// List streams every item of every bound entity in the workspace, in any
	// order. Used by reconcile and initial sync.
	List(ctx context.Context, workspaceID string, fn func(Item) error) error
}

// Writer is implemented by sources the connector may write back to. It is
// required only when some entity is bound Bidirectional — which is the
// default, so a read-only source must declare its entities IngressOnly
// (Config.Bindings) to be allowed to omit it.
//
// The connector delivers at-least-once: Write MUST be an idempotent upsert
// and Delete MUST treat a missing item as a no-op.
type Writer interface {
	// Write upserts engine state into the source and returns the resulting
	// fingerprint (observed after the write, so store-stamped versions are
	// captured).
	Write(ctx context.Context, workspaceID string, msg proto.Message) (fingerprint string, err error)
	// Delete removes the item identified by the entity prototype's type and
	// the engine row key. Deleting a missing item is a no-op.
	Delete(ctx context.Context, workspaceID string, prototype proto.Message, key string) error
}

// Incremental is implemented by sources with a native change cursor (Gmail
// historyId, IMAP HIGHESTMODSEQ, CalDAV sync-token). Connector.Ingress then
// polls Changes instead of re-enumerating the store, and List becomes the
// periodic safety net (Config.FullReconcileInterval) rather than the primary
// ingress path.
//
// Delivery stays at-least-once: the connector persists NextCursor only after
// the page it came with has been applied, so a crash in between redelivers
// that page. Both halves are idempotent — an item whose fingerprint matches
// the shadow is a no-op, and a deletion whose row is already gone writes no
// second tombstone.
type Incremental interface {
	// Checkpoint returns an opaque cursor for the source's position *now*. It
	// is taken BEFORE a full List so that changes racing that list are
	// redelivered by the following Changes call rather than lost between the
	// two.
	Checkpoint(ctx context.Context, workspaceID string) (cursor string, err error)
	// Changes returns everything that changed since cursor. Returning an error
	// that wraps ErrCursorInvalid — UIDVALIDITY bumped, the history window
	// expired, the cursor came from a store that has since been rebuilt —
	// forces a fresh Checkpoint and a full List in the same pass.
	Changes(ctx context.Context, workspaceID, cursor string) (Delta, error)
}

// Delta is one page of source changes.
type Delta struct {
	Items []Item
	// Deletions are applied after Items. A source that must interleave the two
	// (a delete that precedes a recreate under the same key) splits the work
	// across pages with More.
	Deletions []Deletion
	// NextCursor is the position to resume from. It is persisted only after
	// this page has been applied.
	NextCursor string
	// More reports that another page is ready now: the connector loops
	// immediately instead of waiting for the next tick.
	More bool
}

// Deletion is one item the source no longer holds.
type Deletion struct {
	// Prototype is any message of the entity type, as in Writer.Delete.
	Prototype proto.Message
	// Key is the engine row key.
	Key string
}

// Direction declares which way an entity's changes flow.
type Direction int

const (
	// Bidirectional converges the entity both ways: source edits ingress into
	// the engine and engine commits egress back out. The default, and what
	// requires the source to implement Writer.
	Bidirectional Direction = iota
	// IngressOnly mirrors the source into the engine and never writes back.
	// Every egress path skips the entity, so client edits to it stay local to
	// the engine — and are overwritten field-by-field whenever the source
	// changes the same field (see the shadow-base note on Shadow.BaseText).
	IngressOnly
)

// Binding declares one entity and the direction it syncs in. Config.Entities
// is the all-Bidirectional shorthand for the same thing.
type Binding struct {
	Prototype proto.Message
	Direction Direction
}
