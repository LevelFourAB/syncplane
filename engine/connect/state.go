package connect

import (
	"context"
	"time"

	"github.com/levelfourab/syncplane/engine/server"
)

// Shadow records what both sides of one row looked like the last time they
// were known equal. It is the connector's loop-breaker and merge ancestor:
//
//   - EngineVersion lets egress skip changes the connector itself ingressed
//     (change seq <= EngineVersion means "that was us").
//   - SourceFP lets ingress skip source content the connector itself wrote
//     (fingerprint equality means "unchanged, or our own echo").
//   - Base is the entity bytes of the last-synced state — the three-way
//     ancestor that lets ingress apply only the fields the source actually
//     changed.
type Shadow struct {
	SourceFP      string
	EngineVersion uint64
	Base          []byte
	// BaseText is the plain text the source last reported, per collaborative-
	// text field. Those fields carry no comparable bytes on an Item (a source
	// holds strings, not CRDT documents), so this is what ingress diffs a
	// reported string against.
	//
	// For a Bidirectional row it is the rendering of Base's documents, because
	// ingress writes the merge back and both sides genuinely agree. For an
	// IngressOnly row the two legitimately diverge — Base is the SOURCE's view
	// of the row rather than the merged engine row, and its text fields are
	// empty — so this is the only record of what the source last showed. Nil
	// on shadows written before this field existed, where the fallback is to
	// decode Base.
	BaseText map[string]string
}

// SourceSync is the connector's ingress bookkeeping for one workspace: how
// far an Incremental source's change feed has been consumed, and when a full
// enumeration last ran.
type SourceSync struct {
	// Cursor is the opaque position last reported by Incremental (Checkpoint
	// before a full pass, NextCursor after each applied page). Empty means the
	// workspace has never synced incrementally, which forces a full pass.
	Cursor string
	// LastFullSync is when List last enumerated the whole source; zero means
	// never. Config.FullReconcileInterval measures from here.
	LastFullSync time.Time
}

// State is the connector's durable bookkeeping: shadows, the egress cursor
// into the change feed, and the source's ingress position. It is a cache, not
// a ledger — losing it costs a full reconcile (content comparison, idempotent
// writes), never data.
//
// Implementations need no cross-row transactionality; the connector orders
// its writes so that any crash between two State calls is healed by
// redelivery or reconcile. The shipped default is sqlitestate.
type State interface {
	// Shadow returns the shadow for a row, or nil if none is recorded.
	Shadow(ctx context.Context, workspaceID, entity, key string) (*Shadow, error)
	PutShadow(ctx context.Context, workspaceID, entity, key string, s Shadow) error
	DeleteShadow(ctx context.Context, workspaceID, entity, key string) error
	// ScanShadows yields every shadow of the workspace. Used by reconcile to
	// detect rows deleted at the source.
	ScanShadows(ctx context.Context, workspaceID string, fn func(entity, key string, s Shadow) error) error

	// Cursor returns the egress position (zero Cursor if never set).
	Cursor(ctx context.Context, workspaceID string) (server.Cursor, error)
	SetCursor(ctx context.Context, workspaceID string, c server.Cursor) error

	// SourceSync returns the ingress position (zero SourceSync if never set).
	SourceSync(ctx context.Context, workspaceID string) (SourceSync, error)
	SetSourceSync(ctx context.Context, workspaceID string, s SourceSync) error

	Close() error
}
