package upstream

import (
	"context"
	"log/slog"
	"time"

	"connectrpc.com/connect"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/levelfourab/syncplane/engine/server"
)

// Config configures a Link.
//
// The callbacks run on the link's own goroutines, and two of them can run at
// the same time. They may call the link and write to the local engine; the
// workspace's sync waits while one runs.
type Config struct {
	// OriginURL is the base URL of the origin server, for example
	// "https://sync.example.com". The sync service is expected at its
	// standard path below it. Required.
	OriginURL string
	// HTTPClient carries every call to the origin. nil means
	// http.DefaultClient. A client Timeout also ends the live stream when it
	// passes, so leave it unset and bound calls with the link's own limits.
	HTTPClient connect.HTTPClient
	// Interceptors are added to every call to the origin, in order, for
	// example one that sets an Authorization header. The link holds one
	// identity at the origin: every workspace is synced as the caller these
	// establish.
	Interceptors []connect.Interceptor
	// ConnectOptions are added to every call to the origin, for example
	// connect.WithGRPC.
	ConnectOptions []connect.ClientOption

	// Bindings name the entities kept in sync with the origin. An entity no
	// binding names is local-only: the link neither pulls it nor forwards a
	// write to it, and a local mutation that writes only local-only entities
	// is not forwarded at all, a custom mutation the local engine runs
	// included. At least one binding is required.
	Bindings []Binding
	// WriteMode is the write mode of a local write whose context names none
	// (see WriteThrough and WriteBehind). The zero value is ModeWriteBehind.
	WriteMode WriteMode
	// ClientID is the client id the link writes to the origin under. Empty
	// means the id the link stored with its workspaces, or a new random id
	// when it stores none. Two links must never share an id. The id must be
	// 1 to 128 bytes with no control characters (\u0000 to \u001f).
	ClientID string

	// OnRejected is called for each local write the origin rejected, after
	// the link has reverted the write's rows: per workspace one call at a
	// time, in the order the rejections arrived, on a goroutine of its own.
	// Delivery is at least once. A rejection is forgotten once its call
	// returns; one whose call ran when the process crashed, before that, is
	// reported again after the next Start, with the same
	// Rejection.MutationID. Close waits for a running call and then forgets
	// its rejection.
	OnRejected func(Rejection)
	// OnReset is called after the link replaced a workspace's rows with a
	// fresh snapshot of the origin, for a reason other than the workspace's
	// first sync.
	OnReset func(workspace string)
	// OnAccessRevoked is called when the origin refused a workspace for good.
	// The link stops syncing the workspace and refuses local writes to its
	// bound entities.
	OnAccessRevoked func(workspace string)
	// OnStatus is called with a workspace's status each time it changes,
	// except that a change of Status.Bootstrap alone is reported at most
	// every 250 ms, and the newest one within 250 ms. The start and the end
	// of a snapshot download are always reported.
	OnStatus func(Status)
	// OnSchemaSkew decides how a workspace reacts when the local engine's
	// schema revision (server.WithSchemaRevision) differs from the one the
	// origin advertises. It is called once per workspace for each new pair
	// of revisions, before the workspace next forwards local writes. The link
	// compares the revisions before each round of forwarding: after a local
	// write, a Flush, a retry, or a change of the origin's revision. nil
	// means SkewWarn for every pair. The link compares nothing when either
	// side advertises no revision.
	OnSchemaSkew func(workspace, local, origin string) SkewAction
	// DeleteOnRevoke deletes the local rows of the bound entities of a
	// workspace the origin revoked, as RemoveWorkspace with
	// RemoveOptions.DeleteRows does.
	DeleteOnRevoke bool

	// RetryMin and RetryMax bound the backoff after a failed call to the
	// origin: the wait is drawn at random up to a window that doubles from
	// RetryMin to RetryMax. 0 means 1s and 30s.
	RetryMin, RetryMax time.Duration
	// ConcurrentSyncs is how many workspaces bootstrap or catch up with the
	// origin at once; the others wait their turn. 0 means 4.
	ConcurrentSyncs int
	// StreamIdleTimeout is how long the live stream may deliver nothing
	// before the link takes it for dead and opens another. 0 means three
	// times the longest gap seen between the origin's heartbeats, from 2s to
	// 90s; until two heartbeats were seen, 10s, doubled up to 90s each time a
	// stream is ended for silence.
	StreamIdleTimeout time.Duration
	// PageLimit is the most changes the link asks for per pull or stream page.
	// 0 asks for the origin's default.
	PageLimit uint32
	// MaxPushMutations is the most mutations one push carries. 0 means 256.
	MaxPushMutations int
	// MaxPushBytes is the encoded size a push stops adding mutations at. A
	// single larger mutation is pushed alone. 0 means 4 MiB.
	MaxPushBytes int
	// OriginLimits are the limits the origin enforces (its server.Limits). The
	// link reads MaxValueBytes and MaxFieldMaskPaths, and a field at zero or
	// below means the server's default. A local write whose forwarded form
	// is over one of them is refused before it commits, with a server
	// Rejection of code invalid_argument: an Apply returns it, and a
	// downstream client's mutation is rejected with it. A collaborative-text
	// field is measured as the full document the link forwards.
	OriginLimits server.Limits
	// WriteThroughTimeout bounds the call a write-through write makes to the
	// origin. 0 means 10s.
	WriteThroughTimeout time.Duration
	// PoisonThreshold is how many times in a row the origin may fail the
	// first pending write the same way, with mutation_failed,
	// invalid_argument or an unknown code, before the link drops it and
	// reports it as rejected with that code. 0 means 5; negative never drops.
	PoisonThreshold int

	// Logger receives the link's background failures. nil means
	// slog.Default().
	Logger *slog.Logger
	// TracerProvider and MeterProvider are the OpenTelemetry providers the
	// link records on. nil means the global providers.
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
}

// Binding names one entity the link keeps in sync with the origin, by
// Prototype or by Entity name.
type Binding struct {
	// Prototype is a message of the entity. One of Prototype and Entity is
	// required.
	Prototype proto.Message
	// Entity is the entity's wire name, for an entity without a generated
	// message.
	Entity string
	// Direction is how the entity syncs. The zero value is Both.
	Direction Direction
}

// Direction is the way a bound entity syncs.
type Direction int

const (
	// Both pulls the entity from the origin and forwards local writes to it.
	Both Direction = iota
	// PullOnly pulls the entity from the origin. A local write to it is
	// rejected with code permission_denied, except inside a custom mutation,
	// which the origin runs.
	PullOnly
)

// WriteMode is when a local write reaches the origin.
type WriteMode int

const (
	// ModeWriteBehind commits a local write at once and forwards it later. A
	// write the origin rejects is reverted locally and reported through
	// Config.OnRejected.
	ModeWriteBehind WriteMode = iota
	// ModeWriteThrough forwards a local write before it commits locally, after
	// every earlier pending write. The local write fails with the origin's
	// rejection (a server Rejection with the origin's code), or with an error
	// when the origin cannot be reached, and commits nothing then.
	//
	// The call to the origin runs while the local store's write lock is held,
	// for the whole round trip. Every other write to the workspace waits for
	// it, the link's own included, so the workspace's pulls and live stream
	// stall too; on a store with one writer, such as sqlitestore, so does
	// every write to every workspace.
	//
	// A downstream client's push uses this mode too. When the origin cannot
	// be reached, the push's whole batch fails with code internal, which the
	// client retries.
	ModeWriteThrough
)

type writeModeKey struct{}

// WriteThrough returns a context whose local writes use ModeWriteThrough,
// whatever Config.WriteMode says.
func WriteThrough(ctx context.Context) context.Context {
	return context.WithValue(ctx, writeModeKey{}, ModeWriteThrough)
}

// WriteBehind returns a context whose local writes use ModeWriteBehind,
// whatever Config.WriteMode says.
func WriteBehind(ctx context.Context) context.Context {
	return context.WithValue(ctx, writeModeKey{}, ModeWriteBehind)
}

// RowRef names one row.
type RowRef struct {
	Entity string
	Key    string
}

// Rejection is a local write the origin rejected.
type Rejection struct {
	Workspace string
	// LocalSeq is the local sequence the write took, 0 for a custom mutation
	// that wrote no local row.
	LocalSeq uint64
	// MutationID is the id the link pushed the write under.
	MutationID uint64
	// Code is the origin's rejection code, for example "conflict" or
	// "permission_denied".
	Code string
	// Message is human-readable detail for logs.
	Message string
	// Mutator is the name of the custom mutation, "" for a CRUD write.
	Mutator string
	// Rows are the bound rows the write touched locally, which the link
	// reverted.
	Rows []RowRef
}

// RemoveOptions configures RemoveWorkspace.
type RemoveOptions struct {
	// DeleteRows deletes the workspace's local rows of every bound entity.
	// Rows of local-only entities stay.
	DeleteRows bool
}
