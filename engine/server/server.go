// Package server implements the sync engine's ConnectRPC service on top of
// a storage.Adapter.
package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/levelfourab/syncplane/engine/cursorstate"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/internal/lww"
	"github.com/levelfourab/syncplane/engine/internal/naming"
	"github.com/levelfourab/syncplane/engine/internal/pubsub"
	"github.com/levelfourab/syncplane/engine/internal/telemetry"
	"github.com/levelfourab/syncplane/engine/lease"
	"github.com/levelfourab/syncplane/engine/presence"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/text"
	"github.com/levelfourab/syncplane/internal/protoopts"
)

// entityInfo is a registered syncable entity.
type entityInfo struct {
	name      string
	msgType   protoreflect.MessageType
	keyFields []protoreflect.FieldDescriptor
	refFields []refField
	// textFields are the (syncplane.v1.field_behavior).text fields, top-level and
	// nested, discovered at registration. Each gets the built-in text merger
	// auto-installed in mergers, and EntityTx.Put merges them read-modify-
	// write instead of replacing.
	textFields []textField
	// indexes are the entity's secondary indexes, derived at registration.
	// Every row write refreshes all of them together (updateRowIndexes), so
	// this is the ONE list that decides an entity's index write cost.
	indexes []compiledIndex
	// search is the entity's search index, nil for an entity that declares no
	// search fields. Its entries ride the same storage and maintenance as
	// indexes, under the reserved searchIndexName.
	search  *searchIndex
	mergers lww.Mergers
	// createPolicy, updatePolicy and deletePolicy are the entity's
	// (syncplane.v1.entity) write policies. The engine checks them alongside
	// CanMutate (see Server.authorizeWrite); they are held here rather than
	// re-read from the descriptor because they are consulted once per
	// authorized write.
	createPolicy syncplanev1.CreatePolicy
	updatePolicy syncplanev1.UpdatePolicy
	deletePolicy syncplanev1.DeletePolicy
	// serverOnly is (syncplane.v1.entity).server_only: the entity's rows never
	// reach a client. Enforced at the row emitter, which is the chokepoint
	// bootstrap, pull and resync share, and at the two doors a client could name
	// it through — a subscription and a CRUD mutation. See emit.go.
	serverOnly bool
	// history is (syncplane.v1.entity).history when the entity opted in, else
	// nil. Its presence is what puts a write of this entity on the capture path
	// (see history.go).
	history *syncplanev1.HistoryOptions
	// historyFields are the top-level field names a history record captures, in
	// field order: exactly those marked (field_behavior).history. Empty only for
	// an entity that has not opted in — an entity that opts in and marks nothing
	// is refused at registration. Key fields are excluded here and populated
	// separately on the recorded prior value — see the field option's doc for
	// why a mask must never name one.
	historyFields []string
	// protectedFields are the top-level units no authorized caller may write:
	// the fields marked (field_behavior).write_policy TRUSTED_ONLY, with a
	// oneof contributing its group name. Empty for almost every entity, which
	// is what keeps the check off the hot path — see writeGate.
	protectedFields []string
	// access is the entity's place in the access model, nil when it declares
	// no access option. grant is set on an entity whose rows grant roles. See
	// access.go.
	access *entityAccess
	grant  *grantSpec
}

// compiledIndex is one secondary index of an entity: a name scoped to the
// workspace, an encoder producing the entries a row occupies in it, and the
// fingerprint of the declaration those entries were built from.
//
// An index is derived, disposable and never on the wire. Nothing in a request
// or response names one, losing every entry costs a rebuild rather than
// correctness, and a read may always ignore it and scan instead — which is why
// a stale specHash simply means "not usable yet" rather than "corrupt".
type compiledIndex struct {
	name     string
	specHash string
	encoder  *query.IndexEncoder
}

// searchIndex is an entity's term-postings index, compiled from its search
// declaration. It shares the derived/disposable/pure-optimization contract of
// compiledIndex and the same storage, state and build machinery; what differs
// is the entry shape (terms, not field values — see
// engine/query/searchindex.go) and that reads probe it by term rather than
// through the planner.
type searchIndex struct {
	name     string
	specHash string
	decl     *query.SearchDecl
}

// Index names the engine mints for itself all live under a reserved prefix, so
// a schema-declared name can never collide with one however the schema evolves.
// Names are unique within an ENTITY only — they are derived from field names,
// so two entities with a `state` field marked indexed both mint
// "__field_state". Storage keys entries and build state by (entity, index) for
// exactly that reason.
//
// Two kinds are minted:
//
//   - refIndexPrefix, one per reference field, so a reverse lookup ("every
//     issue whose project is p1") is a range probe rather than a scan of the
//     whole holder entity. Derived, never declared.
//   - fieldIndexPrefix, one per field carrying (field_behavior).indexed. The
//     name is derived rather than authored so two schemas marking the same
//     field agree on it.
const (
	reservedIndexPrefix = "__"
	// searchIndexName is the entity's term-postings index (see
	// engine/query/searchindex.go). One per entity, so it needs no derived
	// suffix.
	searchIndexName  = reservedIndexPrefix + "search"
	refIndexPrefix   = reservedIndexPrefix + "ref_"
	fieldIndexPrefix = reservedIndexPrefix + "field_"
)

// refIndexName is the index over one reference's stored target key, named
// by refCasePath.
func refIndexName(ref *lww.Ref) string {
	return refIndexPrefix + refCasePath(ref)
}

// refCasePath names one case of a reference: its field, and for a case of a
// polymorphic reference message, which shares its field with its sibling
// cases, the case's own name after a dot. No field name contains a dot, so
// the path of a case cannot spell the path of another field.
func refCasePath(ref *lww.Ref) string {
	if ref.InnerCase() {
		return string(ref.Field.Name()) + "." + string(ref.Value.Name())
	}
	return string(ref.Field.Name())
}

// textField is one collaborative-text field: its dotted proto path and the
// steps leading to it. A path may cross keyed message lists and message-valued
// maps, in which case the field holds one document PER ELEMENT rather than one
// per row; such a field is nested, and everything that consumes it enumerates
// instances (see instances) instead of walking a fixed chain.
type textField struct {
	path  string
	steps []textStep
	// fds are the step descriptors, in order. Held once so instance tails can
	// be subslices instead of per-call rebuilds.
	fds []protoreflect.FieldDescriptor
	// nested reports whether any step is a container. A fixed field has
	// exactly one instance per row, present or not, which is what the
	// pre-container behavior was.
	nested bool
}

// textStep is one segment of a text field's path: the field walked through,
// plus the element identity when that field is a keyed message list. A map
// step is recognized by fd.IsMap(); any other step is a singular descent.
type textStep struct {
	fd        protoreflect.FieldDescriptor
	keyFields []protoreflect.FieldDescriptor
}

func (st textStep) container() bool { return st.fd.IsMap() || len(st.keyFields) > 0 }

func newTextField(path string, steps []textStep) textField {
	tf := textField{path: path, steps: steps}
	tf.fds = make([]protoreflect.FieldDescriptor, len(steps))
	for i, st := range steps {
		tf.fds[i] = st.fd
		if st.container() {
			tf.nested = true
		}
	}
	return tf
}

// textInstance is one document position of a text field within one row: the
// message holding the leaf's remaining singular chain, plus the instance path
// — the schema path with the item keys the enumeration crossed, which is
// exactly the mask form addressing this document ("items.<key>.body"). The
// instance path is the identity every per-document side table keys on: the
// delta log, compaction floors, the decoded-document cache and the wire's
// FieldDelta.field all carry it.
type textInstance struct {
	path   string
	holder protoreflect.Message
	tail   []protoreflect.FieldDescriptor
}

func (ti textInstance) bytes() []byte { return chainGetBytes(ti.holder, ti.tail) }
func (ti textInstance) set(b []byte)  { chainSetBytes(ti.holder, ti.tail, b) }
func (ti textInstance) clear()        { chainClearBytes(ti.holder, ti.tail) }

// instances returns every position of tf that msg carries, in walk order (list
// order; map entries sorted by encoded key, so the order is deterministic). A
// fixed field yields exactly one instance whether or not the leaf is set —
// mirroring the plain chain walk — while a nested field yields one per element
// reachable through present containers, so zero when a container is absent.
//
// Holders of returned instances are obtained mutably, so set/clear write
// through to msg; enumeration itself materializes nothing, because it only
// descends into fields msg already has.
func (tf textField) instances(msg protoreflect.Message) []textInstance {
	if !tf.nested {
		return []textInstance{{path: tf.path, holder: msg, tail: tf.fds}}
	}
	var out []textInstance
	tf.walkInstances(msg, 0, "", &out)
	return out
}

func (tf textField) walkInstances(cur protoreflect.Message, from int, prefix string, out *[]textInstance) {
	next := -1
	for i := from; i < len(tf.steps); i++ {
		if tf.steps[i].container() {
			next = i
			break
		}
	}
	if next == -1 {
		// No container remains: the rest of the path is a fixed chain from cur.
		path := prefix
		for i := from; i < len(tf.steps); i++ {
			if i > from {
				path += "."
			}
			path += string(tf.steps[i].fd.Name())
		}
		*out = append(*out, textInstance{path: path, holder: cur, tail: tf.fds[from:]})
		return
	}
	// Singular descent to the container; an absent intermediate holds no
	// elements, and neither does an absent container.
	for i := from; i < next; i++ {
		if !cur.Has(tf.steps[i].fd) {
			return
		}
		cur = cur.Mutable(tf.steps[i].fd).Message()
		prefix += string(tf.steps[i].fd.Name()) + "."
	}
	st := tf.steps[next]
	if !cur.Has(st.fd) {
		return
	}
	prefix += string(st.fd.Name()) + "."
	if st.fd.IsMap() {
		mm := cur.Mutable(st.fd).Map()
		segs := make([]string, 0, mm.Len())
		keys := make(map[string]protoreflect.MapKey, mm.Len())
		mm.Range(func(mk protoreflect.MapKey, _ protoreflect.Value) bool {
			seg := lww.EncodeMapKeySegment(st.fd, mk)
			segs = append(segs, seg)
			keys[seg] = mk
			return true
		})
		sort.Strings(segs)
		for _, seg := range segs {
			tf.walkInstances(mm.Mutable(keys[seg]).Message(), next+1, prefix+seg+".", out)
		}
		return
	}
	list := cur.Mutable(st.fd).List()
	for j := 0; j < list.Len(); j++ {
		elem := list.Get(j).Message()
		seg := lww.EncodeItemKey(elem, st.keyFields)
		tf.walkInstances(elem, next+1, prefix+seg+".", out)
	}
}

// refField is a reference field of an entity together with the key fields of
// the entity it targets (resolved off the target message's own options). Used
// to project a stored ref down to its target's key fields (projectRefs).
type refField struct {
	fd        protoreflect.FieldDescriptor
	keyFields []protoreflect.FieldDescriptor
	// ref is the reference's stored shape — where the key and, for a
	// reference message, the workspace sit inside the field value.
	ref *lww.Ref
}

// Limits bounds request payloads and response sizes. It bounds what one
// request may cost, never request rate or concurrent streams; those limits
// belong in the HTTP stack in front of the handlers, where they can attach
// to a connection, an address or a credential. Every field must be
// positive to take effect: zero means "unset", and a negative value is a
// misconfiguration, so both fall back to the documented default (the same rule
// WithChunkSize follows). Letting a negative through would not merely relax a
// bound, it would invert one — the storage contract reads a non-positive scan
// limit as UNLIMITED, so a negative MaxPullChanges would turn every Pull into
// a full-workspace scan that emits nothing and returns a cursor regressed to
// sequence 0, which a well-behaved client then loops on forever.
type Limits struct {
	// Maximum encoded bytes of a single request message (Push/Pull/Query/
	// Bootstrap), enforced by the Connect/HTTP layer before the body is
	// buffered or unmarshaled — and applied to the decompressed size, so it
	// also caps compression bombs. This is the coarse outer wall in front of
	// every finer limit below (and of otherwise unbounded inputs that ride in
	// the body: the subscription set, condition trees, and the opaque pull
	// cursor); an over-limit request is rejected as resource_exhausted before
	// any parsing. It must sit comfortably above your largest legitimate push
	// (roughly MaxMutationsPerPush × MaxValueBytes) — raise it in tandem if you
	// raise those. Default 16 MiB.
	MaxRequestBytes int
	// Maximum mutations per Push request, counted across ALL its per-workspace
	// batches — it bounds the work one request asks for. Default 256.
	MaxMutationsPerPush int
	// Maximum workspaces one request may name: Push batches and Subscribe
	// scopes. Each costs a write transaction or a live stream scan plus a
	// notifier subscription, so this bounds what a single request can commit
	// the server to. A client holding more workspaces than this must split its
	// pushes and open a second stream. Default 64.
	MaxWorkspacesPerRequest int
	// Maximum serialized entity value (crud.value / custom.args) in bytes.
	// Default 1 MiB.
	MaxValueBytes int
	// Maximum field-mask paths per PATCH. Default 64.
	MaxFieldMaskPaths int
	// Maximum row changes per Pull response; larger results paginate via
	// has_more. Also clamps client-requested limits. Default 4096.
	MaxPullChanges int
	// Maximum live rows one Pull examines while delta-resyncing a scope change
	// before it returns a partial page. A scope change compares the workspace's
	// live rows under the old and the new visibility, and rows visible under
	// both emit nothing, so this budget rather than MaxPullChanges decides how
	// many round trips a small difference costs: a larger value settles a
	// widened window over a large workspace in one or two pulls, a smaller one
	// bounds how long a single Pull holds its read transaction.
	//
	// It also bounds candidate narrowing wherever a scope names its rows
	// instead of visiting them — a resync, a bootstrap, and the seeds a
	// following subscription walks from: how many rows any of them may name
	// through secondary indexes and membership sets before it visits the
	// entity row by row instead. A scope selecting more of an entity than this
	// is no cheaper to reach by index, so lowering the limit trades snapshot,
	// resync and follow speed on large scopes for a smaller working set —
	// never a different set of rows.
	//
	// Values below MaxPullChanges are raised to it; a page that stops before
	// it is full only adds round trips. Default 65536.
	MaxResyncScan int
	// Maximum rows per Query response. Also clamps client-requested limits.
	// Default 1024.
	MaxQueryRows int
	// Maximum offset accepted by Query. Offset is the fallback for jumping to
	// an arbitrary rank; sequential paging goes through after/before, which
	// costs neither the working set nor the shifting page an offset does. A
	// query retains the offset+limit rows that sort first, so the offset bounds
	// that in-memory working set however the query is served — from an index or
	// from a scan. Requests beyond this are rejected rather than clamped (a
	// clamp would return the wrong page silently). Default 16384.
	MaxQueryOffset int
	// Maximum length in bytes of an after/before cursor Query accepts. A cursor
	// is a row's position under the query's ordering, so its length follows the
	// order-key values; the limit is what keeps a malformed one from costing
	// work proportional to whatever a client sent. Default 4096.
	MaxQueryCursorBytes int
	// Maximum follow depth a Subscription.follow may request (rounds the path
	// set is re-applied). A request above this is InvalidArgument. Default 4.
	MaxFollowDepth int
	// Maximum number of rows a single request's reference-following closure may
	// contain. Overflow aborts the pull as resource_exhausted, keeping working
	// sets bounded. Also bounds the Cursor.closure key-set. Default 10000.
	MaxClosureSize int
	// Maximum source rows a single reverse-follow hop may match (one parent's
	// children). Overflow is resource_exhausted; protects against a popular
	// parent blowing the working set on the index-less reverse scan. Default
	// 1000.
	MaxReverseFanOut int
	// Maximum rows a single Subscription.window may hold. A larger limit is
	// InvalidArgument rather than clamped: a silently narrower window would
	// look like missing data to the app.
	//
	// Must stay well under MaxPullChanges: a window that swaps wholesale emits
	// up to 2N changes (N values entering, N tombstones leaving) in ONE page,
	// because the membership diff runs at a clean version boundary and has no
	// paging state of its own. Default 1000, against MaxPullChanges' 4096.
	MaxWindowSize int
	// Maximum keys a single Subscription.key_set may name. A larger set is
	// InvalidArgument rather than clamped, for the same reason a window is: a
	// silently shorter set would look like missing data to the app.
	//
	// The set rides on every Pull and Subscribe request the client sends, so
	// this bounds request size as much as it bounds the rows a scope pins.
	// Default 1000.
	MaxSubscriptionKeys int
	// Maximum secondary-index entries one row may occupy across all of its
	// entity's indexes. This is the WRITE cost of indexing: every row write
	// re-encodes and stores the whole set, so the count multiplies the price
	// of every mutation to that entity. Exceeding it fails the write rather
	// than silently dropping entries, which would leave an index that reads
	// as complete and is not.
	//
	// An index over a repeated field stores one entry per ELEMENT, so this is
	// also the ceiling on how long an indexed list may get — roughly
	// (elements x indexes over that list) plus one per ordinary index. The
	// default is set for lists in the hundreds, which is where a to-many
	// reference stops being a reasonable shape anyway; a schema that indexes
	// longer lists has to raise it deliberately, because the cost lands on
	// every write of the row and not on the query that benefits. Default 1024.
	MaxIndexEntriesPerRow int
	// Maximum index entries one range scan may walk while its read filter
	// rejects candidates. An index narrows the candidates; a permission filter
	// or a residual predicate the index cannot express still rejects some of
	// them, and in the worst case rejects nearly all — at which point the
	// "index" walk has become a scan with extra steps. Overflow falls back to
	// the plain scan rather than failing, because index use is an
	// optimization and must never change an answer. Default 10000.
	MaxIndexScanCandidates int
	// DisableIndexes forces every read to take its scan fallback, as if no
	// index were ready. Indexes are still MAINTAINED, so the setting can be
	// flipped without a rebuild.
	//
	// It exists for the differential test: index use is a pure optimization,
	// so the whole suite must produce identical results with it on and off,
	// and any divergence is a bug in the encoding or the planner. It is also
	// the operational escape hatch if one ever is.
	DisableIndexes bool
	// Maximum rooms one Watch stream may carry. Each costs a bus subscription,
	// a join gate and — for an anchored room — a row read on every re-gate, so
	// this bounds what one stream commits the server to. A client watching more
	// rooms than this opens a second stream. Default 64.
	MaxRoomsPerStream int
	// Maximum bytes of one participant's presence state. It is the app's own
	// payload, and it rides in every announcement of that record — so unlike a
	// row, its cost is per interval rather than per write, and it has to fit
	// whatever the presence bus can carry (a Postgres NOTIFY payload is capped
	// at 8000 bytes, base64 included). Default 4 KiB.
	MaxPresenceStateBytes int
	// Maximum bytes of one broadcast message, under the same bus ceiling as
	// MaxPresenceStateBytes. Default 4 KiB.
	MaxBroadcastBytes int
	// Maximum items in one Publish request. Items are independent, so this
	// bounds the gate calls and bus publishes one round trip can ask for.
	// Default 32.
	MaxPublishItems int
	// Maximum participants a room keeps. Beyond it the room keeps the lowest
	// session ids, reports its snapshot truncated, and turns further sessions
	// away — lowest rather than newest so two nodes serving the same over-full
	// room show the same people.
	//
	// It is a rendering bound as much as a memory one: a roster nobody can read
	// is not worth the announcement traffic that maintains it. Default 128.
	MaxParticipantsPerRoom int
	// DisableMembershipMaintenance forces every pull to resolve a window and a
	// follow closure from scratch, as if the cursor said nothing about the
	// previous one.
	//
	// It exists for the same differential test DisableIndexes does, and for the
	// same reason: maintaining a membership set from what changed is a pure
	// optimization over rebuilding it, so the two must produce identical pages
	// however the workspace is driven, and any divergence is a bug in the
	// maintenance rules rather than a tuning question.
	DisableMembershipMaintenance bool
	// Maximum matches() conditions one query predicate may carry. Each crosses
	// into another entity and is answered by its own pass over that entity's
	// rows before the query itself runs, so the count multiplies the price of
	// the request while the predicate's own size says nothing about it.
	//
	// Conditions selecting the same target rows share one pass, so the limit
	// counts what a predicate WRITES rather than what it costs. Default 8.
	MaxRelConditions int
	// Maximum access_parent hops from a row up to a row without an access
	// parent, in a schema that uses the access model. Registration refuses an
	// entity with a longer chain of parent entities, through any case of a
	// polymorphic access parent. In a recursive chain,
	// such as a folder inside a folder, a write that would put a row deeper is
	// refused, and a read stops walking at this depth: a row deeper than it
	// is read as anchored at nothing above that point. Default 32.
	MaxAccessDepth int
	// Maximum facet axes one Query may carry. Each axis costs a bucket per
	// distinct value and a lookup per matching row, and a request carrying any
	// walks every match rather than the page. Default 8.
	MaxQueryFacets int
	// Maximum target rows one matches() condition may select. It is the size of
	// the membership test the condition collapses to, which the server builds
	// and holds for the request, so an unbounded one turns a small request into
	// a large read. Exceeding it fails the query rather than truncating: a
	// truncated key set would silently drop rows that match. Default 50000.
	MaxRelKeys int
}

// maxIndexEntriesPerRow and maxIndexScanCandidates read the defaults through
// a method rather than withDefaults so that a zero Limits — which several
// internal paths construct — still behaves.
func (l Limits) maxIndexEntriesPerRow() int {
	if l.MaxIndexEntriesPerRow <= 0 {
		return 1024
	}
	return l.MaxIndexEntriesPerRow
}

func (l Limits) maxIndexScanCandidates() int {
	if l.MaxIndexScanCandidates <= 0 {
		return 10000
	}
	return l.MaxIndexScanCandidates
}

func (l Limits) maxRelConditions() int {
	if l.MaxRelConditions <= 0 {
		return 8
	}
	return l.MaxRelConditions
}

func (l Limits) maxQueryFacets() int {
	if l.MaxQueryFacets <= 0 {
		return 8
	}
	return l.MaxQueryFacets
}

func (l Limits) maxAccessDepth() int {
	if l.MaxAccessDepth <= 0 {
		return 32
	}
	return l.MaxAccessDepth
}

func (l Limits) maxRelKeys() int {
	if l.MaxRelKeys <= 0 {
		return 50000
	}
	return l.MaxRelKeys
}

func (l Limits) withDefaults() Limits {
	// <= 0 rather than == 0 throughout: see the type doc for why a negative
	// must never reach the request path.
	if l.MaxRequestBytes <= 0 {
		l.MaxRequestBytes = 16 << 20
	}
	if l.MaxMutationsPerPush <= 0 {
		l.MaxMutationsPerPush = 256
	}
	if l.MaxWorkspacesPerRequest <= 0 {
		l.MaxWorkspacesPerRequest = 64
	}
	if l.MaxValueBytes <= 0 {
		l.MaxValueBytes = 1 << 20
	}
	if l.MaxFieldMaskPaths <= 0 {
		l.MaxFieldMaskPaths = 64
	}
	if l.MaxPullChanges <= 0 {
		l.MaxPullChanges = 4096
	}
	if l.MaxResyncScan <= 0 {
		l.MaxResyncScan = 65536
	}
	l.MaxResyncScan = max(l.MaxResyncScan, l.MaxPullChanges)
	if l.MaxQueryRows <= 0 {
		l.MaxQueryRows = 1024
	}
	if l.MaxQueryOffset <= 0 {
		l.MaxQueryOffset = 16384
	}
	if l.MaxQueryCursorBytes <= 0 {
		l.MaxQueryCursorBytes = 4096
	}
	if l.MaxWindowSize <= 0 {
		l.MaxWindowSize = 1000
	}
	if l.MaxSubscriptionKeys <= 0 {
		l.MaxSubscriptionKeys = 1000
	}
	if l.MaxFollowDepth <= 0 {
		l.MaxFollowDepth = 4
	}
	if l.MaxClosureSize <= 0 {
		l.MaxClosureSize = 10000
	}
	if l.MaxReverseFanOut <= 0 {
		l.MaxReverseFanOut = 1000
	}
	if l.MaxIndexEntriesPerRow <= 0 {
		l.MaxIndexEntriesPerRow = 1024
	}
	if l.MaxIndexScanCandidates <= 0 {
		l.MaxIndexScanCandidates = 10000
	}
	if l.MaxRoomsPerStream <= 0 {
		l.MaxRoomsPerStream = 64
	}
	if l.MaxPresenceStateBytes <= 0 {
		l.MaxPresenceStateBytes = 4 << 10
	}
	if l.MaxBroadcastBytes <= 0 {
		l.MaxBroadcastBytes = 4 << 10
	}
	if l.MaxPublishItems <= 0 {
		l.MaxPublishItems = 32
	}
	if l.MaxParticipantsPerRoom <= 0 {
		l.MaxParticipantsPerRoom = 128
	}
	return l
}

// catalog is the immutable set of registered entities plus the schema
// revision they advertise. The server holds it behind an atomic.Pointer so
// the read path (Push/Pull/Query/Bootstrap) loads a stable snapshot without
// locking, while registration (RegisterEntity, RegisterDescriptorSet, and the
// declaration options New applies) builds a fresh catalog and swaps the
// pointer. A re-registration is therefore atomic against in-flight RPCs: a
// request that loaded the old catalog finishes against it, and a schema change
// surfaces to clients as a new schemaRev so they re-bootstrap.
type catalog struct {
	entities  map[string]*entityInfo // by wire name
	byType    map[protoreflect.FullName]*entityInfo
	schemaRev string
	// refGraph mirrors the client's reference graph (forward/reverse relations
	// by entity), built lazily on first following request. It is scoped to this
	// catalog so a re-registration rebuilds it. refGraphErr captures a
	// reverse-name clash.
	refGraphOnce sync.Once
	refGraph     *refGraph
	refGraphErr  error
	// indexMark caches the fingerprint of every declared index, computed on
	// first use by indexBuildMark and scoped to this catalog for refGraph's
	// reason: a re-registration builds a fresh catalog, and the mark has to
	// move with it.
	indexMarkOnce sync.Once
	indexMark     string
	// access is the compiled access model, nil for a schema that does not use
	// it. accessErr is set instead while the catalog breaks a rule of the
	// model, which RegisterEntity can leave behind because it adds one entity
	// at a time; see accessModelOrErr.
	access    *accessModel
	accessErr error
}

// accessModelOrErr returns the catalog's access model, nil when the schema
// does not use one. It fails while the schema breaks a rule of the model, so
// a request against such a schema is refused rather than served with access
// that nothing checked.
func (c *catalog) accessModelOrErr() (*accessModel, error) {
	if c.accessErr != nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, c.accessErr)
	}
	return c.access, nil
}

// recompileAccess compiles the access model for the catalog's current
// entities and the server's mutators. Every swap that adds or replaces an
// entity calls it.
func (c *catalog) recompileAccess(mutators map[string]*mutatorInfo, limits Limits) {
	c.access, c.accessErr = compileAccessModel(c, mutators, limits.maxAccessDepth())
}

// indexBuildMark fingerprints every index of every registered entity, so that
// one equality answers "are this workspace's indexes built under the current
// declarations" for the whole catalog (see
// storage.Adapter.ListStaleIndexWorkspaceIDs). Comparing it is what spares the
// background pass a state read per (workspace × entity × index).
//
// Sorted by (entity, index) so the mark depends on the declarations and not on
// map iteration order, and hashed rather than concatenated because a spec
// fingerprint is a readable string of unbounded length (query.SpecHash) and
// this one is stored per workspace.
func (c *catalog) indexBuildMark() string {
	c.indexMarkOnce.Do(func() {
		specs := make([]string, 0, len(c.entities))
		for _, info := range c.entities {
			for i := range info.indexes {
				specs = append(specs, info.name+"\x00"+info.indexes[i].name+"\x00"+info.indexes[i].specHash)
			}
			if info.search != nil {
				specs = append(specs, info.name+"\x00"+info.search.name+"\x00"+info.search.specHash)
			}
		}
		sort.Strings(specs)
		sum := sha256.Sum256([]byte(strings.Join(specs, "\n")))
		c.indexMark = hex.EncodeToString(sum[:16])
	})
	return c.indexMark
}

// clone returns a shallow copy of the catalog with fresh entity maps (the
// entityInfo pointers are shared — registration replaces whole entries rather
// than mutating in place) and a reset refGraph cache. schemaRev is carried
// over; callers override it when the schema changes.
func (c *catalog) clone() *catalog {
	nc := &catalog{
		entities:  make(map[string]*entityInfo, len(c.entities)+1),
		byType:    make(map[protoreflect.FullName]*entityInfo, len(c.byType)+1),
		schemaRev: c.schemaRev,
		access:    c.access,
		accessErr: c.accessErr,
	}
	for k, v := range c.entities {
		nc.entities[k] = v
	}
	for k, v := range c.byType {
		nc.byType[k] = v
	}
	return nc
}

// Server is one app's sync engine: it implements
// syncplanev1connect.SyncServiceHandler over a storage.Adapter. Build one with
// New, declaring its schema and behaviour as options (WithEntities,
// WithMutator, WithMerger, WithPermissions); serve it with HTTPHandler, or
// Handler to mount it on a mux of your own. Schemas that arrive after
// construction go through RegisterDescriptorSet or RegisterDynamicEntities.
//
// A Server is safe for concurrent use once New returns. Everything a request
// reads is either immutable (the mutator table) or swapped atomically (the
// entity catalog, so runtime registration can land under live traffic).
type Server struct {
	store storage.Adapter
	// replicaReads is the adapter's replica-read capability, nil when the
	// adapter offers none. readTx/readTxAnchored route through it; every
	// other read stays on store.
	replicaReads storage.ReplicaReader
	cat          atomic.Pointer[catalog]
	// mutators is fixed once New returns — mutators are declared only through
	// WithMutator — which is what makes it safe to read from every request
	// goroutine without a lock.
	mutators map[string]*mutatorInfo
	perms    Permissions
	authn    Authenticator
	notifier pubsub.Notifier
	// wsPrefix is this server's share of the workspace-id space, empty unless
	// a Manager hosts it. New applies it to the notifier and the room
	// registry; storage arrives already prefixed (see prefixstore).
	wsPrefix string
	// presenceBus fans room events out to the other nodes; rooms is this node's
	// view built on top of it, and roomSweep is the goroutine driving the
	// registry's announce-and-expire pass. See engine/presence.
	presenceBus  presence.Bus
	roomAnnounce time.Duration
	rooms        *presence.Registry
	roomSweep    *roomSweeper
	// roomSessions binds a session id to the principal that opened its Watch
	// stream, so a Publish naming somebody else's session is refused. It is
	// node-local, like the streams it tracks.
	roomSessions *sessionBindings
	// roomAnchors memoizes the anchor-row read the join gate performs, so a hot
	// document costs one read per interval rather than one per watcher.
	roomAnchors *anchorCache
	// cursorState holds the bulky membership tail of issued cursors so the
	// wire carries a reference instead (see encodeCursor). Nil disables the
	// indirection: every cursor inlines its sets, exactly as cursors always
	// have. cursorStateSet records that WithCursorStateStore was called, so an
	// explicit nil wins over the adapter-provided default New would install.
	cursorState    cursorstate.Store
	cursorStateSet bool
	// leases dedups per-workspace background work across the nodes sharing the
	// store (see the lease package). Never nil once New returns: the adapter's
	// locker when it provides one, a process-local one otherwise.
	leases lease.Locker
	// coalescer is the wrapper New installed over notifier, kept so Close can
	// stop its timers. Nil when coalescing is disabled (WithNotifyCoalescing(0)),
	// in which case notifier is whatever the caller supplied, untouched.
	coalescer     *pubsub.Coalescing
	heartbeat     time.Duration
	notifyWindow  time.Duration
	chunkSize     int
	limits        Limits
	baseSchemaRev string // schema revision from WithSchemaRevision (compiled path)
	gc            *gcLoop
	// indexBuild backfills secondary index entries for rows that predate an
	// index. Always present: index maintenance is not optional, and an index
	// that is never built is one that never gets used.
	indexBuild *indexBuildLoop
	// compact reclaims collaborative-text tombstones. Always present, and inert
	// until WithTextCompaction sets a threshold: the sweep is opt-in, but the
	// loop has to exist for the option to configure.
	compact *compactLoop
	logger  *slog.Logger
	// providers holds the OpenTelemetry providers the options installed (zero
	// value = the globals, which are no-op until an SDK is installed);
	// telemetry the instruments and tracer built from them, reached through
	// tel(); otelInterceptor the RPC-layer instrumentation Handler mounts. All
	// are fixed once New returns.
	providers       telemetry.Providers
	telemetry       *serverTelemetry
	telOnce         sync.Once
	otelInterceptor connect.Interceptor
	// textCacheBytes is the configured budget; textCache is what New builds
	// from it, and is nil when the budget is zero (WithTextCache(0)).
	textCacheBytes int64
	textCache      *textDocCache
	// regMu serializes catalog swaps so concurrent registrations don't clobber
	// each other's clone-then-store.
	regMu sync.Mutex
	// pending* hold what the declaration options recorded. New applies them
	// once the catalog exists and then releases them: they are construction
	// state, not part of a running server.
	pendingEntities []proto.Message
	pendingMutators []pendingMutator
	pendingMergers  []pendingMerger
	// historyFilter is the WithHistoryFilter hook, nil when none was installed.
	// Fixed once New returns — it is a func, like the mutator table — which is
	// what lets the capture path read it without a lock.
	historyFilter HistoryFilterFunc
	// historySession is the WithHistorySession window, resolved by
	// historySessionWindow. Fixed once New returns.
	historySession time.Duration
	// historyOpen notes which history record each row currently has open, so a
	// run of writes folds into one. A hint the capture path re-validates against
	// storage, never authority — see historySessions.
	historyOpen historySessions
	// poison dedupes the quarantine log for rows that will not decode (see
	// poison.go).
	poison poisonReports
	// subjects and globalRoles are the WithSubjects and WithGlobalRoles hooks,
	// nil when not installed. Fixed once New returns.
	subjects    SubjectsFunc
	globalRoles GlobalRolesFunc
}

// catalogPtr returns the current immutable catalog snapshot.
func (s *Server) catalogPtr() *catalog { return s.cat.Load() }

// log returns the server's logger, falling back to slog.Default() so a Server
// constructed directly (e.g. in internal tests) rather than via New never
// panics on a nil logger.
func (s *Server) log() *slog.Logger {
	if s.logger != nil {
		return s.logger
	}
	return slog.Default()
}

// SchemaRevision returns the schema revision the server currently advertises
// to clients — the WithSchemaRevision token on the compiled path, or the hash
// derived from the last registered descriptor set on the runtime path. Empty
// when nothing has been advertised.
func (s *Server) SchemaRevision() string { return s.cat.Load().schemaRev }

// swapCatalog clones the current catalog, lets build mutate the clone, and
// atomically publishes it. build runs under regMu; on error nothing is
// published.
func (s *Server) swapCatalog(build func(*catalog) error) error {
	s.regMu.Lock()
	defer s.regMu.Unlock()
	nc := s.cat.Load().clone()
	if err := build(nc); err != nil {
		return err
	}
	s.cat.Store(nc)
	return nil
}

// Option configures a Server at construction. Two kinds travel through the
// same type: settings (WithPermissions, WithLimits, …), applied in the order
// given so a later one overrides an earlier one; and declarations
// (WithEntities, WithMutator, WithMerger), which only record intent and are
// installed by New in dependency order once the server exists. Declarations
// are therefore order-independent among themselves — WithMerger may name an
// entity a later WithEntities declares.
type Option func(*Server) error

// WithPermissions installs permission hooks (default: allow all).
func WithPermissions(p Permissions) Option {
	return func(s *Server) error { s.perms = p; return nil }
}

// WithNotifier replaces the in-process change-notification fan-out (e.g.
// NATS-backed).
//
// A replacement should also implement pubsub.Activity, or SubscribeActivity
// stops answering and per-workspace background consumers fall back to sweeping
// on a timer to notice work another node's writes created.
//
// One notifier may be shared by every app a Manager hosts: the server prefixes
// the ids it publishes and subscribes to with its own share of the
// workspace-id space, so two apps that both have a workspace "team" do not
// wake each other's clients.
func WithNotifier(n pubsub.Notifier) Option {
	return func(s *Server) error { s.notifier = n; return nil }
}

// WithPresenceBus replaces the in-process fan-out for the ephemeral plane —
// rooms, presence and broadcast. The default is presence.NewLocal(), which
// reaches only this process, so a multi-node deployment that leaves it in place
// shows each client the participants its own node happens to be serving.
//
// engine/presence/pgpresence is one over Postgres LISTEN/NOTIFY. One bus may
// be shared by every app a Manager hosts, on the same terms as a notifier: the
// room registry prefixes the topics it puts on the bus with the server's own
// share of the workspace-id space.
func WithPresenceBus(b presence.Bus) Option {
	return func(s *Server) error {
		if b != nil {
			s.presenceBus = b
		}
		return nil
	}
}

// WithRoomAnnounce sets how often a node re-announces the presence records of
// the sessions whose Watch streams it holds (default 5s), and with it how
// quickly a session that vanished without saying so disappears from everyone
// else's roster — three announce intervals.
//
// It is the ephemeral plane's whole liveness mechanism, so it trades screen
// accuracy against bus traffic: the cost is one event per locally-held record
// per interval, and the benefit is a shorter ghost.
func WithRoomAnnounce(d time.Duration) Option {
	return func(s *Server) error {
		if d > 0 {
			s.roomAnnounce = d
		}
		return nil
	}
}

// WithCursorStateStore installs the store that keeps the membership tail of
// issued cursors — the subscription set and the windowed/closure key-sets —
// so a cursor on the wire is a fixed-size reference instead of restating
// everything the client holds. See the cursorstate package for the contract:
// entries are immutable, eviction is licensed, and a client whose entry was
// evicted is reset onto a fresh bootstrap.
//
// Without this option the server asks its storage adapter for a store
// (cursorstate.Provider), so adapters that can back one give slim cursors
// with no configuration. Passing nil disables the indirection entirely:
// every cursor inlines its sets, which is also the fallback whenever a
// store write fails.
func WithCursorStateStore(st cursorstate.Store) Option {
	return func(s *Server) error { s.cursorState = st; s.cursorStateSet = true; return nil }
}

// WithLeases installs the lease.Locker the server and its background
// consumers dedup per-workspace work with: the GC and index sweeps skip a
// workspace whose lease another node holds, and an effects runtime built on
// this server leases each (consumer, workspace) drain the same way.
//
// Without this option the server asks its storage adapter for a locker
// (lease.Provider) — both shipped adapters provide one — and falls back to a
// process-local locker, which dedups nothing across nodes. Leases are
// advisory: a locker error
// makes the guarded work proceed unleased, so the option changes how often
// idempotent work duplicates, never whether it happens.
func WithLeases(l lease.Locker) Option {
	return func(s *Server) error {
		if l == nil {
			return fmt.Errorf("server: WithLeases requires a locker")
		}
		s.leases = l
		return nil
	}
}

// WithHeartbeat sets the Subscribe stream heartbeat interval (default 30s).
// Zero or negative keeps the default.
//
// The heartbeat is also when a live stream re-checks workspace access, so this
// bounds how long a revoked member keeps receiving a workspace's changes.
// Shortening it tightens that window at the cost of one CanAccessWorkspace call
// per live workspace per tick, per stream.
func WithHeartbeat(d time.Duration) Option {
	return func(s *Server) error {
		if d > 0 {
			s.heartbeat = d
		}
		return nil
	}
}

// WithNotifyCoalescing sets the per-workspace change-notification coalescing
// window (default 50ms). The first change in a quiet workspace notifies
// immediately; further changes within the window collapse into one trailing
// notification, capping the rate at which a workspace can wake its subscribers
// however write-heavy it gets. 0 disables coalescing. Applies on the send side
// of the notifier, so a shared notifier installed via WithNotifier also carries
// less traffic.
func WithNotifyCoalescing(window time.Duration) Option {
	return func(s *Server) error { s.notifyWindow = window; return nil }
}

// WithLimits overrides request/response bounds. Fields that are not positive
// keep their defaults.
func WithLimits(l Limits) Option {
	return func(s *Server) error { s.limits = l.withDefaults(); return nil }
}

// WithSchemaRevision advertises an app-defined schema/protocol revision to
// clients. It is an opaque token (a build hash, a monotonic schema number,
// whatever the app picks) echoed on every response that carries one: stream
// frames, push, pull, query, and bootstrap Begin.
// The server does not interpret it — clients configured with their own
// revision compare the two and react to a mismatch per their skew policy.
// Empty (the default) advertises nothing, so older clients and servers
// behave exactly as before. During a rolling deploy each node should
// advertise the revision of the code it is running, so a client pinned to one
// node sees a stable value.
func WithSchemaRevision(rev string) Option {
	return func(s *Server) error { s.baseSchemaRev = rev; return nil }
}

// WithLogger sets the structured logger the engine's background work (the GC
// loop) writes to. Records are emitted with a "component" attribute so they
// can be filtered. The default is slog.Default(); pass a logger backed by a
// discard handler to silence the engine entirely.
func WithLogger(l *slog.Logger) Option {
	return func(s *Server) error {
		if l != nil {
			s.logger = l
		}
		return nil
	}
}

// WithChunkSize sets the bootstrap chunk size in rows (default 256).
func WithChunkSize(n int) Option {
	return func(s *Server) error {
		if n > 0 {
			s.chunkSize = n
		}
		return nil
	}
}

// WithEntities declares syncable entities: message types carrying
// (syncplane.v1.entity) options. New registers them in the order given and
// returns the first that does not validate, so a whole schema costs one error
// check instead of N. Repeatable; entity names must be unique across all of
// them.
//
// This is the compiled path — schemas known when the binary is built. A schema
// that arrives later goes through RegisterDescriptorSet or
// RegisterDynamicEntities on the live server instead.
func WithEntities(msgs ...proto.Message) Option {
	return func(s *Server) error {
		s.pendingEntities = append(s.pendingEntities, msgs...)
		return nil
	}
}

// WithMerger declares a merge hook for an opaque field of an entity declared
// on the same server. fieldPath is the dotted proto path ("doc",
// "outer.doc"); it must resolve to a singular bytes or message field marked
// (syncplane.v1.field_behavior).opaque.
//
// The hook fires only for PATCH mutations whose mask touches the field while
// BOTH the stored row and the incoming value carry it — PUT (and
// EntityTx.Put) always replace whole rows, and one-sided PATCHes keep plain
// LWW replace/clear semantics.
//
// Mergers are construction-time only: they are funcs, so unlike a schema they
// cannot be provisioned at runtime (see DynamicEntity, the CRUD-only dynamic
// tier).
func WithMerger(prototype proto.Message, fieldPath string, fn MergeFunc) Option {
	return func(s *Server) error {
		s.pendingMergers = append(s.pendingMergers, pendingMerger{prototype: prototype, fieldPath: fieldPath, fn: fn})
		return nil
	}
}

// New builds a sync engine over store. Everything the options declare —
// entities, mutators, mergers — is installed before it returns, so a
// misdeclared schema surfaces as one error here, and nothing (the GC loop
// included) starts unless the whole declaration is good.
//
// New does NOT take ownership of store. Close stops the engine's background
// work; closing the adapter stays the caller's job, because it may outlive the
// server and a Manager shares one root adapter across every app it hosts. The
// usual bring-up is therefore two defers:
//
//	store, err := sqlitestore.Open(path)
//	…
//	defer store.Close()
//	srv, err := server.New(store, server.WithEntities(&todov1.Todo{}))
//	…
//	defer srv.Close()
func New(store storage.Adapter, opts ...Option) (*Server, error) {
	s := &Server{
		store:          store,
		mutators:       make(map[string]*mutatorInfo),
		perms:          AllowAll{},
		notifier:       pubsub.NewLocal(),
		presenceBus:    presence.NewLocal(),
		roomAnnounce:   presence.DefaultAnnounce,
		heartbeat:      30 * time.Second,
		notifyWindow:   50 * time.Millisecond,
		chunkSize:      256,
		limits:         Limits{}.withDefaults(),
		logger:         slog.Default(),
		textCacheBytes: DefaultTextCacheBytes,
		indexBuild:     &indexBuildLoop{interval: defaultIndexBuildInterval, batch: defaultIndexBuildBatch},
		compact:        &compactLoop{interval: defaultTextCompactionInterval, batch: defaultTextCompactionBatch},
	}
	for _, opt := range opts {
		if err := opt(s); err != nil {
			return nil, err
		}
	}
	if !s.cursorStateSet {
		if p, ok := storage.As[cursorstate.Provider](store); ok {
			s.cursorState = p.CursorStateStore()
		}
	}
	if s.leases == nil {
		if p, ok := storage.As[lease.Provider](store); ok {
			s.leases = p.Leases()
		} else {
			s.leases = lease.NewLocal()
		}
	}
	// No option gates this: like leases, whether reads may leave the primary
	// is adapter configuration, and an adapter without a replica returns nil.
	if p, ok := storage.As[storage.ReplicaProvider](store); ok {
		s.replicaReads = p.ReplicaReader()
	}
	// Telemetry is resolved before anything can record on it: the background
	// loops below start inside New, and a nil instrument set would be a panic
	// on the first tick rather than an error here.
	mx, err := newMetrics(s.providers)
	if err != nil {
		return nil, fmt.Errorf("server: telemetry: %w", err)
	}
	s.telemetry = &serverTelemetry{metrics: mx, tracer: s.providers.Tracer()}
	if s.otelInterceptor, err = rpcInterceptor(s.providers); err != nil {
		return nil, fmt.Errorf("server: telemetry: %w", err)
	}
	s.textCache = newTextDocCache(s.textCacheBytes)
	s.cat.Store(&catalog{
		entities:  make(map[string]*entityInfo),
		byType:    make(map[protoreflect.FullName]*entityInfo),
		schemaRev: s.baseSchemaRev,
	})
	// The prefix goes on first, below every other wrapper, so that everything
	// in this server deals in the bare workspace ids its callers use.
	s.notifier = pubsub.Prefixed(s.notifier, s.wsPrefix)
	// Counters on both sides of the coalescing wrapper, so the difference
	// between them is exactly what coalescing absorbed. Inner goes on first so
	// it sits below the wrapper and sees only what got through.
	s.notifier = &countingNotifier{inner: s.notifier, count: s.tel().notifyPublished}
	s.notifier = pubsub.NewCoalescing(s.notifier, s.notifyWindow)
	s.coalescer, _ = s.notifier.(*pubsub.Coalescing)
	s.notifier = &countingNotifier{inner: s.notifier, count: s.tel().notifyRequested}
	s.rooms = presence.NewRegistry(s.presenceBus, presence.Options{
		Announce:        s.roomAnnounce,
		MaxParticipants: s.limits.MaxParticipantsPerRoom,
		WorkspacePrefix: s.wsPrefix,
	})
	s.roomSessions = newSessionBindings()
	s.roomAnchors = newAnchorCache(s.heartbeat)
	if err := s.applyDeclarations(); err != nil {
		return nil, err
	}
	// Entities register one at a time and mutators after them, so the model is
	// judged once both are in.
	if err := s.swapCatalog(func(c *catalog) error {
		c.recompileAccess(s.mutators, s.limits)
		return c.accessErr
	}); err != nil {
		return nil, err
	}
	s.logAccessWarnings(s.catalogPtr())
	if s.gc != nil {
		s.gc.start(s)
	}
	s.indexBuild.start(s)
	s.compact.start(s)
	s.roomSweep = startRoomSweeper(s)
	return s, nil
}

// Leases returns the lease.Locker this server dedups per-workspace background
// work with — the one WithLeases installed, or the resolved default. Never
// nil. Background consumers built on the server (an effects runtime, an
// application worker reacting to this server's workspaces) should lease their
// per-workspace work here so every process sharing the store dedups in one
// space; the lease package documents role naming and what a lease does and
// does not guarantee.
func (s *Server) Leases() lease.Locker {
	return s.leases
}

// applyDeclarations installs what the declaration options recorded, in
// dependency order rather than in the order they were written: entities
// first, because a merger resolves its field against a registered entity, then
// mutators, then mergers. That is what lets an app list its options in
// whatever order reads best.
func (s *Server) applyDeclarations() error {
	for _, msg := range s.pendingEntities {
		if err := s.RegisterEntity(msg); err != nil {
			return err
		}
	}
	for _, m := range s.pendingMutators {
		if err := s.registerMutator(m.name, m.args, m.fn, m.opts...); err != nil {
			return err
		}
	}
	for _, m := range s.pendingMergers {
		if err := s.registerMerger(m.prototype, m.fieldPath, m.fn); err != nil {
			return err
		}
	}
	s.pendingEntities, s.pendingMutators, s.pendingMergers = nil, nil, nil
	return nil
}

// interceptors returns the chain every mounted service shares, outermost
// first.
//
// Authentication (when a WithAuthenticator is installed) runs inside the
// logging interceptor: a rejected request is still logged, with its
// unauthenticated code, and the handler never sees it.
//
// Telemetry sits outside even the logging interceptor. It extracts the incoming
// W3C trace context and opens the RPC span, so everything further in — the auth
// rejection, the handler's own spans, and the trace_id the log lines carry —
// belongs to one trace rather than starting a second one.
func (s *Server) interceptors() []connect.Interceptor {
	interceptors := []connect.Interceptor{}
	if s.otelInterceptor != nil {
		interceptors = append(interceptors, s.otelInterceptor)
	}
	interceptors = append(interceptors, loggingInterceptor(s.log()))
	if s.authn != nil {
		interceptors = append(interceptors, &authInterceptor{authn: s.authn, log: s.log()})
	}
	return interceptors
}

// Handler returns the mount path and HTTP handler for the sync service.
// HTTP/1.1 suffices (unary + server-streaming only). WithReadMaxBytes is the
// front door for MaxRequestBytes: it bounds every request body (and its
// decompressed size) before the body is buffered or unmarshaled, so the
// documented per-request limits no longer sit behind an unbounded read.
//
// The returned handler gzips streaming responses for clients that ask for it
// with Accept-Encoding, which is how a browser asks — see compress.go for why
// Connect's own compression does not cover them.
//
// A server mounted this way serves the sync plane and nothing else. Use
// Handlers to mount every service it offers, rooms included.
func (s *Server) Handler() (string, http.Handler) {
	path, handler := syncplanev1connect.NewSyncServiceHandler(s,
		connect.WithReadMaxBytes(s.limits.MaxRequestBytes),
		connect.WithInterceptors(s.interceptors()...))
	return path, compressStreams(handler)
}

// PresenceHandler returns the mount path and HTTP handler for the presence
// service — the ephemeral plane, see engine/presence.
//
// It shares the sync service's interceptor chain and request-size bound, and
// deliberately does NOT gzip its stream: room frames are small and
// latency-sensitive, and a gzip writer would cost a few hundred KB of deflate
// state for every watching client.
func (s *Server) PresenceHandler() (string, http.Handler) {
	return syncplanev1connect.NewPresenceServiceHandler(s,
		connect.WithReadMaxBytes(s.limits.MaxRequestBytes),
		connect.WithInterceptors(s.interceptors()...))
}

// Handlers returns every Connect service this server mounts, keyed by mount
// path. Mount all of them on a mux that also carries the app's own routes:
//
//	for path, h := range srv.Handlers() {
//		mux.Handle(path, h)
//	}
//
// Ranging over this rather than naming the services one at a time is what keeps
// a deployment from silently missing one the engine gains later.
func (s *Server) Handlers() map[string]http.Handler {
	syncPath, sync := s.Handler()
	presencePath, presence := s.PresenceHandler()
	return map[string]http.Handler{
		syncPath:     sync,
		presencePath: presence,
	}
}

// HTTPHandler returns every service already mounted at its own path, for the
// common case where this server is the only thing the listener serves:
//
//	http.ListenAndServe(addr, srv.HTTPHandler())
//
// Connect routes by full method path, so a service cannot be served at an
// arbitrary root — mounting is not optional, only who does it. Use Handlers
// instead to mount them on a mux that also carries the app's own routes.
func (s *Server) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	for path, h := range s.Handlers() {
		mux.Handle(path, h)
	}
	return mux
}

// RegisterEntity registers a message type carrying syncplane.v1.entity options as
// a syncable entity.
func (s *Server) RegisterEntity(msg proto.Message) error {
	info, err := buildEntityInfo(msg.ProtoReflect().Type())
	if err != nil {
		return err
	}
	if err := s.swapCatalog(func(c *catalog) error {
		if _, dup := c.entities[info.name]; dup {
			return fmt.Errorf("server: entity %q already registered", info.name)
		}
		c.entities[info.name] = info
		c.byType[info.msgType.Descriptor().FullName()] = info
		if err := ensureHistoryEntity(c); err != nil {
			return err
		}
		c.recompileAccess(s.mutators, s.limits)
		return nil
	}); err != nil {
		return err
	}
	s.log().LogAttrs(context.Background(), slog.LevelInfo, "entity registered",
		slog.String("component", "schema"), slog.String("entity", info.name))
	return nil
}

// missingEntityOptions explains why a message read as unannotated. A message
// that was never annotated and one annotated against a divergent
// syncplane/v1/options.proto are indistinguishable to the option read, but only the
// second leaves option bytes this process cannot interpret — so that is the
// case worth naming, since the .proto looks correct and the fix is in codegen
// configuration rather than the schema.
func missingEntityOptions(desc protoreflect.MessageDescriptor) error {
	if protoopts.HasUninterpretedOptions(desc) {
		return fmt.Errorf("server: %s carries message options this build cannot interpret and no "+
			"(syncplane.v1.entity): a vendored syncplane/v1/options.proto generated its own extension type — keep "+
			"its go_package as %q and exclude it from Go codegen", desc.FullName(), protoopts.OptionsGoPackage)
	}
	return fmt.Errorf(`server: %s lacks (syncplane.v1.entity) options: annotate the message in your .proto, `+
		`e.g. option (syncplane.v1.entity) = { name: "thing" key_fields: [ "id" ] };`, desc.FullName())
}

// buildEntityInfo validates that mt carries (syncplane.v1.entity) options and
// precomputes its key fields and reference fields. It is the shared core of
// RegisterEntity (compiled types) and RegisterDescriptorSet (runtime
// descriptor-set types); it does not touch the catalog.
func buildEntityInfo(mt protoreflect.MessageType) (*entityInfo, error) {
	desc := mt.Descriptor()
	eo := protoopts.Entity(desc)
	if eo.GetName() == "" {
		return nil, missingEntityOptions(desc)
	}
	// The name goes into wire mutations, storage and the client's KV keys, so it
	// has to satisfy the shared naming rules before anything is built from it.
	if err := naming.ValidateEntityName(eo.GetName()); err != nil {
		return nil, fmt.Errorf("server: %s: %w", desc.FullName(), err)
	}
	if len(eo.GetKeyFields()) == 0 {
		return nil, fmt.Errorf("server: entity %q declares no key_fields", eo.GetName())
	}
	info := &entityInfo{
		name:         eo.GetName(),
		msgType:      mt,
		createPolicy: eo.GetCreatePolicy(),
		updatePolicy: eo.GetUpdatePolicy(),
		deletePolicy: eo.GetDeletePolicy(),
		serverOnly:   eo.GetServerOnly(),
		history:      eo.GetHistory(),
	}
	for _, kf := range eo.GetKeyFields() {
		fd := desc.Fields().ByName(protoreflect.Name(kf))
		if fd == nil {
			return nil, fmt.Errorf("server: entity %q key field %q not found", eo.GetName(), kf)
		}
		// keyOf renders each key value as text, which only some kinds do the same
		// way in Go and JS — reject the rest at registration rather than mint a
		// key the client would derive differently.
		if err := naming.ValidateKeyFieldKind(fd); err != nil {
			return nil, fmt.Errorf("server: entity %q: %w", eo.GetName(), err)
		}
		// A key must be there on every row of the entity. A oneof member is
		// there only while its case is the active one, so the row key would
		// change identity — or collapse onto the zero value — whenever a sibling
		// won a write.
		if od := lww.OneofOf(fd); od != nil {
			return nil, fmt.Errorf("server: entity %q key field %q is a member of oneof %q; "+
				"a key field cannot be inside a oneof", eo.GetName(), kf, od.Name())
		}
		info.keyFields = append(info.keyFields, fd)
	}
	// A malformed reference message anywhere in the entity's shape is a schema
	// error, not an ordinary sub-message; report it before the field walk
	// below would silently read it as one.
	if err := checkReferenceMessages(desc); err != nil {
		return nil, fmt.Errorf("server: entity %q: %w", info.name, err)
	}
	if err := checkReverseRelations(desc); err != nil {
		return nil, fmt.Errorf("server: entity %q: %w", info.name, err)
	}
	// Precompute reference fields: any field whose (element) message is itself
	// a registered entity, or a reference message wrapping one. Detection and
	// the target's key fields come from the target message's own
	// (syncplane.v1.entity) options, so registration order does not matter (the
	// option is on the type, not on the catalog). A polymorphic reference
	// contributes one entry per case: each case has its own target, its own
	// index and its own reverse relation, and a case that is not the active one
	// holds nothing, which is what every consumer already handles for an unset
	// reference.
	fields := desc.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		refs := lww.RefsOf(fd)
		if len(refs) == 0 {
			continue
		}
		if refs[0].Value != nil {
			if b := protoopts.FieldBehavior(fd); len(b.GetKeyField()) > 0 || b.GetOrderField() != "" {
				return nil, fmt.Errorf("server: entity %q reference field %q: a reference message list has no per-element identity, so key_field and order_field do not apply", info.name, fd.Name())
			}
		}
		// A oneof member is one case of the holder's oneof; a member that is
		// itself a oneof of targets would be cases inside a case, which the
		// reference graph has no name for.
		if od := lww.OneofOf(fd); od != nil && len(refs) > 1 {
			return nil, fmt.Errorf("server: entity %q reference field %q is a member of oneof %q and names one of several targets itself; "+
				"declare the targets as members of %q instead", info.name, fd.Name(), od.Name(), od.Name())
		}
		for _, ref := range refs {
			info.refFields = append(info.refFields, refField{fd: fd, keyFields: ref.KeyFields, ref: ref})
		}
	}
	if err := validateListIdentities(desc, info.name); err != nil {
		return nil, err
	}
	access, err := accessOf(desc, eo, info)
	if err != nil {
		return nil, fmt.Errorf("server: entity %q: %w", info.name, err)
	}
	info.access = access
	if info.grant, err = grantOf(desc, eo, info.access); err != nil {
		return nil, fmt.Errorf("server: entity %q: %w", info.name, err)
	}
	// Derive one index per reference field, over the target key the row
	// stores. This is not a schema decision: a reverse lookup is what every
	// follow spec does on every pull, the engine already knows the exact
	// columns it reads, and an index nobody declared costs nothing to a schema
	// that never reads reverse.
	if err := buildRefIndexes(info); err != nil {
		return nil, fmt.Errorf("server: entity %q: %w", info.name, err)
	}
	// Then the schema's own: a field-level `indexed` mark and an entity-level
	// `index` declaration desugar to the same compiled shape.
	if err := buildDeclaredIndexes(info, eo); err != nil {
		return nil, fmt.Errorf("server: entity %q: %w", info.name, err)
	}
	if err := buildGrantIndex(info); err != nil {
		return nil, fmt.Errorf("server: entity %q: %w", info.name, err)
	}
	// Search: a field-level `searchable` mark and an entity-level `search`
	// declaration compile to one declaration, like the indexes above.
	decl, err := query.CompileSearchDecl(desc)
	if err != nil {
		return nil, fmt.Errorf("server: entity %q: %w", info.name, err)
	}
	if decl != nil {
		info.search = &searchIndex{name: searchIndexName, specHash: decl.SearchSpecHash(), decl: decl}
	}
	// Discover (syncplane.v1.field_behavior).text fields and auto-install the
	// built-in convergent-text merger for each — no RegisterMerger call
	// needed, and PATCHes to the field merge instead of replacing.
	if err := collectTextFields(desc, "", "", "", nil, &info.textFields); err != nil {
		return nil, fmt.Errorf("server: entity %q: %w", info.name, err)
	}
	if len(info.textFields) > 0 {
		info.mergers = lww.Mergers{}
		for _, tf := range info.textFields {
			// MergeAbsentBase: a text PATCH always goes through the merger, even
			// onto a row that does not carry the field yet. An empty document is
			// the identity of the union, so the result is unchanged for a
			// full-state value — but a delta-state fragment MUST be decoded and
			// applied rather than stored raw, and "the row has no document yet"
			// is exactly when a self-contained fragment arrives.
			info.mergers[tf.path] = lww.Merger{Fn: text.Merge, MergeAbsentBase: true}
		}
	}
	protected, err := protectedFieldsOf(desc, info.keyFields)
	if err != nil {
		return nil, fmt.Errorf("server: entity %q: %w", info.name, err)
	}
	info.protectedFields = protected
	if info.history != nil {
		info.historyFields = historyFieldsOf(desc, info.keyFields)
		if len(info.historyFields) == 0 {
			// Refused rather than treated as "capture everything" or "capture
			// nothing": both readings are plausible, and each is a trap in one
			// direction — silent unbounded growth, or a history panel that stays
			// empty forever with nothing to point at.
			return nil, fmt.Errorf("server: entity %q opts into history but marks no field with "+
				"(syncplane.v1.field_behavior).history; capture is explicit", info.name)
		}
	}
	return info, nil
}

// validateListIdentities refuses a keyed-list identity (key_field or
// order_field) that names a member of the element's oneof. An identity that is
// only present while its case is active is not an identity — elements would
// collide on the zero value the moment a sibling case won — and
// lww.keyFieldsFor would treat it as no identity at all, silently downgrading
// the list to whole-replace. The client's validateOneofs is the mirror.
func validateListIdentities(desc protoreflect.MessageDescriptor, entity string) error {
	fields := desc.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if !fd.IsList() || fd.Kind() != protoreflect.MessageKind {
			continue
		}
		b := fieldBehaviorOf(fd)
		identity := b.GetKeyField()
		if of := b.GetOrderField(); of != "" {
			identity = append(identity[:len(identity):len(identity)], of)
		}
		for _, name := range identity {
			member := fd.Message().Fields().ByName(protoreflect.Name(name))
			if member == nil {
				continue
			}
			if od := lww.OneofOf(member); od != nil {
				return fmt.Errorf("server: entity %q list field %q: %q is a member of oneof %q; "+
					"a list identity cannot be inside a oneof", entity, fd.Name(), name, od.Name())
			}
		}
	}
	return nil
}

// historyFieldsOf resolves an entity's capture set: the top-level units a
// history record may name, which are exactly the fields marked
// (field_behavior).history.
//
// Capture is explicit — there is no "opt the entity in and get everything"
// fallback — because what a field costs to record spans orders of magnitude. A
// scalar's before-state is a few bytes; a collaborative-text field's is the
// document as it stood, recorded again on every write that touches it. An
// implicit capture set would opt a schema into that by ADDING a field, which is
// the one schema change the engine otherwise treats as always safe. Marking is
// cheap and says what the app meant.
//
// Returning empty is therefore a schema error rather than a mode, and the caller
// raises it: an entity that opts in and marks nothing has asked for a log that
// can never say anything.
//
// A oneof contributes its GROUP name, once, however many members it has. That
// is the only form a mask may name it in (see engine/internal/lww), and the
// capture set has to speak the same language: it becomes a record's mask, and a
// mask the merge refuses could neither record a prior value nor restore one.
// Marking any member therefore marks the group.
//
// Key fields are dropped however they are marked. A row's identity lives in
// both its storage key and its embedded key fields and the two must agree, so
// the write paths reject a PATCH whose mask would move one; a restore built
// from a mask naming a key field could therefore never be applied. The recorded
// prior value still carries them (see historyPrior), which is what a decoded
// record needs to say which row it describes.
func historyFieldsOf(desc protoreflect.MessageDescriptor, keyFields []protoreflect.FieldDescriptor) []string {
	isKey := make(map[protoreflect.Name]bool, len(keyFields))
	for _, fd := range keyFields {
		isKey[fd.Name()] = true
	}
	fields := desc.Fields()
	var marked []string
	seen := make(map[string]bool, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if isKey[fd.Name()] || !fieldBehaviorOf(fd).GetHistory() {
			continue
		}
		name := string(fd.Name())
		if od := lww.OneofOf(fd); od != nil {
			name = string(od.Name())
		}
		if seen[name] {
			continue
		}
		seen[name] = true
		marked = append(marked, name)
	}
	return marked
}

// protectedFieldsOf resolves an entity's protected set: the top-level units a
// client-authorized write may not change, which are exactly the fields marked
// (field_behavior).write_policy TRUSTED_ONLY.
//
// A oneof contributes its GROUP name, once, however many members are marked.
// That is the only form a mask may name it in (see engine/internal/lww), and a
// group is replaced whole, so protecting one member and not its siblings would
// be a rule no write could observe. Marking any member therefore marks the
// group.
//
// A marked key field is refused rather than dropped. History drops one because
// a capture set that cannot be masked is still a coherent thing to ask for; here
// the mark states a restriction, and one that can never take effect — a PATCH
// may not move a key field, and a PUT that changed one would address a different
// row — is a schema mistake worth naming.
func protectedFieldsOf(
	desc protoreflect.MessageDescriptor,
	keyFields []protoreflect.FieldDescriptor,
) ([]string, error) {
	isKey := make(map[protoreflect.Name]bool, len(keyFields))
	for _, fd := range keyFields {
		isKey[fd.Name()] = true
	}
	fields := desc.Fields()
	var marked []string
	seen := make(map[string]bool, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if fieldBehaviorOf(fd).GetWritePolicy() != syncplanev1.FieldWritePolicy_FIELD_WRITE_POLICY_TRUSTED_ONLY {
			continue
		}
		if isKey[fd.Name()] {
			return nil, fmt.Errorf("key field %q is marked (syncplane.v1.field_behavior).write_policy "+
				"TRUSTED_ONLY; a key field can never be modified in place, so the mark cannot take effect", fd.Name())
		}
		name := string(fd.Name())
		if od := lww.OneofOf(fd); od != nil {
			name = string(od.Name())
		}
		if seen[name] {
			continue
		}
		seen[name] = true
		marked = append(marked, name)
	}
	return marked, nil
}

// collectTextFields walks a message's fields (descending through singular,
// non-opaque, non-reference message fields, keyed message lists and
// message-valued maps) and records every text field, validating its shape: a
// singular bytes field without key_field/order_field. steps is the path walked
// so far (cycle guard — a recursive message type cannot carry a text field
// through itself anyway).
//
// A text field below a keyed list or map is accepted: the element identity —
// the list key or the map key — is stable, a mask path can address the element
// ("items.<key>.body"), and the merge descends into it, so the merger fires per
// element and the field holds one document per element.
//
// oneof names the group crossed on the way here, and unkeyed the message list
// with no element identity (both empty when none). The walk still descends
// through oneof members and unkeyed list elements so that a text field below
// one is reported rather than quietly skipped: its merger could never fire,
// because a oneof is replaced whole, and an element no mask can address is too.
func collectTextFields(
	desc protoreflect.MessageDescriptor,
	prefix string,
	oneof string,
	unkeyed string,
	steps []textStep,
	out *[]textField,
) error {
	fields := desc.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		behavior := fieldBehaviorOf(fd)
		memberOf := oneof
		if od := lww.OneofOf(fd); od != nil {
			memberOf = string(od.Name())
		}
		if behavior.GetText() {
			if fd.IsList() || fd.IsMap() || fd.Kind() != protoreflect.BytesKind {
				return fmt.Errorf("text field %q must be a singular bytes field", prefix+string(fd.Name()))
			}
			if len(behavior.GetKeyField()) > 0 || behavior.GetOrderField() != "" {
				return fmt.Errorf("text field %q cannot combine with key_field/order_field", prefix+string(fd.Name()))
			}
			if memberOf != "" {
				return fmt.Errorf("text field %q is inside oneof %q; a oneof is replaced whole, "+
					"so the text merger could never run", prefix+string(fd.Name()), memberOf)
			}
			if unkeyed != "" {
				return fmt.Errorf("text field %q is inside %s, whose elements have no identity; "+
					"an element is replaced whole, so the text merger could never run — "+
					"give the list a key_field to hold one document per element",
					prefix+string(fd.Name()), unkeyed)
			}
			fieldSteps := make([]textStep, len(steps)+1)
			copy(fieldSteps, steps)
			fieldSteps[len(steps)] = textStep{fd: fd}
			*out = append(*out, newTextField(prefix+string(fd.Name()), fieldSteps))
			continue
		}
		// Descend into embedded sub-messages and collection elements — unkeyed
		// list elements purely to refuse text there. Opaque messages are
		// skipped either way: they are replaced whole, so a nested merger could
		// never fire — and references, which project down to key fields.
		// Recursive types terminate because a message cannot contain itself as
		// a required finite path — guard by refusing to re-enter a type already
		// on the descent path.
		var sub protoreflect.MessageDescriptor
		enclosing := unkeyed
		step := textStep{fd: fd}
		switch {
		case behavior.GetOpaque() || lww.IsRef(fd):
		case fd.IsMap():
			if fd.MapValue().Kind() == protoreflect.MessageKind {
				sub = fd.MapValue().Message()
			}
		case fd.IsList():
			if fd.Kind() == protoreflect.MessageKind {
				sub = fd.Message()
				step.keyFields = lww.KeyFieldsFor(fd)
				if len(step.keyFields) == 0 {
					enclosing = fmt.Sprintf("repeated field %q", prefix+string(fd.Name()))
				}
			}
		case fd.Kind() == protoreflect.MessageKind:
			sub = fd.Message()
		}
		if sub == nil || onChain(steps, sub.FullName()) {
			continue
		}
		if err := collectTextFields(sub, prefix+string(fd.Name())+".",
			memberOf, enclosing, append(steps, step), out); err != nil {
			return err
		}
	}
	return nil
}

// onChain reports whether a message type is already being descended through.
// The type compared is the one the walk enters: a map field's value message,
// not its synthetic entry message.
func onChain(steps []textStep, name protoreflect.FullName) bool {
	for _, st := range steps {
		fd := st.fd
		var md protoreflect.MessageDescriptor
		switch {
		case fd.IsMap():
			if fd.MapValue().Kind() == protoreflect.MessageKind {
				md = fd.MapValue().Message()
			}
		case fd.Kind() == protoreflect.MessageKind:
			md = fd.Message()
		}
		if md != nil && md.FullName() == name {
			return true
		}
	}
	return false
}

// fieldBehaviorOf reads the (syncplane.v1.field_behavior) extension, or nil.
func fieldBehaviorOf(fd protoreflect.FieldDescriptor) *syncplanev1.FieldBehavior {
	return protoopts.FieldBehavior(fd)
}

// MergeFunc combines the stored serialization of an opaque field with an
// incoming PATCH value (e.g. a CRDT merge). base and incoming are the raw
// field values: the bytes content for bytes fields, the marshaled message
// for message fields.
type MergeFunc func(base, incoming []byte) ([]byte, error)

// pendingMerger is one WithMerger declaration, held until New has a catalog to
// resolve it against.
type pendingMerger struct {
	prototype proto.Message
	fieldPath string
	fn        MergeFunc
}

// registerMerger installs a WithMerger declaration against the live catalog.
// See WithMerger for the semantics.
func (s *Server) registerMerger(prototype proto.Message, fieldPath string, fn MergeFunc) error {
	if fn == nil {
		return fmt.Errorf("server: merger for %q needs a func", fieldPath)
	}
	desc := prototype.ProtoReflect().Descriptor()
	info, ok := s.catalogPtr().byType[desc.FullName()]
	if !ok {
		return errNotRegistered(desc.FullName())
	}
	fd, err := resolveMergerField(desc, fieldPath)
	if err != nil {
		return fmt.Errorf("server: merger for %s.%s: %w", info.name, fieldPath, err)
	}
	if !fieldIsOpaque(fd) {
		return fmt.Errorf("server: merger for %s.%s: field is not opaque", info.name, fieldPath)
	}
	switch {
	case fd.IsList() || fd.IsMap():
		return fmt.Errorf("server: merger for %s.%s: repeated and map fields are not supported", info.name, fieldPath)
	case fd.Kind() != protoreflect.BytesKind && fd.Kind() != protoreflect.MessageKind:
		return fmt.Errorf("server: merger for %s.%s: field must be bytes or message, is %s", info.name, fieldPath, fd.Kind())
	}
	if _, dup := info.mergers[fieldPath]; dup {
		return fmt.Errorf("server: merger for %s.%s already registered", info.name, fieldPath)
	}
	// Copy-on-write: clone the entityInfo with the new merger rather than
	// mutating the published one, and swap a fresh catalog into place.
	return s.swapCatalog(func(c *catalog) error {
		cur, ok := c.byType[desc.FullName()]
		if !ok {
			return errNotRegistered(desc.FullName())
		}
		ni := *cur
		ni.mergers = lww.Mergers{}
		for k, v := range cur.mergers {
			ni.mergers[k] = v
		}
		ni.mergers[fieldPath] = lww.Merger{Fn: lww.MergeFunc(fn)}
		c.entities[ni.name] = &ni
		c.byType[desc.FullName()] = &ni
		return nil
	})
}

// resolveMergerField walks a dotted path; intermediates must be singular,
// non-opaque message fields (a merger nested under an opaque field could
// never fire — the ancestor is replaced whole).
func resolveMergerField(desc protoreflect.MessageDescriptor, fieldPath string) (protoreflect.FieldDescriptor, error) {
	segs := strings.Split(fieldPath, ".")
	var fd protoreflect.FieldDescriptor
	for i, seg := range segs {
		if desc == nil {
			return nil, fmt.Errorf("%q is not a message field", segs[i-1])
		}
		fd = desc.Fields().ByName(protoreflect.Name(seg))
		if fd == nil {
			return nil, fmt.Errorf("unknown field %q", seg)
		}
		// A oneof is replaced whole, so a merger anywhere inside one could never
		// fire — the same reason mergers cannot nest under an opaque field.
		if od := lww.OneofOf(fd); od != nil {
			return nil, fmt.Errorf("%q is inside oneof %q; a oneof is replaced whole, "+
				"so a merger there could never run", seg, od.Name())
		}
		if i < len(segs)-1 {
			if fd.Kind() != protoreflect.MessageKind || fd.IsList() || fd.IsMap() {
				return nil, fmt.Errorf("%q is not a singular message field", seg)
			}
			if fieldIsOpaque(fd) {
				return nil, fmt.Errorf("%q is opaque; mergers cannot nest under opaque fields", seg)
			}
			desc = fd.Message()
		}
	}
	return fd, nil
}

func fieldIsOpaque(fd protoreflect.FieldDescriptor) bool {
	behavior := fieldBehaviorOf(fd)
	// text implies opaque: the encoded document is one atomic value (its
	// merger is auto-installed at registration, so a manual RegisterMerger on
	// it reports "already registered").
	return behavior.GetOpaque() || behavior.GetText()
}

// WorkspaceIDs lists the workspaces that hold persisted state, delegating to
// the storage adapter. Used by background consumers (connectors, outbox
// drainers) to discover work. A workspace only ever named by reads — a pull
// or a query against an id nobody has written — is not in the list. The list
// may be boundedly stale (an adapter may answer it from a read replica): a
// just-written workspace appears on a later call, so poll rather than assume
// one call is exhaustive.
func (s *Server) WorkspaceIDs(ctx context.Context) ([]string, error) {
	return s.store.ListWorkspaceIDs(ctx, storage.Range{})
}

// EntityKey derives the wire entity name and row key from a registered
// entity message — the same derivation push and EntityTx use, exposed so
// out-of-process integrations (connectors, outbox consumers) can address
// rows without reimplementing the key-field escaping rules.
func (s *Server) EntityKey(msg proto.Message) (entity, key string, err error) {
	desc := msg.ProtoReflect().Descriptor()
	info, ok := s.catalogPtr().byType[desc.FullName()]
	if !ok {
		return "", "", errNotRegistered(desc.FullName())
	}
	return info.name, info.keyOf(msg.ProtoReflect()), nil
}

// EntityMeta is an exported view of a registered entity, for trusted
// introspection (the admin UI). It surfaces the real registered descriptor and
// key fields — the single source of truth — so reflection-driven callers never
// rebuild a parallel descriptor.
type EntityMeta struct {
	// Name is the wire/storage entity name (CrudMutation.entity).
	Name string
	// Descriptor is the entity message's descriptor, the basis for a generic
	// reflective form/renderer.
	Descriptor protoreflect.MessageDescriptor
	// KeyFields are the fields (in order) whose values form the row key. They
	// are identity, so an editor renders them read-only on edit.
	KeyFields []protoreflect.FieldDescriptor
	// Managed reports an entity the engine registers and writes for itself
	// rather than one the app declared — today only the field-history log. Its
	// rows are engine state whose meaning comes from the writes that produced
	// them, so a generic editor must not offer them: writing one by hand claims
	// a field held a value it never held, and deleting one loses the only copy
	// of a value the row no longer carries. A browser may still read them, and
	// the RPC surface refuses client writes of them outright.
	Managed bool
}

// Entities returns metadata for every registered entity, sorted by name.
func (s *Server) Entities() []EntityMeta {
	c := s.catalogPtr()
	names := c.entityNames()
	metas := make([]EntityMeta, 0, len(names))
	for _, n := range names {
		metas = append(metas, entityMeta(c.entities[n]))
	}
	return metas
}

// Entity returns metadata for the entity with the given wire name, and whether
// it is registered.
func (s *Server) Entity(name string) (EntityMeta, bool) {
	info, ok := s.catalogPtr().entities[name]
	if !ok {
		return EntityMeta{}, false
	}
	return entityMeta(info), true
}

// entityMeta projects an internal entityInfo into the exported view.
func entityMeta(info *entityInfo) EntityMeta {
	return EntityMeta{
		Name:       info.name,
		Descriptor: info.msgType.Descriptor(),
		KeyFields:  info.keyFields,
		Managed:    info.name == historyEntityName,
	}
}

// RefKeyFields classifies a field as a typed reference to one entity: it
// returns the target entity's key fields when fd is a reference (a message
// field whose target message carries (syncplane.v1.entity), or a reference
// message wrapping one), else nil — also for a polymorphic reference message,
// which has one key set per case. The fields are the target's own; on a reference message they sit
// under Reference.Value rather than on the stored value — see RefOf. It is a
// thin re-export of lww.RefKeyFields so the admin form renderer can
// distinguish scalar / sub-message / reference fields without importing
// engine/internal/lww.
func RefKeyFields(fd protoreflect.FieldDescriptor) []protoreflect.FieldDescriptor {
	return lww.RefKeyFields(fd)
}

// Reference is the stored shape of one typed reference field, for callers
// outside the engine that read or build stored reference values. See
// (syncplane.v1.reference) for the two shapes it describes.
type Reference struct {
	// Target is the referenced entity's message descriptor.
	Target protoreflect.MessageDescriptor
	// KeyFields are the target's key fields, declared on Target.
	KeyFields []protoreflect.FieldDescriptor
	// Value is the entity-typed field of a reference message holding the
	// target stub; nil when the field's type is the target entity itself.
	Value protoreflect.FieldDescriptor
	// Workspace is a reference message's workspace field, or nil when every
	// stored value resolves in the holder's own workspace.
	Workspace protoreflect.FieldDescriptor
}

// RefOf describes fd as a typed reference to one entity, or returns nil when
// it is not one — including a polymorphic reference message, whose target is
// a oneof of entities and so has no single shape to describe.
func RefOf(fd protoreflect.FieldDescriptor) *Reference {
	r := lww.RefOf(fd)
	if r == nil {
		return nil
	}
	return &Reference{Target: r.Target, KeyFields: r.KeyFields, Value: r.Value, Workspace: r.Workspace}
}

// Stub returns the target stub inside one stored reference value: the value
// itself, or the entity message a reference message embeds. Reports false for
// a reference message holding no stub.
func (r *Reference) Stub(value protoreflect.Message) (protoreflect.Message, bool) {
	if r.Value == nil {
		return value, true
	}
	if !value.Has(r.Value) {
		return nil, false
	}
	return value.Get(r.Value).Message(), true
}

// MutableStub returns the target stub inside a stored reference value being
// built, creating the embedded message of a reference message when absent.
func (r *Reference) MutableStub(value protoreflect.Message) protoreflect.Message {
	if r.Value == nil {
		return value
	}
	return value.Mutable(r.Value).Message()
}

// WorkspaceOf returns the workspace a stored reference value names, or "" when
// it names none or the reference has no workspace field.
func (r *Reference) WorkspaceOf(value protoreflect.Message) string {
	if r.Workspace == nil {
		return ""
	}
	return value.Get(r.Workspace).String()
}

// KeyPath returns the proto field names from a stored reference value to one
// key field of the target — [key] on an entity-typed reference, [value, key]
// on a reference message — the segments a query path spells the key in.
func (r *Reference) KeyPath(kf protoreflect.FieldDescriptor) []string {
	if r.Value == nil {
		return []string{string(kf.Name())}
	}
	return []string{string(r.Value.Name()), string(kf.Name())}
}

// checkReferenceMessages walks every message type reachable from desc and
// reports the first reference message that breaks its shape rules. Recursive
// types are visited once.
func checkReferenceMessages(desc protoreflect.MessageDescriptor) error {
	seen := map[protoreflect.FullName]bool{}
	var walk func(md protoreflect.MessageDescriptor) error
	walk = func(md protoreflect.MessageDescriptor) error {
		if seen[md.FullName()] {
			return nil
		}
		seen[md.FullName()] = true
		if err := lww.CheckReferenceMessage(md); err != nil {
			return err
		}
		fields := md.Fields()
		for i := 0; i < fields.Len(); i++ {
			fd := fields.Get(i)
			var sub protoreflect.MessageDescriptor
			switch {
			case fd.IsMap():
				if fd.MapValue().Kind() == protoreflect.MessageKind {
					sub = fd.MapValue().Message()
				}
			case fd.Kind() == protoreflect.MessageKind:
				sub = fd.Message()
			}
			if sub == nil {
				continue
			}
			if err := walk(sub); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(desc)
}

// checkReverseRelations reports a (syncplane.v1.field_behavior).reverse_relation
// that no reverse relation takes its name from, or a name that breaks the
// relation-name rule. Only a reference field declared on the entity itself has
// a reverse relation. The walk covers the embedded messages too, and does not
// enter an entity-typed field: the target's own fields name the target's
// relations and are checked when the target registers.
func checkReverseRelations(desc protoreflect.MessageDescriptor) error {
	fields := desc.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		name := protoopts.FieldBehavior(fd).GetReverseRelation()
		if name == "" {
			continue
		}
		if len(lww.RefsOf(fd)) == 0 {
			return fmt.Errorf("field %q sets reverse_relation, but it is not a reference; only a reference has a reverse relation", fd.Name())
		}
		if err := naming.ValidateRelationName(name); err != nil {
			return fmt.Errorf("field %q: %w", fd.Name(), err)
		}
	}
	seen := map[protoreflect.FullName]bool{desc.FullName(): true}
	var walk func(md protoreflect.MessageDescriptor) error
	walk = func(md protoreflect.MessageDescriptor) error {
		fields := md.Fields()
		for i := 0; i < fields.Len(); i++ {
			fd := fields.Get(i)
			var sub protoreflect.MessageDescriptor
			switch {
			case fd.IsMap():
				if fd.MapValue().Kind() == protoreflect.MessageKind {
					sub = fd.MapValue().Message()
				}
			case fd.Kind() == protoreflect.MessageKind:
				sub = fd.Message()
			}
			if sub == nil || seen[sub.FullName()] || protoopts.Entity(sub) != nil {
				continue
			}
			seen[sub.FullName()] = true
			subFields := sub.Fields()
			for j := 0; j < subFields.Len(); j++ {
				if f := subFields.Get(j); protoopts.FieldBehavior(f).GetReverseRelation() != "" {
					return fmt.Errorf("field %q of %s sets reverse_relation, but only a reference field declared on an entity has a reverse relation", f.Name(), sub.FullName())
				}
			}
			if err := walk(sub); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(desc)
}

// entityNames returns registered entity names in stable order.
func (c *catalog) entityNames() []string {
	names := make([]string, 0, len(c.entities))
	for n := range c.entities {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// syncEntityNames is entityNames without the server-only entities: the set a
// client may be shipped rows of. Every sync path resolves its entity list
// through the row emitter, which calls this, so "does not sync" is decided once
// rather than per path.
func (c *catalog) syncEntityNames() []string {
	names := make([]string, 0, len(c.entities))
	for n, info := range c.entities {
		if info.serverOnly {
			continue
		}
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// keyOf derives the row key from the entity's key fields, rendering and joining
// them by the shared rules in engine/internal/naming (which the TS client
// mirrors in client/src/naming.ts). It never fails: the kinds it can meet are
// settled at registration, and the resulting key is validated where it is
// written, not where it is derived — reads derive keys too.
func (info *entityInfo) keyOf(msg protoreflect.Message) string {
	parts := make([]string, len(info.keyFields))
	for i, fd := range info.keyFields {
		parts[i] = naming.ScalarKeyString(fd, msg.Get(fd))
	}
	return naming.RowKey(parts)
}

// checkKey enforces the shared row-key rules on a key about to be WRITTEN, as
// an invalid_argument rejection. A rejection rather than an error because every
// caller is on a mutation path: it is deterministic (retrying produces the same
// key), so it must ack and drain rather than wedge the queue.
func (info *entityInfo) checkKey(key string) error {
	if err := naming.ValidateRowKey(key); err != nil {
		return Reject(CodeInvalidArgument, fmt.Sprintf("entity %q: %v", info.name, err))
	}
	return nil
}

// projectRefs strips every embedded reference stub down to the target's key
// fields: a ref stores only the identity of its target, the live target row is
// resolved on read. Singular refs project the one stub; repeated refs project
// each element. The holder's own fields (including its identity) are never
// touched — a self-referential ref element is a distinct embedded message, so
// clearing its non-key fields is safe. Idempotent: re-projecting an
// already-projected row is a no-op.
//
// A reference message naming a workspace is checked against the workspace-id
// rule and refused as an invalid_argument rejection when it fails: no client
// could ever hold such a workspace, so the reference could never resolve.
func (info *entityInfo) projectRefs(msg protoreflect.Message) error {
	for _, rf := range info.refFields {
		if !msg.Has(rf.fd) {
			continue
		}
		// A reference message keeps its workspace and its entity field; the
		// projection reaches through to the stub inside it.
		project := func(value protoreflect.Message) error {
			if ws := rf.ref.WorkspaceOf(value); ws != "" {
				if err := naming.ValidateWorkspaceID(ws); err != nil {
					return Reject(CodeInvalidArgument, fmt.Sprintf("entity %q: reference %q: %v", info.name, rf.fd.Name(), err))
				}
			}
			if stub, ok := rf.ref.Stub(value); ok {
				projectStub(stub, rf.keyFields)
			}
			return nil
		}
		if rf.fd.IsList() {
			list := msg.Mutable(rf.fd).List()
			for i := 0; i < list.Len(); i++ {
				if err := project(list.Get(i).Message()); err != nil {
					return err
				}
			}
			continue
		}
		if err := project(msg.Mutable(rf.fd).Message()); err != nil {
			return err
		}
	}
	return nil
}

// projectStub clears every field of a ref stub except its target key fields.
func projectStub(stub protoreflect.Message, keyFields []protoreflect.FieldDescriptor) {
	keep := make(map[protoreflect.FieldNumber]bool, len(keyFields))
	for _, kf := range keyFields {
		keep[kf.Number()] = true
	}
	fields := stub.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if !keep[fd.Number()] {
			stub.Clear(fd)
		}
	}
}

// mergeTextFields unions every text document of base into next in place,
// pairing documents by instance path: where both sides carry one for the same
// position — the same fixed field, or the same element of a keyed list or map
// — the merged state replaces next's. One-sided positions keep plain Put
// semantics (next's value, or absent, wins) — the same contract the PATCH
// merger has; in particular an element base holds and next dropped is being
// deleted, and its document goes with it.
//
// tw carries the row's compaction floors (see textfloor.go) — a document with
// one refuses state that predates it — and the decoded-document cache entries
// the merge reads and refreshes.
func (info *entityInfo) mergeTextFields(
	base, next protoreflect.Message,
	tw *textWrite,
) error {
	for _, tf := range info.textFields {
		var baseByPath map[string][]byte
		for _, bi := range tf.instances(base) {
			if b := bi.bytes(); len(b) > 0 {
				if baseByPath == nil {
					baseByPath = make(map[string][]byte)
				}
				if _, ok := baseByPath[bi.path]; !ok {
					baseByPath[bi.path] = b
				}
			}
		}
		if baseByPath == nil {
			continue
		}
		for _, ni := range tf.instances(next) {
			baseBytes := baseByPath[ni.path]
			nextBytes := ni.bytes()
			if len(baseBytes) == 0 || len(nextBytes) == 0 {
				continue
			}
			merged, err := tw.mergeField(ni.path, baseBytes, nextBytes)
			if err != nil {
				return fmt.Errorf("server: entity %q text field %q: %w", info.name, ni.path, err)
			}
			ni.set(merged)
		}
	}
	return nil
}

// canonicalizeTextFields validates every text field msg carries and re-encodes
// it canonically in place. This is the ingress guard for client-supplied
// values — junk bytes in a text field would otherwise store a row every decoder
// rejects, bricking readers by way of an ordinary write, and canonicalizing
// keeps byte-equality short-circuits (the client watch, Merge's fast path)
// meaningful.
//
// fragmentOK decides, per INSTANCE path — the mask form, with item keys for a
// document inside a keyed list or map element — whether delta-state bytes are
// acceptable there; a nil func means "full documents only". Fragments are
// legal exactly where the merger is guaranteed to run and unfold them — a
// PATCH whose mask names the text field itself. A mask that only names an
// ancestor (including a whole element) replaces that subtree whole, which
// would persist the fragment as if it were a document, so those paths must
// reject.
//
// A rejected fragment is reported with a tagged error (text.ErrFragmentUnusable)
// so the caller can answer "resend full state" instead of "your write is
// invalid".
//
// A plain-string value is refused outright, in every position: resolving one
// into edits needs a writer identity, which the client holds and the server does
// not, so the enqueue path resolves before it pushes and the wire carries
// documents only. An unmasked field is refused too, unlike on the in-process
// write path — this guard runs over the bytes a client sent rather than over
// what a merge would reach, the same footing the fragment case above rejects on.
func (info *entityInfo) canonicalizeTextFields(
	msg protoreflect.Message,
	fragmentOK func(path string) bool,
) error {
	for _, tf := range info.textFields {
		for _, ti := range tf.instances(msg) {
			b := ti.bytes()
			if len(b) == 0 {
				continue
			}
			if text.IsPlainString(b) {
				return fmt.Errorf("entity %q text field %q: %w", info.name, ti.path,
					text.ErrPlainStringUnresolved)
			}
			if text.IsFragment(b) {
				if fragmentOK == nil || !fragmentOK(ti.path) {
					return fmt.Errorf("entity %q text field %q: %w: not a mergeable position",
						info.name, ti.path, text.ErrFragmentUnusable)
				}
				f, err := text.DecodeFragment(b)
				if err != nil {
					return fmt.Errorf("entity %q text field %q: %w", info.name, ti.path, err)
				}
				ti.set(f.Encode())
				continue
			}
			d, err := text.Decode(b, "")
			if err != nil {
				return fmt.Errorf("entity %q text field %q: %v", info.name, ti.path, err)
			}
			ti.set(d.Encode())
		}
	}
	return nil
}

// chainGetBytes walks a descriptor chain and returns the leaf bytes value,
// or nil when an intermediate message is unset.
func chainGetBytes(msg protoreflect.Message, chain []protoreflect.FieldDescriptor) []byte {
	cur := msg
	for i, fd := range chain {
		if i == len(chain)-1 {
			return cur.Get(fd).Bytes()
		}
		if !cur.Has(fd) {
			return nil
		}
		cur = cur.Get(fd).Message()
	}
	return nil
}

// chainClearBytes unsets the leaf bytes value, leaving intermediates alone.
// Unlike chainSetBytes it materializes nothing: an unset intermediate already
// means the leaf is unset.
func chainClearBytes(msg protoreflect.Message, chain []protoreflect.FieldDescriptor) {
	cur := msg
	for i, fd := range chain {
		if i == len(chain)-1 {
			cur.Clear(fd)
			return
		}
		if !cur.Has(fd) {
			return
		}
		cur = cur.Mutable(fd).Message()
	}
}

// chainSetBytes writes the leaf bytes value, materializing intermediates.
func chainSetBytes(msg protoreflect.Message, chain []protoreflect.FieldDescriptor, value []byte) {
	cur := msg
	for i, fd := range chain {
		if i == len(chain)-1 {
			cur.Set(fd, protoreflect.ValueOfBytes(value))
			return
		}
		cur = cur.Mutable(fd).Message()
	}
}

func (info *entityInfo) decode(value []byte) (proto.Message, error) {
	msg := info.msgType.New().Interface()
	if err := proto.Unmarshal(value, msg); err != nil {
		return nil, fmt.Errorf("server: decode %s: %w", info.name, err)
	}
	return msg, nil
}
