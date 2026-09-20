export { subscription, SyncClient } from './client.js';
export type {
	DiscardedMutation,
	HoldHandle,
	HydrationInfo,
	ListHandle,
	MirrorTx,
	MutatorMirror,
	QueryAcross,
	QueryAllOptions,
	RejectionInfo,
	RejectionTarget,
	RemoveWorkspaceOptions,
	RemoveWorkspaceResult,
	Row,
	SchemaSkewAction,
	SchemaSkewInfo,
	SubscriptionOptions,
	SubscriptionSpec,
	SyncClientOptions,
	SyncErrorInfo,
	SyncScopeInfo,
	SyncStatus,
	Transaction,
	UndoScope,
	WatchQueryAllOptions,
	WorkspaceAccess,
	WorkspaceClient,
	WorkspacesOption,
	WorkspaceStatus,
} from './client.js';
export {
	CodeConflict,
	CodeFormatUpgraded,
	CodeInternal,
	CodeInvalidArgument,
	CodeMutationIdGap,
	CodePermissionDenied,
	CodePoisonDropped,
	CodeRoomClosed,
	CodeRoomDropped,
	CodeRoomOffline,
	CodeTextMergeFailed,
	CodeUnavailable,
	CodeWatchFailed,
	CodeWorkspaceAccessRevoked,
	CodeWorkspacePurged,
	ErrorCodeMetadataKey,
} from './codes.js';
export type { RejectionCode, SyncErrorCode } from './codes.js';
export { coverageOfEntity } from './coverage.js';
export type { Coverage, CoverageBound, ReadonlyRowSet, RowSet } from './coverage.js';
export type {
	EntityApi,
	EntityApis,
	EntityKey,
	EntityRefs,
	ForwardRefAccessor,
	ReverseRelAccessor,
} from './db.js';
// Console devtools: attach next to the client's construction; everything else
// is reached through the returned (or globalThis-published) handle.
export { attachDevtools } from './devtools.js';
export type {
	DevtoolsOptions,
	DevtoolsSnapshot,
	OverlayEntryInfo,
	QueuedMutationInfo,
	RetainedRejection,
	RoomDebugInfo,
	RowExplanation,
	StreamLeadershipInfo,
	SyncDevtools,
	WorkspaceDebugInfo,
} from './devtools.js';
export { BASE_62_DIGITS, generateKeyBetween, generateNKeysBetween } from './fractional.js';
export { newId } from './id.js';
export { IDBKVStore } from './kv/idb.js';
export { MemoryKVStore } from './kv/memory.js';
export type { IndexEntry, KVRead, KVStore, KVWrite } from './kv/store.js';
// What SyncClientOptions.mergers names, plus the mask merge itself — a reader
// that walks field history folds each record back over the live row with it.
// The registry and the ref plumbing around them stay module-private.
export { mergeMask } from './mutations.js';
export type { MergeFn, MergerSpec } from './mutations.js';
export {
	MAX_ENTITY_NAME_LENGTH,
	MAX_ROW_KEY_BYTES,
	validateEntityName,
	validateKeyField,
	validateRowKey,
} from './naming.js';
export { and, CursorShapeError, not, or } from './query/index.js';
export type {
	CompiledQuery,
	Cond,
	FacetAxis,
	FacetGroup,
	FacetsOf,
	FacetSpec,
	FacetTable,
	Group,
	GroupKeyRef,
	GroupQuery,
	GroupRowsQuery,
	OrderKey,
	Path,
	Query,
	QueryCursor,
	QueryRef,
	RowGroup,
	Scalar,
	TotalOf,
	WorkspaceQuery,
} from './query/index.js';
export type {
	HydratedRow,
	Include,
	IncludeOrderBy,
	IncludeSpec,
	IncludeWhere,
} from './refs.js';
// The ephemeral plane. jsonCodec is the default and is exported so an app can
// wrap it; a cursor room passes encodeAnchor() bytes through its own instead.
export { jsonCodec, RoomSendError } from './room.js';
export type {
	Peer,
	Room,
	RoomCodec,
	RoomError,
	RoomMessage,
	RoomOptions,
	RoomStatus,
} from './room.js';
export { fetchSchema, registerSchema } from './schema/admin.js';
export type { RegisterOptions, RegisterResult } from './schema/admin.js';
export * as schema from './schema/builder.js';
// Exported so an app that changes storagePrefix can name (and delete) the
// database the previous prefix left behind.
export { DEFAULT_STORAGE_PREFIX, storageNames } from './storagenames.js';
export type { StorageNames } from './storagenames.js';
// The observability seam. Types only: events arrive through
// SyncClientOptions.onSyncEvent and are never constructed by an app.
export type {
	BootstrapEvent,
	LeadershipEvent,
	PokeEvent,
	PullEvent,
	PushEvent,
	QueryDeclineReason,
	QueryEvent,
	RebaseEvent,
	ResequenceEvent,
	StreamEvent,
	SyncEvent,
	SyncEventHandler,
	SyncEventOutcome,
} from './sync/events.js';
export {
	COMMENT_PREFIX,
	commentIdOf,
	commentKey,
	commentRanges,
	joinAnnotations,
} from './text/annotations.js';
export type { CommentRange, ResolvedAnnotation } from './text/annotations.js';
export { deltaToUtf16 } from './text/handle.js';
export type {
	TextHandle,
	TextHandleError,
	TextHandleStatus,
	TextPoint,
	TextSpan,
} from './text/handle.js';
export {
	AnchorAfter,
	AnchorBefore,
	AnchorDocEnd,
	AnchorDocStart,
	codePointToUtf16,
	decodeAnchor,
	decodeTextDoc,
	diffText,
	encodeAnchor,
	encodePlainString,
	encodeTextDoc,
	ExpandAfter,
	ExpandBefore,
	ExpandBoth,
	ExpandNone,
	isPlainString,
	mergeText,
	plainString,
	plainTextOf,
	TextDoc,
	textMarksOf,
	textOf,
	utf16ToCodePoint,
} from './text/index.js';
export type {
	Anchor,
	AnchorKind,
	DeltaOp,
	Expand,
	ItemID,
	MarkValue,
	PendingMark,
	Point,
	SegmentMark,
	Span,
	TextDelta,
} from './text/index.js';
export { applyTextInverse } from './text/undo-ops.js';
export type { TextInverseOp } from './text/undo-ops.js';
// Undo is driven through the client (undo/redo/canUndo) — the manager and the
// inversion it runs on are not part of the surface; only the option types are.
export type { UndoOptions, UndoScopeOptions } from './undo.js';
export type {
	KeyedRows,
	KeyedWatchCallback,
	KeyedWatchHandle,
	KeyedWatchOptions,
	RowRef,
	RowSplice,
	ScopedKeyedRow,
	ScopedKeyedRows,
	ScopedKeyedWatchCallback,
	ScopedKeyedWatchHandle,
	WatchCallback,
	WatchDelta,
	WatchHandle,
	WatchOptions,
	WatchWindow,
} from './watch.js';
