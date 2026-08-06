export { subscription, SyncClient } from './client.js';
export type {
	DiscardedMutation,
	HydrationInfo,
	ListHandle,
	MirrorTx,
	MutatorMirror,
	RejectionInfo,
	RejectionTarget,
	RemoveWorkspaceOptions,
	RemoveWorkspaceResult,
	Row,
	SchemaSkewAction,
	SchemaSkewInfo,
	SubscriptionSpec,
	SyncClientOptions,
	SyncErrorInfo,
	SyncScopeInfo,
	SyncStatus,
	Transaction,
	UndoScope,
	WorkspaceAccess,
	WorkspaceClient,
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
export { BASE_62_DIGITS, generateKeyBetween, generateNKeysBetween } from './fractional.js';
export { newId } from './id.js';
export { IDBKVStore } from './kv/idb.js';
export { MemoryKVStore } from './kv/memory.js';
export type { KVRead, KVStore, KVWrite } from './kv/store.js';
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
export { and, not, or } from './query/index.js';
export type {
	CompiledQuery,
	Cond,
	OrderKey,
	Path,
	Query,
	QueryRef,
	Scalar,
} from './query/index.js';
export type { HydratedRow, IncludeSpec } from './refs.js';
export { fetchSchema, registerSchema } from './schema/admin.js';
export type { RegisterOptions, RegisterResult } from './schema/admin.js';
export * as schema from './schema/builder.js';
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
export type { WatchCallback, WatchOptions } from './watch.js';
