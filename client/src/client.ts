import {
	clone,
	create,
	type DescMessage,
	fromBinary,
	type Message,
	type MessageInitShape,
	type MessageShape,
} from '@bufbuild/protobuf';
import { createClient, type Transport } from '@connectrpc/connect';

import { CodeFormatUpgraded, CodeWatchFailed, CodeWorkspacePurged } from './codes.js';
import {
	COMPLETE,
	type Coverage,
	CoverageResolver,
	type PageInfo,
	type RelCoverageRef,
	type RowSet,
	withCoverage,
} from './coverage.js';
import { buildEntityApis, type EntityApis } from './db.js';
import { reconcileStoreEntities, syncableEntities } from './entities.js';
import { debugVerifyTouchesEnv } from './env.js';
import {
	type CrudMutation,
	CrudOp,
	type Mutation,
	MutationSchema,
} from './gen/syncplane/v1/mutation_pb.js';
import {
	type FollowSpec,
	FollowSpecSchema,
	RefPathSchema,
	type Subscription,
	SubscriptionSchema,
	type Window,
	WindowSchema,
} from './gen/syncplane/v1/patch_pb.js';
import { AggregateOp } from './gen/syncplane/v1/query_pb.js';
import { SyncService } from './gen/syncplane/v1/service_pb.js';
import {
	compareWorkspaces,
	confirmedWsPrefix,
	FORMAT_VERSION,
	META_CLIENT_ID,
	META_FORMAT_VERSION,
	META_WS_PREFIX,
	metaLastMutationId,
	metaPushedThrough,
	metaWsPrefix,
	overlayTextWsPrefix,
	overlayWsPrefix,
	PENDING_ROOT,
	pendingPrefix,
	stagingWsRoot,
	workspaceOfPendingKey,
	wsFromSegment,
} from './keys.js';
import { IDBKVStore } from './kv/idb.js';
import { MemoryKVStore } from './kv/memory.js';
import type { KVRead, KVStore } from './kv/store.js';
import {
	applyTextStringsToCrud,
	assertWritePolicy,
	attachTextStrings,
	buildBatch,
	buildCustom,
	buildDelete,
	buildPatch,
	buildPut,
	carryProtectedFields,
	crudHasPlainString,
	encodeItemKey,
	type EntityDef,
	EntityRegistry,
	expandCollectionMask,
	extractTextStrings,
	isTextField,
	keyFieldsFor,
	keyOf,
	maskFromDiff,
	maskFromInit,
	maskNeedsExpansion,
	type MergerSpec,
	type PatchInit,
	protectedFieldOf,
	takeTextStrings,
	textFieldBytes,
	textFieldInit,
	type TextInit,
	textInstancePaths,
	textSchemaPathOf,
	type TextStringEdits,
} from './mutations.js';
import { createKvMutex, type Mutex } from './mutex.js';
import { validateWorkspaceId } from './naming.js';
import { buildIndexes } from './optimistic/index-build.js';
import { IndexCache } from './optimistic/index-maintain.js';
import { enqueuePending, readPending } from './optimistic/queue.js';
import {
	applyCrudLocally,
	applyMutationLocally,
	type MirrorMap,
	type MutatorMirror,
} from './optimistic/rebase.js';
import { generateKeyBetween, newElementId, orderFieldOf, sortOrderedFields } from './ordering.js';
import {
	cursorFromBytes,
	cursorToBytes,
	OrderCursors,
	pageWindowOf,
	type QueryCursor,
} from './query/cursor.js';
import { assertSameResult, type IndexReport, queryByIndex } from './query/execute.js';
import { readGroups, readRowGroups } from './query/group-read.js';
import {
	collectRels,
	compileByWorkspace,
	type CompiledQuery,
	compileGroupQuery,
	compileGroupRowsQuery,
	compileQuery,
	computeRelKeys,
	type Cond,
	evalCond,
	type Group,
	type GroupQuery,
	type GroupRowsQuery,
	hasRel,
	isWorkspaceQuery,
	type OrderKey,
	type PageWindow,
	type Query,
	type QueryRef,
	type RelKeys,
	type RelNode,
	rewriteWithKeys,
	type RowGroup,
	runQueryPage,
	type Scalar,
	type TotalOf,
	type WorkspaceQuery,
} from './query/index.js';
import { condToProto, orderKeyToProto, scalarFromProto } from './query/proto.js';
import {
	type PreparedGroupQuery,
	prepareGroupQuery,
	prepareGroupRowsQuery,
	prepareQuery,
	toWirePaths,
} from './query/wire.js';
import {
	type NamedWorkspaces,
	readMergedRow,
	readMergedRowWithVersion,
	type ReadScope,
	type Row,
	scanDecoded,
} from './read.js';
import {
	hydrate,
	type HydratedRow,
	type Include,
	type IncludeSpec,
	loadForward,
	type RefClient,
	resolveReverse,
} from './refs.js';
import { reportError, reportMisuse, safeInvoke } from './report.js';
import { createRoom, type Room, type RoomOptions } from './room.js';
import {
	DEFAULT_STORAGE_PREFIX,
	type StorageNames,
	storageNames,
	validateStoragePrefix,
} from './storagenames.js';
import { type ChangeShare, startChangeShare } from './sync/changeshare.js';
import {
	chunkBySize,
	Coordinator,
	DEFAULT_MAX_MUTATIONS_PER_PUSH,
	describeMutation,
	type RejectionInfo,
	type RejectionTarget,
} from './sync/coordinator.js';
import type { SyncEvent, SyncEventHandler, UnstampedSyncEvent } from './sync/events.js';
import { emitSyncEvent, queryEventOf } from './sync/events.js';
import { IntentRegistry } from './sync/intent.js';
import { PresenceCoordinator, type PresenceTopic } from './sync/presence.js';
import {
	type SchemaSkewAction,
	StatusTracker,
	type SyncStatus,
	type WorkspaceStatus,
} from './sync/status.js';
import { decodeTextDoc, encodeTextDoc } from './text/codec.js';
import type { TextDoc } from './text/doc.js';
import { openTextHandle, type TextHandle } from './text/handle.js';
import { createPeerLeases, type PeerLease, type PeerLeases } from './text/peer.js';
import { applyTextInverse, reinsertOp, type TextInverseOp } from './text/undo-ops.js';
import { TouchSet } from './touch.js';
import {
	type Inverse,
	invertCrud,
	type StackName,
	UndoManager,
	type UndoOptions,
	type UndoScopeOptions,
} from './undo.js';
import {
	type KeyedWatchCallback,
	type KeyedWatchHandle,
	type KeyedWatchOptions,
	type RowRef,
	type ScopedKeyedWatchCallback,
	type ScopedKeyedWatchHandle,
	type WatchCallback,
	type WatchDelta,
	WatcherHub,
	type WatchHandle,
	type WatchOptions,
	type WatchWindow,
} from './watch.js';

/** The handle a watch on a closed client gets: nothing subscribed, nothing to stop. */
const INERT_WATCH: WatchHandle = Object.freeze({ close: () => {}, setWindow: () => {} });

/** The keyed-watch counterpart of INERT_WATCH. */
const INERT_REF_WATCH: ScopedKeyedWatchHandle = Object.freeze({
	close: () => {},
	setRefs: () => {},
});

export type { MirrorTx, MutatorMirror } from './optimistic/rebase.js';
export type { Row } from './read.js';
export type { RejectionInfo, RejectionTarget } from './sync/coordinator.js';
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
export type {
	HydrationInfo,
	SchemaSkewAction,
	SchemaSkewInfo,
	SyncErrorInfo,
	SyncScopeInfo,
	SyncStatus,
	WorkspaceAccess,
	WorkspaceStatus,
} from './sync/status.js';

/**
 * Per-chunk timeout for the best-effort format-upgrade salvage push. The push
 * runs outside the cross-tab lock, so a stall no longer wedges other tabs, but
 * the bound still keeps a half-open connection from hanging start() forever —
 * the unsent remainder is simply dropped and reported.
 */
const SALVAGE_PUSH_TIMEOUT_MS = 30_000;

/**
 * Field reference a SubscriptionSpec's `where` receives. Bound to the schema
 * when the spec names one (`SubscriptionSpec<typeof TodoSchema>`); left loose
 * on the bare `SubscriptionSpec`, because a spec written as an inline literal
 * inside an array has no schema for TypeScript to infer the predicate from.
 * The `subscription()` builder exists to close that gap and is the path to
 * prefer — it types `where` against the schema in the same call.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type SpecRef<D extends DescMessage> = DescMessage extends D ? any : QueryRef<MessageShape<D>>;

/**
 * Scopes sync to one entity, optionally narrowed by a predicate (same shape
 * as a query `where`).
 *
 * Prefer the `subscription()` builder, which infers the predicate's field
 * types from the schema. Written as a bare literal the spec still works, but
 * `where` receives an untyped reference (see SpecRef); annotating the spec —
 * `const s: SubscriptionSpec<typeof TodoSchema> = { … }` — recovers the typing
 * without the builder.
 *
 * A spec names an entity and a predicate, never a workspace — the same set
 * applies to every workspace the client holds, and `client.ws(id)
 * .setSubscriptions(...)` narrows one of them.
 */
export interface SubscriptionSpec<D extends DescMessage = DescMessage> {
	schema: D;
	where?: (w: SpecRef<D>) => Cond;
	/**
	 * Reference-following scope: also sync the closure reached by walking these
	 * relations from the rows this subscription selects. The option-free shape
	 * of a query `include` (`{ project: true, blockedBy: { project: true } }`);
	 * forward refs resolve by point-lookup, reverse relations by predicate scan,
	 * each bounded by the server's follow limits. A followed row is still subject
	 * to the same read filter as a base row.
	 */
	follow?: IncludeSpec;
	/**
	 * Rounds the follow path set is re-applied (transitive depth, e.g. a
	 * dependency chain over a repeated self-ref). 0/absent ⇒ 1. Capped by the
	 * server's MaxFollowDepth.
	 */
	followDepth?: number;
	/**
	 * Ordering the row cap applies to — the same lambda shape a query's
	 * `orderBy` takes. Only meaningful with `limit`, and mutually exclusive
	 * with `byRecency`.
	 */
	orderBy?: (w: SpecRef<D>) => OrderKey[];
	/**
	 * Caps the synced set at this many rows: only the first `limit` rows of the
	 * predicate match under `orderBy` (or `byRecency`) sync, and a row that
	 * falls off the tail is dropped from the local store.
	 *
	 * This is how a working set stays a policy choice rather than the size of
	 * the entity. It also makes the local store PARTIAL in a way a predicate
	 * alone does not: a client holding the top N by one order cannot answer a
	 * local query ordered by another. Read `coverage` off any result to see
	 * whether the answer is complete, and reach for `queryServer` when it is
	 * not. Capped by the server's MaxWindowSize (default 1000).
	 */
	limit?: number;
	/**
	 * Rank by the row's server write sequence, newest first — "the N most
	 * recently changed matching rows" — instead of by `orderBy`. Cheaper for
	 * the server to maintain, since it needs no field comparison.
	 */
	byRecency?: boolean;
}

/**
 * Typed builder for SubscriptionSpec, and the recommended way to write one:
 * the predicate's field reference is inferred from `schema`, which a bare spec
 * literal in an array cannot get.
 */
export function subscription<D extends DescMessage>(
	schema: D,
	where?: (w: QueryRef<MessageShape<D>>) => Cond,
	opts?: {
		include?: IncludeSpec;
		depth?: number;
		/** Row cap; see SubscriptionSpec.limit. */
		limit?: number;
		orderBy?: (w: QueryRef<MessageShape<D>>) => OrderKey[];
		byRecency?: boolean;
	},
): SubscriptionSpec {
	return {
		schema,
		where: where as SubscriptionSpec['where'],
		follow: opts?.include,
		followDepth: opts?.depth,
		orderBy: opts?.orderBy as SubscriptionSpec['orderBy'],
		limit: opts?.limit,
		byRecency: opts?.byRecency,
	};
}

/** Flattens an include tree into root-to-leaf relation chains (RefPath segments). */
function includeToPaths(spec: IncludeSpec): string[][] {
	const paths: string[][] = [];
	for (const [ relation, sub ] of Object.entries(spec)) {
		if (sub === true) {
			paths.push([ relation ]);
		} else {
			for (const tail of includeToPaths(sub)) {
				paths.push([ relation, ...tail ]);
			}
		}
	}
	return paths;
}

/**
 * Serializes a follow include tree into a wire FollowSpec. The segment names
 * are the same relation accessors the server's reference graph resolves
 * (forward field localName or auto-named reverse relation), so the server picks
 * the direction per segment.
 */
function compileFollow(include: IncludeSpec, depth?: number): FollowSpec {
	return create(FollowSpecSchema, {
		paths: includeToPaths(include).map((segments) => create(RefPathSchema, { segments })),
		depth: depth && depth > 0 ? depth : 0,
	});
}

/**
 * Serializes a spec's row cap into a wire Window, or undefined when the spec
 * sets none. `orderBy` arrives already translated to proto paths, since the
 * window shares the query pipeline that does that translation.
 *
 * A limit is what makes a window; an ordering without one is rejected rather
 * than ignored, because it would silently sync the whole entity.
 */
function compileWindow(spec: SubscriptionSpec, orderBy?: OrderKey[]): Window | undefined {
	if (spec.limit === undefined) {
		if (spec.orderBy || spec.byRecency) {
			throw new Error('subscription orderBy/byRecency needs a limit to bound anything');
		}
		return undefined;
	}
	if (!Number.isInteger(spec.limit) || spec.limit <= 0) {
		throw new Error(`subscription limit must be a positive integer, got ${spec.limit}`);
	}
	if (spec.byRecency && orderBy?.length) {
		throw new Error('subscription cannot set both byRecency and orderBy');
	}
	return create(WindowSchema, {
		limit: spec.limit,
		byRecency: spec.byRecency ?? false,
		orderBy: spec.byRecency ? [] : (orderBy ?? []).map(orderKeyToProto),
	});
}

export interface SyncClientOptions<E extends readonly DescMessage[] = readonly DescMessage[]> {
	/** Base URL of the sync server. Either url or transport is required. */
	url?: string;
	/** Pre-built Connect transport (used by tests / Node). */
	transport?: Transport;
	/**
	 * Encoding the transport built from `url` uses. Ignored when `transport` is
	 * supplied — that transport carries its own.
	 *
	 * Binary is the default and the one to ship: a row's largest field is
	 * `RowChange.value`, the serialized entity, which JSON must base64 at +33%,
	 * and a bootstrap chunk repeats every field name across 256 rows. Decoding
	 * costs less too — JSON arrives as text to parse, then a base64 field to
	 * decode, then the entity to parse out of that.
	 *
	 * `'json'` is for looking at traffic: it makes requests readable in the
	 * browser's network inspector at the cost of the above. The server accepts
	 * either, chosen per request from the content type, so this is a local
	 * debugging switch and never something both ends must agree on.
	 */
	wireFormat?: 'binary' | 'json';
	/**
	 * Single-workspace shorthand for `workspaces: [id], defaultWorkspace: id`.
	 * Give either this or `workspaces`.
	 */
	workspaceId?: string;
	/**
	 * Workspaces to sync. They share one store, one change stream and one push,
	 * but keep separate cursors, pending queues and mutation-id counters — the
	 * server sequences per workspace and requires per-workspace contiguous ids.
	 * Workspaces can be added and removed at runtime (addWorkspace /
	 * removeWorkspace).
	 */
	workspaces?: string[];
	/**
	 * Workspace the client-level API binds to: `client.mutate`, `client.db`,
	 * `client.get/scan/query/watch`, `transact` and undo all target it, so a
	 * single-workspace app never mentions a workspace. Defaults to the only
	 * configured workspace; required (or set later) once there is more than one.
	 * With no default, those methods throw and everything goes through
	 * `client.ws(id)`.
	 */
	defaultWorkspace?: string;
	/**
	 * Storage scope: one IndexedDB database (and one KV mutex, cross-tab change
	 * channel and stream-leadership lock) per account. Defaults to the single
	 * configured workspace's id, which keeps a single-workspace client's store
	 * exactly where it always was; REQUIRED when the client starts with more than
	 * one workspace, since no workspace can stand for the whole account.
	 *
	 * Two accounts must never share an id: a credential is attached per request,
	 * so they cannot share a stream, a store or a `clientId`. Use separate
	 * SyncClients for separate accounts.
	 */
	accountId?: string;
	/**
	 * Stem of every name this client claims on the origin — the IndexedDB
	 * database, the Web Locks, the BroadcastChannels — under which the account
	 * partitions them (see storagenames.ts). Defaults to `sync`. Must be
	 * letters, digits, `_` and `-`, starting with a letter or digit.
	 *
	 * Set it to keep two clients on one origin apart when they cannot be given
	 * distinct account ids, or to keep this client's names clear of another
	 * library's. Changing it for an existing app points the client at a fresh,
	 * empty database and leaves the old one on disk: the client bootstraps from
	 * the server and never reads or deletes what the previous prefix wrote —
	 * pending mutations included. Deleting the abandoned database (or migrating
	 * anything out of it first) is the app's to do.
	 */
	storagePrefix?: string;
	/** Defaults to IndexedDB in browsers, in-memory elsewhere. */
	kvStore?: KVStore;
	/** Generated schemas of all syncable entities. */
	entities: E;
	/** Best-effort local mirrors of server-side custom mutators. */
	mutators?: Record<string, MutatorMirror<DescMessage>>;
	/**
	 * Client mirrors of server merge hooks (Server.RegisterMerger) so
	 * optimistic PATCHes and rebase replay merge opaque fields the same way.
	 * Parity with the Go hook is the app's responsibility; divergence
	 * self-heals on the next rebase.
	 */
	mergers?: MergerSpec[];
	/**
	 * Dead-letter surface for mutations that will never be applied: a server
	 * rejection or a client-side poison drop (code "poison_dropped"). The info
	 * names the workspace the mutation was queued for.
	 *
	 * A rejection whose push response was lost is not dropped: the server records
	 * the verdict with the watermark it bumps and replays it to the resend, so
	 * this fires a sync cycle later than it otherwise would.
	 */
	onMutationRejected?: (info: RejectionInfo) => void;
	/** Called whenever the sync status changes (also available via onStatusChange). */
	onStatusChange?: (status: SyncStatus) => void;
	/**
	 * Observability seam: a typed stream of what the client did — pushes, pulls,
	 * rebases, bootstraps, stream open/close, leadership, and every local query
	 * — each with its own duration and counts.
	 *
	 * This is what to measure sync and local reads with. `onStatusChange`
	 * reports the current state for rendering; these report occurrences for
	 * aggregating, which is the only way to get at latency and at the things a
	 * successful call hides (a push whose mutations were all rejected is a 200
	 * OK; a query that fell back from an index to a scan is an ordinary result).
	 *
	 * Deliberately not an OpenTelemetry integration — the client ships to
	 * browsers, so the vendor choice stays with the app. Map these to whatever
	 * you already run. See sync/events.ts; the handler runs synchronously inside
	 * the sync cycle or the read that fired it, so it must not block.
	 */
	onSyncEvent?: SyncEventHandler;
	/** Fallback poll interval in ms; 0 disables. Default 60s. */
	pollIntervalMs?: number;
	/**
	 * How long a local write waits before it is pushed, restarted by each
	 * further write, so a burst of edits ships as one compacted push instead of
	 * one push per edit. Only outbound writes wait — inbound changes are never
	 * delayed. Default 50ms; 0 pushes on every write.
	 */
	pushDebounceMs?: number;
	/**
	 * Ceiling on that wait, from the first write of a burst, so continuous
	 * writing still pushes regularly. Default 250ms; 0 removes the ceiling.
	 */
	pushMaxDelayMs?: number;
	/** Hold the live change stream open. Default true. */
	streamEnabled?: boolean;
	/**
	 * Hold the room `Watch` stream open. Default true.
	 *
	 * Independent of `streamEnabled`: the two planes are separate streams with
	 * separate leadership, so rooms work on a polling-only client. False leaves
	 * `setPresence` and `send` working — publishing is a unary call — and stops
	 * this client RECEIVING anything, which is what a page that only writes
	 * presence wants, and what gets a browser its second connection slot back.
	 *
	 * A record is normally kept alive by whichever node holds its session's
	 * stream, so with no stream each open room re-publishes its own last state
	 * on the interval the server reports — one unary call per room per interval
	 * in place of the connection.
	 */
	presenceEnabled?: boolean;
	/** Max row changes per pull page; 0 = server default. Pages loop until drained. */
	pullLimit?: number;
	/**
	 * Cross-checks every maintained watch result against a full scan on each
	 * commit and throws when the two disagree. A development and test aid — it
	 * doubles the read cost of every notification — that catches a write path
	 * which changed a row without reporting it, and an ordering that disagrees
	 * with the query evaluator. Default false.
	 */
	debugVerifyTouches?: boolean;
	/**
	 * Also cross-checks every index-served local query against the equivalent
	 * full scan and throws when they disagree. Same intent and same cost profile
	 * as `debugVerifyTouches`, pointed at the other place a cheap answer stands
	 * in for an expensive one — and it defaults on under the same
	 * `SYNC_DEBUG_VERIFY_TOUCHES` environment variable, because an index that
	 * answers differently from a scan is exactly the class of bug that flag
	 * exists to catch.
	 */
	debugVerifyIndexReads?: boolean;
	/**
	 * Identical push failures of the head mutation — an invalid-argument RPC
	 * error, or a batch the server refuses as a whole with an internal or
	 * unrecognized code — before the client drops it to the dead-letter surface
	 * (onMutationRejected, code "poison_dropped"). Backstop against a queue no
	 * amount of retrying can drain. Default 5; <= 0 disables.
	 */
	poisonThreshold?: number;
	/**
	 * Largest number of mutations a single push RPC carries, counted ACROSS the
	 * workspaces it batches (the engine's limit is a per-request total). A bigger
	 * offline burst is split across sequential pushes. Default 256 (the engine's
	 * default limit). Set this to match a server configured with a lower
	 * MaxMutationsPerPush.
	 */
	maxMutationsPerPush?: number;
	/**
	 * Largest number of workspaces one request may name — push batches and
	 * Subscribe scopes. Default 64 (the engine's default
	 * Limits.MaxWorkspacesPerRequest). A client holding more splits its pushes
	 * across requests; the extra workspaces sync through the poll path rather
	 * than the stream.
	 */
	maxWorkspacesPerRequest?: number;
	/**
	 * Partial sync: only the listed entities (narrowed by their predicates)
	 * sync to this client. Empty/absent = full sync. The set applies to EVERY
	 * workspace the client holds; narrow one of them with
	 * `client.ws(id).setSubscriptions(...)`. Changing a set later delta-resyncs
	 * only the difference into the new scope.
	 */
	subscriptions?: SubscriptionSpec[];
	/**
	 * Entities the first snapshot should deliver first, most important first.
	 *
	 * A cold client applies the snapshot entity by entity and can read each one
	 * as it lands, so this is what decides which part of the app has its data
	 * while the rest still downloads — list what the first screen renders.
	 * Everything else follows in the server's own order, so this is a
	 * reordering, never a filter: nothing is dropped by leaving it out.
	 *
	 * Unset, a subscribed client is served in `subscriptions` order and a
	 * full-sync client in the server's entity order. Only cold starts feel it —
	 * a re-bootstrap swaps atomically, so its order is not observable.
	 */
	bootstrapPriority?: readonly DescMessage[];
	/**
	 * This build's schema revision — an opaque token matched against the one the
	 * server advertises (Server.WithSchemaRevision). When the two differ, the
	 * client surfaces the skew on its status (status().schemaSkew) and runs
	 * onSchemaSkew to decide what to do. Leave unset to disable the handshake;
	 * an unset server revision is likewise treated as "no information".
	 */
	schemaRevision?: string;
	/**
	 * Decides how to react to a schema-revision mismatch. Called once per
	 * distinct server revision that differs from schemaRevision; the return
	 * value drives the client:
	 *   - "ignore": do nothing (clears any prior reaction).
	 *   - "warn":   surface schemaSkew on the status, keep syncing normally.
	 *   - "block":  surface schemaSkew AND suspend pushes (read-only / update
	 *               required) until the revisions line up; pulls keep running.
	 *   - "resync": surface schemaSkew AND force a clean re-bootstrap onto the
	 *               server's current state.
	 * Defaults to "warn" when omitted but schemaRevision is set. Has no effect
	 * without schemaRevision.
	 */
	onSchemaSkew?: (info: { client: string; server: string }) => SchemaSkewAction;
	/**
	 * Enables in-memory undo/redo over local CRUD mutations. Custom mutations
	 * are a barrier (both stacks clear). Stacks are PER WORKSPACE — mutation ids
	 * are counted per workspace and a batch is one server savepoint, so an undo
	 * step can never span two of them — and they die with the page.
	 */
	undo?: UndoOptions;
}

/** Compare-and-swap guard shared by the CRUD mutate calls. */
export interface CasOptions {
	/**
	 * When set, the server applies the mutation only if the target row's current
	 * version equals this value, otherwise it rejects with code "conflict" (the
	 * optimistic edit rolls back and onMutationRejected fires). Read the base
	 * version from `Row.version` (scan/query/watch/getRow). 0n means "apply only
	 * if the row does not exist".
	 */
	expectedVersion?: bigint;
}

/** Options for a PATCH: the CAS guard plus an explicit field mask. */
export interface PatchOptions extends CasOptions {
	/** Field mask paths to merge; defaults to the keys present in `init`. */
	paths?: string[];
}

/** A PATCH whose mask is given rather than derived — see PatchInit. */
export interface MaskedPatchOptions extends PatchOptions {
	paths: string[];
}

/** Options for an update: the CAS guard. The mask comes from the diff. */
export type UpdateOptions = CasOptions;

/**
 * Edits a draft row in place. The draft is a private copy, so mutating it
 * cannot disturb a row the app already holds, and only the fields it actually
 * changes are written.
 *
 * Synchronous on purpose: awaiting inside the recipe would widen the window
 * between the read it diffs against and the write it produces, for no gain —
 * do the awaiting before the call and assign the results here.
 */
export type UpdateRecipe<D extends DescMessage> = (draft: MessageShape<D>) => void;

/** The CRUD writes that can join a transact() batch. Bytes-typed init fields
 * also accept plain strings for text fields (see TextInit). */
interface CrudMutateApi {
	put<D extends DescMessage>(
		schema: D,
		value: MessageShape<D> | TextInit<MessageInitShape<D>>,
		opts?: CasOptions,
	): Promise<void>;
	/**
	 * Merges the given fields into a row, leaving every other field alone. The
	 * mask is the keys of `init` — so `init` carries the fields to change, not a
	 * whole row (see PatchInit, and `update` for read-modify-write).
	 */
	patch<D extends DescMessage>(
		schema: D,
		key: string,
		init: PatchInit<D>,
		opts?: PatchOptions,
	): Promise<void>;
	/**
	 * Merges named fields under a mask the caller supplies. The mask decides what
	 * is written, so the source may be any message — this is how fields are
	 * copied off a row that was read.
	 */
	patch<D extends DescMessage>(
		schema: D,
		key: string,
		init: TextInit<MessageInitShape<D>>,
		opts: MaskedPatchOptions,
	): Promise<void>;
	/**
	 * Read-modify-write: reads the row, hands a draft to `recipe`, and PATCHes
	 * the fields the recipe changed. Nothing changed, nothing written.
	 *
	 * This is the safe form of "fetch a row, edit it, save it": because the mask
	 * covers only what moved, a field someone else changed meanwhile is not
	 * written back stale, which a PUT of the same draft would do.
	 *
	 * Throws when the row is not there to read. That is a real state — partial
	 * sync means a key can exist on the server and not locally — and a silent
	 * no-op would hide it.
	 */
	update<D extends DescMessage>(
		schema: D,
		key: string,
		recipe: UpdateRecipe<D>,
		opts?: UpdateOptions,
	): Promise<void>;
	del(schema: DescMessage, key: string, opts?: CasOptions): Promise<void>;
}

interface MutateApi extends CrudMutateApi {
	custom<D extends DescMessage>(
		name: string,
		argsSchema?: D,
		args?: MessageInitShape<D>,
	): Promise<void>;
	/**
	 * Opens a movable-list handle over a keyed `repeated Message` field that
	 * declares an order_field. Insert/move/remove allocate fractional positions
	 * and issue ordinary per-item PATCHes; reads come back sorted by position.
	 */
	list<D extends DescMessage>(schema: D, rowKey: string, field: string): ListHandle;
}

/**
 * Ergonomic handle over a movable ordered list (a keyed message list with an
 * order_field). All operations read the current local snapshot to find
 * neighbors, then enqueue a normal row PATCH; convergence rides Phase 1's
 * per-item merge.
 */
export interface ListHandle {
	/** Inserts a new element at sorted index `index`; returns its element id. */
	insertAt(index: number, itemInit: Record<string, unknown>): Promise<string>;
	/** Inserts a new element just before the element with id `beforeId`. */
	insertBefore(beforeId: string, itemInit: Record<string, unknown>): Promise<string>;
	/** Moves the element with id `id` to sorted index `toIndex` (LWW on pos). */
	move(id: string, toIndex: number): Promise<void>;
	/** Removes the element with id `id` (clear-on-absent). */
	remove(id: string): Promise<void>;
}

/**
 * Handle passed to `transact(fn)`. CRUD writes issued through this handle —
 * `tx.mutate.put` or `tx.db.x.mutate.put` — join the one atomic batch; reads
 * see the pre-batch state (buffered writes are invisible until fn returns).
 *
 * Scoping is lexical-by-handle, not temporal: a write issued through the
 * client itself (`client.mutate.*`) during one of fn's awaits is NOT captured —
 * it queues as its own mutation. Custom mutations cannot batch, so the handle
 * does not expose `custom`; issue those through the client directly.
 *
 * A transaction belongs to ONE workspace — the server commits a batch as one
 * savepoint in one partition's sequence, so there is no cross-workspace atomic
 * write and none is implied.
 */
export interface Transaction<E extends readonly DescMessage[] = readonly DescMessage[]> {
	readonly workspaceId: string;
	get<D extends DescMessage>(schema: D, key: string): Promise<MessageShape<D> | undefined>;
	getRow<D extends DescMessage>(
		schema: D,
		key: string,
	): Promise<Row<MessageShape<D>> | undefined>;
	scan<D extends DescMessage>(schema: D): Promise<RowSet<MessageShape<D>>>;
	query<D extends DescMessage, const Q extends Query<MessageShape<D>>>(
		schema: D,
		q: Q,
		opts?: { include?: Include },
	): Promise<RowSet<MessageShape<D>, TotalOf<Q>>>;
	groupCount<D extends DescMessage>(
		schema: D,
		q: GroupQuery<MessageShape<D>>,
	): Promise<Group[]>;
	distinct<D extends DescMessage>(
		schema: D,
		q: GroupQuery<MessageShape<D>>,
	): Promise<(Scalar | undefined)[]>;
	groupRows<D extends DescMessage>(
		schema: D,
		q: GroupRowsQuery<MessageShape<D>>,
	): Promise<RowGroup<MessageShape<D>>[]>;
	watch<D extends DescMessage>(schema: D, cb: WatchCallback<MessageShape<D>>): WatchHandle;
	watchQuery<D extends DescMessage, const Q extends Query<MessageShape<D>>>(
		schema: D,
		q: Q,
		cb: WatchCallback<MessageShape<D>, TotalOf<Q>>,
		opts?: { include?: Include },
	): WatchHandle;
	resolveRef<D extends DescMessage>(
		holderSchema: D,
		fieldLocalName: string,
		holder: MessageShape<D>,
	): Promise<Row<Message> | Row<Message>[] | undefined>;
	resolveReverseRef<D extends DescMessage>(
		targetSchema: D,
		relationName: string,
		target: MessageShape<D>,
		q?: Query<Message>,
	): Promise<Row<Message>[]>;
	mutate: CrudMutateApi;
	db: EntityApis<E>;
}

/** Options for dropping a workspace from a running client. */
export interface RemoveWorkspaceOptions {
	/**
	 * Also delete the workspace's local data: its confirmed rows, optimistic
	 * overlay, bootstrap staging, cursor, watermarks and every mutation still
	 * queued for it. Use this when access was revoked and the app has decided the
	 * copy must not stay on the device; without it the client merely stops
	 * syncing and the rows remain readable until the workspace is added back.
	 *
	 * The queued mutations are handed back in RemoveWorkspaceResult.discarded —
	 * that is the only copy left, so an app that wants to keep unsent work must
	 * take it from there.
	 *
	 * The store is shared across tabs, so a purge is visible to all of them: a
	 * peer tab still holding the workspace sees its rows disappear and
	 * re-bootstraps it on its next cycle.
	 */
	purge?: boolean;
}

/** A queued mutation that was dropped without ever reaching the server. */
export interface DiscardedMutation {
	workspaceId: string;
	mutationId: bigint;
	/**
	 * The mutation as it sat in the queue, so an app can persist it or re-issue
	 * it against another workspace. Re-enqueueing it as-is is not useful: ids are
	 * minted per (workspace, client) and the server would see a gap.
	 */
	mutation: Mutation;
}

/** What `removeWorkspace` did. */
export interface RemoveWorkspaceResult {
	/** False when the client did not hold the workspace (the call was a no-op). */
	removed: boolean;
	/** Whether local data was deleted, i.e. the purge option was set and ran. */
	purged: boolean;
	/**
	 * Mutations the purge threw away, in queue order. They can never be acked —
	 * the workspace is gone locally and, in the revocation case, the server would
	 * refuse them anyway — so this is where unsent work goes to be preserved if
	 * the app wants it. Empty without `purge`, since the queue then survives.
	 *
	 * Each one is also reported through `onMutationRejected` with code
	 * `workspace_purged`, so ignoring this value still does not lose the loss
	 * silently.
	 */
	discarded: DiscardedMutation[];
}

/**
 * Everything the client does inside one workspace. Reached through
 * `client.ws(id)`; the same surface is mirrored on the client itself, bound to
 * the default workspace, so a single-workspace app never names one.
 */
export interface WorkspaceClient<E extends readonly DescMessage[] = readonly DescMessage[]> {
	readonly workspaceId: string;
	readonly mutate: MutateApi;
	readonly db: EntityApis<E>;
	/** Opens a live text-field handle in this workspace (see TextHandle). */
	text<D extends DescMessage>(schema: D, key: string, field: string): Promise<TextHandle>;
	/**
	 * Opens a room in this workspace: presence and broadcast for state that is
	 * only worth having while someone is connected (see Room). Nothing a room
	 * carries is stored or replayed.
	 *
	 * Anchoring it to a row — `room(Todo, 't1', 'cursors')` — makes membership
	 * inherit that row's read permission, so a cursor room on a document needs
	 * no policy of its own. The unanchored form is gated on workspace access.
	 *
	 * The account holds ONE `Watch` stream however many rooms are open, shared
	 * across tabs; opening a room joins it and closing the last one ends it.
	 */
	room<S, M = S>(name: string, opts?: RoomOptions<S, M>): Room<S, M>;
	room<S, M = S, D extends DescMessage = DescMessage>(
		schema: D,
		key: string,
		name: string,
		opts?: RoomOptions<S, M>,
	): Room<S, M>;
	get<D extends DescMessage>(schema: D, key: string): Promise<MessageShape<D> | undefined>;
	getRow<D extends DescMessage>(schema: D, key: string): Promise<Row<MessageShape<D>> | undefined>;
	scan<D extends DescMessage>(schema: D): Promise<RowSet<MessageShape<D>>>;
	query<D extends DescMessage, const Q extends Query<MessageShape<D>>>(
		schema: D,
		q: Q,
		opts?: { include?: Include },
	): Promise<RowSet<MessageShape<D>, TotalOf<Q>>>;
	queryServer<D extends DescMessage, const Q extends Query<MessageShape<D>>>(
		schema: D,
		q: Q,
	): Promise<RowSet<MessageShape<D>, TotalOf<Q>>>;
	watch<D extends DescMessage>(
		schema: D,
		cb: WatchCallback<MessageShape<D>>,
		opts?: WatchOptions,
	): WatchHandle;
	watchQuery<D extends DescMessage, const Q extends Query<MessageShape<D>>>(
		schema: D,
		q: Q,
		cb: WatchCallback<MessageShape<D>, TotalOf<Q>>,
		opts?: WatchOptions,
	): WatchHandle;
	/**
	 * Watches rows by key, delivering each key as it is first read and again
	 * whenever it moves (see KeyedWatchCallback).
	 *
	 * Costs one point read per key, and per commit one read per watched key that
	 * commit named — nothing here scales with the size of the entity. The key set
	 * moves through the handle, so a view can follow what it shows without
	 * resubscribing.
	 *
	 * Throws synchronously if `schema` names an entity this client does not hold;
	 * a watch that cannot be established at all is reported on `opts.onError` and
	 * never delivers. Rows are shared with the entity's other watchers and reused
	 * across commits, so a callback must not mutate what it receives.
	 */
	watchRows<D extends DescMessage>(
		schema: D,
		keys: Iterable<string>,
		cb: KeyedWatchCallback<MessageShape<D>>,
		opts?: KeyedWatchOptions,
	): KeyedWatchHandle;
	resolveRef<D extends DescMessage>(
		holderSchema: D,
		fieldLocalName: string,
		holder: MessageShape<D>,
	): Promise<Row<Message> | Row<Message>[] | undefined>;
	resolveReverseRef<D extends DescMessage>(
		targetSchema: D,
		relationName: string,
		target: MessageShape<D>,
		q?: Query<Message>,
	): Promise<Row<Message>[]>;
	transact(fn: (tx: Transaction<E>) => Promise<void>): Promise<void>;
	pendingCount(): Promise<number>;
	/** This workspace's slice of the sync status. */
	status(): WorkspaceStatus;
	/** Replaces the subscription set for this workspace alone. */
	setSubscriptions(specs: SubscriptionSpec[]): void;
	undo(): Promise<boolean>;
	redo(): Promise<boolean>;
	canUndo(): boolean;
	canRedo(): boolean;
	onUndoChange(cb: () => void): () => void;
	/**
	 * Opens a nested undo scope: a view of this workspace whose writes record
	 * their inverses on a private stack instead of this one's. Reads,
	 * subscriptions, the pending queue and the push are all shared and
	 * unchanged — only undo bookkeeping forks.
	 *
	 * What it is for: a dialog, an inline editor, any UI whose Cmd+Z must not
	 * reach back into what the surrounding view did. Writes issued through the
	 * returned object (its `db`, `mutate`, `text()`, `transact()`) land on its
	 * stack; writes issued through the workspace keep landing on the
	 * workspace's.
	 *
	 * Close it when that UI goes away — the stack is dropped, not merged into
	 * the parent.
	 */
	undoScope(opts?: UndoScopeOptions): UndoScope<E>;
}

/**
 * A nested undo scope (see WorkspaceClient.undoScope): the full workspace
 * surface, plus a lifetime. Everything but undo behaves exactly as it does on
 * the workspace it was forked from.
 */
export interface UndoScope<E extends readonly DescMessage[] = readonly DescMessage[]>
	extends WorkspaceClient<E>
{
	/**
	 * Drops both stacks and detaches the scope from the workspace. Idempotent,
	 * and safe to call with writes still in flight: the mutations themselves are
	 * already queued and unaffected — only their inverses go away.
	 *
	 * The object stays usable afterwards (reads and writes still work), it
	 * simply records nothing and reports `canUndo()`/`canRedo()` false, so a
	 * late callback firing after teardown is not an error.
	 */
	close(): void;
	readonly closed: boolean;
}

/**
 * One workspace's view of a SyncClient: every read, write, transaction and undo
 * step scoped to a single partition. Holds an undo stack, since mutation ids —
 * which the stacks reference — are counted per workspace.
 *
 * A workspace has one ROOT scope (the object `client.ws(id)` hands back) and any
 * number of nested undo scopes forked off it (see undoScope). They differ in
 * exactly one thing: which UndoManager a write records into. Everything else —
 * reads, the subscription set, the pending queue — is the root's, reached
 * through `root`, so forking is free and no read path has to know scopes exist.
 * The client's scope map holds roots only; the root holds its live children, and
 * that set is what every workspace-wide undo action (rejection, compaction,
 * barrier) fans out over.
 */
class WorkspaceScope<E extends readonly DescMessage[]> implements WorkspaceClient<E> {
	readonly mutate: MutateApi;
	private dbApi?: EntityApis<E>;
	/**
	 * In-memory undo/redo over the mutations issued through THIS scope. Cleared
	 * again when a nested scope closes, which is what makes a closed scope inert.
	 */
	protected mgr?: UndoManager;
	/** This workspace's root scope; `this` for a root. */
	readonly root: WorkspaceScope<E>;
	/** The root's compiled subscription set (wire form); unused on a fork. */
	private subs: Subscription[] = [];
	/** Open nested undo scopes (root only). */
	private readonly forks = new Set<WorkspaceScope<E>>();

	constructor(
		/** @internal the owning client; also read by a fork of this scope. */
		readonly client: SyncClient<E>,
		readonly workspaceId: string,
		subscriptions: Subscription[],
		undoOpts?: UndoOptions,
		root?: WorkspaceScope<E>,
	) {
		this.root = root ?? this;
		this.subs = subscriptions;
		if (undoOpts && undoOpts.enabled !== false) {
			this.mgr = new UndoManager(undoOpts.maxDepth ?? 100);
		}
		if (root) root.forks.add(this);
		this.mutate = {
			...this.client.crudMutateApi(
				(crud) => this.client.enqueue(this, { case: 'crud', value: crud }),
				(schema, key) => this.get(schema, key),
			),
			custom: async (name, argsSchema, args) => {
				await this.client.enqueue(this, {
					case: 'custom',
					value: buildCustom(name, argsSchema, args),
				});
			},
			list: (schema, rowKey, field) => this.client.listHandle(this, schema, rowKey, field),
		};
	}

	/** The undo stack writes through this scope record into, if any. */
	get undoMgr(): UndoManager | undefined {
		return this.mgr;
	}

	/** Only a nested undo scope can close; a workspace's own scope never does. */
	get closed(): boolean {
		return false;
	}

	/**
	 * The subscription set is the workspace's, not the scope's: a fork reads
	 * exactly what its root is subscribed to, and re-pointing one re-points all
	 * of them.
	 */
	get subscriptions(): Subscription[] {
		return this.root.subs;
	}

	set subscriptions(next: Subscription[]) {
		this.root.subs = next;
	}

	/**
	 * Every live undo stack in this workspace: the root's own plus one per open
	 * fork. This is the fan-out a rejection, a compaction or a barrier walks —
	 * all of them are workspace-wide events, and a fork that missed one would
	 * keep replaying inverses of mutations that are gone or renumbered.
	 */
	undoManagers(): UndoManager[] {
		const out: UndoManager[] = [];
		if (this.root.mgr) out.push(this.root.mgr);
		for (const fork of this.root.forks) {
			if (fork.mgr) out.push(fork.mgr);
		}
		return out;
	}

	undoScope(opts?: UndoScopeOptions): UndoScope<E> {
		return new NestedUndoScope<E>(this, opts);
	}

	/** @internal detaches a closing fork; see NestedUndoScope.close. */
	dropFork(fork: WorkspaceScope<E>): void {
		this.root.forks.delete(fork);
	}

	get db(): EntityApis<E> {
		return (this.dbApi ??= buildEntityApis(this.client.entities, this, this.client.entityRegistry));
	}

	// Two names for one thing, deliberately: `textHandle` is the name db.ts's
	// SchemaApi calls (it is implemented by transaction handles too, which have
	// no text), while `text` is the WorkspaceClient counterpart of
	// `client.text()` — so a caller holding either object writes the same call.
	textHandle<D extends DescMessage>(schema: D, key: string, field: string): Promise<TextHandle> {
		return this.client.textHandleIn(this, schema, key, field);
	}

	text<D extends DescMessage>(schema: D, key: string, field: string): Promise<TextHandle> {
		return this.textHandle(schema, key, field);
	}

	room<S, M = S>(name: string, opts?: RoomOptions<S, M>): Room<S, M>;
	room<S, M = S, D extends DescMessage = DescMessage>(
		schema: D,
		key: string,
		name: string,
		opts?: RoomOptions<S, M>,
	): Room<S, M>;
	room<S, M = S>(
		a: string | DescMessage,
		b?: string | RoomOptions<S, M>,
		c?: string,
		d?: RoomOptions<S, M>,
	): Room<S, M> {
		return this.client.roomIn(this.workspaceId, a, b, c, d);
	}

	get<D extends DescMessage>(schema: D, key: string): Promise<MessageShape<D> | undefined> {
		return this.client.getIn(this.workspaceId, schema, key);
	}

	getRow<D extends DescMessage>(
		schema: D,
		key: string,
	): Promise<Row<MessageShape<D>> | undefined> {
		return this.client.getRowIn(this.workspaceId, schema, key);
	}

	scan<D extends DescMessage>(schema: D): Promise<RowSet<MessageShape<D>>> {
		return this.client.scanIn(this.workspaceId, schema);
	}

	query<D extends DescMessage, const Q extends Query<MessageShape<D>>>(
		schema: D,
		q: Q,
		opts?: { include?: Include },
	): Promise<RowSet<MessageShape<D>, TotalOf<Q>>> {
		return this.client.queryIn(this.workspaceId, schema, q, opts);
	}

	queryServer<D extends DescMessage, const Q extends Query<MessageShape<D>>>(
		schema: D,
		q: Q,
	): Promise<RowSet<MessageShape<D>, TotalOf<Q>>> {
		return this.client.queryServerIn(this.workspaceId, schema, q);
	}

	groupCount<D extends DescMessage>(
		schema: D,
		q: GroupQuery<MessageShape<D>>,
	): Promise<Group[]> {
		return this.client.groupIn(this.workspaceId, schema, q, false);
	}

	async distinct<D extends DescMessage>(
		schema: D,
		q: GroupQuery<MessageShape<D>>,
	): Promise<(Scalar | undefined)[]> {
		return (await this.client.groupIn(this.workspaceId, schema, q, true)).map((g) => g.value);
	}

	groupCountServer<D extends DescMessage>(
		schema: D,
		q: GroupQuery<MessageShape<D>>,
	): Promise<Group[]> {
		return this.client.groupServerIn(this.workspaceId, schema, q, false);
	}

	async distinctServer<D extends DescMessage>(
		schema: D,
		q: GroupQuery<MessageShape<D>>,
	): Promise<(Scalar | undefined)[]> {
		return (await this.client.groupServerIn(this.workspaceId, schema, q, true)).map((g) => g.value);
	}

	groupRows<D extends DescMessage>(
		schema: D,
		q: GroupRowsQuery<MessageShape<D>>,
	): Promise<RowGroup<MessageShape<D>>[]> {
		return this.client.groupRowsIn(this.workspaceId, schema, q);
	}

	groupRowsServer<D extends DescMessage>(
		schema: D,
		q: GroupRowsQuery<MessageShape<D>>,
	): Promise<RowGroup<MessageShape<D>>[]> {
		return this.client.groupRowsServerIn(this.workspaceId, schema, q);
	}

	watchGroupRows<D extends DescMessage>(
		schema: D,
		q: GroupRowsQuery<MessageShape<D>>,
		cb: (groups: RowGroup<MessageShape<D>>[]) => void,
		opts?: { onError?: (error: unknown) => void },
	): WatchHandle {
		return this.client.registerRowGroupWatch(this.workspaceId, schema, q, cb, opts?.onError);
	}

	watchGroupCount<D extends DescMessage>(
		schema: D,
		q: GroupQuery<MessageShape<D>>,
		cb: (groups: Group[]) => void,
		opts?: { onError?: (error: unknown) => void },
	): WatchHandle {
		return this.client.registerGroupWatch(
			this.workspaceId,
			schema,
			q,
			false,
			cb,
			opts?.onError,
		);
	}

	watchDistinct<D extends DescMessage>(
		schema: D,
		q: GroupQuery<MessageShape<D>>,
		cb: (values: (Scalar | undefined)[]) => void,
		opts?: { onError?: (error: unknown) => void },
	): WatchHandle {
		return this.client.registerGroupWatch(
			this.workspaceId,
			schema,
			q,
			true,
			(groups) => cb(groups.map((g) => g.value)),
			opts?.onError,
		);
	}

	watch<D extends DescMessage>(
		schema: D,
		cb: WatchCallback<MessageShape<D>>,
		opts?: WatchOptions,
	): WatchHandle {
		return this.client.registerWatch(
			this.workspaceId,
			schema,
			cb,
			undefined,
			opts?.include,
			opts?.onError,
			opts?.window,
		);
	}

	watchQuery<D extends DescMessage, const Q extends Query<MessageShape<D>>>(
		schema: D,
		q: Q,
		cb: WatchCallback<MessageShape<D>, TotalOf<Q>>,
		opts?: WatchOptions,
	): WatchHandle {
		return this.client.registerWatch(
			this.workspaceId,
			schema,
			cb as WatchCallback<MessageShape<D>>,
			prepareQuery(schema, compileQuery(q)),
			opts?.include,
			opts?.onError,
			opts?.window,
		);
	}

	watchRows<D extends DescMessage>(
		schema: D,
		keys: Iterable<string>,
		cb: KeyedWatchCallback<MessageShape<D>>,
		opts?: KeyedWatchOptions,
	): KeyedWatchHandle {
		return this.client.registerKeyedWatch(
			this.workspaceId,
			schema,
			keys,
			cb,
			opts?.include,
			opts?.onError,
		);
	}

	resolveRef<D extends DescMessage>(
		holderSchema: D,
		fieldLocalName: string,
		holder: MessageShape<D>,
	): Promise<Row<Message> | Row<Message>[] | undefined> {
		return this.client.resolveRefIn(this.workspaceId, holderSchema, fieldLocalName, holder);
	}

	resolveReverseRef<D extends DescMessage>(
		targetSchema: D,
		relationName: string,
		target: MessageShape<D>,
		q?: Query<Message>,
	): Promise<Row<Message>[]> {
		return this.client.resolveReverseRefIn(
			this.workspaceId,
			targetSchema,
			relationName,
			target,
			q,
		);
	}

	transact(fn: (tx: Transaction<E>) => Promise<void>): Promise<void> {
		return this.client.transactIn(this, fn);
	}

	pendingCount(): Promise<number> {
		return this.client.pendingCountIn(this.workspaceId);
	}

	status(): WorkspaceStatus {
		return this.client.workspaceStatus(this.workspaceId);
	}

	setSubscriptions(specs: SubscriptionSpec[]): void {
		this.client.setWorkspaceSubscriptions(this.workspaceId, specs);
	}

	undo(): Promise<boolean> {
		return this.client.undoIn(this);
	}

	redo(): Promise<boolean> {
		return this.client.redoIn(this);
	}

	canUndo(): boolean {
		return this.undoMgr?.canUndo() ?? false;
	}

	canRedo(): boolean {
		return this.undoMgr?.canRedo() ?? false;
	}

	onUndoChange(cb: () => void): () => void {
		return this.undoMgr?.onChange(cb) ?? (() => {});
	}
}

/**
 * A scope forked off a workspace for its own undo stack (see
 * WorkspaceClient.undoScope). Adds nothing to the workspace surface but a
 * lifetime: everything else it inherits, and its reads go to the same root.
 *
 * It carries a stack only when the client enabled undo at all; otherwise it is
 * the same inert thing `client.ws(id)` is, so a UI can open one unconditionally.
 */
class NestedUndoScope<E extends readonly DescMessage[]> extends WorkspaceScope<E>
	implements UndoScope<E>
{
	private closedFlag = false;

	constructor(parent: WorkspaceScope<E>, opts?: UndoScopeOptions) {
		const base = parent.client.undoOptions;
		super(
			parent.client,
			parent.workspaceId,
			parent.subscriptions,
			// Depth is the scope's own business; whether undo runs at all is the
			// client's, so a fork of an undo-less client stays undo-less.
			base && base.enabled !== false
				? { enabled: true, maxDepth: opts?.maxDepth ?? base.maxDepth }
				: undefined,
			parent.root,
		);
	}

	get closed(): boolean {
		return this.closedFlag;
	}

	close(): void {
		if (this.closedFlag) return;
		this.closedFlag = true;
		this.root.dropFork(this);
		// Clear before dropping the manager so subscribers still attached to it
		// see the stacks empty out — a toolbar rendered during teardown disables
		// itself rather than holding a stale "can undo".
		this.mgr?.barrier();
		this.mgr = undefined;
	}
}

/**
 * What a cross-workspace read may be given: one query for every workspace, or a
 * WorkspaceQuery choosing its predicate per workspace.
 */
export type QueryAcross<M> = Query<M> | WorkspaceQuery<M>;

/** Narrows a cross-workspace read to workspaces the caller names. */
export interface WorkspacesOption {
	/**
	 * Workspaces the read spans, in place of every workspace the client holds.
	 * Rows, `total` and `coverage` then describe these workspaces and no others.
	 *
	 * Order is not significant and duplicates are ignored. An empty array reads
	 * nothing; it is not shorthand for "all". A workspace the client does not hold
	 * contributes no rows.
	 *
	 * Cost falls to the named workspaces rather than the entity across all of
	 * them, and a set naming exactly one is served by that workspace's secondary
	 * indexes, which no read spanning workspaces can use.
	 */
	workspaces?: readonly string[];
}

/** Options for `queryAll`. */
export interface QueryAllOptions extends WorkspacesOption {
	/** Hydrate result rows against this include tree. */
	include?: Include;
}

/** Options for `watchQueryAll`; `WatchOptions` plus the workspace set. */
export interface WatchQueryAllOptions extends WatchOptions, WorkspacesOption {}

/** A local read's page, before coverage and hydration ride on it. */
interface QueryPage<T> {
	rows: Row<T>[];
	total: number | undefined;
	hasMore: boolean;
}

/**
 * State the devtools helper reads that the public surface has no reason to
 * carry; see SyncClient.inspect(). Every method answers for the moment it is
 * called — nothing here is a live view.
 *
 * @internal
 */
export interface ClientInternals {
	/**
	 * Runs a read against the shared store under the read lock, so the view is
	 * not straddled by a concurrent commit. Rejects once the client is closed.
	 */
	read<T>(fn: (tx: KVRead) => Promise<T>): Promise<T>;
	/** Coverage of one entity, per workspace or across every held one. */
	coverageOf(entity: string, workspaceId?: string): Coverage;
	/** Whether this tab holds the account's stream leadership right now. */
	isStreamLeader(): boolean;
	/** Text writer identities held by this tab's open text handles. */
	textPeers(): string[];
	/** Rooms opened through this client and not yet closed. */
	rooms(): readonly Room<unknown, unknown>[];
}

/**
 * Local-read/local-write sync client. Mutations apply optimistically and
 * queue locally (works fully offline); start() begins background sync.
 *
 * One client covers one ACCOUNT and any number of its workspaces: they share a
 * store, a change stream and a push, so nothing about the client grows with
 * workspace count except the data itself. Reads and writes at this level target
 * the default workspace; `client.ws(id)` targets another, and `scanAll`/
 * `queryAll` read across them at once — every workspace by default, or the set
 * `queryAll`'s `workspaces` option names.
 *
 * Two ACCOUNTS need two clients. A credential is attached per request, so two
 * accounts can never share a stream — federating across them is forced, not
 * chosen.
 */
export class SyncClient<const E extends readonly DescMessage[] = readonly DescMessage[]> {
	/** Store partition; see SyncClientOptions.accountId. */
	readonly accountId: string;
	/**
	 * The origin-scoped names this client claims, derived from the account and
	 * SyncClientOptions.storagePrefix.
	 */
	readonly storageNames: StorageNames;
	/**
	 * This tab's identity for the ephemeral plane: minted per instance, never
	 * persisted, and the key every presence record this client publishes lives
	 * under.
	 *
	 * Distinct from `clientId()`, which is the per-device idempotency handle
	 * mutations are keyed by and survives a reload. One device holds one client
	 * id and as many session ids as it has tabs.
	 */
	readonly sessionId: string = crypto.randomUUID();
	private opts: SyncClientOptions<E>;
	/** @internal shared with the workspace scopes. */
	readonly entityRegistry: EntityRegistry;
	/**
	 * Compiled secondary indexes, shared by the write path (maintenance) and the
	 * read path (planning) so both agree on which index is which and on the spec
	 * fingerprint its entries live under.
	 */
	readonly indexes: IndexCache;
	/** See SyncClientOptions.debugVerifyIndexReads. */
	private readonly verifyIndexReads: boolean;
	private mirrors: MirrorMap;
	private kvPromise?: Promise<KVStore>;
	/**
	 * Guards all KV writes (enqueue, clientId minting, rebase). Cross-tab in
	 * browsers via Web Locks; the lock name matches the IDB database name so
	 * tabs sharing a database share the lock. NON-REENTRANT: never call
	 * kvMutex.run inside a kvMutex.run callback.
	 */
	private kvMutex: Mutex;
	private hubInstance?: WatcherHub;
	private hubPromise?: Promise<WatcherHub>;
	private changeShare?: ChangeShare;
	private coordinator?: Coordinator;
	/**
	 * Cross-tab intent record for this instance (sync/intent.ts): created in
	 * start() and shared with the Coordinator; the enqueue path joins its
	 * undo-ref updates into the enqueue transaction.
	 */
	private intentReg?: IntentRegistry;
	/**
	 * The ephemeral plane's coordinator (sync/presence.ts), created on the first
	 * room rather than at start(): an app that uses no rooms should hold no
	 * second stream and claim no second Web Lock.
	 */
	private presence?: PresenceCoordinator;
	/**
	 * The in-flight (or completed) start, memoized so concurrent callers share
	 * one. Cleared again when that start REJECTS, which is what makes start()
	 * retryable; see start().
	 */
	private startPromise?: Promise<void>;
	private closed = false;
	/** Subscribers of the dead-letter surface; see onMutationRejected(). */
	private rejectionListeners = new Set<(info: RejectionInfo) => void>();
	/** Subscribers of the observability stream; see onSyncEvent(). */
	private syncEventListeners = new Set<SyncEventHandler>();
	/**
	 * Text writer identities held by this tab's open handles, for inspect().
	 * The coordinator's transient restore identities are not in it — they are
	 * held per restore batch, not by anything the app opened.
	 */
	private liveTextPeers = new Set<string>();
	/** Rooms opened through this client and not yet closed, for inspect(). */
	private openRooms = new Set<Room<unknown, unknown>>();
	private statusTracker = new StatusTracker();
	private scopes = new Map<string, WorkspaceScope<E>>();
	/**
	 * Workspaces with an index build running. `again` records that a request
	 * arrived while one was in flight; see buildWorkspaceIndexes().
	 */
	private indexBuilds = new Map<string, { again: boolean }>();
	private defaultWorkspaceId?: string;
	private transportPromise?: Promise<Transport>;
	/**
	 * The memoized device identity; see clientId(). Dropped again when a format
	 * upgrade discards the stored one.
	 */
	private clientIdPromise?: Promise<string>;
	/** Slot pool for text writer identities on this device (see text/peer.ts). */
	private peerLeasesPromise?: Promise<PeerLeases>;

	constructor(opts: SyncClientOptions<E>) {
		const workspaces = resolveWorkspaces(opts);
		this.opts = opts;
		this.accountId = resolveAccountId(opts, workspaces);
		const prefix = opts.storagePrefix ?? DEFAULT_STORAGE_PREFIX;
		validateStoragePrefix(prefix);
		this.storageNames = storageNames(prefix, this.accountId);
		this.kvMutex = createKvMutex(this.storageNames.kvLock);
		if (opts.onStatusChange) this.statusTracker.onChange(opts.onStatusChange);
		this.entityRegistry = new EntityRegistry(opts.entities);
		if (opts.mergers) this.entityRegistry.setMergers(opts.mergers);
		this.indexes = new IndexCache(this.entityRegistry);
		this.verifyIndexReads = opts.debugVerifyIndexReads ?? debugVerifyTouchesEnv();
		this.mirrors = opts.mutators ?? {};
		const initial = this.compileSubscriptions(opts.subscriptions ?? []);
		for (const ws of workspaces) this.createScope(ws, initial);
		this.defaultWorkspaceId = opts.defaultWorkspace
			?? (workspaces.length === 1 ? workspaces[0] : undefined);
		if (this.defaultWorkspaceId && !this.scopes.has(this.defaultWorkspaceId)) {
			throw new Error(
				`defaultWorkspace "${this.defaultWorkspaceId}" is not one of the configured workspaces`,
			);
		}
		this.statusTracker.setDefaultWorkspace(this.defaultWorkspaceId);
	}

	/** @internal the schemas this client was configured with. */
	get entities(): E {
		return this.opts.entities;
	}

	/** The workspaces this client currently syncs, in the order they were added. */
	get workspaces(): string[] {
		return [ ...this.scopes.keys() ];
	}

	/**
	 * The workspace the client-level API binds to, or undefined when the client
	 * holds several and none was named.
	 */
	get defaultWorkspace(): string | undefined {
		return this.defaultWorkspaceId;
	}

	/**
	 * The workspace-scoped view of `workspaceId`. Throws for a workspace this
	 * client does not hold — add it first with addWorkspace().
	 */
	ws(workspaceId: string): WorkspaceClient<E> {
		const scope = this.scopes.get(workspaceId);
		if (!scope) {
			throw new Error(
				`workspace "${workspaceId}" is not held by this client; call addWorkspace() first`,
			);
		}
		return scope;
	}

	/**
	 * Starts syncing another workspace. It bootstraps on the next cycle without
	 * disturbing the others, and joins the existing stream and pushes. Adding a
	 * workspace already held only replaces its subscription set.
	 */
	addWorkspace(workspaceId: string, specs?: SubscriptionSpec[]): WorkspaceClient<E> {
		this.assertOpen();
		validateWorkspaceId(workspaceId);
		const compiled = this.compileSubscriptions(specs ?? this.opts.subscriptions ?? []);
		const existing = this.scopes.get(workspaceId);
		if (existing) {
			existing.subscriptions = compiled;
			this.coordinator?.setSubscriptions(workspaceId, compiled);
			return existing;
		}
		const scope = this.createScope(workspaceId, compiled);
		this.coordinator?.addWorkspace(workspaceId, compiled);
		return scope;
	}

	/**
	 * Stops syncing a workspace, optionally deleting its local data (see
	 * RemoveWorkspaceOptions.purge). Removing the default workspace leaves the
	 * client without one, so the client-level API throws until another is chosen
	 * — pass `defaultWorkspace` on a fresh client or use `client.ws(id)`.
	 * Idempotent for a workspace the client does not hold.
	 *
	 * A purge discards whatever was still queued for the workspace; the result
	 * carries those mutations. Discarding is not an error — it is what a purge
	 * is — so this reports rather than throws, and the same mutations also reach
	 * `onMutationRejected` for apps that would otherwise ignore the result.
	 */
	async removeWorkspace(
		workspaceId: string,
		opts?: RemoveWorkspaceOptions,
	): Promise<RemoveWorkspaceResult> {
		if (!this.scopes.delete(workspaceId)) {
			return { removed: false, purged: false, discarded: [] };
		}
		this.coordinator?.removeWorkspace(workspaceId);
		this.statusTracker.removeWorkspace(workspaceId);
		if (this.defaultWorkspaceId === workspaceId) {
			this.defaultWorkspaceId = undefined;
			this.statusTracker.setDefaultWorkspace(undefined);
		}
		if (!opts?.purge) return { removed: true, purged: false, discarded: [] };
		const discarded = await this.purgeWorkspace(workspaceId);
		return { removed: true, purged: true, discarded };
	}

	/**
	 * Deletes every trace of a workspace from the local store: confirmed rows,
	 * optimistic overlay and its text log, bootstrap staging, pending queue and
	 * cursor. Watchers are notified so the UI drops the rows immediately.
	 *
	 * It sweeps every entity the STORE syncs rather than the ones this build
	 * registered, for the reason wipeConfirmedState does: another build's tab may
	 * have filled rows this one cannot read, and leaving them behind would hand
	 * them to whichever build registers that entity next.
	 *
	 * The mutation-id counter and its push mark are the exception and survive:
	 * the server's per-(workspace, client) watermark is not purged with the
	 * local data, and the clientId is account-wide, so a workspace added back
	 * would mint ids the server has already applied and every edit would be
	 * dropped as already-acked. The counter can never trail that watermark —
	 * the watermark only advances on ids this client minted — so keeping it is
	 * exactly enough to resume above it.
	 *
	 * The pending queue is read inside the same transaction that deletes it, so
	 * what comes back is exactly what was lost — a queue read before the call
	 * could have been appended to in between.
	 */
	private async purgeWorkspace(workspaceId: string): Promise<DiscardedMutation[]> {
		const kv = await this.kv();
		const touched = new TouchSet();
		const discarded = await this.kvMutex.run(async () => {
			const tx = await kv.write();
			try {
				const queued = await readPending(tx, workspaceId);
				const doomed: string[] = [];
				for (const entity of await syncableEntities(tx, this.entityRegistry)) {
					for await (const [ k ] of tx.scan(confirmedWsPrefix(entity, workspaceId))) doomed.push(k);
					for await (const [ k ] of tx.scan(overlayWsPrefix(entity, workspaceId))) doomed.push(k);
					// The text log is overlay state under a root of its own, so it does
					// not come along with the `o/` sweep above; left behind, its
					// fragments would fold onto whatever rows land under this workspace
					// next.
					for await (const [ k ] of tx.scan(overlayTextWsPrefix(entity, workspaceId))) {
						doomed.push(k);
					}
					// The index positions ride on the confirmed rows above, so they
					// are deleted with them and need nothing here.
					touched.entity(workspaceId, entity);
				}
				const kept = new Set([
					metaLastMutationId(workspaceId),
					metaPushedThrough(workspaceId),
				]);
				for (
					const prefix of [
						stagingWsRoot(workspaceId),
						pendingPrefix(workspaceId),
						metaWsPrefix(workspaceId),
					]
				) {
					for await (const [ k ] of tx.scan(prefix)) {
						if (!kept.has(k)) doomed.push(k);
					}
				}
				for (const k of doomed) await tx.del(k);
				await tx.commit();
				return queued.map((m) => ({ workspaceId, mutationId: m.mutationId, mutation: m }));
			} catch (err) {
				tx.release();
				throw err;
			}
		});
		await this.notifyChange(touched);
		// After the commit and the notify: apps observe the rows as already gone,
		// exactly as they do for a rejection rolled back before its callback.
		for (const d of discarded) {
			this.reportRejection({
				workspaceId,
				mutationId: d.mutationId,
				errorCode: CodeWorkspacePurged,
				errorMessage: `workspace ${workspaceId} was purged locally`,
				...describeMutation(d.mutation),
			});
		}
		return discarded;
	}

	private createScope(workspaceId: string, subscriptions: Subscription[]): WorkspaceScope<E> {
		const scope = new WorkspaceScope<E>(this, workspaceId, subscriptions, this.opts.undo);
		this.scopes.set(workspaceId, scope);
		this.statusTracker.addWorkspace(workspaceId);
		return scope;
	}

	/**
	 * Every live undo stack in a workspace: its own plus one per open undo scope
	 * (see WorkspaceScope.undoManagers). Empty for a workspace this client no
	 * longer holds, which is what makes the workspace-wide fan-outs — rejection,
	 * compaction, intent refs — safe to call unconditionally.
	 */
	private undoManagersOf(workspaceId: string): UndoManager[] {
		return this.scopes.get(workspaceId)?.undoManagers() ?? [];
	}

	/** @internal the client-wide undo settings a nested scope inherits. */
	get undoOptions(): UndoOptions | undefined {
		return this.opts.undo;
	}

	/** The default workspace's scope, or a clear error explaining what to do. */
	private defaultScope(): WorkspaceScope<E> {
		const id = this.defaultWorkspaceId;
		const scope = id ? this.scopes.get(id) : undefined;
		if (!scope) {
			throw new Error(
				'this client has no default workspace; use client.ws(workspaceId) '
					+ 'or set defaultWorkspace',
			);
		}
		return scope;
	}

	/**
	 * The workspace this client's own read/write methods target. Equivalent to
	 * `client.ws(client.defaultWorkspace)`.
	 */
	get workspaceId(): string {
		return this.defaultScope().workspaceId;
	}

	/** Writes bound to the default workspace. */
	get mutate(): MutateApi {
		return this.defaultScope().mutate;
	}

	/**
	 * Per-entity typed facade derived from the message names in `entities`,
	 * bound to the default workspace: `client.db.todo.mutate.put({...})`,
	 * `client.db.todo.scan()`. Throws if two entities share a simple name; the
	 * schema-first methods on the client itself remain the escape hatch.
	 */
	get db(): EntityApis<E> {
		return this.defaultScope().db;
	}

	/** Opens a live text-field handle in the default workspace (see TextHandle). */
	text<D extends DescMessage>(schema: D, key: string, field: string): Promise<TextHandle> {
		return this.textHandleIn(this.defaultScope(), schema, key, field);
	}

	/** Opens a room in the default workspace (see WorkspaceClient.room). */
	room<S, M = S>(name: string, opts?: RoomOptions<S, M>): Room<S, M>;
	room<S, M = S, D extends DescMessage = DescMessage>(
		schema: D,
		key: string,
		name: string,
		opts?: RoomOptions<S, M>,
	): Room<S, M>;
	room<S, M = S>(
		a: string | DescMessage,
		b?: string | RoomOptions<S, M>,
		c?: string,
		d?: RoomOptions<S, M>,
	): Room<S, M> {
		return this.roomIn(this.defaultScope().workspaceId, a, b, c, d);
	}

	/**
	 * Opens a room in one workspace, resolving both call shapes: `(name, opts)`
	 * for an unanchored room, `(schema, key, name, opts)` for one anchored to a
	 * row.
	 *
	 * @internal
	 */
	roomIn<S, M = S>(
		workspaceId: string,
		a: string | DescMessage,
		b?: string | RoomOptions<S, M>,
		c?: string,
		d?: RoomOptions<S, M>,
	): Room<S, M> {
		let topic: PresenceTopic;
		let opts: RoomOptions<S, M> | undefined;
		if (typeof a === 'string') {
			topic = { workspaceId, name: a };
			opts = b as RoomOptions<S, M> | undefined;
		} else {
			const def = this.entityRegistry.bySchema(a);
			topic = { workspaceId, name: c!, entity: def.name, key: b as string };
			opts = d;
		}
		const presence = this.presenceCoordinator();
		const room = createRoom<S, M>(
			topic.name,
			this.sessionId,
			opts,
			(sink) => presence.attach(topic, sink),
		);
		// Registered for inspect(), and re-wrapped so closing unregisters. Spread
		// is safe: createRoom returns a literal whose methods close over their
		// state rather than reading `this`.
		const tracked: Room<S, M> = {
			...room,
			close: async () => {
				this.openRooms.delete(tracked as Room<unknown, unknown>);
				await room.close();
			},
		};
		this.openRooms.add(tracked as Room<unknown, unknown>);
		return tracked;
	}

	/** The ephemeral plane's coordinator, started with the first room. */
	private presenceCoordinator(): PresenceCoordinator {
		this.presence ??= new PresenceCoordinator({
			transport: () => this.resolveTransport(),
			sessionId: this.sessionId,
			presenceLock: this.storageNames.presenceLock,
			presenceChannel: this.storageNames.presenceChannel,
			watchEnabled: this.opts.presenceEnabled !== false,
		});
		return this.presence;
	}

	/**
	 * Builds the put/patch/update/del facade over a `sink` that decides where
	 * each built CrudMutation goes: a workspace scope's facade enqueues it as its
	 * own mutation, a transact() handle's facade pushes it into the batch buffer.
	 * `read` supplies the base row `update` diffs against, from the same place
	 * that facade's reads come from — inside a transaction that is the pre-batch
	 * state, matching every other read on the handle.
	 * @internal
	 */
	crudMutateApi(
		sink: (crud: CrudMutation) => Promise<void> | void,
		read: <D extends DescMessage>(schema: D, key: string) => Promise<MessageShape<D> | undefined>,
	): CrudMutateApi {
		// Written as one wide signature rather than inline in the object literal:
		// `patch` is overloaded on the interface, and a single implementation
		// satisfying both arms cannot be contextually typed from them.
		const patch = async <D extends DescMessage>(
			schema: D,
			key: string,
			init: TextInit<MessageInitShape<D>>,
			opts?: PatchOptions,
		): Promise<void> => {
			const def = this.entityRegistry.bySchema(schema);
			const { init: cleaned, texts } = extractTextStrings(schema, init);
			// The mask derives from the ORIGINAL init so extracted text
			// fields keep their entry (explicit paths pass through).
			const paths = opts?.paths ?? maskFromInit(schema, init as object);
			// Authorized on the effective mask, which is what the server sees.
			assertWritePolicy(def, CrudOp.PATCH, opts?.expectedVersion, paths);
			const crud = buildPatch(
				def,
				schema,
				key,
				cleaned as MessageInitShape<D>,
				paths,
				opts?.expectedVersion,
			);
			attachTextStrings(crud, texts);
			await sink(crud);
		};
		return {
			put: async (schema, value, opts) => {
				const def = this.entityRegistry.bySchema(schema);
				assertWritePolicy(def, CrudOp.PUT, opts?.expectedVersion);
				// String-valued text fields ride a side map to enqueue, where
				// they diff against the merged row under a leased peer.
				const { init, texts } = extractTextStrings(schema, value);
				// A text string names its field outright, the way a patch mask
				// does, so a protected one is refused rather than carried over.
				for (const path of texts?.keys() ?? []) {
					const head = protectedFieldOf(def, path);
					if (head !== undefined) {
						throw new Error(
							`entity "${def.name}": field "${head}" may not be written by a client, so the text `
								+ `value for "${path}" is refused.`,
						);
					}
				}
				let msg = create(schema, init as MessageInitShape<typeof schema>);
				if (def.protectedFields.length > 0) {
					msg = carryProtectedFields(def, schema, msg, await read(schema, keyOf(def, msg)));
				}
				const crud = buildPut(def, schema, msg, opts?.expectedVersion);
				attachTextStrings(crud, texts);
				await sink(crud);
			},
			patch,
			update: async (schema, key, recipe, opts) => {
				const def = this.entityRegistry.bySchema(schema);
				const base = await read(schema, key);
				if (base === undefined) {
					throw new Error(
						`update: no row "${key}" for entity "${def.name}" to update. The row may exist on `
							+ `the server without being synced here — read it first, or use put() to write `
							+ `one unconditionally.`,
					);
				}
				// The recipe edits a private copy: a row the app is already holding
				// must not change under it, and a recipe that throws must leave
				// nothing behind.
				const draft = clone(schema, base);
				recipe(draft);
				const paths = maskFromDiff(schema, base, draft);
				// Nothing moved, nothing to send. Dropping the write here rather than
				// enqueuing an empty mask keeps a save-on-every-keystroke caller off
				// the wire, and an empty mask is a no-op merge anyway.
				if (paths.length === 0) return;
				const keyFields = new Set(def.keyFields.map((f) => f.name));
				const changedKeyField = paths.find((p) => keyFields.has(p));
				if (changedKeyField !== undefined) {
					throw new Error(
						`update: recipe changed key field "${changedKeyField}" of entity "${def.name}". `
							+ `A row's identity is fixed; write a new row instead.`,
					);
				}
				await patch(schema, key, draft as TextInit<MessageInitShape<typeof schema>>, {
					paths,
					expectedVersion: opts?.expectedVersion,
				});
			},
			del: async (schema, key, opts) => {
				const def = this.entityRegistry.bySchema(schema);
				assertWritePolicy(def, CrudOp.DELETE, opts?.expectedVersion);
				await sink(buildDelete(def, key, opts?.expectedVersion));
			},
		};
	}

	/**
	 * Builds a movable-list handle (see ListHandle). All operations read the
	 * current sorted snapshot, allocate fractional positions / element ids, and
	 * enqueue an ordinary row PATCH — insert/remove via per-item expansion, move
	 * via an explicit `field.<id>.<pos>` mask so only the position changes (LWW),
	 * preserving the element's other fields against a concurrent edit.
	 * @internal
	 */
	/**
	 * Opens a live handle over a collaborative text field (see TextHandle).
	 * `fieldName` is the field's dotted proto path; a document inside a keyed
	 * list or map element is named by its instance path ("notes.<id>.body"),
	 * which addresses that one element's document. The field must carry
	 * (syncplane.v1.field_behavior).text and the row — and the addressed
	 * element — must already exist: edits ship as PATCHes, which never upsert
	 * the row, and an edit to a removed element is dropped by delete-wins.
	 * Each handle leases its own writer identity for as long as it is open;
	 * close() returns the slot to the pool (see text/peer.ts).
	 * @internal
	 */
	async textHandleIn<D extends DescMessage>(
		scope: WorkspaceScope<E>,
		schema: D,
		rowKey: string,
		fieldName: string,
	): Promise<TextHandle> {
		const def = this.entityRegistry.bySchema(schema);
		// A bare field may be named by its localName for compatibility with the
		// property the row object exposes; a dotted path uses proto names, the
		// mask form.
		const topLevel = fieldName.includes('.')
			? undefined
			: schema.fields.find((f) => f.name === fieldName || f.localName === fieldName);
		const path = topLevel !== undefined ? topLevel.name : fieldName;
		const schemaPath = textSchemaPathOf(schema, path);
		if (schemaPath === undefined) {
			throw new Error(`${schema.typeName}.${fieldName} is not a text field`);
		}
		// undefined means the row — or, for a document inside a keyed list or
		// map, the addressed element — does not exist; an existing position with
		// no document yet reads as the empty document.
		const fieldOf = (row: MessageShape<D> | undefined): Uint8Array | undefined => {
			if (row === undefined) return undefined;
			try {
				if (!textInstancePaths(schema, schemaPath, row).includes(path)) return undefined;
				return textFieldBytes(schema, path, row) ?? new Uint8Array(0);
			} catch {
				return undefined;
			}
		};
		return openTextHandle({
			acquirePeer: async () => {
				const lease = await (await this.textPeerLeases()).acquire();
				this.liveTextPeers.add(lease.peer);
				return {
					peer: lease.peer,
					release: () => {
						this.liveTextPeers.delete(lease.peer);
						lease.release();
					},
				};
			},
			load: async () => fieldOf(await scope.get(schema, rowKey)),
			patch: (state, inverseOps, coalesce) => {
				const crud = buildPatch(
					def,
					schema,
					rowKey,
					textFieldInit(schema, path, state) as MessageInitShape<D>,
					[ path ],
				);
				// The symbolic op-level inverse replaces the snapshot inverse:
				// a state-CRDT snapshot merged back would be a silent no-op.
				const override: Inverse | undefined = inverseOps && {
					kind: 'text',
					entity: def.name,
					key: rowKey,
					field: path,
					ops: inverseOps,
				};
				return this.enqueue(scope, { case: 'crud', value: crud }, undefined, {
					overrides: [ override ],
					coalesce,
				});
			},
			watch: (cb) => {
				const handle = scope.watchRows(schema, [ rowKey ], (rows) => {
					if (!rows.has(rowKey)) return;
					cb(fieldOf(rows.get(rowKey)?.value));
				});
				return () => handle.close();
			},
		});
	}

	listHandle<D extends DescMessage>(
		scope: WorkspaceScope<E>,
		schema: D,
		rowKey: string,
		fieldName: string,
	): ListHandle {
		const field = schema.fields.find((f) => f.name === fieldName || f.localName === fieldName);
		const order = field ? orderFieldOf(field) : undefined;
		if (!field || !order || field.fieldKind !== 'list' || field.listKind !== 'message') {
			throw new Error(`${schema.typeName}.${fieldName} is not a movable ordered list`);
		}
		const keyFields = keyFieldsFor(field);
		if (keyFields.length !== 1) {
			throw new Error(`${schema.typeName}.${fieldName}: movable list handle needs a single key field`);
		}
		const idLocal = keyFields[0]!.localName;
		const posLocal = field.message.fields.find((f) => f.name === order)!.localName;
		const listLocal = field.localName;
		type Elem = Record<string, unknown>;

		const snapshot = async (): Promise<Elem[]> => {
			const row = await this.getIn(scope.workspaceId, schema, rowKey);
			if (row === undefined) throw new Error(`row "${rowKey}" not found`);
			return ((row as Record<string, unknown>)[listLocal] as Elem[] | undefined) ?? [];
		};
		const posOf = (e: Elem | undefined): string | null => (e ? String(e[posLocal] ?? '') : null);
		const patch = (init: Record<string, unknown>, paths: string[]): Promise<void> =>
			scope.mutate.patch(schema, rowKey, init as MessageInitShape<D>, { paths });

		const insertAt = async (index: number, itemInit: Elem): Promise<string> => {
			const list = await snapshot(); // sorted on read
			const clientId = await this.clientId();
			const pos = generateKeyBetween(posOf(list[index - 1]), posOf(list[index]), clientId);
			const id = newElementId(clientId);
			await patch({ [listLocal]: [ ...list, { ...itemInit, [idLocal]: id, [posLocal]: pos } ] }, [
				field.name,
			]);
			return id;
		};

		return {
			insertAt,
			insertBefore: async (beforeId, itemInit) => {
				const list = await snapshot();
				const idx = list.findIndex((e) => e[idLocal] === beforeId);
				return insertAt(idx === -1 ? list.length : idx, itemInit);
			},
			move: async (id, toIndex) => {
				const without = (await snapshot()).filter((e) => e[idLocal] !== id);
				const clientId = await this.clientId();
				const pos = generateKeyBetween(posOf(without[toIndex - 1]), posOf(without[toIndex]), clientId);
				const encId = encodeItemKey({ [idLocal]: id } as unknown as Message, keyFields);
				await patch({ [listLocal]: [ { [idLocal]: id, [posLocal]: pos } ] }, [
					`${field.name}.${encId}.${order}`,
				]);
			},
			remove: async (id) => {
				const list = await snapshot();
				await patch({ [listLocal]: list.filter((e) => e[idLocal] !== id) }, [ field.name ]);
			},
		};
	}

	/**
	 * The device's pool of text writer identities, keyed by clientId. A failed
	 * mint is not cached — the pool needs the clientId, whose minting touches the
	 * store and can fail transiently.
	 * @internal
	 */
	private textPeerLeases(): Promise<PeerLeases> {
		this.peerLeasesPromise ??= this.clientId().then(
			(clientId) => createPeerLeases(clientId, this.storageNames.textPeerLocks),
			(err: unknown) => {
				this.peerLeasesPromise = undefined;
				throw err;
			},
		);
		return this.peerLeasesPromise;
	}

	private kv(): Promise<KVStore> {
		this.kvPromise ??= this.opts.kvStore
			? Promise.resolve(this.opts.kvStore)
			: typeof indexedDB !== 'undefined'
			? IDBKVStore.open(this.storageNames.store)
			: Promise.resolve(new MemoryKVStore());
		return this.kvPromise;
	}

	/**
	 * The one shared WatcherHub. Memoized by PROMISE, not by the resolved
	 * instance: construction awaits the store, so two callers racing in the same
	 * tick (two watch() registrations, or a watch racing a commit's notify) would
	 * otherwise each build a hub, the second overwriting the first — leaving
	 * watchers registered on an orphaned hub that no notify ever reaches.
	 */
	private hub(): Promise<WatcherHub> {
		if (this.hubInstance) return Promise.resolve(this.hubInstance);
		this.hubPromise ??= (async () => {
			const hub = new WatcherHub(await this.kv(), this.entityRegistry, this.kvMutex, {
				debugVerifyTouches: this.opts.debugVerifyTouches,
				coverageOf: (entity, scope, rels) => this.coverage.coverageOf(entity, scope, rels),
				indexes: (entity) => this.indexes.for(entity),
				onRead: (event) => this.emit(event),
			});
			hub.setRefClient(this.refClient());
			// close() may have torn down during the kv await. Do not resurrect the
			// shared hub or its cross-tab BroadcastChannel (close() already stopped
			// the old one) — hand back a transient, watcher-less hub. It is inert
			// (nothing is subscribed, no changeShare) and gets collected on drop.
			if (this.closed) {
				this.hubPromise = undefined;
				return hub;
			}
			this.hubInstance = hub;
			// A commit in another tab updates the shared store but only that tab's
			// hub; mirror its touched set into ours so our watchers stay fresh.
			this.changeShare = startChangeShare(this.storageNames.changeChannel, (tokens) => {
				void hub.notify(TouchSet.fromTokens(tokens));
			});
			return hub;
		})();
		return this.hubPromise;
	}

	/**
	 * The workspace-aware resolver the ref layer and include-hydration run
	 * against. Not `this`: the public read methods take no workspace (they bind
	 * to the default one), while reference resolution must always name the
	 * holder's own workspace.
	 */
	private refClient(): RefClient {
		return {
			getRow: (schema, workspaceId, key) => this.getRowIn(workspaceId, schema, key),
			query: (schema, workspaceId, q) => this.queryIn(workspaceId, schema, q),
		};
	}

	/**
	 * Notifies local watchers of a committed touched set and fans it out to
	 * other tabs. Every path that commits a write (enqueue, rebase, bootstrap,
	 * rollback, purge) routes its notification through here so the broadcast can
	 * never be forgotten alongside a local notify.
	 */
	private async notifyChange(touched: TouchSet): Promise<void> {
		// An in-flight enqueue can land here after close(); skip rather than have
		// hub() spin up a fresh WatcherHub/BroadcastChannel past teardown.
		if (this.closed) return;
		const hub = await this.hub();
		if (this.closed) return;
		await hub.notify(touched);
		// Peers get the compat encoding (keyed tokens PLUS a wildcard per pair),
		// so a tab of an older build still full-scans while a current one patches.
		this.changeShare?.broadcast(touched.broadcastTokens());
	}

	private assertOpen(): void {
		if (this.closed) throw new Error('SyncClient is closed');
	}

	/**
	 * Begins background sync (bootstrap on first run, live stream + poll loops).
	 *
	 * Idempotent, and RETRYABLE: it does not wait for the first sync cycle (an
	 * unreachable server surfaces on the status, not here), but the local steps
	 * it does await — configuration, the KV format check, coordinator start-up —
	 * can reject, and a rejected start leaves the client startable again.
	 * Latching a "started" flag before those awaits instead would turn every
	 * later start() into a silent no-op on a client that never syncs, with no
	 * way back short of constructing a new one.
	 */
	async start(): Promise<void> {
		this.assertOpen();
		if (this.startPromise) return this.startPromise;
		const run = this.startUp();
		this.startPromise = run;
		run.catch(() => {
			// Only drop OUR memo: a start that failed after close(), or one racing
			// a later successful start, must not unlatch someone else's.
			if (this.startPromise === run) this.startPromise = undefined;
		});
		return run;
	}

	private async startUp(): Promise<void> {
		const transport = await this.resolveTransport();
		await this.ensureFormatVersion(transport);
		// After the format check, which may have wiped the store this reads: a
		// wiped store records no entities, and every one of ours is new again.
		const newEntities = await this.reconcileEntities();
		// Bring the hub (and its cross-tab change receiver) up before sync so
		// other tabs' commits reach our watchers even before our first local one.
		await this.hub();
		// close() may have run during any of the awaits above. It is one-shot
		// and already tore down, finding no coordinator to stop; bail before
		// creating one whose stream/poll loops would run forever against the now
		// closed store. (If close() instead races the coordinator.start() await
		// below, it observes the assigned this.coordinator and stops it.)
		if (this.closed) return;
		this.intentReg = new IntentRegistry(this.sessionId, await this.kv(), this.kvMutex);
		const coordinator = new Coordinator({
			transport,
			kv: await this.kv(),
			kvMutex: this.kvMutex,
			storageNames: this.storageNames,
			clientId: await this.clientId(),
			registry: this.entityRegistry,
			indexes: this.indexes,
			onIndexesStale: (workspaceId) => this.startIndexBuild(workspaceId),
			mirrors: this.mirrors,
			notify: (touched) => this.notifyChange(touched),
			status: this.statusTracker,
			scopes: this.ownScopes(),
			// Resolved through the registry so an unregistered schema fails here,
			// at construction, rather than as a name the server silently ignores.
			priorityEntities: this.opts.bootstrapPriority?.map(
				(schema) => this.entityRegistry.bySchema(schema).name,
			),
			onMutationRejected: (info) => this.reportRejection(info),
			onSyncEvent: this.dispatchSyncEvent,
			pollIntervalMs: this.opts.pollIntervalMs,
			pushDebounceMs: this.opts.pushDebounceMs,
			pushMaxDelayMs: this.opts.pushMaxDelayMs,
			streamEnabled: this.opts.streamEnabled,
			pullLimit: this.opts.pullLimit,
			poisonThreshold: this.opts.poisonThreshold,
			maxMutationsPerPush: this.opts.maxMutationsPerPush,
			maxWorkspacesPerRequest: this.opts.maxWorkspacesPerRequest,
			// Undo references forward mutation ids; let compaction protect the ones
			// whose inverse is a snapshot of one specific mutation, and remap them
			// all, so squashing a queue never corrupts that workspace's undo stacks.
			undoRefIds: this.opts.undo
				? (ws) => unionOf(this.undoManagersOf(ws).map((m) => m.foldBarrierIds()))
				: undefined,
			onCompacted: this.opts.undo
				? (ws, remap) => {
					for (const mgr of this.undoManagersOf(ws)) mgr.remapIds(remap);
				}
				: undefined,
			clientRevision: this.opts.schemaRevision,
			onSchemaSkew: this.opts.onSchemaSkew,
			intent: this.intentReg,
			forceResync: newEntities,
		});
		// Published before start() so a racing close() finds it (see above), and
		// withdrawn again if that start fails, so a retry builds a fresh one rather
		// than leaving a half-started coordinator behind it.
		this.coordinator = coordinator;
		try {
			await coordinator.start();
		} catch (err) {
			if (this.coordinator === coordinator) this.coordinator = undefined;
			await coordinator.stop().catch(() => {});
			throw err;
		}
		this.startIndexBuild();
	}

	/**
	 * Backfills index positions onto rows that predate an index, in the background.
	 *
	 * Deliberately not awaited: nothing depends on it. Until an index is marked
	 * built every query scans, which is correct and merely slower, so making
	 * start() wait would trade a real cost (a slow open) for no guarantee. A
	 * failure is reported and dropped for the same reason — an unbuilt index is
	 * a performance state, not a broken one.
	 *
	 * Runs for every held workspace at start(), and for one workspace whenever a
	 * bootstrap swap leaves its indexes unbuilt.
	 */
	private startIndexBuild(workspaceId?: string): void {
		if (this.indexes.empty) return;
		if (workspaceId !== undefined) {
			this.buildWorkspaceIndexes(workspaceId);
			return;
		}
		for (const ws of this.scopes.keys()) this.buildWorkspaceIndexes(ws);
	}

	/**
	 * Builds one workspace's indexes, coalescing concurrent requests.
	 *
	 * A build in flight is not cancelled and restarted — it may be most of the
	 * way through a large entity — but it also cannot be trusted to finish the
	 * job, since the swap that asked for a rebuild replaced the rows it was
	 * walking. So the request is remembered and the walk simply repeated; the
	 * first pass abandons its markers on the generation check (see
	 * index-build.ts) and the repeat claims them, while an unchanged workspace
	 * costs one key lookup per index.
	 */
	private buildWorkspaceIndexes(workspaceId: string): void {
		const inflight = this.indexBuilds.get(workspaceId);
		if (inflight !== undefined) {
			inflight.again = true;
			return;
		}
		const state = { again: false };
		this.indexBuilds.set(workspaceId, state);
		void (async () => {
			const started = performance.now();
			let rows = 0;
			let ready = false;
			try {
				const kv = await this.kv();
				do {
					state.again = false;
					if (this.closed) return;
					const built = await buildIndexes(
						kv,
						(fn) => this.kvMutex.run(fn),
						this.entityRegistry,
						this.indexes,
						workspaceId,
						// Stops the walk between batches when the client closes, so a
						// build over a large store cannot outlive its store.
						() => this.closed,
					);
					rows += built.rows;
					ready = built.ready;
				}
				while (state.again);
				// Only a run that ended with every marker claimed reports: a pass
				// that had nothing to walk yet (no snapshot installed) or was
				// stopped by close() has not made the indexes ready, and the run
				// that follows the bootstrap is the one that will say so.
				if (ready && !this.closed) {
					this.emit({
						type: 'indexing',
						workspaceId,
						rows,
						durationMs: performance.now() - started,
					});
				}
			} catch (err) {
				reportError('secondary index build', err);
			} finally {
				this.indexBuilds.delete(workspaceId);
			}
		})();
	}

	/** Reports one read-path event, on the seam the coordinator reports sync on. */
	private emit(event: UnstampedSyncEvent): void {
		// Gated here so the no-listener default stays free of the stamping the
		// dispatcher would otherwise pay per read.
		if (!this.opts.onSyncEvent && this.syncEventListeners.size === 0) return;
		emitSyncEvent(this.dispatchSyncEvent, event);
	}

	/**
	 * Delivers one stamped event to the option handler and every subscribed
	 * listener, each in isolation — one throwing must not skip the rest, or be
	 * mistaken for a failure of the work that emitted it. Bound, because the
	 * coordinator holds it as its onSyncEvent handler.
	 */
	private readonly dispatchSyncEvent: SyncEventHandler = (event: SyncEvent): void => {
		const handler = this.opts.onSyncEvent;
		if (handler) safeInvoke(handler, event, 'onSyncEvent');
		for (const listener of [ ...this.syncEventListeners ]) {
			safeInvoke(listener, event, 'onSyncEvent listener');
		}
	};

	/**
	 * Subscribes to the observability event stream; returns an unsubscribe
	 * function.
	 *
	 * Reports exactly what the `onSyncEvent` OPTION does — that stays the
	 * single-owner form a client is constructed with, and this is the
	 * multi-listener one, so a debug overlay can attach and detach at any point
	 * without owning (or stealing) the option. Same split as `onStatusChange`
	 * and `onMutationRejected`. Events emitted before a listener subscribes are
	 * not replayed.
	 */
	onSyncEvent(cb: SyncEventHandler): () => void {
		this.syncEventListeners.add(cb);
		return () => {
			this.syncEventListeners.delete(cb);
		};
	}

	/**
	 * The single exit for "this mutation is dead" — server rejection, poison
	 * drop, purge or format upgrade all land here, so every one of them trims the
	 * undo stacks before the app sees it. Callers invoke it only after the local
	 * state has been rolled back or deleted.
	 */
	private reportRejection(info: RejectionInfo): void {
		// The undo stacks must not replay state that builds on the dead mutation.
		// Every stack in the workspace, not just the workspace's own: an open
		// dialog's scope can hold a group built on the very mutation that died.
		// A purge removes the scope first, so its stacks are gone already.
		const managers = this.undoManagersOf(info.workspaceId);
		for (const mgr of managers) mgr.onRejected(info.mutationId);
		// Refresh this tab's intent record from the trimmed stacks. Own-tab
		// only: a peer tab whose stacks reference the rejected mutation never
		// hears about it — reaching it would need a broadcast channel, and its
		// record merely over-protects until then.
		if (managers.length > 0) {
			void this.intentReg?.updateRefs(
				info.workspaceId,
				unionOf(managers.map((m) => m.foldBarrierIds())),
			);
		}
		// Isolated: the rollback has already landed, and the coordinator reports a
		// batch of rejections in a loop — a throwing app handler must not skip the
		// rest of that batch or fail the push cycle that carried them.
		const onRejected = this.opts.onMutationRejected;
		if (onRejected) safeInvoke(onRejected, info, 'onMutationRejected callback');
		for (const listener of [ ...this.rejectionListeners ]) {
			safeInvoke(listener, info, 'onMutationRejected listener');
		}
	}

	/**
	 * Subscribes to the dead-letter surface; returns an unsubscribe function.
	 *
	 * Reports exactly what the `onMutationRejected` OPTION does — that stays the
	 * single-owner form a client is constructed with, and this is the
	 * multi-listener one, so a UI layer can attach and detach with a component's
	 * lifetime without owning (or stealing) the option. Same split as
	 * `onStatusChange`.
	 */
	onMutationRejected(cb: (info: RejectionInfo) => void): () => void {
		this.rejectionListeners.add(cb);
		return () => {
			this.rejectionListeners.delete(cb);
		};
	}

	private ownScopes(): Map<string, Subscription[]> {
		const out = new Map<string, Subscription[]>();
		for (const [ ws, scope ] of this.scopes) out.set(ws, scope.subscriptions);
		return out;
	}

	/**
	 * Answers how much of an entity the local store provably holds, from this
	 * tab's own declared scope and from what the coordinator is still
	 * downloading. Read live rather than snapshotted, so a setSubscriptions() is
	 * reflected by the next read — and so is an entity finishing its first
	 * snapshot. Before start() there is no coordinator and nothing is hydrating:
	 * a client that never syncs holds whatever it holds.
	 */
	private readonly coverage = new CoverageResolver(
		() => this.ownScopes(),
		(workspaceId) => this.coordinator?.hydrationOf(workspaceId),
	);

	/**
	 * Replaces the subscription set of EVERY workspace at runtime. The next sync
	 * cycle carries the new set; the server detects the mismatch against each
	 * cursor and delta-resyncs the difference (rows entering scope arrive, rows
	 * leaving scope drop), falling back to a re-bootstrap when it cannot
	 * reconstruct the old scope. Throws on unknown entities or invalid predicates.
	 */
	setSubscriptions(specs: SubscriptionSpec[]): void {
		this.assertOpen();
		const compiled = this.compileSubscriptions(specs);
		for (const [ ws, scope ] of this.scopes) {
			scope.subscriptions = compiled;
			this.coordinator?.setSubscriptions(ws, compiled);
		}
	}

	/** @internal backs WorkspaceClient.setSubscriptions. */
	setWorkspaceSubscriptions(workspaceId: string, specs: SubscriptionSpec[]): void {
		this.assertOpen();
		const scope = this.scopes.get(workspaceId);
		if (!scope) return;
		scope.subscriptions = this.compileSubscriptions(specs);
		this.coordinator?.setSubscriptions(workspaceId, scope.subscriptions);
	}

	private compileSubscriptions(specs: SubscriptionSpec[]): Subscription[] {
		return specs.map((spec) => {
			const def = this.entityRegistry.bySchema(spec.schema);
			// The server refuses the whole subscription set for one server-only
			// entity, which reaches the app as a sync failure at the next cycle
			// rather than at the call that named it. The option is on the
			// descriptor in hand, so the same set fails here instead — the case
			// that matters is an app deriving its subscriptions from its entity
			// list, where a registered history entity is in the list by design.
			if (def.serverOnly) {
				throw new Error(
					`entity "${def.name}" is server-only and never syncs to a client; `
						+ `remove it from the subscription set and read it with queryServer()`,
				);
			}
			// The predicate and the window ordering go through the same
			// compile → prepare → wire-path pipeline a query does, so a
			// subscription and the equivalent query agree on what they name.
			const prepared = prepareQuery(
				spec.schema,
				compileQuery({ where: spec.where, orderBy: spec.orderBy }),
			);
			// A subscription predicate is evaluated against rows as they change,
			// where a semi-join's membership set has no single moment to be
			// computed at; scope the base entity row-locally and use `follow` to
			// bring the targets along.
			if (hasRel(prepared.where)) {
				throw new Error(
					`subscription predicate for "${def.name}" uses matches(); a subscription scopes `
						+ 'rows by their own fields — follow the reference instead',
				);
			}
			const wire = toWirePaths(spec.schema, prepared);
			return create(SubscriptionSchema, {
				entity: def.name,
				predicate: wire.where ? condToProto(wire.where) : undefined,
				follow: spec.follow ? compileFollow(spec.follow, spec.followDepth) : undefined,
				window: compileWindow(spec, wire.orderBy),
			});
		});
	}

	/** Current sync status snapshot (works before start()). */
	status(): SyncStatus {
		return this.statusTracker.current();
	}

	/** @internal backs WorkspaceClient.status. */
	workspaceStatus(workspaceId: string): WorkspaceStatus {
		return this.statusTracker.current().workspaces[workspaceId]
			?? { workspaceId, pendingMutations: 0 };
	}

	/** Subscribes to sync status changes; returns an unsubscribe function. */
	onStatusChange(cb: (status: SyncStatus) => void): () => void {
		return this.statusTracker.onChange(cb);
	}

	/** @internal backs devtools.ts; see ClientInternals. */
	inspect(): ClientInternals {
		return {
			read: async (fn) => {
				this.assertOpen();
				const kv = await this.kv();
				return this.kvMutex.runShared(async () => {
					const tx = await kv.read();
					try {
						return await fn(tx);
					} finally {
						tx.release();
					}
				});
			},
			coverageOf: (entity, workspaceId) => this.coverage.coverageOf(entity, workspaceId),
			isStreamLeader: () => this.coordinator?.isLeader() ?? false,
			textPeers: () => [ ...this.liveTextPeers ],
			rooms: () => [ ...this.openRooms ],
		};
	}

	/**
	 * The transport, memoized so every caller shares one. The configuration
	 * check is synchronous — a client with neither url nor transport fails out
	 * of start() before it has touched anything — and a REJECTED attempt (the
	 * connect-web import failing) is dropped rather than cached, so the failure
	 * is not baked into the client for good.
	 *
	 * Not validated in the constructor: a client with no transport at all is a
	 * usable local-only store, as long as it is never start()ed.
	 *
	 * useBinaryFormat is passed explicitly because connect-web defaults it to
	 * false — its default suits a hand-inspected API, not a sync engine shipping
	 * base64'd entity bytes by the thousand. See SyncClientOptions.wireFormat.
	 */
	private resolveTransport(): Promise<Transport> {
		if (!this.transportPromise) {
			if (this.opts.transport) {
				this.transportPromise = Promise.resolve(this.opts.transport);
			} else {
				const url = this.opts.url;
				if (!url) throw new Error('SyncClient needs url or transport');
				const useBinaryFormat = this.opts.wireFormat !== 'json';
				const promise = (async () => {
					const { createConnectTransport } = await import('@connectrpc/connect-web');
					return createConnectTransport({ baseUrl: url, useBinaryFormat });
				})();
				this.transportPromise = promise;
				promise.catch(() => {
					if (this.transportPromise === promise) this.transportPromise = undefined;
				});
			}
		}
		return this.transportPromise;
	}

	/**
	 * Verifies the local KV layout version before sync starts. A mismatch
	 * (this build expects a different layout than the store carries) salvages
	 * the pending queues with one best-effort push, then wipes everything
	 * except the clientId and the per-workspace mutation-id counters (whose
	 * pushed-through marks are restamped to match) and stamps the new version —
	 * the missing cursors make the first sync re-bootstrap.
	 * Mutations that could not be pushed are dropped and reported both as a count
	 * on the status surface and one by one through onMutationRejected, under code
	 * "format_upgraded"; in that case the clientId
	 * and counters go too, since the server's watermarks no longer line up
	 * with the local counters and a fresh identity avoids a mutation-id gap.
	 */
	private async ensureFormatVersion(transport: Transport): Promise<void> {
		const kv = await this.kv();

		// Phase 1 — under the cross-tab lock, decide what to do and snapshot the
		// queues. The fast paths (version already matches, or a fresh/pre-versioning
		// store) finish here without ever touching the network. Only a genuine
		// mismatch returns queues to salvage, and the lock is dropped before the
		// push: holding the Web Lock across the network would let a server that
		// accepts the connection then stalls wedge every tab's KV writes.
		const snapshot = await this.kvMutex.run(async () => {
			const tx = await kv.write();
			try {
				const stored = (await tx.get(META_FORMAT_VERSION)) as number | undefined;
				if (stored === FORMAT_VERSION) {
					tx.release();
					// Someone stamped this version, and it may have been another tab
					// whose wipe discarded the identity — nothing distinguishes that
					// store from one we upgraded ourselves. Drop the memo a pre-start
					// mutation may have taken so the next read comes off the store.
					this.clientIdPromise = undefined;
					return null;
				}
				if (stored === undefined) {
					// Fresh (or pre-versioning) store: stamp and carry on.
					await tx.put(META_FORMAT_VERSION, FORMAT_VERSION);
					await tx.commit();
					return null;
				}

				// Queues are per workspace, and the workspace of a queued mutation is
				// only recoverable from its key — the Mutation envelope does not carry
				// one. A store written by a layout that predates the per-workspace
				// queue has keys this parse does not recognize; those mutations count
				// as undecodable and are reported as dropped rather than pushed into
				// the wrong partition.
				const pending = new Map<string, Mutation[]>();
				let undecodable = 0;
				let total = 0;
				for await (const [ k, v ] of tx.scan(PENDING_ROOT)) {
					let workspaceId: string;
					try {
						workspaceId = workspaceOfPendingKey(k);
					} catch {
						undecodable++;
						continue;
					}
					if (!workspaceId) {
						undecodable++;
						continue;
					}
					try {
						const list = pending.get(workspaceId) ?? [];
						list.push(fromBinary(MutationSchema, v as Uint8Array));
						pending.set(workspaceId, list);
						total++;
					} catch {
						undecodable++; // undecodable under this build — part of why we wipe
					}
				}
				tx.release(); // read-only so far; the wipe runs in its own transaction.
				return { pending, undecodable, total };
			} catch (err) {
				tx.release();
				throw err;
			}
		});
		if (!snapshot) return;

		// Phase 2 — best-effort salvage push, OUTSIDE the lock. Chunk to the same
		// per-push cap the coordinator uses: the server rejects an over-limit push
		// outright (counting across workspaces), so a long offline backlog must be
		// split or it would all be rejected and dropped. Each chunk is bounded by a
		// timeout so a half-open connection cannot hang start() forever. Mutation
		// ids are monotonic PER WORKSPACE, so the first failed chunk stops that
		// workspace's salvage — pushing later chunks past the resulting gap would
		// only have the server reject them — while the other workspaces carry on.
		// Mutations that could not be salvaged, kept so each one can be reported
		// individually afterwards rather than as a bare count. The undecodable ones
		// are not among them: nothing decoded them, so there is nothing to report
		// beyond the total.
		const lost: DiscardedMutation[] = [];
		if (snapshot.total > 0) {
			const rpc = createClient(SyncService, transport);
			const cap = Math.max(1, this.opts.maxMutationsPerPush ?? DEFAULT_MAX_MUTATIONS_PER_PUSH);
			for (const [ workspaceId, mutations ] of snapshot.pending) {
				let sent = 0;
				const abandon = () => {
					for (const m of mutations.slice(sent)) {
						lost.push({ workspaceId, mutationId: m.mutationId, mutation: m });
					}
				};
				for (const chunk of chunkBySize(mutations, cap)) {
					try {
						const resp = await rpc.push(
							{ batches: [ { workspaceId, mutations: chunk } ] },
							{ timeoutMs: SALVAGE_PUSH_TIMEOUT_MS },
						);
						// A batch-level refusal (gap, revoked workspace) is not an RPC
						// error any more, so it has to be read off the result or the
						// salvage would count unapplied mutations as sent.
						if (resp.results[0]?.errorCode) {
							abandon();
							break;
						}
						sent += chunk.length;
					} catch {
						abandon();
						break;
					}
				}
			}
		}
		const dropped = snapshot.undecodable + lost.length;

		// Phase 3 — under the lock again, wipe everything but (when the salvage was
		// clean) the identity, and stamp the new version. Another tab may have
		// completed the same upgrade while we were pushing; if the version is
		// already current, leave its result untouched.
		await this.kvMutex.run(async () => {
			const tx = await kv.write();
			try {
				const now = (await tx.get(META_FORMAT_VERSION)) as number | undefined;
				if (now === FORMAT_VERSION) {
					tx.release();
					// Their salvage, not ours, decided whether the identity survived,
					// so our memo of it is no longer evidence of anything.
					this.clientIdPromise = undefined;
					return;
				}

				// The mutation-id counters must survive with the clientId: the
				// server's per-(workspace, client) watermarks do not reset on a local
				// format upgrade, and minting ids below them would get fresh mutations
				// dropped as already-acked. That only holds when the salvage was
				// clean — a dropped mutation leaves a watermark behind its counter,
				// and new ids would gap past it (the server rejects gapped pushes), so
				// in that case start over as a fresh client.
				const keepIdentity = dropped === 0;
				const doomed: string[] = [];
				// Counter value per surviving workspace, to restamp its pushed-through
				// mark below.
				const counters = new Map<string, string>();
				for await (const [ k, v ] of tx.scan('')) {
					if (keepIdentity) {
						if (k === META_CLIENT_ID) continue;
						const ws = workspaceOfMutationCounterKey(k);
						if (ws !== undefined) {
							counters.set(ws, v as string);
							continue;
						}
					}
					doomed.push(k);
				}
				for (const k of doomed) await tx.del(k);
				// A counter without its pushed-through mark is half an invariant:
				// compaction may only renumber ids strictly above the mark, and the
				// wipe empties the queue the coordinator would otherwise re-seed the
				// mark from — leaving it at 0 while the counter still reads 42, so the
				// first burst that folds renumbers from 1 and every mutation up to the
				// watermark is dropped as already applied. Every id the counter has
				// minted is now either acked or salvaged (that is what dropped == 0
				// means), so the server's watermark IS the counter.
				for (const [ ws, id ] of counters) await tx.put(metaPushedThrough(ws), id);
				await tx.put(META_FORMAT_VERSION, FORMAT_VERSION);
				await tx.commit();
				// The stored identity is gone, so the memo of it must go too —
				// a mutation issued before start() has already taken the old id,
				// and keeping it cached would hand the rest of the session the
				// very identity whose watermarks this wipe exists to escape.
				if (!keepIdentity) this.clientIdPromise = undefined;
			} catch (err) {
				tx.release();
				throw err;
			}
		});
		if (dropped > 0) {
			this.statusTracker.update({
				lastError: {
					code: CodeFormatUpgraded,
					message: `local format upgrade dropped ${dropped} unsynced mutation(s)`,
				},
			});
			// The status line carries the count; the dead-letter surface carries the
			// mutations, so an app already handling rejections needs no second code
			// path to learn which edits died here.
			for (const m of lost) {
				this.reportRejection({
					workspaceId: m.workspaceId,
					mutationId: m.mutationId,
					errorCode: CodeFormatUpgraded,
					errorMessage: 'dropped by a local store format upgrade',
					...describeMutation(m.mutation),
				});
			}
		}
	}

	/**
	 * Records this build's entities in the store and reports whether any of them
	 * were new to it — which means their rows are missing locally and only a
	 * snapshot can supply them, so the caller starts the coordinator with a
	 * forced resync. See entities.ts for the whole rule; local-only, so unlike
	 * the format check it never touches the network.
	 */
	private async reconcileEntities(): Promise<boolean> {
		const kv = await this.kv();
		return this.kvMutex.run(async () => {
			const tx = await kv.write();
			try {
				const added = await reconcileStoreEntities(tx, this.entityRegistry);
				await tx.commit();
				return added.length > 0;
			} catch (err) {
				tx.release();
				throw err;
			}
		});
	}

	/** Forces one push+pull cycle across every workspace (mostly for tests). */
	async syncNow(): Promise<void> {
		this.assertOpen();
		if (!this.coordinator) throw new Error('start() first');
		await this.coordinator.syncNow();
	}

	/**
	 * Deletes the account's local data, then closes the client.
	 *
	 * Everything the store holds goes: every workspace's rows, overlay,
	 * bootstrap staging, cursors and queues, and the `clientId` with them.
	 * Watchers are woken before the close, so the UI drops the rows. A client
	 * built on the account afterwards mints a new `clientId` and bootstraps from
	 * empty; to delete one workspace of an account the device keeps, use
	 * `removeWorkspace(id, { purge: true })`.
	 *
	 * Queued mutations are discarded rather than sent, and come back as the
	 * result — grouped by workspace, in queue order within each — which is the
	 * only copy left of them. Each is also reported through `onMutationRejected`
	 * with code `workspace_purged`.
	 *
	 * The client is closed on return and cannot be reopened. A client that was
	 * already closed keeps its data and answers with an empty list, so purge
	 * before closing. Rejects with the store's error if the wipe cannot be
	 * committed, leaving the client open but no longer syncing — call again to
	 * retry the deletion, or `close()`.
	 *
	 * The store's IndexedDB database is left in place, empty. Other tabs sharing
	 * the store see the rows disappear; one whose credential still works reads
	 * the empty store as a fresh install and bootstraps the data back.
	 */
	async purgeLocalData(): Promise<DiscardedMutation[]> {
		if (this.closed) return [];
		// Sync stops before the wipe: a pull or a bootstrap swap committing after
		// it would write the account's rows straight back. Clearing the fields
		// keeps close() below from stopping either one a second time.
		await this.coordinator?.stop();
		this.coordinator = undefined;
		await this.presence?.stop();
		this.presence = undefined;
		// reportRejection refreshes this tab's intent record from the trimmed
		// undo stacks; the registry has to go first, or that refresh writes a
		// record back into the emptied store.
		this.intentReg = undefined;
		const discarded = await this.wipeAccount();
		for (const d of discarded) {
			this.reportRejection({
				workspaceId: d.workspaceId,
				mutationId: d.mutationId,
				errorCode: CodeWorkspacePurged,
				errorMessage: "the account's local data was purged",
				...describeMutation(d.mutation),
			});
		}
		await this.close();
		return discarded;
	}

	/**
	 * Empties the store — every key, the clientId and the mutation-id counters
	 * included — and returns the queues it deleted, read inside the transaction
	 * that deletes them so the result is exactly what was lost. A queue entry
	 * this build cannot decode goes unreported: there is nothing to hand back.
	 *
	 * ensureFormatVersion's wipe keeps the clientId so freshly minted ids stay
	 * above the server's watermarks. This one deletes it, because the counters
	 * go too: a client that kept the identity would mint ids the server has
	 * already acked and have every edit dropped.
	 */
	private async wipeAccount(): Promise<DiscardedMutation[]> {
		const kv = await this.kv();
		const touched = new TouchSet();
		const discarded = await this.kvMutex.run(async () => {
			const tx = await kv.write();
			try {
				const queued: DiscardedMutation[] = [];
				// Every queue the STORE holds, not just this client's workspaces:
				// another tab's unsent work is deleted here too, so it is reported
				// here too. The workspace is only recoverable from the key — the
				// Mutation envelope does not carry one.
				for await (const [ k, v ] of tx.scan(PENDING_ROOT)) {
					let workspaceId: string;
					try {
						workspaceId = workspaceOfPendingKey(k);
					} catch {
						continue;
					}
					if (!workspaceId) continue;
					try {
						const mutation = fromBinary(MutationSchema, v as Uint8Array);
						queued.push({ workspaceId, mutationId: mutation.mutationId, mutation });
					} catch {
						continue;
					}
				}
				// Before the delete: the entity list the watchers are woken for is
				// itself a record this wipe removes.
				for (const entity of await syncableEntities(tx, this.entityRegistry)) {
					for (const workspaceId of this.scopes.keys()) touched.entity(workspaceId, entity);
				}
				const doomed: string[] = [];
				for await (const [ k ] of tx.scan('')) doomed.push(k);
				for (const k of doomed) await tx.del(k);
				await tx.commit();
				return queued;
			} catch (err) {
				tx.release();
				throw err;
			}
		});
		await this.notifyChange(touched);
		return discarded;
	}

	/**
	 * Stops sync, drops watchers and closes the KV store. Idempotent. A
	 * closed client cannot be reopened — create a new SyncClient instead.
	 *
	 * Local data survives for the next client on the account; `purgeLocalData()`
	 * deletes it.
	 */
	async close(): Promise<void> {
		if (this.closed) return;
		this.closed = true;
		await this.coordinator?.stop();
		await this.presence?.stop();
		this.changeShare?.stop();
		this.hubInstance?.close();
		if (this.kvPromise) {
			await (await this.kvPromise).close();
		}
	}

	/**
	 * Stable per-device client id, persisted in the KV store. Account-wide: the
	 * server's watermarks are keyed (workspace, client_id), so one handle covers
	 * every workspace without any two of them sharing a counter.
	 *
	 * Every tab on the device reads the same value, which is what makes it a
	 * device handle — and why it is NOT a text writer identity on its own. Text
	 * peers are leased from it per document instance (see text/peer.ts).
	 *
	 * Memoized by PROMISE: the stored id never changes under a running client,
	 * and every mutation needs it — reading it back per enqueue would put a
	 * second cross-tab lock acquisition and an extra KV write transaction on the
	 * write path, for a value that cannot have moved under a running client. A
	 * failed mint is not cached (minting touches the store and can fail
	 * transiently). The value CAN change across start(), which re-reads it
	 * whenever a local format upgrade — this client's or another tab's — could
	 * have discarded the stored identity.
	 */
	clientId(): Promise<string> {
		if (!this.clientIdPromise) {
			const promise = this.mintClientId();
			this.clientIdPromise = promise;
			promise.catch(() => {
				if (this.clientIdPromise === promise) this.clientIdPromise = undefined;
			});
		}
		return this.clientIdPromise;
	}

	private async mintClientId(): Promise<string> {
		const kv = await this.kv();
		return this.kvMutex.run(async () => {
			const tx = await kv.write();
			try {
				const existing = (await tx.get(META_CLIENT_ID)) as string | undefined;
				if (existing) {
					tx.release();
					return existing;
				}
				const id = crypto.randomUUID();
				await tx.put(META_CLIENT_ID, id);
				await tx.commit();
				return id;
			} catch (err) {
				tx.release();
				throw err;
			}
		});
	}

	watch<D extends DescMessage>(
		schema: D,
		cb: WatchCallback<MessageShape<D>>,
		opts?: WatchOptions,
	): WatchHandle {
		return this.registerWatch(
			this.defaultScope().workspaceId,
			schema,
			cb,
			undefined,
			opts?.include,
			opts?.onError,
			opts?.window,
		);
	}

	/**
	 * Shared registration for watch()/watchQuery(). Registration is deferred
	 * until the hub resolves, but the returned unsubscribe is synchronous — so
	 * any error on the deferred path has no promise to reject and would escape
	 * as an unhandledrejection. Two guards close that gap: the schema is
	 * validated synchronously here (an unregistered entity throws to the caller,
	 * exactly as scan/get/query do), and a store/hub failure on the deferred
	 * path (e.g. kv.open) is surfaced on the status channel callers observe
	 * rather than left to escape globally — and, when the caller asked for it,
	 * on its own `onError`, since an account-wide status field cannot say WHICH
	 * watch is the one that will now never fire.
	 *
	 * `workspaceId` undefined watches every workspace at once.
	 * @internal
	 */
	registerWatch<D extends DescMessage>(
		scope: ReadScope,
		schema: D,
		cb: WatchCallback<MessageShape<D>>,
		compiled?: CompiledQuery,
		include?: Include,
		onError?: (error: unknown) => void,
		initialWindow?: WatchWindow,
	): WatchHandle {
		if (this.closed) return INERT_WATCH;
		this.entityRegistry.bySchema(schema);
		let handle: WatchHandle | undefined;
		let cancelled = false;
		// A window set before the hub resolved is not lost: it becomes the one the
		// watch registers with, so it costs no extra read.
		let window = initialWindow;
		void this.hub().then((hub) => {
			if (cancelled) return;
			handle = hub.watch(schema, scope, cb, compiled, include, window);
		}).catch((err) => {
			this.statusTracker.update({
				lastError: {
					code: CodeWatchFailed,
					message: err instanceof Error ? err.message : String(err),
				},
			});
			// Isolated for the same reason every app callback is: this runs on a
			// promise with nothing to reject into.
			if (!cancelled && onError) safeInvoke(onError, err, 'watch onError callback');
		});
		return {
			close: () => {
				cancelled = true;
				handle?.close();
			},
			setWindow: (next) => {
				window = next;
				handle?.setWindow(next);
			},
		};
	}

	/**
	 * Live grouped read: emits the groups after every commit touching the entity,
	 * suppressing callbacks whose answer is unchanged. The first callback comes
	 * as soon as the store can be read.
	 *
	 * See {@link SyncClient.groupCount} for what a group is.
	 */
	watchGroupCount<D extends DescMessage>(
		schema: D,
		q: GroupQuery<MessageShape<D>>,
		cb: (groups: Group[]) => void,
		opts?: { onError?: (error: unknown) => void },
	): WatchHandle {
		return this.registerGroupWatch(
			this.defaultScope().workspaceId,
			schema,
			q,
			false,
			cb,
			opts?.onError,
		);
	}

	/** {@link SyncClient.distinct}, live. Emits the values after every commit
	 * that changes them. */
	watchDistinct<D extends DescMessage>(
		schema: D,
		q: GroupQuery<MessageShape<D>>,
		cb: (values: (Scalar | undefined)[]) => void,
		opts?: { onError?: (error: unknown) => void },
	): WatchHandle {
		return this.registerGroupWatch(
			this.defaultScope().workspaceId,
			schema,
			q,
			true,
			(groups) => cb(groups.map((g) => g.value)),
			opts?.onError,
		);
	}

	/**
	 * Shared registration for watchGroupCount()/watchDistinct(), under the same
	 * deferred-registration rule as registerWatch: the query is compiled and the
	 * schema validated synchronously, so a bad one throws to the caller rather
	 * than escaping as an unhandled rejection once the hub resolves.
	 * @internal
	 */
	registerGroupWatch<D extends DescMessage>(
		workspaceId: string,
		schema: D,
		q: GroupQuery<MessageShape<D>>,
		distinct: boolean,
		cb: (groups: Group[]) => void,
		onError?: (error: unknown) => void,
	): WatchHandle {
		if (this.closed) return INERT_WATCH;
		this.entityRegistry.bySchema(schema);
		const compiled = prepareGroupQuery(schema, compileGroupQuery(q, distinct));
		let handle: WatchHandle | undefined;
		let cancelled = false;
		void this.hub().then((hub) => {
			if (cancelled) return;
			handle = hub.watchGroups(schema, workspaceId, compiled, cb, onError);
		}).catch((err) => {
			this.statusTracker.update({
				lastError: {
					code: CodeWatchFailed,
					message: err instanceof Error ? err.message : String(err),
				},
			});
			if (!cancelled && onError) safeInvoke(onError, err, 'watch onError callback');
		});
		return {
			close: () => {
				cancelled = true;
				handle?.close();
			},
			setWindow: () => {},
		};
	}

	/**
	 * {@link SyncClient.groupRows}, live: emits the head of each group after
	 * every commit that moves one, suppressing callbacks whose answer is
	 * unchanged.
	 *
	 * A commit re-runs the read rather than splicing the delivered rows, so
	 * nothing here is shared with the row watchers of the same entity and a
	 * callback may keep what it receives.
	 */
	watchGroupRows<D extends DescMessage>(
		schema: D,
		q: GroupRowsQuery<MessageShape<D>>,
		cb: (groups: RowGroup<MessageShape<D>>[]) => void,
		opts?: { onError?: (error: unknown) => void },
	): WatchHandle {
		return this.registerRowGroupWatch(
			this.defaultScope().workspaceId,
			schema,
			q,
			cb,
			opts?.onError,
		);
	}

	/**
	 * watchGroupRows in a named workspace, under registerGroupWatch's
	 * deferred-registration rule.
	 * @internal
	 */
	registerRowGroupWatch<D extends DescMessage>(
		workspaceId: string,
		schema: D,
		q: GroupRowsQuery<MessageShape<D>>,
		cb: (groups: RowGroup<MessageShape<D>>[]) => void,
		onError?: (error: unknown) => void,
	): WatchHandle {
		if (this.closed) return INERT_WATCH;
		this.entityRegistry.bySchema(schema);
		const compiled = prepareGroupRowsQuery(schema, compileGroupRowsQuery(q));
		let handle: WatchHandle | undefined;
		let cancelled = false;
		void this.hub().then((hub) => {
			if (cancelled) return;
			handle = hub.watchRowGroups(schema, workspaceId, compiled, cb, onError);
		}).catch((err) => {
			this.statusTracker.update({
				lastError: {
					code: CodeWatchFailed,
					message: err instanceof Error ? err.message : String(err),
				},
			});
			if (!cancelled && onError) safeInvoke(onError, err, 'watch onError callback');
		});
		return {
			close: () => {
				cancelled = true;
				handle?.close();
			},
			setWindow: () => {},
		};
	}

	watchRows<D extends DescMessage>(
		schema: D,
		keys: Iterable<string>,
		cb: KeyedWatchCallback<MessageShape<D>>,
		opts?: KeyedWatchOptions,
	): KeyedWatchHandle {
		return this.registerKeyedWatch(
			this.defaultScope().workspaceId,
			schema,
			keys,
			cb,
			opts?.include,
			opts?.onError,
		);
	}

	/**
	 * Like watchRows(), but the refs may name rows in any workspace this client
	 * holds — the cross-workspace counterpart of watchQueryAll() for a set that
	 * comes from outside the entity rather than from a predicate.
	 *
	 * Every ref carries its own workspace, so a key present in two workspaces is
	 * two independent refs. One subscription and one batched read serve the whole
	 * set however many workspaces it spans, and a commit re-reads only the
	 * watched refs it named.
	 *
	 * Throws synchronously if `schema` names an entity this client does not hold;
	 * a watch that cannot be established at all is reported on `opts.onError` and
	 * never delivers. Rows are shared with the entity's other cross-workspace
	 * watchers and reused across commits, so a callback must not mutate what it
	 * receives.
	 */
	watchRowsAll<D extends DescMessage>(
		schema: D,
		refs: Iterable<RowRef>,
		cb: ScopedKeyedWatchCallback<MessageShape<D>>,
		opts?: KeyedWatchOptions,
	): ScopedKeyedWatchHandle {
		return this.registerRefWatch(undefined, schema, refs, cb, opts?.include, opts?.onError);
	}

	/**
	 * Shared registration for watchRows() and watchRowsAll(). Deferred and
	 * guarded exactly as registerWatch is; see there for why the schema is
	 * checked synchronously and a hub failure lands on the status channel.
	 * @internal
	 */
	registerRefWatch<D extends DescMessage>(
		workspaceId: string | undefined,
		schema: D,
		refs: Iterable<RowRef>,
		cb: ScopedKeyedWatchCallback<MessageShape<D>>,
		include?: Include,
		onError?: (error: unknown) => void,
	): ScopedKeyedWatchHandle {
		if (this.closed) return INERT_REF_WATCH;
		this.entityRegistry.bySchema(schema);
		let handle: ScopedKeyedWatchHandle | undefined;
		let cancelled = false;
		// Refs set before the hub resolved are not lost: they become the set the
		// watch registers with, so they cost no extra read.
		let current = [ ...refs ];
		void this.hub().then((hub) => {
			if (cancelled) return;
			handle = hub.watchRefs(schema, workspaceId, current, cb, include);
		}).catch((err) => {
			this.statusTracker.update({
				lastError: {
					code: CodeWatchFailed,
					message: err instanceof Error ? err.message : String(err),
				},
			});
			if (!cancelled && onError) safeInvoke(onError, err, 'watch onError callback');
		});
		return {
			close: () => {
				cancelled = true;
				handle?.close();
			},
			setRefs: (next) => {
				current = [ ...next ];
				handle?.setRefs(current);
			},
		};
	}

	/**
	 * Shared registration for the workspace-scoped watchRows(), which names its
	 * rows by bare key and is delivered a keyed map rather than refs.
	 * @internal
	 */
	registerKeyedWatch<D extends DescMessage>(
		workspaceId: string,
		schema: D,
		keys: Iterable<string>,
		cb: KeyedWatchCallback<MessageShape<D>>,
		include?: Include,
		onError?: (error: unknown) => void,
	): KeyedWatchHandle {
		const handle = this.registerRefWatch(
			workspaceId,
			schema,
			[ ...keys ].map((key) => ({ workspaceId, key })),
			(rows, coverage) => {
				const map = new Map<string, Row<MessageShape<D>> | undefined>();
				for (const { key, row } of rows) map.set(key, row);
				cb(map, coverage);
			},
			include,
			onError,
		);
		return {
			close: () => handle.close(),
			setKeys: (next) => {
				handle.setRefs([ ...next ].map((key) => ({ workspaceId, key })));
			},
		};
	}

	scan<D extends DescMessage>(schema: D): Promise<RowSet<MessageShape<D>>> {
		return this.scanIn(this.defaultScope().workspaceId, schema);
	}

	/**
	 * Scans an entity across EVERY workspace this store holds, in
	 * (workspace, key) order. One prefix scan — the entity-first key layout puts
	 * all workspaces' rows of an entity together — so a unified view costs the
	 * same as a per-workspace scan of the same rows. Each row names its
	 * `workspaceId`, which is also what routes a write back through
	 * `client.ws(row.workspaceId)`.
	 */
	scanAll<D extends DescMessage>(schema: D): Promise<RowSet<MessageShape<D>>> {
		return this.scanIn(undefined, schema);
	}

	/** @internal */
	async scanIn<D extends DescMessage>(
		workspaceId: string | undefined,
		schema: D,
	): Promise<RowSet<MessageShape<D>>> {
		this.assertOpen();
		const def = this.entityRegistry.bySchema(schema);
		const kv = await this.kv();
		const rows = await this.kvMutex.runShared(async () => {
			const tx = await kv.read();
			try {
				return await scanDecoded(tx, this.entityRegistry, def, schema, workspaceId);
			} finally {
				tx.release();
			}
		});
		return withCoverage(rows, this.coverage.coverageOf(def.name, workspaceId));
	}

	/**
	 * Runs a typed query against the local merged view of the default workspace.
	 * With `opts.include`, each result row is hydrated: its references resolve to
	 * live rows from the merged store, attached under `row.refs` (orphan-tolerant
	 * — a missing target resolves to undefined/empty). Hydration runs after the
	 * scan, under its own shared reads, so it never nests inside the query's read
	 * tx.
	 */
	query<D extends DescMessage, const Q extends Query<MessageShape<D>>>(
		schema: D,
		q: Q,
		opts?: { include?: Include },
	): Promise<RowSet<MessageShape<D>, TotalOf<Q>>> {
		return this.queryIn(this.defaultScope().workspaceId, schema, q, opts);
	}

	/**
	 * Like query(), but across workspaces: every one the client holds, or the set
	 * `opts.workspaces` names.
	 *
	 * Ordering, `offset`/`limit` and `total` apply to the merged result rather
	 * than per workspace; each row names the workspace it came from, and its
	 * references resolve within that workspace. The result mints no cursors and
	 * the query may carry none, at any workspace count.
	 *
	 * Throws synchronously for a WorkspaceQuery given no `opts.workspaces`, and
	 * rejects for a query carrying `after`/`before`.
	 */
	queryAll<D extends DescMessage, const Q extends QueryAcross<MessageShape<D>>>(
		schema: D,
		q: Q,
		opts?: QueryAllOptions,
	): Promise<RowSet<MessageShape<D>, TotalOf<Q>>> {
		return this.queryIn(readScopeOf(schema, q, opts?.workspaces), schema, q, opts);
	}

	/**
	 * Backs query(), ws(id).query() and queryAll() — a workspace id, a resolved
	 * ReadScope, or nothing for every workspace. A per-workspace predicate rides
	 * on the scope (see readScopeOf), not on `q`, which carries only the half of
	 * the query the merged result shares.
	 * @internal
	 */
	async queryIn<D extends DescMessage, const Q extends QueryAcross<MessageShape<D>>>(
		scope: ReadScope,
		schema: D,
		q: Q,
		opts?: { include?: Include },
	): Promise<RowSet<MessageShape<D>, TotalOf<Q>>> {
		this.assertOpen();
		const def = this.entityRegistry.bySchema(schema);
		const named = typeof scope === 'object' ? scope : undefined;
		const workspaceId = scopedWorkspace(scope);
		// What the scan covers and coverage is reported over: the named prefixes,
		// or the one workspace, or every workspace in the store.
		const scanned = named === undefined ? workspaceId : named.workspaces;
		const compiled = prepareQuery(
			schema,
			compileQuery(q as Query<MessageShape<D>>),
			named?.predicates,
		);
		// A cursor names a position under ONE workspace's ordering, so a read
		// spanning workspaces neither accepts nor mints one; see the tie-break note
		// in query/execute.ts for why the two orders differ. A named set keeps them
		// off even at size one, so that a list shrinking to a single workspace does
		// not silently change what the read returns.
		const cursors = workspaceId === undefined || named !== undefined
			? undefined
			: new OrderCursors(schema, compiled.orderBy ?? []);
		const window = cursors === undefined
			? undefined
			: pageWindowOf(schema, def.name, compiled);
		if (cursors === undefined && (compiled.after !== undefined || compiled.before !== undefined)) {
			throw new Error(
				"query: after/before name a position in one workspace's ordering, which a "
					+ 'cross-workspace query — over every workspace or over a named set of them '
					+ '— has no counterpart for',
			);
		}
		// References the query crosses (`matches`), from the shared predicate and
		// every per-workspace one. Their two-phase rewrite runs inside the read
		// section below, so the membership sets and the rows they are tested
		// against are one consistent view.
		const relNodes = [
			...collectRels(schema, compiled.where),
			...(named?.predicates
				? [ ...named.predicates.values() ].flatMap((where) => collectRels(schema, where))
				: []),
		];
		const kv = await this.kv();
		const report: IndexReport = { candidates: 0 };
		const started = performance.now();
		let entered = started;
		const page = await this.kvMutex.runShared(async () => {
			entered = performance.now();
			const tx = await kv.read();
			try {
				// The effective predicate per workspace: the base one, with each rel
				// node collapsed to that workspace's membership test. Undefined when
				// the query is row-local, which keeps the zero-cost path unchanged.
				const relWhere = relNodes.length === 0 ? undefined : relWhereFor(
					await computeRelKeys(
						tx,
						this.entityRegistry,
						relNodes,
						scanned,
						(entity) => this.indexes.for(entity),
					),
					(ws) => named?.predicates?.get(ws) ?? compiled.where,
				);
				const effective: CompiledQuery = relWhere === undefined
					? compiled
					: workspaceId !== undefined && named === undefined
					? { ...compiled, where: relWhere(workspaceId) }
					: { ...compiled, where: undefined };
				const effNamed = relWhere === undefined || named === undefined ? named : {
					workspaces: named.workspaces,
					predicates: new Map(
						named.workspaces.map((ws): [string, Cond] => [ ws, relWhere(ws)! ]),
					),
				};
				// The definition of the result. A per-workspace predicate is applied
				// before it rather than inside it — the same set, since runQueryPage
				// filters first — leaving one query for the one order it has to be in.
				const scan = async () => {
					const rows = await scanDecoded(tx, this.entityRegistry, def, schema, scanned);
					// A rel query spanning every workspace has a predicate per
					// workspace the store happens to hold, which no NamedWorkspaces
					// can carry; each row is tested under its own workspace's.
					const admitted = relWhere !== undefined && effNamed === undefined
							&& workspaceId === undefined
						? rows.filter((r) => {
							const where = relWhere(r.workspaceId);
							return where === undefined || evalCond(r.value, where, schema);
						})
						: admittedIn(effNamed, rows, schema);
					return runQueryPage(admitted, effective, schema, window);
				};
				// An index narrows which rows are read; it never changes which
				// match, so a decline here is not a fallback in the apologetic
				// sense — the scan above IS the definition, and the index has to
				// reproduce it exactly or decline.
				const found = effNamed === undefined
					? await this.pageByIndex(tx, def, schema, effective, workspaceId, window, report)
					: await this.pageByIndexLegs(tx, def, schema, effective, effNamed, report);
				if (found === undefined) return await scan();
				// Under SYNC_DEBUG_VERIFY_TOUCHES both paths run and must agree.
				// This is the same net the watch hub uses for its maintained
				// results, pointed at the other place a cheap answer stands in
				// for an expensive one: nothing else would notice an index and a
				// scan drifting apart, because both look like perfectly ordinary
				// results.
				if (this.verifyIndexReads) assertSameResult(found, await scan(), def.name);
				return found;
			} finally {
				tx.release();
			}
		});
		this.emit(
			queryEventOf({
				origin: 'query',
				entity: def.name,
				workspaceId,
				rows: page.rows.length,
				total: page.total,
				durationMs: performance.now() - started,
				waitMs: entered - started,
			}, report),
		);
		// Coverage is the BASE entity's — a query answers from what synced, and
		// hydration only decorates the rows it found — plus the relation rule
		// for each reference the predicate crossed.
		const coverage = this.coverage.coverageOf(
			def.name,
			scanned,
			relCoverageRefs(this.entityRegistry, relNodes),
		);
		const info = pageInfoOf(cursors, def.name, page);
		if (!opts?.include) {
			return withCoverage(page.rows, coverage, info) as RowSet<MessageShape<D>, TotalOf<Q>>;
		}
		const hydrated = (await hydrate(
			this.refClient(),
			this.entityRegistry,
			def.name,
			page.rows as Row<Message>[],
			opts.include,
		)) as unknown as HydratedRow<MessageShape<D>>[];
		return withCoverage(hydrated as unknown as Row<MessageShape<D>>[], coverage, info) as RowSet<
			MessageShape<D>,
			TotalOf<Q>
		>;
	}

	/**
	 * Groups an entity's matching rows by one path and counts each group —
	 * a board column's badge, a facet sidebar's numbers.
	 *
	 * Groups come back in ascending value order with the no-value group last;
	 * `q.limit` caps how many. A `by` path through a repeated field groups per
	 * ELEMENT, so the counts do not sum to the number of matching rows. See
	 * {@link GroupQuery}.
	 */
	groupCount<D extends DescMessage>(schema: D, q: GroupQuery<MessageShape<D>>): Promise<Group[]> {
		return this.groupIn(this.defaultScope().workspaceId, schema, q, false);
	}

	/**
	 * The distinct values an entity's matching rows hold at one path, in
	 * ascending order — what a filter dropdown offers, and what a facet sidebar
	 * needs before it can count anything.
	 *
	 * `undefined` is in the result when some matching row holds no value there;
	 * it is a value of the result, not an absence of one.
	 */
	async distinct<D extends DescMessage>(
		schema: D,
		q: GroupQuery<MessageShape<D>>,
	): Promise<(Scalar | undefined)[]> {
		return (await this.groupIn(this.defaultScope().workspaceId, schema, q, true))
			.map((g) => g.value);
	}

	/** @internal */
	async groupIn<D extends DescMessage>(
		workspaceId: string,
		schema: D,
		q: GroupQuery<MessageShape<D>>,
		distinct: boolean,
	): Promise<Group[]> {
		this.assertOpen();
		const def = this.entityRegistry.bySchema(schema);
		const compiled = prepareGroupQuery(schema, compileGroupQuery(q, distinct));
		const kv = await this.kv();
		return this.kvMutex.runShared(async () => {
			const tx = await kv.read();
			try {
				return await readGroups(
					tx,
					this.entityRegistry,
					def,
					schema,
					this.indexes.for(def.name),
					compiled,
					workspaceId,
				);
			} finally {
				tx.release();
			}
		});
	}

	/**
	 * The first rows of each group — a board column's preview, the top item of
	 * every project — instead of one page of the whole match set.
	 *
	 * Groups come back in ascending value order with the no-value group last and
	 * `q.limit` caps how many; inside a group `q.orderBy` decides which rows come
	 * first and `q.perGroup` how many of them. See {@link GroupRowsQuery} for
	 * what a group is and what an index makes of the read.
	 */
	groupRows<D extends DescMessage>(
		schema: D,
		q: GroupRowsQuery<MessageShape<D>>,
	): Promise<RowGroup<MessageShape<D>>[]> {
		return this.groupRowsIn(this.defaultScope().workspaceId, schema, q);
	}

	/** @internal */
	async groupRowsIn<D extends DescMessage>(
		workspaceId: string,
		schema: D,
		q: GroupRowsQuery<MessageShape<D>>,
	): Promise<RowGroup<MessageShape<D>>[]> {
		this.assertOpen();
		const def = this.entityRegistry.bySchema(schema);
		const compiled = prepareGroupRowsQuery(schema, compileGroupRowsQuery(q));
		const kv = await this.kv();
		const groups = await this.kvMutex.runShared(async () => {
			const tx = await kv.read();
			try {
				return await readRowGroups(
					tx,
					this.entityRegistry,
					def,
					schema,
					this.indexes.for(def.name),
					compiled,
					workspaceId,
				);
			} finally {
				tx.release();
			}
		});
		return groups.map((g) => ({ ...g, rows: g.rows.map((entry) => entry.row) }));
	}

	/** {@link SyncClient.groupRows} against the server; see
	 * {@link SyncClient.groupCountServer} for what a server read is and is not. */
	groupRowsServer<D extends DescMessage>(
		schema: D,
		q: GroupRowsQuery<MessageShape<D>>,
	): Promise<RowGroup<MessageShape<D>>[]> {
		return this.groupRowsServerIn(this.defaultScope().workspaceId, schema, q);
	}

	/** @internal */
	async groupRowsServerIn<D extends DescMessage>(
		workspaceId: string,
		schema: D,
		q: GroupRowsQuery<MessageShape<D>>,
	): Promise<RowGroup<MessageShape<D>>[]> {
		this.assertOpen();
		const def = this.entityRegistry.bySchema(schema);
		const compiled = prepareGroupRowsQuery(schema, compileGroupRowsQuery(q));
		const wire = toWirePaths(schema, {
			...(compiled.where ? { where: compiled.where } : {}),
			...(compiled.orderBy ? { orderBy: compiled.orderBy } : {}),
		});
		const rpc = createClient(SyncService, await this.resolveTransport());
		const resp = await rpc.query({
			workspaceId,
			entity: def.name,
			predicate: wire.where ? condToProto(wire.where) : undefined,
			orderBy: (wire.orderBy ?? []).map(orderKeyToProto),
			groupBy: compiled.protoBy,
			aggregate: AggregateOp.ROWS,
			perGroup: compiled.perGroup,
			perGroupTotal: compiled.total !== false,
			limit: compiled.limit ?? 0,
		});
		// The read rode its own RPC, outside the sync cycle, so this is the only
		// place its revision reaches the skew handshake.
		this.coordinator?.observeServerRevision(resp.schemaRevision);
		return resp.rowGroups.map((g) => {
			const rows: Row<MessageShape<D>>[] = [];
			for (const rc of g.rows) {
				if (rc.body.case !== 'value') continue;
				const value = fromBinary(schema, rc.body.value);
				sortOrderedFields(schema, value);
				rows.push({ key: rc.key, value, workspaceId, version: rc.version });
			}
			return {
				value: g.value === undefined ? undefined : scalarFromProto(g.value),
				rows,
				total: compiled.total === false ? undefined : Number(g.total),
				hasMore: g.hasMore,
			};
		});
	}

	/**
	 * Runs a grouped read AGAINST THE SERVER instead of the local store — the
	 * push-down path for counts over rows partial sync did not bring down.
	 *
	 * The answer is a server-state snapshot: it is not merged with the optimistic
	 * overlay and never feeds watchers. Requires connectivity.
	 */
	groupCountServer<D extends DescMessage>(
		schema: D,
		q: GroupQuery<MessageShape<D>>,
	): Promise<Group[]> {
		return this.groupServerIn(this.defaultScope().workspaceId, schema, q, false);
	}

	/** {@link SyncClient.distinct} against the server; see
	 * {@link SyncClient.groupCountServer}. */
	async distinctServer<D extends DescMessage>(
		schema: D,
		q: GroupQuery<MessageShape<D>>,
	): Promise<(Scalar | undefined)[]> {
		return (await this.groupServerIn(this.defaultScope().workspaceId, schema, q, true))
			.map((g) => g.value);
	}

	/** @internal */
	async groupServerIn<D extends DescMessage>(
		workspaceId: string,
		schema: D,
		q: GroupQuery<MessageShape<D>>,
		distinct: boolean,
	): Promise<Group[]> {
		this.assertOpen();
		const def = this.entityRegistry.bySchema(schema);
		const compiled = prepareGroupQuery(schema, compileGroupQuery(q, distinct));
		const wire = toWirePaths(schema, { ...(compiled.where ? { where: compiled.where } : {}) });
		const rpc = createClient(SyncService, await this.resolveTransport());
		const resp = await rpc.query({
			workspaceId,
			entity: def.name,
			predicate: wire.where ? condToProto(wire.where) : undefined,
			groupBy: compiled.protoBy,
			aggregate: distinct ? AggregateOp.DISTINCT : AggregateOp.COUNT,
			limit: compiled.limit ?? 0,
		});
		// The read rode its own RPC, outside the sync cycle, so this is the only
		// place its revision reaches the skew handshake.
		this.coordinator?.observeServerRevision(resp.schemaRevision);
		return resp.groups.map((g) => ({
			value: g.value === undefined ? undefined : scalarFromProto(g.value),
			count: Number(g.count),
		}));
	}

	/** One workspace's — or the whole store's — matching page, from an index. */
	private async pageByIndex<D extends DescMessage>(
		tx: KVRead,
		def: EntityDef,
		schema: D,
		compiled: CompiledQuery,
		workspaceId: string | undefined,
		window: PageWindow | undefined,
		report: IndexReport,
	): Promise<QueryPage<MessageShape<D>> | undefined> {
		const planned = await queryByIndex(
			tx,
			this.entityRegistry,
			def,
			schema,
			this.indexes.for(def.name),
			compiled,
			workspaceId,
			window,
			report,
		);
		return planned && {
			rows: planned.rows.map((entry) => entry.row),
			total: planned.total,
			hasMore: planned.hasMore,
		};
	}

	/**
	 * A named set's matching page, from one index walk per workspace.
	 *
	 * Each leg is a single-workspace query, so none of them meets the decline
	 * that stops a read spanning every workspace; the page is cut from the merged
	 * matches afterwards, which is what makes `limit` the merged result's rather
	 * than each leg's. One leg declining declines the whole read — the scan that
	 * then serves it is the definition, and half a result from an index is not a
	 * result.
	 */
	private async pageByIndexLegs<D extends DescMessage>(
		tx: KVRead,
		def: EntityDef,
		schema: D,
		compiled: CompiledQuery,
		named: NamedWorkspaces,
		report: IndexReport,
	): Promise<QueryPage<MessageShape<D>> | undefined> {
		// A set naming no workspace has no index to consult in; the scan that then
		// serves it reads nothing either way.
		if (named.workspaces.length === 0) {
			report.declined = 'no-index';
			return undefined;
		}
		const matches: Row<MessageShape<D>>[] = [];
		const names = new Set<string>();
		// Walked in key-segment order, so that rows tying on every order key AND
		// on the row key resolve the way the scan's input order resolves them.
		for (const workspaceId of [ ...named.workspaces ].sort(compareWorkspaces)) {
			const leg: IndexReport = { candidates: 0 };
			const found = await queryByIndex(
				tx,
				this.entityRegistry,
				def,
				schema,
				this.indexes.for(def.name),
				{ where: named.predicates?.get(workspaceId) ?? compiled.where, orderBy: compiled.orderBy },
				workspaceId,
				undefined,
				leg,
			);
			report.candidates += leg.candidates;
			if (found === undefined) {
				report.declined = leg.declined;
				return undefined;
			}
			if (leg.index !== undefined) names.add(leg.index);
			for (const entry of found.rows) matches.push(entry.row);
		}
		// One read reports one index. Legs choose independently, so a set whose
		// per-workspace predicates planned differently reports each name it used.
		report.index = [ ...names ].join('+');
		// Ordered, counted and cut here; the predicate is spent, each leg having
		// been walked under its own.
		return runQueryPage(matches, { ...compiled, where: undefined }, schema);
	}

	/**
	 * Resolves a forward reference of a holder value to its live target row(s):
	 * the target row for a to-one ref (undefined when unset or orphaned), or the
	 * resolving rows for a to-many ref (orphans dropped). Backs the generated
	 * `db.<entity>.refs.<field>.load(holder)` accessor.
	 */
	resolveRef<D extends DescMessage>(
		holderSchema: D,
		fieldLocalName: string,
		holder: MessageShape<D>,
	): Promise<Row<Message> | Row<Message>[] | undefined> {
		return this.resolveRefIn(
			this.defaultScope().workspaceId,
			holderSchema,
			fieldLocalName,
			holder,
		);
	}

	/** @internal */
	async resolveRefIn<D extends DescMessage>(
		workspaceId: string,
		holderSchema: D,
		fieldLocalName: string,
		holder: MessageShape<D>,
	): Promise<Row<Message> | Row<Message>[] | undefined> {
		this.assertOpen();
		const def = this.entityRegistry.bySchema(holderSchema);
		return loadForward(
			this.refClient(),
			this.entityRegistry,
			def.name,
			workspaceId,
			fieldLocalName,
			holder as Message,
		);
	}

	/**
	 * Resolves a reverse relation: the rows of the source entity whose forward
	 * reference points at `target`, optionally narrowed by `q`. Backs the
	 * generated `db.<entity>.refs.<reverseRel>(target, q?)` accessor.
	 */
	resolveReverseRef<D extends DescMessage>(
		targetSchema: D,
		relationName: string,
		target: MessageShape<D>,
		q?: Query<Message>,
	): Promise<Row<Message>[]> {
		return this.resolveReverseRefIn(
			this.defaultScope().workspaceId,
			targetSchema,
			relationName,
			target,
			q,
		);
	}

	/** @internal */
	async resolveReverseRefIn<D extends DescMessage>(
		workspaceId: string,
		targetSchema: D,
		relationName: string,
		target: MessageShape<D>,
		q?: Query<Message>,
	): Promise<Row<Message>[]> {
		this.assertOpen();
		const def = this.entityRegistry.bySchema(targetSchema);
		return resolveReverse(
			this.refClient(),
			this.entityRegistry,
			def.name,
			workspaceId,
			relationName,
			target as Message,
			q,
		);
	}

	/**
	 * Runs a typed query AGAINST THE SERVER — the push-down path for data
	 * outside the synced subset. Results are server-state snapshots: they are
	 * not merged with the optimistic overlay, not stored locally, and never
	 * feed watchers. Works before start() (the transport resolves lazily);
	 * requires connectivity. Ordering matches the local evaluator exactly.
	 *
	 * Single-workspace by construction: a query is one predicate against one
	 * partition, so there is no cross-workspace form of it on the wire.
	 */
	queryServer<D extends DescMessage, const Q extends Query<MessageShape<D>>>(
		schema: D,
		q: Q,
	): Promise<RowSet<MessageShape<D>, TotalOf<Q>>> {
		return this.queryServerIn(this.defaultScope().workspaceId, schema, q);
	}

	/** @internal */
	async queryServerIn<D extends DescMessage, const Q extends Query<MessageShape<D>>>(
		workspaceId: string,
		schema: D,
		q: Q,
	): Promise<RowSet<MessageShape<D>, TotalOf<Q>>> {
		this.assertOpen();
		const def = this.entityRegistry.bySchema(schema);
		const compiled = prepareQuery(schema, compileQuery(q));
		const wire = toWirePaths(schema, compiled);
		const rpc = createClient(SyncService, await this.resolveTransport());
		const resp = await rpc.query({
			workspaceId,
			entity: def.name,
			predicate: wire.where ? condToProto(wire.where) : undefined,
			orderBy: (wire.orderBy ?? []).map(orderKeyToProto),
			limit: compiled.limit ?? 0,
			offset: compiled.offset ?? 0,
			// The wire field is bytes and the token a base64url string, so the
			// two forms meet here and nowhere else.
			after: compiled.after ? cursorToBytes(compiled.after) : new Uint8Array(0),
			before: compiled.before ? cursorToBytes(compiled.before) : new Uint8Array(0),
		});
		// The query rode its own RPC, outside the sync cycle, so this is the only
		// place its revision reaches the skew handshake.
		this.coordinator?.observeServerRevision(resp.schemaRevision);
		const rows: Row<MessageShape<D>>[] = [];
		for (const rc of resp.rows) {
			if (rc.body.case === 'value') {
				const value = fromBinary(schema, rc.body.value);
				sortOrderedFields(schema, value);
				rows.push({ key: rc.key, value, workspaceId, version: rc.version });
			}
		}
		// A server query sees the whole entity, so the only thing that can make
		// it partial is its own limit truncating the result. has_more says so
		// exactly; the full-page test is the fallback for a server that predates
		// the field, and it errs toward claiming LESS completeness — an exact-
		// length final page reads as truncated, which is the safe direction.
		const requested = compiled.limit ?? 0;
		const bounded = resp.hasMore || (requested > 0 && rows.length >= requested);
		return withCoverage(
			rows,
			bounded
				? { complete: false, bounds: [ 'window' ], limit: requested > 0 ? requested : rows.length }
				: COMPLETE,
			{
				hasMore: resp.hasMore,
				// The wire carries no match count, so the default total is the page
				// itself; an opt-out reports undefined like the local paths do.
				...(compiled.total === false ? { total: 'uncounted' as const } : {}),
				...(resp.startCursor.length > 0
					? { startCursor: cursorFromBytes(resp.startCursor) }
					: {}),
				...(resp.endCursor.length > 0 ? { endCursor: cursorFromBytes(resp.endCursor) } : {}),
			},
		) as RowSet<MessageShape<D>, TotalOf<Q>>;
	}

	/**
	 * Live query: emits the query result after every commit that touches the
	 * entity, suppressing callbacks when the result is unchanged.
	 */
	watchQuery<D extends DescMessage, const Q extends Query<MessageShape<D>>>(
		schema: D,
		q: Q,
		cb: WatchCallback<MessageShape<D>, TotalOf<Q>>,
		opts?: WatchOptions,
	): WatchHandle {
		if (this.closed) return INERT_WATCH;
		return this.registerWatch(
			this.defaultScope().workspaceId,
			schema,
			cb as WatchCallback<MessageShape<D>>,
			prepareQuery(schema, compileQuery(q)),
			opts?.include,
			opts?.onError,
			opts?.window,
		);
	}

	/**
	 * Like watchQuery(), but live across workspaces: every one the client holds,
	 * or the set `opts.workspaces` names.
	 *
	 * Re-emits after any commit touching the entity in a workspace the watch
	 * spans, and no others. Ordering, the window and `total` apply to the merged
	 * set rather than per workspace; each row names the workspace it came from.
	 * Emitted pages carry no cursors, at any workspace count.
	 *
	 * Throws synchronously for a WorkspaceQuery given no `opts.workspaces`. A
	 * query carrying `after`/`before` is a watch that cannot be established and
	 * so reaches `opts.onError` instead, like any other.
	 */
	watchQueryAll<D extends DescMessage, const Q extends QueryAcross<MessageShape<D>>>(
		schema: D,
		q: Q,
		cb: WatchCallback<MessageShape<D>, TotalOf<Q>>,
		opts?: WatchQueryAllOptions,
	): WatchHandle {
		if (this.closed) return INERT_WATCH;
		const scope = readScopeOf(schema, q as Query<MessageShape<D>>, opts?.workspaces);
		return this.registerWatch(
			scope,
			schema,
			cb as WatchCallback<MessageShape<D>>,
			prepareQuery(
				schema,
				compileQuery(q as Query<MessageShape<D>>),
				typeof scope === 'object' ? scope.predicates : undefined,
			),
			opts?.include,
			opts?.onError,
			opts?.window,
		);
	}

	get<D extends DescMessage>(schema: D, key: string): Promise<MessageShape<D> | undefined> {
		return this.getIn(this.defaultScope().workspaceId, schema, key);
	}

	/** @internal */
	async getIn<D extends DescMessage>(
		workspaceId: string,
		schema: D,
		key: string,
	): Promise<MessageShape<D> | undefined> {
		this.assertOpen();
		const def = this.entityRegistry.bySchema(schema);
		const kv = await this.kv();
		return this.kvMutex.runShared(async () => {
			const tx = await kv.read();
			try {
				const bytes = await readMergedRow(tx, this.entityRegistry, def.name, workspaceId, key);
				if (bytes === undefined) return undefined;
				const value = fromBinary(schema, bytes);
				sortOrderedFields(schema, value);
				return value;
			} finally {
				tx.release();
			}
		});
	}

	/**
	 * Like get(), but returns the row's key/value together with its confirmed
	 * base version (see Row.version) — the value to pass as `expectedVersion`
	 * for a compare-and-swap mutate. Returns undefined for an absent or
	 * locally-deleted row.
	 */
	getRow<D extends DescMessage>(
		schema: D,
		key: string,
	): Promise<Row<MessageShape<D>> | undefined> {
		return this.getRowIn(this.defaultScope().workspaceId, schema, key);
	}

	/** @internal */
	async getRowIn<D extends DescMessage>(
		workspaceId: string,
		schema: D,
		key: string,
	): Promise<Row<MessageShape<D>> | undefined> {
		this.assertOpen();
		const def = this.entityRegistry.bySchema(schema);
		const kv = await this.kv();
		return this.kvMutex.runShared(async () => {
			const tx = await kv.read();
			try {
				const row = await readMergedRowWithVersion(
					tx,
					this.entityRegistry,
					def.name,
					workspaceId,
					key,
				);
				if (row === undefined) return undefined;
				const value = fromBinary(schema, row.v);
				sortOrderedFields(schema, value);
				return { key, value, workspaceId, version: row.version };
			} finally {
				tx.release();
			}
		});
	}

	/** Number of locally queued, not-yet-acked mutations in the default workspace. */
	pendingCount(): Promise<number> {
		return this.pendingCountIn(this.defaultScope().workspaceId);
	}

	/** @internal */
	async pendingCountIn(workspaceId: string): Promise<number> {
		this.assertOpen();
		const kv = await this.kv();
		return this.kvMutex.runShared(async () => {
			const tx = await kv.read();
			try {
				return (await readPending(tx, workspaceId)).length;
			} finally {
				tx.release();
			}
		});
	}

	/**
	 * Writes the pending-queue entry and the optimistic overlay in one KV
	 * transaction (crash-safe), then nudges the push loop. Both are scoped to
	 * `workspaceId`: the queue, the mutation-id counter and the overlay all
	 * partition by workspace. A batch payload applies its sub-mutations in order
	 * so the optimistic overlay matches the server's sequential, single-savepoint
	 * apply. With undo enabled, each sub-mutation's inverse is captured (against
	 * the overlay as it was before that sub-mutation applied) and recorded as one
	 * undo group once the commit succeeds — so undoing a batch replays atomically
	 * and a rejection (which shares the batch's mutation_id) drops the whole
	 * group.
	 *
	 * The caller passes the SCOPE it wrote through, not just a workspace id.
	 * Every scope of a workspace shares this one queue, counter and overlay;
	 * they differ only in which undo stack the inverses land on, and only the
	 * scope knows which one that is (see WorkspaceScope).
	 * @internal
	 */
	async enqueue(
		scope: WorkspaceScope<E>,
		payload: Mutation['payload'],
		captureStack?: StackName,
		undoOpts?: {
			/**
			 * Per-crud inverse overrides, aligned with the payload's CRUD list.
			 * Set for text-field edits, whose snapshot inverse would merge to a
			 * no-op — the override carries the symbolic op-level inverse instead.
			 */
			overrides?: (Inverse | undefined)[];
			/** Join the previous undo group (typing coalescence). */
			coalesce?: boolean;
		},
	): Promise<void> {
		this.assertOpen();
		const workspaceId = scope.workspaceId;
		// Checked against the live scope map rather than trusted: a scope (or a
		// fork of it) outlives removeWorkspace, and writing through one after the
		// workspace is gone would resurrect its keys behind the coordinator's back.
		if (this.scopes.get(workspaceId) !== scope.root) {
			throw new Error(`workspace "${workspaceId}" is not held by this client`);
		}
		const undoMgr = scope.undoMgr;
		const kv = await this.kv();
		const clientId = await this.clientId();
		const touched = new TouchSet();
		let undoBarrier = false;
		let capturedId: bigint | undefined;
		const capturedInverses: (Inverse | undefined)[] = [];
		const payloadCruds = payload.case === 'crud'
			? [ payload.value ]
			: payload.case === 'batch'
			? payload.value.mutations
			: undefined;
		// String-valued text fields convert to document bytes inside the KV
		// transaction below. The writer peer must be leased BEFORE the
		// transaction opens: a Web-Lock await inside an IndexedDB transaction
		// would let it auto-commit under us. One lease covers the whole
		// payload — conversion is serialized here, so one identity is sound.
		const crudTexts = payloadCruds?.map((crud) => takeTextStrings(crud));
		let peerLease: PeerLease | undefined;
		// A plain-string value already in a crud's value (a restore from field
		// history) converts the same way and needs the same lease, so it has to
		// be spotted out here too rather than where it is resolved.
		const needsPeer = crudTexts?.some((t) => t !== undefined)
			|| payloadCruds?.some((crud) => {
				const def = this.entityRegistry.get(crud.entity);
				return def !== undefined
					&& crudHasPlainString(def, crud, this.entityRegistry.textPathsOf(def.name));
			});
		if (needsPeer) {
			peerLease = await (await this.textPeerLeases()).acquire();
		}
		try {
			await this.kvMutex.run(async () => {
				const tx = await kv.write();
				try {
					const lastRaw = (await tx.get(metaLastMutationId(workspaceId))) as string | undefined;
					const mutationId = (lastRaw ? BigInt(lastRaw) : 0n) + 1n;
					const mutation = create(MutationSchema, { clientId, mutationId });
					mutation.payload = payload;
					const cruds = payloadCruds;
					if (cruds) {
						// Capture-then-apply each sub-mutation in turn: a later inverse (and
						// a later expansion's `prev`) must see the overlay left by earlier
						// sub-mutations of the same batch.
						if (undoMgr) capturedId = mutationId;
						let crudIndex = -1;
						for (const crud of cruds) {
							crudIndex++;
							const def = this.entityRegistry.get(crud.entity);
							// Read on first use, not up front. Every branch below can decline
							// to look at the prior row, and for a collaborative-text keystroke
							// all of them do — reading it would mean folding the row's text log
							// back into a whole document (read.ts), which is the O(document)
							// cost this path exists to stop paying per edit.
							let priorRead = false;
							let priorBytes: Uint8Array | undefined;
							const priorRow = async (): Promise<Uint8Array | undefined> => {
								if (!priorRead) {
									priorRead = true;
									priorBytes = def === undefined ? undefined : await readMergedRow(
										tx,
										this.entityRegistry,
										def.name,
										workspaceId,
										crud.key,
									);
								}
								return priorBytes;
							};
							// Convert text strings — the caller's pending ones, and any
							// plain-string value the crud already carries (a restore from
							// field history) — into full-document bytes, diffed against
							// the merged row under the leased peer. Mutates crud.value
							// (and, for PATCH, the mask) in place; mutation.payload shares
							// the crud, so what persists and pushes is the converted,
							// full-state form.
							const texts = crudTexts?.[crudIndex];
							let textEdits: TextStringEdits | undefined;
							if (def) {
								textEdits = await applyTextStringsToCrud(
									def,
									crud,
									priorRow,
									texts,
									peerLease?.peer,
									this.entityRegistry.textPathsOf(def.name),
								);
							}
							// Expand bare collection masks into per-item paths so concurrent
							// adds/edits merge instead of whole-replacing. Mutates the crud's
							// mask in place; mutation.payload shares the crud, so the
							// persisted+pushed mask is the expanded one. Skipped when the prior
							// row is absent/deleted (a PATCH there is a delete-wins no-op), and
							// when the mask names no collection at all — the schema alone
							// settles that, so a text edit pays neither decode.
							if (
								def && crud.op === CrudOp.PATCH && crud.fieldMask
								&& maskNeedsExpansion(def.schema, crud.fieldMask.paths)
								&& (await priorRow()) !== undefined
							) {
								const base = fromBinary(def.schema, priorBytes!);
								const next = fromBinary(def.schema, crud.value);
								crud.fieldMask.paths = expandCollectionMask(
									def.schema,
									base,
									next,
									crud.fieldMask.paths,
								);
							}
							// Reuse the one `prev`: invert against it (undo) and apply locally.
							// A caller-provided override (symbolic text inverse) replaces the
							// snapshot inverse for its crud.
							if (undoMgr) {
								const override = undoOpts?.overrides?.[crudIndex]
									?? this.textStringInverse(crud, textEdits);
								if (override !== undefined) {
									capturedInverses.push(override);
								} else {
									const inv = invertCrud(crud, await priorRow());
									capturedInverses.push(inv && { kind: 'crud', crud: inv });
								}
							}
							// Hand over the prior row only if something above actually read
							// it: nothing since has written the row, so it is still current,
							// and the apply reuses it rather than reading and decoding a
							// second time. Left unread, the apply decides for itself — its
							// text-log fast path never needs it.
							const outcome = await applyCrudLocally(
								tx,
								this.entityRegistry,
								workspaceId,
								crud,
								touched,
								priorRead ? { bytes: priorBytes } : undefined,
								{ mutationId, index: crudIndex },
							);
							if (outcome === 'no-target') warnPatchWithoutTarget(crud, workspaceId);
						}
						// Persist AFTER the loop so the queued+pushed mask is the expanded one.
						await enqueuePending(tx, workspaceId, mutation);
					} else {
						// Custom mutation: an undo barrier, applied in one shot.
						undoBarrier = true;
						await enqueuePending(tx, workspaceId, mutation);
						await applyMutationLocally(
							tx,
							this.entityRegistry,
							this.mirrors,
							workspaceId,
							mutation,
							touched,
						);
					}
					if (this.intentReg && this.opts.undo) {
						// Persist the PREDICTED post-commit fold-barrier set in this same
						// transaction: recordGroup below only runs after the commit, so
						// the intent record is computed from the inputs it will receive
						// (see UndoManager.predictedBarrierIds). Keeps a peer tab's
						// compaction from ever folding a mutation these stacks pin.
						//
						// The record covers the whole workspace, so the writing scope's
						// prediction is unioned with what every OTHER live scope already
						// pins — a mutation an open dialog's stack depends on has to
						// survive compaction just as much as one the workspace's does.
						const refs = undoBarrier
							// A barrier clears every stack in the workspace, so nothing is
							// pinned afterwards, not even a sibling scope's entries.
							? new Set<bigint>()
							: unionOf(
								scope.undoManagers().map((m) =>
									m === undoMgr
										? m.predictedBarrierIds(
											mutationId,
											capturedInverses.some((i) => i !== undefined && i.kind !== 'text'),
											captureStack,
											false,
										)
										: m.foldBarrierIds()
								),
							);
						await this.intentReg.writeRefs(tx, workspaceId, refs);
					}
					await tx.put(metaLastMutationId(workspaceId), mutationId.toString());
					await tx.commit();
				} catch (err) {
					tx.release();
					throw err;
				}
			});
		} finally {
			peerLease?.release();
		}
		if (undoBarrier) {
			// Workspace-wide: a custom mutation is opaque and may have touched
			// anything, so no stack in this workspace can claim to invert what came
			// before it — not the one that issued it, and not a dialog's.
			for (const mgr of scope.undoManagers()) mgr.barrier();
		} else if (undoMgr && capturedId !== undefined) {
			undoMgr.recordGroup(capturedId, capturedInverses, captureStack, undoOpts?.coalesce);
		}
		this.statusTracker.incrementPending(workspaceId);
		await this.notifyChange(touched);
		this.coordinator?.syncAfterWrite();
	}

	/**
	 * Synthesizes a symbolic undo inverse for a text-string PATCH: enqueue
	 * converted the string itself, so no caller-side override exists, but a
	 * snapshot inverse would merge the old document back in — a silent no-op
	 * for a state CRDT. Only the expressible case is covered: a PATCH whose
	 * mask names exactly one text field — by its own path, or by the instance
	 * path of a document inside a keyed list or map element — which is what
	 * `db.x.mutate.patch(key, { body: '...' })` (and its element-borne
	 * equivalent) produces. Anything wider (a PUT, or a mixed mask) keeps the
	 * snapshot inverse: non-text fields still restore, and the text field's
	 * restore degrades to a no-op union.
	 */
	private textStringInverse(
		crud: CrudMutation,
		edits: TextStringEdits | undefined,
	): Inverse | undefined {
		if (edits === undefined || edits.size !== 1 || crud.op !== CrudOp.PATCH) return undefined;
		const [ path, result ] = [ ...edits.entries() ][0]!;
		const def = this.entityRegistry.get(crud.entity);
		if (!def || textSchemaPathOf(def.schema, path) === undefined) return undefined;
		const maskPaths = crud.fieldMask?.paths ?? [];
		if (maskPaths.length !== 1 || maskPaths[0] !== path) return undefined;
		const ops: TextInverseOp[] = [];
		for (const range of result.inserted) {
			if (range.count > 0) {
				ops.push({ type: 'deleteItems', peer: range.peer, start: range.start, count: range.count });
			}
		}
		for (const seg of result.deleted) ops.push(reinsertOp(seg));
		return { kind: 'text', entity: crud.entity, key: crud.key, field: path, ops };
	}

	/**
	 * Runs fn as an atomic transaction in the default workspace. CRUD writes
	 * issued through the handle `tx` (`tx.mutate.put`, `tx.db.x.mutate.put`) are
	 * collected and applied as ONE all-or-nothing BatchMutation — one optimistic
	 * overlay update, one queue entry, one push, one server savepoint, one
	 * ack-or-rejection, and (with undo enabled) one undo step. The sub-mutations
	 * apply in order, so a later one observes earlier ones; but the batch is not
	 * visible to get/scan/watch until fn returns, and if fn throws the whole batch
	 * is discarded (nothing applies).
	 *
	 * Scoping is by handle, not by time: writes issued through the client
	 * itself (`client.mutate.*`) while fn is suspended on an await are NOT part
	 * of the batch — they queue independently. Custom mutations cannot batch and
	 * are not on the handle; issue them through the client. Because each call's
	 * buffer is local, a `transact()` nested inside another's fn simply commits
	 * its own independent batch.
	 *
	 * A batch is one savepoint in one workspace's sequence, so it cannot span
	 * workspaces; use `client.ws(id).transact(...)` for another one.
	 */
	transact(fn: (tx: Transaction<E>) => Promise<void>): Promise<void> {
		return this.transactIn(this.defaultScope(), fn);
	}

	/** @internal */
	async transactIn(
		scope: WorkspaceScope<E>,
		fn: (tx: Transaction<E>) => Promise<void>,
	): Promise<void> {
		this.assertOpen();
		// The buffer lives in this call's closure, not on the client: only writes
		// issued through the handle land here, so a concurrent client.mutate.*
		// running during one of fn's awaits is not swept in.
		const buffer: CrudMutation[] = [];
		const tx: Transaction<E> = {
			workspaceId: scope.workspaceId,
			get: (schema, key) => scope.get(schema, key),
			getRow: (schema, key) => scope.getRow(schema, key),
			scan: (schema) => scope.scan(schema),
			query: (schema, q, opts) => scope.query(schema, q, opts),
			groupCount: (schema, q) => scope.groupCount(schema, q),
			distinct: (schema, q) => scope.distinct(schema, q),
			groupRows: (schema, q) => scope.groupRows(schema, q),
			watch: (schema, cb) => scope.watch(schema, cb),
			watchQuery: (schema, q, cb, opts) => scope.watchQuery(schema, q, cb, opts),
			resolveRef: (schema, field, holder) => scope.resolveRef(schema, field, holder),
			resolveReverseRef: (schema, rel, target, q) => scope.resolveReverseRef(schema, rel, target, q),
			// Reads go to the scope, so `update` diffs against the pre-batch row —
			// the same state every other read on the handle sees.
			mutate: this.crudMutateApi((crud) => {
				buffer.push(crud);
			}, (schema, key) => scope.get(schema, key)),
			db: undefined as unknown as EntityApis<E>,
		};
		tx.db = buildEntityApis(this.opts.entities, tx, this.entityRegistry);
		await fn(tx);
		// fn threw → we never reach here (the buffer is dropped, nothing applied).
		if (buffer.length === 0) return;
		if (buffer.length === 1) {
			await this.enqueue(scope, { case: 'crud', value: buffer[0]! });
		} else {
			await this.enqueue(scope, { case: 'batch', value: buildBatch(buffer) });
		}
	}

	/**
	 * Re-applies the inverse of the default workspace's most recent undo group as
	 * real mutations. Returns false when there is nothing to undo. A
	 * multi-mutation group (a transact() batch) replays atomically as one batch.
	 * Concurrent remote edits to fields named by the inverse get clobbered —
	 * accepted per-field LWW.
	 */
	undo(): Promise<boolean> {
		return this.undoIn(this.defaultScope());
	}

	/** @internal */
	async undoIn(scope: WorkspaceScope<E>): Promise<boolean> {
		this.assertOpen();
		// A closed undo scope has nothing to replay rather than being an error:
		// a keybinding can fire while the dialog that owned it is unmounting.
		if (scope.closed) return false;
		const mgr = requireUndo(scope);
		const group = mgr.takeUndoGroup();
		if (group.length === 0) return false;
		// The group is claimed (popped) up front so two racing undo() calls take
		// distinct groups; if the replay never commits we put it back rather than
		// lose the step. Routing to 'redo' is passed into this one enqueue, not set
		// on the manager, so a user mutation committing during the await below is
		// recorded as a normal edit instead of being misrouted onto the redo stack.
		try {
			await this.enqueueInverses(scope, group.map((entry) => entry.inverse), 'redo');
		} catch (err) {
			mgr.restoreUndoGroup(group);
			throw err;
		}
		return true;
	}

	/** Inverse of undo(). Returns false when there is nothing to redo. */
	redo(): Promise<boolean> {
		return this.redoIn(this.defaultScope());
	}

	/** @internal */
	async redoIn(scope: WorkspaceScope<E>): Promise<boolean> {
		this.assertOpen();
		if (scope.closed) return false;
		const mgr = requireUndo(scope);
		const group = mgr.takeRedoGroup();
		if (group.length === 0) return false;
		try {
			await this.enqueueInverses(scope, group.map((entry) => entry.inverse), 'undo');
		} catch (err) {
			mgr.restoreRedoGroup(group);
			throw err;
		}
		return true;
	}

	/**
	 * Enqueues a taken undo/redo group: a lone inverse as a CRUD, several as a
	 * batch. CRUD inverses replay verbatim; symbolic text inverses
	 * materialise here against the row's CURRENT document — deleting exactly
	 * the recorded item ids, re-inserting recorded content at its anchors,
	 * restoring prior mark values — so only this client's edits revert and a
	 * peer's concurrent work survives. Each materialised patch carries its
	 * own symbolic inverse (captured during the apply) for the opposite stack.
	 */
	private async enqueueInverses(
		scope: WorkspaceScope<E>,
		inverses: Inverse[],
		captureStack: StackName,
	): Promise<void> {
		// Re-inserting deleted content and restoring marks mint new items, so a
		// replay carrying text needs a writer identity of its own — held until the
		// replay's mutation is in the store, so the slot cannot be recycled onto a
		// document that has not seen these items yet.
		const lease = inverses.some((i) => i.kind === 'text')
			? await (await this.textPeerLeases()).acquire()
			: undefined;
		try {
			await this.replayInverses(scope, inverses, captureStack, lease?.peer ?? '');
		} finally {
			lease?.release();
		}
	}

	private async replayInverses(
		scope: WorkspaceScope<E>,
		inverses: Inverse[],
		captureStack: StackName,
		peer: string,
	): Promise<void> {
		const cruds: CrudMutation[] = [];
		const overrides: (Inverse | undefined)[] = [];
		// One document per (entity, key, field) for the whole group. A coalesced
		// group holds several inverses over the same field (a run of backspaces is
		// one undo step), and each has to continue from the document the previous
		// one produced — re-decoding the unchanged row for each would allocate the
		// same counters twice for different content.
		const docs = new Map<string, TextDoc>();
		for (const inverse of inverses) {
			if (inverse.kind === 'crud') {
				cruds.push(inverse.crud);
				overrides.push(undefined);
				continue;
			}
			const materialised = await this.materialiseTextInverse(scope, inverse, docs, peer);
			if (materialised) {
				cruds.push(materialised.crud);
				overrides.push(materialised.opposite);
			}
		}
		if (cruds.length === 0) {
			// Every entry evaporated (e.g. the text rows were deleted since);
			// the step is consumed with nothing to replay.
			return;
		}
		if (cruds.length === 1) {
			await this.enqueue(scope, { case: 'crud', value: cruds[0]! }, captureStack, {
				overrides,
			});
		} else {
			await this.enqueue(
				scope,
				{ case: 'batch', value: buildBatch(cruds) },
				captureStack,
				{ overrides },
			);
		}
	}

	/**
	 * Turns a symbolic text inverse into a concrete PATCH against the row's
	 * current document, returning the patch plus the inverse of the inverse
	 * (what the opposite stack records). Returns undefined when the row (or
	 * the field/entity) no longer exists — delete-wins, nothing to undo.
	 * `docs` carries the group's in-progress documents and `peer` its leased
	 * writer identity (see enqueueInverses).
	 */
	private async materialiseTextInverse(
		scope: WorkspaceScope<E>,
		inverse: Extract<Inverse, { kind: 'text' }>,
		docs: Map<string, TextDoc>,
		peer: string,
	): Promise<{ crud: CrudMutation; opposite: Inverse } | undefined> {
		const def = this.entityRegistry.get(inverse.entity);
		if (!def) return undefined;
		const schemaPath = textSchemaPathOf(def.schema, inverse.field);
		if (schemaPath === undefined) return undefined;
		const cacheKey = `${inverse.entity}\u0000${inverse.key}\u0000${inverse.field}`;
		let doc = docs.get(cacheKey);
		if (doc === undefined) {
			const row = await this.getIn(scope.workspaceId, def.schema, inverse.key);
			if (row === undefined) return undefined;
			// An element that no longer exists is delete-wins for the edits made
			// inside it — nothing to undo. A fixed field reports its own path
			// whether or not it is set, so an empty field still restores.
			if (!textInstancePaths(def.schema, schemaPath, row).includes(inverse.field)) {
				return undefined;
			}
			doc = decodeTextDoc(textFieldBytes(def.schema, inverse.field, row), peer);
			docs.set(cacheKey, doc);
		}
		const oppositeOps = applyTextInverse(doc, inverse.ops);
		const crud = buildPatch(
			def,
			def.schema,
			inverse.key,
			textFieldInit(def.schema, inverse.field, encodeTextDoc(doc)) as MessageInitShape<
				typeof def.schema
			>,
			[ inverse.field ],
		);
		return {
			crud,
			opposite: {
				kind: 'text',
				entity: inverse.entity,
				key: inverse.key,
				field: inverse.field,
				ops: oppositeOps,
			},
		};
	}

	/**
	 * Opens a nested undo scope on the default workspace (see
	 * WorkspaceClient.undoScope).
	 */
	undoScope(opts?: UndoScopeOptions): UndoScope<E> {
		return this.defaultScope().undoScope(opts);
	}

	canUndo(): boolean {
		return this.defaultScope().canUndo();
	}

	canRedo(): boolean {
		return this.defaultScope().canRedo();
	}

	/**
	 * Subscribes to the default workspace's undo/redo availability changes; the
	 * callback fires after any mutation, undo, redo, rejection or compaction that
	 * may have changed what canUndo()/canRedo() return. Returns an unsubscribe
	 * function. A no-op (returns an inert unsubscribe) when undo is not enabled.
	 */
	onUndoChange(cb: () => void): () => void {
		return this.defaultScope().onUndoChange(cb);
	}
}

function requireUndo<E extends readonly DescMessage[]>(scope: WorkspaceScope<E>): UndoManager {
	const mgr = scope.undoMgr;
	if (!mgr) {
		throw new Error('undo is not enabled: pass the `undo` option to SyncClient');
	}
	return mgr;
}

/**
 * Reports a PATCH the app just issued against a row that is not there.
 *
 * Warned about here, on the app's own write, rather than inside
 * applyCrudLocally: a replay during rebase reaches the same outcome whenever
 * the server has deleted a row the queue still patches, and that is delete-wins
 * working rather than a mistake anyone can act on.
 *
 * Worth a warning at all because the write is queued and applies nowhere, so
 * the only other sign is a `conflict` rejection a round trip later. Keys are
 * opaque strings on the wire, which is what lets the usual cause — a key
 * belonging to a different entity than the one it was written through — reach
 * the server at all.
 */
function warnPatchWithoutTarget(crud: CrudMutation, workspaceId: string): void {
	reportMisuse(
		`patch of ${crud.entity} "${crud.key}" in workspace "${workspaceId}" has no row to apply to, `
			+ 'so it was not applied locally and the server will reject it as a conflict. A patch does '
			+ 'not create rows: check that the key names a row of this entity, or write it with put().',
	);
}

/** Flattens sets of pinned mutation ids into one. */
function unionOf(sets: Iterable<ReadonlySet<bigint>>): Set<bigint> {
	const out = new Set<bigint>();
	for (const set of sets) {
		for (const id of set) out.add(id);
	}
	return out;
}

/** Reads the workspace set out of the options, rejecting contradictory input. */
function resolveWorkspaces(opts: { workspaceId?: string; workspaces?: string[] }): string[] {
	const list = opts.workspaces ?? (opts.workspaceId ? [ opts.workspaceId ] : []);
	if (opts.workspaceId && opts.workspaces && !opts.workspaces.includes(opts.workspaceId)) {
		throw new Error('workspaceId must be one of workspaces');
	}
	if (list.length === 0) throw new Error('workspaceId or workspaces is required');
	// Checked here rather than at first use: an id the server will refuse would
	// otherwise reach the KV layout and the change tokens first, where a bad one
	// misroutes reads and notifications long before any RPC says so.
	for (const ws of list) validateWorkspaceId(ws);
	if (new Set(list).size !== list.length) throw new Error('workspaces contains duplicates');
	return list;
}

/**
 * Picks the store partition. A single-workspace client falls back to its
 * workspace id, which keeps its database exactly where it was; a multi-workspace
 * client must say which account the store belongs to, because no one workspace
 * can stand for it (and picking one would silently move the store whenever that
 * workspace was removed).
 */
function resolveAccountId(opts: { accountId?: string }, workspaces: string[]): string {
	if (opts.accountId) return opts.accountId;
	if (workspaces.length === 1) return workspaces[0]!;
	throw new Error('accountId is required when the client holds more than one workspace');
}

const MUTATION_COUNTER_SUFFIX = '/lastMutationId';

/**
 * The workspace a `meta/ws/<ws>/lastMutationId` key counts for, or undefined
 * when the key is not one.
 */
function workspaceOfMutationCounterKey(key: string): string | undefined {
	if (!key.startsWith(META_WS_PREFIX) || !key.endsWith(MUTATION_COUNTER_SUFFIX)) return undefined;
	return wsFromSegment(key.slice(META_WS_PREFIX.length, -MUTATION_COUNTER_SUFFIX.length));
}

/**
 * The scope a cross-workspace read runs under. A uniform query keeps its own
 * predicate and only narrows which workspaces are read; a per-workspace one
 * compiles here, so the workspaces its callback declined never reach the store.
 *
 * The workspace list comes back deduplicated and in key-segment order, so two
 * callers naming the same set in either order produce equal scopes — which is
 * what a subscription identity turns on, and what makes the merged result's
 * tie-break reproducible.
 *
 * Throws if a WorkspaceQuery is given no workspaces to apply to.
 * @internal
 */
export function readScopeOf<M>(
	schema: DescMessage,
	q: Query<M> | WorkspaceQuery<M>,
	workspaces: readonly string[] | undefined,
): ReadScope {
	if (workspaces === undefined) {
		if (!isWorkspaceQuery(q)) return undefined;
		throw new Error(
			'byWorkspace has no workspaces to apply to: a per-workspace predicate is built '
				+ 'from the set it runs over, so pass opts.workspaces',
		);
	}
	const named = [ ...new Set(workspaces) ].sort(compareWorkspaces);
	if (!isWorkspaceQuery(q)) return { workspaces: named };
	const built = compileByWorkspace(q, named);
	// Resolved against the schema exactly as the shared query is: a condition
	// ending at a reference compares by the target's key, and only this step
	// extends the path to it. Skipping it would leave `assignee.eq(id)` — the
	// motivating shape — comparing against a message and matching nothing.
	const predicates = new Map<string, Cond>();
	for (const [ workspaceId, where ] of built.predicates) {
		predicates.set(workspaceId, prepareQuery(schema, { where }).where!);
	}
	return { workspaces: built.workspaces, predicates };
}

/** The single workspace a scope names, if it names exactly one. */
function scopedWorkspace(scope: ReadScope): string | undefined {
	if (typeof scope !== 'object') return scope;
	return scope.workspaces.length === 1 ? scope.workspaces[0] : undefined;
}

/**
 * The effective-predicate resolver of a read whose query crosses references:
 * given the pre-pass key sets, returns each workspace's base predicate with
 * every rel node collapsed to that workspace's membership test, memoized per
 * workspace.
 */
function relWhereFor(
	keys: RelKeys,
	baseOf: (workspaceId: string) => Cond | undefined,
): (workspaceId: string) => Cond | undefined {
	const memo = new Map<string, Cond | undefined>();
	return (workspaceId) => {
		if (memo.has(workspaceId)) return memo.get(workspaceId);
		const base = baseOf(workspaceId);
		const where = base === undefined ? undefined : rewriteWithKeys(base, keys, workspaceId);
		memo.set(workspaceId, where);
		return where;
	};
}

/**
 * The crossed references of a read, in the form the coverage rule consumes:
 * target entity names, plus the ref field's localName for a top-level path —
 * the name a subscription's follow spec would use.
 */
function relCoverageRefs(
	registry: EntityRegistry,
	nodes: readonly RelNode[],
): RelCoverageRef[] | undefined {
	if (nodes.length === 0) return undefined;
	return nodes.map((node) => ({
		targetEntity: registry.bySchema(node.targetSchema).name,
		...(node.cond.path.length === 1 ? { refLocalName: node.cond.path[0] } : {}),
	}));
}

/**
 * Rows a scope's per-workspace predicates admit. A uniform query has none and
 * keeps its own `where`, which the query path applies.
 */
function admittedIn<T>(
	named: NamedWorkspaces | undefined,
	rows: Row<T>[],
	schema: DescMessage,
): Row<T>[] {
	const predicates = named?.predicates;
	if (predicates === undefined) return rows;
	return rows.filter((row) => {
		const where = predicates.get(row.workspaceId);
		return where !== undefined && evalCond(row.value, where, schema);
	});
}

/**
 * The page metadata a local read reports: what the evaluator already counted,
 * plus the positions of the first and last row so a caller can page on.
 *
 * `cursors` is absent for a read that cannot mint one — a read spanning
 * workspaces, whose positions would not be comparable against one workspace's
 * ordering.
 */
function pageInfoOf<T>(
	cursors: OrderCursors | undefined,
	entity: string,
	page: { rows: Row<T>[]; total: number | undefined; hasMore: boolean },
): PageInfo {
	// Both paths leave total undefined exactly when the query opted out, which
	// withCoverage must not confuse with "unpaged, report rows.length".
	const total = page.total ?? 'uncounted';
	if (cursors === undefined || page.rows.length === 0) {
		return { total, hasMore: page.hasMore };
	}
	const first = page.rows[0]!;
	const last = page.rows[page.rows.length - 1]!;
	return {
		total,
		hasMore: page.hasMore,
		startCursor: cursors.encode(entity, first.value, first.key),
		endCursor: cursors.encode(entity, last.value, last.key),
	};
}
