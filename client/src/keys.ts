/**
 * KV key layout. One store per ACCOUNT holds every workspace the client syncs,
 * so `<ws>` is a segment inside the keys rather than the name of the database:
 *
 *   c/<entity>/<ws>/<key>        confirmed (server-acked) row bytes
 *   o/<entity>/<ws>/<key>        optimistic overlay (value or local tombstone)
 *   ot/<entity>/<ws>/<key>/<field>/<id>
 *                                optimistic text log (one delta-state fragment
 *                                per local edit; see overlayTextKey)
 *   b/<gen>/<entity>/<ws>/<key>  bootstrap staging (a generation's incoming snapshot)
 *   p/<ws>/<paddedId>            pending mutation queue (binary Mutation envelope)
 *   meta/ws/<ws>/<name>          cursor, lastMutationId, serverSeq, ackedSeq,
 *                                bootstrapGen, pushedThrough — all per workspace
 *   meta/<name>                  clientId, formatVersion, entities — account-wide
 *   intent/<tabId>               per-tab sync intent (subscriptions, undo refs, heartbeat)
 *
 * The row keyspaces are ENTITY-FIRST, workspace second. A cross-workspace read
 * ("my assigned issues across all projects") is then a single `c/<entity>/`
 * prefix scan, while a per-workspace operation still gets a contiguous
 * `c/<entity>/<ws>/` prefix. Workspace count is the unbounded dimension, so the
 * order that keeps the unbounded fan-out to one scan wins.
 *
 * Secondary indexes have NO keyspace of their own: a confirmed row carries the
 * positions it occupies as an array field, and the KV store maintains one
 * index over that field (see kv/store.ts INDEX_FIELD and indexPositionPrefix
 * below).
 *
 * Reads resolve overlay-first and never touch the b/ staging keyspace, so a
 * bootstrap that has a snapshot to protect streams into b/ without disturbing
 * the live merged view. A bootstrap of a workspace holding NO snapshot skips b/
 * and writes c/ directly — there is no view to keep intact — which is decided
 * once, at the claim (see Coordinator.doBootstrap). Pending ids are zero-padded
 * so lexicographic scan order equals numeric order.
 *
 * Workspace ids are percent-encoded where they appear as a key segment (see
 * `wsSegment`); row keys are NOT, since they are always last and may contain
 * slashes.
 */

/**
 * Encodes a workspace id as a single key segment. Workspace ids are
 * app-supplied strings that may contain `/`, which would otherwise forge extra
 * segments and make `<entity>/<ws>/<key>` ambiguous; percent-encoding leaves no
 * slash in the segment, so the parsers below can split on the first two.
 */
export const wsSegment = (workspaceId: string): string => encodeURIComponent(workspaceId);

/** Inverse of wsSegment. */
export const wsFromSegment = (segment: string): string => decodeURIComponent(segment);

/**
 * Orders two workspace ids the way the store orders their rows — by the encoded
 * key segment rather than the raw id, which percent-encoding does not preserve
 * (`a b` encodes above `a!`, and sorts the other way raw).
 *
 * A read spanning several workspaces has to visit them in this order for its
 * rows to come out in the same order a single scan across all of them would
 * yield.
 */
export const compareWorkspaces = (a: string, b: string): number => {
	const sa = wsSegment(a);
	const sb = wsSegment(b);
	return sa < sb ? -1 : sa > sb ? 1 : 0;
};

export const META_CLIENT_ID = 'meta/clientId';
export const META_FORMAT_VERSION = 'meta/formatVersion';
/**
 * The entity names this store syncs, as a sorted string array — every name
 * registered by any build that has opened it. Account-wide, like the clientId:
 * the row keyspaces are entity-first and span every workspace, so the set that
 * describes them cannot be per workspace. See entities.ts for what it is for
 * and why it only ever grows.
 */
export const META_ENTITIES = 'meta/entities';

/** Root of the per-workspace meta keyspace; scanned to purge a workspace. */
export const META_WS_PREFIX = 'meta/ws/';
/** All per-workspace meta of one workspace: `meta/ws/<ws>/`. */
export const metaWsPrefix = (workspaceId: string): string =>
	`${META_WS_PREFIX}${wsSegment(workspaceId)}/`;

/** Opaque resume cursor for one workspace. */
export const metaCursor = (workspaceId: string): string => `${metaWsPrefix(workspaceId)}cursor`;
/**
 * Per-workspace mutation-id counter. Counters stay per workspace because the
 * server's watermark is keyed (workspace, client_id) and requires
 * `id == watermark + 1`, so an account-wide counter would produce gaps.
 */
export const metaLastMutationId = (workspaceId: string): string =>
	`${metaWsPrefix(workspaceId)}lastMutationId`;
/**
 * Highest mutation id any tab sharing this store has ATTEMPTED to push to the
 * server for this workspace (advanced before the RPC, not after it acks).
 * Compaction may only renumber/fold ids strictly above it, so nothing the server
 * might already have seen is ever touched. Durable + shared — unlike a per-tab
 * in-memory guard it survives a lost push response and is visible to every tab —
 * which is what keeps compaction from silently dropping or double-applying
 * mutations.
 *
 * A resequence lowers it (see resequencePending): the ids it renumbers were
 * refused by the server rather than applied, so leaving the mark above them
 * would freeze compaction out of a queue the server never took. A format
 * upgrade that keeps the counter restamps it from the counter (see
 * SyncClient.ensureFormatVersion), since the salvage it just ran leaves the
 * server holding every id the counter has minted.
 */
export const metaPushedThrough = (workspaceId: string): string =>
	`${metaWsPrefix(workspaceId)}pushedThrough`;
/**
 * Monotonic bootstrap generation for one workspace. A bootstrap bumps it before
 * streaming and stages under that generation; at swap time a mismatch means
 * another bootstrap superseded this one. See Coordinator.doBootstrap.
 */
export const metaBootstrapGen = (workspaceId: string): string =>
	`${metaWsPrefix(workspaceId)}bootstrapGen`;
/**
 * The bootstrap generation whose snapshot is currently INSTALLED — written by
 * the swap that installs it, not by the claim that starts it.
 *
 * That distinction is the whole point, and it is why this cannot be folded into
 * metaBootstrapGen: the claim bumps that one before streaming, so a reader who
 * sampled it and then did a long walk cannot tell whether a swap landed
 * underneath. This changes at exactly one moment — the commit that replaces the
 * confirmed rows — which makes it a usable guard for work done outside that
 * transaction. The index build is the one such reader (see index-build.ts).
 */
export const metaSnapshotGen = (workspaceId: string): string =>
	`${metaWsPrefix(workspaceId)}snapshotGen`;
/**
 * Where an interrupted bootstrap of this workspace got to, so the next attempt
 * continues its snapshot instead of downloading it again: the opaque server
 * tokens plus which generation staged the partial snapshot they describe. It is
 * durable rather than in-memory because the interruption that matters most —
 * closing the tab mid-snapshot on a slow link — takes the process with it.
 *
 * Written as each chunk commits, adopted (and re-stamped with the adopting
 * attempt's generation) by a claim that finds it still current, and deleted by
 * the swap that installs the snapshot. See Coordinator.doBootstrap.
 */
export const metaBootstrapResume = (workspaceId: string): string =>
	`${metaWsPrefix(workspaceId)}bootstrapResume`;
/**
 * Workspace seq the shared store has pulled through (decimal string), written
 * in the same transaction as the cursor. The pull path's monotonic gate
 * compares each incoming page against it and drops stale pages whole, so two
 * tabs pulling concurrently can never commit the shared cursor (or rows) out
 * of order. A bootstrap swap writes it unconditionally — a full snapshot is
 * authoritative even when its seq went backwards (e.g. restored backup).
 */
export const metaServerSeq = (workspaceId: string): string =>
	`${metaWsPrefix(workspaceId)}serverSeq`;
/**
 * Highest workspace seq the server acked one of this store's pushes at
 * (decimal string; MutationResult.seq over APPLIED results). Sent back as
 * PullRequest.min_seq / WorkspaceScope.min_seq so a replica-reading server
 * never serves a snapshot older than the store's own acked writes. Written
 * monotonic-max — tabs push independently and a lower ack must not regress
 * it — and discarded with the cursor lineage: a floor carried across a store
 * swap could sit above the new head forever.
 */
export const metaAckedSeq = (workspaceId: string): string => `${metaWsPrefix(workspaceId)}ackedSeq`;

/**
 * Version of this KV layout. On mismatch the client tries to push the
 * pending queues once, then wipes everything except the clientId and the
 * per-workspace mutation-id counters (and the pushed-through marks that pair
 * with them) and re-bootstraps. Bump when the layout or
 * record encoding changes incompatibly — in either direction: a client that
 * cannot read a newer store's records is as broken as one whose writes a newer
 * client would misread.
 */
export const FORMAT_VERSION = 1;

/** Every workspace's confirmed rows of one entity — the cross-workspace scan. */
export const confirmedPrefix = (entity: string): string => `c/${entity}/`;
/** Every workspace's overlay rows of one entity. */
export const overlayPrefix = (entity: string): string => `o/${entity}/`;
/** One workspace's confirmed rows of one entity. */
export const confirmedWsPrefix = (entity: string, workspaceId: string): string =>
	`c/${entity}/${wsSegment(workspaceId)}/`;
/** One workspace's overlay rows of one entity. */
export const overlayWsPrefix = (entity: string, workspaceId: string): string =>
	`o/${entity}/${wsSegment(workspaceId)}/`;
export const confirmedKey = (entity: string, workspaceId: string, key: string): string =>
	`${confirmedWsPrefix(entity, workspaceId)}${key}`;
export const overlayKey = (entity: string, workspaceId: string, key: string): string =>
	`${overlayWsPrefix(entity, workspaceId)}${key}`;

/**
 * The optimistic TEXT log: one delta-state fragment per local edit to one
 * collaborative-text field, appended rather than folded into the overlay row.
 *
 * A text field's optimistic value is the base document (the overlay row's copy
 * of the field where the overlay holds one, else the confirmed row's) with every
 * fragment under `overlayTextRowPrefix` applied in key order. Keeping the edits
 * out of the row is what makes a keystroke cost O(edit) instead of O(document)
 * on disk: the overlay row is a single value that would have to be rewritten
 * whole, whereas these are separate keys that are only ever appended.
 *
 * Entries are pure derived state — the same fragments sit in the pending queue —
 * and their lifetime is the overlay's: whatever clears `o/` clears these too,
 * and rebase replay re-appends them (see clearOverlayAndReplay). They live in
 * their own keyspace rather than the queue's because a fold needs them BY ROW,
 * and `p/<ws>/<paddedId>` is ordered by mutation instead.
 *
 * `id` orders the fold and must therefore sort in the order the edits were
 * made: it is the minting mutation's zero-padded id, plus its index within a
 * batch. Order matters because a fragment may name items an earlier one minted,
 * and applying it before them fails; the merge itself is a commutative union.
 *
 * NOTE the prefix is `ot/`, not a suffix under `o/`: an `o/<entity>/` scan must
 * never yield these, and it cannot, since `o/` is always followed by an entity
 * name and a slash.
 */
export const OVERLAY_TEXT_ROOT = 'ot/';
/** Every workspace's text log of one entity — mirrors overlayPrefix. */
export const overlayTextPrefix = (entity: string): string => `${OVERLAY_TEXT_ROOT}${entity}/`;
/** One workspace's text log of one entity. */
export const overlayTextWsPrefix = (entity: string, workspaceId: string): string =>
	`${overlayTextPrefix(entity)}${wsSegment(workspaceId)}/`;
/** Every fragment logged for one row, across its text fields. */
export const overlayTextRowPrefix = (
	entity: string,
	workspaceId: string,
	key: string,
): string => `${overlayTextWsPrefix(entity, workspaceId)}${key}/`;
/** Every fragment logged for one field of one row — the unit a fold consumes. */
export const overlayTextFieldPrefix = (
	entity: string,
	workspaceId: string,
	key: string,
	field: string,
): string => `${overlayTextRowPrefix(entity, workspaceId, key)}${field}/`;

/**
 * A secondary-index POSITION: `i/<entity>/<index>@<hash>/<ws>/<entry>`, where
 * the entry is the encoded sort key (see query/indexkey.ts). Positions are the
 * elements of a confirmed row's INDEX_FIELD array, and the KV store's one
 * secondary index orders them — so position order within one prefix IS the row
 * order the index serves, and a bounded indexScan over that prefix reads the
 * slice a query names.
 *
 * They are not KV keys and the store never resolves one, but they keep the
 * shape of the keyspace they replaced. What a position has to be is a string
 * that (a) sorts, within one index and workspace, exactly as the rows do, and
 * (b) shares its prefix with no OTHER index, entity or workspace — every
 * declared index of every entity lands in the same store-maintained B-tree, so
 * the prefix is the only thing keeping their ranges apart.
 *
 * `<ws>` sits INSIDE the prefix, before the entry, rather than outside it as in
 * the row keyspaces. A query is normally workspace-scoped, so selectivity
 * matters more there than cross-workspace fan-out, and an ordered range read
 * has to be contiguous within one workspace or it is not a range read at all.
 * A cross-workspace query pays a k-way merge over per-workspace prefixes
 * instead, bounded by the workspaces it actually names.
 *
 * `@<hash>` is the index's spec fingerprint. A spec change therefore ORPHANS
 * the positions written under the old spec rather than corrupting them: they
 * are unreachable under the new prefix, and the new one starts empty and
 * unbuilt. They are reclaimed by the build, which replaces a row's whole
 * position list rather than adding to it (see index-maintain.ts).
 *
 * Only CONFIRMED rows carry positions. The optimistic overlay is bounded by the
 * pending queue, so a query scans all of it and merges — which keeps the answer
 * identical to the merged view without the index having to track uncommitted
 * state, and is also why an `o/` record must never grow an INDEX_FIELD.
 */
const INDEX_POSITION_ROOT = 'i/';
/** One index of one entity, in one workspace — the prefix a range read bounds
 * itself with, and the unit that must not collide with another index's. */
export const indexPositionPrefix = (
	entity: string,
	index: string,
	specHash: string,
	workspaceId: string,
): string => `${INDEX_POSITION_ROOT}${entity}/${index}@${specHash}/${wsSegment(workspaceId)}/`;
/** One position: the encoded sort key appended to its workspace's prefix. */
export const indexPosition = (
	entity: string,
	index: string,
	specHash: string,
	workspaceId: string,
	entry: string,
): string => `${indexPositionPrefix(entity, index, specHash, workspaceId)}${entry}`;

/**
 * Marks an index as fully built for one workspace: `meta/ws/<ws>/index/<entity>/<name>@<hash>`.
 * Until it exists, entries cover only the rows written since the index was
 * declared, so every read falls back to scanning the merged rows.
 */
export const metaIndexBuilt = (
	workspaceId: string,
	entity: string,
	index: string,
	specHash: string,
): string => `${metaWsPrefix(workspaceId)}index/${entity}/${index}@${specHash}`;
/** Every index-built marker of one workspace, for a wipe. */
export const metaIndexPrefix = (workspaceId: string): string =>
	`${metaWsPrefix(workspaceId)}index/`;
/** Every index-built marker of ONE entity, for a per-entity drop. */
export const metaIndexEntityPrefix = (workspaceId: string, entity: string): string =>
	`${metaIndexPrefix(workspaceId)}${entity}/`;

/**
 * How far an index build has walked one entity, so an interrupted walk resumes
 * instead of starting the entity again: `meta/ws/<ws>/indexbuild/<entity>`.
 *
 * Deliberately NOT under metaIndexPrefix, whose whole keyspace the confirmed
 * wipe deletes — `index/` and `indexbuild/` share a stem but neither prefix
 * selects the other, since `index` is followed by `b` rather than `/`. A cursor
 * is not a claim about coverage the way a built marker is; it is only a
 * position, and it carries the snapshot generation it was taken under so a
 * bootstrap landing in between invalidates it rather than silently resuming
 * into different rows.
 */
export const metaIndexBuildCursor = (workspaceId: string, entity: string): string =>
	`${metaWsPrefix(workspaceId)}indexbuild/${entity}`;

/** A build cursor: the snapshot generation `g` it was taken under, and the last
 * row key `k` whose entries committed. */
export interface IndexBuildCursor {
	g: number | undefined;
	k: string;
}

export const overlayTextKey = (
	entity: string,
	workspaceId: string,
	key: string,
	field: string,
	mutationId: bigint,
	index: number,
): string =>
	`${overlayTextFieldPrefix(entity, workspaceId, key, field)}${
		mutationId.toString().padStart(PAD, '0')
	}/${index.toString().padStart(4, '0')}`;

/**
 * Splits what follows `ot/<entity>/` into its workspace, row key and field.
 *
 * Parsed from the RIGHT, unlike the row keyspaces: a row key may contain
 * slashes, and here it is no longer the last segment. The three trailing
 * segments are the field path (dot-separated, never slashed), the mutation id
 * and the batch index, all of which are slash-free, so everything before them
 * is the row key verbatim — and everything before THAT, up to the first slash,
 * is the workspace segment.
 */
export function splitOverlayTextSuffix(
	suffix: string,
): { workspaceId: string; key: string; field: string } | undefined {
	const wsEnd = suffix.indexOf('/');
	if (wsEnd < 0) return undefined;
	const index = suffix.lastIndexOf('/');
	const id = suffix.lastIndexOf('/', index - 1);
	const field = suffix.lastIndexOf('/', id - 1);
	if (field <= wsEnd) return undefined;
	return {
		workspaceId: wsFromSegment(suffix.slice(0, wsEnd)),
		key: suffix.slice(wsEnd + 1, field),
		field: suffix.slice(field + 1, id),
	};
}

/**
 * Splits the part of a row key that follows `c/<entity>/` or `o/<entity>/` into
 * its workspace and row key. The workspace segment carries no slash
 * (wsSegment), so the first one ends it and everything after is the row key,
 * verbatim.
 */
export function splitWsKey(suffix: string): { workspaceId: string; key: string } {
	const slash = suffix.indexOf('/');
	return {
		workspaceId: wsFromSegment(suffix.slice(0, slash)),
		key: suffix.slice(slash + 1),
	};
}

/** Root of all bootstrap staging, across every workspace. */
export const STAGING_ROOT = 'b/';
/**
 * All of one workspace's bootstrap staging: `b/<ws>/`. Staging is workspace-
 * FIRST (unlike the row keyspaces) because generations are counted per
 * workspace and a bootstrap sweeps every earlier generation of its own
 * workspace when it claims one — sweeping the shared root would discard a
 * concurrent bootstrap of a different workspace mid-stream.
 */
export const stagingWsRoot = (workspaceId: string): string => `b/${wsSegment(workspaceId)}/`;
/** Staging prefix for one workspace's bootstrap generation: `b/<ws>/<gen>/`. */
export const stagingPrefix = (workspaceId: string, gen: number): string =>
	`${stagingWsRoot(workspaceId)}${gen}/`;
/**
 * Staging key for a row of generation `gen`. The suffix after `stagingPrefix`
 * is exactly `<entity>/<key>`, and the workspace is implied by the prefix, so
 * promoting a staged row needs only the entity split off the front — the row
 * key is preserved verbatim even when it contains slashes.
 */
export const stagingKey = (
	workspaceId: string,
	gen: number,
	entity: string,
	key: string,
): string => `${stagingPrefix(workspaceId, gen)}${entity}/${key}`;

/**
 * Per-tab sync intent records (see sync/intent.ts). `intent/<tabId>` holds a
 * tab's per-workspace subscription sets, undo-referenced mutation ids and a
 * heartbeat timestamp, so every tab sharing the store can sync with the union of
 * all live tabs' intent.
 */
export const INTENT_PREFIX = 'intent/';
export const intentKey = (tabId: string): string => `${INTENT_PREFIX}${tabId}`;

const PAD = 16;

/** Root of every workspace's pending queue. */
export const PENDING_ROOT = 'p/';
/** One workspace's pending queue: `p/<ws>/`. */
export const pendingPrefix = (workspaceId: string): string =>
	`${PENDING_ROOT}${wsSegment(workspaceId)}/`;

export function pendingKey(workspaceId: string, mutationId: bigint): string {
	return `${pendingPrefix(workspaceId)}${mutationId.toString().padStart(PAD, '0')}`;
}

/** The workspace a `p/<ws>/<paddedId>` key belongs to. */
export function workspaceOfPendingKey(key: string): string {
	return wsFromSegment(key.slice(PENDING_ROOT.length, key.lastIndexOf('/')));
}

/**
 * Separator inside a touch token. Splitting on the first two occurrences is
 * unambiguous because neither of the two fields before them can contain one:
 * the workspace id and the entity name are both validated identifiers (see
 * naming.ts). Everything after the second separator is a row key verbatim,
 * which may itself contain anything printable.
 *
 * The workspace id is the FIRST field, so this rests on its rule specifically —
 * an id holding a unit separator would shift every field after it and
 * parseTouchKey would hand back a different (workspace, entity, key) triple
 * than the one touchKey was given, waking the wrong watchers.
 */
const TOUCH_SEP = '\u001f';

/**
 * Identifies a (workspace, entity) pair whose merged view changed but whose
 * changed ROWS are unknown — the wildcard token. Watchers match against these
 * and they ride the cross-tab change channel verbatim, so a watcher bound to
 * one workspace is not woken by a commit in another. See TouchSet.
 */
export const touchKey = (workspaceId: string, entity: string): string =>
	`${workspaceId}${TOUCH_SEP}${entity}`;

/**
 * Identifies one changed ROW: the wildcard token plus the row key. Lets a
 * watcher patch its snapshot of the entity instead of re-scanning all of it.
 */
export const rowTouchKey = (workspaceId: string, entity: string, key: string): string =>
	`${workspaceId}${TOUCH_SEP}${entity}${TOUCH_SEP}${key}`;

/**
 * Splits a touch token back into its workspace, entity and — for a row token —
 * its row key. `key` is undefined for a wildcard token.
 */
export function parseTouchKey(
	token: string,
): { workspaceId: string; entity: string; key?: string } {
	const first = token.indexOf(TOUCH_SEP);
	const second = token.indexOf(TOUCH_SEP, first + 1);
	if (second < 0) {
		return { workspaceId: token.slice(0, first), entity: token.slice(first + 1) };
	}
	return {
		workspaceId: token.slice(0, first),
		entity: token.slice(first + 1, second),
		key: token.slice(second + 1),
	};
}

/** Confirmed row record stored at c/ keys. */
export interface ConfirmedRecord {
	/** Serialized entity message. */
	v: Uint8Array;
	/** Row version (workspace seq) as decimal string. */
	ver: string;
	/**
	 * The secondary-index positions this row occupies (see indexPosition), which
	 * is the KV store's INDEX_FIELD — the field name is what ties the two
	 * together, so it cannot be renamed here alone.
	 *
	 * Absent means the row occupies none, which is a legitimate state rather
	 * than a broken one: a bootstrap installs rows unindexed and the build pass
	 * fills them in, and an entity that declares no index never has any. It also
	 * means a writer that rewrites a row without recomputing this UNINDEXES it —
	 * there is no separate keyspace left to hold a stale entry.
	 *
	 * A staged (`b/`) record must never carry it. The store's index spans every
	 * record it holds, so positions on a staged row would be answering queries
	 * from a snapshot that is not installed yet (see promoteStaged).
	 */
	ix?: string[];
}

/** Optimistic overlay record stored at o/ keys. */
export type OverlayRecord = { v: Uint8Array; del?: undefined } | { del: true; v?: undefined };
