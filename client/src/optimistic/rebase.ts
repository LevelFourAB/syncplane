import {
	clone,
	type DescMessage,
	equals,
	fromBinary,
	type Message,
	type MessageShape,
	toBinary,
} from '@bufbuild/protobuf';
import { anyUnpack } from '@bufbuild/protobuf/wkt';

import {
	ACCESS_TOUCH_ENTITY,
	applyAccessDelta,
	type DecodedAccess,
	encodeAccess,
	touchPendingAccess,
	writeChangesAccess,
} from '../access.js';
import { syncableEntities } from '../entities.js';
import {
	type CrudMutation,
	CrudOp,
	type Mutation,
	MutationSchema,
} from '../gen/syncplane/v1/mutation_pb.js';
import type { RowChange } from '../gen/syncplane/v1/patch_pb.js';
import { type CallerAccessDelta, CallerAccessSchema } from '../gen/syncplane/v1/service_pb.js';
import {
	confirmedKey,
	type ConfirmedRecord,
	confirmedWsPrefix,
	isRowTextKey,
	metaAckedSeq,
	metaCallerAccess,
	metaCallerAccessVersion,
	metaCursor,
	metaIndexEntityPrefix,
	metaIndexPrefix,
	metaLastMutationId,
	metaServerSeq,
	overlayKey,
	type OverlayRecord,
	overlayTextKey,
	overlayTextPrefix,
	overlayTextRowPrefix,
	overlayTextWsPrefix,
	overlayWsPrefix,
	pendingKey,
	searchIndexKey,
	type SearchIndexRecord,
	searchIndexWsPrefix,
	splitOverlayTextSuffix,
	stagingKey,
	stagingPrefix,
} from '../keys.js';
import type { KVRead, KVStore, KVWrite } from '../kv/store.js';
import {
	type EntityDef,
	type EntityMergers,
	type EntityRegistry,
	keyOf,
	mergeMask,
	mergeTextFieldsInto,
	textFieldBytes,
	textSchemaPathOf,
} from '../mutations.js';
import { readMergedRow, rowPresent, scanMergedRows } from '../read.js';
import { reportError } from '../report.js';
import { applyTextDeltas, TextDeltaUnusableError } from '../sync/textdelta.js';
import { isFragment } from '../text/index.js';
import { TouchSet } from '../touch.js';
import { markIndexesBuilt } from './index-build.js';
import {
	deletePrefixes,
	type IndexCache,
	indexedRecord,
	samePositions,
	searchIndexOf,
	searchPositions,
	writeSearchRecord,
} from './index-maintain.js';
import {
	dropAcked,
	enqueuePending,
	readPending,
	recountPendingAccess,
	writePushedThrough,
} from './queue.js';

/**
 * Typed storage facade custom-mutation mirrors run against. Reads resolve
 * overlay-first; writes land in the optimistic overlay. Everything is scoped to
 * the workspace of the mutation being mirrored — a mutator cannot reach across
 * the partition, exactly as its server-side twin cannot.
 */
export interface MirrorTx {
	get<D extends DescMessage>(schema: D, key: string): Promise<MessageShape<D> | undefined>;
	scan<D extends DescMessage>(schema: D): Promise<MessageShape<D>[]>;
	put<D extends DescMessage>(schema: D, value: MessageShape<D>): Promise<void>;
	del(schema: DescMessage, key: string): Promise<void>;
}

/**
 * Best-effort local twin of a server mutator. Divergence from the server
 * implementation is fine: it self-heals on the next rebase, because replay
 * always happens on top of server-confirmed state.
 *
 * A rejected `apply` fails the write that issued the mutation. During rebase
 * replay it instead leaves that one mutation without optimistic state and is
 * reported over console.error: the mutation is already queued, and the row it
 * writes appears when the server acks it.
 */
export interface MutatorMirror<D extends DescMessage = DescMessage> {
	args?: D;
	apply(tx: MirrorTx, args: MessageShape<D> | undefined): Promise<void>;
}

export type MirrorMap = Record<string, MutatorMirror<DescMessage>>;

class KVMirrorTx implements MirrorTx {
	constructor(
		private tx: KVWrite,
		private registry: EntityRegistry,
		private workspaceId: string,
		private touched: TouchSet,
	) {}

	async get<D extends DescMessage>(schema: D, key: string): Promise<MessageShape<D> | undefined> {
		const def = this.registry.bySchema(schema);
		const bytes = await readMergedRow(this.tx, this.registry, def.name, this.workspaceId, key);
		return bytes === undefined ? undefined : fromBinary(schema, bytes);
	}

	async scan<D extends DescMessage>(schema: D): Promise<MessageShape<D>[]> {
		const def = this.registry.bySchema(schema);
		const rows = await scanMergedRows(this.tx, this.registry, def.name, this.workspaceId);
		return rows.map((row) => fromBinary(schema, row.v));
	}

	async put<D extends DescMessage>(schema: D, value: MessageShape<D>): Promise<void> {
		const def = this.registry.bySchema(schema);
		const record: OverlayRecord = { v: toBinary(schema, value) };
		const key = keyOf(def, value);
		await this.tx.put(overlayKey(def.name, this.workspaceId, key), record);
		this.touched.row(this.workspaceId, def.name, key);
		this.touchAccess(def.name, false);
	}

	async del(schema: DescMessage, key: string): Promise<void> {
		const def = this.registry.bySchema(schema);
		const record: OverlayRecord = { del: true };
		await this.tx.put(overlayKey(def.name, this.workspaceId, key), record);
		this.touched.row(this.workspaceId, def.name, key);
		this.touchAccess(def.name, true);
	}

	// What a queued custom mutation can do to the caller's grants is decided
	// from what its mirror wrote, and a replay during rebase can write other
	// rows than the first apply did, with no change to the queue.
	private touchAccess(entity: string, del: boolean): void {
		if (writeChangesAccess(this.registry, entity, del)) {
			this.touched.entity(this.workspaceId, ACCESS_TOUCH_ENTITY);
		}
	}
}

/**
 * What one local apply did to the overlay. `no-target` is delete-wins: a PATCH
 * whose row is absent or locally deleted wrote nothing, and the server rejects
 * the queued mutation as a conflict. Only a PATCH ever reports it; every other
 * op writes.
 *
 * What it means is the caller's to decide. A `no-target` on an app's own write
 * names a row that is not there; on a replay during rebase it is the ordinary
 * outcome for a queued PATCH the server has since deleted.
 */
export type LocalApplyOutcome = 'applied' | 'no-target';

/**
 * Applies one CRUD op to the optimistic overlay of `workspaceId`. Shared by the
 * crud and batch cases of applyMutationLocally and by the client's batch
 * undo-capture loop, so all three stay in lockstep with the server's applyCrud.
 *
 * `prior` lets a caller that has already read the merged row hand it over
 * instead of having it read a second time in the same transaction — it is
 * boxed rather than passed bare so an absent row stays distinguishable from an
 * absent argument. It must be the row as of this call: pass it only from the
 * same transaction, and only before anything else in it has written the row.
 *
 * `confirmed` is the row's confirmed base version and the fields its confirmed
 * record hides (see ConfirmedRecord.h), which a caller that read the confirmed
 * record up front already holds. Given it, a write that produces a
 * whole row reports that row on `touched` as well as its key, so a consumer can
 * take the post-commit merged view from there rather than reading it back (see
 * TouchSet.rowWithView). Leaving it out costs a read, not correctness.
 */
export async function applyCrudLocally(
	tx: KVWrite,
	registry: EntityRegistry,
	workspaceId: string,
	crud: CrudMutation,
	touched: TouchSet,
	prior?: { bytes: Uint8Array | undefined },
	slot?: TextLogSlot,
	confirmed?: { version: bigint; hidden?: readonly string[] },
): Promise<LocalApplyOutcome> {
	const def = registry.get(crud.entity);
	if (!def) throw new Error(`unknown entity "${crud.entity}"`);
	const readPrior = async (): Promise<Uint8Array | undefined> =>
		prior ? prior.bytes : readMergedRow(tx, registry, def.name, workspaceId, crud.key);
	// Reported before the switch: the PATCH branch below can bail without
	// writing, and naming a row that did not move only costs the watcher hub one
	// redundant point read — far cheaper than the scan an omission would need.
	touched.row(workspaceId, def.name, crud.key);
	switch (crud.op) {
		case CrudOp.PUT: {
			let value = crud.value;
			// Text fields union with the previous row instead of resetting —
			// the client mirror of the server's PUT exception. Without it a
			// PUT would reset the local document while the server merges,
			// and the divergence would surface as duplicated text on rebase.
			const textPaths = registry.textPathsOf(def.name);
			if (textPaths.length > 0) {
				const prevBytes = await readPrior();
				if (prevBytes !== undefined) {
					const base = fromBinary(def.schema, prevBytes);
					const next = fromBinary(def.schema, crud.value);
					mergeTextFieldsInto(def.schema, textPaths, base, next);
					value = toBinary(def.schema, next);
				}
			}
			const record: OverlayRecord = { v: value };
			await tx.put(overlayKey(def.name, workspaceId, crud.key), record);
			// The row it just wrote already carries the folded text, so the log it
			// was folded from must go — or the next read would apply those edits a
			// second time. (Idempotent for the document, but it would grow forever.)
			await clearRowTextLog(tx, registry, def.name, workspaceId, crud.key);
			// Which is also what makes these bytes the whole merged view: with the
			// log gone there is nothing left for a reader to fold into them.
			if (confirmed) {
				touched.rowWithView(workspaceId, def.name, crud.key, {
					v: value,
					version: confirmed.version,
					hidden: confirmed.hidden,
				});
			}
			return 'applied';
		}
		case CrudOp.PATCH: {
			// A keystroke: every field the mask names is collaborative text, and the
			// handle queued the fragment for its one edit rather than a document.
			// Appending that fragment IS the whole optimistic write — no row is
			// rewritten, so what a keystroke costs on disk stops scaling with the
			// document. The read path folds it back (read.ts).
			if (slot !== undefined) {
				const fast = await appendTextLog(tx, registry, def, workspaceId, crud, slot);
				if (fast !== 'fallthrough') return fast;
			}
			const baseBytes = await readPrior();
			// Delete-wins (mirrors the server's applyCrud PATCH policy): a PATCH
			// whose target row is absent or deleted does not resurrect it — the
			// server rejects it as a conflict, so we must not materialize a ghost in
			// the overlay. Skip; the rejection rolls the pending mutation back on
			// the next push.
			if (baseBytes === undefined) return 'no-target';
			const paths = crud.fieldMask?.paths ?? [];
			const mergers = registry.mergersFor(def.name);
			const base = fromBinary(def.schema, baseBytes);
			const src = fromBinary(def.schema, crud.value);
			let merged = base;
			try {
				mergeMask(def.schema, base, src, paths, mergers);
			} catch (err) {
				merged = withoutUnapplicableText(def, registry, baseBytes, src, paths, mergers, err);
			}
			const bytes = toBinary(def.schema, merged);
			const record: OverlayRecord = { v: bytes };
			await tx.put(overlayKey(def.name, workspaceId, crud.key), record);
			await clearRowTextLog(tx, registry, def.name, workspaceId, crud.key);
			// Whole row, empty log: the same reasoning as the PUT branch above.
			if (confirmed) {
				touched.rowWithView(workspaceId, def.name, crud.key, {
					v: bytes,
					version: confirmed.version,
					hidden: confirmed.hidden,
				});
			}
			return 'applied';
		}
		case CrudOp.DELETE: {
			const record: OverlayRecord = { del: true };
			await tx.put(overlayKey(def.name, workspaceId, crud.key), record);
			await clearRowTextLog(tx, registry, def.name, workspaceId, crud.key);
			// No version to know: a locally deleted row has no merged view whatever
			// the confirmed record behind it says, so this needs no `confirmed`.
			touched.rowWithView(workspaceId, def.name, crud.key, undefined);
			return 'applied';
		}
		default:
			throw new Error(`unknown crud op ${crud.op}`);
	}
}

/**
 * Where in the optimistic text log a crud's fragments go: the id of the
 * mutation minting them and its index within a batch, which together order the
 * fold (see keys.ts). Absent for a caller that has no mutation to attribute the
 * write to, which sends it down the whole-row path instead.
 */
export interface TextLogSlot {
	mutationId: bigint;
	index: number;
}

/**
 * Appends a PATCH's text fragments to the row's log, or returns `fallthrough`
 * to send the caller down the whole-row path. `no-target` is delete-wins,
 * reached without decoding the row.
 *
 * The fast path is deliberately narrow, because it is the one shape where
 * appending is equivalent to merging: the mask must name text fields ONLY, and
 * every one of them must actually hold a fragment. A mask that also names a
 * scalar has a non-text change to apply to the row; a field holding a whole
 * document (a string-valued write, or a re-send after the server asked for full
 * state) is not a delta of anything and would resurrect superseded text if
 * applied over the base rather than replacing it.
 */
async function appendTextLog(
	tx: KVWrite,
	registry: EntityRegistry,
	def: EntityDef,
	workspaceId: string,
	crud: CrudMutation,
	slot: TextLogSlot,
): Promise<LocalApplyOutcome | 'fallthrough'> {
	const paths = crud.fieldMask?.paths ?? [];
	if (paths.length === 0) return 'fallthrough';
	// Every mask path must land on a text field outright — as an instance path
	// where the field sits under a keyed list or map ("items.<key>.body"),
	// which resolves to its schema path here.
	if (registry.textPathsOf(def.name).length === 0) return 'fallthrough';
	if (paths.some((p) => textSchemaPathOf(def.schema, p) === undefined)) return 'fallthrough';
	let src;
	try {
		src = fromBinary(def.schema, crud.value);
	} catch {
		return 'fallthrough';
	}
	const fragments: [string, Uint8Array][] = [];
	for (const path of paths) {
		const bytes = textFieldBytes(def.schema, path, src);
		if (bytes === undefined || !isFragment(bytes)) return 'fallthrough';
		fragments.push([ path, bytes ]);
	}
	if (!(await rowPresent(tx, def.name, workspaceId, crud.key))) return 'no-target';
	for (const [ path, bytes ] of fragments) {
		await tx.put(
			overlayTextKey(def.name, workspaceId, crud.key, path, slot.mutationId, slot.index),
			bytes,
		);
	}
	return 'applied';
}

/**
 * Drops one row's whole text log — see the callers for why each does it. Costs
 * nothing at all for an entity that declares no text field, which the registry
 * settles from the schema without a read.
 */
async function clearRowTextLog(
	tx: KVWrite,
	registry: EntityRegistry,
	entity: string,
	workspaceId: string,
	key: string,
): Promise<void> {
	if (registry.textPathsOf(entity).length === 0) return;
	const keys: string[] = [];
	for await (const [ k ] of tx.scan(overlayTextRowPrefix(entity, workspaceId, key))) {
		if (isRowTextKey(entity, key, k)) keys.push(k);
	}
	for (const k of keys) await tx.del(k);
}

/**
 * Re-applies a PATCH with its text fields left out, for the one failure the
 * overlay can absorb: a queued text delta whose base is gone. A queue entry
 * carries the fragment for one edit (text/handle.ts), so dropping a rejected
 * mutation can strand the entries behind it — their items name parents the
 * rejected one minted. Nothing local can reconstruct those.
 *
 * The row keeps the text it already had rather than failing the replay, which
 * would leave the rebase retrying the same impossible merge forever. The edit
 * itself is not lost: the live handle still holds it, and its next write is
 * refused by the server as `text_full_state_required`, whose recovery re-sends
 * the whole document (sync/coordinator.ts). Anything else rethrows — a mask
 * holding no text path at all, or one whose remaining paths fail too, is a
 * different problem, and half-applying it is worse than leaving it to the caller:
 * replay skips the crud (replayCrud) and a local write fails outright.
 */
function withoutUnapplicableText(
	def: EntityDef,
	registry: EntityRegistry,
	baseBytes: Uint8Array,
	src: Message,
	paths: string[],
	mergers: EntityMergers | undefined,
	err: unknown,
): Message {
	if (registry.textPathsOf(def.name).length === 0) throw err;
	const rest = paths.filter((p) => textSchemaPathOf(def.schema, p) === undefined);
	if (rest.length === paths.length) throw err;
	// From the stored bytes again: the failed merge may have written part of the
	// message before it threw.
	const retry = fromBinary(def.schema, baseBytes);
	mergeMask(def.schema, retry, src, rest, mergers);
	reportError(
		`text: a queued edit to ${def.name} could not be applied to the local row`,
		err,
	);
	return retry;
}

/**
 * Applies one crud of a REPLAYED mutation, skipping and reporting it instead of
 * letting a failure escape into the transaction around it.
 *
 * The pending queue is one per workspace, not one per tab (keys.ts), so an entry
 * is replayed against the registry of whichever tab happens to rebase — and an
 * entry that registry cannot apply is ordinary rather than corrupt: a mask naming
 * a field only the newer build's schema declares (additive evolution), or a value
 * that will not decode as the type this build has for the entity. Every path that
 * rebuilds the overlay does so in the transaction that also advances the
 * workspace, so a throw would leave it unable to pull, roll a rejection back,
 * close a mutation-id gap, or even bootstrap — the swap replays the same entry
 * (see clearOverlayAndReplay's callers).
 *
 * What is skipped is optimism, not the write. The entry stays queued, and the
 * push retires it: the server acks it, rejects it (a rejection is an ack), or the
 * poison backstop dead-letters it after repeated deterministic refusals
 * (sync/coordinator.ts). The tab whose build can apply it holds the optimistic
 * state meanwhile, rebuilt from the same queue on its own next rebase; this one
 * shows the confirmed row until the ack lands. It is the pending queue's half of
 * the rule entities.ts states for confirmed rows.
 */
async function replayCrud(
	tx: KVWrite,
	registry: EntityRegistry,
	workspaceId: string,
	crud: CrudMutation,
	touched: TouchSet,
	slot: TextLogSlot,
): Promise<void> {
	try {
		await applyCrudLocally(tx, registry, workspaceId, crud, touched, undefined, slot);
	} catch (err) {
		reportError(
			`replay: a queued ${crud.entity} write ("${crud.key}") could not be applied locally`,
			err,
		);
	}
}

/**
 * Applies one local mutation to the optimistic overlay of `workspaceId`. Used
 * both when the user mutates (immediate optimism) and when pending mutations
 * replay during rebase. Records what it touched, at row granularity (see
 * TouchSet).
 *
 * A replayed crud naming an entity this build does not register is skipped, as is
 * one it cannot apply — see replayCrud for why a shared queue makes both
 * ordinary. Neither skip covers a locally issued crud: that one reaches
 * applyCrudLocally directly, where an unregistered entity or an unapplicable mask
 * is a programming error and throws.
 */
export async function applyMutationLocally(
	tx: KVWrite,
	registry: EntityRegistry,
	mirrors: MirrorMap,
	workspaceId: string,
	mutation: Mutation,
	touched: TouchSet,
): Promise<void> {
	switch (mutation.payload.case) {
		case 'crud': {
			const crud = mutation.payload.value;
			if (!registry.get(crud.entity)) return;
			await replayCrud(tx, registry, workspaceId, crud, touched, {
				mutationId: mutation.mutationId,
				index: 0,
			});
			return;
		}
		case 'batch': {
			// Apply sub-mutations in order against the evolving overlay so a later
			// op sees earlier ones (e.g. PATCH the row a preceding PUT created),
			// mirroring the server's in-savepoint sequential apply. The index counts
			// skipped sub-mutations too, so the text-log slots a batch writes are the
			// same wherever it replays.
			let index = -1;
			for (const crud of mutation.payload.value.mutations) {
				index++;
				if (!registry.get(crud.entity)) continue;
				await replayCrud(tx, registry, workspaceId, crud, touched, {
					mutationId: mutation.mutationId,
					index,
				});
			}
			return;
		}
		case 'custom': {
			const custom = mutation.payload.value;
			const mirror = mirrors[custom.name];
			if (!mirror) return; // unmirrored custom mutations are optimistic no-ops
			const args = mirror.args && custom.args ? anyUnpack(custom.args, mirror.args) : undefined;
			await mirror.apply(new KVMirrorTx(tx, registry, workspaceId, touched), args);
			return;
		}
		default:
			throw new Error('mutation has no payload');
	}
}

/**
 * Wipes one workspace's server-confirmed state (bootstrap / reset path).
 * Deletes its cursor FIRST: if the process dies mid-wipe or mid-bootstrap, a
 * missing cursor forces a clean re-bootstrap on the next start. The optimistic
 * overlay and pending queue are untouched, so local data still shadows reads
 * while the snapshot streams in. Other workspaces in the store are not
 * disturbed.
 *
 * It sweeps every entity the STORE syncs, not just the ones this build
 * registered: another build's tab may have filled rows this one cannot read, and
 * a wipe that left them behind would hand them to whichever build registers that
 * entity next — arbitrarily stale, and possibly deleted server-side past the
 * tombstone horizon. See entities.ts.
 */
export async function wipeConfirmedState(
	tx: KVWrite,
	registry: EntityRegistry,
	workspaceId: string,
	touched: TouchSet,
): Promise<void> {
	await tx.del(metaCursor(workspaceId));
	// The summary is what the cursor's lineage was told; the snapshot that
	// replaces it brings its own. Announced like the write that stored it, so
	// access listeners stop answering from it now.
	await tx.del(metaCallerAccess(workspaceId));
	await tx.del(metaCallerAccessVersion(workspaceId));
	touched.entity(workspaceId, ACCESS_TOUCH_ENTITY);
	// The acked floor goes with the cursor: it names a seq in the lineage being
	// discarded, and carried across a swap it could sit above the new lineage's
	// head and pin every pull to the primary.
	await tx.del(metaAckedSeq(workspaceId));
	// The built markers say the index covers the whole entity, which stops being
	// true the moment the rows are gone, so they leave in the same transaction.
	// The value positions need no sweep of their own — they ride on the rows
	// below — but the search records are keys beside them and do.
	await deletePrefixes(tx, [ metaIndexPrefix(workspaceId) ]);
	for (const entity of await syncableEntities(tx, registry)) {
		for (
			const prefix of [
				confirmedWsPrefix(entity, workspaceId),
				searchIndexWsPrefix(entity, workspaceId),
			]
		) {
			for await (const [ k ] of tx.scan(prefix)) {
				await tx.del(k);
			}
		}
		touched.entity(workspaceId, entity);
	}
}

/**
 * Deletes one workspace's confirmed rows of the given entities. The server
 * sends Patch.drop_entities when an entity leaves the subscription set during a
 * delta resync — one name instead of a tombstone per row. The optimistic
 * overlay is untouched (finalizeRebase rebuilds it afterwards).
 */
export async function dropConfirmedEntities(
	tx: KVWrite,
	workspaceId: string,
	entities: string[],
	touched: TouchSet,
	indexes?: IndexCache,
): Promise<void> {
	for (const entity of entities) {
		for (
			const prefix of [
				confirmedWsPrefix(entity, workspaceId),
				searchIndexWsPrefix(entity, workspaceId),
			]
		) {
			for await (const [ k ] of tx.scan(prefix)) {
				await tx.del(k);
			}
		}
		// Every marker of this entity goes, including ones naming a spec that
		// has since changed — then the current specs are re-claimed, which they
		// have earned: an index over an entity holding no rows is complete, and
		// the rows that refill it are indexed as they land.
		await deletePrefixes(tx, [ metaIndexEntityPrefix(workspaceId, entity) ]);
		if (indexes) await markIndexesBuilt(tx, indexes, workspaceId, [ entity ]);
		touched.entity(workspaceId, entity);
	}
}

/**
 * The same bytes owning their own buffer, copying only when they do not.
 *
 * Wire messages decoded from binary hand back `bytes` fields as SUBARRAY VIEWS
 * over the whole response buffer — protobuf-es does not copy on read — and
 * structured clone, which is how a value reaches IndexedDB, serializes a view's
 * entire backing ArrayBuffer rather than the window. Storing a row straight off
 * the wire therefore costs the size of the CHUNK it arrived in (a bootstrap
 * chunk is 256 rows) and pins that chunk on the heap for as long as any row of
 * it is held. Measured on the bench corpus, that was ~66x the storage and ~19x
 * the heap, and it put a 16x multiplier on bootstrap's staging phase.
 *
 * Only the binary codec is affected: the JSON one base64-decodes each field
 * into an exact-size array, so the aliasing appeared when the browser transport
 * stopped defaulting to JSON (SyncClientOptions.wireFormat).
 */
function detached(bytes: Uint8Array): Uint8Array {
	const ownsBuffer = bytes.byteOffset === 0 && bytes.byteLength === bytes.buffer.byteLength;
	return ownsBuffer ? bytes : bytes.slice();
}

/** The confirmed record of a value row change whose row bytes are `value`. */
function confirmedRecordOf(change: RowChange, value: Uint8Array): ConfirmedRecord {
	const record: ConfirmedRecord = { v: value, ver: change.version.toString() };
	if (change.hidden.length > 0) record.h = [ ...change.hidden ];
	return record;
}

/**
 * Applies a batch of one workspace's server row changes to the confirmed state.
 *
 * Collaborative-text fields may arrive as delta state (RowChange.text_deltas);
 * they are folded back into whole documents here, so everything downstream of
 * confirmed storage — reads, watches, rebase replay — keeps seeing full rows.
 * Changes apply in order, so a delta cut against a row an earlier change in the
 * same page rewrote still resolves against the right base.
 *
 * Changes for an entity the store does not sync are dropped: a full-sync client
 * is sent every entity the SERVER holds, and nothing here can decode, index or
 * read one this store has no schema for. See entities.ts.
 */
export async function applyServerChanges(
	tx: KVWrite,
	registry: EntityRegistry,
	workspaceId: string,
	changes: RowChange[],
	touched: TouchSet,
	indexes?: IndexCache,
): Promise<void> {
	const syncable = await syncableEntities(tx, registry);
	const storedSearch = await readSearchPositions(tx, syncable, workspaceId, changes, indexes);
	for (const change of changes) {
		if (!syncable.has(change.entity)) continue;
		touched.row(workspaceId, change.entity, change.key);
		const cKey = confirmedKey(change.entity, workspaceId, change.key);
		const idx = indexes?.for(change.entity) ?? [];
		const search = searchIndexOf(idx);
		if (change.body.case === 'tombstone') {
			// The value positions are a field on the row, so the delete takes them
			// with it — but the search record is a key of its own, and one left
			// behind would keep answering term probes for a row that is gone.
			// Deleted whether or not this build compiles a search index: another
			// build's tab may have written it.
			const xKey = searchIndexKey(change.entity, workspaceId, change.key);
			await tx.del(cKey);
			await tx.del(xKey);
			storedSearch.delete(xKey);
		} else if (change.body.case === 'value') {
			const value = detached(
				await applyTextDeltas(tx, registry, workspaceId, change, change.body.value),
			);
			let record = confirmedRecordOf(change, value);
			if (idx.length > 0) {
				// Where the row USED to sit does not matter — the position list is
				// replaced, not amended — so this needs neither the bytes being
				// overwritten nor a read to find them.
				const def = registry.get(change.entity)!;
				const decoded = fromBinary(def.schema, value);
				record = indexedRecord(
					record,
					idx,
					change.entity,
					workspaceId,
					change.key,
					decoded,
					def.schema,
				);
				if (search !== undefined) {
					const xKey = searchIndexKey(change.entity, workspaceId, change.key);
					const positions = searchPositions(
						search,
						change.entity,
						workspaceId,
						change.key,
						decoded,
						def.schema,
					);
					// Most updates leave the searchable text alone, and a put rewrites
					// every position on the record — so the comparison buys back the
					// whole term set on a change to a state, an assignee or a timestamp.
					if (!samePositions(positions, storedSearch.get(xKey) ?? [])) {
						await writeSearchRecord(tx, change.entity, workspaceId, change.key, positions);
						if (positions.length === 0) storedSearch.delete(xKey);
						else storedSearch.set(xKey, positions);
					}
				}
			}
			await tx.put(cKey, record);
		}
	}
}

/**
 * The search positions currently stored for the rows `changes` rewrites, keyed
 * by search-index key; a row with no record is absent.
 *
 * One pipelined getMany for the whole batch rather than a read per row: a pull
 * applies hundreds of changes, and an awaited get ahead of each put would cost
 * that many serialized round trips against the store.
 */
async function readSearchPositions(
	tx: KVWrite,
	syncable: ReadonlySet<string>,
	workspaceId: string,
	changes: readonly RowChange[],
	indexes: IndexCache | undefined,
): Promise<Map<string, string[]>> {
	const stored = new Map<string, string[]>();
	if (indexes === undefined) return stored;
	const keys: string[] = [];
	for (const change of changes) {
		if (change.body.case !== 'value' || !syncable.has(change.entity)) continue;
		if (searchIndexOf(indexes.for(change.entity)) === undefined) continue;
		keys.push(searchIndexKey(change.entity, workspaceId, change.key));
	}
	if (keys.length === 0) return stored;
	const records = await tx.getMany(keys);
	for (let i = 0; i < keys.length; i++) {
		const record = records[i] as SearchIndexRecord | undefined;
		if (record !== undefined) stored.set(keys[i]!, record.ix);
	}
	return stored;
}

/**
 * Writes one batch of incoming bootstrap rows, each at the key `keyFor` gives
 * it. Shared by the two destinations a snapshot streams into — staging, or
 * confirmed state directly — which differ in nothing but that key; see
 * stageServerChanges and applyBootstrapRows for which is used when.
 *
 * A snapshot carries only value rows; a stray tombstone is tolerated as an
 * absence.
 *
 * `index` decides whether the rows carry their secondary-index positions, which
 * costs a decode per row of an indexed entity and a second put per row of a
 * SEARCHABLE one (its `x/` record); without it a chunk costs one put per row
 * and no schema work at all. It also names the workspace the `x/` keys belong
 * to, so only a destination that is confirmed state may pass it. The two
 * destinations differ here — see promoteStaged and applyBootstrapRows.
 *
 * A bootstrap defines the client's state rather than advancing it, so there is
 * nothing for delta state to be relative to; a server that sent some is
 * malfunctioning, and the resulting rows would be missing their documents.
 *
 * Rows for an entity the store does not sync are skipped, exactly as they are on
 * the pull path (see applyServerChanges). `entities` is the set that decides
 * that; omitted, it is read from `tx`.
 */
async function writeSnapshotRows(
	tx: KVWrite,
	registry: EntityRegistry,
	changes: RowChange[],
	keyFor: (entity: string, key: string) => string,
	onEntity?: (entity: string) => void,
	index?: { indexes: IndexCache; workspaceId: string },
	entities?: ReadonlySet<string>,
): Promise<void> {
	const syncable = entities ?? await syncableEntities(tx, registry);
	for (const change of changes) {
		if (!syncable.has(change.entity)) continue;
		if (change.textDeltas.length > 0) {
			throw new TextDeltaUnusableError(
				change.entity,
				change.key,
				change.textDeltas[0]!.field,
				new Error('bootstrap rows must carry full state'),
			);
		}
		const k = keyFor(change.entity, change.key);
		if (change.body.case === 'tombstone') {
			await tx.del(k);
			if (index !== undefined) {
				await tx.del(searchIndexKey(change.entity, index.workspaceId, change.key));
			}
		} else if (change.body.case === 'value') {
			const value = detached(change.body.value);
			let record = confirmedRecordOf(change, value);
			const compiled = index?.indexes.for(change.entity) ?? [];
			if (compiled.length > 0) {
				const def = registry.get(change.entity)!;
				const decoded = fromBinary(def.schema, value);
				record = indexedRecord(
					record,
					compiled,
					change.entity,
					index!.workspaceId,
					change.key,
					decoded,
					def.schema,
				);
				const search = searchIndexOf(compiled);
				if (search !== undefined) {
					// Written without reading what is there: a snapshot defines the
					// workspace rather than advancing it, so there is nothing a
					// comparison could spare.
					await writeSearchRecord(
						tx,
						change.entity,
						index!.workspaceId,
						change.key,
						searchPositions(
							search,
							change.entity,
							index!.workspaceId,
							change.key,
							decoded,
							def.schema,
						),
					);
				}
			}
			await tx.put(k, record);
		}
		onEntity?.(change.entity);
	}
}

/**
 * Stages a batch of incoming bootstrap rows under one workspace's generation
 * `gen`. Writes land in the b/ keyspace, which reads never resolve, so
 * streaming a snapshot never disturbs the live merged view — the swap
 * (promoteStaged) makes it visible atomically.
 *
 * This is the RE-bootstrap path, where the workspace already holds a snapshot
 * that must stay whole until the new one is complete. A cold bootstrap has no
 * such state and writes through applyBootstrapRows instead.
 *
 * Staged rows carry no index positions. The store's index spans every record it
 * holds, so a `b/` row with positions would be answering queries out of a
 * snapshot that is not installed yet.
 *
 * `entities` is the syncable set the rows are filtered against (see
 * syncableEntities); omitted, every call reads it back from `tx`.
 */
export async function stageServerChanges(
	tx: KVWrite,
	registry: EntityRegistry,
	workspaceId: string,
	gen: number,
	changes: RowChange[],
	entities?: ReadonlySet<string>,
): Promise<void> {
	await writeSnapshotRows(
		tx,
		registry,
		changes,
		(entity, key) => stagingKey(workspaceId, gen, entity, key),
		undefined,
		undefined,
		entities,
	);
}

/**
 * Writes a batch of incoming bootstrap rows straight into confirmed state,
 * making each visible as it lands.
 *
 * Only legal where there is no installed snapshot to protect — a workspace with
 * no cursor — because it gives up the staged path's atomicity: a reader mid-
 * stream sees part of the new snapshot rather than all of the old one. On a cold
 * workspace that is not a downgrade, since the alternative it would be showing
 * is an empty keyspace, and it saves the second put and the delete that staging
 * costs every row of the slowest, most visible case there is. See
 * Coordinator.doBootstrap for the conditions and for how two tabs are kept from
 * both writing here.
 *
 * Rows of an entity `indexes` compiles an index for are written carrying their
 * positions, which costs a decode per row and is what lets the install claim the
 * index-built markers instead of leaving the workspace to a background walk (see
 * Coordinator.doBootstrap and index-build.ts). Chunks commit one transaction
 * each and outside the store mutex, so the decode is off the swap's critical
 * path. Omitting `indexes`, or an entity that compiles none, keeps the row at one
 * put and no schema work.
 *
 * Reports whole entities rather than rows: a snapshot replaces the entity
 * outright, so a watcher has to re-scan it either way, and a keyed TouchSet
 * would carry one token per bootstrapped row for nothing.
 *
 * `entities` is the syncable set the rows are filtered against (see
 * syncableEntities); omitted, every call reads it back from `tx`.
 */
export async function applyBootstrapRows(
	tx: KVWrite,
	registry: EntityRegistry,
	workspaceId: string,
	changes: RowChange[],
	touched: TouchSet,
	indexes?: IndexCache,
	entities?: ReadonlySet<string>,
): Promise<void> {
	await writeSnapshotRows(
		tx,
		registry,
		changes,
		(entity, key) => confirmedKey(entity, workspaceId, key),
		(entity) => touched.entity(workspaceId, entity),
		indexes === undefined ? undefined : { indexes, workspaceId },
		entities,
	);
}

/**
 * Promotes a generation's staged rows into confirmed state, deleting the
 * staged copies as it goes. Run inside the swap transaction, after
 * wipeConfirmedState: the staged ConfirmedRecord is copied verbatim to its
 * c/ key. A swap replaces a whole workspace, so it reports whole entities.
 *
 * The staged record moves across VERBATIM, which is what keeps promotion at one
 * put and one delete per row with no schema work at all — and it is why the
 * promoted rows carry no index positions and get no `x/` record: a staged row
 * has none, and giving it some here would mean decoding every row of the
 * snapshot inside the one transaction the swap must commit before anything is
 * readable. The preceding wipeConfirmedState has already removed the search
 * records of the snapshot being replaced, so none is left over. The caller
 * leaves the indexes unbuilt instead — the state a query already handles by
 * scanning — and asks for a background build. See Coordinator.doBootstrap and
 * index-build.ts.
 */
export async function promoteStaged(
	tx: KVWrite,
	workspaceId: string,
	gen: number,
	touched: TouchSet,
): Promise<void> {
	const prefix = stagingPrefix(workspaceId, gen);
	// Collect before mutating: iterating a prefix while writing into it (even a
	// disjoint c/ key) is needlessly subtle across the KV backends.
	const staged: [string, unknown][] = [];
	for await (const entry of tx.scan(prefix)) staged.push(entry);
	for (const [ k, v ] of staged) {
		const suffix = k.slice(prefix.length); // "<entity>/<key>"
		const slash = suffix.indexOf('/');
		const entity = suffix.slice(0, slash);
		const key = suffix.slice(slash + 1);
		await tx.put(confirmedKey(entity, workspaceId, key), v);
		await tx.del(k);
		touched.entity(workspaceId, entity);
	}
}

/**
 * Deletes every staged row under `prefix` — a single generation
 * (stagingPrefix(ws, gen), when discarding a superseded bootstrap) or a
 * workspace's whole staging root (stagingWsRoot(ws), garbage-collecting its
 * generations at a successful swap, which sweeps up rows left by crashed or
 * superseded bootstraps of that workspace).
 */
export async function clearStaging(tx: KVWrite, prefix: string): Promise<void> {
	const keys: string[] = [];
	for await (const [ k ] of tx.scan(prefix)) keys.push(k);
	for (const k of keys) await tx.del(k);
}

export interface FinalizeInput {
	/**
	 * Position to store. Empty leaves the stored cursor alone — a Subscribe
	 * frame the server did not checkpoint. Rows still apply, and `serverSeq`
	 * still advances, so the gate keeps working against re-delivery.
	 */
	nextCursor: Uint8Array;
	lastAckedMutationId: bigint;
	/**
	 * Workspace seq the cursor encodes (PullResponse.seq / Begin.seq). When
	 * nonzero it is persisted alongside the cursor, feeding the monotonic gate in
	 * rebase(). 0/unset (an older server) writes nothing.
	 */
	serverSeq?: bigint;
	/**
	 * Set on a bootstrap swap, where the snapshot defines a fresh baseline that
	 * is authoritative even moving the seq BACKWARDS (a recreated, restored, or
	 * swapped store). Unlike a pull, the baseline is written even when serverSeq
	 * is 0 — a swap to an empty store must clear a stale seq, or the monotonic
	 * gate keeps dropping every page once the new store starts writing.
	 */
	baseline?: boolean;
	/**
	 * When true, keep ALL pending mutations queued instead of dropping the acked
	 * ones. Used on non-final pages of a paged pull: a dropped mutation's
	 * confirmed server change may not land until a later page, so dropping it now
	 * would transiently regress its row mid-drain. The drop runs on the final
	 * page (`!has_more`); replaying still-queued mutations on top of their own
	 * confirmed writes is idempotent, so holding them a few pages longer is safe.
	 *
	 * Also used, by a pull or a bootstrap swap, while a push of the workspace is
	 * unanswered: the server's watermark then covers mutations whose verdicts
	 * only a re-push can collect, and dropping a rejected one would lose its
	 * rejection.
	 */
	deferAcked?: boolean;
	/**
	 * What the caller holds in the workspace, when the server sent it with this
	 * position, encoded by encodeAccess; the transaction stores its `raw`.
	 * Absent leaves the stored summary alone, except on a baseline, which
	 * removes it: a snapshot without one is of a schema without the access
	 * model.
	 */
	callerAccess?: DecodedAccess;
}

/**
 * Rebuilds one workspace's optimistic overlay: clears it, then replays the
 * given pending mutations on top of whatever confirmed state the transaction
 * sees. Only that workspace's overlay is cleared, so a rebase of one partition
 * never disturbs another's optimistic state.
 *
 * The clear is scoped to THIS build's registry, which is the pairing for
 * replay's skip of mutations naming entities it does not register (see
 * applyMutationLocally): a mixed-build pair of tabs leaves the other's
 * optimistic state alone in both directions. Clearing another build's overlay
 * rows here would drop state the tab that can read them still needs and this
 * one cannot rebuild, since it does not replay the mutations behind them
 * either — those stay queued until that tab rebases.
 *
 * Replay is isolated per mutation: an entry this build cannot apply is skipped
 * and reported over console.error, and no failure to replay one reaches the
 * caller's transaction. See replayCrud for why a shared queue makes such an entry
 * ordinary, and what retires it.
 *
 * Reports ROWS: every overlay row it clears and every row the replay writes,
 * each by key, so a rebase costs a watcher the rows that moved rather than a
 * scan of the entity (see TouchSet).
 */
export async function clearOverlayAndReplay(
	tx: KVWrite,
	registry: EntityRegistry,
	mirrors: MirrorMap,
	workspaceId: string,
	remaining: Mutation[],
	touched: TouchSet,
): Promise<void> {
	for (const entity of registry.names()) {
		// The scan has each cleared row's key in hand, so this reports rows rather
		// than the entity: a wildcard here would put every watcher of the entity
		// back on a full scan of its scope, once per rebase (see WatcherHub).
		const rows = overlayWsPrefix(entity, workspaceId);
		for await (const [ k ] of tx.scan(rows)) {
			await tx.del(k);
			touched.row(workspaceId, entity, k.slice(rows.length));
		}
		// The text log is overlay state under a different key, so it clears with
		// the overlay; the replay below rebuilds it from the same queue entries the
		// fragments came from in the first place.
		const log = overlayTextPrefix(entity);
		for await (const [ k ] of tx.scan(overlayTextWsPrefix(entity, workspaceId))) {
			await tx.del(k);
			// The ROW the fragment belongs to: one row logs a fragment per edit per
			// field, and TouchSet folds the repeats. The fallback is a fence, not a
			// path — an unnamed row stays stale in every maintained result, so a key
			// that will not split gives up the whole entity instead.
			const parts = splitOverlayTextSuffix(k.slice(log.length));
			if (parts === undefined) touched.entity(workspaceId, entity);
			else touched.row(workspaceId, entity, parts.key);
		}
	}
	for (const mutation of remaining) {
		try {
			await applyMutationLocally(tx, registry, mirrors, workspaceId, mutation, touched);
		} catch (err) {
			// The backstop for what replayCrud does not cover: a custom mutation's
			// mirror is app code running inside this transaction, and its args unpack
			// against a type this build may not have.
			reportError(
				`replay: queued mutation ${mutation.mutationId} could not be applied locally`,
				err,
			);
		}
	}
}

/**
 * Finishes a rebase of one workspace after its server changes are in:
 *  1. persist the new cursor
 *  2. drop pending mutations the server has acked (unless deferAcked)
 *  3. clear the optimistic overlay
 *  4. replay surviving pending mutations on the new base
 *
 * Returns the number of pending mutations that survived.
 */
export async function finalizeRebase(
	tx: KVWrite,
	registry: EntityRegistry,
	mirrors: MirrorMap,
	workspaceId: string,
	input: FinalizeInput,
	touched: TouchSet,
): Promise<number> {
	// An empty cursor is a Subscribe frame the server chose not to checkpoint
	// (SubscribeRequest.sparse_cursor), not a position: the stored one stays,
	// and a reopened stream re-delivers whatever came after it. Storing the
	// empty value instead would read back as "never synced" and re-bootstrap.
	//
	// A cursor viewing the whole page it arrived on would keep a 4096-change
	// page resident until the next one replaced it, hence detached().
	if (input.nextCursor.length > 0) {
		await tx.put(metaCursor(workspaceId), detached(input.nextCursor));
	}
	// Unconditional (not monotonic) on purpose: pulls already gated in rebase(),
	// and a bootstrap swap must be able to move the seq BACKWARDS — a full
	// snapshot is authoritative even against a server restored from backup. A
	// bootstrap writes the baseline even when it is 0 (an empty store), so a
	// stale gate value cannot survive the swap; a pull with no seq (0/unset, an
	// older server) writes nothing.
	if (input.baseline) {
		await tx.put(metaServerSeq(workspaceId), (input.serverSeq ?? 0n).toString());
		// A snapshot swap replaces the cursor lineage, and the acked floor names
		// a seq in the old one; a floor above the new head would pin every pull
		// to the primary. wipeConfirmedState already dropped it on the direct
		// path — this covers the staged swap, whose wipe and install commit here.
		await tx.del(metaAckedSeq(workspaceId));
	} else if (input.serverSeq) {
		await tx.put(metaServerSeq(workspaceId), input.serverSeq.toString());
	}
	if (input.callerAccess !== undefined) {
		await tx.put(metaCallerAccess(workspaceId), input.callerAccess.raw);
		if (input.callerAccess.version === 0n) {
			await tx.del(metaCallerAccessVersion(workspaceId));
		} else {
			await tx.put(metaCallerAccessVersion(workspaceId), input.callerAccess.version.toString());
		}
		// Announced like a row change, so it reaches this tab's access
		// listeners and every other tab's on the same channel (see
		// SyncClient.onAccessChange).
		touched.entity(workspaceId, ACCESS_TOUCH_ENTITY);
	} else if (input.baseline) {
		await tx.del(metaCallerAccess(workspaceId));
		await tx.del(metaCallerAccessVersion(workspaceId));
		touched.entity(workspaceId, ACCESS_TOUCH_ENTITY);
	}

	let remaining: Mutation[];
	if (input.deferAcked) {
		remaining = await readPending(tx, workspaceId);
	} else {
		const acked = await dropAcked(tx, workspaceId, input.lastAckedMutationId);
		touchPendingAccess(touched, registry, workspaceId, acked.dropped);
		if (acked.dropped.length > 0) {
			await recountPendingAccess(tx, registry, workspaceId, acked.remaining);
		}
		remaining = acked.remaining;
	}
	await clearOverlayAndReplay(tx, registry, mirrors, workspaceId, remaining, touched);
	return remaining.length;
}

/**
 * Immediately rolls rejected mutations back without waiting for a pull: one
 * KV transaction drops the rejected pending entries of `workspaceId` and
 * replays the rest on the current confirmed state. Run under the kv mutex.
 */
export async function rollbackRejected(
	kv: KVStore,
	registry: EntityRegistry,
	mirrors: MirrorMap,
	workspaceId: string,
	rejectedIds: bigint[],
): Promise<RebaseResult> {
	const rejected = new Set(rejectedIds);
	const touched = new TouchSet();
	const tx = await kv.write();
	try {
		const remaining: Mutation[] = [];
		const dropped: Mutation[] = [];
		for (const m of await readPending(tx, workspaceId)) {
			if (rejected.has(m.mutationId)) {
				await tx.del(pendingKey(workspaceId, m.mutationId));
				dropped.push(m);
			} else {
				remaining.push(m);
			}
		}
		touchPendingAccess(touched, registry, workspaceId, dropped);
		if (dropped.length > 0) await recountPendingAccess(tx, registry, workspaceId, remaining);
		await clearOverlayAndReplay(tx, registry, mirrors, workspaceId, remaining, touched);
		await tx.commit();
		return { touched, pendingCount: remaining.length };
	} catch (err) {
		tx.release();
		throw err;
	}
}

export interface RebaseInput {
	changes: RowChange[];
	/** Entities whose confirmed state drops wholesale, before changes apply. */
	dropEntities?: string[];
	nextCursor: Uint8Array;
	lastAckedMutationId: bigint;
	/** Defer the acked-mutation drop to the final page of a paged pull (see
	 * FinalizeInput.deferAcked). */
	deferAcked?: boolean;
	/**
	 * The page's workspace seq (PullResponse.seq), gating application: a page
	 * at or below the workspace's stored seq is dropped whole. 0/unset applies
	 * ungated (older server).
	 */
	serverSeq?: bigint;
	/** The page's CallerAccess (PullResponse.caller_access); see FinalizeInput. */
	callerAccess?: DecodedAccess;
	/**
	 * The page's change to the stored CallerAccess
	 * (PullResponse.caller_access_delta), in place of `callerAccess`.
	 */
	callerAccessDelta?: CallerAccessDelta;
	/**
	 * `callerAccessDelta` applied to the summary whose version is its
	 * `baseVersion`, made before the transaction. Stored when that summary
	 * is still the stored one; without it, the transaction applies the delta
	 * itself.
	 */
	callerAccessApplied?: DecodedAccess;
}

export interface RebaseResult {
	/** What changed — rows where known, whole entities where not. */
	touched: TouchSet;
	/** Pending mutations still queued after dropping acked ones. */
	pendingCount: number;
	/**
	 * True when the monotonic gate dropped the page: nothing was written, so
	 * `touched` is empty and `pendingCount` is meaningless. See rebase().
	 */
	skipped?: boolean;
	/**
	 * The CallerAccess the transaction stored: the page's own, or the stored
	 * one with the page's delta applied. Absent when it stored none.
	 */
	callerAccess?: DecodedAccess;
	/**
	 * Set when the page's CallerAccessDelta applies to another summary than
	 * the stored one. The stored summary stays as it was, and a pull, which
	 * names the version it holds, gets the change from there.
	 */
	accessStale?: boolean;
}

/**
 * The rebase algorithm for one workspace — applyServerChanges + finalizeRebase
 * in a single KV transaction. Used by pull; bootstrap streams chunks across
 * transactions instead (see Coordinator.doBootstrap).
 */
export async function rebase(
	kv: KVStore,
	registry: EntityRegistry,
	mirrors: MirrorMap,
	workspaceId: string,
	input: RebaseInput,
	indexes?: IndexCache,
): Promise<RebaseResult> {
	const touched = new TouchSet();
	const tx = await kv.write();
	try {
		// Monotonic cursor gate: with several tabs pulling the shared store
		// concurrently, responses can commit out of request order. A page whose
		// seq is strictly below what the store already pulled through for this
		// workspace is from a stale read of the shared cursor — its patches AND
		// its cursor are dropped whole, so it can never overwrite newer confirmed
		// rows or regress the cursor. (This replaces the previous undocumented
		// reliance on redundant re-pulls being idempotent.) An EQUAL seq must
		// apply: the server legitimately re-delivers under an unchanged seq when
		// only the scope changed (a setSubscriptions delta-resync) or when a drain
		// pages through rows sharing one seq — such pages are merged snapshots
		// current through that same seq, so applying them regresses nothing. Seq
		// 0/unset applies ungated: an older server sends no seq, and wrongly
		// applying is merely the old redundant-but-idempotent behavior.
		if (input.serverSeq) {
			const stored = (await tx.get(metaServerSeq(workspaceId))) as string | undefined;
			if (stored !== undefined && input.serverSeq < BigInt(stored)) {
				tx.release();
				return { touched, pendingCount: 0, skipped: true };
			}
		}
		await dropConfirmedEntities(tx, workspaceId, input.dropEntities ?? [], touched, indexes);
		await applyServerChanges(tx, registry, workspaceId, input.changes, touched, indexes);
		let callerAccess = input.callerAccess;
		let accessStale = false;
		if (input.callerAccessDelta !== undefined) {
			const applied = await applyStoredAccessDelta(
				tx,
				workspaceId,
				input.callerAccessDelta,
				input.callerAccessApplied,
			);
			callerAccess = applied.access;
			accessStale = applied.stale;
		}
		const pendingCount = await finalizeRebase(
			tx,
			registry,
			mirrors,
			workspaceId,
			callerAccess === input.callerAccess ? input : { ...input, callerAccess },
			touched,
		);
		await tx.commit();
		return {
			touched,
			pendingCount,
			...(callerAccess === undefined ? {} : { callerAccess }),
			...(accessStale ? { accessStale } : {}),
		};
	} catch (err) {
		tx.release();
		throw err;
	}
}

/**
 * Applies `delta` to the CallerAccess stored for `workspaceId`. `access` is
 * the summary for finalizeRebase to store, absent when it stores none.
 * `applied`, when given, is `delta` applied to the summary whose version is
 * `delta.baseVersion`.
 *
 * A delta for another summary is not applied. It changes nothing when the
 * stored summary is already the one the delta gives, which happens when a
 * stream page and another tab's pull both carry the same change. Otherwise
 * `stale` is set and the stored summary stays: it is older than the server's
 * but still the best prediction there is, and the next pull names its
 * version.
 */
async function applyStoredAccessDelta(
	tx: KVWrite,
	workspaceId: string,
	delta: CallerAccessDelta,
	applied: DecodedAccess | undefined,
): Promise<{ access?: DecodedAccess; stale: boolean }> {
	const stored = (await tx.get(metaCallerAccessVersion(workspaceId))) as string | undefined;
	const version = stored === undefined ? 0n : BigInt(stored);
	if (version === delta.version) return { stale: false };
	if (version === 0n || version !== delta.baseVersion) return { stale: true };
	// Equal versions mean equal summaries, so a delta applied before the
	// transaction to a summary of this version is the result here too.
	if (applied !== undefined) return { access: applied, stale: false };
	const raw = await storedAccessAt(tx, workspaceId, delta.baseVersion);
	if (raw === undefined) return { stale: true };
	return { access: withAccessDelta(raw, delta), stale: false };
}

/**
 * The encoded CallerAccess stored for `workspaceId` when its version is
 * `version`. Undefined for a summary of any other version, and when no
 * summary is stored.
 */
export async function storedAccessAt(
	tx: KVRead,
	workspaceId: string,
	version: bigint,
): Promise<Uint8Array | undefined> {
	const [ stored, raw ] = await tx.getMany([
		metaCallerAccessVersion(workspaceId),
		metaCallerAccess(workspaceId),
	]) as [string | undefined, Uint8Array | undefined];
	return stored === version.toString() ? raw : undefined;
}

/**
 * `delta` applied to the CallerAccess encoded as `raw`, as CallerAccessDelta
 * documents it. The caller checks that `raw` is the summary whose version is
 * `delta.baseVersion`.
 */
export function withAccessDelta(raw: Uint8Array, delta: CallerAccessDelta): DecodedAccess {
	return encodeAccess(applyAccessDelta(fromBinary(CallerAccessSchema, raw), delta));
}

/** Outcome of resequencePending. */
export interface ResequenceResult extends RebaseResult {
	/** Old mutation id → new id, for remapping undo bookkeeping. */
	remap: ReadonlyMap<bigint, bigint>;
	/** False when the queue already started at the requested id. */
	changed: boolean;
}

/**
 * Renumbers a workspace's pending queue to start at `firstId`, closing a hole
 * below it. Order and payloads are preserved; only ids move.
 *
 * Mutation ids must reach the server contiguously from its per-(workspace,
 * client) watermark, and only an ack can retire one. A queue whose head sits
 * ABOVE a hole is therefore unreachable on its own: every push is refused for
 * the gap, and no ack can close a hole the server never learned about. This is
 * the way back — but only for a caller that knows the server has taken nothing
 * from `firstId` on, since renumbering an id the server already applied would
 * re-send it under a new one. A refused batch applies nothing, so the refusal
 * itself is that proof. The pushed-through watermark comes back down to
 * `firstId - 1` with the queue for the same reason: those ids were never taken,
 * and leaving it high would freeze compaction out of the whole queue.
 *
 * The overlay and text log are rebuilt from the renumbered queue, because both
 * record the id that wrote them. `changed` reports whether any mutation moved:
 * it is false for a queue already starting at `firstId`, for one the server is
 * ahead of (dropAcked handles that), and for an empty queue — which still
 * writes both marks back down, since the counter is what the next local write
 * mints from.
 *
 * Run under the kv mutex.
 */
export async function resequencePending(
	kv: KVStore,
	registry: EntityRegistry,
	mirrors: MirrorMap,
	workspaceId: string,
	firstId: bigint,
): Promise<ResequenceResult> {
	return runResequence(kv, registry, mirrors, workspaceId, undefined, firstId);
}

/** Outcome of dropAndResequencePending. */
export interface DropResult extends ResequenceResult {
	/**
	 * False when the queue no longer held the mutation as given — a peer tab
	 * acked, dropped or renumbered it first. Nothing was written then.
	 */
	dropped: boolean;
}

/**
 * Drops the pending mutation `drop` of a workspace and renumbers what survives
 * it down onto its id, in ONE transaction. Everything resequencePending
 * documents applies, and the caller owes the same proof: the server has taken
 * nothing from that id on.
 *
 * The queue is shared by every tab, so the id alone may name another mutation
 * by now. The drop applies only while the queue holds `drop` itself at its id;
 * otherwise the result reports `dropped: false` and the queue and both marks
 * stay as they were.
 *
 * The two halves are inseparable because either alone leaves the queue
 * unpushable — the drop makes a hole no ack can ever fill, and only the
 * renumber closes it. A caller that committed them separately would, on a
 * failure in between, strand the workspace with a hole whose one repair is the
 * renumber that just failed.
 *
 * `dropId` names the queue head. A queue that still holds lower ids keeps its
 * numbering (they are contiguous with the watermark already, and moving them UP
 * would open a second hole), so `changed` is false and only the drop lands.
 *
 * Run under the kv mutex.
 */
export async function dropAndResequencePending(
	kv: KVStore,
	registry: EntityRegistry,
	mirrors: MirrorMap,
	workspaceId: string,
	drop: Mutation,
): Promise<DropResult> {
	return runResequence(kv, registry, mirrors, workspaceId, drop, drop.mutationId);
}

async function runResequence(
	kv: KVStore,
	registry: EntityRegistry,
	mirrors: MirrorMap,
	workspaceId: string,
	drop: Mutation | undefined,
	firstId: bigint,
): Promise<DropResult> {
	const touched = new TouchSet();
	const tx = await kv.write();
	try {
		const pending = await readPending(tx, workspaceId);
		if (drop !== undefined) {
			const queued = pending.find((m) => m.mutationId === drop.mutationId);
			if (queued === undefined || !equals(MutationSchema, queued, drop)) {
				// Without the drop there is no hole to close, and a renumber would
				// pull the counter down onto ids a peer tab may have pushed already.
				tx.release();
				return {
					touched,
					pendingCount: pending.length,
					remap: new Map(),
					changed: false,
					dropped: false,
				};
			}
		}
		const survivors: Mutation[] = [];
		let dropped = false;
		for (const m of pending) {
			if (m.mutationId === drop?.mutationId) {
				await tx.del(pendingKey(workspaceId, m.mutationId));
				touchPendingAccess(touched, registry, workspaceId, [ m ]);
				dropped = true;
			} else {
				survivors.push(m);
			}
		}
		const head = survivors[0];
		if (head !== undefined && firstId >= head.mutationId) {
			// The server is at or ahead of the head: not a hole, and renumbering
			// upward would manufacture the very gap this closes. dropAcked retires
			// those ids instead.
			if (dropped) {
				await recountPendingAccess(tx, registry, workspaceId, survivors);
				await clearOverlayAndReplay(tx, registry, mirrors, workspaceId, survivors, touched);
				await tx.commit();
			} else {
				tx.release();
			}
			return { touched, pendingCount: survivors.length, remap: new Map(), changed: false, dropped };
		}
		// Every old key goes before any new one is written: the ranges overlap
		// whenever the queue is longer than the hole, and a write-then-delete
		// order would drop the entry it had just moved.
		const remap = new Map<bigint, bigint>();
		const renumbered: Mutation[] = [];
		let nextId = firstId;
		for (const m of survivors) {
			await tx.del(pendingKey(workspaceId, m.mutationId));
			const moved = clone(MutationSchema, m);
			moved.mutationId = nextId;
			remap.set(m.mutationId, nextId);
			renumbered.push(moved);
			nextId += 1n;
		}
		for (const m of renumbered) await enqueuePending(tx, workspaceId, m);
		// The counter follows the queue down even when the queue is empty: it is
		// what the next local write mints from, and an id above the watermark gaps
		// the workspace again on the very next edit.
		await tx.put(metaLastMutationId(workspaceId), (nextId - 1n).toString());
		await recountPendingAccess(tx, registry, workspaceId, renumbered, nextId - 1n);
		await writePushedThrough(tx, workspaceId, firstId - 1n);
		if (dropped || renumbered.length > 0) {
			await clearOverlayAndReplay(tx, registry, mirrors, workspaceId, renumbered, touched);
		}
		await tx.commit();
		return {
			touched,
			pendingCount: renumbered.length,
			remap,
			changed: renumbered.length > 0,
			dropped,
		};
	} catch (err) {
		tx.release();
		throw err;
	}
}
