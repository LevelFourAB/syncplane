// Keeps queued text edits that a new base would strand.
//
// A keystroke queues a fragment (text/fragment.ts), and the read path folds it
// onto whatever base the row holds (read.ts). A fragment names its parents by
// id, so a base that purged them — a pull or a snapshot carrying a compacted
// document — cannot take it: the fold drops it, and the only state that placed
// it was the base the rebase just overwrote. Nothing else holds the edit once a
// handle has closed. These helpers run on both sides of that overwrite, in the
// same transaction: the first folds each logged row onto the old base, the
// second turns the queued fragments of every field the new base cannot take
// into the whole folded document. The view then keeps the edit, and the push
// carries it to the server, which refuses it as `text_state_stale`; that
// recovery re-reads the row and re-applies the edit (sync/coordinator.ts).

import { fromBinary, toBinary } from '@bufbuild/protobuf';

import type { CrudMutation } from '../gen/syncplane/v1/mutation_pb.js';
import { CrudOp } from '../gen/syncplane/v1/mutation_pb.js';
import {
	confirmedKey,
	type ConfirmedRecord,
	overlayKey,
	type OverlayRecord,
	overlayTextPrefix,
	overlayTextRowPrefix,
	overlayTextWsPrefix,
	splitOverlayTextSuffix,
} from '../keys.js';
import type { KVRead, KVWrite } from '../kv/store.js';
import { type EntityRegistry, setTextFieldBytes, textFieldBytes } from '../mutations.js';
import {
	applyFragment,
	decodeFragment,
	decodeTextDoc,
	encodeTextDoc,
	isFragment,
} from '../text/index.js';
import { enqueuePending, readPending } from './queue.js';

/** One row's text log, folded onto the base it was written against. */
export interface LoggedTextRow {
	entity: string;
	key: string;
	/** Per field path: the logged fragments in fold order, and the folded document. */
	fields: Map<string, { frags: Uint8Array[]; folded: Uint8Array }>;
}

/**
 * Folds the text log of each given row onto the base it holds now, for
 * {@link keepStrandedTextEdits} to check against the base that replaces it.
 * Without `rows`, takes every logged row of the workspace. A field whose log
 * does not fold onto the current base either is left out: there is no edit in
 * it to keep.
 */
export async function captureTextLogs(
	tx: KVRead,
	registry: EntityRegistry,
	workspaceId: string,
	rows?: readonly { entity: string; key: string }[],
): Promise<LoggedTextRow[]> {
	const logs = new Map<string, { entity: string; key: string; frags: Map<string, Uint8Array[]> }>();
	const collect = async (entity: string, prefix: string, key?: string) => {
		for await (const [ k, v ] of tx.scan(prefix)) {
			const parts = splitOverlayTextSuffix(k.slice(overlayTextPrefix(entity).length));
			// The prefix of row "a" also selects the fragments of row "a/b".
			if (parts === undefined || (key !== undefined && parts.key !== key)) continue;
			const id = `${entity}\u0000${parts.key}`;
			let row = logs.get(id);
			if (row === undefined) {
				row = { entity, key: parts.key, frags: new Map() };
				logs.set(id, row);
			}
			const list = row.frags.get(parts.field);
			if (list === undefined) row.frags.set(parts.field, [ v as Uint8Array ]);
			else list.push(v as Uint8Array);
		}
	};
	if (rows === undefined) {
		for (const entity of registry.names()) {
			if (registry.textPathsOf(entity).length === 0) continue;
			await collect(entity, overlayTextWsPrefix(entity, workspaceId));
		}
	} else {
		for (const { entity, key } of rows) {
			if (registry.textPathsOf(entity).length === 0) continue;
			if (logs.has(`${entity}\u0000${key}`)) continue;
			await collect(entity, overlayTextRowPrefix(entity, workspaceId, key), key);
		}
	}
	const out: LoggedTextRow[] = [];
	for (const { entity, key, frags } of logs.values()) {
		const overlay = (await tx.get(overlayKey(entity, workspaceId, key))) as OverlayRecord | undefined;
		if (overlay?.del) continue;
		const base = overlay?.v
			?? ((await tx.get(confirmedKey(entity, workspaceId, key))) as ConfirmedRecord | undefined)?.v;
		if (base === undefined) continue;
		const fields = new Map<string, { frags: Uint8Array[]; folded: Uint8Array }>();
		for (const [ field, list ] of frags) {
			const folded = foldStrict(registry, entity, base, field, list);
			if (folded !== undefined) fields.set(field, { frags: list, folded });
		}
		if (fields.size > 0) out.push({ entity, key, fields });
	}
	return out;
}

/**
 * Rewrites the queued fragments of every captured field whose log does not
 * fold onto the row's confirmed state any more: each pending PATCH that
 * carries a fragment for that field carries the whole folded document from
 * {@link captureTextLogs} instead. Call after the confirmed rows changed and
 * before the overlay is replayed, which then folds the document in. A row the
 * new state does not hold is skipped: a PATCH cannot bring it back.
 */
export async function keepStrandedTextEdits(
	tx: KVWrite,
	registry: EntityRegistry,
	workspaceId: string,
	captured: readonly LoggedTextRow[],
): Promise<void> {
	// Per row: the folded document of each stranded field.
	const stranded = new Map<string, Map<string, Uint8Array>>();
	for (const row of captured) {
		const record = (await tx.get(confirmedKey(row.entity, workspaceId, row.key))) as
			| ConfirmedRecord
			| undefined;
		if (record === undefined) continue;
		for (const [ field, { frags, folded } ] of row.fields) {
			if (foldStrict(registry, row.entity, record.v, field, frags) !== undefined) continue;
			const id = `${row.entity}\u0000${row.key}`;
			const fields = stranded.get(id) ?? new Map<string, Uint8Array>();
			fields.set(field, folded);
			stranded.set(id, fields);
		}
	}
	if (stranded.size === 0) return;
	for (const mutation of await readPending(tx, workspaceId)) {
		const cruds = mutation.payload.case === 'crud'
			? [ mutation.payload.value ]
			: mutation.payload.case === 'batch'
			? mutation.payload.value.mutations
			: [];
		let changed = false;
		for (const crud of cruds) {
			const fields = stranded.get(`${crud.entity}\u0000${crud.key}`);
			if (fields !== undefined && replaceFragments(registry, crud, fields)) changed = true;
		}
		if (changed) await enqueuePending(tx, workspaceId, mutation);
	}
}

/**
 * Sets each named field of a PATCH that holds a fragment to the given
 * document, and reports whether it changed anything.
 */
function replaceFragments(
	registry: EntityRegistry,
	crud: CrudMutation,
	fields: ReadonlyMap<string, Uint8Array>,
): boolean {
	if (crud.op !== CrudOp.PATCH) return false;
	const def = registry.get(crud.entity);
	if (!def) return false;
	const mask = crud.fieldMask?.paths ?? [];
	let value;
	try {
		value = fromBinary(def.schema, crud.value);
	} catch {
		return false;
	}
	let changed = false;
	for (const [ field, doc ] of fields) {
		if (!mask.includes(field)) continue;
		if (!isFragment(textFieldBytes(def.schema, field, value))) continue;
		setTextFieldBytes(def.schema, field, value, doc);
		changed = true;
	}
	if (changed) crud.value = toBinary(def.schema, value);
	return changed;
}

/**
 * The fragments applied in order onto one field of `row`, or undefined when
 * one of them does not apply. Unlike the read path's fold, nothing is dropped.
 */
function foldStrict(
	registry: EntityRegistry,
	entity: string,
	row: Uint8Array,
	field: string,
	frags: readonly Uint8Array[],
): Uint8Array | undefined {
	const def = registry.get(entity);
	if (!def) return undefined;
	try {
		const doc = decodeTextDoc(
			textFieldBytes(def.schema, field, fromBinary(def.schema, row)) ?? new Uint8Array(),
			'',
		);
		for (const frag of frags) applyFragment(doc, decodeFragment(frag));
		return encodeTextDoc(doc);
	} catch {
		return undefined;
	}
}
