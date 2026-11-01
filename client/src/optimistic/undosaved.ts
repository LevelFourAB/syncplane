// Saved undo scopes: the record a scope opened with restoreUndoScope() keeps in
// the store, so its stacks outlive the page.
//
// One record per (workspace, key), at metaUndoScope:
//
//   { v: 1, savedAt, refs, undo, redo }
//
//   - savedAt: epoch ms of the last save; the least recently saved records go
//     first when a workspace holds more than maxScopes.
//   - refs: the tokens of the snapshot inverses (see
//     UndoManager.foldBarrierTokens), as decimal strings, so compaction can
//     protect them without decoding the entries.
//   - undo, redo: the stacks, bottom first. A CRUD inverse is stored as
//     CrudMutation bytes; a text inverse is plain data and is stored as it is.
//
// The record only ever holds what the undo journal (undojournal.ts) does not
// void: a save reconciles the stacks against the journal in its own
// transaction, and a load does it again, since the journal may have moved while
// no tab held the record.
//
// A saved group older than the journal's retention is dropped on load and on
// save: past DEAD_TOKEN_TTL_MS the journal may have pruned the death of the
// mutation the group inverts, so it can no longer say whether the group is
// still valid.

import { fromBinary, toBinary } from '@bufbuild/protobuf';

import { CrudMutationSchema } from '../gen/syncplane/v1/mutation_pb.js';
import { metaUndoScope, metaUndoScopePrefix } from '../keys.js';
import type { KVRead, KVWrite } from '../kv/store.js';
import type { TextInverseOp } from '../text/undo-ops.js';
import type { Inverse, UndoEntry } from '../undo.js';
import { DEAD_TOKEN_TTL_MS, readUndoJournal, voidsToken } from './undojournal.js';

/** Version of the saved record; a record of another version reads as empty. */
export const SAVED_UNDO_VERSION = 1;

interface SavedEntry {
	/** Group id, as the saving manager numbered it. */
	g: number;
	/** Undo token, decimal. */
	t: string;
	/** Recorded at, epoch ms. */
	at: number;
	kind: Inverse['kind'];
	crud?: Uint8Array;
	text?: { entity: string; key: string; field: string; ops: TextInverseOp[] };
	reason?: string;
}

interface SavedRecord {
	v: number;
	savedAt: number;
	refs: string[];
	undo: SavedEntry[];
	redo: SavedEntry[];
}

/** Both stacks of a saved scope, bottom first. */
export interface SavedStacks {
	undo: UndoEntry[];
	redo: UndoEntry[];
}

/**
 * Encodes both stacks as the stored record, or undefined when both are empty
 * (an empty scope keeps no record).
 */
export function encodeSavedUndo(
	stacks: { undo: readonly UndoEntry[]; redo: readonly UndoEntry[] },
	savedAt: number,
): SavedRecord | undefined {
	if (stacks.undo.length === 0 && stacks.redo.length === 0) return undefined;
	const refs = new Set<string>();
	const encode = (e: UndoEntry): SavedEntry => {
		const base = { g: e.groupId, t: e.token.toString(), at: e.at, kind: e.inverse.kind };
		switch (e.inverse.kind) {
			case 'crud':
				refs.add(base.t);
				return { ...base, crud: toBinary(CrudMutationSchema, e.inverse.crud) };
			case 'text': {
				const { entity, key, field, ops } = e.inverse;
				return { ...base, text: { entity, key, field, ops } };
			}
			case 'refused':
				refs.add(base.t);
				return { ...base, reason: e.inverse.reason };
		}
	};
	const undo = stacks.undo.map(encode);
	const redo = stacks.redo.map(encode);
	return { v: SAVED_UNDO_VERSION, savedAt, refs: [ ...refs ], undo, redo };
}

/**
 * Decodes a stored record. A record that is absent, of another version or
 * malformed reads as empty stacks: the saved history is lost, and nothing else.
 */
export function decodeSavedUndo(raw: unknown): SavedStacks {
	const empty: SavedStacks = { undo: [], redo: [] };
	if (!isRecord(raw) || raw.v !== SAVED_UNDO_VERSION) return empty;
	try {
		return { undo: raw.undo.map(decodeEntry), redo: raw.redo.map(decodeEntry) };
	} catch {
		return empty;
	}
}

function isRecord(raw: unknown): raw is SavedRecord {
	if (typeof raw !== 'object' || raw === null) return false;
	const r = raw as Partial<SavedRecord>;
	return typeof r.v === 'number' && typeof r.savedAt === 'number' && Array.isArray(r.refs)
		&& Array.isArray(r.undo) && Array.isArray(r.redo);
}

function decodeEntry(raw: SavedEntry): UndoEntry {
	if (typeof raw.g !== 'number' || typeof raw.t !== 'string' || typeof raw.at !== 'number') {
		throw new Error('malformed saved undo entry');
	}
	const head = { groupId: raw.g, token: BigInt(raw.t), at: raw.at };
	switch (raw.kind) {
		case 'crud':
			if (!(raw.crud instanceof Uint8Array)) throw new Error('malformed saved undo entry');
			return { ...head, inverse: { kind: 'crud', crud: fromBinary(CrudMutationSchema, raw.crud) } };
		case 'text': {
			const t = raw.text;
			if (
				t === undefined || typeof t.entity !== 'string' || typeof t.key !== 'string'
				|| typeof t.field !== 'string' || !Array.isArray(t.ops)
			) {
				throw new Error('malformed saved undo entry');
			}
			return {
				...head,
				inverse: { kind: 'text', entity: t.entity, key: t.key, field: t.field, ops: t.ops },
			};
		}
		case 'refused':
			if (typeof raw.reason !== 'string') throw new Error('malformed saved undo entry');
			return { ...head, inverse: { kind: 'refused', reason: raw.reason } };
		default:
			throw new Error('malformed saved undo entry');
	}
}

/**
 * Reads the saved scope at `key` in `tx`, without the groups the workspace's
 * undo journal voids or that were recorded before `cutoff` (epoch ms).
 * `dropped` tells whether the stored record held any such group.
 */
export async function readSavedUndo(
	tx: KVRead,
	workspaceId: string,
	key: string,
	cutoff: number,
): Promise<SavedStacks & { dropped: boolean }> {
	const raw = await tx.get(metaUndoScope(workspaceId, key));
	const journal = await readUndoJournal(tx, workspaceId);
	const stacks = decodeSavedUndo(raw);
	const doomed = (e: UndoEntry): boolean => e.at < cutoff || voidsToken(journal, e.token);
	const undo = dropGroups(stacks.undo, doomed);
	const redo = dropGroups(stacks.redo, doomed);
	const dropped = undo.length !== stacks.undo.length || redo.length !== stacks.redo.length
		|| (raw !== undefined && undo.length === 0 && redo.length === 0);
	return { undo, redo, dropped };
}

/**
 * Writes `record` as the saved scope at `key` in `tx`, or deletes the saved
 * scope when `record` is undefined. Then deletes the records of `workspaceId`
 * saved before `cutoff`, and the least recently saved ones past `maxScopes`.
 * The record at `key` is never one of them.
 */
export async function writeSavedUndo(
	tx: KVWrite,
	workspaceId: string,
	key: string,
	record: SavedRecord | undefined,
	limits: { maxScopes: number; cutoff: number },
): Promise<void> {
	const own = metaUndoScope(workspaceId, key);
	if (record === undefined) await tx.del(own);
	else await tx.put(own, record);
	const others: { key: string; savedAt: number }[] = [];
	for await (const [ k, v ] of tx.scan(metaUndoScopePrefix(workspaceId))) {
		if (k === own) continue;
		const savedAt = isRecord(v) ? v.savedAt : 0;
		if (savedAt < limits.cutoff) await tx.del(k);
		else others.push({ key: k, savedAt });
	}
	// A record that some other tab holds open can go here too: that tab keeps
	// its stacks in memory, and its next save writes the record again. A close
	// always saves (see UndoSaver.close), so the last save comes at the latest
	// when the holder closes. A tab that dies without closing loses the stacks
	// it held, as it would lose its unsaved changes.
	const room = Math.max(0, limits.maxScopes - (record === undefined ? 0 : 1));
	others.sort((x, y) => y.savedAt - x.savedAt);
	for (const { key: k } of others.slice(room)) await tx.del(k);
}

/** Deletes the saved scope at `key` in `tx`. */
export async function deleteSavedUndo(
	tx: KVWrite,
	workspaceId: string,
	key: string,
): Promise<void> {
	await tx.del(metaUndoScope(workspaceId, key));
}

/**
 * The snapshot-inverse tokens of every saved scope of `workspaceId`, read in
 * `tx`, for compaction to keep from folding. Skips records too old to load and
 * tokens the journal voids, which no load can bring back.
 */
export async function savedUndoRefTokens(
	tx: KVRead,
	workspaceId: string,
	now: number = Date.now(),
): Promise<Set<bigint>> {
	const out = new Set<bigint>();
	let journal: Awaited<ReturnType<typeof readUndoJournal>> | undefined;
	for await (const [ , v ] of tx.scan(metaUndoScopePrefix(workspaceId))) {
		if (!isRecord(v) || v.v !== SAVED_UNDO_VERSION) continue;
		if (now - v.savedAt > DEAD_TOKEN_TTL_MS) continue;
		journal ??= await readUndoJournal(tx, workspaceId);
		for (const raw of v.refs) {
			try {
				const token = BigInt(raw);
				if (!voidsToken(journal, token)) out.add(token);
			} catch {
				// A malformed ref protects nothing.
			}
		}
	}
	return out;
}

/** `stack` without every group that has an entry matching `doom`. */
function dropGroups(stack: UndoEntry[], doom: (e: UndoEntry) => boolean): UndoEntry[] {
	const doomed = new Set(stack.filter(doom).map((e) => e.groupId));
	return doomed.size === 0 ? stack : stack.filter((e) => !doomed.has(e.groupId));
}
