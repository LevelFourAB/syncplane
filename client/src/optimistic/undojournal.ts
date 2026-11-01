// The undo journal: what every tab's undo stacks must learn about mutations
// that another tab handled. Undo stacks live in one tab's memory, but the pending
// queue is shared, so the tab that pushes a mutation and hears its rejection is
// often not the tab whose stack recorded it. Likewise, a custom mutation voids
// every stack in the workspace, not only the stacks of the tab that issued it.
//
// The journal is two keys per workspace, written in the transaction of the
// event itself, and read by a stack before it replays a group:
//
//   - undoBarrier: every group whose token is lower is void. A custom mutation
//     sets it to the token minted in its own enqueue transaction; tokens are
//     ordered across the store (see mintUndoToken), so a group whose token is
//     lower was recorded before the custom mutation committed. Pruning can also
//     raise it (see below). It only ever rises: each value is above every token
//     minted before it.
//   - undoDead: tokens of mutations that died, with the time they died.
//
// Pruning. A dead token can go once no stack can still hold a group that
// references it:
//
//   - a token below the barrier: the barrier voids its group anyway;
//   - a token that died more than DEAD_TOKEN_TTL_MS ago. Every stack, open or
//     saved, drops a group older than this TTL before it can replay it, so no
//     stack still holds a group whose mutation died this long ago;
//   - the oldest entries past MAX_DEAD_TOKENS, a bound on the record's size.
//     Forgetting such a token would let a stack that still holds its group
//     replay it, so the barrier moves above every token pruned this way: the
//     groups recorded before them are void too. That costs old undo steps of
//     a workspace that had this many rejections within the TTL, never a replay
//     of a dead write.
//
// Mutation ids cannot serve here: compaction and resequencing renumber them and
// mint old ids again, while tokens stay with their mutation.

import { metaUndoBarrier, metaUndoDead } from '../keys.js';
import type { KVRead, KVWrite } from '../kv/store.js';
import { GC_TTL_MS } from '../sync/intent.js';

/** How long a dead token stays in the journal; see the module comment. */
export const DEAD_TOKEN_TTL_MS = GC_TTL_MS;

/**
 * Most dead tokens the journal keeps per workspace. Past it, the lowest tokens
 * go and the barrier moves above them.
 */
export const MAX_DEAD_TOKENS = 4096;

/** One workspace's undo journal, decoded. */
export interface UndoJournal {
	/** Every undo group entry with a lower token is void; 0 voids none. */
	readonly barrier: bigint;
	readonly dead: ReadonlySet<bigint>;
}

/** Whether `journal` voids the undo group entry recorded under `token`. */
export function voidsToken(journal: UndoJournal, token: bigint): boolean {
	return token < journal.barrier || journal.dead.has(token);
}

/** The journal keys of a workspace, in the order decodeUndoJournal takes them. */
export function undoJournalKeys(workspaceId: string): [string, string] {
	return [ metaUndoBarrier(workspaceId), metaUndoDead(workspaceId) ];
}

/** Decodes the values read at undoJournalKeys. Malformed values read as empty. */
export function decodeUndoJournal(barrierRaw: unknown, deadRaw: unknown): UndoJournal {
	return {
		barrier: decodeBarrier(barrierRaw),
		dead: new Set(decodeDead(deadRaw).map(([ token ]) => token)),
	};
}

/** Reads a workspace's undo journal in `tx`. */
export async function readUndoJournal(tx: KVRead, workspaceId: string): Promise<UndoJournal> {
	const [ barrier, dead ] = await tx.getMany(undoJournalKeys(workspaceId));
	return decodeUndoJournal(barrier, dead);
}

/**
 * Records in `tx` that a custom mutation with undo token `token` committed:
 * every undo group recorded before it is void.
 */
export async function writeUndoBarrier(
	tx: KVWrite,
	workspaceId: string,
	token: bigint,
): Promise<void> {
	await tx.put(metaUndoBarrier(workspaceId), token.toString());
}

/**
 * Records in `tx` that the mutations carrying `tokens` died, and prunes the
 * record (see the module comment). Writes nothing when `tokens` is empty.
 * `now` is the clock in epoch ms.
 */
export async function noteDeadUndoTokens(
	tx: KVWrite,
	workspaceId: string,
	tokens: readonly bigint[],
	now: number = Date.now(),
): Promise<void> {
	if (tokens.length === 0) return;
	const [ barrierRaw, deadRaw ] = await tx.getMany(undoJournalKeys(workspaceId));
	let barrier = decodeBarrier(barrierRaw);
	const entries = decodeDead(deadRaw);
	for (const token of tokens) entries.push([ token, now ]);
	let kept = entries
		.filter(([ token, diedAt ]) => token >= barrier && now - diedAt <= DEAD_TOKEN_TTL_MS)
		.sort((x, y) => (x[0] < y[0] ? -1 : x[0] > y[0] ? 1 : 0));
	if (kept.length > MAX_DEAD_TOKENS) {
		// Sorted by token, so the pruned tokens are the lowest; the barrier moves
		// past the highest of them (see the module comment).
		const pruned = kept.slice(0, kept.length - MAX_DEAD_TOKENS);
		const above = pruned[pruned.length - 1]![0] + 1n;
		if (above > barrier) {
			barrier = above;
			await writeUndoBarrier(tx, workspaceId, barrier);
		}
		kept = kept.slice(-MAX_DEAD_TOKENS);
	}
	if (kept.length === 0) {
		await tx.del(metaUndoDead(workspaceId));
		return;
	}
	await tx.put(
		metaUndoDead(workspaceId),
		kept.map(([ token, diedAt ]) => [ token.toString(), diedAt ]),
	);
}

function decodeBarrier(raw: unknown): bigint {
	if (typeof raw !== 'string') return 0n;
	try {
		return BigInt(raw);
	} catch {
		return 0n;
	}
}

function decodeDead(raw: unknown): [bigint, number][] {
	if (!Array.isArray(raw)) return [];
	const out: [bigint, number][] = [];
	for (const entry of raw) {
		if (!Array.isArray(entry) || typeof entry[0] !== 'string' || typeof entry[1] !== 'number') {
			continue;
		}
		try {
			out.push([ BigInt(entry[0]), entry[1] ]);
		} catch {
			// A malformed entry voids nothing.
		}
	}
	return out;
}

/**
 * Thrown by an undo or redo replay that the journal voids, before the replay
 * writes anything. Internal: undo() and redo() catch it and move on to the next
 * group, so a caller never sees it.
 */
export class StaleUndoGroupError extends Error {
	constructor(readonly journal: UndoJournal) {
		super('undo group voided by the undo journal');
		this.name = 'StaleUndoGroupError';
	}
}
