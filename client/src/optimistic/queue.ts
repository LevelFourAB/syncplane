import { fromBinary, toBinary } from '@bufbuild/protobuf';

import { countAccessChanges } from '../access.js';
import { type Mutation, MutationSchema } from '../gen/syncplane/v1/mutation_pb.js';
import {
	metaLastMutationId,
	metaPendingAccess,
	metaPushedThrough,
	pendingKey,
	pendingPrefix,
} from '../keys.js';
import type { KVRead, KVWrite } from '../kv/store.js';
import type { EntityRegistry } from '../mutations.js';

/**
 * Appends a mutation to a workspace's pending queue (crash-safe with the
 * overlay write because both happen in the caller's transaction). Queues are per
 * workspace: mutation ids are counted per (workspace, client) and the server
 * requires them gap-free within that pair.
 */
export async function enqueuePending(
	tx: KVWrite,
	workspaceId: string,
	mutation: Mutation,
): Promise<void> {
	await tx.put(
		pendingKey(workspaceId, mutation.mutationId),
		toBinary(MutationSchema, mutation),
	);
}

/** Reads one workspace's pending queue in mutation-id order. */
export async function readPending(tx: KVRead, workspaceId: string): Promise<Mutation[]> {
	const result: Mutation[] = [];
	for await (const [ , v ] of tx.scan(pendingPrefix(workspaceId))) {
		result.push(fromBinary(MutationSchema, v as Uint8Array));
	}
	return result;
}

/**
 * Reads a workspace's durable pushed-through watermark — how much of its queue
 * has been handed to the server. Absent — a workspace that has never pushed —
 * reads as 0. Compaction may only renumber/fold ids strictly above it.
 */
export async function readPushedThrough(tx: KVRead, workspaceId: string): Promise<bigint> {
	const raw = (await tx.get(metaPushedThrough(workspaceId))) as string | undefined;
	return raw ? BigInt(raw) : 0n;
}

/** Advances a workspace's pushed-through watermark. Monotonic; callers raise it. */
export async function writePushedThrough(
	tx: KVWrite,
	workspaceId: string,
	id: bigint,
): Promise<void> {
	await tx.put(metaPushedThrough(workspaceId), id.toString());
}

/**
 * Drops pending mutations of one workspace with id <= acked; returns the
 * survivors and the dropped, each in mutation-id order.
 */
export async function dropAcked(
	tx: KVWrite,
	workspaceId: string,
	acked: bigint,
): Promise<{ remaining: Mutation[]; dropped: Mutation[] }> {
	const remaining: Mutation[] = [];
	const dropped: Mutation[] = [];
	for (const m of await readPending(tx, workspaceId)) {
		if (m.mutationId <= acked) {
			await tx.del(pendingKey(workspaceId, m.mutationId));
			dropped.push(m);
		} else {
			remaining.push(m);
		}
	}
	return { remaining, dropped };
}

/** The value stored at metaPendingAccess. */
interface PendingAccessRecord {
	/** The mutation-id counter when the record was written, `'0'` for none. */
	through: string;
	/** The queued mutations that can change the caller's grants. */
	count: number;
}

/** A stored mutation-id counter as the record names it. */
function counterOf(raw: unknown): string {
	return typeof raw === 'string' ? raw : '0';
}

/**
 * How many mutations of a workspace's pending queue can change the caller's
 * grants, from the values that one transaction read at its metaPendingAccess
 * and its metaLastMutationId. Undefined when the record does not tell: it is
 * absent, or a writer that does not keep it minted ids after it was written.
 */
export function pendingAccessOf(record: unknown, lastMutationId: unknown): number | undefined {
	if (typeof record !== 'object' || record === null) return undefined;
	const { through, count } = record as Partial<PendingAccessRecord>;
	if (typeof count !== 'number' || through !== counterOf(lastMutationId)) return undefined;
	return count;
}

/**
 * Writes the metaPendingAccess of `workspaceId` for a queue that holds
 * exactly `queue` once `tx` commits, with the mutation-id counter at
 * `lastMutationId`, or at the value `tx` reads when that is omitted. Call it
 * in each transaction that removes, renumbers or folds queued mutations; a
 * transaction that only appends calls notePendingAppended.
 */
export async function recountPendingAccess(
	tx: KVWrite,
	registry: EntityRegistry,
	workspaceId: string,
	queue: readonly Mutation[],
	lastMutationId?: bigint,
): Promise<void> {
	const record: PendingAccessRecord = {
		through: lastMutationId?.toString() ?? counterOf(await tx.get(metaLastMutationId(workspaceId))),
		count: countAccessChanges(registry, queue),
	};
	await tx.put(metaPendingAccess(workspaceId), record);
}

/**
 * Brings the metaPendingAccess of `workspaceId` up to date in a transaction
 * that appended `appended` to the queue and moved the mutation-id counter to
 * `lastMutationId`. `before` holds the record and the counter as the
 * transaction read them before the append. When the record told nothing, it
 * counts the whole queue, which reads it.
 */
export async function notePendingAppended(
	tx: KVWrite,
	registry: EntityRegistry,
	workspaceId: string,
	before: { record: unknown; lastMutationId: unknown },
	appended: readonly Mutation[],
	lastMutationId: bigint,
): Promise<void> {
	const count = pendingAccessOf(before.record, before.lastMutationId);
	if (count === undefined) {
		await recountPendingAccess(
			tx,
			registry,
			workspaceId,
			await readPending(tx, workspaceId),
			lastMutationId,
		);
		return;
	}
	const record: PendingAccessRecord = {
		through: lastMutationId.toString(),
		count: count + countAccessChanges(registry, appended),
	};
	await tx.put(metaPendingAccess(workspaceId), record);
}
