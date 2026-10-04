import { fromBinary, toBinary } from '@bufbuild/protobuf';

import { type Mutation, MutationSchema } from '../gen/syncplane/v1/mutation_pb.js';
import { metaPushedThrough, pendingKey, pendingPrefix } from '../keys.js';
import type { KVRead, KVWrite } from '../kv/store.js';

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
