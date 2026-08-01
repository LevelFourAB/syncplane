import { fromBinary, toBinary } from '@bufbuild/protobuf';

import { type Mutation, MutationSchema } from '../gen/syncplane/v1/mutation_pb.js';
import { pendingKey, pendingPrefix } from '../keys.js';
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

/** Drops pending mutations of one workspace with id <= acked; returns the survivors. */
export async function dropAcked(
	tx: KVWrite,
	workspaceId: string,
	acked: bigint,
): Promise<Mutation[]> {
	const remaining: Mutation[] = [];
	for (const m of await readPending(tx, workspaceId)) {
		if (m.mutationId <= acked) {
			await tx.del(pendingKey(workspaceId, m.mutationId));
		} else {
			remaining.push(m);
		}
	}
	return remaining;
}
