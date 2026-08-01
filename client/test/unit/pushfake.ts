import type { Mutation } from '../../src/gen/syncplane/v1/mutation_pb.js';
import { MutationStatus, type PushRequest } from '../../src/gen/syncplane/v1/service_pb.js';

/**
 * Helpers for the fake push handlers the unit suites install.
 *
 * A PushRequest carries one batch per workspace and a PushResponse one result
 * per batch. Most specs drive a single workspace, so the helpers default to
 * unwrapping the first batch; `pushAppliedAll` answers every batch, which is
 * what the multi-workspace specs use. Genuine multi-workspace pushes are also
 * exercised against the real engine in the integration suite.
 */

/** The mutations of the first (usually only) batch of a push. */
export function pushedMutations(req: PushRequest): Mutation[] {
	return req.batches[0]?.mutations ?? [];
}

/** The mutations of one named workspace's batch, or [] when it has none. */
export function pushedFor(req: PushRequest, workspaceId: string): Mutation[] {
	return req.batches.find((b) => b.workspaceId === workspaceId)?.mutations ?? [];
}

/** One mutation's outcome, in the shape a fake handler returns. */
export interface FakeMutationResult {
	mutationId: bigint;
	status: MutationStatus;
	seq?: bigint;
	errorCode?: string;
	errorMessage?: string;
}

/** Wraps per-mutation results as the single workspace's results. */
export function pushResults(req: PushRequest, results: FakeMutationResult[]) {
	return {
		results: [ { workspaceId: req.batches[0]?.workspaceId ?? '', results } ],
	};
}

/** Acks every mutation in the push as APPLIED, numbering seqs by mutation id. */
export function pushApplied(req: PushRequest) {
	return pushResults(
		req,
		pushedMutations(req).map((m) => ({
			mutationId: m.mutationId,
			status: MutationStatus.APPLIED,
			seq: m.mutationId,
		})),
	);
}

/** Acks every mutation of EVERY batch in the push, one result set per workspace. */
export function pushAppliedAll(req: PushRequest) {
	return {
		results: req.batches.map((batch) => ({
			workspaceId: batch.workspaceId,
			results: batch.mutations.map((m) => ({
				mutationId: m.mutationId,
				status: MutationStatus.APPLIED,
				seq: m.mutationId,
			})),
		})),
	};
}

/**
 * Fails the whole batch with a workspace-scoped code (a mutation-id gap, a
 * revoked workspace). Nothing applied, so no per-mutation results ride along.
 */
export function pushFailed(req: PushRequest, errorCode: string, errorMessage = '') {
	return {
		results: [ {
			workspaceId: req.batches[0]?.workspaceId ?? '',
			results: [],
			errorCode,
			errorMessage,
		} ],
	};
}
