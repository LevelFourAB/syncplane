import { create, type DescMessage, type MessageInitShape, toBinary } from '@bufbuild/protobuf';
import { useEffect, useMemo, useState } from 'react';

import type { AccessOperation, CanAnswer } from '../../access.js';
import type { CanOptions, WorkspaceClient } from '../../client.js';
import { useSyncClient, useWorkspaceScope } from '../context.js';
import type { UseWorkspaceOptions } from '../types.js';

/** Options for {@link useCan}. */
export interface UseCanOptions extends UseWorkspaceOptions, CanOptions {}

/**
 * Predicts whether the entity's access rules admit `operation` on the row of
 * `schema` with `key`, for the resolved workspace. See `SyncClient.can` for
 * what the answer means; it is advisory, so use it to shape the UI, not to
 * authorize.
 *
 * Re-evaluates when the row changes, when the server sends a new summary of
 * the caller's access (in this tab or another sharing the store), and when
 * the sync status changes, which covers a queued write that could change the
 * caller's grants being acked. Returns `'unknown'` until the first answer
 * resolves.
 *
 * @example
 * ```tsx
 * function DeleteButton({ id }: { id: string }) {
 *   const can = useCan(CommentSchema, id, 'delete');
 *   return <button disabled={can !== 'yes'}>Delete</button>;
 * }
 * ```
 */
export function useCan<D extends DescMessage>(
	schema: D,
	key: string,
	operation: Exclude<AccessOperation, 'create'>,
	opts?: UseCanOptions,
): CanAnswer {
	const fields = opts?.fields;
	// A fields array written inline is a new array on every render.
	const fieldsKey = fields === undefined ? undefined : JSON.stringify(fields);
	return useAnswer(
		opts?.workspaceId,
		schema,
		key,
		(scope) => scope.can(schema, key, operation, fields === undefined ? undefined : { fields }),
		[ schema, key, operation, fieldsKey ],
	);
}

/**
 * Predicts whether the entity's access rules admit creating `row`, for the
 * resolved workspace. See `SyncClient.canCreate` for what the answer means.
 *
 * Re-evaluates when `row` changes by value, when the server sends a new
 * summary of the caller's access, and when the sync status changes. Returns
 * `'unknown'` until the first answer resolves.
 *
 * @example
 * ```tsx
 * function CommentBox({ taskId }: { taskId: string }) {
 *   const can = useCanCreate(CommentSchema, { id: 'draft', task: { id: taskId } });
 *   return can === 'no' ? null : <textarea />;
 * }
 * ```
 */
export function useCanCreate<D extends DescMessage>(
	schema: D,
	row: MessageInitShape<D>,
	opts?: UseWorkspaceOptions,
): CanAnswer {
	// An object literal is a new object on every render; its encoding is not.
	const encoded = useMemo(() => toBinary(schema, create(schema, row)).join(','), [ schema, row ]);
	return useAnswer(opts?.workspaceId, schema, undefined, (scope) => scope.canCreate(schema, row), [
		schema,
		encoded,
	]);
}

/**
 * Predicts whether the caller holds `permission` at the row of `schema` with
 * `key`, for the resolved workspace. See `SyncClient.hasPermission` for what
 * the answer means. Re-evaluates as {@link useCan} does.
 *
 * @example
 * ```tsx
 * function InviteButton({ projectId }: { projectId: string }) {
 *   const can = useHasPermission(ProjectSchema, projectId, 'invite_members');
 *   return <button disabled={can !== 'yes'}>Invite</button>;
 * }
 * ```
 */
export function useHasPermission<D extends DescMessage>(
	schema: D,
	key: string,
	permission: string,
	opts?: UseWorkspaceOptions,
): CanAnswer {
	return useAnswer(
		opts?.workspaceId,
		schema,
		key,
		(scope) => scope.hasPermission(schema, key, permission),
		[ schema, key, permission ],
	);
}

/**
 * Holds the newest answer `ask` gives, asked again on every change of access
 * summary or sync status, and of the row with `key` when there is one.
 * `deps` decide when `ask` itself changes.
 */
function useAnswer<D extends DescMessage>(
	workspaceId: string | undefined,
	schema: D,
	key: string | undefined,
	ask: (scope: WorkspaceClient) => Promise<CanAnswer>,
	deps: readonly unknown[],
): CanAnswer {
	const client = useSyncClient();
	const scope = useWorkspaceScope(workspaceId);
	const [ answer, setAnswer ] = useState<CanAnswer>('unknown');
	useEffect(() => {
		let live = true;
		// Answers resolve out of order; only the newest one may land.
		let generation = 0;
		const evaluate = () => {
			const mine = ++generation;
			ask(scope).then(
				(a) => {
					if (live && mine === generation) setAnswer(a);
				},
				() => {
					if (live && mine === generation) setAnswer('unknown');
				},
			);
		};
		evaluate();
		const offAccess = client.onAccessChange((ws) => {
			if (ws === scope.workspaceId) evaluate();
		});
		const offStatus = client.onStatusChange(evaluate);
		const rows = key === undefined ? undefined : scope.watchRows(schema, [ key ], evaluate);
		return () => {
			live = false;
			offAccess();
			offStatus();
			rows?.close();
		};
		// `ask` is a new closure on every render; deps lists what it reads.
	}, [ client, scope, ...deps ]);
	return answer;
}
