import type { DescMessage } from '@bufbuild/protobuf';
import { useEffect, useState } from 'react';

import type { CanAnswer } from '../../access.js';
import { useSyncClient, useWorkspaceScope } from '../context.js';
import type { UseWorkspaceOptions } from '../types.js';

/**
 * Predicts whether the caller holds `permission` at the row of `schema` with
 * `key`, for the resolved workspace. See `SyncClient.can` for what the answer
 * means; it is advisory, so use it to shape the UI, not to authorize.
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
 *   const can = useCan(TaskSchema, id, 'edit_tasks');
 *   return <button disabled={can !== 'yes'}>Delete</button>;
 * }
 * ```
 */
export function useCan<D extends DescMessage>(
	schema: D,
	key: string,
	permission: string,
	opts?: UseWorkspaceOptions,
): CanAnswer {
	const client = useSyncClient();
	const scope = useWorkspaceScope(opts?.workspaceId);
	const [ answer, setAnswer ] = useState<CanAnswer>('unknown');
	useEffect(() => {
		let live = true;
		// Answers resolve out of order; only the newest one may land.
		let generation = 0;
		const evaluate = () => {
			const mine = ++generation;
			scope.can(schema, key, permission).then(
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
		const rows = scope.watchRows(schema, [ key ], evaluate);
		return () => {
			live = false;
			offAccess();
			offStatus();
			rows.close();
		};
	}, [ client, scope, schema, key, permission ]);
	return answer;
}
