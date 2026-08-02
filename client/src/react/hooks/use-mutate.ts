import type { DescMessage, MessageInitShape } from '@bufbuild/protobuf';
import { useMemo } from 'react';

import type { PatchOptions } from '../../client.js';
import type { PatchInit } from '../../mutations.js';
import { useWorkspaceScope } from '../context.js';
import type { EntityMutate, UseWorkspaceOptions } from '../types.js';

/**
 * The stable per-entity mutate facade (`{ put, patch, update, del }`), writing
 * to the resolved workspace. Mutations are optimistic — they resolve on local
 * commit; server rejections (including a `CodeConflict` from a failed CAS)
 * arrive asynchronously via the client's `onMutationRejected`, NOT as a
 * rejection of these promises. A hook surfacing those rejections
 * (`useOnMutationRejected`) is phase 2.
 */
export function useMutateHook<D extends DescMessage>(
	schema: D,
	opts?: UseWorkspaceOptions,
): EntityMutate<D> {
	const scope = useWorkspaceScope(opts?.workspaceId, opts?.undo);
	return useMemo<EntityMutate<D>>(
		() => ({
			put: (value, o) => scope.mutate.put(schema, value, o),
			// One wide implementation behind the two `patch` overloads (see
			// EntityMutate). The public signature already refused a whole message;
			// inside the body D is unresolved, so the cast only restates that.
			patch:
				((key: string, init: MessageInitShape<D>, o?: PatchOptions) =>
					scope.mutate.patch(schema, key, init as PatchInit<D>, o)) as EntityMutate<D>['patch'],
			update: (key, recipe, o) => scope.mutate.update(schema, key, recipe, o),
			del: (key, o) => scope.mutate.del(schema, key, o),
		}),
		[ scope, schema ],
	);
}
