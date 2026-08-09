import type { DescMessage } from '@bufbuild/protobuf';

import { useMutateHook } from './hooks/use-mutate.js';
import { useQueryAllHook } from './hooks/use-query-all.js';
import { useQueryHook } from './hooks/use-query.js';
import { useRowHook } from './hooks/use-row.js';
import { useTextHook } from './hooks/use-text.js';
import type { ReactEntityApi, ReactEntityApis } from './types.js';

function uncapitalize(name: string): string {
	return name.charAt(0).toLowerCase() + name.slice(1);
}

/**
 * Builds the per-entity hook map, mirroring `buildEntityApis` (db.ts): same
 * `uncapitalize(schema.name)` key, same duplicate-key throw. The returned `db`
 * is client-agnostic — every hook reads the live client and workspace from
 * context at call time — so one `db` works under any `<SyncProvider client>`
 * and any `<WorkspaceProvider>`.
 */
export function buildReactDb<E extends readonly DescMessage[]>(entities: E): ReactEntityApis<E> {
	const apis: Record<string, ReactEntityApi<DescMessage>> = {};
	for (const schema of entities) {
		const key = uncapitalize(schema.name);
		if (Object.prototype.hasOwnProperty.call(apis, key)) {
			throw new Error(
				`createReactSync: "${schema.typeName}" maps to "${key}" which is already taken; `
					+ `entities with clashing simple names cannot share the hook facade`,
			);
		}
		apis[key] = {
			useQuery: (query, opts) => useQueryHook(schema, query, opts),
			useSuspenseQuery: (query, opts) => useQueryHook(schema, query, opts, true),
			useQueryAll: (query, opts) => useQueryAllHook(schema, query, opts),
			useRow: (rowKey, opts) => useRowHook(schema, rowKey, opts),
			useSuspenseRow: (rowKey, opts) => useRowHook(schema, rowKey, opts, true),
			useMutate: (opts) => useMutateHook(schema, opts),
			useText: (rowKey, field, opts) => useTextHook(schema, rowKey, field, opts),
		};
	}
	return apis as ReactEntityApis<E>;
}
