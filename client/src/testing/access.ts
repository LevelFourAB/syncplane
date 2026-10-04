import { create } from '@bufbuild/protobuf';

import { type CallerAccess, CallerAccessSchema } from '../gen/syncplane/v1/service_pb.js';

/**
 * What a caller holds in a workspace under the access model, permission by
 * permission, as `TestServer.setCallerAccess` takes it.
 */
export interface TestCallerAccess {
	/**
	 * The values grant rows and subject checks compare with, the primary
	 * subject first. Omitted or empty for an anonymous caller.
	 */
	readonly subjects?: readonly string[];
	/** A permission that is not listed is held nowhere. */
	readonly permissions?: readonly TestPermission[];
}

/** Where a caller holds one permission. */
export interface TestPermission {
	readonly permission: string;
	/** Held at the workspace root, which covers every row. */
	readonly root?: boolean;
	/**
	 * The rows the permission is held at, per entity. A permission held at a
	 * row also covers every row whose access parent points at it.
	 */
	readonly anchors?: readonly { readonly entity: string; readonly keys: readonly string[] }[];
}

/**
 * The CallerAccess a server sends for `access`, with seq 0: each anchored row
 * once, in the group of the permissions held there and not at the root, and
 * every list sorted.
 */
export function callerAccessOf(access: TestCallerAccess): CallerAccess {
	const root = new Set<string>();
	for (const p of access.permissions ?? []) {
		if (p.root === true) root.add(p.permission);
	}
	// entity → key → the permissions held at the row.
	const rows = new Map<string, Map<string, Set<string>>>();
	for (const p of access.permissions ?? []) {
		if (root.has(p.permission)) continue;
		for (const a of p.anchors ?? []) {
			let keys = rows.get(a.entity);
			if (keys === undefined) rows.set(a.entity, keys = new Map());
			for (const key of a.keys) {
				let held = keys.get(key);
				if (held === undefined) keys.set(key, held = new Set());
				held.add(p.permission);
			}
		}
	}
	// The permissions of a group, joined, → entity → keys.
	const groups = new Map<string, { permissions: string[]; anchors: Map<string, string[]> }>();
	for (const [ entity, keys ] of rows) {
		for (const [ key, held ] of keys) {
			const permissions = [ ...held ].sort();
			const id = permissions.join('\u0000');
			let group = groups.get(id);
			if (group === undefined) groups.set(id, group = { permissions, anchors: new Map() });
			let groupKeys = group.anchors.get(entity);
			if (groupKeys === undefined) group.anchors.set(entity, groupKeys = []);
			groupKeys.push(key);
		}
	}
	return create(CallerAccessSchema, {
		subjects: [ ...(access.subjects ?? []) ],
		rootPermissions: [ ...root ].sort(),
		anchorGroups: [ ...groups.values() ]
			.sort((a, b) => comparePermissions(a.permissions, b.permissions))
			.map((g) => ({
				permissions: g.permissions,
				anchors: [ ...g.anchors.keys() ].sort().map((entity) => ({
					entity,
					keys: g.anchors.get(entity)!.sort(),
				})),
			})),
	});
}

/** Orders two sorted permission lists element by element, a prefix first. */
function comparePermissions(a: readonly string[], b: readonly string[]): number {
	for (let i = 0; i < Math.min(a.length, b.length); i++) {
		if (a[i] !== b[i]) return a[i]! < b[i]! ? -1 : 1;
	}
	return a.length - b.length;
}
