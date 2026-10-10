import { create, type DescMessage } from '@bufbuild/protobuf';

import { accessParentFields, type AnchoredRow, anchorGroupsOf, compareUtf8 } from '../access.js';
import { type CallerAccess, CallerAccessSchema } from '../gen/syncplane/v1/service_pb.js';
import { entityOptionsOf, refShapes } from '../mutations.js';

/**
 * What a caller holds in a workspace under the access model, as
 * `TestServer.setCallerAccess` takes it: the roles it holds, the permissions
 * it holds outside any role, or both.
 */
export interface TestCallerAccess {
	/**
	 * The values grant rows and subject checks compare with, the primary
	 * subject first. Omitted or empty for an anonymous caller.
	 */
	readonly subjects?: readonly string[];
	/**
	 * The roles the caller holds, each granting the permissions its
	 * declaration and its `includes` give, as grant rows or root roles give
	 * them on the server.
	 */
	readonly roles?: readonly TestRole[];
	/**
	 * Permissions held in addition to those of `roles`. A permission that
	 * neither lists is held nowhere.
	 */
	readonly permissions?: readonly TestPermission[];
}

/**
 * Where a caller holds one role. The role must be declared in the `grant`
 * option of an entity the test server registers.
 */
export interface TestRole {
	readonly role: string;
	/**
	 * The row the role is held at, as a grant row whose access parent points
	 * at it gives the role. Its entity must be one that the access parent of
	 * the role's grant entity can point at. Omitted, the role is held at the
	 * workspace root, as a root role or a grant entity without an access
	 * parent gives it.
	 */
	readonly at?: readonly [entity: DescMessage, key: string];
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

/** A role as the grant option of an entity declares it. */
interface DeclaredRole {
	/** The grant entity that declares the role. */
	readonly entity: string;
	/** Every permission the role grants, its `includes` followed. */
	readonly permissions: readonly string[];
	/**
	 * The entities the role can be held at a row of. Empty when the grant
	 * entity has no access parent, so the role is held at the root alone.
	 */
	readonly anchors: ReadonlySet<string>;
}

/** The roles the `grant` options of a set of entities declare, by name. */
export class RoleTable {
	private readonly roles = new Map<string, DeclaredRole>();

	constructor(schemas: readonly DescMessage[]) {
		for (const schema of schemas) {
			const opts = entityOptionsOf(schema);
			if (opts?.grant === undefined) continue;
			const anchors = new Set<string>();
			for (const field of accessParentFields(schema)) {
				for (const shape of refShapes(field)) {
					const target = entityOptionsOf(shape.targetSchema)?.name;
					if (target !== undefined) anchors.add(target);
				}
			}
			const declared = new Map(opts.grant.roles.map((r) => [ r.name, r ]));
			const expand = (name: string, into: Set<string>, seen: Set<string>) => {
				const role = declared.get(name);
				// The server refuses an include cycle or an unknown include at
				// registration, so neither stops a test here.
				if (role === undefined || seen.has(name)) return;
				seen.add(name);
				for (const permission of role.grants) into.add(permission);
				for (const included of role.includes) expand(included, into, seen);
			};
			for (const role of opts.grant.roles) {
				const permissions = new Set<string>();
				expand(role.name, permissions, new Set());
				this.roles.set(role.name, {
					entity: opts.name,
					permissions: [ ...permissions ].sort(),
					anchors,
				});
			}
		}
	}

	/**
	 * The permissions `roles` grant, where each role is held.
	 *
	 * Throws on a role no entity declares, and on an `at` whose entity the
	 * role cannot be held at.
	 */
	permissionsOf(roles: readonly TestRole[]): TestPermission[] {
		const out: TestPermission[] = [];
		for (const held of roles) {
			const role = this.roles.get(held.role);
			if (role === undefined) {
				const known = [ ...this.roles.keys() ].sort();
				throw new Error(
					`setCallerAccess: no registered entity declares role "${held.role}"`
						+ (known.length === 0 ? '' : `; the declared roles are ${known.join(', ')}`),
				);
			}
			if (held.at === undefined) {
				for (const permission of role.permissions) out.push({ permission, root: true });
				continue;
			}
			const [ schema, key ] = held.at;
			const entity = entityOptionsOf(schema)?.name;
			if (entity === undefined) {
				throw new Error(`setCallerAccess: role "${held.role}": ${schema.typeName} is not an entity`);
			}
			if (!role.anchors.has(entity)) {
				throw new Error(
					`setCallerAccess: role "${held.role}" of ${role.entity} cannot be held at a row of ${entity}; `
						+ (role.anchors.size === 0
							? `${role.entity} has no access parent, so omit at to hold the role at the workspace root`
							: `it is held at a row of ${
								[ ...role.anchors ].sort().join(' or ')
							}, or at the workspace root with at omitted`),
				);
			}
			for (const permission of role.permissions) {
				out.push({ permission, anchors: [ { entity, keys: [ key ] } ] });
			}
		}
		return out;
	}
}

/**
 * The CallerAccess a server sends for `access`, with seq 0: each anchored row
 * once, in the group of the permissions held there and not at the root, and
 * every list sorted.
 */
export function callerAccessOf(access: Omit<TestCallerAccess, 'roles'>): CallerAccess {
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
	const anchored: AnchoredRow[] = [];
	for (const [ entity, keys ] of rows) {
		for (const [ key, held ] of keys) {
			anchored.push({ entity, key, permissions: [ ...held ].sort(compareUtf8) });
		}
	}
	return create(CallerAccessSchema, {
		subjects: [ ...(access.subjects ?? []) ],
		rootPermissions: [ ...root ].sort(compareUtf8),
		anchorGroups: anchorGroupsOf(anchored),
	});
}
