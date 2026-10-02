/**
 * Client glue for the engine's AdminService — the trusted control plane for
 * runtime schema registration.
 *
 * Two directions, both built on the same FileDescriptorSet the schema builder
 * emits (see builder.ts):
 *
 *   - `registerSchema` ships a bundle's bytes to the server so a Node backend
 *     can provision an app's entities over the wire (parallel to the Go
 *     RegisterDescriptorSet path).
 *   - `fetchSchema` pulls an app's bytes back and rebuilds the entity
 *     descriptors — "the server ships its schema", so an SPA can boot its
 *     descriptors from the server instead of importing the bundle.
 */

import type { DescMessage } from '@bufbuild/protobuf';
import { createClient, type Transport } from '@connectrpc/connect';

import { AdminService, type RegisterStatus } from '../gen/syncplane/v1/admin_pb.js';
import { entitiesFromDescriptorSet, type SchemaBundle } from './builder.js';

/** The outcome of registerSchema, mirroring AdminService.RegisterResponse. */
export interface RegisterResult {
	/** The app's schema revision after registration. */
	revision: string;
	/** Whether the app was created, additively updated, or unchanged. */
	status: RegisterStatus;
	/** The entity wire names registered for this app, sorted. */
	entityNames: string[];
}

/** Options for registerSchema. */
export interface RegisterOptions {
	/** Bearer token sent as `Authorization: Bearer <token>` (TokenAuthorizer). */
	token?: string;
	/**
	 * Optimistic-concurrency guard: registration fails unless this equals the
	 * app's current revision.
	 */
	expectedRevision?: string;
	/**
	 * Lets the registration change the access model of an entity that is
	 * already registered: its `access`, `grant` or `.accessParent()`
	 * declarations. Without it, the server refuses such a change.
	 *
	 * @defaultValue false
	 */
	allowAccessChange?: boolean;
}

/**
 * Registers an app's schema with the engine over AdminService. The bundle is
 * anything `defineSchema(...)` produced; its serialized FileDescriptorSet is
 * what the engine ingests. The transport must target the (private) admin
 * surface.
 */
export async function registerSchema(
	transport: Transport,
	appId: string,
	bundle: SchemaBundle,
	opts: RegisterOptions = {},
): Promise<RegisterResult> {
	const client = createClient(AdminService, transport);
	const res = await client.register(
		{
			appId,
			descriptorSet: bundle.toBinary(),
			expectedRevision: opts.expectedRevision ?? '',
			allowAccessChange: opts.allowAccessChange ?? false,
		},
		opts.token ? { headers: { Authorization: `Bearer ${opts.token}` } } : undefined,
	);
	return { revision: res.revision, status: res.status, entityNames: res.entityNames };
}

/**
 * Fetches an app's registered descriptor set from the server and rebuilds its
 * syncable entity descriptors, ready for `new SyncClient({ entities })`.
 * Throws if the server has no schema for the app.
 */
export async function fetchSchema(transport: Transport, appId: string): Promise<DescMessage[]> {
	const client = createClient(AdminService, transport);
	const res = await client.getSchema({ appId });
	if (!res.found) throw new Error(`server has no registered schema for app "${appId}"`);
	return entitiesFromDescriptorSet(res.descriptorSet);
}
