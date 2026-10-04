import {
	create,
	type DescMessage,
	fromBinary,
	type Message,
	type MessageInitShape,
	type MessageShape,
	toBinary,
} from '@bufbuild/protobuf';
import { anyUnpack } from '@bufbuild/protobuf/wkt';
import { Code, ConnectError, createRouterTransport, type Transport } from '@connectrpc/connect';

import {
	CodeConflict,
	CodeInvalidArgument,
	CodeMutationIdGap,
	CodePermissionDenied,
	CodeTextFullStateRequired,
	CodeWorkspaceAccessRevoked,
	ErrorCodeMetadataKey,
} from '../codes.js';
import { type CrudMutation, CrudOp, type Mutation } from '../gen/syncplane/v1/mutation_pb.js';
import {
	type BootstrapRequest,
	BootstrapResponseSchema,
	type CallerAccess,
	MutationStatus,
	type PullRequest,
	type PushRequest,
	type SubscribeRequest,
	SubscribeResponseSchema,
	SyncService,
	WorkspaceDropped_Reason,
} from '../gen/syncplane/v1/service_pb.js';
import {
	EntityRegistry,
	keyOf,
	mergeMask,
	type MergerSpec,
	mergeTextFieldsInto,
	textSchemaPathOf,
} from '../mutations.js';
import { callerAccessOf, type TestCallerAccess } from './access.js';

/** The workspace the kit reads and writes when none is named. */
export const DEFAULT_WORKSPACE = 'main';

/**
 * Rejects the mutation being applied, with a code the client's
 * `onMutationRejected` callback receives. Throw it from a {@link TestMutator}
 * to reject like a server-side mutator would; {@link TestServer.reject} throws
 * it for you from a rule.
 *
 * A rejection is an ack: the client drops the pending mutation, rolls its
 * optimistic state back and fires `onMutationRejected` — it does not retry.
 */
export class Rejection extends Error {
	constructor(readonly code: string, message: string) {
		super(message);
		this.name = 'Rejection';
	}
}

/**
 * Selects mutations for {@link TestServer.reject}. Every given field must
 * match; an empty match selects every mutation. A CRUD batch matches when any
 * of its sub-mutations does.
 */
export interface RejectMatch {
	/** Entity schema or registered entity name the write targets. */
	entity?: DescMessage | string;
	/** Row key the write targets. */
	key?: string;
	op?: 'put' | 'patch' | 'delete';
	/** Custom-mutator name; only custom mutations can match it. */
	mutator?: string;
	clientId?: string;
	/** Drop the rule after its first match. */
	once?: boolean;
}

/** Predicate form of {@link RejectMatch}, for conditions the fields cannot express. */
export type RejectPredicate = (mutation: Mutation, workspaceId: string) => boolean;

/**
 * State a {@link TestMutator} reads and writes. Writes are staged: a
 * {@link Rejection} thrown after them discards everything the mutator wrote.
 */
export interface TestMutatorTx {
	get<D extends DescMessage>(schema: D, key: string): MessageShape<D> | undefined;
	/** Live rows of the entity, in key order. */
	scan<D extends DescMessage>(schema: D): MessageShape<D>[];
	put<D extends DescMessage>(schema: D, value: MessageInitShape<D>): void;
	del(schema: DescMessage, key: string): void;
}

/**
 * Server half of a custom mutation, invoked when a client pushes
 * `mutate.custom(name, args)`. The authoritative side of the pair whose client
 * half is `SyncClientOptions.mutators` — give the two the same effect, or use
 * the divergence deliberately to test that rebase self-heals.
 *
 * Throw {@link Rejection} to reject the mutation; any other throw fails the
 * push RPC.
 */
export interface TestMutator<A extends DescMessage = DescMessage> {
	/** Schema the mutation's `Any` args unpack as; omitted means args are ignored. */
	args?: A;
	apply(tx: TestMutatorTx, args: MessageShape<A> | undefined): void | Promise<void>;
}

export interface TestServerOptions<E extends readonly DescMessage[] = readonly DescMessage[]> {
	entities: E;
	/** Server halves of the app's custom merge hooks (`Server.RegisterMerger`). */
	mergers?: MergerSpec[];
	mutators?: Record<string, TestMutator>;
	/** Revision the server advertises on every response. @defaultValue '' (none) */
	schemaRevision?: string;
	/** Rows per bootstrap chunk. @defaultValue 100 */
	chunkSize?: number;
}

/** How a push held back by {@link TestServer.holdPushes} is let through. */
export interface PushHold {
	/** Lets held pushes proceed. Idempotent. */
	release(): void;
}

interface StoredRow {
	version: bigint;
	/** Serialized entity message; undefined marks a tombstone. */
	value: Uint8Array | undefined;
}

interface Verdict {
	code: string;
	message: string;
}

interface RejectRule {
	matches: (mutation: Mutation, workspaceId: string) => boolean;
	once: boolean;
	code: string;
	message: string;
}

class WorkspaceState {
	seq = 0n;
	/** entity name → row key → stored row, tombstones included. */
	rows = new Map<string, Map<string, StoredRow>>();
	/** clientId → highest mutation id acked (applied or rejected). */
	watermarks = new Map<string, bigint>();
	/** clientId → mutation id → recorded rejection, replayed on resend. */
	verdicts = new Map<string, Map<bigint, Verdict>>();
	/** Cursors below this sequence answer reset=true. */
	horizon = 0n;
	/** Subscribe streams parked on this workspace. */
	wakers = new Set<() => void>();

	rowsOf(entity: string): Map<string, StoredRow> {
		let m = this.rows.get(entity);
		if (!m) {
			m = new Map();
			this.rows.set(entity, m);
		}
		return m;
	}

	wake(): void {
		const wakers = [ ...this.wakers ];
		this.wakers.clear();
		for (const wake of wakers) wake();
	}
}

/**
 * Writes staged by one mutation, over the committed rows. Committing moves
 * them into the workspace; discarding the instance is the savepoint rollback.
 */
class Staging {
	private changes = new Map<string, Map<string, StoredRow>>();

	constructor(private state: WorkspaceState) {}

	get(entity: string, key: string): StoredRow | undefined {
		const staged = this.changes.get(entity);
		if (staged?.has(key)) return staged.get(key);
		return this.state.rows.get(entity)?.get(key);
	}

	entries(entity: string): [string, StoredRow][] {
		const merged = new Map(this.state.rows.get(entity));
		for (const [ key, row ] of this.changes.get(entity) ?? []) merged.set(key, row);
		return [ ...merged ];
	}

	set(entity: string, key: string, row: StoredRow): void {
		let staged = this.changes.get(entity);
		if (!staged) {
			staged = new Map();
			this.changes.set(entity, staged);
		}
		staged.set(key, row);
	}

	commit(): void {
		for (const [ entity, staged ] of this.changes) {
			const rows = this.state.rowsOf(entity);
			for (const [ key, row ] of staged) rows.set(key, row);
		}
	}
}

class MutatorTx implements TestMutatorTx {
	constructor(
		private staging: Staging,
		private registry: EntityRegistry,
		private seq: bigint,
	) {}

	get<D extends DescMessage>(schema: D, key: string): MessageShape<D> | undefined {
		const def = this.registry.bySchema(schema);
		const row = this.staging.get(def.name, key);
		return row?.value !== undefined ? fromBinary(schema, row.value) : undefined;
	}

	scan<D extends DescMessage>(schema: D): MessageShape<D>[] {
		const def = this.registry.bySchema(schema);
		return this.staging.entries(def.name)
			.filter(([ , row ]) => row.value !== undefined)
			.sort(([ a ], [ b ]) => (a < b ? -1 : 1))
			.map(([ , row ]) => fromBinary(schema, row.value!));
	}

	put<D extends DescMessage>(schema: D, value: MessageInitShape<D>): void {
		const def = this.registry.bySchema(schema);
		const msg = create(schema, value);
		this.staging.set(def.name, keyOf(def, msg), {
			version: this.seq,
			value: toBinary(schema, msg),
		});
	}

	del(schema: DescMessage, key: string): void {
		const def = this.registry.bySchema(schema);
		this.staging.set(def.name, key, { version: this.seq, value: undefined });
	}
}

const cursorPrefix = 'tk:';

function encodeCursor(seq: bigint): Uint8Array {
	return new TextEncoder().encode(cursorPrefix + seq);
}

/** undefined means the cursor was not minted by this server — answered as reset. */
function decodeCursor(cursor: Uint8Array): bigint | undefined {
	if (cursor.length === 0) return 0n;
	const text = new TextDecoder().decode(cursor);
	if (!text.startsWith(cursorPrefix)) return undefined;
	try {
		return BigInt(text.slice(cursorPrefix.length));
	} catch {
		return undefined;
	}
}

function rowChangeInit(entity: string, key: string, row: StoredRow) {
	return {
		entity,
		key,
		version: row.version,
		body: row.value !== undefined
			? { case: 'value' as const, value: row.value }
			: { case: 'tombstone' as const, value: true },
	};
}

const opNames = { put: CrudOp.PUT, patch: CrudOp.PATCH, delete: CrudOp.DELETE } as const;

/**
 * In-memory syncplane server for tests: the `SyncService` the real engine
 * serves, held entirely in this process and steerable from the test body.
 * {@link TestServer.transport} plugs into `SyncClientOptions.transport`;
 * `createTestClient` does that wiring, and sharing one TestServer between
 * several test clients is how convergence between replicas is exercised.
 *
 * It applies mutations with the engine's semantics: per-workspace sequence,
 * per-client watermarks (a resend answers `ALREADY_APPLIED`, or replays a
 * recorded rejection; a mutation-id gap fails the batch), PATCH masks merged
 * per field, collaborative-text fields unioned on PUT, `expectedVersion`
 * compare-and-swap, atomic batches, tombstoned deletes.
 *
 * Not simulated: partial-sync subscriptions (predicates, windows and follows
 * are ignored — every row syncs), server queries (`queryServer` and friends
 * fail), presence rooms, and permission hooks beyond {@link TestServer.reject}
 * rules. Behavior that depends on those needs the integration suite's real
 * server.
 *
 * Not safe for concurrent use from multiple test workers; make one per test.
 */
export class TestServer<const E extends readonly DescMessage[] = readonly DescMessage[]> {
	readonly entities: E;
	/** Every PushRequest received, in arrival order — held ones included. */
	readonly pushes: PushRequest[] = [];

	private registry: EntityRegistry;
	private workspaces = new Map<string, WorkspaceState>();
	private mutators = new Map<string, TestMutator>();
	private rules = new Set<RejectRule>();
	private revoked = new Set<string>();
	private callerAccess = new Map<string, CallerAccess>();
	private holds = 0;
	private holdWaiters: (() => void)[] = [];
	private schemaRevision: string;
	private chunkSize: number;
	private transportCache: Transport | undefined;

	constructor(opts: TestServerOptions<E>) {
		this.entities = opts.entities;
		this.registry = new EntityRegistry(opts.entities);
		if (opts.mergers) this.registry.setMergers(opts.mergers);
		for (const [ name, mutator ] of Object.entries(opts.mutators ?? {})) {
			this.mutators.set(name, mutator);
		}
		this.schemaRevision = opts.schemaRevision ?? '';
		this.chunkSize = opts.chunkSize ?? 100;
	}

	/**
	 * The Connect transport serving this server, for
	 * `SyncClientOptions.transport`. Every call returns the same instance.
	 */
	transport(): Transport {
		return (this.transportCache ??= createRouterTransport(({ service }) => {
			service(SyncService, {
				push: (req) => this.handlePush(req),
				pull: (req) => this.handlePull(req),
				bootstrap: (req) => this.handleBootstrap(req),
				subscribe: (req, ctx) => this.handleSubscribe(req, ctx.signal),
				query: () => {
					throw new ConnectError(
						'the syncplane test kit does not serve queries; use the integration suite for queryServer',
						Code.Unimplemented,
					);
				},
			});
		}));
	}

	/**
	 * Installs a rule rejecting matching mutations, the way an app's permission
	 * hook or mutator would. Returns a function that removes the rule; a rule
	 * with `once` removes itself after its first match.
	 *
	 * @param match - Which mutations to reject; omitted rejects every one.
	 * @param code - Rejection code the client sees. @defaultValue 'permission_denied'
	 */
	reject(
		match?: RejectMatch | RejectPredicate,
		code: string = CodePermissionDenied,
		message?: string,
	): () => void {
		const rule: RejectRule = {
			matches: typeof match === 'function' ? match : this.compileMatch(match ?? {}),
			once: typeof match === 'object' && match.once === true,
			code,
			message: message ?? `rejected by test rule (${code})`,
		};
		this.rules.add(rule);
		return () => this.rules.delete(rule);
	}

	/**
	 * Holds every push — received or future — at the server door until the
	 * returned hold is released; the client keeps queueing meanwhile. Holds
	 * nest: pushes proceed when every outstanding hold is released. Pull,
	 * bootstrap and subscribe are unaffected.
	 */
	holdPushes(): PushHold {
		this.holds++;
		let released = false;
		return {
			release: () => {
				if (released) return;
				released = true;
				if (--this.holds === 0) {
					const waiters = this.holdWaiters;
					this.holdWaiters = [];
					for (const wake of waiters) wake();
				}
			},
		};
	}

	/**
	 * Collects the workspace's tombstones and advances its GC horizon to the
	 * current sequence. A client that last synced before this call answers its
	 * next pull with `reset` and transparently re-bootstraps; a client already
	 * current keeps its cursor. Returns the number of tombstones collected.
	 */
	gc(workspaceId: string = DEFAULT_WORKSPACE): number {
		const state = this.ws(workspaceId);
		let collected = 0;
		for (const rows of state.rows.values()) {
			for (const [ key, row ] of rows) {
				if (row.value === undefined) {
					rows.delete(key);
					collected++;
				}
			}
		}
		state.horizon = state.seq;
		return collected;
	}

	/**
	 * Denies the caller all access to a workspace, permanently as far as
	 * clients can tell: pushes fail with `workspace_access_revoked`, pulls and
	 * bootstraps fail as `permission_denied`, and a subscribe stream drops the
	 * workspace with reason ACCESS_REVOKED. The client surfaces it as
	 * `status().workspaces[id].access === 'revoked'`. {@link TestServer.restore}
	 * undoes it.
	 */
	revoke(workspaceId: string): void {
		this.revoked.add(workspaceId);
		this.ws(workspaceId).wake();
	}

	/** Lifts a {@link TestServer.revoke}. Clients notice on their next cycle. */
	restore(workspaceId: string): void {
		this.revoked.delete(workspaceId);
	}

	/**
	 * Sets what clients are told the caller holds in a workspace under the
	 * access model, which `SyncClient.can`, `SyncClient.hasPermission` and the
	 * React access hooks answer from. `subjects` are what the rules' subject
	 * checks compare with, and the first is what a client stores in a creator
	 * field of a row it creates. Every bootstrap, pull and stream page after
	 * this call carries it, and `undefined` stops sending one; a client keeps
	 * the last one it received. Nothing here is enforced: the server still
	 * applies every write, serves every row, and stores a creator field as the
	 * client wrote it.
	 *
	 * @example
	 * ```ts
	 * server.setCallerAccess({
	 *   subjects: [ 'ann' ],
	 *   permissions: [
	 *     { permission: 'view_tasks', root: true },
	 *     { permission: 'edit_tasks', anchors: [ { entity: 'project', keys: [ 'p1' ] } ] },
	 *   ],
	 * });
	 * ```
	 */
	setCallerAccess(access: TestCallerAccess | undefined, opts?: { workspace?: string }): void {
		const workspace = opts?.workspace ?? DEFAULT_WORKSPACE;
		if (access === undefined) {
			this.callerAccess.delete(workspace);
			return;
		}
		this.callerAccess.set(workspace, callerAccessOf(access));
		this.ws(workspace).wake();
	}

	/** Registers a custom mutator; see {@link TestMutator}. */
	mutator(name: string, mutator: TestMutator): void {
		this.mutators.set(name, mutator);
	}

	/**
	 * Writes a row directly into server state, bypassing watermarks and reject
	 * rules — seeding, or a change made by "someone else". Consumes a sequence
	 * number, so synced clients see it as an ordinary remote change on their
	 * next cycle. Returns the row key.
	 */
	put<D extends DescMessage>(
		schema: D,
		value: MessageInitShape<D>,
		opts?: { workspace?: string },
	): string {
		const def = this.registry.bySchema(schema);
		const state = this.ws(opts?.workspace ?? DEFAULT_WORKSPACE);
		const msg = create(schema, value);
		const key = keyOf(def, msg);
		state.seq++;
		state.rowsOf(def.name).set(key, { version: state.seq, value: toBinary(schema, msg) });
		state.wake();
		return key;
	}

	/** Tombstones a row directly in server state; see {@link TestServer.put}. */
	del(schema: DescMessage, key: string, opts?: { workspace?: string }): void {
		const def = this.registry.bySchema(schema);
		const state = this.ws(opts?.workspace ?? DEFAULT_WORKSPACE);
		state.seq++;
		state.rowsOf(def.name).set(key, { version: state.seq, value: undefined });
		state.wake();
	}

	/** The server's committed row, or undefined when absent or tombstoned. */
	get<D extends DescMessage>(
		schema: D,
		key: string,
		opts?: { workspace?: string },
	): MessageShape<D> | undefined {
		const def = this.registry.bySchema(schema);
		const row = this.ws(opts?.workspace ?? DEFAULT_WORKSPACE).rows.get(def.name)?.get(key);
		return row?.value !== undefined ? fromBinary(schema, row.value) : undefined;
	}

	/** The server's committed live rows of an entity, in key order. */
	rows<D extends DescMessage>(schema: D, opts?: { workspace?: string }): MessageShape<D>[] {
		const def = this.registry.bySchema(schema);
		const rows = this.ws(opts?.workspace ?? DEFAULT_WORKSPACE).rows.get(def.name) ?? new Map();
		return [ ...(rows as Map<string, StoredRow>) ]
			.filter(([ , row ]) => row.value !== undefined)
			.sort(([ a ], [ b ]) => (a < b ? -1 : 1))
			.map(([ , row ]) => fromBinary(schema, row.value!));
	}

	private ws(id: string): WorkspaceState {
		let state = this.workspaces.get(id);
		if (!state) {
			state = new WorkspaceState();
			this.workspaces.set(id, state);
		}
		return state;
	}

	private compileMatch(match: RejectMatch): (mutation: Mutation, workspaceId: string) => boolean {
		// Resolved now, not per push, so a schema for an unregistered entity
		// fails at reject() where the typo is.
		const entity = typeof match.entity === 'object'
			? this.registry.bySchema(match.entity).name
			: match.entity;
		const op = match.op !== undefined ? opNames[match.op] : undefined;
		const crudMatches = (crud: CrudMutation): boolean =>
			(entity === undefined || crud.entity === entity)
			&& (match.key === undefined || crud.key === match.key)
			&& (op === undefined || crud.op === op);
		return (mutation) => {
			if (match.clientId !== undefined && mutation.clientId !== match.clientId) return false;
			switch (mutation.payload.case) {
				case 'crud':
					return match.mutator === undefined && crudMatches(mutation.payload.value);
				case 'batch':
					return match.mutator === undefined
						&& mutation.payload.value.mutations.some(crudMatches);
				case 'custom':
					return entity === undefined && match.key === undefined && op === undefined
						&& (match.mutator === undefined || mutation.payload.value.name === match.mutator);
				default:
					return false;
			}
		};
	}

	private pushGate(): Promise<void> {
		if (this.holds === 0) return Promise.resolve();
		return new Promise((resolve) => this.holdWaiters.push(resolve));
	}

	private async handlePush(req: PushRequest) {
		this.pushes.push(req);
		await this.pushGate();
		const results = [];
		for (const batch of req.batches) {
			results.push(await this.pushWorkspace(batch.workspaceId, batch.mutations));
		}
		return { results, schemaRevision: this.schemaRevision };
	}

	private async pushWorkspace(workspaceId: string, mutations: Mutation[]) {
		const fail = (errorCode: string, errorMessage: string) => ({
			workspaceId,
			results: [],
			errorCode,
			errorMessage,
		});
		if (this.revoked.has(workspaceId)) {
			return fail(CodeWorkspaceAccessRevoked, `workspace "${workspaceId}": access denied`);
		}
		const state = this.ws(workspaceId);
		const results = [];
		let applied = false;
		for (const m of mutations) {
			if (m.clientId === '' || m.mutationId === 0n || m.mutationId > 0x7fffffffffffffffn) {
				return fail(
					CodeInvalidArgument,
					`workspace "${workspaceId}": mutation needs client_id and a mutation_id from 1 to 9223372036854775807`,
				);
			}
			const last = state.watermarks.get(m.clientId) ?? 0n;
			if (m.mutationId <= last) {
				// The watermark already covers this id; replay a recorded
				// rejection so a lost response cannot turn a "no" into a
				// silent drop (mirrors engine/server/push.go).
				const verdict = state.verdicts.get(m.clientId)?.get(m.mutationId);
				results.push(
					verdict
						? {
							mutationId: m.mutationId,
							status: MutationStatus.REJECTED,
							errorCode: verdict.code,
							errorMessage: verdict.message,
						}
						: { mutationId: m.mutationId, status: MutationStatus.ALREADY_APPLIED },
				);
				continue;
			}
			// A watermark of 0 adopts any starting id — the engine's recovery
			// for clients whose watermark row was GC'd.
			if (last !== 0n && m.mutationId !== last + 1n) {
				return fail(
					CodeMutationIdGap,
					`workspace "${workspaceId}": mutation id gap: got ${m.mutationId}, want ${last + 1n}`,
				);
			}
			try {
				const seq = await this.applyMutation(state, workspaceId, m);
				applied = true;
				results.push({ mutationId: m.mutationId, status: MutationStatus.APPLIED, seq });
			} catch (err) {
				if (!(err instanceof Rejection)) throw err;
				results.push({
					mutationId: m.mutationId,
					status: MutationStatus.REJECTED,
					errorCode: err.code,
					errorMessage: err.message,
				});
				let verdicts = state.verdicts.get(m.clientId);
				if (!verdicts) {
					verdicts = new Map();
					state.verdicts.set(m.clientId, verdicts);
				}
				verdicts.set(m.mutationId, { code: err.code, message: err.message });
			}
			// Rejection is an ack: the watermark advances either way.
			state.watermarks.set(m.clientId, m.mutationId);
		}
		if (applied) state.wake();
		return { workspaceId, results };
	}

	/** Applies one mutation envelope; throws {@link Rejection} without side effects. */
	private async applyMutation(
		state: WorkspaceState,
		workspaceId: string,
		mutation: Mutation,
	): Promise<bigint> {
		for (const rule of this.rules) {
			if (rule.matches(mutation, workspaceId)) {
				if (rule.once) this.rules.delete(rule);
				throw new Rejection(rule.code, rule.message);
			}
		}
		const seq = state.seq + 1n;
		const staging = new Staging(state);
		switch (mutation.payload.case) {
			case 'crud':
				this.applyCrud(staging, mutation.payload.value, seq);
				break;
			case 'batch': {
				const cruds = mutation.payload.value.mutations;
				if (cruds.length === 0) {
					throw new Rejection(CodeInvalidArgument, 'batch must be non-empty');
				}
				for (const crud of cruds) this.applyCrud(staging, crud, seq);
				break;
			}
			case 'custom': {
				const custom = mutation.payload.value;
				const mutator = this.mutators.get(custom.name);
				if (!mutator) {
					throw new Rejection(CodeInvalidArgument, `unknown mutator "${custom.name}"`);
				}
				const args = mutator.args && custom.args
					? (anyUnpack(custom.args, mutator.args) as Message)
					: undefined;
				await mutator.apply(new MutatorTx(staging, this.registry, seq), args);
				break;
			}
			default:
				throw new Rejection(CodeInvalidArgument, 'mutation has no payload');
		}
		staging.commit();
		state.seq = seq;
		return seq;
	}

	private applyCrud(staging: Staging, crud: CrudMutation, seq: bigint): void {
		const def = this.registry.get(crud.entity);
		if (!def) throw new Rejection(CodeInvalidArgument, `unknown entity "${crud.entity}"`);
		const current = staging.get(crud.entity, crud.key);
		const liveVersion = current !== undefined && current.value !== undefined ? current.version : 0n;
		if (crud.expectedVersion !== undefined && crud.expectedVersion !== liveVersion) {
			throw new Rejection(
				CodeConflict,
				`${crud.entity} "${crud.key}": expected version ${crud.expectedVersion}, have ${liveVersion}`,
			);
		}
		switch (crud.op) {
			case CrudOp.PUT: {
				let value = crud.value;
				// The PUT exception: collaborative-text fields union with the
				// stored document instead of replacing it.
				const textPaths = this.registry.textPathsOf(def.name);
				if (textPaths.length > 0 && current?.value !== undefined) {
					const base = fromBinary(def.schema, current.value);
					const next = fromBinary(def.schema, crud.value);
					mergeTextFieldsInto(def.schema, textPaths, base, next);
					value = toBinary(def.schema, next);
				}
				staging.set(crud.entity, crud.key, { version: seq, value });
				return;
			}
			case CrudOp.PATCH: {
				// Delete-wins: a PATCH does not resurrect an absent or deleted row.
				if (current?.value === undefined) {
					throw new Rejection(CodeConflict, `${crud.entity} "${crud.key}" does not exist`);
				}
				const paths = crud.fieldMask?.paths ?? [];
				const dst = fromBinary(def.schema, current.value);
				const src = fromBinary(def.schema, crud.value);
				try {
					mergeMask(def.schema, dst, src, paths, this.registry.mergersFor(def.name));
				} catch (err) {
					const detail = err instanceof Error ? err.message : String(err);
					if (paths.some((p) => textSchemaPathOf(def.schema, p) !== undefined)) {
						throw new Rejection(
							CodeTextFullStateRequired,
							`${crud.entity} "${crud.key}": ${detail}`,
						);
					}
					throw new Rejection(CodeInvalidArgument, `${crud.entity} "${crud.key}": ${detail}`);
				}
				staging.set(crud.entity, crud.key, { version: seq, value: toBinary(def.schema, dst) });
				return;
			}
			case CrudOp.DELETE:
				staging.set(crud.entity, crud.key, { version: seq, value: undefined });
				return;
			default:
				throw new Rejection(CodeInvalidArgument, `unknown crud op ${crud.op}`);
		}
	}

	private assertAccess(workspaceId: string): void {
		if (!this.revoked.has(workspaceId)) return;
		const err = new ConnectError(
			`workspace "${workspaceId}": access denied`,
			Code.PermissionDenied,
		);
		err.metadata.set(ErrorCodeMetadataKey, CodeWorkspaceAccessRevoked);
		throw err;
	}

	/** One forward page from `from`, in commit order. */
	private page(
		workspaceId: string,
		state: WorkspaceState,
		clientId: string,
		from: bigint,
		limit: number,
	) {
		const changed: { entity: string; key: string; row: StoredRow }[] = [];
		for (const [ entity, rows ] of state.rows) {
			for (const [ key, row ] of rows) {
				if (row.version > from) changed.push({ entity, key, row });
			}
		}
		changed.sort((a, b) => (a.row.version < b.row.version ? -1 : 1));
		const max = limit > 0 ? limit : Infinity;
		const hasMore = changed.length > max;
		const included = hasMore ? changed.slice(0, max) : changed;
		const through = hasMore
			? included[included.length - 1]!.row.version
			: state.seq > from
			? state.seq
			: from;
		const access = this.callerAccess.get(workspaceId);
		return {
			patch: {
				changes: included.map((c) => rowChangeInit(c.entity, c.key, c.row)),
				dropEntities: [],
			},
			nextCursor: encodeCursor(through),
			lastAckedMutationId: state.watermarks.get(clientId) ?? 0n,
			reset: false,
			hasMore,
			seq: through,
			schemaRevision: this.schemaRevision,
			...(access === undefined ? {} : { callerAccess: { ...access, seq: through } }),
		};
	}

	private handlePull(req: PullRequest) {
		this.assertAccess(req.workspaceId);
		const state = this.ws(req.workspaceId);
		const from = decodeCursor(req.cursor);
		if (from === undefined || from < state.horizon) {
			return {
				nextCursor: new Uint8Array(),
				lastAckedMutationId: state.watermarks.get(req.clientId) ?? 0n,
				reset: true,
				hasMore: false,
				seq: 0n,
				schemaRevision: this.schemaRevision,
			};
		}
		return this.page(req.workspaceId, state, req.clientId, from, req.limit);
	}

	private async *handleBootstrap(req: BootstrapRequest) {
		this.assertAccess(req.workspaceId);
		const state = this.ws(req.workspaceId);
		const known = this.registry.syncableNames();
		const priority = req.priorityEntities.filter((name) => known.includes(name));
		const order = [ ...priority, ...known.filter((name) => !priority.includes(name)) ];
		yield create(BootstrapResponseSchema, {
			body: {
				case: 'begin',
				value: {
					cursor: encodeCursor(state.seq),
					lastAckedMutationId: state.watermarks.get(req.clientId) ?? 0n,
					seq: state.seq,
					schemaRevision: this.schemaRevision,
					entities: order,
					resumed: false,
					...(this.callerAccess.has(req.workspaceId)
						? { callerAccess: { ...this.callerAccess.get(req.workspaceId)!, seq: state.seq } }
						: {}),
				},
			},
		});
		for (const entity of order) {
			const live = [ ...(state.rows.get(entity) ?? new Map<string, StoredRow>()) ]
				.filter(([ , row ]) => row.value !== undefined)
				.sort(([ a ], [ b ]) => (a < b ? -1 : 1));
			for (let i = 0; i < live.length; i += this.chunkSize) {
				yield create(BootstrapResponseSchema, {
					body: {
						case: 'chunk',
						value: {
							rows: live.slice(i, i + this.chunkSize)
								.map(([ key, row ]) => rowChangeInit(entity, key, row)),
						},
					},
				});
			}
		}
		yield create(BootstrapResponseSchema, { body: { case: 'end', value: {} } });
	}

	private async *handleSubscribe(req: SubscribeRequest, signal: AbortSignal) {
		const cursors = new Map<string, bigint>();
		const pending = [ ...req.scopes ];
		const drop = (workspaceId: string, reason: WorkspaceDropped_Reason, message: string) => {
			cursors.delete(workspaceId);
			return create(SubscribeResponseSchema, {
				workspaceId,
				schemaRevision: this.schemaRevision,
				body: { case: 'dropped', value: { reason, message } },
			});
		};
		// Catch-up: pages until drained, then caughtUp, per scope.
		for (const scope of pending) {
			const state = this.ws(scope.workspaceId);
			if (this.revoked.has(scope.workspaceId)) {
				yield drop(
					scope.workspaceId,
					WorkspaceDropped_Reason.ACCESS_REVOKED,
					`workspace "${scope.workspaceId}": access denied`,
				);
				continue;
			}
			const from = decodeCursor(scope.cursor);
			if (from === undefined || from < state.horizon) {
				yield drop(scope.workspaceId, WorkspaceDropped_Reason.RESET, 'cursor predates the horizon');
				continue;
			}
			cursors.set(scope.workspaceId, from);
			yield* this.pageFrames(scope.workspaceId, cursors, req);
			yield create(SubscribeResponseSchema, {
				workspaceId: scope.workspaceId,
				schemaRevision: this.schemaRevision,
				body: { case: 'caughtUp', value: true },
			});
		}
		// Live tail: park until a commit touches a subscribed workspace.
		while (cursors.size > 0 && !signal.aborted) {
			await new Promise<void>((resolve) => {
				let done = false;
				const fire = () => {
					if (done) return;
					done = true;
					signal.removeEventListener('abort', fire);
					for (const id of cursors.keys()) this.ws(id).wakers.delete(fire);
					resolve();
				};
				signal.addEventListener('abort', fire, { once: true });
				for (const id of cursors.keys()) this.ws(id).wakers.add(fire);
			});
			if (signal.aborted) return;
			for (const workspaceId of [ ...cursors.keys() ]) {
				if (this.revoked.has(workspaceId)) {
					yield drop(
						workspaceId,
						WorkspaceDropped_Reason.ACCESS_REVOKED,
						`workspace "${workspaceId}": access denied`,
					);
					continue;
				}
				yield* this.pageFrames(workspaceId, cursors, req);
			}
		}
	}

	private *pageFrames(
		workspaceId: string,
		cursors: Map<string, bigint>,
		req: SubscribeRequest,
	) {
		const state = this.ws(workspaceId);
		while (state.seq > cursors.get(workspaceId)!) {
			const page = this.page(workspaceId, state, req.clientId, cursors.get(workspaceId)!, req.limit);
			cursors.set(workspaceId, page.seq);
			yield create(SubscribeResponseSchema, {
				workspaceId,
				schemaRevision: this.schemaRevision,
				body: { case: 'page', value: page },
			});
		}
	}
}

/** Builds a {@link TestServer}. */
export function createTestServer<const E extends readonly DescMessage[]>(
	opts: TestServerOptions<E>,
): TestServer<E> {
	return new TestServer(opts);
}
