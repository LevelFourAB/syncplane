import {
	clearField,
	create,
	type DescMessage,
	fromBinary,
	isFieldSet,
	type Message,
	type MessageInitShape,
	type MessageShape,
	toBinary,
} from '@bufbuild/protobuf';
import { anyUnpack } from '@bufbuild/protobuf/wkt';
import { Code, ConnectError, createRouterTransport, type Transport } from '@connectrpc/connect';

import {
	type AccessChain,
	accessRowId,
	AccessSummary,
	decideRules,
	MAX_ACCESS_CHAIN,
	parentRefsOf,
	type RuleRow,
	ruleRowOf,
} from '../access.js';
import {
	CodeConflict,
	CodeInvalidArgument,
	CodeMutationIdGap,
	CodePermissionDenied,
	CodeTextFullStateRequired,
	CodeWorkspaceAccessRevoked,
	CodeWorkspaceNoAccess,
	ErrorCodeMetadataKey,
} from '../codes.js';
import { type CrudMutation, CrudOp, type Mutation } from '../gen/syncplane/v1/mutation_pb.js';
import {
	type BootstrapRequest,
	BootstrapResponseSchema,
	type CallerAccess,
	type CallerAccessDelta,
	CallerAccessDeltaSchema,
	CallerAccessSchema,
	MutationStatus,
	type PullRequest,
	type PushRequest,
	type SubscribeRequest,
	SubscribeResponseSchema,
	SyncService,
	WorkspaceDropped_Reason,
} from '../gen/syncplane/v1/service_pb.js';
import {
	type EntityDef,
	EntityRegistry,
	keyOf,
	mergeMask,
	type MergerSpec,
	mergeTextFieldsInto,
	textSchemaPathOf,
} from '../mutations.js';
import { accessDeltaOf, callerAccessOf, RoleTable, type TestCallerAccess } from './access.js';

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
	/**
	 * Which rows bootstrap and pull send. `'all'` sends every row.
	 * `'readable'` sends a row of an entity with access rules only while a
	 * read rule admits the caller that {@link TestServer.setCallerAccess} sets
	 * for the workspace, and none before it is set. A field that a field guard
	 * hides from the caller arrives cleared and named in `RowChange.hidden`. A
	 * row the caller stops reading arrives as a tombstone, and one it starts
	 * reading, or whose hidden fields change, arrives on the next pull, as on
	 * the real server. Writes stay unchecked.
	 * @defaultValue 'all'
	 */
	serve?: 'all' | 'readable';
	/**
	 * The server's `Limits.MaxAccessDepth`, which every CallerAccess the
	 * server sends carries. The client's access predictions follow as many
	 * access parent hops as it allows.
	 * @defaultValue 32
	 */
	maxAccessDepth?: number;
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
	/**
	 * clientId → entity name → row key → the fields hidden in the value the
	 * client was last sent of the row, joined by commas. Kept only when the
	 * server serves readable rows.
	 */
	served = new Map<string, Map<string, Map<string, string>>>();

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

/** Where a Subscribe scope is: its seq and the CallerAccess it holds, as `page` takes them. */
interface StreamPosition {
	seq: bigint;
	held: bigint;
	deltas: boolean;
}

/**
 * A cursor at `seq`, issued to a client that holds the CallerAccess whose
 * version is `access`, 0 for none, as the real server's cursor records it.
 */
function encodeCursor(seq: bigint, access: bigint): Uint8Array {
	return new TextEncoder().encode(`${cursorPrefix}${seq}:${access}`);
}

/**
 * The position of `cursor` and the version of the CallerAccess it was issued
 * under. An empty cursor is the start, issued under none. undefined means the
 * cursor was not minted by this server — answered as reset.
 */
function decodeCursor(cursor: Uint8Array): { seq: bigint; access: bigint } | undefined {
	if (cursor.length === 0) return { seq: 0n, access: 0n };
	const text = new TextDecoder().decode(cursor);
	if (!text.startsWith(cursorPrefix)) return undefined;
	const [ seq, access, ...rest ] = text.slice(cursorPrefix.length).split(':');
	if (seq === undefined || access === undefined || rest.length > 0) return undefined;
	try {
		return { seq: BigInt(seq), access: BigInt(access) };
	} catch {
		return undefined;
	}
}

function rowChangeInit(entity: string, key: string, row: StoredRow, hidden?: Hidden) {
	if (row.value === undefined || hidden === undefined || hidden.units.length === 0) {
		return {
			entity,
			key,
			version: row.version,
			body: row.value !== undefined
				? { case: 'value' as const, value: row.value }
				: { case: 'tombstone' as const, value: true },
		};
	}
	const { schema } = hidden.def;
	const msg = fromBinary(schema, row.value);
	for (const unit of hidden.units) {
		const oneof = schema.oneofs.find((o) => o.name === unit);
		for (const field of oneof?.fields ?? schema.fields.filter((f) => f.name === unit)) {
			clearField(msg, field);
		}
	}
	return {
		entity,
		key,
		version: row.version,
		body: { case: 'value' as const, value: toBinary(schema, msg) },
		hidden: [ ...hidden.units ],
	};
}

/** The fields of a row a guard hides from the caller, by proto name, sorted. */
interface Hidden {
	def: EntityDef;
	units: readonly string[];
}

/**
 * What the caller of a workspace reads of the committed rows, decided over
 * them; see TestServer.readableIn.
 */
interface ReadView {
	/** Whether the caller reads the live row of `entity` with `key`. */
	reads(entity: string, key: string): boolean;
	/** The fields of that row a guard hides from the caller. */
	hidden(entity: string, key: string): Hidden | undefined;
}

/** The fields `hidden` names, joined as WorkspaceState.served records them. */
function hiddenSig(hidden: Hidden | undefined): string {
	return hidden === undefined ? '' : hidden.units.join(',');
}

const opNames = { put: CrudOp.PUT, patch: CrudOp.PATCH, delete: CrudOp.DELETE } as const;

/** Records in `served` what a client holds after it applies `changes`. */
function markServed(
	served: Map<string, Map<string, string>>,
	changes: readonly { entity: string; key: string; row: StoredRow }[],
	view: ReadView,
): void {
	for (const { entity, key, row } of changes) {
		let keys = served.get(entity);
		if (keys === undefined) served.set(entity, keys = new Map());
		if (row.value === undefined) keys.delete(key);
		else keys.set(key, hiddenSig(view.hidden(entity, key)));
	}
}

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
 * are ignored — every row syncs, or every readable row under
 * {@link TestServerOptions.serve}), server queries (`queryServer` and friends
 * fail), presence rooms, and write checks beyond {@link TestServer.reject}
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
	/** Workspaces denied to the caller, by how: permanently, or until a grant. */
	private denied = new Map<string, 'revoked' | 'none'>();
	private callerAccess = new Map<string, CallerAccess>();
	/** Every CallerAccess set so far, by version: the bases a delta can apply to. */
	private accessVersions = new Map<bigint, CallerAccess>();
	private summaries = new WeakMap<CallerAccess, AccessSummary>();
	private roleTable: RoleTable;
	private serve: 'all' | 'readable';
	private maxAccessDepth: number;
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
		this.roleTable = new RoleTable(opts.entities);
		this.serve = opts.serve ?? 'all';
		this.maxAccessDepth = opts.maxAccessDepth ?? MAX_ACCESS_CHAIN;
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
		this.denied.set(workspaceId, 'revoked');
		this.ws(workspaceId).wake();
	}

	/**
	 * Answers as the server does for a caller who holds no permission in a
	 * workspace yet: pushes fail with `workspace_no_access`, pulls and
	 * bootstraps fail as `permission_denied`, and a subscribe stream drops the
	 * workspace with reason NO_ACCESS. The client surfaces it as
	 * `status().workspaces[id].access === 'none'` and keeps its queue.
	 * {@link TestServer.restore} stands for the grant that admits the caller.
	 */
	withholdAccess(workspaceId: string): void {
		this.denied.set(workspaceId, 'none');
		this.ws(workspaceId).wake();
	}

	/**
	 * Lifts a {@link TestServer.revoke} or {@link TestServer.withholdAccess}.
	 * Clients notice on their next cycle.
	 */
	restore(workspaceId: string): void {
		this.denied.delete(workspaceId);
	}

	/** The wire code and drop reason of a denied workspace, or undefined. */
	private denial(
		workspaceId: string,
	): { code: string; reason: WorkspaceDropped_Reason; message: string } | undefined {
		switch (this.denied.get(workspaceId)) {
			case 'revoked':
				return {
					code: CodeWorkspaceAccessRevoked,
					reason: WorkspaceDropped_Reason.ACCESS_REVOKED,
					message: `workspace "${workspaceId}": access denied`,
				};
			case 'none':
				return {
					code: CodeWorkspaceNoAccess,
					reason: WorkspaceDropped_Reason.NO_ACCESS,
					message: `workspace "${workspaceId}": the caller holds no permission`,
				};
			default:
				return undefined;
		}
	}

	/**
	 * Sets what clients are told the caller holds in a workspace under the
	 * access model, which `SyncClient.can`, `SyncClient.hasPermission` and the
	 * React access hooks answer from. `subjects` are what the rules' subject
	 * checks compare with, and the first is what a client stores in a creator
	 * field of a row it creates. Each bootstrap after this call carries it,
	 * and a pull or stream page carries it only when the client does not hold
	 * it yet, as the real server sends it: whole, or as a CallerAccessDelta
	 * from the summary the client holds when that is smaller. `undefined`
	 * stops sending one; a client keeps the last one it received. Writes are
	 * not checked against it: the server
	 * still applies every write and stores a creator field as the client wrote
	 * it. Rows are filtered by it only under {@link TestServerOptions.serve}
	 * `'readable'`.
	 *
	 * Throws when a role is not declared by a registered entity, or is held at
	 * a row of an entity that its grant rows cannot point at.
	 *
	 * @example
	 * ```ts
	 * server.setCallerAccess({
	 *   subjects: [ 'ann' ],
	 *   roles: [ { role: 'project_admin', at: [ ProjectSchema, 'p1' ] } ],
	 *   permissions: [ { permission: 'view_labels', root: true } ],
	 * });
	 * ```
	 */
	setCallerAccess(access: TestCallerAccess | undefined, opts?: { workspace?: string }): void {
		const workspace = opts?.workspace ?? DEFAULT_WORKSPACE;
		if (access === undefined) {
			this.callerAccess.delete(workspace);
			this.ws(workspace).wake();
			return;
		}
		const fromRoles = this.roleTable.permissionsOf(access.roles ?? []);
		const summary = callerAccessOf({
			...(access.subjects === undefined ? {} : { subjects: access.subjects }),
			permissions: [ ...fromRoles, ...(access.permissions ?? []) ],
		}, this.maxAccessDepth);
		this.callerAccess.set(workspace, summary);
		this.accessVersions.set(summary.version, summary);
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
		const denial = this.denial(workspaceId);
		if (denial) return fail(denial.code, denial.message);
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
		const view = this.readableIn(workspaceId, state);
		switch (mutation.payload.case) {
			case 'crud':
				this.applyCrud(staging, mutation.payload.value, seq, view);
				break;
			case 'batch': {
				const cruds = mutation.payload.value.mutations;
				if (cruds.length === 0) {
					throw new Rejection(CodeInvalidArgument, 'batch must be non-empty');
				}
				for (const crud of cruds) this.applyCrud(staging, crud, seq, view);
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

	/**
	 * Applies one CRUD. `view` is what the caller reads of the committed rows
	 * when the server serves readable rows: a PUT that updates a row keeps the
	 * stored value of each field a guard hides from the caller, as on the real
	 * server.
	 */
	private applyCrud(
		staging: Staging,
		crud: CrudMutation,
		seq: bigint,
		view: ReadView | undefined,
	): void {
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
				const hidden = current?.value === undefined ? undefined : view?.hidden(crud.entity, crud.key);
				if (hidden !== undefined) {
					const stored = fromBinary(def.schema, current!.value!);
					const next = fromBinary(def.schema, value);
					// The caller received each hidden field cleared, so one the put
					// leaves unset keeps its stored value, and a set one is a value
					// the caller wrote.
					const dst = next as Record<string, unknown>;
					const src = stored as Record<string, unknown>;
					for (const unit of hidden.units) {
						const oneof = def.schema.oneofs.find((o) => o.name === unit);
						if (oneof !== undefined) {
							if ((dst[oneof.localName] as { case?: string }).case === undefined) {
								dst[oneof.localName] = src[oneof.localName];
							}
							continue;
						}
						const field = def.schema.fields.find((f) => f.name === unit)!;
						if (!isFieldSet(next, field)) dst[field.localName] = src[field.localName];
					}
					value = toBinary(def.schema, next);
				}
				// The PUT exception: collaborative-text fields union with the
				// stored document instead of replacing it.
				const textPaths = this.registry.textPathsOf(def.name);
				if (textPaths.length > 0 && current?.value !== undefined) {
					const base = fromBinary(def.schema, current.value);
					const next = fromBinary(def.schema, value);
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

	/**
	 * What the caller of `workspaceId` reads of the live rows, decided over the
	 * committed rows; undefined when the server serves every row. The view
	 * keeps each answer, so ask for a new one after a commit.
	 */
	private readableIn(
		workspaceId: string,
		state: WorkspaceState,
	): ReadView | undefined {
		if (this.serve !== 'readable') return undefined;
		const access = this.callerAccess.get(workspaceId);
		let summary: AccessSummary | undefined;
		if (access !== undefined) {
			summary = this.summaries.get(access);
			if (summary === undefined) this.summaries.set(access, summary = new AccessSummary(access));
		}
		const decoded = new Map<string, Message | undefined>();
		const rowOf = (def: EntityDef, key: string): Message | undefined => {
			const id = accessRowId({ entity: def.name, key });
			if (!decoded.has(id)) {
				const value = state.rows.get(def.name)?.get(key)?.value;
				decoded.set(id, value === undefined ? undefined : fromBinary(def.schema, value));
			}
			return decoded.get(id);
		};
		const verdicts = new Map<string, boolean>();
		const hiddenRows = new Map<string, Hidden | undefined>();
		// The rule row of a live row, as decideRules reads it.
		const ruleRow = (def: EntityDef, key: string, row: Message): RuleRow => {
			// The server holds every row, so a row it does not hold above
			// this one does not exist and leads nowhere, where a client
			// would read it as not known.
			const chain: AccessChain = {
				parents: parentRefsOf(def, row),
				maxHops: summary!.maxAccessDepth,
				above: (at) => {
					const atDef = this.registry.get(at.entity);
					const atRow = atDef === undefined ? undefined : rowOf(atDef, at.key);
					return atRow === undefined ? [] : parentRefsOf(atDef!, atRow) ?? [];
				},
				row: (at) => {
					const atDef = this.registry.get(at.entity);
					const atRow = atDef === undefined ? undefined : rowOf(atDef, at.key);
					return atRow === undefined ? undefined : { def: atDef!, row: atRow };
				},
			};
			return ruleRowOf(summary!, def, key, row, chain);
		};
		return {
			reads: (entity, key) => {
				const def = this.registry.get(entity);
				// An entity outside the model is open to every caller in the workspace.
				if (def?.accessRules === undefined) return true;
				if (summary === undefined) return false;
				const id = accessRowId({ entity, key });
				let verdict = verdicts.get(id);
				if (verdict === undefined) {
					const row = rowOf(def, key);
					verdict = row !== undefined
						&& decideRules(def.accessRules, 'read', summary.subjects, ruleRow(def, key, row)) === 'yes';
					verdicts.set(id, verdict);
				}
				return verdict;
			},
			hidden: (entity, key) => {
				const def = this.registry.get(entity);
				if (def === undefined || def.guards.length === 0 || summary === undefined) return undefined;
				const id = accessRowId({ entity, key });
				if (hiddenRows.has(id)) return hiddenRows.get(id);
				const row = rowOf(def, key);
				let hidden: Hidden | undefined;
				if (row !== undefined) {
					const at = ruleRow(def, key, row);
					const units = def.guards
						.filter((g) => decideRules(g.rules, 'read', summary.subjects, at) !== 'yes')
						.flatMap((g) => g.units)
						.sort();
					if (units.length > 0) hidden = { def, units };
				}
				hiddenRows.set(id, hidden);
				return hidden;
			},
		};
	}

	/** The rows a client was sent a value of, by entity; see WorkspaceState.served. */
	private servedTo(state: WorkspaceState, clientId: string): Map<string, Map<string, string>> {
		let served = state.served.get(clientId);
		if (served === undefined) state.served.set(clientId, served = new Map());
		return served;
	}

	private assertAccess(workspaceId: string): void {
		const denial = this.denial(workspaceId);
		if (!denial) return;
		const err = new ConnectError(denial.message, Code.PermissionDenied);
		err.metadata.set(ErrorCodeMetadataKey, denial.code);
		throw err;
	}

	/**
	 * One forward page from `from`, in commit order, for a client that holds
	 * the CallerAccess whose version is `held`, 0 for none. The page carries
	 * the current CallerAccess only when its version is not `held`, and with
	 * `deltas` as a CallerAccessDelta where one is smaller, as the real server
	 * sends it.
	 */
	private page(
		workspaceId: string,
		state: WorkspaceState,
		clientId: string,
		from: bigint,
		limit: number,
		held: bigint,
		deltas: boolean,
	) {
		const readable = this.readableIn(workspaceId, state);
		const changed: { entity: string; key: string; row: StoredRow }[] = [];
		for (const [ entity, rows ] of state.rows) {
			for (const [ key, row ] of rows) {
				if (row.version <= from) continue;
				if (readable !== undefined && row.value !== undefined && !readable.reads(entity, key)) continue;
				changed.push({ entity, key, row });
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
		// What the caller reads can change with no change to a row: a new
		// access set, or a change to a row above. The rows whose answer moved
		// since the client was last sent them go first, outside the limit.
		const resync: { entity: string; key: string; row: StoredRow }[] = [];
		if (readable !== undefined) {
			const served = this.servedTo(state, clientId);
			for (const [ entity, keys ] of served) {
				for (const [ key, sig ] of keys) {
					const row = state.rows.get(entity)?.get(key);
					if (row?.value === undefined) {
						// A delete after `from` is in `changed` already.
						if (row === undefined || row.version <= from) {
							resync.push({ entity, key, row: { version: state.seq, value: undefined } });
						}
					} else if (!readable.reads(entity, key)) {
						resync.push({ entity, key, row: { version: row.version, value: undefined } });
					} else if (row.version <= from && hiddenSig(readable.hidden(entity, key)) !== sig) {
						resync.push({ entity, key, row });
					}
				}
			}
			for (const [ entity, rows ] of state.rows) {
				for (const [ key, row ] of rows) {
					if (
						row.version <= from && row.value !== undefined && served.get(entity)?.has(key) !== true
						&& readable.reads(entity, key)
					) resync.push({ entity, key, row });
				}
			}
			markServed(served, resync, readable);
			markServed(served, included, readable);
		}
		const access = this.accessUpdate(workspaceId, held, deltas, through);
		return {
			patch: {
				changes: [ ...resync, ...included ].map((c) =>
					rowChangeInit(c.entity, c.key, c.row, readable?.hidden(c.entity, c.key))
				),
				dropEntities: [],
			},
			nextCursor: encodeCursor(through, access.holds),
			lastAckedMutationId: state.watermarks.get(clientId) ?? 0n,
			reset: false,
			hasMore,
			seq: through,
			schemaRevision: this.schemaRevision,
			...(access.whole === undefined ? {} : { callerAccess: access.whole }),
			...(access.delta === undefined ? {} : { callerAccessDelta: access.delta }),
		};
	}

	/**
	 * What a page current through `seq` sends of the CallerAccess of
	 * `workspaceId` to a client that holds the one whose version is `held`:
	 * nothing when the versions are equal or none is set, else the whole
	 * summary, or with `deltas` the change from `held` when the server knows
	 * that summary and the change is smaller. `holds` is the version the
	 * client holds after the page.
	 */
	private accessUpdate(
		workspaceId: string,
		held: bigint,
		deltas: boolean,
		seq: bigint,
	): { whole?: CallerAccess; delta?: CallerAccessDelta; holds: bigint } {
		const now = this.callerAccess.get(workspaceId);
		if (now === undefined || now.version === held) return { holds: held };
		const whole = { ...now, seq };
		const base = deltas && held !== 0n ? this.accessVersions.get(held) : undefined;
		const delta = base === undefined ? undefined : accessDeltaOf(base, now);
		if (
			delta === undefined
			|| toBinary(CallerAccessDeltaSchema, delta).length
				>= toBinary(CallerAccessSchema, { ...now, seq: 0n, version: 0n }).length
		) return { whole, holds: now.version };
		return { delta: { ...delta, seq }, holds: now.version };
	}

	private handlePull(req: PullRequest) {
		this.assertAccess(req.workspaceId);
		const state = this.ws(req.workspaceId);
		const cursor = decodeCursor(req.cursor);
		if (cursor === undefined || cursor.seq < state.horizon) {
			return {
				nextCursor: new Uint8Array(),
				lastAckedMutationId: state.watermarks.get(req.clientId) ?? 0n,
				reset: true,
				hasMore: false,
				seq: 0n,
				schemaRevision: this.schemaRevision,
			};
		}
		const deltas = req.callerAccessVersion !== undefined;
		const held = req.callerAccessVersion ?? (req.cursor.length === 0 ? 0n : cursor.access);
		return this.page(req.workspaceId, state, req.clientId, cursor.seq, req.limit, held, deltas);
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
					cursor: encodeCursor(state.seq, this.callerAccess.get(req.workspaceId)?.version ?? 0n),
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
		const readable = this.readableIn(req.workspaceId, state);
		const served = readable === undefined ? undefined : new Map<string, Map<string, string>>();
		if (served !== undefined) state.served.set(req.clientId, served);
		for (const entity of order) {
			const live = [ ...(state.rows.get(entity) ?? new Map<string, StoredRow>()) ]
				.filter(([ key, row ]) => row.value !== undefined && (readable?.reads(entity, key) ?? true))
				.sort(([ a ], [ b ]) => (a < b ? -1 : 1));
			if (served !== undefined) {
				served.set(
					entity,
					new Map(live.map(([ key ]) => [ key, hiddenSig(readable!.hidden(entity, key)) ])),
				);
			}
			for (let i = 0; i < live.length; i += this.chunkSize) {
				yield create(BootstrapResponseSchema, {
					body: {
						case: 'chunk',
						value: {
							rows: live.slice(i, i + this.chunkSize)
								.map(([ key, row ]) => rowChangeInit(entity, key, row, readable?.hidden(entity, key))),
						},
					},
				});
			}
		}
		yield create(BootstrapResponseSchema, { body: { case: 'end', value: {} } });
	}

	private async *handleSubscribe(req: SubscribeRequest, signal: AbortSignal) {
		const cursors = new Map<string, StreamPosition>();
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
			const denial = this.denial(scope.workspaceId);
			if (denial) {
				yield drop(scope.workspaceId, denial.reason, denial.message);
				continue;
			}
			const cursor = decodeCursor(scope.cursor);
			if (cursor === undefined || cursor.seq < state.horizon) {
				yield drop(scope.workspaceId, WorkspaceDropped_Reason.RESET, 'cursor predates the horizon');
				continue;
			}
			cursors.set(scope.workspaceId, {
				seq: cursor.seq,
				held: scope.callerAccessVersion ?? (scope.cursor.length === 0 ? 0n : cursor.access),
				deltas: scope.callerAccessVersion !== undefined,
			});
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
				const denial = this.denial(workspaceId);
				if (denial) {
					yield drop(workspaceId, denial.reason, denial.message);
					continue;
				}
				yield* this.pageFrames(workspaceId, cursors, req);
			}
		}
	}

	private *pageFrames(
		workspaceId: string,
		cursors: Map<string, StreamPosition>,
		req: SubscribeRequest,
	) {
		const state = this.ws(workspaceId);
		for (;;) {
			const at = cursors.get(workspaceId)!;
			const page = this.page(workspaceId, state, req.clientId, at.seq, req.limit, at.held, at.deltas);
			// A page can carry rows the caller started or stopped reading, or a
			// new CallerAccess, with no new commit.
			const access = page.callerAccess ?? page.callerAccessDelta;
			if (page.seq === at.seq && page.patch.changes.length === 0 && access === undefined) return;
			cursors.set(workspaceId, {
				seq: page.seq,
				held: access?.version ?? at.held,
				deltas: at.deltas,
			});
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
