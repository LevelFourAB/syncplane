// hasPermission(), can() and canCreate() answer from the CallerAccess stored
// for a workspace and from the pending queue, and a screen of buttons asks
// them again on every status change. These specs pin what the answers cost,
// that a remembered answer follows every write that changes it, that an access
// parent this client does not register still decides, how can() fills in
// the inputs of the access rules from local rows, that predictions asked
// together share one read of the store and answer as they do one at a time,
// that the stored CallerAccess is read and decoded again only when it
// changes, and that the pending queue is read only when it can hold a write
// that changes the grants.

import { create, type DescMessage, type MessageInitShape, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it, vi } from 'vitest';

import { type CanAnswer, countAccessChanges } from '../../src/access.js';
import { SyncClient } from '../../src/client.js';
import {
	AccountSchema,
	BinderSchema,
	BulletinSchema,
	CardItemSchema,
	CardSchema,
	ChargeSchema,
	CommentSchema,
	DocSchema,
	DossierSchema,
	DraftSchema,
	FolderSchema,
	InvoiceSchema,
	LeafletSchema,
	MatterSchema,
	MemoSchema,
	PageSchema,
	PinSchema,
	PostSchema,
	ProjectMemberSchema,
	ProjectSchema,
	RoomMemberSchema,
	RoomSchema,
	SettingSchema,
	SheetSchema,
	SpaceSchema,
	StoryNoteSchema,
	StorySchema,
	TaskSchema,
	ThreadSchema,
	TicketSchema,
	TopicSchema,
} from '../../src/gen/syncplane/testdata/access/v1/access_pb.js';
import { CrudOp, MutationSchema } from '../../src/gen/syncplane/v1/mutation_pb.js';
import { CallerAccessSchema } from '../../src/gen/syncplane/v1/service_pb.js';
import {
	accessGeneration,
	metaCallerAccess,
	metaLastMutationId,
	metaPendingAccess,
	pendingKey,
	pendingPrefix,
} from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { GenerationOf, KVRead, KVStore, KVWrite } from '../../src/kv/store.js';
import { EntityRegistry } from '../../src/mutations.js';
import { compactPending } from '../../src/optimistic/compact.js';
import { pendingAccessOf, readPending } from '../../src/optimistic/queue.js';
import {
	dropAndResequencePending,
	resequencePending,
	rollbackRejected,
} from '../../src/optimistic/rebase.js';
import { callerAccessOf } from '../../src/testing/access.js';
import {
	createTestClient,
	DEFAULT_WORKSPACE,
	type TestCallerAccess,
	TestServer,
} from '../../src/testing/index.js';

const WS = 'ws-can';

/** How many times the client decoded a CallerAccess. */
const decodes = vi.hoisted(() => ({ callerAccess: 0 }));

// The real module, with each decode of a CallerAccess counted in `decodes`.
vi.mock('@bufbuild/protobuf', async (importOriginal) => {
	const mod = await importOriginal<typeof import('@bufbuild/protobuf')>();
	const fromBinary: typeof mod.fromBinary = (schema, bytes, options) => {
		if (schema.typeName === 'syncplane.v1.CallerAccess') decodes.callerAccess++;
		return mod.fromBinary(schema, bytes, options);
	};
	return { ...mod, fromBinary };
});

/** A store whose transactions count the scans of WS's pending queue. */
class QueueScanCountingStore implements KVStore {
	queueScans = 0;

	constructor(private inner: KVStore) {}

	async read(): Promise<KVRead> {
		return this.counting(await this.inner.read());
	}

	async write(): Promise<KVWrite> {
		return this.counting(await this.inner.write());
	}

	close(): Promise<void> {
		return this.inner.close();
	}

	trackGenerations(generationOf: GenerationOf): void {
		(this.inner as MemoryKVStore).trackGenerations(generationOf);
	}

	private counting<T extends KVRead>(tx: T): T {
		const store = this;
		return new Proxy(tx, {
			get(target, prop) {
				const value = Reflect.get(target, prop) as unknown;
				if (typeof value !== 'function') return value;
				if (prop === 'scan') {
					return (prefix: string) => {
						if (prefix === pendingPrefix(WS)) store.queueScans++;
						return (value as (p: string) => unknown).call(target, prefix);
					};
				}
				return value.bind(target);
			},
		});
	}
}

/** A store whose transactions count the reads of one key. */
class KeyReadCountingStore implements KVStore {
	keyReads = 0;

	constructor(private inner: KVStore, private key: string) {}

	async read(): Promise<KVRead> {
		return this.counting(await this.inner.read());
	}

	async write(): Promise<KVWrite> {
		return this.counting(await this.inner.write());
	}

	close(): Promise<void> {
		return this.inner.close();
	}

	trackGenerations(generationOf: GenerationOf): void {
		(this.inner as MemoryKVStore).trackGenerations(generationOf);
	}

	private counting<T extends KVRead>(tx: T): T {
		const store = this;
		return new Proxy(tx, {
			get(target, prop) {
				const value = Reflect.get(target, prop) as unknown;
				if (typeof value !== 'function') return value;
				if (prop === 'get' || prop === 'getMany') {
					return (keys: string | readonly string[]) => {
						for (const key of typeof keys === 'string' ? [ keys ] : keys) {
							if (key === store.key) store.keyReads++;
						}
						return (value as (k: unknown) => unknown).call(target, keys);
					};
				}
				return value.bind(target);
			},
		});
	}
}

async function storeSummary(
	kv: KVStore,
	access: TestCallerAccess,
	ws = WS,
): Promise<void> {
	const tx = await kv.write();
	await tx.put(
		metaCallerAccess(ws),
		toBinary(CallerAccessSchema, callerAccessOf(access)),
	);
	await tx.commit();
}

async function deleteSummary(kv: KVStore, ws = WS): Promise<void> {
	const tx = await kv.write();
	await tx.del(metaCallerAccess(ws));
	await tx.commit();
}

function offlineClient(kvStore: KVStore, entities: readonly DescMessage[]): SyncClient {
	return new SyncClient({
		workspaceId: WS,
		kvStore,
		entities,
		streamEnabled: false,
		pollIntervalMs: 0,
	});
}

describe('hasPermission()', () => {
	it('reads the pending queue once for many answers', async () => {
		const store = new QueueScanCountingStore(new MemoryKVStore());
		const c = offlineClient(store, [ ProjectSchema, ProjectMemberSchema, TaskSchema ]);
		// The first answer opens the store, so the summary written next is
		// stamped like one a pull writes.
		expect(await c.hasPermission(TaskSchema, 't0', 'edit_tasks')).toBe('unknown');
		await storeSummary(store, { permissions: [ { permission: 'edit_tasks', root: true } ] });
		store.queueScans = 0;

		const answers = await Promise.all(
			Array.from({ length: 10 }, (_, i) => c.hasPermission(TaskSchema, `t${i}`, 'edit_tasks')),
		);
		expect(answers.every((a) => a === 'yes')).toBe(true);
		expect(await c.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('yes');
		expect(store.queueScans).toBe(1);
		await c.close();
	});

	it('follows a local write that can change the grants', async () => {
		const kv = new MemoryKVStore();
		await storeSummary(kv, { permissions: [ { permission: 'edit_tasks', root: true } ] });
		const c = offlineClient(kv, [ ProjectSchema, ProjectMemberSchema, TaskSchema ]);
		expect(await c.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('yes');

		await c.mutate.put(ProjectMemberSchema, {
			id: 'pm-new',
			project: { id: 'p1' },
			userId: 'someone',
			role: 'project_reader',
		});
		expect(await c.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('unknown');
		await c.close();
	});

	it('finds a key in each of two anchor rows of one entity', async () => {
		const kv = new MemoryKVStore();
		await storeSummary(kv, {
			permissions: [ {
				permission: 'edit_tasks',
				anchors: [ { entity: 'project', keys: [ 'p1' ] }, { entity: 'project', keys: [ 'p2' ] } ],
			} ],
		});
		const c = offlineClient(kv, [ ProjectSchema, ProjectMemberSchema, TaskSchema ]);
		const answers = await Promise.all(
			[ 'p1', 'p2', 'p3' ].map((k) => c.hasPermission(ProjectSchema, k, 'edit_tasks')),
		);
		expect(answers).toEqual([ 'yes', 'yes', 'no' ]);
		await c.close();
	});

	// Two clients on one store stand in for two tabs: only the store can tell
	// the first that the second queued a write.
	it('follows a write another client queues on the same store', async () => {
		const kv = new MemoryKVStore();
		const first = offlineClient(kv, [ ProjectSchema, ProjectMemberSchema, TaskSchema ]);
		const second = offlineClient(kv, [ ProjectSchema, ProjectMemberSchema, TaskSchema ]);
		expect(await first.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('unknown');
		await storeSummary(kv, { permissions: [ { permission: 'edit_tasks', root: true } ] });
		expect(await first.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('yes');

		await second.mutate.put(ProjectMemberSchema, {
			id: 'pm-new',
			project: { id: 'p1' },
			userId: 'someone',
			role: 'project_reader',
		});
		expect(await first.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('unknown');
		await first.close();
		await second.close();
	});

	it('follows a new summary from the server', async () => {
		const t = await createTestClient({
			entities: [ ProjectSchema, ProjectMemberSchema, TaskSchema ],
		});
		t.server.put(TaskSchema, { id: 't1', project: { id: 'p1' } });
		t.server.setCallerAccess({ permissions: [ { permission: 'view_tasks', root: true } ] });
		await t.sync();
		expect(await t.client.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('no');

		t.server.setCallerAccess({ permissions: [ { permission: 'edit_tasks', root: true } ] });
		await t.sync();
		expect(await t.client.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('yes');
		await t.client.close();
	});

	it('decides at an access parent this client does not register', async () => {
		const kv = new MemoryKVStore();
		await storeSummary(kv, {
			permissions: [ {
				permission: 'view_tasks',
				anchors: [ { entity: 'project', keys: [ 'p1' ] } ],
			} ],
		});
		const c = offlineClient(kv, [ TaskSchema ]);
		await c.mutate.put(TaskSchema, { id: 't1', project: { id: 'p1' }, title: 'one' });
		await c.mutate.put(TaskSchema, { id: 't2', project: { id: 'p2' }, title: 'two' });

		expect(await c.hasPermission(TaskSchema, 't1', 'view_tasks')).toBe('yes');
		expect(await c.hasPermission(TaskSchema, 't2', 'view_tasks')).toBe('no');
		expect(await c.hasPermission(TaskSchema, 't3', 'view_tasks')).toBe('unknown');
		await c.close();
	});

	it('walks the chain of access parents through the rows held locally', async () => {
		const t = await createTestClient({ entities: [ ProjectSchema, TaskSchema, CommentSchema ] });
		t.server.put(TaskSchema, { id: 't1', project: { id: 'p1' } });
		t.server.put(TaskSchema, { id: 't2', project: { id: 'p2' } });
		t.server.put(CommentSchema, { id: 'c1', task: { id: 't1' } });
		t.server.put(CommentSchema, { id: 'c2', task: { id: 't2' } });
		t.server.put(CommentSchema, { id: 'c9', task: { id: 't9' } });
		t.server.setCallerAccess({
			permissions: [ {
				permission: 'view_tasks',
				anchors: [ { entity: 'project', keys: [ 'p1' ] } ],
			} ],
		});
		await t.sync();
		expect(await t.client.hasPermission(CommentSchema, 'c1', 'view_tasks')).toBe('yes');
		expect(await t.client.hasPermission(CommentSchema, 'c2', 'view_tasks')).toBe('no');
		expect(await t.client.hasPermission(CommentSchema, 'c9', 'view_tasks')).toBe('unknown');

		// A queued move counts where the row will be once the server accepts it.
		await t.client.mutate.patch(TaskSchema, 't2', { project: { id: 'p1' } });
		expect(await t.client.hasPermission(CommentSchema, 'c2', 'view_tasks')).toBe('yes');
		await t.client.close();
	});

	// The board x and the space x share a key, so only the case tells the two
	// parents apart.
	it('follows the case of a polymorphic access parent that a row holds', async () => {
		const t = await createTestClient({ entities: [ CardSchema, CardItemSchema, PinSchema ] });
		t.server.put(CardSchema, { id: 'c-b', place: { case: 'board', value: { id: 'x' } } });
		t.server.put(CardSchema, { id: 'c-s', place: { case: 'space', value: { id: 'x' } } });
		t.server.put(CardItemSchema, { id: 'i-b', card: { id: 'c-b' } });
		t.server.put(CardItemSchema, { id: 'i-s', card: { id: 'c-s' } });
		t.server.put(PinSchema, { id: 'p-b', place: { target: { case: 'board', value: { id: 'x' } } } });
		t.server.put(PinSchema, { id: 'p-s', place: { target: { case: 'space', value: { id: 'x' } } } });
		t.server.setCallerAccess({
			permissions: [
				{ permission: 'view_cards', anchors: [ { entity: 'board', keys: [ 'x' ] } ] },
				{ permission: 'view_pins', anchors: [ { entity: 'board', keys: [ 'x' ] } ] },
			],
		});
		await t.sync();
		expect(await t.client.hasPermission(CardSchema, 'c-b', 'view_cards')).toBe('yes');
		expect(await t.client.hasPermission(CardSchema, 'c-s', 'view_cards')).toBe('no');
		expect(await t.client.hasPermission(CardItemSchema, 'i-b', 'view_cards')).toBe('yes');
		expect(await t.client.hasPermission(CardItemSchema, 'i-s', 'view_cards')).toBe('no');
		expect(await t.client.hasPermission(PinSchema, 'p-b', 'view_pins')).toBe('yes');
		expect(await t.client.hasPermission(PinSchema, 'p-s', 'view_pins')).toBe('no');

		// A queued move to the other case counts where the row will be.
		await t.client.mutate.patch(CardSchema, 'c-s', { place: { case: 'board', value: { id: 'x' } } });
		expect(await t.client.hasPermission(CardItemSchema, 'i-s', 'view_cards')).toBe('yes');
		await t.client.close();
	});

	it('follows a queued delete of a row that a case of a polymorphic access parent names', async () => {
		const kv = new MemoryKVStore();
		await storeSummary(kv, { permissions: [ { permission: 'view_cards', root: true } ] });
		const c = new SyncClient({
			workspaceId: WS,
			kvStore: kv,
			entities: [ SpaceSchema, CardSchema ],
			streamEnabled: false,
			pollIntervalMs: 0,
		});
		expect(await c.hasPermission(CardSchema, 'c1', 'view_cards')).toBe('yes');
		await c.mutate.del(SpaceSchema, 's1');
		expect(await c.hasPermission(CardSchema, 'c1', 'view_cards')).toBe('unknown');
		await c.close();
	});

	it('walks a recursive chain', async () => {
		const t = await createTestClient({ entities: [ FolderSchema ] });
		t.server.put(FolderSchema, { id: 'top' });
		t.server.put(FolderSchema, { id: 'mid', parent: { id: 'top' } });
		t.server.put(FolderSchema, { id: 'low', parent: { id: 'mid' } });
		t.server.setCallerAccess({
			permissions: [ {
				permission: 'view_folders',
				anchors: [ { entity: 'folder', keys: [ 'mid' ] } ],
			} ],
		});
		await t.sync();
		expect(await t.client.hasPermission(FolderSchema, 'low', 'view_folders')).toBe('yes');
		expect(await t.client.hasPermission(FolderSchema, 'top', 'view_folders')).toBe('no');
		await t.client.close();
	});
});

describe('several access parents', () => {
	const at = (permission: string, entity: string, ...keys: string[]) => ({
		permission,
		anchors: [ { entity, keys } ],
	});

	it('decides through any parent of a row and any row above it', async () => {
		const t = await createTestClient({
			entities: [ StorySchema, StoryNoteSchema, LeafletSchema, TopicSchema ],
		});
		t.server.put(StorySchema, { id: 's-pc', product: { id: 'p1' }, cycle: { id: 'c1' } });
		t.server.put(StorySchema, { id: 's-p', product: { id: 'p1' } });
		t.server.put(StoryNoteSchema, { id: 'n-pc', story: { id: 's-pc' } });
		t.server.put(StoryNoteSchema, { id: 'n-far', story: { id: 's-far' } });
		t.server.put(LeafletSchema, { id: 'l-12', shelves: [ { id: 'h1' }, { id: 'h2' } ] });
		t.server.put(TopicSchema, { id: 'top' });
		t.server.put(TopicSchema, { id: 'mid', parents: [ { id: 'top' } ] });
		t.server.put(TopicSchema, { id: 'low', parents: [ { id: 'side' }, { id: 'mid' } ] });
		t.server.setCallerAccess({
			permissions: [
				at('view_stories', 'cycle', 'c1'),
				at('view_leaflets', 'shelf', 'h2'),
				at('view_topics', 'topic', 'top'),
			],
		});
		await t.sync();
		expect(await t.client.hasPermission(StorySchema, 's-pc', 'view_stories')).toBe('yes');
		expect(await t.client.hasPermission(StorySchema, 's-p', 'view_stories')).toBe('no');
		expect(await t.client.hasPermission(StoryNoteSchema, 'n-pc', 'view_stories')).toBe('yes');
		expect(await t.client.hasPermission(StoryNoteSchema, 'n-far', 'view_stories')).toBe('unknown');
		expect(await t.client.hasPermission(LeafletSchema, 'l-12', 'view_leaflets')).toBe('yes');
		expect(await t.client.hasPermission(TopicSchema, 'low', 'view_topics')).toBe('yes');

		// A queued write that adds a parent counts where the row will be.
		await t.client.mutate.patch(StorySchema, 's-p', { cycle: { id: 'c1' } });
		expect(await t.client.hasPermission(StorySchema, 's-p', 'view_stories')).toBe('yes');
		await t.client.close();
	});

	it('checks a create through each parent the row names', async () => {
		const t = await createTestClient({ entities: [ StorySchema, LeafletSchema ] });
		t.server.setCallerAccess({
			permissions: [ at('edit_stories', 'product', 'p1'), at('edit_leaflets', 'shelf', 'h1') ],
		});
		await t.sync();
		expect(await t.client.canCreate(StorySchema, { id: 'n', product: { id: 'p1' } })).toBe('yes');
		expect(
			await t.client.canCreate(StorySchema, { id: 'n', product: { id: 'p1' }, cycle: { id: 'c1' } }),
		)
			.toBe('no');
		expect(await t.client.canCreate(LeafletSchema, { id: 'n', shelves: [ { id: 'h1' } ] })).toBe(
			'yes',
		);
		expect(
			await t.client.canCreate(LeafletSchema, { id: 'n', shelves: [ { id: 'h1' }, { id: 'h2' } ] }),
		)
			.toBe('no');
		await t.client.close();

		const both = await createTestClient({ entities: [ StorySchema ] });
		both.server.setCallerAccess({
			permissions: [ {
				permission: 'edit_stories',
				anchors: [
					{ entity: 'product', keys: [ 'p1' ] },
					{ entity: 'cycle', keys: [ 'c1' ] },
				],
			} ],
		});
		await both.sync();
		expect(
			await both.client.canCreate(StorySchema, {
				id: 'n',
				product: { id: 'p1' },
				cycle: { id: 'c1' },
			}),
		)
			.toBe('yes');
		await both.client.close();
	});
});

const roomEntities = [
	RoomSchema,
	RoomMemberSchema,
	PostSchema,
	DraftSchema,
	TicketSchema,
	SettingSchema,
] as const;

/**
 * A test client over the room schema: a post, a draft and a ticket of ann's
 * and of bob's in room r1, and the caller is ann, posting in r1.
 */
async function roomClient(permissions: TestCallerAccess['permissions']) {
	const t = await createTestClient({ entities: roomEntities });
	t.server.put(RoomSchema, { id: 'r1' });
	t.server.put(PostSchema, { id: 'po-ann', room: { id: 'r1' }, authorId: 'ann', body: 'hi' });
	t.server.put(PostSchema, { id: 'po-bob', room: { id: 'r1' }, authorId: 'bob', body: 'yo' });
	t.server.put(PostSchema, { id: 'po-far', room: { id: 'r9' }, authorId: 'ann' });
	t.server.put(DraftSchema, { id: 'd-ann', room: { id: 'r1' }, authorId: 'ann' });
	t.server.put(DraftSchema, { id: 'd-bob', room: { id: 'r1' }, authorId: 'bob' });
	t.server.put(TicketSchema, { id: 'tk-ann', room: { id: 'r1' }, assigneeId: 'ann' });
	t.server.put(TicketSchema, { id: 'tk-bob', room: { id: 'r1' }, assigneeId: 'bob' });
	t.server.setCallerAccess({ subjects: [ 'ann' ], permissions });
	await t.sync();
	return t;
}

const inR1 = (...perms: string[]) =>
	perms.map((permission) => ({ permission, anchors: [ { entity: 'room', keys: [ 'r1' ] } ] }));

describe('can()', () => {
	it('lets the author update the fields the rule lists, and delete', async () => {
		const t = await roomClient(inR1('view_posts', 'post'));
		expect(await t.client.can(PostSchema, 'po-ann', 'update', { fields: [ 'body' ] })).toBe('yes');
		expect(await t.client.can(PostSchema, 'po-ann', 'update', { fields: [ 'body', 'title' ] })).toBe(
			'no',
		);
		expect(await t.client.can(PostSchema, 'po-ann', 'delete')).toBe('yes');
		expect(await t.client.can(PostSchema, 'po-ann', 'read')).toBe('yes');
		await t.client.close();
	});

	it('refuses a caller who is not the author', async () => {
		const t = await roomClient(inR1('view_posts', 'post'));
		expect(await t.client.can(PostSchema, 'po-bob', 'update', { fields: [ 'body' ] })).toBe('no');
		expect(await t.client.can(PostSchema, 'po-bob', 'delete')).toBe('no');
		await t.client.close();
	});

	it('counts only rules without a fields list for an update that names no fields', async () => {
		const t = await roomClient(inR1('view_posts', 'post'));
		expect(await t.client.can(PostSchema, 'po-ann', 'update')).toBe('no');
		await t.client.close();

		const mod = await roomClient(inR1('view_posts', 'post', 'moderate_posts'));
		expect(await mod.client.can(PostSchema, 'po-bob', 'update')).toBe('yes');
		await mod.client.close();
	});

	it('still checks the subject of a caller who holds the permission at the root', async () => {
		const t = await roomClient([ { permission: 'view_posts', root: true } ]);
		expect(await t.client.can(DraftSchema, 'd-ann', 'read')).toBe('yes');
		expect(await t.client.can(DraftSchema, 'd-bob', 'read')).toBe('no');
		await t.client.close();
	});

	it('lets the assignee change the status', async () => {
		const t = await roomClient(inR1('view_posts'));
		expect(await t.client.can(TicketSchema, 'tk-ann', 'update', { fields: [ 'status' ] })).toBe(
			'yes',
		);
		expect(await t.client.can(TicketSchema, 'tk-bob', 'update', { fields: [ 'status' ] })).toBe('no');
		expect(await t.client.can(TicketSchema, 'tk-ann', 'update', { fields: [ 'title' ] })).toBe('no');
		await t.client.close();
	});

	it('answers unknown where the new value of a subject field or the access parent decides', async () => {
		const t = await roomClient(inR1('view_posts'));
		expect(await t.client.can(DraftSchema, 'd-ann', 'update', { fields: [ 'author_id' ] })).toBe(
			'unknown',
		);
		expect(await t.client.can(DraftSchema, 'd-ann', 'update', { fields: [ 'room' ] })).toBe(
			'unknown',
		);
		expect(await t.client.can(DraftSchema, 'd-ann', 'update', { fields: [ 'body' ] })).toBe('yes');
		await t.client.close();

		// Held at the root, the permission holds wherever the row moves.
		const root = await roomClient([ { permission: 'view_posts', root: true } ]);
		expect(await root.client.can(DraftSchema, 'd-ann', 'update', { fields: [ 'room' ] })).toBe('yes');
		await root.client.close();
	});

	it('answers unknown where the row is not held', async () => {
		const t = await roomClient(inR1('view_posts', 'post'));
		expect(await t.client.can(PostSchema, 'po-gone', 'read')).toBe('unknown');
		// A room has no access parent, so a room the client does not hold
		// still decides: nothing above it can hold the permission.
		expect(await t.client.can(PostSchema, 'po-far', 'update', { fields: [ 'body' ] })).toBe('no');
		await t.client.close();
	});

	it('answers unknown before any summary arrives and for an entity outside the model', async () => {
		const t = await createTestClient({ entities: roomEntities });
		t.server.put(PostSchema, { id: 'po-ann', room: { id: 'r1' }, authorId: 'ann' });
		await t.sync();
		expect(await t.client.can(PostSchema, 'po-ann', 'read')).toBe('unknown');
		await t.client.close();
	});
});

describe('canCreate()', () => {
	it('checks the row as the server would store it, with the caller as its creator', async () => {
		const t = await roomClient(inR1('view_posts', 'post'));
		// The author field is the creator's whatever the row says.
		expect(await t.client.canCreate(PostSchema, { id: 'new', room: { id: 'r1' }, authorId: 'bob' }))
			.toBe('yes');
		expect(await t.client.canCreate(PostSchema, { id: 'new', room: { id: 'r9' } })).toBe('no');
		await t.client.close();

		const viewer = await roomClient(inR1('view_posts'));
		expect(await viewer.client.canCreate(PostSchema, { id: 'new', room: { id: 'r1' } })).toBe('no');
		await viewer.client.close();
	});

	it('refuses a row whose subject is someone else', async () => {
		const t = await roomClient(inR1('view_posts'));
		expect(await t.client.canCreate(SettingSchema, { userId: 'ann' })).toBe('yes');
		expect(await t.client.canCreate(SettingSchema, { userId: 'bob' })).toBe('no');
		await t.client.close();
	});
});

/**
 * A test client over the room schema with threads and bulletins: an open, an
 * archived and a hidden thread in room r1, and a published bulletin and a
 * draft by bob. The caller is ann.
 */
async function threadClient(permissions: TestCallerAccess['permissions']) {
	const t = await createTestClient({ entities: [ ...roomEntities, ThreadSchema, BulletinSchema ] });
	t.server.put(RoomSchema, { id: 'r1' });
	t.server.put(ThreadSchema, { id: 'th-open', room: { id: 'r1' } });
	t.server.put(ThreadSchema, { id: 'th-archived', room: { id: 'r1' }, archived: true });
	t.server.put(ThreadSchema, { id: 'th-hidden', room: { id: 'r1' }, hidden: true });
	t.server.put(BulletinSchema, { id: 'bu-pub', authorId: 'bob', status: 'published' });
	t.server.put(BulletinSchema, { id: 'bu-draft', authorId: 'bob', status: 'draft' });
	t.server.setCallerAccess({ subjects: [ 'ann' ], permissions });
	await t.sync();
	return t;
}

describe('can() with a when check', () => {
	it('evaluates the condition on the row held locally', async () => {
		const t = await threadClient(inR1('view_posts', 'post'));
		expect(await t.client.can(ThreadSchema, 'th-open', 'read')).toBe('yes');
		expect(await t.client.can(ThreadSchema, 'th-hidden', 'read')).toBe('no');
		expect(await t.client.can(ThreadSchema, 'th-open', 'update', { fields: [ 'title' ] })).toBe(
			'yes',
		);
		expect(await t.client.can(ThreadSchema, 'th-archived', 'update', { fields: [ 'title' ] })).toBe(
			'no',
		);
		expect(await t.client.can(ThreadSchema, 'th-open', 'delete')).toBe('yes');
		expect(await t.client.can(ThreadSchema, 'th-archived', 'delete')).toBe('no');
		await t.client.close();
	});

	it('answers unknown where the new value of a field the condition reads decides', async () => {
		const t = await threadClient(inR1('view_posts', 'post'));
		expect(await t.client.can(ThreadSchema, 'th-open', 'update', { fields: [ 'archived' ] })).toBe(
			'unknown',
		);
		// The stored row already fails the condition, whatever the update does.
		expect(await t.client.can(ThreadSchema, 'th-archived', 'update', { fields: [ 'archived' ] }))
			.toBe(
				'no',
			);
		await t.client.close();
	});

	it('lets a rule without the check decide a row that fails it', async () => {
		const t = await threadClient(inR1('view_posts', 'post', 'moderate_posts'));
		expect(await t.client.can(ThreadSchema, 'th-hidden', 'read')).toBe('yes');
		expect(await t.client.can(ThreadSchema, 'th-archived', 'update', { fields: [ 'archived' ] }))
			.toBe(
				'yes',
			);
		await t.client.close();
	});

	it('still checks the condition for a rule that admits every admitted caller', async () => {
		const t = await threadClient(inR1('view_posts'));
		expect(await t.client.can(BulletinSchema, 'bu-pub', 'read')).toBe('yes');
		expect(await t.client.can(BulletinSchema, 'bu-draft', 'read')).toBe('no');
		await t.client.close();
	});

	it('checks a create on the row as the server would store it', async () => {
		const t = await threadClient(inR1('view_posts', 'post'));
		expect(await t.client.canCreate(ThreadSchema, { id: 'new', room: { id: 'r1' } })).toBe('yes');
		expect(await t.client.canCreate(ThreadSchema, { id: 'new', room: { id: 'r1' }, archived: true }))
			.toBe('no');
		await t.client.close();
	});

	it('answers unknown where the row is not held', async () => {
		const t = await threadClient(inR1('view_posts', 'post'));
		expect(await t.client.can(ThreadSchema, 'th-gone', 'read')).toBe('unknown');
		await t.client.close();
	});
});

/**
 * A test client over the within schema, as `subject`: d-open is open and owned
 * by olga, d-priv private and owned by olga, d-arch archived and owned by
 * cora, each with one matter and one memo; m-far sits in a dossier the client
 * does not hold. b-top is bea's and holds ian's b-inner, each with a sheet.
 */
async function dossierClient(subject: string, permissions: TestCallerAccess['permissions']) {
	const t = await createTestClient({
		entities: [ DossierSchema, MatterSchema, MemoSchema, BinderSchema, SheetSchema ],
	});
	t.server.put(DossierSchema, { id: 'd-open', ownerId: 'olga' });
	t.server.put(DossierSchema, { id: 'd-priv', private: true, ownerId: 'olga' });
	t.server.put(DossierSchema, { id: 'd-arch', archived: true, ownerId: 'cora' });
	for (const d of [ 'open', 'priv', 'arch' ]) {
		t.server.put(MatterSchema, { id: `m-${d}`, dossier: { id: `d-${d}` } });
		t.server.put(MemoSchema, { id: `memo-${d}`, matter: { id: `m-${d}` } });
	}
	t.server.put(MatterSchema, { id: 'm-far', dossier: { id: 'd-far' } });
	t.server.put(BinderSchema, { id: 'b-top', ownerId: 'bea' });
	t.server.put(BinderSchema, { id: 'b-inner', parent: { id: 'b-top' }, ownerId: 'ian' });
	t.server.put(SheetSchema, { id: 's-top', binder: { id: 'b-top' } });
	t.server.put(SheetSchema, { id: 's-inner', binder: { id: 'b-inner' } });
	t.server.setCallerAccess({ subjects: [ subject ], permissions });
	await t.sync();
	return t;
}

const atRoot = (...perms: string[]) => perms.map((permission) => ({ permission, root: true }));

describe('can() with a within entry', () => {
	it('checks the row above that the entry names', async () => {
		const t = await dossierClient('cora', atRoot('view_open_matters', 'edit_matters'));
		expect(await t.client.can(MatterSchema, 'm-open', 'read')).toBe('yes');
		expect(await t.client.can(MatterSchema, 'm-priv', 'read')).toBe('no');
		expect(await t.client.can(MemoSchema, 'memo-open', 'read')).toBe('yes');
		expect(await t.client.can(MemoSchema, 'memo-priv', 'read')).toBe('no');
		expect(await t.client.can(MatterSchema, 'm-open', 'update', { fields: [ 'title' ] })).toBe('yes');
		expect(await t.client.can(MatterSchema, 'm-arch', 'update', { fields: [ 'title' ] })).toBe('no');
		await t.client.close();
	});

	it('admits the owner of a row above through a subject check', async () => {
		const t = await dossierClient('olga', atRoot('view_open_matters'));
		expect(await t.client.can(MemoSchema, 'memo-priv', 'read')).toBe('yes');
		expect(await t.client.can(MemoSchema, 'memo-priv', 'update', { fields: [ 'title' ] })).toBe(
			'yes',
		);
		expect(await t.client.can(MemoSchema, 'memo-arch', 'update', { fields: [ 'title' ] })).toBe('no');
		expect(await t.client.canCreate(MemoSchema, { id: 'new', matter: { id: 'm-priv' } })).toBe('yes');
		await t.client.close();
	});

	it('reads the row itself and every row above it in a recursive tree', async () => {
		const bea = await dossierClient('bea', atRoot('view_open_matters'));
		expect(await bea.client.can(BinderSchema, 'b-top', 'read')).toBe('yes');
		expect(await bea.client.can(BinderSchema, 'b-inner', 'read')).toBe('yes');
		expect(await bea.client.can(SheetSchema, 's-inner', 'update', { fields: [ 'title' ] })).toBe(
			'yes',
		);
		await bea.client.close();
		const ian = await dossierClient('ian', atRoot('view_open_matters'));
		expect(await ian.client.can(BinderSchema, 'b-inner', 'read')).toBe('yes');
		expect(await ian.client.can(SheetSchema, 's-top', 'read')).toBe('no');
		await ian.client.close();
	});

	it('answers unknown where a row the entry reads is not held', async () => {
		const t = await dossierClient('cora', atRoot('view_open_matters', 'edit_matters'));
		expect(await t.client.can(MatterSchema, 'm-far', 'read')).toBe('unknown');
		await t.client.close();
	});

	it('answers unknown for an update that moves the row or changes what the entry reads of it', async () => {
		const t = await dossierClient('cora', atRoot('view_open_matters', 'edit_matters'));
		expect(await t.client.can(MatterSchema, 'm-open', 'update', { fields: [ 'dossier' ] })).toBe(
			'unknown',
		);
		await t.client.close();
		const bea = await dossierClient('bea', atRoot('view_open_matters'));
		expect(await bea.client.can(BinderSchema, 'b-top', 'update', { fields: [ 'owner_id' ] })).toBe(
			'unknown',
		);
		expect(await bea.client.can(BinderSchema, 'b-top', 'update', { fields: [ 'title' ] })).toBe(
			'yes',
		);
		await bea.client.close();
	});
});

/**
 * A test client over the billing schema: invoice i1 in account a1 with charge
 * c1, invoice i2 in account a2, and charge c-far on an invoice the client does
 * not hold. The caller is ann.
 */
async function billingClient(permissions: TestCallerAccess['permissions']) {
	const t = await createTestClient({ entities: [ AccountSchema, InvoiceSchema, ChargeSchema ] });
	t.server.put(AccountSchema, { id: 'a1' });
	t.server.put(AccountSchema, { id: 'a2' });
	t.server.put(InvoiceSchema, { id: 'i1', account: { id: 'a1' } });
	t.server.put(InvoiceSchema, { id: 'i2', account: { id: 'a2' } });
	t.server.put(ChargeSchema, { id: 'c1', invoice: { id: 'i1' } });
	t.server.put(ChargeSchema, { id: 'c-far', invoice: { id: 'i9' } });
	t.server.setCallerAccess({ subjects: [ 'ann' ], permissions });
	await t.sync();
	return t;
}

const inAccount = (account: string, ...perms: string[]) =>
	perms.map((permission) => ({ permission, anchors: [ { entity: 'account', keys: [ account ] } ] }));

describe('can() with a rule that needs several permissions', () => {
	it('answers yes where the caller holds each of them, at different places on the chain', async () => {
		const t = await billingClient([
			...inAccount('a1', 'view_invoices', 'edit_invoices'),
			{ permission: 'read_billing', root: true },
			{ permission: 'use_billing', root: true },
		]);
		expect(await t.client.can(InvoiceSchema, 'i1', 'read')).toBe('yes');
		expect(await t.client.can(ChargeSchema, 'c1', 'read')).toBe('yes');
		expect(await t.client.can(InvoiceSchema, 'i1', 'update', { fields: [ 'title' ] })).toBe('yes');
		expect(await t.client.canCreate(ChargeSchema, { id: 'c2', invoice: { id: 'i1' } })).toBe('yes');
		await t.client.close();
	});

	it('answers no where the caller holds one of them', async () => {
		const t = await billingClient([
			...inAccount('a1', 'view_invoices', 'edit_invoices'),
			{ permission: 'read_billing', root: true },
		]);
		expect(await t.client.can(InvoiceSchema, 'i1', 'read')).toBe('yes');
		expect(await t.client.can(InvoiceSchema, 'i1', 'update', { fields: [ 'title' ] })).toBe('no');
		expect(await t.client.canCreate(ChargeSchema, { id: 'c2', invoice: { id: 'i1' } })).toBe('no');
		await t.client.close();
	});

	it("needs each of them on the row's own chain", async () => {
		const t = await billingClient([
			{ permission: 'view_invoices', anchors: [ { entity: 'account', keys: [ 'a1', 'a2' ] } ] },
			{ permission: 'read_billing', anchors: [ { entity: 'account', keys: [ 'a2' ] } ] },
		]);
		expect(await t.client.can(InvoiceSchema, 'i1', 'read')).toBe('no');
		expect(await t.client.can(InvoiceSchema, 'i2', 'read')).toBe('yes');
		await t.client.close();
	});

	it('answers unknown where a row on the chain is not held', async () => {
		const t = await billingClient([
			...inAccount('a1', 'view_invoices'),
			{ permission: 'read_billing', root: true },
		]);
		expect(await t.client.can(ChargeSchema, 'c-far', 'read')).toBe('unknown');
		await t.client.close();
	});
});

describe('a creator field', () => {
	it("holds the caller's primary subject on a row this client creates", async () => {
		const t = await roomClient(inR1('view_posts', 'post'));
		t.server.setCallerAccess({
			subjects: [ 'user:ann', 'ann' ],
			permissions: inR1('view_posts', 'post'),
		});
		await t.sync();
		await t.client.mutate.put(PostSchema, { id: 'po-new', room: { id: 'r1' }, authorId: 'bob' });
		expect((await t.client.get(PostSchema, 'po-new'))?.authorId).toBe('user:ann');
		await t.client.close();
	});

	it('keeps the stored value on a put over a row this client holds', async () => {
		const t = await roomClient(inR1('view_posts', 'post'));
		await t.client.mutate.put(PostSchema, {
			id: 'po-bob',
			room: { id: 'r1' },
			authorId: 'ann',
			body: 'x',
		});
		expect((await t.client.get(PostSchema, 'po-bob'))?.authorId).toBe('bob');
		await t.client.close();
	});

	it('is empty on a row created before any summary arrives', async () => {
		const t = await createTestClient({ entities: roomEntities });
		await t.client.mutate.put(PostSchema, { id: 'po-new', room: { id: 'r1' }, authorId: 'bob' });
		expect((await t.client.get(PostSchema, 'po-new'))?.authorId).toBe('');
		await t.client.close();
	});

	it('refuses a patch that names it', async () => {
		const t = await roomClient(inR1('view_posts', 'post'));
		await expect(t.client.mutate.patch(PostSchema, 'po-ann', { authorId: 'bob' }))
			.rejects.toThrow(/field "author_id" may not be written by a client/);
		await t.client.close();
	});

	it('does not decode the CallerAccess again for each row created', async () => {
		const t = await roomClient(inR1('view_posts', 'post'));
		decodes.callerAccess = 0;
		for (let i = 0; i < 3; i++) {
			await t.client.mutate.put(PostSchema, { id: `po-new${i}`, room: { id: 'r1' } });
			expect((await t.client.get(PostSchema, `po-new${i}`))?.authorId).toBe('ann');
		}
		expect(decodes.callerAccess).toBe(1);
		await t.client.close();
	});

	it('follows a CallerAccess that another client stores or deletes', async () => {
		const kv = new MemoryKVStore();
		const c = offlineClient(kv, roomEntities);
		const createPost = async (id: string) => {
			await c.mutate.put(PostSchema, { id, room: { id: 'r1' } });
			return (await c.get(PostSchema, id))?.authorId;
		};
		// The first create opens the store, so the summaries written next are
		// stamped like ones a pull writes.
		expect(await createPost('po-0')).toBe('');
		await storeSummary(kv, { subjects: [ 'ann' ] });
		expect(await createPost('po-1')).toBe('ann');
		await storeSummary(kv, { subjects: [ 'bob' ] });
		expect(await createPost('po-2')).toBe('bob');
		await deleteSummary(kv);
		expect(await createPost('po-3')).toBe('');
		await c.close();
	});
});

/**
 * A store that counts the reads it opens and the getMany calls made on them,
 * and fails every `get` or every `getMany` while `failing` names it.
 */
class ReadCountingStore implements KVStore {
	reads = 0;
	getManys = 0;
	failing: 'get' | 'getMany' | undefined;

	constructor(private inner: KVStore) {}

	async read(): Promise<KVRead> {
		this.reads++;
		const tx = await this.inner.read();
		const store = this;
		return new Proxy(tx, {
			get(target, prop) {
				const value = Reflect.get(target, prop) as unknown;
				if (typeof value !== 'function') return value;
				if (prop === 'get' || prop === 'getMany') {
					return (...args: unknown[]) => {
						if (prop === 'getMany') store.getManys++;
						if (store.failing === prop) return Promise.reject(new Error(`${prop} failed`));
						return (value as (...a: unknown[]) => unknown).apply(target, args);
					};
				}
				return value.bind(target);
			},
		});
	}

	write(): Promise<KVWrite> {
		return this.inner.write();
	}

	close(): Promise<void> {
		return this.inner.close();
	}
}

/** Asks each prediction after the one before it has answered. */
async function oneByOne(asks: readonly (() => Promise<CanAnswer>)[]): Promise<CanAnswer[]> {
	const out: CanAnswer[] = [];
	for (const ask of asks) out.push(await ask());
	return out;
}

describe('predictions asked together', () => {
	it('answer as the same predictions asked one at a time', async () => {
		// Projects are not registered, so a project counts as a row not held.
		// A project has no access parent, so the chain still ends there.
		const t = await createTestClient({
			entities: [ TaskSchema, CommentSchema, PageSchema, FolderSchema, DocSchema ],
		});
		for (
			const [ id, project ] of [ [ 't1', 'p1' ], [ 't2', 'p2' ], [ 't3', 'p1' ], [ 't4', 'p2' ] ]
		) {
			t.server.put(TaskSchema, { id, project: { id: project } });
		}
		for (
			const [ id, task ] of [ [ 'c1a', 't1' ], [ 'c1b', 't1' ], [ 'c1c', 't1' ], [ 'c2', 't2' ], [
				'c3',
				't3',
			], [ 'c9', 't9' ] ]
		) {
			t.server.put(CommentSchema, { id, task: { id: task } });
		}
		t.server.put(PageSchema, { id: 'pg1', task: { id: 't1' }, ownerId: 'bob' });
		t.server.put(PageSchema, { id: 'pg4', task: { id: 't4' }, ownerId: 'ann' });
		t.server.put(PageSchema, { id: 'pg5', task: { id: 't4' }, ownerId: 'bob' });
		t.server.put(FolderSchema, { id: 'top' });
		t.server.put(FolderSchema, { id: 'mid', parent: { id: 'top' } });
		t.server.put(FolderSchema, { id: 'low', parent: { id: 'mid' } });
		t.server.put(FolderSchema, { id: 'deep', parent: { id: 'low' } });
		t.server.put(FolderSchema, { id: 'side' });
		t.server.put(DocSchema, { id: 'd-deep', folder: { id: 'deep' } });
		t.server.put(DocSchema, { id: 'd-top', folder: { id: 'top' } });
		t.server.put(DocSchema, { id: 'd-side', folder: { id: 'side' } });
		t.server.put(DocSchema, { id: 'd-lost', folder: { id: 'nowhere' } });
		t.server.setCallerAccess({
			subjects: [ 'ann' ],
			permissions: [
				{ permission: 'view_tasks', anchors: [ { entity: 'project', keys: [ 'p1' ] } ] },
				{ permission: 'view_projects', anchors: [ { entity: 'project', keys: [ 'p2' ] } ] },
				{ permission: 'edit_tasks', anchors: [ { entity: 'task', keys: [ 't3' ] } ] },
				{ permission: 'view_folders', anchors: [ { entity: 'folder', keys: [ 'mid' ] } ] },
				{ permission: 'edit_folders', anchors: [ { entity: 'folder', keys: [ 'side' ] } ] },
			],
		});
		await t.sync();
		// Queued and not acked: two moves to another parent, a delete, and a
		// row that exists only locally.
		await t.client.mutate.patch(TaskSchema, 't2', { project: { id: 'p1' } });
		await t.client.mutate.patch(FolderSchema, 'side', { parent: { id: 'mid' } });
		await t.client.mutate.del(CommentSchema, 'c1c');
		await t.client.mutate.put(CommentSchema, { id: 'c-new', task: { id: 't3' } });

		const c = t.client;
		const cases: [() => Promise<CanAnswer>, CanAnswer][] = [
			[ () => c.can(CommentSchema, 'c1a', 'read'), 'yes' ],
			[ () => c.can(CommentSchema, 'c1b', 'update', { fields: [ 'body' ] }), 'no' ],
			[ () => c.can(CommentSchema, 'c1c', 'read'), 'unknown' ],
			[ () => c.can(CommentSchema, 'c2', 'read'), 'yes' ],
			[ () => c.can(CommentSchema, 'c3', 'update', { fields: [ 'body' ] }), 'yes' ],
			[ () => c.can(CommentSchema, 'c9', 'read'), 'unknown' ],
			[ () => c.can(CommentSchema, 'c-gone', 'read'), 'unknown' ],
			[ () => c.can(CommentSchema, 'c-new', 'delete'), 'yes' ],
			[ () => c.can(TaskSchema, 't2', 'read'), 'yes' ],
			[ () => c.can(TaskSchema, 't3', 'update', { fields: [ 'project' ] }), 'yes' ],
			[ () => c.can(CommentSchema, 'c3', 'update', { fields: [ 'task' ] }), 'unknown' ],
			[ () => c.can(PageSchema, 'pg1', 'read'), 'yes' ],
			[ () => c.can(PageSchema, 'pg4', 'read'), 'yes' ],
			[ () => c.can(PageSchema, 'pg5', 'read'), 'no' ],
			[ () => c.can(DocSchema, 'd-deep', 'read'), 'yes' ],
			[ () => c.can(DocSchema, 'd-top', 'read'), 'no' ],
			[ () => c.can(DocSchema, 'd-side', 'read'), 'yes' ],
			[ () => c.can(DocSchema, 'd-side', 'update', { fields: [ 'title' ] }), 'yes' ],
			[ () => c.can(DocSchema, 'd-lost', 'read'), 'unknown' ],
			[ () => c.hasPermission(FolderSchema, 'low', 'view_folders'), 'yes' ],
			[ () => c.hasPermission(FolderSchema, 'mid', 'view_folders'), 'yes' ],
			[ () => c.hasPermission(FolderSchema, 'top', 'view_folders'), 'no' ],
			[ () => c.hasPermission(CommentSchema, 'c9', 'view_tasks'), 'unknown' ],
			[ () => c.hasPermission(TaskSchema, 't4', 'view_tasks'), 'no' ],
			[ () => c.canCreate(CommentSchema, { id: 'n1', task: { id: 't1' } }), 'no' ],
			[ () => c.canCreate(CommentSchema, { id: 'n3', task: { id: 't3' } }), 'yes' ],
			[ () => c.canCreate(CommentSchema, { id: 'n9', task: { id: 't9' } }), 'unknown' ],
			[ () => c.canCreate(DocSchema, { id: 'nd', folder: { id: 'side' } }), 'yes' ],
		];
		const expected = cases.map(([ , answer ]) => answer);
		expect(await oneByOne(cases.map(([ ask ]) => ask))).toEqual(expected);
		expect(await Promise.all(cases.map(([ ask ]) => ask()))).toEqual(expected);
		await t.client.close();
	});

	it('answer in each workspace from its own summary and rows', async () => {
		const [ A, B, C ] = [ 'ws-a', 'ws-b', 'ws-c' ];
		const kv = new MemoryKVStore();
		const p1 = [ { permission: 'view_tasks', anchors: [ { entity: 'project', keys: [ 'p1' ] } ] } ];
		await storeSummary(kv, { permissions: p1 }, A);
		await storeSummary(kv, { permissions: [ { permission: 'view_tasks', root: true } ] }, B);
		const c = new SyncClient({
			accountId: 'acct',
			workspaces: [ A, B, C ],
			defaultWorkspace: A,
			kvStore: kv,
			entities: [ TaskSchema, CommentSchema ],
			streamEnabled: false,
			pollIntervalMs: 0,
		});
		for (const ws of [ A, B, C ]) {
			await c.ws(ws).mutate.put(TaskSchema, { id: 't1', project: { id: 'p1' } });
			await c.ws(ws).mutate.put(TaskSchema, { id: 't2', project: { id: 'p2' } });
			await c.ws(ws).mutate.put(CommentSchema, { id: 'c1', task: { id: 't1' } });
			await c.ws(ws).mutate.put(CommentSchema, { id: 'c2', task: { id: 't2' } });
		}
		// The same key under another parent in B alone.
		await c.ws(B).mutate.patch(CommentSchema, 'c1', { task: { id: 't9' } });

		const cases: [() => Promise<CanAnswer>, CanAnswer][] = [
			[ () => c.ws(A).can(CommentSchema, 'c1', 'read'), 'yes' ],
			[ () => c.ws(B).can(CommentSchema, 'c1', 'read'), 'yes' ],
			[ () => c.ws(C).can(CommentSchema, 'c1', 'read'), 'unknown' ],
			[ () => c.ws(A).can(CommentSchema, 'c2', 'read'), 'no' ],
			[ () => c.ws(B).can(CommentSchema, 'c2', 'read'), 'yes' ],
			[ () => c.ws(A).hasPermission(CommentSchema, 'c2', 'view_tasks'), 'no' ],
			[ () => c.ws(B).hasPermission(CommentSchema, 'c1', 'edit_tasks'), 'unknown' ],
			[ () => c.ws(C).hasPermission(CommentSchema, 'c2', 'view_tasks'), 'unknown' ],
			[ () => c.ws(A).canCreate(CommentSchema, { id: 'n', task: { id: 't1' } }), 'no' ],
		];
		const expected = cases.map(([ , answer ]) => answer);
		expect(await oneByOne(cases.map(([ ask ]) => ask))).toEqual(expected);
		expect(await Promise.all(cases.map(([ ask ]) => ask()))).toEqual(expected);
		await c.close();
	});

	it('share one read of the store, one getMany for the summary and one per level of their chains', async () => {
		const store = new ReadCountingStore(new MemoryKVStore());
		const p1 = [ { permission: 'view_tasks', anchors: [ { entity: 'project', keys: [ 'p1' ] } ] } ];
		await storeSummary(store, { permissions: p1 });
		const c = offlineClient(store, [ TaskSchema, CommentSchema ]);
		for (let i = 0; i < 5; i++) {
			await c.mutate.put(TaskSchema, { id: `t${i}`, project: { id: i < 3 ? 'p1' : 'p2' } });
		}
		for (let i = 0; i < 50; i++) {
			await c.mutate.put(CommentSchema, { id: `c${i}`, task: { id: `t${i % 5}` } });
		}

		store.reads = 0;
		store.getManys = 0;
		const answers = await Promise.all(
			Array.from({ length: 50 }, (_, i) => c.can(CommentSchema, `c${i}`, 'read')),
		);
		expect(answers).toEqual(Array.from({ length: 50 }, (_, i) => i % 5 < 3 ? 'yes' : 'no'));
		expect(store.reads).toBe(1);
		expect(store.getManys).toBe(3);

		store.reads = 0;
		expect(await c.can(CommentSchema, 'c3', 'read')).toBe('no');
		expect(store.reads).toBe(1);
		await c.close();
	});

	it('all reject when the read fails, and the next ones answer', async () => {
		const store = new ReadCountingStore(new MemoryKVStore());
		const p1 = [ { permission: 'view_tasks', anchors: [ { entity: 'project', keys: [ 'p1' ] } ] } ];
		await storeSummary(store, { permissions: p1 });
		const c = offlineClient(store, [ TaskSchema, CommentSchema ]);
		await c.mutate.put(TaskSchema, { id: 't1', project: { id: 'p1' } });
		await c.mutate.put(CommentSchema, { id: 'c1', task: { id: 't1' } });
		const asks = () => [
			c.can(CommentSchema, 'c1', 'read'),
			c.can(TaskSchema, 't1', 'update', { fields: [ 'title' ] }),
			c.hasPermission(CommentSchema, 'c1', 'view_tasks'),
			c.canCreate(CommentSchema, { id: 'n', task: { id: 't1' } }),
		];

		for (const failing of [ 'get', 'getMany' ] as const) {
			store.failing = failing;
			const settled = await Promise.allSettled(asks());
			expect(settled.map((s) => s.status === 'rejected' && (s.reason as Error).message))
				.toEqual(Array(4).fill(`${failing} failed`));
		}
		store.failing = undefined;
		expect(await Promise.all(asks())).toEqual([ 'yes', 'no', 'yes', 'no' ]);
		await c.close();
	});

	it('answer around one that fails to decide', async () => {
		const kv = new MemoryKVStore();
		const p1 = [ { permission: 'view_tasks', anchors: [ { entity: 'project', keys: [ 'p1' ] } ] } ];
		await storeSummary(kv, { permissions: p1 });
		const c = offlineClient(kv, [ TaskSchema, CommentSchema ]);
		await c.mutate.put(TaskSchema, { id: 't1', project: { id: 'p1' } });
		await c.mutate.put(CommentSchema, { id: 'c1', task: { id: 't1' } });

		const settled = await Promise.allSettled([
			c.can(CommentSchema, 'c1', 'read'),
			// Not a list, so deciding the update throws.
			c.can(CommentSchema, 'c1', 'update', { fields: 42 as unknown as string[] }),
			c.hasPermission(CommentSchema, 'c1', 'view_tasks'),
		]);
		expect(settled.map((s) => s.status)).toEqual([ 'fulfilled', 'rejected', 'fulfilled' ]);
		expect(settled[0]).toEqual({ status: 'fulfilled', value: 'yes' });
		expect(settled[2]).toEqual({ status: 'fulfilled', value: 'yes' });
		await c.close();
	});
});

describe('the stored CallerAccess', () => {
	const entities = [ ProjectSchema, ProjectMemberSchema, TaskSchema ];

	it('is not decoded again for a local write that cannot change the grants', async () => {
		const kv = new MemoryKVStore();
		const c = offlineClient(kv, entities);
		// The first answer opens the store, so the summary written next is
		// stamped like one a pull writes.
		expect(await c.can(TaskSchema, 't1', 'read')).toBe('unknown');
		await storeSummary(kv, {
			permissions: [ {
				permission: 'edit_tasks',
				anchors: [ { entity: 'project', keys: [ 'p1' ] } ],
			} ],
		});
		await c.mutate.put(TaskSchema, { id: 't1', project: { id: 'p1' } });
		await c.mutate.put(TaskSchema, { id: 't2', project: { id: 'p2' } });
		const ask = () =>
			Promise.all([ 't1', 't2' ].map((k) => c.can(TaskSchema, k, 'update', { fields: [ 'title' ] })));

		decodes.callerAccess = 0;
		expect(await ask()).toEqual([ 'yes', 'no' ]);
		expect(decodes.callerAccess).toBe(1);
		for (let i = 0; i < 3; i++) {
			const before = await generationIn(kv, WS);
			await c.mutate.patch(TaskSchema, 't1', { title: `edit ${i}` });
			// The write moved the generation, so the answers are decided again.
			expect(await generationIn(kv, WS)).not.toBe(before);
			expect(await ask()).toEqual([ 'yes', 'no' ]);
		}
		expect(decodes.callerAccess).toBe(1);
		await c.close();
	});

	it('is not read again for a local write that cannot change the grants', async () => {
		const store = new KeyReadCountingStore(new MemoryKVStore(), metaCallerAccess(WS));
		const c = offlineClient(store, entities);
		// The first answer opens the store, so the summary written next is
		// stamped like one a pull writes.
		expect(await c.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('unknown');
		await storeSummary(store, {
			permissions: [ {
				permission: 'edit_tasks',
				anchors: [ { entity: 'project', keys: [ 'p1' ] } ],
			} ],
		});
		await c.mutate.put(TaskSchema, { id: 't1', project: { id: 'p1' } });
		expect(await c.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('yes');

		store.keyReads = 0;
		for (let i = 0; i < 3; i++) {
			await c.mutate.patch(TaskSchema, 't1', { title: `edit ${i}` });
			expect(await c.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('yes');
		}
		expect(store.keyReads).toBe(0);
		await c.close();
	});

	it('is read again once another client stores one, and not before', async () => {
		const server = new TestServer({ entities });
		server.put(TaskSchema, { id: 't1', project: { id: 'p1' } }, { workspace: WS });
		const grant = (...projects: string[]) =>
			server.setCallerAccess({
				permissions: [ {
					permission: 'edit_tasks',
					anchors: [ { entity: 'project', keys: projects } ],
				} ],
			}, { workspace: WS });
		// Two clients on one store stand in for two tabs, and only `applying`
		// syncs.
		const store = new KeyReadCountingStore(new MemoryKVStore(), metaCallerAccess(WS));
		const applying = new SyncClient({
			workspaceId: WS,
			kvStore: store,
			entities,
			transport: server.transport(),
			streamEnabled: false,
			presenceEnabled: false,
			pollIntervalMs: 0,
		});
		const tab = offlineClient(store, entities);
		await applying.start();
		const ask = () => tab.hasPermission(TaskSchema, 't1', 'edit_tasks');

		grant('p1');
		await applying.syncNow();
		expect(await ask()).toBe('yes');
		store.keyReads = 0;
		await tab.mutate.patch(TaskSchema, 't1', { title: 'local' });
		expect(await ask()).toBe('yes');
		expect(store.keyReads).toBe(0);

		grant('p2');
		await applying.syncNow();
		expect(await ask()).toBe('no');
		expect(await ask()).toBe('no');
		expect(store.keyReads).toBe(1);
		await applying.close();
		await tab.close();
	});

	it('answers unknown once another client deletes it', async () => {
		const kv = new MemoryKVStore();
		const c = offlineClient(kv, entities);
		expect(await c.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('unknown');
		await storeSummary(kv, { permissions: [ { permission: 'edit_tasks', root: true } ] });
		await c.mutate.put(TaskSchema, { id: 't1', project: { id: 'p1' } });
		expect(await c.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('yes');

		await deleteSummary(kv);
		expect(await c.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('unknown');
		await c.mutate.patch(TaskSchema, 't1', { title: 'local' });
		expect(await c.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('unknown');
		await storeSummary(kv, { permissions: [ { permission: 'edit_tasks', root: true } ] });
		expect(await c.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('yes');
		await c.close();
	});

	it('answers unknown once a bootstrap brings none', async () => {
		const t = await createTestClient({ entities });
		t.server.put(TaskSchema, { id: 't1', project: { id: 'p1' } });
		t.server.setCallerAccess({ permissions: [ { permission: 'edit_tasks', root: true } ] });
		await t.sync();
		const ask = () => t.client.hasPermission(TaskSchema, 't1', 'edit_tasks');
		expect(await ask()).toBe('yes');
		await t.client.mutate.patch(TaskSchema, 't1', { title: 'local' });
		expect(await ask()).toBe('yes');

		// A horizon past the client's cursor makes the next pull bootstrap
		// again, and the snapshot carries no CallerAccess.
		t.server.setCallerAccess(undefined);
		t.server.put(TaskSchema, { id: 't2', project: { id: 'p1' } });
		t.server.gc();
		await t.sync();
		expect(await t.client.get(TaskSchema, 't2')).toBeDefined();
		expect(await ask()).toBe('unknown');
		await t.close();
	});

	it('follows each change on a store that keeps no generations', async () => {
		const kv = new MemoryKVStore();
		// A store of the app's own need not keep generations.
		const untracked: KVStore = {
			read: () => kv.read(),
			write: () => kv.write(),
			close: () => kv.close(),
		};
		const c = offlineClient(untracked, roomEntities);
		const createPost = async (id: string) => {
			await c.mutate.put(PostSchema, { id, room: { id: 'r1' } });
			return (await c.get(PostSchema, id))?.authorId;
		};
		await storeSummary(kv, { subjects: [ 'ann' ], permissions: inR1('post') });
		expect(await c.hasPermission(RoomSchema, 'r1', 'post')).toBe('yes');
		expect(await createPost('po-1')).toBe('ann');
		expect(await c.hasPermission(RoomSchema, 'r1', 'post')).toBe('yes');

		await storeSummary(kv, { subjects: [ 'bob' ], permissions: inR1('view_posts') });
		expect(await c.hasPermission(RoomSchema, 'r1', 'post')).toBe('no');
		expect(await createPost('po-2')).toBe('bob');

		await deleteSummary(kv);
		expect(await c.hasPermission(RoomSchema, 'r1', 'post')).toBe('unknown');
		expect(await createPost('po-3')).toBe('');
		await c.close();
	});

	it('changes the answers as soon as the server sends a new one, of the same length too', async () => {
		const t = await createTestClient({ entities });
		t.server.put(TaskSchema, { id: 't1', project: { id: 'p1' } });
		t.server.put(TaskSchema, { id: 't2', project: { id: 'p2' } });
		const ask = () =>
			Promise.all(
				[ 't1', 't2' ].map((k) => t.client.can(TaskSchema, k, 'update', { fields: [ 'title' ] })),
			);
		const stored = () =>
			t.client.inspect().read(async (tx) =>
				(await tx.get(metaCallerAccess(DEFAULT_WORKSPACE))) as Uint8Array
			);
		const grant = (...projects: string[]) =>
			t.server.setCallerAccess({
				permissions: [ {
					permission: 'edit_tasks',
					anchors: [ { entity: 'project', keys: projects } ],
				} ],
			});

		grant('p1');
		await t.sync();
		expect(await ask()).toEqual([ 'yes', 'no' ]);
		const first = await stored();

		grant('p2');
		await t.sync();
		const second = await stored();
		expect(second.length).toBe(first.length);
		expect(second).not.toEqual(first);
		expect(await ask()).toEqual([ 'no', 'yes' ]);

		grant('p1', 'p2');
		await t.sync();
		expect(await ask()).toEqual([ 'yes', 'yes' ]);

		grant();
		await t.sync();
		expect(await ask()).toEqual([ 'no', 'no' ]);
		await t.close();
	});

	it('answers unknown while a write to a grant row is queued, and is not decoded again over it', async () => {
		const t = await createTestClient({ entities });
		t.server.put(TaskSchema, { id: 't1', project: { id: 'p1' } });
		t.server.setCallerAccess({ permissions: [ { permission: 'edit_tasks', root: true } ] });
		await t.sync();
		const ask = () => t.client.can(TaskSchema, 't1', 'update', { fields: [ 'title' ] });

		decodes.callerAccess = 0;
		expect(await ask()).toBe('yes');
		await t.client.mutate.put(ProjectMemberSchema, {
			id: 'pm-new',
			project: { id: 'p1' },
			userId: 'someone',
			role: 'project_reader',
		});
		expect(await ask()).toBe('unknown');
		await t.client.mutate.patch(TaskSchema, 't1', { title: 'queued too' });
		expect(await ask()).toBe('unknown');

		// The server sends a CallerAccess only when it changes, so the pull that
		// acks the writes carries none and the client keeps the one it holds.
		t.server.setCallerAccess(undefined);
		await t.sync();
		expect(await ask()).toBe('yes');
		expect(decodes.callerAccess).toBe(1);
		await t.close();
	});

	it('is not decoded by the client that stored it, until another client stores one', async () => {
		const server = new TestServer({ entities });
		server.put(TaskSchema, { id: 't1', project: { id: 'p1' } }, { workspace: WS });
		server.put(TaskSchema, { id: 't2', project: { id: 'p2' } }, { workspace: WS });
		const grant = (...projects: string[]) =>
			server.setCallerAccess({
				permissions: [ {
					permission: 'edit_tasks',
					anchors: [ { entity: 'project', keys: projects } ],
				} ],
			}, { workspace: WS });
		// Two clients on one store stand in for two tabs, and only `applying`
		// syncs.
		const kv = new MemoryKVStore();
		const applying = new SyncClient({
			workspaceId: WS,
			kvStore: kv,
			entities,
			transport: server.transport(),
			streamEnabled: false,
			presenceEnabled: false,
			pollIntervalMs: 0,
		});
		const other = offlineClient(kv, entities);
		await applying.start();
		const ask = (c: SyncClient) =>
			Promise.all([ 't1', 't2' ].map((k) => c.can(TaskSchema, k, 'update', { fields: [ 'title' ] })));

		// A client keeps what it stores only for a workspace it asks in.
		expect(await ask(applying)).toEqual([ 'unknown', 'unknown' ]);
		decodes.callerAccess = 0;
		grant('p1');
		await applying.syncNow();
		expect(await ask(applying)).toEqual([ 'yes', 'no' ]);
		grant('p2');
		await applying.syncNow();
		expect(await ask(applying)).toEqual([ 'no', 'yes' ]);
		expect(decodes.callerAccess).toBe(0);
		expect(await ask(other)).toEqual([ 'no', 'yes' ]);
		expect(decodes.callerAccess).toBe(1);

		await storeSummary(kv, { permissions: [ { permission: 'edit_tasks', root: true } ] });
		expect(await ask(applying)).toEqual([ 'yes', 'yes' ]);
		expect(decodes.callerAccess).toBe(2);
		await applying.close();
		await other.close();
	});

	it('answers unknown once the workspace is purged', async () => {
		const kv = new MemoryKVStore();
		// Two clients on one store stand in for two tabs.
		const tab = offlineClient(kv, entities);
		const purging = offlineClient(kv, entities);
		expect(await tab.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('unknown');
		await storeSummary(kv, { permissions: [ { permission: 'edit_tasks', root: true } ] });
		expect(await tab.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('yes');
		expect(await purging.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('yes');

		await purging.removeWorkspace(WS, { purge: true });
		expect(await tab.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('unknown');
		expect(await purging.addWorkspace(WS).hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe(
			'unknown',
		);
		await tab.close();
		await purging.close();
	});
});

describe('the pending queue', () => {
	const entities = [ ProjectSchema, ProjectMemberSchema, TaskSchema ];
	const member = { project: { id: 'p1' }, userId: 'someone', role: 'project_reader' };
	const root = { permissions: [ { permission: 'edit_tasks', root: true } ] };

	it('is not read for a prediction after a local write that cannot change the grants', async () => {
		const store = new QueueScanCountingStore(new MemoryKVStore());
		const c = offlineClient(store, entities);
		// The first answer opens the store, so the summary written next is
		// stamped like one a pull writes.
		expect(await c.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('unknown');
		await storeSummary(store, root);
		await c.mutate.put(TaskSchema, { id: 't1', project: { id: 'p1' } });
		expect(await c.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('yes');

		store.queueScans = 0;
		for (let i = 0; i < 3; i++) {
			await c.mutate.patch(TaskSchema, 't1', { title: `edit ${i}` });
			expect(await c.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('yes');
		}
		expect(store.queueScans).toBe(0);
		await c.close();
	});

	// Two clients on one store stand in for two tabs.
	it('answers unknown while either client has a write to a grant row queued', async () => {
		const kv = new MemoryKVStore();
		const first = offlineClient(kv, entities);
		const second = offlineClient(kv, entities);
		const ask = () =>
			Promise.all([ first, second ].map((c) => c.hasPermission(TaskSchema, 't1', 'edit_tasks')));
		expect(await ask()).toEqual([ 'unknown', 'unknown' ]);
		await storeSummary(kv, root);
		for (let i = 0; i < 3; i++) {
			await second.mutate.put(TaskSchema, { id: `t${i}`, project: { id: 'p1' } });
		}
		expect(await ask()).toEqual([ 'yes', 'yes' ]);

		await second.mutate.put(ProjectMemberSchema, { id: 'pm-second', ...member });
		expect(await ask()).toEqual([ 'unknown', 'unknown' ]);
		await first.mutate.patch(TaskSchema, 't1', { title: 'queued behind it' });
		expect(await ask()).toEqual([ 'unknown', 'unknown' ]);
		await first.mutate.put(ProjectMemberSchema, { id: 'pm-first', ...member });
		await second.mutate.patch(TaskSchema, 't1', { title: 'and behind that' });
		expect(await ask()).toEqual([ 'unknown', 'unknown' ]);
		await first.close();
		await second.close();
	});

	it('is not read once the write to a grant row is acked or rolled back', async () => {
		const server = new TestServer({ entities });
		server.put(TaskSchema, { id: 't1', project: { id: 'p1' } }, { workspace: WS });
		server.setCallerAccess(root, { workspace: WS });
		const store = new QueueScanCountingStore(new MemoryKVStore());
		const never = 2 ** 31 - 1;
		const c = new SyncClient({
			workspaceId: WS,
			kvStore: store,
			entities,
			transport: server.transport(),
			streamEnabled: false,
			presenceEnabled: false,
			pollIntervalMs: 0,
			// Pushes only when the spec syncs, so no push scans the queue between
			// a write and the prediction after it.
			pushDebounceMs: never,
			pushMaxDelayMs: never,
		});
		await c.start();
		await c.syncNow();
		const ask = () => c.hasPermission(TaskSchema, 't1', 'edit_tasks');
		/** Writes once more, and counts the scans of the queue the next answer makes. */
		const scansAfterWrite = async () => {
			store.queueScans = 0;
			await c.mutate.patch(TaskSchema, 't1', { title: `edit ${Math.random()}` });
			expect(await ask()).toBe('yes');
			return store.queueScans;
		};
		// The first write counts the queue, which no write has counted yet.
		await c.mutate.patch(TaskSchema, 't1', { title: 'first' });
		expect(await ask()).toBe('yes');
		expect(await scansAfterWrite()).toBe(0);

		await c.mutate.put(ProjectMemberSchema, { id: 'pm-acked', ...member });
		expect(await ask()).toBe('unknown');
		// The server sends a CallerAccess only when it changes, so the pull that
		// acks the write carries none and the client keeps the one it holds.
		server.setCallerAccess(undefined, { workspace: WS });
		await c.syncNow();
		expect(await ask()).toBe('yes');
		expect(await scansAfterWrite()).toBe(0);

		// Acks the patch first, so the pull after the rollback drops nothing and
		// the rollback alone has to bring the answer back.
		await c.syncNow();
		server.reject({ entity: ProjectMemberSchema });
		await c.mutate.put(ProjectMemberSchema, { id: 'pm-rejected', ...member });
		expect(await ask()).toBe('unknown');
		await c.syncNow();
		expect(await ask()).toBe('yes');
		expect(await scansAfterWrite()).toBe(0);
		await c.close();
	});

	it('keeps the answer through a compaction of the queue', async () => {
		const store = new QueueScanCountingStore(new MemoryKVStore());
		const c = offlineClient(store, entities);
		const compact = () =>
			compactPending(store, new EntityRegistry(entities), {}, WS, () => new Set<bigint>());
		expect(await c.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('unknown');
		await storeSummary(store, root);
		await c.mutate.put(TaskSchema, { id: 't1', project: { id: 'p1' } });
		for (let i = 0; i < 3; i++) await c.mutate.patch(TaskSchema, 't1', { title: `edit ${i}` });
		expect(await c.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('yes');

		expect((await compact()).changed).toBe(true);
		store.queueScans = 0;
		expect(await c.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('yes');
		await c.mutate.patch(TaskSchema, 't1', { title: 'after the compaction' });
		expect(await c.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('yes');
		expect(store.queueScans).toBe(0);

		// The fold lowers the mutation-id counter to the value it held before
		// the write that a build without the record queued.
		await c.mutate.patch(TaskSchema, 't1', { title: 'folds into the one before' });
		await queueUnrecorded(store, { id: 'pm-old', ...member });
		expect(await c.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('unknown');
		expect((await compact()).changed).toBe(true);
		expect(await c.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('unknown');
		await c.close();
	});

	it('answers unknown while a build that does not count them has a write to a grant row queued', async () => {
		const kv = new MemoryKVStore();
		const c = offlineClient(kv, entities);
		expect(await c.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('unknown');
		await storeSummary(kv, root);
		await c.mutate.put(TaskSchema, { id: 't1', project: { id: 'p1' } });
		expect(await c.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('yes');

		await queueUnrecorded(kv, { id: 'pm-old', ...member });
		expect(await c.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('unknown');
		await c.mutate.patch(TaskSchema, 't1', { title: 'queued behind it' });
		expect(await c.hasPermission(TaskSchema, 't1', 'edit_tasks')).toBe('unknown');
		await c.close();
	});

	it('keeps its count of writes that can change the grants exact through each rewrite', async () => {
		const kv = new MemoryKVStore();
		const c = offlineClient(kv, entities);
		const registry = new EntityRegistry(entities);
		/** The count the record gives, and the count of the queue itself. */
		const counts = async () => {
			const tx = await kv.read();
			try {
				const [ record, last ] = await tx.getMany([ metaPendingAccess(WS), metaLastMutationId(WS) ]);
				return {
					recorded: pendingAccessOf(record, last),
					queued: countAccessChanges(registry, await readPending(tx, WS)),
				};
			} finally {
				tx.release();
			}
		};
		await c.mutate.put(TaskSchema, { id: 't1', project: { id: 'p1' } });
		await c.mutate.put(ProjectMemberSchema, { id: 'pm-a', ...member });
		await c.mutate.patch(TaskSchema, 't1', { title: 'a' });
		await c.mutate.patch(TaskSchema, 't1', { title: 'b' });
		await c.mutate.put(ProjectMemberSchema, { id: 'pm-b', ...member });
		expect(await counts()).toEqual({ recorded: 2, queued: 2 });

		expect((await compactPending(kv, registry, {}, WS, () => new Set<bigint>())).changed).toBe(true);
		expect(await counts()).toEqual({ recorded: 2, queued: 2 });
		// Drops the put of the task at the head, and renumbers the rest from 1.
		expect((await dropAndResequencePending(kv, registry, {}, WS, 1n)).changed).toBe(true);
		expect(await counts()).toEqual({ recorded: 2, queued: 2 });
		// The put of pm-a, renumbered to 1.
		await rollbackRejected(kv, registry, {}, WS, [ 1n ]);
		expect(await counts()).toEqual({ recorded: 1, queued: 1 });
		expect((await resequencePending(kv, registry, {}, WS, 1n)).changed).toBe(true);
		expect(await counts()).toEqual({ recorded: 1, queued: 1 });
		await c.mutate.patch(TaskSchema, 't1', { title: 'c' });
		expect(await counts()).toEqual({ recorded: 1, queued: 1 });
		// The put of pm-b, at 2 behind the patch at 1, so nothing is renumbered.
		expect((await dropAndResequencePending(kv, registry, {}, WS, 2n)).changed).toBe(false);
		expect(await counts()).toEqual({ recorded: 0, queued: 0 });
		await c.close();
	});

	it('keeps the answer through a purge of the workspace', async () => {
		const kv = new MemoryKVStore();
		// Two clients on one store stand in for two tabs.
		const tab = offlineClient(kv, entities);
		const purging = offlineClient(kv, entities);
		const ask = () => tab.hasPermission(TaskSchema, 't1', 'edit_tasks');
		expect(await ask()).toBe('unknown');
		await storeSummary(kv, root);
		await purging.mutate.put(ProjectMemberSchema, { id: 'pm-purged', ...member });
		expect(await ask()).toBe('unknown');

		await purging.removeWorkspace(WS, { purge: true });
		await storeSummary(kv, root);
		expect(await ask()).toBe('yes');
		const scope = purging.addWorkspace(WS);
		await scope.mutate.put(TaskSchema, { id: 't1', project: { id: 'p1' } });
		expect(await ask()).toBe('yes');
		await scope.mutate.put(ProjectMemberSchema, { id: 'pm-after', ...member });
		expect(await ask()).toBe('unknown');
		await tab.close();
		await purging.close();
	});
});

/**
 * Queues a put of a project member in WS, and moves the mutation-id counter,
 * as a build that keeps no pending-access record does.
 */
async function queueUnrecorded(
	kv: KVStore,
	row: MessageInitShape<typeof ProjectMemberSchema>,
): Promise<void> {
	const tx = await kv.write();
	const raw = (await tx.get(metaLastMutationId(WS))) as string | undefined;
	const mutationId = (raw ? BigInt(raw) : 0n) + 1n;
	const mutation = create(MutationSchema, {
		clientId: 'older-build',
		mutationId,
		payload: {
			case: 'crud',
			value: {
				entity: 'project_member',
				key: String(row.id),
				op: CrudOp.PUT,
				value: toBinary(ProjectMemberSchema, create(ProjectMemberSchema, row)),
			},
		},
	});
	await tx.put(pendingKey(WS, mutationId), toBinary(MutationSchema, mutation));
	await tx.put(metaLastMutationId(WS), mutationId.toString());
	await tx.commit();
}

async function generationIn(kv: KVStore, workspaceId: string): Promise<unknown> {
	const tx = await kv.read();
	try {
		return await tx.get(accessGeneration(workspaceId));
	} finally {
		tx.release();
	}
}
