// hasPermission(), can() and canCreate() answer from the CallerAccess stored
// for a workspace and from the pending queue, and a screen of buttons asks
// them again on every status change. These specs pin what the answers cost,
// that a remembered answer follows every write that changes it, that an access
// parent this client does not register still decides, and how can() fills in
// the inputs of the access rules from local rows.

import { create, type MessageInitShape, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import {
	CardItemSchema,
	CardSchema,
	CommentSchema,
	DraftSchema,
	FolderSchema,
	PinSchema,
	PostSchema,
	ProjectMemberSchema,
	ProjectSchema,
	RoomMemberSchema,
	RoomSchema,
	SettingSchema,
	SpaceSchema,
	TaskSchema,
	TicketSchema,
} from '../../src/gen/syncplane/testdata/access/v1/access_pb.js';
import { CallerAccessSchema } from '../../src/gen/syncplane/v1/service_pb.js';
import { metaCallerAccess, pendingPrefix } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { GenerationOf, KVRead, KVStore, KVWrite } from '../../src/kv/store.js';
import { createTestClient } from '../../src/testing/index.js';

const WS = 'ws-can';

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

async function storeSummary(
	kv: KVStore,
	access: MessageInitShape<typeof CallerAccessSchema>,
): Promise<void> {
	const tx = await kv.write();
	await tx.put(
		metaCallerAccess(WS),
		toBinary(CallerAccessSchema, create(CallerAccessSchema, access)),
	);
	await tx.commit();
}

function offlineClient(
	kvStore: KVStore,
	entities: readonly (typeof TaskSchema | typeof ProjectSchema | typeof ProjectMemberSchema)[],
): SyncClient {
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
async function roomClient(permissions: MessageInitShape<typeof CallerAccessSchema>['permissions']) {
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
});
