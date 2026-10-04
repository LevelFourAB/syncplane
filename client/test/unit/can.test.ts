// hasPermission(), can() and canCreate() answer from the CallerAccess stored
// for a workspace and from the pending queue, and a screen of buttons asks
// them again on every status change. These specs pin what the answers cost,
// that a remembered answer follows every write that changes it, that an access
// parent this client does not register still decides, how can() fills in
// the inputs of the access rules from local rows, that predictions asked
// together share one read of the store and answer as they do one at a time,
// and that the stored CallerAccess is decoded again only when it changes.

import { create, type DescMessage, type MessageInitShape, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it, vi } from 'vitest';

import type { CanAnswer } from '../../src/access.js';
import { SyncClient } from '../../src/client.js';
import {
	CardItemSchema,
	CardSchema,
	CommentSchema,
	DocSchema,
	DraftSchema,
	FolderSchema,
	PageSchema,
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
import { accessGeneration, metaCallerAccess, pendingPrefix } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { GenerationOf, KVRead, KVStore, KVWrite } from '../../src/kv/store.js';
import { createTestClient, DEFAULT_WORKSPACE, TestServer } from '../../src/testing/index.js';

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

async function storeSummary(
	kv: KVStore,
	access: MessageInitShape<typeof CallerAccessSchema>,
	ws = WS,
): Promise<void> {
	const tx = await kv.write();
	await tx.put(
		metaCallerAccess(ws),
		toBinary(CallerAccessSchema, create(CallerAccessSchema, access)),
	);
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

	it('share one read of the store, and one getMany per level of their chains', async () => {
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
		expect(store.getManys).toBe(2);

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

async function generationIn(kv: KVStore, workspaceId: string): Promise<unknown> {
	const tx = await kv.read();
	try {
		return await tx.get(accessGeneration(workspaceId));
	} finally {
		tx.release();
	}
}
