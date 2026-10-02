// can() answers from the CallerAccess stored for a workspace and from the
// pending queue, and a screen of useCan buttons asks it again on every status
// change. These specs pin what the answers cost, that a remembered answer
// follows every write that changes it, and that an access parent this client
// does not register still decides.

import { create, type MessageInitShape, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import {
	CommentSchema,
	FolderSchema,
	ProjectMemberSchema,
	ProjectSchema,
	TaskSchema,
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

describe('can()', () => {
	it('reads the pending queue once for many answers', async () => {
		const store = new QueueScanCountingStore(new MemoryKVStore());
		const c = offlineClient(store, [ ProjectSchema, ProjectMemberSchema, TaskSchema ]);
		// The first answer opens the store, so the summary written next is
		// stamped like one a pull writes.
		expect(await c.can(TaskSchema, 't0', 'edit_tasks')).toBe('unknown');
		await storeSummary(store, { permissions: [ { permission: 'edit_tasks', root: true } ] });
		store.queueScans = 0;

		const answers = await Promise.all(
			Array.from({ length: 10 }, (_, i) => c.can(TaskSchema, `t${i}`, 'edit_tasks')),
		);
		expect(answers.every((a) => a === 'yes')).toBe(true);
		expect(await c.can(TaskSchema, 't1', 'edit_tasks')).toBe('yes');
		expect(store.queueScans).toBe(1);
		await c.close();
	});

	it('follows a local write that can change the grants', async () => {
		const kv = new MemoryKVStore();
		await storeSummary(kv, { permissions: [ { permission: 'edit_tasks', root: true } ] });
		const c = offlineClient(kv, [ ProjectSchema, ProjectMemberSchema, TaskSchema ]);
		expect(await c.can(TaskSchema, 't1', 'edit_tasks')).toBe('yes');

		await c.mutate.put(ProjectMemberSchema, {
			id: 'pm-new',
			project: { id: 'p1' },
			userId: 'someone',
			role: 'project_reader',
		});
		expect(await c.can(TaskSchema, 't1', 'edit_tasks')).toBe('unknown');
		await c.close();
	});

	// Two clients on one store stand in for two tabs: only the store can tell
	// the first that the second queued a write.
	it('follows a write another client queues on the same store', async () => {
		const kv = new MemoryKVStore();
		const first = offlineClient(kv, [ ProjectSchema, ProjectMemberSchema, TaskSchema ]);
		const second = offlineClient(kv, [ ProjectSchema, ProjectMemberSchema, TaskSchema ]);
		expect(await first.can(TaskSchema, 't1', 'edit_tasks')).toBe('unknown');
		await storeSummary(kv, { permissions: [ { permission: 'edit_tasks', root: true } ] });
		expect(await first.can(TaskSchema, 't1', 'edit_tasks')).toBe('yes');

		await second.mutate.put(ProjectMemberSchema, {
			id: 'pm-new',
			project: { id: 'p1' },
			userId: 'someone',
			role: 'project_reader',
		});
		expect(await first.can(TaskSchema, 't1', 'edit_tasks')).toBe('unknown');
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
		expect(await t.client.can(TaskSchema, 't1', 'edit_tasks')).toBe('no');

		t.server.setCallerAccess({ permissions: [ { permission: 'edit_tasks', root: true } ] });
		await t.sync();
		expect(await t.client.can(TaskSchema, 't1', 'edit_tasks')).toBe('yes');
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

		expect(await c.can(TaskSchema, 't1', 'view_tasks')).toBe('yes');
		expect(await c.can(TaskSchema, 't2', 'view_tasks')).toBe('no');
		expect(await c.can(TaskSchema, 't3', 'view_tasks')).toBe('unknown');
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
		expect(await t.client.can(CommentSchema, 'c1', 'view_tasks')).toBe('yes');
		expect(await t.client.can(CommentSchema, 'c2', 'view_tasks')).toBe('no');
		expect(await t.client.can(CommentSchema, 'c9', 'view_tasks')).toBe('unknown');

		// A queued move counts where the row will be once the server accepts it.
		await t.client.mutate.patch(TaskSchema, 't2', { project: { id: 'p1' } });
		expect(await t.client.can(CommentSchema, 'c2', 'view_tasks')).toBe('yes');
		await t.client.close();
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
		expect(await t.client.can(FolderSchema, 'low', 'view_folders')).toBe('yes');
		expect(await t.client.can(FolderSchema, 'top', 'view_folders')).toBe('no');
		await t.client.close();
	});
});
