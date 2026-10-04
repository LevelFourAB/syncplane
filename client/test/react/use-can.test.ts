// The access hooks over the testing kit: the in-memory server sends the
// CallerAccess a spec sets, and each hook answers from it and follows it as it
// changes.

import { act, renderHook, waitFor } from '@testing-library/react';
import { createElement, type ReactNode } from 'react';
import { describe, expect, it } from 'vitest';

import type { SyncClient } from '../../src/client.js';
import {
	CommentSchema,
	LabelSchema,
	NoteSchema,
	OrgMemberSchema,
	PostSchema,
	ProjectMemberSchema,
	ProjectSchema,
	RoomMemberSchema,
	RoomSchema,
	TaskSchema,
} from '../../src/gen/syncplane/testdata/access/v1/access_pb.js';
import { createReactSync, useCan, useCanCreate, useHasPermission } from '../../src/react/index.js';
import { createTestClient } from '../../src/testing/index.js';

const entities = [
	OrgMemberSchema,
	ProjectSchema,
	ProjectMemberSchema,
	TaskSchema,
	LabelSchema,
	NoteSchema,
] as const;
const { SyncProvider } = createReactSync(entities);

function wrapper(client: SyncClient) {
	return function Wrapper({ children }: { children: ReactNode }): ReactNode {
		return createElement(SyncProvider, { client }, children);
	};
}

/** Counts the predictions the hooks ask `client` for. */
function countAsks(client: SyncClient): { count: number } {
	const asks = { count: 0 };
	const canIn = client.canIn.bind(client);
	const hasPermissionIn = client.hasPermissionIn.bind(client);
	client.canIn = ((...args: Parameters<typeof canIn>) => {
		asks.count++;
		return canIn(...args);
	}) as typeof canIn;
	client.hasPermissionIn = ((...args: Parameters<typeof hasPermissionIn>) => {
		asks.count++;
		return hasPermissionIn(...args);
	}) as typeof hasPermissionIn;
	return asks;
}

/** The hub's live watchers of both kinds: one per underlying subscription. */
function watcherCount(client: SyncClient): number {
	const hub = (client as unknown as {
		hubInstance?: { watchers: Set<unknown>; keyWatchers: Set<unknown> };
	}).hubInstance;
	return (hub?.watchers.size ?? 0) + (hub?.keyWatchers.size ?? 0);
}

/** The keys the hub's keyed watchers read. */
function watchedKeys(client: SyncClient): string[] {
	const hub = (client as unknown as {
		hubInstance?: { keyWatchers: Set<{ refs: Map<string, { key: string }> }> };
	}).hubInstance;
	const out: string[] = [];
	for (const w of hub?.keyWatchers ?? []) {
		for (const ref of w.refs.values()) out.push(ref.key);
	}
	return out.sort();
}

/** Lets every answer a change set off land. */
async function settle(): Promise<void> {
	await act(async () => {
		await new Promise((r) => setTimeout(r, 20));
	});
}

describe('useHasPermission', () => {
	it('answers from the summary the server sends, and follows it', async () => {
		const t = await createTestClient({ entities });
		t.server.put(ProjectSchema, { id: 'p1' });
		t.server.put(TaskSchema, { id: 't1', project: { id: 'p1' } });
		t.server.setCallerAccess({
			permissions: [ {
				permission: 'view_tasks',
				anchors: [ { entity: 'project', keys: [ 'p1' ] } ],
			} ],
		});
		await t.sync();

		const { result } = renderHook(() => useHasPermission(TaskSchema, 't1', 'edit_tasks'), {
			wrapper: wrapper(t.client as unknown as SyncClient),
		});
		await waitFor(() => expect(result.current).toBe('no'));

		t.server.setCallerAccess({
			permissions: [
				{ permission: 'view_tasks', anchors: [ { entity: 'project', keys: [ 'p1' ] } ] },
				{ permission: 'edit_tasks', anchors: [ { entity: 'project', keys: [ 'p1' ] } ] },
			],
		});
		await act(() => t.sync());
		await waitFor(() => expect(result.current).toBe('yes'));
		await t.client.close();
	});

	it('is unknown before any summary arrives', async () => {
		const t = await createTestClient({ entities });
		const { result } = renderHook(() => useHasPermission(TaskSchema, 't1', 'view_tasks'), {
			wrapper: wrapper(t.client as unknown as SyncClient),
		});
		await act(async () => {
			await new Promise((r) => setTimeout(r, 0));
		});
		expect(result.current).toBe('unknown');
		await t.client.close();
	});
});

const roomEntities = [ RoomSchema, RoomMemberSchema, PostSchema ] as const;
const { db: roomDb } = createReactSync(roomEntities);
const posting = (...perms: string[]) => ({
	subjects: [ 'ann' ],
	permissions: perms.map((permission) => ({
		permission,
		anchors: [ { entity: 'room', keys: [ 'r1' ] } ],
	})),
});

describe('useCan', () => {
	it('answers for an operation and follows the summary', async () => {
		const t = await createTestClient({ entities: roomEntities });
		t.server.put(RoomSchema, { id: 'r1' });
		t.server.put(PostSchema, { id: 'po', room: { id: 'r1' }, authorId: 'ann' });
		t.server.setCallerAccess(posting('view_posts', 'post'));
		await t.sync();

		const { result } = renderHook(() => useCan(PostSchema, 'po', 'update', { fields: [ 'body' ] }), {
			wrapper: wrapper(t.client as unknown as SyncClient),
		});
		await waitFor(() => expect(result.current).toBe('yes'));

		t.server.setCallerAccess(posting('view_posts'));
		await act(() => t.sync());
		await waitFor(() => expect(result.current).toBe('no'));
		await t.client.close();
	});

	// The test server sends the summary with every pull, which announces an
	// access change on its own. The specs below stop it after the first sync,
	// so only the write under test can make a hook ask again.

	it('answers unknown while a grant write waits for its ack, and answers again once acked', async () => {
		const t = await createTestClient({ entities: roomEntities });
		t.server.put(RoomSchema, { id: 'r1' });
		t.server.put(PostSchema, { id: 'po', room: { id: 'r1' }, authorId: 'ann' });
		t.server.setCallerAccess(posting('view_posts', 'post'));
		await t.sync();
		t.server.setCallerAccess(undefined);

		const { result } = renderHook(() => useCan(PostSchema, 'po', 'update', { fields: [ 'body' ] }), {
			wrapper: wrapper(t.client as unknown as SyncClient),
		});
		await waitFor(() => expect(result.current).toBe('yes'));

		await act(() =>
			t.client.mutate.put(RoomMemberSchema, {
				id: 'rm-bob',
				room: { id: 'r1' },
				userId: 'bob',
				role: 'room_viewer',
			})
		);
		await waitFor(() => expect(result.current).toBe('unknown'));

		await act(() => t.sync());
		await waitFor(() => expect(result.current).toBe('yes'));
		await t.client.close();
	});

	it('answers again when the server rejects a grant write', async () => {
		const t = await createTestClient({ entities: roomEntities });
		t.server.put(RoomSchema, { id: 'r1' });
		t.server.put(PostSchema, { id: 'po', room: { id: 'r1' }, authorId: 'ann' });
		t.server.setCallerAccess(posting('view_posts', 'post'));
		await t.sync();
		t.server.setCallerAccess(undefined);
		t.server.reject({ entity: RoomMemberSchema });

		const { result } = renderHook(() => useCan(PostSchema, 'po', 'update', { fields: [ 'body' ] }), {
			wrapper: wrapper(t.client as unknown as SyncClient),
		});
		await waitFor(() => expect(result.current).toBe('yes'));

		await act(() =>
			t.client.mutate.put(RoomMemberSchema, {
				id: 'rm-bob',
				room: { id: 'r1' },
				userId: 'bob',
				role: 'room_viewer',
			})
		);
		await waitFor(() => expect(result.current).toBe('unknown'));

		await act(() => t.sync());
		expect(t.rejections).toHaveLength(1);
		await waitFor(() => expect(result.current).toBe('yes'));
		await t.client.close();
	});

	it('does not ask again for a write to a row that no other row sits below', async () => {
		const t = await createTestClient({ entities: roomEntities });
		t.server.put(RoomSchema, { id: 'r1' });
		t.server.put(PostSchema, { id: 'po', room: { id: 'r1' }, authorId: 'ann' });
		t.server.setCallerAccess(posting('view_posts', 'post'));
		await t.sync();
		t.server.setCallerAccess(undefined);

		const asks = countAsks(t.client as unknown as SyncClient);
		const { result } = renderHook(() => useCan(PostSchema, 'po', 'update', { fields: [ 'body' ] }), {
			wrapper: wrapper(t.client as unknown as SyncClient),
		});
		await waitFor(() => expect(result.current).toBe('yes'));
		await settle();
		const before = asks.count;

		await act(() => t.client.mutate.put(PostSchema, { id: 'other', room: { id: 'r1' }, body: 'hi' }));
		await settle();
		expect(asks.count).toBe(before);

		await act(() => t.sync());
		await settle();
		expect(asks.count).toBe(before);
		expect(result.current).toBe('yes');
		await t.client.close();
	});
});

describe('access hooks on a row', () => {
	// Rooms r1 and r2, and post po in r1 by ann, who may post in r1 only. The
	// summary stops after the first sync, so only the row can set off an ask.
	async function room(posts: readonly string[] = [ 'po' ]) {
		const t = await createTestClient({ entities: roomEntities });
		t.server.put(RoomSchema, { id: 'r1' });
		t.server.put(RoomSchema, { id: 'r2' });
		for (const id of posts) t.server.put(PostSchema, { id, room: { id: 'r1' }, authorId: 'ann' });
		t.server.setCallerAccess(posting('view_posts', 'post'));
		await t.sync();
		t.server.setCallerAccess(undefined);
		const client = t.client as unknown as SyncClient;
		return { t, client, asks: countAsks(client) };
	}

	const canEdit = (key: string) => useCan(PostSchema, key, 'update', { fields: [ 'body' ] });

	it('share one keyed watch across the rows of an entity, and ask once each at mount', async () => {
		const ids = [ 'p0', 'p1', 'p2', 'p3', 'p4' ];
		const { t, client, asks } = await room(ids);

		const { result, unmount } = renderHook(
			() => [ ...ids.map(canEdit), ...ids.map((id) => useHasPermission(PostSchema, id, 'post')) ],
			{ wrapper: wrapper(client) },
		);
		await waitFor(() => expect(result.current).toEqual([ ...ids, ...ids ].map(() => 'yes')));
		await settle();
		expect(asks.count).toBe(2 * ids.length);
		expect(watcherCount(client)).toBe(1);
		expect(watchedKeys(client)).toEqual(ids);

		unmount();
		await settle();
		expect(watcherCount(client)).toBe(0);
		await t.client.close();
	});

	it('asks again for a change of the access parent or a subject field, and not for other fields', async () => {
		const { t, client, asks } = await room();
		const { result } = renderHook(() => canEdit('po'), { wrapper: wrapper(client) });
		await waitFor(() => expect(result.current).toBe('yes'));
		await settle();
		expect(asks.count).toBe(1);

		await act(() => t.client.mutate.patch(PostSchema, 'po', { title: 'renamed' }));
		await settle();
		await act(() => t.sync());
		await settle();
		expect(asks.count).toBe(1);

		t.server.put(PostSchema, { id: 'po', room: { id: 'r1' }, authorId: 'bob' });
		await act(() => t.sync());
		await waitFor(() => expect(result.current).toBe('no'));
		await settle();
		expect(asks.count).toBe(2);

		t.server.put(PostSchema, { id: 'po', room: { id: 'r2' }, authorId: 'ann' });
		await act(() => t.sync());
		await settle();
		expect(asks.count).toBe(3);
		expect(result.current).toBe('no');

		await act(() => t.client.mutate.patch(PostSchema, 'po', { room: { id: 'r1' } }));
		await waitFor(() => expect(result.current).toBe('yes'));
		await settle();
		expect(asks.count).toBe(4);
		// The ack keeps the inputs the local write already showed.
		await act(() => t.sync());
		await settle();
		expect(asks.count).toBe(4);
		await t.client.close();
	});

	it('asks again when the row is deleted and when it is created again', async () => {
		const { t, client, asks } = await room();
		const { result } = renderHook(() => canEdit('po'), { wrapper: wrapper(client) });
		await waitFor(() => expect(result.current).toBe('yes'));
		await settle();

		t.server.del(PostSchema, 'po');
		await act(() => t.sync());
		await waitFor(() => expect(result.current).toBe('unknown'));
		await settle();
		expect(asks.count).toBe(2);

		await act(() => t.client.mutate.put(PostSchema, { id: 'po', room: { id: 'r1' }, body: 'again' }));
		await waitFor(() => expect(result.current).toBe('yes'));
		await settle();
		expect(asks.count).toBe(3);
		await t.client.close();
	});

	it('does not ask useHasPermission again for a subject field', async () => {
		const { t, client, asks } = await room();
		const { result } = renderHook(() => useHasPermission(PostSchema, 'po', 'post'), {
			wrapper: wrapper(client),
		});
		await waitFor(() => expect(result.current).toBe('yes'));
		await settle();
		expect(asks.count).toBe(1);

		t.server.put(PostSchema, { id: 'po', room: { id: 'r1' }, authorId: 'bob' });
		await act(() => t.sync());
		await settle();
		expect(asks.count).toBe(1);

		t.server.put(PostSchema, { id: 'po', room: { id: 'r2' }, authorId: 'bob' });
		await act(() => t.sync());
		await waitFor(() => expect(result.current).toBe('no'));
		await settle();
		expect(asks.count).toBe(2);
		await t.client.close();
	});

	it('shares the watch with useRow on the row, and each keeps working without the other', async () => {
		const { t, client } = await room();
		const can = renderHook(() => canEdit('po'), { wrapper: wrapper(client) });
		const row = renderHook(() => roomDb.post.useRow('po'), { wrapper: wrapper(client) });
		await waitFor(() => expect(can.result.current).toBe('yes'));
		await waitFor(() => expect(row.result.current.row?.value.authorId).toBe('ann'));
		expect(watcherCount(client)).toBe(1);

		row.unmount();
		await settle();
		expect(watcherCount(client)).toBe(1);
		t.server.put(PostSchema, { id: 'po', room: { id: 'r2' }, authorId: 'ann' });
		await act(() => t.sync());
		await waitFor(() => expect(can.result.current).toBe('no'));

		const again = renderHook(() => roomDb.post.useRow('po'), { wrapper: wrapper(client) });
		await waitFor(() => expect(again.result.current.row?.value.room?.id).toBe('r2'));
		can.unmount();
		await settle();
		expect(watcherCount(client)).toBe(1);
		t.server.put(PostSchema, { id: 'po', room: { id: 'r2' }, authorId: 'ann', title: 'kept' });
		await act(() => t.sync());
		await waitFor(() => expect(again.result.current.row?.value.title).toBe('kept'));

		again.unmount();
		await settle();
		expect(watcherCount(client)).toBe(0);
		await t.client.close();
	});
});

const chainEntities = [ ProjectSchema, ProjectMemberSchema, TaskSchema, CommentSchema ] as const;

describe('useHasPermission below an anchor', () => {
	it('answers again when a row up the chain moves, by pull and by a local write', async () => {
		const t = await createTestClient({ entities: chainEntities });
		t.server.put(ProjectSchema, { id: 'p1' });
		t.server.put(ProjectSchema, { id: 'p2' });
		t.server.put(TaskSchema, { id: 't1', project: { id: 'p1' } });
		t.server.put(CommentSchema, { id: 'c1', task: { id: 't1' } });
		t.server.setCallerAccess({
			permissions: [ {
				permission: 'view_tasks',
				anchors: [ { entity: 'project', keys: [ 'p1' ] } ],
			} ],
		});
		await t.sync();
		t.server.setCallerAccess(undefined);

		const { result } = renderHook(() => useHasPermission(CommentSchema, 'c1', 'view_tasks'), {
			wrapper: wrapper(t.client as unknown as SyncClient),
		});
		await waitFor(() => expect(result.current).toBe('yes'));

		t.server.put(TaskSchema, { id: 't1', project: { id: 'p2' } });
		await act(() => t.sync());
		await waitFor(() => expect(result.current).toBe('no'));

		await act(() => t.client.mutate.patch(TaskSchema, 't1', { project: { id: 'p1' } }));
		await waitFor(() => expect(result.current).toBe('yes'));
		await t.client.close();
	});
});

describe('useCanCreate', () => {
	it('answers for a draft row and follows the summary', async () => {
		const t = await createTestClient({ entities: roomEntities });
		t.server.put(RoomSchema, { id: 'r1' });
		t.server.setCallerAccess(posting('view_posts'));
		await t.sync();

		const { result } = renderHook(() => useCanCreate(PostSchema, { id: 'new', room: { id: 'r1' } }), {
			wrapper: wrapper(t.client as unknown as SyncClient),
		});
		await waitFor(() => expect(result.current).toBe('no'));

		t.server.setCallerAccess(posting('view_posts', 'post'));
		await act(() => t.sync());
		await waitFor(() => expect(result.current).toBe('yes'));
		await t.client.close();
	});
});
