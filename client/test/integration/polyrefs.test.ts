import { describe, expect, it } from 'vitest';

import { subscription } from '../../src/client.js';
import {
	AttachmentSchema,
	type Memo,
	MemoSchema,
	type OneofRefEntity,
	OneofRefEntitySchema,
	type Project,
	ProjectSchema,
	type User,
	UserSchema,
} from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import type { Row } from '../../src/read.js';
import { freshWorkspace, newClient, newMultiClient } from './helpers.js';

// Polymorphic references against the real engine: a oneof of references on
// the holder (OneofRefEntity.target), inside a workspace-qualified reference
// message (Memo's OwnerRef) and mixed with a plain member (Attachment.parent).
// The server projects whichever case a row holds, follows the relation by the
// oneof's name into every case's entity, and answers matches() on one case
// inside the request's workspace.

const entities = {
	entities: [ ProjectSchema, UserSchema, OneofRefEntitySchema, MemoSchema, AttachmentSchema ],
};
const keysOf = (rows: readonly { key: string }[]) => rows.map((r) => r.key).sort();
const nameOf = (row: Row<unknown> | undefined) =>
	(row?.value as { name?: string } | undefined)?.name;

describe('polymorphic references', () => {
	it('round-trips whichever case a row holds, projected down to its key', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws, entities);
		await writer.start();
		await writer.mutate.put(ProjectSchema, { id: 'p1', name: 'Alpha' });
		await writer.mutate.put(UserSchema, { id: 'u1', name: 'Maya' });
		await writer.mutate.put(OneofRefEntitySchema, {
			id: 'r-proj',
			// A fat stub: the server keeps the key alone.
			target: { case: 'project', value: { id: 'p1', name: 'smuggled' } },
		});
		await writer.mutate.put(OneofRefEntitySchema, {
			id: 'r-user',
			target: { case: 'user', value: { id: 'u1' } },
		});
		await writer.mutate.put(MemoSchema, {
			id: 'm1',
			owner: { owner: { case: 'user', value: { id: 'u1', name: 'smuggled' } } },
			mentions: [ { owner: { case: 'project', value: { id: 'p1' } } }, {
				owner: { case: 'user', value: { id: 'u1' } },
			} ],
		});
		await writer.mutate.put(AttachmentSchema, {
			id: 'a-label',
			parent: { case: 'label', value: 'loose' },
		});
		await writer.mutate.put(AttachmentSchema, {
			id: 'a-user',
			parent: { case: 'user', value: { user: { id: 'u1' } } },
		});
		await writer.syncNow();
		expect(writer.status().pendingMutations).toBe(0);

		const reader = newClient(ws, entities);
		await reader.start();
		await reader.syncNow();
		const rProj = (await reader.get(OneofRefEntitySchema, 'r-proj'))!;
		expect(rProj.target).toMatchObject({ case: 'project', value: { id: 'p1', name: '' } });
		const m1 = (await reader.get(MemoSchema, 'm1'))!;
		expect(m1.owner?.owner).toMatchObject({ case: 'user', value: { id: 'u1', name: '' } });
		expect(m1.mentions.map((e) => e.owner.case)).toEqual([ 'project', 'user' ]);
		expect((await reader.get(AttachmentSchema, 'a-label'))?.parent).toEqual({
			case: 'label',
			value: 'loose',
		});

		expect(
			nameOf(
				await reader.resolveRef(OneofRefEntitySchema, 'target', rProj) as Row<Project> | undefined,
			),
		).toBe('Alpha');
		const rUser = (await reader.get(OneofRefEntitySchema, 'r-user'))!;
		expect(
			nameOf(await reader.resolveRef(OneofRefEntitySchema, 'target', rUser) as Row<User> | undefined),
		).toBe('Maya');
		const mentions = await reader.resolveRef(MemoSchema, 'mentions', m1) as Row<Project | User>[];
		expect(mentions.map(nameOf)).toEqual([ 'Alpha', 'Maya' ]);
		const aUser = (await reader.get(AttachmentSchema, 'a-user'))!;
		expect(
			nameOf(await reader.resolveRef(AttachmentSchema, 'parent', aUser) as Row<User> | undefined),
		).toBe('Maya');

		const user = (await reader.get(UserSchema, 'u1'))!;
		expect(
			keysOf(await reader.resolveReverseRef(UserSchema, 'oneof_refs', user) as Row<OneofRefEntity>[]),
		).toEqual([ 'r-user' ]);
		expect(keysOf(await reader.resolveReverseRef(UserSchema, 'memosByOwner', user) as Row<Memo>[]))
			.toEqual([ 'm1' ]);
		expect(keysOf(await reader.resolveReverseRef(UserSchema, 'attachments', user) as Row<Memo>[]))
			.toEqual([ 'a-user' ]);

		await reader.close();
		await writer.close();
	});

	it('switches the case with a PATCH naming the group', async () => {
		const ws = freshWorkspace();
		const client = newClient(ws, entities);
		await client.start();
		await client.mutate.put(ProjectSchema, { id: 'p1', name: 'Alpha' });
		await client.mutate.put(UserSchema, { id: 'u1', name: 'Maya' });
		await client.mutate.put(OneofRefEntitySchema, {
			id: 'r1',
			target: { case: 'project', value: { id: 'p1' } },
		});
		await client.syncNow();
		await client.mutate.patch(OneofRefEntitySchema, 'r1', {
			target: { case: 'user', value: { id: 'u1' } },
		});
		await client.syncNow();
		expect(client.status().pendingMutations).toBe(0);

		const reader = newClient(ws, entities);
		await reader.start();
		await reader.syncNow();
		expect((await reader.get(OneofRefEntitySchema, 'r1'))?.target).toMatchObject({
			case: 'user',
			value: { id: 'u1' },
		});
		await reader.close();
		await client.close();
	});

	it("follows the relation by the oneof's name into every case's entity", async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws, entities);
		await writer.start();
		await writer.mutate.put(ProjectSchema, { id: 'p1', name: 'Alpha' });
		await writer.mutate.put(ProjectSchema, { id: 'p9', name: 'Unreferenced' });
		await writer.mutate.put(UserSchema, { id: 'u1', name: 'Maya' });
		await writer.mutate.put(UserSchema, { id: 'u9', name: 'Unreferenced' });
		await writer.mutate.put(OneofRefEntitySchema, {
			id: 'r-proj',
			target: { case: 'project', value: { id: 'p1' } },
		});
		await writer.mutate.put(OneofRefEntitySchema, {
			id: 'r-user',
			target: { case: 'user', value: { id: 'u1' } },
		});
		await writer.syncNow();

		const reader = newClient(ws, {
			...entities,
			subscriptions: [ subscription(OneofRefEntitySchema, undefined, { include: { target: true } }) ],
		});
		await reader.start();
		await reader.syncNow();
		expect((await reader.get(ProjectSchema, 'p1'))?.name).toBe('Alpha');
		expect((await reader.get(UserSchema, 'u1'))?.name).toBe('Maya');
		expect(await reader.get(ProjectSchema, 'p9')).toBeUndefined();
		expect(await reader.get(UserSchema, 'u9')).toBeUndefined();

		// Switching r-user's case to the project drops the user from the closure.
		await writer.mutate.patch(OneofRefEntitySchema, 'r-user', {
			target: { case: 'project', value: { id: 'p1' } },
		});
		await writer.syncNow();
		await reader.syncNow();
		expect(await reader.get(UserSchema, 'u1')).toBeUndefined();
		expect((await reader.get(ProjectSchema, 'p1'))?.name).toBe('Alpha');

		await writer.close();
		await reader.close();
	});

	it('is answered per case by the server, inside one workspace', async () => {
		const proj = freshWorkspace();
		const org = freshWorkspace();
		const client = newMultiClient([ proj, org ], entities);
		await client.start();
		await client.ws(proj).mutate.put(ProjectSchema, { id: 'p1', name: 'Alpha' });
		await client.ws(proj).mutate.put(UserSchema, { id: 'u1', name: 'Proj Maya' });
		await client.ws(org).mutate.put(UserSchema, { id: 'u1', name: 'Org Maya' });
		await client.ws(proj).mutate.put(MemoSchema, {
			id: 'm-proj',
			owner: { owner: { case: 'project', value: { id: 'p1' } } },
		});
		await client.ws(proj).mutate.put(MemoSchema, {
			id: 'm-user',
			owner: { owner: { case: 'user', value: { id: 'u1' } } },
		});
		await client.ws(proj).mutate.put(MemoSchema, {
			id: 'm-org',
			owner: { workspaceId: org, owner: { case: 'user', value: { id: 'u1' } } },
		});
		await client.syncNow();

		expect(keysOf(
			await client.ws(proj).queryServer(MemoSchema, {
				where: (m) => m.owner.owner.project.matches((p) => p.name.eq('Alpha')),
			}),
		)).toEqual([ 'm-proj' ]);
		expect(keysOf(
			await client.ws(proj).queryServer(MemoSchema, {
				where: (m) => m.owner.owner.user.matches((u) => u.name.eq('Proj Maya')),
			}),
		)).toEqual([ 'm-user' ]);
		// The server evaluates inside the request's workspace: m-org's stub
		// names another one, so it matches nothing there.
		expect(keysOf(
			await client.ws(proj).queryServer(MemoSchema, {
				where: (m) => m.owner.owner.user.matches((u) => u.name.eq('Org Maya')),
			}),
		)).toEqual([]);
		// The client holds both, and resolves the stub where it points.
		expect(keysOf(
			await client.ws(proj).query(MemoSchema, {
				where: (m) => m.owner.owner.user.matches((u) => u.name.eq('Org Maya')),
			}),
		)).toEqual([ 'm-org' ]);

		await client.close();
	});
});
