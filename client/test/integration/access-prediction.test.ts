// can() and canCreate() promise to evaluate the access rules "as the server
// does": a 'yes' tells an app to show a control. These specs pin the
// predictions against the real server for the write checks that sit beside
// the rules: the subset rule on grant rows, the read rules of the rows below a
// row whose change moves what those rows show, the workspace gate, which
// refuses a caller that holds no permission, and rules that need two
// permissions the caller holds at different anchors. Each spec asks for the
// prediction, makes the write, and reads the server's verdict.

import { createConnectTransport } from '@connectrpc/connect-node';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { type RejectionInfo, SyncClient } from '../../src/client.js';
import {
	AccountMemberSchema,
	AccountSchema,
	BillerSchema,
	ChargeSchema,
	DossierSchema,
	FolderMemberSchema,
	FolderSchema,
	InvoiceSchema,
	MatterSchema,
	MemoSchema,
	NoteSchema,
	ProjectMemberSchema,
	ProjectSchema,
	StaffSchema,
	TaskSchema,
} from '../../src/gen/syncplane/testdata/access/v1/access_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { startTestServer, type TestServer } from '../support/testserver.js';
import { freshWorkspace } from './helpers.js';

let server: TestServer;
beforeAll(async () => {
	server = await startTestServer([ '-access' ]);
});
afterAll(() => server?.stop());

const entities = [
	ProjectSchema,
	ProjectMemberSchema,
	TaskSchema,
	DossierSchema,
	MatterSchema,
	MemoSchema,
	FolderSchema,
	FolderMemberSchema,
	StaffSchema,
	NoteSchema,
	AccountSchema,
	AccountMemberSchema,
	BillerSchema,
	InvoiceSchema,
	ChargeSchema,
];

const open: SyncClient[] = [];
afterEach(async () => {
	await Promise.all(open.splice(0).map((c) => c.close()));
});

/** A started, synced client for `user` that records each rejection in `rejections`. */
async function clientAs(
	user: string,
	ws: string,
	rejections: RejectionInfo[],
): Promise<SyncClient> {
	const client = new SyncClient({
		transport: createConnectTransport({
			baseUrl: server.url,
			httpVersion: '1.1',
			interceptors: [ (next) => (req) => {
				req.header.set('Authorization', `Bearer ${user}`);
				return next(req);
			} ],
		}),
		workspaceId: ws,
		kvStore: new MemoryKVStore(),
		entities,
		pollIntervalMs: 0,
		streamEnabled: false,
		onMutationRejected: (info) => rejections.push(info),
	});
	open.push(client);
	await client.start();
	await client.syncNow();
	return client;
}

async function trusted(workspace: string, writes: object[]): Promise<void> {
	const res = await fetch(`${server.url}/debug/trusted`, {
		method: 'POST',
		body: JSON.stringify({ workspace, writes }),
	});
	if (!res.ok) throw new Error(`trusted write failed: ${await res.text()}`);
}

/** The keys of the rows whose writes the server rejected, as `entity/key`. */
const rejectedRows = (rejections: RejectionInfo[]) =>
	rejections.flatMap((r) => r.targets.map((t) => `${t.entity}/${t.key}`));

describe('a prediction about a grant row', () => {
	// ada is project_admin at p1: she manages its members, but she does not
	// hold edit_projects, which project_owner grants. olaf owns p1 and rita
	// reads it.
	async function seed(ws: string): Promise<void> {
		await trusted(ws, [
			{ entity: 'project', value: { id: 'p1', name: 'one' } },
			{
				entity: 'project_member',
				value: { id: 'pm-ada', project: { id: 'p1' }, userId: 'ada', role: 'project_admin' },
			},
			{
				entity: 'project_member',
				value: { id: 'pm-olaf', project: { id: 'p1' }, userId: 'olaf', role: 'project_owner' },
			},
			{
				entity: 'project_member',
				value: { id: 'pm-rita', project: { id: 'p1' }, userId: 'rita', role: 'project_reader' },
			},
		]);
	}

	it('does not answer yes for the delete of a grant whose role holds more than the caller holds', async () => {
		// Given
		const ws = freshWorkspace();
		await seed(ws);
		const rejections: RejectionInfo[] = [];
		const ada = await clientAs('ada', ws, rejections);
		expect(await ada.get(ProjectMemberSchema, 'pm-olaf')).toBeDefined();

		// When
		const removeOwner = await ada.can(ProjectMemberSchema, 'pm-olaf', 'delete');
		const removeReader = await ada.can(ProjectMemberSchema, 'pm-rita', 'delete');
		await ada.mutate.del(ProjectMemberSchema, 'pm-olaf');
		await ada.mutate.del(ProjectMemberSchema, 'pm-rita');
		await ada.syncNow();

		// Then the server refuses the first delete and applies the second ...
		expect(rejectedRows(rejections)).toEqual([ 'project_member/pm-olaf' ]);
		expect(rejections[0]?.errorCode).toBe('access_denied');
		expect(removeReader).toBe('yes');
		// ... and the prediction for the refused one is not 'yes'. The client
		// holds every input: the role of pm-olaf, what project_owner grants, and
		// what ada holds at p1.
		expect(removeOwner).not.toBe('yes');
	});

	it('does not answer yes for the create of a grant whose role holds more than the caller holds', async () => {
		// Given
		const ws = freshWorkspace();
		await seed(ws);
		const rejections: RejectionInfo[] = [];
		const ada = await clientAs('ada', ws, rejections);

		// When
		const owner = { id: 'pm-bob', project: { id: 'p1' }, userId: 'bob', role: 'project_owner' };
		const reader = { id: 'pm-cid', project: { id: 'p1' }, userId: 'cid', role: 'project_reader' };
		const inviteOwner = await ada.canCreate(ProjectMemberSchema, owner);
		const inviteReader = await ada.canCreate(ProjectMemberSchema, reader);
		await ada.mutate.put(ProjectMemberSchema, owner);
		await ada.mutate.put(ProjectMemberSchema, reader);
		await ada.syncNow();

		// Then
		expect(rejectedRows(rejections)).toEqual([ 'project_member/pm-bob' ]);
		expect(inviteReader).toBe('yes');
		expect(inviteOwner).not.toBe('yes');
	});

	it('does not answer yes for an update of the role, which the new role decides', async () => {
		// Given
		const ws = freshWorkspace();
		await seed(ws);
		const rejections: RejectionInfo[] = [];
		const ada = await clientAs('ada', ws, rejections);

		// When
		const promote = await ada.can(ProjectMemberSchema, 'pm-rita', 'update', { fields: [ 'role' ] });
		await ada.mutate.patch(ProjectMemberSchema, 'pm-rita', { role: 'project_owner' });
		await ada.syncNow();

		// Then
		expect(rejectedRows(rejections)).toEqual([ 'project_member/pm-rita' ]);
		expect(promote).not.toBe('yes');
	});
});

describe('a prediction about a row that rows sit below', () => {
	// ed is dossier_editor at d-open: he edits the dossier, but he reads none
	// of the matters and memos below it. A change of `private` changes who
	// reads those rows, which the server admits only for a caller who reads
	// them where the dossier is.
	async function seed(ws: string): Promise<void> {
		await trusted(ws, [
			{ entity: 'dossier', value: { id: 'd-open', title: 'open', ownerId: 'olga' } },
			{ entity: 'matter', value: { id: 'm-open', dossier: { id: 'd-open' }, title: 'open' } },
			{
				entity: 'dossier_member',
				value: { id: 'dm-ed', dossier: { id: 'd-open' }, userId: 'ed', role: 'dossier_editor' },
			},
		]);
	}

	it('does not answer yes for a change of a field that a within entry below reads, by a caller who does not read the rows below', async () => {
		// Given
		const ws = freshWorkspace();
		await seed(ws);
		const rejections: RejectionInfo[] = [];
		const ed = await clientAs('ed', ws, rejections);
		expect(await ed.get(DossierSchema, 'd-open')).toBeDefined();

		// When
		const rename = await ed.can(DossierSchema, 'd-open', 'update', { fields: [ 'title' ] });
		const hide = await ed.can(DossierSchema, 'd-open', 'update', { fields: [ 'private' ] });
		await ed.mutate.patch(DossierSchema, 'd-open', { title: 'renamed' });
		await ed.mutate.patch(DossierSchema, 'd-open', { private: true });
		await ed.syncNow();

		// Then
		expect(rejectedRows(rejections)).toEqual([ 'dossier/d-open' ]);
		expect(rejections[0]?.errorMessage).toContain('needs one of the permissions');
		expect(rename).toBe('yes');
		// ed holds view_matters and view_open_matters nowhere, which the
		// summary shows, so the client can tell.
		expect(hide).not.toBe('yes');
	});
});

describe('a prediction about a move of a row with guarded fields', () => {
	// s1 sits in p1. No client reads its bonus or pin, and only guest, whom it
	// is about, reads its review, wherever it sits. A caller with
	// edit_projects reads its salary and bio where it sits. olaf owns p1 and
	// p2. ada is project_admin at p1, without edit_projects, and owns p2.
	async function seed(ws: string): Promise<void> {
		await trusted(ws, [
			{ entity: 'project', value: { id: 'p1', name: 'one' } },
			{ entity: 'project', value: { id: 'p2', name: 'two' } },
			{
				entity: 'staff',
				value: {
					id: 's1',
					project: { id: 'p1' },
					name: 'ann',
					personId: 'guest',
					bonus: '7',
					pin: '1234',
					review: 'good',
				},
			},
			...[ [ 'pm-olaf1', 'p1', 'olaf', 'project_owner' ], [
				'pm-olaf2',
				'p2',
				'olaf',
				'project_owner',
			], [
				'pm-ada1',
				'p1',
				'ada',
				'project_admin',
			], [ 'pm-ada2', 'p2', 'ada', 'project_owner' ] ].map(([ id, project, userId, role ]) => ({
				entity: 'project_member',
				value: { id, project: { id: project }, userId, role },
			})),
		]);
	}

	it('does not answer no for a caller who reads every field another caller reads where the row sits, as the server admits', async () => {
		// Given
		const ws = freshWorkspace();
		await seed(ws);
		const rejections: RejectionInfo[] = [];
		const olaf = await clientAs('olaf', ws, rejections);
		expect(await olaf.get(StaffSchema, 's1')).toBeDefined();

		// When
		const move = await olaf.can(StaffSchema, 's1', 'update', { fields: [ 'project' ] });
		await olaf.mutate.patch(StaffSchema, 's1', { project: { id: 'p2' } });
		await olaf.syncNow();

		// Then the server applies the move. The client cannot answer yes, as
		// olaf's grants sit at projects and the new parent decides.
		expect(rejections).toEqual([]);
		expect(move).toBe('unknown');
	});

	it('does not answer yes for a caller who does not read the salary where the row sits, as the server refuses', async () => {
		// Given
		const ws = freshWorkspace();
		await seed(ws);
		const rejections: RejectionInfo[] = [];
		const ada = await clientAs('ada', ws, rejections);
		expect(await ada.get(StaffSchema, 's1')).toBeDefined();

		// When
		const move = await ada.can(StaffSchema, 's1', 'update', { fields: [ 'project' ] });
		await ada.mutate.patch(StaffSchema, 's1', { project: { id: 'p2' } });
		await ada.syncNow();

		// Then
		expect(rejectedRows(rejections)).toEqual([ 'staff/s1' ]);
		expect(move).not.toBe('yes');
	});
});

describe('a prediction about a write that adds an access parent in a recursive chain', () => {
	// Folders sit in folders. eve is folder_editor at f0, which holds f1. She
	// holds no grant at f-secret, so she does not read it.
	async function seed(ws: string): Promise<void> {
		await trusted(ws, [
			{ entity: 'folder', value: { id: 'f0', name: 'f0' } },
			{ entity: 'folder', value: { id: 'f1', name: 'f1', parent: { id: 'f0' } } },
			{ entity: 'folder', value: { id: 'f-secret', name: 'secret' } },
			{
				entity: 'folder_member',
				value: { id: 'fm-eve', folder: { id: 'f0' }, userId: 'eve', role: 'folder_editor' },
			},
		]);
	}

	it('answers yes only for a parent the caller reads, as the server admits', async () => {
		// Given
		const ws = freshWorkspace();
		await seed(ws);
		const rejections: RejectionInfo[] = [];
		const eve = await clientAs('eve', ws, rejections);
		expect(await eve.get(FolderSchema, 'f1')).toBeDefined();
		expect(await eve.get(FolderSchema, 'f-secret')).toBeUndefined();

		// When she asks about a folder in f1, one in f-secret and one in a
		// folder that does not exist, and then creates each
		const inRead = { id: 'g1', name: 'g1', parent: { id: 'f1' } };
		const inHidden = { id: 'g2', name: 'g2', parent: { id: 'f-secret' } };
		const inMissing = { id: 'g3', name: 'g3', parent: { id: 'f-missing' } };
		const predictions = [
			await eve.canCreate(FolderSchema, inRead),
			await eve.canCreate(FolderSchema, inHidden),
			await eve.canCreate(FolderSchema, inMissing),
		];
		await eve.mutate.put(FolderSchema, inRead);
		await eve.mutate.put(FolderSchema, inHidden);
		await eve.mutate.put(FolderSchema, inMissing);
		await eve.syncNow();

		// Then
		expect(rejections.map((r) => [ r.errorCode, ...r.targets.map((t) => `${t.entity}/${t.key}`) ]))
			.toEqual([ [ 'access_denied', 'folder/g2' ], [ 'access_denied', 'folder/g3' ] ]);
		expect(predictions[0]).toBe('yes');
		expect(predictions[1]).not.toBe('yes');
		expect(predictions[2]).not.toBe('yes');
	});
});

describe('the summary on the wire', () => {
	it('is left out of a pull whose cursor already carries it, so no access listener runs', async () => {
		// Given rita, who reads p1, synced once
		const ws = freshWorkspace();
		await trusted(ws, [
			{ entity: 'project', value: { id: 'p1', name: 'one' } },
			{
				entity: 'project_member',
				value: { id: 'pm-rita', project: { id: 'p1' }, userId: 'rita', role: 'project_reader' },
			},
		]);
		const rita = await clientAs('rita', ws, []);
		const changes: string[] = [];
		rita.onAccessChange((w) => changes.push(w));

		// When she pulls again with nothing changed
		await rita.syncNow();
		await rita.syncNow();

		// Then
		expect(changes).toEqual([]);
	});
});

describe('a caller who loses access', () => {
	// rita reads p1 through pm-rita-1, and in some specs p2 through pm-rita-2
	// as well. t1 sits in p1. Every caller the workspace gate admits writes
	// note n1.
	async function seed(ws: string, both: boolean): Promise<void> {
		await trusted(ws, [
			{ entity: 'project', value: { id: 'p1', name: 'one' } },
			{ entity: 'project', value: { id: 'p2', name: 'two' } },
			{ entity: 'task', value: { id: 't1', project: { id: 'p1' }, title: 'secret' } },
			{ entity: 'note', value: { id: 'n1', body: 'hello' } },
			{
				entity: 'project_member',
				value: { id: 'pm-rita-1', project: { id: 'p1' }, userId: 'rita', role: 'project_reader' },
			},
			...(both
				? [ {
					entity: 'project_member',
					value: { id: 'pm-rita-2', project: { id: 'p2' }, userId: 'rita', role: 'project_reader' },
				} ]
				: []),
		]);
	}

	/** Deletes rita's grant at p1 as a trusted write. */
	async function revokeP1(ws: string): Promise<void> {
		const res = await fetch(`${server.url}/debug/trusted`, {
			method: 'POST',
			body: JSON.stringify({
				workspace: ws,
				writes: [ { entity: 'project_member', key: 'pm-rita-1', delete: true } ],
			}),
		});
		if (!res.ok) throw new Error(`trusted delete failed: ${await res.text()}`);
	}

	it('drops the rows below an anchor the caller loses while it keeps another grant', async () => {
		// Given
		const ws = freshWorkspace();
		await seed(ws, true);
		const rita = await clientAs('rita', ws, []);
		expect((await rita.get(TaskSchema, 't1'))?.title).toBe('secret');

		// When
		await revokeP1(ws);
		await rita.syncNow();

		// Then
		expect(await rita.get(TaskSchema, 't1')).toBeUndefined();
	});

	it('drops the rows below an anchor the caller loses when it was its last grant', async () => {
		// Given
		const ws = freshWorkspace();
		await seed(ws, false);
		const rita = await clientAs('rita', ws, []);
		expect((await rita.get(TaskSchema, 't1'))?.title).toBe('secret');

		// When
		await revokeP1(ws);
		await rita.syncNow();

		// Then the server answers that rita holds nothing here ...
		expect(rita.status().workspaces[ws]?.access).toBe('none');
		// ... and the task she may no longer read is not left on her device,
		// as it is not when she keeps another grant.
		expect(await rita.get(TaskSchema, 't1')).toBeUndefined();
	});

	it("answers no for a write that every admitted caller may make once it was the caller's last grant", async () => {
		// Given
		const ws = freshWorkspace();
		await seed(ws, false);
		const rejections: RejectionInfo[] = [];
		const rita = await clientAs('rita', ws, rejections);
		expect(await rita.can(NoteSchema, 'n1', 'update')).toBe('yes');

		// When
		await revokeP1(ws);
		await rita.syncNow();
		expect(rita.status().workspaces[ws]?.access).toBe('none');

		// Then the gate refuses rita, so no rule admits her
		expect(await rita.can(NoteSchema, 'n1', 'update')).toBe('no');
		expect(await rita.canCreate(NoteSchema, { id: 'n2', body: 'new' })).toBe('no');
		expect(await rita.hasPermission(ProjectSchema, 'p1', 'edit_tasks')).toBe('no');

		// And when she keeps another grant, the gate admits her
		const ws2 = freshWorkspace();
		await seed(ws2, true);
		const kept = await clientAs('rita', ws2, rejections);
		await revokeP1(ws2);
		await kept.syncNow();
		expect(await kept.can(NoteSchema, 'n1', 'update')).toBe('yes');
	});
});

describe('a prediction about a rule that needs two permissions held at different anchors', () => {
	// a1 and a2 hold an invoice and a charge each; the invoice rules need an
	// account permission and a billing permission. ed edits a1 and is a
	// billing clerk at the root; vi edits a1 and bills a2 through account
	// roles only.
	async function seed(ws: string): Promise<void> {
		await trusted(ws, [
			{ entity: 'account', value: { id: 'a1' } },
			{ entity: 'account', value: { id: 'a2' } },
			{ entity: 'invoice', value: { id: 'i1', account: { id: 'a1' }, title: 'one' } },
			{ entity: 'invoice', value: { id: 'i2', account: { id: 'a2' }, title: 'two' } },
			{ entity: 'charge', value: { id: 'c1', invoice: { id: 'i1' }, memo: 'x' } },
			{ entity: 'charge', value: { id: 'c2', invoice: { id: 'i2' }, memo: 'y' } },
			{
				entity: 'account_member',
				value: { id: 'am-ed', account: { id: 'a1' }, userId: 'ed', role: 'account_editor' },
			},
			{ entity: 'biller', value: { id: 'b-ed', userId: 'ed', role: 'billing_clerk' } },
			{
				entity: 'account_member',
				value: { id: 'am-vi1', account: { id: 'a1' }, userId: 'vi', role: 'account_editor' },
			},
			{
				entity: 'account_member',
				value: { id: 'am-vi2', account: { id: 'a2' }, userId: 'vi', role: 'account_biller' },
			},
		]);
	}

	it('answers yes for a caller who holds one at the row and one at the root, as the server admits', async () => {
		// Given
		const ws = freshWorkspace();
		await seed(ws);
		const rejections: RejectionInfo[] = [];
		const ed = await clientAs('ed', ws, rejections);

		// When
		const answers = {
			readI1: await ed.can(InvoiceSchema, 'i1', 'read'),
			updateC1: await ed.can(ChargeSchema, 'c1', 'update'),
			createC: await ed.canCreate(ChargeSchema, { id: 'c9', invoice: { id: 'i1' } }),
		};
		await ed.mutate.patch(ChargeSchema, 'c1', { memo: 'edited' });
		await ed.mutate.put(ChargeSchema, { id: 'c9', invoice: { id: 'i1' }, memo: 'new' });
		await ed.syncNow();
		await ed.syncNow();

		// Then
		expect(answers).toEqual({ readI1: 'yes', updateC1: 'yes', createC: 'yes' });
		expect(await ed.get(InvoiceSchema, 'i1')).toBeDefined();
		expect(rejections.map((r) => r.errorMessage)).toEqual([]);
	});

	it('does not answer yes for a caller who holds each at a different account, as the server sends neither invoice', async () => {
		// Given
		const ws = freshWorkspace();
		await seed(ws);
		const vi = await clientAs('vi', ws, []);

		// When
		const answers = {
			readI1: await vi.can(InvoiceSchema, 'i1', 'read'),
			readI2: await vi.can(InvoiceSchema, 'i2', 'read'),
			createC1: await vi.canCreate(ChargeSchema, { id: 'c8', invoice: { id: 'i1' } }),
			createC2: await vi.canCreate(ChargeSchema, { id: 'c9', invoice: { id: 'i2' } }),
		};

		// Then
		expect({
			...answers,
			heldI1: (await vi.get(InvoiceSchema, 'i1')) !== undefined,
			heldI2: (await vi.get(InvoiceSchema, 'i2')) !== undefined,
		}).toEqual({
			readI1: expect.not.stringMatching(/^yes$/),
			readI2: expect.not.stringMatching(/^yes$/),
			createC1: expect.not.stringMatching(/^yes$/),
			createC2: expect.not.stringMatching(/^yes$/),
			heldI1: false,
			heldI2: false,
		});
	});
});
