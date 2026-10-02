import { describe, expect, it } from 'vitest';

import {
	OrgMemberSchema,
	ProjectMemberSchema,
	ProjectSchema,
	StaffSchema,
} from '../../src/gen/syncplane/testdata/access/v1/access_pb.js';
import { UndoRefusedError } from '../../src/index.js';
import { createTestClient, createTestServer } from '../../src/testing/index.js';

// Field read rules on the client: the "staff" fixture of the access test proto
// lets only edit_projects read `salary` and `bio`, only the person a record is
// about read `review`, and no client read `bonus` or `pin`. A test server that
// serves readable rows sends them as the real server does, cleared and named
// in RowChange.hidden.

const entities = [ OrgMemberSchema, ProjectSchema, ProjectMemberSchema, StaffSchema ] as const;

function seeded() {
	const server = createTestServer({ entities, serve: 'readable' });
	server.put(ProjectSchema, { id: 'p1' });
	server.put(StaffSchema, {
		id: 's1',
		project: { id: 'p1' },
		name: 'ann',
		salary: 5000n,
		review: 'good',
		personId: 'guest',
		bonus: 7n,
		pin: '1234',
	});
	return server;
}

/** Access as a reader of p1, with subject `guest`. */
const reader = {
	subjects: [ 'guest' ],
	roles: [ { role: 'project_reader', at: [ ProjectSchema, 'p1' ] as const } ],
};
/** Access as the owner of p1, with subject `owner`. */
const owner = {
	subjects: [ 'owner' ],
	roles: [ { role: 'project_owner', at: [ ProjectSchema, 'p1' ] as const } ],
};

describe('hidden fields', () => {
	it('names on a row each field a guard hides, by accessor name', async () => {
		const server = seeded();
		server.setCallerAccess(reader);
		const t = await createTestClient({ server });
		try {
			await t.sync();
			const row = await t.client.getRow(StaffSchema, 's1');
			expect(row?.hidden).toEqual([ 'bio', 'bonus', 'pin', 'salary' ]);
			expect(row?.value.salary).toBe(0n);
			expect(row?.value.review).toBe('good');
			expect(row?.value.name).toBe('ann');

			const [ scanned ] = await t.db.staff.scan();
			expect(scanned?.hidden).toEqual([ 'bio', 'bonus', 'pin', 'salary' ]);
		} finally {
			await t.close();
		}
	});

	it('leaves hidden unset on a row the caller reads whole', async () => {
		const server = createTestServer({ entities, serve: 'readable' });
		server.put(ProjectSchema, { id: 'p1' });
		server.setCallerAccess(reader);
		const t = await createTestClient({ server });
		try {
			await t.sync();
			const row = await t.client.getRow(ProjectSchema, 'p1');
			expect(row).toBeDefined();
			expect(row).not.toHaveProperty('hidden');
		} finally {
			await t.close();
		}
	});

	it('shows a field when the caller gains access to it, and tells a watcher', async () => {
		const server = seeded();
		server.setCallerAccess(reader);
		const t = await createTestClient({ server });
		const seen: (readonly string[] | undefined)[] = [];
		const handle = t.client.watchRows(StaffSchema, [ 's1' ], (rows) => {
			if (rows.has('s1')) seen.push(rows.get('s1')?.hidden);
		});
		try {
			await t.sync();
			server.setCallerAccess({ ...owner, subjects: [ 'guest' ] });
			await t.sync();

			const row = await t.client.getRow(StaffSchema, 's1');
			expect(row?.hidden).toEqual([ 'bonus', 'pin' ]);
			expect(row?.value.salary).toBe(5000n);
			await expect.poll(() => seen.at(-1)).toEqual([ 'bonus', 'pin' ]);
		} finally {
			handle.close();
			await t.close();
		}
	});

	it('keeps the hidden fields of a row under a local write', async () => {
		const server = seeded();
		server.setCallerAccess(reader);
		const t = await createTestClient({ server });
		try {
			await t.sync();
			t.net.offline();
			await t.db.staff.mutate.patch('s1', { name: 'anna' });

			const row = await t.client.getRow(StaffSchema, 's1');
			expect(row?.value.name).toBe('anna');
			expect(row?.hidden).toEqual([ 'bio', 'bonus', 'pin', 'salary' ]);
		} finally {
			await t.close();
		}
	});

	it('keeps on the server the fields a put did not read', async () => {
		const server = seeded();
		server.setCallerAccess(reader);
		const t = await createTestClient({ server });
		try {
			await t.sync();
			const row = await t.client.getRow(StaffSchema, 's1');
			await t.db.staff.mutate.put({ ...row!.value, name: 'anna' });
			await t.sync();

			const stored = server.get(StaffSchema, 's1');
			expect(stored?.name).toBe('anna');
			expect(stored?.salary).toBe(5000n);
			expect(stored?.pin).toBe('1234');
		} finally {
			await t.close();
		}
	});

	it('lands on the server a value a put sets in a field the caller writes and does not read', async () => {
		const server = seeded();
		server.setCallerAccess(owner);
		const t = await createTestClient({ server });
		try {
			await t.sync();
			const row = await t.client.getRow(StaffSchema, 's1');
			expect(row?.hidden).toContain('review');
			await t.db.staff.mutate.put({ ...row!.value, review: 'great' });
			await t.sync();

			expect(server.get(StaffSchema, 's1')?.review).toBe('great');
		} finally {
			await t.close();
		}
	});

	it('answers can read per field', async () => {
		const server = seeded();
		server.setCallerAccess(reader);
		const t = await createTestClient({ server });
		try {
			await t.sync();
			expect(await t.client.can(StaffSchema, 's1', 'read')).toBe('yes');
			expect(await t.client.can(StaffSchema, 's1', 'read', { fields: [ 'name' ] })).toBe('yes');
			expect(await t.client.can(StaffSchema, 's1', 'read', { fields: [ 'review' ] })).toBe('yes');
			expect(await t.client.can(StaffSchema, 's1', 'read', { fields: [ 'salary' ] })).toBe('no');
			expect(await t.client.can(StaffSchema, 's1', 'read', { fields: [ 'name', 'bonus' ] })).toBe(
				'no',
			);

			server.setCallerAccess(owner);
			await t.sync();
			expect(await t.client.can(StaffSchema, 's1', 'read', { fields: [ 'salary' ] })).toBe('yes');
			expect(await t.client.can(StaffSchema, 's1', 'read', { fields: [ 'review' ] })).toBe('no');
		} finally {
			await t.close();
		}
	});

	it('refuses a text handle on a field the caller does not read', async () => {
		const server = seeded();
		server.setCallerAccess(reader);
		const t = await createTestClient({ server });
		try {
			await t.sync();
			await expect(t.client.text(StaffSchema, 's1', 'bio')).rejects.toThrow(
				'text: field "bio" of row "s1" is hidden from this caller by a field guard',
			);
		} finally {
			await t.close();
		}
	});

	it('refuses to undo the delete of a row with hidden fields, and uses the step up', async () => {
		const server = seeded();
		server.setCallerAccess(reader);
		const t = await createTestClient({ server, undo: {} });
		try {
			await t.sync();
			await t.db.staff.mutate.patch('s1', { name: 'anna' });
			await t.db.staff.mutate.del('s1');

			await expect(t.client.undo()).rejects.toBeInstanceOf(UndoRefusedError);
			expect(await t.client.getRow(StaffSchema, 's1')).toBeUndefined();
			// The step before it still undoes.
			expect(await t.client.undo()).toBe(true);
		} finally {
			await t.close();
		}
	});
	it('keeps the hidden fields on the rows a watcher receives after a local write', async () => {
		// Given a reader of p1 who watches s1 by key and by query
		const server = seeded();
		server.setCallerAccess(reader);
		const t = await createTestClient({ server });
		const byKey: (readonly string[] | undefined)[] = [];
		const byQuery: (readonly string[] | undefined)[] = [];
		const keyed = t.client.watchRows(StaffSchema, [ 's1' ], (rows) => {
			if (rows.has('s1')) byKey.push(rows.get('s1')?.hidden);
		});
		const queried = t.db.staff.watchQuery({}, (rows) => {
			const row = rows.find((r) => r.key === 's1');
			if (row !== undefined) byQuery.push(row.hidden);
		});
		try {
			await t.sync();
			await expect.poll(() => byKey.at(-1)).toEqual([ 'bio', 'bonus', 'pin', 'salary' ]);
			await expect.poll(() => byQuery.at(-1)).toEqual([ 'bio', 'bonus', 'pin', 'salary' ]);
			const keyedBefore = byKey.length;
			const queriedBefore = byQuery.length;

			// When the client renames the record locally
			t.net.offline();
			await t.db.staff.mutate.patch('s1', { name: 'anna' });

			// Then each watcher gets the row again, with the same fields hidden
			await expect.poll(() => byKey.length).toBeGreaterThan(keyedBefore);
			await expect.poll(() => byQuery.length).toBeGreaterThan(queriedBefore);
			expect(byKey.at(-1)).toEqual([ 'bio', 'bonus', 'pin', 'salary' ]);
			expect(byQuery.at(-1)).toEqual([ 'bio', 'bonus', 'pin', 'salary' ]);
		} finally {
			keyed.close();
			queried.close();
			await t.close();
		}
	});
});
