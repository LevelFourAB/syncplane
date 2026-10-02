import { describe, expect, it } from 'vitest';

import {
	OrgMemberSchema,
	ProjectMemberSchema,
	ProjectSchema,
	StaffSchema,
} from '../../src/gen/syncplane/testdata/access/v1/access_pb.js';
import { createTestClient, createTestServer } from '../../src/testing/index.js';

// A field the caller writes and does not read, such as the `pin` or `review`
// of the "staff" fixture, reaches the client cleared. A PUT that leaves it
// unset keeps the stored value on the server, so a PUT carries a write of such
// a field only when it sets it. The pending queue folds writes of one row into
// one write, and that fold must not lose a write of the field that only one of
// the folded writes expressed.

const entities = [ OrgMemberSchema, ProjectSchema, ProjectMemberSchema, StaffSchema ] as const;

/** Staff s1 in p1 with pin 1234 and review "good", and a client of the
 * project's owner, who writes both fields and reads neither. */
async function ownerOfStaff() {
	const server = createTestServer({ entities, serve: 'readable' });
	server.put(ProjectSchema, { id: 'p1' });
	server.put(StaffSchema, {
		id: 's1',
		project: { id: 'p1' },
		name: 'ann',
		personId: 'guest',
		pin: '1234',
		review: 'good',
	});
	server.setCallerAccess({
		subjects: [ 'owner' ],
		roles: [ { role: 'project_owner', at: [ ProjectSchema, 'p1' ] } ],
	});
	const t = await createTestClient({ server });
	await t.sync();
	return { server, t };
}

describe('a queued write of a field the caller writes and does not read', () => {
	it('reaches the server when a later put of the row from an earlier read leaves the field unset', async () => {
		// Given
		const { server, t } = await ownerOfStaff();
		try {
			const read = await t.client.getRow(StaffSchema, 's1');
			expect(read?.hidden).toContain('review');

			// When, offline, one form writes the review and another saves the
			// name from the row it read before
			await t.net.offline();
			await t.db.staff.mutate.put({ ...read!.value, review: 'great' });
			await t.db.staff.mutate.put({ ...read!.value, name: 'anna' });
			await t.net.online();
			await t.sync();

			// Then the review is on the server. Applied one by one, the first put
			// writes it and the second keeps it; folded into the second put
			// alone, the review keeps "good".
			expect(server.get(StaffSchema, 's1')?.review).toBe('great');
			expect(server.get(StaffSchema, 's1')?.name).toBe('anna');
		} finally {
			await t.close();
		}
	});

	it('is not replaced by the stored value when a patch that clears the field follows a put of the row under one expected version', async () => {
		// Given
		const { server, t } = await ownerOfStaff();
		try {
			const read = await t.client.getRow(StaffSchema, 's1');
			const version = read!.version;

			// When, offline, the owner writes the review and then clears it,
			// both guarded by the version the form read
			await t.net.offline();
			await t.db.staff.mutate.put({ ...read!.value, review: 'great' }, { expectedVersion: version });
			await t.db.staff.mutate.patch('s1', { review: '' }, {
				expectedVersion: version,
				paths: [ 'review' ],
			});
			await t.net.online();
			await t.sync();

			// Then the server holds what the two writes give one by one: the put
			// writes "great" and the patch is refused on its stale version.
			// Folded into one put, the clear leaves the review unset, and the
			// server keeps "good".
			expect(server.get(StaffSchema, 's1')?.review).toBe('great');
		} finally {
			await t.close();
		}
	});
});

describe('a queued clear of a field the caller writes and does not read', () => {
	it('reaches the server when a put of the row under the same expected version follows it', async () => {
		// Given staff s1 with pin 1234, and its project's owner, who writes the
		// pin and does not read it
		const server = createTestServer({ entities, serve: 'readable' });
		server.put(ProjectSchema, { id: 'p1' });
		server.put(StaffSchema, { id: 's1', project: { id: 'p1' }, name: 'ann', pin: '1234' });
		server.setCallerAccess({
			subjects: [ 'owner' ],
			roles: [ { role: 'project_owner', at: [ ProjectSchema, 'p1' ] } ],
		});
		const t = await createTestClient({ server });
		try {
			await t.sync();
			const row = await t.client.getRow(StaffSchema, 's1');
			expect(row?.hidden).toContain('pin');
			const version = row!.version;

			// When, offline, the owner clears the pin and then saves the form,
			// both guarded by the version the form read
			await t.net.offline();
			await t.db.staff.mutate.patch('s1', { pin: '' }, { expectedVersion: version, paths: [ 'pin' ] });
			const edited = await t.client.getRow(StaffSchema, 's1');
			await t.db.staff.mutate.put({ ...edited!.value, name: 'anna' }, { expectedVersion: version });
			await t.net.online();
			await t.sync();

			// Then the clear the owner asked for is on the server. Applied one by
			// one, the patch clears the pin and the put is refused on its stale
			// version; folded into the put alone, the pin keeps 1234.
			expect(server.get(StaffSchema, 's1')?.pin).toBe('');
		} finally {
			await t.close();
		}
	});
});
