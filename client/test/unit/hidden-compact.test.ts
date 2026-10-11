import { describe, expect, it } from 'vitest';

import {
	OrgMemberSchema,
	ProjectMemberSchema,
	ProjectSchema,
	StaffSchema,
} from '../../src/gen/syncplane/testdata/access/v1/access_pb.js';
import { createTestClient, createTestServer } from '../../src/testing/index.js';

// A field the caller writes and does not read, such as the `pin` of the
// "staff" fixture, reaches the client cleared. A PUT that leaves it unset
// keeps the stored value on the server, so only a PATCH can clear it. The
// pending queue folds a PATCH and a later PUT of one row into one PUT when
// both name the same expected version, and that fold must not lose a clear
// that only the PATCH could express.

const entities = [ OrgMemberSchema, ProjectSchema, ProjectMemberSchema, StaffSchema ] as const;

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
