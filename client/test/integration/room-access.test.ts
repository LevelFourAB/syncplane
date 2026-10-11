// A room in a workspace under the access model, against the real server: a
// caller that holds no grant yet is refused the room as NO_ACCESS, which a
// later grant lifts, so the room waits rather than ending as dropped.

import { createConnectTransport } from '@connectrpc/connect-node';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { CodeRoomDropped } from '../../src/codes.js';
import {
	OrgMemberSchema,
	ProjectMemberSchema,
	ProjectSchema,
} from '../../src/gen/syncplane/testdata/access/v1/access_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { Room, RoomError, RoomStatus } from '../../src/room.js';
import { startTestServer, type TestServer } from '../support/testserver.js';
import { freshWorkspace } from './helpers.js';

let server: TestServer;
beforeAll(async () => {
	server = await startTestServer([ '-access' ]);
});
afterAll(() => server?.stop());

function clientAs(user: string, ws: string): SyncClient {
	return new SyncClient({
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
		entities: [ OrgMemberSchema, ProjectSchema, ProjectMemberSchema ],
		pollIntervalMs: 0,
		streamEnabled: false,
	});
}

async function trusted(workspace: string, writes: object[]): Promise<void> {
	const res = await fetch(`${server.url}/debug/trusted`, {
		method: 'POST',
		body: JSON.stringify({ workspace, writes }),
	});
	if (!res.ok) throw new Error(`trusted write failed: ${await res.text()}`);
}

/** Waits until the room's state is `state`, or fails. */
async function untilState(room: Room<unknown>, state: RoomStatus['state']): Promise<void> {
	const deadline = Date.now() + 10_000;
	while (room.status().state !== state) {
		if (Date.now() > deadline) {
			throw new Error(`timed out waiting for ${state}; the room is ${room.status().state}`);
		}
		await new Promise((r) => setTimeout(r, 25));
	}
}

describe('a room of a caller with no grant yet', () => {
	it('waits for the grant, and opens when a grant admits the caller', async () => {
		// Given a workspace where ann holds a grant and uma holds none
		const ws = freshWorkspace();
		await trusted(ws, [
			{ entity: 'org_member', value: { id: 'om-ann', userId: 'ann', role: 'org_member' } },
		]);
		const uma = clientAs('uma', ws);
		await uma.start();
		const errors: RoomError[] = [];

		// When uma opens a room and sets her presence
		const room = uma.ws(ws).room<string>('cursors');
		room.onError((err) => errors.push(err));
		room.setPresence('here');

		// Then the room waits, with no roster and not as dropped. The state she
		// set before the refusal arrived could not be published.
		await untilState(room, 'waiting');
		expect(room.status().error).toBeUndefined();
		expect(room.peers()).toEqual([]);

		// And when a grant names uma, and her client learns of it
		await trusted(ws, [
			{ entity: 'org_member', value: { id: 'om-uma', userId: 'uma', role: 'org_member' } },
		]);
		await uma.syncNow();

		// Then the room opens, and her presence reaches it
		await untilState(room, 'open');
		const deadline = Date.now() + 10_000;
		while (!room.peers().some((p) => p.self && p.state === 'here')) {
			if (Date.now() > deadline) throw new Error(`no own record: ${JSON.stringify(room.peers())}`);
			await new Promise((r) => setTimeout(r, 25));
		}
		expect(errors.map((e) => e.code)).not.toContain(CodeRoomDropped);

		await room.close();
		await uma.close();
	});
});
