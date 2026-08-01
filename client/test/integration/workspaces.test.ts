import { describe, expect, it } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { eventually, freshWorkspace, newClient, newMultiClient } from './helpers.js';

// One client holding several workspaces, against the real engine: the push
// carries a batch per workspace in one request, the Subscribe stream carries
// every scope on one connection, and each workspace keeps its own sequence,
// cursor and mutation-id watermark.

describe('multi-workspace client', () => {
	it('pushes and pulls several workspaces in one sync cycle', async () => {
		const a = freshWorkspace();
		const b = freshWorkspace();
		const client = newMultiClient([ a, b ]);
		await client.start();

		await client.ws(a).mutate.put(TodoSchema, { id: 't1', title: 'in A' });
		await client.ws(b).mutate.put(TodoSchema, { id: 't1', title: 'in B' });
		await client.syncNow();

		expect(client.status().pendingMutations).toBe(0);

		// Each workspace's rows landed in its own partition server-side: a second,
		// single-workspace client sees only that workspace's copy.
		const peerA = newClient(a);
		const peerB = newClient(b);
		await peerA.start();
		await peerB.start();
		await peerA.syncNow();
		await peerB.syncNow();
		expect((await peerA.get(TodoSchema, 't1'))?.title).toBe('in A');
		expect((await peerB.get(TodoSchema, 't1'))?.title).toBe('in B');

		// And the reverse: a cross-workspace read on the multi client attributes
		// each row to where it came from.
		const all = await client.scanAll(TodoSchema);
		expect(all.map((r) => r.value.title).sort()).toEqual([ 'in A', 'in B' ]);
		expect(new Set(all.map((r) => r.workspaceId))).toEqual(new Set([ a, b ]));

		await peerA.close();
		await peerB.close();
		await client.close();
	});

	it('keeps mutation-id watermarks separate, so neither workspace gaps', async () => {
		const a = freshWorkspace();
		const b = freshWorkspace();
		const client = newMultiClient([ a, b ]);
		await client.start();

		// Interleaved writes: an account-wide counter would hand B ids 2 and 4,
		// which the server would reject as a gap against its own watermark.
		await client.ws(a).mutate.put(TodoSchema, { id: 'a1', title: 'a1' });
		await client.ws(b).mutate.put(TodoSchema, { id: 'b1', title: 'b1' });
		await client.ws(a).mutate.put(TodoSchema, { id: 'a2', title: 'a2' });
		await client.ws(b).mutate.put(TodoSchema, { id: 'b2', title: 'b2' });
		await client.syncNow();

		expect(client.status().pendingMutations).toBe(0);
		expect(client.status().lastError).toBeUndefined();
		expect((await client.ws(a).scan(TodoSchema)).map((r) => r.key)).toEqual([ 'a1', 'a2' ]);
		expect((await client.ws(b).scan(TodoSchema)).map((r) => r.key)).toEqual([ 'b1', 'b2' ]);

		await client.close();
	});

	it('receives changes for every held workspace over one live stream', async () => {
		const a = freshWorkspace();
		const b = freshWorkspace();
		const reader = newMultiClient([ a, b ], { streamEnabled: true });
		await reader.start();
		// Let the stream open and catch both scopes up before anything is written.
		await reader.syncNow();

		const writerA = newClient(a);
		const writerB = newClient(b);
		await writerA.start();
		await writerB.start();
		await writerA.mutate.put(TodoSchema, { id: 't1', title: 'pushed to A' });
		await writerB.mutate.put(TodoSchema, { id: 't1', title: 'pushed to B' });
		await writerA.syncNow();
		await writerB.syncNow();

		// No syncNow on the reader: both must arrive on the multiplexed stream.
		await eventually(async () =>
			(await reader.ws(a).get(TodoSchema, 't1'))?.title === 'pushed to A'
			&& (await reader.ws(b).get(TodoSchema, 't1'))?.title === 'pushed to B'
		);

		await writerA.close();
		await writerB.close();
		await reader.close();
	});

	it('bootstraps a workspace added to a running client without disturbing the others', async () => {
		const a = freshWorkspace();
		const b = freshWorkspace();

		// Seed b from another client, so adding it has something to bootstrap.
		const seeder = newClient(b);
		await seeder.start();
		await seeder.mutate.put(TodoSchema, { id: 'seeded', title: 'already there' });
		await seeder.syncNow();
		await seeder.close();

		const client = newMultiClient([ a ]);
		await client.start();
		await client.ws(a).mutate.put(TodoSchema, { id: 't1', title: 'in A' });
		await client.syncNow();

		client.addWorkspace(b);
		await client.syncNow();

		expect((await client.ws(b).scan(TodoSchema)).map((r) => r.value.title))
			.toEqual([ 'already there' ]);
		// A is untouched by B's bootstrap.
		expect((await client.ws(a).scan(TodoSchema)).map((r) => r.value.title)).toEqual([ 'in A' ]);

		// Removing it with purge drops the local copy and leaves A alone.
		await client.removeWorkspace(b, { purge: true });
		expect((await client.scanAll(TodoSchema)).map((r) => r.value.title)).toEqual([ 'in A' ]);

		await client.close();
	});
});
