import { describe, expect, it } from 'vitest';

import {
	type BootstrapProgressInfo,
	subscription,
	type SyncClient,
	type SyncStatus,
} from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { freshWorkspace, newClient } from './helpers.js';

// The row estimates on Begin cross the wire from the Go server's store count to
// the TS client's status surface, which only an end-to-end run observes.
//
// testserver filters todo rows (a title with "secret" is hidden), so a snapshot
// that ships todo gets no estimate: a count would tell the caller how many rows
// it may not read.
describe('bootstrap progress', () => {
	async function seed(ws: string): Promise<void> {
		const writer = newClient(ws);
		await writer.start();
		for (let i = 0; i < 5; i++) await writer.mutate.put(WidgetSchema, { id: `w${i}`, count: i });
		await writer.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		await writer.syncNow();
		await writer.close();
	}

	/** Syncs `reader` once and returns every bootstrap progress the status published. */
	async function progressDuringSync(
		reader: SyncClient,
		ws: string,
	): Promise<BootstrapProgressInfo[]> {
		const seen: BootstrapProgressInfo[] = [];
		reader.onStatusChange((status: SyncStatus) => {
			const p = status.workspaces[ws]?.bootstrap;
			if (p !== undefined) seen.push(p);
		});
		await reader.start();
		await reader.syncNow();
		expect(reader.status().workspaces[ws]?.bootstrap).toBeUndefined();
		return seen;
	}

	it('reports the rows applied against the server row estimate', async () => {
		const ws = freshWorkspace();
		await seed(ws);
		const reader = newClient(ws, { subscriptions: [ subscription(WidgetSchema) ] });

		const seen = await progressDuringSync(reader, ws);

		expect(seen.length).toBeGreaterThan(0);
		// A whole-entity subscription with no read filter ships every live row,
		// so the estimate is exact here.
		expect(seen.at(-1)).toEqual({
			rowsApplied: 5,
			rowsEstimated: 5,
			entitiesDone: 1,
			entitiesTotal: 1,
		});
		// The pull after the snapshot reports the server's head, and the client
		// holds everything up to it.
		expect(reader.status().workspaces[ws]?.behind).toBe(0);
		await reader.close();
	});

	it('gives no estimate when a read filter hides rows of a shipped entity', async () => {
		const ws = freshWorkspace();
		await seed(ws);
		const reader = newClient(ws);

		const seen = await progressDuringSync(reader, ws);

		expect(seen.length).toBeGreaterThan(0);
		expect(seen.every((p) => p.rowsEstimated === 0)).toBe(true);
		expect(seen.at(-1)?.rowsApplied).toBe(6);
		await reader.close();
	});
});
