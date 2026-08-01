import { describe, expect, it } from 'vitest';

import type { HydrationInfo, SyncClientOptions, SyncStatus } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { freshWorkspace, newClient } from './helpers.js';

// A cold client applies its first snapshot entity by entity and reads each one
// as it lands, so two things have to cross the wire: the order entities are
// delivered in (which the client asks for), and which of them have finished
// (which the server announces on Begin). Both are only observable end to end.
describe('cold-start hydration', () => {
	async function seed(ws: string): Promise<void> {
		const writer = newClient(ws);
		await writer.start();
		await writer.mutate.put(WidgetSchema, { id: 'w1', count: 1 });
		await writer.mutate.put(WidgetSchema, { id: 'w2', count: 2 });
		await writer.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		await writer.mutate.put(TodoSchema, { id: 't2', title: 'two' });
		await writer.syncNow();
		await writer.close();
	}

	/**
	 * Cold-syncs a workspace with the given priority and returns every distinct
	 * hydration state the status surface published while it did.
	 *
	 * Recorded from the status listener rather than polled: the coordinator
	 * publishes on every change, so the sequence is exact no matter how fast the
	 * snapshot lands.
	 */
	async function hydrationDuringSync(
		ws: string,
		bootstrapPriority: SyncClientOptions['bootstrapPriority'],
	): Promise<(HydrationInfo | undefined)[]> {
		const reader = newClient(ws, { bootstrapPriority });
		const seen: (HydrationInfo | undefined)[] = [];
		reader.onStatusChange((status: SyncStatus) => {
			const info = status.workspaces[ws]?.hydration;
			if (seen.length === 0 || seen[seen.length - 1] !== info) seen.push(info);
		});
		await reader.start();
		await reader.syncNow();
		// Everything arrived, and nothing is reported as still coming.
		expect((await reader.scan(WidgetSchema)).map((r) => r.key).sort()).toEqual([ 'w1', 'w2' ]);
		expect((await reader.scan(TodoSchema)).map((r) => r.key).sort()).toEqual([ 't1', 't2' ]);
		expect(reader.status().workspaces[ws]?.hydration).toBeUndefined();
		expect((await reader.scan(WidgetSchema)).coverage).toEqual({ complete: true, bounds: [] });
		await reader.close();
		return seen;
	}

	it('delivers the priority entity first and reports the rest as still arriving', async () => {
		const ws = freshWorkspace();
		await seed(ws);
		const seen = await hydrationDuringSync(ws, [ WidgetSchema ]);

		// The server's announced order, priority first.
		const announced = seen.find((info) => info?.pending[0] === 'widget');
		expect(announced).toBeDefined();
		expect(announced!.done).toBe(0);
		// Widget's rows are in and readable while todo's are still coming — the
		// state the whole feature exists to make expressible.
		const midway = seen.find((info) => info !== undefined && !info.pending.includes('widget'));
		expect(midway?.pending).toContain('todo');
		expect(midway!.done).toBeGreaterThan(0);
	});

	it('honours the opposite priority just as literally', async () => {
		const ws = freshWorkspace();
		await seed(ws);
		const seen = await hydrationDuringSync(ws, [ TodoSchema ]);

		expect(seen.find((info) => info?.pending[0] === 'todo')).toBeDefined();
		const midway = seen.find((info) => info !== undefined && !info.pending.includes('todo'));
		expect(midway?.pending).toContain('widget');
	});
});
