import { describe, expect, it } from 'vitest';

import { subscription } from '../../src/client.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { eventually, freshWorkspace, newClient } from './helpers.js';

const WINDOW = 40;

// Padded so the window's key-set on the cursor outweighs the page carrying a
// single changed row, which is what makes the server withhold cursors at all.
const widgetKey = (i: number) => `widget-${String(i).padStart(6, '0')}-${'x'.repeat(24)}`;

const topByCount = subscription(WidgetSchema, undefined, {
	limit: WINDOW,
	orderBy: (w) => [ w.count.desc() ],
});

// A windowed subscription's cursor carries the client's whole window
// membership, so the server sends it as a periodic checkpoint rather than on
// every stream frame. The client then holds a cursor OLDER than the rows it
// has applied — which only matters when it reconnects from it.
describe('sparse subscribe cursors', () => {
	it('converges after a reconnect from a cursor behind the rows already applied', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws);
		await writer.start();
		for (let i = 0; i < WINDOW; i++) {
			await writer.mutate.put(WidgetSchema, { id: widgetKey(i), count: i });
		}
		await writer.syncNow();

		const store = new MemoryKVStore();
		const reader = newClient(ws, {
			kvStore: store,
			subscriptions: [ topByCount ],
			streamEnabled: true,
			pollIntervalMs: 0,
		});
		await reader.start();
		await eventually(async () => (await reader.scan(WidgetSchema)).length === WINDOW);

		// Each write enters the window and evicts its tail, so every frame is a
		// page the server can withhold the cursor on.
		const added: string[] = [];
		for (let i = 0; i < 12; i++) {
			const id = widgetKey(WINDOW + i);
			added.push(id);
			await writer.mutate.put(WidgetSchema, { id, count: WINDOW + i });
			await writer.syncNow();
			await eventually(async () => (await reader.get(WidgetSchema, id)) !== undefined);
		}
		await reader.close();

		// Same store, new stream: whatever the reader's stored cursor lags by,
		// the server re-delivers and the window ends up exactly as the writer
		// left it — the newest WINDOW rows and nothing that fell off the tail.
		const resumed = newClient(ws, {
			kvStore: store,
			subscriptions: [ topByCount ],
			streamEnabled: true,
			pollIntervalMs: 0,
		});
		await resumed.start();
		await resumed.syncNow();

		const rows = await resumed.scan(WidgetSchema);
		expect(rows).toHaveLength(WINDOW);
		const keys = new Set(rows.map((r) => r.key));
		for (const id of added) expect(keys).toContain(id);
		expect(keys).not.toContain(widgetKey(0));

		await writer.close();
		await resumed.close();
	});
});
