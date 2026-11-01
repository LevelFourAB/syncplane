// Nested undo scopes: a fork of a workspace that records its own inverses, for
// UI (a dialog, an inline editor) whose undo must not reach into the history of
// the view around it. Everything but the undo stack is shared with the
// workspace, so most of these specs are about what does NOT cross over.

import { createRouterTransport } from '@connectrpc/connect';
import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { CompleteAllArgsSchema, TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { INTENT_PREFIX, metaUndoTokens } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { IntentRecord } from '../../src/sync/intent.js';

function newLocalClient(undo: { enabled?: boolean; maxDepth?: number } | undefined = {}) {
	return new SyncClient({
		workspaceId: 'ws-undo',
		kvStore: new MemoryKVStore(),
		entities: [ TodoSchema ],
		undo,
	});
}

describe('undo scopes', () => {
	it("records a scope's writes on its own stack, not the workspace's", async () => {
		const c = newLocalClient();
		await c.mutate.put(TodoSchema, { id: 't1', title: 'from the board' });

		const dialog = c.undoScope();
		await dialog.mutate.patch(TodoSchema, 't1', { title: 'from the dialog' });

		// The dialog's edit is the dialog's to undo; the board still sees only
		// its own put.
		expect(dialog.canUndo()).toBe(true);
		expect(await dialog.undo()).toBe(true);
		expect((await c.get(TodoSchema, 't1'))?.title).toBe('from the board');
		expect(dialog.canUndo()).toBe(false);

		// And the board's stack was never touched by any of it.
		expect(c.canUndo()).toBe(true);
		expect(await c.undo()).toBe(true);
		expect(await c.get(TodoSchema, 't1')).toBeUndefined();
		await c.close();
	});

	it('leaves the workspace redo stack alone when a scope edits', async () => {
		const c = newLocalClient();
		await c.mutate.put(TodoSchema, { id: 't1', title: 'v1' });
		await c.undo();
		expect(c.canRedo()).toBe(true);

		// A new user edit clears the redo stack — but only the stack it lands on.
		const dialog = c.undoScope();
		await dialog.mutate.put(TodoSchema, { id: 't2', title: 'other' });
		expect(c.canRedo()).toBe(true);
		expect(dialog.canRedo()).toBe(false);

		// The reverse holds too: the workspace's redo survives its own replay
		// while the dialog's stack sits untouched beside it.
		expect(await c.redo()).toBe(true);
		expect(dialog.canUndo()).toBe(true);
		await c.close();
	});

	it('shares reads, the queue and the overlay with the workspace', async () => {
		const c = newLocalClient();
		const dialog = c.undoScope();
		await dialog.mutate.put(TodoSchema, { id: 't1', title: 'written in the dialog' });

		// One store, one queue: the write is visible everywhere immediately and
		// queued once.
		expect((await c.get(TodoSchema, 't1'))?.title).toBe('written in the dialog');
		expect((await dialog.get(TodoSchema, 't1'))?.title).toBe('written in the dialog');
		expect(await c.pendingCount()).toBe(1);
		expect(dialog.workspaceId).toBe(c.workspaceId);
		await c.close();
	});

	it('gives each scope its own stack, and forks the workspace rather than nesting', async () => {
		const c = newLocalClient();
		const first = c.undoScope();
		const second = c.undoScope();
		await first.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		await second.mutate.put(TodoSchema, { id: 't2', title: 'two' });

		expect(await second.undo()).toBe(true);
		expect(await c.get(TodoSchema, 't2')).toBeUndefined();
		// first's own step is untouched by its sibling's undo.
		expect(first.canUndo()).toBe(true);
		expect(c.canUndo()).toBe(false);

		// A scope forked off a scope still forks the workspace: it starts empty
		// and its parent keeps its entries.
		const third = first.undoScope();
		expect(third.canUndo()).toBe(false);
		expect(first.canUndo()).toBe(true);
		await c.close();
	});

	it("honours a per-scope maxDepth, falling back to the client's", async () => {
		const c = newLocalClient({ maxDepth: 1 });
		const deep = c.undoScope({ maxDepth: 3 });
		for (const id of [ 'a', 'b', 'c' ]) {
			await deep.mutate.put(TodoSchema, { id, title: id });
		}
		expect(await deep.undo()).toBe(true);
		expect(await deep.undo()).toBe(true);
		expect(await deep.undo()).toBe(true);

		// Without an override the scope inherits the client's depth.
		const shallow = c.undoScope();
		await shallow.mutate.put(TodoSchema, { id: 'd', title: 'd' });
		await shallow.mutate.put(TodoSchema, { id: 'e', title: 'e' });
		expect(await shallow.undo()).toBe(true);
		expect(await shallow.undo()).toBe(false);
		await c.close();
	});

	it("drops the scope's stack on close, leaving the workspace's intact", async () => {
		const c = newLocalClient();
		await c.mutate.put(TodoSchema, { id: 't1', title: 'board' });
		const dialog = c.undoScope();
		await dialog.mutate.put(TodoSchema, { id: 't2', title: 'dialog' });

		let changes = 0;
		dialog.onUndoChange(() => changes++);
		dialog.close();
		// Subscribers hear the stacks empty out, so a toolbar rendered during
		// teardown disables itself rather than holding a stale "can undo".
		expect(changes).toBe(1);
		expect(dialog.closed).toBe(true);
		expect(dialog.canUndo()).toBe(false);
		// The step is dropped, NOT merged into the workspace: t2 stays written and
		// the workspace's own stack still holds exactly its own step.
		expect((await c.get(TodoSchema, 't2'))?.title).toBe('dialog');
		expect(await dialog.undo()).toBe(false);
		expect(await c.undo()).toBe(true);
		expect(await c.get(TodoSchema, 't1')).toBeUndefined();
		expect(await c.undo()).toBe(false);

		dialog.close(); // idempotent
		await c.close();
	});

	it('stays usable after close, recording nothing', async () => {
		const c = newLocalClient();
		const dialog = c.undoScope();
		dialog.close();

		// A late callback from a torn-down dialog is not an error: the write
		// lands, it simply has no undo step.
		await dialog.mutate.put(TodoSchema, { id: 't1', title: 'late' });
		expect((await c.get(TodoSchema, 't1'))?.title).toBe('late');
		expect(dialog.canUndo()).toBe(false);
		expect(c.canUndo()).toBe(false);
		await c.close();
	});

	it('clears every stack in the workspace on a custom mutation (barrier)', async () => {
		const c = newLocalClient();
		await c.mutate.put(TodoSchema, { id: 't1', title: 'board' });
		const dialog = c.undoScope();
		await dialog.mutate.put(TodoSchema, { id: 't2', title: 'dialog' });

		// A custom mutation is opaque and may have touched anything, so no stack
		// in the workspace can still claim to invert what came before it —
		// including one issued through a scope.
		await dialog.mutate.custom('complete_all', CompleteAllArgsSchema, {});
		expect(c.canUndo()).toBe(false);
		expect(dialog.canUndo()).toBe(false);
		await c.close();
	});

	it('is inert when the client has undo disabled', async () => {
		const c = new SyncClient({
			workspaceId: 'ws-undo',
			kvStore: new MemoryKVStore(),
			entities: [ TodoSchema ],
		});
		// A UI can open a scope unconditionally: it behaves exactly like the
		// workspace it forked, undo-less included.
		const dialog = c.undoScope();
		await dialog.mutate.put(TodoSchema, { id: 't1', title: 'x' });
		expect(dialog.canUndo()).toBe(false);
		await expect(dialog.undo()).rejects.toThrow('undo is not enabled');
		await c.close();
	});

	it('refuses writes through a scope whose workspace was removed', async () => {
		const c = new SyncClient({
			workspaces: [ 'ws-a', 'ws-b' ],
			accountId: 'acct',
			kvStore: new MemoryKVStore(),
			entities: [ TodoSchema ],
			undo: {},
		});
		const dialog = c.ws('ws-a').undoScope();
		await c.removeWorkspace('ws-a');
		await expect(dialog.mutate.put(TodoSchema, { id: 't1', title: 'x' }))
			.rejects.toThrow('is not held by this client');
		await c.close();
	});

	it("pins every scope's referenced mutations in the cross-tab intent record", async () => {
		// Compaction may not fold away a mutation any live stack still inverts,
		// and the record a peer tab reads has to cover the whole workspace — an
		// open dialog's stack included.
		const kv = new MemoryKVStore();
		const c = new SyncClient({
			workspaceId: 'ws-undo',
			kvStore: kv,
			entities: [ TodoSchema ],
			transport: createRouterTransport(() => {}),
			streamEnabled: false,
			pollIntervalMs: 0,
			undo: {},
		});
		await c.start();

		await c.mutate.put(TodoSchema, { id: 't1', title: 'board' }); // id 1
		const dialog = c.undoScope();
		await dialog.mutate.put(TodoSchema, { id: 't2', title: 'dialog' }); // id 2
		expect(await refIds(kv)).toEqual(await tokensOf(kv, [ 1n, 2n ]));

		// Closing the dialog releases its pins; the workspace's own stay.
		dialog.close();
		await c.mutate.put(TodoSchema, { id: 't3', title: 'board again' }); // id 3
		expect(await refIds(kv)).toEqual(await tokensOf(kv, [ 1n, 3n ]));
		await c.close();
	});
});

/** The undo tokens the pending mutations at `ids` carry, sorted. */
async function tokensOf(kv: MemoryKVStore, ids: bigint[]): Promise<string[]> {
	const tx = await kv.read();
	try {
		const tokens: string[] = [];
		for (const id of ids) tokens.push(...((await tx.get(metaUndoTokens('ws-undo', id))) as string[]));
		return tokens.sort();
	} finally {
		tx.release();
	}
}

/** The undo tokens this tab published for its (single) workspace. */
async function refIds(kv: MemoryKVStore): Promise<string[]> {
	const tx = await kv.read();
	try {
		const ids: string[] = [];
		for await (const [ , value ] of tx.scan(INTENT_PREFIX)) {
			for (const list of Object.values((value as IntentRecord).undoRefTokens)) ids.push(...list);
		}
		return ids.sort();
	} finally {
		tx.release();
	}
}
