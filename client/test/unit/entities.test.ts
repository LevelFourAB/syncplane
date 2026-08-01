import { create, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { reconcileStoreEntities, syncableEntities } from '../../src/entities.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { type RowChange, RowChangeSchema } from '../../src/gen/syncplane/v1/patch_pb.js';
import {
	confirmedKey,
	META_ENTITIES,
	metaBootstrapResume,
	metaCursor,
	metaLastMutationId,
	stagingKey,
} from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { EntityRegistry } from '../../src/mutations.js';
import {
	applyServerChanges,
	stageServerChanges,
	wipeConfirmedState,
} from '../../src/optimistic/rebase.js';
import { TouchSet } from '../../src/touch.js';

const todoOnly = new EntityRegistry([ TodoSchema ]);
const todoAndWidget = new EntityRegistry([ TodoSchema, WidgetSchema ]);
const WS = 'ws1';

/** A change for any entity name, including ones no registry here knows. */
function rowOf(entity: string, key: string, version: bigint): RowChange {
	return create(RowChangeSchema, {
		entity,
		key,
		version,
		body: {
			case: 'value',
			value: toBinary(TodoSchema, create(TodoSchema, { id: key, title: entity })),
		},
	});
}

async function reconcile(kv: MemoryKVStore, registry: EntityRegistry): Promise<string[]> {
	const tx = await kv.write();
	const added = await reconcileStoreEntities(tx, registry);
	await tx.commit();
	return added;
}

async function get(kv: MemoryKVStore, key: string): Promise<unknown> {
	const tx = await kv.read();
	const value = await tx.get(key);
	tx.release();
	return value;
}

async function apply(kv: MemoryKVStore, registry: EntityRegistry, ...changes: RowChange[]) {
	const tx = await kv.write();
	await applyServerChanges(tx, registry, WS, changes, new TouchSet());
	await tx.commit();
}

describe('reconcileStoreEntities', () => {
	it('stamps a fresh store with the registry and reports every name as new', async () => {
		const kv = new MemoryKVStore();
		expect(await reconcile(kv, todoAndWidget)).toEqual([ 'todo', 'widget' ]);
		expect(await get(kv, META_ENTITIES)).toEqual([ 'todo', 'widget' ]);
	});

	it('is a no-op once the store already records the registry', async () => {
		const kv = new MemoryKVStore();
		await reconcile(kv, todoOnly);
		const tx = await kv.write();
		await tx.put(metaCursor(WS), new Uint8Array([ 7 ]));
		await tx.commit();

		expect(await reconcile(kv, todoOnly)).toEqual([]);
		expect(await get(kv, metaCursor(WS))).toEqual(new Uint8Array([ 7 ]));
	});

	// The rows of a newly registered entity were never stored and all sit below
	// the cursor, so only a snapshot can supply them.
	it('drops every workspace cursor when the registry gains an entity', async () => {
		const kv = new MemoryKVStore();
		await reconcile(kv, todoOnly);
		const tx = await kv.write();
		await tx.put(metaCursor(WS), new Uint8Array([ 1 ]));
		await tx.put(metaCursor('ws/two'), new Uint8Array([ 2 ]));
		await tx.put(metaLastMutationId(WS), '9');
		// An interrupted bootstrap of the third workspace, waiting to be resumed.
		await tx.put(metaBootstrapResume('ws3'), {
			gen: 1,
			snapshotGen: 1,
			direct: true,
			cursor: new Uint8Array([ 3 ]),
			token: new Uint8Array([ 1 ]),
		});
		await tx.commit();

		expect(await reconcile(kv, todoAndWidget)).toEqual([ 'widget' ]);
		expect(await get(kv, metaCursor(WS))).toBeUndefined();
		// Resuming that snapshot would have the server skip the entities the
		// interrupted attempt "delivered" — including the ones whose rows it
		// dropped for want of a schema, which no later pull can supply.
		expect(await get(kv, metaBootstrapResume('ws3'))).toBeUndefined();
		// Workspace ids are percent-encoded as key segments; a slashed one has to
		// survive the round trip out of the key and back into it.
		expect(await get(kv, metaCursor('ws/two'))).toBeUndefined();
		// Nothing else about the workspace is disturbed — the mutation counter in
		// particular, whose server-side watermark does not reset with the rows.
		expect(await get(kv, metaLastMutationId(WS))).toBe('9');
		expect(await get(kv, META_ENTITIES)).toEqual([ 'todo', 'widget' ]);
	});

	// Tabs of different builds share one store: the set is the union of every
	// registry that has opened it, so the tab with fewer entities does not narrow
	// what the others sync.
	it('keeps names this build no longer registers', async () => {
		const kv = new MemoryKVStore();
		await reconcile(kv, todoAndWidget);

		expect(await reconcile(kv, todoOnly)).toEqual([]);
		expect(await get(kv, META_ENTITIES)).toEqual([ 'todo', 'widget' ]);
	});
});

describe('syncableEntities', () => {
	it('unions the store record with the registry', async () => {
		const kv = new MemoryKVStore();
		await reconcile(kv, todoAndWidget);
		const tx = await kv.read();
		const names = await syncableEntities(tx, new EntityRegistry([ TodoSchema ]));
		tx.release();
		expect([ ...names ].sort()).toEqual([ 'todo', 'widget' ]);
	});
});

describe('server changes for entities the store does not sync', () => {
	it('are not applied to confirmed state', async () => {
		const kv = new MemoryKVStore();
		await reconcile(kv, todoOnly);
		await apply(kv, todoOnly, rowOf('todo', 't1', 1n), rowOf('mystery', 'm1', 2n));

		expect(await get(kv, confirmedKey('todo', WS, 't1'))).toBeDefined();
		expect(await get(kv, confirmedKey('mystery', WS, 'm1'))).toBeUndefined();
	});

	it('are not staged by a bootstrap', async () => {
		const kv = new MemoryKVStore();
		await reconcile(kv, todoOnly);
		const tx = await kv.write();
		await stageServerChanges(tx, todoOnly, WS, 1, [
			rowOf('todo', 't1', 1n),
			rowOf('mystery', 'm1', 2n),
		]);
		await tx.commit();

		expect(await get(kv, stagingKey(WS, 1, 'todo', 't1'))).toBeDefined();
		expect(await get(kv, stagingKey(WS, 1, 'mystery', 'm1'))).toBeUndefined();
	});

	// The half that keeps a mixed-build store consistent: this tab cannot read a
	// widget, but the tab that registered it can, and stays up to date only if
	// this one keeps storing them.
	it('still applies entities the store records but this build lacks', async () => {
		const kv = new MemoryKVStore();
		await reconcile(kv, todoAndWidget);
		await apply(kv, todoOnly, rowOf('widget', 'w1', 1n));

		expect(await get(kv, confirmedKey('widget', WS, 'w1'))).toBeDefined();
	});
});

describe('wipeConfirmedState', () => {
	// The rows a re-bootstrap exists to replace include the ones this build
	// cannot read: leaving them behind would hand arbitrarily stale rows to
	// whichever build registers that entity next.
	it('sweeps entities the store records but this build lacks', async () => {
		const kv = new MemoryKVStore();
		await reconcile(kv, todoAndWidget);
		await apply(kv, todoAndWidget, rowOf('todo', 't1', 1n), rowOf('widget', 'w1', 2n));

		const tx = await kv.write();
		await wipeConfirmedState(tx, todoOnly, WS, new TouchSet());
		await tx.commit();

		expect(await get(kv, confirmedKey('todo', WS, 't1'))).toBeUndefined();
		expect(await get(kv, confirmedKey('widget', WS, 'w1'))).toBeUndefined();
	});
});
