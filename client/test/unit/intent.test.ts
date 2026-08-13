import { create, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it, vi } from 'vitest';

import { type Subscription, SubscriptionSchema } from '../../src/gen/syncplane/v1/patch_pb.js';
import { intentKey } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { ChainMutex } from '../../src/mutex.js';
import {
	GC_TTL_MS,
	type IntentRecord,
	IntentRegistry,
	type IntentRegistryOptions,
	SUBS_TTL_MS,
} from '../../src/sync/intent.js';

// The cross-tab intent registry: per-tab records of per-workspace
// subscriptions + undo refs in the shared KV store, unioned with TTL-based
// liveness. No locks, no channels — plain Node suffices.

/** The workspace most specs use; the multi-workspace ones name their own. */
const WS = 'ws1';

function sub(entity: string): Subscription {
	return create(SubscriptionSchema, { entity });
}

/** A one-workspace scope map, the shape almost every spec here wants. */
function scope(subs: Subscription[], workspaceId = WS): Map<string, Subscription[]> {
	return new Map([ [ workspaceId, subs ] ]);
}

/** Refs for one workspace. */
function refsOf(ids: Set<bigint>, workspaceId = WS): Map<string, Set<bigint>> {
	return new Map([ [ workspaceId, ids ] ]);
}

function entities(union: ReadonlyMap<string, Subscription[]>, workspaceId = WS): string[] {
	return (union.get(workspaceId) ?? []).map((s) => s.entity).sort();
}

describe('IntentRegistry', () => {
	function harness(startAt = 1_000_000) {
		const kv = new MemoryKVStore();
		const mutex = new ChainMutex();
		let now = startAt;
		const clock = { now: () => now, advance: (ms: number) => (now += ms) };
		const registry = (opts: Omit<IntentRegistryOptions, 'now'> = {}) =>
			new IntentRegistry(crypto.randomUUID(), kv, mutex, { ...opts, now: clock.now });
		const readRecord = async (tabId: string): Promise<IntentRecord | undefined> => {
			const tx = await kv.read();
			try {
				return (await tx.get(intentKey(tabId))) as IntentRecord | undefined;
			} finally {
				tx.release();
			}
		};
		return { kv, mutex, clock, registry, readRecord };
	}

	it('seed writes the record shape: encoded subs, decimal refs, updatedAt', async () => {
		const h = harness();
		const reg = h.registry();
		await reg.seed(scope([ sub('todo') ]), refsOf(new Set([ 7n, 12n ])));

		const rec = await h.readRecord(reg.tabId);
		expect(rec).toBeDefined();
		expect(rec!.subs).toEqual({ [WS]: [ toBinary(SubscriptionSchema, sub('todo')) ] });
		expect([ ...rec!.undoRefIds[WS]! ].sort()).toEqual([ '12', '7' ]);
		expect(rec!.updatedAt).toBe(h.clock.now());
	});

	it('writeRefs joins an open transaction, preserves subs and bumps updatedAt', async () => {
		const h = harness();
		const reg = h.registry();
		await reg.seed(scope([ sub('todo') ]), refsOf(new Set()));
		const seededAt = (await h.readRecord(reg.tabId))!.updatedAt;

		h.clock.advance(1234);
		const tx = await h.kv.write();
		await reg.writeRefs(tx, WS, new Set([ 3n ]));
		await tx.commit();

		const rec = (await h.readRecord(reg.tabId))!;
		expect(rec.subs).toEqual({ [WS]: [ toBinary(SubscriptionSchema, sub('todo')) ] });
		expect(rec.undoRefIds).toEqual({ [WS]: [ '3' ] });
		expect(rec.updatedAt).toBe(seededAt + 1234);
	});

	it('writeRefs before seed() is a no-op', async () => {
		const h = harness();
		const reg = h.registry();
		const tx = await h.kv.write();
		await reg.writeRefs(tx, WS, new Set([ 1n ]));
		await tx.commit();
		expect(await h.readRecord(reg.tabId)).toBeUndefined();
	});

	it('unions subscriptions across live records, deduplicating identical ones', async () => {
		const h = harness();
		const a = h.registry();
		const b = h.registry();
		await a.seed(scope([ sub('todo') ]), refsOf(new Set()));
		await b.seed(scope([ sub('project'), sub('todo') ]), refsOf(new Set()));

		// Both tabs compute the same union; the shared 'todo' dedups by encoding.
		expect(entities(await a.unionScopes(scope([ sub('todo') ]))))
			.toEqual([ 'project', 'todo' ]);
		expect(entities(await b.unionScopes(scope([ sub('project'), sub('todo') ]))))
			.toEqual([ 'project', 'todo' ]);
	});

	it('always includes the own in-memory set, even before its record lands', async () => {
		const h = harness();
		const a = h.registry();
		await a.seed(scope([ sub('todo') ]), refsOf(new Set()));
		// 'widget' was never persisted anywhere — only passed in-memory.
		expect(entities(await a.unionScopes(scope([ sub('widget') ]))))
			.toEqual([ 'todo', 'widget' ]);
	});

	it('full sync wins: an empty own set or any live empty record yields []', async () => {
		const h = harness();
		const a = h.registry();
		const b = h.registry();
		await a.seed(scope([ sub('todo') ]), refsOf(new Set()));
		expect(entities(await a.unionScopes(scope([])))).toEqual([]);

		await b.seed(scope([]), refsOf(new Set())); // a full-sync tab
		expect(entities(await a.unionScopes(scope([ sub('todo') ])))).toEqual([]);
	});

	it('drops a stale record from the subs union but keeps protecting its undo refs', async () => {
		const h = harness();
		const a = h.registry();
		const b = h.registry();
		await b.seed(scope([ sub('project') ]), refsOf(new Set([ 42n ])));
		await a.seed(scope([ sub('todo') ]), refsOf(new Set()));

		// One hour idle: past SUBS_TTL_MS, far inside GC_TTL_MS.
		h.clock.advance(SUBS_TTL_MS + 30 * 60_000);
		// Keep a's own record fresh so only b's is judged stale.
		await a.updateScopes(scope([ sub('todo') ]));

		expect(entities(await a.unionScopes(scope([ sub('todo') ])))).toEqual([ 'todo' ]);

		const tx = await h.kv.read();
		try {
			const refs = await a.unionUndoRefIds(tx, WS, new Set([ 1n ]));
			expect(refs).toEqual(new Set([ 1n, 42n ]));
		} finally {
			tx.release();
		}
	});

	it('garbage-collects records older than GC_TTL_MS', async () => {
		const h = harness();
		const a = h.registry();
		const b = h.registry();
		await b.seed(scope([ sub('project') ]), refsOf(new Set([ 42n ])));
		h.clock.advance(GC_TTL_MS + 1);
		await a.seed(scope([ sub('todo') ]), refsOf(new Set()));

		// The expired record no longer protects refs...
		const tx = await h.kv.read();
		try {
			expect(await a.unionUndoRefIds(tx, WS, new Set())).toEqual(new Set());
		} finally {
			tx.release();
		}

		// ...and a union scan fires its deletion (fire-and-forget).
		expect(entities(await a.unionScopes(scope([ sub('todo') ])))).toEqual([ 'todo' ]);
		await vi.waitFor(async () => {
			expect(await h.readRecord(b.tabId)).toBeUndefined();
		});
		expect(await h.readRecord(a.tabId)).toBeDefined();
	});

	it('heartbeatIfStale refreshes updatedAt only past the heartbeat interval', async () => {
		const h = harness();
		const reg = h.registry({ heartbeatMs: 1000 });
		await reg.seed(scope([ sub('todo') ]), refsOf(new Set()));
		const seededAt = (await h.readRecord(reg.tabId))!.updatedAt;

		h.clock.advance(500);
		await reg.heartbeatIfStale(); // fresh: no write
		expect((await h.readRecord(reg.tabId))!.updatedAt).toBe(seededAt);

		h.clock.advance(600);
		await reg.heartbeatIfStale(); // stale: refresh
		expect((await h.readRecord(reg.tabId))!.updatedAt).toBe(seededAt + 1100);
	});

	it('delete removes the record and disarms later writes', async () => {
		const h = harness();
		const reg = h.registry();
		await reg.seed(scope([ sub('todo') ]), refsOf(new Set()));
		await reg.delete();
		expect(await h.readRecord(reg.tabId)).toBeUndefined();

		await reg.updateRefs(WS, new Set([ 1n ]));
		await reg.heartbeatIfStale();
		expect(await h.readRecord(reg.tabId)).toBeUndefined();
	});

	it('reap removes a peer record so it stops widening the union', async () => {
		const h = harness();
		const a = h.registry();
		const b = h.registry();
		await a.seed(scope([ sub('todo') ]), refsOf(new Set()));
		await b.seed(scope([]), refsOf(new Set())); // full-sync peer
		// The full-sync peer collapses a's union to [] (full sync)...
		expect(entities(await a.unionScopes(scope([ sub('todo') ])))).toEqual([]);

		// ...until a reaps it on a departure signal; then a's scope takes effect.
		await a.reap(b.tabId);
		expect(await h.readRecord(b.tabId)).toBeUndefined();
		expect(entities(await a.unionScopes(scope([ sub('todo') ])))).toEqual([ 'todo' ]);
	});

	it('reap never deletes the own record (only delete() does that)', async () => {
		const h = harness();
		const reg = h.registry();
		await reg.seed(scope([ sub('todo') ]), refsOf(new Set()));
		await reg.reap(reg.tabId);
		expect(await h.readRecord(reg.tabId)).toBeDefined();
	});

	it('reap of an absent record is a harmless no-op', async () => {
		const h = harness();
		const reg = h.registry();
		await reg.seed(scope([ sub('todo') ]), refsOf(new Set()));
		await expect(reg.reap('never-existed')).resolves.toBeUndefined();
	});

	it('ignores foreign or malformed values under the intent prefix', async () => {
		const h = harness();
		const a = h.registry();
		await a.seed(scope([ sub('todo') ]), refsOf(new Set()));
		const tx = await h.kv.write();
		await tx.put(intentKey('mangled'), { wat: true });
		await tx.commit();

		expect(entities(await a.unionScopes(scope([ sub('todo') ])))).toEqual([ 'todo' ]);
		const read = await h.kv.read();
		try {
			expect(await a.unionUndoRefIds(read, WS, new Set([ 5n ]))).toEqual(new Set([ 5n ]));
		} finally {
			read.release();
		}
	});
});
