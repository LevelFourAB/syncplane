import { afterEach, describe, expect, it, vi } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { ChainMutex, createKvMutex, WebLockMutex } from '../../src/mutex.js';
import { readPending } from '../../src/optimistic/queue.js';

/**
 * Minimal Web Locks fake: exclusive, queued per lock name — the semantics
 * WebLockMutex relies on. Shared across "tabs" like the real LockManager.
 */
class FakeLockManager {
	private queues = new Map<string, Promise<unknown>>();

	request<T>(name: string, _opts: unknown, fn: () => Promise<T>): Promise<T> {
		const prev = this.queues.get(name) ?? Promise.resolve();
		const result = prev.then(fn, fn);
		this.queues.set(
			name,
			result.catch(() => undefined),
		);
		return result;
	}
}

function installFakeLocks(): FakeLockManager {
	const locks = new FakeLockManager();
	vi.stubGlobal('navigator', { locks });
	return locks;
}

afterEach(() => {
	vi.unstubAllGlobals();
});

/** Runs `count` overlapping critical sections and records enter/exit order. */
async function runOverlapping(mutex: { run<T>(fn: () => Promise<T>): Promise<T> }, count: number) {
	const events: string[] = [];
	await Promise.all(
		Array.from({ length: count }, (_, i) =>
			mutex.run(async () => {
				events.push(`enter:${i}`);
				await new Promise((r) => setTimeout(r, 1));
				events.push(`exit:${i}`);
			})),
	);
	return events;
}

function expectSerialized(events: string[], count: number) {
	expect(events).toHaveLength(count * 2);
	for (let i = 0; i < events.length; i += 2) {
		const enter = events[i];
		const exit = events[i + 1];
		expect(enter).toMatch(/^enter:/);
		expect(exit).toBe(enter!.replace('enter', 'exit'));
	}
}

describe('WebLockMutex', () => {
	it('serializes overlapping runs through navigator.locks', async () => {
		installFakeLocks();
		const mutex = new WebLockMutex('test-lock');
		expectSerialized(await runOverlapping(mutex, 5), 5);
	});

	it('serializes two mutex instances sharing a lock name (two tabs)', async () => {
		installFakeLocks();
		const a = new WebLockMutex('shared');
		const b = new WebLockMutex('shared');
		const events: string[] = [];
		await Promise.all([
			a.run(async () => {
				events.push('enter:a');
				await new Promise((r) => setTimeout(r, 1));
				events.push('exit:a');
			}),
			b.run(async () => {
				events.push('enter:b');
				await new Promise((r) => setTimeout(r, 1));
				events.push('exit:b');
			}),
		]);
		expect(events).toEqual([ 'enter:a', 'exit:a', 'enter:b', 'exit:b' ]);
	});

	it('requests exclusive for run and shared for runShared', async () => {
		const modes: string[] = [];
		vi.stubGlobal('navigator', {
			locks: {
				request<T>(_name: string, opts: { mode: string }, fn: () => Promise<T>): Promise<T> {
					modes.push(opts.mode);
					return fn();
				},
			},
		});
		const mutex = new WebLockMutex('modes');
		await mutex.run(async () => {});
		await mutex.runShared(async () => {});
		expect(modes).toEqual([ 'exclusive', 'shared' ]);
	});

	it('is non-reentrant: a nested run cannot enter while the outer holds the lock', async () => {
		// Awaiting a nested run inside a run would therefore deadlock — never
		// nest kvMutex.run calls.
		installFakeLocks();
		const mutex = new WebLockMutex('nested');
		let innerEnteredDuringOuter = false;
		await mutex.run(async () => {
			let entered = false;
			void mutex.run(async () => {
				entered = true;
			});
			await new Promise((r) => setTimeout(r, 5));
			innerEnteredDuringOuter = entered;
		});
		expect(innerEnteredDuringOuter).toBe(false);
	});
});

describe('ChainMutex', () => {
	it('serializes overlapping runs in-process', async () => {
		expectSerialized(await runOverlapping(new ChainMutex(), 5), 5);
	});

	it('runs shared (reader) sections concurrently', async () => {
		const mutex = new ChainMutex();
		let active = 0;
		let maxActive = 0;
		await Promise.all(
			Array.from({ length: 4 }, () =>
				mutex.runShared(async () => {
					active++;
					maxActive = Math.max(maxActive, active);
					await new Promise((r) => setTimeout(r, 2));
					active--;
				})),
		);
		// All four readers overlap — shared access is not serialized.
		expect(maxActive).toBe(4);
	});

	it('excludes a writer from any reader (no overlap) and vice versa', async () => {
		const mutex = new ChainMutex();
		let readers = 0;
		let writers = 0;
		let violations = 0;
		const reader = () =>
			mutex.runShared(async () => {
				readers++;
				if (writers !== 0) violations++;
				await new Promise((r) => setTimeout(r, 2));
				if (writers !== 0) violations++;
				readers--;
			});
		const writer = () =>
			mutex.run(async () => {
				writers++;
				if (readers !== 0 || writers !== 1) violations++;
				await new Promise((r) => setTimeout(r, 2));
				if (readers !== 0 || writers !== 1) violations++;
				writers--;
			});
		await Promise.all([ reader(), writer(), reader(), reader(), writer(), reader() ]);
		expect(violations).toBe(0);
	});

	it('does not starve a writer behind a steady reader stream', async () => {
		const mutex = new ChainMutex();
		const order: string[] = [];
		// A writer requested while a reader holds the lock; later readers must
		// queue behind it rather than keep the batch open indefinitely.
		const r1 = mutex.runShared(async () => {
			order.push('r1');
			await new Promise((r) => setTimeout(r, 5));
		});
		const w = mutex.run(async () => {
			order.push('w');
		});
		const r2 = mutex.runShared(async () => {
			order.push('r2');
		});
		await Promise.all([ r1, w, r2 ]);
		// The writer runs before the reader that arrived after it.
		expect(order.indexOf('w')).toBeLessThan(order.indexOf('r2'));
	});

	it('mixed reader/writer load settles without hanging or overlap violations', async () => {
		const mutex = new ChainMutex();
		let held = 0;
		let readers = 0;
		let maxReaders = 0;
		let violations = 0;
		const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
		const tasks: Promise<unknown>[] = [];
		for (let i = 0; i < 300; i++) {
			const dur = i % 3;
			if (i % 10 < 6) {
				tasks.push(
					mutex.runShared(async () => {
						if (held !== 0) violations++;
						readers++;
						maxReaders = Math.max(maxReaders, readers);
						await sleep(dur);
						if (held !== 0) violations++;
						readers--;
					}),
				);
			} else {
				tasks.push(
					mutex.run(async () => {
						if (held !== 0 || readers !== 0) violations++;
						held++;
						await sleep(dur);
						held--;
					}),
				);
			}
		}
		await Promise.all(tasks);
		expect(violations).toBe(0);
		expect(maxReaders).toBeGreaterThanOrEqual(2);
	});

	it('keeps the chain alive when a section throws', async () => {
		const mutex = new ChainMutex();
		await expect(mutex.run(async () => {
			throw new Error('boom');
		})).rejects.toThrow('boom');
		await expect(mutex.runShared(async () => {
			throw new Error('boom-shared');
		})).rejects.toThrow('boom-shared');
		// A subsequent acquisition still proceeds.
		expect(await mutex.run(async () => 42)).toBe(42);
		expect(await mutex.runShared(async () => 7)).toBe(7);
	});
});

describe('createKvMutex', () => {
	it('uses Web Locks when navigator.locks exists', () => {
		installFakeLocks();
		expect(createKvMutex('x')).toBeInstanceOf(WebLockMutex);
	});

	it('falls back to ChainMutex without navigator.locks', () => {
		vi.stubGlobal('navigator', {});
		expect(createKvMutex('x')).toBeInstanceOf(ChainMutex);
	});

	it('falls back to ChainMutex on a server runtime despite navigator.locks', () => {
		// Node >= 26 exposes navigator.locks, but a workspace-named Web Lock would
		// serialize two unrelated private stores in one process; ChainMutex keeps
		// each client's store independent.
		const locks = installFakeLocks();
		vi.stubGlobal('navigator', { locks, userAgent: 'Node.js/26' });
		expect(createKvMutex('x')).toBeInstanceOf(ChainMutex);
	});
});

describe('cross-tab mutation-id minting', () => {
	it('two clients sharing one store mint strictly increasing unique ids', async () => {
		installFakeLocks();
		const kv = new MemoryKVStore();
		const newTab = () =>
			new SyncClient({
				workspaceId: 'ws1',
				kvStore: kv,
				entities: [ TodoSchema ],
				streamEnabled: false,
				pollIntervalMs: 0,
			});
		const tabA = newTab();
		const tabB = newTab();

		// Interleave writes from both tabs without awaiting in between, so the
		// read-modify-write of the workspace's mutation-id counter overlaps maximally.
		await Promise.all(
			Array.from({ length: 10 }, (_, i) =>
				Promise.all([
					tabA.mutate.put(TodoSchema, { id: `a${i}`, title: 'from A' }),
					tabB.mutate.put(TodoSchema, { id: `b${i}`, title: 'from B' }),
				])),
		);

		const tx = await kv.read();
		const pending = await readPending(tx, 'ws1');
		tx.release();

		const ids = pending.map((m) => m.mutationId);
		expect(ids).toHaveLength(20);
		expect(new Set(ids.map(String)).size).toBe(20);
		for (let i = 0; i < ids.length; i++) {
			expect(ids[i]).toBe(BigInt(i + 1));
		}
		const clientIds = new Set(pending.map((m) => m.clientId));
		expect(clientIds.size).toBe(1);
	});
});
