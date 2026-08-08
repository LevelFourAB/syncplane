// Upstream delta state: what the coordinator puts on the wire for a
// collaborative-text PATCH, and what it does when the server refuses it.
//
// The invariant every spec here circles: applying the shipped value to the
// state the server already has must reach the same bytes as merging the full
// document would have. That holds whichever form the queue entry took — a whole
// document from a string-valued write, or the one edit's fragment a live text
// handle queues — because the push path normalizes both to the same thing:
// everything queued for that field beyond the confirmed row.

import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { createRouterTransport, type Transport } from '@connectrpc/connect';
import { beforeEach, describe, expect, it } from 'vitest';

import { CodeTextFullStateRequired } from '../../src/codes.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import type { CrudMutation } from '../../src/gen/syncplane/v1/mutation_pb.js';
import { MutationSchema } from '../../src/gen/syncplane/v1/mutation_pb.js';
import { MutationStatus, SyncService } from '../../src/gen/syncplane/v1/service_pb.js';
import {
	confirmedKey,
	metaCursor,
	metaLastMutationId,
	metaPushedThrough,
} from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { buildPatch, EntityRegistry } from '../../src/mutations.js';
import { createKvMutex } from '../../src/mutex.js';
import { enqueuePending, readPending } from '../../src/optimistic/queue.js';
import { applyMutationLocally } from '../../src/optimistic/rebase.js';
import { DEFAULT_STORAGE_PREFIX, storageNames } from '../../src/storagenames.js';
import { Coordinator, type RejectionInfo } from '../../src/sync/coordinator.js';
import { StatusTracker } from '../../src/sync/status.js';
import {
	decodeTextDoc,
	encodeFragmentFrom,
	encodeTextDoc,
	isFragment,
	mergeText,
	plainTextOf,
	TextDoc,
} from '../../src/text/index.js';
import { TouchSet } from '../../src/touch.js';
import { WatcherHub } from '../../src/watch.js';
import { pushedMutations, pushResults } from './pushfake.js';

const WS = 'ws';

/** An encoded document of `repeats` sentences — large enough that shipping the
 * whole thing for a one-word edit is visibly wasteful. */
function bigDoc(peer: string, repeats: number): Uint8Array {
	const d = new TextDoc(peer);
	d.insert(0, 'lorem ipsum dolor sit amet '.repeat(repeats));
	return encodeTextDoc(d);
}

/** Applies an edit to an encoded document under a fresh writer identity. */
function edit(state: Uint8Array, peer: string, fn: (d: TextDoc) => void): Uint8Array {
	const d = decodeTextDoc(state, peer);
	fn(d);
	return encodeTextDoc(d);
}

/** The crud list of a pushed mutation batch, flattened. */
function crudsOf(mutations: readonly { payload: unknown }[]): CrudMutation[] {
	return (mutations as { payload: { case?: string; value: unknown } }[]).flatMap((m) =>
		m.payload.case === 'crud'
			? [ m.payload.value as CrudMutation ]
			: m.payload.case === 'batch'
			? (m.payload.value as { mutations: CrudMutation[] }).mutations
			: []
	);
}

/** The body bytes carried by the first crud of a push. */
function bodyOf(cruds: CrudMutation[]): Uint8Array {
	return fromBinary(WidgetSchema, cruds[0]!.value).body;
}

describe('upstream text delta state', () => {
	let kv: MemoryKVStore;
	let registry: EntityRegistry;
	let rejections: RejectionInfo[];
	let pushes: CrudMutation[][];

	beforeEach(() => {
		kv = new MemoryKVStore();
		registry = new EntityRegistry([ WidgetSchema ]);
		rejections = [];
		pushes = [];
	});

	/**
	 * Seeds a confirmed widget row plus one pending PATCH carrying `local` for
	 * its body, its optimistic overlay, a cursor (so sync skips bootstrap) and
	 * the mutation-id counter. A `confirmed` of undefined leaves the row absent.
	 */
	async function seed(confirmed: Uint8Array | undefined, local: Uint8Array): Promise<void> {
		const def = registry.bySchema(WidgetSchema);
		const crud = buildPatch(def, WidgetSchema, 'w1', { body: local });
		const mutation = create(MutationSchema, {
			clientId: 'c1',
			mutationId: 1n,
			payload: { case: 'crud', value: crud },
		});
		const tx = await kv.write();
		if (confirmed !== undefined) {
			await tx.put(confirmedKey('widget', WS, 'w1'), {
				v: toBinary(WidgetSchema, create(WidgetSchema, { id: 'w1', body: confirmed })),
				ver: '1',
			});
		}
		await enqueuePending(tx, WS, mutation);
		await applyMutationLocally(tx, registry, {}, WS, mutation, new TouchSet());
		await tx.put(metaCursor(WS), new Uint8Array([ 1 ]));
		await tx.put(metaLastMutationId(WS), '1');
		await tx.commit();
	}

	/** Records each pushed batch's cruds and answers every mutation with `answer`. */
	function fakeTransport(
		answer: (m: { mutationId: bigint }) => {
			mutationId: bigint;
			status: MutationStatus;
			seq?: bigint;
			errorCode?: string;
			errorMessage?: string;
		},
	): Transport {
		return createRouterTransport(({ service }) => {
			service(SyncService, {
				push(req) {
					const mutations = pushedMutations(req);
					pushes.push(crudsOf(mutations));
					return pushResults(req, mutations.map(answer));
				},
				pull() {
					return {
						patch: { changes: [], dropEntities: [] },
						nextCursor: new Uint8Array([ 1 ]),
						lastAckedMutationId: 0n,
						reset: false,
						hasMore: false,
						seq: 0n,
					};
				},
				// eslint-disable-next-line require-yield
				async *bootstrap() {
					throw new Error('bootstrap should not run with a seeded cursor');
				},
				query() {
					return { rows: [] };
				},
			});
		});
	}

	const applied = (m: { mutationId: bigint }) => ({
		mutationId: m.mutationId,
		status: MutationStatus.APPLIED,
		seq: m.mutationId,
	});

	function newCoordinator(transport: Transport): Coordinator {
		const kvMutex = createKvMutex('test');
		const hub = new WatcherHub(kv, registry, kvMutex);
		return new Coordinator({
			transport,
			kv,
			kvMutex,
			storageNames: storageNames(DEFAULT_STORAGE_PREFIX, 'acct'),
			scopes: new Map([ [ WS, [] ] ]),
			clientId: 'c1',
			registry,
			mirrors: {},
			notify: (touched) => hub.notify(touched),
			status: new StatusTracker(),
			onMutationRejected: (info) => rejections.push(info),
			streamEnabled: false,
			pollIntervalMs: 0,
		});
	}

	it('ships a fragment, and applying it reaches the full-state merge byte for byte', async () => {
		const confirmed = bigDoc('seed', 200);
		const local = edit(confirmed, 'alice', (d) => d.insert(0, 'HELLO '));
		await seed(confirmed, local);

		const coord = newCoordinator(fakeTransport(applied));
		await coord.syncNow();
		await coord.stop();

		const shipped = bodyOf(pushes[0]!);
		expect(isFragment(shipped)).toBe(true);
		expect(shipped.length).toBeLessThan(local.length * 0.5);
		// The whole point: the server's row ends up where a full-state PATCH
		// would have put it.
		expect(mergeText(confirmed, shipped)).toEqual(mergeText(confirmed, local));
		expect(plainTextOf(mergeText(confirmed, shipped))).toBe(plainTextOf(local));
	});

	it('leaves the queued mutation as it was, so rebase replays the same bytes', async () => {
		const confirmed = bigDoc('seed', 200);
		const local = edit(confirmed, 'alice', (d) => d.insert(0, 'HELLO '));
		await seed(confirmed, local);

		const coord = newCoordinator(fakeTransport(applied));
		await coord.syncNow();
		await coord.stop();

		const tx = await kv.read();
		const pending = await readPending(tx, WS);
		tx.release();
		const queued = bodyOf(crudsOf(pending));
		expect(isFragment(queued)).toBe(false);
		expect(queued).toEqual(local);
	});

	it('ships full state when there is no confirmed row to diff against', async () => {
		const local = bigDoc('alice', 200);
		await seed(undefined, local);

		const coord = newCoordinator(fakeTransport(applied));
		await coord.syncNow();
		await coord.stop();

		expect(bodyOf(pushes[0]!)).toEqual(local);
	});

	it('ships full state when the fragment would not be enough smaller', async () => {
		// Rewriting nearly the whole document leaves a fragment about as big as
		// the document, so the substitution is not worth its decode path.
		const confirmed = bigDoc('seed', 4);
		const local = edit(confirmed, 'alice', (d) => {
			d.setString('completely different text, several times over'.repeat(4));
		});
		await seed(confirmed, local);

		const coord = newCoordinator(fakeTransport(applied));
		await coord.syncNow();
		await coord.stop();

		expect(isFragment(bodyOf(pushes[0]!))).toBe(false);
	});

	// A live text handle queues one fragment per edit, so the push path receives
	// deltas rather than documents and has to turn each into the cumulative delta
	// against the confirmed row.
	describe('when the queue holds delta state', () => {
		/**
		 * Seeds a confirmed row and `edits` keystrokes queued as fragments, each
		 * cut against the state the one before it left. Returns the local document
		 * they add up to.
		 */
		async function seedFragments(
			confirmed: Uint8Array,
			edits: number,
			opts?: { alreadyAttempted?: boolean },
		): Promise<Uint8Array> {
			const def = registry.bySchema(WidgetSchema);
			const tx = await kv.write();
			await tx.put(confirmedKey('widget', WS, 'w1'), {
				v: toBinary(WidgetSchema, create(WidgetSchema, { id: 'w1', body: confirmed })),
				ver: '1',
			});
			const doc = decodeTextDoc(confirmed, 'alice');
			for (let i = 0; i < edits; i++) {
				const range = doc.insert(i, String(i));
				const fragment = encodeFragmentFrom(
					doc,
					[ { peer: range.peer, counter: range.start } ],
					[],
				);
				const mutation = create(MutationSchema, {
					clientId: 'c1',
					mutationId: BigInt(i + 1),
					payload: {
						case: 'crud',
						value: buildPatch(def, WidgetSchema, 'w1', { body: fragment }),
					},
				});
				await enqueuePending(tx, WS, mutation);
				await applyMutationLocally(tx, registry, {}, WS, mutation, new TouchSet());
			}
			await tx.put(metaCursor(WS), new Uint8Array([ 1 ]));
			await tx.put(metaLastMutationId(WS), String(edits));
			if (opts?.alreadyAttempted) {
				// Compaction may only touch ids the server cannot have seen, so this
				// is how a queue reaches the push path unfolded.
				await tx.put(metaPushedThrough(WS), String(edits));
			}
			await tx.commit();
			return encodeTextDoc(doc);
		}

		it('folds a typing burst into one mutation carrying every edit', async () => {
			const confirmed = bigDoc('seed', 50);
			const local = await seedFragments(confirmed, 3);

			const coord = newCoordinator(fakeTransport(applied));
			await coord.syncNow();
			await coord.stop();

			// Nothing pins these mutations — a symbolic text inverse does not — so
			// compaction squashes the burst before it is ever pushed.
			const cruds = pushes[0]!;
			expect(cruds).toHaveLength(1);
			const shipped = fromBinary(WidgetSchema, cruds[0]!.value).body;
			expect(isFragment(shipped)).toBe(true);
			expect(mergeText(confirmed, shipped)).toEqual(mergeText(confirmed, local));
		});

		it('ships each delta with everything queued before it', async () => {
			const confirmed = bigDoc('seed', 50);
			const local = await seedFragments(confirmed, 3, { alreadyAttempted: true });

			const coord = newCoordinator(fakeTransport(applied));
			await coord.syncNow();
			await coord.stop();

			const cruds = pushes[0]!;
			expect(cruds).toHaveLength(3);
			// Every mutation applies to the confirmed row on its own — the property
			// that survives one of them being rejected. A fragment carrying only its
			// own edit would name a parent the row does not have.
			let state = confirmed;
			for (const crud of cruds) {
				const shipped = fromBinary(WidgetSchema, crud.value).body;
				expect(isFragment(shipped)).toBe(true);
				expect(() => mergeText(confirmed, shipped)).not.toThrow();
				state = mergeText(state, shipped);
			}
			// And together they reach exactly the local document.
			expect(state).toEqual(mergeText(confirmed, local));
			expect(plainTextOf(state)).toBe(plainTextOf(local));
		});

		it('leaves the queue holding one edit per entry', async () => {
			const confirmed = bigDoc('seed', 50);
			await seedFragments(confirmed, 3);

			const coord = newCoordinator(fakeTransport(applied));
			await coord.syncNow();
			await coord.stop();

			const tx = await kv.read();
			const pending = await readPending(tx, WS);
			tx.release();
			for (const crud of crudsOf(pending)) {
				const queued = fromBinary(WidgetSchema, crud.value).body;
				expect(isFragment(queued)).toBe(true);
				expect(queued.length).toBeLessThan(confirmed.length / 20);
			}
		});

		it('expands a delta to full state for a pinned row', async () => {
			const confirmed = bigDoc('seed', 50);
			const local = await seedFragments(confirmed, 1);

			// One refusal pins the row; from then on its writes must be documents,
			// which for a queued delta means expanding it against the confirmed row.
			let refuseNext = true;
			const coord = newCoordinator(
				fakeTransport((m) => (refuseNext ? refuse(m) : applied(m))),
			);
			await coord.syncNow();
			refuseNext = false;
			await coord.syncNow();
			await coord.stop();

			const resent = bodyOf(pushes[pushes.length - 1]!);
			expect(isFragment(resent)).toBe(false);
			expect(plainTextOf(resent)).toBe(plainTextOf(local));
		});
	});

	const refuse = (m: { mutationId: bigint }) => ({
		mutationId: m.mutationId,
		status: MutationStatus.REJECTED,
		errorCode: CodeTextFullStateRequired,
		errorMessage: 'fragment does not apply',
	});

	describe('when the server refuses the fragment', () => {
		it('re-sends the same edit as full state and keeps it out of the app', async () => {
			const confirmed = bigDoc('seed', 200);
			const local = edit(confirmed, 'alice', (d) => d.insert(0, 'HELLO '));
			await seed(confirmed, local);

			// First cycle: fragment goes out, comes back refused. Second cycle
			// ships whatever the recovery re-queued.
			let refuseNext = true;
			const coord = newCoordinator(
				fakeTransport((m) => (refuseNext ? refuse(m) : applied(m))),
			);
			await coord.syncNow();
			refuseNext = false;
			await coord.syncNow();
			await coord.stop();

			expect(isFragment(bodyOf(pushes[0]!))).toBe(true);
			expect(pushes.length).toBeGreaterThanOrEqual(2);
			const resent = bodyOf(pushes[pushes.length - 1]!);
			expect(isFragment(resent)).toBe(false);
			// The edit survives the round trip: the re-sent document is the one
			// the rejected fragment described.
			expect(plainTextOf(resent)).toBe(plainTextOf(local));
			// A refused fragment is not a refused edit, so the app hears nothing.
			expect(rejections).toEqual([]);
		});

		it('pins the row to full state for the rest of the session', async () => {
			const confirmed = bigDoc('seed', 200);
			const local = edit(confirmed, 'alice', (d) => d.insert(0, 'HELLO '));
			await seed(confirmed, local);

			let refuseNext = true;
			const coord = newCoordinator(
				fakeTransport((m) => (refuseNext ? refuse(m) : applied(m))),
			);
			await coord.syncNow();
			refuseNext = false;
			await coord.syncNow();

			// A further edit to the same row must not probe with delta state again.
			const merged = mergeText(local, edit(local, 'alice2', (d) => d.insert(0, 'AGAIN ')));
			const def = registry.bySchema(WidgetSchema);
			const crud = buildPatch(def, WidgetSchema, 'w1', { body: merged });
			const mutation = create(MutationSchema, {
				clientId: 'c1',
				mutationId: 9n,
				payload: { case: 'crud', value: crud },
			});
			const tx = await kv.write();
			await enqueuePending(tx, WS, mutation);
			await applyMutationLocally(tx, registry, {}, WS, mutation, new TouchSet());
			await tx.put(metaLastMutationId(WS), '9');
			await tx.commit();

			await coord.syncNow();
			await coord.stop();

			expect(isFragment(bodyOf(pushes[pushes.length - 1]!))).toBe(false);
		});

		it('hands a batch back to the app when only part of it can be captured', async () => {
			// w1 is a live row whose text can be re-sent; w2 was never materialized
			// locally (its PATCH found no row, so delete-wins skipped the overlay), so
			// its half of the batch has nothing left to re-send. Recovering only w1
			// would tell the app the whole batch succeeded.
			const confirmed = bigDoc('seed', 200);
			const local = edit(confirmed, 'alice', (d) => d.insert(0, 'HELLO '));
			const def = registry.bySchema(WidgetSchema);
			const mutation = create(MutationSchema, {
				clientId: 'c1',
				mutationId: 1n,
				payload: {
					case: 'batch',
					value: {
						mutations: [
							buildPatch(def, WidgetSchema, 'w1', { body: local }),
							buildPatch(def, WidgetSchema, 'w2', { body: local }),
						],
					},
				},
			});
			const tx = await kv.write();
			await tx.put(confirmedKey('widget', WS, 'w1'), {
				v: toBinary(WidgetSchema, create(WidgetSchema, { id: 'w1', body: confirmed })),
				ver: '1',
			});
			await enqueuePending(tx, WS, mutation);
			await applyMutationLocally(tx, registry, {}, WS, mutation, new TouchSet());
			await tx.put(metaCursor(WS), new Uint8Array([ 1 ]));
			await tx.put(metaLastMutationId(WS), '1');
			await tx.commit();

			const coord = newCoordinator(fakeTransport(refuse));
			await coord.syncNow();
			await coord.stop();

			expect(rejections.map((r) => r.mutationId)).toEqual([ 1n ]);
			// Nothing was re-queued behind the app's back.
			const read = await kv.read();
			try {
				expect(await readPending(read, WS)).toEqual([]);
			} finally {
				read.release();
			}
		});
	});
});
