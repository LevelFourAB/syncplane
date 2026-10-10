// Conformance vectors shared with the Go engine
// (engine/server/accesssync_delta_internal_test.go): the engine finds each
// delta from base to want, and the client must turn base into want with it.
// Then the rebase transaction that applies a page's delta to the stored
// summary.

import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { ACCESS_TOUCH_ENTITY, applyAccessDelta, encodeAccess } from '../../src/access.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import {
	type AnchorGroup,
	type AnchorRows,
	type CallerAccess,
	CallerAccessDeltaSchema,
	CallerAccessSchema,
} from '../../src/gen/syncplane/v1/service_pb.js';
import { metaCallerAccess, metaCallerAccessVersion, touchKey } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { EntityRegistry } from '../../src/mutations.js';
import { finalizeRebase, rebase } from '../../src/optimistic/rebase.js';
import { TouchSet } from '../../src/touch.js';

interface Summary {
	subjects: string[];
	root: string[];
	rows: Record<string, string[]>;
}

interface DeltaCase {
	name: string;
	base: Summary;
	want: Summary;
	delta: { set: Record<string, string[]>; removed: string[] } | null;
}

const vectorsPath = join(
	dirname(fileURLToPath(import.meta.url)),
	'../../../testdata/access/delta.json',
);
const vectors = JSON.parse(readFileSync(vectorsPath, 'utf-8')) as { cases: DeltaCase[] };

/** Groups rows ("entity/key" to permissions) as CallerAccess.anchor_groups orders them. */
function groupsOf(rows: Record<string, string[]>): AnchorGroup[] {
	const byPerms = new Map<string, string[]>();
	for (const [ row, perms ] of Object.entries(rows)) {
		const id = perms.join('\u0000');
		byPerms.set(id, [ ...(byPerms.get(id) ?? []), row ]);
	}
	return [ ...byPerms.entries() ]
		.map(([ id, rows ]) => ({ permissions: id.split('\u0000'), anchors: anchorsOf(rows) }))
		.sort((a, b) => {
			const n = Math.min(a.permissions.length, b.permissions.length);
			for (let i = 0; i < n; i++) {
				if (a.permissions[i] !== b.permissions[i]) {
					return a.permissions[i]! < b.permissions[i]!
						? -1
						: 1;
				}
			}
			return a.permissions.length - b.permissions.length;
		}) as AnchorGroup[];
}

function anchorsOf(rows: string[]): AnchorRows[] {
	const out: { entity: string; keys: string[] }[] = [];
	for (const row of [ ...rows ].sort()) {
		const [ entity, key ] = row.split('/') as [string, string];
		const last = out[out.length - 1];
		if (last?.entity === entity) last.keys.push(key);
		else out.push({ entity, keys: [ key ] });
	}
	return out as AnchorRows[];
}

function summaryOf(s: Summary, version: bigint): CallerAccess {
	return create(CallerAccessSchema, {
		seq: 1n,
		subjects: s.subjects,
		rootPermissions: s.root,
		anchorGroups: groupsOf(s.rows),
		version,
	});
}

describe('CallerAccess delta conformance vectors', () => {
	it('has cases', () => {
		expect(vectors.cases.length).toBeGreaterThan(0);
	});

	for (const c of vectors.cases) {
		if (c.delta === null) continue;
		const set = c.delta.set;
		const removed = c.delta.removed;
		it(c.name, () => {
			const delta = create(CallerAccessDeltaSchema, {
				seq: 7n,
				baseVersion: 1n,
				version: 2n,
				anchorGroups: groupsOf(set),
				removed: anchorsOf(removed),
			});
			const got = applyAccessDelta(summaryOf(c.base, 1n), delta);
			const want = { ...summaryOf(c.want, 2n), seq: 7n };
			// Bytes, not structure: a summary in another order is the same
			// access but does not encode as the server's does.
			expect(toBinary(CallerAccessSchema, got)).toEqual(toBinary(CallerAccessSchema, want));
		});
	}
});

describe('rebase with a CallerAccessDelta', () => {
	const registry = new EntityRegistry([ TodoSchema ]);
	const WS = 'ws1';
	const base: Summary = { subjects: [ 'u' ], root: [], rows: { 'project/p1': [ 'a' ] } };
	const delta = create(CallerAccessDeltaSchema, {
		seq: 5n,
		baseVersion: 1n,
		version: 2n,
		anchorGroups: groupsOf({ 'project/p2': [ 'b' ] }),
	});

	async function storeWith(access: CallerAccess | undefined): Promise<MemoryKVStore> {
		const kv = new MemoryKVStore();
		const tx = await kv.write();
		await finalizeRebase(tx, registry, {}, WS, {
			nextCursor: new Uint8Array([ 1 ]),
			lastAckedMutationId: 0n,
			...(access === undefined ? {} : { callerAccess: encodeAccess(access) }),
		}, new TouchSet());
		await tx.commit();
		return kv;
	}

	async function stored(kv: MemoryKVStore): Promise<{ access?: CallerAccess; version?: string }> {
		const tx = await kv.read();
		try {
			const raw = (await tx.get(metaCallerAccess(WS))) as Uint8Array | undefined;
			const version = (await tx.get(metaCallerAccessVersion(WS))) as string | undefined;
			return {
				...(raw === undefined ? {} : { access: fromBinary(CallerAccessSchema, raw) }),
				...(version === undefined ? {} : { version }),
			};
		} finally {
			tx.release();
		}
	}

	const page = {
		changes: [],
		nextCursor: new Uint8Array([ 2 ]),
		lastAckedMutationId: 0n,
		callerAccessDelta: delta,
	};

	it('applies the delta to the summary it was made for', async () => {
		const kv = await storeWith(summaryOf(base, 1n));
		const result = await rebase(kv, registry, {}, WS, page);

		const { access, version } = await stored(kv);
		expect(access?.version).toBe(2n);
		expect(access?.seq).toBe(5n);
		expect(version).toBe('2');
		expect(result.accessStale).toBeUndefined();
		expect(result.callerAccess?.summary.holds('b', 'project', 'p2')).toBe(true);
		expect(result.touched.tokens().has(touchKey(WS, ACCESS_TOUCH_ENTITY))).toBe(true);
	});

	it('stores the summary applied before the transaction', async () => {
		const kv = await storeWith(summaryOf(base, 1n));
		const applied = encodeAccess(applyAccessDelta(summaryOf(base, 1n), delta));
		const result = await rebase(kv, registry, {}, WS, { ...page, callerAccessApplied: applied });

		expect(result.callerAccess).toBe(applied);
		expect((await stored(kv)).version).toBe('2');
	});

	it('changes nothing when the summary already is the one the delta gives', async () => {
		const kv = await storeWith(summaryOf(base, 2n));
		const result = await rebase(kv, registry, {}, WS, page);

		const { access, version } = await stored(kv);
		expect(access?.version).toBe(2n);
		expect(access?.seq).toBe(1n);
		expect(version).toBe('2');
		expect(result.callerAccess).toBeUndefined();
		expect(result.accessStale).toBeUndefined();
	});

	it('keeps a summary the delta was not made for and reports it stale', async () => {
		const kv = await storeWith(summaryOf(base, 9n));
		const applied = encodeAccess(applyAccessDelta(summaryOf(base, 1n), delta));
		const result = await rebase(kv, registry, {}, WS, { ...page, callerAccessApplied: applied });

		const { access, version } = await stored(kv);
		expect(access?.version).toBe(9n);
		expect(version).toBe('9');
		expect(result.accessStale).toBe(true);
		expect(result.callerAccess).toBeUndefined();
		expect(result.touched.tokens().has(touchKey(WS, ACCESS_TOUCH_ENTITY))).toBe(false);
	});

	it('reports the delta stale when no summary is stored', async () => {
		const kv = await storeWith(undefined);
		const result = await rebase(kv, registry, {}, WS, page);

		expect(await stored(kv)).toEqual({});
		expect(result.accessStale).toBe(true);
	});

	it('stores no version for a summary that has none', async () => {
		const kv = await storeWith(summaryOf(base, 3n));
		await rebase(kv, registry, {}, WS, {
			changes: [],
			nextCursor: new Uint8Array([ 3 ]),
			lastAckedMutationId: 0n,
			callerAccess: encodeAccess(summaryOf(base, 0n)),
		});

		const { access, version } = await stored(kv);
		expect(access?.version).toBe(0n);
		expect(version).toBeUndefined();
	});
});
