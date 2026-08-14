// The one-hop semi-join (`matches`): DSL shape, prepare-time validation, the
// two-phase rewrite, local query execution, coverage and live watchers.
// Cross-side agreement with the Go evaluator is pinned by the relatedCases
// vectors (query-vectors.test.ts); this file covers the client pipeline around
// them.

import { create, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { subscription, SyncClient } from '../../src/client.js';
import {
	IssueSchema,
	PairSchema,
	ProjectSchema,
	UserSchema,
	WidgetSchema,
} from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import type { Issue, User } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { RowChangeSchema } from '../../src/gen/syncplane/v1/patch_pb.js';
import { confirmedPrefix, metaCursor, overlayPrefix } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { KVRead, KVStore } from '../../src/kv/store.js';
import { EntityRegistry } from '../../src/mutations.js';
import { ChainMutex } from '../../src/mutex.js';
import { buildIndexes } from '../../src/optimistic/index-build.js';
import { IndexCache } from '../../src/optimistic/index-maintain.js';
import { applyServerChanges } from '../../src/optimistic/rebase.js';
import type { Cond } from '../../src/query/ast.js';
import { and, collectRels, computeRelKeys, not } from '../../src/query/index.js';
import { compileQuery, rootRef } from '../../src/query/proxy.js';
import type { MsgRef, QueryRef } from '../../src/query/types.js';
import { prepareQuery } from '../../src/query/wire.js';
import type { Row } from '../../src/read.js';
import { defineSchema, entity, type Infer, ref, text } from '../../src/schema/builder.js';
import { TouchSet } from '../../src/touch.js';

const flush = () => new Promise((r) => setTimeout(r, 5));

function newClient() {
	return new SyncClient({
		workspaceId: 'ws1',
		kvStore: new MemoryKVStore(),
		entities: [ IssueSchema, ProjectSchema, UserSchema, PairSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
	});
}

async function seed(client: ReturnType<typeof newClient>) {
	await client.db.user.mutate.put({ id: 'u1', name: 'Maya' });
	await client.db.user.mutate.put({ id: 'u2', name: 'Noor' });
	await client.db.pair.mutate.put({ ns: 'a', id: 'p1', label: 'keep' });
	await client.db.pair.mutate.put({ ns: 'b', id: 'p1', label: 'other' });
	await client.db.issue.mutate.put({
		id: 'i1',
		state: 'open',
		assignee: { id: 'u1' },
		pair: { ns: 'a', id: 'p1' },
	});
	await client.db.issue.mutate.put({ id: 'i2', state: 'open', assignee: { id: 'u2' } });
	await client.db.issue.mutate.put({
		id: 'i3',
		state: 'done',
		blockedBy: [ { id: 'i1' }, { id: 'ghost' } ],
		pair: { ns: 'b', id: 'p1' },
	});
	await client.db.issue.mutate.put({ id: 'i4', state: 'done', assignee: { id: 'ghost' } });
}

const keysOf = (rows: readonly { key: string }[]) => rows.map((r) => r.key);

describe('matches DSL and prepare', () => {
	const prepared = (where: (i: QueryRef<Issue>) => Cond) =>
		prepareQuery(IssueSchema, compileQuery({ where })).where!;

	it('records a rel node whose sub-condition is rooted at the target row', () => {
		const i = rootRef<Issue>();
		const cond = i.assignee.matches((a) => a.name.eq('Maya'));
		expect(cond).toEqual({
			kind: 'rel',
			path: [ 'assignee' ],
			cond: { kind: 'cmp', path: [ 'name' ], op: 'eq', value: 'Maya' },
		});
	});

	it('prepares the sub-condition against the target schema', () => {
		// A bare ref leaf inside the TARGET row still resolves to its key —
		// proof the inner condition went through the same resolution a query
		// over the target would.
		const cond = prepareQuery(IssueSchema, {
			where: {
				kind: 'rel',
				path: [ 'blockedBy' ],
				cond: { kind: 'cmp', path: [ 'project' ], op: 'eq', value: 'p1' },
			},
		}).where;
		expect(cond).toEqual({
			kind: 'rel',
			path: [ 'blockedBy' ],
			cond: { kind: 'cmp', path: [ 'project', 'id' ], op: 'eq', value: 'p1' },
		});
	});

	it('rejects matches on a field that is not a reference', () => {
		// A plain message field: drillable, but its target declares no entity.
		expect(() =>
			prepareQuery(WidgetSchema, {
				where: {
					kind: 'rel',
					path: [ 'settings' ],
					cond: { kind: 'cmp', path: [ 'color' ], op: 'eq', value: 'red' },
				},
			})
		).toThrow(/matches\(\) needs a reference field/);
		// A scalar field.
		expect(() =>
			prepareQuery(UserSchema, {
				where: {
					kind: 'rel',
					path: [ 'name' ],
					cond: { kind: 'cmp', path: [ 'x' ], op: 'eq', value: 1 },
				},
			})
		).toThrow(/matches\(\) needs a reference field/);
	});

	it('rejects matches inside any()', () => {
		const i = rootRef<Issue>();
		const cond = i.blockedBy.any((b) =>
			(b as unknown as MsgRef<Issue>).assignee.matches((a) => (a as MsgRef<User>).name.eq('x'))
		);
		expect(() => prepareQuery(IssueSchema, { where: cond }))
			.toThrow(/matches\(\) cannot nest inside any\(\)/);
	});

	it('rejects matches inside matches', () => {
		const i = rootRef<Issue>();
		const cond = i.blockedBy.matches((b) => b.assignee.matches((a) => a.name.eq('x')));
		expect(() => prepareQuery(IssueSchema, { where: cond }))
			.toThrow(/matches\(\) cannot nest inside matches\(\)/);
	});

	it('rejects an inner condition naming a field the target does not have', () => {
		expect(() =>
			prepareQuery(IssueSchema, {
				where: {
					kind: 'rel',
					path: [ 'assignee' ],
					cond: { kind: 'cmp', path: [ 'title' ], op: 'eq', value: 'x' },
				},
			})
		).toThrow(/has no field "title"/);
	});
});

describe('local queries with matches', () => {
	it('answers a to-one semi-join, skipping unset refs and orphans', async () => {
		const client = newClient();
		await seed(client);
		const rows = await client.db.issue.query({
			where: (i) => i.assignee.matches((a) => a.name.eq('Maya')),
		});
		expect(keysOf(rows)).toEqual([ 'i1' ]);
		// i3 has no assignee and i4's stub is orphaned: neither matches even the
		// always-true target condition.
		const anyUser = await client.db.issue.query({
			where: (i) => i.assignee.matches((a) => a.id.startsWith('')),
		});
		expect(keysOf(anyUser)).toEqual([ 'i1', 'i2' ]);
	});

	it('answers a repeated-ref semi-join when SOME target matches', async () => {
		const client = newClient();
		await seed(client);
		const rows = await client.db.issue.query({
			where: (i) => i.blockedBy.matches((b) => b.state.eq('open')),
		});
		expect(keysOf(rows)).toEqual([ 'i3' ]);
		const none = await client.db.issue.query({
			where: (i) => i.blockedBy.matches((b) => b.state.eq('nope')),
		});
		expect(keysOf(none)).toEqual([]);
	});

	it('compares composite keys as whole tuples', async () => {
		const client = newClient();
		await seed(client);
		const rows = await client.db.issue.query({
			where: (i) => i.pair.matches((p) => p.label.eq('keep')),
		});
		// i3's pair shares the id but not the ns of the matching target.
		expect(keysOf(rows)).toEqual([ 'i1' ]);
	});

	it('composes with row-local conditions and not()', async () => {
		const client = newClient();
		await seed(client);
		const rows = await client.db.issue.query({
			where: (i) =>
				and(
					i.state.eq('open'),
					not(i.assignee.matches((a) => a.name.eq('Maya'))),
				),
		});
		expect(keysOf(rows)).toEqual([ 'i2' ]);
	});

	it('keeps workspaces apart in a cross-workspace read', async () => {
		const client = new SyncClient({
			accountId: 'acct',
			workspaces: [ 'ws-a', 'ws-b' ],
			defaultWorkspace: 'ws-a',
			kvStore: new MemoryKVStore(),
			entities: [ IssueSchema, ProjectSchema, UserSchema, PairSchema ],
			streamEnabled: false,
			pollIntervalMs: 0,
		});
		// The same user id matches in A and not in B; each workspace's issues
		// must answer against their own partition's targets.
		await client.ws('ws-a').mutate.put(UserSchema, { id: 'u1', name: 'Maya' });
		await client.ws('ws-b').mutate.put(UserSchema, { id: 'u1', name: 'Noor' });
		await client.ws('ws-a').mutate.put(IssueSchema, { id: 'i1', assignee: { id: 'u1' } });
		await client.ws('ws-b').mutate.put(IssueSchema, { id: 'i1', assignee: { id: 'u1' } });
		const rows = await client.queryAll(IssueSchema, {
			where: (i) => i.assignee.matches((a) => a.name.eq('Maya')),
		});
		expect(rows.map((r) => r.workspaceId)).toEqual([ 'ws-a' ]);
		const named = await client.queryAll(IssueSchema, {
			where: (i) => i.assignee.matches((a) => a.name.eq('Noor')),
		}, { workspaces: [ 'ws-a', 'ws-b' ] });
		expect(named.map((r) => r.workspaceId)).toEqual([ 'ws-b' ]);
	});

	it('rejects matches in a forward relation $where, which no query backs', async () => {
		const client = newClient();
		await seed(client);
		// A forward relation's options are evaluated in memory over point-read
		// targets, so a semi-join has no pre-pass to ride on there.
		await expect(
			client.db.issue.query({}, {
				include: {
					blockedBy: {
						$where: (b: MsgRef<Issue>) => b.assignee.matches((a) => a.name.eq('Maya')),
					},
				},
			}),
		).rejects.toThrow(/\$where cannot use matches\(\)/);
	});

	it('serves matches in a reverse relation $where through the query path', async () => {
		const client = newClient();
		await seed(client);
		// A reverse relation's $where joins a real query, whose execute pipeline
		// runs the same two-phase rewrite as any other.
		const rows = await client.db.user.query({}, {
			include: {
				issuesByAssignee: {
					$where: (i: MsgRef<Issue>) => i.pair.matches((p) => p.label.eq('keep')),
				},
			},
		});
		const hydrated = rows as unknown as (Row<User> & {
			refs: Record<string, { key: string }[]>;
		})[];
		const u1 = hydrated.find((r) => r.key === 'u1')!;
		const u2 = hydrated.find((r) => r.key === 'u2')!;
		expect(keysOf(u1.refs.issuesByAssignee!)).toEqual([ 'i1' ]);
		expect(keysOf(u2.refs.issuesByAssignee!)).toEqual([]);
	});

	it('rejects matches in a subscription predicate', () => {
		expect(() =>
			new SyncClient({
				workspaceId: 'ws1',
				kvStore: new MemoryKVStore(),
				entities: [ IssueSchema, ProjectSchema, UserSchema, PairSchema ],
				streamEnabled: false,
				pollIntervalMs: 0,
				subscriptions: [
					subscription(IssueSchema, (i) => i.assignee.matches((a) => a.name.eq('Maya'))),
				],
			})
		).toThrow(/subscription predicate .* uses matches\(\)/);
	});
});

describe('coverage of matches queries', () => {
	const newScopedClient = (subs: ReturnType<typeof subscription>[]) =>
		new SyncClient({
			workspaceId: 'ws1',
			kvStore: new MemoryKVStore(),
			entities: [ IssueSchema, ProjectSchema, UserSchema, PairSchema ],
			streamEnabled: false,
			pollIntervalMs: 0,
			subscriptions: subs,
		});

	it('is complete under full sync', async () => {
		const client = newClient();
		await seed(client);
		const rows = await client.db.issue.query({
			where: (i) => i.assignee.matches((a) => a.name.eq('Maya')),
		});
		expect(rows.coverage).toEqual({ complete: true, bounds: [] });
	});

	it('reports the relation bound when the target entity is scoped', async () => {
		const client = newScopedClient([
			subscription(IssueSchema),
			subscription(UserSchema, (u) => u.name.eq('Maya')),
		]);
		await seed(client);
		const rows = await client.db.issue.query({
			where: (i) => i.assignee.matches((a) => a.name.eq('Maya')),
		});
		expect(rows.coverage.complete).toBe(false);
		expect(rows.coverage.bounds).toContain('relation');
	});

	it('trusts the follow closure when every base subscription follows the ref', async () => {
		const client = newScopedClient([
			subscription(IssueSchema, (i) => i.state.eq('open'), { include: { assignee: true } }),
			subscription(UserSchema, (u) => u.name.eq('Maya')),
		]);
		await seed(client);
		const rows = await client.db.issue.query({
			where: (i) => i.assignee.matches((a) => a.name.eq('Maya')),
		});
		// The base entity's own predicate bound stays; the relation bound does
		// not, because every holder's targets ride the follow closure.
		expect(rows.coverage.bounds).toContain('predicate');
		expect(rows.coverage.bounds).not.toContain('relation');
	});

	it('does not trust the follow closure for a different ref path', async () => {
		const client = newScopedClient([
			subscription(IssueSchema, (i) => i.state.eq('open'), { include: { project: true } }),
			subscription(UserSchema, (u) => u.name.eq('Maya')),
		]);
		await seed(client);
		const rows = await client.db.issue.query({
			where: (i) => i.assignee.matches((a) => a.name.eq('Maya')),
		});
		expect(rows.coverage.bounds).toContain('relation');
	});
});

describe('watching matches queries', () => {
	it('re-fires when a TARGET row changes the membership', async () => {
		const client = newClient();
		await seed(client);
		const seen: string[][] = [];
		const handle = client.db.issue.watchQuery(
			{ where: (i) => i.assignee.matches((a) => a.name.eq('Maya')) },
			(rows) => seen.push(keysOf(rows)),
		);
		await flush();
		expect(seen.at(-1)).toEqual([ 'i1' ]);
		// A commit to the USER entity — no issue row moves, but i2's assignee is
		// now named Maya, so the result must.
		await client.db.user.mutate.patch('u2', { name: 'Maya' });
		await flush();
		expect(seen.at(-1)).toEqual([ 'i1', 'i2' ]);
		// And back out.
		await client.db.user.mutate.patch('u2', { name: 'Noor' });
		await flush();
		expect(seen.at(-1)).toEqual([ 'i1' ]);
		handle.close();
	});

	it('suppresses re-emits for target commits that do not move the membership', async () => {
		const client = newClient();
		await seed(client);
		const seen: string[][] = [];
		const handle = client.db.issue.watchQuery(
			{ where: (i) => i.assignee.matches((a) => a.name.eq('Maya')) },
			(rows) => seen.push(keysOf(rows)),
		);
		await flush();
		const emitted = seen.length;
		// u2 stays a non-match: the membership keys are unchanged, so the memo
		// suppresses the callback.
		await client.db.user.mutate.patch('u2', { name: 'Noor N.' });
		await flush();
		expect(seen.length).toBe(emitted);
		handle.close();
	});

	it('still maintains base-row changes incrementally', async () => {
		const client = newClient();
		await seed(client);
		const seen: string[][] = [];
		const handle = client.db.issue.watchQuery(
			{ where: (i) => i.assignee.matches((a) => a.name.eq('Maya')) },
			(rows) => seen.push(keysOf(rows)),
		);
		await flush();
		await client.db.issue.mutate.put({ id: 'i5', assignee: { id: 'u1' } });
		await flush();
		expect(seen.at(-1)).toEqual([ 'i1', 'i5' ]);
		await client.db.issue.mutate.del('i5');
		await flush();
		expect(seen.at(-1)).toEqual([ 'i1' ]);
		handle.close();
	});

	it('runs one pre-pass for watchers crossing the same relation', async () => {
		// Two watchers asking the same thing of the same target entity over the
		// same scope are entitled to one answer per commit, whatever their own
		// queries do with it. Counted as READS of the target entity rather than
		// as scans, so the property survives a change of how a pass is served.
		const passes = async (watchers: number): Promise<number> => {
			const kv = new MemoryKVStore();
			const counting = countingReads(kv, overlayPrefix('user'));
			const client = new SyncClient({
				workspaceId: 'ws1',
				kvStore: counting,
				entities: [ IssueSchema, ProjectSchema, UserSchema, PairSchema ],
				streamEnabled: false,
				pollIntervalMs: 0,
			});
			try {
				await seed(client);
				const handles = Array.from({ length: watchers }, () =>
					client.db.issue.watchQuery(
						{ where: (i) => i.assignee.matches((a) => a.name.eq('Maya')) },
						() => {},
					));
				await flush();
				counting.reads = 0;
				// One commit to the target entity, which is what makes every
				// watcher re-run its pass.
				await client.db.user.mutate.patch('u2', { name: 'Maya' });
				await flush();
				for (const h of handles) h.close();
				return counting.reads;
			} finally {
				await client.close();
			}
		};
		expect(await passes(2)).toBe(await passes(1));
	});

	it('updates the membership from the rows a commit named, not from a pass', async () => {
		const kv = new MemoryKVStore();
		const counting = countingReads(kv, overlayPrefix('user'));
		const client = new SyncClient({
			workspaceId: 'ws1',
			kvStore: counting,
			entities: [ IssueSchema, ProjectSchema, UserSchema, PairSchema ],
			streamEnabled: false,
			pollIntervalMs: 0,
		});
		try {
			await seed(client);
			const seen: string[][] = [];
			const handle = client.db.issue.watchQuery(
				{ where: (i) => i.assignee.matches((a) => a.name.eq('Maya')) },
				(rows) => seen.push(keysOf(rows)),
			);
			await flush();
			counting.scans = 0;
			await client.db.user.mutate.patch('u2', { name: 'Maya' });
			await flush();
			// The answer still moves...
			expect(seen.at(-1)).toEqual([ 'i1', 'i2' ]);
			// ...and it moved without asking the user entity for its rows.
			expect(counting.scans).toBe(0);
			handle.close();
		} finally {
			await client.close();
		}
	});

	it('retires a membership key when its target row is deleted', async () => {
		const client = newClient();
		await seed(client);
		const seen: string[][] = [];
		const handle = client.db.issue.watchQuery(
			{ where: (i) => i.assignee.matches((a) => a.name.eq('Maya')) },
			(rows) => seen.push(keysOf(rows)),
		);
		await flush();
		expect(seen.at(-1)).toEqual([ 'i1' ]);
		// A delete reaches the update as a row that no longer reads back, which
		// has to leave the set the same way a row that stopped matching does —
		// i1's assignee is now an orphan stub.
		await client.db.user.mutate.del('u1');
		await flush();
		expect(seen.at(-1)).toEqual([]);
		handle.close();
	});

	it('keeps a maintained membership equal to a pass under verification', async () => {
		// debugVerifyTouches re-runs the pre-pass after every commit and throws
		// if the maintained membership disagrees, so the assertion here is that
		// this sequence completes at all.
		const client = new SyncClient({
			workspaceId: 'ws1',
			kvStore: new MemoryKVStore(),
			entities: [ IssueSchema, ProjectSchema, UserSchema, PairSchema ],
			streamEnabled: false,
			pollIntervalMs: 0,
			debugVerifyTouches: true,
		});
		try {
			await seed(client);
			const seen: string[][] = [];
			const handle = client.db.issue.watchQuery(
				{ where: (i) => i.assignee.matches((a) => a.name.eq('Maya')) },
				(rows) => seen.push(keysOf(rows)),
			);
			await flush();
			await client.db.user.mutate.patch('u2', { name: 'Maya' });
			await flush();
			await client.db.user.mutate.patch('u1', { name: 'Noor' });
			await flush();
			await client.db.user.mutate.del('u2');
			await flush();
			await client.db.user.mutate.put({ id: 'u3', name: 'Maya' });
			await client.db.issue.mutate.put({ id: 'i6', assignee: { id: 'u3' } });
			await flush();
			expect(seen.at(-1)).toEqual([ 'i6' ]);
			handle.close();
		} finally {
			await client.close();
		}
	});

	it('handles a self-reference, where the base entity is its own target', async () => {
		const client = newClient();
		await seed(client);
		const seen: string[][] = [];
		const handle = client.db.issue.watchQuery(
			{ where: (i) => i.blockedBy.matches((b) => b.state.eq('open')) },
			(rows) => seen.push(keysOf(rows)),
		);
		await flush();
		expect(seen.at(-1)).toEqual([ 'i3' ]);
		// Closing the blocker changes the membership through the issue entity
		// itself.
		await client.db.issue.mutate.patch('i1', { state: 'done' });
		await flush();
		expect(seen.at(-1)).toEqual([]);
		handle.close();
	});
});

// The pre-pass is an ordinary query over the target entity, so a declared index
// covering the inner condition serves it — the difference between reading the
// matches and decoding every target row the store holds. These build the same
// rows twice, with the index built and unbuilt, and hold the two answers
// against each other: the scan is the definition, and an index that answered
// differently would be a bug rather than a faster path.

const account = entity('account', {
	id: text().key(),
	team: text().indexed(),
});
const ticket = entity('ticket', {
	id: text().key(),
	title: text(),
	owner: ref(() => account),
}, {
	// The holder side of the reference. The client derives no index per ref
	// field the way the engine does, so this declaration is what lets a read go
	// from a target key back to the rows holding it — the walk a membership move
	// needs to find the rows it reached.
	indexes: [ { name: 'by_owner', key: [ { path: 'owner' } ] } ],
});
type Ticket = Infer<typeof ticket>;
const relBundle = defineSchema(account, ticket);
const AccountSchema = relBundle.descMessage('account');
const TicketSchema = relBundle.descMessage('ticket');
const relRegistry = new EntityRegistry([ AccountSchema, TicketSchema ]);

const ACCOUNTS = [
	{ id: 'a1', team: 'core' },
	{ id: 'a2', team: 'ops' },
	{ id: 'a3', team: 'core' },
];
const TICKETS = [
	{ id: 't1', title: 'one', owner: { id: 'a1' } },
	{ id: 't2', title: 'two', owner: { id: 'a2' } },
	{ id: 't3', title: 'three', owner: { id: 'a3' } },
	// An orphan and an unset ref: neither contributes a match, whichever path
	// the pre-pass took.
	{ id: 't4', title: 'four', owner: { id: 'gone' } },
	{ id: 't5', title: 'five' },
];

const OWNED_BY_CORE = { where: (t: QueryRef<Ticket>) => t.owner.matches((a) => a.team.eq('core')) };

/** Plants both entities as CONFIRMED state in each workspace — what an index covers. */
async function seedRel(kv: KVStore, indexes: IndexCache, workspaces: string[]): Promise<void> {
	const tx = await kv.write();
	const changes = (
		entityName: string,
		schema: typeof AccountSchema,
		rows: readonly { id: string }[],
	) =>
		rows.map((row, i) =>
			create(RowChangeSchema, {
				entity: entityName,
				key: row.id,
				version: BigInt(i + 1),
				body: { case: 'value', value: toBinary(schema, create(schema, row as never)) },
			})
		);
	for (const ws of workspaces) {
		await tx.put(metaCursor(ws), new Uint8Array([ 1 ]));
		await applyServerChanges(
			tx,
			relRegistry,
			ws,
			[
				...changes('account', AccountSchema, ACCOUNTS),
				...changes('ticket', TicketSchema, TICKETS),
			],
			new TouchSet(),
			indexes,
		);
	}
	await tx.commit();
}

/**
 * A store that tallies how many reads touch a key prefix — `reads` counts scans
 * of it and point reads under it alike, `scans` only the former. Counting reads
 * rather than rows is what answers "how many times was this asked", which is
 * the question when two readers should have shared one answer; separating the
 * scans answers "was it asked of the whole entity".
 */
function countingReads(inner: KVStore, prefix: string): KVStore & { reads: number; scans: number } {
	const under = (k: string) => k.startsWith(prefix);
	const store = {
		reads: 0,
		scans: 0,
		read: async (): Promise<KVRead> => {
			const read = await inner.read();
			async function* counted(p: string): AsyncIterable<[string, unknown]> {
				if (p.startsWith(prefix) || prefix.startsWith(p)) {
					store.reads++;
					store.scans++;
				}
				yield* read.scan(p);
			}
			return {
				get: (k) => {
					if (under(k)) store.reads++;
					return read.get(k);
				},
				getMany: (ks) => {
					if (ks.some(under)) store.reads++;
					return read.getMany(ks);
				},
				has: (k) => read.has(k),
				scan: (p) => counted(p),
				scanRange: (lo, hi) => read.scanRange(lo, hi),
				indexScan: (lo, hi) => read.indexScan(lo, hi),
				indexCount: (lo, hi) => read.indexCount(lo, hi),
				indexSeek: (lo, hi, dir) => read.indexSeek(lo, hi, dir),
				release: () => read.release(),
			};
		},
		write: () => inner.write(),
		close: () => inner.close(),
	};
	return store;
}

/**
 * A store that tallies the confirmed rows of one entity a read walks — the
 * cost the index is there to remove, and the only thing separating the two
 * arms once their answers have been shown to agree.
 */
function countingStore(inner: KVStore, entityName: string): KVStore & { scanned: number } {
	const prefix = confirmedPrefix(entityName);
	const store = {
		scanned: 0,
		read: async (): Promise<KVRead> => {
			const read = await inner.read();
			async function* counted(p: string): AsyncIterable<[string, unknown]> {
				for await (const entry of read.scan(p)) {
					if (p.startsWith(prefix)) store.scanned++;
					yield entry;
				}
			}
			return {
				get: (k) => read.get(k),
				getMany: (ks) => read.getMany(ks),
				has: (k) => read.has(k),
				scan: (p) => counted(p),
				scanRange: (lo, hi) => read.scanRange(lo, hi),
				indexScan: (lo, hi) => read.indexScan(lo, hi),
				indexCount: (lo, hi) => read.indexCount(lo, hi),
				indexSeek: (lo, hi, dir) => read.indexSeek(lo, hi, dir),
				release: () => read.release(),
			};
		},
		write: () => inner.write(),
		close: () => inner.close(),
	};
	return store;
}

async function relKeysWith(
	built: boolean,
	scope: string | string[] | undefined,
): Promise<{ keys: string[]; scanned: number }> {
	const kv = new MemoryKVStore();
	const indexes = new IndexCache(relRegistry);
	await seedRel(kv, indexes, [ 'ws1', 'ws2' ]);
	if (built) {
		const mutex = new ChainMutex();
		for (const ws of [ 'ws1', 'ws2' ]) {
			await buildIndexes(kv, (fn) => mutex.run(fn), relRegistry, indexes, ws);
		}
	}
	const counting = countingStore(kv, 'account');
	const nodes = collectRels(
		TicketSchema,
		prepareQuery(TicketSchema, compileQuery(OWNED_BY_CORE as never)).where,
	);
	const tx = await counting.read();
	try {
		const keys = await computeRelKeys(
			tx,
			relRegistry,
			nodes,
			scope,
			built ? (name) => indexes.for(name) : undefined,
		);
		const flat: string[] = [];
		for (const perWs of keys.values()) {
			for (const [ ws, keyed ] of [ ...perWs ].sort(([ a ], [ b ]) => (a < b ? -1 : 1))) {
				for (const tuple of keyed.values()) flat.push(`${ws}/${tuple.join(',')}`);
			}
		}
		return { keys: flat, scanned: counting.scanned };
	} finally {
		tx.release();
	}
}

describe('the rel pre-pass and the index', () => {
	it('selects the same targets from an index as from the entity', async () => {
		const scanned = await relKeysWith(false, 'ws1');
		const indexed = await relKeysWith(true, 'ws1');
		expect(indexed.keys).toEqual(scanned.keys);
		expect(scanned.keys).toEqual([ 'ws1/a1', 'ws1/a3' ]);
		// Which is the whole point: the target entity is never read.
		expect(scanned.scanned).toBe(ACCOUNTS.length);
		expect(indexed.scanned).toBe(0);
	});

	it('walks one leg per workspace for a named set', async () => {
		const scanned = await relKeysWith(false, [ 'ws1', 'ws2' ]);
		const indexed = await relKeysWith(true, [ 'ws1', 'ws2' ]);
		expect(indexed.keys).toEqual(scanned.keys);
		expect(scanned.keys).toEqual([ 'ws1/a1', 'ws1/a3', 'ws2/a1', 'ws2/a3' ]);
		expect(indexed.scanned).toBe(0);
	});

	it('reads the entity for a scope naming no workspace', async () => {
		// Every workspace the store holds: an index walk covers one, so there is
		// no leg to walk and the read that is the definition serves it.
		const scanned = await relKeysWith(false, undefined);
		const indexed = await relKeysWith(true, undefined);
		expect(indexed.keys).toEqual(scanned.keys);
		expect(scanned.keys).toEqual([ 'ws1/a1', 'ws1/a3', 'ws2/a1', 'ws2/a3' ]);
		expect(indexed.scanned).toBe(scanned.scanned);
	});
});

describe('matches queries over an indexed target', () => {
	const run = async (built: boolean): Promise<string[]> => {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(relRegistry);
		await seedRel(kv, indexes, [ 'ws1' ]);
		if (built) {
			const mutex = new ChainMutex();
			await buildIndexes(kv, (fn) => mutex.run(fn), relRegistry, indexes, 'ws1');
		}
		const client = new SyncClient({
			workspaceId: 'ws1',
			kvStore: kv,
			entities: [ AccountSchema, TicketSchema ],
			streamEnabled: false,
			pollIntervalMs: 0,
		}) as unknown as SyncClient<never>;
		try {
			const rows = await client.query(TicketSchema, OWNED_BY_CORE as never);
			return keysOf(rows as unknown as readonly Row<Ticket>[]);
		} finally {
			await client.close();
		}
	};

	it('answers the same whether or not the target entity is indexed', async () => {
		expect(await run(true)).toEqual(await run(false));
		expect(await run(false)).toEqual([ 't1', 't3' ]);
	});

	it('splices the rows a membership move reached, without re-deriving', async () => {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(relRegistry);
		await seedRel(kv, indexes, [ 'ws1' ]);
		const mutex = new ChainMutex();
		await buildIndexes(kv, (fn) => mutex.run(fn), relRegistry, indexes, 'ws1');
		const counting = countingReads(kv, confirmedPrefix('ticket'));
		const client = new SyncClient({
			workspaceId: 'ws1',
			kvStore: counting,
			entities: [ AccountSchema, TicketSchema ],
			streamEnabled: false,
			pollIntervalMs: 0,
		}) as unknown as SyncClient<never>;
		try {
			const seen: string[][] = [];
			const deltas: string[] = [];
			const handle = client.watchQuery(
				TicketSchema,
				OWNED_BY_CORE as never,
				(rows: readonly { key: string }[], delta: { kind: string }) => {
					seen.push(keysOf(rows));
					deltas.push(delta.kind);
				},
			);
			await flush();
			expect(seen.at(-1)).toEqual([ 't1', 't3' ]);
			counting.scans = 0;
			// a2 joins the team, so t2 joins the result — a row the commit never
			// named, reached only through the reference.
			await client.mutate.put(AccountSchema, { id: 'a2', team: 'core' } as never);
			await flush();
			expect(seen.at(-1)).toEqual([ 't1', 't2', 't3' ]);
			// The two receipts: the row arrived as an edit rather than a new
			// result, and finding it cost no pass over the holder entity.
			expect(deltas.at(-1)).toBe('splice');
			expect(counting.scans).toBe(0);
			// And back out again, which is the half no query finds: the leaver is
			// looked for among the rows already held.
			counting.scans = 0;
			await client.mutate.put(AccountSchema, { id: 'a2', team: 'ops' } as never);
			await flush();
			expect(seen.at(-1)).toEqual([ 't1', 't3' ]);
			expect(deltas.at(-1)).toBe('splice');
			expect(counting.scans).toBe(0);
			handle.close();
		} finally {
			await client.close();
		}
	});

	it('re-derives the result when a negated matches makes the move ambiguous', async () => {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(relRegistry);
		await seedRel(kv, indexes, [ 'ws1' ]);
		const mutex = new ChainMutex();
		await buildIndexes(kv, (fn) => mutex.run(fn), relRegistry, indexes, 'ws1');
		const client = new SyncClient({
			workspaceId: 'ws1',
			kvStore: kv,
			entities: [ AccountSchema, TicketSchema ],
			streamEnabled: false,
			pollIntervalMs: 0,
		}) as unknown as SyncClient<never>;
		try {
			const seen: string[][] = [];
			const handle = client.watchQuery(
				TicketSchema,
				{
					where: (t: QueryRef<Ticket>) => not(t.owner.matches((a) => a.team.eq('core'))),
				} as never,
				(rows: readonly { key: string }[]) => seen.push(keysOf(rows)),
			);
			await flush();
			// The orphan and the unset ref: a matches is false for both, so the
			// negation admits them.
			expect(seen.at(-1)).toEqual([ 't2', 't4', 't5' ]);
			// Under a negation a key LEAVING is what admits a row, which is the
			// implication the reached-rows walk does not follow — so this has to
			// come back through the re-derivation, correct either way.
			await client.mutate.put(AccountSchema, { id: 'a1', team: 'ops' } as never);
			await flush();
			expect(seen.at(-1)).toEqual([ 't1', 't2', 't4', 't5' ]);
			handle.close();
		} finally {
			await client.close();
		}
	});

	it('follows an optimistic edit to a target the index does not cover', async () => {
		const kv = new MemoryKVStore();
		const indexes = new IndexCache(relRegistry);
		await seedRel(kv, indexes, [ 'ws1' ]);
		const mutex = new ChainMutex();
		await buildIndexes(kv, (fn) => mutex.run(fn), relRegistry, indexes, 'ws1');
		const client = new SyncClient({
			workspaceId: 'ws1',
			kvStore: kv,
			entities: [ AccountSchema, TicketSchema ],
			streamEnabled: false,
			pollIntervalMs: 0,
		}) as unknown as SyncClient<never>;
		try {
			// The overlay is not indexed, and the pre-pass has to see it anyway:
			// a2 joining the team makes t2 a match.
			await client.mutate.put(AccountSchema, { id: 'a2', team: 'core' } as never);
			const rows = await client.query(TicketSchema, OWNED_BY_CORE as never);
			expect(keysOf(rows as unknown as readonly Row<Ticket>[])).toEqual([ 't1', 't2', 't3' ]);
		} finally {
			await client.close();
		}
	});
});
