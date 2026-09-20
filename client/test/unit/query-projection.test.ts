// Candidate rows decoded through a projection — the descriptor holding only
// the fields a query can observe — while the page it returns is decoded whole.
//
// The specs pin the two halves of that trade. The answer must not move: rows,
// total, hasMore and facets come out the same whether the projection was used
// or not, which the `limit`-less twin of each query is the control for (a query
// returning every match is decoded whole either way). And the rows handed back
// must be complete: every field outside the projection is there, a movable list
// is ordered, and a collaborative-text field carries the local edits folded in.

import { create, type DescMessage, fromBinary, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import type { RowSet } from '../../src/coverage.js';
import { type Widget, WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { RowChangeSchema } from '../../src/gen/syncplane/v1/patch_pb.js';
import { metaCursor } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { KVStore } from '../../src/kv/store.js';
import { EntityRegistry } from '../../src/mutations.js';
import { ChainMutex } from '../../src/mutex.js';
import { buildIndexes } from '../../src/optimistic/index-build.js';
import { IndexCache } from '../../src/optimistic/index-maintain.js';
import { applyServerChanges } from '../../src/optimistic/rebase.js';
import type { CompiledQuery, Cond } from '../../src/query/ast.js';
import {
	and,
	compileGroupQuery,
	compileGroupRowsQuery,
	compileQuery,
	type FacetTable,
	type Group,
	type GroupQuery,
	type GroupRowsQuery,
	type Query,
	type RowGroup,
	type StringOps,
} from '../../src/query/index.js';
import { projectionFor, projectionForGroup } from '../../src/query/projection.js';
import { prepareGroupQuery, prepareGroupRowsQuery, prepareQuery } from '../../src/query/wire.js';
import {
	defineSchema,
	entity,
	type Infer,
	int32,
	list,
	message,
	text,
} from '../../src/schema/builder.js';
import { decodeTextDoc, encodeTextDoc, plainTextOf, TextDoc } from '../../src/text/index.js';
import { TouchSet } from '../../src/touch.js';

const WS = 'ws-a';

// An entity whose bulk (`body`, `notes`) is outside every query below, so a
// page row still carrying them is evidence the page was decoded whole.
const task = entity('task', {
	id: text().key(),
	state: text().indexed(),
	rank: int32(),
	body: text(),
	notes: list(text()),
	labels: list(text()),
	meta: message({ region: text(), size: int32() }),
}, {
	indexes: [ { name: 'by_state_rank', key: [ { path: 'state' }, { path: 'rank', desc: true } ] } ],
});

type Task = Infer<typeof task>;
const TaskSchema = defineSchema(task).descMessage('task');
const taskRegistry = new EntityRegistry([ TaskSchema ]);

const BODY = 'x'.repeat(400);

const taskRow = (
	id: string,
	state: string,
	rank: number,
	region: string,
	labels: string[],
): Task => ({
	id,
	state,
	rank,
	body: `${BODY}/${id}`,
	notes: [ `note-${id}`, 'shared' ],
	labels,
	meta: { region, size: rank * 2 },
});

const TASKS: Task[] = [
	taskRow('t1', 'open', 9, 'eu', [ 'ui' ]),
	taskRow('t2', 'open', 7, 'eu', [ 'ui', 'bug' ]),
	taskRow('t3', 'open', 5, 'us', [ 'bug' ]),
	taskRow('t4', 'open', 3, 'eu', []),
	taskRow('t5', 'done', 1, 'eu', [ 'ui' ]),
];

async function seed(
	kv: KVStore,
	registry: EntityRegistry,
	indexes: IndexCache,
	entityName: string,
	rows: { key: string; value: Uint8Array }[],
): Promise<void> {
	const tx = await kv.write();
	await tx.put(metaCursor(WS), new Uint8Array([ 1 ]));
	await applyServerChanges(
		tx,
		registry,
		WS,
		rows.map((r, i) =>
			create(RowChangeSchema, {
				entity: entityName,
				key: r.key,
				version: BigInt(i + 1),
				body: { case: 'value', value: r.value },
			})
		),
		new TouchSet(),
		indexes,
	);
	await tx.commit();
}

/**
 * A client over `rows`. With `built`, the entity's indexes are built first, so
 * the read is served by the index walk rather than by the scan — the two paths
 * the projection lives on.
 */
async function clientWith(
	registry: EntityRegistry,
	schemas: DescMessage[],
	entityName: string,
	rows: { key: string; value: Uint8Array }[],
	built: boolean,
): Promise<SyncClient<never>> {
	const kv = new MemoryKVStore();
	const indexes = new IndexCache(registry);
	await seed(kv, registry, indexes, entityName, rows);
	if (built) {
		const mutex = new ChainMutex();
		await buildIndexes(kv, (fn) => mutex.run(fn), registry, indexes, WS);
	}
	return new SyncClient({
		accountId: 'acct',
		workspaces: [ WS ],
		defaultWorkspace: WS,
		kvStore: kv,
		entities: schemas,
		streamEnabled: false,
		pollIntervalMs: 0,
		debugVerifyIndexReads: true,
	}) as unknown as SyncClient<never>;
}

const taskRows = TASKS.map((t) => ({
	key: t.id,
	value: toBinary(TaskSchema, create(TaskSchema, t)),
}));

const taskClient = (built: boolean): Promise<SyncClient<never>> =>
	clientWith(taskRegistry, [ TaskSchema ], 'task', taskRows, built);

type Faceted = RowSet<Task, number | undefined, FacetTable>;

const readTasks = (client: SyncClient<never>, q: Query<Task>): Promise<Faceted> =>
	client.query(TaskSchema, q as never) as unknown as Promise<Faceted>;

/** Runs a read on both the index arm and the scan arm and asserts they agree. */
async function onBothArms(
	fn: (client: SyncClient<never>) => Promise<Faceted>,
): Promise<[Faceted, Faceted]> {
	const scanned = await taskClient(false);
	const indexed = await taskClient(true);
	try {
		const s = await fn(scanned);
		const i = await fn(indexed);
		expect(i.map((r) => r.key)).toEqual(s.map((r) => r.key));
		expect(i.total).toEqual(s.total);
		expect(i.facets).toEqual(s.facets);
		return [ s, i ];
	} finally {
		await scanned.close();
		await indexed.close();
	}
}

describe('a query decoded through a projection', () => {
	// state is the indexed prefix, meta.region the residual the index cannot
	// express, and labels a repeated path only the facet fold reads — so the
	// projection has to enumerate all three kinds of path.
	const faceted = (limit?: number): Query<Task> => ({
		where: (t) => and(t.state.eq('open'), t.meta.region.eq('eu')),
		orderBy: (t) => [ t.rank.desc() ],
		...(limit === undefined ? {} : { limit }),
		facets: { region: (t) => t.meta.region, label: (t) => t.labels },
	});

	it('answers as the whole-row decode does', async () => {
		const [ paged ] = await onBothArms((c) => readTasks(c, faceted(2)));
		// The control: no limit means every match is returned, which is decoded
		// whole on both paths.
		const [ whole ] = await onBothArms((c) => readTasks(c, faceted()));

		expect(paged.map((r) => r.key)).toEqual([ 't1', 't2' ]);
		expect(paged.total).toBe(3);
		expect(paged.hasMore).toBe(true);
		expect(whole.map((r) => r.key)).toEqual([ 't1', 't2', 't4' ]);
		expect(whole.total).toBe(3);
		expect(whole.hasMore).toBe(false);
		expect(paged.facets).toEqual(whole.facets);
		expect(paged.facets).toEqual({
			region: [ { value: 'eu', count: 3, workspaceId: WS } ],
			label: [
				{ value: 'bug', count: 1, workspaceId: WS },
				{ value: 'ui', count: 2, workspaceId: WS },
				{ value: undefined, count: 1, workspaceId: WS },
			],
		});
	});

	it('returns page rows carrying the fields no part of the query reads', async () => {
		const [ scanned, indexed ] = await onBothArms((c) => readTasks(c, faceted(2)));
		for (const rows of [ scanned, indexed ]) {
			expect(rows.map((r) => r.value.body)).toEqual([ `${BODY}/t1`, `${BODY}/t2` ]);
			expect(rows.map((r) => r.value.notes)).toEqual([
				[ 'note-t1', 'shared' ],
				[ 'note-t2', 'shared' ],
			]);
			expect(rows.map((r) => r.value.meta?.size)).toEqual([ 18, 14 ]);
		}
	});

	it('keeps a query the index cannot serve on the same answer', async () => {
		const unindexed: Query<Task> = {
			where: (t) => t.body.contains('/t3'),
			limit: 5,
			facets: { region: (t) => t.meta.region },
		};
		const [ rows ] = await onBothArms((c) => readTasks(c, unindexed));
		expect(rows.map((r) => r.key)).toEqual([ 't3' ]);
		expect(rows[0]!.value.notes).toEqual([ 'note-t3', 'shared' ]);
		expect(rows.facets).toEqual({ region: [ { value: 'us', count: 1, workspaceId: WS } ] });
	});
});

describe('a grouped read decoded through a projection', () => {
	/** Runs a grouped read on both arms and asserts they agree. */
	async function onBothGroupArms<T>(fn: (client: SyncClient<never>) => Promise<T>): Promise<T> {
		const scanned = await taskClient(false);
		const indexed = await taskClient(true);
		try {
			const s = await fn(scanned);
			expect(await fn(indexed)).toEqual(s);
			return s;
		} finally {
			await scanned.close();
			await indexed.close();
		}
	}

	const counts = (groups: Group[]) => groups.map((g) => [ g.value, g.count ] as const);

	it('counts by a path no index partitions', async () => {
		// No index leads with `meta.region`, so both arms bucket a scan — the
		// projection's only reader there, since a grouped count hands back no row.
		const q: GroupQuery<Task> = {
			by: (t) => t.meta.region,
			where: (t) => t.state.eq('open'),
		};
		const groups = await onBothGroupArms((c) => c.groupCount(TaskSchema, q as never));
		expect(counts(groups)).toEqual([ [ 'eu', 3 ], [ 'us', 1 ] ]);
	});

	it('counts under a residual the index range cannot express', async () => {
		const q: GroupQuery<Task> = {
			by: (t) => t.state,
			where: (t) => t.meta.region.eq('eu'),
		};
		const groups = await onBothGroupArms((c) => c.groupCount(TaskSchema, q as never));
		expect(counts(groups)).toEqual([ [ 'done', 1 ], [ 'open', 3 ] ]);
	});

	it('names the distinct values of a repeated path', async () => {
		const values = await onBothGroupArms((c) =>
			c.distinct(TaskSchema, { by: (t: Task) => t.labels } as never)
		);
		expect(values).toEqual([ 'bug', 'ui', undefined ]);
	});

	it('returns each group head whole', async () => {
		const q: GroupRowsQuery<Task> = {
			by: (t) => t.state,
			where: (t) => t.meta.region.eq('eu'),
			orderBy: (t) => [ t.rank.desc() ],
			perGroup: 2,
		};
		const groups = await onBothGroupArms((c) =>
			c.groupRows(TaskSchema, q as never)
		) as unknown as RowGroup<Task>[];
		expect(groups.map((g) => [ g.value, g.rows.map((r) => r.key), g.total, g.hasMore ])).toEqual([
			[ 'done', [ 't5' ], 1, false ],
			[ 'open', [ 't1', 't2' ], 3, true ],
		]);
		// The head rows are what the app receives, so nothing the projection
		// dropped may be missing from them.
		const open = groups.find((g) => g.value === 'open')!;
		expect(open.rows.map((r) => r.value.body)).toEqual([ `${BODY}/t1`, `${BODY}/t2` ]);
		expect(open.rows.map((r) => r.value.notes)).toEqual([
			[ 'note-t1', 'shared' ],
			[ 'note-t2', 'shared' ],
		]);
		expect(open.rows.map((r) => r.value.meta?.size)).toEqual([ 18, 14 ]);
	});
});

describe('the projection a compiled grouped query gets', () => {
	const ofGroup = (q: GroupQuery<Task>) =>
		projectionForGroup(
			TaskSchema,
			prepareGroupQuery(TaskSchema, compileGroupQuery(q as never, false)),
		);
	const ofGroupRows = (q: GroupRowsQuery<Task>) =>
		projectionForGroup(
			TaskSchema,
			prepareGroupRowsQuery(TaskSchema, compileGroupRowsQuery(q as never)),
		);
	const fields = (projected: ReturnType<typeof ofGroup>) =>
		projected?.fields.map((f) => f.localName).sort();

	it('holds the grouping path and the predicate paths', () => {
		expect(fields(ofGroup({ by: (t) => t.meta.region, where: (t) => t.state.eq('open') })))
			.toEqual([ 'meta', 'state' ]);
	});

	it('holds the ordering paths of a grouped row read', () => {
		expect(fields(ofGroupRows({
			by: (t) => t.state,
			where: (t) => t.meta.region.eq('eu'),
			orderBy: (t) => [ t.rank.desc() ],
			perGroup: 2,
		}))).toEqual([ 'meta', 'rank', 'state' ]);
	});

	it('projects a read that returns every group', () => {
		// The rule a row query declines on: no limit means every match comes back
		// and is decoded whole anyway. A grouped count hands back no row at all,
		// so the projection pays whatever the limit is.
		expect(ofGroup({ by: (t) => t.state })).toBeDefined();
	});

	it('declines a grouping path naming no field of the entity', () => {
		expect(projectionForGroup(TaskSchema, { by: [ 'nope' ], distinct: false })).toBeUndefined();
	});

	it('declines a predicate whose paths cannot be enumerated', () => {
		expect(projectionForGroup(TaskSchema, {
			by: [ 'state' ],
			distinct: false,
			where: { kind: 'match', path: [], query: 'x' },
		})).toBeUndefined();
	});
});

// Widget is the hand-written entity carrying the two field kinds a projection
// has to be careful with: `items` is a movable list (read order is by `pos`,
// not by stored order) and `body` is collaborative text (its value is the
// stored document with the local edit log folded in).
const widgetRegistry = new EntityRegistry([ WidgetSchema ]);

const doc = (s: string): Uint8Array => {
	const d = new TextDoc('seed');
	d.insert(0, s);
	return encodeTextDoc(d);
};

const widget = (id: string, label: string, s: string, items: [string, string][]): Uint8Array =>
	toBinary(
		WidgetSchema,
		create(WidgetSchema, {
			id,
			label,
			body: doc(s),
			items: items.map(([ itemId, pos ]) => ({ id: itemId, pos, text: itemId })),
		}),
	);

const readWidgets = (
	client: SyncClient<never>,
	q: Query<Widget>,
): Promise<RowSet<Widget, number | undefined, FacetTable>> =>
	client.query(WidgetSchema, q as never) as unknown as Promise<
		RowSet<Widget, number | undefined, FacetTable>
	>;

const widgetRows = [
	// Stored out of `pos` order, so a row read in stored order would fail.
	{ key: 'w1', value: widget('w1', 'first', 'hello world', [ [ 'b', 'a2' ], [ 'a', 'a1' ] ]) },
	{ key: 'w2', value: widget('w2', 'second', 'goodbye', [ [ 'c', 'a1' ] ]) },
];

describe('a projection over folded and ordered fields', () => {
	it('reads a collaborative-text field as its visible text and returns the document', async () => {
		const client = await clientWith(
			widgetRegistry,
			[ WidgetSchema ],
			'widget',
			widgetRows,
			false,
		);
		try {
			// A local edit lives in the text log beside the row, so the value the
			// predicate sees is the fold rather than the stored document.
			const edited = decodeTextDoc(fromBinary(WidgetSchema, widgetRows[0]!.value).body, 'peer');
			edited.insert(5, ' brave');
			await client.mutate.patch(WidgetSchema, 'w1', { body: encodeTextDoc(edited) } as never);

			const q: Query<Widget> = {
				// A collaborative-text field surfaces as bytes in the generated
				// type; the evaluator compares its visible text.
				where: (w) => (w.body as unknown as StringOps).contains('brave'),
				limit: 5,
				facets: { label: (w) => w.label },
			};
			const rows = await readWidgets(client, q);
			expect(rows.map((r) => r.key)).toEqual([ 'w1' ]);
			expect(plainTextOf(rows[0]!.value.body)).toBe('hello brave world');
			expect(rows.facets).toEqual({ label: [ { value: 'first', count: 1, workspaceId: WS } ] });
		} finally {
			await client.close();
		}
	});

	it('returns a movable list in its read order', async () => {
		const client = await clientWith(
			widgetRegistry,
			[ WidgetSchema ],
			'widget',
			widgetRows,
			false,
		);
		try {
			const q: Query<Widget> = {
				where: (w) => w.label.eq('first'),
				limit: 5,
				facets: { label: (w) => w.label },
			};
			const rows = await readWidgets(client, q);
			expect(rows.map((r) => r.value.items.map((i) => i.id))).toEqual([ [ 'a', 'b' ] ]);
		} finally {
			await client.close();
		}
	});
});

describe('the projection a compiled query gets', () => {
	const projectionOf = (q: Query<Task>) =>
		projectionFor(TaskSchema, prepareQuery(TaskSchema, compileQuery(q as never)));

	it('holds the paths the query reads and nothing else', () => {
		const projected = projectionOf({
			where: (t) => and(t.state.eq('open'), t.meta.region.eq('eu')),
			orderBy: (t) => [ t.rank.desc() ],
			limit: 2,
			facets: { label: (t) => t.labels },
		});
		expect(projected?.fields.map((f) => f.localName).sort()).toEqual([
			'labels',
			'meta',
			'rank',
			'state',
		]);
	});

	it('is the same descriptor for the same field set', () => {
		const a = projectionOf({ where: (t) => t.state.eq('open'), limit: 2 });
		const b = projectionOf({ where: (t) => t.state.eq('done'), limit: 9 });
		expect(a).toBeDefined();
		expect(a).toBe(b);
		// A different field set is a different descriptor.
		expect(projectionOf({ where: (t) => t.rank.gt(1), limit: 2 })).not.toBe(a);
	});

	it('declines a query that returns every match', () => {
		expect(projectionOf({ where: (t) => t.state.eq('open') })).toBeUndefined();
	});

	it('declines a query whose paths cannot be enumerated', () => {
		// A full-text match reads the entity's declared search fields, and a
		// reference hop reads another entity's rows — neither is a path on this
		// row that the AST names.
		const compiled = (where: Cond): CompiledQuery => ({ where, limit: 2 });
		expect(projectionFor(TaskSchema, compiled({ kind: 'match', path: [], query: 'x' })))
			.toBeUndefined();
		expect(projectionFor(
			TaskSchema,
			compiled({
				kind: 'rel',
				path: [ 'meta' ],
				cond: { kind: 'present', path: [ 'id' ], op: 'isSet' },
			}),
		)).toBeUndefined();
	});

	it('declines a path naming no field of the entity', () => {
		expect(projectionFor(TaskSchema, {
			where: { kind: 'present', path: [ 'nope' ], op: 'isSet' },
			limit: 2,
		})).toBeUndefined();
	});
});
