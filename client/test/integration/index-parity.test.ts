// Cross-language parity for index-served queries.
//
// The two implementations index independently — the server maintains its own
// entries in SQLite, the client its own in the KV — from encoders that must
// agree byte for byte. The shared vectors pin the encoding and the planner
// decisions in isolation; this pins the thing those exist FOR: the same query
// against the same rows returns the same answer whichever side serves it, and
// whether or not an index does.
//
// Issue carries every declaration surface — a `state` field mark, the
// `by_state_rank` composite, the `labels` fan-out and the to-many `blocked_by`
// reference the engine indexes on its own — so a handful of queries exercise
// all of them. The fan-out cases are also the wire test for `any()`: the
// condition only means anything if both compilers read the same ExistsCond.

import { describe, expect, it } from 'vitest';

import type { MessageShape } from '@bufbuild/protobuf';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import {
	IssueSchema,
	PairSchema,
	ProjectSchema,
	UserSchema,
	WidgetSchema,
} from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { not, type Query } from '../../src/query/index.js';
import { freshWorkspace, newClient } from './helpers.js';

type IssueQuery = Query<MessageShape<typeof IssueSchema>>;

const SEED = [
	{
		id: 'i1',
		title: 'one',
		state: 'open',
		rank: 5,
		labels: [ 'bug', 'ui' ],
		blockedBy: [ { id: 'i2' } ],
	},
	{ id: 'i2', title: 'two', state: 'open', rank: 9, labels: [ 'ui' ] },
	{ id: 'i3', title: 'three', state: 'done', rank: 3, labels: [ 'bug' ] },
	// No labels at all: still reachable through by_label_rank, which is what
	// the missing tag on an empty list is for.
	{ id: 'i4', title: 'four', state: 'open', rank: 1 },
	// A duplicate element collapses to one entry, so this row comes back once.
	{
		id: 'i5',
		title: 'five',
		state: 'done',
		rank: 7,
		labels: [ 'ui', 'ui' ],
		blockedBy: [ { id: 'i2' } ],
	},
];

describe('secondary index parity between client and server', () => {
	/** Seeds a workspace and returns a synced client over it. */
	async function seeded() {
		const ws = freshWorkspace();
		const client = newClient(ws, {
			entities: [ TodoSchema, WidgetSchema, PairSchema, ProjectSchema, UserSchema, IssueSchema ],
		});
		await client.start();
		for (const row of SEED) await client.mutate.put(IssueSchema, row);
		await client.syncNow();
		return client;
	}

	/** The local answer and the pushed-down one, as row-key lists. */
	async function bothSides(q: IssueQuery): Promise<{ local: string[]; server: string[] }> {
		const client = await seeded();
		try {
			return {
				local: (await client.query(IssueSchema, q)).map((r) => r.key),
				server: (await client.queryServer(IssueSchema, q)).map((r) => r.key),
			};
		} finally {
			await client.close();
		}
	}

	const parity = (name: string, q: IssueQuery, want: string[]) =>
		it(name, async () => {
			const { local, server } = await bothSides(q);
			expect(server).toEqual(local);
			expect(local).toEqual(want);
		});

	parity('equality on a field-marked index', { where: (i) => i.state.eq('open') }, [
		'i1',
		'i2',
		'i4',
	]);

	parity('equality plus the ordering the composite supplies', {
		where: (i) => i.state.eq('open'),
		orderBy: (i) => [ i.rank.desc() ],
	}, [ 'i2', 'i1', 'i4' ]);

	parity('equality, ordering and a limit', {
		where: (i) => i.state.eq('open'),
		orderBy: (i) => [ i.rank.desc() ],
		limit: 2,
	}, [ 'i2', 'i1' ]);

	parity('an inequality bounds the range', {
		where: (i) => i.rank.gte(5),
		orderBy: (i) => [ i.rank.asc() ],
	}, [ 'i1', 'i5', 'i2' ]);

	parity('an ordering with no predicate', {
		orderBy: (i) => [ i.rank.desc() ],
	}, [ 'i2', 'i5', 'i1', 'i3', 'i4' ]);

	parity('membership over a repeated field', {
		where: (i) => i.labels.has('bug'),
	}, [ 'i1', 'i3' ]);

	parity('membership plus the ordering the fan-out index supplies', {
		where: (i) => i.labels.has('ui'),
		orderBy: (i) => [ i.rank.desc() ],
	}, [ 'i2', 'i5', 'i1' ]);

	parity('hasAny returns each matching row once, across several ranges', {
		where: (i) => i.labels.hasAny([ 'bug', 'ui' ]),
		orderBy: (i) => [ i.rank.desc() ],
	}, [ 'i2', 'i5', 'i1', 'i3' ]);

	parity('hasAll re-checks what the range cannot pin', {
		where: (i) => i.labels.hasAll([ 'bug', 'ui' ]),
	}, [ 'i1' ]);

	parity('a row with no elements is still found by isEmpty', {
		where: (i) => i.labels.isEmpty(),
	}, [ 'i4' ]);

	// any() over a to-many reference: the element is the target's stub, so only
	// its key field is addressable. Both sides must read the same ExistsCond off
	// the wire for this to mean anything.
	parity('any() over a to-many reference', {
		where: (i) => i.blockedBy.any((b) => b.id.eq('i2')),
		orderBy: (i) => [ i.rank.asc() ],
	}, [ 'i1', 'i5' ]);

	parity('any() matching no element', {
		where: (i) => i.blockedBy.any((b) => b.id.eq('i9')),
	}, []);

	parity('not(any()) means no element matches', {
		where: (i) => not(i.blockedBy.any((b) => b.id.eq('i2'))),
		orderBy: (i) => [ i.rank.asc() ],
	}, [ 'i4', 'i3', 'i2' ]);

	it('agrees before either side has built an index', async () => {
		// Neither the server's background pass nor the client's has necessarily
		// run yet, so this is the scan-vs-scan baseline the index has to match.
		// It is the same assertion as above by design: that IS the point.
		const { local, server } = await bothSides({
			where: (i) => i.state.eq('done'),
			orderBy: (i) => [ i.rank.desc() ],
		});
		expect(server).toEqual(local);
		expect(local).toEqual([ 'i5', 'i3' ]);
	});
});
