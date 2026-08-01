import type { MessageShape } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import type { Query } from '../../src/query/index.js';
import { freshWorkspace, newClient } from './helpers.js';

type WidgetQuery = Query<MessageShape<typeof WidgetSchema>>;

// A oneof is one field on both sides of the wire: the client masks the GROUP
// and the engine merges the group. These are the cross-language half — the unit
// suites pin each implementation on its own, and only a real server can show
// that the mask the client sends is the one the engine accepts.
describe('oneofs (sync)', () => {
	it('patching the group replaces the case and leaves other fields alone', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();
		await b.start();

		await a.mutate.put(WidgetSchema, {
			id: 'w1',
			label: 'keep me',
			choice: { case: 'choiceText', value: 'first' },
		});
		await a.syncNow();
		await b.syncNow();

		await b.mutate.patch(WidgetSchema, 'w1', { choice: { case: 'choiceNumber', value: 42 } });
		await b.syncNow();
		await a.syncNow();

		for (const client of [ a, b ]) {
			const w = (await client.get(WidgetSchema, 'w1'))!;
			expect(w.choice).toEqual({ case: 'choiceNumber', value: 42 });
			expect(w.label).toBe('keep me');
		}

		await a.close();
		await b.close();
	});

	// The group is one field, so it takes part in per-field LWW like any other:
	// a writer who never touched it cannot clobber it.
	it('a concurrent write to another field leaves the group alone', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();
		await b.start();

		await a.mutate.put(WidgetSchema, { id: 'w1', label: 'base' });
		await a.syncNow();
		await b.syncNow();

		await a.mutate.patch(WidgetSchema, 'w1', { choice: { case: 'choiceText', value: 'mine' } });
		await b.mutate.patch(WidgetSchema, 'w1', { label: 'theirs' });

		await a.syncNow();
		await b.syncNow();
		await a.syncNow();

		for (const client of [ a, b ]) {
			const w = (await client.get(WidgetSchema, 'w1'))!;
			expect(w.choice).toEqual({ case: 'choiceText', value: 'mine' });
			expect(w.label).toBe('theirs');
		}

		await a.close();
		await b.close();
	});

	// Two writers on the same group: it resolves as ONE field, so the row ends
	// up holding one writer's case whole and both clients agree on which — the
	// loser rebases onto it instead of keeping a case the server dropped.
	//
	// Which one wins is push-arrival order, which this test does not control (a
	// client's coordinator may push before the explicit syncNow), so the
	// assertion is the invariant rather than the winner.
	it('concurrent writes to the group converge on one whole case', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();
		await b.start();

		await a.mutate.put(WidgetSchema, { id: 'w1' });
		await a.syncNow();
		await b.syncNow();

		await a.mutate.patch(WidgetSchema, 'w1', { choice: { case: 'choiceText', value: 'from-a' } });
		await b.mutate.patch(WidgetSchema, 'w1', { choice: { case: 'choiceNumber', value: 9 } });

		await a.syncNow();
		await b.syncNow();
		await a.syncNow();

		await b.syncNow();

		const fromA = (await a.get(WidgetSchema, 'w1'))!.choice;
		const fromB = (await b.get(WidgetSchema, 'w1'))!.choice;
		expect(fromA).toEqual(fromB);
		expect([
			{ case: 'choiceText', value: 'from-a' },
			{ case: 'choiceNumber', value: 9 },
		]).toContainEqual(fromA);

		await a.close();
		await b.close();
	});

	it('patching the group with no case clears it', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		await a.start();

		await a.mutate.put(WidgetSchema, {
			id: 'w1',
			label: 'keep me',
			choice: { case: 'choiceText', value: 'gone soon' },
		});
		await a.syncNow();
		await a.mutate.patch(WidgetSchema, 'w1', { choice: { case: undefined } });
		await a.syncNow();

		const w = (await a.get(WidgetSchema, 'w1'))!;
		expect(w.choice.case).toBeUndefined();
		expect(w.label).toBe('keep me');

		await a.close();
	});

	// The read side is member-granular, and the two evaluators must agree on
	// which rows that selects — the local one reads a `{ case, value }` property
	// while the engine reads the member through protoreflect.
	it('filters on a member identically on both sides', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws);
		await writer.start();

		await writer.mutate.put(WidgetSchema, {
			id: 'w1',
			choice: { case: 'choiceText', value: 'hit' },
		});
		await writer.mutate.put(WidgetSchema, {
			id: 'w2',
			choice: { case: 'choiceText', value: 'miss' },
		});
		await writer.mutate.put(WidgetSchema, { id: 'w3', choice: { case: 'choiceNumber', value: 1 } });
		await writer.mutate.put(WidgetSchema, { id: 'w4' });
		await writer.syncNow();

		const eq: WidgetQuery = { where: (w) => w.choice.choiceText.eq('hit') };
		expect((await writer.queryServer(WidgetSchema, eq)).map((r) => r.key)).toEqual([ 'w1' ]);
		expect((await writer.query(WidgetSchema, eq)).map((r) => r.key)).toEqual([ 'w1' ]);

		// isSet on a member IS the case test: only the rows whose active case is
		// that member, whatever value it holds.
		const active: WidgetQuery = { where: (w) => w.choice.choiceText.isSet() };
		expect((await writer.queryServer(WidgetSchema, active)).map((r) => r.key))
			.toEqual([ 'w1', 'w2' ]);
		expect((await writer.query(WidgetSchema, active)).map((r) => r.key)).toEqual([ 'w1', 'w2' ]);

		await writer.close();
	});
});
