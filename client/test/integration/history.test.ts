// Field history end to end: the server records what a field held before a
// write, the client reads the log back through queryServer, and restoring is an
// ordinary PATCH of the recorded mask.
//
// This tier is the one that can observe any of it. Capture happens on the Go
// write path, the log is a server-only entity no client ever syncs, and the
// restore has to survive a cross-language round trip through the collaborative
// text CRDT — none of which a unit suite against a fake store can see.

import { fromBinary, type MessageShape } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { HistoriedSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { FieldHistorySchema } from '../../src/gen/syncplane/v1/history_pb.js';
import { mergeMask } from '../../src/mutations.js';
import { and, type Query } from '../../src/query/index.js';
import {
	decodeTextDoc,
	encodeTextDoc,
	isPlainString,
	plainTextOf,
	TextDoc,
} from '../../src/text/index.js';
import { freshWorkspace, newClient } from './helpers.js';

type HistoryQuery = Query<MessageShape<typeof FieldHistorySchema>>;

/** The query a history panel runs: one row's log, newest first.
 *
 * Two equality terms plus an ordering on a THIRD field is the shape a
 * single-field index cannot serve at O(limit), which is why FieldHistory
 * declares the composite `by_source` — and why its `seq` component is
 * descending. */
const logOf = (key: string): HistoryQuery => ({
	where: (h) => and(h.sourceEntity.eq('historied'), h.sourceKey.eq(key)),
	orderBy: (h) => [ h.seq.desc() ],
});

describe('field history', () => {
	it("returns one row's log newest first, and never syncs it", async () => {
		const ws = freshWorkspace();
		const client = newClient(ws);
		await client.start();

		await client.mutate.put(HistoriedSchema, { id: 'h1', title: 'one' });
		await client.syncNow();
		await client.mutate.patch(HistoriedSchema, 'h1', { title: 'two' });
		await client.syncNow();
		await client.mutate.patch(HistoriedSchema, 'h1', { title: 'three' });
		await client.syncNow();

		const log = await client.queryServer(FieldHistorySchema, logOf('h1'));
		expect(log.length).toBe(3);
		const seqs = log.map((r) => Number(r.value.seq));
		// A row key renders `seq` as unpadded decimal, so key order is not
		// sequence order — the ordering has to come from the index.
		expect(seqs).toEqual([ ...seqs ].sort((a, b) => b - a));
		expect(log[0]?.value.mask?.paths).toEqual([ 'title' ]);
		expect(fromBinary(HistoriedSchema, log[0]!.value.prior).title).toBe('two');
		// The record for the PUT that created the row carries nothing: there was
		// no before-state to record.
		expect(log[2]?.value.rowCreated).toBe(true);
		expect(log[2]?.value.mask?.paths ?? []).toEqual([]);
		expect(log[0]?.value.rowCreated).toBe(false);
		// A record spans the writes folded into it. This server runs with folding
		// off (see cmd/testserver), so each span is one write — which is also the
		// shape a panel has to render when a session holds exactly one.
		expect(log.map((r) => r.value.writes)).toEqual([ 1, 1, 1 ]);
		expect(log.every((r) => r.value.throughSeq === r.value.seq)).toBe(true);

		// This client names no subscriptions, so it is on FULL SYNC — the case
		// subscriptions could never protect, and therefore the one worth
		// asserting. It holds the source row and nothing else.
		expect((await client.scan(HistoriedSchema)).map((r) => r.key)).toEqual([ 'h1' ]);
		expect(await client.scan(FieldHistorySchema)).toEqual([]);

		await client.close();
	});

	it('restores a scalar field by patching the recorded mask', async () => {
		const ws = freshWorkspace();
		const client = newClient(ws);
		await client.start();

		await client.mutate.put(HistoriedSchema, { id: 'h1', title: 'the good title', note: 'keep me' });
		await client.syncNow();
		await client.mutate.patch(HistoriedSchema, 'h1', { title: 'oops' });
		await client.syncNow();
		expect((await client.get(HistoriedSchema, 'h1'))?.title).toBe('oops');

		// The whole restore: read the log, decode the prior value as the source
		// entity, PATCH the mask it recorded.
		const log = await client.queryServer(FieldHistorySchema, logOf('h1'));
		const rec = log[0]!.value;
		const prior = fromBinary(HistoriedSchema, rec.prior);
		await client.mutate.patch(HistoriedSchema, 'h1', prior, { paths: rec.mask!.paths });
		await client.syncNow();

		const row = await client.get(HistoriedSchema, 'h1');
		expect(row?.title).toBe('the good title');
		// A field the mask never named is untouched. That is what makes restore
		// a PATCH rather than a PUT of the decoded prior, which carries only the
		// recorded fields and would blank everything else.
		expect(row?.note).toBe('keep me');

		// The restore is an ordinary optimistic mutation, so it is itself
		// captured — undoing a restore is just another restore.
		const after = await client.queryServer(FieldHistorySchema, logOf('h1'));
		expect(after.length).toBe(log.length + 1);
		expect(fromBinary(HistoriedSchema, after[0]!.value.prior).title).toBe('oops');

		await client.close();
	});

	it('restores a collaborative-text field without losing a concurrent edit', async () => {
		// The restore is the ordinary one — decode the prior, PATCH it back under
		// the recorded mask — with nothing said about the text field. The prior
		// carries a plain-string value there, which the enqueue path resolves by
		// diffing it against the stored document and applying the difference as
		// real edits; that is also what lets a peer's concurrent typing survive.
		//
		// The server records the text rather than the document precisely so that
		// this is the only shape available. Document bytes handed back — even
		// under a PATCH — would merge with the stored document, where the delete
		// markers win, and the restore would change nothing at all.
		const ws = freshWorkspace();
		const author = newClient(ws);
		const peer = newClient(ws);
		await author.start();
		await peer.start();

		const original = new TextDoc('author');
		original.insert(0, 'chapter one');
		await author.mutate.put(HistoriedSchema, { id: 'h1', body: encodeTextDoc(original) });
		await author.syncNow();
		await peer.syncNow();

		// The mistake: the whole body is wiped.
		const wrecked = decodeTextDoc((await author.get(HistoriedSchema, 'h1'))?.body, 'author');
		wrecked.setString('');
		await author.mutate.patch(HistoriedSchema, 'h1', { body: encodeTextDoc(wrecked) });
		await author.syncNow();
		expect(plainTextOf((await author.get(HistoriedSchema, 'h1'))?.body)).toBe('');

		const log = await author.queryServer(FieldHistorySchema, logOf('h1'));
		const rec = log.find((r) => r.value.mask?.paths.includes('body'))!.value;
		const prior = fromBinary(HistoriedSchema, rec.prior);
		// A legal value of the field, tagged so a reader can tell the two forms
		// apart — which is what lets the restore below say nothing about it.
		expect(isPlainString(prior.body)).toBe(true);
		expect(plainTextOf(prior.body)).toBe('chapter one');

		// A peer types into the same document, unaware, between the read and the
		// restore. Its edit has to survive.
		await peer.syncNow();
		const peerDoc = decodeTextDoc((await peer.get(HistoriedSchema, 'h1'))?.body, 'peer');
		peerDoc.insert(peerDoc.length, '(draft)');
		await peer.mutate.patch(HistoriedSchema, 'h1', { body: encodeTextDoc(peerDoc) });
		await peer.syncNow();

		await author.mutate.patch(HistoriedSchema, 'h1', prior, { paths: rec.mask!.paths });
		await author.syncNow();
		await peer.syncNow();

		const restored = plainTextOf((await peer.get(HistoriedSchema, 'h1'))?.body);
		// Back exactly once — a PUT would have left the old and the new document
		// unioned into one, and the raw bytes would have restored nothing.
		expect(restored).toContain('chapter one');
		expect(restored.match(/chapter one/g)?.length).toBe(1);
		// And the peer's concurrent edit is still there.
		expect(restored).toContain('(draft)');

		await author.close();
		await peer.close();
	});

	it('records changes, not writes', async () => {
		// The ordinary local-first shape: `put` sends the whole row every time,
		// so most writes re-send fields nobody touched. The server diffs the row
		// it holds against the one arriving and records only what moved — a
		// write that moves nothing lands no record at all.
		const ws = freshWorkspace();
		const client = newClient(ws);
		await client.start();

		await client.mutate.put(HistoriedSchema, { id: 'h1', title: 'one', note: 'n' });
		await client.syncNow();
		await client.mutate.put(HistoriedSchema, { id: 'h1', title: 'one', note: 'n' });
		await client.mutate.put(HistoriedSchema, { id: 'h1', title: 'one', note: 'n' });
		await client.syncNow();

		expect(await client.queryServer(FieldHistorySchema, logOf('h1'))).toHaveLength(1);

		// `note` carries no history mark and `body` never moved, so a real edit
		// to `title` still names `title` alone.
		await client.mutate.put(HistoriedSchema, { id: 'h1', title: 'two', note: 'n2' });
		await client.syncNow();

		const log = await client.queryServer(FieldHistorySchema, logOf('h1'));
		expect(log).toHaveLength(2);
		expect(log[0]?.value.mask?.paths).toEqual([ 'title' ]);

		await client.close();
	});

	it('folds the log back over the live row into a before/after timeline', async () => {
		// A record says what a field held BEFORE its write; what it changed TO is
		// the next newer record, or the live row. This is the fold the how-to
		// documents, and the reason `mergeMask` is exported.
		const ws = freshWorkspace();
		const client = newClient(ws);
		await client.start();

		await client.mutate.put(HistoriedSchema, { id: 'h1', title: 'one' });
		await client.syncNow();
		await client.mutate.patch(HistoriedSchema, 'h1', { title: 'two' });
		await client.syncNow();
		await client.mutate.patch(HistoriedSchema, 'h1', { title: 'three' });
		await client.syncNow();

		const log = await client.queryServer(FieldHistorySchema, logOf('h1'));
		let after = (await client.get(HistoriedSchema, 'h1'))!;
		const timeline = log.map(({ value: rec }) => {
			const before = fromBinary(HistoriedSchema, rec.prior);
			const paths = rec.mask?.paths ?? [];
			const entry = { seq: rec.seq, paths, before, after };
			after = { ...after };
			mergeMask(HistoriedSchema, after, before, paths);
			return entry;
		});

		expect(timeline.map((e) => [ e.before.title, e.after.title ])).toEqual([
			[ 'two', 'three' ],
			[ 'one', 'two' ],
			// The creation record: no mask, so the fold steps nothing back and its
			// "before" is the zero value rather than a recorded one.
			[ '', 'one' ],
		]);
		expect(timeline.at(-1)?.paths).toEqual([]);
		expect(log.at(-1)?.value.rowCreated).toBe(true);

		await client.close();
	});
});
