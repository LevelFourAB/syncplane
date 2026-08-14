// Full-text search across the wire: a match predicate and a relevance
// ordering must return the same rows in the same order from the local
// evaluator and from queryServer, and a relevance cursor minted by either
// side must bound the other. The rank tuple itself is pinned by the shared
// vectors (testdata/search); this tier proves the two implementations agree
// through a real server.

import { describe, expect, it } from 'vitest';

import type { MessageShape } from '@bufbuild/protobuf';
import { ArticleSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import type { Query } from '../../src/query/index.js';
import { freshWorkspace, newClient } from './helpers.js';

type ArticleQuery = Query<MessageShape<typeof ArticleSchema>>;

describe('full-text search push-down', () => {
	const milk: ArticleQuery = {
		where: (a) => a.match('milk'),
		orderBy: (a) => [ a.relevance() ],
	};

	async function seeded() {
		const ws = freshWorkspace();
		const writer = newClient(ws);
		await writer.start();
		// Best match first by attribute: title beats summary beats meta.author.
		await writer.mutate.put(ArticleSchema, { id: 'a1', title: 'Milk cow' });
		await writer.mutate.put(ArticleSchema, { id: 'a2', summary: 'milk' });
		await writer.mutate.put(ArticleSchema, { id: 'a3', title: 'water' });
		await writer.mutate.put(ArticleSchema, { id: 'a4', meta: { author: 'milk' } });
		await writer.syncNow();
		return { ws, writer };
	}

	it('matches and ranks identically locally and on the server', async () => {
		const { writer } = await seeded();

		const local = await writer.query(ArticleSchema, milk);
		expect(local.map((r) => r.key)).toEqual([ 'a1', 'a2', 'a4' ]);

		const remote = await writer.queryServer(ArticleSchema, milk);
		expect(remote.map((r) => r.key)).toEqual(local.map((r) => r.key));

		await writer.close();
	});

	it('exchanges relevance cursors between the two sides', async () => {
		const { ws, writer } = await seeded();
		const reader = newClient(ws);

		const localHead = await writer.query(ArticleSchema, { ...milk, limit: 2 });
		expect(localHead.map((r) => r.key)).toEqual([ 'a1', 'a2' ]);

		// A relevance cursor the CLIENT minted, replayed against the server.
		const fromServer = await reader.queryServer(ArticleSchema, {
			...milk,
			after: localHead.endCursor,
		});
		expect(fromServer.map((r) => r.key)).toEqual([ 'a4' ]);

		// And the SERVER's cursor replayed against the local evaluator.
		const serverHead = await reader.queryServer(ArticleSchema, { ...milk, limit: 2 });
		expect(serverHead.map((r) => r.key)).toEqual([ 'a1', 'a2' ]);
		expect(serverHead.hasMore).toBe(true);
		const fromLocal = await writer.query(ArticleSchema, {
			...milk,
			after: serverHead.endCursor,
		});
		expect(fromLocal.map((r) => r.key)).toEqual([ 'a4' ]);

		await writer.close();
		await reader.close();
	});

	it('typeahead prefixes and analyzer folding agree across the wire', async () => {
		const { writer } = await seeded();
		const q: ArticleQuery = { where: (a) => a.match('MIL') };

		const local = await writer.query(ArticleSchema, q);
		const remote = await writer.queryServer(ArticleSchema, q);
		expect(local.map((r) => r.key)).toEqual([ 'a1', 'a2', 'a4' ]);
		expect(remote.map((r) => r.key)).toEqual(local.map((r) => r.key));

		await writer.close();
	});
});
