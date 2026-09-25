import { describe, expect, expectTypeOf, it } from 'vitest';

import type { Message } from '@bufbuild/protobuf';
import { SyncClient } from '../../src/client.js';
import type { ForwardRefAccessor, ReverseRelAccessor } from '../../src/db.js';
import {
	type Issue,
	IssueSchema,
	PairSchema,
	type Project,
	ProjectSchema,
	UserSchema,
} from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { KVRead, KVStore } from '../../src/kv/store.js';
import { EntityRegistry } from '../../src/mutations.js';
import type { Row } from '../../src/read.js';
import type { HydratedRow } from '../../src/refs.js';
import {
	defineSchema,
	entity,
	type EntitySchema,
	type Field,
	type Infer,
	ref,
	refList,
	text,
} from '../../src/schema/builder.js';

function newClient() {
	return new SyncClient({
		workspaceId: 'ws1',
		kvStore: new MemoryKVStore(),
		entities: [ IssueSchema, ProjectSchema, UserSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
	});
}

/** Registers Pair too, so issue.pair is a ref and pair gains a reverse relation. */
function newClientWithPair() {
	return new SyncClient({
		workspaceId: 'ws1',
		kvStore: new MemoryKVStore(),
		entities: [ IssueSchema, ProjectSchema, UserSchema, PairSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
	});
}

const flush = () => new Promise((r) => setTimeout(r, 0));

describe('reference graph naming', () => {
	const registry = new EntityRegistry([ IssueSchema, ProjectSchema, UserSchema ]);

	it('exposes forward refs by their field localName', () => {
		expect(registry.forwardRefsOf('issue').map((r) => r.localName).sort()).toEqual([
			'assignee',
			'blockedBy',
			'project',
			'reporter',
		]);
		// pair → Pair is not registered here, so it is omitted from the graph.
	});

	it('names a reverse relation after its source entity and field', () => {
		expect(registry.reverseRefsOf('project').map((r) => r.name)).toEqual([ 'issuesByProject' ]);
		expect(registry.reverseRefsOf('issue').map((r) => r.name)).toEqual([ 'issuesByBlockedBy' ]); // self-ref
		expect(registry.reverseRefsOf('user').map((r) => r.name).sort()).toEqual([
			'issuesByAssignee',
			'issuesByReporter',
		]);
	});

	// engine/server/reversenames_test.go pins the same rule and wording for the
	// engine's mirror of the graph.
	describe('in a runtime schema', () => {
		const user = entity('user', { id: text().key() });
		const namesOn = (...schemas: EntitySchema<any>[]) =>
			new EntityRegistry(defineSchema(user, ...schemas).descMessages())
				.reverseRefsOf('user').map((r) => r.name).sort();

		it('spells a snake_case entity and field in camelCase', () => {
			const taskList = entity('task_list', { id: text().key(), createdBy: ref(() => user) });
			expect(namesOn(taskList)).toEqual([ 'taskListsByCreatedBy' ]);
		});

		// The rule exists for this: a name is a wire contract that deployed
		// clients follow by, so a reference added later must not rename the
		// ones before it.
		it('keeps the name of an existing reference when another joins it', () => {
			const post = entity('post', {
				id: text().key(),
				author: ref(() => user),
				editor: ref(() => user),
			});
			expect(namesOn(post)).toEqual([ 'postsByAuthor', 'postsByEditor' ]);
		});

		it('uses the name a field sets', () => {
			const post = entity('post', {
				id: text().key(),
				author: ref(() => user),
				editor: ref(() => user).reverseRelation('editedPosts'),
			});
			expect(namesOn(post)).toEqual([ 'editedPosts', 'postsByAuthor' ]);
		});

		it('throws on two references of one entity that set one name on one target', () => {
			const post = entity('post', {
				id: text().key(),
				author: ref(() => user).reverseRelation('posts'),
				editor: ref(() => user).reverseRelation('posts'),
			});
			expect(() => namesOn(post)).toThrow(
				'reverse relation "posts" on entity "user" is named by both post.author and post.editor; '
					+ 'set (syncplane.v1.field_behavior).reverse_relation on one of them to give it another name',
			);
		});

		it("throws on a name that another entity's reference already takes on the target", () => {
			const post = entity('post', { id: text().key(), author: ref(() => user) });
			const comment = entity('comment', {
				id: text().key(),
				author: ref(() => user).reverseRelation('postsByAuthor'),
			});
			expect(() => namesOn(post, comment)).toThrow(
				'reverse relation "postsByAuthor" on entity "user" is named by both comment.author and post.author',
			);
		});
	});

	it('throws when a ref field of the same name would shadow a reverse relation', () => {
		// One namespace per entity: `post.comments` is a forward ref list, and the
		// reverse of `comment.post` is named "comments" on post as well. Every
		// resolver here and on the server tries forward first, so letting it build
		// would leave the reverse half unreachable and answer `comments` with a
		// different row set. The engine's buildRefGraph rejects the same schema
		// with the same wording.
		type PostFields = {
			id: Field<string>;
			comments: Field<Infer<EntitySchema<CommentFields>>[]>;
		};
		type CommentFields = {
			id: Field<string>;
			post: Field<Infer<EntitySchema<PostFields>>>;
		};
		const post: EntitySchema<PostFields> = entity('post', {
			id: text().key(),
			comments: refList(() => comment),
		});
		const comment: EntitySchema<CommentFields> = entity('comment', {
			id: text().key(),
			post: ref(() => post).reverseRelation('comments'),
		});
		expect(() => new EntityRegistry(defineSchema(post, comment).descMessages())).toThrow(
			'reverse relation "comments" on entity "post" collides with the reference field of that name; '
				+ 'set (syncplane.v1.field_behavior).reverse_relation on comment.post to name it otherwise',
		);
	});
});

describe('forward reference typing', () => {
	it('resolves a to-one ref to the target type without a cast', async () => {
		const client = newClient();
		await client.db.project.mutate.put({ id: 'p1', name: 'Apollo' });
		await client.db.issue.mutate.put({ id: 'i1', title: 'a', project: { id: 'p1' } });

		const i1 = (await client.db.issue.get('i1'))!;
		const resolved = await client.db.issue.refs.project.load(i1);
		expectTypeOf(resolved).toEqualTypeOf<Row<Project> | undefined>();
		// The point of the typing: reading a target field needs no cast.
		expect(resolved?.value.name).toBe('Apollo');
		await client.close();
	});

	it('resolves a to-many ref to a list of the target type', async () => {
		const client = newClient();
		await client.db.issue.mutate.put({ id: 'i1', title: 'blocker' });
		await client.db.issue.mutate.put({
			id: 'i2',
			title: 'blocked',
			blockedBy: [ { id: 'i1' } ],
		});

		const i2 = (await client.db.issue.get('i2'))!;
		const blockers = await client.db.issue.refs.blockedBy.load(i2);
		expectTypeOf(blockers).toEqualTypeOf<Row<Issue>[]>();
		expect(blockers.map((r) => r.value.title)).toEqual([ 'blocker' ]);
		await client.close();
	});

	it('names what went wrong for an accessor that does not exist', async () => {
		const client = newClient();
		// `pair` is a message field of Issue, so it looks like a forward ref to
		// the type system, but Pair is not registered here — the registry has no
		// accessor for it and the failure has to say so.
		expect(() => client.db.issue.refs.pair.load).toThrow(
			/not a reference field or reverse relation/,
		);
		expect(() => (client.db.issue.refs as Record<string, unknown>).nope).toThrow(/Known: /);
		await client.close();
	});
});

describe('forward reference resolution', () => {
	it('resolves a to-one ref to the live target, orphans to undefined', async () => {
		const client = newClient();
		await client.db.project.mutate.put({ id: 'p1', name: 'Apollo' });
		await client.db.issue.mutate.put({ id: 'i1', title: 'a', project: { id: 'p1' } });
		await client.db.issue.mutate.put({ id: 'i2', title: 'b', project: { id: 'missing' } });

		const projectRef = client.db.issue.refs.project as ForwardRefAccessor;
		const i1 = (await client.db.issue.get('i1'))!;
		const resolved = await projectRef.load(i1 as Message);
		expect(
			resolved && !Array.isArray(resolved) && (resolved.value as unknown as { name: string }).name,
		)
			.toBe('Apollo');

		const i2 = (await client.db.issue.get('i2'))!;
		expect(await projectRef.load(i2 as Message)).toBeUndefined();
		await client.close();
	});

	it('resolves a to-many self-ref, dropping orphans', async () => {
		const client = newClient();
		await client.db.issue.mutate.put({ id: 'a', title: 'A' });
		await client.db.issue.mutate.put({ id: 'b', title: 'B' });
		await client.db.issue.mutate.put({
			id: 'i1',
			title: 'blocked',
			blockedBy: [ { id: 'a' }, { id: 'gone' }, { id: 'b' } ],
		});
		const i1 = (await client.db.issue.get('i1'))!;
		const resolved = await (client.db.issue.refs.blockedBy as ForwardRefAccessor).load(i1 as Message);
		expect(Array.isArray(resolved)).toBe(true);
		expect((resolved as Row<Message>[]).map((r) => r.key).sort()).toEqual([ 'a', 'b' ]);
		await client.close();
	});
});

describe('reverse reference resolution', () => {
	it('returns the source rows pointing at a target', async () => {
		const client = newClient();
		await client.db.project.mutate.put({ id: 'p1', name: 'Apollo' });
		await client.db.issue.mutate.put({ id: 'i1', title: 'a', project: { id: 'p1' } });
		await client.db.issue.mutate.put({ id: 'i2', title: 'b', project: { id: 'p1' } });
		await client.db.issue.mutate.put({ id: 'i3', title: 'c', project: { id: 'other' } });

		const p1 = (await client.db.project.get('p1'))!;
		const issues = await (client.db.project.refs.issuesByProject as ReverseRelAccessor)(
			p1 as Message,
		);
		expect(issues.map((r) => r.key).sort()).toEqual([ 'i1', 'i2' ]);
		await client.close();
	});

	it('resolves a reverse relation that runs through a to-many reference', async () => {
		const client = newClient();
		await client.db.issue.mutate.put({ id: 'a', title: 'A' });
		await client.db.issue.mutate.put({ id: 'i1', title: 'one', blockedBy: [ { id: 'a' } ] });
		await client.db.issue.mutate.put({
			id: 'i2',
			title: 'two',
			blockedBy: [ { id: 'gone' }, { id: 'a' } ],
		});
		await client.db.issue.mutate.put({ id: 'i3', title: 'three', blockedBy: [ { id: 'i1' } ] });

		// "issues blocked by a" — the reverse of the repeated self-ref blocked_by.
		const a = (await client.db.issue.get('a'))!;
		const blocking = await (client.db.issue.refs.issuesByBlockedBy as ReverseRelAccessor)(
			a as Message,
		);
		expect(blocking.map((r) => r.key).sort()).toEqual([ 'i1', 'i2' ]);
		await client.close();
	});

	it('resolves a reverse relation onto a composite-key target', async () => {
		const client = newClientWithPair();
		await client.db.pair.mutate.put({ ns: 'n1', id: 'p1', label: 'P' });
		await client.db.issue.mutate.put({ id: 'i1', title: 'a', pair: { ns: 'n1', id: 'p1' } });
		// Same id under a different namespace must NOT match: the whole key tuple
		// is compared, not one field of it.
		await client.db.issue.mutate.put({ id: 'i2', title: 'b', pair: { ns: 'n2', id: 'p1' } });
		await client.db.issue.mutate.put({ id: 'i3', title: 'c' });

		const p1 = (await client.db.pair.get('n1/p1'))!;
		const issues = await (client.db.pair.refs.issuesByPair as ReverseRelAccessor)(p1 as Message);
		expect(issues.map((r) => r.key)).toEqual([ 'i1' ]);
		await client.close();
	});

	it('applies limit and offset to the rows that matched the relation', async () => {
		const client = newClient();
		await client.db.issue.mutate.put({ id: 'a', title: 'A' });
		for (const id of [ 'i1', 'i2', 'i3' ]) {
			await client.db.issue.mutate.put({ id, title: id, blockedBy: [ { id: 'a' } ] });
		}
		await client.db.issue.mutate.put({ id: 'z9', title: 'unrelated' });

		const a = (await client.db.issue.get('a'))!;
		const page = await (client.db.issue.refs.issuesByBlockedBy as ReverseRelAccessor)(a as Message, {
			orderBy: (i: { id: { asc(): unknown } }) => [ i.id.asc() ],
			limit: 2,
			offset: 1,
		} as never);
		expect(page.map((r) => r.key)).toEqual([ 'i2', 'i3' ]);
		await client.close();
	});

	it('resolves a disambiguated reverse relation', async () => {
		const client = newClient();
		await client.db.user.mutate.put({ id: 'u1', name: 'Ada' });
		await client.db.issue.mutate.put({ id: 'i1', title: 'a', assignee: { id: 'u1' } });
		await client.db.issue.mutate.put({ id: 'i2', title: 'b', reporter: { id: 'u1' } });

		const u1 = (await client.db.user.get('u1'))!;
		const assigned = await (client.db.user.refs.issuesByAssignee as ReverseRelAccessor)(
			u1 as Message,
		);
		const reported = await (client.db.user.refs.issuesByReporter as ReverseRelAccessor)(
			u1 as Message,
		);
		expect(assigned.map((r) => r.key)).toEqual([ 'i1' ]);
		expect(reported.map((r) => r.key)).toEqual([ 'i2' ]);
		await client.close();
	});
});

describe('include hydration', () => {
	it('attaches resolved refs under row.refs', async () => {
		const client = newClient();
		await client.db.project.mutate.put({ id: 'p1', name: 'Apollo' });
		await client.db.issue.mutate.put({ id: 'i1', title: 'a', project: { id: 'p1' } });
		await client.db.issue.mutate.put({ id: 'i2', title: 'b', project: { id: 'gone' } });

		const rows = await client.db.issue.query(
			{ orderBy: (i) => [ i.id.asc() ] },
			{ include: { project: true } },
		) as unknown as (Row<Message> & { refs: Record<string, Row<Message> | undefined> })[];
		expect((rows[0]!.refs.project?.value as { name: string } | undefined)?.name).toBe('Apollo');
		expect(rows[1]!.refs.project).toBeUndefined(); // orphan tolerated
		await client.close();
	});
});

describe('watch include reactivity', () => {
	it('re-emits when an included target row changes', async () => {
		const client = newClient();
		await client.db.project.mutate.put({ id: 'p1', name: 'A' });
		await client.db.issue.mutate.put({ id: 'i1', title: 't', project: { id: 'p1' } });

		const names: (string | undefined)[] = [];
		const unsub = client.db.issue.watchQuery(
			{},
			(rows) => {
				const r = rows[0] as
					| (Row<Message> & { refs: Record<string, Row<Message> | undefined> })
					| undefined;
				names.push((r?.refs.project?.value as { name: string } | undefined)?.name);
			},
			{ include: { project: true } },
		);
		await flush();
		expect(names.at(-1)).toBe('A');

		await client.db.project.mutate.put({ id: 'p1', name: 'B' });
		await flush();
		expect(names.at(-1)).toBe('B');
		unsub.close();
		await client.close();
	});

	it('does not re-hydrate when only its own rows moved, and they did not', async () => {
		// An include watcher has to over-fire on a TARGET change — a changed
		// project is invisible in the issue rows it holds. A change to its own
		// entity is not invisible, so the maintained result settles it, and a
		// commit that leaves the result where it was costs no hydration at all.
		const client = newClient();
		await client.db.project.mutate.put({ id: 'p1', name: 'A' });
		await client.db.issue.mutate.put({ id: 'i1', title: 't', project: { id: 'p1' } });

		let emits = 0;
		const unsub = client.db.issue.watchQuery(
			{ where: (i) => i.title.eq('t') },
			() => emits++,
			{ include: { project: true } },
		);
		await flush();
		const initial = emits;

		// Its own entity, but a row the query rejects: the result cannot move.
		await client.db.issue.mutate.put({ id: 'i2', title: 'other', project: { id: 'p1' } });
		await flush();
		expect(emits).toBe(initial);

		// A matching row entering does move it.
		await client.db.issue.mutate.put({ id: 'i3', title: 't', project: { id: 'p1' } });
		await flush();
		expect(emits).toBe(initial + 1);

		// And a target change still re-hydrates, invisible in the base rows.
		await client.db.project.mutate.put({ id: 'p1', name: 'B' });
		await flush();
		expect(emits).toBe(initial + 2);
		unsub.close();
		await client.close();
	});

	it('hydrates a keyed watch, and re-delivers its keys when a target moves', async () => {
		// A keyed watch has the same blind spot as a query watch: the changed
		// project is nowhere in the issue rows it holds, so the keys have to be
		// re-delivered on a target change even though none of them moved.
		const client = newClient();
		await client.db.project.mutate.put({ id: 'p1', name: 'A' });
		await client.db.issue.mutate.put({ id: 'i1', title: 't', project: { id: 'p1' } });
		await client.db.issue.mutate.put({ id: 'i2', title: 'u', project: { id: 'p1' } });

		const seen: (string | undefined)[][] = [];
		const handle = client.ws('ws1').watchRows(
			IssueSchema,
			[ 'i1', 'i2' ],
			(rows) => {
				seen.push([ ...rows.values() ].map((row) => {
					const refs = (row as
						| (Row<Message> & {
							refs: Record<string, Row<Message> | undefined>;
						})
						| undefined)?.refs;
					return (refs?.project?.value as { name: string } | undefined)?.name;
				}));
			},
			{ include: { project: true } },
		);
		await flush();
		expect(seen.at(-1)).toEqual([ 'A', 'A' ]);

		await client.db.project.mutate.put({ id: 'p1', name: 'B' });
		await flush();
		expect(seen.at(-1)).toEqual([ 'B', 'B' ]);

		// Any project change reaches the watcher — which project a row points at is
		// not something the touch set says — so this over-fires, and must still be
		// right about what it hands back.
		await client.db.project.mutate.put({ id: 'p2', name: 'C' });
		await flush();
		expect(seen.at(-1)).toEqual([ 'B', 'B' ]);
		handle.close();
		await client.close();
	});
});

describe('batched include hydration', () => {
	/** A client that counts one-shot local reads via the query sync event. */
	function countingClient() {
		let queries = 0;
		const client = new SyncClient({
			workspaceId: 'ws1',
			kvStore: new MemoryKVStore(),
			entities: [ IssueSchema, ProjectSchema, UserSchema ],
			streamEnabled: false,
			pollIntervalMs: 0,
			onSyncEvent: (event) => {
				if (event.type === 'query' && event.origin === 'query') queries++;
			},
		});
		return { client, queries: () => queries };
	}

	it('resolves a reverse include with one query for the whole page', async () => {
		const { client, queries } = countingClient();
		await client.db.project.mutate.put({ id: 'p1', name: 'A' });
		await client.db.project.mutate.put({ id: 'p2', name: 'B' });
		await client.db.issue.mutate.put({ id: 'i1', title: 'a', project: { id: 'p1' } });
		await client.db.issue.mutate.put({ id: 'i2', title: 'b', project: { id: 'p1' } });
		await client.db.issue.mutate.put({ id: 'i3', title: 'c', project: { id: 'p2' } });

		const before = queries();
		const rows = await client.db.project.query(
			{ orderBy: (p) => [ p.id.asc() ] },
			{ include: { issuesByProject: true } },
		) as unknown as HydratedRow[];
		// One read for the page, ONE for the whole reverse relation — not one
		// per row.
		expect(queries() - before).toBe(2);
		expect((rows[0]!.refs.issuesByProject as HydratedRow[]).map((r) => r.key)).toEqual([
			'i1',
			'i2',
		]);
		expect((rows[1]!.refs.issuesByProject as HydratedRow[]).map((r) => r.key)).toEqual([ 'i3' ]);
		await client.close();
	});

	it('batches a reverse relation through a to-many ref into one scan', async () => {
		const { client, queries } = countingClient();
		await client.db.issue.mutate.put({ id: 'a', title: 'A' });
		await client.db.issue.mutate.put({ id: 'b', title: 'B' });
		await client.db.issue.mutate.put({ id: 'i1', title: 'one', blockedBy: [ { id: 'a' } ] });
		await client.db.issue.mutate.put({
			id: 'i2',
			title: 'two',
			blockedBy: [ { id: 'a' }, { id: 'b' } ],
		});

		const before = queries();
		const rows = await client.db.issue.query(
			{ where: (i) => i.id.in([ 'a', 'b' ]), orderBy: (i) => [ i.id.asc() ] },
			{ include: { issuesByBlockedBy: true } },
		) as unknown as HydratedRow[];
		expect(queries() - before).toBe(2);
		expect((rows[0]!.refs.issuesByBlockedBy as HydratedRow[]).map((r) => r.key)).toEqual([
			'i1',
			'i2',
		]);
		expect((rows[1]!.refs.issuesByBlockedBy as HydratedRow[]).map((r) => r.key)).toEqual([ 'i2' ]);
		await client.close();
	});

	it('hands rows sharing a target the same hydrated object, nested refs included', async () => {
		const { client, queries } = countingClient();
		await client.db.project.mutate.put({ id: 'p1', name: 'A' });
		await client.db.issue.mutate.put({ id: 'i1', title: 'a', project: { id: 'p1' } });
		await client.db.issue.mutate.put({ id: 'i2', title: 'b', project: { id: 'p1' } });

		const before = queries();
		const rows = await client.db.issue.query(
			{ orderBy: (i) => [ i.id.asc() ] },
			{ include: { project: { issuesByProject: true } } },
		) as unknown as HydratedRow[];
		// Page read + one reverse read for the DISTINCT projects; the forward
		// hop is point reads.
		expect(queries() - before).toBe(2);
		expect(rows[0]!.refs.project).toBe(rows[1]!.refs.project);
		const project = rows[0]!.refs.project as HydratedRow;
		expect((project.refs.issuesByProject as HydratedRow[]).map((r) => r.key)).toEqual([ 'i1', 'i2' ]);
		await client.close();
	});

	/** Seeds `rows` issues, each with its own project, assignee and reporter. */
	async function seedFan(client: ReturnType<typeof newClient>, rows: number): Promise<void> {
		for (let i = 0; i < rows; i++) {
			await client.db.project.mutate.put({ id: `p${i}`, name: `P${i}` });
			await client.db.user.mutate.put({ id: `u${i}`, name: `U${i}` });
			await client.db.user.mutate.put({ id: `r${i}`, name: `R${i}` });
			await client.db.issue.mutate.put({
				id: `i${i}`,
				title: 't',
				project: { id: `p${i}` },
				assignee: { id: `u${i}` },
				reporter: { id: `r${i}` },
			});
		}
	}

	/** Read transactions one hydrated query opens over a page of `rows` issues. */
	async function readsFor(
		rows: number,
		include: Record<string, true>,
	): Promise<number> {
		const counting = countingReads(new MemoryKVStore());
		const client = new SyncClient({
			workspaceId: 'ws1',
			kvStore: counting,
			entities: [ IssueSchema, ProjectSchema, UserSchema ],
			streamEnabled: false,
			pollIntervalMs: 0,
		});
		try {
			await seedFan(client, rows);
			counting.reads = 0;
			const hydrated = await client.db.issue.query(
				{ orderBy: (i) => [ i.id.asc() ] },
				{ include },
			) as unknown as HydratedRow[];
			expect(hydrated).toHaveLength(rows);
			expect(hydrated.every((r) => r.refs.project !== undefined)).toBe(true);
			return counting.reads;
		} finally {
			await client.close();
		}
	}

	it('costs the same reads for a forward include however wide the page is', async () => {
		// The whole point of the batch: the transaction count follows the include
		// tree, not the row count. Three references over twelve rows is thirty-six
		// stubs, and they cost what three stubs do.
		const wide = await readsFor(12, { project: true, assignee: true, reporter: true });
		const narrow = await readsFor(3, { project: true, assignee: true, reporter: true });
		expect(wide).toBe(narrow);
		// One for the page and one per relation, with nothing else in between.
		expect(wide).toBeLessThanOrEqual(4);
	});

	it('reads a target once when two relations of one level share it', async () => {
		const reads = async (include: Record<string, true>): Promise<number> => {
			const counting = countingReads(new MemoryKVStore());
			const client = new SyncClient({
				workspaceId: 'ws1',
				kvStore: counting,
				entities: [ IssueSchema, ProjectSchema, UserSchema ],
				streamEnabled: false,
				pollIntervalMs: 0,
			});
			try {
				for (let i = 0; i < 6; i++) {
					await client.db.user.mutate.put({ id: `u${i}`, name: `U${i}` });
					await client.db.issue.mutate.put({
						id: `i${i}`,
						title: 't',
						assignee: { id: `u${i}` },
						reporter: { id: `u${i}` },
					});
				}
				counting.reads = 0;
				await client.db.issue.query({}, { include });
				return counting.reads;
			} finally {
				await client.close();
			}
		};
		// Every reporter is its row's assignee, so the second relation adds keys
		// the first already resolved and must add no read.
		expect(await reads({ assignee: true, reporter: true })).toBe(await reads({ assignee: true }));
	});
});

/**
 * A store that tallies the read transactions opened against it — the cost
 * batching is there to remove, and the one an in-memory store still charges
 * for at the same rate IndexedDB does.
 */
function countingReads(inner: KVStore): KVStore & { reads: number } {
	const store = {
		reads: 0,
		read: (): Promise<KVRead> => {
			store.reads++;
			return inner.read();
		},
		write: () => inner.write(),
		close: () => inner.close(),
	};
	return store;
}

describe('include relation options', () => {
	it('narrows, orders and pages a reverse relation', async () => {
		const client = newClient();
		await client.db.project.mutate.put({ id: 'p1', name: 'A' });
		await client.db.issue.mutate.put({ id: 'i1', title: 'x', project: { id: 'p1' } });
		await client.db.issue.mutate.put({ id: 'i2', title: 'y', project: { id: 'p1' } });
		await client.db.issue.mutate.put({ id: 'i3', title: 'x', project: { id: 'p1' } });

		const rows = await client.db.project.query({}, {
			include: {
				issuesByProject: {
					$where: (i) => i.title.eq('x'),
					$orderBy: (i) => [ i.id.desc() ],
					$limit: 1,
				},
			},
		}) as unknown as HydratedRow[];
		expect((rows[0]!.refs.issuesByProject as HydratedRow[]).map((r) => r.key)).toEqual([ 'i3' ]);

		const offset = await client.db.project.query({}, {
			include: {
				issuesByProject: { $orderBy: (i) => [ i.id.desc() ], $limit: 2, $offset: 1 },
			},
		}) as unknown as HydratedRow[];
		expect((offset[0]!.refs.issuesByProject as HydratedRow[]).map((r) => r.key)).toEqual([
			'i2',
			'i1',
		]);
		await client.close();
	});

	it('pages each holder independently', async () => {
		const client = newClient();
		await client.db.project.mutate.put({ id: 'p1', name: 'A' });
		await client.db.project.mutate.put({ id: 'p2', name: 'B' });
		await client.db.issue.mutate.put({ id: 'i1', title: 'a', project: { id: 'p1' } });
		await client.db.issue.mutate.put({ id: 'i2', title: 'b', project: { id: 'p1' } });
		await client.db.issue.mutate.put({ id: 'i3', title: 'c', project: { id: 'p2' } });

		const rows = await client.db.project.query(
			{ orderBy: (p) => [ p.id.asc() ] },
			{ include: { issuesByProject: { $orderBy: (i) => [ i.id.asc() ], $limit: 1 } } },
		) as unknown as HydratedRow[];
		// The limit cuts EACH project's list, not the batched read behind them.
		expect((rows[0]!.refs.issuesByProject as HydratedRow[]).map((r) => r.key)).toEqual([ 'i1' ]);
		expect((rows[1]!.refs.issuesByProject as HydratedRow[]).map((r) => r.key)).toEqual([ 'i3' ]);
		await client.close();
	});

	it('applies options to a to-many forward ref', async () => {
		const client = newClient();
		await client.db.issue.mutate.put({ id: 'a', title: 'keep' });
		await client.db.issue.mutate.put({ id: 'b', title: 'drop' });
		await client.db.issue.mutate.put({ id: 'c', title: 'keep' });
		await client.db.issue.mutate.put({
			id: 'i1',
			title: 'holder',
			blockedBy: [ { id: 'c' }, { id: 'b' }, { id: 'a' } ],
		});

		const rows = await client.db.issue.query(
			{ where: (i) => i.id.eq('i1') },
			{
				include: {
					blockedBy: { $where: (i) => i.title.eq('keep'), $orderBy: (i) => [ i.id.asc() ] },
				},
			},
		) as unknown as HydratedRow[];
		expect((rows[0]!.refs.blockedBy as HydratedRow[]).map((r) => r.key)).toEqual([ 'a', 'c' ]);
		await client.close();
	});

	it('rejects options on a to-one ref', async () => {
		const client = newClient();
		await client.db.issue.mutate.put({ id: 'i1', title: 'a', project: { id: 'p1' } });
		await expect(
			client.db.issue.query({}, { include: { project: { $limit: 1 } } }),
		).rejects.toThrow(/list relation/);
		await client.close();
	});

	it('rejects an option at the tree root and an unknown option', async () => {
		const client = newClient();
		await client.db.issue.mutate.put({ id: 'i1', title: 'a' });
		await expect(
			client.db.issue.query({}, { include: { $limit: 1 } }),
		).rejects.toThrow(/tree root/);
		await expect(
			client.db.issue.query({}, { include: { blockedBy: { $take: 1 } as never } }),
		).rejects.toThrow(/unknown option/);
		await client.close();
	});

	it('keeps a watched include narrowed on re-hydration', async () => {
		const client = newClient();
		await client.db.project.mutate.put({ id: 'p1', name: 'A' });
		await client.db.issue.mutate.put({ id: 'i1', title: 'x', project: { id: 'p1' } });

		const seen: string[][] = [];
		const unsub = client.db.project.watchQuery(
			{},
			(rows) => {
				const refs = (rows[0] as unknown as HydratedRow | undefined)?.refs;
				seen.push(((refs?.issuesByProject as HydratedRow[] | undefined) ?? []).map((r) => r.key));
			},
			{ include: { issuesByProject: { $where: (i) => i.title.eq('x') } } },
		);
		await new Promise((r) => setTimeout(r, 0));
		expect(seen.at(-1)).toEqual([ 'i1' ]);

		// A source change re-hydrates; the filter still applies to the new set.
		await client.db.issue.mutate.put({ id: 'i2', title: 'y', project: { id: 'p1' } });
		await client.db.issue.mutate.put({ id: 'i3', title: 'x', project: { id: 'p1' } });
		await new Promise((r) => setTimeout(r, 0));
		expect(seen.at(-1)).toEqual([ 'i1', 'i3' ]);
		unsub.close();
		await client.close();
	});
});
