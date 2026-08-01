import { create, fromBinary } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import {
	IssueSchema,
	PairSchema,
	type Widget,
	WidgetSchema,
} from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import {
	buildPatch,
	buildPut,
	entityDefOf,
	expandCollectionMask,
	isRef,
	keyOf,
	maskFromDiff,
	maskFromInit,
	projectRefs,
} from '../../src/mutations.js';

// A row's identity lives in both its storage key and its embedded key fields,
// and the two must agree. buildPatch refuses to name a key field in the mask
// (the server rejects it too) so a patch can never drift identity.
describe('buildPatch key-field guard', () => {
	const todo = entityDefOf(TodoSchema);
	const pair = entityDefOf(PairSchema);

	it('rejects a mask derived from an init object that names the key field', () => {
		expect(() => buildPatch(todo, TodoSchema, 't1', { id: 't2', title: 'x' }))
			.toThrow(/key field "id"/);
	});

	it('rejects an explicit paths list that names the key field', () => {
		expect(() => buildPatch(todo, TodoSchema, 't1', { title: 'x' }, [ 'title', 'id' ]))
			.toThrow(/key field "id"/);
	});

	it('rejects a composite key field', () => {
		expect(() => buildPatch(pair, PairSchema, 'a/b', { label: 'x' }, [ 'ns' ]))
			.toThrow(/key field "ns"/);
	});

	it('allows a mask that touches only non-key fields', () => {
		const crud = buildPatch(todo, TodoSchema, 't1', { title: 'renamed' });
		expect(crud.fieldMask?.paths).toEqual([ 'title' ]);
	});
});

// A reference field stores only its target's key fields; projectRefs strips fat
// stubs before marshaling, mirroring the server (rebase parity).
describe('reference projection', () => {
	const issue = entityDefOf(IssueSchema);

	it('detects entity-typed fields as references', () => {
		const byName = Object.fromEntries(IssueSchema.fields.map((f) => [ f.localName, f ]));
		expect(isRef(byName.project!)).toBe(true); // singular to-one
		expect(isRef(byName.blockedBy!)).toBe(true); // repeated self-ref
		expect(isRef(byName.pair!)).toBe(true); // composite key
		expect(isRef(byName.title!)).toBe(false); // scalar
	});

	it('strips a fat to-one stub down to its key field', () => {
		const projected = projectRefs(
			IssueSchema,
			create(IssueSchema, { id: 'i1', title: 't', project: { id: 'p1', name: 'smuggled' } }),
		);
		expect(projected.project?.id).toBe('p1');
		expect(projected.project?.name).toBe('');
		expect(projected.title).toBe('t');
	});

	it('does not mutate the input value', () => {
		const value = create(IssueSchema, { id: 'i1', project: { id: 'p1', name: 'keep' } });
		projectRefs(IssueSchema, value);
		expect(value.project?.name).toBe('keep');
	});

	it('projects to-many and composite refs, keeping the holder identity', () => {
		const projected = projectRefs(
			IssueSchema,
			create(IssueSchema, {
				id: 'i1',
				blockedBy: [ { id: 'i2', title: 'noise' } ],
				pair: { ns: 'n', id: 'k', label: 'drop' },
			}),
		);
		expect(projected.id).toBe('i1');
		expect(projected.blockedBy[0]?.id).toBe('i2');
		expect(projected.blockedBy[0]?.title).toBe('');
		expect(projected.pair?.ns).toBe('n');
		expect(projected.pair?.id).toBe('k');
		expect(projected.pair?.label).toBe('');
	});

	it('buildPut marshals a projected value', () => {
		const crud = buildPut(
			issue,
			IssueSchema,
			create(IssueSchema, { id: 'i1', project: { id: 'p1', name: 'smuggled' } }),
		);
		const decoded = fromBinary(IssueSchema, crud.value);
		expect(decoded.project?.id).toBe('p1');
		expect(decoded.project?.name).toBe('');
	});

	it('buildPatch marshals a projected value', () => {
		const crud = buildPatch(issue, IssueSchema, 'i1', { project: { id: 'p2', name: 'smuggled' } });
		const decoded = fromBinary(IssueSchema, crud.value);
		expect(decoded.project?.id).toBe('p2');
		expect(decoded.project?.name).toBe('');
	});
});

// expandCollectionMask narrows a bare collection mask path to the exact items
// that changed, so per-item merge replaces "adding clobbers the list".
describe('expandCollectionMask', () => {
	const w = (init: Parameters<typeof create<typeof WidgetSchema>>[1]): Widget =>
		create(WidgetSchema, init);
	const expand = (prior: Widget, next: Widget, paths: string[]): string[] =>
		expandCollectionMask(WidgetSchema, prior, next, paths);

	describe('maps', () => {
		it('emits added, removed, and changed keys; skips unchanged', () => {
			const prior = w({ attrs: { a: '1', b: '2', d: '4' } });
			const next = w({ attrs: { a: '1', b: '9', c: '3' } }); // b changed, c added, d removed, a same
			expect(expand(prior, next, [ 'attrs' ]).sort())
				.toEqual([ 'attrs.b', 'attrs.c', 'attrs.d' ]);
		});

		it('escapes a key with a dot', () => {
			const prior = w({});
			const next = w({ attrs: { 'a.b': '1' } });
			expect(expand(prior, next, [ 'attrs' ])).toEqual([ 'attrs.a\\.b' ]);
		});
	});

	describe('keyed message lists', () => {
		it('emits added/removed/changed elements only', () => {
			const prior = w({ members: [ { uid: 'u1', name: 'A' }, { uid: 'u2', name: 'B' } ] });
			const next = w({ members: [ { uid: 'u1', name: 'A2' }, { uid: 'u3', name: 'C' } ] });
			// u1 changed, u3 added, u2 removed.
			expect(expand(prior, next, [ 'members' ]).sort())
				.toEqual([ 'members.u1', 'members.u2', 'members.u3' ]);
		});

		it('emits nothing when an element is unchanged', () => {
			const prior = w({ members: [ { uid: 'u1', name: 'A' } ] });
			const next = w({ members: [ { uid: 'u1', name: 'A' } ] });
			expect(expand(prior, next, [ 'members' ])).toEqual([]);
		});

		it('never emits a top-level key-field path', () => {
			const prior = w({ members: [ { uid: 'u1', name: 'A' } ] });
			const next = w({ members: [ { uid: 'u1', name: 'A2' }, { uid: 'u2', name: 'B' } ] });
			const out = expand(prior, next, [ 'members' ]);
			expect(out.length).toBeGreaterThan(0);
			for (const p of out) expect(p.split('.')[0]).toBe('members');
		});
	});

	describe('scalar sets', () => {
		it('emits values present on exactly one side', () => {
			const prior = w({ tags: [ 'a', 'b' ] });
			const next = w({ tags: [ 'b', 'c' ] }); // a removed, c added, b same
			expect(expand(prior, next, [ 'tags' ]).sort()).toEqual([ 'tags.a', 'tags.c' ]);
		});

		it('collapses duplicate values to one emitted path', () => {
			const prior = w({ tags: [] });
			const next = w({ tags: [ 'x', 'x' ] });
			expect(expand(prior, next, [ 'tags' ])).toEqual([ 'tags.x' ]);
		});
	});

	describe('pass-through', () => {
		it('leaves opaque collection paths untouched', () => {
			const prior = w({ atomicTags: [ 'a' ] });
			const next = w({ atomicTags: [ 'b' ] });
			expect(expand(prior, next, [ 'atomic_tags' ])).toEqual([ 'atomic_tags' ]);
		});

		it('leaves scalar and singular-message paths untouched', () => {
			const prior = w({ label: 'x', settings: { color: 'r' } });
			const next = w({ label: 'y', settings: { color: 'b' } });
			expect(expand(prior, next, [ 'label', 'settings', 'settings.color' ]))
				.toEqual([ 'label', 'settings', 'settings.color' ]);
		});

		it('leaves an already-expanded sub-path untouched', () => {
			const prior = w({ attrs: { a: '1' } });
			const next = w({ attrs: { a: '2' } });
			expect(expand(prior, next, [ 'attrs.a' ])).toEqual([ 'attrs.a' ]);
		});
	});
});

// maskFromInit turns a partial init into a mask. A whole message is refused
// there rather than accepted: protobuf-es hangs $typeName off every message, so
// its keys are not a partial, and a mask naming every field would turn the
// PATCH into a PUT.
describe('maskFromInit', () => {
	it('maps init keys to proto field names', () => {
		expect(maskFromInit(WidgetSchema, { label: 'x', byId: {} })).toEqual([ 'label', 'by_id' ]);
	});

	it('rejects a whole message, naming the verbs that do take one', () => {
		const msg = create(WidgetSchema, { id: 'w1', label: 'x' });
		expect(() => maskFromInit(WidgetSchema, msg)).toThrow(/is a whole .*Widget message/);
		expect(() => maskFromInit(WidgetSchema, msg)).toThrow(/update\(key, draft/);
		expect(() => maskFromInit(WidgetSchema, msg)).toThrow(/put\(\)/);
	});

	it('still reports an unknown key', () => {
		expect(() => maskFromInit(WidgetSchema, { nope: 1 })).toThrow(/unknown field "nope"/);
	});
});

// maskFromDiff is what update() sends: the top-level fields that actually
// changed, so untouched fields stay out of the mask and survive a concurrent
// writer.
describe('maskFromDiff', () => {
	const w = (init: Parameters<typeof create<typeof WidgetSchema>>[1]): Widget =>
		create(WidgetSchema, init);
	const diff = (prior: Widget, next: Widget): string[] =>
		maskFromDiff(WidgetSchema, prior, next).sort();

	it('is empty when nothing changed', () => {
		expect(
			diff(w({ id: 'w1', label: 'a', tags: [ 'x' ] }), w({ id: 'w1', label: 'a', tags: [ 'x' ] })),
		)
			.toEqual([]);
	});

	it('names only the changed scalars', () => {
		expect(diff(w({ label: 'a', count: 1, flag: false }), w({ label: 'b', count: 1, flag: true })))
			.toEqual([ 'flag', 'label' ]);
	});

	it('compares int64 as bigint and double by value', () => {
		expect(diff(w({ big: 1n, ratio: 1.5 }), w({ big: 2n, ratio: 1.5 }))).toEqual([ 'big' ]);
	});

	it('treats NaN as equal to itself so a float field is not always dirty', () => {
		expect(diff(w({ ratio: NaN }), w({ ratio: NaN }))).toEqual([]);
	});

	it('sees presence on an explicit-presence field', () => {
		expect(diff(w({}), w({ maybe: '' }))).toEqual([ 'maybe' ]);
		expect(diff(w({ maybe: '' }), w({}))).toEqual([ 'maybe' ]);
		expect(diff(w({ maybe: 'x' }), w({ maybe: 'x' }))).toEqual([]);
	});

	it('sees presence on a submessage, and changes inside one', () => {
		expect(diff(w({}), w({ settings: {} }))).toEqual([ 'settings' ]);
		expect(diff(w({ settings: { color: 'red' } }), w({ settings: { color: 'blue' } })))
			.toEqual([ 'settings' ]);
		expect(diff(w({ settings: { color: 'red' } }), w({ settings: { color: 'red' } }))).toEqual([]);
	});

	it('names a changed collection whole — the enqueue path narrows it per item', () => {
		expect(diff(w({ tags: [ 'a' ] }), w({ tags: [ 'a', 'b' ] }))).toEqual([ 'tags' ]);
		expect(diff(w({ attrs: { a: '1' } }), w({ attrs: { a: '2' } }))).toEqual([ 'attrs' ]);
		expect(diff(w({ attrs: { a: '1' } }), w({ attrs: { a: '1', b: '2' } }))).toEqual([ 'attrs' ]);
		expect(
			diff(w({ members: [ { uid: 'u1', name: 'a' } ] }), w({ members: [ { uid: 'u1', name: 'b' } ] })),
		)
			.toEqual([ 'members' ]);
	});

	it('compares list order, since an atomic list is replaced whole', () => {
		expect(diff(w({ atomicTags: [ 'a', 'b' ] }), w({ atomicTags: [ 'b', 'a' ] })))
			.toEqual([ 'atomic_tags' ]);
	});

	it('compares bytes by content, not identity', () => {
		expect(diff(w({ raw: new Uint8Array([ 1, 2 ]) }), w({ raw: new Uint8Array([ 1, 2 ]) })))
			.toEqual([]);
		expect(diff(w({ raw: new Uint8Array([ 1, 2 ]) }), w({ raw: new Uint8Array([ 1, 3 ]) })))
			.toEqual([ 'raw' ]);
	});
});

// Mirrors the Go server's keyOf: both sides must derive identical row keys,
// since the client sends the key it derives and the server validates it.
describe('keyOf', () => {
	const widget = entityDefOf(WidgetSchema);
	const pair = entityDefOf(PairSchema);

	it('returns single-field keys verbatim, even with a slash', () => {
		expect(keyOf(widget, create(WidgetSchema, { id: 'a/b' }))).toBe('a/b');
	});

	it('joins composite keys with a separator', () => {
		expect(keyOf(pair, create(PairSchema, { ns: 'a', id: 'b' }))).toBe('a/b');
	});

	it('escapes the separator so distinct field tuples never collide', () => {
		// ("a/b", "c") and ("a", "b/c") both naively join to "a/b/c".
		const left = keyOf(pair, create(PairSchema, { ns: 'a/b', id: 'c' }));
		const right = keyOf(pair, create(PairSchema, { ns: 'a', id: 'b/c' }));
		expect(left).toBe('a\\/b/c');
		expect(right).toBe('a/b\\/c');
		expect(left).not.toBe(right);
	});

	it('escapes the escape character itself', () => {
		expect(keyOf(pair, create(PairSchema, { ns: 'a\\', id: 'b' }))).toBe('a\\\\/b');
	});
});
