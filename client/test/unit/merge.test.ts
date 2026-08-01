import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { WireType } from '@bufbuild/protobuf/wire';
import { describe, expect, it } from 'vitest';

import {
	BlobSchema,
	type Widget,
	WidgetSchema,
} from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import {
	type EntityMergers,
	EntityRegistry,
	type MergeFn,
	mergeMask,
} from '../../src/mutations.js';

// Mirrors the Go lww suite: both implementations must agree, since pending
// mutations replay locally with this merge while the server applies its own.
describe('mergeMask', () => {
	const base = (): Widget =>
		create(WidgetSchema, {
			id: 'w1',
			label: 'original',
			count: 10,
			settings: { color: 'red', size: 4 },
			doc: { data: new Uint8Array([ 1, 2, 3 ]), note: 'v1' },
			tags: [ 'a', 'b' ],
		});

	it('copies only masked scalar fields', () => {
		const dst = base();
		mergeMask(WidgetSchema, dst, create(WidgetSchema, { label: 'patched', count: 99 }), [ 'label' ]);
		expect(dst.label).toBe('patched');
		expect(dst.count).toBe(10);
	});

	it('clears a masked field that is zero in the source', () => {
		const dst = base();
		mergeMask(WidgetSchema, dst, create(WidgetSchema), [ 'label' ]);
		expect(dst.label).toBe('');
	});

	it('replaces repeated fields whole', () => {
		const dst = base();
		mergeMask(WidgetSchema, dst, create(WidgetSchema, { tags: [ 'x' ] }), [ 'tags' ]);
		expect(dst.tags).toEqual([ 'x' ]);
	});

	it('merges nested paths without touching sibling fields', () => {
		const dst = base();
		mergeMask(WidgetSchema, dst, create(WidgetSchema, { settings: { color: 'blue' } }), [
			'settings.color',
		]);
		expect(dst.settings?.color).toBe('blue');
		expect(dst.settings?.size).toBe(4);
	});

	it('replaces a whole message field when masked at the top level', () => {
		const dst = base();
		mergeMask(WidgetSchema, dst, create(WidgetSchema, { settings: { color: 'green' } }), [
			'settings',
		]);
		expect(dst.settings?.color).toBe('green');
		expect(dst.settings?.size).toBe(0);
	});

	it('creates intermediate messages for nested paths on an empty base', () => {
		const dst = create(WidgetSchema);
		mergeMask(WidgetSchema, dst, create(WidgetSchema, { settings: { color: 'blue' } }), [
			'settings.color',
		]);
		expect(dst.settings?.color).toBe('blue');
	});

	// Presence of a nested message is observable (isSet/isNotSet) and can drive
	// subscription predicates, so a leaf write must never move it on its own.
	it('does not create a nested message when the masked leaf is unset in the source', () => {
		const dst = create(WidgetSchema, { id: 'w1' });
		mergeMask(WidgetSchema, dst, create(WidgetSchema, { settings: {} }), [ 'settings.color' ]);
		expect(dst.settings).toBeUndefined();
	});

	it('does not create a nested message when the masked opaque leaf is unset in the source', () => {
		const dst = create(WidgetSchema, { id: 'w1' });
		mergeMask(WidgetSchema, dst, create(WidgetSchema, { settings: {} }), [ 'settings.inner.data' ]);
		expect(dst.settings).toBeUndefined();
	});

	it('drops a nested message emptied by clearing its last field', () => {
		const dst = create(WidgetSchema, { id: 'w1', settings: { color: 'red' } });
		mergeMask(WidgetSchema, dst, create(WidgetSchema), [ 'settings.color' ]);
		expect(dst.settings).toBeUndefined();
	});

	it('keeps a nested message whose siblings survive the clear', () => {
		const dst = base();
		mergeMask(WidgetSchema, dst, create(WidgetSchema), [ 'settings.color' ]);
		expect(dst.settings?.size).toBe(4);
	});

	it('keeps a nested message that carries only unknown fields', () => {
		const dst = create(WidgetSchema, { id: 'w1', settings: { color: 'red' } });
		// Field 15, varint 1 — a field this build's schema does not know.
		dst.settings!.$unknown = [ { no: 15, wireType: WireType.Varint, data: new Uint8Array([ 1 ]) } ];
		mergeMask(WidgetSchema, dst, create(WidgetSchema), [ 'settings.color' ]);
		expect(dst.settings).toBeDefined();
	});

	it('replaces opaque fields whole even when the mask names sub-fields', () => {
		const dst = base();
		mergeMask(WidgetSchema, dst, create(WidgetSchema, { doc: { note: 'v2' } }), [ 'doc.note' ]);
		expect(dst.doc?.note).toBe('v2');
		expect(dst.doc?.data.length).toBe(0);
	});

	it('clears an opaque field when the source has none', () => {
		const dst = base();
		mergeMask(WidgetSchema, dst, create(WidgetSchema), [ 'doc' ]);
		expect(dst.doc).toBeUndefined();
	});

	it('does not alias source sub-messages into the destination', () => {
		const dst = base();
		const src = create(WidgetSchema, { settings: { color: 'blue' } });
		mergeMask(WidgetSchema, dst, src, [ 'settings' ]);
		src.settings!.color = 'mutated-after-merge';
		expect(dst.settings?.color).toBe('blue');
	});

	it('rejects unknown fields in the mask', () => {
		expect(() => mergeMask(WidgetSchema, base(), create(WidgetSchema), [ 'nope' ])).toThrow();
	});

	it('rejects sub-paths through scalar fields', () => {
		expect(() => mergeMask(WidgetSchema, base(), create(WidgetSchema), [ 'label.x' ])).toThrow();
	});
});

// Mirrors the Go lww mergers suite (engine/internal/lww/mergers_test.go).
describe('mergeMask with mergers', () => {
	const concatBlobs: MergeFn = (baseRaw, incomingRaw) => {
		const b = fromBinary(BlobSchema, baseRaw);
		const inc = fromBinary(BlobSchema, incomingRaw);
		b.data = new Uint8Array([ ...b.data, ...inc.data ]);
		b.note += inc.note;
		return toBinary(BlobSchema, b);
	};
	const docMergers: EntityMergers = new Map([ [ 'doc', concatBlobs ] ]);

	const withDoc = (data: string, note = ''): Widget =>
		create(WidgetSchema, { id: 'w1', doc: { data: new TextEncoder().encode(data), note } });

	it('calls the merger when both sides carry the opaque field', () => {
		const dst = withDoc('A', 'a');
		mergeMask(WidgetSchema, dst, withDoc('B', 'b'), [ 'doc' ], docMergers);
		expect(new TextDecoder().decode(dst.doc?.data)).toBe('AB');
		expect(dst.doc?.note).toBe('ab');
	});

	it('keeps replace semantics when the destination lacks the field', () => {
		const dst = create(WidgetSchema, { id: 'w1' });
		mergeMask(WidgetSchema, dst, withDoc('B'), [ 'doc' ], docMergers);
		expect(new TextDecoder().decode(dst.doc?.data)).toBe('B');
	});

	it('keeps clear semantics when the source lacks the field', () => {
		const dst = withDoc('A');
		mergeMask(WidgetSchema, dst, create(WidgetSchema), [ 'doc' ], docMergers);
		expect(dst.doc).toBeUndefined();
	});

	it('merges when the mask names a sub-path of the opaque field', () => {
		const dst = withDoc('A');
		mergeMask(WidgetSchema, dst, withDoc('B'), [ 'doc.data' ], docMergers);
		expect(new TextDecoder().decode(dst.doc?.data)).toBe('AB');
	});

	it('merges opaque bytes fields only when both sides are non-empty', () => {
		const concatRaw: EntityMergers = new Map([
			[ 'raw', (a: Uint8Array, b: Uint8Array) => new Uint8Array([ ...a, ...b ]) ],
		]);
		const dst = create(WidgetSchema, { id: 'w1', raw: new Uint8Array([ 65 ]) });
		mergeMask(
			WidgetSchema,
			dst,
			create(WidgetSchema, { raw: new Uint8Array([ 66 ]) }),
			[ 'raw' ],
			concatRaw,
		);
		expect([ ...dst.raw ]).toEqual([ 65, 66 ]);

		const empty = create(WidgetSchema, { id: 'w1' });
		mergeMask(
			WidgetSchema,
			empty,
			create(WidgetSchema, { raw: new Uint8Array([ 66 ]) }),
			[ 'raw' ],
			concatRaw,
		);
		expect([ ...empty.raw ]).toEqual([ 66 ]);
	});

	it('threads the path prefix to nested opaque fields', () => {
		const mergers: EntityMergers = new Map([ [ 'settings.inner', concatBlobs ] ]);
		const dst = create(WidgetSchema, {
			id: 'w1',
			settings: { color: 'red', inner: { data: new Uint8Array([ 65 ]) } },
		});
		const src = create(WidgetSchema, { settings: { inner: { data: new Uint8Array([ 66 ]) } } });
		mergeMask(WidgetSchema, dst, src, [ 'settings.inner' ], mergers);
		expect([ ...(dst.settings?.inner?.data ?? []) ]).toEqual([ 65, 66 ]);
		expect(dst.settings?.color).toBe('red');
	});

	it('propagates merger errors', () => {
		const boom: EntityMergers = new Map([
			[
				'doc',
				() => {
					throw new Error('merge exploded');
				},
			],
		]);
		expect(() => mergeMask(WidgetSchema, withDoc('A'), withDoc('B'), [ 'doc' ], boom)).toThrow(
			'merge exploded',
		);
	});
});

describe('EntityRegistry.setMergers', () => {
	const registry = (): EntityRegistry => new EntityRegistry([ WidgetSchema ]);
	const noop: MergeFn = (a) => a;

	it('accepts opaque message, opaque bytes and nested opaque fields', () => {
		registry().setMergers([
			{ schema: WidgetSchema, field: 'doc', merge: noop },
			{ schema: WidgetSchema, field: 'raw', merge: noop },
			{ schema: WidgetSchema, field: 'settings.inner', merge: noop },
		]);
	});

	it('rejects non-opaque fields', () => {
		expect(() => registry().setMergers([ { schema: WidgetSchema, field: 'settings', merge: noop } ]))
			.toThrow(
				'not opaque',
			);
	});

	it('rejects unknown fields', () => {
		expect(() => registry().setMergers([ { schema: WidgetSchema, field: 'nope', merge: noop } ]))
			.toThrow(
				'no field "nope"',
			);
	});

	it('rejects duplicates', () => {
		expect(() =>
			registry().setMergers([
				{ schema: WidgetSchema, field: 'doc', merge: noop },
				{ schema: WidgetSchema, field: 'doc', merge: noop },
			])
		).toThrow('registered twice');
	});
});

// Per-item merge: a mask naming a sub-path under a map/list merges that one item
// instead of replacing the whole collection. Mirrors lww peritem_test.go.
describe('mergeMask per-item', () => {
	const w = (init: Parameters<typeof create<typeof WidgetSchema>>[1]): Widget =>
		create(WidgetSchema, init);
	const merge = (dst: Widget, src: Widget, paths: string[]): void =>
		mergeMask(WidgetSchema, dst, src, paths);

	describe('maps', () => {
		it('adds an entry without disturbing others', () => {
			const dst = w({ attrs: { a: '1', b: '2' } });
			merge(dst, w({ attrs: { c: '3' } }), [ 'attrs.c' ]);
			expect(dst.attrs).toEqual({ a: '1', b: '2', c: '3' });
		});

		it('replaces a single entry value', () => {
			const dst = w({ attrs: { a: '1', b: '2' } });
			merge(dst, w({ attrs: { a: '9' } }), [ 'attrs.a' ]);
			expect(dst.attrs).toEqual({ a: '9', b: '2' });
		});

		it('clears an entry the source lacks', () => {
			const dst = w({ attrs: { a: '1', b: '2' } });
			merge(dst, w({}), [ 'attrs.a' ]);
			expect(dst.attrs).toEqual({ b: '2' });
		});

		it('handles non-string (int64) keys', () => {
			const dst = w({ byId: { '1': 'one' } });
			merge(dst, w({ byId: { '2': 'two' } }), [ 'by_id.2' ]);
			expect(dst.byId).toEqual({ 1: 'one', 2: 'two' });
		});

		it('round-trips a key with dots and backslashes', () => {
			const dst = w({ attrs: { x: '0' } });
			merge(dst, w({ attrs: { 'a.b\\c': '1' } }), [ 'attrs.a\\.b\\\\c' ]);
			expect(dst.attrs).toEqual({ x: '0', 'a.b\\c': '1' });
		});
	});

	describe('keyed message lists', () => {
		it('adds a new keyed element', () => {
			const dst = w({ members: [ { uid: 'u1', name: 'A' } ] });
			merge(dst, w({ members: [ { uid: 'u2', name: 'B' } ] }), [ 'members.u2' ]);
			expect(dst.members.map((m) => m.uid)).toEqual([ 'u1', 'u2' ]);
		});

		it('overwrites an element in place', () => {
			const dst = w({ members: [ { uid: 'u1', name: 'A' }, { uid: 'u2', name: 'B' } ] });
			merge(dst, w({ members: [ { uid: 'u1', name: 'A2', role: 5 } ] }), [ 'members.u1' ]);
			expect(dst.members).toHaveLength(2);
			expect(dst.members[0]).toMatchObject({ name: 'A2', role: 5 });
			expect(dst.members[1]!.uid).toBe('u2');
		});

		it('removes an element the source dropped', () => {
			const dst = w({ members: [ { uid: 'u1', name: 'A' }, { uid: 'u2', name: 'B' } ] });
			merge(dst, w({}), [ 'members.u1' ]);
			expect(dst.members.map((m) => m.uid)).toEqual([ 'u2' ]);
		});

		it('merges a sub-field of one element only', () => {
			const dst = w({ members: [ { uid: 'u1', name: 'A', role: 1 } ] });
			merge(dst, w({ members: [ { uid: 'u1', name: 'renamed' } ] }), [ 'members.u1.name' ]);
			expect(dst.members[0]).toMatchObject({ name: 'renamed', role: 1 });
		});

		it('creates + seeds the key field for an absent element', () => {
			const dst = w({});
			merge(dst, w({ members: [ { uid: 'u9', name: 'new' } ] }), [ 'members.u9.name' ]);
			expect(dst.members).toHaveLength(1);
			expect(dst.members[0]).toMatchObject({ uid: 'u9', name: 'new' });
		});

		it('infers identity from the element entity key (composite)', () => {
			const dst = w({ refs: [ { ns: 'n', id: '1', label: 'L1' } ] });
			merge(dst, w({ refs: [ { ns: 'n', id: '2', label: 'L2' } ] }), [ 'refs.n/2' ]);
			expect(dst.refs.map((r) => r.id)).toEqual([ '1', '2' ]);
		});
	});

	describe('scalar set lists', () => {
		it('adds a missing value', () => {
			const dst = w({ tags: [ 'a', 'b' ] });
			merge(dst, w({ tags: [ 'c' ] }), [ 'tags.c' ]);
			expect(dst.tags).toEqual([ 'a', 'b', 'c' ]);
		});

		it('is idempotent for an existing value', () => {
			const dst = w({ tags: [ 'a', 'b' ] });
			merge(dst, w({ tags: [ 'a' ] }), [ 'tags.a' ]);
			expect(dst.tags).toEqual([ 'a', 'b' ]);
		});

		it('removes every occurrence the source lacks', () => {
			const dst = w({ tags: [ 'a', 'b', 'a' ] });
			merge(dst, w({}), [ 'tags.a' ]);
			expect(dst.tags).toEqual([ 'b' ]);
		});
	});

	describe('opaque + bare collection paths', () => {
		it('replaces an opaque list whole at a sub-path', () => {
			const dst = w({ atomicTags: [ 'a', 'b' ] });
			merge(dst, w({ atomicTags: [ 'x' ] }), [ 'atomic_tags.x' ]);
			expect(dst.atomicTags).toEqual([ 'x' ]);
		});

		it('replaces a map whole at a bare path', () => {
			const dst = w({ attrs: { a: '1', b: '2' } });
			merge(dst, w({ attrs: { c: '3' } }), [ 'attrs' ]);
			expect(dst.attrs).toEqual({ c: '3' });
		});
	});

	it('treats a movable list pos as an ordinary LWW sub-field', () => {
		const dst = w({ items: [ { id: 'i1', pos: 'a0', text: 'hi' } ] });
		merge(dst, w({ items: [ { id: 'i1', pos: 'a5' } ] }), [ 'items.i1.pos' ]);
		expect(dst.items[0]).toMatchObject({ pos: 'a5', text: 'hi' });
	});
});
