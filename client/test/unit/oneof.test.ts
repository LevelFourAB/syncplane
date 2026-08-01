import { create } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import {
	OneofKeyEntitySchema,
	OneofNestedTextEntitySchema,
	OneofRefEntitySchema,
	OneofTextEntitySchema,
	ProjectSchema,
	UserSchema,
	type Widget,
	WidgetSchema,
} from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { EntityRegistry, maskFromDiff, maskFromInit, mergeMask } from '../../src/mutations.js';
import { evalCond, resolvePath } from '../../src/query/evaluate.js';
import { compileQuery } from '../../src/query/proxy.js';
import { prepareQuery, toWirePaths } from '../../src/query/wire.js';

// A oneof is ONE field: the group is what a mask names and what a write
// replaces. Mirrors engine/internal/lww/oneof_test.go — the two merges must
// agree, since pending mutations replay locally with this one while the server
// applies its own.
describe('oneof merges', () => {
	const base = (): Widget =>
		create(WidgetSchema, {
			id: 'w1',
			label: 'original',
			choice: { case: 'choiceText', value: 'hello' },
		});

	it("replaces the active case with the source's", () => {
		const dst = base();
		mergeMask(
			WidgetSchema,
			dst,
			create(WidgetSchema, { choice: { case: 'choiceNumber', value: 7 } }),
			[ 'choice' ],
		);
		expect(dst.choice).toEqual({ case: 'choiceNumber', value: 7 });
	});

	it('sets a case on a base that had none', () => {
		const dst = create(WidgetSchema, { id: 'w1' });
		mergeMask(
			WidgetSchema,
			dst,
			create(WidgetSchema, { choice: { case: 'choiceText', value: 'set' } }),
			[ 'choice' ],
		);
		expect(dst.choice).toEqual({ case: 'choiceText', value: 'set' });
	});

	it('clears the group when the source has no case', () => {
		const dst = base();
		mergeMask(WidgetSchema, dst, create(WidgetSchema), [ 'choice' ]);
		expect(dst.choice.case).toBeUndefined();
	});

	it('leaves the group alone when the mask does not name it', () => {
		const dst = base();
		mergeMask(WidgetSchema, dst, create(WidgetSchema, { label: 'patched' }), [ 'label' ]);
		expect(dst.choice).toEqual({ case: 'choiceText', value: 'hello' });
	});

	it('replaces a message case whole', () => {
		const dst = create(WidgetSchema, {
			id: 'w1',
			choice: { case: 'choiceSettings', value: { color: 'red', size: 4 } },
		});
		mergeMask(
			WidgetSchema,
			dst,
			create(WidgetSchema, { choice: { case: 'choiceSettings', value: { color: 'blue' } } }),
			[ 'choice' ],
		);
		expect(dst.choice.case).toBe('choiceSettings');
		expect(dst.choice.value).toMatchObject({ color: 'blue', size: 0 });
	});

	it('does not alias a source message case into the destination', () => {
		const dst = base();
		const src = create(WidgetSchema, {
			choice: { case: 'choiceSettings', value: { color: 'blue' } },
		});
		mergeMask(WidgetSchema, dst, src, [ 'choice' ]);
		if (src.choice.case !== 'choiceSettings') throw new Error('unreachable');
		src.choice.value.color = 'mutated-after-merge';
		expect((dst.choice.value as { color: string }).color).toBe('blue');
	});

	it('rejects a mask that names a member', () => {
		const dst = base();
		expect(() => mergeMask(WidgetSchema, dst, create(WidgetSchema), [ 'choice_text' ]))
			.toThrow(/member of oneof "choice"/);
	});

	it('rejects a sub-path into a message case', () => {
		const dst = base();
		expect(() =>
			mergeMask(WidgetSchema, dst, create(WidgetSchema), [ 'choice.choice_settings.color' ])
		).toThrow(/does not support sub-paths/);
	});

	// proto3 `optional` compiles to a synthetic oneof in Go. protobuf-es keeps it
	// off DescField.oneof, and this pins that the two agree about what is an
	// ordinary field.
	it('still masks an optional field by its own name', () => {
		const dst = create(WidgetSchema, { id: 'w1' });
		mergeMask(WidgetSchema, dst, create(WidgetSchema, { maybe: 'here' }), [ 'maybe' ]);
		expect(dst.maybe).toBe('here');
		mergeMask(WidgetSchema, dst, create(WidgetSchema), [ 'maybe' ]);
		expect(dst.maybe).toBeUndefined();
	});
});

describe('oneof masks', () => {
	it('names the group for an init that addresses it', () => {
		expect(maskFromInit(WidgetSchema, { choice: { case: 'choiceText', value: 'x' } }))
			.toEqual([ 'choice' ]);
	});

	it('names the group when a diff changed the case', () => {
		const prior = create(WidgetSchema, { choice: { case: 'choiceText', value: 'a' } });
		const next = create(WidgetSchema, { choice: { case: 'choiceNumber', value: 1 } });
		expect(maskFromDiff(WidgetSchema, prior, next)).toEqual([ 'choice' ]);
	});

	it('names the group when a diff changed the value inside one case', () => {
		const prior = create(WidgetSchema, { choice: { case: 'choiceText', value: 'a' } });
		const next = create(WidgetSchema, { choice: { case: 'choiceText', value: 'b' } });
		expect(maskFromDiff(WidgetSchema, prior, next)).toEqual([ 'choice' ]);
	});

	it('leaves an untouched group out of the diff', () => {
		const prior = create(WidgetSchema, { label: 'a', choice: { case: 'choiceText', value: 'x' } });
		const next = create(WidgetSchema, { label: 'b', choice: { case: 'choiceText', value: 'x' } });
		expect(maskFromDiff(WidgetSchema, prior, next)).toEqual([ 'label' ]);
	});
});

// Reading is member-granular even though writing is not: "which case is set" is
// a question the data answers unambiguously. The engine resolves the same paths
// through protoreflect, so these pin the local evaluator against it.
describe('oneof queries', () => {
	it('resolves a member only while its case is active', () => {
		const w = create(WidgetSchema, { choice: { case: 'choiceText', value: 'hi' } });
		expect(resolvePath(w, [ 'choiceText' ], WidgetSchema)).toEqual({ found: true, value: 'hi' });
		expect(resolvePath(w, [ 'choiceNumber' ], WidgetSchema)).toEqual({ found: false });
	});

	// The empty string is a real value for an active case, and the engine agrees
	// (a oneof member has explicit presence, so Has() is true at the zero value).
	it('reports an active case holding a zero value as present', () => {
		const w = create(WidgetSchema, { choice: { case: 'choiceText', value: '' } });
		expect(resolvePath(w, [ 'choiceText' ], WidgetSchema)).toEqual({ found: true, value: '' });
	});

	it('descends into a message case', () => {
		const w = create(WidgetSchema, {
			choice: { case: 'choiceSettings', value: { color: 'red' } },
		});
		expect(resolvePath(w, [ 'choiceSettings', 'color' ], WidgetSchema))
			.toEqual({ found: true, value: 'red' });
	});

	it('filters on a member', () => {
		const active = create(WidgetSchema, { id: 'a', choice: { case: 'choiceText', value: 'hi' } });
		const other = create(WidgetSchema, { id: 'b', choice: { case: 'choiceNumber', value: 1 } });
		const cond = prepareQuery(
			WidgetSchema,
			compileQuery<Widget>({ where: (w) => w.choice.choiceText.eq('hi') }),
		).where!;
		expect(evalCond(active, cond, WidgetSchema)).toBe(true);
		expect(evalCond(other, cond, WidgetSchema)).toBe(false);
	});

	it('tests which case is active with isSet', () => {
		const cond = prepareQuery(
			WidgetSchema,
			compileQuery<Widget>({ where: (w) => w.choice.choiceNumber.isSet() }),
		).where!;
		expect(evalCond(
			create(WidgetSchema, { id: 'a', choice: { case: 'choiceNumber', value: 0 } }),
			cond,
			WidgetSchema,
		)).toBe(true);
		expect(evalCond(
			create(WidgetSchema, { id: 'b', choice: { case: 'choiceText', value: 'x' } }),
			cond,
			WidgetSchema,
		)).toBe(false);
	});

	// The group is a property on the message shape, so the DSL path carries it;
	// the wire speaks proto field names, where a member stands on its own.
	it('drops the group segment from the wire path', () => {
		const compiled = prepareQuery(
			WidgetSchema,
			compileQuery<Widget>({ where: (w) => w.choice.choiceText.eq('hi') }),
		);
		expect(compiled.where).toMatchObject({ path: [ 'choiceText' ] });
		expect(toWirePaths(WidgetSchema, compiled).where).toMatchObject({ path: [ 'choice_text' ] });
	});

	it('rejects a path that stops at the group', () => {
		expect(() =>
			prepareQuery(WidgetSchema, {
				where: { kind: 'present', path: [ 'choice' ], op: 'isSet' },
			})
		).toThrow(/is a oneof, not a field/);
	});
});

// Anything that needs a field to be reliably THERE cannot sit inside a oneof,
// because the group is replaced whole and nothing below it is ever addressed on
// its own. Mirrors the engine's registration checks.
describe('oneof registration', () => {
	it('rejects a key field inside a oneof', () => {
		expect(() => new EntityRegistry([ OneofKeyEntitySchema ]))
			.toThrow(/key field "id" is a member of oneof "which"/);
	});

	it('rejects a text field inside a oneof', () => {
		expect(() => new EntityRegistry([ OneofTextEntitySchema ]))
			.toThrow(/text field is inside oneof "which"/);
	});

	it('rejects a text field below a oneof member', () => {
		expect(() => new EntityRegistry([ OneofNestedTextEntitySchema ]))
			.toThrow(/text field is inside oneof "which"/);
	});

	it('rejects a reference inside a oneof', () => {
		expect(() => new EntityRegistry([ ProjectSchema, UserSchema, OneofRefEntitySchema ]))
			.toThrow(/reference field is a member of oneof "target"/);
	});

	it('accepts an entity that merely has a oneof', () => {
		expect(() => new EntityRegistry([ WidgetSchema ])).not.toThrow();
	});
});
