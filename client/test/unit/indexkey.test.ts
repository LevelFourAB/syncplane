// Unit coverage for the parts of the index-entry encoding the shared vectors
// do not reach: what the compiler rejects, the probe helpers, and the spec
// fingerprint. The bytes themselves are pinned in
// test/unit/indexkey-vectors.test.ts against the Go encoder.

import { describe, expect, it } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { IssueSchema, WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import {
	compileIndexComponent,
	IndexEncoder,
	specHash,
	upperBound,
} from '../../src/query/indexkey.js';

const comp = (path: string[], desc = false) => compileIndexComponent(WidgetSchema, path, desc);

describe('compileIndexComponent', () => {
	it('rejects a collaborative text field', () => {
		expect(() => comp([ 'body' ])).toThrow(/collaborative text field cannot be indexed/);
	});

	it('points a bare map at the fan-out syntax', () => {
		// An index component names a SHAPE, so a map means "every entry" and has
		// to say so; a literal key would declare an index over one entry, which is
		// a different feature.
		expect(() => comp([ 'attrs' ])).toThrow(/must be followed by "\*"/);
		expect(() => comp([ 'attrs', 'color' ])).toThrow(/must be followed by "\*"/);
	});

	it('rejects a bare trailing `*`, which does not say which side of the entry it means', () => {
		// map<string, string>: both sides are strings, so a default would be a coin
		// flip with no error to notice — which is why the selector is required.
		expect(() => comp([ 'attrs', '*' ])).toThrow(/must be followed by "key" or "value"/);
	});

	it('rejects an unknown selector after `*`', () => {
		expect(() => comp([ 'attrs', '*', 'entry' ]))
			.toThrow(/"entry" after "\*" must be "key" or "value"/);
	});

	it('indexes either side of a scalar-valued map entry', () => {
		const key = comp([ 'attrs', '*', 'key' ]);
		expect(key.keyComponent).toBe(true);
		expect(key.protoPath).toEqual([ 'attrs' ]);
		expect(key.fans).toEqual([ { at: 0, isMap: true, keyKind: 'string' } ]);

		const value = comp([ 'attrs', '*', 'value' ]);
		expect(value.keyComponent).toBe(false);
		expect(value.protoPath).toEqual([ 'attrs' ]);
	});

	it('rejects a path continuing past the entry key, which is a scalar', () => {
		expect(() => comp([ 'attrs', '*', 'key', 'nope' ])).toThrow(/"key" is the entry key/);
	});

	it('rejects descending into the value of a scalar-valued map', () => {
		expect(() => comp([ 'attrs', '*', 'value', 'nope' ])).toThrow(/cannot be descended into/);
	});

	it('points a component ending at a message-valued entry at a field inside it', () => {
		expect(() => comp([ 'custom', '*', 'value' ]))
			.toThrow(/index a scalar field inside it instead/);
	});

	it('rejects `*` after a field that is not a map', () => {
		expect(() => comp([ 'tags', '*' ])).toThrow(/is not a message field/);
	});

	it('descends into a message-valued entry, and on through a list inside it', () => {
		const c = comp([ 'custom', '*', 'value', 'labels', 'values' ]);
		expect(c.protoPath).toEqual([ 'custom', 'labels', 'values' ]);
		expect(c.fans).toEqual([
			{ at: 0, isMap: true, keyKind: 'string' },
			{ at: 2, isMap: false },
		]);
	});

	it('accepts a repeated scalar, which fans out over its elements', () => {
		expect(comp([ 'tags' ]).fans).toEqual([ { at: 0, isMap: false } ]);
	});

	it('accepts a path through a repeated message field', () => {
		expect(comp([ 'members', 'uid' ]).fans).toEqual([ { at: 0, isMap: false } ]);
	});

	it('points a component ending at a message list at a field of the elements', () => {
		expect(() => comp([ 'members' ])).toThrow(/index a scalar field of the elements/);
	});

	it('extends a to-many single-key reference to its key field', () => {
		const c = compileIndexComponent(IssueSchema, [ 'blocked_by' ], false);
		expect(c.protoPath).toEqual([ 'blocked_by', 'id' ]);
		expect(c.fans).toEqual([ { at: 0, isMap: false } ]);
	});

	it('rejects two repeated fields on one component', () => {
		expect(() => comp([ 'members', 'tags' ])).toThrow(/second repeated field/);
	});
});

describe('IndexEncoder fan-out', () => {
	it('lets several components fan out over the same list', () => {
		expect(() => new IndexEncoder([ comp([ 'members', 'uid' ]), comp([ 'members', 'role' ]) ]))
			.not.toThrow();
	});

	it('refuses to fan out over two different lists', () => {
		expect(() => new IndexEncoder([ comp([ 'members', 'uid' ]), comp([ 'tags' ]) ]))
			.toThrow(/one fan-out chain per index/);
	});

	it('writes one entry per element and collapses duplicates', () => {
		const enc = new IndexEncoder([ comp([ 'tags' ]) ]);
		expect(enc.encode({ tags: [ 'a', 'b', 'c' ] }, 'w1', WidgetSchema)).toHaveLength(3);
		expect(enc.encode({ tags: [ 'a', 'a', 'b' ] }, 'w1', WidgetSchema)).toHaveLength(2);
	});

	it('still writes one entry for an empty list, so every row is reachable', () => {
		const enc = new IndexEncoder([ comp([ 'tags' ]) ]);
		const entries = enc.encode({ tags: [] }, 'w1', WidgetSchema);
		expect(entries).toHaveLength(1);
		// Missing rather than present-and-empty: an empty list has no element to
		// match, so a probe for any value must not find this row.
		expect(entries[0]!.charCodeAt(0)).toBe(0xff);
	});

	it('rejects an unknown field', () => {
		expect(() => comp([ 'nope' ])).toThrow(/has no field "nope"/);
	});

	it('extends a single-key reference to its key field, as a query does', () => {
		const c = compileIndexComponent(IssueSchema, [ 'project' ], false);
		expect(c.path).toEqual([ 'project', 'id' ]);
		expect(c.protoPath).toEqual([ 'project', 'id' ]);
		expect(c.kind).toBe('string');
	});

	it('resolves PROTO field names, which is the form a spec states them in', () => {
		// An index spec is read off a descriptor, so its paths are proto names,
		// while the resolver and the evaluator address fields by localName. Every
		// field on the testdata schemas spells the two the same way, so only a
		// snake_case one can tell a resolver looking at the wrong name apart from
		// a working one — and the failure is silent, since compileIndexes drops
		// an index it cannot compile.
		const c = compileIndexComponent(TodoSchema, [ 'sort_key' ], false);
		expect(c.path).toEqual([ 'sortKey' ]);
		expect(c.protoPath).toEqual([ 'sort_key' ]);
		expect(c.kind).toBe('string');
	});

	it('records signedness, which picks the bigint payload', () => {
		expect(comp([ 'big' ]).unsigned).toBe(false);
		expect(comp([ 'ubig' ]).unsigned).toBe(true);
	});
});

describe('specHash', () => {
	it('is stable for the same spec', () => {
		const a = specHash([ comp([ 'label' ]) ]);
		expect(a).toBe(specHash([ comp([ 'label' ]) ]));
		expect(a).toHaveLength(16);
	});

	it('changes with the direction', () => {
		expect(specHash([ comp([ 'label' ]) ])).not.toBe(specHash([ comp([ 'label' ], true) ]));
	});

	it('changes with the component order', () => {
		expect(specHash([ comp([ 'label' ]), comp([ 'count' ]) ]))
			.not.toBe(specHash([ comp([ 'count' ]), comp([ 'label' ]) ]));
	});

	it('changes when signedness changes, since the payload does', () => {
		expect(specHash([ comp([ 'big' ]) ])).not.toBe(specHash([ comp([ 'ubig' ]) ]));
	});

	// Nothing forces the two implementations to agree — a fingerprint never
	// crosses the wire — but they are written to, so a divergence here means
	// one side's canonical string drifted and the two are no longer mirrors.
	it('matches the Go SpecHash for the same spec', () => {
		expect(specHash([ comp([ 'label' ]) ])).toBe(goSpecHash([ 'label:string:false:false:-1' ]));
		expect(specHash([ comp([ 'settings', 'size' ], true) ]))
			.toBe(goSpecHash([ 'settings.size:number:false:true:-1' ]));
		expect(specHash([ comp([ 'ubig' ]) ])).toBe(goSpecHash([ 'ubig:bigint:true:false:-1' ]));
		// A fan-out component records WHERE it fans out, so a field that becomes
		// repeated changes the fingerprint without changing the path.
		expect(specHash([ comp([ 'tags' ]) ])).toBe(goSpecHash([ 'tags:string:false:false:0' ]));
		expect(specHash([ comp([ 'members', 'uid' ]) ]))
			.toBe(goSpecHash([ 'members.uid:string:false:false:0' ]));
	});

	it('changes when a component starts fanning out', () => {
		expect(specHash([ comp([ 'tags' ]) ])).not.toBe(specHash([ comp([ 'label' ]) ]));
	});

	it('renders a map fan the way Go does, selector and all', () => {
		// The `key`/`value` selector is NOT part of the rendered path — a `:key`
		// suffix carries it instead — which is what keeps a `[key, value]` layout
		// fingerprinting exactly as it did before the selector was spelled out.
		expect(specHash([ comp([ 'attrs', '*', 'key' ]) ]))
			.toBe(goSpecHash([ 'attrs.*:string:false:false:0:key' ]));
		expect(specHash([ comp([ 'attrs', '*', 'value' ]) ]))
			.toBe(goSpecHash([ 'attrs.*:string:false:false:0' ]));
		expect(specHash([ comp([ 'custom', '*', 'value', 'number' ]) ]))
			.toBe(goSpecHash([ 'custom.*.number:number:false:false:0' ]));
	});

	it('tells the two sides of one entry apart', () => {
		// On a map<string, string> both sides have the same path and the same
		// payload shape, so nothing but the selector distinguishes them.
		expect(specHash([ comp([ 'attrs', '*', 'key' ]) ]))
			.not.toBe(specHash([ comp([ 'attrs', '*', 'value' ]) ]));
	});

	it('makes the two layouts different indexes', () => {
		const key = comp([ 'attrs', '*', 'key' ]);
		const value = comp([ 'attrs', '*', 'value' ]);
		expect(specHash([ key, value ])).not.toBe(specHash([ value, key ]));
	});
});

/** FNV-1a/64 over the canonical spec string both languages build. */
function goSpecHash(components: string[]): string {
	const PRIME = 1099511628211n;
	const MASK = (1n << 64n) - 1n;
	const s = `v1|${components.join('|')}`;
	let h = 14695981039346656037n;
	for (let i = 0; i < s.length; i++) {
		h = ((h ^ BigInt(s.charCodeAt(i) & 0xff)) * PRIME) & MASK;
	}
	return h.toString(16).padStart(16, '0');
}

describe('upperBound', () => {
	it('increments the last character below 0xff', () => {
		expect(upperBound('\u0001\u0002')).toBe('\u0001\u0003');
	});

	it('carries past trailing 0xff characters', () => {
		expect(upperBound('\u0001\u00ff\u00ff')).toBe('\u0002');
	});

	it('is unbounded above an all-0xff prefix', () => {
		expect(upperBound('\u00ff\u00ff')).toBeUndefined();
	});

	it('is unbounded above an empty prefix, which every entry starts with', () => {
		expect(upperBound('')).toBeUndefined();
	});
});

describe('encodePrefix', () => {
	it('brackets exactly the rows sharing the probed component', () => {
		const encoder = new IndexEncoder([ comp([ 'label' ]) ]);
		const lo = encoder.encodePrefix([ 'hi' ]);
		const hi = upperBound(lo)!;

		const entry = (label: string) => {
			const entries = encoder.encode({ label, id: 'w1' }, 'w1', WidgetSchema);
			expect(entries).toHaveLength(1);
			return entries[0]!;
		};

		expect(entry('hi') >= lo).toBe(true);
		expect(entry('hi') < hi).toBe(true);
		expect(entry('hh') < lo).toBe(true);
		// "hia" extends the probed value, so it is a DIFFERENT value and must
		// fall outside — the terminator is what separates them.
		expect(entry('hia') >= hi).toBe(true);
	});

	it('rejects more values than the index has components', () => {
		expect(() => new IndexEncoder([]).encodePrefix([ 'x' ])).toThrow(/has 0 components/);
	});
});
