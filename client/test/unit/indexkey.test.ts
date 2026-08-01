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

	it('rejects a leaf that could never take part in a comparison', () => {
		expect(() => comp([ 'attrs' ])).toThrow(/not comparable/);
	});

	it('accepts a repeated scalar, which fans out over its elements', () => {
		expect(comp([ 'tags' ]).multiAt).toBe(0);
	});

	it('accepts a path through a repeated message field', () => {
		expect(comp([ 'members', 'uid' ]).multiAt).toBe(0);
	});

	it('points a component ending at a message list at a field of the elements', () => {
		expect(() => comp([ 'members' ])).toThrow(/index a scalar field of the elements/);
	});

	it('extends a to-many single-key reference to its key field', () => {
		const c = compileIndexComponent(IssueSchema, [ 'blocked_by' ], false);
		expect(c.protoPath).toEqual([ 'blocked_by', 'id' ]);
		expect(c.multiAt).toBe(0);
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
			.toThrow(/one repeated field per index/);
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
