import { describe, expect, it } from 'vitest';

import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { freshWorkspace, newClient } from './helpers.js';

// Values at the edges of what the TS client and the Go server encode the same
// way. Each spec writes through one client, syncs, and reads the row through a
// second client that only ever saw the server's copy — so what it reads is
// what the Go server stored, decoded again in TS.
describe('value encoding across Go and TS', () => {
	async function roundTrip(
		write: (c: ReturnType<typeof newClient>) => Promise<void>,
	) {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();
		await b.start();
		await write(a);
		await a.syncNow();
		await b.syncNow();
		const fromA = await a.get(WidgetSchema, 'w1');
		const fromB = await b.get(WidgetSchema, 'w1');
		await a.close();
		await b.close();
		return { fromA, fromB };
	}

	// Given a widget whose int64/uint64 fields hold their extremes, When a
	// client PUTs it, Then every replica reads the exact same values. Must NOT:
	// round through a JS number.
	it('keeps int64 and uint64 extremes exact', async () => {
		const { fromA, fromB } = await roundTrip(async (c) => {
			await c.mutate.put(WidgetSchema, {
				id: 'w1',
				big: -9223372036854775808n,
				ubig: 18446744073709551615n,
				byId: { '-9223372036854775808': 'min', '9223372036854775807': 'max' },
			});
		});
		for (const w of [ fromA, fromB ]) {
			expect(w!.big).toBe(-9223372036854775808n);
			expect(w!.ubig).toBe(18446744073709551615n);
			expect(w!.byId).toEqual({ '-9223372036854775808': 'min', '9223372036854775807': 'max' });
		}
	});

	// Given a row whose int64-keyed map holds both extremes, When a client
	// updates ONE entry (a mask path like `by_id.-9223372036854775808`), Then
	// only that entry changes. Must NOT: drop or rewrite the other entry.
	it('patches one int64 map entry at the extreme without touching the other', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();
		await b.start();
		await a.mutate.put(WidgetSchema, {
			id: 'w1',
			byId: { '-9223372036854775808': 'min', '9223372036854775807': 'max' },
		});
		await a.syncNow();
		await a.mutate.update(WidgetSchema, 'w1', (w) => {
			w.byId['-9223372036854775808'] = 'min2';
		});
		await a.syncNow();
		await b.syncNow();
		expect((await b.get(WidgetSchema, 'w1'))!.byId).toEqual({
			'-9223372036854775808': 'min2',
			'9223372036854775807': 'max',
		});
		await a.close();
		await b.close();
	});

	// Given NaN and the infinities in a double field, When written and
	// synced, Then each replica reads the same special value back.
	it.each([ Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY ])(
		'keeps the double %s',
		async (value) => {
			const { fromB } = await roundTrip(async (c) => {
				await c.mutate.put(WidgetSchema, { id: 'w1', ratio: value });
			});
			expect(Object.is(fromB!.ratio, value)).toBe(true);
		},
	);

	// The encoder leaves an implicit-presence -0 off the wire, so the server
	// stores +0 (docs/explanation/limits-and-trade-offs.md). The writer's own
	// view must read +0 as well, or its replica disagrees with every other one.
	it('writes -0 in an implicit-presence double as +0 on every replica', async () => {
		const { fromA, fromB } = await roundTrip(async (c) => {
			await c.mutate.put(WidgetSchema, { id: 'w1', ratio: -0, settings: { ratio: -0 } });
		});
		for (const w of [ fromA, fromB ]) {
			expect(Object.is(w!.ratio, 0)).toBe(true);
			expect(Object.is(w!.settings?.ratio, 0)).toBe(true);
		}
	});

	it('keeps the sign of -0 in a double with explicit presence', async () => {
		const { fromA, fromB } = await roundTrip(async (c) => {
			await c.mutate.put(WidgetSchema, {
				id: 'w1',
				custom: { size: { kind: { case: 'number', value: -0 } } },
			});
		});
		for (const w of [ fromA, fromB ]) {
			expect(w!.custom.size?.kind.case).toBe('number');
			expect(Object.is(w!.custom.size?.kind.value, -0)).toBe(true);
		}
	});

	// Given two string map keys that look the same — "é" in NFC (one code
	// point) and in NFD (e + combining accent) — When both are written and one
	// is then updated by mask, Then both entries survive as distinct keys and
	// only the updated one changes. Must NOT: normalize one onto the other.
	it('keeps NFC and NFD map keys apart through PUT and PATCH', async () => {
		const nfc = 'é';
		const nfd = 'é';
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();
		await b.start();
		await a.mutate.put(WidgetSchema, { id: 'w1', attrs: { [nfc]: 'composed', [nfd]: 'decomposed' } });
		await a.syncNow();
		await a.mutate.update(WidgetSchema, 'w1', (w) => {
			w.attrs[nfd] = 'decomposed2';
		});
		await a.syncNow();
		await b.syncNow();
		expect((await b.get(WidgetSchema, 'w1'))!.attrs).toEqual({
			[nfc]: 'composed',
			[nfd]: 'decomposed2',
		});
		await a.close();
		await b.close();
	});

	// Given map keys holding the mask-path escape characters (`.`, `\`)
	// and U+0000, When one entry is updated by mask, Then the right entry
	// changes. Must NOT: address a different entry or split the key at the
	// dot.
	it('patches map entries whose keys hold mask-path metacharacters', async () => {
		const keys = [ 'a.b', 'a', 'a\\.b', 'a\\', 'x\u0000y', '' ];
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();
		await b.start();
		await a.mutate.put(WidgetSchema, {
			id: 'w1',
			attrs: Object.fromEntries(keys.map((k) => [ k, 'v0' ])),
		});
		await a.syncNow();
		await a.mutate.update(WidgetSchema, 'w1', (w) => {
			w.attrs['a.b'] = 'v1';
			w.attrs['x\u0000y'] = 'v1';
		});
		await a.syncNow();
		await b.syncNow();
		const want = Object.fromEntries(keys.map((k) => [ k, 'v0' ]));
		want['a.b'] = 'v1';
		want['x\u0000y'] = 'v1';
		expect((await b.get(WidgetSchema, 'w1'))!.attrs).toEqual(want);
		await a.close();
		await b.close();
	});
});
