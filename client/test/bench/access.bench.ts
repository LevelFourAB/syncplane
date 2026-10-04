/**
 * What a CallerAccess costs the client for a caller with many grants.
 *
 * A pull or bootstrap that carries a CallerAccess decodes it, and the client
 * encodes it again to store it. The first `can()` after the write then needs
 * an AccessSummary of the stored bytes. The tab that applied the pull pays the
 * whole chain ("apply" cases): when it keeps what encodeAccess gave it, it
 * compares the stored bytes with those and summarizes the message it holds,
 * and when it does not, it decodes the stored bytes. Every other tab decodes
 * the stored bytes ("summarize"). After that, each `can()` looks up its row
 * and the rows up its chain of access parents ("lookups"). The Go mirror is
 * engine/server/access_bench_internal_test.go, which measures the server half
 * and the wire size at the same scales; the shape here matches its repo_writer
 * case: four permissions, each held at every repo, with UUID-shaped keys.
 *
 * V8 keeps the hash of a string after its first use as a key, so a case that
 * uses the same message or keys again leaves out the hashing that a freshly
 * decoded message pays. The "apply" and "summarize" cases decode in each
 * iteration and pay it; "AccessSummary, warm" and "lookups" do not.
 *
 * The IndexedDB write of the encoded bytes and the copy a read gives back are
 * not measured: fake-indexeddb says nothing about a browser's cost.
 *
 * Run with: client/node_modules/.bin/vitest bench --project bench test/bench/access.bench.ts
 */

import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { bench, describe } from 'vitest';

import { AccessSummary, canAt, decodeAccess, encodeAccess } from '../../src/access.js';
import { type CallerAccess, CallerAccessSchema } from '../../src/gen/syncplane/v1/service_pb.js';

const SIZES = [ 50, 1000, 5000 ];
const PERMISSIONS = [ 'edit_issues', 'view_issues', 'view_repo_members', 'view_repos' ];
/** How many `canAt` calls one "lookups" iteration makes. */
const LOOKUPS = 10_000;

/** The key of repo r, as the Go benchmark writes it. */
function repoKey(r: number): string {
	return `${r.toString(16).padStart(8, '0')}-0000-4000-8000-${r.toString(16).padStart(12, '0')}`;
}

/** A key of an issue, which no permission is held at. */
function issueKey(i: number): string {
	return `${i.toString(16).padStart(8, '0')}-1111-4000-8000-${i.toString(16).padStart(12, '0')}`;
}

/** A CallerAccess holding every permission at `repos` repos. */
function callerAccess(repos: number): CallerAccess {
	const keys = Array.from({ length: repos }, (_, r) => repoKey(r)).sort();
	return create(CallerAccessSchema, {
		subjects: [ 'me' ],
		seq: 1n,
		permissions: PERMISSIONS.map((permission) => ({
			permission,
			anchors: [ { entity: 'repo', keys } ],
		})),
	});
}

for (const n of SIZES) {
	describe(`${n} repos`, () => {
		const access = callerAccess(n);
		const bytes = toBinary(CallerAccessSchema, access);
		// What a read of the store gives back: equal bytes in another buffer.
		const stored = bytes.slice();
		console.log(`${n} repos: CallerAccess is ${bytes.length} bytes`);

		bench('decode', () => {
			fromBinary(CallerAccessSchema, bytes);
		});

		bench('encode', () => {
			toBinary(CallerAccessSchema, access);
		});

		bench('AccessSummary, warm (keys hashed before)', () => {
			new AccessSummary(access);
		});

		bench('summarize (decode + AccessSummary)', () => {
			new AccessSummary(fromBinary(CallerAccessSchema, bytes));
		});

		bench('apply, decode the stored bytes (wire decode + encode + decode + AccessSummary)', () => {
			const received = fromBinary(CallerAccessSchema, bytes);
			toBinary(CallerAccessSchema, received);
			void decodeAccess(stored, undefined).summary;
		});

		bench('apply, reuse the message (wire decode + encode + compare + AccessSummary)', () => {
			const received = fromBinary(CallerAccessSchema, bytes);
			void decodeAccess(stored, encodeAccess(received)).summary;
		});

		// Each prediction asks for an issue, which holds nothing, and then for
		// the repo above it, which half the time is outside the grants.
		const summary = new AccessSummary(access);
		const asks = Array.from({ length: LOOKUPS }, (_, i) => ({
			permission: PERMISSIONS[i % PERMISSIONS.length]!,
			key: issueKey(i),
			chain: { rows: [ { entity: 'repo', key: repoKey(i % (2 * n)) } ], complete: true },
		}));
		bench(`lookups (${LOOKUPS} canAt over a one-row chain)`, () => {
			for (const ask of asks) canAt(summary, ask.permission, 'issue', ask.key, ask.chain);
		});
	});
}
