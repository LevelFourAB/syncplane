/**
 * What a CallerAccess costs the client for a caller with many grants.
 *
 * A pull or bootstrap that carries a CallerAccess decodes it, the client
 * re-encodes it to store it, and the first `can()` after the write decodes it
 * again into an AccessSummary, in each tab. The Go mirror is
 * engine/server/access_bench_internal_test.go, which measures the server half
 * and the wire size at the same scales; the shape here matches its repo_writer
 * case: four permissions, each held at every repo, with UUID-shaped keys.
 *
 * The IndexedDB write of the encoded bytes is not measured: fake-indexeddb
 * says nothing about a browser's cost.
 *
 * Run with: client/node_modules/.bin/vitest bench --project bench test/bench/access.bench.ts
 */

import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { bench, describe } from 'vitest';

import { AccessSummary } from '../../src/access.js';
import { type CallerAccess, CallerAccessSchema } from '../../src/gen/syncplane/v1/service_pb.js';

const SIZES = [ 50, 1000, 5000 ];
const PERMISSIONS = [ 'edit_issues', 'view_issues', 'view_repo_members', 'view_repos' ];

/** The key of repo r, as the Go benchmark writes it. */
function repoKey(r: number): string {
	return `${r.toString(16).padStart(8, '0')}-0000-4000-8000-${r.toString(16).padStart(12, '0')}`;
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
		console.log(`${n} repos: CallerAccess is ${bytes.length} bytes`);

		bench('decode', () => {
			fromBinary(CallerAccessSchema, bytes);
		});

		bench('encode', () => {
			toBinary(CallerAccessSchema, access);
		});

		bench('summarize (decode + AccessSummary)', () => {
			new AccessSummary(fromBinary(CallerAccessSchema, bytes));
		});
	});
}
