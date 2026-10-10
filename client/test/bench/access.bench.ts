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
 * and the wire size at the same scales. The "one role" cases match its
 * repo_writer case: four permissions, each held at every repo, with
 * UUID-shaped keys. The "two roles" cases hold repo_reader (three
 * permissions) at half the repos and repo_admin (six permissions) at the
 * other half.
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

import { fromBinary, toBinary } from '@bufbuild/protobuf';
import { bench, describe } from 'vitest';

import { AccessSummary, canAt, decodeAccess, encodeAccess } from '../../src/access.js';
import { type CallerAccess, CallerAccessSchema } from '../../src/gen/syncplane/v1/service_pb.js';
import { callerAccessOf } from '../../src/testing/access.js';

const SIZES = [ 50, 1000, 5000 ];
const READER = [ 'view_issues', 'view_repo_members', 'view_repos' ];
const WRITER = [ 'edit_issues', 'view_issues', 'view_repo_members', 'view_repos' ];
const ADMIN = [
	'edit_issues',
	'edit_repos',
	'manage_repo_members',
	'view_issues',
	'view_repo_members',
	'view_repos',
];
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

/** Who holds what: the permissions held at repo r, and those the lookups ask for. */
interface Scenario {
	readonly name: string;
	readonly permissions: readonly string[];
	role(r: number): readonly string[];
}

const SCENARIOS: readonly Scenario[] = [
	{ name: 'one role', permissions: WRITER, role: () => WRITER },
	{ name: 'two roles', permissions: ADMIN, role: (r) => (r % 2 === 0 ? READER : ADMIN) },
];

/** The CallerAccess a server sends for the scenario's role at each of `repos` repos. */
function callerAccess(scenario: Scenario, repos: number): CallerAccess {
	const byPermission = new Map<string, string[]>();
	for (let r = 0; r < repos; r++) {
		for (const permission of scenario.role(r)) {
			let keys = byPermission.get(permission);
			if (keys === undefined) byPermission.set(permission, keys = []);
			keys.push(repoKey(r));
		}
	}
	const access = callerAccessOf({
		subjects: [ 'me' ],
		permissions: [ ...byPermission ].map(([ permission, keys ]) => ({
			permission,
			anchors: [ { entity: 'repo', keys } ],
		})),
	});
	access.seq = 1n;
	return access;
}

for (const scenario of SCENARIOS) {
	for (const n of SIZES) {
		describe(`${scenario.name}, ${n} repos`, () => {
			const access = callerAccess(scenario, n);
			const bytes = toBinary(CallerAccessSchema, access);
			// What a read of the store gives back: equal bytes in another buffer.
			const stored = bytes.slice();
			console.log(`${scenario.name}, ${n} repos: CallerAccess is ${bytes.length} bytes`);

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
				permission: scenario.permissions[i % scenario.permissions.length]!,
				key: issueKey(i),
				chain: { parents: [ { entity: 'repo', key: repoKey(i % (2 * n)) } ], above: () => [] },
			}));
			bench(`lookups (${LOOKUPS} canAt over a one-row chain)`, () => {
				for (const ask of asks) canAt(summary, ask.permission, 'issue', ask.key, ask.chain);
			});
		});
	}
}
