import { appendFileSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname } from 'node:path';

/**
 * Where a benchmark's numbers go.
 *
 * Playwright's own reporters say whether a test passed, which is not what this
 * tier produces: every spec here passes as long as it ran, and the output IS
 * the measurement. So each result is appended to a JSONL file as it is taken —
 * one line per scenario, written immediately rather than accumulated in memory
 * — and global teardown turns the file into the table a human reads.
 *
 * Appending as it goes is what makes a partial run useful: an xl scenario that
 * times out still leaves every number taken before it on disk.
 */
export const RESULTS_ENV = 'SYNC_BENCH_RESULTS';

/**
 * What crossed the wire during a measurement, from the server's side.
 *
 * Bytes are the one part of a benchmark that survives the move off localhost:
 * the same client on a real link ships exactly these, and only the time changes.
 * That is what makes [projectOnLink] possible at all.
 *
 * Three sizes, because two different things compress at two different points:
 *
 *   - `bytes*` — what the handler wrote. The engine's own output.
 *   - `gzip*` — that same output compressed. Meaningful only when it was not
 *     compressed already; gzipping a gzipped body reports ~1.0x and means
 *     nothing.
 *   - `wire*` — what a real link carries, chosen per request from whichever of
 *     the two applies. **This is the number to estimate with.**
 *
 * The distinction is not theoretical. Connect compresses a UNARY response
 * through the ordinary HTTP Content-Encoding header, so Pull already arrives
 * compressed; a STREAMING response cannot use that header, and connect-web
 * refuses protocol-level frame compression, so Bootstrap arrives raw and only
 * gets compressed if a proxy in front does it.
 */
export interface Wire {
	/** Connect calls. A server-streaming method counts once, not once per frame. */
	roundTrips: number;
	bytesDown: number;
	bytesUp: number;
	gzipDown: number;
	gzipUp: number;
	wireDown: number;
	wireUp: number;
	/**
	 * Time the server's handlers spent on those calls. The rest of the wall time
	 * a scenario measured was the client's, or the loopback between them.
	 */
	serverMs: number;
}

/** One scenario's measurement. `metrics` is free-form on purpose. */
export interface BenchResult {
	/** Scenario name, stable across runs — the key a comparison joins on. */
	scenario: string;
	/** Corpus size ('s' | 'm' | 'l' | 'xl'), or absent when size-independent. */
	tier?: string;
	/** Subscription set the scenario ran under. */
	scope?: string;
	/** Whatever the scenario measured. Durations end in Ms, sizes in Bytes. */
	metrics: Record<string, number | string | boolean>;
	/**
	 * Wire cost plus the wall time it was measured against, when the scenario
	 * has one operation worth projecting onto a real link. Scenarios that only
	 * read locally leave it out.
	 */
	wire?: Wire & {
		/** Wall time of the operation these bytes belong to, on localhost. */
		localMs: number;
		/** Which metric that duration came from, so the table can name it. */
		of: string;
	};
}

function resultsPath(): string {
	const path = process.env[RESULTS_ENV];
	if (path === undefined) throw new Error(`${RESULTS_ENV} unset — global setup did not run`);
	return path;
}

/** Records one scenario's numbers. Safe to call from any spec. */
export function report(result: BenchResult): void {
	const path = resultsPath();
	mkdirSync(dirname(path), { recursive: true });
	appendFileSync(path, `${JSON.stringify(result)}\n`);
}

/**
 * Formats the run as a table, one section per scenario. Metric columns are
 * derived from the results rather than declared, so a scenario that measures
 * something new prints it without this file having to know about it.
 */
export function formatResults(results: BenchResult[]): string {
	const out: string[] = [];
	const scenarios = [ ...new Set(results.map((r) => r.scenario)) ];
	for (const scenario of scenarios) {
		const rows = results.filter((r) => r.scenario === scenario);
		const metrics = [ ...new Set(rows.flatMap((r) => Object.keys(r.metrics))) ];
		const header = [ 'tier', 'scope', ...metrics ];
		const body = rows.map((r) => [
			r.tier ?? '-',
			r.scope ?? '-',
			...metrics.map((m) => format(r.metrics[m])),
		]);
		const widths = header.map((h, i) =>
			Math.max(h.length, ...body.map((cells) => cells[i]?.length ?? 0))
		);
		const line = (cells: string[]) =>
			cells.map((cell, i) => cell.padStart(widths[i] ?? 0)).join('  ');
		out.push(`\n${scenario}`);
		out.push(line(header));
		out.push(widths.map((w) => '-'.repeat(w)).join('  '));
		for (const cells of body) out.push(line(cells));
	}
	return out.join('\n');
}

function format(value: number | string | boolean | undefined): string {
	if (value === undefined) return '';
	if (typeof value !== 'number') return String(value);
	if (Number.isInteger(value)) return String(value);
	// Two decimals is the resolution these numbers deserve: everything here is
	// wall time on a real browser, and the run-to-run spread is far wider.
	return value.toFixed(2);
}

/** Reads a run's JSONL back, skipping a trailing partial line from a kill. */
export function readResults(path: string): BenchResult[] {
	let raw: string;
	try {
		raw = readFileSync(path, 'utf8');
	} catch {
		return [];
	}
	const results: BenchResult[] = [];
	for (const line of raw.split('\n')) {
		if (line.trim() === '') continue;
		try {
			results.push(JSON.parse(line) as BenchResult);
		} catch {
			// A run killed mid-write leaves half a line; the numbers before it
			// are still good.
		}
	}
	return results;
}

/**
 * Link profiles the results are projected onto. Round numbers on purpose —
 * these exist to answer "roughly what does this feel like away from localhost",
 * and a table claiming 3.7 significant figures about somebody's train wifi would
 * be pretending.
 */
export interface Link {
	name: string;
	/** Usable throughput, megabits per second. */
	mbps: number;
	/** Round-trip time, milliseconds. */
	rttMs: number;
}

export const LINKS: Link[] = [
	{ name: 'lan', mbps: 200, rttMs: 2 },
	{ name: 'fiber', mbps: 100, rttMs: 15 },
	{ name: 'good-4g', mbps: 20, rttMs: 50 },
	{ name: 'cafe-wifi', mbps: 5, rttMs: 100 },
	{ name: 'bad-3g', mbps: 1.5, rttMs: 300 },
];

/**
 * Estimated wall time for a measured operation over `link`.
 *
 * The model is deliberately crude and additive:
 *
 *     total ≈ localMs + downloadMs + uploadMs + roundTrips × rtt
 *
 * `localMs` is the measurement from localhost, standing in for everything that
 * is not the network — server work, decoding, IndexedDB writes — because those
 * do not get slower when the link does.
 *
 * What it ignores, and which way each one pushes:
 *
 *   - TLS handshakes, TCP slow start, HTTP/2 flow control — all make it slower,
 *     and slow start in particular hurts a large bootstrap over a high-RTT link
 *     far more than one round trip's worth.
 *   - Transfer overlapping with server work and with client apply — makes it
 *     faster, since a streamed bootstrap does not wait for the whole body.
 *   - Loss, jitter, and a shared link — slower, unboundedly.
 *
 * So it is an order of magnitude, not a prediction, and it is most trustworthy
 * exactly where it matters least (small payloads, where the RTT term dominates
 * and the RTT term is the one thing here that is simple).
 */
export function projectOnLink(wire: Wire & { localMs: number }, link: Link): number {
	const bitsPerMs = link.mbps * 1000;
	const downMs = (wire.wireDown * 8) / bitsPerMs;
	const upMs = (wire.wireUp * 8) / bitsPerMs;
	return wire.localMs + downMs + upMs + wire.roundTrips * link.rttMs;
}

/**
 * The data-volume table: what each measured operation shipped, and roughly what
 * it would cost on each link profile.
 *
 * Reported per scenario row rather than aggregated, because the interesting
 * comparison is between scopes — a windowed bootstrap and a full one differ by
 * two orders of magnitude in bytes, and that difference is the whole argument
 * for partial sync.
 */
export function formatWireProjection(results: BenchResult[]): string {
	const rows = results.filter((r) => r.wire !== undefined);
	if (rows.length === 0) return '';

	const header = [
		'scenario',
		'tier',
		'scope',
		'measured',
		'rawKB',
		'wireKB',
		'upKB',
		'trips',
		'localMs',
		'serverMs',
		...LINKS.map((l) => l.name),
	];
	const body = rows.map((r) => {
		const wire = r.wire as NonNullable<BenchResult['wire']>;
		return [
			r.scenario,
			r.tier ?? '-',
			r.scope ?? '-',
			wire.of,
			kb(wire.bytesDown),
			kb(wire.wireDown),
			kb(wire.wireUp),
			String(wire.roundTrips),
			format(wire.localMs),
			format(wire.serverMs),
			...LINKS.map((l) => seconds(projectOnLink(wire, l))),
		];
	});
	const widths = header.map((h, i) =>
		Math.max(h.length, ...body.map((cells) => cells[i]?.length ?? 0))
	);
	const line = (cells: string[]) => cells.map((c, i) => c.padStart(widths[i] ?? 0)).join('  ');
	return [
		'\ndata volume and estimated time per link (localMs + bytes/bandwidth + trips×RTT)',
		line(header),
		widths.map((w) => '-'.repeat(w)).join('  '),
		...body.map(line),
	].join('\n');
}

function kb(bytes: number): string {
	return (bytes / 1024).toFixed(bytes < 1024 * 10 ? 1 : 0);
}

/** Seconds to two figures — see LINKS on why more would be pretending. */
function seconds(ms: number): string {
	if (ms < 1000) return `${Math.round(ms)}ms`;
	return `${(ms / 1000).toFixed(ms < 10_000 ? 1 : 0)}s`;
}

/** Writes the run's results as one JSON document, for tooling and diffing. */
export function writeSummary(path: string, results: BenchResult[]): void {
	mkdirSync(dirname(path), { recursive: true });
	writeFileSync(path, `${JSON.stringify(results, null, '\t')}\n`);
}
