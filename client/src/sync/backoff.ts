/**
 * Exponential backoff with full jitter: a delay drawn uniformly from zero up to
 * a window that doubles per attempt, 1s to 30s by default.
 *
 * The delay spreads a reconnect surge as much as it paces one client, and a
 * jittered floor is precisely the part that cannot spread — every client a lost
 * node drops starts its first retry inside one window of the same instant, so
 * the width of that window is the whole of the spread.
 */
export class Backoff {
	private attempt = 0;

	constructor(
		private baseMs = 1_000,
		private maxMs = 30_000,
	) {}

	/** Next delay in ms (and advances the attempt counter). */
	next(): number {
		const cap = Math.min(this.maxMs, this.baseMs * 2 ** this.attempt);
		this.attempt = Math.min(this.attempt + 1, 10);
		return Math.random() * cap;
	}

	reset(): void {
		this.attempt = 0;
	}
}

/**
 * How long a working stream must stay open before its close counts as an
 * ordinary disconnect rather than a flap. Resetting the reconnect backoff on the
 * first frame instead would let a peer that accepts the stream, delivers one
 * frame and hangs up — an aggressive idle timeout in a proxy, a load balancer
 * culling long-lived requests — hold every client inside the first window
 * indefinitely. Below this threshold the backoff keeps escalating toward its cap,
 * so a persistent flap costs the server a connection per cap, not per second.
 */
export const STREAM_STABLE_MS = 10_000;

export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
	return new Promise((resolve) => {
		if (signal?.aborted) return resolve();
		const timer = setTimeout(done, ms);
		function done() {
			signal?.removeEventListener('abort', done);
			clearTimeout(timer);
			resolve();
		}
		signal?.addEventListener('abort', done);
	});
}
