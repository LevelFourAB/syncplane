/** Exponential backoff with full jitter: 1s doubling to 30s. */
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
		return cap / 2 + Math.random() * (cap / 2);
	}

	reset(): void {
		this.attempt = 0;
	}
}

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
