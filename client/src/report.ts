/**
 * Isolation for user-supplied callbacks: an exception is caught and reported
 * rather than propagating into the machinery that fired it.
 *
 * Watch callbacks, status listeners, rejection callbacks, text-delta
 * subscribers and the schema-skew policy all run synchronously inside the
 * mutation commit path or the sync cycle. A throw there must not be mistaken
 * for a failed write, abort a dispatch loop partway, or mark a sync cycle as
 * errored — the commit/pull already succeeded by the time we notify. We surface
 * the error via console.error (visible in both browser and Node, crashes
 * neither) and carry on.
 */

/** Invokes a one-argument callback in isolation. */
export function safeInvoke<T>(cb: (arg: T) => void, arg: T, context: string): void {
	try {
		cb(arg);
	} catch (err) {
		reportCallbackError(context, err);
	}
}

/** Invokes an arbitrary callback expression in isolation, ignoring its result. */
export function safeRun(fn: () => void, context: string): void {
	try {
		fn();
	} catch (err) {
		reportCallbackError(context, err);
	}
}

/**
 * Invokes a value-returning callback in isolation, substituting `fallback` when
 * it throws. For callbacks whose answer steers the library (the skew policy),
 * where "the app's handler broke" has to resolve to a decision rather than an
 * exception travelling up the sync path.
 */
export function safeCall<R>(fn: () => R, context: string, fallback: R): R {
	try {
		return fn();
	} catch (err) {
		reportCallbackError(context, err);
		return fallback;
	}
}

/**
 * Reports a library-side failure that has already been contained — the caller
 * has handled it and is carrying on — over the same channel as the callback
 * isolation above. For conditions an app may have no listener for (a text
 * handle that stopped merging), where silence would be the worst outcome.
 */
export function reportError(message: string, err: unknown): void {
	try {
		console.error(`[sync] ${message}`, err);
	} catch {
		// A console that itself throws must not take down the caller either.
	}
}

/**
 * Reports API misuse over console.warn: a call the library carried out as asked
 * that cannot do what the caller meant. For mistakes whose only other sign
 * arrives too far from the call to be traced back to it.
 */
export function reportMisuse(message: string): void {
	try {
		console.warn(`[sync] ${message}`);
	} catch {
		// A console that itself throws must not take down the caller either.
	}
}

function reportCallbackError(context: string, err: unknown): void {
	reportError(`${context} threw; ignoring`, err);
}
