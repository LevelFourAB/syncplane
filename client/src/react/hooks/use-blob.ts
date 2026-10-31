import { useEffect, useMemo, useState } from 'react';

import type { BlobRefLike, BlobStatus } from '../../blob/api.js';
import { useWorkspaceScope } from '../context.js';

/** What useBlob returns. */
export interface UseBlobResult {
	/** The blob's status (see WorkspaceBlobs.watch); undefined until it is first known. */
	status: BlobStatus | undefined;
	/** The bytes, once read; undefined while the blob cannot be read yet. */
	blob: Blob | undefined;
	/**
	 * An object URL of `blob` for `<img src>` and the like. Revoked when the
	 * component unmounts or the ref changes. Undefined where the runtime has no
	 * `URL.createObjectURL`.
	 */
	url: string | undefined;
	/** Why the last read of the bytes failed; cleared by the next read. */
	error: Error | undefined;
}

/** Options for useBlob. */
export interface UseBlobOptions {
	workspaceId?: string;
}

interface BlobState extends UseBlobResult {
	identity: string;
}

function empty(identity: string): BlobState {
	return { identity, status: undefined, blob: undefined, url: undefined, error: undefined };
}

/**
 * Watches one blob of the resolved workspace and reads its bytes once they
 * can be read: at once when this device has them queued or cached, and after
 * the upload completes otherwise. A failed read is tried again on the next
 * status change.
 *
 * Pass `undefined` or `null` for a row with no blob; the result then stays
 * empty. A change of digest or media type starts over.
 */
export function useBlob(
	ref: BlobRefLike | undefined | null,
	opts?: UseBlobOptions,
): UseBlobResult {
	const scope = useWorkspaceScope(opts?.workspaceId);
	const digest = ref?.digest;
	const size = ref?.size;
	const mediaType = ref?.mediaType;
	const identity = digest === undefined
		? ''
		: [ scope.workspaceId, digest, mediaType ?? '' ].join('\u0000');

	const [ state, setState ] = useState<BlobState>(() => empty(identity));

	useEffect(() => {
		if (digest === undefined) return;
		const target: BlobRefLike = { digest, size: size ?? 0n, mediaType: mediaType ?? '' };
		let cancelled = false;
		let url: string | undefined;
		let have = false;
		let reading = false;

		const read = (): void => {
			if (have || reading) return;
			reading = true;
			scope.blobs.get(target).then(
				(blob) => {
					reading = false;
					if (cancelled) return;
					have = true;
					url = typeof URL.createObjectURL === 'function' ? URL.createObjectURL(blob) : undefined;
					setState((s) => ({ ...s, identity, blob, url, error: undefined }));
				},
				(err: unknown) => {
					reading = false;
					if (cancelled) return;
					const error = err instanceof Error ? err : new Error(String(err));
					setState((s) => ({ ...s, identity, error }));
				},
			);
		};

		let unsub: (() => void) | undefined;
		try {
			unsub = scope.blobs.watch(target, (status) => {
				if (cancelled) return;
				setState((s) => ({ ...(s.identity === identity ? s : empty(identity)), status }));
				// Every state but pending has bytes somewhere this client can read.
				if (status.state !== 'pending') read();
			});
		} catch (err) {
			setState({ ...empty(identity), error: err instanceof Error ? err : new Error(String(err)) });
		}

		return () => {
			cancelled = true;
			unsub?.();
			if (url !== undefined) URL.revokeObjectURL(url);
		};
	}, [ scope, identity, digest, size, mediaType ]);

	const blank = useMemo(() => empty(identity), [ identity ]);
	const current = state.identity === identity ? state : blank;
	return useMemo<UseBlobResult>(() => ({
		status: current.status,
		blob: current.blob,
		url: current.url,
		error: current.error,
	}), [ current ]);
}
