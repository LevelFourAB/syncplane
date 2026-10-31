/** Receives what a {@link FrameChannel} delivers. */
export interface FrameReceiver {
	/** Called once for each message, in the order the peer sent them. */
	frame(frame: Uint8Array): void;
	/**
	 * Called once when the channel ends, for any reason, `close()` included.
	 * No frame follows it. `error` describes an end the channel did not ask
	 * for; it is absent after `close()`.
	 */
	closed(error?: Error): void;
}

/**
 * One message channel to a tunnel server: anything that carries whole binary
 * messages in order and without loss. {@link fromWebSocket},
 * {@link fromMessagePort} and {@link fromByteStream} adapt the common ones; a
 * host's own channel API needs an object with these three methods.
 *
 * The tunnel calls `start` once, before it sends anything. It does not use a
 * frame again after passing it to `send`, so an adapter may transfer the
 * frame's buffer.
 */
export interface FrameChannel {
	/** Begins delivering messages to `receiver`. */
	start(receiver: FrameReceiver): void;
	/**
	 * Sends one frame as one message. It must not block or throw. A frame sent
	 * before the connection under the channel opens goes out once it does; a
	 * frame sent after the channel ended is dropped.
	 */
	send(frame: Uint8Array): void;
	/** Ends the channel and calls the receiver's `closed`. Safe to call more than once. */
	close(): void;
}
