/**
 * The contract between the page harness (test/e2e/app/harness.ts, running in
 * the browser) and the specs (running in Node and reaching in through
 * `page.evaluate`). Everything crossing that boundary must be structured-clone
 * safe, so the API deals only in plain JSON — no protobuf messages, no bigint,
 * no handles.
 *
 * One tab holds exactly one SyncClient: tabs ARE the unit under test here, and
 * giving a page two clients would make "what does this account do across tabs"
 * meaningless.
 */

export interface OpenOptions {
	workspaceId: string;
	/**
	 * Storage scope. Defaults (as the client does) to the workspace id, so two
	 * tabs opened on the same workspace share one IndexedDB database, one KV
	 * mutex and one clientId — which is the whole point of this tier. Set it
	 * explicitly to give a tab a store of its own.
	 */
	accountId?: string;
	/** Hold the live Subscribe stream open. Default false. */
	streamEnabled?: boolean;
	/** Fallback poll interval; 0 (the default here) disables it. */
	pollIntervalMs?: number;
	/** Begin background sync right away. Default false. */
	start?: boolean;
	/**
	 * Count what the client commits to storage, split by keyspace (writeBytes).
	 * Off by default: it wraps the store the client would have picked anyway, but
	 * a tab that does not ask should exercise the untouched default.
	 */
	measureWrites?: boolean;
	/**
	 * Which entities this tab's build registers. Defaults to all of them; a tab
	 * that asks for a subset IS an older build of the app, sharing the store and
	 * the pending queue with newer tabs that register more.
	 */
	entities?: ('todo' | 'widget')[];
}

export interface TodoView {
	id: string;
	title: string;
	completed: boolean;
}

export interface StatusView {
	state: string;
	online: boolean;
	dirty: boolean;
	pendingMutations: number;
}

/**
 * Bytes the client committed to storage over some window, split by keyspace
 * (see src/keys.ts for what each prefix holds). `total` covers every keyspace,
 * including any the named ones do not; `commits` is how many transactions the
 * total was spread across.
 */
export interface WriteBytesView {
	overlay: number;
	overlayText: number;
	pending: number;
	confirmed: number;
	staging: number;
	meta: number;
	total: number;
	commits: number;
}

/**
 * A resolved mark span in UTF-16 indices — the client's `TextSpan` narrowed to
 * string values, which is all this boundary can carry and all the specs set.
 */
export interface MarkView {
	start: number;
	end: number;
	key: string;
	value: string;
}

/**
 * One participant of a room, narrowed to what crosses the page boundary. The
 * app state is a plain string here, which is all the specs publish.
 */
export interface PeerView {
	sessionId: string;
	subject: string;
	state: string | undefined;
	self: boolean;
}

export interface HarnessApi {
	/** Creates (and optionally starts) this tab's client. Returns its clientId. */
	open(opts: OpenOptions): Promise<string>;
	/** Begins background sync. */
	start(): Promise<void>;
	/** Runs one sync cycle to completion. */
	syncNow(): Promise<void>;
	/** Closes the client and stops watching. Idempotent. */
	close(): Promise<void>;
	/** The store's client id — shared by every tab on the same account. */
	clientId(): Promise<string>;

	putTodo(id: string, title: string): Promise<void>;
	/** Enqueues every id concurrently, to maximise interleaving inside the tab. */
	putTodos(ids: string[]): Promise<void>;
	patchTodo(id: string, fields: Partial<Omit<TodoView, 'id'>>): Promise<void>;
	getTodo(id: string): Promise<TodoView | null>;
	/** Every todo id in the local store, sorted. */
	todoIds(): Promise<string[]>;

	/** Starts recording watcher deliveries. Call before the write under test. */
	watchTodos(): Promise<void>;
	/** Union of the todo ids this tab's watcher has ever been handed, sorted. */
	watched(): Promise<string[]>;
	/** How many times the watcher has fired since watchTodos(). */
	notifications(): Promise<number>;

	/**
	 * Creates the row a text handle needs — a Widget whose `body` is a
	 * collaborative text field, starting empty. PATCH never upserts, so the row
	 * has to exist before a handle can open on it.
	 */
	createTextRow(id: string): Promise<void>;
	/** Opens a live handle on `body`; returns the number the spec refers to it by. */
	openText(id: string): Promise<number>;
	/**
	 * The writer identity that handle leased. Distinct per live handle — across
	 * tabs and within one — which is the invariant this tier exists to check,
	 * since Node has no shared lock manager to break.
	 */
	textPeer(handle: number): Promise<string>;
	textValue(handle: number): Promise<string>;
	insertText(handle: number, index: number, s: string): Promise<void>;
	/** Rewrites the whole value through the multi-hunk diff (whole-value writers). */
	setTextString(handle: number, s: string): Promise<void>;
	/** Applies key=value over [start, end) so a spec can watch a mark travel. */
	markText(handle: number, start: number, end: number, key: string, value: string): Promise<void>;
	/** The live document's resolved mark spans. */
	textMarks(handle: number): Promise<MarkView[]>;
	/** Closes the handle, returning its slot to the device's pool. */
	closeText(handle: number): Promise<void>;

	/**
	 * Rooms — the ephemeral plane. Handles are numbers for the same reason text
	 * handles are: a Room is not structured-clone safe, so nothing but JSON
	 * crosses the page boundary.
	 */
	joinRoom(name: string): Promise<number>;
	/** This tab's presence session id, which its own record is keyed on. */
	sessionId(): Promise<string>;
	setPresence(room: number, state: string): Promise<void>;
	sendRoomMessage(room: number, message: string): Promise<void>;
	/** The room's roster, ordered by session id. */
	roomPeers(room: number): Promise<PeerView[]>;
	/** Broadcast messages this room has received, oldest first. */
	roomMessages(room: number): Promise<string[]>;
	/** Leaves the room, withdrawing this session's record. */
	leaveRoom(room: number): Promise<void>;
	/** The stored document's visible text, read fresh from the local store. */
	storedText(id: string): Promise<string>;
	/** The stored document's mark spans, projected without opening a handle. */
	storedMarks(id: string): Promise<MarkView[]>;
	/** The peers in the stored document's version vector, sorted. */
	storedPeers(id: string): Promise<string[]>;
	/** Live tombstone count of the stored document — what compaction reclaims. */
	storedTombstones(id: string): Promise<number>;

	/**
	 * String writes at the mutation boundary: the value is a plain string, and
	 * the client diffs it into CRDT edits under a leased writer identity. PUT
	 * creates the row (and unions text with whatever is stored), PATCH requires
	 * it to exist.
	 */
	putTextString(id: string, s: string): Promise<void>;
	patchTextString(id: string, s: string): Promise<void>;
	/**
	 * Writes a non-text field of the same row — the case where the text field
	 * has not moved and the pull should say so rather than re-ship the document.
	 */
	patchWidgetLabel(id: string, label: string): Promise<void>;

	/**
	 * The visible text of the CONFIRMED row (`c/…`) as it sits in IndexedDB,
	 * bypassing the optimistic overlay. Downstream delta state is folded into
	 * that row, so this is what says the fold produced a whole document rather
	 * than the overlay covering for it. Null when the row has not been
	 * confirmed yet.
	 */
	confirmedText(id: string): Promise<string | null>;

	/**
	 * Names of the IndexedDB databases this origin holds. A tier that silently
	 * degraded to MemoryKVStore would still pass most of the suite, so at least
	 * one spec checks the store really is on disk under `sync:<accountId>`.
	 */
	databases(): Promise<string[]>;

	pendingCount(): Promise<number>;
	/**
	 * Bytes the pending queue occupies in IndexedDB. What a keystroke costs on
	 * disk is only observable here — the queue is private to the client, and a
	 * fake store would not answer the question the measurement is about.
	 */
	pendingBytes(): Promise<number>;
	/**
	 * Bytes committed to storage since the last resetWriteBytes, per keyspace.
	 * Unlike pendingBytes this is a FLOW, not a level: an overlay row is
	 * overwritten by the next write to the same row, so what it cost is only
	 * visible as it goes past. Requires open({ measureWrites: true }).
	 */
	writeBytes(): Promise<WriteBytesView>;
	/** Zeroes the write counters, so a spec measures one operation. */
	resetWriteBytes(): Promise<void>;
	status(): Promise<StatusView>;
	/** Messages from onMutationRejected — expected to stay empty. */
	rejections(): Promise<string[]>;

	/**
	 * Overrides `document.visibilityState` and fires `visibilitychange`, which
	 * is exactly the pair stream leadership reacts to. Playwright cannot put a
	 * page into a real background state that all engines agree on, so the suite
	 * drives the API the client actually reads.
	 */
	setVisibility(state: 'visible' | 'hidden'): Promise<void>;
}

declare global {
	interface Window {
		harness: HarnessApi;
	}
}
