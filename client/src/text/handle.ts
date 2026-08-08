// Live handle over a collaborative text field of one row — the client API
// boundary where JS string (UTF-16) indices convert to the code-point indices
// the CRDT uses. Edits apply to a local TextDoc immediately, then ship as an
// ordinary PATCH carrying the delta-state fragment for that one edit; remote
// changes arrive through the row watch and are merged in, with an item-graph
// delta delivered to subscribers so editor bindings can patch in place.
//
// Each edit records the items it minted or tombstoned as it applies them, so
// encoding the fragment costs the size of the edit rather than the size of the
// document — the difference between a keystroke writing a byte or two to
// IndexedDB and writing the whole document twice.

import { CodeTextMergeFailed } from '../codes.js';
import { reportError, safeInvoke, safeRun } from '../report.js';
import { decodeTextDoc, encodeTextDoc } from './codec.js';
import { purgedByRemote } from './compact.js';
import {
	type Anchor,
	AnchorAfter,
	AnchorBefore,
	codePointOffsets,
	codePointToUtf16,
	type DeletedSegment,
	type Expand,
	ExpandNone,
	type InsertedRange,
	type ItemID,
	type MarkValue,
	type PendingMark,
	TextDoc,
	utf16ToCodePoint,
} from './doc.js';
import { encodeFragmentFrom } from './fragment.js';
import { diffText, mergeInto, type TextDelta } from './merge.js';
import type { PeerLease } from './peer.js';
import { recordMarkInverse, reinsertOp, type TextInverseOp } from './undo-ops.js';

/**
 * Why a handle stopped merging.
 *
 * Terminal by nature: every producer of it means this document and the state it
 * was handed cannot be reconciled, and re-reading the same row would hand it
 * the same bytes. The handle keeps serving reads (`value`, `marks`, anchors) so
 * the app can salvage what the user was looking at, but the text is stale from
 * then on and further edits throw. Recovery is to close the handle and open a
 * new one, which builds a fresh document from the stored row.
 */
export interface TextHandleError {
	message: string;
	code: typeof CodeTextMergeFailed;
	/** The exception that caused it, for logs. */
	cause: unknown;
}

/** Lifecycle of a handle (see TextHandle.status). */
export interface TextHandleStatus {
	/**
	 * `open` — live. `failed` — stopped merging, see `error`. `closed` — the app
	 * closed it; a handle that failed and was then closed reports `closed` and
	 * keeps its `error`.
	 */
	state: 'open' | 'failed' | 'closed';
	/** Set exactly when the handle failed, whether or not it was closed after. */
	error?: TextHandleError;
}

/** A resolved mark span in UTF-16 string indices. */
export interface TextSpan {
	start: number;
	end: number;
	key: string;
	value: Exclude<MarkValue, null>;
}

/** A resolved zero-width mark, at a UTF-16 string index. */
export interface TextPoint {
	index: number;
	key: string;
	value: Exclude<MarkValue, null>;
}

/**
 * Live view of one text field. All indices are UTF-16 code units (plain JS
 * string indices); conversion to the CRDT's code points happens inside.
 */
export interface TextHandle {
	/** Current visible text. */
	readonly value: string;
	/** The underlying document (code-point indexed) for advanced use. */
	readonly doc: TextDoc;
	/** Inserts text at a UTF-16 index. */
	insert(index: number, text: string): Promise<void>;
	/** Deletes length UTF-16 units starting at index. */
	delete(index: number, length: number): Promise<void>;
	/** Rewrites the whole value with a minimal diff (whole-value writers). */
	setString(text: string): Promise<void>;
	/**
	 * Applies key=value over [start, end); returns the mark op id.
	 *
	 * start === end is a collapsed mark: with ExpandBoth it is a pending
	 * format that the next character typed at the position takes on (and that
	 * marksAt reports until then), with ExpandNone a note attached to the
	 * position itself (which points reports). See TextDoc.mark.
	 */
	mark(
		start: number,
		end: number,
		key: string,
		value: Exclude<MarkValue, null>,
		expand?: Expand,
	): Promise<ItemID>;
	/** Removes key over [start, end); start === end cancels a collapsed mark. */
	unmark(start: number, end: number, key: string, expand?: Expand): Promise<ItemID>;
	/** Resolved mark spans over the current value. */
	marks(): TextSpan[];
	/**
	 * What text typed at a UTF-16 index would be marked with — the active
	 * formatting for a caret. Reports pending formats, which cover no
	 * character and so never appear in marks().
	 */
	marksAt(index: number): PendingMark[];
	/** Resolved zero-width marks over the current value. */
	points(): TextPoint[];
	/** A stable anchor at a caret position (defaults to sticking rightward). */
	anchorAt(index: number, kind?: typeof AnchorBefore | typeof AnchorAfter): Anchor;
	/** Resolves an anchor back to a UTF-16 index in the current value. */
	resolve(anchor: Anchor): number;
	/**
	 * Subscribes to changes. Local edits deliver their own exact delta;
	 * remote changes deliver the item-graph diff, so positions survive. Ops
	 * are in UTF-16 units over the pre-change value.
	 */
	subscribe(cb: (delta: TextDelta, value: string) => void): () => void;
	/** Whether the handle is still live, and why it is not (see TextHandleStatus). */
	status(): TextHandleStatus;
	/**
	 * Subscribes to the handle's terminal failure. Fires at most once — the
	 * condition never clears — and immediately if the handle has already failed,
	 * so a listener registered late still learns about it.
	 */
	onError(cb: (err: TextHandleError) => void): () => void;
	/**
	 * Stops watching the row and returns this handle's writer identity. Edits
	 * after close throw.
	 *
	 * PATCHes queued before the close still go out: they carry edits the user
	 * made, and dropping them would lose text. The returned promise resolves once
	 * they have been enqueued — `await handle.close()` is the quiescence point —
	 * and never rejects, because a failed write is reported to whoever made the
	 * edit.
	 */
	close(): Promise<void>;
}

/** Wiring the handle needs from the client (see SyncClient.textHandleIn). */
export interface TextHandleDeps {
	/**
	 * Leases this handle's writer identity, released by close(). It must be
	 * exclusive to this one document instance for as long as the handle lives —
	 * a peer shared with another tab, or with a second handle on the same field,
	 * mints colliding item ids (see text/peer.ts).
	 */
	acquirePeer(): Promise<PeerLease>;
	/** Reads the field's current bytes (undefined = row missing). */
	load(): Promise<Uint8Array | undefined>;
	/**
	 * Enqueues a PATCH carrying the edit's new state for the field: a
	 * delta-state fragment covering just this edit, or — when the handle cannot
	 * describe the change as a delta of what it last pushed — the whole
	 * document. Either applies to the stored row as the same union (see
	 * text/merge.ts), so the queue holds O(edit) bytes per keystroke while
	 * rows, the overlay and every read path keep whole documents.
	 *
	 * inverseOps is the symbolic op-level undo record for this edit; coalesce
	 * joins it onto the previous undo group (typing).
	 */
	patch(state: Uint8Array, inverseOps?: TextInverseOp[], coalesce?: boolean): Promise<void>;
	/** Watches the row's field bytes; fires on every row change. */
	watch(cb: (state: Uint8Array | undefined) => void): () => void;
}

/** Consecutive typing edits within this window coalesce into one undo group
 * (a whitespace insert ends the chain — roughly one group per word). */
const COALESCE_WINDOW_MS = 1000;

/** Converts a code-point delta to UTF-16 units against the pre-change text. */
export function deltaToUtf16(prevText: string, delta: TextDelta): TextDelta {
	const cps = [ ...prevText ];
	let cp = 0;
	const ops = delta.ops.map((op) => {
		if ('insert' in op) return { insert: op.insert };
		if ('retain' in op) {
			const units = cps.slice(cp, cp + op.retain).join('').length;
			cp += op.retain;
			return { retain: units };
		}
		const units = cps.slice(cp, cp + op.delete).join('').length;
		cp += op.delete;
		return { delete: units };
	});
	return { ops, marksChanged: delta.marksChanged };
}

/**
 * What one edit changed, in the terms a fragment needs: the items it minted or
 * tombstoned and the mark ops it added. Recorded as the edit applies, which is
 * the only moment it is known without walking the document — a tombstone mints
 * no id, so nothing about the resulting state reveals which items flipped.
 */
interface RecordedEdit {
	items: ItemID[];
	marks: ItemID[];
}

/** Ids of the items one insert minted (consecutive counters). */
function insertedIds(range: InsertedRange): ItemID[] {
	const out: ItemID[] = [];
	for (let c = range.start; c < range.start + range.count; c++) {
		out.push({ peer: range.peer, counter: c });
	}
	return out;
}

/** Ids of the items a set of deleted segments tombstoned. */
function deletedIds(segments: readonly DeletedSegment[]): ItemID[] {
	const out: ItemID[] = [];
	for (const s of segments) out.push(...s.ids);
	return out;
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
	if (a.length !== b.length) return false;
	for (let i = 0; i < a.length; i++) {
		if (a[i] !== b[i]) return false;
	}
	return true;
}

/** Opens a live handle. The row must exist (PATCH is not an upsert). */
export async function openTextHandle(deps: TextHandleDeps): Promise<TextHandle> {
	// One lease per handle, held until close(). Two handles over the same field
	// — two tabs, or an editor binding mounted twice — build two independent
	// documents, and two documents sharing a peer allocate colliding item ids.
	//
	// The lease is taken BEFORE the row is read, not after. A free slot is
	// recycled immediately (text/peer.ts), and what makes that safe is that the
	// next holder builds its document from a store already holding the previous
	// holder's writes. Reading first leaves a window in which the previous holder
	// writes and releases between the two, leaving this handle to speak its peer
	// through a document that has not seen those writes — and to allocate the
	// counters they used.
	const lease = await deps.acquirePeer();
	try {
		const initial = await deps.load();
		if (initial === undefined) {
			throw new Error('text: row not found — create the row before opening a text handle');
		}
		return buildHandle(deps, initial, lease);
	} catch (err) {
		lease.release();
		throw err;
	}
}

function buildHandle(deps: TextHandleDeps, initial: Uint8Array, lease: PeerLease): TextHandle {
	const doc = decodeTextDoc(initial, lease.peer);

	const subscribers = new Set<(delta: TextDelta, value: string) => void>();
	const errorListeners = new Set<(err: TextHandleError) => void>();
	let closed = false;
	let failure: TextHandleError | undefined;
	// Local edits chain so their PATCHes enqueue in order even when callers
	// fire without awaiting.
	let writeChain: Promise<void> = Promise.resolve();
	// Typing-coalescence state: whether the previous edit left an open chain
	// and when it happened.
	let chainOpen = false;
	let lastEditAt = 0;
	/**
	 * The most recent state already folded into `doc` — what this handle last
	 * merged in, or last pushed as full state. Merging is a union, so a row
	 * change carrying exactly these bytes cannot add anything and is skipped.
	 *
	 * It exists because the row watch fires on every change to ANY field of the
	 * row, including the overlay write this handle's own keystroke just made, and
	 * answering "is there anything new here?" by re-encoding the whole document
	 * was the largest single cost of a keystroke. Undefined means "not known" —
	 * a miss only falls through to that encode, so it costs time and never
	 * correctness.
	 */
	let folded: Uint8Array | undefined = initial;

	const emit = (delta: TextDelta, prevText: string): void => {
		if (delta.ops.length === 0 && !delta.marksChanged) return;
		const d16 = deltaToUtf16(prevText, delta);
		const value = doc.plainText();
		// Isolated: emit runs both from local edits (before the PATCH is queued —
		// the doc has already changed, so a throw would strand the edit unwritten)
		// and from the row watch. One broken editor binding must not do that, nor
		// hide the change from the other subscribers.
		for (const cb of [ ...subscribers ]) {
			safeRun(() => cb(d16, value), 'text subscribe callback');
		}
	};

	/**
	 * Queues one edit. `edit` describes it as a delta of the state this handle
	 * has already pushed, which is what keeps a keystroke's queue entry the size
	 * of the keystroke; pass undefined to send the whole document, for edits
	 * that are not such a delta (a post-compaction rebuild).
	 */
	const push = (
		edit: RecordedEdit | undefined,
		inverseOps?: TextInverseOp[],
		typing?: { keepChainOpen: boolean },
	): Promise<void> => {
		let state: Uint8Array;
		if (edit === undefined) {
			state = encodeTextDoc(doc);
			// Full state is exactly what the row watch will report back.
			folded = state;
		} else {
			state = encodeFragmentFrom(doc, edit.items, edit.marks);
			// The row this write lands in will hold the document the fragment
			// expands to, which is this doc — but naming those bytes means
			// encoding it, the O(document) cost the fragment exists to avoid. The
			// watch falls back to encoding once, on its next fire, and a miss here
			// only ever costs time (see `folded`).
			folded = undefined;
		}
		const coalesce = typing !== undefined && chainOpen
			&& Date.now() - lastEditAt <= COALESCE_WINDOW_MS;
		chainOpen = typing?.keepChainOpen ?? false;
		lastEditAt = Date.now();
		const ops = inverseOps && inverseOps.length > 0 ? inverseOps : undefined;
		writeChain = writeChain.then(() => deps.patch(state, ops, coalesce));
		return writeChain;
	};

	const assertOpen = (): void => {
		if (closed) throw new Error('text: handle is closed');
		if (failure !== undefined) {
			throw new Error(`${failure.message} — close this handle and open a new one`);
		}
	};

	/**
	 * Puts the handle into its terminal failed state: it stops merging, refuses
	 * edits and tells whoever is listening. Only the first failure is recorded —
	 * a handle that has already given up has nothing to add.
	 *
	 * The lease is deliberately NOT released here. A failed handle still answers
	 * reads out of a document holding items minted under that peer, and the slot
	 * must not go to a second handle while it does (see text/peer.ts); close()
	 * returns it like it does for any other handle.
	 */
	const fail = (stage: string, err: unknown): void => {
		if (failure !== undefined) return;
		const detail = err instanceof Error ? err.message : String(err);
		failure = {
			message: `text: handle stopped merging while ${stage}: ${detail}`,
			code: CodeTextMergeFailed,
			cause: err,
		};
		// Nothing further will be merged, so the watch is dead weight.
		unwatch?.();
		// Reported even with no listener: a handle that has stopped converging
		// looks exactly like an idle one from the outside.
		reportError(failure.message, err);
		for (const cb of [ ...errorListeners ]) {
			safeInvoke(cb, failure, 'text handle onError listener');
		}
	};

	// Assigned by the registration below; the watch may in principle fire during
	// it, so fail() and close() reach it optionally.
	let unwatch: (() => void) | undefined;
	unwatch = deps.watch((state) => {
		if (closed || failure !== undefined || state === undefined) return;
		// Everything mergeRemote does — decoding the incoming bytes, unioning
		// them in, diffing the result, rebuilding onto a compacted state —
		// throws on input this document cannot be reconciled with: a row an
		// older or buggy client wrote, state that predates a compaction, ids
		// minted twice. This callback runs inside the store's watcher notify
		// loop, which has no way to absorb that; the throw would escape into
		// whatever else that loop was about to notify, on a path with no promise
		// to reject. The failure is this handle's alone, so it stops here.
		try {
			mergeRemote(state);
		} catch (err) {
			fail('merging a remote change', err);
		}
	});
	if (failure !== undefined) unwatch();

	/**
	 * Folds one row change into the live document. Callers must treat a throw as
	 * terminal (see fail): a merge that fails partway has already put some of the
	 * incoming items into `doc`, so the document is no longer a state anything
	 * can be diffed against — only read.
	 */
	function mergeRemote(state: Uint8Array): void {
		if (folded !== undefined && bytesEqual(state, folded)) return;
		const cur = encodeTextDoc(doc);
		folded = cur;
		if (bytesEqual(state, cur)) return;
		// Merge the incoming state in (a union — safe even when the watch
		// lags behind local edits) and report what changed as a delta. The
		// snapshot is a diff base only, never edited, so it takes no peer.
		const prev = decodeTextDoc(cur, '');
		const prevText = prev.plainText();
		const incoming = decodeTextDoc(state, '');
		// Recorded before both of the calls below, never after: each of them can
		// re-enter this handle — rebuildOnCompaction pushes, and emit runs
		// subscriber callbacks that may edit — and whatever they record about the
		// document is newer than this.
		folded = state;
		if (purgedByRemote(doc, incoming)) {
			rebuildOnCompaction(incoming, prev, prevText);
			return;
		}
		mergeInto(doc, incoming);
		emit(diffText(prev, doc), prevText);
	}

	/**
	 * Rebases the live document onto state the server has compacted (see
	 * text/compact.ts). A union would keep the characters the server dropped and
	 * every subsequent write would be refused as stale, so the document is
	 * rebuilt from the incoming state and the local visible text re-applied on
	 * top as a fresh minimal edit.
	 *
	 * The visible text is what survives; mark operations this handle minted over
	 * dropped characters do not. Nothing visible changes, so subscribers get an
	 * empty delta that only asks them to re-read the marks. The rebuilt state is
	 * pushed because the edits it re-expresses were queued against ids the
	 * server no longer has: those PATCHes are being rejected, and this is what
	 * replaces them.
	 */
	function rebuildOnCompaction(incoming: TextDoc, prev: TextDoc, prevText: string): void {
		doc.replaceWith(incoming);
		const { deleted, inserted } = doc.setString(prevText);
		emit({ ops: [], marksChanged: prev.marks.length > 0 || doc.marks.length > 0 }, prevText);
		if (deleted.length > 0 || inserted.length > 0) {
			// Full state, not a fragment: the document was just rebuilt onto a
			// compacted state, so this is not a delta of anything the server can
			// still be holding — the very ids a fragment would build on are what
			// it purged.
			//
			// No undo record: this restores an edit the user already made, so it
			// must not push a second entry onto any stack. Nobody awaits this one,
			// so its failure is reported rather than left as an unhandled
			// rejection — the document is still correct, it is the queue that
			// refused, and the next edit rides the same chain.
			void push(undefined).catch((err: unknown) => {
				reportError('text handle could not queue its post-compaction rebase', err);
			});
		}
	}

	return {
		get value(): string {
			return doc.plainText();
		},
		doc,

		insert(index, text) {
			assertOpen();
			if (text === '') return Promise.resolve();
			const prevText = doc.plainText();
			const cp = utf16ToCodePoint(prevText, index);
			const range = doc.insert(cp, text);
			emit({
				ops: cp > 0 ? [ { retain: cp }, { insert: text } ] : [ { insert: text } ],
				// Same rule diffText applies: an insert can extend or split a
				// resolved span, so a marked document must re-resolve.
				marksChanged: doc.marks.length > 0,
			}, prevText);
			return push(
				{ items: insertedIds(range), marks: [] },
				[ { type: 'deleteItems', peer: range.peer, start: range.start, count: range.count } ],
				// A whitespace insert ends the word: it still joins the current
				// undo group, but the next keystroke starts a fresh one.
				{ keepChainOpen: !/\s/.test(text) },
			);
		},

		delete(index, length) {
			assertOpen();
			if (length === 0) return Promise.resolve();
			const prevText = doc.plainText();
			const cpStart = utf16ToCodePoint(prevText, index);
			const cpEnd = utf16ToCodePoint(prevText, index + length);
			const segments = doc.delete(cpStart, cpEnd - cpStart);
			emit({
				ops: cpStart > 0
					? [ { retain: cpStart }, { delete: cpEnd - cpStart } ]
					: [ { delete: cpEnd - cpStart } ],
				// A delete can shrink a resolved span to nothing, so a marked
				// document must re-resolve (see diffText).
				marksChanged: doc.marks.length > 0,
			}, prevText);
			return push(
				{ items: deletedIds(segments), marks: [] },
				segments.map(reinsertOp),
				{ keepChainOpen: true },
			);
		},

		setString(text) {
			assertOpen();
			const prevEnc = encodeTextDoc(doc);
			const prev = decodeTextDoc(prevEnc, ''); // diff base only
			const prevText = prev.plainText();
			const { deleted, inserted } = doc.setString(text);
			const delta = diffText(prev, doc);
			emit(delta, prevText);
			// Both arrays arrive in application order (hunks right to left).
			// Inverses keep that order: inserted ids are deleted by id, and
			// reinsert targets are anchors, so no step invalidates another's
			// position.
			const inverse: TextInverseOp[] = [];
			const items: ItemID[] = [];
			for (const range of inserted) {
				if (range.count > 0) {
					inverse.push({
						type: 'deleteItems',
						peer: range.peer,
						start: range.start,
						count: range.count,
					});
					items.push(...insertedIds(range));
				}
			}
			for (const s of deleted) inverse.push(reinsertOp(s));
			items.push(...deletedIds(deleted));
			// setString diffs minimally, so a whole-value writer that rewrites the
			// field on every keystroke still queues one keystroke's worth of state.
			return push({ items, marks: [] }, inverse, {
				// Same word rule insert applies, read off the diff rather than the
				// argument: the caller passes the whole value, which almost always
				// contains whitespace that has nothing to do with this edit.
				keepChainOpen: !delta.ops.some((op) => 'insert' in op && /\s/.test(op.insert)),
			});
		},

		async mark(start, end, key, value, expand = ExpandNone) {
			assertOpen();
			const text = doc.plainText();
			const cpStart = utf16ToCodePoint(text, start);
			const cpEnd = utf16ToCodePoint(text, end);
			// Capture what this op overwrites BEFORE applying it.
			const inverse = recordMarkInverse(doc, cpStart, cpEnd, key);
			const id = doc.mark(cpStart, cpEnd, key, value, expand);
			emit({ ops: [], marksChanged: true }, text);
			// Only the op itself: its anchors name items the receiver already has,
			// since they were resolved against a document this handle built on
			// state it has pushed.
			await push({ items: [], marks: [ id ] }, [ inverse ]);
			return id;
		},

		async unmark(start, end, key, expand = ExpandNone) {
			assertOpen();
			const text = doc.plainText();
			const cpStart = utf16ToCodePoint(text, start);
			const cpEnd = utf16ToCodePoint(text, end);
			const inverse = recordMarkInverse(doc, cpStart, cpEnd, key);
			const id = doc.unmark(cpStart, cpEnd, key, expand);
			emit({ ops: [], marksChanged: true }, text);
			await push({ items: [], marks: [ id ] }, [ inverse ]);
			return id;
		},

		marks() {
			const text = doc.plainText();
			// One offset table for the whole call: converting each boundary with
			// codePointToUtf16 would rescan the text per span, which a document
			// with many marks (one key per comment) pays for twice per span.
			const offsets = codePointOffsets(text);
			return doc.resolveMarks().map((s) => ({
				start: offsets[s.start]!,
				end: offsets[s.end]!,
				key: s.key,
				value: s.value,
			}));
		},

		marksAt(index) {
			return doc.marksAt(utf16ToCodePoint(doc.plainText(), index));
		},

		points() {
			const offsets = codePointOffsets(doc.plainText());
			return doc.points().map((p) => ({ index: offsets[p.index]!, key: p.key, value: p.value }));
		},

		anchorAt(index, kind = AnchorAfter) {
			return doc.anchorAt(utf16ToCodePoint(doc.plainText(), index), kind);
		},

		resolve(anchor) {
			return codePointToUtf16(doc.plainText(), doc.resolveAnchor(anchor));
		},

		subscribe(cb) {
			subscribers.add(cb);
			return () => {
				subscribers.delete(cb);
			};
		},

		status() {
			if (closed) return { state: 'closed', error: failure };
			return failure === undefined ? { state: 'open' } : { state: 'failed', error: failure };
		},

		onError(cb) {
			if (failure !== undefined) {
				// The failure has already fired; a listener that only ever hears
				// about future ones would never hear about anything.
				safeInvoke(cb, failure, 'text handle onError listener');
				return () => {};
			}
			errorListeners.add(cb);
			return () => {
				errorListeners.delete(cb);
			};
		},

		close() {
			if (!closed) {
				closed = true;
				subscribers.clear();
				errorListeners.clear();
				unwatch?.();
				// The peer slot goes back to the pool once these writes are in the
				// store, not now: the next handle to take it builds its document from
				// that store, and a document missing them would allocate the counters
				// they already used. Attached here rather than to the promise below so
				// that a second close() does not free the slot a second time.
				void writeChain.then(() => lease.release(), () => lease.release());
			}
			// The chain is drained, not cut: its links are PATCHes for edits the
			// user already made, and a closed handle is not a reason to drop them.
			// Rejections belong to the caller that made the edit (push hands them
			// the same promise), so closing never reports one.
			return writeChain.then(() => {}, () => {});
		},
	};
}
