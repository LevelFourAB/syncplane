import {
	commentKey,
	decodeTextDoc,
	encodeTextDoc,
	ExpandNone,
	generateKeyBetween,
	joinAnnotations,
	newId,
	subscription,
	SyncClient,
	type SyncStatus,
	type TextDelta,
	TextDoc,
	type TextHandle,
	utf16ToCodePoint,
} from 'syncplane';
import {
	type Annotation,
	AnnotationSchema,
	NoteSchema,
	type Todo,
	TodoSchema,
} from 'syncplane/gen/example/todo/v1/todo_pb.js';

const SERVER_URL = import.meta.env.VITE_SYNC_URL ?? 'http://127.0.0.1:8787';

const client = new SyncClient({
	url: SERVER_URL,
	workspaceId: 'demo',
	entities: [ TodoSchema, NoteSchema, AnnotationSchema ],
	// In-memory undo/redo over local mutations; inverses push like any edit.
	undo: {},
	onMutationRejected: (info) => {
		toast(`Server rejected a change: ${info.errorMessage}`);
	},
});

const form = document.querySelector<HTMLFormElement>('#new-todo-form')!;
const input = document.querySelector<HTMLInputElement>('#new-todo')!;
const list = document.querySelector<HTMLUListElement>('#list')!;
const empty = document.querySelector<HTMLParagraphElement>('#empty')!;
const count = document.querySelector<HTMLSpanElement>('#count')!;
const telemetry = document.querySelector<HTMLSpanElement>('#telemetry')!;
const statusDot = document.querySelector<HTMLSpanElement>('#status-dot')!;
const statusText = document.querySelector<HTMLSpanElement>('#status-text')!;
const undoBtn = document.querySelector<HTMLButtonElement>('#undo')!;
const redoBtn = document.querySelector<HTMLButtonElement>('#redo')!;
const completeAll = document.querySelector<HTMLButtonElement>('#complete-all')!;
const syncActive = document.querySelector<HTMLInputElement>('#sync-active')!;
const scopeNote = document.querySelector<HTMLParagraphElement>('#scope-note')!;
const tabs = [ ...document.querySelectorAll<HTMLButtonElement>('#filters button') ];
const toastEl = document.querySelector<HTMLDivElement>('#toast')!;

let toastTimer: number | undefined;
function toast(message: string): void {
	toastEl.textContent = message;
	toastEl.hidden = false;
	clearTimeout(toastTimer);
	toastTimer = window.setTimeout(() => (toastEl.hidden = true), 4000);
}

// ---- ordering (fractional indexing) ---------------------------------------

/** List order: sort_key string compare; legacy rows without one sort first
 * by creation time. Reordering only ever patches one row's sort_key. */
function ordered(todos: Todo[]): Todo[] {
	return [ ...todos ].sort((a, b) => {
		if (a.sortKey !== b.sortKey) {
			if (!a.sortKey) return -1;
			if (!b.sortKey) return 1;
			return a.sortKey < b.sortKey ? -1 : 1;
		}
		return Number(a.createdAtMs - b.createdAtMs);
	});
}

const keyOrNull = (todo: Todo | undefined): string | null => todo?.sortKey || null;

// ---- state + rendering -----------------------------------------------------

type Filter = 'all' | 'active' | 'completed';

let allTodos: Todo[] = [];
let filter: Filter = 'all';
let draggedId: string | undefined;

function visibleTodos(): Todo[] {
	const sorted = ordered(allTodos);
	if (filter === 'active') return sorted.filter((t) => !t.completed);
	if (filter === 'completed') return sorted.filter((t) => t.completed);
	return sorted;
}

const EMPTY_COPY: Record<Filter, string> = {
	all: 'no todos yet — add one above',
	active: 'nothing active · inbox zero',
	completed: 'nothing completed yet',
};

// IDs visible in the previous render — used to animate only rows that are
// new to the visible set, not every row on every rebuild.
let visibleIds = new Set<string>();

function render(): void {
	const todos = visibleTodos();
	const nextVisible = new Set<string>();
	list.replaceChildren(...todos.map((todo, i) => {
		const li = renderTodo(todo, todos, i);
		if (!visibleIds.has(todo.id)) li.classList.add('enter');
		nextVisible.add(todo.id);
		return li;
	}));
	visibleIds = nextVisible;
	list.hidden = todos.length === 0;
	empty.hidden = todos.length !== 0;
	empty.textContent = EMPTY_COPY[filter];
	for (const tab of tabs) {
		tab.classList.toggle('active', tab.dataset.filter === filter);
	}
	const active = allTodos.filter((t) => !t.completed).length;
	count.textContent = `${active} active`;
	refreshControls();
}

function renderTodo(todo: Todo, visible: Todo[], index: number): HTMLLIElement {
	const li = document.createElement('li');
	li.className = todo.completed ? 'done' : '';
	li.draggable = true;
	li.dataset.id = todo.id;

	const handle = document.createElement('span');
	handle.className = 'handle';
	handle.textContent = '⠿';
	handle.title = 'Drag to reorder';

	const checkbox = document.createElement('input');
	checkbox.type = 'checkbox';
	checkbox.checked = todo.completed;
	checkbox.addEventListener('change', () => {
		void client.db.todo.mutate.patch(todo.id, { completed: checkbox.checked });
	});

	const title = document.createElement('span');
	title.className = 'title';
	title.textContent = todo.title;
	title.title = 'Double-click to edit';
	title.addEventListener('dblclick', () => startEdit(li, title, todo));

	const remove = document.createElement('button');
	remove.className = 'remove';
	remove.textContent = '×';
	remove.title = 'Delete';
	remove.setAttribute('aria-label', 'Delete');
	remove.addEventListener('click', () => {
		void client.db.todo.mutate.del(todo.id);
	});

	li.append(handle, checkbox, title, remove);

	// ---- drag & drop reorder -------------------------------------------------
	li.addEventListener('dragstart', (e) => {
		draggedId = todo.id;
		li.classList.add('dragging');
		e.dataTransfer?.setData('text/plain', todo.id);
	});
	li.addEventListener('dragend', () => {
		draggedId = undefined;
		li.classList.remove('dragging');
		list.querySelectorAll('.drop-above, .drop-below').forEach((el) => {
			el.classList.remove('drop-above', 'drop-below');
		});
	});
	li.addEventListener('dragover', (e) => {
		if (!draggedId || draggedId === todo.id) return;
		e.preventDefault();
		const rect = li.getBoundingClientRect();
		const above = e.clientY < rect.top + rect.height / 2;
		li.classList.toggle('drop-above', above);
		li.classList.toggle('drop-below', !above);
	});
	li.addEventListener('dragleave', () => {
		li.classList.remove('drop-above', 'drop-below');
	});
	li.addEventListener('drop', (e) => {
		e.preventDefault();
		if (!draggedId || draggedId === todo.id) return;
		const rect = li.getBoundingClientRect();
		const above = e.clientY < rect.top + rect.height / 2;
		// Insert between the drop target and its neighbor on the chosen side,
		// skipping the dragged row itself when it is that neighbor.
		let before = above ? visible[index - 1] : todo;
		let after = above ? todo : visible[index + 1];
		if (before?.id === draggedId) before = visible[index - 2];
		if (after?.id === draggedId) after = visible[index + 2];
		try {
			// Throws when the neighbors carry equal keys (possible after
			// concurrent appends from two tabs picked the same key).
			const sortKey = generateKeyBetween(keyOrNull(before), keyOrNull(after));
			void client.db.todo.mutate.patch(draggedId, { sortKey });
		} catch {
			toast("Couldn't reorder here — neighbors share an order key");
		}
	});

	return li;
}

function startEdit(li: HTMLLIElement, title: HTMLSpanElement, todo: Todo): void {
	const editor = document.createElement('input');
	editor.type = 'text';
	editor.className = 'editor';
	editor.value = todo.title;
	li.replaceChild(editor, title);
	editor.focus();
	editor.select();

	let done = false;
	const commit = () => {
		if (done) return;
		done = true;
		const next = editor.value.trim();
		if (next && next !== todo.title) {
			void client.db.todo.mutate.patch(todo.id, { title: next });
		} else {
			render();
		}
	};
	editor.addEventListener('blur', commit);
	editor.addEventListener('keydown', (e) => {
		if (e.key === 'Enter') commit();
		if (e.key === 'Escape') {
			done = true;
			render();
		}
	});
}

/** Enable/disable the undo/redo controls (not part of SyncStatus). */
function refreshControls(): void {
	undoBtn.disabled = !client.canUndo();
	redoBtn.disabled = !client.canRedo();
}

function relTime(at: Date): string {
	const secs = Math.max(0, Math.round((Date.now() - at.getTime()) / 1000));
	if (secs < 2) return 'just now';
	if (secs < 60) return `${secs}s ago`;
	if (secs < 3600) return `${Math.floor(secs / 60)}m ago`;
	return `${Math.floor(secs / 3600)}h ago`;
}

// Live telemetry: the dot colour + pulse and the readout are driven straight
// from the client's reactive sync status, so the instrument reflects the real
// background sync loop instead of a guess.
let lastStatus: SyncStatus | undefined;

function applyStatus(s: SyncStatus): void {
	lastStatus = s;

	const tone = (!s.online || s.state === 'error')
		? 'is-err'
		: (s.dirty || s.state === 'syncing')
		? 'is-busy'
		: 'is-ok';
	statusDot.className = `dot ${tone}${s.state === 'syncing' ? ' is-pulsing' : ''}`;
	telemetry.classList.toggle('is-err', tone === 'is-err' && !!s.lastError);

	let text: string;
	if (s.lastError) {
		text = `error · ${s.lastError.code ?? 'sync failed'}`;
	} else if (!s.online) {
		text = s.dirty ? `offline · ${s.pendingMutations} queued` : 'offline';
	} else if (s.state === 'syncing') {
		text = 'syncing…';
	} else if (s.dirty) {
		text = `${s.pendingMutations} pending`;
	} else {
		text = s.lastSyncedAt ? `synced · ${relTime(s.lastSyncedAt)}` : 'idle';
	}
	statusText.textContent = text;
	telemetry.title = s.lastError
		? s.lastError.message
		: s.online
		? 'connected to sync server'
		: 'not connected — writes queue locally and replay on reconnect';

	renderScope(s);
	refreshControls();
}

// Surface the one case worth explaining: this tab asked for a partial scope
// ("sync only active") but the EFFECTIVE scope is wider. All tabs in a browser
// share one store and sync the union of every live tab's subscriptions, and
// full sync is the absorbing element of that union — so a single full-sync tab
// (or a record lingering from a recently-closed one) pulls everything for
// everyone. The matching, non-widened case needs no note: the toggle already
// says it. Rendered as its own banner row so it never reshuffles the footer.
function renderScope(s: SyncStatus): void {
	const widened = !!s.scope && s.scope.own > 0 && s.scope.widened;
	scopeNote.hidden = !widened;
	if (!widened) return;
	scopeNote.textContent =
		'⚠ another tab is syncing everything — “sync only active” has no effect here';
	scopeNote.title = 'All tabs in this browser share one store and sync the union of their '
		+ 'scopes. Another tab (or one closed in the last few minutes, before its '
		+ 'record expired) is full-sync, so the whole workspace syncs. Close it to '
		+ 'let this scope take effect.';
}

// ---- new todos --------------------------------------------------------------

form.addEventListener('submit', (event) => {
	event.preventDefault();
	const title = input.value.trim();
	if (!title) return;
	input.value = '';
	const last = ordered(allTodos).at(-1);
	// create() mints a time-ordered id for us; no need to supply one.
	void client.db.todo.mutate.create({
		title,
		completed: false,
		createdAtMs: BigInt(Date.now()),
		sortKey: generateKeyBetween(keyOrNull(last), null),
	});
});

// ---- undo / redo ------------------------------------------------------------

async function doUndo(): Promise<void> {
	if (!(await client.undo())) toast('Nothing to undo');
	refreshControls();
}

async function doRedo(): Promise<void> {
	if (!(await client.redo())) toast('Nothing to redo');
	refreshControls();
}

undoBtn.addEventListener('click', () => void doUndo());
redoBtn.addEventListener('click', () => void doRedo());

window.addEventListener('keydown', (e) => {
	// Leave native text-editing undo alone while typing.
	const target = e.target as HTMLElement | null;
	if (target instanceof HTMLInputElement && target.type === 'text') return;
	if (!(e.metaKey || e.ctrlKey)) return;
	const key = e.key.toLowerCase();
	if (key === 'z' && !e.shiftKey) {
		e.preventDefault();
		void doUndo();
	} else if ((key === 'z' && e.shiftKey) || key === 'y') {
		e.preventDefault();
		void doRedo();
	}
});

// ---- complete all (one undo step via transact) -------------------------------

completeAll.addEventListener('click', () => {
	void (async () => {
		const open = allTodos.filter((t) => !t.completed);
		if (open.length === 0) return;
		// transact() groups the patches into ONE undo step; a single Cmd+Z
		// reopens everything.
		await client.transact(async (tx) => {
			for (const todo of open) {
				await tx.db.todo.mutate.patch(todo.id, { completed: true });
			}
		});
		refreshControls();
	})();
});

// ---- filters + partial sync ---------------------------------------------------

for (const tab of tabs) {
	tab.addEventListener('click', () => {
		filter = tab.dataset.filter as Filter;
		render();
	});
}

syncActive.addEventListener('change', () => {
	// Partial sync: with the box checked this client only syncs open todos —
	// completing one makes it leave the subset entirely (watch the "completed"
	// tab drain after sync). Unchecking re-bootstraps back to the full set.
	client.setSubscriptions(
		syncActive.checked ? [ subscription(TodoSchema, (w) => w.completed.eq(false)) ] : [],
	);
});

// ---- shared notes (collaborative text + anchored comments) -------------------
//
// One well-known Note row holds a convergent text document; comments are
// Annotation rows whose RANGE lives inside the document as a
// "comment:<id>" mark, so it follows the text as it moves. The mark PATCH
// and the annotation row write go in one transact() batch — atomic, and one
// undo step.

const NOTE_ID = 'workspace-notes';
const notesEl = document.querySelector<HTMLTextAreaElement>('#notes')!;
const commentBtn = document.querySelector<HTMLButtonElement>('#comment-btn')!;
const commentsList = document.querySelector<HTMLUListElement>('#comments')!;
const commentsEmpty = document.querySelector<HTMLParagraphElement>('#comments-empty')!;

let notesHandle: TextHandle | undefined;
let annotations: Annotation[] = [];

/** Shifts a caret index through a remote delta so the cursor stays put. */
function transformIndex(delta: TextDelta, index: number): number {
	let pos = 0; // walked position in the pre-change text
	let out = index;
	for (const op of delta.ops) {
		if ('retain' in op) {
			pos += op.retain;
		} else if ('insert' in op) {
			if (pos < index) out += op.insert.length;
		} else {
			const removed = Math.min(op.delete, Math.max(0, index - pos));
			out -= removed;
			pos += op.delete;
		}
		if (pos >= index) break;
	}
	return out;
}

function renderComments(): void {
	const handle = notesHandle;
	if (!handle) return;
	const joined = joinAnnotations(annotations, (a) => a.id, handle.marks());
	commentsList.replaceChildren(...joined.map(({ row, ranges }) => {
		const li = document.createElement('li');
		li.className = `comment${row.resolved ? ' resolved' : ''}${
			ranges.length === 0 ? ' orphaned' : ''
		}`;

		const quote = document.createElement('span');
		quote.className = 'comment__quote mono';
		const range = ranges[0];
		quote.textContent = range
			? `“${handle.value.slice(range.start, range.end)}”`
			: '(text removed)';
		if (range) {
			quote.title = 'Select in the note';
			quote.addEventListener('click', () => {
				notesEl.focus();
				notesEl.setSelectionRange(range.start, range.end);
			});
		}

		const body = document.createElement('span');
		body.className = 'comment__body';
		body.textContent = row.body;

		const resolve = document.createElement('button');
		resolve.className = 'comment__resolve';
		resolve.textContent = row.resolved ? 'reopen' : 'resolve';
		resolve.addEventListener('click', () => {
			void client.db.annotation.mutate.patch(row.id, { resolved: !row.resolved });
		});

		const remove = document.createElement('button');
		remove.className = 'remove';
		remove.textContent = '×';
		remove.title = 'Delete comment';
		remove.addEventListener('click', () => {
			// Row delete and mark removal in one atomic batch (one undo step).
			void client.transact(async (tx) => {
				const note = await tx.db.note.get(NOTE_ID);
				if (note) {
					const doc = decodeTextDoc(note.body, await client.clientId());
					for (const r of ranges) {
						const text = doc.plainText();
						doc.unmark(
							utf16ToCodePoint(text, r.start),
							utf16ToCodePoint(text, r.end),
							commentKey(row.id),
							ExpandNone,
						);
					}
					await tx.db.note.mutate.patch(NOTE_ID, { body: encodeTextDoc(doc) });
				}
				await tx.db.annotation.mutate.del(row.id);
			});
		});

		li.append(quote, body, resolve, remove);
		return li;
	}));
	commentsEmpty.hidden = joined.length > 0;
}

function refreshCommentButton(): void {
	commentBtn.disabled = !notesHandle
		|| notesEl.selectionStart === notesEl.selectionEnd;
}

notesEl.addEventListener('input', () => {
	// The prefix/suffix diff turns a textarea edit into minimal CRDT ops —
	// good for typing, paste and IME alike.
	void notesHandle?.setString(notesEl.value);
	renderComments();
});
document.addEventListener('selectionchange', refreshCommentButton);

commentBtn.addEventListener('click', () => {
	void (async () => {
		const handle = notesHandle;
		if (!handle) return;
		const start = notesEl.selectionStart;
		const end = notesEl.selectionEnd;
		if (start === end) return;
		const text = prompt('Comment:')?.trim();
		if (!text) return;
		const id = newId();
		const clientId = await client.clientId();
		// The documented atomic pattern: mark + row in ONE batch.
		await client.transact(async (tx) => {
			const note = await tx.db.note.get(NOTE_ID);
			if (!note) return;
			const doc = decodeTextDoc(note.body, clientId);
			const plain = doc.plainText();
			doc.mark(
				utf16ToCodePoint(plain, start),
				utf16ToCodePoint(plain, end),
				commentKey(id),
				true,
				ExpandNone,
			);
			await tx.db.note.mutate.patch(NOTE_ID, { body: encodeTextDoc(doc) });
			await tx.db.annotation.mutate.put({
				id,
				note: { id: NOTE_ID },
				body: text,
				createdAtMs: BigInt(Date.now()),
			});
		});
		renderComments();
	})();
});

async function bootNotes(): Promise<void> {
	// Ensure the shared row exists (races between tabs both write an empty
	// doc, which merge to an empty doc — harmless).
	if ((await client.db.note.get(NOTE_ID)) === undefined) {
		await client.db.note.mutate.put({
			id: NOTE_ID,
			body: encodeTextDoc(new TextDoc(await client.clientId())),
		});
	}
	const handle = await client.db.note.text(NOTE_ID, 'body');
	notesHandle = handle;
	notesEl.value = handle.value;
	handle.subscribe((delta, value) => {
		if (notesEl.value !== value) {
			// Remote change: patch the textarea in place, carrying the caret
			// through the delta so it doesn't jump.
			const { selectionStart, selectionEnd } = notesEl;
			notesEl.value = value;
			notesEl.setSelectionRange(
				transformIndex(delta, selectionStart),
				transformIndex(delta, selectionEnd),
			);
		}
		renderComments();
	});
	renderComments();
	refreshCommentButton();
}

client.db.annotation.watch((rows) => {
	annotations = rows.map((r) => r.value);
	renderComments();
});

void bootNotes().catch((err: unknown) => {
	toast(`Notes failed to load: ${err instanceof Error ? err.message : String(err)}`);
});

// ---- boot ---------------------------------------------------------------------

client.db.todo.watch((rows) => {
	allTodos = rows.map((r) => r.value);
	render();
});

// Reactive sync telemetry — push, not poll.
client.onStatusChange(applyStatus);
applyStatus(client.status());

// The only thing that needs a heartbeat is the "synced · 12s ago" relative
// clock; re-render it from the last snapshot without touching the client.
setInterval(() => {
	if (lastStatus && !lastStatus.dirty && lastStatus.online && lastStatus.state === 'idle') {
		applyStatus(lastStatus);
	}
}, 1000);

void client.start();
