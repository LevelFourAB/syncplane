import { create } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { buildDelete, buildPut, entityDefOf } from '../../src/mutations.js';
import {
	MAX_ENTITY_NAME_LENGTH,
	MAX_ROOM_NAME_BYTES,
	MAX_ROW_KEY_BYTES,
	MAX_WORKSPACE_ID_BYTES,
	rowKey,
	validateEntityName,
	validateKeyField,
	validatePermissionName,
	validateRelationName,
	validateRoleName,
	validateRoomName,
	validateRowKey,
	validateWorkspaceId,
} from '../../src/naming.js';
import { bytes, double, entity, text } from '../../src/schema/builder.js';

// The rules themselves. engine/internal/naming holds the Go mirror and its
// suite asserts the same cases — the two must answer alike, since the client
// derives the key it sends and the server re-derives it.
describe('validateEntityName', () => {
	it('accepts identifiers', () => {
		for (const name of [ 'todo', 'Todo', 'todo_list', '_private', 'a1', 't2m' ]) {
			expect(() => validateEntityName(name)).not.toThrow();
		}
	});

	it('rejects an empty name', () => {
		expect(() => validateEntityName('')).toThrow(/empty/);
	});

	// The one that corrupts the KV layout: entity "a" row "b/x" and entity
	// "a/b" row "x" would occupy the same key.
	it('rejects a slash', () => {
		expect(() => validateEntityName('a/b')).toThrow(/not an identifier/);
	});

	it('rejects punctuation, spaces and a leading digit', () => {
		for (const name of [ 'todo-list', 'todo.list', 'todo list', '1todo', 'todo!' ]) {
			expect(() => validateEntityName(name)).toThrow(/not an identifier/);
		}
	});

	it('caps the length', () => {
		expect(() => validateEntityName('a'.repeat(MAX_ENTITY_NAME_LENGTH))).not.toThrow();
		expect(() => validateEntityName('a'.repeat(MAX_ENTITY_NAME_LENGTH + 1)))
			.toThrow(/the limit is/);
	});
});

describe('validatePermissionName and validateRoleName', () => {
	it('accept identifiers', () => {
		for (const name of [ 'view_tasks', 'editTasks', '_internal', 'v2' ]) {
			expect(() => validatePermissionName(name)).not.toThrow();
			expect(() => validateRoleName(name)).not.toThrow();
		}
	});

	it('reject anything that is not an identifier', () => {
		for (const name of [ 'view-tasks', 'view.tasks', '1view', 'view tasks' ]) {
			expect(() => validatePermissionName(name)).toThrow(
				`permission name "${name}" is not an identifier`,
			);
			expect(() => validateRoleName(name)).toThrow(`role name "${name}" is not an identifier`);
		}
		expect(() => validatePermissionName('')).toThrow('permission name is empty');
	});
});

describe('validateRelationName', () => {
	it('accepts identifiers', () => {
		for (const name of [ 'issues', 'assignedIssues', 'issues_by_owner', '_private', 'v2' ]) {
			expect(() => validateRelationName(name)).not.toThrow();
		}
	});

	// A dot would read as two follow segments, and a name that is not an
	// identifier cannot be a property of the refs facade.
	it('rejects anything that is not an identifier', () => {
		for (
			const name of [ 'assigned.issues', 'assigned-issues', 'assigned issues', '1issues', '$issues' ]
		) {
			expect(() => validateRelationName(name)).toThrow(
				`reverse relation name "${name}" is not an identifier`,
			);
		}
	});

	it('rejects an empty name and one past the length cap', () => {
		expect(() => validateRelationName('')).toThrow('reverse relation name is empty');
		expect(() => validateRelationName('a'.repeat(MAX_ENTITY_NAME_LENGTH + 1))).toThrow(
			/the limit is/,
		);
	});
});

describe('validateWorkspaceId', () => {
	it('accepts the id shapes apps actually use', () => {
		for (
			const id of [
				'w1',
				'acme',
				'acme-corp',
				'team_1',
				'user:xB12dL',
				'org:acme:team:eng',
				'550e8400-e29b-41d4-a716-446655440000',
				'01ARZ3NDEKTSV4RRFFQ69G5FAV',
				'42',
			]
		) {
			expect(() => validateWorkspaceId(id)).not.toThrow();
		}
	});

	it('rejects an empty id', () => {
		expect(() => validateWorkspaceId('')).toThrow(/empty/);
	});

	// Each of these corrupts one consumer that parses the id back out; see the
	// doc comment on WORKSPACE_ID for which.
	it('rejects the characters its consumers cannot round-trip', () => {
		for (
			const id of [
				'a\u001fb', // shifts the fields of a cross-tab change token
				'*', // the "every workspace" scan sentinel
				'a/b', // forges a segment in the KV key layout
				'..', // a path traversal in the admin UI's URLs
				'a.b',
				'a b',
				'a\u0000b',
				'åäö',
			]
		) {
			expect(() => validateWorkspaceId(id), id).toThrow(/is not valid/);
		}
	});

	it('rejects punctuation in the first position', () => {
		for (const id of [ '-a', '_a', ':a' ]) {
			expect(() => validateWorkspaceId(id), id).toThrow(/starting with a letter or digit/);
		}
	});

	it('caps the length', () => {
		expect(() => validateWorkspaceId('a'.repeat(MAX_WORKSPACE_ID_BYTES))).not.toThrow();
		expect(() => validateWorkspaceId('a'.repeat(MAX_WORKSPACE_ID_BYTES + 1)))
			.toThrow(/the limit is/);
	});
});

describe('validateKeyField', () => {
	const fieldOf = (name: string) => WidgetSchema.fields.find((f) => f.name === name)!;

	it('accepts strings, integers and bools', () => {
		for (const f of [ 'id', 'count', 'big', 'flag' ]) {
			expect(() => validateKeyField('widget', fieldOf(f))).not.toThrow();
		}
	});

	// bytes and double render differently here than in Go, so a key built from
	// one would name a different row on each side.
	it('rejects bytes and floating point', () => {
		expect(() => validateKeyField('widget', fieldOf('raw')))
			.toThrow(/must be a string, integer, bool or enum/);
		expect(() => validateKeyField('widget', fieldOf('ratio')))
			.toThrow(/must be a string, integer, bool or enum/);
	});

	it('rejects messages, lists and maps', () => {
		for (const f of [ 'settings', 'tags', 'attrs' ]) {
			expect(() => validateKeyField('widget', fieldOf(f)))
				.toThrow(/must be a string, integer, bool or enum/);
		}
	});
});

describe('validateRowKey', () => {
	it('accepts ordinary keys, including composite and non-ASCII ones', () => {
		for (const key of [ 't1', 'ns/id', 'a\\/b', 'åäö', '\u{1f600}' ]) {
			expect(() => validateRowKey('todo', key)).not.toThrow();
		}
	});

	// What a write whose key field was never set derives — locally it would
	// address `c/todo/<ws>/`, the entity prefix itself.
	it('rejects the empty key', () => {
		expect(() => validateRowKey('todo', '')).toThrow(/every key field must be set/);
	});

	it('rejects control characters', () => {
		expect(() => validateRowKey('todo', 'a\u0000b')).toThrow(/\\u0000/);
		// U+001F separates the fields of a cross-tab change token.
		expect(() => validateRowKey('todo', 'a\u001fb')).toThrow(/\\u001f/);
		expect(() => validateRowKey('todo', 'a\u007f')).toThrow(/\\u007f/);
	});

	// TextEncoder would silently substitute U+FFFD, so the key the server
	// stored would not be the key this client derived.
	it('rejects an unpaired surrogate', () => {
		expect(() => validateRowKey('todo', 'a\ud800b')).toThrow(/unpaired surrogate/);
		expect(() => validateRowKey('todo', 'a\udc00')).toThrow(/unpaired surrogate/);
		expect(() => validateRowKey('todo', 'a😀')).not.toThrow();
	});

	it('caps the length in UTF-8 bytes, not characters', () => {
		expect(() => validateRowKey('todo', 'a'.repeat(MAX_ROW_KEY_BYTES))).not.toThrow();
		expect(() => validateRowKey('todo', 'a'.repeat(MAX_ROW_KEY_BYTES + 1))).toThrow(/the limit is/);
		// Two bytes per character, so half the cap in characters is the limit.
		expect(() => validateRowKey('todo', 'å'.repeat(MAX_ROW_KEY_BYTES / 2 + 1)))
			.toThrow(/the limit is/);
	});
});

describe('validateRoomName', () => {
	// A character rule, not an identifier rule: nothing declares a room ahead
	// of time, so an app names one after whatever it is showing.
	it('accepts the names apps address rooms by', () => {
		for (const name of [ 'cursors', 'doc:42/cursors', 'Board — Sprint 14', 'åäö' ]) {
			expect(() => validateRoomName(name)).not.toThrow();
		}
	});

	it('rejects the empty name', () => {
		expect(() => validateRoomName('')).toThrow(/empty/);
	});

	it('rejects control characters', () => {
		expect(() => validateRoomName('a\u0000b')).toThrow(/\\u0000/);
		// U+001F separates the fields of the cross-tab room protocol.
		expect(() => validateRoomName('a\u001fb')).toThrow(/\\u001f/);
		expect(() => validateRoomName('a\u007f')).toThrow(/\\u007f/);
	});

	it('rejects an unpaired surrogate', () => {
		expect(() => validateRoomName('a\ud800b')).toThrow(/unpaired surrogate/);
		expect(() => validateRoomName('a😀')).not.toThrow();
	});

	it('caps the length in UTF-8 bytes, not characters', () => {
		expect(() => validateRoomName('a'.repeat(MAX_ROOM_NAME_BYTES))).not.toThrow();
		expect(() => validateRoomName('a'.repeat(MAX_ROOM_NAME_BYTES + 1))).toThrow(/the limit is/);
		expect(() => validateRoomName('å'.repeat(MAX_ROOM_NAME_BYTES / 2 + 1))).toThrow(/the limit is/);
	});
});

describe('rowKey', () => {
	it('uses a single key field verbatim', () => {
		expect(rowKey([ 'a/b' ])).toBe('a/b');
	});

	it('escapes the separator so composite tuples cannot collide', () => {
		expect(rowKey([ 'a', 'b' ])).toBe('a/b');
		expect(rowKey([ 'a/b', 'c' ])).not.toBe(rowKey([ 'a', 'b/c' ]));
		expect(rowKey([ 'a\\', 'b' ])).toBe('a\\\\/b');
	});
});

// Where the rules are enforced. A local write that would be refused by the
// server has to fail here instead of round-tripping through the overlay and
// coming back as a remote rejection.
describe('naming rules on the write path', () => {
	const todo = entityDefOf(TodoSchema);

	it('refuses a PUT whose key field was never set', () => {
		expect(() => buildPut(todo, TodoSchema, create(TodoSchema, { title: 'no id' })))
			.toThrow(/every key field must be set/);
	});

	it('refuses a DELETE of an unnameable key', () => {
		expect(() => buildDelete(todo, 't\u001f1')).toThrow(/\\u001f/);
	});
});

// The declaration-time half: a schema declared at runtime fails at the line
// that declares it, not later at client construction.
describe('naming rules in the schema builder', () => {
	it('refuses an entity name that is not an identifier', () => {
		expect(() => entity('to-do', { id: text().key() })).toThrow(/not an identifier/);
	});

	it('refuses a key field whose kind has no shared rendering', () => {
		expect(() => bytes().key()).toThrow(/must be a string, integer or bool/);
		expect(() => double().key()).toThrow(/must be a string, integer or bool/);
	});
});
