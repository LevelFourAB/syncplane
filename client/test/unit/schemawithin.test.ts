import { describe, expect, it } from 'vitest';

import {
	bool,
	defineSchema,
	entity,
	type EntitySchema,
	type Field,
	type Infer,
	ref,
	text,
	within,
} from '../../src/schema/builder.js';

// Field names in access rules are checked at compile time where the entity is
// known. A within entry names a field of another entity, so its check needs
// that entity's fields: the type of within() carries them.
describe('within entries in the schema builder', () => {
	const doc = entity('doc', { id: text().key(), ownerId: text(), published: bool() }, {
		access: { allow: [ { read: true, requires: 'view_docs' } ] },
	});
	const comment = (entry: ReturnType<typeof within>) =>
		entity('comment', { id: text().key(), doc: ref(() => doc) }, {
			access: {
				parents: [ 'doc' ],
				allow: [ { read: true, requires: 'view_docs' }, {
					write: true,
					admitted: true,
					within: entry,
				} ],
			},
		});

	it('refuses a within() callerIs that is not a text() field, at compile time', () => {
		// Given doc, whose published field is a bool()
		// When a within entry built with within() names it as callerIs
		// @ts-expect-error callerIs names a text() field
		const entry = within(doc, { callerIs: 'published' });

		// Then defineSchema() refuses it at run time too
		expect(() => defineSchema(comment(entry), doc)).toThrow(
			'callerIs field "published" must be a singular text() field',
		);
	});

	it('refuses a misspelled within() callerIs at compile time', () => {
		// Given doc, whose owner field is ownerId
		// When a within entry built with within() misspells it
		// @ts-expect-error not a field of a doc
		const entry = within(doc, { callerIs: 'ownrId' });

		// Then defineSchema() refuses it at run time too
		expect(() => defineSchema(comment(entry), doc)).toThrow(
			'callerIs field "ownrId" is not declared',
		);
	});

	it('refuses a misspelled callerIs in a plain within entry at compile time', () => {
		// Given doc, whose owner field is ownerId
		// When a rule writes a plain within entry, the form the reference
		// shows for callerIs, and misspells the field
		const build = () =>
			entity('comment', { id: text().key(), doc: ref(() => doc) }, {
				access: {
					parents: [ 'doc' ],
					allow: [
						{ read: true, requires: 'view_docs' },
						// @ts-expect-error not a field of a doc
						{ write: true, admitted: true, within: { entity: doc.name, callerIs: 'ownrId' } },
					],
				},
			});

		// Then defineSchema() refuses it at run time too
		expect(() => defineSchema(build(), doc)).toThrow('callerIs field "ownrId" is not declared');
	});
});

// The server refuses a guard that can hide a field a within entry reads,
// because a client decides access from the rows it holds. On the rule's own
// entity the builder has every field it needs to refuse it too.
describe('a guard that can hide a field a self within entry reads', () => {
	it('is refused before registration', () => {
		// Given a folder tree whose rule reads isPrivate on the folder and the
		// folders above it, and a guard that hides isPrivate from every client
		type FolderFields = {
			id: Field<string>;
			parent: Field<Infer<EntitySchema<FolderFields>>>;
			isPrivate: Field<boolean>;
		};
		const build = () => {
			const folder: EntitySchema<FolderFields> = entity('folder', {
				id: text().key(),
				parent: ref(() => folder),
				isPrivate: bool(),
			}, {
				access: {
					parents: [ 'parent' ],
					allow: [
						{ read: true, requires: 'view_private' },
						{ read: true, admitted: true, within: { self: true, when: (f) => f.isPrivate.eq(false) } },
					],
					guard: [ { fields: [ 'isPrivate' ], allow: [] } ],
				},
			});
			return defineSchema(folder);
		};

		// When the schema is defined
		// Then it is refused, as registration would refuse it
		expect(build).toThrow(/isPrivate|is_private/);
	});
});
