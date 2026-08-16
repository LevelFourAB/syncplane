import { describe, expect, it } from 'vitest';

import {
	KeyedTextEntitySchema,
	ListTextEntitySchema,
	MapTextEntitySchema,
} from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { EntityRegistry } from '../../src/mutations.js';

// A text field may sit under keyed message lists and message-valued maps —
// the element identity is stable and a mask path addresses the element, so
// the merger fires per element and each element holds its own document. An
// UNKEYED repeated element has no identity a mask could name, so it is
// replaced whole and text inside it is refused at registration, mirroring
// the engine (engine/server collectTextFields).
describe('text field registration', () => {
	it('rejects a text field inside an unkeyed repeated element', () => {
		expect(() => new EntityRegistry([ ListTextEntitySchema ]))
			.toThrow(/text field is inside repeated field "notes"/);
	});

	it('accepts a text field inside a keyed list element and a map value', () => {
		const registry = new EntityRegistry([ KeyedTextEntitySchema, MapTextEntitySchema ]);
		expect(registry.textPathsOf('keyed_text'))
			.toEqual([ 'notes.body', 'notes.deep.body', 'sections.body' ]);
		expect(registry.textPathsOf('map_text')).toEqual([ 'notes.body' ]);
	});
});
