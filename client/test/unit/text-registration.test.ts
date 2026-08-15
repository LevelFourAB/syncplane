import { describe, expect, it } from 'vitest';

import {
	ListTextEntitySchema,
	MapTextEntitySchema,
} from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { EntityRegistry } from '../../src/mutations.js';

// A text field must sit on a path of singular fields. Inside a repeated
// element or a map value a masked write replaces the element whole, so the
// CRDT merger could never run — refused at registration, mirroring the
// engine (engine/server collectTextFields).
describe('text field registration', () => {
	it('rejects a text field inside a repeated element', () => {
		expect(() => new EntityRegistry([ ListTextEntitySchema ]))
			.toThrow(/text field is inside repeated field "notes"/);
	});

	it('rejects a text field inside a map value', () => {
		expect(() => new EntityRegistry([ MapTextEntitySchema ]))
			.toThrow(/text field is inside map field "notes"/);
	});
});
