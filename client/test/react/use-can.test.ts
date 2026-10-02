// useCan over the testing kit: the in-memory server sends the CallerAccess a
// spec sets, and the hook answers from it and follows it as it changes.

import { act, renderHook, waitFor } from '@testing-library/react';
import { createElement, type ReactNode } from 'react';
import { describe, expect, it } from 'vitest';

import type { SyncClient } from '../../src/client.js';
import {
	LabelSchema,
	NoteSchema,
	OrgMemberSchema,
	ProjectMemberSchema,
	ProjectSchema,
	TaskSchema,
} from '../../src/gen/syncplane/testdata/access/v1/access_pb.js';
import { createReactSync, useCan } from '../../src/react/index.js';
import { createTestClient } from '../../src/testing/index.js';

const entities = [
	OrgMemberSchema,
	ProjectSchema,
	ProjectMemberSchema,
	TaskSchema,
	LabelSchema,
	NoteSchema,
] as const;
const { SyncProvider } = createReactSync(entities);

function wrapper(client: SyncClient) {
	return function Wrapper({ children }: { children: ReactNode }): ReactNode {
		return createElement(SyncProvider, { client }, children);
	};
}

describe('useCan', () => {
	it('answers from the summary the server sends, and follows it', async () => {
		const t = await createTestClient({ entities });
		t.server.put(ProjectSchema, { id: 'p1' });
		t.server.put(TaskSchema, { id: 't1', project: { id: 'p1' } });
		t.server.setCallerAccess({
			permissions: [ {
				permission: 'view_tasks',
				anchors: [ { entity: 'project', keys: [ 'p1' ] } ],
			} ],
		});
		await t.sync();

		const { result } = renderHook(() => useCan(TaskSchema, 't1', 'edit_tasks'), {
			wrapper: wrapper(t.client as unknown as SyncClient),
		});
		await waitFor(() => expect(result.current).toBe('no'));

		t.server.setCallerAccess({
			permissions: [
				{ permission: 'view_tasks', anchors: [ { entity: 'project', keys: [ 'p1' ] } ] },
				{ permission: 'edit_tasks', anchors: [ { entity: 'project', keys: [ 'p1' ] } ] },
			],
		});
		await act(() => t.sync());
		await waitFor(() => expect(result.current).toBe('yes'));
		await t.client.close();
	});

	it('is unknown before any summary arrives', async () => {
		const t = await createTestClient({ entities });
		const { result } = renderHook(() => useCan(TaskSchema, 't1', 'view_tasks'), {
			wrapper: wrapper(t.client as unknown as SyncClient),
		});
		await act(async () => {
			await new Promise((r) => setTimeout(r, 0));
		});
		expect(result.current).toBe('unknown');
		await t.client.close();
	});
});
