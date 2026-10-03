import { describe, expect, it } from 'vitest';

import { subscription, type SyncClientOptions } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import {
	IssueSchema,
	PairSchema,
	ProjectSchema,
	UserSchema,
	WidgetSchema,
} from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { freshWorkspace, newClient } from './helpers.js';

// Reference-following subscriptions end-to-end: a followed target arrives
// without being explicitly subscribed, a re-point delivers the new target and
// drops the old one, and reverse following pulls a subscribed parent's
// children.
const refEntities: Partial<SyncClientOptions> = {
	entities: [ TodoSchema, WidgetSchema, PairSchema, ProjectSchema, UserSchema, IssueSchema ],
};

// Base scope: all issues; follow issue→project so projects arrive only via the
// closure.
const followProject = subscription(IssueSchema, undefined, { include: { project: true } });

describe('reference-following subscriptions', () => {
	it('delivers a followed target with no explicit subscription on it', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws, refEntities);
		await writer.start();
		await writer.mutate.put(ProjectSchema, { id: 'A', name: 'alpha' });
		await writer.mutate.put(ProjectSchema, { id: 'B', name: 'unreferenced' });
		await writer.mutate.put(IssueSchema, { id: 'i1', title: 'one', project: { id: 'A' } });
		await writer.syncNow();

		const reader = newClient(ws, { ...refEntities, subscriptions: [ followProject ] });
		await reader.start();
		await reader.syncNow();

		// The referenced project A arrives; the unreferenced B does not.
		expect((await reader.get(ProjectSchema, 'A'))?.name).toBe('alpha');
		expect(await reader.get(ProjectSchema, 'B')).toBeUndefined();
		expect(await reader.get(IssueSchema, 'i1')).toBeDefined();

		await writer.close();
		await reader.close();
	});

	it('re-points to an old target: new one arrives, old one drops', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws, refEntities);
		await writer.start();
		await writer.mutate.put(ProjectSchema, { id: 'A', name: 'alpha' });
		await writer.mutate.put(ProjectSchema, { id: 'B', name: 'beta' });
		await writer.mutate.put(IssueSchema, { id: 'i1', title: 'one', project: { id: 'A' } });
		await writer.syncNow();

		const reader = newClient(ws, { ...refEntities, subscriptions: [ followProject ] });
		await reader.start();
		await reader.syncNow();
		expect(await reader.get(ProjectSchema, 'A')).toBeDefined();
		expect(await reader.get(ProjectSchema, 'B')).toBeUndefined();

		// Re-point i1 to the already-old B.
		await writer.mutate.put(IssueSchema, { id: 'i1', title: 'one', project: { id: 'B' } });
		await writer.syncNow();
		await reader.syncNow();

		expect((await reader.get(ProjectSchema, 'B'))?.name).toBe('beta');
		// A is no longer referenced by any issue and must drop (no zombie).
		expect(await reader.get(ProjectSchema, 'A')).toBeUndefined();

		await writer.close();
		await reader.close();
	});

	it('drops a followed target when the subscription stops following it', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws, refEntities);
		await writer.start();
		await writer.mutate.put(ProjectSchema, { id: 'A', name: 'alpha' });
		await writer.mutate.put(IssueSchema, { id: 'i1', title: 'one', project: { id: 'A' } });
		await writer.syncNow();

		const reader = newClient(ws, { ...refEntities, subscriptions: [ followProject ] });
		await reader.start();
		await reader.syncNow();
		expect(await reader.get(ProjectSchema, 'A')).toBeDefined();

		// Keep the issues, stop following issue→project.
		reader.setSubscriptions([ subscription(IssueSchema) ]);
		await reader.syncNow();

		expect(await reader.get(ProjectSchema, 'A')).toBeUndefined();
		expect(await reader.get(IssueSchema, 'i1')).toBeDefined();

		await writer.close();
		await reader.close();
	});

	it('reverse-follows a subscribed parent to its children', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws, refEntities);
		await writer.start();
		await writer.mutate.put(ProjectSchema, { id: 'P', name: 'proj' });
		await writer.mutate.put(IssueSchema, { id: 'i1', title: 'one', project: { id: 'P' } });
		await writer.mutate.put(IssueSchema, { id: 'i2', title: 'two', project: { id: 'P' } });
		await writer.syncNow();

		// Subscribe the project, follow its reverse "issuesByProject" relation.
		const followIssues = subscription(ProjectSchema, undefined, {
			include: { issuesByProject: true },
		});
		const reader = newClient(ws, { ...refEntities, subscriptions: [ followIssues ] });
		await reader.start();
		await reader.syncNow();

		expect((await reader.scan(IssueSchema)).map((r) => r.key).sort()).toEqual([ 'i1', 'i2' ]);
		expect(await reader.get(ProjectSchema, 'P')).toBeDefined();

		await writer.close();
		await reader.close();
	});
});
