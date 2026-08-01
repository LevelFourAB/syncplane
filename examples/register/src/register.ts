/**
 * Worked example: a Node backend that owns an app's schema and registers it
 * with a running engine over the trusted AdminService.
 *
 * One TypeScript definition (the schema builder) yields both the bytes the
 * engine ingests and the descriptors a browser client syncs with. Run an
 * syncplaned (cmd/syncplaned) with a -token, then:
 *
 *   ADMIN_URL=http://127.0.0.1:8788 TOKEN=dev-secret pnpm register
 *
 * After this, point a SyncClient at http://<sync-addr>/<appId> with
 * `entities: bundle.descMessages()` (or fetchSchema) and it syncs full CRUD
 * against a schema that exists only as runtime-registered bytes.
 */

import { createConnectTransport } from '@connectrpc/connect-node';

import { registerSchema, schema } from 'syncplane';

const { bool, defineSchema, entity, int64, list, text } = schema;

const todo = entity('todo', {
	id: text().key(),
	title: text(),
	completed: bool(),
	createdAtMs: int64(),
	tags: list(text()),
});

const bundle = defineSchema(todo);

const appId = process.env.APP_ID ?? 'todo';
const adminUrl = process.env.ADMIN_URL ?? 'http://127.0.0.1:8788';
const token = process.env.TOKEN ?? '';

const transport = createConnectTransport({ baseUrl: adminUrl, httpVersion: '1.1' });

const result = await registerSchema(transport, appId, bundle, { token });
console.log(`registered app "${appId}":`, {
	revision: result.revision,
	status: result.status,
	entities: result.entityNames,
});
