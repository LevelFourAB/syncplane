/**
 * The entity set a store syncs — what `meta/entities` records, and the rule
 * every path that touches confirmed rows resolves entity names against.
 *
 * The client and the server each hold their own registry, and they routinely
 * disagree: a full-sync client (no subscriptions) is sent every entity the
 * server has, which may include ones this build never declared. Rows for such
 * an entity cannot be decoded, indexed, read or merged here — they are bytes
 * with no schema — so nothing stores them. The rule:
 *
 *   an entity syncs if THIS build's registry knows it, or the store records it.
 *
 * The second half is what makes the first half safe across tabs. Tabs of
 * different builds share one store (see touch.ts), so a store whose entity set
 * were narrowed to whichever build started last would have the older tab
 * silently dropping changes for an entity the newer one displays. Recording the
 * union instead means every tab keeps every other tab's rows up to date, whether
 * or not it can read them itself.
 *
 * The set therefore only ever GROWS. A name no build registers any more keeps
 * its rows synced and stored until the next format-version wipe — the cost of
 * the guarantee, and cheap next to dropping rows a live tab is reading.
 *
 * Growth is the interesting direction: an entity added to a build's registry has
 * no confirmed rows here (nothing stored them) and no way to acquire them, since
 * every one of them sits below the workspace cursors this client has already
 * advanced past. Registering it therefore has to re-bootstrap — which is exactly
 * what reconcileStoreEntities arranges, and why the alternative (keeping
 * undecodable rows around on the chance a later build wants them) is worse than
 * it looks: those rows would be stale by an unbounded amount, and possibly
 * deleted server-side past the tombstone horizon, which is the staleness
 * re-bootstrapping exists to prevent.
 */

import {
	META_ENTITIES,
	META_WS_PREFIX,
	metaBootstrapResume,
	metaCursor,
	wsFromSegment,
} from './keys.js';
import type { KVRead, KVWrite } from './kv/store.js';
import type { EntityRegistry } from './mutations.js';

/** The entity names the store records, or an empty array on a fresh store. */
export async function readStoreEntities(tx: KVRead): Promise<string[]> {
	return ((await tx.get(META_ENTITIES)) as string[] | undefined) ?? [];
}

/**
 * The names that count as syncable right now: this build's registry plus
 * whatever the store records. Read once per batch of changes rather than per
 * change — it is one point read, and every caller already holds a transaction.
 */
export async function syncableEntities(
	tx: KVRead,
	registry: EntityRegistry,
): Promise<ReadonlySet<string>> {
	const names = new Set(await readStoreEntities(tx));
	for (const name of registry.names()) names.add(name);
	return names;
}

/**
 * Brings the store's entity set up to date with this build's registry, and
 * returns the names that were missing from it (empty when nothing changed).
 *
 * A name this build registers that the store has never held means the confirmed
 * state predates the entity and holds nothing for it, so every workspace's
 * cursor goes: a workspace with no cursor re-bootstraps, which is the only way
 * to acquire rows written before the client started skipping them. Any bootstrap
 * resume goes with it, and for the same reason: the interrupted snapshot dropped
 * the new entity's rows as it streamed them, and continuing it would have the
 * server skip the entities that attempt already "delivered" — leaving rows that
 * sit below the snapshot's cursor and so can never arrive by pull. Nothing else
 * is touched — the pending queue, the overlay and the client identity all
 * survive, because none of them depend on which entities the store has seen.
 *
 * Dropping the cursors covers workspaces this tab never opens (another tab, or
 * a later session, picks up the bootstrap); the caller additionally arms the
 * coordinator's forced resync for the ones it does, so a concurrent pull
 * committing a fresh cursor in the gap cannot quietly reinstate the state this
 * was meant to rebuild.
 *
 * Runs on a fresh store too, where the set is empty and every registered name is
 * new: there are no cursors to drop, so it is exactly the stamp it should be.
 */
export async function reconcileStoreEntities(
	tx: KVWrite,
	registry: EntityRegistry,
): Promise<string[]> {
	const stored = await readStoreEntities(tx);
	const known = new Set(stored);
	const added = registry.names().filter((name) => !known.has(name));
	if (added.length === 0) return [];
	for (const segment of await workspaceSegments(tx)) {
		const workspaceId = wsFromSegment(segment);
		await tx.del(metaCursor(workspaceId));
		await tx.del(metaBootstrapResume(workspaceId));
	}
	await tx.put(META_ENTITIES, [ ...stored, ...added ].sort());
	return added;
}

/** Every workspace the store holds meta for, as encoded key segments. */
async function workspaceSegments(tx: KVRead): Promise<Set<string>> {
	const out = new Set<string>();
	for await (const [ k ] of tx.scan(META_WS_PREFIX)) {
		const rest = k.slice(META_WS_PREFIX.length);
		const slash = rest.indexOf('/');
		if (slash > 0) out.add(rest.slice(0, slash));
	}
	return out;
}
