package pgstore

import (
	"fmt"
)

// The delta log and the consumer shadows are stored with int ids in place of
// the workspace id, the entity, the field and the consumer, which would
// otherwise repeat in every entry and in each index of it. field_deltas holds
// the workspace number, wsn, that index_scopes gives the workspace, and the id
// that text_fields gives the (entity, field) of the entry. consumer_shadows
// holds wsn and the id that shadow_sets gives the (consumer, entity) of the
// shadow. Both keep the row key as text.
//
// The ids of text_fields and shadow_sets are BIGINT, from the sequence
// name_ids, and none is used again. A write gives the ids it needs, and a read
// that finds no id finds no entry. A field path can hold the key of a list
// element, so a workspace can have a name for each element it ever had; a
// transaction therefore reads only the names it uses, each with the statement
// that reads its entries where it can, and keeps them in pgTx.names.
// PruneFieldDeltas deletes the text_fields row of each id it deletes the last
// entry of, and DeleteConsumer and CollectIdleConsumers the shadow_sets rows of
// the consumers they remove.
//
// A name is deleted only by a write transaction, which holds the workspace
// lock, and only the transactions of its workspace use it. So a writer that
// reads or gives an id cannot see a delete of that name commit before it
// commits itself. A read transaction reads the names and the entries from one
// snapshot.

// nameKind tells the two kinds of name apart in nameKey.
type nameKind uint8

const (
	// textField is the (entity, field) of a text field: a is the entity and b
	// the field.
	textField nameKind = iota
	// shadowSet is the (consumer, entity) of a set of shadows: a is the
	// consumer and b the entity.
	shadowSet
)

// nameKey names one row of text_fields or shadow_sets.
type nameKey struct {
	kind nameKind
	a, b string
}

const (
	// textFieldSQL returns the id of the field ($2 entity, $3 field) of
	// workspace $1, and gives it one when it has none; shadowSetSQL does the
	// same for the set ($2 consumer, $3 entity). nextval runs only when the
	// name is missing, so a name the table holds takes no id. The caller holds
	// the workspace lock, so no other transaction gives the name at the same
	// time.
	textFieldSQL = `WITH got AS (
	SELECT id FROM text_fields WHERE workspace_id = $1 AND entity = $2 AND field = $3
), made AS (
	INSERT INTO text_fields (workspace_id, entity, field, id)
	SELECT $1, $2, $3, nextval('name_ids') WHERE NOT EXISTS (SELECT 1 FROM got)
	RETURNING id
)
SELECT id FROM got UNION ALL SELECT id FROM made`
	shadowSetSQL = `WITH got AS (
	SELECT id FROM shadow_sets WHERE workspace_id = $1 AND consumer = $2 AND entity = $3
), made AS (
	INSERT INTO shadow_sets (workspace_id, consumer, entity, id)
	SELECT $1, $2, $3, nextval('name_ids') WHERE NOT EXISTS (SELECT 1 FROM got)
	RETURNING id
)
SELECT id FROM got UNION ALL SELECT id FROM made`
)

// workspaceNumber returns the workspace number, and false when the workspace
// has none and so holds no entry and no shadow.
func (t *pgTx) workspaceNumber() (int32, bool, error) {
	if err := t.loadIndexScopes(); err != nil {
		return 0, false, err
	}
	wsn, ok := t.indexScopes[indexKey{}]
	return wsn, ok, nil
}

// nameIDs returns the workspace number and the id of k, and gives the
// workspace and k the ids they do not have, which costs a round trip for each.
// Only a write transaction may call it.
func (t *pgTx) nameIDs(k nameKey) (wsn int32, id int64, err error) {
	wsn, hasWS, err := t.workspaceNumber()
	if err != nil {
		return 0, 0, err
	}
	if !hasWS {
		if err := t.makeIndexScopes([]indexKey{{}}); err != nil {
			return 0, 0, err
		}
		wsn = t.indexScopes[indexKey{}]
	}
	if id, ok := t.names[k]; ok {
		return wsn, id, nil
	}
	sql := textFieldSQL
	if k.kind == shadowSet {
		sql = shadowSetSQL
	}
	if err := t.queryRow(sql, t.ws, k.a, k.b).Scan(&id); err != nil {
		return 0, 0, fmt.Errorf("pgstore: give an id to %q %q: %w", k.a, k.b, err)
	}
	t.rememberName(k, id)
	return wsn, id, nil
}

// rememberName records that k has id in the table it names.
func (t *pgTx) rememberName(k nameKey, id int64) {
	if t.names == nil {
		t.names = make(map[nameKey]int64)
	}
	t.names[k] = id
}

// forgetNames drops the names that drop reports true for from the memo.
func (t *pgTx) forgetNames(drop func(nameKey) bool) {
	for k := range t.names {
		if drop(k) {
			delete(t.names, k)
		}
	}
}
