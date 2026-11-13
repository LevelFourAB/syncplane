package pgstore

import (
	"fmt"

	"github.com/jackc/pgx/v5"
)

// The delta log and the consumer shadows are stored with int ids in place of
// the workspace id, the entity, the field and the consumer, which would
// otherwise repeat in every entry and in each index of it. field_deltas holds
// the workspace number, wsn, that index_scopes gives the workspace, and the id
// that text_fields gives the (entity, field) of the entry. consumer_shadows
// holds wsn and the id that shadow_sets gives the (consumer, entity) of the
// shadow. Both keep the row key as text.
//
// The ids of text_fields and shadow_sets are INTEGER, from the sequence
// name_ids, and none is used again. A write gives the ids it needs, once for
// each name in the life of a workspace, and a read that finds no id finds no
// entry. DeleteConsumer and CollectIdleConsumers delete the shadow_sets rows of
// the consumers they remove; text_fields rows go only with their workspace.

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
	// namesSQL reads every text_fields and shadow_sets row of workspace $1.
	namesSQL = `SELECT 0, entity, field, id FROM text_fields WHERE workspace_id = $1
UNION ALL
SELECT 1, consumer, entity, id FROM shadow_sets WHERE workspace_id = $1`

	// textFieldSQL gives the field ($2 entity, $3 field) of workspace $1 an id
	// and returns it, and shadowSetSQL the set ($2 consumer, $3 entity). The
	// caller holds the workspace lock and names only what the workspace does
	// not hold, so a name the table holds already fails on the primary key
	// instead of taking a second id.
	textFieldSQL = `INSERT INTO text_fields (workspace_id, entity, field, id)
VALUES ($1, $2, $3, nextval('name_ids')::int4) RETURNING id`
	shadowSetSQL = `INSERT INTO shadow_sets (workspace_id, consumer, entity, id)
VALUES ($1, $2, $3, nextval('name_ids')::int4) RETURNING id`
)

// decodeNames reads the result of namesSQL from br.
func decodeNames(br pgx.BatchResults) (map[nameKey]int32, error) {
	rows, err := br.Query()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	names := make(map[nameKey]int32)
	for rows.Next() {
		var (
			kind int32
			k    nameKey
			id   int32
		)
		if err := rows.Scan(&kind, &k.a, &k.b, &id); err != nil {
			return nil, err
		}
		k.kind = nameKind(kind)
		names[k] = id
	}
	return names, rows.Err()
}

// nameIDs returns the workspace number and the id of k, and false when either
// is missing. With create, it gives the workspace and k the ids they do not
// have, which costs a round trip for each; only a write transaction may ask
// for that.
func (t *pgTx) nameIDs(k nameKey, create bool) (wsn, id int32, ok bool, err error) {
	if err := t.loadNames(); err != nil {
		return 0, 0, false, err
	}
	wsn, hasWS := t.indexScopes[indexKey{}]
	id, hasID := t.names[k]
	if hasWS && hasID {
		return wsn, id, true, nil
	}
	if !create {
		return 0, 0, false, nil
	}
	if !hasWS {
		if err := t.makeIndexScopes([]indexKey{{}}); err != nil {
			return 0, 0, false, err
		}
		wsn = t.indexScopes[indexKey{}]
	}
	if !hasID {
		sql := textFieldSQL
		if k.kind == shadowSet {
			sql = shadowSetSQL
		}
		if err := t.queryRow(sql, t.ws, k.a, k.b).Scan(&id); err != nil {
			return 0, 0, false, fmt.Errorf("pgstore: give an id to %q %q: %w", k.a, k.b, err)
		}
		t.names[k] = id
	}
	return wsn, id, true, nil
}

// shadowSetIDs returns the workspace number and the ids of every shadow set of
// consumer, and false when the workspace has no number.
func (t *pgTx) shadowSetIDs(consumer string) (wsn int32, ids []int32, ok bool, err error) {
	if err := t.loadNames(); err != nil {
		return 0, nil, false, err
	}
	wsn, ok = t.indexScopes[indexKey{}]
	for k, id := range t.names {
		if k.kind == shadowSet && k.a == consumer {
			ids = append(ids, id)
		}
	}
	return wsn, ids, ok, nil
}
