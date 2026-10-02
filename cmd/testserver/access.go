package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// accessPrototypes are the entities of the access-model test schema, by
// entity name.
var accessPrototypes = map[string]proto.Message{
	"org_member":     &accessv1.OrgMember{},
	"project":        &accessv1.Project{},
	"project_member": &accessv1.ProjectMember{},
	"task":           &accessv1.Task{},
	"label":          &accessv1.Label{},
	"note":           &accessv1.Note{},
}

// accessOptions is the access-model test schema, which the -access flag
// serves instead of the default one. A schema that uses the model admits a
// caller only with grant rows, so a suite seeds them through /debug/trusted.
func accessOptions() []server.Option {
	msgs := make([]proto.Message, 0, len(accessPrototypes))
	for _, m := range accessPrototypes {
		msgs = append(msgs, m)
	}
	return []server.Option{server.WithEntities(msgs...)}
}

// trustedWriteHandler writes rows as a trusted caller: POST a JSON body
// {"workspace": "...", "writes": [{"entity": "...", "value": {...}} |
// {"entity": "...", "key": "...", "delete": true}]}. Values are the entity
// message in protojson form. Every write lands in one transaction.
func trustedWriteHandler(srv *server.Server) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Workspace string `json:"workspace"`
			Writes    []struct {
				Entity string          `json:"entity"`
				Key    string          `json:"key"`
				Delete bool            `json:"delete"`
				Value  json.RawMessage `json:"value"`
			} `json:"writes"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		err := srv.Mutate(server.Trusted(r.Context()), req.Workspace, func(tx *server.EntityTx) error {
			for _, wr := range req.Writes {
				proto, ok := accessPrototypes[wr.Entity]
				if !ok {
					return fmt.Errorf("unknown entity %q", wr.Entity)
				}
				if wr.Delete {
					if err := tx.Delete(proto, wr.Key); err != nil {
						return err
					}
					continue
				}
				msg := proto.ProtoReflect().New().Interface()
				if err := protojson.Unmarshal(wr.Value, msg); err != nil {
					return err
				}
				if err := tx.Put(msg); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
}
