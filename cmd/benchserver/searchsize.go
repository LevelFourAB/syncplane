package main

import (
	"net/http"
	"time"

	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/internal/benchapp"
)

// seedSearchHandler writes the text-heavy search corpus (see
// benchapp.SeedSearchSize) into benchapp.SearchSizeWorkspace. POST, no body;
// responds with the workspace, the row counts and how long it took. The
// client storage size benchmark (client/test/idbsize) calls it once per
// server: a second call writes the corpus again over the same keys.
func seedSearchHandler(srv *server.Server) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		started := time.Now()
		ws := benchapp.SearchSizeWorkspace
		counts, err := benchapp.SeedSearchSize(r.Context(), srv, ws)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, struct {
			Workspace string          `json:"workspace"`
			Counts    benchapp.Counts `json:"counts"`
			TookMs    int64           `json:"tookMs"`
		}{ws, counts, time.Since(started).Milliseconds()})
	})
}
