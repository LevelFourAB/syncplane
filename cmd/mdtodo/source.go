package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/levelfourab/syncplane/engine/connect"
	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
)

// mdSource implements connect.Source over a folder of Markdown files: one
// file per todo at <root>/<workspace>/<id>.md, scalar fields in a front
// matter block, the title as the body. Editing a file (or dropping a new one
// in) is a write into the sync log; deleting it deletes the todo everywhere.
//
//	---
//	id: t1
//	completed: true
//	created_at_ms: 1718000000000
//	sort_key: a0
//	---
//	Buy milk
type mdSource struct {
	root string
}

// The folder is written back to as well as read, so the connector needs both
// halves of the port.
var (
	_ connect.Source = (*mdSource)(nil)
	_ connect.Writer = (*mdSource)(nil)
)

// fileName makes an id filesystem-safe; PathEscape leaves ordinary ids
// untouched and the appended extension keeps even "." and ".." inert.
func fileName(id string) string {
	return url.PathEscape(id) + ".md"
}

func (s *mdSource) wsDir(workspaceID string) string {
	return filepath.Join(s.root, url.PathEscape(workspaceID))
}

// notesDir holds the Note documents: one plain Markdown file per note at
// <root>/<workspace>/notes/<id>.md — the whole file IS the note's text. The
// text field syncs through Item.TextFields, so an external file edit merges
// with concurrent client edits instead of replacing the shared document.
func (s *mdSource) notesDir(workspaceID string) string {
	return filepath.Join(s.wsDir(workspaceID), "notes")
}

func (s *mdSource) List(ctx context.Context, workspaceID string, fn func(connect.Item) error) error {
	entries, err := os.ReadDir(s.wsDir(workspaceID))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".md") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(s.wsDir(workspaceID), e.Name()))
		if err != nil {
			return err
		}
		todo, err := parseTodo(raw)
		if err != nil {
			return fmt.Errorf("mdtodo: %s: %w", e.Name(), err)
		}
		if err := fn(connect.Item{Msg: todo, Fingerprint: fingerprint(raw)}); err != nil {
			return err
		}
	}
	noteEntries, err := os.ReadDir(s.notesDir(workspaceID))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range noteEntries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".md") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(s.notesDir(workspaceID), e.Name()))
		if err != nil {
			return err
		}
		id, err := url.PathUnescape(strings.TrimSuffix(e.Name(), ".md"))
		if err != nil {
			return fmt.Errorf("mdtodo: notes/%s: %w", e.Name(), err)
		}
		if err := fn(connect.Item{
			Msg:         &todov1.Note{Id: id},
			TextFields:  map[string]string{"body": strings.TrimSuffix(string(raw), "\n")},
			Fingerprint: fingerprint(raw),
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *mdSource) Write(ctx context.Context, workspaceID string, msg proto.Message) (string, error) {
	var (
		dir  string
		name string
		raw  []byte
	)
	switch m := msg.(type) {
	case *todov1.Todo:
		dir, name, raw = s.wsDir(workspaceID), fileName(m.GetId()), renderTodo(m)
	case *todov1.Note:
		// Egress renders the document to its plain string — the only
		// representation the file holds.
		body, err := connect.PlainTextField(m, "body")
		if err != nil {
			return "", err
		}
		dir, name, raw = s.notesDir(workspaceID), fileName(m.GetId()), []byte(body+"\n")
	default:
		return "", fmt.Errorf("mdtodo: unexpected entity %T", msg)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	// Atomic write: editors and the watcher never observe a torn file.
	tmp, err := os.CreateTemp(dir, ".mdtodo-*")
	if err != nil {
		return "", err
	}
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return "", err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return "", err
	}
	if err := os.Rename(tmp.Name(), filepath.Join(dir, name)); err != nil {
		os.Remove(tmp.Name())
		return "", err
	}
	return fingerprint(raw), nil
}

func (s *mdSource) Delete(ctx context.Context, workspaceID string, prototype proto.Message, key string) error {
	dir := s.wsDir(workspaceID)
	if _, ok := prototype.(*todov1.Note); ok {
		dir = s.notesDir(workspaceID)
	}
	err := os.Remove(filepath.Join(dir, fileName(key)))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func fingerprint(raw []byte) string {
	return fmt.Sprintf("%x", sha256.Sum256(raw))
}

func renderTodo(t *todov1.Todo) []byte {
	var b strings.Builder
	b.WriteString("---\n")
	fmt.Fprintf(&b, "id: %s\n", t.GetId())
	fmt.Fprintf(&b, "completed: %t\n", t.GetCompleted())
	fmt.Fprintf(&b, "created_at_ms: %d\n", t.GetCreatedAtMs())
	fmt.Fprintf(&b, "sort_key: %s\n", t.GetSortKey())
	b.WriteString("---\n")
	b.WriteString(t.GetTitle())
	b.WriteString("\n")
	return []byte(b.String())
}

func parseTodo(raw []byte) (*todov1.Todo, error) {
	sc := bufio.NewScanner(strings.NewReader(string(raw)))
	if !sc.Scan() || strings.TrimSpace(sc.Text()) != "---" {
		return nil, fmt.Errorf("missing front matter")
	}
	todo := &todov1.Todo{}
	for sc.Scan() {
		line := sc.Text()
		if strings.TrimSpace(line) == "---" {
			var body []string
			for sc.Scan() {
				body = append(body, sc.Text())
			}
			todo.Title = strings.TrimRight(strings.Join(body, "\n"), "\n")
			if todo.GetId() == "" {
				return nil, fmt.Errorf("front matter lacks id")
			}
			return todo, sc.Err()
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		value = strings.TrimSpace(value)
		switch strings.TrimSpace(key) {
		case "id":
			todo.Id = value
		case "completed":
			todo.Completed = value == "true"
		case "created_at_ms":
			ms, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("bad created_at_ms %q", value)
			}
			todo.CreatedAtMs = ms
		case "sort_key":
			todo.SortKey = value
		}
	}
	return nil, fmt.Errorf("unterminated front matter")
}
