// Command admindemo is a throwaway harness for eyeballing the built-in admin
// UI. It registers a couple of dynamic apps (with references + sub-objects),
// seeds a workspace with a few rows so the browse pages have something to show,
// and serves the admin UI. Delete it whenever; it is not part of the product.
//
//	go run ./cmd/admindemo
//	# then open the printed URL
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
	"github.com/levelfourab/syncplane/internal/admin"
)

const (
	addr = "127.0.0.1:8090"
	ws   = "production"
	pkg  = "taskflow.v1"
)

func main() {
	ctx := context.Background()

	dir, err := os.MkdirTemp("", "admindemo-*")
	must(err)
	store, err := sqlitestore.Open(filepath.Join(dir, "demo.db"))
	must(err)
	defer store.Close()

	mgr, err := server.NewManager(ctx, store)
	must(err)
	defer mgr.Close()

	// App "taskflow": users, tags, projects and todos — exercising the new
	// admin UI's reference chips/avatars (assignees, tags, project), a bool
	// pill (done), a sub-message keyed list (checklist) and plain fields.
	user := server.DynamicEntity{
		Name: "user", Package: pkg, MessageName: "User", KeyFields: []string{"id"},
		Fields: []server.DynamicField{
			{Name: "id", Number: 1, Kind: protoreflect.StringKind},
			{Name: "name", Number: 2, Kind: protoreflect.StringKind},
		},
	}
	tag := server.DynamicEntity{
		Name: "tag", Package: pkg, MessageName: "Tag", KeyFields: []string{"id"},
		Fields: []server.DynamicField{
			{Name: "id", Number: 1, Kind: protoreflect.StringKind},
			{Name: "name", Number: 2, Kind: protoreflect.StringKind},
		},
	}
	project := server.DynamicEntity{
		Name: "project", Package: pkg, MessageName: "Project", KeyFields: []string{"id"},
		Fields: []server.DynamicField{
			{Name: "id", Number: 1, Kind: protoreflect.StringKind},
			{Name: "name", Number: 2, Kind: protoreflect.StringKind},
		},
	}
	todo := server.DynamicEntity{
		Name: "todo", Package: pkg, MessageName: "Todo", KeyFields: []string{"id"},
		Fields: []server.DynamicField{
			{Name: "id", Number: 1, Kind: protoreflect.StringKind},
			{Name: "title", Number: 2, Kind: protoreflect.StringKind},
			{Name: "description", Number: 3, Kind: protoreflect.StringKind},
			{Name: "done", Number: 4, Kind: protoreflect.BoolKind},
			{Name: "project", Number: 5, Ref: "project"},
			{Name: "assignees", Number: 6, Ref: "user", Repeated: true},
			{Name: "tags", Number: 7, Ref: "tag", Repeated: true},
			{Name: "checklist", Number: 8, Repeated: true, KeyField: []string{"id"}, Message: []server.DynamicField{
				{Name: "id", Number: 1, Kind: protoreflect.StringKind},
				{Name: "text", Number: 2, Kind: protoreflect.StringKind},
				{Name: "done", Number: 3, Kind: protoreflect.BoolKind},
			}},
			{Name: "due_date", Number: 9, Kind: protoreflect.StringKind},
			{Name: "created_at", Number: 10, Kind: protoreflect.StringKind},
		},
	}
	registerApp(ctx, mgr, "taskflow", user, tag, project, todo)

	// App "notes": a single flat entity, so the app switcher has more than one.
	note := server.DynamicEntity{
		Name: "note", Package: pkg, MessageName: "Note", KeyFields: []string{"id"},
		Fields: []server.DynamicField{
			{Name: "id", Number: 1, Kind: protoreflect.StringKind},
			{Name: "body", Number: 2, Kind: protoreflect.StringKind},
		},
	}
	registerApp(ctx, mgr, "notes", note)

	// Seed reference targets first, then todos that point at them.
	seed(ctx, mgr, "taskflow", "user", map[string]any{"id": "u_maya", "name": "Maya Chen"})
	seed(ctx, mgr, "taskflow", "user", map[string]any{"id": "u_sam", "name": "Sam Okafor"})
	seed(ctx, mgr, "taskflow", "user", map[string]any{"id": "u_tariq", "name": "Tariq Rahman"})
	seed(ctx, mgr, "taskflow", "user", map[string]any{"id": "u_lena", "name": "Lena Hart"})
	seed(ctx, mgr, "taskflow", "tag", map[string]any{"id": "backend", "name": "backend"})
	seed(ctx, mgr, "taskflow", "tag", map[string]any{"id": "urgent", "name": "urgent"})
	seed(ctx, mgr, "taskflow", "tag", map[string]any{"id": "infra", "name": "infra"})
	seed(ctx, mgr, "taskflow", "project", map[string]any{"id": "sync_core", "name": "Sync Core"})
	seed(ctx, mgr, "taskflow", "project", map[string]any{"id": "mobile", "name": "Mobile"})

	seed(ctx, mgr, "taskflow", "todo", map[string]any{
		"id": "td_01", "title": "Resolve conflict-merge for offline edits",
		"description": "Last-writer-wins is dropping nested message fields when two clients edit the same sub-object offline.",
		"project":     ref("id", "sync_core"),
		"assignees":   refs("id", "u_maya", "u_sam"),
		"tags":        refs("id", "backend", "urgent"),
		// The checklists across td_01 and td_02 are shaped so a filter group
		// shows its work: both rows have an item about a "spec", but only
		// td_02's is still open, so "has an item where text contains spec AND
		// done is no" separates them while either half alone does not.
		"checklist": items(
			map[string]any{"id": "c1", "text": "Write the merge spec", "done": true},
			map[string]any{"id": "c2", "text": "Ship the rebase fix"},
		),
		"due_date": "2026-06-24", "created_at": "2026-05-30",
	})
	seed(ctx, mgr, "taskflow", "todo", map[string]any{
		"id": "td_02", "title": "Mobile delta-sync battery profiling",
		"assignees": refs("id", "u_tariq", "u_lena"),
		"tags":      refs("id", "infra"),
		"project":   ref("id", "mobile"),
		"checklist": items(
			map[string]any{"id": "c1", "text": "Write the profiling spec"},
			map[string]any{"id": "c2", "text": "Collect battery traces", "done": true},
		),
		"due_date": "2026-06-26", "created_at": "2026-06-01",
	})
	seed(ctx, mgr, "taskflow", "todo", map[string]any{
		"id": "td_03", "title": "Billing usage metering events",
		"assignees": refs("id", "u_sam", "u_tariq"),
		"tags":      refs("id", "backend"),
		"due_date":  "2026-07-12", "created_at": "2026-06-05",
	})
	seed(ctx, mgr, "taskflow", "todo", map[string]any{
		"id": "td_04", "title": "Audit log for entity mutations", "done": true,
		"assignees": refs("id", "u_lena"),
		"tags":      refs("id", "backend"),
		"due_date":  "2026-06-05", "created_at": "2026-05-12",
	})

	seed(ctx, mgr, "notes", "note", map[string]any{"id": "n1", "body": "hello"})

	fmt.Printf("ADMIN  http://%s/\n", addr)
	fmt.Printf("       taskflow todos: http://%s/apps/taskflow/ws/%s/e/todo\n", addr, ws)
	log.Fatal(http.ListenAndServe(addr, admin.Handler(mgr)))
}

func registerApp(ctx context.Context, mgr *server.Manager, appID string, specs ...server.DynamicEntity) {
	_, err := mgr.Register(ctx, appID, descriptorSetBytes(specs...))
	must(err)
}

// seed writes one row of entity in app appID, in workspace ws, from a field map.
// A field value may be a ref(...) to set a reference's key fields.
func seed(ctx context.Context, mgr *server.Manager, appID, entity string, fields map[string]any) {
	srv := mgr.App(appID)
	meta, ok := srv.Entity(entity)
	if !ok {
		log.Fatalf("unknown entity %q in app %q", entity, appID)
	}
	msg := dynamicpb.NewMessage(meta.Descriptor)
	setFields(msg, fields)
	must(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error { return tx.Put(msg) }))
}

type refVal struct {
	field string
	value string
}

type refList struct {
	field  string
	values []string
}

// msgList is a keyed message list, one field map per element.
type msgList struct {
	elems []map[string]any
}

func ref(field, value string) refVal { return refVal{field: field, value: value} }

// refs builds a to-many reference value from several target keys.
func refs(field string, values ...string) refList { return refList{field: field, values: values} }

// items builds a keyed message-list value, one field map per element.
func items(elems ...map[string]any) msgList { return msgList{elems: elems} }

func setFields(m protoreflect.Message, fields map[string]any) {
	fds := m.Descriptor().Fields()
	for name, v := range fields {
		fd := fds.ByName(protoreflect.Name(name))
		if fd == nil {
			log.Fatalf("unknown field %q", name)
		}
		switch val := v.(type) {
		case string:
			m.Set(fd, protoreflect.ValueOfString(val))
		case bool:
			m.Set(fd, protoreflect.ValueOfBool(val))
		case int:
			m.Set(fd, protoreflect.ValueOfInt32(int32(val)))
		case refVal:
			m.Set(fd, refMessage(m, fd, val.field, val.value))
		case refList:
			list := m.Mutable(fd).List()
			for _, target := range val.values {
				list.Append(refMessage(m, fd, val.field, target))
			}
		case msgList:
			list := m.Mutable(fd).List()
			for _, elem := range val.elems {
				e := list.NewElement()
				setFields(e.Message(), elem)
				list.Append(e)
			}
		default:
			log.Fatalf("unsupported seed value for %q: %T", name, v)
		}
	}
}

// refMessage builds one reference element for fd, setting its target key field.
func refMessage(m protoreflect.Message, fd protoreflect.FieldDescriptor, field, value string) protoreflect.Value {
	elem := m.NewField(fd)
	if fd.IsList() {
		elem = m.Mutable(fd).List().NewElement()
	}
	kf := elem.Message().Descriptor().Fields().ByName(protoreflect.Name(field))
	elem.Message().Set(kf, protoreflect.ValueOfString(value))
	return elem
}

// descriptorSetBytes assembles the FileDescriptorSet an app registers:
// descriptor.proto + syncplane/v1/options.proto + one synthesized file holding every
// entity (mirrors the test helper of the same name).
func descriptorSetBytes(specs ...server.DynamicEntity) []byte {
	descFile := (&descriptorpb.FileDescriptorProto{}).ProtoReflect().Descriptor().ParentFile()
	set := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{
			protodesc.ToFileDescriptorProto(descFile),
			protodesc.ToFileDescriptorProto(syncplanev1.File_syncplane_v1_options_proto),
		},
	}
	fd, err := server.BuildDynamicFile(specs...)
	must(err)
	set.File = append(set.File, protodesc.ToFileDescriptorProto(fd))
	raw, err := proto.Marshal(set)
	must(err)
	return raw
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
