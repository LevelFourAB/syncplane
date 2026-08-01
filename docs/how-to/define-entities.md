# Define entities in your own module

An entity is a proto message annotated with `(syncplane.v1.entity)`:

```proto
message Thing {
	option (syncplane.v1.entity) = {
		name: "thing"
		key_fields: [ "id" ]
	};

	string id = 1;
	string title = 2;
}
```

Declaring it is one option, and it gives you the full CRUD surface on both
sides:

```go
srv, err := server.New(store,
	server.WithEntities(&mycorpv1.Thing{}),   // reads (syncplane.v1.entity) options
)
```

See [Proto options](../reference/proto-options.md) for `key_fields`, keyed
lists, movable lists, opaque fields and collaborative text.

Two other routes exist if proto codegen is not what you want: declare the schema
[in TypeScript](define-schemas-in-typescript.md) and register it at runtime, or
build it dynamically in Go with `server.DynamicField`.

## Getting `options.proto` into your codegen

Your `.proto` imports `syncplane/v1/options.proto`, so your codegen needs that
file. It ships as the buf module `buf.build/levelfourab/syncplane`, so add a
dependency rather than vendoring:

```yaml
# buf.yaml
version: v2
modules:
  - path: proto
deps:
  - buf.build/levelfourab/syncplane
```

`buf dep update` pins it in your `buf.lock`. The module contains `syncplane/v1`
only — the server's cursor types, the test fixtures and the demo schema are not
published.

That settles where the file comes from. What it does **not** settle is the
constraint below, which applies however you obtain the file — dependency,
vendored copy or `buf export`.

**`options.proto`'s `go_package` must keep pointing at the
`github.com/levelfourab/syncplane` Go module, and you must not generate Go for
it.** `protoc-gen-go` compiles an options-only import into a blank import of
whatever `go_package` the imported file declares:

```go
// thing.pb.go, generated from your own proto
import _ "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
```

That blank import is the whole mechanism — it pulls in the extension types the
engine compiled against, so your annotations resolve to the same
`(syncplane.v1.entity)` the engine reads. Generating your own copy instead puts
a second `syncplane/v1/options.proto` in the binary, which protobuf-go's global
registry rejects at init:

```
panic: proto: file "syncplane/v1/options.proto" is already registered
```

### With buf

Managed mode rewrites `go_package` for every file it sees, and a dependency is
no exception — the file arrives from the registry with the right `go_package`
and managed mode overwrites it. Exempt it by path:

```yaml
# buf.gen.yaml
version: v2
managed:
  enabled: true
  override:
    - file_option: go_package_prefix
      value: github.com/mycorp/app/gen
  disable:
    # Share the engine's extension types rather than registering a second copy.
    - file_option: go_package
      path: syncplane/v1/options.proto
inputs:
  - directory: proto
    paths:
      - proto/mycorp/v1/thing.proto # options.proto is an import, not a target
plugins:
  - remote: buf.build/protocolbuffers/go:v1.36.11
    out: gen
    opt: paths=source_relative
```

Nothing generates Go for `options.proto` here: it reaches the compiler as an
import of your own file, never as a target.

### With protoc

`protoc` cannot resolve a buf dependency, so put the file on disk first —
`buf export buf.build/levelfourab/syncplane -o vendor/proto` writes
`syncplane/v1/options.proto` with its `go_package` intact. Then pass
`Msyncplane/v1/options.proto=github.com/levelfourab/syncplane/engine/gen/syncplane/v1`
and leave `options.proto` off the list of target files.

Registration is then identical to an in-repo entity.

## If it goes wrong anyway

Should the extension types diverge — a renumbered option, or a build that
suppressed the registry conflict — the engine still recovers the annotation from
the option bytes it cannot interpret (`internal/protoopts`), and
Registration reports the `go_package` mismatch instead of claiming the
message is unannotated.

## Two consequences worth knowing

- The `github.com/levelfourab/syncplane` module path and the
  `engine/gen/syncplane/v1` package layout are **part of the public contract**,
  since they are baked into every adopter's generated code.
- An app that would rather not run Go codegen at all can bypass this entirely by
  shipping a `FileDescriptorSet` — see [Define schemas in TypeScript](define-schemas-in-typescript.md).
