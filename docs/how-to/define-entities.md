# Defining entities in your own module

This guide shows you how to define Syncplane entities in your own Protocol Buffers schemas, generate Go code for them, and register them with a Syncplane server.

Use this guide when you maintain proto schemas outside the Syncplane repository and want full CRUD support for your entities.

If you do not want to use proto code generation, you can declare the schema [in TypeScript](define-schemas-in-typescript.md) and register it at runtime, or build it dynamically in Go with `server.DynamicField`. You can also bypass Go code generation by shipping a `FileDescriptorSet`.

## Prerequisites

Before you begin, ensure you have:

- A Protocol Buffers toolchain (`buf` or `protoc`).
- A Go project with `github.com/levelfourab/syncplane` installed.

## 1. Add the Syncplane proto dependency

Your `.proto` definitions import `syncplane/v1/options.proto`, which ships in the Buf module `buf.build/levelfourab/syncplane`.

If you use `buf`, add the dependency to `buf.yaml`:

```yaml
# buf.yaml
version: v2
modules:
  - path: proto
deps:
  - buf.build/levelfourab/syncplane
```

Run `buf dep update` to pin the dependency in your `buf.lock` file.

**Note:** The module contains `syncplane/v1` only. Server cursor types, test fixtures, and the demo schema are not published.

If you use `protoc`, export the dependency to your local file system:

```
buf export buf.build/levelfourab/syncplane -o vendor/proto
```

This exports `syncplane/v1/options.proto` with its `go_package` option intact.

## 2. Annotate your proto message

Import `syncplane/v1/options.proto` and annotate your message with `(syncplane.v1.entity)`:

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

For more options, such as `key_fields`, keyed lists, movable lists, opaque fields, and collaborative text, see [Proto options](../reference/proto-options.md).

## 3. Configure Go code generation

Configure your code generator so that `options.proto` points to the `github.com/levelfourab/syncplane` Go module and does not generate Go code in your project.

### If you use buf

Managed mode rewrites `go_package` for every file by default. Disable `go_package` rewriting for `syncplane/v1/options.proto` and exclude it from the target inputs in `buf.gen.yaml`:

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

### If you use protoc

Pass `Msyncplane/v1/options.proto=github.com/levelfourab/syncplane/engine/gen/syncplane/v1` to `protoc` and omit `options.proto` from the list of target files.

## 4. Register the entity with the server

Pass your generated entity to `server.WithEntities` when creating a server:

```go
srv, err := server.New(store,
	server.WithEntities(&mycorpv1.Thing{}),   // reads (syncplane.v1.entity) options
)
```

Registration is identical to registering an in-repo entity.

## Confirming the registration

Verify that your generated code imports the Syncplane Go package:

```go
// thing.pb.go, generated from your own proto
import _ "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
```

When you start your server, verify that initialization completes without panicking. The server reads the `(syncplane.v1.entity)` options and provides CRUD operations for the entity.

## Troubleshooting

### Duplicate proto registration panic

If you generate Go code for `options.proto` in your own repository instead of importing the engine package, protobuf-go rejects the second copy at initialization:

```
panic: proto: file "syncplane/v1/options.proto" is already registered
```

To resolve this panic, verify that `options.proto` is an import rather than a generation target, and ensure that its `go_package` option points to `github.com/levelfourab/syncplane/engine/gen/syncplane/v1`.

### Option extension type mismatch

If extension types diverge due to a renumbered option or a build that suppressed the registry conflict, the engine recovers the annotation from uninterpreted option bytes (`internal/protoopts`). Registration reports the `go_package` mismatch instead of reporting that the message is unannotated.

## Related

- [Limits and trade-offs](../explanation/limits-and-trade-offs.md)
