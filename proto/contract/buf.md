# syncplane

Wire contract for [syncplane](https://github.com/levelfourab/syncplane), a sync
engine for local-first apps. Clients read and write a local KV store; a
server-authoritative mutation log keeps replicas converged by optimistic rebase
on the client.

The module holds `syncplane/v1`: the RPC surface (`SyncService`,
`AdminService`), the patch and query types, and `options.proto`, which carries
the annotations you put on your own messages. The server's cursor internals,
the test fixtures and the demo schema stay in the repository by design.

## Depending on it

```yaml
# buf.yaml
version: v2
modules:
  - path: proto
deps:
  - buf.build/levelfourab/syncplane
```

Then `buf dep update` to pin it in `buf.lock`. Your entity imports the
annotations directly:

```proto
import "syncplane/v1/options.proto";

message Thing {
	option (syncplane.v1.entity) = {
		name: "thing"
		key_fields: [ "id" ]
	};

	string id = 1;
	string title = 2;
}
```

## One constraint, if you generate Go

`options.proto` declares
`go_package = "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"`, and
that is load-bearing: `protoc-gen-go` turns your annotation import into a blank
import of whatever `go_package` the imported file declares, which is how your
annotations resolve to the same extension types the engine reads.

Managed mode rewrites `go_package` for every file it sees, including this one.
Exempt it:

```yaml
# buf.gen.yaml
managed:
  enabled: true
  override:
    - file_option: go_package_prefix
      value: github.com/mycorp/app/gen
  disable:
    # Share the engine's extension types rather than registering a second copy.
    - file_option: go_package
      path: syncplane/v1/options.proto
```

Generating your own copy instead puts a second `syncplane/v1/options.proto` in
the binary, and protobuf-go's global registry rejects that at init.

## Documentation

- [Define entities in your own module](https://github.com/levelfourab/syncplane/blob/main/docs/how-to/define-entities.md)
- [Proto options reference](https://github.com/levelfourab/syncplane/blob/main/docs/reference/proto-options.md)
- [Documentation index](https://github.com/levelfourab/syncplane/blob/main/docs/README.md)
