# Example of using asset-fuse and rules_gcs together

This example demonstrates how to use FUSE if available, but fall back to a repository rule if needed.

## How to use with FUSE:

Terminal 1:
```
umount mnt
go run ../../cmd/asset-fuse mount --manifest=manifest.json --view=bazel_repo --log_level=debug mnt
```

Terminal 2:
```
bazel build //:air_quality_line_count
bazel shutdown
```

After the build, ensure no process is holding on to files in `mnt` (by killing the Bazel server and not being in the directory with your shell), then unmount: `umount mnt`.

## How to use without FUSE:

```
bazel build @hello_world//:hello_world
```
