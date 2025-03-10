# asset-fuse (work in progress)

## How to simulate a Bazel workspace for testing:

Terminal 1:
```
mkdir -p mnt
go run ./cmd/asset-fuse mount --manifest examples/manifest_source_code.json --log_level=debug mnt
```

Terminal 2:
```
cd mnt
bazel --unix_digest_hash_attribute_name=user.sha256 build ... --noexperimental_convenience_symlinks
bazel --unix_digest_hash_attribute_name=user.sha256 shutdown
```

After the build, ensure no process is holding on to files in `mnt` (by killing the Bazel server and not being in the directory with your shell), then unmount: `umount mnt`.
