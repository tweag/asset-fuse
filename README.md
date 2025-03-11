# asset-fuse (work in progress)

## Simple demo (no remote cache required)

Terminal 1:
```
mkdir -p mnt
go run ./cmd/asset-fuse mount --manifest examples/manifest_source_code.json --log_level=debug mnt
```

Terminal 2:
```
cd mnt
# now you can browse around in mnt
find .
getfattr -R -n user.sha256 mnt
cat print_fortune.go
```

After the build, ensure no process is holding on to files in `mnt` (by killing the Bazel server and not being in the directory with your shell), then unmount: `umount mnt`.


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
