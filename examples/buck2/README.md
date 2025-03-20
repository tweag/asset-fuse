# Example of using asset-fuse with Buck2

Just like Bazel, Buck2 can read digests from extended attributes (xattr) if the underlying filesystem supports it.
With this optimization, source files can be immediately used as action inputs without their contents being read by Buck2.
Unlike Bazel, Buck2 can download files in build rules via `ctx.actions.download_file`. This should make it possible to optimize downloads away if
the files exist remotely, but so far the author wasn't able to avoid eager downloads with the buck2 download action.
With FUSE, we can definitely get opimal performance by avoiding file downloads.

## How to use:

Terminal 1:
```
umount mnt
go run ../../cmd/asset-fuse mount --unix_digest_hash_attribute_encoding=hex --manifest ../manifests/manifest_datasets.json --log_level=debug mnt
```

Terminal 2:
```
buck2 build //:fast_air_quality_line_count
buck2 clean
# this disables the optimization
BUCK2_DISABLE_FILE_ATTR=1 buck2 build //:fast_air_quality_line_count
buck2 kill
```

Notice how (depending on your settings), Buck2 will either call `read` on the file, or only issue a `getxattr` (both should be clearly visible in the FUSE logs):

After the build, ensure no process is holding on to files in `mnt` (by killing the Buck2 server and not being in the directory with your shell), then unmount: `umount mnt`.
