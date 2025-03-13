# Example of using asset-fuse with Bazel

> Build-without-the-bytes for assets

This example demonstrates the biggest optimization provided by the FUSE filesystem.
The following conditions need to be met:
- Bazel must be configured to read digests from extended file attributes (`--unix_digest_hash_attribute_name`)
- You need a remote build execution (RBE) backend with the following features:
    - remote cache (content-addressable storage)
    - remote asset service
    - remote execution
- Bazel needs to be configured to use remote execution (`--remote_executor`)
- The filesystem needs to use the same RBE backend as Bazel (`--remote`)
- The filesystem needs to be mounted inside of a Bazel workspace (or a local repository), so that the containing files are visible as source files.

When all of these conditions are met, Bazel can avoid ever calling `read` on files mounted via FUSE.
Instead, Bazel learns the digest (size and hash) of each file via file attributes.
In the background, we use the remote asset API to tell the remote cache to make the requested blobs available for remote execution.
With this, source files that live in FUSE can immediately be used as action inputs for remote execution, without ever being read by Bazel. In simpler terms, this avoids downloading large files on the machine that Bazel runs on, leaving this work to the RBE backend (and only doing it once).

## How to use:

Terminal 1:
```
umount mnt
go run ../../cmd/asset-fuse mount --manifest ../manifests/manifest_datasets.json --log_level=debug mnt
```

Terminal 2:
```
bazel build //...
# this disables the optimization
bazel --unix_digest_hash_attribute_name= build //...
bazel shutdown
```

Notice how (depending on your settings), Bazel will either call `read` on the file, or only issue a `getxattr` (both should be clearly visible in the FUSE logs):

After the build, ensure no process is holding on to files in `mnt` (by killing the Bazel server and not being in the directory with your shell), then unmount: `umount mnt`.
