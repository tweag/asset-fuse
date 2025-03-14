package main

import (
	"context"
	"os"

	"github.com/tweag/asset-fuse/cmd/root"
)

// TODO:
// - add subcommands for different operations
//   - `asset-fuse mount` to mount the filesystem
//   - `asset-fuse serve` to start a (local) unix domain socket server (what does this really expose? gRPC services? REST API? NFS?)
//   - `asset-fuse push` to push a file (or directory) to the remote asset service
//   - `asset-fuse download` to fetch uri(s) (thereby populating the [remote and/or local] cache with the asset(s)
//   - `asset-fuse export` to copy all assets in the manifest to a directory or archive (for offline use without the asset-fuse filesystem)
//   - `asset-fuse manifest add` to add a file to the manifest
//   - `asset-fuse manifest update` to get the status of the asset-fuse service
func main() {
	root.Run(context.Background(), os.Args)
}
