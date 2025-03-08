package main

import (
	"context"
	"os"

	"github.com/tweag/asset-fuse/cmd/root"
)

// TODO:
// - add flag handling
// - add logging
// - add subcommands for different operations
//   - `asset-fuse mount` to mount the filesystem
//   - `asset-fuse serve` to start a (local) unix domain socket server
//   - `asset-fuse add` to add a file to the manifest
//   - `asset-fuse get` to fetch uri(s) (thereby populating the [remote and/or local] cache with the asset(s), or simply to check if the asset is available and get the digest)
//   - `asset-fuse put` to push a file to the remote asset service
//   - `asset-fuse prefetch` to prefetch all assets in the manifest
//   - `asset-fuse vendor` to copy all assets in the manifest to a directory or archive (for offline use without the asset-fuse filesystem)
func main() {
	root.Run(context.Background(), os.Args)
}
