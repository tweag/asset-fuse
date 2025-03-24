package manifest

import (
	"context"
	"fmt"
	"os"

	mainfestdump "github.com/tweag/asset-fuse/cmd/manifest-dump"
	manifestupdate "github.com/tweag/asset-fuse/cmd/manifest-update"
)

const usage = `Usage: asset-fuse manifest [COMMAND] [ARGS...]

Commands:
  update  Update integrity checksums in the manifest
  dump    Dump the resolved manifest to stdout`

func Run(ctx context.Context, args []string) {
	if len(args) < 1 {
		printUsage()
	}

	command := args[0]
	switch command {
	case "update":
		manifestupdate.Run(ctx, args[1:])
	case "dump":
		mainfestdump.Run(ctx, args[1:])
	default:
		printUsage()
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, usage)
	os.Exit(1)
}
