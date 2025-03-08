package root

import (
	"context"
	"fmt"
	"os"

	"github.com/tweag/asset-fuse/api"
	"github.com/tweag/asset-fuse/cmd/mount"
	"github.com/tweag/asset-fuse/internal/logging"
)

const usage = `Usage: asset-fuse [COMMAND] [ARGS...]

Commands:
  mount  Mount the filesystem`

func Run(ctx context.Context, args []string) {
	setLogLevel()
	if len(args) < 2 {
		printUsage()
	}

	command := args[1]
	switch command {
	case "mount":
		mount.Run(ctx, args[2:])
	default:
		printUsage()
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, usage)
	os.Exit(1)
}

func setLogLevel() {
	level, ok := os.LookupEnv(api.LogLevelEnv)
	if !ok {
		return
	}
	logging.SetLevel(logging.FromString(level))
}
