package mount

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	goFUSEfs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/tweag/asset-fuse/cmd/internal/cmdhelper"
	"github.com/tweag/asset-fuse/fs/manifest"
	"github.com/tweag/asset-fuse/fs/watcher"
	"github.com/tweag/asset-fuse/integrity"
	"github.com/tweag/asset-fuse/internal/logging"
	"github.com/tweag/asset-fuse/service/asset"
	"github.com/tweag/asset-fuse/service/cas"
	"github.com/tweag/asset-fuse/service/downloader"
	"github.com/tweag/asset-fuse/service/prefetcher"
)

func Run(ctx context.Context, args []string) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	var viewName string

	flagSet := flag.NewFlagSet("mount", flag.ExitOnError)
	flagSet.Usage = func() {
		fmt.Fprintf(flagSet.Output(), "Mounts the asset-fuse filesystem at the specified mountpoint.\n\n")
		fmt.Fprintf(flagSet.Output(), "Usage: asset-fuse mount [mountpoint]\n")
		flagSet.PrintDefaults()
		examples := []string{
			"asset-fuse mount ./mnt",
		}
		fmt.Fprintf(flagSet.Output(), "\nExamples:\n")
		for _, example := range examples {
			fmt.Fprintf(flagSet.Output(), "  $ %s\n", example)
		}
		os.Exit(1)
	}
	// TODO: validate viewName against the allowed values and print usage if needed
	flagSet.StringVar(&viewName, "view", "default", "The view to use on the manifest. Can be used to display assets in different hierarchies. Allowed values: [default, uri, repository_cache, bazel_disk_cache]")
	globalConfig, err := cmdhelper.InjectGlobalFlagsAndConfigure(args, flagSet, cmdhelper.FlagPresetRemote|cmdhelper.FlagPresetDiskCache|cmdhelper.FlagPresetFUSE)
	if err != nil {
		cmdhelper.FatalFmt("%v", err)
	}

	if flagSet.NArg() != 1 {
		flagSet.Usage()
	}

	mountPoint := flagSet.Arg(0)
	manifestFile, err := os.Open(globalConfig.ManifestPath)
	if err != nil {
		cmdhelper.FatalFmt("opening manifest file %s: %v", manifestFile, err)
	}
	defer manifestFile.Close()
	digestFunction, ok := integrity.AlgorithmFromString(globalConfig.DigestFunction)
	view, ok := manifest.ViewFromString(viewName)
	if !ok {
		cmdhelper.FatalFmt("invalid view: %s", viewName)
	}
	diskCache, err := cas.NewDisk(cmdhelper.SubstituteHome(globalConfig.DiskCachePath))
	if err != nil {
		cmdhelper.FatalFmt("creating disk cache at %s: %v", globalConfig.DiskCachePath, err)
	}
	remoteCache, err := cas.NewRemote("grpcs://remote.buildbuddy.io")
	if err != nil {
		cmdhelper.FatalFmt("creating remote cache at %s: %v", globalConfig.Remote, err)
	}
	remoteAsset, err := asset.NewRemote("grpcs://remote.buildbuddy.io")
	prefetcher := prefetcher.NewPrefetcher(diskCache, remoteCache, remoteAsset, downloader.Downloader{}, digestFunction)

	logging.Basicf("Mounting %s at %s", globalConfig.ManifestPath, mountPoint)

	watcher, root, err := watcher.New(view, globalConfig, prefetcher)
	if err != nil {
		cmdhelper.FatalFmt("creating manifest watcher: %v", err)
	}

	opts := goFUSEfs.Options{
		// We probably want different timeouts, depending
		// on whether we allow live-reloading of the manifest
		// or not.
		EntryTimeout: &defaultGoFUSETimeout,
		AttrTimeout:  &defaultGoFUSETimeout,
		MountOptions: fuse.MountOptions{
			Debug:                globalConfig.FUSEDebugEnable(),
			IgnoreSecurityLabels: true,
			FsName:               "asset-fuse",
			Name:                 "asset",
		},
	}
	rawFS := goFUSEfs.NewNodeFS(root, &opts)
	server, err := fuse.NewServer(rawFS, mountPoint, &opts.MountOptions)
	if err != nil {
		logging.Errorf("%v", err)
		cmdhelper.FatalFmt("Mounting the filesystem at %q failed. "+
			"In most cases, this is because of a previous instance of the filesystem wasn't properly unmounted. "+
			"Please ensure the mount point is ready by running:\n"+
			"  $ umount %s", mountPoint, mountPoint)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		// TODO: implement signal handler / context cancellation
		server.Serve()
	}()

	if err := server.WaitMount(); err != nil {
		cmdhelper.FatalFmt("mounting: %v", err)
	}

	// Starts the manifest watcher in the background.
	// Adds itself to the wait group.
	watcher.Start(ctx, wg)

	server.Wait()
}

var defaultGoFUSETimeout = 60 * time.Second
