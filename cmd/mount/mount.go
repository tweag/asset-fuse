package mount

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	goFUSEfs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/tweag/asset-fuse/auth/credential"
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
	digestFunction, ok := integrity.AlgorithmFromString(globalConfig.DigestFunction)
	view, ok := manifest.ViewFromString(viewName)
	if !ok {
		cmdhelper.FatalFmt("invalid view: %s", viewName)
	}
	diskCache, err := cas.NewDisk(cmdhelper.SubstituteHome(globalConfig.DiskCachePath))
	if err != nil {
		cmdhelper.FatalFmt("creating disk cache at %s: %v", globalConfig.DiskCachePath, err)
	}
	var credentialHelper credential.Helper
	if len(globalConfig.CredentialHelper) > 0 {
		credentialHelper = credential.New(globalConfig.CredentialHelper)
	} else {
		logging.Warningf("No credential helper specified. Authentication may be required for some URIs.")
		credentialHelper = credential.NopHelper()
	}
	httpClient := &http.Client{Transport: credential.RoundTripper(credentialHelper)}
	downloader := downloader.New(diskCache, httpClient)
	var remoteCache cas.CAS
	var remoteAsset asset.Asset
	if len(globalConfig.Remote) > 0 {
		var err error
		remoteCache, err = cas.NewRemote(globalConfig.Remote, credentialHelper)
		if err != nil {
			cmdhelper.FatalFmt("creating remote cache at %s: %v", globalConfig.Remote, err)
		}
		var propagateCredentials bool
		if globalConfig.RemoteDownloaderPropagateCredentials != nil {
			propagateCredentials = *globalConfig.RemoteDownloaderPropagateCredentials
		}
		remoteAsset, err = asset.NewRemote(globalConfig.Remote, credentialHelper, propagateCredentials)
		if err != nil {
			cmdhelper.FatalFmt("creating remote asset service at %s: %v", globalConfig.Remote, err)
		}
		logging.Basicf("REAPI server: %s", globalConfig.Remote)
	} else {
		logging.Warningf("No REAPI server specified. Running in local mode.")
		// TODO: instead of nil, use an implementation that returns an error for all operations
		// to make sure that the code is not accidentally using the remote cache.
		// Additionally, we can signal to the prefetcher that it should not try to fetch anything.
	}
	checksumCache := integrity.NewCache()
	prefetcher := prefetcher.NewPrefetcher(diskCache, remoteCache, remoteAsset, downloader, checksumCache, digestFunction)
	stopPrefetcher, err := prefetcher.Start(ctx)
	if err != nil {
		cmdhelper.FatalFmt("starting prefetcher: %v", err)
	}
	defer stopPrefetcher()

	mountStat, err := os.Stat(mountPoint)
	if os.IsNotExist(err) {
		cmdhelper.FatalFmt("mount point %s does not exist", mountPoint)
	} else if err != nil {
		cmdhelper.FatalFmt("statting mount point %s: %v", mountPoint, err)
	}
	if !mountStat.IsDir() {
		cmdhelper.FatalFmt("mount point %s is not a directory", mountPoint)
	}

	logging.Basicf("Mounting %s at %s", globalConfig.ManifestPath, mountPoint)

	watcher, root, err := watcher.New(view, globalConfig, checksumCache, prefetcher)
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
