package download

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/tweag/asset-fuse/api"
	"github.com/tweag/asset-fuse/auth/credential"
	"github.com/tweag/asset-fuse/cmd/internal/cmdhelper"
	"github.com/tweag/asset-fuse/fs/manifest"
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
	var destination string

	flagSet := flag.NewFlagSet("download", flag.ExitOnError)
	flagSet.Usage = func() {
		fmt.Fprintf(flagSet.Output(), "Fetches assets to the disk cache (or remote cache) for offline use.\n\n")
		fmt.Fprintf(flagSet.Output(), "Usage: asset-fuse download [ARGS...] [TARGETS]\n")
		flagSet.PrintDefaults()
		examples := []string{
			"asset-fuse download",
			"asset-fuse download --destination=remote",
			"asset-fuse download production/vm_image.qcow2",
		}
		fmt.Fprintf(flagSet.Output(), "\nExamples:\n")
		for _, example := range examples {
			fmt.Fprintf(flagSet.Output(), "  $ %s\n", example)
		}
		os.Exit(1)
	}

	flagSet.StringVar(&destination, "destination", "disk", `The destination of the downloaded assets. Allowed values: ["disk", "remote"]`)
	globalConfig, err := cmdhelper.InjectGlobalFlagsAndConfigure(args, flagSet, cmdhelper.FlagPresetRemote|cmdhelper.FlagPresetDiskCache)
	if err != nil {
		cmdhelper.FatalFmt("%v", err)
	}
	flagSet.Parse(args)
	if destination != "disk" && destination != "remote" {
		logging.Errorf("Invalid destination: %s", destination)
		flagSet.Usage()
	}

	rawManifest, err := os.ReadFile(globalConfig.ManifestPath)
	if err != nil {
		cmdhelper.FatalFmt("reading manifest file: %v", err)
	}
	initialManifest, err := manifest.ParseManifest(bytes.NewReader(rawManifest))
	if err != nil {
		cmdhelper.FatalFmt("parsing manifest: %v", err)
	}
	paths := initialManifest.Process()
	if err != nil {
		cmdhelper.FatalFmt("parsing manifest: %v", err)
	}

	digestFunction, ok := integrity.AlgorithmFromString(globalConfig.DigestFunction)
	if !ok {
		cmdhelper.FatalFmt("invalid digest function: %s", globalConfig.DigestFunction)
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

	targets := flagSet.Args()
	var pathsToDownload map[string]manifest.Leaf
	if len(targets) == 0 {
		logging.Basicf("Downloading all targets into %s cache", destination)
		pathsToDownload = make(map[string]manifest.Leaf, len(paths))
		for path, entry := range paths {
			leaf, err := manifest.LeafFromEntry(entry)
			if err != nil {
				cmdhelper.FatalFmt("creating leaf node for %s: %v", path, err)
			}
			pathsToDownload[path] = leaf
		}
	} else {
		logging.Debugf("Downloading the following targets into %s cache: %v", destination, strings.Join(targets, " "))
		pathsToDownload = make(map[string]manifest.Leaf)
		for _, target := range targets {
			if entry, ok := paths[target]; ok {
				leaf, err := manifest.LeafFromEntry(entry)
				if err != nil {
					cmdhelper.FatalFmt("creating leaf node for %s: %v", target, err)
				}
				pathsToDownload[target] = leaf
			} else {
				cmdhelper.FatalFmt("path %s not found in the manifest", target)
			}
		}
	}
	for _, leaf := range pathsToDownload {
		// Try to prefill the checksum cache with the checksums from the initial manifest.
		if checksum, ok := leaf.Integrity.ChecksumForAlgorithm(digestFunction); ok && leaf.SizeHint >= 0 {
			digest := integrity.NewDigest(checksum.Hash, leaf.SizeHint, digestFunction)
			checksumCache.PutIntegrity(leaf.Integrity, digest)
		}
	}

	if err := download(pathsToDownload, destination, prefetcher); err != nil {
		cmdhelper.FatalFmt("%v", err)
	}
	logging.Basicf("Downloaded %d assets", len(pathsToDownload))
}

func download(pathsToDownload map[string]manifest.Leaf, destination string, prefetcher *prefetcher.Prefetcher) error {
	var enqueueForDownload func(asset api.Asset, callbacks ...func(api.Asset, integrity.Digest, error))
	switch destination {
	case "disk":
		enqueueForDownload = prefetcher.EnqueueLocalDownload
	case "remote":
		enqueueForDownload = prefetcher.EnqueueRemoteDownload
	}

	results := make(chan downloadResult, len(pathsToDownload))
	defer close(results)

	for _, leaf := range pathsToDownload {
		asset := api.Asset{
			URIs:      leaf.URIs,
			Integrity: leaf.Integrity,
		}
		enqueueForDownload(asset, func(asset api.Asset, digest integrity.Digest, err error) {
			results <- downloadResult{asset, digest, err}
		})
	}

	var errors []error
	for range pathsToDownload {
		result := <-results
		if result.err != nil {
			logging.Errorf("download: %v", result.err)
			errors = append(errors, result.err)
		}
	}
	if len(errors) > 0 {
		return fmt.Errorf("Not all assets were downloaded successfully. %d errors occurred.", len(errors))
	}
	return nil
}

type downloadResult struct {
	asset  api.Asset
	digest integrity.Digest
	err    error
}
