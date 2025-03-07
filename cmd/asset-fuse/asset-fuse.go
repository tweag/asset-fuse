package main

import (
	"os"
	"time"

	goFUSEfs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/tweag/asset-fuse/fs"
	"github.com/tweag/asset-fuse/fs/manifest"
	"github.com/tweag/asset-fuse/integrity"
	"github.com/tweag/asset-fuse/service/asset"
	"github.com/tweag/asset-fuse/service/cas"
	"github.com/tweag/asset-fuse/service/downloader"
	"github.com/tweag/asset-fuse/service/prefetcher"
)

func main() {
	manifestFile, err := os.Open("manifest.json")
	if err != nil {
		panic(err)
	}
	defer manifestFile.Close()

	view, ok := manifest.ViewFromString("default")
	if !ok {
		panic("unknown view")
	}
	manifest, err := manifest.TreeFromManifest(manifestFile, view, integrity.SHA256)
	if err != nil {
		panic(err)
	}

	diskCache, err := cas.NewDisk("/home/malte/.cache/asset-fuse")
	if err != nil {
		panic(err)
	}
	remoteCache, err := cas.NewRemote("grpcs://remote.buildbuddy.io")
	if err != nil {
		panic(err)
	}
	remoteAsset, err := asset.NewRemote("grpcs://remote.buildbuddy.io")
	prefetcher := prefetcher.NewPrefetcher(diskCache, remoteCache, remoteAsset, downloader.Downloader{}, integrity.SHA256)

	opts := goFUSEfs.Options{
		// We probably want different timeouts, depending
		// on whether we allow live-reloading of the manifest
		// or not.
		EntryTimeout: &defaultGoFUSETimeout,
		AttrTimeout:  &defaultGoFUSETimeout,
		// TODO: set good default mount options
		MountOptions: fuse.MountOptions{
			Debug: true,
		},
	}
	root := fs.Root(manifest, integrity.SHA256, time.Now(), "", prefetcher)

	server, err := goFUSEfs.Mount(os.Args[1], root, &opts)
	if err != nil {
		panic(err)
	}

	server.Wait()
}

var defaultGoFUSETimeout = time.Second
