package export

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
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
	"golang.org/x/sys/unix"
)

func Run(ctx context.Context, args []string) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	var hollow bool
	var destTypeTar bool
	var destTypeDir bool
	var xattrMode string
	var destination string
	var destType destinationType

	flagSet := flag.NewFlagSet("export", flag.ExitOnError)
	flagSet.Usage = func() {
		fmt.Fprintf(flagSet.Output(), "Exports the manifest to a directory or archive.\n\n")
		fmt.Fprintf(flagSet.Output(), "Usage: asset-fuse export [ARGS...] [DESTINATION]\n")
		flagSet.PrintDefaults()
		examples := []string{
			"asset-fuse export ./vendor-dir/",
			"asset-fuse export --hollow --xattr-digest-mode=enforce ./sparse-vendor-dir/",
			"asset-fuse export assets.tar",
			"asset-fuse export - | gzip -c > assets.tar.gz",
		}
		fmt.Fprintf(flagSet.Output(), "\nExamples:\n")
		for _, example := range examples {
			fmt.Fprintf(flagSet.Output(), "  $ %s\n", example)
		}
		os.Exit(1)
	}

	flagSet.BoolVar(&destTypeTar, "tar", false, `Export to a tar archive`)
	flagSet.BoolVar(&destTypeDir, "dir", false, `Export to a directory`)
	flagSet.BoolVar(&hollow, "hollow", false, `Creates sparse files (no data - just holes containing 0x00), instead of real files`)
	flagSet.StringVar(&xattrMode, "xattr-digest-mode", "auto", `Controls the writing of extended attributes containing digests for exported files. Allowed values: ["auto", "off", "enforce"]`)
	globalConfig, err := cmdhelper.InjectGlobalFlagsAndConfigure(args, flagSet, cmdhelper.FlagPresetRemote|cmdhelper.FlagPresetDiskCache)
	if err != nil {
		cmdhelper.FatalFmt("%v", err)
	}
	flagSet.Parse(args)

	if flagSet.NArg() != 1 {
		flagSet.Usage()
	}

	destination = flagSet.Arg(0)
	switch {
	case destTypeTar && destTypeDir:
		logging.Errorf("Cannot specify both --tar and --dir")
		flagSet.Usage()
	case !destTypeTar && !destTypeDir:
		// auto detect
		if destination == "-" {
			destType = destinationTypeTar
			break
		}
		fInfo, err := os.Stat(destination)
		if err == nil && fInfo.IsDir() {
			destType = destinationTypeDir
			break
		} else if err == nil && fInfo.Mode().IsRegular() {
			destType = destinationTypeTar
			break
		}
		if strings.HasSuffix(destination, ".tar") {
			destType = destinationTypeTar
			break
		}
		if strings.HasSuffix(destination, "/") {
			destType = destinationTypeDir
			break
		}
		cmdhelper.FatalFmt("Cannot determine destination type for %s automatically. Specify --tar or --dir.", destination)
	case destTypeTar:
		destType = destinationTypeTar
	case destTypeDir:
		destType = destinationTypeDir
	}
	switch xattrMode {
	case "auto", "off", "enforce":
		// allowed
	default:
		logging.Errorf("Invalid xattr-digest-mode: %s", xattrMode)
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

	pathsToExport := make(map[string]manifest.Leaf, len(paths))
	for path, entry := range paths {
		leaf, err := manifest.LeafFromEntry(entry)
		if err != nil {
			cmdhelper.FatalFmt("creating leaf node for %s: %v", path, err)
		}
		pathsToExport[path] = leaf
	}
	for _, leaf := range pathsToExport {
		// Try to prefill the checksum cache with the checksums from the initial manifest.
		if checksum, ok := leaf.Integrity.ChecksumForAlgorithm(digestFunction); ok && leaf.SizeHint >= 0 {
			digest := integrity.NewDigest(checksum.Hash, leaf.SizeHint, digestFunction)
			checksumCache.PutIntegrity(leaf.Integrity, digest)
		}
	}

	if err := export(ctx, pathsToExport, destination, destType, xattrMode, hollow, prefetcher, globalConfig); err != nil {
		cmdhelper.FatalFmt("%v", err)
	}
	logging.Basicf("Exported %d assets", len(pathsToExport))
}

func export(
	ctx context.Context, pathsToExport map[string]manifest.Leaf, destination string, destType destinationType,
	xattrMode string, isHollow bool, prefetcher *prefetcher.Prefetcher,
	globalConfig api.GlobalConfig,
) error {
	// prepare the destination
	var tarWriter *tar.Writer
	switch destType {
	case destinationTypeDir:
		if err := os.Mkdir(destination, 0o755); err != nil && !os.IsExist(err) {
			return fmt.Errorf("creating destination directory: %w", err)
		}
	case destinationTypeTar:
		var output *os.File
		if destination == "-" {
			output = os.Stdout
		} else {
			var err error
			output, err = os.Create(destination)
			if err != nil {
				return fmt.Errorf("creating destination tar file: %w", err)
			}
			defer output.Close()
		}
		tarWriter = tar.NewWriter(output)
		defer tarWriter.Close()
	}

	pathnames := make([]string, 0, len(pathsToExport))
	for path := range pathsToExport {
		pathnames = append(pathnames, path)
	}
	slices.Sort(pathnames)

	for _, path := range pathnames {
		leaf := pathsToExport[path]
		asset := api.Asset{
			URIs:      leaf.URIs,
			Integrity: leaf.Integrity,
		}

		digest, err := prefetcher.AssetDigest(ctx, asset)
		if err != nil {
			return fmt.Errorf("fetching digest for %s: %w", path, err)
		}

		reader, err := prefetcher.RandomAccessStream(ctx, asset, 0, 0)
		if err != nil {
			return fmt.Errorf("downloading %s: %w", path, err)
		}
		// TODO: maybe close explicitly to avoid defer in a loop
		defer reader.Close()

		switch destType {
		case destinationTypeDir:
			if err := streamIntoDir(asset, reader, destination, path, digest.SizeBytes, leaf.Executable, xattrMode, isHollow, globalConfig); err != nil {
				return err
			}
		case destinationTypeTar:
			if err := streamIntoTar(asset, reader, tarWriter, path, digest.SizeBytes, leaf.Executable, xattrMode, isHollow, globalConfig); err != nil {
				return err
			}
		}
	}
	return nil
}

func streamIntoDir(asset api.Asset, reader io.Reader, destdir, path string, size int64, isExecutable bool, xattrMode string, isHollow bool, globalConfig api.GlobalConfig) error {
	destPath := filepath.Join(destdir, path)
	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return fmt.Errorf("creating directory %s: %w", filepath.Dir(destPath), err)
	}
	output, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("creating file %s: %w", destPath, err)
	}
	defer output.Close()
	if isHollow {
		if err := output.Truncate(size); err != nil {
			return fmt.Errorf("creating sparse file %s: %w", destPath, err)
		}
	} else {
		if _, err := io.Copy(output, reader); err != nil {
			return fmt.Errorf("writing file %s: %w", destPath, err)
		}
	}
	mode := os.FileMode(0o644)
	if isExecutable {
		mode = 0o755
	}
	if err := output.Chmod(mode); err != nil {
		return fmt.Errorf("setting permissions for %s: %w", destPath, err)
	}

	// maybe set xattrs
	if xattrMode == "off" {
		return nil
	}
	xattrKvPairs := xattrsForAsset(asset, globalConfig)
	for name, value := range xattrKvPairs {
		if err := unix.Fsetxattr(int(output.Fd()), name, value, 0); err != nil {
			if xattrMode == "enforce" {
				return fmt.Errorf("setting xattr for %s: %w", destPath, err)
			}
			logging.Warningf("Failed to set xattr for %s: %v", destPath, err)
		}
	}
	return nil
}

func streamIntoTar(asset api.Asset, reader io.Reader, output *tar.Writer, path string, size int64, isExecutable bool, xattrMode string, isHollow bool, globalConfig api.GlobalConfig) error {
	// TODO: sparse files ("TypeGNUSparse", readGNUSparsePAXHeaders)
	if isHollow {
		return errors.New("sparse file support is not yet implemented for tar archives")
	}
	header := &tar.Header{
		Name: path,
		Size: size,
		Mode: 0o644,
	}
	if isExecutable {
		header.Mode = 0o755
	}
	if xattrMode != "off" {
		xattrKvPairs := xattrsForAsset(asset, globalConfig)
		if len(xattrKvPairs) > 0 {
			header.PAXRecords = make(map[string]string, len(xattrKvPairs))
		}
		for name, value := range xattrKvPairs {
			header.PAXRecords["SCHILY.xattr."+name] = string(value)
		}
	}
	if err := output.WriteHeader(header); err != nil {
		return fmt.Errorf("writing tar header for %s: %w", path, err)
	}
	if _, err := io.Copy(output, reader); err != nil {
		return fmt.Errorf("writing tar data for %s: %w", path, err)
	}
	return nil
}

func xattrsForAsset(asset api.Asset, globalConfig api.GlobalConfig) map[string][]byte {
	xattrs := make(map[string][]byte)
	for checksum := range asset.Integrity.Items() {
		xattrs[fmt.Sprintf("user.%s", checksum.Algorithm.String())] = checksum.Hash
	}
	digestFunction, _ := integrity.AlgorithmFromString(globalConfig.DigestFunction)
	if checksum, ok := asset.Integrity.ChecksumForAlgorithm(digestFunction); ok && globalConfig.DigestXattrName != "" {
		var digestValue []byte
		switch globalConfig.DigestXattrEncoding {
		case "hex":
			digestValue = []byte(checksum.Hex())
		case "raw":
			digestValue = checksum.Hash
		default:
			logging.Warningf("Invalid digest xattr encoding: %s", globalConfig.DigestXattrEncoding)
			digestValue = checksum.Hash
		}
		xattrs[globalConfig.DigestXattrName] = digestValue
	}
	return xattrs
}

type destinationType int

const (
	destinationTypeUnknown destinationType = iota
	destinationTypeTar
	destinationTypeDir
)
