package manifestdump

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/tweag/asset-fuse/cmd/internal/cmdhelper"
	"github.com/tweag/asset-fuse/fs/manifest"
	"github.com/tweag/asset-fuse/integrity"
	"github.com/tweag/asset-fuse/internal/logging"
)

func Run(ctx context.Context, args []string) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	var format string

	flagSet := flag.NewFlagSet("dump", flag.ExitOnError)
	flagSet.Usage = func() {
		fmt.Fprintf(flagSet.Output(), "Dump the resolved manifest to stdout.\n\n")
		fmt.Fprintf(flagSet.Output(), "Usage: asset-fuse manifest dump [ARGS...]\n")
		flagSet.PrintDefaults()
		examples := []string{
			"asset-fuse manifest dump --manifest=./acme_manifest.json",
		}
		fmt.Fprintf(flagSet.Output(), "\nExamples:\n")
		for _, example := range examples {
			fmt.Fprintf(flagSet.Output(), "  $ %s\n", example)
		}
		os.Exit(1)
	}

	flagSet.StringVar(&format, "format", "manifest", "The format to use when dumping the manifest. Allowed values: [manifest, bazel_download]")
	globalConfig, err := cmdhelper.InjectGlobalFlagsAndConfigure(args, flagSet, cmdhelper.FlagPresetNone)
	if err != nil {
		cmdhelper.FatalFmt("%v", err)
	}
	flagSet.Parse(args)

	if len(flagSet.Args()) > 0 {
		cmdhelper.FatalFmt("unexpected arguments: %v", flagSet.Args())
	}

	switch format {
	case "manifest", "bazel_download":
		// this is fine
	default:
		cmdhelper.FatalFmt("invalid format: %s", format)
	}

	manifestFile, err := os.OpenFile(globalConfig.ManifestPath, os.O_RDWR, 0)
	if err != nil {
		cmdhelper.FatalFmt("opening manifest file %s: %v", manifestFile, err)
	}
	defer manifestFile.Close()

	rawManifest, err := io.ReadAll(manifestFile)
	if err != nil {
		cmdhelper.FatalFmt("reading manifest file: %v", err)
	}
	initialManifest, err := manifest.ParseManifest(bytes.NewReader(rawManifest))
	if err != nil {
		cmdhelper.FatalFmt("parsing manifest: %v", err)
	}
	paths := initialManifest.Process()
	var validationErr manifest.ValidationError
	if errors.As(err, &validationErr) {
		logging.Warningf("manifest is invalid or incomplete: %v", err)
	} else if err != nil {
		cmdhelper.FatalFmt("parsing manifest: %v", err)
	}

	var output any

	switch format {
	case "manifest":
		output = formatManifest(paths)
	case "bazel_download":
		output = formatBazelDownload(paths)
	}

	if err := json.NewEncoder(os.Stdout).Encode(output); err != nil {
		cmdhelper.FatalFmt("encoding manifest as json: %v", err)
	}
}

func formatManifest(paths manifest.ManifestPaths) any {
	return manifest.Manifest{
		Paths: paths,
	}
}

func formatBazelDownload(paths manifest.ManifestPaths) any {
	var downloadList []bazelDownloadArgs
	for output, manifestPath := range paths {
		rawIntegrityStrings, err := manifestPath.GetIntegrity()
		if err != nil {
			cmdhelper.FatalFmt("%s: %v", output, err)
		}
		downloadIntegrity, err := integrity.IntegrityFromString(rawIntegrityStrings...)
		if err != nil {
			cmdhelper.FatalFmt("%s: %v", output, err)
		}
		downloadList = append(downloadList, bazelDownloadArgs{
			URL:        manifestPath.URIs,
			Output:     output,
			Executable: manifestPath.Executable,
			Integrity:  downloadIntegrity.ToSRIString(),
		})
	}
	return downloadList
}

type bazelDownloadArgs struct {
	URL        []string `json:"url"`
	Output     string   `json:"output"`
	Executable bool     `json:"executable,omitempty"`
	Integrity  string   `json:"integrity"`
}
