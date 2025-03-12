package manifestupdate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/tweag/asset-fuse/auth/credential"
	"github.com/tweag/asset-fuse/cmd/internal/cmdhelper"
	"github.com/tweag/asset-fuse/fs/manifest"
	"github.com/tweag/asset-fuse/integrity"
	"github.com/tweag/asset-fuse/internal/logging"
)

func Run(ctx context.Context, args []string) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	var all bool

	flagSet := flag.NewFlagSet("update", flag.ExitOnError)
	flagSet.Usage = func() {
		fmt.Fprintf(flagSet.Output(), "Updates integrity checksums in the manifest.\n\n")
		fmt.Fprintf(flagSet.Output(), "Usage: asset-fuse manifest update [ARGS...] [TARGETS]\n")
		flagSet.PrintDefaults()
		examples := []string{
			"asset-fuse manifest update --all",
			"asset-fuse manifest update --manifest=./acme_manifest.json testdata/largefile.bin production/vm_image.qcow2",
		}
		fmt.Fprintf(flagSet.Output(), "\nExamples:\n")
		for _, example := range examples {
			fmt.Fprintf(flagSet.Output(), "  $ %s\n", example)
		}
		os.Exit(1)
	}

	flagSet.BoolVar(&all, "all", false, "Update all targets in the manifest")
	globalConfig, err := cmdhelper.InjectGlobalFlagsAndConfigure(args, flagSet, cmdhelper.FlagPresetNone)
	if err != nil {
		cmdhelper.FatalFmt("%v", err)
	}
	flagSet.Parse(args)

	if !all && flagSet.NArg() == 0 {
		logging.Errorf("No targets specified. Use --all to update all targets in the manifest.")
		flagSet.Usage()
	}
	if all && flagSet.NArg() > 0 {
		logging.Errorf("Cannot specify targets when using --all.")
		flagSet.Usage()
	}

	manifestFile, err := os.OpenFile(globalConfig.ManifestPath, os.O_RDWR, 0)
	if err != nil {
		cmdhelper.FatalFmt("opening manifest file %s: %v", manifestFile, err)
	}
	defer manifestFile.Close()

	digestFunction, ok := integrity.AlgorithmFromString(globalConfig.DigestFunction)
	if !ok {
		cmdhelper.FatalFmt("invalid digest function: %s", globalConfig.DigestFunction)
	}

	view, ok := manifest.ViewFromString("default")
	if !ok {
		cmdhelper.FatalFmt("invalid view: %s", "default")
	}

	targets := flagSet.Args()
	if all {
		logging.Basicf("Updating all targets")
	} else {
		logging.Debugf("Updating targets: %v", strings.Join(targets, " "))
	}

	rawManifest, err := io.ReadAll(manifestFile)
	if err != nil {
		cmdhelper.FatalFmt("reading manifest file: %v", err)
	}
	manifestFile.Seek(0, 0)
	initialManifestTree, err := manifest.TreeFromManifest(bytes.NewReader(rawManifest), view, digestFunction)
	if err != nil {
		cmdhelper.FatalFmt("parsing manifest: %v", err)
	}

	var credentialHelper credential.Helper
	if len(globalConfig.CredentialHelper) > 0 {
		credentialHelper = credential.New(globalConfig.CredentialHelper)
	} else {
		logging.Warningf("No credential helper specified. Authentication may be required for some URIs.")
		credentialHelper = credential.NopHelper()
	}

	httpClient := &http.Client{Transport: credential.RoundTripper(credentialHelper)}

	updatedPaths, unchangedPaths, err := updateManifest(ctx, initialManifestTree, targets, digestFunction, httpClient)
	if err != nil {
		cmdhelper.FatalFmt("updating manifest: %v", err)
	}
	if len(updatedPaths) == 0 {
		logging.Basicf("No paths changed")
		return
	}
	logging.Basicf("%d paths changed, %d paths unchanged", len(updatedPaths), len(unchangedPaths))

	// write the updated manifest back to the file
	manifest, err := manifest.ParseManifest(bytes.NewReader(rawManifest))
	if err != nil {
		cmdhelper.FatalFmt("parsing manifest: %v", err)
	}
	for path, leaf := range updatedPaths {
		entry := manifest.Paths[path]
		sriList := leaf.Integrity.ToSRIList()
		var sriMessage json.RawMessage
		var marshalErr error
		if len(sriList) == 1 {
			sriMessage, marshalErr = json.Marshal(sriList[0])
		} else {
			sriMessage, marshalErr = json.Marshal(leaf.Integrity.ToSRIList())
		}
		if marshalErr != nil {
			cmdhelper.FatalFmt("marshalling integrity: %v", err)
		}
		entry.Integrity = json.RawMessage(sriMessage)
		entry.Size = &leaf.SizeHint
		manifest.Paths[path] = entry
	}

	updatedRawManifest, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		cmdhelper.FatalFmt("marshalling manifest: %v", err)
	}
	os.WriteFile(globalConfig.ManifestPath, updatedRawManifest, 0o644)
}

func updateManifest(ctx context.Context, oldManifest manifest.ManifestTree, targets []string, digestFunction integrity.Algorithm, httpClient *http.Client) (updatedPaths map[string]manifest.Leaf, unchangedPaths []string, err error) {
	targetMap := make(map[string]manifest.Leaf)
	if len(targets) == 0 {
		// "--all" mode
		for p, leaf := range oldManifest.Leafs {
			targetMap[p] = *leaf
		}
	} else {
		for _, target := range targets {
			leaf, ok := oldManifest.Leafs[target]
			if !ok {
				return nil, nil, fmt.Errorf("target not found: %s", target)
			}
			targetMap[target] = *leaf
		}
	}

	updatedPaths = make(map[string]manifest.Leaf, len(targetMap))
	var updateErrors []error
	for path, leaf := range targetMap {
		updatedLeaf, changed, err := updateLeaf(ctx, path, leaf, digestFunction, httpClient)
		if err != nil {
			updateErrors = append(updateErrors, fmt.Errorf("updating %s: %v", path, err))
			continue
		}
		if changed {
			updatedPaths[path] = updatedLeaf
		} else {
			unchangedPaths = append(unchangedPaths, path)
		}
	}
	if len(updateErrors) > 0 {
		return nil, nil, fmt.Errorf("errors updating manifest:\n%w", errors.Join(updateErrors...))
	}
	return updatedPaths, unchangedPaths, nil
}

func updateLeaf(ctx context.Context, path string, leaf manifest.Leaf, digestFunction integrity.Algorithm, httpClient *http.Client) (manifest.Leaf, bool, error) {
	// let's take a random URI from the list - this might help uncover dead links.
	uri := leaf.URIs[rand.Intn(len(leaf.URIs))]
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, http.NoBody)
	if err != nil {
		return manifest.Leaf{}, false, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return manifest.Leaf{}, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return manifest.Leaf{}, false, fmt.Errorf("unexpected HTTP status code for %s: %d", uri, resp.StatusCode)
	}

	// TODO: make digest functions configurable
	updatedIntegrity, sizeBytes, err := integrity.IntegrityFromContent(resp.Body, digestFunction)
	if err != nil {
		return manifest.Leaf{}, false, err
	}

	var changed bool
	oldIntegrity := leaf.Integrity
	oldSize := leaf.SizeHint

	// check if the new digest is different from the old one
	if !updatedIntegrity.Equivalent(leaf.Integrity) {
		changed = true
		leaf.Integrity = updatedIntegrity
	}
	// check if the new size hint is different from the old one
	if leaf.SizeHint != sizeBytes {
		changed = true
		leaf.SizeHint = sizeBytes
	}
	if changed {
		logging.Basicf("Updated integrity for %s: %q (%d bytes) → %q (%d bytes)", path, oldIntegrity.ToSRIString(), oldSize, updatedIntegrity.ToSRIString(), sizeBytes)
	}
	return leaf, changed, nil
}
