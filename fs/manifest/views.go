package manifest

import (
	"bytes"
	"fmt"
	"net/url"
	"path"
	"strings"
	"text/template"

	"github.com/tweag/asset-fuse/integrity"
	"github.com/tweag/asset-fuse/internal/logging"
)

// View describes a view onto the manifest.
type View struct {
	name          string
	treeGenerator func(ManifestPaths, integrity.Algorithm) (ManifestTree, error)
	FakeLeafs     map[string][]byte
}

func ViewFromString(name string) (View, bool) {
	v, ok := knownViews[name]
	return v, ok
}

func (v View) Tree(paths ManifestPaths, digestFunction integrity.Algorithm) (ManifestTree, error) {
	tree, err := v.treeGenerator(paths, digestFunction)
	if err != nil {
		return tree, err
	}
	for name, content := range v.FakeLeafs {
		digest, err := digestFunction.CalculateDigest(bytes.NewReader(content))
		if err != nil {
			return tree, fmt.Errorf("calculating digest for fake leaf %s: %w", name, err)
		}
		if err := tree.Insert(name, Leaf{
			Integrity: integrity.IntegrityFromChecksums(integrity.ChecksumFromDigest(digest, digestFunction)),
			SizeHint:  int64(len(content)),
		}); err != nil {
			return tree, fmt.Errorf("inserting fake leaf %s into tree: %w", name, err)
		}
		logging.Debugf("Inserted fake leaf %s into the tree", name)
	}
	return tree, nil
}

func defaultTreeView(paths ManifestPaths, _ integrity.Algorithm) (ManifestTree, error) {
	tree := NewTree()
	for path, entry := range paths {
		leaf, err := LeafFromEntry(entry)
		if err != nil {
			return ManifestTree{}, fmt.Errorf("building leaf node %s for tree from manifest: %w", path, err)
		}
		if err := tree.Insert(path, leaf); err != nil {
			return ManifestTree{}, fmt.Errorf("inserting %s from manifest into tree: %w", path, err)
		}
	}

	return tree, nil
}

func bazelRepoTreeView(paths ManifestPaths, digestFunction integrity.Algorithm) (ManifestTree, error) {
	tree, err := defaultTreeView(paths, digestFunction)
	if err != nil {
		return ManifestTree{}, err
	}

	return tree, nil
}

func uriTreeView(paths ManifestPaths, _ integrity.Algorithm) (ManifestTree, error) {
	tree := NewTree()
	for path, entry := range paths {
		for _, uri := range entry.URIs {
			leaf, err := LeafFromEntry(entry)
			if err != nil {
				return ManifestTree{}, fmt.Errorf("building leaf node %s for tree from manifest: %w", path, err)
			}

			uriPath, ok := pathForURIView(uri)
			if !ok {
				return ManifestTree{}, fmt.Errorf("building leaf node %s for tree from manifest: uri is not usable as a path", uri)
			}
			if err := tree.Insert(uriPath, leaf); err != nil && err != insertionPathConflictError {
				return ManifestTree{}, fmt.Errorf("inserting uri %s from manifest into tree: %w", uri, err)
			}
		}
	}

	return tree, nil
}

func pathForURIView(uri string) (string, bool) {
	if !strings.Contains(uri, "/") {
		return uri, true
	}
	if u, err := url.Parse(uri); err == nil {
		if len(u.Scheme) > 0 {
			return path.Join(u.Scheme, u.Host, u.Path), true
		}
		return path.Join(u.Host, u.Path), true
	}
	// path contains slashes but is not parseable as a URL
	return "", false
}

func casView(paths ManifestPaths, templateStr string, onlyPrimaryAlgorithm bool, digestFunction integrity.Algorithm) (ManifestTree, error) {
	tpl := template.Must(template.New("casView").Parse(templateStr))

	tree := NewTree()
	pathsToCreateWithURIs := map[integrity.Algorithm]map[integrity.Digest]casViewLeafInfo{}
	for path, entry := range paths {
		sriList, err := entry.getIntegrity()
		if err != nil {
			return ManifestTree{}, fmt.Errorf("building leaf node %s for tree from manifest: %w", path, err)
		}
		leafIntegrity, err := integrity.IntegrityFromString(sriList...)
		if err != nil {
			return ManifestTree{}, fmt.Errorf("building leaf node %s for tree from manifest: %w", path, err)
		}
		if onlyPrimaryAlgorithm {
			// Filter for the primary algorithm
			checksum, ok := leafIntegrity.ChecksumForAlgorithm(digestFunction)
			if !ok {
				// the primary algorithm is not present in the leaf
				continue
			}
			leafIntegrity = integrity.IntegrityFromChecksums(checksum)
		}
		var sizeBytes int64 = -1
		if entry.Size != nil {
			sizeBytes = *entry.Size
		}
		for checksum := range leafIntegrity.Items() {
			// technically, the digest is incorrect, because we fake the size
			// We only need it as a key to group paths, so it's fine.
			digest := integrity.NewDigest(checksum.Hash, 0, checksum.Algorithm)
			if _, ok := pathsToCreateWithURIs[checksum.Algorithm]; !ok {
				pathsToCreateWithURIs[checksum.Algorithm] = map[integrity.Digest]casViewLeafInfo{}
			}
			leafInfo, ok := pathsToCreateWithURIs[checksum.Algorithm][digest]
			if !ok {
				pathsToCreateWithURIs[checksum.Algorithm][digest] = casViewLeafInfo{
					URIs:       []string{},
					SizeHint:   sizeBytes,
					Executable: entry.Executable,
				}
			}
			leafInfo.URIs = append(leafInfo.URIs, entry.URIs...)
			leafInfo.SizeHint = sizeBytes
			pathsToCreateWithURIs[checksum.Algorithm][digest] = leafInfo
		}
	}

	for algorithm, digestsToURIs := range pathsToCreateWithURIs {
		for digest, casViewLeafInfo := range digestsToURIs {
			leaf := Leaf{
				URIs:       casViewLeafInfo.URIs,
				Integrity:  integrity.IntegrityFromChecksums(integrity.ChecksumFromDigest(digest, algorithm)),
				SizeHint:   casViewLeafInfo.SizeHint,
				Executable: casViewLeafInfo.Executable,
			}
			var pathBuilder strings.Builder
			tpl.Execute(&pathBuilder, casViewTemplateData{
				Algorithm: algorithm.String(),
				DigestHex: digest.Hex(algorithm),
			})
			if err := tree.Insert(pathBuilder.String(), leaf); err != nil {
				return ManifestTree{}, fmt.Errorf("inserting cas entry %s from manifest into tree: %w", pathBuilder.String(), err)
			}
		}
	}

	return tree, nil
}

type casViewLeafInfo struct {
	URIs       []string
	SizeHint   int64
	Executable bool
}

type casViewTemplateData struct {
	Algorithm string
	DigestHex string
}

var (
	defaultView   = View{name: "default", treeGenerator: defaultTreeView}
	bazelRepoView = View{
		name:          "bazel_repo",
		treeGenerator: bazelRepoTreeView,
		// hack: we inject some additional entries into the tree
		FakeLeafs: map[string][]byte{
			"REPO.bazel":  emptyRepoFile,
			"BUILD.bazel": bazelBUILDFileExportAll,
		},
	}
	uriView             = View{name: "uri", treeGenerator: uriTreeView}
	repositoryCacheView = View{name: "repository_cache", treeGenerator: func(paths ManifestPaths, digestFunction integrity.Algorithm) (ManifestTree, error) {
		return casView(paths, "content_addressable/{{ .Algorithm }}/{{ .DigestHex }}/file", false, digestFunction)
	}}
	diskCacheView = View{name: "bazel_disk_cache", treeGenerator: func(paths ManifestPaths, digestFunction integrity.Algorithm) (ManifestTree, error) {
		return casView(paths, `cas/{{ printf "%.2s" .DigestHex }}/{{ .DigestHex }}`, true, digestFunction)
	}}
)

var (
	bazelBUILDFileExportAll = []byte("# generated by asset-fuse\nexports_files(glob([\"**\"]))\n")
	emptyRepoFile           = []byte{}
)

var knownViews = map[string]View{
	"default":          defaultView,
	"bazel_repo":       bazelRepoView,
	"uri":              uriView,
	"repository_cache": repositoryCacheView,
	"bazel_disk_cache": diskCacheView,
}
