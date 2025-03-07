package manifest

import (
	"fmt"
	"net/url"
	"path"
	"strings"
	"text/template"

	"github.com/tweag/asset-fuse/integrity"
)

// View describes a view onto the manifest.
type View struct {
	name          string
	treeGenerator func(Manifest, integrity.Algorithm) (ManifestTree, error)
}

func ViewFromString(name string) (View, bool) {
	v, ok := knownViews[name]
	return v, ok
}

func (v View) Tree(manifest Manifest, digestFunction integrity.Algorithm) (ManifestTree, error) {
	return v.treeGenerator(manifest, digestFunction)
}

func defaultTreeView(manifest Manifest, _ integrity.Algorithm) (ManifestTree, error) {
	tree := NewTree()
	for path, entry := range manifest {
		sriList, err := entry.getIntegrity()
		if err != nil {
			return ManifestTree{}, fmt.Errorf("building leaf node %s for tree from manifest: %w", path, err)
		}
		leafIntegrity, err := integrity.IntegrityFromString(sriList...)
		if err != nil {
			return ManifestTree{}, fmt.Errorf("building leaf node %s for tree from manifest: %w", path, err)
		}
		leaf := Leaf{
			URIs:       entry.URIs,
			Integrity:  leafIntegrity,
			SizeHint:   -1,
			Executable: entry.Executable,
		}
		if entry.Size != nil {
			leaf.SizeHint = *entry.Size
		}
		if err := tree.Insert(path, leaf); err != nil {
			return ManifestTree{}, fmt.Errorf("inserting %s from manifest into tree: %w", path, err)
		}
	}

	return tree, nil
}

func uriTreeView(manifest Manifest, _ integrity.Algorithm) (ManifestTree, error) {
	tree := NewTree()
	for path, entry := range manifest {
		sriList, err := entry.getIntegrity()
		if err != nil {
			return ManifestTree{}, fmt.Errorf("building leaf node %s for tree from manifest: %w", path, err)
		}
		leafIntegrity, err := integrity.IntegrityFromString(sriList...)
		if err != nil {
			return ManifestTree{}, fmt.Errorf("building leaf node %s for tree from manifest: %w", path, err)
		}
		for _, uri := range entry.URIs {
			leaf := Leaf{
				URIs:       []string{uri},
				Integrity:  leafIntegrity,
				SizeHint:   -1,
				Executable: entry.Executable,
			}
			if entry.Size != nil {
				leaf.SizeHint = *entry.Size
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

func casView(manifest Manifest, templateStr string, onlyPrimaryAlgorithm bool, digestFunction integrity.Algorithm) (ManifestTree, error) {
	tpl := template.Must(template.New("casView").Parse(templateStr))

	tree := NewTree()
	pathsToCreateWithURIs := map[integrity.Algorithm]map[integrity.Digest]casViewLeafInfo{}
	for path, entry := range manifest {
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
	defaultView         = View{name: "default", treeGenerator: defaultTreeView}
	uriView             = View{name: "uri", treeGenerator: uriTreeView}
	repositoryCacheView = View{name: "repository_cache", treeGenerator: func(manifest Manifest, digestFunction integrity.Algorithm) (ManifestTree, error) {
		return casView(manifest, "content_addressable/{{ .Algorithm }}/{{ .DigestHex }}/file", false, digestFunction)
	}}
	diskCacheView = View{name: "bazel_disk_cache", treeGenerator: func(manifest Manifest, digestFunction integrity.Algorithm) (ManifestTree, error) {
		return casView(manifest, `cas/{{ printf "%.2s" .DigestHex }}/{{ .DigestHex }}`, true, digestFunction)
	}}
)

var knownViews = map[string]View{
	"default":          defaultView,
	"uri":              uriView,
	"repository_cache": repositoryCacheView,
	"bazel_disk_cache": diskCacheView,
}
