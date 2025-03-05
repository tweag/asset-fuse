package manifest

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/tweag/asset-fuse/integrity"
)

// Manifest describes the JSON manifest file format.
type Manifest map[string]ManifestEntry

func (m Manifest) validate() error {
	if len(m) == 0 {
		return errors.New("empty manifest")
	}
	issues := []string{}
	for path, entry := range m {
		issuesForPath := []string{}
		if len(path) == 0 || path[0] == '/' {
			issuesForPath = append(issuesForPath, "path must a non-empty path to the artifact, relative to the mount point")
		}
		if len(entry.URIs) == 0 {
			issuesForPath = append(issuesForPath, "entry must have at least one URI")
		} else {
			for _, uri := range entry.URIs {
				if len(uri) == 0 {
					issuesForPath = append(issuesForPath, `"uri" must be a non-empty string`)
				} else if !strings.HasPrefix(uri, "http://") && !strings.HasPrefix(uri, "https://") {
					// allow other schemes in the future
					issuesForPath = append(issuesForPath, `"uri" must start with "http://" or "https://"`)
				}
			}
		}
		integrity, err := entry.getIntegrity()
		if err != nil {
			issuesForPath = append(issuesForPath, err.Error())
		} else if len(integrity) == 0 {
			issuesForPath = append(issuesForPath, `"integrity" may not be empty`)
		}
		if entry.Size != nil && *entry.Size < 0 {
			issuesForPath = append(issuesForPath, `"size" must be a non-negative integer`)
		}
		if len(issuesForPath) > 0 {
			issues = append(issues, path+": "+strings.Join(issuesForPath, ", "))
		}
	}
	if len(issues) > 0 {
		return errors.New("manifest validation failed: \n  " + strings.Join(issues, "\n  "))
	}
	return nil
}

// ManifestEntry describes a single entry in the (JSON) manifest.
type ManifestEntry struct {
	// URIs is a list of mirror urls pointing to the same artifact.
	URIs []string `json:"uris"`
	// Integrity is a string or a list of strings containing the expected SRI digests of the artifact.
	// See https://developer.mozilla.org/en-US/docs/Web/Security/Subresource_Integrity
	// for more information.
	// When a list is used, only only one digest per algorithm is allowed.
	// The digests must all be of the same data.
	// The digest algorithm used by the CAS must be provided (default is sha256).
	Integrity json.RawMessage `json:"integrity"`
	// Size is the (optional) size of the artifact in bytes.
	// If provided, the size can be returned to the client before the artifact is fetched.
	// Otherwise, the size can be determined after fetching the artifact.
	// You can also use an integrity hash h = hash("") to represent an empty file.
	Size *int `json:"size,omitempty"`
}

func (e *ManifestEntry) getIntegrity() ([]string, error) {
	var integrity []string
	var singleIntegrity string
	if err := json.Unmarshal(e.Integrity, &integrity); err == nil {
		// do nothing - the integrity is already parsed
	} else if err := json.Unmarshal(e.Integrity, &singleIntegrity); err == nil {
		integrity = []string{singleIntegrity}
	} else {
		return nil, errors.New(`"integrity" must be a string or a list of strings`)
	}
	return integrity, nil
}

type Leaf struct {
	Checksum  integrity.Checksum
	Integrity integrity.Integrity
	// SizeHint is the size of the artifact in bytes.
	// If provided, the size can be returned to the client before the artifact is fetched.
	// Otherwise, the size can be determined after fetching the artifact.
	// A negative value indicates that the size is unknown.
	SizeHint int
}

type Directory struct {
	// Children is a map from the name of the child to the child node.
	// The name must be a valid directory entry name (no "/" or "\0").
	// The child node can be a directory or a leaf.
	Children map[string]any
}

type ManifestTree struct {
	Root *Directory
}

func (t ManifestTree) Insert(leafPath string, leaf Leaf) error {
	if leafPath == "" || leafPath[0] == '/' {
		return errors.New("path must be a non-empty path to the artifact, relative to the mount point")
	}
	segments := strings.Split(leafPath, "/")
	// validate segments of the path
	// they need to be in canonical form:
	// - no empty segments
	// - no "." or ".." segments
	// - no leading or trailing slashes
	for _, segment := range segments {
		if segment == "" {
			return errors.New("path must not contain empty segments")
		}
		if segment == "." || segment == ".." {
			return errors.New("path must not contain '.' or '..' segments")
		}
		if segment[0] == '/' || segment[len(segment)-1] == '/' {
			return errors.New("path must not contain leading or trailing slashes")
		}
	}

	current := t.Root
	for _, segment := range segments[:len(segments)-1] {
		child, ok := current.Children[segment]
		if !ok {
			child = &Directory{}
			current.Children[segment] = child
		}
		dir, ok := child.(*Directory)
		if !ok {
			return errors.New("insertion path conflicts with existing leaf")
		}
		current = dir
	}

	leafName := segments[len(segments)-1]
	if _, ok := current.Children[leafName]; ok {
		// This should be unreachable because we read paths from the a map,
		// where each key is a unqique leaf path.
		// If we ever get here, the canonicalization of paths is broken.
		return errors.New("insertion path conflicts with existing leaf")
	}
	current.Children[leafName] = &leaf
	return nil
}

func NewTree() ManifestTree {
	return ManifestTree{Root: &Directory{Children: map[string]any{}}}
}

func TreeFromManifest(reader io.Reader) (ManifestTree, error) {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return ManifestTree{}, err
	}
	if err := manifest.validate(); err != nil {
		return ManifestTree{}, err
	}

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
			Integrity: leafIntegrity,
			SizeHint:  -1,
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
