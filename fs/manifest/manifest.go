package manifest

import (
	"encoding/json"
	"errors"
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
		// TODO: lift this restriction when we support learning the size
		if entry.Size == nil {
			issuesForPath = append(issuesForPath, `"size" must be provided (for now - this is a temporary restriction)`)
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
	Size *int64 `json:"size,omitempty"`
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

// Leaf: Same as ManifestEntry, but with Size being a value instead of a pointer.
// This is used in the tree representation.
type Leaf struct {
	URIs      []string
	Integrity integrity.Integrity
	// SizeHint is the size of the artifact in bytes.
	// If provided, the size can be returned to the client before the artifact is fetched.
	// Otherwise, the size can be determined after fetching the artifact.
	// A negative value indicates that the size is unknown.
	SizeHint int64
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
			child = &Directory{
				Children: map[string]any{},
			}
			current.Children[segment] = child
		}
		dir, ok := child.(*Directory)
		if !ok {
			return insertionChildOfLeafError
		}
		current = dir
	}

	leafName := segments[len(segments)-1]
	if _, ok := current.Children[leafName]; ok {
		if _, ok := current.Children[leafName].(*Leaf); ok {
			// This should be unreachable because we read paths from the a map,
			// where each key is a unqique leaf path (at least for the default view)
			// If we ever get here, the canonicalization of paths is broken (or we have a non-unique view).
			return insertionPathConflictError
		}
		return insertingPathConflictAndKindError
	}
	current.Children[leafName] = &leaf
	return nil
}

func NewTree() ManifestTree {
	return ManifestTree{Root: &Directory{Children: map[string]any{}}}
}

func TreeFromManifest(reader io.Reader, view View, digestFunction integrity.Algorithm) (ManifestTree, error) {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return ManifestTree{}, err
	}
	if err := manifest.validate(); err != nil {
		return ManifestTree{}, err
	}

	// TODO: allow for multiple views of the same manifest
	// - default: render leafs using their path
	// - uri: render leafs using their URIs
	// - cas: render leafs using their (hex) digest with modes: [repository_cache, remote_cache, nix_store, (docker / oci image blobs)]
	return view.Tree(manifest, digestFunction)
}

var (
	insertionChildOfLeafError         = errors.New("insertion path conflicts with existing leaf")
	insertionPathConflictError        = errors.New("insertion path conflicts with existing entry")
	insertingPathConflictAndKindError = errors.New("insertion path conflicts with existing entry (which is a directory)")
)
