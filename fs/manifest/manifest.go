package manifest

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"
	"syscall"

	"github.com/tweag/asset-fuse/integrity"
	"github.com/tweag/asset-fuse/internal/logging"
)

// Manifest describes the JSON manifest file format.
type Manifest struct {
	Paths        ManifestPaths `json:"paths"`
	URITemplates []string      `json:"uri_templates,omitempty"`
}

func (m *Manifest) Process() ManifestPaths {
	paths := make(ManifestPaths, len(m.Paths))
	for path, entry := range m.Paths {
		if len(entry.URIs) == 0 {
			for _, uri := range m.URITemplates {
				entry.URIs = append(entry.URIs, applyTemplateToEntry(uri, path, entry))
			}
		}
		paths[path] = entry
	}
	return paths
}

func applyTemplateToEntry(template, entrypath string, entry ManifestEntry) string {
	replacements := []string{
		"{path}", entrypath,
		"{path_urlencoded}", url.PathEscape(entrypath),
		"{dirname}", path.Dir(entrypath),
		"{basename}", path.Base(entrypath),
		"{stem}", strings.TrimSuffix(path.Base(entrypath), path.Ext(entrypath)),
		"{ext}", path.Ext(entrypath),
	}
	if entry.Size != nil {
		replacements = append(replacements, "{size}", fmt.Sprintf("%d", *entry.Size))
	}
	integrityStrings, getIntegrityErr := entry.GetIntegrity()
	checksums, integrityFromStringErr := integrity.IntegrityFromString(integrityStrings...)
	if getIntegrityErr == nil && integrityFromStringErr == nil {
		for checksum := range checksums.Items() {
			replacements = append(replacements, fmt.Sprintf("{%s}", checksum.Algorithm.String()), hex.EncodeToString(checksum.Hash))
		}
	}
	return strings.NewReplacer(replacements...).Replace(template)
}

type ManifestPaths map[string]ManifestEntry

func (m ManifestPaths) validate() error {
	if len(m) == 0 {
		return errors.New("empty manifest")
	}
	issues := []string{}
	warnings := []string{}
	for path, entry := range m {
		issuesForPath := []string{}
		warningsForPath := []string{}
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
		integrity, err := entry.GetIntegrity()
		if err != nil {
			issuesForPath = append(issuesForPath, err.Error())
		} else if len(integrity) == 0 {
			issuesForPath = append(issuesForPath, `"integrity" may not be empty`)
		}
		if entry.Size != nil && *entry.Size < 0 {
			issuesForPath = append(issuesForPath, `"size" must be a non-negative integer`)
		}
		if entry.Size == nil {
			warningsForPath = append(warningsForPath, `"size" was not provided - this may cause performance issues`)
		}
		if len(issuesForPath) > 0 {
			issues = append(issues, path+": "+strings.Join(issuesForPath, ", "))
		}
		if len(warningsForPath) > 0 {
			warnings = append(warnings, path+": "+strings.Join(warningsForPath, ", "))
		}
	}
	if len(warnings) > 0 {
		logging.Warningf("manifest validation warnings:\n  %s", strings.Join(warnings, "\n  "))
	}
	if len(issues) > 0 {
		return ValidationError{issues: issues}
	}
	return nil
}

// ManifestEntry describes a single entry in the (JSON) manifest.
type ManifestEntry struct {
	// URIs is a list of mirror urls pointing to the same artifact.
	URIs []string `json:"uris,omitempty"`
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
	// Executable marks the file as executable.
	Executable bool `json:"executable,omitempty"`
}

func (e *ManifestEntry) GetIntegrity() ([]string, error) {
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

type ValidationError struct {
	issues []string
}

func (e ValidationError) Error() string {
	return "manifest validation failed:\n  " + strings.Join(e.issues, "\n  ")
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
	SizeHint   int64
	Executable bool
}

func LeafFromEntry(entry ManifestEntry) (Leaf, error) {
	integrityStrings, err := entry.GetIntegrity()
	if err != nil {
		return Leaf{}, err
	}
	leafIntegrity, err := integrity.IntegrityFromString(integrityStrings...)
	if err != nil {
		return Leaf{}, err
	}
	sizeHint := int64(-1)
	if entry.Size != nil {
		sizeHint = *entry.Size
	}
	return Leaf{
		URIs:       entry.URIs,
		Integrity:  leafIntegrity,
		SizeHint:   sizeHint,
		Executable: entry.Executable,
	}, nil
}

func (l *Leaf) Mode() uint32 {
	var mode uint32 = modeRegularReadonly
	if l.Executable {
		mode |= 0o111
	}
	return mode
}

type Directory struct {
	// Children is a map from the name of the child to the child node.
	// The name must be a valid directory entry name (no "/" or "\0").
	// The child node can be a directory or a leaf.
	Children map[string]any
}

func (d *Directory) Mode() uint32 {
	return syscall.S_IFDIR | 0o555
}

type ManifestTree struct {
	Root  *Directory
	Leafs map[string]*Leaf
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
	t.Leafs[leafPath] = &leaf
	return nil
}

func ParseManifest(reader io.Reader) (Manifest, error) {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func NewTree() ManifestTree {
	return ManifestTree{
		Root:  &Directory{Children: map[string]any{}},
		Leafs: map[string]*Leaf{},
	}
}

func TreeFromManifest(reader io.Reader, view View, digestFunction integrity.Algorithm) (ManifestTree, error) {
	manifest, err := ParseManifest(reader)
	if err != nil {
		return ManifestTree{}, ManifestDecodeError{Inner: err}
	}
	paths := manifest.Process()
	if err := paths.validate(); err != nil {
		return ManifestTree{}, err
	}

	// TODO: allow for multiple views of the same manifest
	// - default: render leafs using their path
	// - uri: render leafs using their URIs
	// - cas: render leafs using their (hex) digest with modes: [repository_cache, remote_cache, nix_store, (docker / oci image blobs)]
	return view.Tree(paths, digestFunction)
}

type ManifestDecodeError struct {
	Inner error
}

func (e ManifestDecodeError) Error() string {
	return e.Inner.Error()
}

var (
	insertionChildOfLeafError         = errors.New("insertion path conflicts with existing leaf")
	insertionPathConflictError        = errors.New("insertion path conflicts with existing entry")
	insertingPathConflictAndKindError = errors.New("insertion path conflicts with existing entry (which is a directory)")
)

// modeRegularReadonly is the mode for regular files that are read-only.
// This sets the r bit for all users.
const modeRegularReadonly = syscall.S_IFREG | 0o444

// modeDirReadonly is the mode for directories that are read-only
// This sets the r and x bits for all users,
// which is needed to "cd" into the directory and list its contents.
const modeDirReadonly = syscall.S_IFDIR | 0o555
