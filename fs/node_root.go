package fs

import (
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/tweag/asset-fuse/integrity"
)

type root struct {
	// the root node is a directory
	dirent

	// additionally, the root node holds configuration
	// and handles on external services

	// The single digest algorithm used (as opposed to multiple checksums / integrity values that are allowed in the manifest)
	digestAlgorithm integrity.Algorithm

	// mtime (and ctime) of inodes
	// this is either set by the user
	// or derived from the mtime of the manifest file
	mtime time.Time

	// the name of the extended attribute of leaf nodes that holds the digest hash
	// This should be the same as "--unix_digest_hash_attribute_name" in Bazel.
	// In addition, we always support the "user." prefix for Buck2.
	digestHashAttributeName string
}

// ensure root type embeds fs.Inode
var _ = (fs.InodeEmbedder)((*root)(nil))

// root should inherit the ability to look up children
// from dirent
var _ = (fs.NodeLookuper)((*root)(nil))

// root should inherit the ability to read attributes
// from dirent
var (
	_ = (fs.NodeGetattrer)((*root)(nil))
	_ = (fs.NodeGetxattrer)((*root)(nil))
	_ = (fs.NodeListxattrer)((*root)(nil))
)
