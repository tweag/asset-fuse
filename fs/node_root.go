package fs

import (
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/tweag/asset-fuse/fs/manifest"
	"github.com/tweag/asset-fuse/integrity"
	"github.com/tweag/asset-fuse/internal/logging"
	"github.com/tweag/asset-fuse/service/prefetcher"
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
	digestHashXattrName string

	// the encoding of the digest hash in the extended attribute
	// For Bazel, this is raw. For Buck2, this is hex.
	digestHashXattrEncoding xattrEncoding

	// whether to fail reads on leaf nodes
	failReads bool

	prefetcher *prefetcher.Prefetcher
}

func Root(
	manifestTree manifest.ManifestTree,
	digestAlgorithm integrity.Algorithm, mtime time.Time, digestHashAttributeName string, xattrEncoding xattrEncoding, failReads bool,
	prefetcher *prefetcher.Prefetcher,
) *root {
	return &root{
		dirent: dirent{
			manifestNode: manifestTree.Root,
		},
		digestAlgorithm:         digestAlgorithm,
		mtime:                   mtime,
		digestHashXattrName:     digestHashAttributeName,
		digestHashXattrEncoding: xattrEncoding,
		failReads:               failReads,
		prefetcher:              prefetcher,
	}
}

func (r *root) UpdateMtime(mtime time.Time) {
	r.mtime = mtime
}

type xattrEncoding int

const (
	XattrEncodingRaw xattrEncoding = iota
	XattrEncodingHex
)

func XattrEncodingFromString(s string) xattrEncoding {
	switch s {
	case "raw":
		return XattrEncodingRaw
	case "hex":
		return XattrEncodingHex
	default:
		logging.Errorf("Unknown xattr encoding %q, falling back to raw", s)
		return XattrEncodingRaw
	}
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
