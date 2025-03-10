package fs

import (
	"context"
	"io"
	"slices"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/tweag/asset-fuse/api"
	"github.com/tweag/asset-fuse/fs/manifest"
	"github.com/tweag/asset-fuse/integrity"
	"github.com/tweag/asset-fuse/internal/logging"
)

// leaf is a regular file in the filesystem.
// This corresponds to an asset in the manifest,
// which is identified by a path, uri(s), and digest.
type leaf struct {
	fs.Inode
	manifestNode *manifest.Leaf
}

func (l *leaf) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// we are a leaf node - we can't have children
	return nil, syscall.ENOENT
}

func (l *leaf) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	// TODO: decide if / when reading attributes should issue a fetch on the remote asset API
	root := l.Root().Operations().(*root)
	out.Mode = l.manifestNode.Mode()
	// TODO: should ctime be the same as mtime?
	out.SetTimes(nil, &root.mtime, &root.mtime)
	size, ok := l.size(ctx)
	if !ok {
		logging.Warningf("%s: reporting unknown size - consider adding the size to the manifest if it is known", l.Path(l.Root()))
		size = 0
	}
	out.Size = uint64(size)
	out.Blocks = (out.Size + 511) / 512

	return 0
}

func (l *leaf) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	root := l.Root().Operations().(*root)

	var algorithm integrity.Algorithm
	if len(root.digestHashXattrName) > 0 && attr == root.digestHashXattrName {
		algorithm = root.digestAlgorithm
	} else if !strings.HasPrefix(attr, "user.") {
		// unsupported attribute name
		return 0, syscall.ENODATA
	} else {
		// fallback: check for to the "user." prefix for Buck2
		algorithmName := strings.TrimPrefix(attr, "user.")
		var validAlgorithm bool
		algorithm, validAlgorithm = integrity.AlgorithmFromString(algorithmName)
		if !validAlgorithm {
			// unsupported attribute name
			return 0, syscall.ENODATA
		}
	}

	csum, err := l.checksum(ctx, algorithm)
	if err != nil {
		return 0, syscall.ENODATA
	}

	// Someone is trying to read the digest hash via xattr.
	// We can infer that they are coming from Bazel, Buck2, or a similar tool.
	// Performance hack: we will use this opportunity to prefetch the asset into the remote cache.
	// This way, remote execution can magically use this file as an action input (without us uploading it to the remote cache).
	// TODO: make this configurable and non-blocking.
	if _, err := root.prefetcher.Prefetch(ctx, l.toAsset()); err != nil {
		logging.Warningf("prefetch failed: %v", err)
	}

	var destSizeBytes uint32 = uint32(algorithm.SizeBytes())

	if len(dest) < int(destSizeBytes) {
		// buffer too small
		return destSizeBytes, syscall.ERANGE
	}

	if n := copy(dest, csum.Hash); n != len(csum.Hash) {
		// should be unreachable (we checked the buffer size in advance)
		// and there is no other reason for the copy to fail
		return 0, syscall.EIO
	}

	return destSizeBytes, 0
}

func (l *leaf) Listxattr(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	// we support up to two extended attributes:
	// - the user-defined attribute name
	// - fallback attribute names (for Buck2): "user." + algorithm
	//
	// These could be identical, or the user-defined name could be empty.
	// We don't support any other extended attributes.
	root := l.Root().Operations().(*root)

	var supportedAttributes []string
	for integrityForAlgorithm := range l.manifestNode.Integrity.Items() {
		supportedAttributes = append(supportedAttributes, "user."+integrityForAlgorithm.Algorithm.String())
	}
	if len(root.digestHashXattrName) > 0 && !slices.Contains(supportedAttributes, root.digestHashXattrName) {
		supportedAttributes = append(supportedAttributes, root.digestHashXattrName)
	}

	// calculate the total size of the attribute names
	var destSizeBytes uint32
	for _, attr := range supportedAttributes {
		destSizeBytes += uint32(len(attr) + 1)
	}

	if len(dest) < int(destSizeBytes) {
		// buffer too small
		return destSizeBytes, syscall.ERANGE
	}

	// copy the attribute names into the buffer
	// separated by null bytes
	current := dest
	for _, attr := range supportedAttributes {
		copy(current, attr)
		current = current[len(attr):]
		current[0] = 0
		current = current[1:]
	}

	return destSizeBytes, 0
}

func (l *leaf) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	// TODO: decide if / when opening a leaf should issue a fetch on the remote asset API
	// TODO: decide if opening a leaf should prefetch the contents into the local cache [materialize] (assuming that we expect the leaf to be read soon)
	// TODO: support non-blocking io (O_NONBLOCK, O_NDELAY)
	root := l.Root().Operations().(*root)

	var supportedFlags uint32 = FMODE_READ | FMODE_LSEEK | FMODE_PREAD | FMODE_EXEC | FMODE_NOCMTIME | FMODE_RANDOM | FMODE_ATOMIC_POS | FMODE_CAN_READ
	var allowedAccess uint32 = FMODE_READ
	if l.manifestNode.Executable {
		allowedAccess |= FMODE_EXEC
	}

	unsupportedFlags := flags &^ supportedFlags
	if unsupportedFlags != 0 {
		return nil, 0, syscall.EINVAL
	}

	if (flags&FMODE_ACCESS)&^allowedAccess != 0 {
		// only support read-only access
		return nil, 0, syscall.EACCES
	}

	asset := l.toAsset()

	// We are about the read the file, so we materialize the data.
	// TODO: this is blocking and fills the local cache with the
	// full contents of the leaf - optimize this.
	if err := root.prefetcher.Materialize(ctx, asset); err != nil {
		return nil, 0, syscall.EIO
	}

	// TODO: properly initialize the file handle if needed
	reader, err := root.prefetcher.RandomAccessStream(ctx, asset, 0, 0)
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return nil, 0, errno
		}
		// unknown error
		return nil, 0, syscall.EIO
	}
	return &leafHandle{
		failReads: root.failReads,
		reader:    reader,
		inode:     l,
	}, 0, 0
}

// This function is used during manifest reloads.
func (n *leaf) UpdateManifest(manifestNode *manifest.Leaf) {
	n.manifestNode = manifestNode
}

func (l *leaf) toAsset() api.Asset {
	// TODO: make Qualifiers configurable
	return leafToAsset(l.manifestNode)
}

// checksum returns the checksum for the leaf node.
// If the checksum is not available, it returns ENODATA (which is expected for xattr retrieval).
func (l *leaf) checksum(ctx context.Context, algorithm integrity.Algorithm) (integrity.Checksum, error) {
	root := l.Root().Operations().(*root)
	return leafChecksum(ctx, l.manifestNode, algorithm, root)
}

func (l *leaf) size(ctx context.Context) (int64, bool) {
	root := l.Root().Operations().(*root)
	return leafSize(ctx, l.manifestNode, root.digestAlgorithm, root)
}

func leafToAsset(leafNode *manifest.Leaf) api.Asset {
	// TODO: make Qualifiers configurable
	return api.Asset{
		URIs:      leafNode.URIs,
		Integrity: leafNode.Integrity,
	}
}

func leafDigest(ctx context.Context, manifestLeaf *manifest.Leaf, root *root) (integrity.Digest, error) {
	if digest, ok := root.checksumCache.FromIntegrity(manifestLeaf.Integrity); ok {
		return digest, nil
	}
	// TODO: make this behavior configurable:
	// - fail the operation
	// - fetch the digest
	// - use a default size

	digest, err := root.prefetcher.Prefetch(ctx, leafToAsset(manifestLeaf))
	if err != nil {
		return integrity.Digest{}, err
	}
	return digest, nil
}

// checksum returns the checksum for the leaf node.
// If the checksum is not available, it returns ENODATA (which is expected for xattr retrieval).
func leafChecksum(ctx context.Context, manifestLeaf *manifest.Leaf, algorithm integrity.Algorithm, root *root) (integrity.Checksum, error) {
	if checksum, ok := manifestLeaf.Integrity.ChecksumForAlgorithm(algorithm); ok {
		return checksum, nil
	}
	if algorithm == root.digestAlgorithm {
		digest, err := leafDigest(ctx, manifestLeaf, root)
		if err == nil {
			return integrity.ChecksumFromDigest(digest, algorithm), nil
		}
	}
	return integrity.Checksum{}, syscall.ENODATA
}

func leafSize(ctx context.Context, manifestLeaf *manifest.Leaf, algorithm integrity.Algorithm, root *root) (int64, bool) {
	if manifestLeaf.SizeHint >= 0 {
		return manifestLeaf.SizeHint, true
	}
	digest, err := leafDigest(ctx, manifestLeaf, root)
	if err != nil {
		return 0, false
	}
	return digest.SizeBytes, true
}

type leafHandle struct {
	// TODO: think about concurrency / mutexes for every method in leafHandle
	failReads bool

	// TODO: io.ReaderAt assumes random access to the data.
	// We should think about how to handle sequential reads and prefetching.
	reader readerAtCloser

	// the leaf inode that this handle belongs to
	inode *leaf
}

func (h *leafHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	// TODO: have a policy for Read:
	// - read disabled (this ensures that data will only be accessed via the CAS and never downloaded locally)
	// - read enabled (this ensures that data will materialized when read)
	// For now, we assume that reading is allowed and we materialize the data.

	if h.failReads && len(dest) > 0 {
		// This is useful to test if prefetching and xattr optimizations are working with Buck2 and Bazel:
		// When remote execution is used and the remote asset service is available,
		// Buck2 and Bazel should read digests via xattr and never try to get file contents locally.
		// Instead, they should always use the remote asset service to fetch the file contents directly
		// from the internet into the remote CAS.
		return nil, syscall.EBADF
	}

	// TODO: handle blocking and non-blocking reads (for now, we assume that reads are blocking)
	n, err := h.reader.ReadAt(dest, off)
	if err != nil && err != io.EOF {
		if errno, ok := err.(syscall.Errno); ok {
			return nil, errno
		}
		// unknown error
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(dest[:n]), 0
}

func (h *leafHandle) Release(ctx context.Context) syscall.Errno {
	err := h.reader.Close()
	if errno, ok := err.(syscall.Errno); ok {
		return errno
	}
	if err != nil {
		// unknown error
		return syscall.EIO
	}
	return 0
}

// ensure leaf type embeds fs.Inode
var _ = (fs.InodeEmbedder)((*leaf)(nil))

// leaf implements lookup (but only to return ENOENT)
var _ = (fs.NodeLookuper)((*leaf)(nil))

// leaf needs to implement ways of reading file attributes
var (
	_ = (fs.NodeGetattrer)((*leaf)(nil))
	_ = (fs.NodeGetxattrer)((*leaf)(nil))
	_ = (fs.NodeListxattrer)((*leaf)(nil))
)

// leaf needs to implement Open, a way to open the file for reading
var _ = (fs.NodeOpener)((*leaf)(nil))

// leaf handles need to implement Read, a way to read the contents of the file
var _ = (fs.FileReader)((*leafHandle)(nil))

// leaf handles need to implement Release, a way to release the file handle
var _ = (fs.FileReleaser)((*leafHandle)(nil))
