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
)

// leaf is a regular file in the filesystem.
// This corresponds to an asset in the manifest,
// which is identified by a path, uri(s), and digest.
type leaf struct {
	fs.Inode
	// The digest of the leaf
	digest       integrity.Digest
	manifestNode *manifest.Leaf
}

func (l *leaf) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// we are a leaf node - we can't have children
	return nil, syscall.ENOENT
}

func (l *leaf) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	// TODO: decide if / when reading attributes should issue a fetch on the remote asset API
	root := l.Root().Operations().(*root)
	if l.digest.Uninitialized() {
		// for now, we assume that "Lookup" on the parent must have initialized the digest
		// and we don't try to fetch here.
		// TODO: think about a better way to handle this.
		panic("digest uninitialized in Getattr - this should never happen but is a temporary restriction")
	}
	out.Mode = modeRegularReadonly
	// TODO: should ctime be the same as mtime?
	out.SetTimes(nil, &root.mtime, &root.mtime)
	out.Size = uint64(l.digest.SizeBytes)
	out.Blocks = (out.Size + 511) / 512

	return 0
}

func (l *leaf) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	root := l.Root().Operations().(*root)

	var digest integrity.Digest
	var algorithm integrity.Algorithm
	if attr == root.digestHashAttributeName {
		digest = l.digest
		algorithm = root.digestAlgorithm
	} else if !strings.HasPrefix(attr, "user.") {
		// unsupported attribute name
		return 0, syscall.ENODATA
	} else {
		// fallback: check for to the "user." prefix for Buck2
		algorithmName := strings.TrimPrefix(attr, "user.")
		algorithm, ok := integrity.AlgorithmFromString(algorithmName)
		if !ok {
			// unsupported attribute name
			return 0, syscall.ENODATA
		}
		checksum, ok := l.manifestNode.Integrity.ChecksumForAlgorithm(algorithm)
		if !ok {
			// integrity doesn't have a checksum for the algorithm
			return 0, syscall.ENODATA
		}
		// TODO: this reaches into l.digest internals - is this ok?
		digest = integrity.NewDigest(checksum.Hash, l.digest.SizeBytes, checksum.Algorithm)
		algorithm = checksum.Algorithm
	}

	// Someone is trying to read the digest hash via xattr.
	// We can infer that they are coming from Bazel, Buck2, or a similar tool.
	// Performance hack: we will use this opportunity to prefetch the asset into the remote cache.
	// This way, remote execution can magically use this file as an action input (without us uploading it to the remote cache).
	// TODO: make this configurable and non-blocking.
	if err := root.prefetcher.Prefetch(ctx, l.toAsset()); err != nil {
		panic("prefetch failed - this should be handled gracefully, but is a temporary restriction")
	}

	var destSizeBytes uint32 = uint32(algorithm.SizeBytes())

	if len(dest) < int(destSizeBytes) {
		// buffer too small
		return destSizeBytes, syscall.ERANGE
	}

	if err := digest.CopyHashInto(dest, algorithm); err != nil {
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
	if len(root.digestHashAttributeName) > 0 && !slices.Contains(supportedAttributes, root.digestHashAttributeName) {
		supportedAttributes = append(supportedAttributes, root.digestHashAttributeName)
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

	switch {
	case flags&syscall.O_ACCMODE != syscall.O_RDONLY,
		flags&syscall.O_TRUNC != 0,
		flags&syscall.O_APPEND != 0,
		flags&syscall.O_CREAT != 0,
		flags&syscall.O_EXCL != 0:
		// only support read-only access
		return nil, 0, syscall.EACCES
	}

	// syscall.O_LARGEFILE is 0x0 on x86_64, but the kernel
	// supplies 0x8000 anyway, except on mips64el, where 0x8000 is
	// used for O_DIRECT.
	const explicitLargeFileFlag = 0x8000
	supportedFlags := uint32(syscall.O_RDONLY | syscall.O_LARGEFILE | explicitLargeFileFlag | syscall.O_NOATIME | syscall.O_NOFOLLOW)
	unsupportedFlags := flags &^ supportedFlags
	if unsupportedFlags != 0 {
		return nil, 0, syscall.EINVAL
	}

	if l.digest.Uninitialized() {
		// for now, we assume that "Lookup" on the parent must have initialized the digest
		// and we don't try to fetch here.
		// TODO: think about a better way to handle this.
		return nil, 0, syscall.EIO
	}

	asset := l.toAsset()

	// We are about the read the file, so we materialize the data.
	// TODO: this is blocking and fills the local cache with the
	// full contents of the leaf - optimize this.
	if err := root.prefetcher.Materialize(ctx, asset); err != nil {
		return nil, 0, syscall.EIO
	}

	// TODO: properly initialize the file handle if needed
	reader, err := root.prefetcher.RandomAccessStream(ctx, asset)
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			return nil, 0, errno
		}
		// unknown error
		return nil, 0, syscall.EIO
	}
	return &leafHandle{
		reader: reader,
		inode:  l,
	}, 0, 0
}

func (l *leaf) toAsset() api.Asset {
	// TODO: make Qualifiers configurable
	return api.Asset{
		URIs:      l.manifestNode.URIs,
		Integrity: l.manifestNode.Integrity,
		SizeHint:  l.manifestNode.SizeHint,
	}
}

type leafHandle struct {
	// TODO: think about concurrency / mutexes for every method in leafHandle

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
