package fs

import (
	"context"
	"io"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/tweag/asset-fuse/fs/manifest"
	"github.com/tweag/asset-fuse/integrity"
)

// leaf is a regular file in the filesystem.
// This corresponds to an asset in the manifest,
// which is identified by a path, uri(s), and digest.
type leaf struct {
	fs.Inode
	// The digest of the leaf
	// It is either fully initialized or not initialized at all
	// (i.e. the size is 0 and the hash is all zero bytes, or size is correct and hash is correct)
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
		return syscall.EIO
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
	fallbackName := "user." + root.digestAlgorithm.String()

	if len(root.digestHashAttributeName) > 0 && attr == root.digestHashAttributeName || attr == fallbackName {
		// we have a match
	} else {
		// unsupported attribute name
		return 0, syscall.ENODATA
	}

	var destSizeBytes uint32 = uint32(root.digestAlgorithm.SizeBytes())

	if len(dest) < int(destSizeBytes) {
		// buffer too small
		return destSizeBytes, syscall.ERANGE
	}

	if err := l.digest.CopyHashInto(dest, root.digestAlgorithm); err != nil {
		// should be unreachable (we checked the buffer size in advance)
		// and there is no other reason for the copy to fail
		return 0, syscall.EIO
	}

	return destSizeBytes, 0
}

func (l *leaf) Listxattr(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	// we support up to two extended attributes:
	// - the user-defined attribute name
	// - the fallback attribute name
	//
	// These could be identical, or the user-defined name could be empty.
	// We don't support any other attributes.
	root := l.Root().Operations().(*root)
	fallbackName := "user." + root.digestAlgorithm.String()

	var supportedAttributes []string
	switch {
	case len(root.digestHashAttributeName) == 0, root.digestHashAttributeName == fallbackName:
		// user-defined name is empty
		// or the user-defined name is the same as the fallback name
		supportedAttributes = []string{fallbackName}
	default:
		// both the user-defined and the fallback name are supported
		supportedAttributes = []string{root.digestHashAttributeName, fallbackName}
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

	switch {
	case flags&syscall.O_ACCMODE != syscall.O_RDONLY,
		flags&syscall.O_TRUNC != 0,
		flags&syscall.O_APPEND != 0,
		flags&syscall.O_CREAT != 0,
		flags&syscall.O_EXCL != 0:
		// only support read-only access
		return nil, 0, syscall.EACCES
	}

	supportedFlags := uint32(syscall.O_RDONLY)
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

	// TODO: properly initialize the file handle if needed
	return &leafHandle{
		reader: nil, // TODO: initialize the reader (this must be somehow connected to the remote asset API, remote CAS, and local cache)
		inode:  l,
	}, 0, 0
}

type leafHandle struct {
	// TODO: think about concurrency / mutexes for every method in leafHandle

	// TODO: io.ReaderAt assumes random access to the data.
	// We should think about how to handle sequential reads and prefetching.
	reader readAtCloser

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
