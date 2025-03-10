package fs

import (
	"context"
	"path"
	"slices"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/tweag/asset-fuse/fs/manifest"
	"github.com/tweag/asset-fuse/internal/logging"
)

type dirent struct {
	fs.Inode
	manifestNode *manifest.Directory
}

func (n *dirent) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// TODO: decide how to handle TTLs for attributes of leaf nodes
	// - out.SetAttrTimeout()
	// - out.SetEntryTimeout()
	//
	// in theory, the information in the manifest is static
	// so we could set a very long TTL.
	// However, we want to know periodically if a leaf is still being used,
	// so we can ensure it is still available in the CAS.

	root := n.Root().Operations().(*root)

	child, ok := n.manifestNode.Children[name]
	if !ok {
		// child not found
		return nil, syscall.ENOENT
	}

	var ops fs.InodeEmbedder
	var stableAttr fs.StableAttr
	switch child := child.(type) {
	case *manifest.Directory:
		// child is a readonly directory
		ops = &dirent{manifestNode: child}
		out.Mode = child.Mode()
		stableAttr.Mode = syscall.S_IFDIR
		out.SetAttrTimeout(direntTTL)
		out.SetEntryTimeout(direntTTL)
	case *manifest.Leaf:
		// child is a readonly leaf
		ops = &leaf{
			manifestNode: child,
		}
		size, ok := leafSize(ctx, child, root.digestAlgorithm, root)
		if !ok {
			logging.Warningf("%s: reporting unknown size - consider adding the size to the manifest if it is known", path.Join(n.Path(n.Root()), name))
			size = 0
		}
		out.Size = uint64(size)
		out.Blocks = (out.Size + 511) / 512
		out.Mode = child.Mode()

		stableAttr.Mode = syscall.S_IFREG
	default:
		return nil, syscall.EIO
	}

	// TODO: should ctime be the same as mtime?
	out.SetTimes(nil, &root.mtime, &root.mtime)

	return n.NewInode(ctx, ops, stableAttr), 0
}

func (n *dirent) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	// preallocate the slice to contain all children, plus "." and ".."
	var entries []fuse.DirEntry = make([]fuse.DirEntry, 0, len(n.manifestNode.Children)+2)
	entries = append(entries,
		fuse.DirEntry{Name: ".", Mode: n.Mode()},
		fuse.DirEntry{Name: "..", Mode: n.Mode()},
	)

	// sort the names to ensure a deterministic order
	names := make([]string, 0, len(n.manifestNode.Children))
	for name := range n.manifestNode.Children {
		names = append(names, name)
	}
	slices.Sort(names)

	for _, name := range names {
		var mode uint32
		switch child := n.manifestNode.Children[name].(type) {
		case *manifest.Directory:
			mode = child.Mode()
		case *manifest.Leaf:
			mode = child.Mode()
		default:
			return nil, syscall.EIO
		}

		entries = append(entries, fuse.DirEntry{
			Name: name,
			Mode: mode,
		})
	}
	return fs.NewListDirStream(entries), 0
}

func (n *dirent) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	root := n.Root().Operations().(*root)
	out.Mode = n.manifestNode.Mode()
	out.SetTimes(nil, &root.mtime, &root.mtime)
	out.SetTimeout(direntTTL)
	return 0
}

func (n *dirent) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	// dirent nodes do not have extended attributes
	return 0, syscall.ENODATA
}

func (n *dirent) Listxattr(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	// dirent nodes do not have extended attributes
	return 0, 0
}

// This function is used during manifest reloads.
func (n *dirent) UpdateManifest(manifestNode *manifest.Directory) {
	n.manifestNode = manifestNode
}

// directory entries are completely virtual and
// depend only on information in the manifest
// so we can set a long TTL
const direntTTL = 24 * time.Hour

// ensure dirent type embeds fs.Inode
var _ = (fs.InodeEmbedder)((*dirent)(nil))

// dirent needs to implement Lookup, a way to find a child node by name
var _ = (fs.NodeLookuper)((*dirent)(nil))

// dirent needs to list its children
var _ = (fs.NodeReaddirer)((*dirent)(nil))

// dirent needs to implement ways of reading file attributes
var (
	_ = (fs.NodeGetattrer)((*dirent)(nil))
	_ = (fs.NodeGetxattrer)((*dirent)(nil))
	_ = (fs.NodeListxattrer)((*dirent)(nil))
)
