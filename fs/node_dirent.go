package fs

import (
	"context"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/tweag/asset-fuse/fs/manifest"
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
		out.Mode = modeDirReadonly
		stableAttr.Mode = syscall.S_IFDIR
		out.SetAttrTimeout(direntTTL)
		out.SetEntryTimeout(direntTTL)
	case *manifest.Leaf:
		// child is a readonly leaf
		ops = &leaf{manifestNode: child}
		out.Mode = modeRegularReadonly
		stableAttr.Mode = syscall.S_IFREG
	default:
		return nil, syscall.EIO
	}

	// TODO: should ctime be the same as mtime?
	out.SetTimes(nil, &root.mtime, &root.mtime)

	return n.NewInode(ctx, ops, stableAttr), 0
}

func (n *dirent) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	root := n.Root().Operations().(*root)
	out.Mode = modeDirReadonly
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

// directory entries are completely virtual and
// depend only on information in the manifest
// so we can set a long TTL
const direntTTL = 24 * time.Hour

// ensure dirent type embeds fs.Inode
var _ = (fs.InodeEmbedder)((*dirent)(nil))

// dirent needs to implement Lookup, a way to find a child node by name
var _ = (fs.NodeLookuper)((*dirent)(nil))

// dirent needs to implement ways of reading file attributes
var (
	_ = (fs.NodeGetattrer)((*dirent)(nil))
	_ = (fs.NodeGetxattrer)((*dirent)(nil))
	_ = (fs.NodeListxattrer)((*dirent)(nil))
)
