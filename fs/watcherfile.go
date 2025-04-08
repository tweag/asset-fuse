package fs

import (
	"context"
	"fmt"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/tweag/asset-fuse/internal/logging"
)

// watcherfile is a special leaf that is not visible
// as a dirent, but can be stated and opened.
// It is guaranteed to be present in any asset-fuse moumt.
// It is intended to be watched by tools like Bazel
// to be notified whenever the filesystem is mounted / unmounted.
type watcherfile struct {
	fs.Inode
}

func (w *watcherfile) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// we are a leaf node - we can't have children
	return nil, syscall.ENOENT
}

func (w *watcherfile) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	root := w.Root().Operations().(*root)
	out.Mode = syscall.S_IFREG | 0o444
	out.SetTimes(nil, &root.mtime, &root.mtime)
	content := fmt.Sprintf("%d", root.mtime.Unix())
	out.Size = uint64(len(content))
	out.Blocks = (out.Size + 511) / 512

	return 0
}

func (w *watcherfile) Open(ctx context.Context, openFlags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	return nil, 0, 0
}

func (w *watcherfile) Read(ctx context.Context, f fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	root := w.Root().Operations().(*root)
	logging.Debugf("watcherfile read(%s) at %d", w.Path(w.Root()), off)
	content := fmt.Sprintf("%d", root.mtime.Unix())
	if off >= int64(len(content)) {
		return nil, 0
	}
	n := copy(dest, content[off:])
	if n == 0 {
		return nil, syscall.EINVAL
	}
	return fuse.ReadResultData(dest[:n]), 0
}
