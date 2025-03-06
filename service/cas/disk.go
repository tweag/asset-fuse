package cas

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/tweag/asset-fuse/integrity"
)

// Disk is a local content-addressable storage that stores blobs on disk.
type Disk struct {
	rootDir string
}

// NewDisk creates a new Disk CAS with the given root directory.
func NewDisk(rootDir string) *Disk {
	return &Disk{rootDir: rootDir}
}

// TODO: we probably want a complete or partial in-memory representation of the CAS for better performance.
// For now, we always ask the filesystem for the data.
func (d *Disk) FindMissingBlobs(ctx context.Context, blobDigests []integrity.Digest, digestFunction integrity.Algorithm) ([]integrity.Digest, error) {
	missing := make([]integrity.Digest, 0, len(blobDigests))
	for _, digest := range blobDigests {
		fileInfo, err := os.Stat(d.blobPath(digest, digestFunction))
		if err != nil {
			if !os.IsNotExist(err) {
				// TODO: try to understand what other errors could happen
				// here and how to handle them.
				return nil, err
			}
			missing = append(missing, digest)
		}
		if fileInfo != nil && fileInfo.IsDir() {
			// our cache is corrupted
			return nil, fmt.Errorf("blob path %s is a directory", d.blobPath(digest, digestFunction))
		}
	}
	return missing, nil
}

func (d *Disk) BatchReadBlobs(ctx context.Context, blobDigests []integrity.Digest, digestFunction integrity.Algorithm) (BatchReadBlobsResponse, error) {
	panic("not implemented")
}

func (d *Disk) BatchUpdateBlobs(ctx context.Context, blobData DigestsAndData, digestFunction integrity.Algorithm) (BatchUpdateBlobsResponse, error) {
	panic("not implemented")
}

func (d *Disk) ReadStream(ctx context.Context, blobDigest integrity.Digest, digestFunction integrity.Algorithm, offset, limit int64) (io.ReadCloser, error) {
	panic("not implemented")
}

func (d *Disk) ReadRandomAccessStream(ctx context.Context, blobDigest integrity.Digest, digestFunction integrity.Algorithm, offset, limit int64) (ReaderAtCloser, error) {
	normalReader, err := d.ReadStream(ctx, blobDigest, digestFunction, offset, limit)
	if err != nil {
		return nil, err
	}
	// We know that the normal reader is a os.File, so we can safely allow random access.
	randomAccessReader, ok := normalReader.(ReaderAtCloser)
	if !ok {
		normalReader.Close()
		return nil, fmt.Errorf("stream does not support random access")
	}
	return randomAccessReader, nil
}

func (d *Disk) WriteStream(ctx context.Context, blobDigest integrity.Digest, digestFunction integrity.Algorithm, offset int64) (io.WriteCloser, error) {
	panic("not implemented")
}

// blobPath returns the path to the blob with the given digest.
// The directory structure used here is very similar to the one used by Bazel's local cache.
// The only difference is that we allow for different digest functions, by using a subdirectory for each digest function.
// You can still use the this directory structure with Bazel's local cache, by using a subdir:
//
//	bazel build --disk_cache=/path/to/cache/root/sha256
func (d *Disk) blobPath(digest integrity.Digest, digestFunction integrity.Algorithm) string {
	hex := digest.Hex(digestFunction)
	return filepath.Join(d.rootDir, digestFunction.String(), "cas", hex[:2], hex)
}

var (
	_ CAS                = (*Disk)(nil)
	_ RandomAccessStream = (*Disk)(nil)
)
