package cas

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/tweag/asset-fuse/integrity"
	"github.com/tweag/asset-fuse/service/status"
)

// Disk is a local content-addressable storage that stores blobs on disk.
type Disk struct {
	rootDir string
}

// NewDisk creates a new Disk CAS with the given root directory.
func NewDisk(rootDir string) (*Disk, error) {
	disk := &Disk{rootDir: rootDir}
	if err := disk.initializeCacheDir(); err != nil {
		return nil, err
	}
	return disk, nil
}

// TODO: we probably want a complete or partial in-memory representation of the CAS for better performance.
// For now, we always ask the filesystem for the data.
func (d *Disk) FindMissingBlobs(ctx context.Context, blobDigests []integrity.Digest, digestFunction integrity.Algorithm) ([]integrity.Digest, error) {
	missing := make([]integrity.Digest, 0, len(blobDigests))
	for _, digest := range blobDigests {
		fileInfo, err := os.Stat(d.blobPath(integrity.ChecksumFromDigest(digest, digestFunction)))
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
			return nil, fmt.Errorf("blob path %s is a directory", d.blobPath(integrity.ChecksumFromDigest(digest, digestFunction)))
		}
	}
	return missing, nil
}

func (d *Disk) BatchReadBlobs(ctx context.Context, blobDigests []integrity.Digest, digestFunction integrity.Algorithm) (BatchReadBlobsResponse, error) {
	responses := make(BatchReadBlobsResponse, 0, len(blobDigests))
	for _, digest := range blobDigests {
		data, err := os.ReadFile(d.blobPath(integrity.ChecksumFromDigest(digest, digestFunction)))
		if err != nil && os.IsNotExist(err) {
			responses = append(responses, ReadBlobsResponse{
				Digest: digest,
				Status: status.Status{Code: status.Status_NOT_FOUND},
			})
		} else if err != nil {
			responses = append(responses, ReadBlobsResponse{
				Digest: digest,
				Status: status.Status{Code: status.Status_UNKNOWN},
			})
		}
		responses = append(responses, ReadBlobsResponse{
			Digest: digest,
			Data:   data,
			Status: status.Status{Code: status.Status_OK},
		})
	}
	var issues int
	for _, response := range responses {
		if response.Data == nil || response.Status.Code != status.Status_OK {
			issues++
		}
	}
	if issues > 0 {
		return responses, BatchResponseHasNonZeroStatus
	}
	return responses, nil
}

func (d *Disk) BatchUpdateBlobs(ctx context.Context, blobData DigestsAndData, digestFunction integrity.Algorithm) (BatchUpdateBlobsResponse, error) {
	responses := make(BatchUpdateBlobsResponse, 0, len(blobData))
	for _, item := range blobData {
		staging, err := d.stagingFile(item.Digest, digestFunction)
		if err != nil {
			responses = append(responses, UpdateBlobsResponse{item.Digest, status.Status{Code: status.Status_INTERNAL, Message: err.Error()}})
			continue
		}

		_, writeErr := staging.Write(item.Data)
		if writeErr != nil && os.IsPermission(writeErr) {
			staging.Close()
			responses = append(responses, UpdateBlobsResponse{item.Digest, status.Status{Code: status.Status_PERMISSION_DENIED, Message: writeErr.Error()}})
			continue
		} else if writeErr != nil {
			staging.Close()
			responses = append(responses, UpdateBlobsResponse{item.Digest, status.Status{Code: status.Status_INTERNAL, Message: writeErr.Error()}})
			continue
		}

		finalizerErr := staging.Close()
		if finalizerErr != nil && os.IsPermission(finalizerErr) {
			responses = append(responses, UpdateBlobsResponse{item.Digest, status.Status{Code: status.Status_PERMISSION_DENIED, Message: finalizerErr.Error()}})
			continue
		} else if finalizerErr != nil {
			responses = append(responses, UpdateBlobsResponse{item.Digest, status.Status{Code: status.Status_INTERNAL, Message: finalizerErr.Error()}})
			continue
		}

		responses = append(responses, UpdateBlobsResponse{item.Digest, status.Status{Code: status.Status_OK}})
	}
	var issues int
	for _, response := range responses {
		if response.Status.Code != status.Status_OK {
			issues++
		}
	}
	if issues > 0 {
		return responses, BatchResponseHasNonZeroStatus
	}
	return responses, nil
}

func (d *Disk) ReadStream(ctx context.Context, blobDigest integrity.Digest, digestFunction integrity.Algorithm, offset, limit int64) (io.ReadCloser, error) {
	file, err := os.Open(d.blobPath(integrity.ChecksumFromDigest(blobDigest, digestFunction)))
	if err != nil {
		return nil, err
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		file.Close()
		return nil, err
	}
	var limitReader io.Reader
	var sectionReader io.ReaderAt
	if limit == 0 {
		// Zero means no limit.
		limitReader = file
		sectionReader = file
	} else {
		limitReader = io.LimitReader(file, limit)
		sectionReader = io.NewSectionReader(file, offset, limit)
	}
	// We return a struct that implements both io.Reader, io.ReaderAt, and io.Closer.
	// This is a bit of a hack, but it allows us to return a reader that is limited to the given range.
	return struct {
		io.Reader
		io.ReaderAt
		io.Closer
	}{limitReader, sectionReader, file}, nil
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

func (d *Disk) WriteStream(ctx context.Context, blobDigest integrity.Digest, digestFunction integrity.Algorithm) (io.WriteCloser, error) {
	file, err := d.stagingFile(blobDigest, digestFunction)
	if err != nil {
		return nil, err
	}
	return file, nil
}

// ImportBlob imports a blob from the given reader.
// It tries to optimize the import by skipping checksum validation if the integrity was already validated.
// The caller must ensure that prevalidatedIntegrity was actually validated.
// Additionally, optionalDigest can be provided to skip the checksum calculation if the digest (for digestFunction) is already known.
// The default value for integrity.Digest is an empty struct, which means that the real digest will be calculated.
func (d *Disk) ImportBlob(ctx context.Context, prevalidatedIntegrity integrity.Integrity, optionalDigest integrity.Digest, digestFunction integrity.Algorithm, data io.Reader) (integrity.Digest, error) {
	var knownChecksum integrity.Checksum
	if !optionalDigest.Uninitialized() {
		knownChecksum = integrity.ChecksumFromDigest(optionalDigest, digestFunction)
	} else if prevalidatedChecksum, ok := prevalidatedIntegrity.ChecksumForAlgorithm(digestFunction); ok {
		knownChecksum = prevalidatedChecksum
	} else {
		// TODO: slow checksum validation path
		panic("ImportBlob: slow checksum validation path not implemented")
	}
	if knownChecksum.Empty() {
		// we should never get here, but better safe than sorry
		return integrity.Digest{}, errors.New("ImportBlob called without a known checksum")
	}

	targetLocation := d.blobPath(knownChecksum)
	sizeBytes, err := hardlinkOrCopy(data, targetLocation)
	if err != nil {
		return integrity.Digest{}, err
	}

	return integrity.NewDigest(knownChecksum.Hash, sizeBytes, digestFunction), nil
}

// blobPath returns the path to the blob with the given digest.
// The directory structure used here is very similar to the one used by Bazel's local cache.
// The only difference is that we allow for different digest functions, by using a subdirectory for each digest function.
// You can still use the this directory structure with Bazel's local cache, by using a subdir:
//
//	bazel build --disk_cache=/path/to/cache/root/sha256
func (d *Disk) blobPath(checksum integrity.Checksum) string {
	hex := checksum.Hex()
	return filepath.Join(d.rootDir, checksum.Algorithm.String(), "cas", hex[:2], hex)
}

func (d *Disk) stagingFile(digest integrity.Digest, digestFunction integrity.Algorithm) (WriterAtCloser, error) {
	hex := digest.Hex(digestFunction)
	dir := filepath.Join(d.rootDir, digestFunction.String(), "staging")
	tmpfile, err := os.CreateTemp(dir, hex+"-")
	if err != nil {
		return nil, err
	}
	// try to preallocate the file to the expected size
	_ = tmpfile.Truncate(digest.SizeBytes)
	return &blobFinalizer{
		File:        tmpfile,
		stagingPath: tmpfile.Name(),
		finalPath:   d.blobPath(integrity.ChecksumFromDigest(digest, digestFunction)),

		digest:         digest,
		digestFunction: digestFunction,
	}, nil
}

func (d *Disk) initializeCacheDir() error {
	// initialize the cache directory
	// <rootDir>/cas/<digestFunction>/<first 2 hex>/
	// <rootDir>/staging/<digestFunction>/
	if err := os.MkdirAll(d.rootDir, 0o755); err != nil {
		return err
	}
	for digestFunction := range integrity.SupportedAlgorithms() {
		digestPrefix := filepath.Join(d.rootDir, digestFunction.String())
		if err := os.Mkdir(digestPrefix, 0o755); err != nil && !os.IsExist(err) {
			return err
		}
		if err := os.Mkdir(filepath.Join(digestPrefix, "cas"), 0o755); err != nil && !os.IsExist(err) {
			return err
		}
		for i := 0; i < 256; i++ {
			if err := os.Mkdir(filepath.Join(digestPrefix, "cas", fmt.Sprintf("%02x", i)), 0o755); err != nil && !os.IsExist(err) {
				return err
			}
		}
		if err := os.Mkdir(filepath.Join(digestPrefix, "staging"), 0o755); err != nil && !os.IsExist(err) {
			return err
		}
		// try to clean up the staging directory from any leftover files
		// (this assumes that the directory is only used by this process)
		files, err := os.ReadDir(filepath.Join(digestPrefix, "staging"))
		if err != nil {
			return err
		}
		for _, file := range files {
			if err := os.Remove(filepath.Join(digestPrefix, "staging", file.Name())); err != nil {
				return err
			}
		}
	}

	return nil
}

type blobFinalizer struct {
	*os.File
	stagingPath string
	finalPath   string

	digest         integrity.Digest
	digestFunction integrity.Algorithm
}

func (b *blobFinalizer) Close() error {
	b.File.Close()
	defer os.Remove(b.stagingPath)

	// verify that the file contents are correct
	validationFile, err := os.OpenFile(b.stagingPath, os.O_RDONLY, 0o444)
	if err != nil {
		return fmt.Errorf("failed to open staging file %s for validation: %w", b.stagingPath, err)
	}
	defer validationFile.Close()
	if err := b.digest.CheckContent(validationFile, b.digestFunction); err != nil {
		return fmt.Errorf("failed to validate staging file %s: %w", b.stagingPath, err)
	}

	// move the file to its final location
	if err := os.MkdirAll(filepath.Dir(b.finalPath), 0o755); err != nil {
		return fmt.Errorf("failed to create directory for final blob %s: %w", b.finalPath, err)
	}
	if err := os.Rename(b.stagingPath, b.finalPath); err != nil {
		return fmt.Errorf("failed to rename staging file %s to final blob %s: %w", b.stagingPath, b.finalPath, err)
	}

	return nil
}

func hardlinkOrCopy(source io.Reader, target string) (fileSize int64, err error) {
	defer func() {
		// learn size on function return and cleanup on error
		if err != nil {
			os.Remove(target)
			return
		}
		fileInfo, statErr := os.Stat(target)
		if statErr != nil {
			err = statErr
			return
		}
		fileSize = fileInfo.Size()
		return
	}()

	if sourceFile, ok := source.(*os.File); ok {
		// try to hardlink the file
		if err := os.Link(sourceFile.Name(), target); err == nil {
			return 0, nil
		}
	}
	// if we can't hardlink, we need to copy the file atomically
	tmpFile, err := os.CreateTemp(filepath.Dir(target), "tmp-")
	if err != nil {
		return 0, err
	}
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	_, err = io.Copy(tmpFile, source)
	if err != nil {
		return 0, err
	}
	if err := tmpFile.Close(); err != nil {
		return 0, err
	}
	if err := os.Rename(tmpFile.Name(), target); err != nil {
		return 0, err
	}
	return 0, nil
}

var _ LocalCAS = (*Disk)(nil)
