package cas

import (
	"context"
	"io"

	"github.com/tweag/asset-fuse/integrity"
	"github.com/tweag/asset-fuse/service/status"
)

// CAS is the interface for a content-addressable storage system.
// It is modeled after the remote execution API's ContentAddressableStorage service.
// However, it does not assume that the storage system is remote or that it is accessed via gRPC.
type CAS interface {
	Checker
	Reader
	Writer
}

type LocalCAS interface {
	CAS
	RandomAccessStream
}

type Checker interface {
	FindMissingBlobs(ctx context.Context, blobDigests []integrity.Digest, digestFunction integrity.Algorithm) ([]integrity.Digest, error)
}

type Reader interface {
	BatchReadBlobs(ctx context.Context, blobDigests []integrity.Digest, digestFunction integrity.Algorithm) (BatchReadBlobsResponse, error)
	ReadStream(ctx context.Context, blobDigest integrity.Digest, digestFunction integrity.Algorithm, offset, limit int64) (io.ReadCloser, error)
	// GetTree is not supported for now
}

type Writer interface {
	BatchUpdateBlobs(ctx context.Context, blobData DigestsAndData, digestFunction integrity.Algorithm) (BatchUpdateBlobsResponse, error)
	WriteStream(ctx context.Context, blobDigest integrity.Digest, digestFunction integrity.Algorithm, offset int64) (io.WriteCloser, error)
}

// RandomAccessStream is an interface for reading blobs at arbitrary offsets (random accesss via ReadAt).
// For now, this is only implemented by the disk CAS.
// TODO: think about a good abstraction to make this more generic, while being efficient.
type RandomAccessStream interface {
	ReadRandomAccessStream(ctx context.Context, blobDigest integrity.Digest, digestFunction integrity.Algorithm, offset, limit int64) (ReaderAtCloser, error)
}

type ReaderAtCloser interface {
	io.ReaderAt
	io.Closer
}

type BatchReadBlobsResponse []ReadBlobsResponse

type ReadBlobsResponse struct {
	Digest integrity.Digest
	Data   []byte
	// TODO: handle compression (for now we just assume that the data is not compressed)
}

type BatchUpdateBlobsRequest []UpdateBlobsRequest

type UpdateBlobsRequest struct {
	Digest integrity.Digest
	Data   []byte
	// TODO: handle compression (for now we just assume that the data is not compressed)
}

type BatchUpdateBlobsResponse []UpdateBlobsResponse

type UpdateBlobsResponse struct {
	Digest integrity.Digest
	Status status.Status
}

type DigestAndData struct {
	Digest integrity.Digest
	Data   []byte
}

type DigestsAndData []DigestAndData
