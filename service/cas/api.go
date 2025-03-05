package cas

import (
	"context"
	"io"

	"github.com/tweag/asset-fuse/integrity"
	"github.com/tweag/asset-fuse/service/api"
)

// CAS is the interface for a content-addressable storage system.
// It is modeled after the remote execution API's ContentAddressableStorage service.
// However, it does not assume that the storage system is remote or that it is accessed via gRPC.
type CAS interface {
	Checker
	Reader
	Writer
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
	Status api.Status
}

type DigestAndData struct {
	Digest integrity.Digest
	Data   []byte
}

type DigestsAndData []DigestAndData
