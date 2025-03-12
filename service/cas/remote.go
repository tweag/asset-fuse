package cas

import (
	"bytes"
	"context"
	"fmt"
	"io"

	remoteexecution_proto "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/tweag/asset-fuse/auth/credential"
	"github.com/tweag/asset-fuse/integrity"
	"github.com/tweag/asset-fuse/service/internal/protohelper"
	"github.com/tweag/asset-fuse/service/status"
	bytestream_proto "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
)

// Remote uses the remote execution API's ContentAddressableStorage service to store and retrieve blobs.
// See also: https://raw.githubusercontent.com/bazelbuild/remote-apis/refs/tags/v2.11.0-rc2/build/bazel/remote/execution/v2/remote_execution.proto
//
// TODO: this implementation is incomplete and doesn't correctly handle well-defined cases mentioned in the proto file. This needs to be addressed before v1.0.0.
type Remote struct {
	casClient        remoteexecution_proto.ContentAddressableStorageClient
	byteStreamClient bytestream_proto.ByteStreamClient
}

func NewRemote(target string, helper credential.Helper, opts ...grpc.DialOption) (*Remote, error) {
	conn, err := protohelper.Client(target, helper, opts...)
	if err != nil {
		return nil, err
	}

	return &Remote{
		casClient:        remoteexecution_proto.NewContentAddressableStorageClient(conn),
		byteStreamClient: bytestream_proto.NewByteStreamClient(conn),
	}, nil
}

func (r *Remote) FindMissingBlobs(ctx context.Context, blobDigests []integrity.Digest, digestFunction integrity.Algorithm) ([]integrity.Digest, error) {
	resp, err := r.casClient.FindMissingBlobs(ctx, protoFindMissingBlobsRequest(blobDigests, digestFunction))
	if err != nil {
		return nil, err
	}
	return fromProtoFindMissingBlobsResponse(resp, digestFunction)
}

func (r *Remote) BatchReadBlobs(ctx context.Context, blobDigests []integrity.Digest, digestFunction integrity.Algorithm) (BatchReadBlobsResponse, error) {
	resp, err := r.casClient.BatchReadBlobs(ctx, protoBatchReadBlobsRequest(blobDigests, digestFunction))
	if err != nil {
		return nil, err
	}
	return fromProtoBatchReadBlobsResponse(resp, digestFunction)
}

func (r *Remote) BatchUpdateBlobs(ctx context.Context, blobData DigestsAndData, digestFunction integrity.Algorithm) (BatchUpdateBlobsResponse, error) {
	// TODO: for now, we only fill the remote CAS by using the remote asset API.
	// implement BatchUpdateBlobs if we ever need to write to the remote CAS directly.
	return nil, fmt.Errorf("asset-fuse does not yet implement BatchUpdateBlobs - please implement me if you need me :)")
}

func (r *Remote) ReadStream(ctx context.Context, blobDigest integrity.Digest, digestFunction integrity.Algorithm, offset, limit int64) (io.ReadCloser, error) {
	ctx, cancel := context.WithCancel(ctx)

	stream, err := r.byteStreamClient.Read(ctx, protoReadRequest(blobDigest, digestFunction, offset, limit))
	if err != nil {
		cancel()
		return nil, err
	}
	return &byteStreamReadCloser{
		stream: stream,
		cancel: cancel,
		limit:  limit,
	}, nil
}

func (r *Remote) WriteStream(ctx context.Context, blobDigest integrity.Digest, digestFunction integrity.Algorithm) (io.WriteCloser, error) {
	// TODO: for now, we only fill the remote CAS by using the remote asset API.
	// implement WriteStream if we ever need to write to the remote CAS directly.
	return nil, fmt.Errorf("asset-fuse does not yet implement WriteStream - please implement me if you need me :)")
}

type byteStreamReadCloser struct {
	stream bytestream_proto.ByteStream_ReadClient
	buf    bytes.Buffer
	eof    bool
	cancel context.CancelFunc

	limit          int64
	readFromRemote int64
	writtenToOut   int64
}

func (b *byteStreamReadCloser) Read(p []byte) (n int, err error) {
	// first, check if we have data from the previous read
	budget := len(p)
	availableFromLastRead := b.buf.Len()
	copyFromLastRead := min(budget, availableFromLastRead)
	if copyFromLastRead > 0 {
		n := copy(p, b.buf.Next(copyFromLastRead))
		if n > budget {
			// should never happen
			panic(fmt.Sprintf("copy(%d, %d) > %d (budget exceeded)", n, copyFromLastRead, budget))
		}
		if n != copyFromLastRead {
			// should never happen
			panic(fmt.Sprintf("copy(%d, %d) != %d (logic flaw)", n, copyFromLastRead, n))
		}
		b.writtenToOut += int64(n)
		budget -= n
	}
	if budget == 0 {
		// we can fulfill the request with buffered data
		return len(p), b.nilOrEOF()
	}
	// buffer was drained

	if b.eof {
		// we are at the end of the stream
		// and drained the buffer
		// the reader is done
		return 0, io.EOF
	}

	// read from the stream
	resp, err := b.stream.Recv()
	var readFromRemoteNow int
	if resp != nil {
		readFromRemoteNow = len(resp.Data)
	}
	if err == io.EOF {
		// we are at the end of the stream
		// we will also not call Recv again
		// we will return EOF after the buffer is drained
		b.eof = true
	} else if err != nil {
		return 0, err
	}
	b.readFromRemote += int64(readFromRemoteNow)

	// copy the data to the buffer
	n = 0
	if resp != nil {
		n = copy(p[copyFromLastRead:], resp.Data)
	}
	b.writtenToOut += int64(n)
	if n < readFromRemoteNow {
		// we have more data than the requested read wants
		// buffer for next call
		b.buf.Write(resp.Data[n:])
	}
	copiedToOutTotal := copyFromLastRead + n
	return copiedToOutTotal, b.nilOrEOF()
}

func (b *byteStreamReadCloser) Close() error {
	// cancel the context to
	// stop the stream from our side
	b.cancel()
	return nil
}

func (b *byteStreamReadCloser) nilOrEOF() error {
	if b.eof && b.buf.Len() == 0 {
		return io.EOF
	}
	return nil
}

func protoFindMissingBlobsRequest(blobDigests []integrity.Digest, digestFunction integrity.Algorithm) *remoteexecution_proto.FindMissingBlobsRequest {
	req := &remoteexecution_proto.FindMissingBlobsRequest{
		BlobDigests:    make([]*remoteexecution_proto.Digest, len(blobDigests)),
		DigestFunction: protohelper.ProtoDigestFunction(digestFunction),
	}
	for i, blobDigest := range blobDigests {
		req.BlobDigests[i] = &remoteexecution_proto.Digest{
			Hash:      blobDigest.Hex(digestFunction),
			SizeBytes: blobDigest.SizeBytes,
		}
	}
	return req
}

func fromProtoFindMissingBlobsResponse(resp *remoteexecution_proto.FindMissingBlobsResponse, digestFunction integrity.Algorithm) ([]integrity.Digest, error) {
	missingDigests := make([]integrity.Digest, len(resp.MissingBlobDigests))
	for i, protoDigest := range resp.MissingBlobDigests {
		var decodeErr error
		missingDigests[i], decodeErr = integrity.DigestFromHex(protoDigest.Hash, protoDigest.SizeBytes, digestFunction)
		if decodeErr != nil {
			return nil, fmt.Errorf("failed to decode digest %d: %w", i, decodeErr)
		}
	}
	return missingDigests, nil
}

func protoBatchReadBlobsRequest(blobDigests []integrity.Digest, digestFunction integrity.Algorithm) *remoteexecution_proto.BatchReadBlobsRequest {
	req := &remoteexecution_proto.BatchReadBlobsRequest{
		DigestFunction: protohelper.ProtoDigestFunction(digestFunction),
	}
	for _, blobDigest := range blobDigests {
		req.Digests = append(req.Digests, &remoteexecution_proto.Digest{
			Hash:      blobDigest.Hex(digestFunction),
			SizeBytes: blobDigest.SizeBytes,
		})
	}
	return req
}

func fromProtoBatchReadBlobsResponse(resp *remoteexecution_proto.BatchReadBlobsResponse, digestFunction integrity.Algorithm) (BatchReadBlobsResponse, error) {
	readResponses := make(BatchReadBlobsResponse, len(resp.Responses))
	for i, protoResponse := range resp.Responses {
		var decodeErr error
		readResponses[i].Digest, decodeErr = integrity.DigestFromHex(protoResponse.Digest.Hash, protoResponse.Digest.SizeBytes, digestFunction)
		if decodeErr != nil {
			return nil, fmt.Errorf("failed to decode digest %d: %w", i, decodeErr)
		}
		readResponses[i].Status = protohelper.FromProtoStatus(protoResponse.Status)
		// we create a new slice to avoid sharing the underlying buffer
		// TODO: check if proto / gRPC in Go actually recycles the buffer
		// or if we can avoid this copy
		readResponses[i].Data = make([]byte, len(protoResponse.Data))
		copy(readResponses[i].Data, protoResponse.Data)
	}

	var issues int
	for _, response := range readResponses {
		if response.Data == nil || response.Status.Code != status.Status_OK {
			issues++
		}
	}
	if issues > 0 {
		return readResponses, BatchResponseHasNonZeroStatus
	}

	return readResponses, nil
}

func protoReadRequest(blobDigest integrity.Digest, digestFunction integrity.Algorithm, offset, limit int64) *bytestream_proto.ReadRequest {
	return &bytestream_proto.ReadRequest{
		ReadOffset:   offset,
		ReadLimit:    limit,
		ResourceName: fmt.Sprintf("blobs/%s/%d", blobDigest.Hex(digestFunction), blobDigest.SizeBytes),
	}
}

var _ CAS = (*Remote)(nil)
