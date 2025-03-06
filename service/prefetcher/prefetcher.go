package prefetcher

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	integritypkg "github.com/tweag/asset-fuse/integrity"
	"github.com/tweag/asset-fuse/service/api"
	assetService "github.com/tweag/asset-fuse/service/asset"
	casService "github.com/tweag/asset-fuse/service/cas"
	"github.com/tweag/asset-fuse/service/downloader"
)

// Prefetcher implements a simple prefetching mechanism.
//
// It is designed to be used by the FUSE filesystem and has the following properties:
// - Return data immediately if it is already in the cache.
// - Prefetch data in the background if we know or suspect it will be needed soon.
// - Have different aggressive prefetching strategies.
//
// Prefetcher has public methods that can be invoked concurrently.
// Internally, it uses goroutines to wait for long-running operations.
//
// TODO: for now, the prefetcher doesn't cache a lot of information in memory.
// This is a proof of concept and should be improved in the future.
type Prefetcher struct {
	remoteCAS      casService.CAS
	localCAS       casService.CAS
	remoteAsset    assetService.Asset
	downloader     downloader.Downloader
	digestFunction integritypkg.Algorithm
}

// NewPrefetcher creates a new Prefetcher.
func NewPrefetcher(remoteCAS casService.CAS, localCAS casService.CAS, remoteAsset assetService.Asset, downloader downloader.Downloader, digestFunction integritypkg.Algorithm) *Prefetcher {
	return &Prefetcher{
		remoteCAS:      remoteCAS,
		localCAS:       localCAS,
		remoteAsset:    remoteAsset,
		downloader:     downloader,
		digestFunction: digestFunction,
	}
}

func (p *Prefetcher) Start(ctx context.Context) (stopFunc func() error, err error) {
	panic("implement me")
}

// Stream creates a reader for an asset.
// It is used to implement reading from a leaf file handle.
// Prefetcher can choose to stream the asset from any source.
// The caller is responsible for closing the reader.
// TODO: use information we get from streaming to fill local cache (or in-memory cache) if needed.
// TODO: make the source configurable via a policy. This could mean to only stream from local cache,
// instead of allowing the prefetcher to choose the source.
func (p *Prefetcher) Stream(ctx context.Context, uris []string, integrity integritypkg.Integrity, qualifiers map[string]string) (io.ReadCloser, error) {
	panic("implement me")
}

// RandomAccessStream creates a reader for an asset that supports random access (ReadAt).
// It is used to implement reading from a leaf file handle.
// Prefetcher can choose to stream the asset from any source.
// The caller is responsible for closing the reader.
// TODO: for now, we only support streaming from the local cache (so we have random access via file io).
// TODO: Remote random access is theoretically possible, but requires intelligent caching / prefetching to be efficient.
func (p *Prefetcher) RandomAccessStream(ctx context.Context) (readerAtCloser, error) {
	panic("implement me")
}

// Prefetch ensures that the asset referenced by the given URIs and integrity is available in the remote CAS.
// Our only goal is to make the data available remotely, so we efficiently access it for remote execution.
// This means that calling Prefetch doesn't guarantee that the data is available locally.
// TODO: decide how users can get notified when the prefetching is done.
// TODO: deduplicate requests.
func (p *Prefetcher) Prefetch(ctx context.Context, uris []string, integrity integritypkg.Integrity, qualifiers map[string]string) error {
	// TODO: make this non-blocking with a method to get notified when the prefetching is done.
	// TODO: for now, this is blocking - bad.

	panic("implement me")
}

// Materialize ensures that the asset referenced by the given URIs and integrity is available in the local cache for reading.
// Our only goal is to make the data available locally, so we can stop as soon as localCAS has the expected data.
// This means that calling Materialize doesn't guarantee that the data is available remotely.
func (p *Prefetcher) Materialize(ctx context.Context, uris []string, integrity integritypkg.Integrity, qualifiers map[string]string, sizeHint int64) error {
	// TODO: make this non-blocking with a method to get notified when the prefetching is done.
	// TODO: for now, this is blocking - bad.
	if p.localCAS == nil {
		return errors.New("Materialize called without disk cache")
	}

	checksum, haveExpectedChecksum := integrity.ChecksumForAlgorithm(p.digestFunction)

	if sizeHint >= 0 && haveExpectedChecksum {
		// we know the hash and size of the expected data
		// we can construct the digest in advance
		return p.materializeWithDigest(ctx, uris, integrity, integritypkg.NewDigest(checksum.Hash, sizeHint, p.digestFunction), qualifiers)
	}

	panic("implement materialize if the digest is not known in advance (missing hash or size)")
}

func (p *Prefetcher) casRemoteToLocalTransfer(ctx context.Context, digests ...integritypkg.Digest) error {
	if p.localCAS == nil {
		return errors.New("cannot transfer data from remote CAS to disk cache without disk cache")
	}
	if p.remoteCAS == nil {
		return errors.New("cannot transfer data from remote CAS to disk cache without remote CAS")
	}

	cumulativeSize := int64(0)
	for _, digest := range digests {
		cumulativeSize += digest.SizeBytes
	}

	if cumulativeSize < byteStreamThreshold {
		// we can fetch the data in a single request
		readResponses, err := p.remoteCAS.BatchReadBlobs(ctx, digests, p.digestFunction)
		if err != nil {
			return err
		}
		if len(readResponses) != len(digests) {
			return fmt.Errorf("unexpected number of responses from remote CAS: expected %d, got %d", len(digests), len(readResponses))
		}

		digestsAndData := make(casService.DigestsAndData, len(digests))
		for i, readResponse := range readResponses {
			digestsAndData[i] = casService.DigestAndData{Digest: digests[i], Data: readResponse.Data}
		}

		response, err := p.localCAS.BatchUpdateBlobs(ctx, digestsAndData, p.digestFunction)
		if err != nil {
			return err
		}

		if len(response) != 1 {
			return errors.New("unexpected number of responses from local CAS: expected 1, got " + string(len(response)))
		}
		// TODO: follow the spec and properly check the response
		if response[0].Status.Code != api.Status_OK {
			return errors.New("unexpected status from local CAS: " + response[0].Status.Message)
		}
		return nil
	}

	// we need to stream the data
	panic("implement data streaming when transferring blobs from remote CAS to disk cache")
}

func (p *Prefetcher) materializeWithDigest(ctx context.Context, uris []string, integrity integritypkg.Integrity, digest integritypkg.Digest, qualifiers map[string]string) error {
	// first, check if the data is already in the local cache
	missingBlobs, err := p.localCAS.FindMissingBlobs(ctx, []integritypkg.Digest{digest}, p.digestFunction)
	if err != nil {
		return err
	}
	if len(missingBlobs) == 0 {
		// the data is already in the local cache
		return nil
	}

	// the data is not in the local cache - check all remote sources we have
	if p.remoteCAS != nil {
		missingBlobs, err := p.remoteCAS.FindMissingBlobs(ctx, missingBlobs, p.digestFunction)
		if err != nil {
			return err
		}
		if len(missingBlobs) == 0 {
			// the data is already in the remote CAS
			return nil
		}
	}

	if p.remoteAsset != nil {
		// TODO: make timeout and oldestContentAccepted configurable.
		// TODO: choose reasonable defaults.
		fetchBlobResponse, err := p.remoteAsset.FetchBlob(ctx, noFetchTimeout, noFetchOldestContentAcceptable, uris, integrity, qualifiers, p.digestFunction)
		if err != nil {
			return err
		}
		// We now assume that the data is in the remote CAS.
		// We simply download it from the remote CAS to the local CAS.
		return p.casRemoteToLocalTransfer(ctx, fetchBlobResponse.BlobDigest)
	}

	panic("implement materialize when the data is neither available in the local cache nor in the remote CAS")
	// TODO: fall back to remote asset API
	// TODO: finally, fall back to using HTTP requests directly
}

var (
	noFetchTimeout                 = time.Duration(0)
	noFetchOldestContentAcceptable = time.Unix(0, 0).UTC()
)

// byteStreamThreshold is the threshold at which we switch
// fetching data in a single request to streaming (1 MiB).
//
// This value was chosen arbitrarily.
// TODO: make this configurable.
// TODO: use capabilities API to determine the best value.
const byteStreamThreshold = 1 << 20
