package prefetcher

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/tweag/asset-fuse/api"
	"github.com/tweag/asset-fuse/integrity"
	integritypkg "github.com/tweag/asset-fuse/integrity"
	"github.com/tweag/asset-fuse/internal/logging"
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
	localCAS       casService.LocalCAS
	remoteAsset    assetService.Asset
	downloader     downloader.Downloader
	checksumCache  *integrity.ChecksumCache
	digestFunction integritypkg.Algorithm
}

// NewPrefetcher creates a new Prefetcher.
func NewPrefetcher(localCAS casService.LocalCAS, remoteCAS casService.CAS, remoteAsset assetService.Asset, downloader downloader.Downloader, checksumCache *integritypkg.ChecksumCache, digestFunction integritypkg.Algorithm) *Prefetcher {
	return &Prefetcher{
		localCAS:       localCAS,
		remoteCAS:      remoteCAS,
		remoteAsset:    remoteAsset,
		downloader:     downloader,
		checksumCache:  checksumCache,
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
func (p *Prefetcher) Stream(ctx context.Context, asset api.Asset, offset, limit int64) (io.ReadCloser, error) {
	panic("implement me")
}

// RandomAccessStream creates a reader for an asset that supports random access (ReadAt).
// It is used to implement reading from a leaf file handle.
// Prefetcher can choose to stream the asset from any source.
// The caller is responsible for closing the reader.
// TODO: for now, we only support streaming from the local cache (so we have random access via file io).
// TODO: Remote random access is theoretically possible, but requires intelligent caching / prefetching to be efficient.
func (p *Prefetcher) RandomAccessStream(ctx context.Context, asset api.Asset, offset, limit int64) (readerAtCloser, error) {
	// since we only have random access to the local cache,
	// we just materialize the data and return a reader for the local cache.
	if err := p.Materialize(ctx, asset); err != nil {
		return nil, err
	}

	digest, digestIsKnown := p.checksumCache.FromIntegrity(asset.Integrity)

	if !digestIsKnown {
		// unable to construct the digest in advance
		panic("implement random access streaming when the digest is not known in advance")
	}
	return p.localCAS.ReadRandomAccessStream(ctx, digest, p.digestFunction, offset, min(limit, digest.SizeBytes))
}

// Prefetch ensures that the asset referenced by the given URIs and integrity is available in the remote CAS.
// Our only goal is to make the data available remotely, so we efficiently access it for remote execution.
// This means that calling Prefetch doesn't guarantee that the data is available locally.
// TODO: decide how users can get notified when the prefetching is done.
// TODO: deduplicate requests.
// TODO: cache the result of the prefetching with a configurable TTL.
func (p *Prefetcher) Prefetch(ctx context.Context, asset api.Asset) (integrity.Digest, error) {
	// TODO: make this non-blocking with a method to get notified when the prefetching is done.
	// TODO: for now, this is blocking - bad.

	if p.remoteAsset == nil {
		return integritypkg.Digest{}, errors.New("Prefetch called without remote asset service")
	}

	knownDigest, digestIsKnown := p.checksumCache.FromIntegrity(asset.Integrity)

	if p.remoteCAS != nil && digestIsKnown {
		// check if the remote cache has the data already (without fetching)
		missingBlobs, err := p.remoteCAS.FindMissingBlobs(ctx, []integritypkg.Digest{knownDigest}, p.digestFunction)
		if err != nil {
			return integritypkg.Digest{}, err
		}
		if len(missingBlobs) == 0 {
			// the data is already in the remote cache
			return knownDigest, nil
		}
		// otherwise, we know the expected digest, but the remote cache doesn't have the data... continue with fetching.
	}

	fetchBlobResponse, err := p.remoteAsset.FetchBlob(ctx, noFetchTimeout, noFetchOldestContentAcceptable, asset, p.digestFunction)
	if err != nil {
		return integritypkg.Digest{}, err
	}
	// try to validate the response with the cached checksum
	if digestIsKnown {
		if !knownDigest.Equals(fetchBlobResponse.BlobDigest, p.digestFunction) {
			return integritypkg.Digest{}, fmt.Errorf("expected digest %s, got %s", knownDigest.Hex(p.digestFunction), fetchBlobResponse.BlobDigest.Hex(p.digestFunction))
		}
	} else {
		// we learned a new association between the asset and the digest
		var integrityStrings []string
		for integrityString := range asset.Integrity.Items() {
			integrityStrings = append(integrityStrings, integrityString.ToSRI())
		}
		logging.Basicf("Learned new association: %v -> %s (content size: %d bytes)", integrityStrings, fetchBlobResponse.BlobDigest.Hex(p.digestFunction), fetchBlobResponse.BlobDigest.SizeBytes)
		p.checksumCache.PutIntegrity(asset.Integrity, fetchBlobResponse.BlobDigest)
	}
	return fetchBlobResponse.BlobDigest, nil
}

// Materialize ensures that the asset referenced by the given URIs and integrity is available in the local cache for reading.
// Our only goal is to make the data available locally, so we can stop as soon as localCAS has the expected data.
// This means that calling Materialize doesn't guarantee that the data is available remotely.
func (p *Prefetcher) Materialize(ctx context.Context, asset api.Asset) error {
	// TODO: make this non-blocking with a method to get notified when the prefetching is done.
	// TODO: for now, this is blocking - bad.
	if p.localCAS == nil {
		return errors.New("Materialize called without disk cache")
	}

	if digest, ok := p.checksumCache.FromIntegrity(asset.Integrity); ok {
		// we know the hash and size of the expected data
		// we can construct the digest in advance
		return p.materializeWithDigest(ctx, asset, digest)
	}

	digest, err := p.Prefetch(ctx, asset)
	if err != nil {
		return fmt.Errorf("materializing asset %v failed when trying to learn digest: %w", asset, err)
	}

	return p.materializeWithDigest(ctx, asset, digest)
}

func (p *Prefetcher) casRemoteToLocalTransfer(ctx context.Context, digests ...integritypkg.Digest) error {
	if p.localCAS == nil {
		return errors.New("cannot transfer data from remote CAS to disk cache without disk cache")
	}
	if p.remoteCAS == nil {
		return errors.New("cannot transfer data from remote CAS to disk cache without remote CAS")
	}

	var err error
	for len(digests) > 0 {
		digests, err = p.casRemoteToLocalTransferPart(ctx, digests...)
		if err != nil {
			return err
		}
		if len(digests) == 0 {
			break
		}
	}
	return nil
}

// casRemoteToLocalTransferPart transfers a part of the data from the remote CAS to the local cache.
// It returns the digests of the data that is still missing in the local cache.
func (p *Prefetcher) casRemoteToLocalTransferPart(ctx context.Context, digests ...integritypkg.Digest) ([]integritypkg.Digest, error) {
	if len(digests) == 0 {
		return nil, nil
	}
	if digests[0].SizeBytes >= byteStreamThreshold {
		// The single blob is too large to fetch in a single request.
		// We need to stream it.
		reader, err := p.remoteCAS.ReadStream(ctx, digests[0], p.digestFunction, 0, 0)
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		writer, err := p.localCAS.WriteStream(ctx, digests[0], p.digestFunction)
		if err != nil {
			return nil, err
		}
		defer writer.Close()

		n, err := io.Copy(writer, reader)
		if err != nil {
			return nil, err
		}
		if n != digests[0].SizeBytes {
			return nil, fmt.Errorf("transfering data from remote to local cas: expected to read %d bytes, got %d", digests[0].SizeBytes, n)
		}
		return digests[1:], nil
	}

	// otherwise, get as much data as possible in a single request
	cumulativeSize := int64(0)
	numDigests := 0
	for _, digest := range digests {
		if cumulativeSize+digest.SizeBytes >= byteStreamThreshold {
			break
		}
		cumulativeSize += digest.SizeBytes
		numDigests++
	}

	readResponses, err := p.remoteCAS.BatchReadBlobs(ctx, digests[:numDigests], p.digestFunction)
	if err != nil {
		return nil, err
	}
	if len(readResponses) != numDigests {
		return nil, fmt.Errorf("unexpected number of responses from remote CAS: expected %d, got %d", numDigests, len(readResponses))
	}

	digestsAndData := make(casService.DigestsAndData, numDigests)
	for i, readResponse := range readResponses {
		digestsAndData[i] = casService.DigestAndData{Digest: digests[i], Data: readResponse.Data}
	}

	response, err := p.localCAS.BatchUpdateBlobs(ctx, digestsAndData, p.digestFunction)
	if err != nil {
		return nil, err
	}

	if len(response) != numDigests {
		return nil, fmt.Errorf("unexpected number of responses from local CAS: expected %d, got %d", len(digests), numDigests)
	}
	return digests[numDigests:], nil
}

func (p *Prefetcher) materializeWithDigest(ctx context.Context, asset api.Asset, digest integritypkg.Digest) error {
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
			return p.casRemoteToLocalTransfer(ctx, digest)
		}
	}

	if p.remoteAsset != nil {
		// TODO: make timeout and oldestContentAccepted configurable.
		// TODO: choose reasonable defaults.
		fetchBlobResponse, err := p.remoteAsset.FetchBlob(ctx, noFetchTimeout, noFetchOldestContentAcceptable, asset, p.digestFunction)
		if err != nil {
			return err
		}
		if !digest.Equals(fetchBlobResponse.BlobDigest, p.digestFunction) {
			return fmt.Errorf("expected digest %s, got %s", digest.Hex(p.digestFunction), fetchBlobResponse.BlobDigest.Hex(p.digestFunction))
		}
		// We now assume that the data is in the remote CAS.
		// We simply download it from the remote CAS to the local CAS.
		return p.casRemoteToLocalTransfer(ctx, digest)
	}

	panic("implement fallback to local downloader")
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
