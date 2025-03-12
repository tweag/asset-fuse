package prefetcher

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/tweag/asset-fuse/api"
	"github.com/tweag/asset-fuse/fs/handle"
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
	downloader     *downloader.Downloader
	checksumCache  *integrity.ChecksumCache
	digestFunction integritypkg.Algorithm
}

// NewPrefetcher creates a new Prefetcher.
func NewPrefetcher(localCAS casService.LocalCAS, remoteCAS casService.CAS, remoteAsset assetService.Asset, downloader *downloader.Downloader, checksumCache *integritypkg.ChecksumCache, digestFunction integritypkg.Algorithm) *Prefetcher {
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

// RandomAccessStream creates a reader for an asset.
// It is used to implement reading from a leaf file handle.
// Prefetcher can choose to stream the asset from any source.
// The caller is responsible for closing the reader.
// TODO: for now, we only download small files eagerly. Instead, we could emply different hybrid strategies:
// - Download small files eagerly.
// - TODO: Stream large files from the remote CAS, but also download them to the local cache (in the background).
// - TODO: For very large files, we could stream them from the remote CAS and store in-demand chunks in the local cache.
func (p *Prefetcher) RandomAccessStream(ctx context.Context, asset api.Asset, offset, limit int64) (readerAtCloser, error) {
	digest, err := p.getOrLearnDigest(ctx, asset)
	if err != nil {
		return nil, fmt.Errorf("obtaining digest to stream asset: %w", err)
	}

	// check if materializing is efficient or necessary
	if digest.SizeBytes < byteStreamThreshold || p.remoteCAS == nil {
		// One of the following conditions is true:
		// - The file is small enough to download in a single request
		// - We don't have a remote CAS to stream from
		if err := p.Materialize(ctx, asset); err != nil {
			return nil, err
		}
		return p.localCAS.ReadRandomAccessStream(ctx, digest, p.digestFunction, offset, min(limit, digest.SizeBytes))
	}

	// check if blob is already in the local cache
	missingLocal, err := p.localCAS.FindMissingBlobs(ctx, []integritypkg.Digest{digest}, p.digestFunction)
	if err != nil {
		return nil, err
	}
	if len(missingLocal) == 0 {
		// The data is already in the local cache.
		return p.localCAS.ReadRandomAccessStream(ctx, digest, p.digestFunction, offset, min(limit, digest.SizeBytes))
	}

	// We can and should stream the data from the remote CAS.
	// use handle.NewStreamingFileHandle to stream data from CAS.
	if _, err = p.Prefetch(ctx, asset); err != nil {
		return nil, err
	}
	logging.Debugf("streaming asset from remote CAS (%s: %s; %d bytes)", p.digestFunction.String(), digest.Hex(p.digestFunction), digest.SizeBytes)
	return handle.NewStreamingFileHandle(p.remoteCAS, digest, p.digestFunction, offset), nil
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

	// we don't know the hash and size of the expected data
	// we need to fetch the data to learn the hash and size
	if digest, err := p.Prefetch(ctx, asset); err != nil {
		logging.Debugf("materializing asset %v failed when trying to prefetch remotely - falling back to direct download: %v", asset, err)
	} else {
		return p.materializeWithDigest(ctx, asset, digest)
	}

	// we failed to prefetch the data remotely
	// we need to fall back to direct download
	resp, err := p.downloader.FetchBlob(ctx, noFetchTimeout, noFetchOldestContentAcceptable, asset, p.digestFunction)
	if err != nil {
		logging.Warningf("materializing asset failed when trying to download directly: %v", err)
		return err
	}
	// we learned a new association between the asset and the digest
	var integrityStrings []string
	for integrityString := range asset.Integrity.Items() {
		integrityStrings = append(integrityStrings, integrityString.ToSRI())
	}
	logging.Basicf("Learned new association: %v -> %s (content size: %d bytes)", integrityStrings, resp.BlobDigest.Hex(p.digestFunction), resp.BlobDigest.SizeBytes)
	p.checksumCache.PutIntegrity(asset.Integrity, resp.BlobDigest)
	return nil
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
		logging.Debugf("streaming large blob from remote to local CAS (%s: %s; %d bytes)", p.digestFunction.String(), digests[0].Hex(p.digestFunction), digests[0].SizeBytes)

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

	// the data is not in the local cache - check all remote sources we have:
	// 1. remote CAS (knowing the digest)
	// 2. Refill remote CAS from remote asset API
	// 3. Direct download from URIs

	var isAvailableRemotely bool
	if p.remoteCAS != nil {
		missingBlobs, err := p.remoteCAS.FindMissingBlobs(ctx, missingBlobs, p.digestFunction)
		if err != nil {
			return err
		}
		if len(missingBlobs) == 0 {
			isAvailableRemotely = true
		}
	}

	if !isAvailableRemotely && p.remoteAsset != nil && p.remoteCAS != nil {
		// TODO: make timeout and oldestContentAccepted configurable.
		// TODO: choose reasonable defaults.
		fetchBlobResponse, err := p.remoteAsset.FetchBlob(ctx, noFetchTimeout, noFetchOldestContentAcceptable, asset, p.digestFunction)
		if err != nil {
			logging.Errorf("failed to fetch asset remotely - falling back to direct download: %v", err)
		} else {
			if !digest.Equals(fetchBlobResponse.BlobDigest, p.digestFunction) {
				return fmt.Errorf("expected digest %s, got %s", digest.Hex(p.digestFunction), fetchBlobResponse.BlobDigest.Hex(p.digestFunction))
			}
			isAvailableRemotely = true
		}
	}

	if isAvailableRemotely {
		// We now assume that the data is in the remote CAS.
		// We simply download it from the remote CAS to the local CAS.
		if err = p.casRemoteToLocalTransfer(ctx, digest); err != nil {
			logging.Errorf("failed to fetch remotely and transfer data to local CAS - falling back to direct download: %v", err)
		} else {
			return nil
		}
	}

	// finally, fall back to using HTTP requests directly
	_, err = p.downloader.FetchBlob(ctx, noFetchTimeout, noFetchOldestContentAcceptable, asset, p.digestFunction)
	if err != nil {
		return err
	}
	logging.Debugf("successfully downloaded asset (%s: %s; %d bytes)", p.digestFunction.String(), digest.Hex(p.digestFunction), digest.SizeBytes)
	return nil
}

func (p *Prefetcher) getOrLearnDigest(ctx context.Context, asset api.Asset) (digest integritypkg.Digest, err error) {
	if digest, ok := p.checksumCache.FromIntegrity(asset.Integrity); ok {
		return digest, nil
	}

	defer func() {
		if digest.Uninitialized() {
			return
		}
		// any digest we learn is stored in the cache
		// we learned a new association between the asset and the digest
		var integrityStrings []string
		for integrityString := range asset.Integrity.Items() {
			integrityStrings = append(integrityStrings, integrityString.ToSRI())
		}
		logging.Basicf("Learned new association: %v -> %s (content size: %d bytes)", integrityStrings, digest.Hex(p.digestFunction), digest.SizeBytes)
		p.checksumCache.PutIntegrity(asset.Integrity, digest)
	}()

	if p.remoteAsset != nil {
		fetchBlobResponse, err := p.remoteAsset.FetchBlob(ctx, noFetchTimeout, noFetchOldestContentAcceptable, asset, p.digestFunction)
		if err != nil {
			logging.Errorf("failed to learn digest via Remote Asset API - falling back to direct download: %v", err)
		} else {
			return fetchBlobResponse.BlobDigest, nil
		}
	}

	if p.downloader != nil {
		resp, err := p.downloader.FetchBlob(ctx, noFetchTimeout, noFetchOldestContentAcceptable, asset, p.digestFunction)
		if err != nil {
			logging.Errorf("failed to learn digest via direct download: %v", err)
		} else {
			return resp.BlobDigest, nil
		}
	}
	return integritypkg.Digest{}, errors.New("failed to learn digest")
}

var (
	noFetchTimeout                 = time.Duration(0)
	noFetchOldestContentAcceptable = time.Unix(0, 0).UTC()
)

const (
	// byteStreamThreshold is the threshold at which we switch
	// fetching data in a single request to streaming (1 MiB).
	//
	// This value was chosen arbitrarily.
	// TODO: make this configurable.
	// TODO: use capabilities API to determine the best value.
	byteStreamThreshold = 1 << 20
	// downloadLimit is the maximum blob size that we consider adding to the local cache (64 MiB).
	// If the blob is larger than this, we will always stream it.
	// Smaller files may still be streamed, but the prefetcher can try
	// to make them available in the local cache asynchronously.
	// Very small files (below byteStreamThreshold) are always fetched
	// in a single request and will always be in the local cache.
	//
	// This value was chosen arbitrarily.
	downloadLimit = 1 << 26
)
