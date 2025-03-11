package downloader

import (
	"bytes"
	"context"
	"fmt"
	"hash"
	"io"
	"maps"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tweag/asset-fuse/api"
	"github.com/tweag/asset-fuse/integrity"
	"github.com/tweag/asset-fuse/internal/logging"
	"github.com/tweag/asset-fuse/service/asset"
	casService "github.com/tweag/asset-fuse/service/cas"
	"github.com/tweag/asset-fuse/service/status"
)

// Downloader is a service that downloads files directly into the local CAS.
// It performs HTTP requests locally and never invokes the remote asset API or the remote CAS.
type Downloader struct {
	localCAS   casService.LocalCAS
	httpClient *http.Client
}

func New(localCAS casService.LocalCAS, httpClient *http.Client) *Downloader {
	return &Downloader{
		localCAS:   localCAS,
		httpClient: httpClient,
	}
}

// Fetch implements the Fetch service in the remote asset API.
// Downloader is the local variant of the remote asset service:
// It directly downloads the asset from the URIs and stores it in the local CAS.
func (d *Downloader) FetchBlob(
	ctx context.Context, timeout time.Duration, oldestContentAccepted time.Time,
	apiAsset api.Asset, digestFunction integrity.Algorithm,
) (asset.FetchBlobResponse, error) {
	logging.Debugf("downloading asset specified by %v", apiAsset.URIs)
	// TODO: caching based on URI, integrity, and qualifiers (while respecting oldestContentAccepted)
	// TODO: errors returned here should follow the remote asset API's error model (i.e, by setting meaningful status codes)
	sharedHeaders, perURIHeaders, err := headersFromQualifiersAndIntegrity(apiAsset.Qualifiers, apiAsset.Integrity, len(apiAsset.URIs))
	if err != nil {
		return asset.FetchBlobResponse{}, err
	}
	var digest integrity.Digest
	var uriUsed string
	var uriIssues []string
	for i, uri := range apiAsset.URIs {
		var requestHeaders map[string][]string
		if len(perURIHeaders[i]) > 0 {
			// merge the shared headers with the per-uri headers
			requestHeaders = maps.Clone(sharedHeaders)
			maps.Copy(requestHeaders, perURIHeaders[i])
		} else {
			// no per-uri headers - use the shared headers
			requestHeaders = sharedHeaders
		}
		digestForURI, err := d.downloadBlobFromURI(ctx, timeout, uri, requestHeaders, apiAsset.Integrity, digestFunction)
		if err == nil {
			digest = digestForURI
			uriUsed = uri
			break
		}
		uriIssues = append(uriIssues, fmt.Sprintf("%s: %v", uri, err))

	}
	if digest.Uninitialized() {
		return asset.FetchBlobResponse{}, fmt.Errorf("unable to download asset from any uri:\n  %v", strings.Join(uriIssues, "\n  "))
	}
	logging.Debugf("successfully downloaded asset from %s (%s: %s; %d bytes)", uriUsed, digestFunction.String(), digest.Hex(digestFunction), digest.SizeBytes)

	// form a well-specified response
	return asset.FetchBlobResponse{
		Status:     status.Status{Code: status.Status_OK},
		URI:        uriUsed,
		Qualifiers: apiAsset.Qualifiers,
		// TODO: set ExpiresAt?
		BlobDigest:     digest,
		DigestFunction: digestFunction,
	}, nil
}

func (d *Downloader) Client() *http.Client {
	return d.httpClient
}

func (d *Downloader) downloadBlobFromURI(ctx context.Context, timeout time.Duration,
	uri string, headers map[string][]string, expectedContent integrity.Integrity, digestFunction integrity.Algorithm,
) (integrity.Digest, error) {
	if expectedContent.Empty() {
		return integrity.Digest{}, fmt.Errorf("downloading blob from %s: no digests to validate", uri)
	}

	if timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, http.NoBody)
	if err != nil {
		return integrity.Digest{}, err
	}
	maps.Copy(req.Header, headers)
	resp, err := d.httpClient.Do(req)
	if err != nil {
		return integrity.Digest{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return integrity.Digest{}, fmt.Errorf("downloading blob from %s: unexpected status code %d", uri, resp.StatusCode)
	}
	// Check if the body is known to fit in memory.
	canDownloadInMemory := resp.ContentLength >= 0 && resp.ContentLength <= maxInMemoryDownloadSize

	var bodyStagingArea io.ReadWriter
	var bodyRewinder func() error
	if canDownloadInMemory {
		bodyStagingArea = &bytes.Buffer{}
	} else {
		tmpFile, fileErr := os.CreateTemp("", "asset-fuse-download-")
		if fileErr != nil {
			return integrity.Digest{}, fileErr
		}
		defer os.Remove(tmpFile.Name())
		defer tmpFile.Close()
		bodyStagingArea = tmpFile
		bodyRewinder = func() error {
			_, err := tmpFile.Seek(0, io.SeekStart)
			return err
		}
	}

	// calculate all digests at once
	digestWriters := []hash.Hash{}
	writers := []io.Writer{bodyStagingArea}
	var needHasherForSingleDigest bool = true
	for checksum := range expectedContent.Items() {
		if checksum.Algorithm == digestFunction {
			needHasherForSingleDigest = false
		}
		hasher := checksum.Algorithm.Hasher()
		digestWriters = append(digestWriters, hasher)
		writers = append(writers, hasher)
	}
	var hasherForSingleDigest hash.Hash
	if needHasherForSingleDigest {
		logging.Warningf("downloading blob from %s: no known %s checksum, calculating it manually", uri, digestFunction)
		hasherForSingleDigest = digestFunction.Hasher()
		writers = append(writers, hasherForSingleDigest)
	}

	multiWriter := io.MultiWriter(writers...)
	n, err := io.Copy(multiWriter, resp.Body)
	if err != nil {
		return integrity.Digest{}, err
	}
	// we need to move the file pointer back to the start
	if bodyRewinder != nil {
		if err := bodyRewinder(); err != nil {
			return integrity.Digest{}, err
		}
	}

	if resp.ContentLength >= 0 && n != resp.ContentLength {
		return integrity.Digest{}, fmt.Errorf("downloading blob from %s: unexpected content length %d bytes expected, got %d", uri, resp.ContentLength, n)
	}

	// validate all digests
	var knownDigest integrity.Digest
	var checksumValidationErrors []error
	var i int
	for expectedChecksum := range expectedContent.Items() {
		hasher := digestWriters[i]
		gotChecksum := integrity.Checksum{
			Algorithm: expectedChecksum.Algorithm,
			Hash:      hasher.Sum(nil),
		}
		if !expectedChecksum.Equals(gotChecksum) {
			checksumValidationErrors = append(checksumValidationErrors, fmt.Errorf("invalid %s: expected %x, got %x", expectedChecksum.Algorithm, expectedChecksum.Hash, gotChecksum.Hash))
		}
		if expectedChecksum.Algorithm == digestFunction {
			knownDigest = integrity.NewDigest(gotChecksum.Hash, n, digestFunction)
		}
		i++
	}
	if needHasherForSingleDigest {
		learnedHash := hasherForSingleDigest.Sum(nil)
		learnedChecksum := integrity.Checksum{Algorithm: digestFunction, Hash: learnedHash}
		logging.Basicf("downloading blob from %s: learned %s: %s", uri, digestFunction, learnedChecksum.ToSRI())
		knownDigest = integrity.NewDigest(learnedHash, n, digestFunction)
	}
	if len(checksumValidationErrors) > 0 {
		return integrity.Digest{}, fmt.Errorf("downloading blob from %s: %v", uri, checksumValidationErrors)
	}

	return d.localCAS.ImportBlob(ctx, expectedContent, knownDigest, digestFunction, bodyStagingArea)
}

func headersFromQualifiersAndIntegrity(qualifiers map[string]string, expectedIntegrity integrity.Integrity, numberOfURIs int) (shared http.Header, perUri []http.Header, err error) {
	shared = make(http.Header)
	perUri = make([]http.Header, numberOfURIs)
	for key, value := range qualifiers {
		if sharedHeaderName, ok := strings.CutPrefix(key, "http_header:"); ok {
			shared.Add(sharedHeaderName, value)
		} else if perUriHeaderName, ok := strings.CutPrefix(key, "http_header_uri:"); ok {
			parts := strings.SplitN(perUriHeaderName, ":", 2)
			if len(parts) != 2 {
				return nil, nil, fmt.Errorf("invalid http_header_uri: key %s", key)
			}
			uriIndex, err := strconv.Atoi(parts[0])
			if err != nil {
				return nil, nil, fmt.Errorf("invalid http_header_uri: key %s", key)
			}
			headerName := parts[1]
			if uriIndex < 0 || uriIndex >= numberOfURIs {
				return nil, nil, fmt.Errorf("invalid http_header_uri: uri index %d out of range", uriIndex)
			}
			if perUri[uriIndex] == nil {
				perUri[uriIndex] = make(http.Header)
			}
			perUri[uriIndex].Add(headerName, value)
		} else {
			return nil, nil, fmt.Errorf("unknown qualifier name %s", key)
		}
	}
	// TODO: add integrity headers
	return shared, perUri, nil
}

// maxInMemoryDownloadSize is the maximum size of a blob that we are willing to download in memory.
// If the blob is larger, we will download it to a temporary file.
// (64 MiB)
const maxInMemoryDownloadSize = 1 << 26
