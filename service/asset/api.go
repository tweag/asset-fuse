package asset

import (
	"context"
	"time"

	"github.com/tweag/asset-fuse/integrity"
	"github.com/tweag/asset-fuse/service/api"
)

// Asset is the interface for a (remote) asset service.
type Asset interface {
	Fetch
	// This may be extended to also support the Push service in the future.
}

// Fetch is equivalent to the Fetch service in the remote asset API.
type Fetch interface {
	FetchBlob(
		ctx context.Context, timeout time.Duration, oldestContentAccepted time.Time,
		uris []string, integrity integrity.Integrity, qualifiers map[string]string,
		digestFunction integrity.Algorithm,
	) (FetchBlobResponse, error)
	// This may be extended to also support the FetchDirectory rpc in the future.
}

type FetchBlobResponse struct {
	Status         api.Status
	URI            string
	Qualifiers     map[string]string
	ExpiresAt      time.Time
	BlobDigest     integrity.Digest
	DigestFunction integrity.Algorithm
}
