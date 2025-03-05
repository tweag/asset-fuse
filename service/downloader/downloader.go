package downloader

import (
	casService "github.com/tweag/asset-fuse/service/cas"
)

// Downloader is a service that downloads files directly into the local CAS.
// It performs HTTP requests locally and never invokes the remote asset API or the remote CAS.
type Downloader struct {
	localCAS casService.CAS
}
