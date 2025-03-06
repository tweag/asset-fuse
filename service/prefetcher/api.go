package prefetcher

import "io"

type readerAtCloser interface {
	io.ReaderAt
	io.Closer
}
