package prefetcher

import "io"

type readerAtCloser interface {
	io.Reader
	io.ReaderAt
	io.Closer
}
