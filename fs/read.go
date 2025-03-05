package fs

import "io"

type leafReader struct{}

func (l *leafReader) ReadAt(p []byte, off int64) (n int, err error) {
	panic("implement me")
}

func (l *leafReader) Close() error {
	panic("implement me")
}

// ensure leafReader can be used in the leafHandle.
var _ readAtCloser = (*leafReader)(nil)

type readAtCloser interface {
	io.ReaderAt
	io.Closer
}
