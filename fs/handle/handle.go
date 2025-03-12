package handle

import (
	"context"
	"io"
	"sync"

	"github.com/tweag/asset-fuse/integrity"
	"github.com/tweag/asset-fuse/service/cas"
)

// FileHandle describes a handle to an open file.
type FileHandle interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
}

type streamingFileHandle struct {
	// TODO: think about how to handle file closing

	// source is the underlying data source, such as a handle
	// that can perform reads and seeks via ByteStream.
	// Sadly, true random access is not possible with ByteStream,
	// so emulation is required (which is not efficient).
	// streamingFileHandle assumes exclusive access to the source.
	source io.ReadSeekCloser
	size   int64
	// TODO: we could cache file contents in memory to speed up repeated reads,
	// but the kernel already does this, so it may not be worth it.
	// contents [][4096]byte

	// The offset of the next read operation.
	// This may be different from the offset of the underlying source,
	// because we may have changed it to emulate random access.
	readOffset int64

	// The offset of the underlying source.
	// This is used to determine if we need to reposition the source
	// before reading.
	sourceOffset int64

	mux sync.Mutex
}

// NewStreamingFileHandle creates a new streaming file handle.
func NewStreamingFileHandle(cas cas.CAS, digest integrity.Digest, algorithm integrity.Algorithm, offset int64) FileHandle {
	source := byteStreamReadSeekCloser{
		offset: offset,
		size:   digest.SizeBytes,
		reconnectAtOffset: func(offset int64) (io.Reader, context.CancelFunc, error) {
			ctx, cancel := context.WithCancel(context.TODO())
			r, err := cas.ReadStream(ctx, digest, algorithm, offset, max(0, digest.SizeBytes-offset))
			return r, cancel, err
		},
	}

	return &streamingFileHandle{
		source:       &source,
		size:         digest.SizeBytes,
		readOffset:   offset,
		sourceOffset: offset,
	}
}

// Read reads up to len(p) bytes into p. It returns the number of bytes read (0 <= n <= len(p)) and any error encountered.
func (f *streamingFileHandle) Read(p []byte) (n int, err error) {
	f.mux.Lock()
	defer f.mux.Unlock()

	if f.readOffset != f.sourceOffset {
		// We need to reposition the source before reading.
		_, err = f.source.Seek(f.readOffset, io.SeekStart)
		if err != nil {
			return 0, err
		}
		f.sourceOffset = f.readOffset
	}

	n, err = f.source.Read(p)
	f.readOffset += int64(n)
	f.sourceOffset += int64(n)
	return n, err
}

// Seek sets the offset for the next Read or Write to offset,
// interpreted according to whence:
// [SeekStart] means relative to the start of the file,
// [SeekCurrent] means relative to the current offset, and
// [SeekEnd] means relative to the end
// (for example, offset = -2 specifies the penultimate byte of the file).
// Seek returns the new offset relative to the start of the
// file or an error, if any.
//
// Seeking to an offset before the start of the file is an error.
// Seeking to any positive offset may be allowed, but if the new offset exceeds
// the size of the underlying object the behavior of subsequent I/O operations
// is implementation-dependent.
func (f *streamingFileHandle) Seek(offset int64, whence int) (int64, error) {
	f.mux.Lock()
	defer f.mux.Unlock()

	// We need to emulate the correct behavior for the given whence.
	// SeekStart and SeekEnd behave as expected.
	// SeekCurrent is a bit more complicated, because the caller expects
	// the new offset to be relative to readOffset, not sourceOffset.
	if whence == io.SeekCurrent && f.readOffset != f.sourceOffset {
		offset += f.readOffset - f.sourceOffset
	}

	newOffset, err := f.source.Seek(offset, whence)
	if err != nil {
		return 0, err
	}
	f.readOffset = newOffset
	f.sourceOffset = newOffset
	return newOffset, nil
}

// ReadAt reads len(p) bytes into p starting at the specified offset.
// For a streaming file handle, this needs to be emulated.
func (f *streamingFileHandle) ReadAt(p []byte, off int64) (n int, err error) {
	f.mux.Lock()
	defer f.mux.Unlock()

	// ReadAt moves the offset of the underlying source, but not the offset the
	// caller expects for the next Read operation.
	if off != f.sourceOffset {
		_, err = f.source.Seek(off, io.SeekStart)
		if err != nil {
			return 0, err
		}
		f.sourceOffset = off
	}

	n, err = f.source.Read(p)
	f.sourceOffset += int64(n)
	return n, err
}

// Close closes the file handle.
func (f *streamingFileHandle) Close() error {
	return f.source.Close()
}

type proxyFileHandle struct {
	// TODO: think about how to handle file closing
	underlying proxyFile
}

type proxyFile interface {
	io.Reader
	io.Seeker
	io.ReaderAt
}
