package handle

import (
	"context"
	"io"
)

// byteStreamReadSeekCloser is a ReadSeeker that reads from a ByteStream.
// Seek operations require opening a new stream (which is not efficient).
type byteStreamReadSeekCloser struct {
	offset            int64
	size              int64
	reconnectAtOffset func(offset int64) (io.Reader, context.CancelFunc, error)
	reader            io.Reader
	cancel            context.CancelFunc
}

func (b *byteStreamReadSeekCloser) Read(p []byte) (n int, err error) {
	if b.reader == nil {
		// connect on first read
		// this is an optimization for handles that are never read
		reader, cancel, err := b.reconnectAtOffset(0)
		if err != nil {
			return 0, err
		}
		b.reader = reader
		b.cancel = cancel
	}
	n, err = b.reader.Read(p)
	b.offset += int64(n)
	return n, err
}

func (b *byteStreamReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	var targetOffset int64
	switch whence {
	case io.SeekStart:
		targetOffset = offset
	case io.SeekCurrent:
		targetOffset = b.offset + offset
	case io.SeekEnd:
		targetOffset = b.size + offset
	default:
		return 0, io.ErrUnexpectedEOF
	}

	if targetOffset < 0 {
		// TODO: ensure the error actually signals that the offset is invalid
		return 0, io.ErrUnexpectedEOF
	}
	if targetOffset > b.size {
		// TODO: ensure the error actually signals that the offset is invalid
		return 0, io.ErrUnexpectedEOF
	}
	if targetOffset == b.offset {
		return b.offset, nil
	}

	// if the distance is small, we can just read forward
	if targetOffset > b.offset && targetOffset-b.offset < 1<<20 {
		_, err := io.CopyN(io.Discard, b, targetOffset-b.offset)
		if err != nil {
			return 0, err
		}
		b.offset = targetOffset
		return b.offset, nil
	}

	// reconnect at the new offset
	if b.cancel != nil {
		b.cancel()
		b.cancel = nil
		b.reader = nil
	}
	reader, cancel, err := b.reconnectAtOffset(targetOffset)
	if err != nil {
		return 0, err
	}
	b.reader = reader
	b.cancel = cancel
	b.offset = targetOffset
	return b.offset, nil
}

func (b *byteStreamReadSeekCloser) Close() error {
	if b.cancel != nil {
		b.cancel()
		b.cancel = nil
		b.reader = nil
	}
	return nil
}
