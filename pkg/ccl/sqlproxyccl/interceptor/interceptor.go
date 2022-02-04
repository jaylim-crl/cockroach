// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package interceptor

import (
	"encoding/binary"
	"io"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgproto3/v2"
)

// pgHeaderSizeBytes represents the number of bytes of a pgwire message header
// (i.e. one byte for type, and an int for the body size, inclusive of the
// length itself).
const pgHeaderSizeBytes = 5

// ErrSmallBuffer indicates that the requested buffer for the interceptor is
// too small.
var ErrSmallBuffer = errors.New("buffer is too small")

// ErrInterceptorClosed is the returned error whenever the intercept is closed.
// When this happens, the caller should terminate both dst and src to guarantee
// correctness.
var ErrInterceptorClosed = errors.New("interceptor is closed")

// ErrProtocolError indicates that the packets are malformed, and are not as
// expected.
var ErrProtocolError = errors.New("protocol error")

// pgInterceptor provides a convenient way to read and forward Postgres
// messages, while minimizing IO reads and memory allocations.
//
// NOTE: Methods on the interceptor are not thread-safe.
type pgInterceptor struct {
	src io.Reader
	dst io.Writer

	// buf stores bytes which have been read, but have not been processed yet.
	buf []byte
	// readPos and writePos indicates the read and write pointers for bytes in
	// the buffer buf.
	readPos, writePos int

	// closed indicates that the interceptor is closed. This will be set to
	// true whenever there's an error within one of the interceptor's operations
	// leading to an ambiguity. Once an interceptor is closed, all subsequent
	// method calls on the interceptor will return ErrInterceptorClosed.
	closed bool
}

// newPgInterceptor creates a new instance of the interceptor with an internal
// buffer of bufSize bytes. bufSize must be at least the size of a pgwire
// message header.
func newPgInterceptor(src io.Reader, dst io.Writer, bufSize int) (*pgInterceptor, error) {
	// The internal buffer must be able to fit the header.
	if bufSize < pgHeaderSizeBytes {
		return nil, ErrSmallBuffer
	}
	return &pgInterceptor{
		src: src,
		dst: dst,
		buf: make([]byte, bufSize),
	}, nil
}

// ensureNextNBytes blocks on IO reads until the buffer has at least n bytes.
func (p *pgInterceptor) ensureNextNBytes(n int) error {
	if n < 0 || n > len(p.buf) {
		return errors.AssertionFailedf(
			"invalid number of bytes %d for buffer size %d", n, len(p.buf))
	}

	// Buffer already has n bytes.
	if p.ReadSize() >= n {
		return nil
	}

	// Not enough empty slots to fit the unread bytes, so re-align bytes.
	minReadCount := n - p.ReadSize()
	if p.WriteSize() < minReadCount {
		p.writePos = copy(p.buf, p.buf[p.readPos:p.writePos])
		p.readPos = 0
	}

	c, err := io.ReadAtLeast(p.src, p.buf[p.writePos:], minReadCount)
	p.writePos += c
	return err
}

// PeekMsg returns the header of the current pgwire message without advancing
// the interceptor. On return, err == nil if and only if the entire header can
// be read. Note that size corresponds to the body size, and does not account
// for the size field itself. This will return ErrProtocolError if the packets
// are malformed.
//
// If the interceptor is closed, PeekMsg returns ErrInterceptorClosed.
func (p *pgInterceptor) PeekMsg() (typ byte, size int, err error) {
	if p.closed {
		return 0, 0, ErrInterceptorClosed
	}

	if err := p.ensureNextNBytes(pgHeaderSizeBytes); err != nil {
		// Possibly due to a timeout or context cancellation.
		return 0, 0, err
	}

	typ = p.buf[p.readPos]
	size = int(binary.BigEndian.Uint32(p.buf[p.readPos+1:]))

	// Size has to be at least itself based on pgwire's protocol.
	if size < 4 {
		return 0, 0, ErrProtocolError
	}

	return typ, size - 4, nil
}

// WriteMsg writes the given bytes to the writer dst. If err != nil and a Write
// was attempted, the interceptor will be closed.
//
// If the interceptor is closed, WriteMsg returns ErrInterceptorClosed.
func (p *pgInterceptor) WriteMsg(data []byte) (n int, err error) {
	if p.closed {
		return 0, ErrInterceptorClosed
	}
	defer func() {
		// Close the interceptor if there was an error. Theoretically, we only
		// need to close here if n > 0, but for consistency with the other
		// methods, we will do that here too.
		if err != nil {
			p.Close()
		}
	}()
	return p.dst.Write(data)
}

// ReadMsg returns the current pgwire message in bytes. It also advances the
// interceptor to the next message. On return, the msg field is valid if and
// only if err == nil. If err != nil and a Read was attempted because the buffer
// did not have enough bytes, the interceptor will be closed.
//
// The interceptor retains ownership of all the memory returned by ReadMsg; the
// caller is allowed to hold on to this memory *until* the next moment other
// methods on the interceptor are called. The data will only be valid until then
// as well.
//
// If the interceptor is closed, ReadMsg returns ErrInterceptorClosed.
func (p *pgInterceptor) ReadMsg() (msg []byte, err error) {
	// Technically this is redundant since PeekMsg will do the same thing, but
	// we do so here for clarity.
	if p.closed {
		return nil, ErrInterceptorClosed
	}

	// Peek header of the current message for body size.
	_, size, err := p.PeekMsg()
	if err != nil {
		return nil, err
	}
	msgSizeBytes := pgHeaderSizeBytes + size

	// Can the entire message fit into the buffer?
	if msgSizeBytes <= len(p.buf) {
		if err := p.ensureNextNBytes(msgSizeBytes); err != nil {
			// Possibly due to a timeout or context cancellation.
			return nil, err
		}

		// Return a slice to the internal buffer to avoid an allocation here.
		retBuf := p.buf[p.readPos : p.readPos+msgSizeBytes]
		p.readPos += msgSizeBytes
		return retBuf, nil
	}

	// Message cannot fit, so we will have to allocate.
	msg = make([]byte, msgSizeBytes)

	// Copy bytes which have already been read.
	toCopy := msgSizeBytes
	if p.ReadSize() < msgSizeBytes {
		toCopy = p.ReadSize()
	}
	n := copy(msg, p.buf[p.readPos:p.readPos+toCopy])
	p.readPos += n // toCopy has to be the same as n here.

	defer func() {
		// Close the interceptor because we read the data (both buffered and
		// possibly newer ones) into msg, which is larger than the buffer's
		// size, and there's no easy way to recover. We could technically fix
		// some of the situations, especially when no bytes were read, but at
		// this point, it's likely that the one end of the interceptor is
		// already gone, or the proxy is shutting down, so there's no point
		// trying to save a disconnect.
		if err != nil {
			p.Close()
		}
	}()

	// Read more bytes.
	if _, err := io.ReadFull(p.src, msg[n:]); err != nil {
		return nil, err
	}

	return msg, nil
}

// ForwardMsg sends the current pgwire message to the destination, and advances
// the interceptor to the next message. On return, n == pgwire message size if
// and only if err == nil. If err != nil and a Write was attempted, the
// interceptor will be closed.
//
// If the interceptor is closed, ForwardMsg returns ErrInterceptorClosed.
func (p *pgInterceptor) ForwardMsg() (n int, err error) {
	// Technically this is redundant since PeekMsg will do the same thing, but
	// we do so here for clarity.
	if p.closed {
		return 0, ErrInterceptorClosed
	}

	// Retrieve header of the current message for body size.
	_, size, err := p.PeekMsg()
	if err != nil {
		return 0, err
	}

	// Handle overflows as current message may not fit in the current buffer.
	startPos := p.readPos
	endPos := startPos + pgHeaderSizeBytes + size
	remainingBytes := 0
	if endPos > p.writePos {
		remainingBytes = endPos - p.writePos
		endPos = p.writePos
	}
	p.readPos = endPos

	defer func() {
		// State may be invalid depending on whether bytes have been written.
		// To reduce complexity, we'll just close the interceptor, and the
		// caller should just terminate both ends.
		//
		// If src has been closed, the dst state may be invalid. If dst has been
		// closed, buffered bytes no longer represent the protocol correctly
		// even if we slurped the remaining bytes for the current message.
		if err != nil {
			p.Close()
		}
	}()

	// Forward the message to the destination.
	n, err = p.dst.Write(p.buf[startPos:endPos])
	if err != nil {
		return n, err
	}
	// n shouldn't be larger than the size of the buffer unless the
	// implementation of Write for dst is incorrect. This shouldn't be the case
	// if we're using a TCP connection here.
	if n < endPos-startPos {
		return n, io.ErrShortWrite
	}

	// Message was partially buffered, so copy the remaining.
	if remainingBytes > 0 {
		m, err := io.CopyN(p.dst, p.src, int64(remainingBytes))
		n += int(m)
		if err != nil {
			return n, err
		}
		// n shouldn't be larger than remainingBytes unless the internal Read
		// and Write calls for either of src or dst are incorrect. This
		// shouldn't be the case if we're using a TCP connection here.
		if int(m) < remainingBytes {
			return n, io.ErrShortWrite
		}
	}
	return n, nil
}

// ReadSize returns the number of bytes read by the interceptor. If the
// interceptor is closed, this will return 0.
func (p *pgInterceptor) ReadSize() int {
	if p.closed {
		return 0
	}
	return p.writePos - p.readPos
}

// WriteSize returns the remaining number of bytes that could fit into the
// internal buffer before needing to be re-aligned. If the interceptor is
// closed, this will return 0.
func (p *pgInterceptor) WriteSize() int {
	if p.closed {
		return 0
	}
	return len(p.buf) - p.writePos
}

// Close closes the interceptor, and prevents further operations on it.
func (p *pgInterceptor) Close() {
	p.closed = true
}

var errInvalidRead = errors.New("invalid read in chunkReader")

var _ pgproto3.ChunkReader = &chunkReader{}

// chunkReader is a wrapper on a single Postgres message, and is meant to be
// used with the Receive method on pgproto3.{NewFrontend, NewBackend}.
type chunkReader struct {
	msg []byte
	pos int
}

func newChunkReader(msg []byte) pgproto3.ChunkReader {
	return &chunkReader{msg: msg}
}

// Next implements the pgproto3.ChunkReader interface. An io.EOF will be
// returned once the entire message has been read. If the caller tries to read
// more bytes than it could, an errInvalidRead will be returned.
func (cr *chunkReader) Next(n int) (buf []byte, err error) {
	if cr.pos == len(cr.msg) {
		return nil, io.EOF
	}
	if cr.pos+n > len(cr.msg) {
		return nil, errInvalidRead
	}
	buf = cr.msg[cr.pos : cr.pos+n]
	cr.pos += n
	return buf, nil
}

// FrontendInterceptor is a client interceptor for the Postgres frontend
// protocol.
type FrontendInterceptor struct {
	p *pgInterceptor
}

// NewFrontendInterceptor creates a FrontendInterceptor. bufSize must be at
// least the size of a pgwire message header.
func NewFrontendInterceptor(
	src io.Reader, dst io.Writer, bufSize int,
) (*FrontendInterceptor, error) {
	pgi, err := newPgInterceptor(src, dst, bufSize)
	if err != nil {
		return nil, err
	}
	return &FrontendInterceptor{p: pgi}, nil
}

// PeekMsg returns the header of the current pgwire message without advancing
// the interceptor. See pgInterceptor.PeekMsg for more information.
func (fi *FrontendInterceptor) PeekMsg() (typ pgwirebase.ServerMessageType, size int, err error) {
	byteType, size, err := fi.p.PeekMsg()
	return pgwirebase.ServerMessageType(byteType), size, err
}

// WriteMsg writes the given bytes to the writer dst. See pgInterceptor.WriteMsg
// for more information.
func (fi *FrontendInterceptor) WriteMsg(data pgproto3.BackendMessage) (n int, err error) {
	return fi.p.WriteMsg(data.Encode(nil))
}

// ReadMsg decodes the current pgwire message and returns a BackendMessage.
// This also advances the interceptor to the next message. See
// pgInterceptor.ReadMsg for more information.
func (fi *FrontendInterceptor) ReadMsg() (msg pgproto3.BackendMessage, err error) {
	msgBytes, err := fi.p.ReadMsg()
	if err != nil {
		return nil, err
	}
	// errPanicWriter is used here because Receive must not Write.
	return pgproto3.NewFrontend(newChunkReader(msgBytes), &errPanicWriter{}).Receive()
}

// ForwardMsg sends the current pgwire message to the destination without any
// decoding, and advances the interceptor to the next message. See
// pgInterceptor.ForwardMsg for more information.
func (fi *FrontendInterceptor) ForwardMsg() (n int, err error) {
	return fi.p.ForwardMsg()
}

// BackendInterceptor is a server interceptor for the Postgres backend protocol.
type BackendInterceptor struct {
	p *pgInterceptor
}

// NewBackendInterceptor creates a BackendInterceptor. bufSize must be at least
// the size of a pgwire message header.
func NewBackendInterceptor(src io.Reader, dst io.Writer, bufSize int) (*BackendInterceptor, error) {
	pgi, err := newPgInterceptor(src, dst, bufSize)
	if err != nil {
		return nil, err
	}
	return &BackendInterceptor{p: pgi}, nil
}

// PeekMsg returns the header of the current pgwire message without advancing
// the interceptor. See pgInterceptor.PeekMsg for more information.
func (bi *BackendInterceptor) PeekMsg() (typ pgwirebase.ClientMessageType, size int, err error) {
	byteType, size, err := bi.p.PeekMsg()
	return pgwirebase.ClientMessageType(byteType), size, err
}

// WriteMsg writes the given bytes to the writer dst. See pgInterceptor.WriteMsg
// for more information.
func (bi *BackendInterceptor) WriteMsg(data pgproto3.FrontendMessage) (n int, err error) {
	return bi.p.WriteMsg(data.Encode(nil))
}

// ReadMsg decodes the current pgwire message and returns a FrontendMessage.
// This also advances the interceptor to the next message. See
// pgInterceptor.ReadMsg for more information.
func (bi *BackendInterceptor) ReadMsg() (msg pgproto3.FrontendMessage, err error) {
	msgBytes, err := bi.p.ReadMsg()
	if err != nil {
		return nil, err
	}
	// errPanicWriter is used here because Receive must not Write.
	return pgproto3.NewBackend(newChunkReader(msgBytes), &errPanicWriter{}).Receive()
}

// ForwardMsg sends the current pgwire message to the destination without any
// decoding, and advances the interceptor to the next message. See
// pgInterceptor.ForwardMsg for more information.
func (bi *BackendInterceptor) ForwardMsg() (n int, err error) {
	return bi.p.ForwardMsg()
}

var _ io.Writer = &errPanicWriter{}

// errPanicWriter is an io.Writer that panics whenever a Write call is made.
type errPanicWriter struct{}

// Write implements the io.Writer interface.
func (w *errPanicWriter) Write(p []byte) (int, error) {
	panic("unexpected Write call")
}
