// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlproxyccl

import (
	"context"
	"net"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/interceptor"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/errors"
)

// defaultBufferSize is the default buffer size for each of the interceptors
// created by the forwarder. 8K was chosen to match Postgres' send and receive
// buffer sizes.
//
// See: https://github.com/postgres/postgres/blob/249d64999615802752940e017ee5166e726bc7cd/src/backend/libpq/pqcomm.c#L134-L135.
const defaultBufferSize = 2 << 13 // 8K

// ErrForwarderClosed indicates that the forwarder has been closed.
var ErrForwarderClosed = errors.New("forwarder has been closed")

// forwarder is used to forward pgwire messages from the client to the server,
// and vice-versa. At the moment, this does a direct proxying, and there is
// no intercepting. Once https://github.com/cockroachdb/cockroach/issues/76000
// has been addressed, we will start intercepting pgwire messages at their
// boundaries here.
//
// The forwarder instance should always be constructed through the forward
// function, which also starts the forwarder.
type forwarder struct {
	// ctx is a single context used to control all goroutines spawned by the
	// forwarder.
	ctx       context.Context
	ctxCancel context.CancelFunc

	// serverConn is only set after the authentication phase for the initial
	// connection. In the context of a connection migration, serverConn is only
	// replaced once the session has successfully been deserialized, and the
	// old connection will be closed.
	//
	// All reads from these connections must go through the interceptors. It is
	// not safe to read from these directly as the interceptors may have
	// buffered data.
	clientConn net.Conn // client <-> proxy
	serverConn net.Conn // proxy <-> server

	// clientInterceptor and serverInterceptor provides a convenient way to
	// read and forward Postgres messages, while minimizing IO reads and memory
	// allocations.
	//
	// These interceptors have to match clientConn and serverConn. See comment
	// above on when those fields will be updated.
	//
	// TODO(jaylim-crl): Add updater functions that sets both conn and
	// interceptor fields at the same time. At the moment, there's no use case
	// besides the forward function. When connection migration happens, we
	// will need to update the destination for clientInterceptor, and create
	// a new serverInterceptor.
	clientInterceptor *interceptor.BackendInterceptor  // clientConn -> serverConn
	serverInterceptor *interceptor.FrontendInterceptor // serverConn -> clientConn

	// errChan is a buffered channel that contains the first forwarder error.
	// This channel may receive nil errors.
	errChan chan error
}

// forward returns a new instance of forwarder, and starts forwarding messages
// from clientConn to serverConn. When this is called, it is expected that the
// caller passes ownership of serverConn to the forwarder, which implies that
// the forwarder will clean up serverConn.
//
// All goroutines spun up must check on f.ctx to prevent leaks, if possible. If
// there was an error within the goroutines, the forwarder will be closed, and
// the first error can be found in f.errChan.
//
// clientConn and serverConn must not be nil in all cases except testing.
//
// Note that callers MUST call Close in all cases (regardless of IsStopped)
// since we only check on context cancellation there. There could be a
// possibility where the top-level context was cancelled, but the forwarder
// has not cleaned up.
func forward(ctx context.Context, clientConn, serverConn net.Conn) (f *forwarder, retErr error) {
	if clientConn == nil || serverConn == nil {
		return nil, errors.AssertionFailedf("clientConn and serverConn cannot be nil")
	}

	ctx, cancelFn := context.WithCancel(ctx)

	f = &forwarder{
		ctx:        ctx,
		ctxCancel:  cancelFn,
		clientConn: clientConn,
		serverConn: serverConn,
		errChan:    make(chan error, 1),
	}
	defer func() {
		if retErr != nil {
			f.Close()
		}
	}()

	// The net.Conn object for the client is switched to a net.Conn that
	// unblocks Read every second on idle to check for exit conditions.
	// This is mainly used to unblock the request processor whenever the
	// forwarder has stopped, or a transfer has been requested.
	f.clientConn = pgwire.NewReadTimeoutConn(f.clientConn, func() error {
		// Context was cancelled.
		if f.IsStopped() {
			return f.ctx.Err()
		}
		// TODO(jaylim-crl): Check for transfer state here.
		return nil
	})

	// Create initial interceptors.
	var err error
	f.clientInterceptor, err = interceptor.NewBackendInterceptor(
		f.clientConn,
		f.serverConn,
		defaultBufferSize,
	)
	if err != nil {
		return nil, errors.AssertionFailedf("initializing client interceptor: %v", err)
	}
	f.serverInterceptor, err = interceptor.NewFrontendInterceptor(
		f.serverConn,
		f.clientConn,
		defaultBufferSize,
	)
	if err != nil {
		return nil, errors.AssertionFailedf("initializing server interceptor: %v", err)
	}

	// Copy all pgwire messages from frontend to backend connection until we
	// encounter an error or shutdown signal.
	go func() {
		defer f.Close()

		err := f.startConnectionCopy()
		select {
		case f.errChan <- err: /* error reported */
		default: /* the channel already contains an error */
		}
	}()

	return f, nil
}

// Close closes the forwarder, and stops the forwarding process. This is
// idempotent.
func (f *forwarder) Close() {
	f.ctxCancel()

	// Since Close is idempotent, we'll ignore the error from Close in case it
	// has already been closed.
	f.serverConn.Close()
}

// IsStopped returns a boolean indicating that the forwarder has stopped
// forwarding messages. The forwarder will be stopped when one calls Close
// explicitly, or when any of its main goroutines is terminated, whichever that
// happens first.
//
// A new forwarder instance will have to be recreated if one wants to reuse the
// same pair of connections.
func (f *forwarder) IsStopped() bool {
	return f.ctx.Err() != nil
}

// startConnectionCopy does a bidirectional copy between the backend and
// frontend connections. It terminates when one of connections terminate.
func (f *forwarder) startConnectionCopy() error {
	errOutgoing := make(chan error, 1)
	errIncoming := make(chan error, 1)

	go func() {
		// Start request processor.
		err := f.processClient()
		errOutgoing <- err
	}()
	go func() {
		// Start response processor.
		err := f.processServer()
		errIncoming <- err
	}()

	select {
	// NB: when using pgx, we see a nil errIncoming first on clean connection
	// termination. Using psql I see a nil errOutgoing first. I think the PG
	// protocol stipulates sending a message to the server at which point the
	// server closes the connection (errIncoming), but presumably the client
	// gets to close the connection once it's sent that message, meaning either
	// case is possible.
	case err := <-errIncoming:
		// err cannot be nil, but we'll check anyway. For now, we'll return nil
		// when we get a context cancellation, but ideally we should return an
		// AdminShutdown error.
		if err == nil || errors.Is(err, context.Canceled) {
			return
		}
		if codeErr := (*codeError)(nil); errors.As(err, &codeErr) &&
			codeErr.code == codeExpiredClientConnection {
			return codeErr
		}
		if ne := (net.Error)(nil); errors.As(err, &ne) && ne.Timeout() {
			return newErrorf(codeIdleDisconnect, "terminating connection due to idle timeout: %v", err)
		}
		return newErrorf(codeBackendDisconnected, "copying from target server to client: %s", err)
	case err := <-errOutgoing:
		// err cannot be nil, but we'll check anyway. For now, we'll return nil
		// when we get a context cancellation, but ideally we should return an
		// AdminShutdown error.
		if err == nil || errors.Is(err, context.Canceled) {
			return
		}
		return newErrorf(codeClientDisconnected, "copying from client to target server: %v", err)
	}
}

// processClient, also known as request processor, handles the communication
// from the client to the server. This returns a context cancellation error
// whenever the forwarder is stopped, or whenever forwarding fails. When
// ForwardMsg gets blocked, we will unblock that through our custom
// readTimeoutConn wrapper, which gets triggered when the forwarder is stopped.
func (f *forwarder) processClient() error {
	for !f.IsStopped() {
		if _, err := f.clientInterceptor.ForwardMsg(); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// processServer, also known as response processor, handles the communication
// from the server to the client. This returns a context cancellation error
// whenever the forwarder is stopped, or whenever forwarding fails. When
// ForwardMsg gets blocked, we will unblock that by closing serverConn, which
// gets triggered when the forwarder is stopped.
func (f *forwarder) processServer() error {
	for !f.IsStopped() {
		if _, err := f.serverInterceptor.ForwardMsg(); err != nil {
			return err
		}
	}
	return ctx.Err()
}
