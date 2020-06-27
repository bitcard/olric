// Copyright 2018-2020 Burak Sezer
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package transport

import (
	"context"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buraksezer/olric/internal/flog"
	"github.com/buraksezer/olric/internal/protocol"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
)

const (
	idleConn uint32 = 0
	busyConn uint32 = 1
)

// Server implements a concurrent TCP server.
type Server struct {
	bindAddr        string
	bindPort        int
	keepAlivePeriod time.Duration
	log             *flog.Logger
	wg              sync.WaitGroup
	listener        net.Listener
	dispatcher      func(*protocol.Message) *protocol.Message
	StartCh         chan struct{}
	ctx             context.Context
	cancel          context.CancelFunc
}

// NewServer creates and returns a new Server.
func NewServer(bindAddr string, bindPort int, keepalivePeriod time.Duration, logger *flog.Logger) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		bindAddr:        bindAddr,
		bindPort:        bindPort,
		keepAlivePeriod: keepalivePeriod,
		log:             logger,
		StartCh:         make(chan struct{}),
		ctx:             ctx,
		cancel:          cancel,
	}
}

func (s *Server) SetDispatcher(f func(*protocol.Message) *protocol.Message) {
	s.dispatcher = f
}

func (s *Server) controlConnLifeCycle(conn io.ReadWriteCloser, connStatus *uint32, done chan struct{}) {
	// Control connection state and close it.
	defer s.wg.Done()

	select {
	case <-s.ctx.Done():
		// The server is down.
	case <-done:
		// The main loop is quit. TCP socket may be closed or a protocol error occurred.
	}

	if atomic.LoadUint32(connStatus) != idleConn {
		s.log.V(3).Printf("[DEBUG] Connection is busy, waiting")
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		//
		// WARNING: I added this context to fix a deadlock issue when an Olric node is being closed.
		// Debugging such an error is pretty hard and it blocks me. Normally I expect that SetDeadline
		// should fix the problem but It doesn't work. I don't know why. But this hack works well.
		//
		// TODO: Make this parametric.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
	loop:
		for {
			select {
			// Wait for the current request. When it mark the connection as idle, break the loop.
			case <-ticker.C:
				if atomic.LoadUint32(connStatus) == idleConn {
					s.log.V(3).Printf("[DEBUG] Connection is idle, closing")
					break loop
				}
			case <-ctx.Done():
				s.log.V(3).Printf("[DEBUG] Connection is still in-use. Aborting.")
				break loop
			}
		}
	}

	// Close the connection and quit.
	if err := conn.Close(); err != nil {
		s.log.V(3).Printf("[DEBUG] Failed to close TCP connection: %v", err)
	}
}

// processMessage waits for a new request, handles it and returns the appropriate response.
func (s *Server) processMessage(conn io.ReadWriteCloser, connStatus *uint32) error {
	var req protocol.Message
	// Read reads the incoming message from the underlying TCP socket and parses
	err := req.Read(conn)
	if err != nil {
		return errors.WithMessage(err, "failed to read request")
	}

	// Mark connection as busy.
	atomic.StoreUint32(connStatus, busyConn)

	// Mark connection as idle before start waiting a new request
	defer atomic.StoreUint32(connStatus, idleConn)

	// Conn is required for streams
	req.SetConn(conn)

	// The dispatcher is defined by olric package and responsible to evaluate the incoming message.
	resp := s.dispatcher(&req)
	return resp.Write(conn)
}

// processConn waits for requests and calls request handlers to generate a response. The connections are reusable.
func (s *Server) processConn(conn net.Conn) {
	defer s.wg.Done()

	// connStatus is useful for closing the server gracefully.
	var connStatus uint32
	done := make(chan struct{})
	defer close(done)

	s.wg.Add(1)
	go s.controlConnLifeCycle(conn, &connStatus, done)

	for {
		// processMessage waits to read a message from the TCP socket.
		// Then calls its handler to generate a response.
		err := s.processMessage(conn, &connStatus)
		if err != nil {
			s.log.V(3).Printf("[ERROR] Failed to process the incoming request: %v", err)

			// The socket probably would have been closed by the client.
			if errors.Cause(err) == io.EOF || errors.Cause(err) == protocol.ErrConnClosed {
				s.log.V(3).Printf("[ERROR] End of the TCP connection: %v", err)
				break
			}
		}
	}
}

// listenAndServe calls Accept on given net.Listener.
func (s *Server) listenAndServe() error {
	close(s.StartCh)

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				// the server is closed. just quit.
				return nil
			default:
			}
			s.log.V(3).Printf("[DEBUG] Failed to accept TCP connection: %v", err)
			continue
		}
		if s.keepAlivePeriod.Seconds() != 0 {
			err = conn.(*net.TCPConn).SetKeepAlive(true)
			if err != nil {
				return err
			}
			err = conn.(*net.TCPConn).SetKeepAlivePeriod(s.keepAlivePeriod)
			if err != nil {
				return err
			}
		}
		s.wg.Add(1)
		go s.processConn(conn)
	}
}

// ListenAndServe listens on the TCP network address addr.
func (s *Server) ListenAndServe() error {
	defer func() {
		select {
		case <-s.StartCh:
			return
		default:
		}
		close(s.StartCh)
	}()

	addr := net.JoinHostPort(s.bindAddr, strconv.Itoa(s.bindPort))
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.listener = l
	return s.listenAndServe()
}

// Shutdown gracefully shuts down the server without interrupting any active connections.
// Shutdown works by first closing all open listeners, then closing all idle connections,
// and then waiting indefinitely for connections to return to idle and then shut down.
// If the provided context expires before the shutdown is complete, Shutdown returns
// the context's error, otherwise it returns any error returned from closing the Server's
// underlying Listener(s).
func (s *Server) Shutdown(ctx context.Context) error {
	select {
	case <-s.ctx.Done():
		// It's already closed.
		return nil
	default:
	}

	var result error
	s.cancel()
	err := s.listener.Close()
	if err != nil {
		result = multierror.Append(result, err)
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		err = ctx.Err()
		if err != nil {
			result = multierror.Append(result, err)
		}
	case <-done:
	}
	return result
}
