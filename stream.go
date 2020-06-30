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

package olric

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync"

	"github.com/buraksezer/olric/internal/protocol"
)

// streams maps StreamIDs to streams
type streams struct {
	mu sync.RWMutex

	m map[uint64]*stream
}

// streams provides a bidirectional communication channel between Olric nodes and clients. It can also be used
// for node-to-node communication.
type stream struct {
	read   chan protocol.MessageReadWriter
	write  chan protocol.MessageReadWriter
	ctx    context.Context
	cancel context.CancelFunc
}

// close sends OpStreamClosed command to other side of the channel and cancels underlying context.
func (s *stream) close() error {
	defer s.cancel()

	req := protocol.NewDMapMessage(protocol.OpStreamClosed)
	select {
	case s.write <- req:
		return nil
	default:
	}
	return fmt.Errorf("impossible to send StreamClosed message: channel busy")
}

func (db *Olric) readFromStream(conn io.ReadWriteCloser, bufCh chan<- protocol.MessageReadWriter, errCh chan<- error) {
	defer db.wg.Done()

	f := func() error {
		buf := bufferPool.Get()
		defer bufferPool.Put(buf)

		_, err := protocol.ReadMessage(conn, buf)
		if err != nil {
			return err

		}
		msg := protocol.NewDMapMessageFromRequest(buf)
		err = msg.Decode()
		if err != nil {
			return err
		}
		bufCh <- msg
		return nil
	}

	for {
		if err := f(); err != nil {
			errCh <- err
			break
		}
	}
}

func (db *Olric) createStreamOperation(w, r protocol.MessageReadWriter) {
	req := r.(*protocol.DMapMessage)
	conn := req.Buffer()

	// Now, we have a TCP socket here.
	streamID := rand.Uint64()
	ctx, cancel := context.WithCancel(context.Background())
	db.streams.mu.Lock()
	s := &stream{
		read:   make(chan protocol.MessageReadWriter, 1),
		write:  make(chan protocol.MessageReadWriter, 1),
		ctx:    ctx,
		cancel: cancel,
	}
	db.streams.m[streamID] = s
	db.streams.mu.Unlock()

	//errCh := make(chan error, 1)
	bufCh := make(chan protocol.MessageReadWriter, 1)
	db.wg.Add(1)
	//go db.readFromStream(conn, bufCh, errCh)

	defer func() {
		db.streams.mu.Lock()
		delete(db.streams.m, streamID)
		db.streams.mu.Unlock()
	}()

	rq := protocol.NewDMapMessage(protocol.OpStreamCreated)
	rq.SetExtra(protocol.StreamCreatedExtra{
		StreamID: streamID,
	})
	s.write <- rq
loop:
	for {
		select {
		case <-ctx.Done():
			// close method is called
			break loop
		case <-db.ctx.Done():
			// server is gone
			break loop
		case msg := <-s.write:
			msg.SetBuffer(conn)
			err := msg.Encode()
			if err != nil {
				db.errorResponse(w, err)
				return
			}
		case buf := <-bufCh:
			s.read <- buf
		}
	}
	w.SetStatus(protocol.StatusOK)
}
