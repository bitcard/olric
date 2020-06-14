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

package client

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/buraksezer/olric/internal/protocol"
)

var errTooManyListener = errors.New("stream has too many listeners")

const maxListeners = 1024

type listener struct {
}

// streams provides a bidirectional communication channel between Olric nodes and clients. It can also be used
// for node-to-node communication.
type stream struct {
	mu sync.RWMutex

	listeners map[uint64]*listener
	read      chan *protocol.Message
	write     chan *protocol.Message
	errCh     chan error
	ctx       context.Context
	cancel    context.CancelFunc
}

// streams maps StreamIDs to streams
type streams struct {
	mu sync.RWMutex

	m map[uint64]*stream
}

func (c *Client) listenStream(s *stream) {
	defer c.wg.Done()

	for {
		select {
		case <-s.ctx.Done():
			return
		}
	}
}

func (c *Client) createStream() (*stream, error) {
	ctx, cancel := context.WithCancel(context.Background())
	s := &stream{
		read:   make(chan *protocol.Message, 1),
		write:  make(chan *protocol.Message, 1),
		errCh:  make(chan error, 1),
		ctx:    ctx,
		cancel: cancel,
	}

	// Pick a random addr to dial
	if len(c.config.Addrs) == 0 {
		return nil, fmt.Errorf("no addr found to dial")
	}
	idx := rand.Intn(len(c.config.Addrs))
	addr := c.config.Addrs[idx]

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		s.errCh <- c.client.CreateStream(ctx, addr, s.read, s.write)
	}()

	select {
	case err := <-s.errCh:
		return nil, err
	case msg := <-s.read:
		if msg.Op != protocol.OpStreamCreated {
			return nil, fmt.Errorf("server returned OpCode: %d instead of %d", msg.Op, protocol.OpStreamCreated)
		}

		streamID := msg.Extra.(protocol.StreamCreatedExtra).StreamID
		c.streams.m[streamID] = s
		c.wg.Add(1)
		go c.listenStream(s)
	case <-time.After(5 * time.Second):
		return nil, fmt.Errorf("streamID could not be retrieved")
	}
	return s, nil
}

func (c *Client) addStreamListener(l *listener) (uint64, error) {
	c.streams.mu.Lock()
	defer c.streams.mu.Unlock()

	add := func(s *stream) (uint64, error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		if len(s.listeners) >= maxListeners {
			return 0, errTooManyListener
		}
		listenerID := rand.Uint64()
		s.listeners[listenerID] = l
		return listenerID, nil
	}

	for _, s := range c.streams.m {
		listenerID, err := add(s)
		if err == errTooManyListener {
			continue
		}
		return listenerID, err
	}

	s, err := c.createStream()
	if err != nil {
		return 0, err
	}
	return add(s)
}
