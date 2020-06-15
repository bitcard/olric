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
	"fmt"
	"io"
	"os"

	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/pool"
)

func readFromStream(conn io.Reader, bufCh chan<- *protocol.Message, errCh chan<- error) {
	for {
		var msg protocol.Message
		err := msg.Read(conn)
		if err != nil {
			errCh <- err
			return
		}
		bufCh <- &msg
	}
}

// CreateStream creates a new Stream connection which provides a bidirectional communication channel between Olric nodes and clients.
func (c *Client) CreateStream(ctx context.Context, addr string, read chan<- *protocol.Message, write <-chan *protocol.Message) error {
	cpool, err := c.getPool(addr)
	if err != nil {
		return err
	}

	req := protocol.NewMessage(protocol.OpCreateStream)
	conn, err := cpool.Get()
	if err != nil {
		return err
	}

	defer func() {
		// marks the connection not usable any more, to let the pool close it instead of returning it to pool.
		pc, _ := conn.(*pool.PoolConn)
		pc.MarkUnusable()
		if err = pc.Close(); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "[ERROR] Failed to close connection: %v", err)
		}
	}()

	// Create a new byte stream
	err = req.Write(conn)
	if err != nil {
		return err
	}

	errCh := make(chan error, 1)
	bufCh := make(chan *protocol.Message, 1)

	go readFromStream(conn, bufCh, errCh)
	for {
		select {
		case msg := <-write:
			err = msg.Write(conn)
			if err != nil {
				return err
			}
		case buf := <-bufCh:
			read <- buf
		case err = <-errCh:
			return err
		case <-ctx.Done():
			return nil
		}
	}
}
