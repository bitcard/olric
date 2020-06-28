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
	"bytes"
	"io"

	"github.com/buraksezer/olric/internal/protocol"
)

type pipelineConn struct {
	*bytes.Buffer
}

func (p *pipelineConn) Close() error {
	return nil
}

func newPipelineConn(data []byte) *pipelineConn {
	return &pipelineConn{
		Buffer: bytes.NewBuffer(data),
	}
}

func (db *Olric) pipelineOperation(w, r protocol.MessageReadWriter) {
	req := r.(*protocol.DMapMessage)
	conn := newPipelineConn(req.Value())
	// Decode the pipelined messages into an in-memory buffer.
	for {
		preq := protocol.NewDMapMessageFromRequest(conn)
		err := preq.Decode()
		if err == io.EOF {
			// It's done. The last message has been read.
			break
		}

		// Return an error message in pipelined response.
		if err != nil {
			preq.SetStatus(protocol.StatusInternalServerError)
			preq.SetValue([]byte(err.Error()))
			continue
		}
		f, ok := db.operations[preq.Op]
		if !ok {
			preq.SetStatus(protocol.StatusInternalServerError)
			preq.SetValue([]byte(ErrUnknownOperation.Error()))
			continue
		}

		presp := preq.Response()
		// Call its function to prepare a response.
		f(presp, preq)
		err = presp.Encode()
		if err != nil {
			db.errorResponse(w, err)
			return
		}
	}

	// Create a success response and assign pipelined responses as value.
	w.SetStatus(protocol.StatusOK)
	w.SetValue(conn.Bytes())
}
