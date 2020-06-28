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

/*Package protocol implements Olric Binary Protocol.*/
package protocol

import (
	"encoding/binary"
	"io"
	"strings"

	"github.com/buraksezer/olric/internal/bufpool"
	"github.com/pkg/errors"
)

// pool is good for recycling memory while reading messages from the socket.
var pool = bufpool.New()

const headerSize int64 = 12

// MagicCode defines an unique code to distinguish a request message from a response message in Olric Binary Protocol.
type MagicCode uint8

const (
	// MagicReq defines an magic code for REQUEST in Olric Binary Protocol
	MagicReq MagicCode = 0xE2

	// MagicRes defines an magic code for RESPONSE in Olric Binary Protocol
	MagicRes MagicCode = 0xE3
)

type MessageReadWriter interface {
	Encode() error

	Decode() error

	SetStatus(StatusCode)

	Status() StatusCode

	SetValue([]byte)

	Value() []byte

	OpCode() OpCode

	SetConn(io.ReadWriteCloser)

	Conn() io.ReadWriteCloser

	SetExtra(interface{})

	Extra() interface{}

	Response() MessageReadWriter
}

func ExtractMagic(conn io.ReadWriteCloser) (MagicCode, error) {
	buf := pool.Get()
	defer pool.Put(buf)

	_, err := io.CopyN(buf, conn, 1)
	if err != nil {
		return 0, filterNetworkErrors(err)
	}

	var code MagicCode
	err = binary.Read(buf, binary.BigEndian, &code)
	if err != nil {
		return 0, err
	}
	return code, nil
}

// Header defines a message header for both request and response.
type Header struct {
	Magic    MagicCode  // 1
	Op       OpCode     // 1
	DMapLen  uint16     // 2
	KeyLen   uint16     // 2
	ExtraLen uint8      // 1
	Status   StatusCode // 1
	BodyLen  uint32     // 4
}

func NewStreamMessage(listenerID uint64) *DMapMessage {
	m := NewDMapMessage(OpStreamMessage)
	m.SetExtra(StreamMessageExtra{
		ListenerID: listenerID,
	})
	return m
}

// ErrConnClosed means that the underlying TCP connection has been closed
// by the client or operating system.
var ErrConnClosed = errors.New("connection closed")

func filterNetworkErrors(err error) error {
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "use of closed network connection") {
		return ErrConnClosed
	}
	return err
}