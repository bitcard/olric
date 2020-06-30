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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strings"

	"github.com/buraksezer/olric/internal/bufpool"
	"github.com/pkg/errors"
)

// pool is good for recycling memory while reading messages from the socket.
var pool = bufpool.New()

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

	SetBuffer(*bytes.Buffer)

	Buffer() *bytes.Buffer

	SetExtra(interface{})

	Extra() interface{}

	Response() MessageReadWriter
}

const headerSize int64 = 5

type Header struct {
	Magic         MagicCode // 1 byte
	MessageLength uint32    // 4 bytes
}

func readHeader(conn io.ReadWriteCloser) (Header, error) {
	buf := pool.Get()
	defer pool.Put(buf)

	var header Header
	_, err := io.CopyN(buf, conn, headerSize)
	if err != nil {
		return header, filterNetworkErrors(err)
	}

	err = binary.Read(buf, binary.BigEndian, &header)
	if err != nil {
		return header, err
	}
	return header, nil
}

func ReadMessage(src io.ReadWriteCloser, dst *bytes.Buffer) (Header, error) {
	header, err := readHeader(src)
	if err != nil {
		return Header{}, err
	}

	length := int64(header.MessageLength)
	nr, err := io.CopyN(dst, src, length)
	if err != nil {
		return Header{}, err
	}
	if nr != length {
		return Header{}, fmt.Errorf("byte count mismatch")
	}
	return header, nil
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
