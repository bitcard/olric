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
	"fmt"
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

// Message defines a protocol message in Olric Binary Protocol.
type Message struct {
	Header             // [0..10]
	Extra  interface{} // [11..(m-1)] Command specific extras (In)
	DMap   string      // [m..(n-1)] DMap (as needed, length in Header)
	Key    string      // [n..(x-1)] Key (as needed, length in Header)
	Value  []byte      // [x..y] Value (as needed, length in Header)
	conn   io.ReadWriteCloser
}

func (m *Message) SetConn(conn io.ReadWriteCloser) {
	m.conn = conn
}

func (m *Message) GetConn() (io.ReadWriteCloser, error) {
	if m.conn == nil {
		return nil, fmt.Errorf("conn is nil")
	}
	return m.conn, nil
}

func NewMessage(opcode OpCode) *Message {
	return &Message{
		Header: Header{
			Magic: MagicReq,
			Op:    opcode,
		},
	}
}

func NewStreamMessage(listenerID uint64) *Message {
	m := NewMessage(OpStreamMessage)
	m.Extra = StreamMessageExtra{
		ListenerID: listenerID,
	}
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

// Decode reads a whole protocol message(including the value) from given connection
// by decoding it.
func (m *Message) Decode(conn io.Reader) error {
	buf := pool.Get()
	defer pool.Put(buf)

	_, err := io.CopyN(buf, conn, headerSize)
	if err != nil {
		return filterNetworkErrors(err)
	}
	err = binary.Read(buf, binary.BigEndian, &m.Header)
	if err != nil {
		return err
	}
	if m.Magic != MagicReq && m.Magic != MagicRes {
		return fmt.Errorf("invalid message")
	}

	// Decode Key, DMap name and message extras here.
	_, err = io.CopyN(buf, conn, int64(m.BodyLen))
	if err != nil {
		return filterNetworkErrors(err)
	}

	if m.Magic == MagicReq && m.ExtraLen > 0 {
		raw := buf.Next(int(m.ExtraLen))
		extra, err := loadExtras(raw, m.Op)
		if err != nil {
			return err
		}
		m.Extra = extra
	}
	m.DMap = string(buf.Next(int(m.DMapLen)))
	m.Key = string(buf.Next(int(m.KeyLen)))

	// There is no maximum value for BodyLen which includes ValueLen.
	// So our limit is available memory amount at the time of operation.
	// Please note that maximum partition size should not exceed 50MB for a smooth operation.
	vlen := int(m.BodyLen) - int(m.ExtraLen) - int(m.KeyLen) - int(m.DMapLen)
	if vlen != 0 {
		m.Value = make([]byte, vlen)
		copy(m.Value, buf.Next(vlen))
	}
	return nil
}

// Encode writes a protocol message to given TCP connection by encoding it.
func (m *Message) Encode(conn io.Writer) error {
	buf := pool.Get()
	defer pool.Put(buf)

	m.DMapLen = uint16(len(m.DMap))
	m.KeyLen = uint16(len(m.Key))
	if m.Extra != nil {
		m.ExtraLen = uint8(binary.Size(m.Extra))
	}
	m.BodyLen = uint32(len(m.DMap) + len(m.Key) + len(m.Value) + int(m.ExtraLen))
	err := binary.Write(buf, binary.BigEndian, m.Header)
	if err != nil {
		return err
	}

	if m.Extra != nil {
		err = binary.Write(buf, binary.BigEndian, m.Extra)
		if err != nil {
			return err
		}
	}

	_, err = buf.WriteString(m.DMap)
	if err != nil {
		return err
	}

	_, err = buf.WriteString(m.Key)
	if err != nil {
		return err
	}

	_, err = buf.Write(m.Value)
	if err != nil {
		return err
	}

	_, err = buf.WriteTo(conn)
	return filterNetworkErrors(err)
}

// Error generates an error message for the request.
func (m *Message) Error(status StatusCode, err interface{}) *Message {
	getError := func(err interface{}) string {
		switch val := err.(type) {
		case string:
			return val
		case error:
			return val.Error()
		default:
			return ""
		}
	}

	return &Message{
		Header: Header{
			Magic:  MagicRes,
			Op:     m.Op,
			Status: status,
		},
		Value: []byte(getError(err)),
	}
}

// Success generates a success message for the request.
func (m *Message) Success() *Message {
	return &Message{
		Header: Header{
			Magic:  MagicRes,
			Op:     m.Op,
			Status: StatusOK,
		},
	}
}
