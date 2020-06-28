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

package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

const DMapMessageHeaderSize int64 = 11

const (
	MagicDMapReq MagicCode = 0xE4
	MagicDMapRes MagicCode = 0xE5
)

// Header defines a message header for both request and response.
type DMapMessageHeader struct {
	Op       OpCode     // 1
	DMapLen  uint16     // 2
	KeyLen   uint16     // 2
	ExtraLen uint8      // 1
	Status   StatusCode // 1
	BodyLen  uint32     // 4
}

type DMapMessage struct {
	Magic             MagicCode   // [0]
	DMapMessageHeader             // [1..10]
	Extra             interface{} // [11..(m-1)] Command specific extras (In)
	DMap              string      // [m..(n-1)] DMap (as needed, length in Header)
	Key               string      // [n..(x-1)] Key (as needed, length in Header)
	Value             []byte      // [x..y] Value (as needed, length in Header)
	conn              io.ReadWriteCloser
}

func NewDMapMessage(opcode OpCode, conn io.ReadWriteCloser) *DMapMessage {
	return &DMapMessage{
		Magic: MagicDMapReq,
		DMapMessageHeader: DMapMessageHeader{
			Op: opcode,
		},
		conn: conn,
	}
}

func NewDMapMessageRequest(conn io.ReadWriteCloser) *DMapMessage {
	return &DMapMessage{
		Magic:             MagicDMapReq,
		DMapMessageHeader: DMapMessageHeader{},
		conn:              conn,
	}
}

func NewDMapMessageResponse(conn io.ReadWriteCloser) *DMapMessage {
	return &DMapMessage{
		Magic:             MagicDMapRes,
		DMapMessageHeader: DMapMessageHeader{},
		conn:              conn,
	}
}

func (d *DMapMessage) SetStatus(code StatusCode) {
	d.Status = code
}

func (d *DMapMessage) SetValue(value []byte) {
	d.Value = value
}

func (d *DMapMessage) OpCode() OpCode {
	return d.Op
}

// TODO: Remove this after implementing StreamMessage type
func (d *DMapMessage) GetConn() io.ReadWriteCloser {
	return d.conn
}

func (d *DMapMessage) Decode() error {
	buf := pool.Get()
	defer pool.Put(buf)

	_, err := io.CopyN(buf, d.conn, DMapMessageHeaderSize)
	if err != nil {
		return filterNetworkErrors(err)
	}
	err = binary.Read(buf, binary.BigEndian, &d.DMapMessageHeader)
	if err != nil {
		return err
	}
	if d.Magic != MagicDMapReq && d.Magic != MagicDMapRes {
		return fmt.Errorf("invalid DMap message")
	}

	// Decode Key, DMap name and message extras here.
	_, err = io.CopyN(buf, d.conn, int64(d.BodyLen))
	if err != nil {
		return filterNetworkErrors(err)
	}

	if d.Magic == MagicDMapReq && d.ExtraLen > 0 {
		raw := buf.Next(int(d.ExtraLen))
		extra, err := loadExtras(raw, d.Op)
		if err != nil {
			return err
		}
		d.Extra = extra
	}
	d.DMap = string(buf.Next(int(d.DMapLen)))
	d.Key = string(buf.Next(int(d.KeyLen)))

	// There is no maximum value for BodyLen which includes ValueLen.
	// So our limit is available memory amount at the time of operation.
	// Please note that maximum partition size should not exceed 50MB for a smooth operation.
	vlen := int(d.BodyLen) - int(d.ExtraLen) - int(d.KeyLen) - int(d.DMapLen)
	if vlen != 0 {
		d.Value = make([]byte, vlen)
		copy(d.Value, buf.Next(vlen))
	}
	return nil
}

// Encode writes a protocol message to given TCP connection by encoding it.
func (d *DMapMessage) Encode() error {
	buf := pool.Get()
	defer pool.Put(buf)

	// Calculate lengths here
	d.DMapLen = uint16(len(d.DMap))
	d.KeyLen = uint16(len(d.Key))
	if d.Extra != nil {
		d.ExtraLen = uint8(binary.Size(d.Extra))
	}
	d.BodyLen = uint32(len(d.DMap) + len(d.Key) + len(d.Value) + int(d.ExtraLen))

	err := binary.Write(buf, binary.BigEndian, d.Magic)
	if err != nil {
		return err
	}

	err = binary.Write(buf, binary.BigEndian, d.DMapMessageHeader)
	if err != nil {
		return err
	}

	if d.Extra != nil {
		err = binary.Write(buf, binary.BigEndian, d.Extra)
		if err != nil {
			return err
		}
	}

	_, err = buf.WriteString(d.DMap)
	if err != nil {
		return err
	}

	_, err = buf.WriteString(d.Key)
	if err != nil {
		return err
	}

	_, err = buf.Write(d.Value)
	if err != nil {
		return err
	}

	_, err = buf.WriteTo(d.conn)
	return filterNetworkErrors(err)
}

// Error generates an error message for the request.
func (d *DMapMessage) Error(status StatusCode, err interface{}) *DMapMessage {
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

	return &DMapMessage{
		Magic:  MagicDMapRes,
		DMapMessageHeader: DMapMessageHeader{
			Op:     d.Op,
			Status: status,
		},
		Value: []byte(getError(err)),
	}
}

// Success generates a success message for the request.
func (d *DMapMessage) Success() *DMapMessage {
	return &DMapMessage{
		Magic:  MagicDMapRes,
		DMapMessageHeader: DMapMessageHeader{
			Op:     d.Op,
			Status: StatusOK,
		},
	}
}
