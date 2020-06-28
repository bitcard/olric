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
	MagicDMapReq MagicCode = 0xE2
	MagicDMapRes MagicCode = 0xE3
)

// Header defines a message header for both request and response.
type DMapMessageHeader struct {
	Op         OpCode     // 1
	DMapLen    uint16     // 2
	KeyLen     uint16     // 2
	ExtraLen   uint8      // 1
	StatusCode StatusCode // 1
	BodyLen    uint32     // 4
}

type DMapMessage struct {
	magic             MagicCode   // [0]
	DMapMessageHeader             // [1..10]
	extra             interface{} // [11..(m-1)] Command specific extras (In)
	dmap              string      // [m..(n-1)] dmap (as needed, length in Header)
	key               string      // [n..(x-1)] key (as needed, length in Header)
	value             []byte      // [x..y] value (as needed, length in Header)
	conn              io.ReadWriteCloser
}

func NewDMapMessage(opcode OpCode) *DMapMessage {
	return &DMapMessage{
		magic: MagicDMapReq,
		DMapMessageHeader: DMapMessageHeader{
			Op: opcode,
		},
	}
}

func NewDMapMessageFromRequest(conn io.ReadWriteCloser) *DMapMessage {
	return &DMapMessage{
		magic:             MagicDMapReq,
		DMapMessageHeader: DMapMessageHeader{},
		conn:              conn,
	}
}

func (d *DMapMessage) Response() MessageReadWriter {
	return &DMapMessage{
		magic: MagicDMapRes,
		DMapMessageHeader: DMapMessageHeader{
			Op: d.Op,
		},
		conn: d.conn,
	}
}

func (d *DMapMessage) SetStatus(code StatusCode) {
	d.StatusCode = code
}

func (d *DMapMessage) Status() StatusCode {
	return d.StatusCode
}

func (d *DMapMessage) SetValue(value []byte) {
	d.value = value
}

func (d *DMapMessage) Value() []byte {
	return d.value
}

func (d *DMapMessage) OpCode() OpCode {
	return d.Op
}

func (d *DMapMessage) SetConn(conn io.ReadWriteCloser) {
	d.conn = conn
}

func (d *DMapMessage) Conn() io.ReadWriteCloser {
	return d.conn
}

func (d *DMapMessage) SetDMap(dmap string) {
	d.dmap = dmap
}

func (d *DMapMessage) DMap() string {
	return d.dmap
}

func (d *DMapMessage) SetKey(key string) {
	d.key = key
}

func (d *DMapMessage) Key() string {
	return d.key
}

func (d *DMapMessage) SetExtra(extra interface{}) {
	d.extra = extra
}

func (d *DMapMessage) Extra() interface{} {
	return d.extra
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
	if d.magic != MagicDMapReq && d.magic != MagicDMapRes {
		return fmt.Errorf("invalid dmap message")
	}

	// Decode key, dmap name and message extras here.
	_, err = io.CopyN(buf, d.conn, int64(d.BodyLen))
	if err != nil {
		return filterNetworkErrors(err)
	}

	if d.magic == MagicDMapReq && d.ExtraLen > 0 {
		raw := buf.Next(int(d.ExtraLen))
		extra, err := loadExtras(raw, d.Op)
		if err != nil {
			return err
		}
		d.extra = extra
	}
	d.dmap = string(buf.Next(int(d.DMapLen)))
	d.key = string(buf.Next(int(d.KeyLen)))

	// There is no maximum value for BodyLen which includes ValueLen.
	// So our limit is available memory amount at the time of operation.
	// Please note that maximum partition size should not exceed 50MB for a smooth operation.
	vlen := int(d.BodyLen) - int(d.ExtraLen) - int(d.KeyLen) - int(d.DMapLen)
	if vlen != 0 {
		d.value = make([]byte, vlen)
		copy(d.value, buf.Next(vlen))
	}
	return nil
}

// Encode writes a protocol message to given TCP connection by encoding it.
func (d *DMapMessage) Encode() error {
	buf := pool.Get()
	defer pool.Put(buf)

	// Calculate lengths here
	d.DMapLen = uint16(len(d.dmap))
	d.KeyLen = uint16(len(d.key))
	if d.extra != nil {
		d.ExtraLen = uint8(binary.Size(d.extra))
	}
	d.BodyLen = uint32(len(d.dmap) + len(d.key) + len(d.value) + int(d.ExtraLen))

	err := binary.Write(buf, binary.BigEndian, d.magic)
	if err != nil {
		return err
	}

	err = binary.Write(buf, binary.BigEndian, d.DMapMessageHeader)
	if err != nil {
		return err
	}

	if d.extra != nil {
		err = binary.Write(buf, binary.BigEndian, d.extra)
		if err != nil {
			return err
		}
	}

	_, err = buf.WriteString(d.dmap)
	if err != nil {
		return err
	}

	_, err = buf.WriteString(d.key)
	if err != nil {
		return err
	}

	_, err = buf.Write(d.value)
	if err != nil {
		return err
	}

	_, err = buf.WriteTo(d.conn)
	return filterNetworkErrors(err)
}
