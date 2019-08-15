// Copyright 2018-2019 Burak Sezer
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

/*Package pipeline implements pipelining for Olric Binary Protocol. It enables to send multiple
commands to the server without waiting for the replies at all, and finally read the replies
in a single step.*/
package pipeline

import (
	"bytes"
	"fmt"
	"time"

	"github.com/buraksezer/olric"

	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/transport"
)

type Pipeline struct {
	buf        *bytes.Buffer
	client     *transport.Client
	serializer olric.Serializer
}

type Config struct {
	Addr        string
	DialTimeout time.Duration
	KeepAlive   time.Duration
	Serializer  olric.Serializer
}

func New(c *Config) (*Pipeline, error) {
	if c == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if len(c.Addr) == 0 {
		return nil, fmt.Errorf("Addr cannot be empty")
	}
	if c.Serializer == nil {
		c.Serializer = olric.NewGobSerializer()
	}

	// TODO: Check MaxConn
	cc := &transport.ClientConfig{
		Addrs:       []string{c.Addr},
		DialTimeout: c.DialTimeout,
		KeepAlive:   c.KeepAlive,
	}
	return &Pipeline{
		buf:        new(bytes.Buffer),
		client:     transport.NewClient(cc),
		serializer: c.Serializer,
	}, nil
}

func (p *Pipeline) Put(dmap, key string, value interface{}) error {
	data, err := p.serializer.Marshal(value)
	if err != nil {
		return err
	}
	m := &protocol.Message{
		Header: protocol.Header{
			Magic: protocol.MagicReq,
			Op: protocol.OpPut,
		},
		DMap:  dmap,
		Key:   key,
		Value: data,
	}
	return m.Write(p.buf)
}

func (p *Pipeline) PutEx(dmap, key string, value interface{}, timeout time.Duration) error {
	data, err := p.serializer.Marshal(value)
	if err != nil {
		return err
	}
	m := &protocol.Message{
		Header: protocol.Header{
			Magic: protocol.MagicReq,
			Op: protocol.OpPutEx,
		},
		DMap:  dmap,
		Key:   key,
		Extra: protocol.PutExExtra{TTL: timeout.Nanoseconds()},
		Value: data,
	}
	return m.Write(p.buf)
}

func (p *Pipeline) Flush() error {
	p.client.RequestTo()
	return nil
}