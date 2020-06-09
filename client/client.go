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

/*Package client implements a Golang client to access an Olric cluster from outside. */
package client // import "github.com/buraksezer/olric/client"

import (
	"fmt"
	"time"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/transport"
	"github.com/buraksezer/olric/serializer"
	"github.com/buraksezer/olric/stats"
	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack"
)

// Client implements Go client of Olric Binary Protocol and its methods.
type Client struct {
	config     *Config
	client     *transport.Client
	serializer serializer.Serializer
}

// Config includes configuration parameters for the Client.
type Config struct {
	Addrs       []string
	Serializer  serializer.Serializer
	DialTimeout time.Duration
	KeepAlive   time.Duration
	MaxConn     int
}

// New returns a new Client instance. The second parameter is serializer, it can be nil.
func New(c *Config) (*Client, error) {
	if c == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if len(c.Addrs) == 0 {
		return nil, fmt.Errorf("addrs list cannot be empty")
	}
	if c.Serializer == nil {
		c.Serializer = serializer.NewGobSerializer()
	}
	if c.MaxConn == 0 {
		c.MaxConn = 1
	}
	cc := &transport.ClientConfig{
		Addrs:       c.Addrs,
		DialTimeout: c.DialTimeout,
		KeepAlive:   c.KeepAlive,
		MaxConn:     c.MaxConn,
	}
	return &Client{
		config:     c,
		client:     transport.NewClient(cc),
		serializer: c.Serializer,
	}, nil
}

// Ping sends a dummy protocol messsage to the given host. This is useful to
// measure RTT between hosts. It also can be used as aliveness check.
func (c *Client) Ping(addr string) error {
	req := &protocol.Message{}
	_, err := c.client.RequestTo(addr, protocol.OpPing, req)
	return err
}

// Stats exposes some useful metrics to monitor an Olric node.
func (c *Client) Stats(addr string) (stats.Stats, error) {
	s := stats.Stats{}
	req := &protocol.Message{}
	resp, err := c.client.RequestTo(addr, protocol.OpStats, req)
	if err != nil {
		return s, err
	}
	err = checkStatusCode(resp)
	if err != nil {
		return s, err
	}

	err = msgpack.Unmarshal(resp.Value, &s)
	if err != nil {
		return s, err
	}
	return s, nil
}

// Close cancels underlying context and cancels ongoing requests.
func (c *Client) Close() {
	c.client.Close()
}

// NewDMap creates and returns a new DMap instance to access DMaps on the cluster.
func (c *Client) NewDMap(name string) *DMap {
	return &DMap{
		Client: c,
		name:   name,
	}
}

func checkStatusCode(resp *protocol.Message) error {
	switch {
	case resp.Status == protocol.StatusOK:
		return nil
	case resp.Status == protocol.StatusInternalServerError:
		return errors.Wrap(olric.ErrInternalServerError, string(resp.Value))
	case resp.Status == protocol.StatusErrNoSuchLock:
		return olric.ErrNoSuchLock
	case resp.Status == protocol.StatusErrLockNotAcquired:
		return olric.ErrLockNotAcquired
	case resp.Status == protocol.StatusErrKeyNotFound:
		return olric.ErrKeyNotFound
	case resp.Status == protocol.StatusErrWriteQuorum:
		return olric.ErrWriteQuorum
	case resp.Status == protocol.StatusErrReadQuorum:
		return olric.ErrReadQuorum
	case resp.Status == protocol.StatusErrOperationTimeout:
		return olric.ErrOperationTimeout
	case resp.Status == protocol.StatusErrKeyFound:
		return olric.ErrKeyFound
	case resp.Status == protocol.StatusErrClusterQuorum:
		return olric.ErrClusterQuorum
	case resp.Status == protocol.StatusErrEndOfQuery:
		return olric.ErrEndOfQuery
	case resp.Status == protocol.StatusErrUnknownOperation:
		return olric.ErrUnknownOperation
	default:
		return fmt.Errorf("unknown status: %v", resp.Status)
	}
}

func (c *Client) unmarshalValue(rawval interface{}) (interface{}, error) {
	var value interface{}
	err := c.serializer.Unmarshal(rawval.([]byte), &value)
	if err != nil {
		return nil, err
	}
	if _, ok := value.(struct{}); ok {
		return nil, nil
	}
	return value, nil
}
