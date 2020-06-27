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
	"bytes"
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/protocol"
)

func mockCreateStream(ctx context.Context, _ string, read chan<- *protocol.Message, write <-chan *protocol.Message) error {
	rq := protocol.NewMessage(protocol.OpStreamCreated)
	rq.Extra = protocol.StreamCreatedExtra{
		StreamID: rand.Uint64(),
	}
	read <- rq
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-write:
			read <- msg
		}
	}
}

func TestStream_EchoListener(t *testing.T) {
	db, done, err := newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Fatalf("Expected nil. Got %v", serr)
		}
		<-done
	}()

	c, err := New(testConfig)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	// Mock transport.Client.CreateStream
	createStreamFunction = mockCreateStream

	l := newListener()
	_, listenerID, err := c.addStreamListener(l)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	req := protocol.NewStreamMessage(listenerID)
	req.DMap = "mydmap"
	req.Key = "mykey"
	req.Value = []byte("myvalue")
	l.write <- req

	select {
	case msg := <-l.read:
		if msg.DMap != "mydmap" {
			t.Fatalf("Expected DMap: %s. Got: %s", req.DMap, msg.DMap)
		}
		if msg.Key != "mykey" {
			t.Fatalf("Expected Key: %s. Got: %s", req.Key, msg.Key)
		}
		if !bytes.Equal(msg.Value, []byte("myvalue")) {
			t.Fatalf("Expected Value: %s. Got: %s", string(req.Value), msg.Value)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("No message received from listener")
	}
}

func TestStream_CreateNewStream(t *testing.T) {
	db, done, err := newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Fatalf("Expected nil. Got %v", serr)
		}
		<-done
	}()
	modifiedTestConfig := *testConfig
	modifiedTestConfig.MaxListenersPerStream = 1
	c, err := New(&modifiedTestConfig)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	// Mock transport.Client.CreateStream
	createStreamFunction = mockCreateStream

	l1 := newListener()
	_, _, err = c.addStreamListener(l1)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	l2 := newListener()
	_, _, err = c.addStreamListener(l2)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	c.streams.mu.RLock()
	defer c.streams.mu.RUnlock()
	if len(c.streams.m) != 2 {
		t.Fatalf("Expected stream count is 2. Got: %d", len(c.streams.m))
	}
}

func TestStream_MultipleListener(t *testing.T) {
	db, done, err := newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Fatalf("Expected nil. Got %v", serr)
		}
		<-done
	}()
	c, err := New(testConfig)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	// Mock transport.Client.CreateStream
	createStreamFunction = mockCreateStream

	listeners := make(map[uint64]*listener)

	l1 := newListener()
	_, listenerID1, err := c.addStreamListener(l1)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	listeners[listenerID1] = l1

	l2 := newListener()
	_, listenerID2, err := c.addStreamListener(l2)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	listeners[listenerID2] = l2

	c.streams.mu.RLock()
	defer c.streams.mu.RUnlock()
	if len(c.streams.m) != 1 {
		t.Fatalf("Expected stream count is 1. Got: %d", len(c.streams.m))
	}

	for id, l := range listeners {
		req := protocol.NewStreamMessage(id)
		req.DMap = "mydmap"
		req.Key = "mykey"
		req.Value = []byte("myvalue")

		l.write <- req

		select {
		case msg := <-l.read:
			if msg.DMap != "mydmap" {
				t.Fatalf("Expected DMap: %s. Got: %s", req.DMap, msg.DMap)
			}
			if msg.Key != "mykey" {
				t.Fatalf("Expected Key: %s. Got: %s", req.Key, msg.Key)
			}
			if !bytes.Equal(msg.Value, []byte("myvalue")) {
				t.Fatalf("Expected Value: %s. Got: %s", string(req.Value), msg.Value)
			}
			listenerID := msg.Extra.(protocol.StreamMessageExtra).ListenerID
			if listenerID != id {
				t.Fatalf("Expected ListenerID: %d. Got: %d", id, listenerID)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("No message received from listener")
		}
	}
}
