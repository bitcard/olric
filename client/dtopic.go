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
	"github.com/vmihailenco/msgpack"
	"sync"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/internal/protocol"
)

type DTopic struct {
	*Client
	name string

	mu        sync.Mutex
	listeners map[uint64]struct{}
}

func (c *Client) NewDTopic(name string) *DTopic {
	return &DTopic{
		Client:    c,
		name:      name,
		listeners: make(map[uint64]struct{}),
	}
}

func (dt *DTopic) Publish(msg interface{}) error {
	value, err := dt.serializer.Marshal(msg)
	if err != nil {
		return err
	}
	req := protocol.NewDMapMessage(protocol.OpDTopicPublish)
	req.SetDMap(dt.name)
	req.SetValue(value)
	resp, err := dt.client.Request(req)
	if err != nil {
		return err
	}
	return checkStatusCode(resp)
}

func (dt *DTopic) AddListener(f func(olric.DTopicMessage)) (uint64, error) {
	l := newListener()
	streamID, listenerID, err := dt.addStreamListener(l)
	if err != nil {
		return 0, err
	}

	req := protocol.NewDMapMessage(protocol.OpDTopicAddListener)
	req.SetDMap(dt.name)
	req.SetExtra(protocol.DTopicAddListenerExtra{
		ListenerID: listenerID,
		StreamID:   streamID,
	})
	resp, err := dt.client.Request(req)
	if err != nil {
		_ = dt.removeStreamListener(listenerID)
		return 0, err
	}
	err = checkStatusCode(resp)
	if err != nil {
		_ = dt.removeStreamListener(listenerID)
		return 0, err
	}

	dt.wg.Add(1)
	go func(l *listener) {
		defer dt.wg.Done()
		select {
		case <-l.ctx.Done():
			return
		case req := <-l.read:
			var msg olric.DTopicMessage
			err = msgpack.Unmarshal(req.Value, &msg)
			if err != nil {
				// TODO: Log this
			}
			f(msg)
		}
	}(l)
	// This DTopic needs its listeners.
	dt.mu.Lock()
	dt.listeners[listenerID] = struct{}{}
	dt.mu.Unlock()
	return listenerID, nil
}

func (dt *DTopic) RemoveListener(listenerID uint64) error {
	req := protocol.NewDMapMessage(protocol.OpDTopicRemoveListener)
	req.SetDMap(dt.name)
	req.SetExtra(protocol.DTopicAddListenerExtra{
		ListenerID: listenerID,
	})
	resp, err := dt.client.Request(req)
	if err != nil {
		return err
	}
	err = checkStatusCode(resp)
	if err != nil {
		return err
	}
	err = dt.removeStreamListener(listenerID)
	if err != nil {
		return err
	}

	dt.mu.Lock()
	delete(dt.listeners, listenerID)
	dt.mu.Unlock()
	return nil
}

func (dt *DTopic) Destroy() error {
	req := protocol.NewDMapMessage(protocol.OpDTopicDestroy)
	req.SetDMap(dt.name)
	resp, err := dt.client.Request(req)
	if err != nil {
		return err
	}
	err = checkStatusCode(resp)
	if err != nil {
		return err
	}

	// Remove local listeners
	dt.mu.Lock()
	defer dt.mu.Unlock()
	for listenerID, _ := range dt.listeners {
		err = dt.removeStreamListener(listenerID)
		if err != nil {
			// TODO: Log this
			continue
		}
	}
	// I don't know that it's good to remove the map items while iterating over the same map.
	for listenerID, _ := range dt.listeners {
		delete(dt.listeners, listenerID)
	}
	return nil
}
