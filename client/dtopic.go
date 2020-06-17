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
	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/internal/protocol"
)

type DTopic struct {
	*Client
	name string
}

func (c *Client) NewDTopic(name string) *DTopic {
	return &DTopic{
		Client: c,
		name:   name,
	}
}

func (dt *DTopic) Publish(msg interface{}) error {
	data, err := dt.serializer.Marshal(msg)
	if err != nil {
		return err
	}
	m := &protocol.Message{
		DMap:  dt.name,
		Value: data,
	}
	resp, err := dt.client.Request(protocol.OpDTopicPublish, m)
	if err != nil {
		return err
	}
	return checkStatusCode(resp)
}

func (dt *DTopic) AddListener(f func(olric.DTopicMessage)) (uint64, error) {
	l := &listener{
		read: make(chan *protocol.Message, 1),
		write: make(chan *protocol.Message, 1),
	}
	streamID, listenerID, err := dt.addStreamListener(l)
	if err != nil {
		return 0, err
	}
	// TODO: Remove this listener if any of the steps return an error.

	m := &protocol.Message{
		DMap: dt.name,
		Extra: protocol.DTopicAddListenerExtra{
			ListenerID: listenerID,
			StreamID: streamID,
		},
	}
	resp, err := dt.client.Request(protocol.OpDTopicAddListener, m)
	if err != nil {
		return 0, err
	}
	err = checkStatusCode(resp)
	if err != nil {
		return 0, err
	}
	go func(l *listener) {
		// TODO: Graceful shutdown
		select {
		case req := <-l.read:
			msg, err := dt.unmarshalValue(req.Value)
			if err != nil {
				// TODO: Log this
			}
			f(msg.(olric.DTopicMessage))
		}
	}(l)
	return listenerID, nil
}

/*
func (d *DTopic) RemoveListener(regID int64) error {
	m := &protocol.Message{
		DMap: d.name,
	}
	resp, err := d.client.Request(protocol.OpDTopicRemoveListener, m)
	if err != nil {
		return err
	}
	return checkStatusCode(resp)
}

func (d *DTopic) Destroy() error {
	m := &protocol.Message{
		DMap: d.name,
	}
	resp, err := d.client.Request(protocol.OpDTopicDestroy, m)
	if err != nil {
		return err
	}
	return checkStatusCode(resp)
}
*/
