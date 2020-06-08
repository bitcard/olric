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

package olric

import (
	"fmt"
	"math/rand"
	"sync"
)

type listener struct {
	f func(message TopicMessage)
}

type listeners struct {
	m map[uint64]*listener
}

type topics struct {
	mtx sync.RWMutex
	m   map[string]*listeners
}

type dtopic struct {
	topics *topics
}

func newDTopic() *dtopic {
	return &dtopic{
		topics: &topics{m: make(map[string]*listeners)},
	}
}

func (d *dtopic) addListener(topic string, f func(TopicMessage)) (uint64, error) {
	d.topics.mtx.Lock()
	defer d.topics.mtx.Unlock()

	regID := rand.Uint64()
	l, ok := d.topics.m[topic]
	if ok {
		l.m[regID] = &listener{f: f}
	} else {
		d.topics.m[topic] = &listeners{
			m: make(map[uint64]*listener),
		}
		d.topics.m[topic].m[regID] = &listener{f: f}
	}
	return regID, nil
}

func (d *dtopic) removeListener(topic string, regID uint64) error {
	d.topics.mtx.Lock()
	defer d.topics.mtx.Unlock()

	l, ok := d.topics.m[topic]
	if !ok {
		return fmt.Errorf("topic not found: %s", topic)
	}

	_, ok = l.m[regID]
	if !ok {
		return fmt.Errorf("listener not found: %s", topic)
	}

	delete(l.m, regID)
	if len(l.m) == 0 {
		delete(d.topics.m, topic)
	}
	return nil
}

func (d *dtopic) dispatch(topic string, msg interface{}) error {
	d.topics.mtx.RLock()
	defer d.topics.mtx.RUnlock()

	l, ok := d.topics.m[topic]
	if !ok {
		return fmt.Errorf("topic not found: %s", topic)
	}

	tm := TopicMessage{
		Message: msg,
	}
	for _, ll := range l.m {
		ll.f(tm)
	}
	return nil
}
