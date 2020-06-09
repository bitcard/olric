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
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync"

	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/protocol"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

type TopicMessage struct {
	Message interface{}
}

// DTopic implements a distributed topic to deliver messages between clients and Olric nodes.
// Message order and delivery are not guaranteed.
type DTopic struct {
	name string
	db   *Olric
}

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
	ctx    context.Context
}

func newDTopic(ctx context.Context) *dtopic {
	return &dtopic{
		topics: &topics{m: make(map[string]*listeners)},
		ctx:    ctx,
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
		return fmt.Errorf("topic not found: %s: %w", topic, ErrInvalidArgument)
	}

	_, ok = l.m[regID]
	if !ok {
		return fmt.Errorf("listener not found: %s: %w", topic, ErrInvalidArgument)
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
		// there is no listener for this topic on this node.
		return fmt.Errorf("topic not found: %s: %w", topic, ErrInvalidArgument)
	}

	tm := TopicMessage{
		Message: msg,
	}

	var wg sync.WaitGroup
	num := int64(runtime.NumCPU())
	sem := semaphore.NewWeighted(num)

	for _, ll := range l.m {
		if err := sem.Acquire(d.ctx, 1); err != nil {
			return err
		}

		wg.Add(1)
		go func(f func(message TopicMessage)) {
			defer wg.Done()
			defer sem.Release(1)
			f(tm)
		}(ll.f)
	}
	wg.Wait()
	return nil
}

func (d *dtopic) destroy(topic string) {
	d.topics.mtx.Lock()
	defer d.topics.mtx.Unlock()
	delete(d.topics.m, topic)
}

// NewDTopic returns a new distributed topic instance.
func (db *Olric) NewDTopic(name string) (*DTopic, error) {
	// Check operation status first:
	//
	// * Checks member count in the cluster, returns ErrClusterQuorum if
	//   the quorum value cannot be satisfied,
	// * Checks bootstrapping status and awaits for a short period before
	//   returning ErrRequest timeout.
	if err := db.checkOperationStatus(); err != nil {
		return nil, err
	}
	return &DTopic{
		name: name,
		db:   db,
	}, nil
}

func (db *Olric) publishDTopicMessageOperation(req *protocol.Message) *protocol.Message {
	msg, err := db.unmarshalValue(req.Value)
	if err != nil {
		req.Error(protocol.StatusInternalServerError, err)
	}
	// req.DMap is a wrong name here. We will refactor the protocol and use a generic name.
	err = db.dtopic.dispatch(req.DMap, msg)
	if err != nil {
		req.Error(protocol.StatusInternalServerError, err)
	}
	return req.Success()
}

func (db *Olric) publishDTopicMessageToAddr(member discovery.Member, topic string, data []byte, sem *semaphore.Weighted) error {
	defer db.wg.Done()
	defer sem.Release(1)

	if hostCmp(member, db.this) {
		// Dispatch messages in this process.
		var msg interface{}
		if err := db.serializer.Unmarshal(data, &msg); err != nil {
			return err
		}
		err := db.dtopic.dispatch(topic, msg)
		if err != nil {
			db.log.V(6).Printf("[ERROR] Failed to dispatch message on this node: %v", err)
			if !errors.Is(err, ErrInvalidArgument) {
				return err
			}
			return nil
		}
	}

	// TODO: We need to find a better name for DMap in this struct.
	req := &protocol.Message{
		DMap:  topic,
		Value: data,
	}
	_, err := db.requestTo(member.String(), protocol.OpPublishDTopicMessage, req)
	if err != nil {
		db.log.V(2).Printf("[ERROR] Failed to publish message to %s: %v", member, err)
		return err
	}
	return nil
}

func (db *Olric) publishDTopicMessage(topic string, data []byte) error {
	db.members.mtx.RLock()
	defer db.members.mtx.RUnlock()

	// Propagate the message to the cluster in a parallel manner but
	// control concurrency. In order to prevent overloaded servers
	// because of network I/O, we use a semaphore.
	num := int64(runtime.NumCPU())
	sem := semaphore.NewWeighted(num)
	var g errgroup.Group
	for _, member := range db.members.m {
		if !db.isAlive() {
			return ErrServerGone
		}

		if err := sem.Acquire(db.ctx, 1); err != nil {
			db.log.V(3).Printf("[ERROR] Failed to acquire semaphore: %v", err)
			return err
		}

		db.wg.Add(1)
		member := member // https://golang.org/doc/faq#closures_and_goroutines
		g.Go(func() error {
			return db.publishDTopicMessageToAddr(member, topic, data, sem)
		})
	}
	// Wait blocks until all function calls from the Go method have returned,
	// then returns the first non-nil error (if any) from them.
	return g.Wait()
}

// Publish publishes the given message to listeners of the topic. Message order and delivery are not guaranteed.
func (dt *DTopic) Publish(msg interface{}) error {
	data, err := dt.db.serializer.Marshal(msg)
	if err != nil {
		return err
	}
	return dt.db.publishDTopicMessage(dt.name, data)
}

// AddListener adds a new listener for the topic. Returns a registration ID or an non-nil error.
// Registered functions are run by parallel.
func (dt *DTopic) AddListener(f func(TopicMessage)) (uint64, error) {
	return dt.db.dtopic.addListener(dt.name, f)
}

// RemoveListener removes a listener with the given regID.
func (dt *DTopic) RemoveListener(regID uint64) error {
	return dt.db.dtopic.removeListener(dt.name, regID)
}

func (db *Olric) destroyDTopicOperation(req *protocol.Message) *protocol.Message {
	// req.DMap is topic name in this context. This confusion will be fixed.
	db.dtopic.destroy(req.DMap)
	return req.Success()
}

func (db *Olric) destroyDTopicOnCluster(topic string) error {
	db.members.mtx.RLock()
	defer db.members.mtx.RUnlock()

	var g errgroup.Group
	for _, member := range db.members.m {
		if !db.isAlive() {
			return ErrServerGone
		}

		member := member // https://golang.org/doc/faq#closures_and_goroutines
		g.Go(func() error {
			// TODO: We need to find a better name for DMap in this struct.
			req := &protocol.Message{
				DMap: topic,
			}
			_, err := db.requestTo(member.String(), protocol.OpDestroyDTopic, req)
			if err != nil {
				db.log.V(2).Printf("[ERROR] Failed to call Destroy on %s, topic: %s : %v", member, topic, err)
				return err
			}
			return nil
		})
	}
	return g.Wait()
}

// Destroy removes all listeners for this topic on the cluster. If Publish function is called again after Destroy, the topic will be
// recreated.
func (dt *DTopic) Destroy() error {
	return dt.db.destroyDTopicOnCluster(dt.name)
}
