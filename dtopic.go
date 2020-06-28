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
	"github.com/vmihailenco/msgpack"
	"math/rand"
	"runtime"
	"sync"
	"time"

	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/protocol"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

var errListenerIDCollision = errors.New("given listenerID already exists")

// DTopicMessage denotes a distributed topic message.
type DTopicMessage struct {
	Message       interface{}
	PublisherAddr string
	PublishedAt   int64
}

// DTopic implements a distributed topic to deliver messages between clients and Olric nodes.
// Message order and delivery are not guaranteed.
type DTopic struct {
	name string
	db   *Olric
}

type listener struct {
	f func(message DTopicMessage)
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

func (dt *dtopic) _addListener(listenerID uint64, topic string, f func(DTopicMessage)) error {
	dt.topics.mtx.Lock()
	defer dt.topics.mtx.Unlock()

	l, ok := dt.topics.m[topic]
	if ok {
		if _, ok = l.m[listenerID]; ok {
			return errListenerIDCollision
		}
		l.m[listenerID] = &listener{f: f}
	} else {
		dt.topics.m[topic] = &listeners{
			m: make(map[uint64]*listener),
		}
		dt.topics.m[topic].m[listenerID] = &listener{f: f}
	}
	return nil
}

func (dt *dtopic) addListener(topic string, f func(DTopicMessage)) (uint64, error) {
	listenerID := rand.Uint64()
	err := dt._addListener(listenerID, topic, f)
	if err != nil {
		return 0, err
	}
	return listenerID, nil
}

func (dt *dtopic) addRemoteListener(listenerID uint64, topic string, f func(DTopicMessage)) error {
	return dt._addListener(listenerID, topic, f)
}

func (dt *dtopic) removeListener(topic string, listenerID uint64) error {
	dt.topics.mtx.Lock()
	defer dt.topics.mtx.Unlock()

	l, ok := dt.topics.m[topic]
	if !ok {
		return fmt.Errorf("topic not found: %s: %w", topic, ErrInvalidArgument)
	}

	_, ok = l.m[listenerID]
	if !ok {
		return fmt.Errorf("listener not found: %s: %w", topic, ErrInvalidArgument)
	}

	delete(l.m, listenerID)
	if len(l.m) == 0 {
		delete(dt.topics.m, topic)
	}
	return nil
}

func (dt *dtopic) dispatch(topic string, msg *DTopicMessage) error {
	dt.topics.mtx.RLock()
	defer dt.topics.mtx.RUnlock()

	l, ok := dt.topics.m[topic]
	if !ok {
		// there is no listener for this topic on this node.
		return fmt.Errorf("topic not found: %s: %w", topic, ErrInvalidArgument)
	}

	var wg sync.WaitGroup
	num := int64(runtime.NumCPU())
	sem := semaphore.NewWeighted(num)

	for _, ll := range l.m {
		if err := sem.Acquire(dt.ctx, 1); err != nil {
			return err
		}

		wg.Add(1)
		// Dereference the pointer and make a copy of DTopicMessage for every listener.
		go func(f func(message DTopicMessage)) {
			defer wg.Done()
			defer sem.Release(1)
			f(*msg)
		}(ll.f)
	}
	wg.Wait()
	return nil
}

func (dt *dtopic) destroy(topic string) {
	dt.topics.mtx.Lock()
	defer dt.topics.mtx.Unlock()
	delete(dt.topics.m, topic)
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

func (db *Olric) publishDTopicMessageOperation(w, r protocol.MessageReadWriter) {
	req := r.(*protocol.DMapMessage)
	rawmsg, err := db.unmarshalValue(req.Value())
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	msg, ok := rawmsg.(DTopicMessage)
	if !ok {
		db.errorResponse(w, fmt.Errorf("invalid distributed topic message received"))
		return
	}

	// req.DMap() is a wrong name here. We will refactor the protocol and use a generic name.
	err = db.dtopic.dispatch(req.DMap(), &msg)
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}

func (db *Olric) publishDTopicMessageToAddr(member discovery.Member, topic string, msg *DTopicMessage, sem *semaphore.Weighted) error {
	defer db.wg.Done()
	defer sem.Release(1)

	if hostCmp(member, db.this) {
		// Dispatch messages in this process.
		err := db.dtopic.dispatch(topic, msg)
		if err != nil {
			db.log.V(6).Printf("[ERROR] Failed to dispatch message on this node: %v", err)
			if !errors.Is(err, ErrInvalidArgument) {
				return err
			}
			return nil
		}
		return nil
	}
	data, err := msgpack.Marshal(*msg)
	if err != nil {
		return err
	}
	// TODO: DTopicMessage
	req := protocol.NewDMapMessage(protocol.OpPublishDTopicMessage)
	req.SetDMap(topic)
	req.SetValue(data)
	_, err = db.requestTo(member.String(), req)
	if err != nil {
		db.log.V(2).Printf("[ERROR] Failed to publish message to %s: %v", member, err)
		return err
	}
	return nil
}

func (db *Olric) publishDTopicMessage(topic string, msg *DTopicMessage) error {
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
			return db.publishDTopicMessageToAddr(member, topic, msg, sem)
		})
	}
	// Wait blocks until all function calls from the Go method have returned,
	// then returns the first non-nil error (if any) from them.
	return g.Wait()
}

func (db *Olric) exDTopicAddListenerOperation(w, r protocol.MessageReadWriter) {
	req := r.(*protocol.DMapMessage)
	name := req.DMap()
	streamID := req.Extra().(protocol.DTopicAddListenerExtra).StreamID
	db.streams.mu.RLock()
	_, ok := db.streams.m[streamID]
	db.streams.mu.RUnlock()
	if !ok {
		err := fmt.Errorf("%w: StreamID could not be found", ErrInvalidArgument)
		db.errorResponse(w, err)
		return
	}

	// Local listener
	listenerID := req.Extra().(protocol.DTopicAddListenerExtra).ListenerID

	f := func(msg DTopicMessage) {
		db.streams.mu.RLock()
		s, ok := db.streams.m[streamID]
		db.streams.mu.RUnlock()
		if !ok {
			db.log.V(2).Printf("[ERROR] Stream could not be found with the given StreamID: %d", streamID)
			err := db.dtopic.removeListener(name, listenerID)
			if err != nil {
				db.log.V(2).Printf("[ERROR] Listener could not be removed with ListenerID: %d: %v", listenerID, err)
			}
			return
		}
		value, err := msgpack.Marshal(msg)
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to serialize DTopicMessage: %v", err)
			return
		}
		m := protocol.NewStreamMessage(listenerID)
		m.SetDMap(name)
		m.SetValue(value)
		s.write <- m
	}
	err := db.dtopic.addRemoteListener(listenerID, name, f)
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}

func (db *Olric) exDTopicPublishOperation(w, r protocol.MessageReadWriter) {
	req := r.(*protocol.DMapMessage)
	msg, err := db.unmarshalValue(req.Value())
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	tm := &DTopicMessage{
		Message:       msg,
		PublisherAddr: "",
		PublishedAt:   time.Now().UnixNano(),
	}
	err = db.publishDTopicMessage(req.DMap(), tm)
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}

// Publish publishes the given message to listeners of the topic. Message order and delivery are not guaranteed.
func (dt *DTopic) Publish(msg interface{}) error {
	tm := &DTopicMessage{
		Message:       msg,
		PublisherAddr: dt.db.this.String(),
		PublishedAt:   time.Now().UnixNano(),
	}
	return dt.db.publishDTopicMessage(dt.name, tm)
}

// AddListener adds a new listener for the topic. Returns a registration ID or an non-nil error.
// Registered functions are run by parallel.
func (dt *DTopic) AddListener(f func(DTopicMessage)) (uint64, error) {
	return dt.db.dtopic.addListener(dt.name, f)
}

func (db *Olric) exDTopicRemoveListenerOperation(w, r protocol.MessageReadWriter) {
	req := r.(*protocol.DMapMessage)
	extra, ok := req.Extra().(protocol.DTopicRemoveListenerExtra)
	if !ok {
		db.errorResponse(w, fmt.Errorf("%w: wrong extra type", ErrInvalidArgument))
		return
	}
	err := db.dtopic.removeListener(req.DMap(), extra.ListenerID)
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}

// RemoveListener removes a listener with the given listenerID.
func (dt *DTopic) RemoveListener(listenerID uint64) error {
	return dt.db.dtopic.removeListener(dt.name, listenerID)
}

func (db *Olric) destroyDTopicOperation(w, r protocol.MessageReadWriter) {
	req := r.(*protocol.DMapMessage)
	// req.DMap() is topic name in this context. This confusion will be fixed.
	db.dtopic.destroy(req.DMap())
	w.SetStatus(protocol.StatusOK)
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
			// TODO: DTopicMessage
			req := protocol.NewDMapMessage(protocol.OpDestroyDTopic)
			req.SetDMap(topic)
			_, err := db.requestTo(member.String(), req)
			if err != nil {
				db.log.V(2).Printf("[ERROR] Failed to call Destroy on %s, topic: %s : %v", member, topic, err)
				return err
			}
			return nil
		})
	}
	return g.Wait()
}

func (db *Olric) exDTopicDestroyOperation(w, r protocol.MessageReadWriter) {
	req := r.(*protocol.DMapMessage)
	err := db.destroyDTopicOnCluster(req.DMap())
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}

// Destroy removes all listeners for this topic on the cluster. If Publish function is called again after Destroy, the topic will be
// recreated.
func (dt *DTopic) Destroy() error {
	return dt.db.destroyDTopicOnCluster(dt.name)
}
