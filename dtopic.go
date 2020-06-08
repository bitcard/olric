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
	"runtime"

	"github.com/buraksezer/olric/internal/protocol"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

type TopicMessage struct {
	Message interface{}
}

// DTopic implements a distributed topic to deliver messages between clients and Olric nodes.
type DTopic struct {
	name string
	db   *Olric
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

func (db *Olric) publishDTopicMessageToAddr(addr, topic string, data []byte, sem *semaphore.Weighted) error {
	defer db.wg.Done()
	defer sem.Release(1)

	// TODO: We need to find a better name for DMap in this struct.
	req := &protocol.Message{
		DMap:  topic,
		Value: data,
	}
	_, err := db.requestTo(addr, protocol.OpPublishDTopicMessage, req)
	if err != nil {
		db.log.V(2).Printf("[ERROR] Failed to publish message to %s: %v", addr, err)
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
		g.Go(func() error {
			return db.publishDTopicMessageToAddr(member.String(), topic, data, sem)
		})
	}
	// Wait blocks until all function calls from the Go method have returned,
	// then returns the first non-nil error (if any) from them.
	return g.Wait()
}

// Publish publishes the given message to listeners of the topic.
func (dt *DTopic) Publish(msg interface{}) error {
	data, err := dt.db.serializer.Marshal(msg)
	if err != nil {
		return err
	}

	return dt.db.publishDTopicMessage(dt.name, data)
}

func (dt *DTopic) AddListener(f func(TopicMessage)) (uint64, error) {
	return dt.db.dtopic.addListener(dt.name, f)
}

func (dt *DTopic) RemoveListener(regID uint64) error {
	return dt.db.dtopic.removeListener(dt.name, regID)
}
