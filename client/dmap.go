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
	"github.com/buraksezer/olric/internal/protocol"
	"time"
)

// DMap provides methods to access distributed maps on Olric cluster.
type DMap struct {
	*Client
	name string
}

func (c *Client) processGetResponse(resp *protocol.Message) (interface{}, error) {
	if err := checkStatusCode(resp); err != nil {
		return nil, err
	}
	return c.unmarshalValue(resp.Value)
}

// Get gets the value for the given key. It returns ErrKeyNotFound if the DB does not contains the key.
// It's thread-safe. It is safe to modify the contents of the returned value.
// It is safe to modify the contents of the argument after Get returns.
func (d *DMap) Get(key string) (interface{}, error) {
	m := &protocol.Message{
		DMap: d.name,
		Key:  key,
	}
	resp, err := d.client.Request(protocol.OpGet, m)
	if err != nil {
		return nil, err
	}
	return d.processGetResponse(resp)
}

// Put sets the value for the given key. It overwrites any previous value for that key and it's thread-safe.
// It is safe to modify the contents of the arguments after Put returns but not before.
func (d *DMap) Put(key string, value interface{}) error {
	data, err := d.serializer.Marshal(value)
	if err != nil {
		return err
	}
	m := &protocol.Message{
		DMap:  d.name,
		Key:   key,
		Value: data,
		Extra: protocol.PutExtra{
			Timestamp: time.Now().UnixNano(),
		},
	}
	resp, err := d.client.Request(protocol.OpPut, m)
	if err != nil {
		return err
	}
	return checkStatusCode(resp)
}

// PutEx sets the value for the given key with TTL. It overwrites any previous value for that key.
// It's thread-safe. It is safe to modify the contents of the arguments after Put returns but not before.
func (d *DMap) PutEx(key string, value interface{}, timeout time.Duration) error {
	data, err := d.serializer.Marshal(value)
	if err != nil {
		return err
	}
	m := &protocol.Message{
		DMap:  d.name,
		Key:   key,
		Value: data,
		Extra: protocol.PutExExtra{
			TTL:       timeout.Nanoseconds(),
			Timestamp: time.Now().UnixNano(),
		},
	}
	resp, err := d.client.Request(protocol.OpPutEx, m)
	if err != nil {
		return err
	}
	return checkStatusCode(resp)
}

// Delete deletes the value for the given key. Delete will not return error if key doesn't exist.
// It's thread-safe. It is safe to modify the contents of the argument after Delete returns.
func (d *DMap) Delete(key string) error {
	m := &protocol.Message{
		DMap: d.name,
		Key:  key,
	}
	resp, err := d.client.Request(protocol.OpDelete, m)
	if err != nil {
		return err
	}
	return checkStatusCode(resp)
}

// LockContext is returned by Lock and LockWithTimeout methods.
// It should be stored in a proper way to release the lock.
type LockContext struct {
	name  string
	key   string
	token []byte
	dmap  *DMap
}

// LockWithTimeout sets a lock for the given key. If the lock is still unreleased the end of given period of time,
// it automatically releases the lock. Acquired lock is only for the key in this DMap.
//
// It returns immediately if it acquires the lock for the given key. Otherwise, it waits until deadline.
//
// You should know that the locks are approximate, and only to be used for non-critical purposes.
func (d *DMap) LockWithTimeout(key string, timeout, deadline time.Duration) (*LockContext, error) {
	m := &protocol.Message{
		DMap: d.name,
		Key:  key,
		Extra: protocol.LockWithTimeoutExtra{
			Timeout:  timeout.Nanoseconds(),
			Deadline: deadline.Nanoseconds(),
		},
	}
	resp, err := d.client.Request(protocol.OpLockWithTimeout, m)
	if err != nil {
		return nil, err
	}
	err = checkStatusCode(resp)
	if err != nil {
		return nil, err
	}
	ctx := &LockContext{
		name:  d.name,
		key:   key,
		token: resp.Value,
		dmap:  d,
	}
	return ctx, nil
}

// Lock sets a lock for the given key. Acquired lock is only for the key in this DMap.
//
// It returns immediately if it acquires the lock for the given key. Otherwise, it waits until deadline.
//
// You should know that the locks are approximate, and only to be used for non-critical purposes.
func (d *DMap) Lock(key string, deadline time.Duration) (*LockContext, error) {
	m := &protocol.Message{
		DMap: d.name,
		Key:  key,
		Extra: protocol.LockExtra{
			Deadline: deadline.Nanoseconds(),
		},
	}
	resp, err := d.client.Request(protocol.OpLock, m)
	if err != nil {
		return nil, err
	}
	err = checkStatusCode(resp)
	if err != nil {
		return nil, err
	}
	ctx := &LockContext{
		name:  d.name,
		key:   key,
		token: resp.Value,
		dmap:  d,
	}
	return ctx, nil
}

// Unlock releases an acquired lock for the given key.
// It returns olric.ErrNoSuchLock if there is no lock for the given key.
func (l *LockContext) Unlock() error {
	m := &protocol.Message{
		DMap:  l.name,
		Key:   l.key,
		Value: l.token,
	}
	resp, err := l.dmap.client.Request(protocol.OpUnlock, m)
	if err != nil {
		return err
	}
	return checkStatusCode(resp)
}

// Destroy flushes the given DMap on the cluster. You should know that there is no global lock on DMaps.
// So if you call Put/PutEx/PutIf/PutIfEx and Destroy methods concurrently on the cluster,
// those calls may set new values to the DMap.
func (d *DMap) Destroy() error {
	m := &protocol.Message{
		DMap: d.name,
	}
	resp, err := d.client.Request(protocol.OpDestroy, m)
	if err != nil {
		return err
	}
	return checkStatusCode(resp)
}

func (c *Client) processIncrDecrResponse(resp *protocol.Message) (int, error) {
	if err := checkStatusCode(resp); err != nil {
		return 0, err
	}
	res, err := c.unmarshalValue(resp.Value)
	if err != nil {
		return 0, err
	}
	return res.(int), nil
}

func (c *Client) incrDecr(op protocol.OpCode, name, key string, delta int) (int, error) {
	value, err := c.serializer.Marshal(delta)
	if err != nil {
		return 0, err
	}
	m := &protocol.Message{
		DMap:  name,
		Key:   key,
		Value: value,
		Extra: protocol.AtomicExtra{
			Timestamp: time.Now().UnixNano(),
		},
	}
	resp, err := c.client.Request(op, m)
	if err != nil {
		return 0, err
	}
	return c.processIncrDecrResponse(resp)
}

// Incr atomically increments key by delta. The return value is the new value after being incremented or an error.
func (d *DMap) Incr(key string, delta int) (int, error) {
	return d.incrDecr(protocol.OpIncr, d.name, key, delta)
}

// Decr atomically decrements key by delta. The return value is the new value after being decremented or an error.
func (d *DMap) Decr(key string, delta int) (int, error) {
	return d.incrDecr(protocol.OpDecr, d.name, key, delta)
}

func (c *Client) processGetPutResponse(resp *protocol.Message) (interface{}, error) {
	if err := checkStatusCode(resp); err != nil {
		return nil, err
	}
	if len(resp.Value) == 0 {
		return nil, nil
	}
	oldval, err := c.unmarshalValue(resp.Value)
	if err != nil {
		return nil, err
	}
	return oldval, nil
}

// GetPut atomically sets key to value and returns the old value stored at key.
func (d *DMap) GetPut(key string, value interface{}) (interface{}, error) {
	data, err := d.serializer.Marshal(value)
	if err != nil {
		return nil, err
	}
	m := &protocol.Message{
		DMap:  d.name,
		Key:   key,
		Value: data,
		Extra: protocol.AtomicExtra{
			Timestamp: time.Now().UnixNano(),
		},
	}
	resp, err := d.client.Request(protocol.OpGetPut, m)
	if err != nil {
		return nil, err
	}
	return d.processGetPutResponse(resp)
}

// Expire updates the expiry for the given key. It returns ErrKeyNotFound if the
// DB does not contains the key. It's thread-safe.
func (d *DMap) Expire(key string, timeout time.Duration) error {
	m := &protocol.Message{
		DMap: d.name,
		Key:  key,
		Extra: protocol.ExpireExtra{
			TTL:       timeout.Nanoseconds(),
			Timestamp: time.Now().UnixNano(),
		},
	}
	resp, err := d.client.Request(protocol.OpExpire, m)
	if err != nil {
		return err
	}
	return checkStatusCode(resp)
}

// PutIf sets the value for the given key. It overwrites any previous value for that key and it's thread-safe.
// It is safe to modify the contents of the arguments after PutIf returns but not before.
// Flag argument currently has two different options:
//
// olric.IfNotFound: Only set the key if it does not already exist.
// It returns olric.ErrFound if the key already exist.
//
// olric.IfFound: Only set the key if it already exist.
// It returns olric.ErrKeyNotFound if the key does not exist.
func (d *DMap) PutIf(key string, value interface{}, flags int16) error {
	data, err := d.serializer.Marshal(value)
	if err != nil {
		return err
	}
	m := &protocol.Message{
		DMap:  d.name,
		Key:   key,
		Value: data,
		Extra: protocol.PutIfExtra{
			Flags:     flags,
			Timestamp: time.Now().UnixNano(),
		},
	}
	resp, err := d.client.Request(protocol.OpPutIf, m)
	if err != nil {
		return err
	}
	return checkStatusCode(resp)
}

// PutIfEx sets the value for the given key with TTL. It overwrites any previous value for that key. It's thread-safe.
// It is safe to modify the contents of the arguments after PutIfEx returns but not before.
// Flag argument currently has two different options:
//
// olric.IfNotFound: Only set the key if it does not already exist.
// It returns olric.ErrFound if the key already exist.
//
// olric.IfFound: Only set the key if it already exist.
// It returns olric.ErrKeyNotFound if the key does not exist.
func (d *DMap) PutIfEx(key string, value interface{}, timeout time.Duration, flags int16) error {
	data, err := d.serializer.Marshal(value)
	if err != nil {
		return err
	}
	m := &protocol.Message{
		DMap:  d.name,
		Key:   key,
		Value: data,
		Extra: protocol.PutIfExExtra{
			Flags:     flags,
			TTL:       timeout.Nanoseconds(),
			Timestamp: time.Now().UnixNano(),
		},
	}
	resp, err := d.client.Request(protocol.OpPutIfEx, m)
	if err != nil {
		return err
	}
	return checkStatusCode(resp)
}
