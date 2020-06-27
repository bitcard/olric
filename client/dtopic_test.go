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
	"context"
	"github.com/buraksezer/olric"
	"testing"
	"time"
)

func TestClient_DTopicPublish(t *testing.T) {
	db, done, err := newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Errorf("Expected nil. Got %v", serr)
		}
		<-done
	}()

	c, err := New(testConfig)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	dt := c.NewDTopic("my-dtopic")
	err = dt.Publish("my-message")
	if err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}
}

func TestClient_DTopicAddListener(t *testing.T) {
	db, done, err := newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Errorf("Expected nil. Got %v", serr)
		}
		<-done
	}()

	c, err := New(testConfig)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	onMessage := func(message olric.DTopicMessage) {}
	dt := c.NewDTopic("my-dtopic")
	_, err = dt.AddListener(onMessage)
	if err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}
}

func TestClient_DTopicOnMessage(t *testing.T) {
	db, done, err := newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Errorf("Expected nil. Got %v", serr)
		}
		<-done
	}()

	c, err := New(testConfig)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	onMessage := func(msg olric.DTopicMessage) {
		defer cancel()
		if msg.Message != "my-message" {
			t.Fatalf("Expected my-message. Got: %s", msg.Message)
		}
		if msg.PublishedAt <= 0 {
			t.Fatalf("Invalid published at: %d", msg.PublishedAt)
		}
	}

	dt := c.NewDTopic("my-dtopic")
	_, err = dt.AddListener(onMessage)
	if err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}

	err = dt.Publish("my-message")
	if err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}

	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatalf("No message received in 5 seconds")
	}
}

func TestClient_DTopicRemoveListener(t *testing.T) {
	db, done, err := newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Errorf("Expected nil. Got %v", serr)
		}
		<-done
	}()

	c, err := New(testConfig)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	onMessage := func(message olric.DTopicMessage) {}
	dt := c.NewDTopic("my-dtopic")
	listenerID, err := dt.AddListener(onMessage)
	if err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}
	err = dt.RemoveListener(listenerID)
	if err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}
	dt.streams.mu.RLock()
	defer dt.streams.mu.RUnlock()

	for _, s := range dt.streams.m {
		for id, _ := range s.listeners {
			if id == listenerID {
				t.Fatalf("ListenerID: %d is still exist", id)
			}
		}
	}
}

func TestClient_DTopicDestroy(t *testing.T) {
	db, done, err := newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Errorf("Expected nil. Got %v", serr)
		}
		<-done
	}()

	c, err := New(testConfig)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	onMessage := func(message olric.DTopicMessage) {}
	dt := c.NewDTopic("my-dtopic")
	listenerID, err := dt.AddListener(onMessage)
	if err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}
	err = dt.RemoveListener(listenerID)
	if err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}
	dt.streams.mu.RLock()
	defer dt.streams.mu.RUnlock()

	for _, s := range dt.streams.m {
		for id, _ := range s.listeners {
			if id == listenerID {
				t.Fatalf("ListenerID: %d is still exist", id)
			}
		}
	}
}