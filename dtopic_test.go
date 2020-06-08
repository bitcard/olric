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
	"testing"
	"time"
)

func TestDTopic_Publish(t *testing.T) {
	c := newTestCluster(nil)
	defer c.teardown()

	db, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	dt, err := db.NewDTopic("my-topic")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	onMessage := func(msg TopicMessage) {
		defer cancel()
		if msg.Message.(string) != "message" {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	regID, err := dt.AddListener(onMessage)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = dt.RemoveListener(regID)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}()

	err = dt.Publish("message")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Failed to call onMessage function")
	}
}

func TestDTopic_RemoveListener(t *testing.T) {
	c := newTestCluster(nil)
	defer c.teardown()

	db, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	dt, err := db.NewDTopic("my-topic")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	onMessage := func(msg TopicMessage) {}
	regID, err := dt.AddListener(onMessage)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = dt.RemoveListener(regID)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}
