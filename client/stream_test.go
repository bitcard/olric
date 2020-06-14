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
	"fmt"
	"github.com/buraksezer/olric/internal/protocol"
	"testing"
)

func TestClient_Stream(t *testing.T) {
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

	l := &listener{
		read: make(chan *protocol.Message, 1),
		write: make(chan *protocol.Message, 1),
	}
	_, err = c.addStreamListener(l)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	req := protocol.NewRequest(protocol.OpPut)
	l.write <- req

	msg := <-l.read
	fmt.Println(msg)
}