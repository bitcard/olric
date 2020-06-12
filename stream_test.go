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
	"fmt"
	"testing"

	"github.com/buraksezer/olric/internal/protocol"
)

func TestStream_OpenStream(t *testing.T) {
	db, err := newDB(testSingleReplicaConfig())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db.Shutdown(context.Background())
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	readCh := make(chan *protocol.Message, 1)
	writeCh := make(chan *protocol.Message, 1)
	go func() {
		err = db.client.CreateStream(ctx, db.this.String(), readCh, writeCh)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}()

	for {
		select {
		case msg := <-readCh:
			fmt.Println(msg.DMap)
			fmt.Println(msg.Key)
			fmt.Println(string(msg.Value))
		case msg := <-writeCh:
			fmt.Println(msg)
		}
	}
}
