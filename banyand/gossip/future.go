// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package gossip

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	gossipv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/gossip/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
)

type future struct {
	localFuture bus.Future
	server      *service
	remoteMsg   atomic.Pointer[bus.Message]
	timeout     <-chan time.Time
	wg          sync.WaitGroup
	messageID   bus.MessageID
	hasRemote   bool
}

func newFuture(server *service, msg bus.Message, localFuture bus.Future, hasRemote bool, timeout time.Duration) *future {
	f := &future{
		server:      server,
		messageID:   msg.ID(),
		localFuture: localFuture,
		hasRemote:   hasRemote,
		remoteMsg:   atomic.Pointer[bus.Message]{},
		wg:          sync.WaitGroup{},
		timeout:     time.After(timeout),
	}
	f.wg.Add(1)
	return f
}

func (f *future) Get() (bus.Message, error) {
	defer func() {
		// remove the future from the wait list
		f.server.removeFromWait(f)
	}()
	localResp, err := f.localFuture.Get()
	if err != nil {
		return bus.Message{}, err
	}

	if !f.hasRemote {
		return localResp, nil
	}

	localData := localResp.Data().(*gossipv1.PropagationMessageResponse)
	if !localData.Success {
		return localResp, nil
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		f.wg.Wait()
	}()

	select {
	case <-done:
		loaded := f.remoteMsg.Load()
		if loaded == nil {
			return bus.Message{}, fmt.Errorf("failed to load remote message")
		}
		return *loaded, nil
	case <-f.timeout:
		return bus.Message{}, fmt.Errorf("timeout waiting for remote response")
	}
}

func (f *future) saveRemoteResponse(message bus.Message) {
	f.remoteMsg.Store(&message)
	f.wg.Done()
}
