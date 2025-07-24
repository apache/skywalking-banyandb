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

	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
)

type future struct {
	server    *service
	result    atomic.Pointer[propertyv1.PropagationResponse]
	timeout   <-chan time.Time
	wg        sync.WaitGroup
	messageID uint64
}

func newFuture(server *service, req *propertyv1.PropagationRequest, timeout time.Duration) *future {
	f := &future{
		server:    server,
		messageID: req.Context.OriginMessageId,
		result:    atomic.Pointer[propertyv1.PropagationResponse]{},
		wg:        sync.WaitGroup{},
		timeout:   time.After(timeout),
	}
	f.wg.Add(1)
	return f
}

func (f *future) Get() (*propertyv1.PropagationResponse, error) {
	defer func() {
		// remove the future from the wait list
		f.server.removeFromWait(f)
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		f.wg.Wait()
	}()

	select {
	case <-done:
		loaded := f.result.Load()
		if loaded == nil {
			return nil, fmt.Errorf("failed to load remote message")
		}
		return loaded, nil
	case <-f.timeout:
		return nil, fmt.Errorf("timeout waiting for remote response")
	}
}

func (f *future) saveRemoteResponse(d *propertyv1.PropagationResponse) {
	f.result.Store(d)
	f.wg.Done()
}
