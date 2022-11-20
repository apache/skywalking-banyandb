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

package run

import (
	"context"
	"sync"
)

// Closer can close a goroutine then wait for it to stop.
type Closer struct {
	waiting sync.WaitGroup
	lock    sync.RWMutex
	closed  bool

	ctx    context.Context
	cancel context.CancelFunc
}

// NewCloser instances a new Closer.
func NewCloser(initial int) *Closer {
	c := &Closer{}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.waiting.Add(initial)
	return c
}

// AddRunning adds a running task.
func (c *Closer) AddRunning() bool {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if c.closed {
		return false
	}
	c.waiting.Add(1)
	return true
}

// CloseNotify receives a signal from Close.
func (c *Closer) CloseNotify() <-chan struct{} {
	return c.ctx.Done()
}

// Done notifies that one task is done.
func (c *Closer) Done() {
	c.waiting.Done()
}

// CloseThenWait closes all tasks then waits till they are done.
func (c *Closer) CloseThenWait() {
	c.cancel()
	c.lock.Lock()
	c.closed = true
	c.lock.Unlock()
	c.waiting.Wait()
}

// Closed returns whether the Closer is closed
func (c *Closer) Closed() bool {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.closed
}
