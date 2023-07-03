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

var dummyChannelCloserChan <-chan struct{}

// ChannelCloser can close a goroutine then wait for it to stop.
type ChannelCloser struct {
	ctx      context.Context
	cancel   context.CancelFunc
	sender   sync.WaitGroup
	receiver sync.WaitGroup
	lock     sync.RWMutex
	closed   bool
}

// NewChannelCloser instances a new ChannelCloser.
func NewChannelCloser(initial int) *ChannelCloser {
	c := &ChannelCloser{}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.sender.Add(1)
	c.receiver.Add(initial)
	return c
}

// AddRunning adds a running task.
func (c *ChannelCloser) AddRunning() bool {
	if c == nil {
		return false
	}
	c.lock.RLock()
	defer c.lock.RUnlock()
	if c.closed {
		return false
	}
	c.sender.Add(1)
	return true
}

// RunningDone notifies that running task is done.
func (c *ChannelCloser) RunningDone() {
	if c == nil {
		return
	}
	c.sender.Done()
}

// CloseNotify receives a signal from Close.
func (c *ChannelCloser) CloseNotify() <-chan struct{} {
	if c == nil {
		return dummyChannelCloserChan
	}
	return c.ctx.Done()
}

// Done notifies that receiver task is done.
func (c *ChannelCloser) Done() {
	if c == nil {
		return
	}
	c.receiver.Done()
}

// CloseThenWait closes all tasks then waits till they are done.
func (c *ChannelCloser) CloseThenWait() {
	if c == nil {
		return
	}

	c.lock.Lock()
	c.closed = true
	c.lock.Unlock()

	c.sender.Done()
	c.sender.Wait()

	c.cancel()
	c.receiver.Wait()
}

// Closed returns whether the ChannelCloser is closed.
func (c *ChannelCloser) Closed() bool {
	if c == nil {
		return true
	}
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.closed
}
