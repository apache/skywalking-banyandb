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
	"sync"
)

// ChannelGroupCloser can close a goroutine group then wait for it to stop.
type ChannelGroupCloser struct {
	group  []*ChannelCloser
	lock   sync.RWMutex
	closed bool
}

// NewChannelGroupCloser instances a new ChannelGroupCloser.
func NewChannelGroupCloser(closer ...*ChannelCloser) *ChannelGroupCloser {
	return &ChannelGroupCloser{group: closer}
}

// CloseThenWait closes all closer then waits till they are done.
func (c *ChannelGroupCloser) CloseThenWait() {
	if c == nil {
		return
	}

	c.lock.Lock()
	c.closed = true
	c.lock.Unlock()

	for _, closer := range c.group {
		closer.CloseThenWait()
	}
}

// Closed returns whether the ChannelGroupCloser is closed.
func (c *ChannelGroupCloser) Closed() bool {
	if c == nil {
		return true
	}
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.closed
}
