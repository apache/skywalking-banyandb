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

type Chan[T any] struct {
	ch     chan T
	closer sync.WaitGroup
}

func NewChan[T any](ch chan T) *Chan[T] {
	return &Chan[T]{
		ch: ch,
	}
}

func (c *Chan[T]) Write(item T) {
	c.closer.Add(1)
	defer c.closer.Done()
	c.ch <- item
}

func (c *Chan[T]) Read() (T, bool) {
	item, more := <-c.ch
	return item, more
}

func (c *Chan[T]) Close() error {
	c.closer.Wait()
	close(c.ch)
	return nil
}
