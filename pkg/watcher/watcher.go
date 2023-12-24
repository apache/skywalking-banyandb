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

// Package watcher provides a watcher to watch the epoch.
package watcher

// Epoch is a epoch watcher.
// It will be notified when the epoch is reached.
type Epoch struct {
	epoch uint64
	ch    chan struct{}
}

// Watch returns a channel that will be notified when the epoch is reached.
func (e *Epoch) Watch() <-chan struct{} {
	return e.ch
}

// Epochs is a list of epoch watchers.
type Epochs []*Epoch

// Add adds a epoch watcher.
func (e *Epochs) Add(epoch *Epoch) {
	*e = append(*e, epoch)
}

// Notify notifies all epoch watchers that the epoch is reached.
func (e *Epochs) Notify(epoch uint64) {
	var remained Epochs
	for _, ep := range *e {
		if ep.epoch < epoch {
			close(ep.ch)
			continue
		}
		remained.Add(ep)
	}
	*e = remained
}

// Channel is a channel of epoch watchers.
type Channel chan *Epoch

// Add adds a epoch watcher.
// It returns nil if the channel is closed.
func (w Channel) Add(epoch uint64, closeCh <-chan struct{}) *Epoch {
	ep := &Epoch{
		epoch: epoch,
		ch:    make(chan struct{}, 1),
	}
	select {
	case w <- ep:
	case <-closeCh:
		return nil
	}
	return ep
}
