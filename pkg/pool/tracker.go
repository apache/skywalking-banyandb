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

//go:build !slim
// +build !slim

package pool

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
)

// Tracker tracks object lifecycle (Acquire/Release) without pooling.
// It integrates with AllRefsCount/AllStacks to help detect leaked resources.
type Tracker struct {
	stacks     map[uint64]string
	idMap      map[any]uint64
	active     sync.Map
	idCounter  atomic.Uint64
	stacksLock sync.Mutex
	refs       atomic.Int32
}

// RegisterTracker registers a new lifecycle tracker with the given name.
func RegisterTracker(name string) *Tracker {
	t := new(Tracker)
	if _, ok := poolMap.LoadOrStore(name, t); ok {
		panic(fmt.Sprintf("duplicated tracker: %s", name))
	}
	return t
}

// Acquire marks an object as acquired.
func (t *Tracker) Acquire(obj any) {
	if obj == nil {
		return
	}
	if _, loaded := t.active.LoadOrStore(obj, struct{}{}); loaded {
		// Double acquire is a logic bug, but avoid inflating refs.
		return
	}
	t.refs.Add(1)

	if !stackTrackingEnabled.Load() {
		return
	}

	t.stacksLock.Lock()
	if t.stacks == nil {
		t.stacks = make(map[uint64]string)
		t.idMap = make(map[any]uint64)
	}
	id := t.idCounter.Add(1)
	buf := make([]byte, 4096)
	n := runtime.Stack(buf, false)
	t.idMap[obj] = id
	t.stacks[id] = "Tracker.Acquire() called:\n" + string(buf[:n])
	t.stacksLock.Unlock()
}

// Release marks an object as released.
func (t *Tracker) Release(obj any) {
	if obj == nil {
		return
	}
	if _, loaded := t.active.LoadAndDelete(obj); !loaded {
		// Avoid negative refs on double-release or releasing an untracked object.
		return
	}
	t.refs.Add(-1)

	if !stackTrackingEnabled.Load() {
		return
	}

	t.stacksLock.Lock()
	if t.idMap != nil {
		if id, ok := t.idMap[obj]; ok {
			delete(t.stacks, id)
			delete(t.idMap, obj)
		}
	}
	t.stacksLock.Unlock()
}

// RefsCount returns the reference count of tracked objects.
func (t *Tracker) RefsCount() int {
	return int(t.refs.Load())
}

// Stacks returns recorded stack traces for acquired-but-not-released objects.
func (t *Tracker) Stacks() []string {
	t.stacksLock.Lock()
	defer t.stacksLock.Unlock()

	if t.stacks == nil {
		return nil
	}

	result := make([]string, 0, len(t.stacks))
	for _, stack := range t.stacks {
		result = append(result, stack)
	}
	return result
}
