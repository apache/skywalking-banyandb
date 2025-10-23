// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Package pool provides a pool for reusing objects.
package pool

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
)

var (
	poolMap              = sync.Map{}
	stackTrackingEnabled atomic.Bool
)

// EnableStackTracking enables or disables stack tracking for all pools.
func EnableStackTracking(enabled bool) {
	stackTrackingEnabled.Store(enabled)
}

// Register registers a new pool with the given name.
func Register[T any](name string) *Synced[T] {
	p := new(Synced[T])
	if _, ok := poolMap.LoadOrStore(name, p); ok {
		panic(fmt.Sprintf("duplicated pool: %s", name))
	}
	return p
}

// AllRefsCount returns the reference count of all pools.
func AllRefsCount() map[string]int {
	result := make(map[string]int)
	poolMap.Range(func(key, value any) bool {
		result[key.(string)] = value.(Trackable).RefsCount()
		return true
	})
	return result
}

// AllStacks returns all recorded stack traces for leaked objects from all pools.
func AllStacks() map[string][]string {
	result := make(map[string][]string)
	poolMap.Range(func(key, value any) bool {
		if st, ok := value.(StackTracker); ok {
			stacks := st.Stacks()
			if len(stacks) > 0 {
				result[key.(string)] = stacks
			}
		}
		return true
	})
	return result
}

// Trackable is the interface that wraps the RefsCount method.
type Trackable interface {
	// RefsCount returns the reference count of the pool.
	RefsCount() int
}

// StackTracker is the interface that wraps the Stacks method.
type StackTracker interface {
	// Stacks returns all recorded stack traces for objects in the pool.
	Stacks() []string
}

// Synced is a pool that is safe for concurrent use.
type Synced[T any] struct {
	sync.Pool
	stacks      map[uint64]string
	idMap       map[any]uint64
	idCounter   atomic.Uint64
	stacksMutex sync.Mutex
	refs        atomic.Int32
}

// Get returns an object from the pool.
// If the pool is empty, nil is returned.
func (p *Synced[T]) Get() T {
	v := p.Pool.Get()
	p.refs.Add(1)

	var result T
	if v != nil {
		result = v.(T)
	}

	// Capture stack trace if tracking is enabled
	if stackTrackingEnabled.Load() {
		// Lazy initialize maps on first use
		p.stacksMutex.Lock()
		if p.stacks == nil {
			p.stacks = make(map[uint64]string)
			p.idMap = make(map[any]uint64)
		}

		// Generate unique ID and capture stack trace
		id := p.idCounter.Add(1)
		buf := make([]byte, 4096)
		n := runtime.Stack(buf, false)

		p.idMap[any(result)] = id
		p.stacks[id] = "Pool.Get() called:\n" + string(buf[:n])
		p.stacksMutex.Unlock()
	}

	return result
}

// Put puts an object back to the pool.
func (p *Synced[T]) Put(v T) {
	p.Pool.Put(v)
	p.refs.Add(-1)

	// Remove the stack trace for this object if tracking is enabled
	if stackTrackingEnabled.Load() {
		p.stacksMutex.Lock()
		if p.idMap != nil {
			if id, exists := p.idMap[any(v)]; exists {
				delete(p.stacks, id)
				delete(p.idMap, any(v))
			}
		}
		p.stacksMutex.Unlock()
	}
}

// RefsCount returns the reference count of the pool.
func (p *Synced[T]) RefsCount() int {
	return int(p.refs.Load())
}

// Stacks returns all recorded stack traces for objects in this pool.
func (p *Synced[T]) Stacks() []string {
	p.stacksMutex.Lock()
	defer p.stacksMutex.Unlock()

	if p.stacks == nil {
		return nil
	}

	result := make([]string, 0, len(p.stacks))
	for _, stack := range p.stacks {
		result = append(result, stack)
	}
	return result
}
