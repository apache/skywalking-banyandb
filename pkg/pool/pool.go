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

// Register registers a new pool with the given name.
func Register[T any](name string) *Synced[T] {
	p := new(Synced[T])
	if _, ok := poolMap.LoadOrStore(name, p); ok {
		panic(fmt.Sprintf("duplicated pool: %s", name))
	}
	return p
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

type boundedEntry[T comparable] struct {
	value T
	size  int64
}

// Bounded is a deterministic pool whose retained objects cannot exceed a configured aggregate size.
type Bounded[T comparable] struct {
	newValue     func() T
	sizeOf       func(T) int64
	stacks       map[uint64]string
	idMap        map[T]uint64
	entries      []boundedEntry[T]
	retainedSize int64
	maxSize      int64
	idCounter    atomic.Uint64
	mutex        sync.Mutex
	stacksMutex  sync.Mutex
	refs         atomic.Int32
}

// RegisterBounded registers an aggregate-size-bounded pool with the given name.
func RegisterBounded[T comparable](name string, maxSize int64, newValue func() T, sizeOf func(T) int64) *Bounded[T] {
	if maxSize < 0 {
		panic("bounded pool maximum size cannot be negative")
	}
	if newValue == nil || sizeOf == nil {
		panic("bounded pool callbacks cannot be nil")
	}
	boundedPool := &Bounded[T]{
		newValue: newValue,
		sizeOf:   sizeOf,
		maxSize:  maxSize,
	}
	if _, ok := poolMap.LoadOrStore(name, boundedPool); ok {
		panic(fmt.Sprintf("duplicated pool: %s", name))
	}
	return boundedPool
}

// Get obtains an object from the pool or creates one when the pool is empty.
func (p *Bounded[T]) Get() T {
	p.mutex.Lock()
	entryCount := len(p.entries)
	if entryCount == 0 {
		p.mutex.Unlock()
		value := p.newValue()
		p.trackGet(value)
		return value
	}
	entry := p.entries[entryCount-1]
	var zero boundedEntry[T]
	p.entries[entryCount-1] = zero
	p.entries = p.entries[:entryCount-1]
	p.retainedSize -= entry.size
	p.mutex.Unlock()
	p.trackGet(entry.value)
	return entry.value
}

// Put releases an object and retains it when the aggregate size limit permits reuse.
func (p *Bounded[T]) Put(value T) bool {
	p.releaseTracking(value)
	valueSize := p.sizeOf(value)
	if valueSize < 0 || valueSize > p.maxSize {
		return false
	}
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if valueSize > p.maxSize-p.retainedSize {
		return false
	}
	p.entries = append(p.entries, boundedEntry[T]{value: value, size: valueSize})
	p.retainedSize += valueSize
	return true
}

// Discard releases an object without retaining it for reuse.
func (p *Bounded[T]) Discard(value T) {
	p.releaseTracking(value)
}

// RetainedSize returns the aggregate size of objects currently retained by the pool.
func (p *Bounded[T]) RetainedSize() int64 {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.retainedSize
}

// RefsCount returns the number of objects currently checked out from the pool.
func (p *Bounded[T]) RefsCount() int {
	return int(p.refs.Load())
}

// Stacks returns recorded stack traces for checked-out objects.
func (p *Bounded[T]) Stacks() []string {
	p.stacksMutex.Lock()
	defer p.stacksMutex.Unlock()
	result := make([]string, 0, len(p.stacks))
	for _, stack := range p.stacks {
		result = append(result, stack)
	}
	return result
}

func (p *Bounded[T]) trackGet(value T) {
	p.refs.Add(1)
	if !stackTrackingEnabled.Load() {
		return
	}
	p.stacksMutex.Lock()
	if p.stacks == nil {
		p.stacks = make(map[uint64]string)
		p.idMap = make(map[T]uint64)
	}
	id := p.idCounter.Add(1)
	buf := make([]byte, 4096)
	n := runtime.Stack(buf, false)
	p.idMap[value] = id
	p.stacks[id] = "Bounded.Get() called:\n" + string(buf[:n])
	p.stacksMutex.Unlock()
}

func (p *Bounded[T]) releaseTracking(value T) {
	if stackTrackingEnabled.Load() {
		p.stacksMutex.Lock()
		if id, exists := p.idMap[value]; exists {
			delete(p.stacks, id)
			delete(p.idMap, value)
		}
		p.stacksMutex.Unlock()
	}
	p.refs.Add(-1)
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

	// Capture stack trace if tracking is enabled.
	// Skip tracking when the pool returns nil because the caller will create
	// a new object whose pointer won't match the nil key in idMap,
	// so Put() would never clean up the entry.
	if v != nil && stackTrackingEnabled.Load() {
		p.stacksMutex.Lock()
		if p.stacks == nil {
			p.stacks = make(map[uint64]string)
			p.idMap = make(map[any]uint64)
		}
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
	p.releaseTracking(v)
	p.Pool.Put(v)
	p.refs.Add(-1)
}

// Discard releases a checked-out object without returning it to the pool.
func (p *Synced[T]) Discard(v T) {
	p.releaseTracking(v)
	p.refs.Add(-1)
}

func (p *Synced[T]) releaseTracking(v T) {
	// Remove the stack trace before making the object available again.
	// Otherwise another goroutine's Get() can reuse the pointer and
	// overwrite its idMap entry, causing this release to delete the wrong
	// stack and orphan the original one.
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
