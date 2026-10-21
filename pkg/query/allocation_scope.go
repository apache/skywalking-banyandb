// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for additional
// information regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package query

import (
	"context"
	"errors"
	"math"
	"sync"
)

type allocationScopeKey struct{}

// AllocationScope holds selected native allocations until their source closes.
// It shares execution accounting without coupling native storage to its caller.
type AllocationScope struct {
	reserveFn    func(int64) error
	releaseFn    func(int64)
	reservations map[any]uint64
	lastKey      any
	lastSize     uint64
	mu           sync.Mutex
	reserved     int64
	closed       bool
}

// WithAllocationScope gives one source ownership of its allocation reservations.
func WithAllocationScope(ctx context.Context, reserve func(int64) error, release func(int64)) (context.Context, *AllocationScope) {
	scope := &AllocationScope{reserveFn: reserve, releaseFn: release, reservations: make(map[any]uint64)}
	return context.WithValue(ctx, allocationScopeKey{}, scope), scope
}

// WithExistingAllocationScope retains a source's accounting while using the
// current pull's cancellation and deadline.
func WithExistingAllocationScope(ctx context.Context, scope *AllocationScope) context.Context {
	return context.WithValue(ctx, allocationScopeKey{}, scope)
}

// AllocationScopeFromContext returns the selected-allocation scope, when present.
func AllocationScopeFromContext(ctx context.Context) (*AllocationScope, bool) {
	scope, ok := ctx.Value(allocationScopeKey{}).(*AllocationScope)
	return scope, ok
}

// ReserveAllocation reserves selected storage bytes before allocating or borrowing.
// Calls without a source scope leave the existing admission accounting unchanged.
func ReserveAllocation(ctx context.Context, bytes uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	scope, ok := ctx.Value(allocationScopeKey{}).(*AllocationScope)
	if !ok {
		return nil
	}
	scope.mu.Lock()
	defer scope.mu.Unlock()
	return scope.reserve(bytes)
}

// ReserveAllocationOnce accounts the current live size of a reusable selected
// buffer. The key must be comparable; growth reserves only the positive delta.
func ReserveAllocationOnce(ctx context.Context, key any, bytes uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	scope, ok := ctx.Value(allocationScopeKey{}).(*AllocationScope)
	if !ok {
		return nil
	}
	scope.mu.Lock()
	defer scope.mu.Unlock()
	if scope.closed {
		return scope.reserve(0)
	}
	previous := scope.lastSize
	if scope.lastKey != key {
		previous = scope.reservations[key]
	}
	if bytes <= previous {
		scope.lastKey, scope.lastSize = key, previous
		return nil
	}
	if err := scope.reserve(bytes - previous); err != nil {
		return err
	}
	scope.reservations[key] = bytes
	scope.lastKey, scope.lastSize = key, bytes
	return nil
}

func (s *AllocationScope) reserve(bytes uint64) error {
	if s.closed {
		return errors.New("query allocation source is closed")
	}
	if bytes > math.MaxInt64-uint64(s.reserved) {
		return errors.New("query allocation size overflows")
	}
	if err := s.reserveFn(int64(bytes)); err != nil {
		return err
	}
	s.reserved += int64(bytes)
	return nil
}

// Close releases the source reservations exactly once.
func (s *AllocationScope) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	s.releaseFn(s.reserved)
	s.reserved = 0
	s.reservations = nil
	s.lastKey, s.lastSize = nil, 0
	return nil
}
