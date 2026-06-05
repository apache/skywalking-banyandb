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

package vectorized

import (
	"fmt"
	"sync/atomic"
)

// MemoryTracker is a lock-free per-query memory budget.
//
// Reserve grows used iff used+bytes <= limit; otherwise it returns an error and
// leaves used unchanged. Release shrinks used. Used reads the current value.
type MemoryTracker struct {
	used  atomic.Int64
	limit int64
}

// NewMemoryTracker returns a tracker with the given byte limit.
func NewMemoryTracker(limit int64) *MemoryTracker {
	return &MemoryTracker{limit: limit}
}

// Reserve attempts to allocate bytes from the budget.
// On success used grows by bytes; on failure used is unchanged.
//
// A negative bytes value indicates a programmer error (Reserve must not be
// used to decrement) and panics. Zero bytes is a valid no-op.
func (m *MemoryTracker) Reserve(bytes int64) error {
	if bytes < 0 {
		panic("vectorized: MemoryTracker.Reserve called with negative bytes")
	}
	if bytes == 0 {
		return nil
	}
	for {
		current := m.used.Load()
		next := current + bytes
		if next > m.limit {
			return fmt.Errorf("memory budget exceeded: used %d + requested %d > limit %d",
				current, bytes, m.limit)
		}
		if m.used.CompareAndSwap(current, next) {
			return nil
		}
	}
}

// Release returns bytes to the budget.
// A negative bytes value indicates a programmer error and panics.
func (m *MemoryTracker) Release(bytes int64) {
	if bytes < 0 {
		panic("vectorized: MemoryTracker.Release called with negative bytes")
	}
	if bytes == 0 {
		return
	}
	m.used.Add(-bytes)
}

// Used returns the current outstanding reservation.
func (m *MemoryTracker) Used() int64 {
	return m.used.Load()
}
