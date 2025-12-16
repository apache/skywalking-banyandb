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

package flightrecorder

import "sync"

// RingBuffer is a generic circular buffer that stores values of type T.
type RingBuffer[T any] struct {
	values []T          // Fixed-size buffer for values of type T
	next   int          // Next write position in the circular buffer
	size   int          // Actual number of values stored (capped at len(values), used to detect wrap)
	mu     sync.RWMutex // Protects concurrent access to values, next, and size
}

// NewRingBuffer creates a new RingBuffer.
func NewRingBuffer[T any]() *RingBuffer[T] {
	return &RingBuffer[T]{
		next:   0,
		values: make([]T, 0),
	}
}

// Add adds a value to the ring buffer.
func (rb *RingBuffer[T]) Add(v T) bool {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	// Initialize buffer with default capacity if empty
	if len(rb.values) == 0 {
		rb.values = make([]T, 5) // Default capacity
		rb.next = 0
		rb.size = 0
	}

	rb.values[rb.next%len(rb.values)] = v
	rb.next = (rb.next + 1) % len(rb.values)
	// Increment size until it reaches capacity, then keep it at capacity
	if rb.size < len(rb.values) {
		rb.size++
	}
	return true
}

// SetCapacity sets the capacity of the ring buffer and uses FIFO strategy to remove oldest data if needed.
func (rb *RingBuffer[T]) SetCapacity(capacity int) {
	if capacity <= 0 {
		return
	}

	rb.mu.Lock()
	defer rb.mu.Unlock()

	currentLen := len(rb.values)

	if capacity < currentLen {
		// Shrink: keep the most recent items using FIFO strategy
		newValues := make([]T, capacity)

		// Calculate how many items to keep (most recent)
		// We want to keep the last 'capacity' items, or all items if we have fewer
		itemsToKeep := capacity
		if rb.size < capacity {
			itemsToKeep = rb.size
		}

		// Determine the start index for copying the most recent itemsToKeep items
		var startIdx int
		if rb.size >= currentLen {
			// Buffer has wrapped: oldest is at next, most recent itemsToKeep items start from
			// (next - itemsToKeep) mod currentLen, but we need to handle negative modulo
			oldestIdx := rb.next % currentLen
			startIdx = (oldestIdx + (currentLen - itemsToKeep)) % currentLen
		} else {
			// Buffer hasn't wrapped: values are stored from 0 to next-1
			// Most recent itemsToKeep items start from max(0, next - itemsToKeep)
			if rb.next >= itemsToKeep {
				startIdx = rb.next - itemsToKeep
			} else {
				startIdx = 0
			}
		}

		for i := 0; i < itemsToKeep; i++ {
			sourceIdx := (startIdx + i) % currentLen
			newValues[i] = rb.values[sourceIdx]
		}

		rb.values = newValues
		// After shrinking, values are stored starting from index 0
		rb.next = itemsToKeep
		rb.size = itemsToKeep
		return
	}
	if capacity > currentLen {
		// Grow: expand the buffer and preserve circular order
		newValues := make([]T, capacity)
		itemsToCopy := 0
		if currentLen > 0 {
			// Copy values in FIFO order starting from oldest position
			var oldestIdx int
			if rb.size >= currentLen {
				// Buffer has wrapped, oldest is at next
				oldestIdx = rb.next % currentLen
			} else {
				// Buffer hasn't wrapped, oldest is at 0
				oldestIdx = 0
			}
			itemsToCopy = rb.size
			for i := 0; i < itemsToCopy; i++ {
				sourceIdx := (oldestIdx + i) % currentLen
				newValues[i] = rb.values[sourceIdx]
			}
		}
		rb.values = newValues
		// After growing, values are stored starting from index 0
		// Update size to reflect the actual number of values we kept
		rb.size = itemsToCopy
		rb.next = itemsToCopy
		return
	}
	if currentLen == 0 {
		// Initialize: create buffer with specified capacity
		rb.values = make([]T, capacity)
		rb.next = 0
		rb.size = 0
	}
}

// Get returns the value at the specified index in FIFO order (oldest at index 0).
func (rb *RingBuffer[T]) Get(index int) T {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	var zero T
	if len(rb.values) == 0 || index < 0 || index >= len(rb.values) {
		return zero
	}

	var startIdx int
	if rb.size >= len(rb.values) {
		// Buffer has wrapped, oldest is at next
		startIdx = rb.next
	} else {
		// Buffer hasn't wrapped, oldest is at 0
		startIdx = 0
	}
	actualIdx := (startIdx + index) % len(rb.values)
	return rb.values[actualIdx]
}

// Len returns the current length of the buffer.
func (rb *RingBuffer[T]) Len() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return len(rb.values)
}

// Cap returns the current capacity of the buffer.
func (rb *RingBuffer[T]) Cap() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return cap(rb.values)
}

// GetCurrentValue returns the most recently written value.
func (rb *RingBuffer[T]) GetCurrentValue() T {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	var zero T
	if len(rb.values) == 0 {
		return zero
	}
	// The most recent value is at (next - 1) mod len, but if next is 0, it's at len-1
	if rb.next == 0 {
		return rb.values[len(rb.values)-1]
	}
	return rb.values[rb.next-1]
}

// GetAllValues returns all values in the buffer in order (oldest to newest).
func (rb *RingBuffer[T]) GetAllValues() []T {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if len(rb.values) == 0 {
		return nil
	}

	numValues := rb.size

	if numValues == 0 {
		return nil
	}

	result := make([]T, numValues)
	var startIdx int
	if rb.size >= len(rb.values) {
		startIdx = rb.next
	} else {
		startIdx = 0
	}

	// Copy only the actual values starting from the oldest position
	for i := 0; i < numValues; i++ {
		idx := (startIdx + i) % len(rb.values)
		result[i] = rb.values[idx]
	}

	return result
}

// Copy creates a deep copy of the RingBuffer.
func (rb *RingBuffer[T]) Copy() *RingBuffer[T] {
	if rb == nil {
		return nil
	}

	rb.mu.RLock()
	defer rb.mu.RUnlock()

	copyRB := &RingBuffer[T]{
		next:   rb.next,
		size:   rb.size,
		values: make([]T, len(rb.values)),
	}
	copy(copyRB.values, rb.values)
	return copyRB
}
