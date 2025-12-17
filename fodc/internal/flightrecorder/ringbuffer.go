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
	lastVisibleVal T            // Cached last visible value, updated after each Add() completes
	values         []T          // Fixed-size buffer for values of type T
	mu             sync.RWMutex // Protects concurrent access to values, next, and size
	lastVisibleMu  sync.RWMutex // Protects lastVisibleVal updates
	next           int          // Next write position in the circular buffer
	size           int          // Actual number of values stored (capped at len(values), used to detect wrap)
	visibleSize    int          // Number of visible (finalized) values - counts towards capacity
}

// NewRingBuffer creates a new RingBuffer.
func NewRingBuffer[T any]() *RingBuffer[T] {
	return &RingBuffer[T]{
		next:   0,
		size:   0,
		values: make([]T, 0),
	}
}

// Add adds a value to the ring buffer.
func (rb *RingBuffer[T]) Add(v T) bool {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if len(rb.values) == 0 {
		rb.values = make([]T, 5) // Default capacity
		rb.next = 0
		rb.size = 0
	}

	rb.values[rb.next%len(rb.values)] = v
	rb.next = (rb.next + 1) % len(rb.values)

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
		// Shrink: keep the most recent VISIBLE items using FIFO strategy
		// Only visible values count towards capacity
		newValues := make([]T, capacity)

		// Calculate how many VISIBLE items to keep (most recent)
		// We want to keep the last 'capacity' visible items, or all visible items if we have fewer
		itemsToKeep := capacity
		if rb.visibleSize < capacity {
			itemsToKeep = rb.visibleSize
		}

		// Determine the start index for copying the most recent itemsToKeep items
		var startIdx int
		if rb.size >= currentLen {
			oldestIdx := rb.next % currentLen
			startIdx = (oldestIdx + (currentLen - itemsToKeep)) % currentLen
		} else {
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
		rb.visibleSize = itemsToKeep // All kept items are visible
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
		rb.size = itemsToCopy
		rb.next = itemsToCopy
		// Preserve visible size (only visible items count towards capacity)
		if rb.visibleSize > itemsToCopy {
			rb.visibleSize = itemsToCopy
		}
		return
	}
	if currentLen == 0 {
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

// VisibleSize returns the number of visible (finalized) values that count towards capacity.
func (rb *RingBuffer[T]) VisibleSize() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.visibleSize
}

// GetCurrentValue returns the most recently written value.
func (rb *RingBuffer[T]) GetCurrentValue() T {
	// Try to acquire read lock - if successful, read the actual current value
	// If lock is held by a writer (batch update in progress), return cached last visible value
	acquired := rb.mu.TryRLock()
	if acquired {
		defer rb.mu.RUnlock()

		var zero T
		if len(rb.values) == 0 {
			return zero
		}
		var currentValue T
		if rb.next == 0 {
			currentValue = rb.values[len(rb.values)-1]
		} else {
			currentValue = rb.values[rb.next-1]
		}

		rb.lastVisibleMu.Lock()
		rb.lastVisibleVal = currentValue
		rb.lastVisibleMu.Unlock()

		return currentValue
	}

	// Lock is held by writer (batch update in progress), return cached last visible value
	rb.lastVisibleMu.RLock()
	defer rb.lastVisibleMu.RUnlock()
	return rb.lastVisibleVal
}

// FinalizeLastVisible updates the last visible value to the current value.
func (rb *RingBuffer[T]) FinalizeLastVisible() {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if len(rb.values) == 0 {
		return
	}

	var currentValue T
	if rb.next == 0 {
		currentValue = rb.values[len(rb.values)-1]
	} else {
		currentValue = rb.values[rb.next-1]
	}

	// Update cached value
	rb.lastVisibleMu.Lock()
	rb.lastVisibleVal = currentValue
	rb.lastVisibleMu.Unlock()

	// Increment visible size - this value now counts towards capacity
	if rb.visibleSize < rb.size {
		rb.visibleSize++
	}
}

// GetAllValues returns all values in the buffer in order (oldest to newest).
func (rb *RingBuffer[T]) GetAllValues() []T {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if len(rb.values) == 0 {
		return nil
	}

	// Return only visible (finalized) values - these count towards capacity
	numValues := rb.visibleSize

	if numValues == 0 {
		return nil
	}

	result := make([]T, numValues)
	var startIdx int
	// Calculate start index based on visible size
	if rb.visibleSize >= len(rb.values) {
		// Buffer has wrapped, oldest visible is at (next - visibleSize) mod len
		startIdx = (rb.next - rb.visibleSize + len(rb.values)) % len(rb.values)
	} else {
		// Buffer hasn't wrapped, oldest visible is at (next - visibleSize)
		if rb.next >= rb.visibleSize {
			startIdx = rb.next - rb.visibleSize
		} else {
			startIdx = 0
		}
	}

	// Copy only the visible values starting from the oldest visible position
	for i := 0; i < numValues; i++ {
		idx := (startIdx + i) % len(rb.values)
		result[i] = rb.values[idx]
	}

	return result
}
