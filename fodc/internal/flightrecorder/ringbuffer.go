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

// RingBuffer is a generic circular buffer that stores values of type T.
type RingBuffer[T any] struct {
	values []T // Fixed-size buffer for values of type T
	next   int // Next write position in the circular buffer
}

// NewRingBuffer creates a new RingBuffer.
func NewRingBuffer[T any]() *RingBuffer[T] {
	return &RingBuffer[T]{
		next:   0,
		values: make([]T, 0),
	}
}

// Add adds a value to the ring buffer with the specified capacity.
// If the buffer needs to be resized, it uses FIFO strategy to remove oldest data.
func (rb *RingBuffer[T]) Add(v T, capacity int) {
	if capacity <= 0 {
		return
	}

	currentLen := len(rb.values)

	if capacity < currentLen {
		newValues := make([]T, capacity)

		startIdx := (rb.next - capacity + currentLen) % currentLen
		if startIdx < 0 {
			startIdx += currentLen
		}

		for i := 0; i < capacity; i++ {
			sourceIdx := (startIdx + i) % currentLen
			newValues[i] = rb.values[sourceIdx]
		}

		rb.values = newValues
		// After shrinking, next should point to where we'll write next
		// Since we kept the most recent items and filled positions 0 to capacity-1,
		// next should be at capacity (which will wrap to 0 on next write)
		rb.next = capacity
	} else if capacity > currentLen {
		newValues := make([]T, capacity)
		copy(newValues, rb.values)
		rb.values = newValues
		if rb.next >= len(rb.values) {
			rb.next = len(rb.values)
		}
	}

	if len(rb.values) > 0 {
		rb.values[rb.next%len(rb.values)] = v
		rb.next = (rb.next + 1) % len(rb.values)
	}
}

// Get returns the value at the specified index.
func (rb *RingBuffer[T]) Get(index int) T {
	var zero T
	if index < 0 || index >= len(rb.values) {
		return zero
	}
	return rb.values[index]
}

// Len returns the current length of the buffer.
func (rb *RingBuffer[T]) Len() int {
	return len(rb.values)
}

// Cap returns the current capacity of the buffer.
func (rb *RingBuffer[T]) Cap() int {
	return cap(rb.values)
}

// GetCurrentValue returns the most recently written value.
func (rb *RingBuffer[T]) GetCurrentValue() T {
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
	if len(rb.values) == 0 {
		return nil
	}

	result := make([]T, len(rb.values))
	startIdx := rb.next

	// Copy values starting from the oldest position
	for i := 0; i < len(rb.values); i++ {
		idx := (startIdx + i) % len(rb.values)
		result[i] = rb.values[idx]
	}

	return result
}
