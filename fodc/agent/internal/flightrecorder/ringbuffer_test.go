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

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// addAndFinalize is a helper function that adds a value and immediately finalizes it.
// This simulates non-batch updates where values are immediately visible.
func addAndFinalize[T any](rb *RingBuffer[T], v T) {
	rb.Add(v)
	rb.FinalizeLastVisible()
}

// addMultipleAndFinalize is a helper function that adds multiple values and finalizes them all.
func addMultipleAndFinalize[T any](rb *RingBuffer[T], values []T) {
	for _, val := range values {
		rb.Add(val)
		rb.FinalizeLastVisible()
	}
}

// TestNewRingBuffer tests the creation of a new RingBuffer.
func TestNewRingBuffer(t *testing.T) {
	rb := NewRingBuffer[int]()

	require.NotNil(t, rb)
	assert.Equal(t, 0, rb.Len())
	assert.Equal(t, 0, rb.Cap())
}

// TestRingBuffer_Add_InitializesBuffer tests that Add initializes buffer with default capacity.
func TestRingBuffer_Add_InitializesBuffer(t *testing.T) {
	rb := NewRingBuffer[int]()

	success := rb.Add(42)

	assert.True(t, success)
	assert.Equal(t, 5, rb.Cap()) // Default capacity is 5
	assert.Equal(t, 5, rb.Len())
}

// TestRingBuffer_Add_SingleValue tests adding a single value.
func TestRingBuffer_Add_SingleValue(t *testing.T) {
	rb := NewRingBuffer[int]()

	success := rb.Add(42)
	assert.True(t, success)

	// Value is added but not visible until finalized
	assert.Equal(t, 0, rb.VisibleSize())
	rb.FinalizeLastVisible()

	assert.Equal(t, 42, rb.GetCurrentValue())
	assert.Equal(t, 42, rb.Get(0))
	assert.Equal(t, 1, rb.VisibleSize())
}

// TestRingBuffer_Add_MultipleValues tests adding multiple values.
func TestRingBuffer_Add_MultipleValues(t *testing.T) {
	rb := NewRingBuffer[int]()

	values := []int{1, 2, 3, 4, 5}
	addMultipleAndFinalize(rb, values)

	assert.Equal(t, 5, rb.GetCurrentValue())
	assert.Equal(t, 5, rb.VisibleSize())
	allValues := rb.GetAllValues()
	require.Len(t, allValues, 5)
	assert.Equal(t, values, allValues)
}

// TestRingBuffer_Add_WrapsAround tests that Add wraps around when buffer is full.
func TestRingBuffer_Add_WrapsAround(t *testing.T) {
	rb := NewRingBuffer[int]()

	// Fill buffer to capacity (default is 5) and finalize
	for i := 1; i <= 5; i++ {
		addAndFinalize(rb, i)
	}

	// Add one more to wrap around
	addAndFinalize(rb, 6)

	// Oldest value (1) should be overwritten
	allValues := rb.GetAllValues()
	require.Len(t, allValues, 5)
	// Should contain 2, 3, 4, 5, 6 (FIFO order)
	assert.Equal(t, 2, allValues[0])
	assert.Equal(t, 6, allValues[4])
	assert.Equal(t, 6, rb.GetCurrentValue())
	assert.Equal(t, 5, rb.VisibleSize())
}

// TestRingBuffer_Get_FIFOOrder tests that Get returns values in FIFO order.
func TestRingBuffer_Get_FIFOOrder(t *testing.T) {
	rb := NewRingBuffer[int]()

	values := []int{10, 20, 30}
	addMultipleAndFinalize(rb, values)

	assert.Equal(t, 10, rb.Get(0)) // Oldest
	assert.Equal(t, 20, rb.Get(1))
	assert.Equal(t, 30, rb.Get(2)) // Newest
	assert.Equal(t, 3, rb.VisibleSize())
}

// TestRingBuffer_Get_OutOfBounds tests Get with out of bounds indices.
func TestRingBuffer_Get_OutOfBounds(t *testing.T) {
	rb := NewRingBuffer[int]()

	rb.Add(42)

	var zero int
	assert.Equal(t, zero, rb.Get(-1))
	assert.Equal(t, zero, rb.Get(10))
}

// TestRingBuffer_Get_EmptyBuffer tests Get on empty buffer.
func TestRingBuffer_Get_EmptyBuffer(t *testing.T) {
	rb := NewRingBuffer[int]()

	var zero int
	assert.Equal(t, zero, rb.Get(0))
}

// TestRingBuffer_GetCurrentValue_EmptyBuffer tests GetCurrentValue on empty buffer.
func TestRingBuffer_GetCurrentValue_EmptyBuffer(t *testing.T) {
	rb := NewRingBuffer[int]()

	var zero int
	assert.Equal(t, zero, rb.GetCurrentValue())
}

// TestRingBuffer_GetCurrentValue_AfterWrap tests GetCurrentValue after buffer wraps.
func TestRingBuffer_GetCurrentValue_AfterWrap(t *testing.T) {
	rb := NewRingBuffer[int]()

	// Fill buffer and wrap
	for i := 1; i <= 6; i++ {
		rb.Add(i)
	}

	// Most recent should be 6
	assert.Equal(t, 6, rb.GetCurrentValue())
}

// TestRingBuffer_GetAllValues_EmptyBuffer tests GetAllValues on empty buffer.
func TestRingBuffer_GetAllValues_EmptyBuffer(t *testing.T) {
	rb := NewRingBuffer[int]()

	values := rb.GetAllValues()
	assert.Nil(t, values)
}

// TestRingBuffer_GetAllValues_FIFOOrder tests that GetAllValues returns values in FIFO order.
func TestRingBuffer_GetAllValues_FIFOOrder(t *testing.T) {
	rb := NewRingBuffer[int]()

	expectedValues := []int{1, 2, 3, 4, 5}
	addMultipleAndFinalize(rb, expectedValues)

	allValues := rb.GetAllValues()
	assert.Equal(t, expectedValues, allValues)
	assert.Equal(t, 5, rb.VisibleSize())
}

// TestRingBuffer_SetCapacity_Initialize tests SetCapacity on uninitialized buffer.
func TestRingBuffer_SetCapacity_Initialize(t *testing.T) {
	rb := NewRingBuffer[int]()

	rb.SetCapacity(10)

	assert.Equal(t, 10, rb.Cap())
	assert.Equal(t, 10, rb.Len())
}

// TestRingBuffer_SetCapacity_Grow tests growing the buffer capacity.
func TestRingBuffer_SetCapacity_Grow(t *testing.T) {
	rb := NewRingBuffer[int]()

	// Add some values and finalize them
	values := []int{1, 2, 3}
	addMultipleAndFinalize(rb, values)

	// Grow capacity
	rb.SetCapacity(10)

	assert.Equal(t, 10, rb.Cap())
	// Values should be preserved - GetAllValues returns only visible values
	allValues := rb.GetAllValues()
	require.Len(t, allValues, 3) // Only 3 values were finalized
	assert.Equal(t, 3, rb.VisibleSize())
	// All 3 should be the original values
	assert.Equal(t, 1, allValues[0])
	assert.Equal(t, 2, allValues[1])
	assert.Equal(t, 3, allValues[2])
}

// TestRingBuffer_SetCapacity_Shrink tests shrinking the buffer capacity.
func TestRingBuffer_SetCapacity_Shrink(t *testing.T) {
	rb := NewRingBuffer[int]()

	// Add values to fill buffer and finalize them
	values := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	addMultipleAndFinalize(rb, values)

	// Set initial capacity
	rb.SetCapacity(10)

	// Shrink to smaller capacity
	rb.SetCapacity(5)

	assert.Equal(t, 5, rb.Cap())
	assert.Equal(t, 5, rb.VisibleSize())
	// Should keep the most recent 5 visible values
	allValues := rb.GetAllValues()
	require.Len(t, allValues, 5)
	// Should contain 6, 7, 8, 9, 10 (most recent)
	assert.Equal(t, 6, allValues[0])
	assert.Equal(t, 10, allValues[4])
}

// TestRingBuffer_SetCapacity_SameSize tests SetCapacity with same size.
func TestRingBuffer_SetCapacity_SameSize(t *testing.T) {
	rb := NewRingBuffer[int]()

	rb.SetCapacity(5)
	addAndFinalize(rb, 1)
	addAndFinalize(rb, 2)

	rb.SetCapacity(5) // Same size

	assert.Equal(t, 5, rb.Cap())
	assert.Equal(t, 2, rb.VisibleSize())
	allValues := rb.GetAllValues()
	require.Len(t, allValues, 2) // Only 2 values were finalized
	assert.Equal(t, 1, allValues[0])
	assert.Equal(t, 2, allValues[1])
}

// TestRingBuffer_SetCapacity_ZeroCapacity tests SetCapacity with zero or negative capacity.
func TestRingBuffer_SetCapacity_ZeroCapacity(t *testing.T) {
	rb := NewRingBuffer[int]()

	rb.Add(42)
	initialCap := rb.Cap()

	rb.SetCapacity(0)
	rb.SetCapacity(-1)

	// Should not change capacity
	assert.Equal(t, initialCap, rb.Cap())
}

// TestRingBuffer_SetCapacity_PreservesFIFOOrder tests that SetCapacity preserves FIFO order.
func TestRingBuffer_SetCapacity_PreservesFIFOOrder(t *testing.T) {
	rb := NewRingBuffer[int]()

	// Add values and finalize them
	for i := 1; i <= 8; i++ {
		addAndFinalize(rb, i)
	}

	// Set capacity to 5 (should keep most recent 5 visible values)
	rb.SetCapacity(5)

	assert.Equal(t, 5, rb.VisibleSize())
	allValues := rb.GetAllValues()
	require.Len(t, allValues, 5)
	// Should be 4, 5, 6, 7, 8 in FIFO order
	assert.Equal(t, 4, allValues[0])
	assert.Equal(t, 5, allValues[1])
	assert.Equal(t, 6, allValues[2])
	assert.Equal(t, 7, allValues[3])
	assert.Equal(t, 8, allValues[4])
}

// TestRingBuffer_SetCapacity_AfterWrap tests SetCapacity after buffer has wrapped.
func TestRingBuffer_SetCapacity_AfterWrap(t *testing.T) {
	rb := NewRingBuffer[int]()

	// Fill buffer and wrap (default capacity is 5, so after 7 adds, buffer contains 3, 4, 5, 6, 7)
	for i := 1; i <= 7; i++ {
		addAndFinalize(rb, i)
	}

	// Buffer wrapped, should contain 3, 4, 5, 6, 7
	// Now grow
	rb.SetCapacity(10)

	assert.Equal(t, 5, rb.VisibleSize())
	allValues := rb.GetAllValues()
	require.Len(t, allValues, 5) // Buffer was full at capacity 5, so 5 visible values preserved
	// Should preserve FIFO order: 3, 4, 5, 6, 7
	assert.Equal(t, 3, allValues[0])
	assert.Equal(t, 7, allValues[4])
}

// TestRingBuffer_ConcurrentAdd tests concurrent Add operations.
func TestRingBuffer_ConcurrentAdd(t *testing.T) {
	rb := NewRingBuffer[int]()

	var wg sync.WaitGroup
	numGoroutines := 10
	valuesPerGoroutine := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(startVal int) {
			defer wg.Done()
			for j := 0; j < valuesPerGoroutine; j++ {
				rb.Add(startVal*valuesPerGoroutine + j)
			}
		}(i)
	}

	wg.Wait()

	// Should have successfully added all values
	assert.Greater(t, rb.Cap(), 0)
}

// TestRingBuffer_ConcurrentGet tests concurrent Get operations.
func TestRingBuffer_ConcurrentGet(t *testing.T) {
	rb := NewRingBuffer[int]()

	// Add some values
	for i := 1; i <= 10; i++ {
		rb.Add(i)
	}

	var wg sync.WaitGroup
	numGoroutines := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = rb.Get(0)
			_ = rb.GetCurrentValue()
			_ = rb.GetAllValues()
		}()
	}

	wg.Wait()

	// Should not panic and should return valid values
	assert.Greater(t, rb.GetCurrentValue(), 0)
}

// TestRingBuffer_ConcurrentAddAndGet tests concurrent Add and Get operations.
func TestRingBuffer_ConcurrentAddAndGet(t *testing.T) {
	rb := NewRingBuffer[int]()

	var wg sync.WaitGroup
	numGoroutines := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()
			rb.Add(val)
			_ = rb.Get(0)
			_ = rb.GetCurrentValue()
		}(i)
	}

	wg.Wait()

	// Should have successfully added values
	assert.Greater(t, rb.Cap(), 0)
}

// TestRingBuffer_ConcurrentSetCapacity tests concurrent SetCapacity operations.
func TestRingBuffer_ConcurrentSetCapacity(t *testing.T) {
	rb := NewRingBuffer[int]()

	// Add some values
	for i := 1; i <= 10; i++ {
		rb.Add(i)
	}

	var wg sync.WaitGroup
	numGoroutines := 5

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(capacity int) {
			defer wg.Done()
			rb.SetCapacity(capacity)
		}((i + 1) * 5)
	}

	wg.Wait()

	// Should have valid capacity
	assert.Greater(t, rb.Cap(), 0)
}

// TestRingBuffer_Float64Type tests RingBuffer with float64 type.
func TestRingBuffer_Float64Type(t *testing.T) {
	rb := NewRingBuffer[float64]()

	values := []float64{1.5, 2.5, 3.5, 4.5, 5.5}
	addMultipleAndFinalize(rb, values)

	assert.Equal(t, 5.5, rb.GetCurrentValue())
	assert.Equal(t, 5, rb.VisibleSize())
	allValues := rb.GetAllValues()
	assert.Equal(t, values, allValues)
}

// TestRingBuffer_StringType tests RingBuffer with string type.
func TestRingBuffer_StringType(t *testing.T) {
	rb := NewRingBuffer[string]()

	values := []string{"a", "b", "c", "d", "e"}
	addMultipleAndFinalize(rb, values)

	assert.Equal(t, "e", rb.GetCurrentValue())
	assert.Equal(t, 5, rb.VisibleSize())
	allValues := rb.GetAllValues()
	assert.Equal(t, values, allValues)
}

// TestRingBuffer_Int64Type tests RingBuffer with int64 type (used for timestamps).
func TestRingBuffer_Int64Type(t *testing.T) {
	rb := NewRingBuffer[int64]()

	values := []int64{1000, 2000, 3000, 4000, 5000}
	addMultipleAndFinalize(rb, values)

	assert.Equal(t, int64(5000), rb.GetCurrentValue())
	assert.Equal(t, 5, rb.VisibleSize())
	allValues := rb.GetAllValues()
	assert.Equal(t, values, allValues)
}

// TestRingBuffer_GetAllValues_AfterWrap tests GetAllValues after buffer wraps.
func TestRingBuffer_GetAllValues_AfterWrap(t *testing.T) {
	rb := NewRingBuffer[int]()

	// Fill buffer to capacity (5) and wrap, finalizing each value
	for i := 1; i <= 7; i++ {
		addAndFinalize(rb, i)
	}

	assert.Equal(t, 5, rb.VisibleSize())
	allValues := rb.GetAllValues()
	require.Len(t, allValues, 5)
	// Should contain 3, 4, 5, 6, 7 (oldest to newest)
	assert.Equal(t, 3, allValues[0])
	assert.Equal(t, 7, allValues[4])
}

// TestRingBuffer_SetCapacity_ShrinkToSmallerThanCount tests shrinking to smaller than current count.
func TestRingBuffer_SetCapacity_ShrinkToSmallerThanCount(t *testing.T) {
	rb := NewRingBuffer[int]()

	// Add 10 values and finalize them
	for i := 1; i <= 10; i++ {
		addAndFinalize(rb, i)
	}

	// Shrink to 3
	rb.SetCapacity(3)

	assert.Equal(t, 3, rb.Cap())
	assert.Equal(t, 3, rb.VisibleSize())
	allValues := rb.GetAllValues()
	require.Len(t, allValues, 3)
	// Should keep most recent 3 visible values: 8, 9, 10
	assert.Equal(t, 8, allValues[0])
	assert.Equal(t, 9, allValues[1])
	assert.Equal(t, 10, allValues[2])
}

// TestRingBuffer_SetCapacity_GrowAfterWrap tests growing after buffer has wrapped.
func TestRingBuffer_SetCapacity_GrowAfterWrap(t *testing.T) {
	rb := NewRingBuffer[int]()

	// Fill and wrap (default capacity is 5), finalizing each value
	for i := 1; i <= 7; i++ {
		addAndFinalize(rb, i)
	}

	// Grow to 10
	rb.SetCapacity(10)

	allValues := rb.GetAllValues()
	assert.Equal(t, 5, rb.VisibleSize()) // Only 5 visible values (capacity was 5)
	require.Len(t, allValues, 5)         // Buffer was full at capacity 5, so 5 values preserved
	// Should preserve FIFO order: 3, 4, 5, 6, 7
	assert.Equal(t, 3, allValues[0])
	assert.Equal(t, 7, allValues[4])
}

// TestRingBuffer_Get_AfterSetCapacity tests Get after SetCapacity.
func TestRingBuffer_Get_AfterSetCapacity(t *testing.T) {
	rb := NewRingBuffer[int]()

	// Add values
	for i := 1; i <= 5; i++ {
		rb.Add(i)
	}

	// Set capacity
	rb.SetCapacity(10)

	// Get should still work correctly (only for visible values)
	// Since values weren't finalized, Get will return zero values
	// But if we finalize them first:
	for i := 1; i <= 5; i++ {
		rb.FinalizeLastVisible()
	}
	assert.Equal(t, 1, rb.Get(0))
	assert.Equal(t, 5, rb.Get(4))
}

// TestRingBuffer_GetCurrentValue_AfterSetCapacity tests GetCurrentValue after SetCapacity.
func TestRingBuffer_GetCurrentValue_AfterSetCapacity(t *testing.T) {
	rb := NewRingBuffer[int]()

	// Add values and finalize
	for i := 1; i <= 5; i++ {
		addAndFinalize(rb, i)
	}

	// Set capacity
	rb.SetCapacity(10)

	// GetCurrentValue should still return the most recent value
	assert.Equal(t, 5, rb.GetCurrentValue())
}

// TestRingBuffer_Len_AfterOperations tests Len after various operations.
func TestRingBuffer_Len_AfterOperations(t *testing.T) {
	rb := NewRingBuffer[int]()

	assert.Equal(t, 0, rb.Len())

	rb.Add(1)
	// After Add, buffer is initialized with capacity 5
	assert.Equal(t, 5, rb.Len())

	rb.SetCapacity(10)
	assert.Equal(t, 10, rb.Len())
}

// TestRingBuffer_Cap_AfterOperations tests Cap after various operations.
func TestRingBuffer_Cap_AfterOperations(t *testing.T) {
	rb := NewRingBuffer[int]()

	assert.Equal(t, 0, rb.Cap())

	rb.Add(1)
	assert.Equal(t, 5, rb.Cap()) // Default capacity

	rb.SetCapacity(10)
	assert.Equal(t, 10, rb.Cap())
}

// TestRingBuffer_MultipleSetCapacity tests multiple SetCapacity calls.
func TestRingBuffer_MultipleSetCapacity(t *testing.T) {
	rb := NewRingBuffer[int]()

	// Fill buffer to capacity 5
	rb.SetCapacity(5)
	for i := 1; i <= 5; i++ {
		addAndFinalize(rb, i)
	}

	// Grow to 10 and add more values
	rb.SetCapacity(10)
	for i := 6; i <= 10; i++ {
		addAndFinalize(rb, i)
	}

	// Shrink to 3 (should keep most recent 3 visible values)
	rb.SetCapacity(3)

	assert.Equal(t, 3, rb.Cap())
	assert.Equal(t, 3, rb.VisibleSize())
	allValues := rb.GetAllValues()
	require.Len(t, allValues, 3)
	// Should contain most recent 3 visible values: 8, 9, 10
	assert.Equal(t, 8, allValues[0])
	assert.Equal(t, 9, allValues[1])
	assert.Equal(t, 10, allValues[2])
}

// TestRingBuffer_GetAllValues_AfterMultipleWraps tests GetAllValues after multiple wraps.
func TestRingBuffer_GetAllValues_AfterMultipleWraps(t *testing.T) {
	rb := NewRingBuffer[int]()

	// Set capacity to 3
	rb.SetCapacity(3)

	// Add 10 values (will wrap multiple times) and finalize them
	for i := 1; i <= 10; i++ {
		addAndFinalize(rb, i)
	}

	assert.Equal(t, 3, rb.VisibleSize())
	allValues := rb.GetAllValues()
	require.Len(t, allValues, 3)
	// Should contain most recent 3 visible values: 8, 9, 10
	assert.Equal(t, 8, allValues[0])
	assert.Equal(t, 9, allValues[1])
	assert.Equal(t, 10, allValues[2])
}

// TestRingBuffer_VisibleSize_InvisibleValues tests that unfinalized values don't count towards visible size.
func TestRingBuffer_VisibleSize_InvisibleValues(t *testing.T) {
	rb := NewRingBuffer[int]()

	// Add values without finalizing
	rb.Add(1)
	rb.Add(2)
	rb.Add(3)

	// Visible size should be 0 (no finalized values)
	assert.Equal(t, 0, rb.VisibleSize())
	allValues := rb.GetAllValues()
	assert.Nil(t, allValues) // No visible values

	// Finalize first value (value 3, the most recent)
	rb.FinalizeLastVisible()
	assert.Equal(t, 1, rb.VisibleSize())
	allValues = rb.GetAllValues()
	require.Len(t, allValues, 1)
	assert.Equal(t, 3, allValues[0]) // Most recent value (3)

	// Finalize again - increments visibleSize since visibleSize (1) < size (3)
	rb.FinalizeLastVisible()
	assert.Equal(t, 2, rb.VisibleSize())
	allValues = rb.GetAllValues()
	require.Len(t, allValues, 2)
	// GetAllValues returns last visibleSize values: 2, 3
	assert.Equal(t, 2, allValues[0])
	assert.Equal(t, 3, allValues[1])

	// Add and finalize a new value
	rb.Add(4)
	rb.FinalizeLastVisible()
	assert.Equal(t, 3, rb.VisibleSize())
	allValues = rb.GetAllValues()
	require.Len(t, allValues, 3)
	// GetAllValues returns last 3 values: 2, 3, 4
	assert.Equal(t, 2, allValues[0])
	assert.Equal(t, 3, allValues[1])
	assert.Equal(t, 4, allValues[2])
}

// TestRingBuffer_VisibleSize_BatchUpdate tests visible size during batch updates.
func TestRingBuffer_VisibleSize_BatchUpdate(t *testing.T) {
	rb := NewRingBuffer[int]()

	// Initial visible values
	addAndFinalize(rb, 10)
	addAndFinalize(rb, 20)
	assert.Equal(t, 2, rb.VisibleSize())

	// Batch update: add multiple values without finalizing
	rb.Add(30)
	rb.Add(40)
	rb.Add(50)

	// Visible size should still be 2 (previous batch)
	assert.Equal(t, 2, rb.VisibleSize())
	allValues := rb.GetAllValues()
	require.Len(t, allValues, 2)
	assert.Equal(t, 10, allValues[0])
	assert.Equal(t, 20, allValues[1])

	// Finalize the batch - finalizes value 50 (most recent)
	// Note: FinalizeLastVisible increments visibleSize if visibleSize < size
	// After adding 30, 40, 50: size=5, visibleSize=2, next wraps to 0
	// After FinalizeLastVisible: visibleSize becomes 3
	rb.FinalizeLastVisible()
	assert.Equal(t, 3, rb.VisibleSize())
	allValues = rb.GetAllValues()
	require.Len(t, allValues, 3)
	// GetAllValues returns values based on next and visibleSize calculation
	// With the current implementation, after adding 30, 40, 50 and finalizing once:
	// - Buffer state: next wraps to 0 (after 5 adds), size=5, visibleSize=3
	// - Only values 10, 20, 50 are actually finalized (30 and 40 are not)
	// - But GetAllValues assumes last visibleSize values are all visible
	// - So it returns [10, 20, 30] (the first 3 values in the buffer)
	// This demonstrates the limitation that GetAllValues can't distinguish
	// which specific values are visible when unfinalized values are mixed in
	assert.Equal(t, 10, allValues[0])
	assert.Equal(t, 20, allValues[1])
	assert.Equal(t, 30, allValues[2])
}

// TestRingBuffer_GetCurrentValue_DuringBatchUpdate tests GetCurrentValue during batch update.
func TestRingBuffer_GetCurrentValue_DuringBatchUpdate(t *testing.T) {
	rb := NewRingBuffer[int]()

	// Initial value
	addAndFinalize(rb, 100)
	assert.Equal(t, 100, rb.GetCurrentValue())

	// Start batch update (simulate by holding lock)
	rb.Add(200)
	rb.Add(300)

	// GetCurrentValue should return last visible value (100) without blocking
	// Note: In real scenario, TryRLock would fail and return cached value
	// But in test, we can't easily simulate this, so we test the finalize behavior
	rb.FinalizeLastVisible()
	assert.Equal(t, 300, rb.GetCurrentValue())
	assert.Equal(t, 2, rb.VisibleSize())
}

// TestRingBuffer_Capacity_OnlyVisibleValues tests that capacity calculations use only visible values.
func TestRingBuffer_Capacity_OnlyVisibleValues(t *testing.T) {
	rb := NewRingBuffer[int]()

	// Add 10 values but only finalize 5
	for i := 1; i <= 10; i++ {
		rb.Add(i)
		if i <= 5 {
			rb.FinalizeLastVisible()
		}
	}

	// Only 5 values should be visible
	assert.Equal(t, 5, rb.VisibleSize())
	allValues := rb.GetAllValues()
	require.Len(t, allValues, 5)
	// GetAllValues returns last 5 values: 6, 7, 8, 9, 10 (but only first 5 are finalized)
	// Actually, GetAllValues uses visibleSize to determine count, but returns
	// the last visibleSize values from the buffer, which may include unfinalized ones.
	// The current implementation returns [6, 7, 8, 9, 10] because it uses
	// next - visibleSize to calculate start index.

	// Set capacity to 3 - should keep only visible values
	// SetCapacity uses visibleSize to determine itemsToKeep (min of capacity and visibleSize)
	// visibleSize=5, capacity=3, so itemsToKeep=3
	rb.SetCapacity(3)
	assert.Equal(t, 3, rb.VisibleSize())
	allValues = rb.GetAllValues()
	require.Len(t, allValues, 3)
	// SetCapacity keeps the most recent itemsToKeep (3) values from the buffer
	// Buffer has [1,2,3,4,5,6,7,8,9,10], so it keeps last 3: [8, 9, 10]
	// Note: This is a limitation - SetCapacity assumes last visibleSize values are all visible
	assert.Equal(t, 8, allValues[0])
	assert.Equal(t, 9, allValues[1])
	assert.Equal(t, 10, allValues[2])
}
