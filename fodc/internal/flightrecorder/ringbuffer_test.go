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
	assert.Equal(t, 42, rb.GetCurrentValue())
	assert.Equal(t, 42, rb.Get(0))
}

// TestRingBuffer_Add_MultipleValues tests adding multiple values.
func TestRingBuffer_Add_MultipleValues(t *testing.T) {
	rb := NewRingBuffer[int]()

	values := []int{1, 2, 3, 4, 5}
	for _, val := range values {
		success := rb.Add(val)
		assert.True(t, success)
	}

	assert.Equal(t, 5, rb.GetCurrentValue())
	allValues := rb.GetAllValues()
	require.Len(t, allValues, 5)
	assert.Equal(t, values, allValues)
}

// TestRingBuffer_Add_WrapsAround tests that Add wraps around when buffer is full.
func TestRingBuffer_Add_WrapsAround(t *testing.T) {
	rb := NewRingBuffer[int]()

	// Fill buffer to capacity (default is 5)
	for i := 1; i <= 5; i++ {
		rb.Add(i)
	}

	// Add one more to wrap around
	rb.Add(6)

	// Oldest value (1) should be overwritten
	allValues := rb.GetAllValues()
	require.Len(t, allValues, 5)
	// Should contain 2, 3, 4, 5, 6 (FIFO order)
	assert.Equal(t, 2, allValues[0])
	assert.Equal(t, 6, allValues[4])
	assert.Equal(t, 6, rb.GetCurrentValue())
}

// TestRingBuffer_Get_FIFOOrder tests that Get returns values in FIFO order.
func TestRingBuffer_Get_FIFOOrder(t *testing.T) {
	rb := NewRingBuffer[int]()

	values := []int{10, 20, 30}
	for _, val := range values {
		rb.Add(val)
	}

	assert.Equal(t, 10, rb.Get(0)) // Oldest
	assert.Equal(t, 20, rb.Get(1))
	assert.Equal(t, 30, rb.Get(2)) // Newest
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
	for _, val := range expectedValues {
		rb.Add(val)
	}

	allValues := rb.GetAllValues()
	assert.Equal(t, expectedValues, allValues)
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

	// Add some values
	values := []int{1, 2, 3}
	for _, val := range values {
		rb.Add(val)
	}

	// Grow capacity
	rb.SetCapacity(10)

	assert.Equal(t, 10, rb.Cap())
	// Values should be preserved - GetAllValues returns only actual written values
	allValues := rb.GetAllValues()
	require.Len(t, allValues, 3) // Only 3 values were written
	// All 3 should be the original values
	assert.Equal(t, 1, allValues[0])
	assert.Equal(t, 2, allValues[1])
	assert.Equal(t, 3, allValues[2])
}

// TestRingBuffer_SetCapacity_Shrink tests shrinking the buffer capacity.
func TestRingBuffer_SetCapacity_Shrink(t *testing.T) {
	rb := NewRingBuffer[int]()

	// Add values to fill buffer
	values := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for _, val := range values {
		rb.Add(val)
	}

	// Set initial capacity
	rb.SetCapacity(10)

	// Shrink to smaller capacity
	rb.SetCapacity(5)

	assert.Equal(t, 5, rb.Cap())
	// Should keep the most recent 5 values
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
	rb.Add(1)
	rb.Add(2)

	rb.SetCapacity(5) // Same size

	assert.Equal(t, 5, rb.Cap())
	allValues := rb.GetAllValues()
	require.Len(t, allValues, 2) // Only 2 values were written
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

	// Add values
	for i := 1; i <= 8; i++ {
		rb.Add(i)
	}

	// Set capacity to 5 (should keep most recent 5)
	rb.SetCapacity(5)

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
		rb.Add(i)
	}

	// Buffer wrapped, should contain 3, 4, 5, 6, 7
	// Now grow
	rb.SetCapacity(10)

	allValues := rb.GetAllValues()
	require.Len(t, allValues, 5) // Buffer was full at capacity 5, so 5 values preserved
	// Should preserve FIFO order: 3, 4, 5, 6, 7
	assert.Equal(t, 3, allValues[0])
	assert.Equal(t, 7, allValues[4])
}

// TestRingBuffer_Copy tests the Copy method.
func TestRingBuffer_Copy(t *testing.T) {
	rb := NewRingBuffer[int]()

	values := []int{1, 2, 3, 4, 5}
	for _, val := range values {
		rb.Add(val)
	}

	copyRB := rb.Copy()

	require.NotNil(t, copyRB)
	assert.NotSame(t, rb, copyRB)
	assert.Equal(t, rb.GetAllValues(), copyRB.GetAllValues())
	assert.Equal(t, rb.GetCurrentValue(), copyRB.GetCurrentValue())

	// Modify original
	rb.Add(6)

	// Copy should not be affected
	assert.Equal(t, 5, copyRB.GetCurrentValue())
}

// TestRingBuffer_Copy_Nil tests Copy on nil buffer.
func TestRingBuffer_Copy_Nil(t *testing.T) {
	var rb *RingBuffer[int]

	copyRB := rb.Copy()

	assert.Nil(t, copyRB)
}

// TestRingBuffer_Copy_EmptyBuffer tests Copy on empty buffer.
func TestRingBuffer_Copy_EmptyBuffer(t *testing.T) {
	rb := NewRingBuffer[int]()

	copyRB := rb.Copy()

	require.NotNil(t, copyRB)
	assert.Equal(t, 0, copyRB.Len())
	assert.Equal(t, 0, copyRB.Cap())
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
	for _, val := range values {
		rb.Add(val)
	}

	assert.Equal(t, 5.5, rb.GetCurrentValue())
	allValues := rb.GetAllValues()
	assert.Equal(t, values, allValues)
}

// TestRingBuffer_StringType tests RingBuffer with string type.
func TestRingBuffer_StringType(t *testing.T) {
	rb := NewRingBuffer[string]()

	values := []string{"a", "b", "c", "d", "e"}
	for _, val := range values {
		rb.Add(val)
	}

	assert.Equal(t, "e", rb.GetCurrentValue())
	allValues := rb.GetAllValues()
	assert.Equal(t, values, allValues)
}

// TestRingBuffer_Int64Type tests RingBuffer with int64 type (used for timestamps).
func TestRingBuffer_Int64Type(t *testing.T) {
	rb := NewRingBuffer[int64]()

	values := []int64{1000, 2000, 3000, 4000, 5000}
	for _, val := range values {
		rb.Add(val)
	}

	assert.Equal(t, int64(5000), rb.GetCurrentValue())
	allValues := rb.GetAllValues()
	assert.Equal(t, values, allValues)
}

// TestRingBuffer_GetAllValues_AfterWrap tests GetAllValues after buffer wraps.
func TestRingBuffer_GetAllValues_AfterWrap(t *testing.T) {
	rb := NewRingBuffer[int]()

	// Fill buffer to capacity (5) and wrap
	for i := 1; i <= 7; i++ {
		rb.Add(i)
	}

	allValues := rb.GetAllValues()
	require.Len(t, allValues, 5)
	// Should contain 3, 4, 5, 6, 7 (oldest to newest)
	assert.Equal(t, 3, allValues[0])
	assert.Equal(t, 7, allValues[4])
}

// TestRingBuffer_SetCapacity_ShrinkToSmallerThanCount tests shrinking to smaller than current count.
func TestRingBuffer_SetCapacity_ShrinkToSmallerThanCount(t *testing.T) {
	rb := NewRingBuffer[int]()

	// Add 10 values
	for i := 1; i <= 10; i++ {
		rb.Add(i)
	}

	// Shrink to 3
	rb.SetCapacity(3)

	assert.Equal(t, 3, rb.Cap())
	allValues := rb.GetAllValues()
	require.Len(t, allValues, 3)
	// Should keep most recent 3: 8, 9, 10
	assert.Equal(t, 8, allValues[0])
	assert.Equal(t, 9, allValues[1])
	assert.Equal(t, 10, allValues[2])
}

// TestRingBuffer_SetCapacity_GrowAfterWrap tests growing after buffer has wrapped.
func TestRingBuffer_SetCapacity_GrowAfterWrap(t *testing.T) {
	rb := NewRingBuffer[int]()

	// Fill and wrap (default capacity is 5)
	for i := 1; i <= 7; i++ {
		rb.Add(i)
	}

	// Grow to 10
	rb.SetCapacity(10)

	allValues := rb.GetAllValues()
	require.Len(t, allValues, 5) // Buffer was full at capacity 5, so 5 values preserved
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

	// Get should still work correctly
	assert.Equal(t, 1, rb.Get(0))
	assert.Equal(t, 5, rb.Get(4))
}

// TestRingBuffer_GetCurrentValue_AfterSetCapacity tests GetCurrentValue after SetCapacity.
func TestRingBuffer_GetCurrentValue_AfterSetCapacity(t *testing.T) {
	rb := NewRingBuffer[int]()

	// Add values
	for i := 1; i <= 5; i++ {
		rb.Add(i)
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
		rb.Add(i)
	}

	// Grow to 10 and add more values
	rb.SetCapacity(10)
	for i := 6; i <= 10; i++ {
		rb.Add(i)
	}

	// Shrink to 3 (should keep most recent 3)
	rb.SetCapacity(3)

	assert.Equal(t, 3, rb.Cap())
	allValues := rb.GetAllValues()
	require.Len(t, allValues, 3)
	// Should contain most recent 3 values: 8, 9, 10
	assert.Equal(t, 8, allValues[0])
	assert.Equal(t, 9, allValues[1])
	assert.Equal(t, 10, allValues[2])
}

// TestRingBuffer_GetAllValues_AfterMultipleWraps tests GetAllValues after multiple wraps.
func TestRingBuffer_GetAllValues_AfterMultipleWraps(t *testing.T) {
	rb := NewRingBuffer[int]()

	// Set capacity to 3
	rb.SetCapacity(3)

	// Add 10 values (will wrap multiple times)
	for i := 1; i <= 10; i++ {
		rb.Add(i)
	}

	allValues := rb.GetAllValues()
	require.Len(t, allValues, 3)
	// Should contain most recent 3: 8, 9, 10
	assert.Equal(t, 8, allValues[0])
	assert.Equal(t, 9, allValues[1])
	assert.Equal(t, 10, allValues[2])
}
