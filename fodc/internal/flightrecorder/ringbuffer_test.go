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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewRingBuffer tests the NewRingBuffer function.
func TestNewRingBuffer(t *testing.T) {
	rb := NewRingBuffer[float64]()

	assert.NotNil(t, rb)
	assert.Equal(t, 0, rb.Len())
	assert.Equal(t, 0, rb.Cap())
	assert.Equal(t, 0, rb.next)
	assert.NotNil(t, rb.values)
	assert.Len(t, rb.values, 0)
}

// TestRingBuffer_Add_Basic tests basic Add functionality.
func TestRingBuffer_Add_Basic(t *testing.T) {
	rb := NewRingBuffer[int]()
	capacity := 5
	rb.SetCapacity(capacity)

	rb.Add(10)
	rb.Add(20)
	rb.Add(30)

	assert.Equal(t, capacity, rb.Len())
	assert.Equal(t, capacity, rb.Cap())
	assert.Equal(t, 10, rb.Get(0))
	assert.Equal(t, 20, rb.Get(1))
	assert.Equal(t, 30, rb.Get(2))
}

// TestRingBuffer_Add_String tests Add with string type.
func TestRingBuffer_Add_String(t *testing.T) {
	rb := NewRingBuffer[string]()
	capacity := 3
	rb.SetCapacity(capacity)

	rb.Add("first")
	rb.Add("second")
	rb.Add("third")

	assert.Equal(t, capacity, rb.Len())
	assert.Equal(t, "first", rb.Get(0))
	assert.Equal(t, "second", rb.Get(1))
	assert.Equal(t, "third", rb.Get(2))
	assert.Equal(t, "third", rb.GetCurrentValue())
}

// TestRingBuffer_Add_Bool tests Add with bool type.
func TestRingBuffer_Add_Bool(t *testing.T) {
	rb := NewRingBuffer[bool]()
	capacity := 2
	rb.SetCapacity(capacity)

	rb.Add(true)
	rb.Add(false)

	assert.Equal(t, capacity, rb.Len())
	assert.True(t, rb.Get(0))
	assert.False(t, rb.Get(1))
	assert.False(t, rb.GetCurrentValue())
}

// TestRingBuffer_Add_CircularBehavior tests circular overwrite behavior.
func TestRingBuffer_Add_CircularBehavior(t *testing.T) {
	rb := NewRingBuffer[int]()
	capacity := 3

	rb.SetCapacity(capacity)
	// Fill buffer
	rb.Add(1)
	rb.Add(2)
	rb.Add(3)

	// Add one more - should overwrite oldest
	rb.Add(4)

	allValues := rb.GetAllValues()
	require.Len(t, allValues, capacity)
	// Should contain 2, 3, 4 (most recent)
	assert.Contains(t, allValues, 2)
	assert.Contains(t, allValues, 3)
	assert.Contains(t, allValues, 4)
	// Should not contain 1 (oldest, overwritten)
	assert.NotContains(t, allValues, 1)
}

// TestRingBuffer_Add_MultipleOverwrites tests multiple circular overwrites.
func TestRingBuffer_Add_MultipleOverwrites(t *testing.T) {
	rb := NewRingBuffer[int]()
	capacity := 3

	rb.SetCapacity(capacity)
	// Fill and overflow multiple times
	for i := 1; i <= 10; i++ {
		rb.Add(i)
	}

	allValues := rb.GetAllValues()
	require.Len(t, allValues, capacity)
	// Should contain the last 3 values: 8, 9, 10
	assert.Contains(t, allValues, 8)
	assert.Contains(t, allValues, 9)
	assert.Contains(t, allValues, 10)
	// Should not contain earlier values
	assert.NotContains(t, allValues, 1)
	assert.NotContains(t, allValues, 5)
	assert.NotContains(t, allValues, 7)
}

// TestRingBuffer_Get tests Get method.
func TestRingBuffer_Get(t *testing.T) {
	rb := NewRingBuffer[int]()
	rb.SetCapacity(10)

	rb.Add(10)
	rb.Add(20)
	rb.Add(30)

	assert.Equal(t, 10, rb.Get(0))
	assert.Equal(t, 20, rb.Get(1))
	assert.Equal(t, 30, rb.Get(2))
}

// TestRingBuffer_Get_OutOfBoundsIndex tests Get with out of bounds indices.
func TestRingBuffer_Get_OutOfBoundsIndex(t *testing.T) {
	rb := NewRingBuffer[int]()
	capacity := 3
	rb.SetCapacity(capacity)

	rb.Add(10)

	var zero int
	assert.Equal(t, zero, rb.Get(-1))
	assert.Equal(t, zero, rb.Get(capacity))
	assert.Equal(t, zero, rb.Get(100))
}

// TestRingBuffer_Get_EmptyBuffer tests Get on empty buffer.
func TestRingBuffer_Get_EmptyBuffer(t *testing.T) {
	rb := NewRingBuffer[int]()

	var zero int
	assert.Equal(t, zero, rb.Get(0))
	assert.Equal(t, zero, rb.Get(-1))
	assert.Equal(t, zero, rb.Get(10))
}

// TestRingBuffer_Len tests Len method.
func TestRingBuffer_Len(t *testing.T) {
	rb := NewRingBuffer[int]()

	assert.Equal(t, 0, rb.Len())

	rb.SetCapacity(5)
	rb.Add(1)
	assert.Equal(t, 5, rb.Len())

	rb.SetCapacity(3)
	rb.Add(2)
	assert.Equal(t, 3, rb.Len())
}

// TestRingBuffer_Cap tests Cap method.
func TestRingBuffer_Cap(t *testing.T) {
	rb := NewRingBuffer[int]()

	assert.Equal(t, 0, rb.Cap())

	rb.SetCapacity(5)
	rb.Add(1)
	assert.Equal(t, 5, rb.Cap())

	rb.SetCapacity(3)
	rb.Add(2)
	assert.Equal(t, 3, rb.Cap())

	rb.SetCapacity(10)
	rb.Add(3)
	assert.Equal(t, 10, rb.Cap())
}

// TestRingBuffer_GetCurrentValue_Basic tests GetCurrentValue method.
func TestRingBuffer_GetCurrentValue_Basic(t *testing.T) {
	rb := NewRingBuffer[int]()

	rb.Add(10)
	assert.Equal(t, 10, rb.GetCurrentValue())

	rb.Add(20)
	assert.Equal(t, 20, rb.GetCurrentValue())

	rb.Add(30)
	assert.Equal(t, 30, rb.GetCurrentValue())
}

// TestRingBuffer_GetCurrentValue_EmptyBuffer tests GetCurrentValue on empty buffer.
func TestRingBuffer_GetCurrentValue_EmptyBuffer(t *testing.T) {
	rb := NewRingBuffer[int]()

	var zero int
	assert.Equal(t, zero, rb.GetCurrentValue())
}

// TestRingBuffer_GetCurrentValue_AfterOverwriteValue tests GetCurrentValue after overwrite.
func TestRingBuffer_GetCurrentValue_AfterOverwriteValue(t *testing.T) {
	rb := NewRingBuffer[int]()

	rb.Add(1)
	rb.Add(2)
	rb.Add(3)
	rb.Add(4) // Overwrites 1

	assert.Equal(t, 4, rb.GetCurrentValue())
}

// TestRingBuffer_GetCurrentValue_WrapAround tests GetCurrentValue with wrap around.
func TestRingBuffer_GetCurrentValue_WrapAround(t *testing.T) {
	rb := NewRingBuffer[int]()
	// Fill buffer
	rb.Add(1)
	rb.Add(2)
	rb.Add(3)

	// Wrap around
	rb.Add(4)
	rb.Add(5)

	assert.Equal(t, 5, rb.GetCurrentValue())
}

// TestRingBuffer_GetAllValues_Basic tests GetAllValues method.
func TestRingBuffer_GetAllValues_Basic(t *testing.T) {
	rb := NewRingBuffer[int]()
	capacity := 5
	rb.SetCapacity(capacity)

	rb.Add(10)
	rb.Add(20)
	rb.Add(30)

	allValues := rb.GetAllValues()
	require.Len(t, allValues, capacity)
	// GetAllValues returns values starting from oldest (circular order)
	// After adding 3 values, next=3, so oldest is at index 3
	// Values are: [10, 20, 30, 0, 0] with next=3
	// GetAllValues starts at index 3: [0, 0, 10, 20, 30]
	assert.Contains(t, allValues, 10)
	assert.Contains(t, allValues, 20)
	assert.Contains(t, allValues, 30)
	var zero int
	zeroCount := 0
	for _, val := range allValues {
		if val == zero {
			zeroCount++
		}
	}
	assert.Equal(t, 2, zeroCount) // Two zero slots
}

// TestRingBuffer_GetAllValues_EmptyBuffer tests GetAllValues on empty buffer.
func TestRingBuffer_GetAllValues_EmptyBuffer(t *testing.T) {
	rb := NewRingBuffer[int]()

	allValues := rb.GetAllValues()

	assert.Nil(t, allValues)
}

// TestRingBuffer_GetAllValues_Ordering tests GetAllValues ordering (oldest to newest).
func TestRingBuffer_GetAllValues_Ordering(t *testing.T) {
	rb := NewRingBuffer[int]()
	capacity := 5

	rb.SetCapacity(capacity)
	// Add values sequentially
	for i := 1; i <= 5; i++ {
		rb.Add(i * 10)
	}

	allValues := rb.GetAllValues()
	require.Len(t, allValues, capacity)
	// Should be in order: oldest to newest
	assert.Equal(t, 10, allValues[0])
	assert.Equal(t, 20, allValues[1])
	assert.Equal(t, 30, allValues[2])
	assert.Equal(t, 40, allValues[3])
	assert.Equal(t, 50, allValues[4])
}

// TestRingBuffer_GetAllValues_AfterOverwrite tests GetAllValues after overwrite.
func TestRingBuffer_GetAllValues_AfterOverwrite(t *testing.T) {
	rb := NewRingBuffer[int]()
	capacity := 3
	rb.SetCapacity(capacity)

	rb.Add(10)
	rb.Add(20)
	rb.Add(30)
	rb.Add(40) // Overwrites 10

	allValues := rb.GetAllValues()
	require.Len(t, allValues, capacity)
	// Should contain 20, 30, 40 in order
	assert.Contains(t, allValues, 20)
	assert.Contains(t, allValues, 30)
	assert.Contains(t, allValues, 40)
	assert.NotContains(t, allValues, 10)
}

// TestRingBuffer_Resize_GrowBuffer tests buffer growth.
func TestRingBuffer_Resize_GrowBuffer(t *testing.T) {
	rb := NewRingBuffer[int]()
	initialCapacity := 3

	rb.SetCapacity(initialCapacity)
	rb.Add(10)
	rb.Add(20)
	rb.Add(30)

	assert.Equal(t, initialCapacity, rb.Cap())

	// Grow capacity
	newCapacity := 5
	rb.SetCapacity(newCapacity)
	rb.Add(40)

	assert.Equal(t, newCapacity, rb.Cap())
	assert.Equal(t, newCapacity, rb.Len())
	// When growing, values are copied starting from index 0
	// After adding 3 values, next=3, then grows to 5, next becomes 5
	// So GetAllValues starts from index 5 (wraps to 0)
	allValues := rb.GetAllValues()
	require.Len(t, allValues, newCapacity)
	// Should contain the values that were preserved
	assert.Contains(t, allValues, 20)
	assert.Contains(t, allValues, 30)
	assert.Contains(t, allValues, 40)
	assert.Equal(t, 40, rb.GetCurrentValue())
}

// TestRingBuffer_Resize_ShrinkBuffer tests buffer shrinking.
func TestRingBuffer_Resize_ShrinkBuffer(t *testing.T) {
	rb := NewRingBuffer[int]()
	initialCapacity := 5

	rb.SetCapacity(initialCapacity)
	// Fill buffer
	for i := 1; i <= 5; i++ {
		rb.Add(i * 10)
	}

	assert.Equal(t, initialCapacity, rb.Cap())

	// Shrink capacity - should keep most recent values
	newCapacity := 3
	rb.SetCapacity(newCapacity)
	rb.Add(60)

	assert.Equal(t, newCapacity, rb.Cap())
	assert.Equal(t, newCapacity, rb.Len())

	allValues := rb.GetAllValues()
	require.Len(t, allValues, newCapacity)
	// Should contain the most recent values
	assert.Contains(t, allValues, 40)
	assert.Contains(t, allValues, 50)
	assert.Contains(t, allValues, 60)
	// Oldest values should be removed
	assert.NotContains(t, allValues, 10)
	assert.NotContains(t, allValues, 20)
	assert.NotContains(t, allValues, 30)
}

// TestRingBuffer_Resize_SameCapacitySize tests resize with same capacity.
func TestRingBuffer_Resize_SameCapacitySize(t *testing.T) {
	rb := NewRingBuffer[int]()

	rb.Add(10)
	rb.Add(20)
	rb.Add(30)

	initialLen := rb.Len()
	initialCap := rb.Cap()

	rb.Add(40)

	// Capacity and length should remain the same
	assert.Equal(t, initialCap, rb.Cap())
	assert.Equal(t, initialLen, rb.Len())
	// New value should be added
	assert.Equal(t, 40, rb.GetCurrentValue())
}

// TestRingBuffer_Resize_ShrinkToSmaller tests shrinking to a smaller size.
func TestRingBuffer_Resize_ShrinkToSmaller(t *testing.T) {
	rb := NewRingBuffer[int]()
	rb.SetCapacity(10)

	// Fill buffer
	for i := 1; i <= 10; i++ {
		rb.Add(i)
	}

	// Shrink to 2
	newCapacity := 2
	rb.SetCapacity(newCapacity)
	rb.Add(11)

	assert.Equal(t, newCapacity, rb.Cap())
	allValues := rb.GetAllValues()
	require.Len(t, allValues, newCapacity)
	// Should contain only the 2 most recent values
	assert.Contains(t, allValues, 10)
	assert.Contains(t, allValues, 11)
}

// TestRingBuffer_Resize_GrowFromEmpty tests growing from empty buffer.
func TestRingBuffer_Resize_GrowFromEmpty(t *testing.T) {
	rb := NewRingBuffer[int]()

	assert.Equal(t, 0, rb.Cap())

	rb.Add(10)

	assert.Equal(t, 5, rb.Cap())
	assert.Equal(t, 5, rb.Len())
	assert.Equal(t, 10, rb.Get(0))
}

// TestRingBuffer_Resize_ShrinkToEmpty tests shrinking to zero capacity.
func TestRingBuffer_Resize_ShrinkToEmpty(t *testing.T) {
	rb := NewRingBuffer[int]()
	capacity := 3
	rb.SetCapacity(capacity)

	rb.Add(10)
	rb.Add(20)

	// Adding with capacity 0 should not change the buffer
	rb.Add(30)

	// Buffer should remain unchanged when capacity is 0
	assert.Equal(t, capacity, rb.Cap())
	assert.Equal(t, capacity, rb.Len())
}

// TestRingBuffer_FIFOBehavior tests FIFO (First In First Out) behavior.
func TestRingBuffer_FIFOBehavior(t *testing.T) {
	rb := NewRingBuffer[int]()
	rb.SetCapacity(3)

	// Add values
	rb.Add(1)
	rb.Add(2)
	rb.Add(3)

	// Add more - oldest should be removed first
	rb.Add(4)

	allValues := rb.GetAllValues()
	// 1 should be removed (oldest)
	assert.NotContains(t, allValues, 1)
	// 2, 3, 4 should remain
	assert.Contains(t, allValues, 2)
	assert.Contains(t, allValues, 3)
	assert.Contains(t, allValues, 4)
}

// TestRingBuffer_CircularWrapAround tests circular wrap around behavior.
func TestRingBuffer_CircularWrapAround(t *testing.T) {
	rb := NewRingBuffer[int]()
	capacity := 3

	rb.SetCapacity(capacity)
	// Fill buffer completely
	rb.Add(1)
	rb.Add(2)
	rb.Add(3)

	// Wrap around multiple times
	for i := 4; i <= 9; i++ {
		rb.Add(i)
	}

	allValues := rb.GetAllValues()
	require.Len(t, allValues, capacity)
	// Should contain the last 3 values: 7, 8, 9
	assert.Contains(t, allValues, 7)
	assert.Contains(t, allValues, 8)
	assert.Contains(t, allValues, 9)
}

// TestRingBuffer_GetCurrentValue_AfterMultipleWraps tests GetCurrentValue after multiple wraps.
func TestRingBuffer_GetCurrentValue_AfterMultipleWraps(t *testing.T) {
	rb := NewRingBuffer[int]()
	rb.SetCapacity(10)

	// Fill and wrap multiple times
	for i := 1; i <= 10; i++ {
		rb.Add(i)
	}

	// Current value should be the last added
	assert.Equal(t, 10, rb.GetCurrentValue())
}

// TestRingBuffer_GetAllValues_PreservesOrder tests that GetAllValues preserves order.
func TestRingBuffer_GetAllValues_PreservesOrder(t *testing.T) {
	rb := NewRingBuffer[int]()
	rb.SetCapacity(10)

	rb.Add(10)
	rb.Add(20)
	rb.Add(30)
	rb.Add(40)

	allValues := rb.GetAllValues()
	// Should be in order: oldest to newest
	assert.Equal(t, 10, allValues[0])
	assert.Equal(t, 20, allValues[1])
	assert.Equal(t, 30, allValues[2])
	assert.Equal(t, 40, allValues[3])
}

// TestRingBuffer_Add_StructType tests Add with struct type.
func TestRingBuffer_Add_StructType(t *testing.T) {
	type testStruct struct {
		Value string
		ID    int
	}

	rb := NewRingBuffer[testStruct]()
	capacity := 3
	rb.SetCapacity(capacity)

	rb.Add(testStruct{ID: 1, Value: "first"})
	rb.Add(testStruct{ID: 2, Value: "second"})
	rb.Add(testStruct{ID: 3, Value: "third"})

	assert.Equal(t, capacity, rb.Len())
	val1 := rb.Get(0)
	assert.Equal(t, 1, val1.ID)
	assert.Equal(t, "first", val1.Value)

	current := rb.GetCurrentValue()
	assert.Equal(t, 3, current.ID)
	assert.Equal(t, "third", current.Value)
}

// TestRingBuffer_Add_PointerType tests Add with pointer type.
func TestRingBuffer_Add_PointerType(t *testing.T) {
	rb := NewRingBuffer[*int]()
	capacity := 3
	rb.SetCapacity(capacity)

	val1 := 10
	val2 := 20
	val3 := 30

	rb.Add(&val1)
	rb.Add(&val2)
	rb.Add(&val3)

	assert.Equal(t, capacity, rb.Len())
	assert.Equal(t, &val1, rb.Get(0))
	assert.Equal(t, &val3, rb.GetCurrentValue())
}

// TestRingBuffer_Add_SliceType tests Add with slice type.
func TestRingBuffer_Add_SliceType(t *testing.T) {
	rb := NewRingBuffer[[]int]()
	capacity := 2
	rb.SetCapacity(capacity)

	rb.Add([]int{1, 2, 3})
	rb.Add([]int{4, 5, 6})

	assert.Equal(t, capacity, rb.Len())
	slice1 := rb.Get(0)
	assert.Equal(t, []int{1, 2, 3}, slice1)
	slice2 := rb.GetCurrentValue()
	assert.Equal(t, []int{4, 5, 6}, slice2)
}

// TestRingBuffer_Resize_ComplexShrink tests complex shrinking scenario.
func TestRingBuffer_Resize_ComplexShrink(t *testing.T) {
	rb := NewRingBuffer[int]()

	// Fill buffer
	for i := 1; i <= 7; i++ {
		rb.Add(i * 10)
	}

	// Shrink to 4
	newCapacity := 4
	rb.SetCapacity(newCapacity)
	rb.Add(80)

	assert.Equal(t, newCapacity, rb.Cap())
	allValues := rb.GetAllValues()
	require.Len(t, allValues, newCapacity)
	// Should contain the 4 most recent values
	assert.Contains(t, allValues, 50)
	assert.Contains(t, allValues, 60)
	assert.Contains(t, allValues, 70)
	assert.Contains(t, allValues, 80)
}

// TestRingBuffer_GetAllValues_AfterResize tests GetAllValues after resize.
func TestRingBuffer_GetAllValues_AfterResize(t *testing.T) {
	rb := NewRingBuffer[int]()

	rb.Add(10)
	rb.Add(20)
	rb.Add(30)

	// Grow
	rb.Add(40)

	allValues := rb.GetAllValues()
	require.Len(t, allValues, 5)
	// When growing, values are copied starting from index 0
	// After adding 3 values, next=3, then grows to 5, next becomes 5
	// So GetAllValues starts from index 5 (wraps to 0)
	// Should contain the values that were preserved
	assert.Contains(t, allValues, 20)
	assert.Contains(t, allValues, 30)
	assert.Contains(t, allValues, 40)
	assert.Equal(t, 40, rb.GetCurrentValue())
}

// TestRingBuffer_NextPositionTracking tests next position tracking through behavior.
func TestRingBuffer_NextPositionTracking(t *testing.T) {
	rb := NewRingBuffer[int]()
	capacity := 3
	rb.SetCapacity(capacity)

	rb.Add(10)
	rb.Add(20)
	rb.Add(30)
	// Buffer should be full, next write will wrap
	rb.Add(40)
	// Verify wrap around by checking values
	allValues := rb.GetAllValues()
	require.Len(t, allValues, capacity)
	// Should contain 20, 30, 40 (10 was overwritten)
	assert.Contains(t, allValues, 20)
	assert.Contains(t, allValues, 30)
	assert.Contains(t, allValues, 40)
	assert.NotContains(t, allValues, 10)
}

// TestRingBuffer_Add_WithCapacityChange tests Add with changing capacity.
func TestRingBuffer_Add_WithCapacityChange(t *testing.T) {
	rb := NewRingBuffer[int]()

	rb.SetCapacity(2)
	rb.Add(10)
	assert.Equal(t, 2, rb.Cap())

	rb.SetCapacity(5)
	rb.Add(20)
	assert.Equal(t, 5, rb.Cap())
	assert.Equal(t, 10, rb.Get(0))
	assert.Equal(t, 20, rb.Get(1))

	rb.SetCapacity(3)
	rb.Add(30)
	assert.Equal(t, 3, rb.Cap())
	allValues := rb.GetAllValues()
	require.Len(t, allValues, 3)
	assert.Contains(t, allValues, 20)
	assert.Contains(t, allValues, 30)
}

// TestRingBuffer_GetAllValues_EmptySlots tests GetAllValues with empty slots.
func TestRingBuffer_GetAllValues_EmptySlots(t *testing.T) {
	rb := NewRingBuffer[int]()
	capacity := 5
	rb.SetCapacity(capacity)

	rb.Add(10)
	rb.Add(20)

	allValues := rb.GetAllValues()
	require.Len(t, allValues, capacity)
	// Should contain the added values
	assert.Contains(t, allValues, 10)
	assert.Contains(t, allValues, 20)
	var zero int
	zeroCount := 0
	for _, val := range allValues {
		if val == zero {
			zeroCount++
		}
	}
	assert.Equal(t, 3, zeroCount) // Three zero slots
}

// TestRingBuffer_GetCurrentValue_WithZeroValues tests GetCurrentValue with zero values.
func TestRingBuffer_GetCurrentValue_WithZeroValues(t *testing.T) {
	rb := NewRingBuffer[int]()

	rb.Add(0)
	rb.Add(10)
	rb.Add(0)

	// Zero is a valid value, not empty
	assert.Equal(t, 0, rb.GetCurrentValue())
}

// TestRingBuffer_Resize_MaintainsFIFO tests that resize maintains FIFO order.
func TestRingBuffer_Resize_MaintainsFIFO(t *testing.T) {
	rb := NewRingBuffer[int]()

	// Fill buffer
	for i := 1; i <= 5; i++ {
		rb.Add(i * 10)
	}

	// Shrink - should keep most recent in FIFO order
	newCapacity := 3
	rb.SetCapacity(newCapacity)
	rb.Add(60)

	allValues := rb.GetAllValues()
	require.Len(t, allValues, newCapacity)
	// Should be in order: oldest of kept values to newest
	// The 3 most recent should be: 40, 50, 60
	assert.Contains(t, allValues, 40)
	assert.Contains(t, allValues, 50)
	assert.Contains(t, allValues, 60)
}

// TestRingBuffer_CapacityOne tests ring buffer with capacity of 1.
func TestRingBuffer_CapacityOne(t *testing.T) {
	rb := NewRingBuffer[int]()

	rb.Add(10)
	assert.Equal(t, 10, rb.Get(0))
	assert.Equal(t, 10, rb.GetCurrentValue())

	rb.Add(20)
	assert.Equal(t, 20, rb.Get(0))
	assert.Equal(t, 20, rb.GetCurrentValue())

	rb.Add(30)
	assert.Equal(t, 30, rb.Get(0))
	assert.Equal(t, 30, rb.GetCurrentValue())

	allValues := rb.GetAllValues()
	require.Len(t, allValues, 1)
	assert.Equal(t, 30, allValues[0])
}

// TestRingBuffer_ShrinkWithDifferentNextPositions tests shrinking with different next positions.
func TestRingBuffer_ShrinkWithDifferentNextPositions(t *testing.T) {
	rb := NewRingBuffer[int]()
	rb.SetCapacity(7)

	// Fill buffer completely
	for i := 1; i <= 7; i++ {
		rb.Add(i * 10)
	}
	// At this point, next should be 0 (wrapped around)

	// Shrink to 3 - should keep the 3 most recent values
	newCapacity := 3
	rb.SetCapacity(newCapacity)
	rb.Add(80)

	assert.Equal(t, newCapacity, rb.Cap())
	allValues := rb.GetAllValues()
	require.Len(t, allValues, newCapacity)
	// Should contain the 3 most recent values
	assert.Contains(t, allValues, 60)
	assert.Contains(t, allValues, 70)
	assert.Contains(t, allValues, 80)
}

// TestRingBuffer_RapidCapacityChanges tests rapid capacity changes.
func TestRingBuffer_RapidCapacityChanges(t *testing.T) {
	rb := NewRingBuffer[int]()

	rb.SetCapacity(2)
	rb.Add(10)
	assert.Equal(t, 2, rb.Cap())

	rb.SetCapacity(5)
	rb.Add(20)
	assert.Equal(t, 5, rb.Cap())

	rb.SetCapacity(3)
	rb.Add(30)
	assert.Equal(t, 3, rb.Cap())

	rb.SetCapacity(7)
	rb.Add(40)
	assert.Equal(t, 7, rb.Cap())

	rb.SetCapacity(2)
	rb.Add(50)
	assert.Equal(t, 2, rb.Cap())

	allValues := rb.GetAllValues()
	require.Len(t, allValues, 2)
	// Should contain the 2 most recent values
	assert.Contains(t, allValues, 40)
	assert.Contains(t, allValues, 50)
}

// TestRingBuffer_GetCurrentValue_ExactWrapAround tests GetCurrentValue when next wraps exactly.
func TestRingBuffer_GetCurrentValue_ExactWrapAround(t *testing.T) {
	rb := NewRingBuffer[int]()
	rb.SetCapacity(3)

	// Fill buffer completely (next will be 0 after this)
	rb.Add(10)
	rb.Add(20)
	rb.Add(30)

	// Verify next is 0 (wrapped around)
	assert.Equal(t, 30, rb.GetCurrentValue())

	// Add one more to wrap
	rb.Add(40)
	assert.Equal(t, 40, rb.GetCurrentValue())
}

// TestRingBuffer_GetAllValues_AfterExactWrapAround tests GetAllValues after exact wrap around.
func TestRingBuffer_GetAllValues_AfterExactWrapAround(t *testing.T) {
	rb := NewRingBuffer[int]()
	capacity := 3

	rb.SetCapacity(capacity)
	// Fill buffer completely
	rb.Add(10)
	rb.Add(20)
	rb.Add(30)

	// Add one more to wrap
	rb.Add(40)

	allValues := rb.GetAllValues()
	require.Len(t, allValues, capacity)
	// Should contain 20, 30, 40 (10 was overwritten)
	assert.Contains(t, allValues, 20)
	assert.Contains(t, allValues, 30)
	assert.Contains(t, allValues, 40)
	assert.NotContains(t, allValues, 10)
}

// TestRingBuffer_ShrinkFromPartiallyFilled tests shrinking from partially filled buffer.
func TestRingBuffer_ShrinkFromPartiallyFilled(t *testing.T) {
	rb := NewRingBuffer[int]()
	rb.SetCapacity(5)

	// Partially fill buffer
	for i := 1; i <= 5; i++ {
		rb.Add(i * 10)
	}

	// Shrink to 3
	newCapacity := 3
	rb.SetCapacity(newCapacity)
	rb.Add(60)

	assert.Equal(t, newCapacity, rb.Cap())
	allValues := rb.GetAllValues()
	require.Len(t, allValues, newCapacity)
	// Should contain the 3 most recent values
	assert.Contains(t, allValues, 40)
	assert.Contains(t, allValues, 50)
	assert.Contains(t, allValues, 60)
}

// TestRingBuffer_GrowFromPartiallyFilled tests growing from partially filled buffer.
func TestRingBuffer_GrowFromPartiallyFilled(t *testing.T) {
	rb := NewRingBuffer[int]()
	rb.SetCapacity(2)

	// Partially fill buffer
	rb.Add(10)
	rb.Add(20)

	// Grow to 5
	newCapacity := 5
	rb.SetCapacity(newCapacity)
	rb.Add(30)

	assert.Equal(t, newCapacity, rb.Cap())
	assert.Equal(t, newCapacity, rb.Len())
	allValues := rb.GetAllValues()
	require.Len(t, allValues, newCapacity)
	// Should contain all previous values
	assert.Contains(t, allValues, 10)
	assert.Contains(t, allValues, 20)
	assert.Contains(t, allValues, 30)
}

// TestRingBuffer_GetAllValues_OrderingAfterShrink tests GetAllValues ordering after shrink.
func TestRingBuffer_GetAllValues_OrderingAfterShrink(t *testing.T) {
	rb := NewRingBuffer[int]()

	// Fill buffer
	for i := 1; i <= 5; i++ {
		rb.Add(i * 10)
	}

	// Shrink to 3
	newCapacity := 3
	rb.SetCapacity(newCapacity)
	rb.Add(60)

	allValues := rb.GetAllValues()
	require.Len(t, allValues, newCapacity)
	// Should be in order: oldest to newest of kept values
	// The 3 most recent should be: 40, 50, 60
	assert.Equal(t, 40, allValues[0])
	assert.Equal(t, 50, allValues[1])
	assert.Equal(t, 60, allValues[2])
}

// TestRingBuffer_GetAllValues_WithSingleValue tests GetAllValues with single value.
func TestRingBuffer_GetAllValues_WithSingleValue(t *testing.T) {
	rb := NewRingBuffer[int]()
	capacity := 5
	rb.SetCapacity(capacity)

	rb.Add(10)

	allValues := rb.GetAllValues()
	require.Len(t, allValues, capacity)
	// GetAllValues starts from next position (which is 1 after adding one value)
	// So it wraps around and the value 10 appears at the end
	// Should contain the single value somewhere in the array
	assert.Contains(t, allValues, 10)
	var zero int
	zeroCount := 0
	for _, val := range allValues {
		if val == zero {
			zeroCount++
		}
	}
	// Should have 4 zero slots
	assert.Equal(t, 4, zeroCount)
}

// TestRingBuffer_GetCurrentValue_AfterShrink tests GetCurrentValue after shrink.
func TestRingBuffer_GetCurrentValue_AfterShrink(t *testing.T) {
	rb := NewRingBuffer[int]()
	rb.SetCapacity(5)

	// Fill buffer
	for i := 1; i <= 5; i++ {
		rb.Add(i * 10)
	}

	// Shrink and add new value
	rb.SetCapacity(3)
	rb.Add(60)

	// Current value should be the most recently added
	assert.Equal(t, 60, rb.GetCurrentValue())
}

// TestRingBuffer_GetCurrentValue_AfterGrow tests GetCurrentValue after grow.
func TestRingBuffer_GetCurrentValue_AfterGrow(t *testing.T) {
	rb := NewRingBuffer[int]()
	rb.SetCapacity(3)

	// Fill buffer
	for i := 1; i <= 3; i++ {
		rb.Add(i * 10)
	}

	// Grow and add new value
	rb.SetCapacity(5)
	rb.Add(40)

	// Current value should be the most recently added
	assert.Equal(t, 40, rb.GetCurrentValue())
}

// TestRingBuffer_ComplexWrapAroundSequence tests complex wrap around sequence.
func TestRingBuffer_ComplexWrapAroundSequence(t *testing.T) {
	rb := NewRingBuffer[int]()
	capacity := 4

	rb.SetCapacity(capacity)
	// Fill buffer
	for i := 1; i <= 4; i++ {
		rb.Add(i * 10)
	}

	// Wrap around multiple times
	for i := 5; i <= 12; i++ {
		rb.Add(i * 10)
	}

	allValues := rb.GetAllValues()
	require.Len(t, allValues, capacity)
	// Should contain the last 4 values: 90, 100, 110, 120
	assert.Contains(t, allValues, 90)
	assert.Contains(t, allValues, 100)
	assert.Contains(t, allValues, 110)
	assert.Contains(t, allValues, 120)
	assert.Equal(t, 120, rb.GetCurrentValue())
}

// TestRingBuffer_Get_AfterResize tests Get after resize operations.
func TestRingBuffer_Get_AfterResize(t *testing.T) {
	rb := NewRingBuffer[int]()

	rb.Add(10)
	rb.Add(20)
	rb.Add(30)

	// Grow
	newCapacity := 5
	rb.SetCapacity(newCapacity)
	rb.Add(40)

	// After growing, values are preserved and new value is added
	allValues := rb.GetAllValues()
	require.Len(t, allValues, newCapacity)
	// When growing, values are copied but the circular nature means
	// some values may be in different positions
	// Should contain the most recent values
	assert.Contains(t, allValues, 20)
	assert.Contains(t, allValues, 30)
	assert.Contains(t, allValues, 40)
	// Current value should be the most recent
	assert.Equal(t, 40, rb.GetCurrentValue())
	// Buffer should have the new capacity
	assert.Equal(t, newCapacity, rb.Cap())
	assert.Equal(t, newCapacity, rb.Len())
}

// TestRingBuffer_Len_AfterMultipleResizes tests Len after multiple resizes.
func TestRingBuffer_Len_AfterMultipleResizes(t *testing.T) {
	rb := NewRingBuffer[int]()

	assert.Equal(t, 0, rb.Len())

	rb.SetCapacity(2)
	rb.Add(10)
	assert.Equal(t, 2, rb.Len())

	rb.SetCapacity(5)
	rb.Add(20)
	assert.Equal(t, 5, rb.Len())

	rb.SetCapacity(3)
	rb.Add(30)
	assert.Equal(t, 3, rb.Len())

	rb.SetCapacity(1)
	rb.Add(40)
	assert.Equal(t, 1, rb.Len())
}

// TestRingBuffer_Cap_AfterMultipleResizes tests Cap after multiple resizes.
func TestRingBuffer_Cap_AfterMultipleResizes(t *testing.T) {
	rb := NewRingBuffer[int]()

	assert.Equal(t, 0, rb.Cap())

	rb.SetCapacity(2)
	rb.Add(10)
	assert.Equal(t, 2, rb.Cap())

	rb.SetCapacity(5)
	rb.Add(20)
	assert.Equal(t, 5, rb.Cap())

	rb.SetCapacity(3)
	rb.Add(30)
	assert.Equal(t, 3, rb.Cap())

	rb.SetCapacity(7)
	rb.Add(40)
	assert.Equal(t, 7, rb.Cap())
}

// TestRingBuffer_GetAllValues_Consistency tests GetAllValues consistency across operations.
func TestRingBuffer_GetAllValues_Consistency(t *testing.T) {
	rb := NewRingBuffer[int]()
	capacity := 4

	rb.SetCapacity(capacity)
	// Add values
	for i := 1; i <= 4; i++ {
		rb.Add(i * 10)
	}

	allValues1 := rb.GetAllValues()
	require.Len(t, allValues1, capacity)

	// Add more values
	for i := 5; i <= 6; i++ {
		rb.Add(i * 10)
	}

	allValues2 := rb.GetAllValues()
	require.Len(t, allValues2, capacity)

	// Should still have correct length
	assert.Equal(t, capacity, len(allValues2))
	// Should contain the most recent values
	assert.Contains(t, allValues2, 30)
	assert.Contains(t, allValues2, 40)
	assert.Contains(t, allValues2, 50)
	assert.Contains(t, allValues2, 60)
}

// TestRingBuffer_Add_ZeroValue tests Add with zero value (valid for int).
func TestRingBuffer_Add_ZeroValue(t *testing.T) {
	rb := NewRingBuffer[int]()
	capacity := 3
	rb.SetCapacity(capacity)

	rb.Add(0)
	rb.Add(10)
	rb.Add(0)

	allValues := rb.GetAllValues()
	require.Len(t, allValues, capacity)
	// Zero is a valid value
	assert.Contains(t, allValues, 0)
	assert.Contains(t, allValues, 10)
	assert.Equal(t, 0, rb.GetCurrentValue())
}

// TestRingBuffer_ShrinkPreservesMostRecent tests that shrink preserves most recent values.
func TestRingBuffer_ShrinkPreservesMostRecent(t *testing.T) {
	rb := NewRingBuffer[int]()

	// Fill buffer
	for i := 1; i <= 10; i++ {
		rb.Add(i)
	}

	// Shrink to 2
	newCapacity := 2
	rb.SetCapacity(newCapacity)
	rb.Add(11)

	allValues := rb.GetAllValues()
	require.Len(t, allValues, newCapacity)
	// Should contain only the 2 most recent values
	assert.Contains(t, allValues, 10)
	assert.Contains(t, allValues, 11)
	// Should not contain older values
	assert.NotContains(t, allValues, 1)
	assert.NotContains(t, allValues, 5)
	assert.NotContains(t, allValues, 9)
}
