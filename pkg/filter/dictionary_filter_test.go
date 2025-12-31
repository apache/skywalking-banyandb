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

package filter

import (
	stdbytes "bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

const (
	entityDelimiter = '|'
	escape          = '\\'
)

func marshalVarArray(dest, src []byte) []byte {
	if stdbytes.IndexByte(src, entityDelimiter) < 0 && stdbytes.IndexByte(src, escape) < 0 {
		dest = append(dest, src...)
		dest = append(dest, entityDelimiter)
		return dest
	}
	for _, b := range src {
		if b == entityDelimiter || b == escape {
			dest = append(dest, escape)
		}
		dest = append(dest, b)
	}
	dest = append(dest, entityDelimiter)
	return dest
}

func TestDictionaryFilter(t *testing.T) {
	assert := assert.New(t)

	items := [][]byte{
		[]byte("skywalking"),
		[]byte("banyandb"),
		[]byte(""),
		[]byte("hello"),
		[]byte("world"),
	}

	df := &DictionaryFilter{}
	df.Set(items[:3], pbv1.ValueTypeStr)
	assert.NotNil(df)

	for i := 0; i < 3; i++ {
		mightContain := df.MightContain(items[i])
		assert.True(mightContain)
	}

	for i := 3; i < 5; i++ {
		mightContain := df.MightContain(items[i])
		assert.False(mightContain)
	}
}

func TestDictionaryFilterResetClearsValues(t *testing.T) {
	assert := assert.New(t)

	key := []byte("reuse-key")

	df := &DictionaryFilter{}
	df.Set([][]byte{key}, pbv1.ValueTypeStr)
	assert.True(df.MightContain(key))

	df.Reset()
	assert.False(df.MightContain(key))
}

func TestDictionaryFilterInt64Arr(t *testing.T) {
	assert := assert.New(t)

	// Stored array: [1, 2, 3] (sorted at write time)
	items := []int64{1, 2, 3}
	dst := make([]byte, 0, 24)
	for _, item := range items {
		dst = append(dst, convert.Int64ToBytes(item)...)
	}

	df := &DictionaryFilter{}
	df.Set([][]byte{dst}, pbv1.ValueTypeInt64Arr)

	// Array types don't support MightContain - should always return false
	for _, item := range items {
		query := convert.Int64ToBytes(item)
		assert.False(df.MightContain(query), "MightContain should return false for array types, element %d", item)
	}

	// Test elements that don't exist - should also return false
	notFound := []int64{4, 5}
	for _, item := range notFound {
		query := convert.Int64ToBytes(item)
		assert.False(df.MightContain(query), "MightContain should return false for array types, element %d", item)
	}
}

func TestDictionaryFilterStrArr(t *testing.T) {
	assert := assert.New(t)

	// Stored array: ["", "banyandb", "skywalking"] (must be sorted like write path does)
	items := [][]byte{
		[]byte(""),
		[]byte("banyandb"),
		[]byte("skywalking"),
	}
	dst := make([]byte, 0)
	for _, item := range items {
		dst = marshalVarArray(dst, item)
	}

	df := &DictionaryFilter{}
	df.Set([][]byte{dst}, pbv1.ValueTypeStrArr)

	// Array types don't support MightContain - should always return false
	for _, item := range items {
		assert.False(df.MightContain(item), "MightContain should return false for array types, element %s", string(item))
	}

	// Test elements that don't exist - should also return false
	notFound := [][]byte{
		[]byte("hello"),
		[]byte("world"),
	}
	for _, item := range notFound {
		assert.False(df.MightContain(item), "MightContain should return false for array types, element %s", string(item))
	}
}

func TestDictionaryFilterContainsAll_NonArray_ANDSemantics(t *testing.T) {
	assert := assert.New(t)

	// Set up filter with some values
	items := [][]byte{
		[]byte("skywalking"),
		[]byte("banyandb"),
		[]byte("hello"),
		[]byte("world"),
	}

	df := &DictionaryFilter{}
	df.Set(items, pbv1.ValueTypeStr)

	// Empty items should return true
	assert.True(df.ContainsAll([][]byte{}), "empty items should return true")

	// Multiple items that all exist - should return true (AND semantics)
	assert.True(df.ContainsAll([][]byte{
		[]byte("skywalking"),
		[]byte("banyandb"),
	}), "multiple existing items should return true for non-array types")

	// Multiple items with one missing - should return false
	assert.False(df.ContainsAll([][]byte{
		[]byte("skywalking"),
		[]byte("nonexistent"),
	}), "multiple items should return false if any item is missing")

	// Single item exists - should return true
	assert.True(df.ContainsAll([][]byte{
		[]byte("hello"),
	}), "single existing item should return true")

	// Multiple items that don't exist - should return false
	assert.False(df.ContainsAll([][]byte{
		[]byte("nonexistent1"),
		[]byte("nonexistent2"),
	}), "multiple non-existing items should return false")

	// Single item doesn't exist - should return false
	assert.False(df.ContainsAll([][]byte{
		[]byte("nonexistent"),
	}), "single non-existing item should return false")
}

func TestDictionaryFilterContainsAll_Int64Arr_ANDSemantics(t *testing.T) {
	assert := assert.New(t)

	// Create multiple arrays
	arrays := make([][]byte, 3)
	// Array 0: [1, 2, 3]
	arrays[0] = append(arrays[0], convert.Int64ToBytes(1)...)
	arrays[0] = append(arrays[0], convert.Int64ToBytes(2)...)
	arrays[0] = append(arrays[0], convert.Int64ToBytes(3)...)
	// Array 1: [4, 5, 6]
	arrays[1] = append(arrays[1], convert.Int64ToBytes(4)...)
	arrays[1] = append(arrays[1], convert.Int64ToBytes(5)...)
	arrays[1] = append(arrays[1], convert.Int64ToBytes(6)...)
	// Array 2: [7, 8, 9]
	arrays[2] = append(arrays[2], convert.Int64ToBytes(7)...)
	arrays[2] = append(arrays[2], convert.Int64ToBytes(8)...)
	arrays[2] = append(arrays[2], convert.Int64ToBytes(9)...)

	df := &DictionaryFilter{}
	df.Set(arrays, pbv1.ValueTypeInt64Arr)

	// Empty items should return true
	assert.True(df.ContainsAll([][]byte{}), "empty items should return true")

	// All items form a subset of array 0 - should return true (AND semantics)
	assert.True(df.ContainsAll([][]byte{
		convert.Int64ToBytes(1),
		convert.Int64ToBytes(2),
	}), "all items form subset should return true")

	// All items form a subset (full match) - should return true
	assert.True(df.ContainsAll([][]byte{
		convert.Int64ToBytes(1),
		convert.Int64ToBytes(2),
		convert.Int64ToBytes(3),
	}), "all items form full subset should return true")

	// Single item that exists - should return true
	assert.True(df.ContainsAll([][]byte{
		convert.Int64ToBytes(5),
	}), "single existing item should return true")

	// Some items exist but not all form a subset - should return false (AND semantics)
	assert.False(df.ContainsAll([][]byte{
		convert.Int64ToBytes(1),
		convert.Int64ToBytes(4), // exists but in different array
	}), "items from different arrays should return false (AND semantics)")

	// None form a subset - should return false
	assert.False(df.ContainsAll([][]byte{
		convert.Int64ToBytes(10),
		convert.Int64ToBytes(11),
	}), "no items form subset should return false")

	// Single item doesn't exist - should return false
	assert.False(df.ContainsAll([][]byte{
		convert.Int64ToBytes(99),
	}), "single non-existing item should return false")

	// Partial subset (some elements match but not all) - should return false
	assert.False(df.ContainsAll([][]byte{
		convert.Int64ToBytes(1),
		convert.Int64ToBytes(2),
		convert.Int64ToBytes(10), // doesn't exist in any array
	}), "partial subset should return false")
}

func TestDictionaryFilterContainsAll_StrArr_ANDSemantics(t *testing.T) {
	assert := assert.New(t)

	// Create multiple arrays
	arrays := make([][]byte, 3)
	// Array 0: ["apple", "banana", "cherry"]
	arrays[0] = marshalVarArray(arrays[0], []byte("apple"))
	arrays[0] = marshalVarArray(arrays[0], []byte("banana"))
	arrays[0] = marshalVarArray(arrays[0], []byte("cherry"))
	// Array 1: ["date", "elderberry", "fig"]
	arrays[1] = marshalVarArray(arrays[1], []byte("date"))
	arrays[1] = marshalVarArray(arrays[1], []byte("elderberry"))
	arrays[1] = marshalVarArray(arrays[1], []byte("fig"))
	// Array 2: ["grape", "honeydew", "kiwi"]
	arrays[2] = marshalVarArray(arrays[2], []byte("grape"))
	arrays[2] = marshalVarArray(arrays[2], []byte("honeydew"))
	arrays[2] = marshalVarArray(arrays[2], []byte("kiwi"))

	df := &DictionaryFilter{}
	df.Set(arrays, pbv1.ValueTypeStrArr)

	// Empty items should return true
	assert.True(df.ContainsAll([][]byte{}), "empty items should return true")

	// All items form a subset of array 0 - should return true (AND semantics)
	// Note: ContainsAll expects individual string elements, not serialized arrays
	assert.True(df.ContainsAll([][]byte{
		[]byte("apple"),
		[]byte("banana"),
	}), "all items form subset should return true")

	// All items form a subset (full match) - should return true
	assert.True(df.ContainsAll([][]byte{
		[]byte("apple"),
		[]byte("banana"),
		[]byte("cherry"),
	}), "all items form full subset should return true")

	// Single item that exists - should return true
	assert.True(df.ContainsAll([][]byte{
		[]byte("elderberry"),
	}), "single existing item should return true")

	// Some items exist but not all form a subset - should return false (AND semantics)
	assert.False(df.ContainsAll([][]byte{
		[]byte("apple"),
		[]byte("date"), // exists but in different array
	}), "items from different arrays should return false (AND semantics)")

	// None form a subset - should return false
	assert.False(df.ContainsAll([][]byte{
		[]byte("zebra"),
		[]byte("yak"),
	}), "no items form subset should return false")

	// Single item doesn't exist - should return false
	assert.False(df.ContainsAll([][]byte{
		[]byte("nonexistent"),
	}), "single non-existing item should return false")

	// Partial subset (some elements match but not all) - should return false
	assert.False(df.ContainsAll([][]byte{
		[]byte("apple"),
		[]byte("banana"),
		[]byte("nonexistent"), // doesn't exist in any array
	}), "partial subset should return false")
}

func TestDictionaryFilterContainsAll_EmptyFilter(t *testing.T) {
	assert := assert.New(t)

	df := &DictionaryFilter{}
	df.Set([][]byte{}, pbv1.ValueTypeStr)

	// Empty items should return true even with empty filter
	assert.True(df.ContainsAll([][]byte{}), "empty items should return true")

	// Any items should return false with empty filter
	assert.False(df.ContainsAll([][]byte{
		[]byte("any"),
	}), "any items should return false with empty filter")
}

func TestDictionaryFilterContainsAll_ResetBehavior(t *testing.T) {
	assert := assert.New(t)

	df := &DictionaryFilter{}
	df.Set([][]byte{[]byte("test")}, pbv1.ValueTypeStr)
	assert.True(df.ContainsAll([][]byte{[]byte("test")}), "should contain before reset")

	df.Reset()
	assert.False(df.ContainsAll([][]byte{[]byte("test")}), "should not contain after reset")
}

func TestDictionaryFilterContainsAll_UnsortedItems(t *testing.T) {
	assert := assert.New(t)

	// Test that unsorted items are handled correctly (should be sorted internally)
	// Array: [1, 2, 3, 4, 5]
	arrays := make([][]byte, 1)
	arrays[0] = append(arrays[0], convert.Int64ToBytes(1)...)
	arrays[0] = append(arrays[0], convert.Int64ToBytes(2)...)
	arrays[0] = append(arrays[0], convert.Int64ToBytes(3)...)
	arrays[0] = append(arrays[0], convert.Int64ToBytes(4)...)
	arrays[0] = append(arrays[0], convert.Int64ToBytes(5)...)

	df := &DictionaryFilter{}
	df.Set(arrays, pbv1.ValueTypeInt64Arr)

	// Test with unsorted items - should still work
	assert.True(df.ContainsAll([][]byte{
		convert.Int64ToBytes(3),
		convert.Int64ToBytes(1),
		convert.Int64ToBytes(2),
	}), "unsorted items should be sorted internally and work correctly")

	// Test with unsorted string array items
	strArrays := make([][]byte, 1)
	strArrays[0] = marshalVarArray(strArrays[0], []byte("apple"))
	strArrays[0] = marshalVarArray(strArrays[0], []byte("banana"))
	strArrays[0] = marshalVarArray(strArrays[0], []byte("cherry"))

	df2 := &DictionaryFilter{}
	df2.Set(strArrays, pbv1.ValueTypeStrArr)

	// Test with unsorted items - should still work
	assert.True(df2.ContainsAll([][]byte{
		[]byte("cherry"),
		[]byte("apple"),
		[]byte("banana"),
	}), "unsorted string items should be sorted internally and work correctly")
}

// generateDictionaryValues creates test data with n unique string values.
func generateDictionaryValues(n int) [][]byte {
	values := make([][]byte, n)
	for i := 0; i < n; i++ {
		values[i] = []byte(fmt.Sprintf("value_%d", i))
	}
	return values
}

// generateInt64Values creates test data with n unique int64 values.
func generateInt64Values(n int) [][]byte {
	values := make([][]byte, n)
	for i := 0; i < n; i++ {
		values[i] = convert.Int64ToBytes(int64(i))
	}
	return values
}

// benchmarkDictionaryFilterMightContain is a helper function that benchmarks MightContain
// for a given value type, size, and item existence status.
func benchmarkDictionaryFilterMightContain(b *testing.B, valueType pbv1.ValueType, size int, existing bool) {
	var values [][]byte
	var testItem []byte

	switch valueType {
	case pbv1.ValueTypeStr:
		values = generateDictionaryValues(size)
		if existing {
			// Use middle item for existing
			testItem = values[size/2]
		} else {
			testItem = []byte("not_found")
		}
	case pbv1.ValueTypeInt64:
		values = generateInt64Values(size)
		if existing {
			// Use middle item for existing
			testItem = values[size/2]
		} else {
			testItem = convert.Int64ToBytes(int64(size * 2))
		}
	default:
		b.Fatalf("unsupported value type: %v", valueType)
	}

	df := &DictionaryFilter{}
	df.Set(values, valueType)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		df.MightContain(testItem)
	}
}

// BenchmarkDictionaryFilterMightContain_Str_Small benchmarks string type with 16 values (small dictionary).
func BenchmarkDictionaryFilterMightContain_Str_Small(b *testing.B) {
	b.Run("Existing", func(b *testing.B) {
		benchmarkDictionaryFilterMightContain(b, pbv1.ValueTypeStr, 16, true)
	})
	b.Run("NonExisting", func(b *testing.B) {
		benchmarkDictionaryFilterMightContain(b, pbv1.ValueTypeStr, 16, false)
	})
}

// BenchmarkDictionaryFilterMightContain_Str_Medium benchmarks string type with 128 values.
func BenchmarkDictionaryFilterMightContain_Str_Medium(b *testing.B) {
	b.Run("Existing", func(b *testing.B) {
		benchmarkDictionaryFilterMightContain(b, pbv1.ValueTypeStr, 128, true)
	})
	b.Run("NonExisting", func(b *testing.B) {
		benchmarkDictionaryFilterMightContain(b, pbv1.ValueTypeStr, 128, false)
	})
}

// BenchmarkDictionaryFilterMightContain_Str_Max benchmarks string type with 256 values (max dictionary size).
func BenchmarkDictionaryFilterMightContain_Str_Max(b *testing.B) {
	b.Run("Existing", func(b *testing.B) {
		benchmarkDictionaryFilterMightContain(b, pbv1.ValueTypeStr, 256, true)
	})
	b.Run("NonExisting", func(b *testing.B) {
		benchmarkDictionaryFilterMightContain(b, pbv1.ValueTypeStr, 256, false)
	})
}

// BenchmarkDictionaryFilterMightContain_Int64_Small benchmarks int64 type with 16 values (small dictionary).
func BenchmarkDictionaryFilterMightContain_Int64_Small(b *testing.B) {
	b.Run("Existing", func(b *testing.B) {
		benchmarkDictionaryFilterMightContain(b, pbv1.ValueTypeInt64, 16, true)
	})
	b.Run("NonExisting", func(b *testing.B) {
		benchmarkDictionaryFilterMightContain(b, pbv1.ValueTypeInt64, 16, false)
	})
}

// BenchmarkDictionaryFilterMightContain_Int64_Medium benchmarks int64 type with 128 values.
func BenchmarkDictionaryFilterMightContain_Int64_Medium(b *testing.B) {
	b.Run("Existing", func(b *testing.B) {
		benchmarkDictionaryFilterMightContain(b, pbv1.ValueTypeInt64, 128, true)
	})
	b.Run("NonExisting", func(b *testing.B) {
		benchmarkDictionaryFilterMightContain(b, pbv1.ValueTypeInt64, 128, false)
	})
}

// BenchmarkDictionaryFilterMightContain_Int64_Max benchmarks int64 type with 256 values (max dictionary size).
func BenchmarkDictionaryFilterMightContain_Int64_Max(b *testing.B) {
	b.Run("Existing", func(b *testing.B) {
		benchmarkDictionaryFilterMightContain(b, pbv1.ValueTypeInt64, 256, true)
	})
	b.Run("NonExisting", func(b *testing.B) {
		benchmarkDictionaryFilterMightContain(b, pbv1.ValueTypeInt64, 256, false)
	})
}

// linearSearchMightContain is the old O(n) implementation for comparison.
func linearSearchMightContain(values [][]byte, item []byte) bool {
	for _, v := range values {
		if stdbytes.Equal(v, item) {
			return true
		}
	}
	return false
}

// BenchmarkLinearSearch benchmarks the old linear search implementation for comparison.
func BenchmarkLinearSearch_Max(b *testing.B) {
	values := generateDictionaryValues(256)

	lastItem := values[255]
	nonExistingItem := []byte("not_found")

	var result bool
	b.Run("ExistingLast", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result = linearSearchMightContain(values, lastItem)
		}
	})

	b.Run("NonExisting", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result = linearSearchMightContain(values, nonExistingItem)
		}
	})
	_ = result // Prevent compiler optimization
}

// generateInt64QueryItems creates query items for an int64 array (subset of array elements),
// starting from the given base value.
func generateInt64QueryItems(arrayLen int, percentage int, base int64) [][]byte {
	queryLen := (arrayLen * percentage) / 100
	if queryLen == 0 {
		queryLen = 1
	}
	items := make([][]byte, queryLen)
	for i := 0; i < queryLen; i++ {
		items[i] = convert.Int64ToBytes(base + int64(i))
	}
	return items
}

// generateStrQueryItems creates query items for string array (subset of array elements).
func generateStrQueryItems(arrayLen int, percentage int) [][]byte {
	queryLen := (arrayLen * percentage) / 100
	if queryLen == 0 {
		queryLen = 1
	}
	items := make([][]byte, queryLen)
	for i := 0; i < queryLen; i++ {
		// Must match the stored benchmark pattern used by ValueTypeStrArr:
		// fmt.Sprintf("element_%d_%d", arrayIdx, elemIdx). For "Existing" queries
		// we target the first array (arrayIdx=0).
		items[i] = []byte(fmt.Sprintf("element_%d_%d", 0, i))
	}
	return items
}

// generateInt64QueryItemsNonExisting creates query items that don't exist in any of the arrays.
func generateInt64QueryItemsNonExisting(arrayLen int, percentage int, arrayCount int) [][]byte {
	queryLen := (arrayLen * percentage) / 100
	if queryLen == 0 {
		queryLen = 1
	}
	items := make([][]byte, queryLen)
	// Arrays are populated with contiguous ranges:
	// array i holds values [i*arrayLen, i*arrayLen+arrayLen-1].
	// So values starting at arrayCount*arrayLen are guaranteed to be absent from all arrays.
	start := int64(arrayCount * arrayLen)
	for i := 0; i < queryLen; i++ {
		items[i] = convert.Int64ToBytes(start + int64(i))
	}
	return items
}

// generateStrQueryItemsNonExisting creates query items that don't exist in the array.
func generateStrQueryItemsNonExisting(arrayLen int, percentage int) [][]byte {
	queryLen := (arrayLen * percentage) / 100
	if queryLen == 0 {
		queryLen = 1
	}
	items := make([][]byte, queryLen)
	// Use values that are definitely not in the array
	for i := 0; i < queryLen; i++ {
		items[i] = []byte(fmt.Sprintf("nonexistent_%d", arrayLen+i))
	}
	return items
}

// benchmarkDictionaryFilterContainsAll is a helper function that benchmarks ContainsAll
// for array types with given array count, array length, and query percentage.
// arrayCount is the number of arrays in the dictionary (small=16, medium=128, max=256).
// arrayLen is the number of elements in each array.
func benchmarkDictionaryFilterContainsAll(b *testing.B, valueType pbv1.ValueType, arrayCount int, arrayLen int, percentage int, existing bool) {
	var arrays [][]byte
	var queryItems [][]byte

	switch valueType {
	case pbv1.ValueTypeInt64Arr:
		arrays = make([][]byte, arrayCount)
		for i := 0; i < arrayCount; i++ {
			// Create arrays with different starting values to ensure variety
			arr := make([]byte, 0, arrayLen*8)
			for j := 0; j < arrayLen; j++ {
				arr = append(arr, convert.Int64ToBytes(int64(i*arrayLen+j))...)
			}
			arrays[i] = arr
		}
		// Query items intentionally target the first array (index 0), which contains values [0..arrayLen-1].
		if existing {
			queryItems = generateInt64QueryItems(arrayLen, percentage, 0)
		} else {
			queryItems = generateInt64QueryItemsNonExisting(arrayLen, percentage, arrayCount)
		}
	case pbv1.ValueTypeStrArr:
		arrays = make([][]byte, arrayCount)
		for i := 0; i < arrayCount; i++ {
			// Create arrays with different starting values to ensure variety
			arr := make([]byte, 0)
			for j := 0; j < arrayLen; j++ {
				arr = marshalVarArray(arr, []byte(fmt.Sprintf("element_%d_%d", i, j)))
			}
			arrays[i] = arr
		}
		// Query items should match elements from the first array (index 0)
		if existing {
			queryItems = generateStrQueryItems(arrayLen, percentage)
		} else {
			queryItems = generateStrQueryItemsNonExisting(arrayLen, percentage)
		}
	default:
		b.Fatalf("unsupported value type for ContainsAll benchmark: %v", valueType)
	}

	df := &DictionaryFilter{}
	df.Set(arrays, valueType)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		df.ContainsAll(queryItems)
	}
}

// BenchmarkDictionaryFilterContainsAll_Int64Arr_Small benchmarks ContainsAll for int64 arrays with 16 arrays (small).
func BenchmarkDictionaryFilterContainsAll_Int64Arr_Small(b *testing.B) {
	const arrayCount = 16
	arrayLengths := []int{10, 50, 100, 200, 300, 400, 500}
	percentages := []int{20, 50, 90}

	for _, arrayLen := range arrayLengths {
		for _, pct := range percentages {
			b.Run(fmt.Sprintf("ArrayLen%d_Pct%d_Existing", arrayLen, pct), func(b *testing.B) {
				benchmarkDictionaryFilterContainsAll(b, pbv1.ValueTypeInt64Arr, arrayCount, arrayLen, pct, true)
			})
			b.Run(fmt.Sprintf("ArrayLen%d_Pct%d_NonExisting", arrayLen, pct), func(b *testing.B) {
				benchmarkDictionaryFilterContainsAll(b, pbv1.ValueTypeInt64Arr, arrayCount, arrayLen, pct, false)
			})
		}
	}
}

// BenchmarkDictionaryFilterContainsAll_Int64Arr_Medium benchmarks ContainsAll for int64 arrays with 128 arrays (medium).
func BenchmarkDictionaryFilterContainsAll_Int64Arr_Medium(b *testing.B) {
	const arrayCount = 128
	arrayLengths := []int{10, 50, 100, 200, 300, 400, 500}
	percentages := []int{20, 50, 90}

	for _, arrayLen := range arrayLengths {
		for _, pct := range percentages {
			b.Run(fmt.Sprintf("ArrayLen%d_Pct%d_Existing", arrayLen, pct), func(b *testing.B) {
				benchmarkDictionaryFilterContainsAll(b, pbv1.ValueTypeInt64Arr, arrayCount, arrayLen, pct, true)
			})
			b.Run(fmt.Sprintf("ArrayLen%d_Pct%d_NonExisting", arrayLen, pct), func(b *testing.B) {
				benchmarkDictionaryFilterContainsAll(b, pbv1.ValueTypeInt64Arr, arrayCount, arrayLen, pct, false)
			})
		}
	}
}

// BenchmarkDictionaryFilterContainsAll_Int64Arr_Max benchmarks ContainsAll for int64 arrays with 256 arrays (max).
func BenchmarkDictionaryFilterContainsAll_Int64Arr_Max(b *testing.B) {
	const arrayCount = 256
	arrayLengths := []int{10, 50, 100, 200, 300, 400, 500}
	percentages := []int{20, 50, 90}

	for _, arrayLen := range arrayLengths {
		for _, pct := range percentages {
			b.Run(fmt.Sprintf("ArrayLen%d_Pct%d_Existing", arrayLen, pct), func(b *testing.B) {
				benchmarkDictionaryFilterContainsAll(b, pbv1.ValueTypeInt64Arr, arrayCount, arrayLen, pct, true)
			})
			b.Run(fmt.Sprintf("ArrayLen%d_Pct%d_NonExisting", arrayLen, pct), func(b *testing.B) {
				benchmarkDictionaryFilterContainsAll(b, pbv1.ValueTypeInt64Arr, arrayCount, arrayLen, pct, false)
			})
		}
	}
}

// BenchmarkDictionaryFilterContainsAll_StrArr_Small benchmarks ContainsAll for string arrays with 16 arrays (small).
func BenchmarkDictionaryFilterContainsAll_StrArr_Small(b *testing.B) {
	const arrayCount = 16
	arrayLengths := []int{10, 50, 100, 200, 300, 400, 500}
	percentages := []int{20, 50, 90}

	for _, arrayLen := range arrayLengths {
		for _, pct := range percentages {
			b.Run(fmt.Sprintf("ArrayLen%d_Pct%d_Existing", arrayLen, pct), func(b *testing.B) {
				benchmarkDictionaryFilterContainsAll(b, pbv1.ValueTypeStrArr, arrayCount, arrayLen, pct, true)
			})
			b.Run(fmt.Sprintf("ArrayLen%d_Pct%d_NonExisting", arrayLen, pct), func(b *testing.B) {
				benchmarkDictionaryFilterContainsAll(b, pbv1.ValueTypeStrArr, arrayCount, arrayLen, pct, false)
			})
		}
	}
}

// BenchmarkDictionaryFilterContainsAll_StrArr_Medium benchmarks ContainsAll for string arrays with 128 arrays (medium).
func BenchmarkDictionaryFilterContainsAll_StrArr_Medium(b *testing.B) {
	const arrayCount = 128
	arrayLengths := []int{10, 50, 100, 200, 300, 400, 500}
	percentages := []int{20, 50, 90}

	for _, arrayLen := range arrayLengths {
		for _, pct := range percentages {
			b.Run(fmt.Sprintf("ArrayLen%d_Pct%d_Existing", arrayLen, pct), func(b *testing.B) {
				benchmarkDictionaryFilterContainsAll(b, pbv1.ValueTypeStrArr, arrayCount, arrayLen, pct, true)
			})
			b.Run(fmt.Sprintf("ArrayLen%d_Pct%d_NonExisting", arrayLen, pct), func(b *testing.B) {
				benchmarkDictionaryFilterContainsAll(b, pbv1.ValueTypeStrArr, arrayCount, arrayLen, pct, false)
			})
		}
	}
}

// BenchmarkDictionaryFilterContainsAll_StrArr_Max benchmarks ContainsAll for string arrays with 256 arrays (max).
func BenchmarkDictionaryFilterContainsAll_StrArr_Max(b *testing.B) {
	const arrayCount = 256
	arrayLengths := []int{10, 50, 100, 200, 300, 400, 500}
	percentages := []int{20, 50, 90}

	for _, arrayLen := range arrayLengths {
		for _, pct := range percentages {
			b.Run(fmt.Sprintf("ArrayLen%d_Pct%d_Existing", arrayLen, pct), func(b *testing.B) {
				benchmarkDictionaryFilterContainsAll(b, pbv1.ValueTypeStrArr, arrayCount, arrayLen, pct, true)
			})
			b.Run(fmt.Sprintf("ArrayLen%d_Pct%d_NonExisting", arrayLen, pct), func(b *testing.B) {
				benchmarkDictionaryFilterContainsAll(b, pbv1.ValueTypeStrArr, arrayCount, arrayLen, pct, false)
			})
		}
	}
}
