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

	df := NewDictionaryFilter(items[:3])
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

	df := NewDictionaryFilter([][]byte{key})
	assert.True(df.MightContain(key))

	df.Reset()
	assert.False(df.MightContain(key))
}

func TestDictionaryFilterInt64Arr(t *testing.T) {
	assert := assert.New(t)

	items := []int64{1, 2, 3, 4, 5}
	dst := make([]byte, 0, 24)
	for i := 0; i < 3; i++ {
		dst = append(dst, convert.Int64ToBytes(items[i])...)
	}

	df := NewDictionaryFilter([][]byte{dst})
	df.SetValueType(pbv1.ValueTypeInt64Arr)

	for i := 0; i < 3; i++ {
		assert.True(df.MightContain(convert.Int64ToBytes(items[i])))
	}
	for i := 3; i < 5; i++ {
		assert.False(df.MightContain(convert.Int64ToBytes(items[i])))
	}
}

func TestDictionaryFilterStrArr(t *testing.T) {
	assert := assert.New(t)

	items := [][]byte{
		[]byte("skywalking"),
		[]byte("banyandb"),
		[]byte(""),
		[]byte("hello"),
		[]byte("world"),
	}
	dst := make([]byte, 0)
	for i := 0; i < 3; i++ {
		dst = marshalVarArray(dst, items[i])
	}

	df := NewDictionaryFilter([][]byte{dst})
	df.SetValueType(pbv1.ValueTypeStrArr)

	for i := 0; i < 3; i++ {
		assert.True(df.MightContain(items[i]))
	}
	for i := 3; i < 5; i++ {
		assert.False(df.MightContain(items[i]))
	}
}

// generateDictionaryValues creates test data with n unique values.
func generateDictionaryValues(n int) [][]byte {
	values := make([][]byte, n)
	for i := 0; i < n; i++ {
		values[i] = []byte(fmt.Sprintf("value_%d", i))
	}
	return values
}

// BenchmarkDictionaryFilterMightContain_Small benchmarks with 16 values (small dictionary).
func BenchmarkDictionaryFilterMightContain_Small(b *testing.B) {
	values := generateDictionaryValues(16)
	df := NewDictionaryFilter(values)

	// Test item that exists (middle of list)
	existingItem := values[8]
	// Test item that doesn't exist
	nonExistingItem := []byte("not_found")

	b.Run("Existing", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			df.MightContain(existingItem)
		}
	})

	b.Run("NonExisting", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			df.MightContain(nonExistingItem)
		}
	})
}

// BenchmarkDictionaryFilterMightContain_Medium benchmarks with 128 values.
func BenchmarkDictionaryFilterMightContain_Medium(b *testing.B) {
	values := generateDictionaryValues(128)
	df := NewDictionaryFilter(values)

	// Test item that exists (middle of list)
	existingItem := values[64]
	// Test item that doesn't exist
	nonExistingItem := []byte("not_found")

	b.Run("Existing", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			df.MightContain(existingItem)
		}
	})

	b.Run("NonExisting", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			df.MightContain(nonExistingItem)
		}
	})
}

// BenchmarkDictionaryFilterMightContain_Max benchmarks with 256 values (max dictionary size).
func BenchmarkDictionaryFilterMightContain_Max(b *testing.B) {
	values := generateDictionaryValues(256)
	df := NewDictionaryFilter(values)

	// Test item at the end (worst case for linear search)
	lastItem := values[255]
	// Test item that doesn't exist
	nonExistingItem := []byte("not_found")

	b.Run("ExistingLast", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			df.MightContain(lastItem)
		}
	})

	b.Run("NonExisting", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			df.MightContain(nonExistingItem)
		}
	})
}

// BenchmarkDictionaryFilterMightContain_Int64Arr benchmarks array type lookups.
func BenchmarkDictionaryFilterMightContain_Int64Arr(b *testing.B) {
	// Create multiple int64 arrays
	arrays := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		arr := make([]byte, 0, 24)
		for j := 0; j < 3; j++ {
			arr = append(arr, convert.Int64ToBytes(int64(i*3+j))...)
		}
		arrays[i] = arr
	}

	df := NewDictionaryFilter(arrays)
	df.SetValueType(pbv1.ValueTypeInt64Arr)

	existingItem := convert.Int64ToBytes(15) // exists in arrays[5]
	nonExistingItem := convert.Int64ToBytes(999)

	b.Run("Existing", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			df.MightContain(existingItem)
		}
	})

	b.Run("NonExisting", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			df.MightContain(nonExistingItem)
		}
	})
}

// BenchmarkDictionaryFilterMightContain_StrArr benchmarks string array type lookups.
func BenchmarkDictionaryFilterMightContain_StrArr(b *testing.B) {
	// Create multiple string arrays
	arrays := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		dst := make([]byte, 0)
		for j := 0; j < 3; j++ {
			dst = marshalVarArray(dst, []byte(fmt.Sprintf("element_%d_%d", i, j)))
		}
		arrays[i] = dst
	}

	df := NewDictionaryFilter(arrays)
	df.SetValueType(pbv1.ValueTypeStrArr)

	existingItem := []byte("element_5_1")
	nonExistingItem := []byte("not_found")

	b.Run("Existing", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			df.MightContain(existingItem)
		}
	})

	b.Run("NonExisting", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			df.MightContain(nonExistingItem)
		}
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
