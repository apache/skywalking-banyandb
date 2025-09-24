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

package stream

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/pkg/filter"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func TestEncodeAndDecodeBloomFilter(t *testing.T) {
	assert := assert.New(t)

	bf := filter.NewBloomFilter(3)

	items := [][]byte{
		[]byte("skywalking"),
		[]byte("banyandb"),
		[]byte(""),
		[]byte("hello"),
		[]byte("world"),
	}

	for i := 0; i < 3; i++ {
		bf.Add(items[i])
	}

	buf := make([]byte, 0)
	buf = encodeBloomFilter(buf, bf)
	bf2 := filter.NewBloomFilter(0)
	bf2 = decodeBloomFilter(buf, bf2)

	for i := 0; i < 3; i++ {
		mightContain := bf2.MightContain(items[i])
		assert.True(mightContain)
	}

	for i := 3; i < 5; i++ {
		mightContain := bf2.MightContain(items[i])
		assert.False(mightContain)
	}
}

type mockReader struct {
	data []byte
}

func (mr *mockReader) Path() string {
	return "mock"
}

func (mr *mockReader) Read(offset int64, buffer []byte) (int, error) {
	if offset >= int64(len(mr.data)) {
		return 0, nil
	}
	n := copy(buffer, mr.data[offset:])
	return n, nil
}

func (mr *mockReader) SequentialRead() fs.SeqReader {
	return nil
}

func (mr *mockReader) Close() error {
	return nil
}

func generateMetaAndFilter(tagCount int, itemsPerTag int) ([]byte, []byte) {
	tfm := generateTagFamilyMetadata()
	defer releaseTagFamilyMetadata(tfm)
	filterBuf := bytes.Buffer{}

	for i := 0; i < tagCount; i++ {
		bf := filter.NewBloomFilter(itemsPerTag)
		for j := 0; j < itemsPerTag; j++ {
			item := make([]byte, 8)
			binary.BigEndian.PutUint64(item, uint64(i*itemsPerTag+j))
			bf.Add(item)
		}
		buf := make([]byte, 0)
		buf = encodeBloomFilter(buf, bf)

		tm := &tagMetadata{
			name:      fmt.Sprintf("tag_%d", i),
			valueType: pbv1.ValueTypeInt64,
			min:       make([]byte, 8),
			max:       make([]byte, 8),
		}
		binary.BigEndian.PutUint64(tm.min, uint64(i*itemsPerTag))
		binary.BigEndian.PutUint64(tm.max, uint64(i*itemsPerTag+itemsPerTag-1))
		tm.filterBlock.offset = uint64(filterBuf.Len())
		tm.filterBlock.size = uint64(len(buf))
		tfm.tagMetadata = append(tfm.tagMetadata, *tm)

		filterBuf.Write(buf)
	}

	metaBuf := make([]byte, 0)
	metaBuf = tfm.marshal(metaBuf)
	return metaBuf, filterBuf.Bytes()
}

func TestTagFamilyFiltersHaving(t *testing.T) {
	assert := assert.New(t)

	// Create a tag family filter with test data
	bf := filter.NewBloomFilter(100)
	bf.Add([]byte("service-1"))
	bf.Add([]byte("service-2"))
	bf.Add([]byte("service-3"))

	// Create tag filter
	tf := &tagFilter{
		filter: bf,
		min:    []byte{},
		max:    []byte{},
	}

	// Create tag family filter map
	tff := &tagFamilyFilter{
		"service": tf,
	}

	// Create tag family filters
	tfs := &tagFamilyFilters{
		tagFamilyFilters: []*tagFamilyFilter{tff},
	}

	tests := []struct {
		name           string
		tagName        string
		description    string
		tagValues      []string
		expectedResult bool
	}{
		{
			name:           "all values might exist",
			tagName:        "service",
			tagValues:      []string{"service-1", "service-2"},
			expectedResult: true,
			description:    "should return true when all values might exist in bloom filter",
		},
		{
			name:           "some values might exist",
			tagName:        "service",
			tagValues:      []string{"service-1", "unknown-service"},
			expectedResult: true,
			description:    "should return true when at least one value might exist in bloom filter",
		},
		{
			name:           "no values might exist",
			tagName:        "service",
			tagValues:      []string{"unknown-1", "unknown-2"},
			expectedResult: false,
			description:    "should return false when no values might exist in bloom filter",
		},
		{
			name:           "single value exists",
			tagName:        "service",
			tagValues:      []string{"service-3"},
			expectedResult: true,
			description:    "should return true when single value might exist in bloom filter",
		},
		{
			name:           "single value does not exist",
			tagName:        "service",
			tagValues:      []string{"definitely-not-there"},
			expectedResult: false,
			description:    "should return false when single value doesn't exist in bloom filter",
		},
		{
			name:           "empty values list",
			tagName:        "service",
			tagValues:      []string{},
			expectedResult: false,
			description:    "should return false when no values provided",
		},
		{
			name:           "tag not found",
			tagName:        "non-existent-tag",
			tagValues:      []string{"service-1"},
			expectedResult: true,
			description:    "should return true (conservative) when tag is not found",
		},
		{
			name:           "early exit - first match",
			tagName:        "service",
			tagValues:      []string{"service-1", "unknown-1", "unknown-2"},
			expectedResult: true,
			description:    "should return true immediately when first value matches",
		},
		{
			name:           "late match",
			tagName:        "service",
			tagValues:      []string{"unknown-1", "unknown-2", "service-2"},
			expectedResult: true,
			description:    "should return true when last value matches",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tfs.Having(tt.tagName, tt.tagValues)
			assert.Equal(t, tt.expectedResult, result, tt.description)
		})
	}
}

func TestTagFamilyFiltersHavingWithoutBloomFilter(t *testing.T) {
	assert := assert.New(t)

	// Create tag filter without bloom filter
	tf := &tagFilter{
		filter: nil, // No bloom filter
		min:    []byte{},
		max:    []byte{},
	}

	// Create tag family filter map
	tff := &tagFamilyFilter{
		"service": tf,
	}

	// Create tag family filters
	tfs := &tagFamilyFilters{
		tagFamilyFilters: []*tagFamilyFilter{tff},
	}

	// When no bloom filter is available, should always return true (conservative)
	result := tfs.Having("service", []string{"any-service", "another-service"})
	assert.True(result, "should return true when no bloom filter is available (conservative approach)")

	// Test with empty list too
	result = tfs.Having("service", []string{})
	assert.True(result, "should return true even with empty list when no bloom filter (conservative approach)")
}

func TestTagFamilyFiltersHavingMultipleTagFamilies(t *testing.T) {
	assert := assert.New(t)

	// Create first tag family filter
	bf1 := filter.NewBloomFilter(50)
	bf1.Add([]byte("service-1"))
	bf1.Add([]byte("service-2"))

	tf1 := &tagFilter{
		filter: bf1,
		min:    []byte{},
		max:    []byte{},
	}

	tff1 := &tagFamilyFilter{
		"service": tf1,
	}

	// Create second tag family filter
	bf2 := filter.NewBloomFilter(50)
	bf2.Add([]byte("user-1"))
	bf2.Add([]byte("user-2"))

	tf2 := &tagFilter{
		filter: bf2,
		min:    []byte{},
		max:    []byte{},
	}

	tff2 := &tagFamilyFilter{
		"user": tf2,
	}

	// Create tag family filters with multiple families
	tfs := &tagFamilyFilters{
		tagFamilyFilters: []*tagFamilyFilter{tff1, tff2},
	}

	// Test service tag (should find in first family)
	result := tfs.Having("service", []string{"service-1", "unknown"})
	assert.True(result, "should find service-1 in first tag family")

	// Test user tag (should find in second family)
	result = tfs.Having("user", []string{"user-2", "unknown"})
	assert.True(result, "should find user-2 in second tag family")

	// Test non-existent values
	result = tfs.Having("service", []string{"unknown-1", "unknown-2"})
	assert.False(result, "should not find unknown values")
}

func TestTagFamilyFiltersHavingLargeList(t *testing.T) {
	assert := assert.New(t)

	// Create a bloom filter with one target value
	bf := filter.NewBloomFilter(1000)
	bf.Add([]byte("target-service"))

	tf := &tagFilter{
		filter: bf,
		min:    []byte{},
		max:    []byte{},
	}

	tff := &tagFamilyFilter{
		"service": tf,
	}

	tfs := &tagFamilyFilters{
		tagFamilyFilters: []*tagFamilyFilter{tff},
	}

	// Create a large list with the target at the end
	largeList := make([]string, 1000)
	for i := 0; i < 999; i++ {
		largeList[i] = fmt.Sprintf("non-existent-service-%d", i)
	}
	largeList[999] = "target-service"

	result := tfs.Having("service", largeList)
	assert.True(result, "should handle large lists and find target value")

	// Test with large list that has no matches
	noMatchList := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		noMatchList[i] = fmt.Sprintf("definitely-not-there-%d", i)
	}

	result = tfs.Having("service", noMatchList)
	assert.False(result, "should return false for large list with no matches")
}

func BenchmarkTagFamilyFiltersHaving(b *testing.B) {
	// Setup bloom filter with test data
	bf := filter.NewBloomFilter(1000)
	for i := 0; i < 500; i++ {
		bf.Add([]byte(fmt.Sprintf("service-%d", i)))
	}

	tf := &tagFilter{
		filter: bf,
		min:    []byte{},
		max:    []byte{},
	}

	tff := &tagFamilyFilter{
		"service": tf,
	}

	tfs := &tagFamilyFilters{
		tagFamilyFilters: []*tagFamilyFilter{tff},
	}

	// Test different sizes of input lists
	b.Run("small list (5 items)", func(b *testing.B) {
		testValues := []string{"service-1", "service-2", "service-3", "unknown-1", "unknown-2"}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tfs.Having("service", testValues)
		}
	})

	b.Run("medium list (50 items)", func(b *testing.B) {
		testValues := make([]string, 50)
		for i := 0; i < 50; i++ {
			if i < 25 {
				testValues[i] = fmt.Sprintf("service-%d", i)
			} else {
				testValues[i] = fmt.Sprintf("unknown-%d", i)
			}
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tfs.Having("service", testValues)
		}
	})

	b.Run("large list (500 items)", func(b *testing.B) {
		testValues := make([]string, 500)
		for i := 0; i < 500; i++ {
			if i < 250 {
				testValues[i] = fmt.Sprintf("service-%d", i)
			} else {
				testValues[i] = fmt.Sprintf("unknown-%d", i)
			}
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tfs.Having("service", testValues)
		}
	})

	b.Run("early exit (match first)", func(b *testing.B) {
		testValues := make([]string, 100)
		testValues[0] = "service-1" // This will match
		for i := 1; i < 100; i++ {
			testValues[i] = fmt.Sprintf("unknown-%d", i)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tfs.Having("service", testValues)
		}
	})

	b.Run("no matches", func(b *testing.B) {
		testValues := make([]string, 100)
		for i := 0; i < 100; i++ {
			testValues[i] = fmt.Sprintf("definitely-not-there-%d", i)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tfs.Having("service", testValues)
		}
	})
}

func BenchmarkTagFamilyFiltersUnmarshal(b *testing.B) {
	testCases := []struct {
		tagFamilyCount int
		tagCount       int
		itemsPerTag    int
	}{
		{1, 5, 10},
		{2, 10, 100},
		{3, 15, 1000},
	}

	for _, tc := range testCases {
		b.Run(fmt.Sprintf("tagFamilies=%d_tags=%d_items=%d", tc.tagFamilyCount, tc.tagCount, tc.itemsPerTag), func(b *testing.B) {
			tagFamilies := make(map[string]*dataBlock)
			metaReaders := make(map[string]fs.Reader)
			filterReaders := make(map[string]fs.Reader)
			for i := 0; i < tc.tagFamilyCount; i++ {
				familyName := fmt.Sprintf("tagFamily_%d", i)
				metaBuf, filterBuf := generateMetaAndFilter(tc.tagCount, tc.itemsPerTag)
				tagFamilies[familyName] = &dataBlock{
					offset: 0,
					size:   uint64(len(metaBuf)),
				}
				metaReaders[familyName] = &mockReader{data: metaBuf}
				filterReaders[familyName] = &mockReader{data: filterBuf}
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				tfs := generateTagFamilyFilters()
				tfs.unmarshal(tagFamilies, metaReaders, filterReaders)
				releaseTagFamilyFilters(tfs)
			}
		})
	}
}
