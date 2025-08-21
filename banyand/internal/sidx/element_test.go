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

package sidx

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func TestElementPoolAllocation(t *testing.T) {
	// Test pool allocation correctness
	t.Run("element pool allocation", func(t *testing.T) {
		e1 := generateElement()
		require.NotNil(t, e1)
		assert.True(t, e1.pooled, "element should be marked as pooled")

		e2 := generateElement()
		require.NotNil(t, e2)
		assert.True(t, e2.pooled, "element should be marked as pooled")

		// Elements should be different instances
		assert.NotSame(t, e1, e2, "pool should provide different instances")

		releaseElement(e1)
		releaseElement(e2)
	})

	t.Run("elements pool allocation", func(t *testing.T) {
		es1 := generateElements()
		require.NotNil(t, es1)
		assert.True(t, es1.pooled, "elements should be marked as pooled")

		es2 := generateElements()
		require.NotNil(t, es2)
		assert.True(t, es2.pooled, "elements should be marked as pooled")

		// Elements should be different instances
		assert.NotSame(t, es1, es2, "pool should provide different instances")

		releaseElements(es1)
		releaseElements(es2)
	})

	t.Run("tag pool allocation", func(t *testing.T) {
		t1 := generateTag()
		require.NotNil(t, t1)

		t2 := generateTag()
		require.NotNil(t, t2)

		// Tags should be different instances
		assert.NotSame(t, t1, t2, "pool should provide different instances")

		releaseTag(t1)
		releaseTag(t2)
	})
}

func TestElementReset(t *testing.T) {
	t.Run("element reset functionality", func(t *testing.T) {
		e := generateElement()

		// Set up element with data
		e.seriesID = 123
		e.userKey = 456
		e.data = []byte("test data")
		e.tags = []tag{
			{name: "service", value: []byte("test-service"), valueType: pbv1.ValueTypeStr, indexed: true},
			{name: "endpoint", value: []byte("test-endpoint"), valueType: pbv1.ValueTypeStr, indexed: false},
		}

		// Reset the element
		e.reset()

		// Verify all fields are cleared
		assert.Equal(t, common.SeriesID(0), e.seriesID, "seriesID should be reset to 0")
		assert.Equal(t, int64(0), e.userKey, "userKey should be reset to 0")
		assert.Len(t, e.data, 0, "data slice should be empty but reusable")
		assert.Len(t, e.tags, 0, "tags slice should be empty but reusable")
		assert.False(t, e.pooled, "pooled flag should be reset")

		releaseElement(e)
	})

	t.Run("tag reset functionality", func(t *testing.T) {
		tag := generateTag()

		// Set up tag with data
		tag.name = "test-tag"
		tag.value = []byte("test-value")
		tag.valueType = pbv1.ValueTypeStr
		tag.indexed = true

		// Reset the tag
		tag.reset()

		// Verify all fields are cleared
		assert.Equal(t, "", tag.name, "name should be empty")
		assert.Nil(t, tag.value, "value should be nil")
		assert.Equal(t, pbv1.ValueTypeUnknown, tag.valueType, "valueType should be unknown")
		assert.False(t, tag.indexed, "indexed should be false")

		releaseTag(tag)
	})

	t.Run("elements reset functionality", func(t *testing.T) {
		es := generateElements()

		// Set up elements with data
		es.seriesIDs = []common.SeriesID{1, 2, 3}
		es.userKeys = []int64{100, 200, 300}
		es.data = [][]byte{[]byte("data1"), []byte("data2"), []byte("data3")}
		es.tags = [][]tag{
			{{name: "tag1", value: []byte("value1")}},
			{{name: "tag2", value: []byte("value2")}},
			{{name: "tag3", value: []byte("value3")}},
		}

		// Reset the elements
		es.reset()

		// Verify all slices are cleared but reusable
		assert.Len(t, es.seriesIDs, 0, "seriesIDs should be empty")
		assert.Len(t, es.userKeys, 0, "userKeys should be empty")
		assert.Len(t, es.data, 0, "data should be empty")
		assert.Len(t, es.tags, 0, "tags should be empty")
		assert.False(t, es.pooled, "pooled flag should be reset")

		releaseElements(es)
	})
}

func TestMemoryReuse(t *testing.T) {
	t.Run("element slice reuse", func(t *testing.T) {
		e := generateElement()

		// Add small data that should be reused
		smallData := make([]byte, 100)
		e.data = smallData

		// Add small number of tags that should be reused
		e.tags = make([]tag, 5)

		originalDataCap := cap(e.data)
		originalTagsCap := cap(e.tags)

		// Reset and verify slices are reused
		e.reset()

		// Add new data and verify capacity is preserved
		e.data = append(e.data, []byte("new data")...)
		e.tags = append(e.tags, tag{name: "new-tag"})

		assert.GreaterOrEqual(t, cap(e.data), originalDataCap, "data slice capacity should be preserved")
		assert.GreaterOrEqual(t, cap(e.tags), originalTagsCap, "tags slice capacity should be preserved")

		releaseElement(e)
	})

	t.Run("oversized slice release", func(t *testing.T) {
		e := generateElement()

		// Add oversized data that should be released
		oversizedData := make([]byte, maxPooledSliceSize+1)
		e.data = oversizedData

		// Add too many tags that should be released
		e.tags = make([]tag, maxPooledTagCount+1)

		// Reset and verify slices are released
		e.reset()

		assert.Nil(t, e.data, "oversized data slice should be released")
		assert.Nil(t, e.tags, "oversized tags slice should be released")

		releaseElement(e)
	})
}

func TestSizeCalculation(t *testing.T) {
	t.Run("element size calculation", func(t *testing.T) {
		e := generateElement()
		e.seriesID = 123
		e.userKey = 456
		e.data = []byte("test data") // 9 bytes
		e.tags = []tag{
			{name: "service", value: []byte("test-service"), valueType: pbv1.ValueTypeStr},   // 7 + 12 + 1 = 20 bytes
			{name: "endpoint", value: []byte("test-endpoint"), valueType: pbv1.ValueTypeStr}, // 8 + 13 + 1 = 22 bytes
		}

		expectedSize := 8 + 8 + 9 + 20 + 22 // seriesID + userKey + data + tag1 + tag2
		actualSize := e.size()

		assert.Equal(t, expectedSize, actualSize, "element size calculation should be accurate")

		releaseElement(e)
	})

	t.Run("tag size calculation", func(t *testing.T) {
		tag := generateTag()
		tag.name = "test-tag"             // 8 bytes
		tag.value = []byte("test-value")  // 10 bytes
		tag.valueType = pbv1.ValueTypeStr // 1 byte

		expectedSize := 8 + 10 + 1
		actualSize := tag.size()

		assert.Equal(t, expectedSize, actualSize, "tag size calculation should be accurate")

		releaseTag(tag)
	})

	t.Run("elements size calculation", func(t *testing.T) {
		es := generateElements()
		es.seriesIDs = []common.SeriesID{1, 2}
		es.userKeys = []int64{100, 200}
		es.data = [][]byte{[]byte("data1"), []byte("data2")} // 5 + 5 = 10 bytes
		es.tags = [][]tag{
			{{name: "tag1", value: []byte("value1")}}, // 4 + 6 + 1 = 11 bytes
			{{name: "tag2", value: []byte("value2")}}, // 4 + 6 + 1 = 11 bytes
		}

		expectedSize := 2*8 + 2*8 + 10 + 11 + 11 // seriesIDs + userKeys + data + tags
		actualSize := es.size()

		assert.Equal(t, expectedSize, actualSize, "elements size calculation should be accurate")

		releaseElements(es)
	})

	t.Run("empty element size", func(t *testing.T) {
		e := generateElement()
		expectedSize := 8 + 8 // just seriesID + userKey
		actualSize := e.size()

		assert.Equal(t, expectedSize, actualSize, "empty element should have minimal size")

		releaseElement(e)
	})
}

func TestElementsSorting(t *testing.T) {
	t.Run("sort by seriesID then userKey", func(t *testing.T) {
		es := generateElements()
		es.seriesIDs = []common.SeriesID{3, 1, 2, 1}
		es.userKeys = []int64{100, 300, 200, 100}
		es.data = [][]byte{[]byte("data3"), []byte("data1"), []byte("data2"), []byte("data1b")}
		es.tags = [][]tag{
			{{name: "tag3"}},
			{{name: "tag1"}},
			{{name: "tag2"}},
			{{name: "tag1b"}},
		}

		// Sort elements
		sort.Sort(es)

		// Verify sorting: seriesID first, then userKey
		expectedSeriesIDs := []common.SeriesID{1, 1, 2, 3}
		expectedUserKeys := []int64{100, 300, 200, 100}

		assert.Equal(t, expectedSeriesIDs, es.seriesIDs, "elements should be sorted by seriesID first")
		assert.Equal(t, expectedUserKeys, es.userKeys, "elements with same seriesID should be sorted by userKey")

		// Verify that data and tags follow the sorting
		assert.Equal(t, []byte("data1b"), es.data[0], "data should follow sorting")
		assert.Equal(t, []byte("data1"), es.data[1], "data should follow sorting")
		assert.Equal(t, []byte("data2"), es.data[2], "data should follow sorting")
		assert.Equal(t, []byte("data3"), es.data[3], "data should follow sorting")

		releaseElements(es)
	})

	t.Run("sort interface implementation", func(t *testing.T) {
		es := generateElements()
		es.seriesIDs = []common.SeriesID{2, 1, 3}
		es.userKeys = []int64{200, 100, 300}
		es.data = [][]byte{[]byte("data2"), []byte("data1"), []byte("data3")}
		es.tags = [][]tag{
			{{name: "tag2"}},
			{{name: "tag1"}},
			{{name: "tag3"}},
		}

		// Test Len
		assert.Equal(t, 3, es.Len(), "Len should return number of elements")

		// Test Less
		assert.True(t, es.Less(1, 0), "seriesID 1 < seriesID 2")
		assert.False(t, es.Less(0, 1), "seriesID 2 > seriesID 1")

		// Test Swap
		es.Swap(0, 1)
		assert.Equal(t, common.SeriesID(1), es.seriesIDs[0], "swap should exchange seriesIDs")
		assert.Equal(t, common.SeriesID(2), es.seriesIDs[1], "swap should exchange seriesIDs")
		assert.Equal(t, int64(100), es.userKeys[0], "swap should exchange userKeys")
		assert.Equal(t, int64(200), es.userKeys[1], "swap should exchange userKeys")
		assert.Equal(t, []byte("data1"), es.data[0], "swap should exchange data")
		assert.Equal(t, []byte("data2"), es.data[1], "swap should exchange data")

		releaseElements(es)
	})

	t.Run("sort with duplicate seriesID", func(t *testing.T) {
		es := generateElements()
		es.seriesIDs = []common.SeriesID{1, 1, 1}
		es.userKeys = []int64{300, 100, 200}
		es.data = [][]byte{[]byte("data300"), []byte("data100"), []byte("data200")}
		es.tags = [][]tag{
			{{name: "tag300"}},
			{{name: "tag100"}},
			{{name: "tag200"}},
		}

		sort.Sort(es)

		// With same seriesID, should sort by userKey
		expectedUserKeys := []int64{100, 200, 300}
		assert.Equal(t, expectedUserKeys, es.userKeys, "same seriesID should sort by userKey")

		// Verify data follows the sorting
		assert.Equal(t, []byte("data100"), es.data[0])
		assert.Equal(t, []byte("data200"), es.data[1])
		assert.Equal(t, []byte("data300"), es.data[2])

		releaseElements(es)
	})
}

func TestNilSafety(t *testing.T) {
	t.Run("release nil element", func(_ *testing.T) {
		// Should not panic
		releaseElement(nil)
	})

	t.Run("release nil elements", func(_ *testing.T) {
		// Should not panic
		releaseElements(nil)
	})

	t.Run("release nil tag", func(_ *testing.T) {
		// Should not panic
		releaseTag(nil)
	})

	t.Run("release non-pooled element", func(_ *testing.T) {
		e := &element{pooled: false}
		// Should not panic or add to pool
		releaseElement(e)
	})

	t.Run("release non-pooled elements", func(_ *testing.T) {
		es := &elements{pooled: false}
		// Should not panic or add to pool
		releaseElements(es)
	})
}
