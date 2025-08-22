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
		assert.Equal(t, pbv1.ValueTypeUnknown, tag.valueType, "valueType should be reset")
		assert.False(t, tag.indexed, "indexed should be reset")

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

		// Reset elements
		es.reset()

		// Verify all slices are cleared but reusable
		assert.Len(t, es.seriesIDs, 0, "seriesIDs slice should be empty")
		assert.Len(t, es.userKeys, 0, "userKeys slice should be empty")
		assert.Len(t, es.data, 0, "data slice should be empty")
		assert.Len(t, es.tags, 0, "tags slice should be empty")
		assert.False(t, es.pooled, "pooled flag should be reset")

		releaseElements(es)
	})
}

func TestSizeCalculation(t *testing.T) {
	t.Run("tag size calculation", func(t *testing.T) {
		tag := generateTag()
		tag.name = "test-tag"       // 8 bytes
		tag.value = []byte("value") // 5 bytes
		tag.valueType = pbv1.ValueTypeStr

		expectedSize := 8 + 5 + 1 // name + value + valueType
		actualSize := tag.size()

		assert.Equal(t, expectedSize, actualSize, "tag size calculation should be correct")
		releaseTag(tag)
	})

	t.Run("elements size calculation", func(t *testing.T) {
		es := generateElements()
		es.seriesIDs = []common.SeriesID{1, 2}
		es.userKeys = []int64{100, 200}
		es.data = [][]byte{[]byte("data1"), []byte("data2")} // 5 + 5 = 10 bytes
		es.tags = [][]tag{
			{{name: "tag1", value: []byte("val1")}}, // 4 + 4 + 1 = 9 bytes
			{{name: "tag2", value: []byte("val2")}}, // 4 + 4 + 1 = 9 bytes
		}

		expectedSize := 2*8 + 2*8 + 10 + 9 + 9 // seriesIDs + userKeys + data + tags
		actualSize := es.size()

		assert.Equal(t, expectedSize, actualSize, "elements size calculation should be correct")
		releaseElements(es)
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

		// Test sort.Interface methods
		assert.Equal(t, 3, es.Len(), "Len() should return correct length")
		assert.True(t, es.Less(1, 0), "Less(1, 0) should be true (seriesID 1 < 2)")
		assert.False(t, es.Less(0, 1), "Less(0, 1) should be false (seriesID 2 > 1)")

		// Test Swap
		es.Swap(0, 1)
		assert.Equal(t, common.SeriesID(1), es.seriesIDs[0], "Swap should exchange seriesIDs")
		assert.Equal(t, common.SeriesID(2), es.seriesIDs[1], "Swap should exchange seriesIDs")
		assert.Equal(t, int64(100), es.userKeys[0], "Swap should exchange userKeys")
		assert.Equal(t, int64(200), es.userKeys[1], "Swap should exchange userKeys")

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

		// When seriesID is the same, should sort by userKey
		expectedUserKeys := []int64{100, 200, 300}
		assert.Equal(t, expectedUserKeys, es.userKeys, "elements with same seriesID should be sorted by userKey")

		// Verify data follows the userKey sorting
		assert.Equal(t, []byte("data100"), es.data[0], "data should follow userKey sorting")
		assert.Equal(t, []byte("data200"), es.data[1], "data should follow userKey sorting")
		assert.Equal(t, []byte("data300"), es.data[2], "data should follow userKey sorting")

		releaseElements(es)
	})
}

func TestNilSafety(t *testing.T) {
	t.Run("release nil elements", func(_ *testing.T) {
		// Should not panic
		releaseElements(nil)
	})

	t.Run("release nil tag", func(_ *testing.T) {
		// Should not panic
		releaseTag(nil)
	})

	t.Run("release non-pooled elements", func(_ *testing.T) {
		es := &elements{pooled: false}
		// Should not panic or add to pool
		releaseElements(es)
	})
}
