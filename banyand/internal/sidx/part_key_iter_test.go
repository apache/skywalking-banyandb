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
	"errors"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func setupPartForKeyIter(t *testing.T, elems []testElement) (*part, func()) {
	t.Helper()

	elements := createTestElements(elems)

	mp := GenerateMemPart()
	mp.mustInitFromElements(elements)

	part := openMemPart(mp)

	cleanup := func() {
		part.close()
		ReleaseMemPart(mp)
		releaseElements(elements)
	}
	return part, cleanup
}

func TestPartKeyIterOrdersBlocksByMinKey(t *testing.T) {
	part, cleanup := setupPartForKeyIter(t, []testElement{
		{seriesID: 1, userKey: 100, data: []byte("s1")},
		{seriesID: 2, userKey: 50, data: []byte("s2")},
		{seriesID: 3, userKey: 150, data: []byte("s3")},
	})
	defer cleanup()

	iter := generatePartKeyIter()
	defer releasePartKeyIter(iter)

	iter.init(part, []common.SeriesID{1, 2, 3}, 0, 200, nil)

	var (
		seriesIDs []common.SeriesID
		minKeys   []int64
	)
	for iter.nextBlock() {
		blocks := iter.currentBlocks()
		require.NotEmpty(t, blocks)
		for _, block := range blocks {
			seriesIDs = append(seriesIDs, block.seriesID)
			minKeys = append(minKeys, block.minKey)
		}
	}

	require.NoError(t, iter.error())
	require.Len(t, seriesIDs, 3, "expected one block per series")
	assert.Equal(t, []common.SeriesID{2, 1, 3}, seriesIDs, "blocks should be emitted in ascending minKey order")
	require.True(t, sort.SliceIsSorted(minKeys, func(i, j int) bool {
		return minKeys[i] <= minKeys[j]
	}), "minKeys should be non-decreasing")
}

func TestPartKeyIterFiltersSeriesIDs(t *testing.T) {
	part, cleanup := setupPartForKeyIter(t, []testElement{
		{seriesID: 1, userKey: 100, data: []byte("s1")},
		{seriesID: 2, userKey: 50, data: []byte("s2")},
		{seriesID: 3, userKey: 150, data: []byte("s3")},
	})
	defer cleanup()

	iter := generatePartKeyIter()
	defer releasePartKeyIter(iter)

	iter.init(part, []common.SeriesID{1, 3}, 0, 200, nil)

	var seriesIDs []common.SeriesID
	for iter.nextBlock() {
		blocks := iter.currentBlocks()
		require.NotEmpty(t, blocks)
		for _, block := range blocks {
			seriesIDs = append(seriesIDs, block.seriesID)
		}
	}

	require.NoError(t, iter.error())
	assert.Equal(t, []common.SeriesID{1, 3}, seriesIDs)
}

func TestPartKeyIterAppliesKeyRange(t *testing.T) {
	part, cleanup := setupPartForKeyIter(t, []testElement{
		{seriesID: 1, userKey: 100, data: []byte("s1")},
		{seriesID: 2, userKey: 50, data: []byte("s2")},
		{seriesID: 3, userKey: 150, data: []byte("s3")},
	})
	defer cleanup()

	iter := generatePartKeyIter()
	defer releasePartKeyIter(iter)

	iter.init(part, []common.SeriesID{1, 2, 3}, 120, 200, nil)

	var seriesIDs []common.SeriesID
	for iter.nextBlock() {
		blocks := iter.currentBlocks()
		require.NotEmpty(t, blocks)
		for _, block := range blocks {
			seriesIDs = append(seriesIDs, block.seriesID)
		}
	}

	require.NoError(t, iter.error())
	assert.Equal(t, []common.SeriesID{3}, seriesIDs, "only series with blocks overlapping the key range should be returned")
}

func TestPartKeyIterNoBlocksInRange(t *testing.T) {
	part, cleanup := setupPartForKeyIter(t, []testElement{
		{seriesID: 1, userKey: 100, data: []byte("s1")},
		{seriesID: 2, userKey: 50, data: []byte("s2")},
	})
	defer cleanup()

	iter := generatePartKeyIter()
	defer releasePartKeyIter(iter)

	iter.init(part, []common.SeriesID{1, 2}, 1000, 2000, nil)

	assert.False(t, iter.nextBlock(), "no blocks should fall within the specified range")
	assert.NoError(t, iter.error())
}

func TestPartKeyIterHandlesEmptySeries(t *testing.T) {
	part, cleanup := setupPartForKeyIter(t, []testElement{
		{seriesID: 1, userKey: 100, data: []byte("s1")},
	})
	defer cleanup()

	iter := generatePartKeyIter()
	defer releasePartKeyIter(iter)

	iter.init(part, []common.SeriesID{}, 0, 200, nil)

	assert.False(t, iter.nextBlock(), "no iteration should occur without query series")
	assert.NoError(t, iter.error())
}

func TestPartKeyIterHonorsBlockFilter(t *testing.T) {
	part, cleanup := setupPartForKeyIter(t, []testElement{
		{seriesID: 1, userKey: 100, data: []byte("s1")},
		{seriesID: 2, userKey: 150, data: []byte("s2")},
	})
	defer cleanup()

	iter := generatePartKeyIter()
	defer releasePartKeyIter(iter)

	iter.init(part, []common.SeriesID{1, 2}, 0, 200, &mockBlockFilter{shouldSkip: true})

	assert.False(t, iter.nextBlock(), "filter skipping all blocks should prevent iteration")
	assert.NoError(t, iter.error())
}

func TestPartKeyIterPropagatesFilterError(t *testing.T) {
	part, cleanup := setupPartForKeyIter(t, []testElement{
		{seriesID: 1, userKey: 100, data: []byte("s1")},
	})
	defer cleanup()

	iter := generatePartKeyIter()
	defer releasePartKeyIter(iter)

	expectedErr := errors.New("filter failure")
	iter.init(part, []common.SeriesID{1}, 0, 200, &mockBlockFilter{err: expectedErr})

	assert.False(t, iter.nextBlock(), "initialisation error should block iteration")
	require.Error(t, iter.error())
	assert.ErrorIs(t, iter.error(), expectedErr)
}

func TestPartKeyIterBreaksTiesBySeriesID(t *testing.T) {
	part, cleanup := setupPartForKeyIter(t, []testElement{
		{seriesID: 1, userKey: 100, data: []byte("s1")},
		{seriesID: 2, userKey: 100, data: []byte("s2")},
	})
	defer cleanup()

	iter := generatePartKeyIter()
	defer releasePartKeyIter(iter)

	iter.init(part, []common.SeriesID{1, 2}, 0, 200, nil)

	var groups [][]common.SeriesID
	for iter.nextBlock() {
		blocks := iter.currentBlocks()
		require.NotEmpty(t, blocks)
		var ids []common.SeriesID
		for _, block := range blocks {
			ids = append(ids, block.seriesID)
		}
		groups = append(groups, ids)
	}

	require.NoError(t, iter.error())
	require.Len(t, groups, 1, "equal minKey blocks should be grouped together")
	assert.Equal(t, []common.SeriesID{1, 2}, groups[0], "tie on minKey should fall back to seriesID ordering within the group")
}

func TestPartKeyIterGroupsOverlappingRanges(t *testing.T) {
	part, cleanup := setupPartForKeyIter(t, []testElement{
		{seriesID: 1, userKey: 100, data: []byte("s1a")},
		{seriesID: 1, userKey: 180, data: []byte("s1b")},
		{seriesID: 2, userKey: 120, data: []byte("s2a")},
		{seriesID: 2, userKey: 220, data: []byte("s2b")},
		{seriesID: 3, userKey: 400, data: []byte("s3")},
	})
	defer cleanup()

	iter := generatePartKeyIter()
	defer releasePartKeyIter(iter)

	iter.init(part, []common.SeriesID{1, 2, 3}, 0, 500, nil)

	var groups [][]common.SeriesID
	for iter.nextBlock() {
		blocks := iter.currentBlocks()
		require.NotEmpty(t, blocks)
		var ids []common.SeriesID
		for _, block := range blocks {
			ids = append(ids, block.seriesID)
		}
		groups = append(groups, ids)
	}

	require.NoError(t, iter.error())
	require.GreaterOrEqual(t, len(groups), 2, "expected at least two block groups")
	assert.Equal(t, []common.SeriesID{1, 2}, groups[0], "overlapping ranges should be grouped together")
	assert.Contains(t, groups[1:], []common.SeriesID{3}, "non-overlapping series should appear in subsequent groups")
}

func TestPartKeyIterSelectiveFilterAllowsLaterBlocks(t *testing.T) {
	const elementsPerBatch = maxBlockLength + 10

	var elems []testElement
	for i := 0; i < elementsPerBatch; i++ {
		elems = append(elems, testElement{
			seriesID: 1,
			userKey:  int64(i),
			data:     []byte("pending"),
			tags: []tag{
				{
					name:      "status",
					value:     []byte("pending"),
					valueType: pbv1.ValueTypeStr,
				},
			},
		})
	}
	for i := 0; i < elementsPerBatch; i++ {
		elems = append(elems, testElement{
			seriesID: 1,
			userKey:  int64(20000 + i),
			data:     []byte("success"),
			tags: []tag{
				{
					name:      "status",
					value:     []byte("success"),
					valueType: pbv1.ValueTypeStr,
				},
			},
		})
	}
	elems = append(elems, testElement{
		seriesID: 2,
		userKey:  5000,
		data:     []byte("series2"),
		tags: []tag{
			{
				name:      "status",
				value:     []byte("other"),
				valueType: pbv1.ValueTypeStr,
			},
		},
	})

	part, cleanup := setupPartForKeyIter(t, elems)
	defer cleanup()

	iter := generatePartKeyIter()
	defer releasePartKeyIter(iter)

	filter := &selectiveMockBlockFilter{
		tagName:   "status",
		skipValue: "pending",
	}

	iter.init(part, []common.SeriesID{1, 2}, 0, 50000, filter)

	var (
		foundSuccess bool
		groups       [][]common.SeriesID
	)
	for iter.nextBlock() {
		blocks := iter.currentBlocks()
		require.NotEmpty(t, blocks)
		var ids []common.SeriesID
		for _, block := range blocks {
			ids = append(ids, block.seriesID)
			if block.seriesID == 1 {
				require.GreaterOrEqual(t, block.minKey, int64(20000), "pending block should be skipped")
				if block.minKey >= 20000 {
					foundSuccess = true
				}
			}
		}
		groups = append(groups, ids)
	}

	require.NoError(t, iter.error())
	assert.Greater(t, filter.skipCallCount, 0, "filter should have been invoked")
	assert.True(t, foundSuccess, "iterator should continue after skipped blocks and return later blocks")
	assert.NotEmpty(t, groups, "expected at least one result group")
}

func TestPartKeyIterExhaustion(t *testing.T) {
	part, cleanup := setupPartForKeyIter(t, []testElement{
		{seriesID: 1, userKey: 100, data: []byte("s1")},
		{seriesID: 2, userKey: 120, data: []byte("s2")},
	})
	defer cleanup()

	iter := generatePartKeyIter()
	defer releasePartKeyIter(iter)

	iter.init(part, []common.SeriesID{1, 2}, 0, 200, nil)

	for iter.nextBlock() {
		blocks := iter.currentBlocks()
		require.NotEmpty(t, blocks)
	}
	require.NoError(t, iter.error())

	assert.False(t, iter.nextBlock(), "iterator should report exhaustion")
	assert.NoError(t, iter.error())
}

func TestPartKeyIterSkipsPrimaryBeyondMaxSID(t *testing.T) {
	part, cleanup := setupPartForKeyIter(t, []testElement{
		{seriesID: 1, userKey: 100, data: []byte("s1")},
		{seriesID: 50, userKey: 200, data: []byte("s50")},
	})

	defer cleanup()

	require.NotEmpty(t, part.primaryBlockMetadata, "part should have at least one primary block")

	first := part.primaryBlockMetadata[0]
	part.primaryBlockMetadata = append(part.primaryBlockMetadata, primaryBlockMetadata{
		seriesID: 50,
		minKey:   300,
		maxKey:   300,
		dataBlock: dataBlock{
			offset: first.offset + first.size + 1,
			size:   1,
		},
	})

	iter := generatePartKeyIter()
	defer releasePartKeyIter(iter)

	iter.init(part, []common.SeriesID{1}, 0, 500, nil)

	require.NoError(t, iter.error(), "initialisation should succeed despite poisoned secondary primary block")

	var seriesIDs []common.SeriesID
	for iter.nextBlock() {
		blocks := iter.currentBlocks()
		require.NotEmpty(t, blocks)
		for _, block := range blocks {
			seriesIDs = append(seriesIDs, block.seriesID)
		}
	}
	require.NoError(t, iter.error())
	assert.Equal(t, []common.SeriesID{1}, seriesIDs, "only requested series should appear")
	assert.Equal(t, 1, len(iter.primaryCache), "only one primary block should be cached")
}

func TestPartKeyIterRequeuesOnGapBetweenBlocks(t *testing.T) {
	const elementsPerBatch = maxBlockLength + 10

	var elems []testElement
	for i := 0; i < elementsPerBatch; i++ {
		elems = append(elems, testElement{
			seriesID: 1,
			userKey:  int64(i),
			data:     []byte("batch1"),
		})
	}
	for i := 0; i < 16; i++ {
		elems = append(elems, testElement{
			seriesID: 1,
			userKey:  int64(50000 + i),
			data:     []byte("batch2"),
		})
	}

	part, cleanup := setupPartForKeyIter(t, elems)
	defer cleanup()

	iter := generatePartKeyIter()
	defer releasePartKeyIter(iter)

	iter.init(part, []common.SeriesID{1}, 0, 100000, nil)

	var groups []struct {
		min int64
		max int64
	}
	for iter.nextBlock() {
		blocks := iter.currentBlocks()
		require.NotEmpty(t, blocks)
		group := struct {
			min int64
			max int64
		}{min: blocks[0].minKey, max: blocks[0].maxKey}
		for _, block := range blocks[1:] {
			if block.minKey < group.min {
				group.min = block.minKey
			}
			if block.maxKey > group.max {
				group.max = block.maxKey
			}
		}
		groups = append(groups, group)
	}

	require.NoError(t, iter.error())
	require.GreaterOrEqual(t, len(groups), 2, "expected at least two block groups for the same series")

	first := groups[0]
	second := groups[1]
	assert.Greater(t, second.min, first.max, "subsequent block group should start after the previous group's boundary, indicating requeue")
}
