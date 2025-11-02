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
	"github.com/apache/skywalking-banyandb/pkg/index"
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

type blockExpectation struct {
	seriesID common.SeriesID
	minKey   int64
	maxKey   int64
}

func runPartKeyIterPass(t *testing.T, part *part, sids []common.SeriesID, minKey, maxKey int64, blockFilter index.Filter, asc bool) ([]blockExpectation, error) {
	iter := generatePartKeyIter()
	defer releasePartKeyIter(iter)

	iter.init(part, sids, minKey, maxKey, blockFilter, asc)

	var results []blockExpectation
	for iter.nextBlock() {
		block, _ := iter.current()
		require.NotNil(t, block)
		results = append(results, blockExpectation{
			seriesID: block.seriesID,
			minKey:   block.minKey,
			maxKey:   block.maxKey,
		})
	}

	return results, iter.error()
}

func runPartKeyIterDoublePass(t *testing.T, part *part, sids []common.SeriesID, minKey, maxKey int64,
	blockFilter index.Filter,
) (ascBlocks, descBlocks []blockExpectation, ascErr, descErr error) {
	ascBlocks, ascErr = runPartKeyIterPass(t, part, sids, minKey, maxKey, blockFilter, true)
	descBlocks, descErr = runPartKeyIterPass(t, part, sids, minKey, maxKey, blockFilter, false)
	return
}

func orderName(asc bool) string {
	if asc {
		return "asc"
	}
	return "desc"
}

func verifyDescendingOrder(t *testing.T, blocks []blockExpectation) {
	require.True(t, sort.SliceIsSorted(blocks, func(i, j int) bool {
		if blocks[i].minKey == blocks[j].minKey {
			return blocks[i].seriesID > blocks[j].seriesID
		}
		return blocks[i].minKey >= blocks[j].minKey
	}), "blocks should be in non-increasing order by minKey")
}

func verifyAscendingOrder(t *testing.T, blocks []blockExpectation) {
	require.True(t, sort.SliceIsSorted(blocks, func(i, j int) bool {
		if blocks[i].minKey == blocks[j].minKey {
			return blocks[i].seriesID < blocks[j].seriesID
		}
		return blocks[i].minKey <= blocks[j].minKey
	}), "blocks should be in non-decreasing order by minKey")
}

func TestPartKeyIterOrdersBlocksByMinKey(t *testing.T) {
	part, cleanup := setupPartForKeyIter(t, []testElement{
		{seriesID: 1, userKey: 100, data: []byte("s1")},
		{seriesID: 2, userKey: 50, data: []byte("s2")},
		{seriesID: 3, userKey: 150, data: []byte("s3")},
	})
	defer cleanup()

	asc, desc, ascErr, descErr := runPartKeyIterDoublePass(t, part, []common.SeriesID{1, 2, 3}, 0, 200, nil)
	require.NoError(t, ascErr)
	require.NoError(t, descErr)

	require.Len(t, asc, 3, "expected three blocks in ascending order")
	verifyAscendingOrder(t, asc)
	assert.Equal(t, []common.SeriesID{2, 1, 3}, []common.SeriesID{asc[0].seriesID, asc[1].seriesID, asc[2].seriesID})

	require.Len(t, desc, 3, "expected three blocks in descending order")
	verifyDescendingOrder(t, desc)
	assert.Equal(t, []common.SeriesID{3, 1, 2}, []common.SeriesID{desc[0].seriesID, desc[1].seriesID, desc[2].seriesID})
}

func TestPartKeyIterFiltersSeriesIDs(t *testing.T) {
	part, cleanup := setupPartForKeyIter(t, []testElement{
		{seriesID: 1, userKey: 100, data: []byte("s1")},
		{seriesID: 2, userKey: 50, data: []byte("s2")},
		{seriesID: 3, userKey: 150, data: []byte("s3")},
	})
	defer cleanup()

	asc, desc, ascErr, descErr := runPartKeyIterDoublePass(t, part, []common.SeriesID{1, 3}, 0, 200, nil)
	require.NoError(t, ascErr)
	require.NoError(t, descErr)
	require.Len(t, asc, 2)
	require.Len(t, desc, 2)
	require.Equal(t, []common.SeriesID{1, 3}, []common.SeriesID{asc[0].seriesID, asc[1].seriesID})
	require.Equal(t, []common.SeriesID{3, 1}, []common.SeriesID{desc[0].seriesID, desc[1].seriesID})
}

func TestPartKeyIterAppliesKeyRange(t *testing.T) {
	part, cleanup := setupPartForKeyIter(t, []testElement{
		{seriesID: 1, userKey: 100, data: []byte("s1")},
		{seriesID: 2, userKey: 50, data: []byte("s2")},
		{seriesID: 3, userKey: 150, data: []byte("s3")},
	})
	defer cleanup()

	asc, desc, ascErr, descErr := runPartKeyIterDoublePass(t, part, []common.SeriesID{1, 2, 3}, 120, 200, nil)
	require.NoError(t, ascErr)
	require.NoError(t, descErr)
	require.Len(t, asc, 1)
	require.Len(t, desc, 1)
	assert.Equal(t, common.SeriesID(3), asc[0].seriesID)
	assert.Equal(t, common.SeriesID(3), desc[0].seriesID)
}

func TestPartKeyIterNoBlocksInRange(t *testing.T) {
	part, cleanup := setupPartForKeyIter(t, []testElement{
		{seriesID: 1, userKey: 100, data: []byte("s1")},
		{seriesID: 2, userKey: 50, data: []byte("s2")},
	})
	defer cleanup()

	asc, desc, ascErr, descErr := runPartKeyIterDoublePass(t, part, []common.SeriesID{1, 2}, 1000, 2000, nil)
	require.NoError(t, ascErr)
	require.NoError(t, descErr)
	require.Empty(t, asc)
	require.Empty(t, desc)
}

func TestPartKeyIterHandlesEmptySeries(t *testing.T) {
	part, cleanup := setupPartForKeyIter(t, []testElement{
		{seriesID: 1, userKey: 100, data: []byte("s1")},
	})
	defer cleanup()

	asc, desc, ascErr, descErr := runPartKeyIterDoublePass(t, part, []common.SeriesID{}, 0, 200, nil)
	require.NoError(t, ascErr)
	require.NoError(t, descErr)
	require.Len(t, asc, 0)
	require.Len(t, desc, 0)
}

func TestPartKeyIterHonorsBlockFilter(t *testing.T) {
	part, cleanup := setupPartForKeyIter(t, []testElement{
		{seriesID: 1, userKey: 100, data: []byte("s1")},
		{seriesID: 2, userKey: 150, data: []byte("s2")},
	})
	defer cleanup()

	asc, desc, ascErr, descErr := runPartKeyIterDoublePass(t, part, []common.SeriesID{1, 2}, 0, 200, &mockBlockFilter{shouldSkip: true})
	require.NoError(t, ascErr)
	require.NoError(t, descErr)
	require.Empty(t, asc)
	require.Empty(t, desc)
}

func TestPartKeyIterPropagatesFilterError(t *testing.T) {
	part, cleanup := setupPartForKeyIter(t, []testElement{
		{seriesID: 1, userKey: 100, data: []byte("s1")},
	})
	defer cleanup()

	expectedErr := errors.New("filter failure")
	asc, desc, ascErr, descErr := runPartKeyIterDoublePass(t, part, []common.SeriesID{1}, 0, 200, &mockBlockFilter{err: expectedErr})
	require.ErrorIs(t, ascErr, expectedErr)
	require.ErrorIs(t, descErr, expectedErr)
	require.Empty(t, asc)
	require.Empty(t, desc)
}

func TestPartKeyIterBreaksTiesBySeriesID(t *testing.T) {
	part, cleanup := setupPartForKeyIter(t, []testElement{
		{seriesID: 1, userKey: 100, data: []byte("s1")},
		{seriesID: 2, userKey: 100, data: []byte("s2")},
	})
	defer cleanup()

	asc, desc, ascErr, descErr := runPartKeyIterDoublePass(t, part, []common.SeriesID{1, 2}, 0, 200, nil)
	require.NoError(t, ascErr)
	require.NoError(t, descErr)
	require.Len(t, asc, 2)
	require.Len(t, desc, 2)
	assert.Equal(t, []common.SeriesID{1, 2}, []common.SeriesID{asc[0].seriesID, asc[1].seriesID})
	assert.Equal(t, []common.SeriesID{2, 1}, []common.SeriesID{desc[0].seriesID, desc[1].seriesID})
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

	asc, desc, ascErr, descErr := runPartKeyIterDoublePass(t, part, []common.SeriesID{1, 2, 3}, 0, 500, nil)
	require.NoError(t, ascErr)
	require.NoError(t, descErr)
	require.Len(t, asc, 3)
	require.Len(t, desc, 3)

	expectedAsc := []blockExpectation{
		{seriesID: 1, minKey: 100, maxKey: 180},
		{seriesID: 2, minKey: 120, maxKey: 220},
		{seriesID: 3, minKey: 400, maxKey: 400},
	}
	assert.Equal(t, expectedAsc, asc)
	for i := range desc {
		assert.Equal(t, expectedAsc[len(expectedAsc)-1-i], desc[i])
	}

	iter := generatePartKeyIter()
	defer releasePartKeyIter(iter)
	iter.init(part, []common.SeriesID{1, 2, 3}, 0, 500, nil, true)
	var ids []common.SeriesID
	for iter.nextBlock() {
		block, _ := iter.current()
		require.NotNil(t, block)
		ids = append(ids, block.seriesID)
	}
	require.NoError(t, iter.error())
	require.GreaterOrEqual(t, len(ids), 3, "expected at least three blocks")
	// Verify we get blocks from all three series
	require.Contains(t, ids, common.SeriesID(1))
	require.Contains(t, ids, common.SeriesID(2))
	require.Contains(t, ids, common.SeriesID(3))
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

	filter := &selectiveMockBlockFilter{
		tagName:   "status",
		skipValue: "pending",
	}

	asc, desc, ascErr, descErr := runPartKeyIterDoublePass(t, part, []common.SeriesID{1, 2}, 0, 50000, filter)
	require.NoError(t, ascErr)
	require.NoError(t, descErr)
	assert.Greater(t, filter.skipCallCount, 0, "filter should have been invoked")

	require.NotEmpty(t, asc)
	require.NotEmpty(t, desc)

	seriesTwoCountAsc := 0
	for _, block := range asc {
		if block.seriesID == 1 {
			require.GreaterOrEqual(t, block.minKey, int64(20000), "pending block should be skipped in ascending order")
		}
		if block.seriesID == 2 {
			seriesTwoCountAsc++
		}
	}
	require.Equal(t, 1, seriesTwoCountAsc, "series 2 block should appear once in ascending results")

	seriesTwoCountDesc := 0
	for _, block := range desc {
		if block.seriesID == 1 {
			require.GreaterOrEqual(t, block.minKey, int64(20000), "pending block should be skipped in descending order")
		}
		if block.seriesID == 2 {
			seriesTwoCountDesc++
		}
	}
	require.Equal(t, 1, seriesTwoCountDesc, "series 2 block should appear once in descending results")
}

func TestPartKeyIterExhaustion(t *testing.T) {
	part, cleanup := setupPartForKeyIter(t, []testElement{
		{seriesID: 1, userKey: 100, data: []byte("s1")},
		{seriesID: 2, userKey: 120, data: []byte("s2")},
	})
	defer cleanup()

	for _, asc := range []bool{true, false} {
		t.Run(orderName(asc), func(t *testing.T) {
			iter := generatePartKeyIter()
			defer releasePartKeyIter(iter)

			iter.init(part, []common.SeriesID{1, 2}, 0, 200, nil, asc)

			blockCount := 0
			for iter.nextBlock() {
				block, _ := iter.current()
				require.NotNil(t, block)
				blockCount++
			}
			require.NoError(t, iter.error())
			require.Greater(t, blockCount, 0, "iterator should yield at least one block")

			assert.False(t, iter.nextBlock(), "iterator should report exhaustion")
			assert.NoError(t, iter.error())
		})
	}
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

	asc, desc, ascErr, descErr := runPartKeyIterDoublePass(t, part, []common.SeriesID{1}, 0, 500, nil)
	require.NoError(t, ascErr)
	require.NoError(t, descErr)
	require.NotEmpty(t, asc)
	require.NotEmpty(t, desc)
	for _, block := range asc {
		assert.Equal(t, common.SeriesID(1), block.seriesID)
	}
	for _, block := range desc {
		assert.Equal(t, common.SeriesID(1), block.seriesID)
	}
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

	for _, asc := range []bool{true, false} {
		t.Run(orderName(asc), func(t *testing.T) {
			iter := generatePartKeyIter()
			defer releasePartKeyIter(iter)

			iter.init(part, []common.SeriesID{1}, 0, 100000, nil, asc)

			var blocks []struct {
				min int64
				max int64
			}
			for iter.nextBlock() {
				block, _ := iter.current()
				require.NotNil(t, block)
				blocks = append(blocks, struct {
					min int64
					max int64
				}{min: block.minKey, max: block.maxKey})
			}

			require.NoError(t, iter.error())
			require.GreaterOrEqual(t, len(blocks), 2, "expected at least two blocks for the same series")

			// Verify blocks are in proper order
			for i := 1; i < len(blocks); i++ {
				prev := blocks[i-1]
				curr := blocks[i]
				if asc {
					assert.LessOrEqual(t, prev.min, curr.min, "ascending iteration should maintain order")
				} else {
					assert.GreaterOrEqual(t, prev.max, curr.max, "descending iteration should maintain order")
				}
			}
		})
	}
}
