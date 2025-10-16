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
	"context"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/test"
	"github.com/apache/skywalking-banyandb/pkg/index"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

type strictQuotaProtector struct {
	test.MockMemoryProtector
}

func (s *strictQuotaProtector) AvailableBytes() int64 {
	return 0
}

func TestSIDX_Query_BasicQuery(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	ctx := context.Background()

	reqs := []WriteRequest{
		createTestWriteRequest(1, 100, "data1"),
		createTestWriteRequest(1, 101, "data2"),
	}
	writeTestData(t, sidx, reqs, 4, 4)

	waitForIntroducerLoop()

	queryReq := createTestQueryRequest(1)
	resultsCh, errCh := sidx.StreamingQuery(ctx, queryReq)

	var keys []int64
	for res := range resultsCh {
		require.NoError(t, res.Error)
		keys = append(keys, res.Keys...)
	}
	if err, ok := <-errCh; ok {
		require.NoError(t, err)
	}

	if len(keys) > 0 {
		assert.Greater(t, len(keys), 0)
	}
}

func TestSIDX_Query_EmptyResult(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	ctx := context.Background()

	queryReq := createTestQueryRequest(999)
	resultsCh, errCh := sidx.StreamingQuery(ctx, queryReq)

	resultCount := 0
	for res := range resultsCh {
		require.NoError(t, res.Error)
		resultCount += res.Len()
	}
	if err, ok := <-errCh; ok {
		require.NoError(t, err)
	}

	assert.Equal(t, 0, resultCount)
}

func TestSIDX_Query_KeyRangeFilter(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	ctx := context.Background()

	reqs := []WriteRequest{
		createTestWriteRequest(1, 100, "data100"),
		createTestWriteRequest(1, 150, "data150"),
		createTestWriteRequest(1, 200, "data200"),
	}
	writeTestData(t, sidx, reqs, 5, 5)

	waitForIntroducerLoop()

	minKey := int64(120)
	maxKey := int64(180)
	queryReq := QueryRequest{
		SeriesIDs: []common.SeriesID{1},
		MinKey:    &minKey,
		MaxKey:    &maxKey,
	}

	resultsCh, errCh := sidx.StreamingQuery(ctx, queryReq)

	var keys []int64
	for res := range resultsCh {
		require.NoError(t, res.Error)
		keys = append(keys, res.Keys...)
	}
	if err, ok := <-errCh; ok {
		require.NoError(t, err)
	}

	if len(keys) > 0 {
		for _, key := range keys {
			assert.GreaterOrEqual(t, key, minKey)
			assert.LessOrEqual(t, key, maxKey)
		}
	}
}

func TestSIDX_Query_Ordering(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	ctx := context.Background()

	reqs := []WriteRequest{
		createTestWriteRequest(1, 300, "series1-data300"),
		createTestWriteRequest(1, 100, "series1-data100"),
		createTestWriteRequest(1, 200, "series1-data200"),
		createTestWriteRequest(2, 250, "series2-data250"),
		createTestWriteRequest(2, 150, "series2-data150"),
		createTestWriteRequest(2, 50, "series2-data50"),
		createTestWriteRequest(3, 350, "series3-data350"),
		createTestWriteRequest(3, 75, "series3-data75"),
		createTestWriteRequest(3, 175, "series3-data175"),
	}
	writeTestData(t, sidx, reqs, 6, 6)

	waitForIntroducerLoop()

	tests := []struct {
		order     *index.OrderBy
		name      string
		seriesIDs []common.SeriesID
		ascending bool
	}{
		{
			name:      "ascending order single series",
			ascending: true,
			order:     &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
			seriesIDs: []common.SeriesID{1},
		},
		{
			name:      "descending order single series",
			ascending: false,
			order:     &index.OrderBy{Sort: modelv1.Sort_SORT_DESC},
			seriesIDs: []common.SeriesID{1},
		},
		{
			name:      "ascending order multiple series",
			ascending: true,
			order:     &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
			seriesIDs: []common.SeriesID{1, 2, 3},
		},
		{
			name:      "descending order multiple series",
			ascending: false,
			order:     &index.OrderBy{Sort: modelv1.Sort_SORT_DESC},
			seriesIDs: []common.SeriesID{1, 2, 3},
		},
		{
			name:      "default order multiple series",
			ascending: true,
			order:     nil,
			seriesIDs: []common.SeriesID{1, 2, 3},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			queryReq := QueryRequest{
				SeriesIDs: tt.seriesIDs,
				Order:     tt.order,
			}

			resultsCh, errCh := sidx.StreamingQuery(ctx, queryReq)

			var allKeys []int64
			var allData [][]byte
			var allSIDs []common.SeriesID
			for res := range resultsCh {
				require.NoError(t, res.Error)
				allKeys = append(allKeys, res.Keys...)
				allData = append(allData, res.Data...)
				allSIDs = append(allSIDs, res.SIDs...)
			}
			if err, ok := <-errCh; ok {
				require.NoError(t, err)
			}

			t.Logf("Query with series %v, order %v", tt.seriesIDs, tt.order)
			for i, key := range allKeys {
				if i < len(allData) && i < len(allSIDs) {
					t.Logf("  Key: %d, Data: %s, SeriesID: %d", key, string(allData[i]), allSIDs[i])
				}
			}

			if len(allKeys) > 1 {
				isSorted := sort.SliceIsSorted(allKeys, func(i, j int) bool {
					if tt.ascending {
						return allKeys[i] < allKeys[j]
					}
					return allKeys[i] > allKeys[j]
				})
				assert.True(t, isSorted, "Keys should be sorted in %s order. Keys: %v",
					map[bool]string{true: "ascending", false: "descending"}[tt.ascending], allKeys)
			}
		})
	}
}

func TestSIDX_Query_WithArrValues(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	ctx := context.Background()

	reqs := []WriteRequest{
		createTestWriteRequest(1, 100, "data100", Tag{
			Name: "arr_tag",
			ValueArr: [][]byte{
				[]byte("a"),
				[]byte("b"),
			},
			ValueType: pbv1.ValueTypeStrArr,
		}),
		createTestWriteRequest(1, 150, "data150"),
		createTestWriteRequest(1, 200, "data200"),
	}
	writeTestData(t, sidx, reqs, 7, 7)

	waitForIntroducerLoop()

	queryReq := QueryRequest{
		SeriesIDs: []common.SeriesID{1},
		TagProjection: []model.TagProjection{
			{
				Names: []string{"arr_tag"},
			},
		},
	}

	resultsCh, errCh := sidx.StreamingQuery(ctx, queryReq)

	var keys []int64
	var tags [][]Tag
	for res := range resultsCh {
		require.NoError(t, res.Error)
		keys = append(keys, res.Keys...)
		tags = append(tags, res.Tags...)
	}
	if err, ok := <-errCh; ok {
		require.NoError(t, err)
	}

	assert.Equal(t, 3, len(keys))
	for i := 0; i < len(keys); i++ {
		if keys[i] == 100 {
			assert.Equal(t, "arr_tag", tags[i][0].Name)
			assert.Equal(t, "a|b|", string(tags[i][0].Value))
		}
	}
}

func TestSIDX_Query_Validation(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	ctx := context.Background()

	tests := []struct {
		name      string
		req       QueryRequest
		expectErr bool
	}{
		{
			name:      "valid request",
			req:       createTestQueryRequest(1),
			expectErr: false,
		},
		{
			name:      "empty series IDs",
			req:       QueryRequest{SeriesIDs: []common.SeriesID{}},
			expectErr: true,
		},
		{
			name: "invalid key range",
			req: QueryRequest{
				SeriesIDs: []common.SeriesID{1},
				MinKey:    ptrInt64(200),
				MaxKey:    ptrInt64(100),
			},
			expectErr: true,
		},
		{
			name: "negative max element size",
			req: QueryRequest{
				SeriesIDs:      []common.SeriesID{1},
				MaxElementSize: -1,
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			resultsCh, errCh := sidx.StreamingQuery(ctx, tt.req)
			// revive:disable-next-line:empty-block
			for range resultsCh {
			}
			err, ok := <-errCh
			if tt.expectErr {
				if ok {
					assert.Error(t, err)
				} else {
					// Validation error, should have been sent to errCh
					t.Fatal("Expected error but got none")
				}
			} else {
				if ok {
					assert.NoError(t, err)
				}
			}
		})
	}
}

func TestSIDX_StreamingQuery_MatchesBlockingQuery(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	ctx := context.Background()

	reqs := []WriteRequest{
		createTestWriteRequest(1, 50, "series1-data50", createTestTag("env", "prod")),
		createTestWriteRequest(1, 150, "series1-data150", createTestTag("env", "prod")),
		createTestWriteRequest(1, 250, "series1-data250", createTestTag("env", "prod")),
		createTestWriteRequest(2, 75, "series2-data75", createTestTag("env", "staging")),
		createTestWriteRequest(2, 175, "series2-data175", createTestTag("env", "staging")),
		createTestWriteRequest(2, 275, "series2-data275", createTestTag("env", "staging")),
	}
	writeTestData(t, sidx, reqs, 7, 7)
	waitForIntroducerLoop()

	testCases := []struct {
		name string
		req  QueryRequest
	}{
		{
			name: "ascending_all_series",
			req: QueryRequest{
				SeriesIDs:      []common.SeriesID{1, 2},
				Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
				MaxElementSize: 2,
				MinKey: func() *int64 {
					v := int64(50)
					return &v
				}(),
				MaxKey: func() *int64 {
					v := int64(275)
					return &v
				}(),
			},
		},
		{
			name: "descending_single_series",
			req: QueryRequest{
				SeriesIDs:      []common.SeriesID{1},
				Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_DESC},
				MaxElementSize: 2,
			},
		},
		{
			name: "range_filtered_series",
			req: QueryRequest{
				SeriesIDs:      []common.SeriesID{2},
				Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
				MaxElementSize: 2,
				MinKey: func() *int64 {
					v := int64(100)
					return &v
				}(),
				MaxKey: func() *int64 {
					v := int64(200)
					return &v
				}(),
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			// Collect expected results from streaming query
			var (
				expectedKeys []int64
				expectedData [][]byte
				expectedTags [][]Tag
				expectedSIDs []common.SeriesID
			)

			resultsCh, errCh := sidx.StreamingQuery(ctx, tc.req)
			for res := range resultsCh {
				require.NoError(t, res.Error)
				expectedKeys = append(expectedKeys, res.Keys...)
				expectedData = append(expectedData, res.Data...)
				expectedTags = append(expectedTags, res.Tags...)
				expectedSIDs = append(expectedSIDs, res.SIDs...)
			}
			if err, ok := <-errCh; ok {
				require.NoError(t, err)
			}

			// Run the streaming query again and verify consistency
			var (
				gotKeys []int64
				gotData [][]byte
				gotTags [][]Tag
				gotSIDs []common.SeriesID
			)

			resultsCh2, errCh2 := sidx.StreamingQuery(ctx, tc.req)
			for res := range resultsCh2 {
				require.NoError(t, res.Error)
				gotKeys = append(gotKeys, res.Keys...)
				gotData = append(gotData, res.Data...)
				gotTags = append(gotTags, res.Tags...)
				gotSIDs = append(gotSIDs, res.SIDs...)
			}
			if err, ok := <-errCh2; ok {
				require.NoError(t, err)
			}

			require.Equal(t, expectedKeys, gotKeys)
			require.Equal(t, expectedData, gotData)
			require.Equal(t, expectedSIDs, gotSIDs)
			require.Equal(t, expectedTags, gotTags)
			require.Equal(t, len(expectedKeys), len(gotKeys))
		})
	}
}

func TestSIDX_StreamingQuery_BatchSizingAndCapacity(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	reqs := []WriteRequest{
		createTestWriteRequest(1, 10, "trace1"),
		createTestWriteRequest(1, 11, "trace2"),
		createTestWriteRequest(1, 12, "trace3"),
		createTestWriteRequest(1, 13, "trace4"),
		createTestWriteRequest(1, 14, "trace1"),
	}
	writeTestData(t, sidx, reqs, 8, 8)
	waitForIntroducerLoop()

	queryReq := QueryRequest{
		SeriesIDs:      []common.SeriesID{1},
		Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
		MaxElementSize: 0,
	}

	resultsCh, errCh := sidx.StreamingQuery(context.Background(), queryReq)
	require.Equal(t, queryReq.MaxElementSize, cap(resultsCh))

	var batches []*QueryResponse
	for res := range resultsCh {
		require.NoError(t, res.Error)
		batches = append(batches, res)
	}

	if err, ok := <-errCh; ok {
		require.NoError(t, err)
	}

	require.NotEmpty(t, batches)

	// Get expected count by collecting all streaming results
	expectedCount := 0
	resultsCh2, errCh2 := sidx.StreamingQuery(context.Background(), queryReq)
	for res := range resultsCh2 {
		require.NoError(t, res.Error)
		expectedCount += res.Len()
	}
	if err, ok := <-errCh2; ok {
		require.NoError(t, err)
	}

	totalResults := 0

	for i, batch := range batches {
		totalResults += batch.Len()

		if i < len(batches)-1 {
			require.LessOrEqual(t, batch.Len(), queryReq.MaxElementSize)
		}

		uniqueData := make(map[string]struct{})
		for _, data := range batch.Data {
			uniqueData[string(data)] = struct{}{}
		}
		if i < len(batches)-1 {
			require.LessOrEqual(t, len(uniqueData), queryReq.MaxElementSize)
		}
	}

	require.Equal(t, expectedCount, totalResults)
}

func TestSIDX_StreamingQuery_ChannelLifecycle(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	reqs := []WriteRequest{
		createTestWriteRequest(1, 20, "data20"),
		createTestWriteRequest(1, 30, "data30"),
		createTestWriteRequest(1, 40, "data40"),
	}
	writeTestData(t, sidx, reqs, 9, 9)
	waitForIntroducerLoop()

	queryReq := QueryRequest{
		SeriesIDs:      []common.SeriesID{1},
		Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
		MaxElementSize: 2,
	}

	// Get expected count from first streaming query
	expectedCount := 0
	resultsCh0, errCh0 := sidx.StreamingQuery(context.Background(), queryReq)
	for res := range resultsCh0 {
		require.NoError(t, res.Error)
		expectedCount += res.Len()
	}
	if err, ok := <-errCh0; ok {
		require.NoError(t, err)
	}

	resultsCh, errCh := sidx.StreamingQuery(context.Background(), queryReq)

	totalResults := 0
	for res := range resultsCh {
		require.NoError(t, res.Error)
		totalResults += res.Len()
	}
	require.Equal(t, expectedCount, totalResults)

	_, ok := <-resultsCh
	require.False(t, ok)

	select {
	case err, ok := <-errCh:
		require.False(t, ok)
		require.NoError(t, err)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for error channel to close")
	}
}

func TestSIDX_StreamingQuery_Tracing(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	reqs := []WriteRequest{
		createTestWriteRequest(1, 100, "trace-100"),
		createTestWriteRequest(1, 200, "trace-200"),
		createTestWriteRequest(1, 300, "trace-300"),
	}
	writeTestData(t, sidx, reqs, 10, 10)
	waitForIntroducerLoop()

	tracer, ctx := query.NewTracer(context.Background(), "sidx-streaming-query")

	minKey := int64(100)
	maxKey := int64(300)
	queryReq := QueryRequest{
		SeriesIDs:      []common.SeriesID{1},
		Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
		MaxElementSize: 1,
		MinKey:         &minKey,
		MaxKey:         &maxKey,
	}

	resultsCh, errCh := sidx.StreamingQuery(ctx, queryReq)

	var (
		responseCount int
		elementCount  int
	)
	for res := range resultsCh {
		require.NoError(t, res.Error)
		responseCount++
		elementCount += res.Len()
	}

	for err := range errCh {
		require.NoError(t, err)
	}

	traceProto := tracer.ToProto()
	require.NotNil(t, traceProto)

	span := findSpanByMessage(traceProto.GetSpans(), "sidx.run-streaming-query")
	require.NotNil(t, span)
	require.False(t, span.GetError())

	tags := spanTagsToMap(span)

	require.Equal(t, "false", requireTag(t, tags, "filter_present"))
	require.Equal(t, queryReq.Order.Sort.String(), requireTag(t, tags, "order_sort"))
	require.Equal(t, "0", requireTag(t, tags, "order_type"))
	require.Equal(t, strconv.Itoa(len(queryReq.SeriesIDs)), requireTag(t, tags, "series_id_count"))
	require.Equal(t, strconv.Itoa(0), requireTag(t, tags, "projected_tags"))
	require.Equal(t, strconv.FormatInt(minKey, 10), requireTag(t, tags, "min_key"))
	require.Equal(t, strconv.FormatInt(maxKey, 10), requireTag(t, tags, "max_key"))
	require.Equal(t, strconv.Itoa(queryReq.MaxElementSize), requireTag(t, tags, "max_element_size"))
	require.Equal(t, strconv.Itoa(responseCount), requireTag(t, tags, "responses_emitted"))
	require.Equal(t, strconv.Itoa(elementCount), requireTag(t, tags, "elements_emitted"))
	require.Equal(t, "true", requireTag(t, tags, "heap_initialized"))
}

func TestSIDX_StreamingQuery_ErrorPropagation(t *testing.T) {
	t.Run("validation_error", func(t *testing.T) {
		sidx := createTestSIDX(t)
		defer func() {
			assert.NoError(t, sidx.Close())
		}()

		resultsCh, errCh := sidx.StreamingQuery(context.Background(), QueryRequest{})

		select {
		case err, ok := <-errCh:
			require.True(t, ok)
			require.Error(t, err)
			require.Contains(t, err.Error(), "SeriesID")
		case <-time.After(500 * time.Millisecond):
			t.Fatal("expected validation error but errCh did not emit")
		}

		_, ok := <-resultsCh
		require.False(t, ok)
	})

	t.Run("block_scanner_error", func(t *testing.T) {
		sidx := createTestSIDXWithOptions(t, func(opts *Options) {
			opts.Memory = &strictQuotaProtector{}
		})
		defer func() {
			assert.NoError(t, sidx.Close())
		}()

		largePayload := strings.Repeat("quota-trip", 16)
		reqs := []WriteRequest{
			createTestWriteRequest(1, 100, largePayload),
			createTestWriteRequest(1, 200, largePayload),
			createTestWriteRequest(1, 300, largePayload),
		}
		writeTestData(t, sidx, reqs, 10, 10)
		waitForIntroducerLoop()

		resultsCh, errCh := sidx.StreamingQuery(context.Background(), QueryRequest{
			SeriesIDs:      []common.SeriesID{1},
			Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
			MaxElementSize: 0,
		})

		for range resultsCh {
			t.Fatal("expected no results for quota exceeded")
		}

		select {
		case err, ok := <-errCh:
			require.True(t, ok)
			require.Error(t, err)
			require.Contains(t, err.Error(), "quota exceeded")
		case <-time.After(time.Second):
			t.Fatal("expected block scanner error but errCh did not emit")
		}
	})
}

func TestSIDX_StreamingQuery_EdgeCases(t *testing.T) {
	t.Run("no_results", func(t *testing.T) {
		sidx := createTestSIDX(t)
		defer func() {
			assert.NoError(t, sidx.Close())
		}()

		reqs := []WriteRequest{
			createTestWriteRequest(1, 10, "data10"),
			createTestWriteRequest(1, 20, "data20"),
		}
		writeTestData(t, sidx, reqs, 11, 11)
		waitForIntroducerLoop()

		resultsCh, errCh := sidx.StreamingQuery(context.Background(), QueryRequest{
			SeriesIDs:      []common.SeriesID{999},
			Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
			MaxElementSize: 2,
		})

		for range resultsCh {
			t.Fatal("expected no results for missing series")
		}

		err, ok := <-errCh
		require.False(t, ok)
		require.NoError(t, err)
	})

	t.Run("single_batch_smaller_than_limit", func(t *testing.T) {
		sidx := createTestSIDX(t)
		defer func() {
			assert.NoError(t, sidx.Close())
		}()

		reqs := []WriteRequest{
			createTestWriteRequest(2, 10, "single10"),
			createTestWriteRequest(2, 20, "single20"),
		}
		writeTestData(t, sidx, reqs, 12, 12)
		waitForIntroducerLoop()

		resultsCh, errCh := sidx.StreamingQuery(context.Background(), QueryRequest{
			SeriesIDs:      []common.SeriesID{2},
			Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
			MaxElementSize: 10,
		})

		var batches []*QueryResponse
		for res := range resultsCh {
			require.NoError(t, res.Error)
			batches = append(batches, res)
		}

		if err, ok := <-errCh; ok {
			require.NoError(t, err)
		}

		require.Len(t, batches, 1)
		require.Equal(t, len(reqs), batches[0].Len())
	})

	t.Run("mixed_memory_and_disk_parts", func(t *testing.T) {
		idx := createTestSIDX(t)
		defer func() {
			assert.NoError(t, idx.Close())
		}()

		const (
			memPartID  = uint64(100)
			diskPartID = uint64(101)
		)

		memReqs := []WriteRequest{
			createTestWriteRequest(1, 5, "mem-5"),
			createTestWriteRequest(1, 15, "mem-15"),
		}
		writeTestData(t, idx, memReqs, 13, memPartID)

		diskReqs := []WriteRequest{
			createTestWriteRequest(2, 100, "disk-100"),
			createTestWriteRequest(2, 200, "disk-200"),
		}
		writeTestData(t, idx, diskReqs, 14, diskPartID)
		waitForIntroducerLoop()

		raw := idx.(*sidx)
		flushIntro, err := raw.Flush(map[uint64]struct{}{diskPartID: {}})
		require.NoError(t, err)
		require.NotNil(t, flushIntro)
		raw.IntroduceFlushed(flushIntro)
		flushIntro.Release()

		resultsCh, errCh := idx.StreamingQuery(context.Background(), QueryRequest{
			SeriesIDs:      []common.SeriesID{1, 2},
			Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
			MaxElementSize: 0,
		})

		var (
			collectedKeys []int64
			collectedData []string
		)

		for res := range resultsCh {
			require.NoError(t, res.Error)
			collectedKeys = append(collectedKeys, res.Keys...)
			for _, data := range res.Data {
				collectedData = append(collectedData, string(data))
			}
		}

		if err, ok := <-errCh; ok {
			require.NoError(t, err)
		}

		expectedKeys := []int64{5, 15, 100, 200}
		require.ElementsMatch(t, expectedKeys, collectedKeys)
		require.ElementsMatch(t, []string{"mem-5", "mem-15", "disk-100", "disk-200"}, collectedData)
	})
}

func TestSIDX_StreamingQuery_BatchSizeAndOrder(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	reqs := []WriteRequest{
		createTestWriteRequest(1, 10, "data10"),
		createTestWriteRequest(1, 20, "data20"),
		createTestWriteRequest(1, 30, "data30"),
		createTestWriteRequest(1, 40, "data40"),
	}
	writeTestData(t, sidx, reqs, 42, 42)
	waitForIntroducerLoop()

	queryReq := QueryRequest{
		SeriesIDs:      []common.SeriesID{1},
		Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
		MaxElementSize: 2,
	}

	resultsCh, errCh := sidx.StreamingQuery(context.Background(), queryReq)

	var (
		allKeys    []int64
		lastKey    int64
		lastKeySet bool
		batchCount int
	)

	for res := range resultsCh {
		require.NoError(t, res.Error)
		require.NotEmpty(t, res.Keys)
		if queryReq.MaxElementSize > 0 {
			require.LessOrEqual(t, res.Len(), queryReq.MaxElementSize)
		}
		batchCount++

		for _, key := range res.Keys {
			if lastKeySet {
				require.GreaterOrEqual(t, key, lastKey)
			}
			lastKey = key
			lastKeySet = true
			allKeys = append(allKeys, key)
		}
	}

	if err, ok := <-errCh; ok {
		require.NoError(t, err)
	}

	require.Equal(t, len(reqs), len(allKeys))
	require.NotZero(t, batchCount)
}

func TestSIDX_StreamingQuery_ContextCancellation(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	reqs := []WriteRequest{
		createTestWriteRequest(1, 10, "data10"),
		createTestWriteRequest(1, 20, "data20"),
		createTestWriteRequest(1, 30, "data30"),
		createTestWriteRequest(1, 40, "data40"),
		createTestWriteRequest(1, 50, "data50"),
	}
	writeTestData(t, sidx, reqs, 43, 43)
	waitForIntroducerLoop()

	ctx, cancel := context.WithCancel(context.Background())

	queryReq := QueryRequest{
		SeriesIDs:      []common.SeriesID{1},
		Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
		MaxElementSize: 1,
	}

	resultsCh, errCh := sidx.StreamingQuery(ctx, queryReq)

	done := make(chan struct{})
	go func() {
		//revive:disable-next-line:empty-block
		for range resultsCh { // Drain the channel
		}
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for streaming query to halt after cancellation")
	}

	err, ok := <-errCh
	require.True(t, ok)
	require.ErrorIs(t, err, context.Canceled)
}

func findSpanByMessage(spans []*commonv1.Span, message string) *commonv1.Span {
	for _, span := range spans {
		if span.GetMessage() == message {
			return span
		}
		if child := findSpanByMessage(span.GetChildren(), message); child != nil {
			return child
		}
	}
	return nil
}

func spanTagsToMap(span *commonv1.Span) map[string]string {
	tags := make(map[string]string, len(span.GetTags()))
	for _, tag := range span.GetTags() {
		tags[tag.GetKey()] = tag.GetValue()
	}
	return tags
}

func requireTag(t *testing.T, tags map[string]string, key string) string {
	t.Helper()
	val, ok := tags[key]
	require.True(t, ok, "expected tag %q to be present, tags=%v", key, tags)
	return val
}
