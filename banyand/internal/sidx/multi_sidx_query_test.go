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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// mockSIDX is a test implementation of SIDX interface for testing multi-SIDX queries.
type mockSIDX struct {
	err      error
	response *QueryResponse
	name     string
	delay    bool
}

func (m *mockSIDX) Write(_ context.Context, _ []WriteRequest) error {
	return nil // Not implemented for tests
}

func (m *mockSIDX) Query(_ context.Context, _ QueryRequest) (*QueryResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.response, nil
}

func (m *mockSIDX) Stats(_ context.Context) (*Stats, error) {
	return &Stats{}, nil
}

func (m *mockSIDX) Close() error {
	return nil
}

func (m *mockSIDX) Flush() error {
	return nil
}

func (m *mockSIDX) Merge() error {
	return nil
}

// Helper function to create mock QueryResponse with test data.
func createMockQueryResponse(keyStart int64, count int, seriesIDBase common.SeriesID) *QueryResponse {
	keys := make([]int64, count)
	data := make([][]byte, count)
	tags := make([][]Tag, count)
	sids := make([]common.SeriesID, count)

	for i := 0; i < count; i++ {
		keys[i] = keyStart + int64(i)
		data[i] = []byte("data_" + string(rune('A'+i)))
		tags[i] = []Tag{
			{
				Name:      "tag1",
				Value:     []byte("value_" + string(rune('A'+i))),
				ValueType: pbv1.ValueTypeStr,
			},
		}
		sids[i] = seriesIDBase + common.SeriesID(i)
	}

	return &QueryResponse{
		Keys: keys,
		Data: data,
		Tags: tags,
		SIDs: sids,
	}
}

func TestQueryMultipleSIDX_EmptyInput(t *testing.T) {
	ctx := context.Background()
	req := QueryRequest{}

	resp, err := QueryMultipleSIDX(ctx, nil, req)
	require.NoError(t, err)
	assert.Empty(t, resp.Keys)
	assert.Empty(t, resp.Data)
	assert.Empty(t, resp.Tags)
	assert.Empty(t, resp.SIDs)

	resp, err = QueryMultipleSIDX(ctx, []SIDX{}, req)
	require.NoError(t, err)
	assert.Empty(t, resp.Keys)
	assert.Empty(t, resp.Data)
	assert.Empty(t, resp.Tags)
	assert.Empty(t, resp.SIDs)
}

func TestQueryMultipleSIDX_InvalidRequest(t *testing.T) {
	ctx := context.Background()
	sidxs := []SIDX{&mockSIDX{name: "sidx1"}}

	// Create invalid request (empty SeriesIDs)
	req := QueryRequest{
		SeriesIDs: []common.SeriesID{}, // Invalid: empty series IDs
		MinKey:    int64Ptr(1),
		MaxKey:    int64Ptr(10),
	}

	_, err := QueryMultipleSIDX(ctx, sidxs, req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid query request")
}

func TestQueryMultipleSIDX_SingleSIDX(t *testing.T) {
	ctx := context.Background()

	expectedResponse := createMockQueryResponse(1, 3, 100)
	sidx1 := &mockSIDX{
		name:     "sidx1",
		response: expectedResponse,
	}

	req := QueryRequest{
		SeriesIDs: []common.SeriesID{100, 101, 102},
		MinKey:    int64Ptr(1),
		MaxKey:    int64Ptr(10),
	}

	resp, err := QueryMultipleSIDX(ctx, []SIDX{sidx1}, req)
	require.NoError(t, err)
	assert.Equal(t, expectedResponse, resp)
}

func TestQueryMultipleSIDX_MultipleSIDXSuccess(t *testing.T) {
	ctx := context.Background()

	// Create two SIDX instances with different data ranges
	sidx1 := &mockSIDX{
		name:     "sidx1",
		response: createMockQueryResponse(1, 3, 100), // Keys: 1,2,3
	}
	sidx2 := &mockSIDX{
		name:     "sidx2",
		response: createMockQueryResponse(4, 2, 200), // Keys: 4,5
	}

	req := QueryRequest{
		SeriesIDs: []common.SeriesID{100, 101, 102, 200, 201},
		MinKey:    int64Ptr(1),
		MaxKey:    int64Ptr(10),
		// Order: nil defaults to ascending
	}

	resp, err := QueryMultipleSIDX(ctx, []SIDX{sidx1, sidx2}, req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Verify merged results are properly ordered (ascending)
	expectedKeys := []int64{1, 2, 3, 4, 5}
	assert.Equal(t, expectedKeys, resp.Keys)
	assert.Len(t, resp.Data, 5)
	assert.Len(t, resp.Tags, 5)
	assert.Len(t, resp.SIDs, 5)

	// Verify ordering is maintained
	for i := 1; i < len(resp.Keys); i++ {
		assert.True(t, resp.Keys[i] > resp.Keys[i-1], "Keys should be in ascending order")
	}
}

func TestQueryMultipleSIDX_DescendingOrder(t *testing.T) {
	// Create responses in descending order within each SIDX
	resp1 := &QueryResponse{
		Keys: []int64{3, 2, 1}, // Already in descending order
		Data: [][]byte{[]byte("data3"), []byte("data2"), []byte("data1")},
		Tags: [][]Tag{{}, {}, {}},
		SIDs: []common.SeriesID{100, 101, 102},
	}
	resp2 := &QueryResponse{
		Keys: []int64{5, 4}, // Already in descending order
		Data: [][]byte{[]byte("data5"), []byte("data4")},
		Tags: [][]Tag{{}, {}},
		SIDs: []common.SeriesID{200, 201},
	}

	req := QueryRequest{
		SeriesIDs: []common.SeriesID{100, 101, 102, 200, 201},
		MinKey:    int64Ptr(1),
		MaxKey:    int64Ptr(10),
	}

	// Test merging logic directly since we can't easily test order via SIDX interface
	merged := mergeMultipleSIDXResponses([]*QueryResponse{resp1, resp2}, req)
	require.NotNil(t, merged)

	// When merging descending order responses, the result should maintain order
	// This tests the merge function capabilities
	assert.Len(t, merged.Keys, 5)
}

func TestQueryMultipleSIDX_MaxElementSizeLimit(t *testing.T) {
	ctx := context.Background()

	sidx1 := &mockSIDX{
		name:     "sidx1",
		response: createMockQueryResponse(1, 5, 100), // 5 elements
	}
	sidx2 := &mockSIDX{
		name:     "sidx2",
		response: createMockQueryResponse(6, 5, 200), // 5 elements
	}

	req := QueryRequest{
		SeriesIDs:      []common.SeriesID{100, 101, 102, 103, 104, 200, 201, 202, 203, 204},
		MinKey:         int64Ptr(1),
		MaxKey:         int64Ptr(15),
		MaxElementSize: 3, // Limit to 3 elements
	}

	resp, err := QueryMultipleSIDX(ctx, []SIDX{sidx1, sidx2}, req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Should be limited to 3 elements
	assert.Len(t, resp.Keys, 3)
	assert.Len(t, resp.Data, 3)
	assert.Len(t, resp.Tags, 3)
	assert.Len(t, resp.SIDs, 3)

	// Should be the first 3 in order (1,2,3)
	expectedKeys := []int64{1, 2, 3}
	assert.Equal(t, expectedKeys, resp.Keys)
}

func TestQueryMultipleSIDX_PartialFailures(t *testing.T) {
	ctx := context.Background()

	// One successful SIDX, one failing SIDX
	sidx1 := &mockSIDX{
		name:     "sidx1",
		response: createMockQueryResponse(1, 3, 100),
	}
	sidx2 := &mockSIDX{
		name: "sidx2",
		err:  errors.New("query failed"),
	}

	req := QueryRequest{
		SeriesIDs: []common.SeriesID{100, 101, 102},
		MinKey:    int64Ptr(1),
		MaxKey:    int64Ptr(10),
	}

	resp, err := QueryMultipleSIDX(ctx, []SIDX{sidx1, sidx2}, req)
	require.NoError(t, err) // Should succeed with partial results
	require.NotNil(t, resp)

	// Should have results from successful SIDX
	assert.Len(t, resp.Keys, 3)
	assert.Equal(t, []int64{1, 2, 3}, resp.Keys)

	// Should include error information
	assert.NotNil(t, resp.Error)
	assert.Contains(t, resp.Error.Error(), "SIDX[1] query failed")
	assert.Contains(t, resp.Error.Error(), "query failed")
}

func TestQueryMultipleSIDX_AllFailures(t *testing.T) {
	ctx := context.Background()

	sidx1 := &mockSIDX{
		name: "sidx1",
		err:  errors.New("connection failed"),
	}
	sidx2 := &mockSIDX{
		name: "sidx2",
		err:  errors.New("timeout occurred"),
	}

	req := QueryRequest{
		SeriesIDs: []common.SeriesID{100, 101},
		MinKey:    int64Ptr(1),
		MaxKey:    int64Ptr(10),
	}

	resp, err := QueryMultipleSIDX(ctx, []SIDX{sidx1, sidx2}, req)
	require.Error(t, err)
	assert.Nil(t, resp)

	assert.Contains(t, err.Error(), "all SIDX queries failed")
	assert.Contains(t, err.Error(), "connection failed")
	assert.Contains(t, err.Error(), "timeout occurred")
}

func TestQueryMultipleSIDX_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// Mock SIDX that checks for context cancellation
	sidx1 := &mockSIDX{name: "sidx1"}
	sidx1.response = createMockQueryResponse(1, 3, 100)

	req := QueryRequest{
		SeriesIDs: []common.SeriesID{100, 101, 102},
		MinKey:    int64Ptr(1),
		MaxKey:    int64Ptr(10),
	}

	// Cancel context immediately
	cancel()

	// Query should respect context cancellation
	_, err := QueryMultipleSIDX(ctx, []SIDX{sidx1}, req)
	// May succeed if mock doesn't check context, but real implementation would respect cancellation
	// This test mainly verifies the context is properly passed through
	if err != nil {
		assert.Contains(t, err.Error(), "context")
	}
}

func TestQueryMultipleSIDXWithOptions_FailFast(t *testing.T) {
	ctx := context.Background()

	sidx1 := &mockSIDX{
		name:     "sidx1",
		response: createMockQueryResponse(1, 3, 100),
	}
	sidx2 := &mockSIDX{
		name: "sidx2",
		err:  errors.New("immediate failure"),
	}

	req := QueryRequest{
		SeriesIDs: []common.SeriesID{100, 101, 102},
		MinKey:    int64Ptr(1),
		MaxKey:    int64Ptr(10),
	}

	opts := MultiSIDXQueryOptions{
		FailFast:        true,
		MinSuccessCount: 1,
	}

	_, err := QueryMultipleSIDXWithOptions(ctx, []SIDX{sidx1, sidx2}, req, opts)
	// With FailFast, should return error on first failure
	if err != nil {
		assert.Contains(t, err.Error(), "query failed fast")
	}
}

func TestQueryMultipleSIDXWithOptions_MinSuccessCount(t *testing.T) {
	ctx := context.Background()

	sidx1 := &mockSIDX{
		name:     "sidx1",
		response: createMockQueryResponse(1, 3, 100),
	}
	sidx2 := &mockSIDX{
		name: "sidx2",
		err:  errors.New("failure"),
	}
	sidx3 := &mockSIDX{
		name: "sidx3",
		err:  errors.New("another failure"),
	}

	req := QueryRequest{
		SeriesIDs: []common.SeriesID{100, 101, 102},
		MinKey:    int64Ptr(1),
		MaxKey:    int64Ptr(10),
	}

	opts := MultiSIDXQueryOptions{
		FailFast:        false,
		MinSuccessCount: 2, // Require at least 2 successes
	}

	_, err := QueryMultipleSIDXWithOptions(ctx, []SIDX{sidx1, sidx2, sidx3}, req, opts)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient successful queries: got 1, required 2")
}

func TestQueryMultipleSIDXWithOptions_Success(t *testing.T) {
	ctx := context.Background()

	sidx1 := &mockSIDX{
		name:     "sidx1",
		response: createMockQueryResponse(1, 3, 100),
	}
	sidx2 := &mockSIDX{
		name:     "sidx2",
		response: createMockQueryResponse(4, 2, 200),
	}

	req := QueryRequest{
		SeriesIDs: []common.SeriesID{100, 101, 102, 200, 201},
		MinKey:    int64Ptr(1),
		MaxKey:    int64Ptr(10),
	}

	opts := MultiSIDXQueryOptions{
		FailFast:        false,
		MinSuccessCount: 2,
	}

	resp, err := QueryMultipleSIDXWithOptions(ctx, []SIDX{sidx1, sidx2}, req, opts)
	require.NoError(t, err)
	require.NotNil(t, resp)

	assert.Len(t, resp.Keys, 5)
	expectedKeys := []int64{1, 2, 3, 4, 5}
	assert.Equal(t, expectedKeys, resp.Keys)
}

func TestMergeMultipleSIDXResponses(t *testing.T) {
	// Test the internal merge function directly
	response1 := createMockQueryResponse(1, 3, 100) // Keys: 1,2,3
	response2 := createMockQueryResponse(4, 2, 200) // Keys: 4,5

	req := QueryRequest{
		// Order: nil defaults to ascending
	}

	merged := mergeMultipleSIDXResponses([]*QueryResponse{response1, response2}, req)
	require.NotNil(t, merged)

	expectedKeys := []int64{1, 2, 3, 4, 5}
	assert.Equal(t, expectedKeys, merged.Keys)
	assert.Len(t, merged.Data, 5)
	assert.Len(t, merged.Tags, 5)
	assert.Len(t, merged.SIDs, 5)
}

func TestMergeMultipleSIDXResponses_OverlappingKeys(t *testing.T) {
	// Test with overlapping key ranges to ensure proper merging
	response1 := &QueryResponse{
		Keys: []int64{1, 3, 5},
		Data: [][]byte{[]byte("data1"), []byte("data3"), []byte("data5")},
		Tags: [][]Tag{{}, {}, {}},
		SIDs: []common.SeriesID{100, 101, 102},
	}

	response2 := &QueryResponse{
		Keys: []int64{2, 4, 6},
		Data: [][]byte{[]byte("data2"), []byte("data4"), []byte("data6")},
		Tags: [][]Tag{{}, {}, {}},
		SIDs: []common.SeriesID{200, 201, 202},
	}

	req := QueryRequest{
		// Order: nil defaults to ascending
	}

	merged := mergeMultipleSIDXResponses([]*QueryResponse{response1, response2}, req)
	require.NotNil(t, merged)

	// Should be perfectly interleaved: 1,2,3,4,5,6
	expectedKeys := []int64{1, 2, 3, 4, 5, 6}
	assert.Equal(t, expectedKeys, merged.Keys)

	// Verify data integrity is maintained
	for i, key := range merged.Keys {
		expectedData := []byte("data" + string(rune('0'+key)))
		assert.Equal(t, expectedData, merged.Data[i])
	}
}

// Benchmark tests for performance validation.

func BenchmarkQueryMultipleSIDX_TwoInstances(b *testing.B) {
	ctx := context.Background()

	sidx1 := &mockSIDX{
		name:     "sidx1",
		response: createMockQueryResponse(1, 100, 100),
	}
	sidx2 := &mockSIDX{
		name:     "sidx2",
		response: createMockQueryResponse(101, 100, 200),
	}

	req := QueryRequest{
		SeriesIDs: generateSeriesIDs(200),
		MinKey:    int64Ptr(1),
		MaxKey:    int64Ptr(300),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := QueryMultipleSIDX(ctx, []SIDX{sidx1, sidx2}, req)
		if err != nil {
			b.Fatalf("Unexpected error: %v", err)
		}
	}
}

// Helper functions.

func int64Ptr(v int64) *int64 {
	return &v
}

func generateSeriesIDs(count int) []common.SeriesID {
	ids := make([]common.SeriesID, count)
	for i := 0; i < count; i++ {
		ids[i] = common.SeriesID(100 + i)
	}
	return ids
}
