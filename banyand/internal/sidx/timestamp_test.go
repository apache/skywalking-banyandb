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
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// writeTestDataWithTimeRange writes test data with optional min/max timestamps (back-compat: nil means no timestamps).
func writeTestDataWithTimeRange(t *testing.T, sidx SIDX, reqs []WriteRequest, segmentID int64, partID uint64, minTS, maxTS *int64) {
	t.Helper()
	memPart, err := sidx.ConvertToMemPart(reqs, segmentID, minTS, maxTS)
	require.NoError(t, err)
	require.NotNil(t, memPart)
	sidx.IntroduceMemPart(partID, memPart)
}

func ptrInt64Ts(v int64) *int64 {
	val := v
	return &val
}

// TestConvertToMemPart_Timestamps verifies ConvertToMemPart with and without timestamps.
func TestConvertToMemPart_Timestamps(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	reqs := []WriteRequest{
		createTestWriteRequest(1, 1000, "data1"),
		createTestWriteRequest(1, 2000, "data2"),
	}

	t.Run("nil_timestamps_back_compat", func(t *testing.T) {
		mp, err := sidx.ConvertToMemPart(reqs, 1, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, mp)
		require.Nil(t, mp.partMetadata.MinTimestamp)
		require.Nil(t, mp.partMetadata.MaxTimestamp)
		ReleaseMemPart(mp)
	})

	t.Run("with_timestamps", func(t *testing.T) {
		minTS := int64(500)
		maxTS := int64(2500)
		mp, err := sidx.ConvertToMemPart(reqs, 1, &minTS, &maxTS)
		require.NoError(t, err)
		require.NotNil(t, mp)
		require.NotNil(t, mp.partMetadata.MinTimestamp)
		require.NotNil(t, mp.partMetadata.MaxTimestamp)
		assert.Equal(t, int64(500), *mp.partMetadata.MinTimestamp)
		assert.Equal(t, int64(2500), *mp.partMetadata.MaxTimestamp)
		ReleaseMemPart(mp)
	})
}

// TestManifest_BackCompat verifies manifest.json omits timestamps when nil (back-compat).
func TestManifest_BackCompat(t *testing.T) {
	dir := t.TempDir()
	fileSystem := fs.NewLocalFileSystem()
	opts := NewDefaultOptions()
	opts.Memory = protector.NewMemory(observability.NewBypassRegistry())
	opts.Path = dir

	sidxIface, err := NewSIDX(fileSystem, opts)
	require.NoError(t, err)
	raw := sidxIface.(*sidx)
	defer func() {
		assert.NoError(t, raw.Close())
	}()

	reqs := []WriteRequest{
		createTestWriteRequest(1, 100, "data"),
	}

	t.Run("manifest_without_timestamps", func(t *testing.T) {
		writeTestDataWithTimeRange(t, raw, reqs, 1, 1, nil, nil)
		flushIntro, err := raw.Flush(map[uint64]struct{}{1: {}})
		require.NoError(t, err)
		raw.IntroduceFlushed(flushIntro)
		flushIntro.Release()

		manifestPath := filepath.Join(dir, "0000000000000001", manifestFilename)
		data, readErr := fileSystem.Read(manifestPath)
		require.NoError(t, readErr)

		var m struct {
			MinTimestamp *int64 `json:"minTimestamp,omitempty"`
			MaxTimestamp *int64 `json:"maxTimestamp,omitempty"`
			MinKey       int64  `json:"minKey"`
			MaxKey       int64  `json:"maxKey"`
		}
		require.NoError(t, json.Unmarshal(data, &m))
		require.Nil(t, m.MinTimestamp)
		require.Nil(t, m.MaxTimestamp)
		assert.Equal(t, int64(100), m.MinKey)
		assert.Equal(t, int64(100), m.MaxKey)
	})

	t.Run("manifest_with_timestamps", func(t *testing.T) {
		minTS := int64(1000)
		maxTS := int64(2000)
		writeTestDataWithTimeRange(t, raw, reqs, 2, 2, &minTS, &maxTS)
		flushIntro, err := raw.Flush(map[uint64]struct{}{2: {}})
		require.NoError(t, err)
		raw.IntroduceFlushed(flushIntro)
		flushIntro.Release()

		manifestPath := filepath.Join(dir, "0000000000000002", manifestFilename)
		data, readErr := fileSystem.Read(manifestPath)
		require.NoError(t, readErr)

		var m struct {
			MinTimestamp *int64 `json:"minTimestamp,omitempty"`
			MaxTimestamp *int64 `json:"maxTimestamp,omitempty"`
		}
		require.NoError(t, json.Unmarshal(data, &m))
		require.NotNil(t, m.MinTimestamp)
		require.NotNil(t, m.MaxTimestamp)
		assert.Equal(t, int64(1000), *m.MinTimestamp)
		assert.Equal(t, int64(2000), *m.MaxTimestamp)
	})
}

// TestQuery_TimestampSelection verifies timestamp-aware part selection and key-range fallback.
func TestQuery_TimestampSelection(t *testing.T) {
	reqs := []WriteRequest{
		createTestWriteRequest(1, 1000, "data1000"),
		createTestWriteRequest(1, 2000, "data2000"),
	}

	t.Run("back_compat_key_range_only", func(t *testing.T) {
		sidx := createTestSIDX(t)
		defer func() {
			assert.NoError(t, sidx.Close())
		}()
		writeTestDataWithTimeRange(t, sidx, reqs, 1, 1, nil, nil)
		waitForIntroducerLoop()

		minKey := int64(500)
		maxKey := int64(2500)
		qr := QueryRequest{
			SeriesIDs: []common.SeriesID{1},
			MinKey:    &minKey,
			MaxKey:    &maxKey,
		}
		resultsCh, errCh := sidx.StreamingQuery(context.Background(), qr)
		var keys []int64
		for res := range resultsCh {
			require.NoError(t, res.Error)
			keys = append(keys, res.Keys...)
		}
		select {
		case err, ok := <-errCh:
			if ok {
				require.NoError(t, err)
			}
		default:
		}
		assert.Equal(t, 2, len(keys), "key-range filter should work without timestamps")
	})

	t.Run("timestamp_filter_overlaps", func(t *testing.T) {
		sidx := createTestSIDX(t)
		defer func() {
			assert.NoError(t, sidx.Close())
		}()
		minTS := ptrInt64Ts(1000)
		maxTS := ptrInt64Ts(3000)
		writeTestDataWithTimeRange(t, sidx, reqs, 1, 1, minTS, maxTS)
		waitForIntroducerLoop()

		qMinTS := ptrInt64Ts(1500)
		qMaxTS := ptrInt64Ts(2500)
		qr := QueryRequest{
			SeriesIDs:    []common.SeriesID{1},
			MinTimestamp: qMinTS,
			MaxTimestamp: qMaxTS,
		}
		resultsCh, errCh := sidx.StreamingQuery(context.Background(), qr)
		var count int
		for res := range resultsCh {
			require.NoError(t, res.Error)
			count += res.Len()
		}
		select {
		case err, ok := <-errCh:
			if ok {
				require.NoError(t, err)
			}
		default:
		}
		assert.Greater(t, count, 0, "query with overlapping timestamp range")
	})

	t.Run("timestamp_filter_no_overlap_excludes_part", func(t *testing.T) {
		sidx := createTestSIDX(t)
		defer func() {
			assert.NoError(t, sidx.Close())
		}()
		minTS := ptrInt64Ts(1000)
		maxTS := ptrInt64Ts(2000)
		writeTestDataWithTimeRange(t, sidx, reqs, 1, 1, minTS, maxTS)
		waitForIntroducerLoop()

		qMinTS := ptrInt64Ts(5000)
		qMaxTS := ptrInt64Ts(6000)
		qr := QueryRequest{
			SeriesIDs:    []common.SeriesID{1},
			MinTimestamp: qMinTS,
			MaxTimestamp: qMaxTS,
		}
		resultsCh, errCh := sidx.StreamingQuery(context.Background(), qr)
		var count int
		for res := range resultsCh {
			require.NoError(t, res.Error)
			count += res.Len()
		}
		select {
		case err, ok := <-errCh:
			if ok {
				require.NoError(t, err)
			}
		default:
		}
		assert.Equal(t, 0, count, "parts outside timestamp range should be excluded")
	})
}

// TestScanQuery_TimestampSelection verifies ScanQuery timestamp-aware part selection.
func TestScanQuery_TimestampSelection(t *testing.T) {
	reqs := []WriteRequest{
		createTestWriteRequest(1, 100, "data"),
	}

	t.Run("back_compat_no_timestamp_filter", func(t *testing.T) {
		sidx := createTestSIDX(t)
		defer func() {
			assert.NoError(t, sidx.Close())
		}()
		writeTestDataWithTimeRange(t, sidx, reqs, 1, 1, nil, nil)
		waitForIntroducerLoop()

		sqr := ScanQueryRequest{}
		res, err := sidx.ScanQuery(context.Background(), sqr)
		require.NoError(t, err)
		total := 0
		for _, r := range res {
			total += r.Len()
		}
		assert.Equal(t, 1, total, "expected one row from back-compat scan without timestamp filter")
	})

	t.Run("timestamp_filter_excludes_non_overlapping", func(t *testing.T) {
		sidx := createTestSIDX(t)
		defer func() {
			assert.NoError(t, sidx.Close())
		}()
		minTS := ptrInt64Ts(1000)
		maxTS := ptrInt64Ts(2000)
		writeTestDataWithTimeRange(t, sidx, reqs, 1, 1, minTS, maxTS)
		waitForIntroducerLoop()

		sqr := ScanQueryRequest{
			MinTimestamp: ptrInt64Ts(5000),
			MaxTimestamp: ptrInt64Ts(6000),
		}
		res, err := sidx.ScanQuery(context.Background(), sqr)
		require.NoError(t, err)
		total := 0
		for _, r := range res {
			total += r.Len()
		}
		assert.Equal(t, 0, total, "parts outside timestamp range should be excluded in scan")
	})
}

// TestMerge_TimestampPropagation verifies merge aggregates timestamps and back-compat when absent.
func TestMerge_TimestampPropagation(t *testing.T) {
	reqs := []WriteRequest{
		createTestWriteRequest(1, 100, "data"),
	}

	t.Run("merge_parts_without_timestamps_back_compat", func(t *testing.T) {
		dir := t.TempDir()
		fileSystem := fs.NewLocalFileSystem()
		opts := NewDefaultOptions()
		opts.Memory = protector.NewMemory(observability.NewBypassRegistry())
		opts.Path = dir
		opts.AvailablePartIDs = []uint64{1, 2, 10}

		sidxIface, err := NewSIDX(fileSystem, opts)
		require.NoError(t, err)
		raw := sidxIface.(*sidx)
		defer func() {
			assert.NoError(t, raw.Close())
		}()

		writeTestDataWithTimeRange(t, raw, reqs, 1, 1, nil, nil)
		writeTestDataWithTimeRange(t, raw, reqs, 2, 2, nil, nil)
		waitForIntroducerLoop()

		flushIntro, err := raw.Flush(map[uint64]struct{}{1: {}, 2: {}})
		require.NoError(t, err)
		raw.IntroduceFlushed(flushIntro)
		flushIntro.Release()

		mergeIntro, mergeErr := raw.Merge(nil, map[uint64]struct{}{1: {}, 2: {}}, 10, nil)
		require.NoError(t, mergeErr)
		require.NotNil(t, mergeIntro)
		raw.IntroduceMerged(mergeIntro)()

		manifestPath := filepath.Join(dir, "000000000000000a", manifestFilename)
		data, readErr := fileSystem.Read(manifestPath)
		require.NoError(t, readErr)

		var m struct {
			MinTimestamp *int64 `json:"minTimestamp,omitempty"`
			MaxTimestamp *int64 `json:"maxTimestamp,omitempty"`
		}
		require.NoError(t, json.Unmarshal(data, &m))
		require.Nil(t, m.MinTimestamp)
		require.Nil(t, m.MaxTimestamp)
	})

	t.Run("merge_parts_with_timestamps_aggregates", func(t *testing.T) {
		dir := t.TempDir()
		fileSystem := fs.NewLocalFileSystem()
		opts := NewDefaultOptions()
		opts.Memory = protector.NewMemory(observability.NewBypassRegistry())
		opts.Path = dir
		opts.AvailablePartIDs = []uint64{1, 2, 10}

		sidxIface, err := NewSIDX(fileSystem, opts)
		require.NoError(t, err)
		raw := sidxIface.(*sidx)
		defer func() {
			assert.NoError(t, raw.Close())
		}()

		min1, max1 := int64(100), int64(200)
		min2, max2 := int64(150), int64(300)
		writeTestDataWithTimeRange(t, raw, reqs, 1, 1, &min1, &max1)
		writeTestDataWithTimeRange(t, raw, reqs, 2, 2, &min2, &max2)
		waitForIntroducerLoop()

		flushIntro, err := raw.Flush(map[uint64]struct{}{1: {}, 2: {}})
		require.NoError(t, err)
		raw.IntroduceFlushed(flushIntro)
		flushIntro.Release()

		mergeIntro, mergeErr := raw.Merge(nil, map[uint64]struct{}{1: {}, 2: {}}, 10, nil)
		require.NoError(t, mergeErr)
		require.NotNil(t, mergeIntro)
		raw.IntroduceMerged(mergeIntro)()

		manifestPath := filepath.Join(dir, "000000000000000a", manifestFilename)
		data, readErr := fileSystem.Read(manifestPath)
		require.NoError(t, readErr)

		var m struct {
			MinTimestamp *int64 `json:"minTimestamp,omitempty"`
			MaxTimestamp *int64 `json:"maxTimestamp,omitempty"`
		}
		require.NoError(t, json.Unmarshal(data, &m))
		require.NotNil(t, m.MinTimestamp)
		require.NotNil(t, m.MaxTimestamp)
		assert.Equal(t, int64(100), *m.MinTimestamp)
		assert.Equal(t, int64(300), *m.MaxTimestamp)
	})
}

func TestRequestForPartUsesTimestampFilterOnlyWhenNeeded(t *testing.T) {
	minTimestamp := int64(100)
	maxTimestamp := int64(200)
	timeFilter := &mockTagFilterMatcher{}
	req := QueryRequest{
		MinTimestamp:  &minTimestamp,
		MaxTimestamp:  &maxTimestamp,
		TagFilter:     nil,
		TimeTagFilter: timeFilter,
		TimeTagName:   "timestamp",
	}
	baseTags := map[string]struct{}{"service": {}}

	coveredMin, coveredMax := int64(120), int64(180)
	coveredPart := &part{partMetadata: &partMetadata{MinTimestamp: &coveredMin, MaxTimestamp: &coveredMax}}
	coveredRequest, coveredTags := requestForPart(req, baseTags, coveredPart)
	assert.Nil(t, coveredRequest.TagFilter)
	assert.Equal(t, baseTags, coveredTags)

	partialMin, partialMax := int64(80), int64(180)
	partialPart := &part{partMetadata: &partMetadata{MinTimestamp: &partialMin, MaxTimestamp: &partialMax}}
	partialRequest, partialTags := requestForPart(req, baseTags, partialPart)
	assert.Same(t, timeFilter, partialRequest.TagFilter)
	assert.Contains(t, partialTags, "timestamp")
	assert.NotContains(t, baseTags, "timestamp")

	unknownPart := &part{partMetadata: &partMetadata{}}
	unknownRequest, _ := requestForPart(req, baseTags, unknownPart)
	assert.Same(t, timeFilter, unknownRequest.TagFilter)
}

func TestTimestampFilterPartVariantsPreserveBaseFilterAndNilProjection(t *testing.T) {
	sidx := createTestSIDX(t)
	defer func() {
		assert.NoError(t, sidx.Close())
	}()

	partMin, partMax := int64(0), int64(20)
	requests := []WriteRequest{createTestWriteRequest(1, 1, "trace", Tag{Name: "other", Value: []byte("present"), ValueType: pbv1.ValueTypeStr})}
	writeTestDataWithTimeRange(t, sidx, requests, 1, 1, &partMin, &partMax)
	waitForIntroducerLoop()

	queryMin, queryMax := int64(0), int64(20)
	baseFilter := &mockTagFilterMatcher{decoder: testTagValueDecoder, matchFunc: func(_ []*modelv1.Tag) (bool, error) { return false, nil }}
	coveredResponses, queryErr := sidx.QuerySync(context.Background(), QueryRequest{
		SeriesIDs:        []common.SeriesID{1},
		TagFilter:        baseFilter,
		TimeTagFilter:    &mockTagFilterMatcher{},
		TimeTagName:      "timestamp",
		MinTimestamp:     &queryMin,
		MaxTimestamp:     &queryMax,
		TimeIncludeStart: true,
		TimeIncludeEnd:   true,
	})
	require.NoError(t, queryErr)
	require.Empty(t, coveredResponses, "coverage bypass must retain the original predicate")

	// Widening the conservative envelope makes the same row partial. A nil projection
	// retains its historic all-tags behavior so the time matcher can still inspect
	// non-projected delegate inputs.
	partialMin := int64(-1)
	memPart, convertErr := sidx.ConvertToMemPart(requests, 2, &partialMin, &partMax)
	require.NoError(t, convertErr)
	sidx.IntroduceMemPart(2, memPart)
	waitForIntroducerLoop()
	seenOtherTag := false
	partialResponses, queryErr := sidx.QuerySync(context.Background(), QueryRequest{
		SeriesIDs: []common.SeriesID{1},
		TimeTagFilter: &mockTagFilterMatcher{decoder: testTagValueDecoder, matchFunc: func(tags []*modelv1.Tag) (bool, error) {
			for _, tag := range tags {
				if tag.GetKey() == "other" {
					seenOtherTag = true
				}
			}
			return true, nil
		}},
		TimeTagName:      "timestamp",
		MinTimestamp:     &queryMin,
		MaxTimestamp:     &queryMax,
		TimeIncludeStart: true,
		TimeIncludeEnd:   true,
	})
	require.NoError(t, queryErr)
	require.NotEmpty(t, partialResponses)
	require.True(t, seenOtherTag)
}

func TestMergeUnknownTimestampEnvelopeCannotSkipOrBypassFiltering(t *testing.T) {
	instance := createTestSIDX(t)
	t.Cleanup(func() {
		assert.NoError(t, instance.Close())
	})
	knownMin, knownMax := int64(100), int64(200)
	writeTestDataWithTimeRange(t, instance, []WriteRequest{createTestWriteRequest(1, 1, "known")}, 1, 1, &knownMin, &knownMax)
	writeTestDataWithTimeRange(t, instance, []WriteRequest{createTestWriteRequest(1, 2, "unknown")}, 1, 2, nil, nil)
	partIDs := map[uint64]struct{}{1: {}, 2: {}}
	flushIntroduction, flushErr := instance.Flush(partIDs)
	require.NoError(t, flushErr)
	require.NotNil(t, flushIntroduction)
	instance.IntroduceFlushed(flushIntroduction)
	flushIntroduction.Release()
	mergeIntroduction, mergeErr := instance.Merge(nil, partIDs, 10, nil)
	require.NoError(t, mergeErr)
	require.NotNil(t, mergeIntroduction)
	instance.IntroduceMerged(mergeIntroduction)()

	queryMin, queryMax := int64(300), int64(400)
	request := QueryRequest{
		SeriesIDs:    []common.SeriesID{1},
		MinTimestamp: &queryMin,
		MaxTimestamp: &queryMax,
	}
	responses, queryErr := instance.QuerySync(context.Background(), request)
	require.NoError(t, queryErr)
	var values []string
	for _, response := range responses {
		for _, value := range response.Data {
			values = append(values, string(value))
		}
	}
	require.ElementsMatch(t, []string{"known", "unknown"}, values, "unknown merged bounds cannot prove disjointness")

	request.MinTimestamp, request.MaxTimestamp = &knownMin, &knownMax
	request.TimeIncludeStart, request.TimeIncludeEnd = true, true
	filterCalls := 0
	request.TimeTagFilter = &mockTagFilterMatcher{
		decoder: testTagValueDecoder,
		matchFunc: func(_ []*modelv1.Tag) (bool, error) {
			filterCalls++
			return false, nil
		},
	}
	responses, queryErr = instance.QuerySync(context.Background(), request)
	require.NoError(t, queryErr)
	require.Empty(t, responses)
	require.Equal(t, 2, filterCalls, "known input bounds cannot prove coverage of the unknown input")
}
