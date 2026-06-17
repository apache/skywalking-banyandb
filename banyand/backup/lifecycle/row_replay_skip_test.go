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

package lifecycle

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// TestSkipContext_WrapsErrSkipSeries proves a located skip still satisfies the
// errors.Is(err, errSkipSeries) detection the replay loop uses, while asSkipError
// recovers the location through arbitrary wrapping.
func TestSkipContext_WrapsErrSkipSeries(t *testing.T) {
	sc := &skipError{partPath: "/seg/shard/0001", seriesID: 42, reason: skipReasonIncompletePart}

	assert.True(t, errors.Is(sc, errSkipSeries), "skipError must be detected as a skip")
	assert.Contains(t, sc.Error(), "seriesID=42")
	assert.Contains(t, sc.Error(), string(skipReasonIncompletePart))

	got := asSkipError(fmt.Errorf("emit row: %w", sc))
	require.NotNil(t, got, "asSkipError must recover the location through wrapping")
	assert.Equal(t, uint64(42), got.seriesID)
	assert.Equal(t, skipReasonIncompletePart, got.reason)

	assert.Nil(t, asSkipError(errSkipSeries), "a bare errSkipSeries carries no location")
}

// TestRecordSkip_CountsAllButBoundsDetail proves every skip increments the count
// while the retained detail sample is capped at maxSkipDetail to bound memory.
func TestRecordSkip_CountsAllButBoundsDetail(t *testing.T) {
	s := &batchSender{}
	const total = maxSkipDetail + 37
	for i := 0; i < total; i++ {
		s.recordSkip(&skipError{partPath: "p", seriesID: uint64(i), reason: skipReasonRebuildFailed})
	}
	assert.Equal(t, total, s.skippedRows, "every skip must be counted")
	assert.Len(t, s.skippedDetail, maxSkipDetail, "detail must be capped")

	// A bare errSkipSeries (no location) still counts but adds no detail entry.
	before := len(s.skippedDetail)
	s.recordSkip(errSkipSeries)
	assert.Equal(t, total+1, s.skippedRows)
	assert.Len(t, s.skippedDetail, before, "a location-less skip adds no detail")
}

// TestRecordSkip_DeduplicatesContiguousSeries proves the bounded detail sample is
// effectively per-series: a high-row-count series contributes one entry instead of
// flooding the sample, while distinct (part, series, reason) locations each add one.
func TestRecordSkip_DeduplicatesContiguousSeries(t *testing.T) {
	s := &batchSender{}

	// 1000 skipped rows of the same series collapse to a single detail entry.
	for i := 0; i < 1000; i++ {
		s.recordSkip(&skipError{partPath: "p", seriesID: 7, reason: skipReasonRebuildFailed})
	}
	assert.Equal(t, 1000, s.skippedRows, "every skipped row must be counted")
	require.Len(t, s.skippedDetail, 1, "one series must contribute a single detail entry")

	// A different series (emitted contiguously after the first) adds one entry.
	s.recordSkip(&skipError{partPath: "p", seriesID: 8, reason: skipReasonRebuildFailed})
	require.Len(t, s.skippedDetail, 2)

	// Same series but a different reason is a distinct location and adds one entry.
	s.recordSkip(&skipError{partPath: "p", seriesID: 8, reason: skipReasonIncompletePart})
	require.Len(t, s.skippedDetail, 3)

	// Same series in a different part is a distinct location and adds one entry.
	s.recordSkip(&skipError{partPath: "q", seriesID: 8, reason: skipReasonIncompletePart})
	require.Len(t, s.skippedDetail, 4)

	assert.Equal(t, []skipError{
		{partPath: "p", seriesID: 7, reason: skipReasonRebuildFailed},
		{partPath: "p", seriesID: 8, reason: skipReasonRebuildFailed},
		{partPath: "p", seriesID: 8, reason: skipReasonIncompletePart},
		{partPath: "q", seriesID: 8, reason: skipReasonIncompletePart},
	}, s.skippedDetail)
}

// TestExcludeRetainedSuffixes proves the deletion-gating filter: a source segment
// whose start instant is retained (because row-replay skipped unresolved rows in
// it) is removed from the delete-after-migration suffix set, while the others pass
// through unchanged.
func TestExcludeRetainedSuffixes(t *testing.T) {
	rule := storage.IntervalRule{Unit: storage.DAY, Num: 1}
	suffixes := []string{"20260607", "20260608", "20260609"}

	retainStart, err := storage.ParseSegmentTime("20260608", rule)
	require.NoError(t, err)

	l := logger.GetLogger("exclude-retained-test")
	got := excludeRetainedSuffixes(suffixes, []int64{retainStart.UnixNano()}, rule, l)
	assert.Equal(t, []string{"20260607", "20260609"}, got, "the retained suffix must be excluded from deletion")
}

// TestExcludeRetainedSuffixes_NoneRetained proves the filter is a no-op when no
// segment was retained, returning the original slice untouched.
func TestExcludeRetainedSuffixes_NoneRetained(t *testing.T) {
	rule := storage.IntervalRule{Unit: storage.DAY, Num: 1}
	suffixes := []string{"20260607", "20260608"}
	l := logger.GetLogger("exclude-retained-test")

	got := excludeRetainedSuffixes(suffixes, nil, rule, l)
	assert.Equal(t, suffixes, got)
}

// TestStreamMigrationVisitor_SidxGapRetention proves the stream visitor now has
// the same sidx-gap source retention as measure: a recorded skipped source
// segment surfaces from SkippedSourceSegmentStarts and is excluded from the
// delete-after-migration suffix set.
func TestStreamMigrationVisitor_SidxGapRetention(t *testing.T) {
	mv := &streamMigrationVisitor{skippedSourceTracker: newSkippedSourceTracker()}
	require.Empty(t, mv.SkippedSourceSegmentStarts(), "no skips recorded yet")

	rule := storage.IntervalRule{Unit: storage.DAY, Num: 1}
	retainStart, err := storage.ParseSegmentTime("20260608", rule)
	require.NoError(t, err)
	segmentTR := &timestamp.TimeRange{Start: retainStart, End: retainStart.Add(24 * time.Hour)}

	mv.recordSkippedSource(segmentTR)
	// Idempotent: recording the same source twice keeps a single entry.
	mv.recordSkippedSource(segmentTR)
	starts := mv.SkippedSourceSegmentStarts()
	require.Len(t, starts, 1)
	assert.Equal(t, retainStart.UnixNano(), starts[0])

	l := logger.GetLogger("stream-retention-test")
	suffixes := []string{"20260607", "20260608", "20260609"}
	got := excludeRetainedSuffixes(suffixes, mv.SkippedSourceSegmentStarts(), rule, l)
	assert.Equal(t, []string{"20260607", "20260609"}, got,
		"a stream source segment with a sidx-gap skip must be retained (excluded from deletion)")
}
