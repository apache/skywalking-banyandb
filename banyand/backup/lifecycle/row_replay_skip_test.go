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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/logger"
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
