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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
)

// TestStreamingParts_Timestamps verifies that StreamingParts propagates
// MinTimestamp and MaxTimestamp from partMetadata (not SegmentID) to
// StreamingPartData. This prevents zero-timestamp segments from being
// created on the receiving node during distributed sync.
func TestStreamingParts_Timestamps(t *testing.T) {
	reqs := []WriteRequest{
		createTestWriteRequest(1, 100, "data1"),
		createTestWriteRequest(1, 200, "data2"),
	}

	t.Run("with_timestamps_set", func(t *testing.T) {
		sidxIface := createTestSIDX(t)
		raw := sidxIface.(*sidx)
		defer func() {
			assert.NoError(t, raw.Close())
		}()

		minTS := int64(1700000000)
		maxTS := int64(1700001000)
		writeTestDataWithTimeRange(t, raw, reqs, 1, 1, &minTS, &maxTS)

		flushIntro, err := raw.Flush(map[uint64]struct{}{1: {}})
		require.NoError(t, err)
		raw.IntroduceFlushed(flushIntro)
		flushIntro.Release()

		partIDs := map[uint64]struct{}{1: {}}
		parts, releaseFuncs := raw.StreamingParts(partIDs, "test-group", 0, "test-sidx")
		defer func() {
			for _, release := range releaseFuncs {
				release()
			}
		}()

		require.Len(t, parts, 1)
		assert.Equal(t, uint64(1), parts[0].ID)
		assert.Equal(t, int64(1700000000), parts[0].MinTimestamp,
			"MinTimestamp should come from partMetadata.MinTimestamp, not SegmentID")
		assert.Equal(t, int64(1700001000), parts[0].MaxTimestamp,
			"MaxTimestamp should come from partMetadata.MaxTimestamp")
		assert.Equal(t, "test-group", parts[0].Group)
		assert.Equal(t, uint32(0), parts[0].ShardID)
		assert.Equal(t, data.TopicTracePartSync.String(), parts[0].Topic)
		assert.Equal(t, "test-sidx", parts[0].PartType)
	})

	t.Run("nil_timestamps_fallback_to_segment_id", func(t *testing.T) {
		sidxIface := createTestSIDX(t)
		raw := sidxIface.(*sidx)
		defer func() {
			assert.NoError(t, raw.Close())
		}()

		// segmentID=1000, nil timestamps — should fall back to SegmentID
		writeTestDataWithTimeRange(t, raw, reqs, 1000, 1, nil, nil)

		flushIntro, err := raw.Flush(map[uint64]struct{}{1: {}})
		require.NoError(t, err)
		raw.IntroduceFlushed(flushIntro)
		flushIntro.Release()

		partIDs := map[uint64]struct{}{1: {}}
		parts, releaseFuncs := raw.StreamingParts(partIDs, "test-group", 0, "test-sidx")
		defer func() {
			for _, release := range releaseFuncs {
				release()
			}
		}()

		require.Len(t, parts, 1)
		assert.Equal(t, int64(1000), parts[0].MinTimestamp,
			"MinTimestamp should fall back to SegmentID when partMetadata.MinTimestamp is nil")
		assert.Equal(t, int64(1000), parts[0].MaxTimestamp,
			"MaxTimestamp should fall back to MinTimestamp when partMetadata.MaxTimestamp is nil")
	})

	t.Run("nil_timestamps_zero_segment_id_panics", func(t *testing.T) {
		sidxIface := createTestSIDX(t)
		raw := sidxIface.(*sidx)
		defer func() {
			assert.NoError(t, raw.Close())
		}()

		// segmentID=0, nil timestamps — should panic
		writeTestDataWithTimeRange(t, raw, reqs, 0, 1, nil, nil)

		flushIntro, err := raw.Flush(map[uint64]struct{}{1: {}})
		require.NoError(t, err)
		raw.IntroduceFlushed(flushIntro)
		flushIntro.Release()

		partIDs := map[uint64]struct{}{1: {}}
		assert.Panics(t, func() {
			parts, releaseFuncs := raw.StreamingParts(partIDs, "test-group", 0, "test-sidx")
			for _, release := range releaseFuncs {
				release()
			}
			_ = parts
		}, "Should panic when both MinTimestamp and SegmentID are zero")
	})

	t.Run("nil_snapshot_returns_nil", func(t *testing.T) {
		sidxIface := createTestSIDX(t)
		raw := sidxIface.(*sidx)
		defer func() {
			assert.NoError(t, raw.Close())
		}()

		partIDs := map[uint64]struct{}{1: {}}
		parts, releaseFuncs := raw.StreamingParts(partIDs, "test-group", 0, "test-sidx")
		defer func() {
			for _, release := range releaseFuncs {
				release()
			}
		}()
		assert.Nil(t, parts)
	})

	t.Run("multiple_parts_sorted_by_id", func(t *testing.T) {
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

		min1, max1 := int64(1700000000), int64(1700001000)
		min2, max2 := int64(1800000000), int64(1800001000)
		writeTestDataWithTimeRange(t, raw, reqs, 1, 2, &min1, &max1)
		writeTestDataWithTimeRange(t, raw, reqs, 2, 3, &min2, &max2)

		flushIntro, flushErr := raw.Flush(map[uint64]struct{}{2: {}, 3: {}})
		require.NoError(t, flushErr)
		raw.IntroduceFlushed(flushIntro)
		flushIntro.Release()

		partIDs := map[uint64]struct{}{2: {}, 3: {}}
		parts, releaseFuncs := raw.StreamingParts(partIDs, "test-group", 0, "test-sidx")
		defer func() {
			for _, release := range releaseFuncs {
				release()
			}
		}()

		require.Len(t, parts, 2)
		// Parts should be sorted by ID
		assert.Equal(t, uint64(2), parts[0].ID)
		assert.Equal(t, int64(1700000000), parts[0].MinTimestamp)
		assert.Equal(t, int64(1700001000), parts[0].MaxTimestamp)
		assert.Equal(t, uint64(3), parts[1].ID)
		assert.Equal(t, int64(1800000000), parts[1].MinTimestamp)
		assert.Equal(t, int64(1800001000), parts[1].MaxTimestamp)
	})
}
