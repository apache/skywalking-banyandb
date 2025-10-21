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

package trace

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/watcher"
)

func Test_tsTable_mustAddTraces(t *testing.T) {
	tests := []struct {
		name   string
		tsList []*traces
		want   int
	}{
		{
			name: "Test with empty traces",
			tsList: []*traces{
				{
					traceIDs:   []string{},
					timestamps: []int64{},
					tags:       [][]*tagValue{},
					spans:      [][]byte{},
					spanIDs:    []string{},
				},
			},
			want: 0,
		},
		{
			name: "Test with one item in traces",
			tsList: []*traces{
				{
					traceIDs:   []string{"trace1"},
					timestamps: []int64{1},
					tags: [][]*tagValue{
						{
							{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value1"), []byte("value2")}},
							{tag: "intArrTag", valueType: pbv1.ValueTypeInt64Arr, value: nil, valueArr: [][]byte{convert.Int64ToBytes(25), convert.Int64ToBytes(30)}},
						},
					},
					spans:   [][]byte{[]byte("span1")},
					spanIDs: []string{"span1"},
				},
			},
			want: 1,
		},
		{
			name: "Test with multiple calls to mustAddTraces",
			tsList: []*traces{
				tsTS1,
				tsTS2,
			},
			want: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tst := &tsTable{
				loopCloser:    run.NewCloser(2),
				introductions: make(chan *introduction),
			}
			flushCh := make(chan *flusherIntroduction)
			mergeCh := make(chan *mergerIntroduction)
			introducerWatcher := make(watcher.Channel, 1)
			go tst.introducerLoop(flushCh, mergeCh, introducerWatcher, 1)
			defer tst.Close()
			for _, ts := range tt.tsList {
				tst.mustAddTraces(ts, nil)
				time.Sleep(100 * time.Millisecond)
			}
			s := tst.currentSnapshot()
			if s == nil {
				s = new(snapshot)
			}
			defer s.decRef()
			assert.Equal(t, tt.want, len(s.parts))
			var lastVersion uint64
			for _, pw := range s.parts {
				require.Greater(t, pw.ID(), uint64(0))
				if lastVersion == 0 {
					lastVersion = pw.ID()
				} else {
					require.Less(t, lastVersion, pw.ID())
				}
			}
		})
	}
}

func Test_tstIter(t *testing.T) {
	type testCtx struct {
		tsList       []*traces
		wantErr      error
		name         string
		tid          string
		want         []blockMetadata
		minTimestamp int64
		maxTimestamp int64
	}
	bma := generateBlockMetadataArray()
	defer releaseBlockMetadataArray(bma)

	verify := func(t *testing.T, tt testCtx, tst *tsTable) uint64 {
		defer tst.Close()
		s := tst.currentSnapshot()
		if s == nil {
			s = new(snapshot)
		}
		defer s.decRef()
		pp, n := s.getParts(nil, tt.minTimestamp, tt.maxTimestamp, []string{tt.tid})
		require.Equal(t, len(s.parts), n)
		ti := &tstIter{}
		ti.init(bma, pp, []string{tt.tid})
		var got []blockMetadata
		for ti.nextBlock() {
			if ti.piPool[ti.idx].curBlock.traceID == "" {
				t.Errorf("Expected curBlock to be initialized, but it was nil")
			}
			var bm blockMetadata
			bm.copyFrom(ti.piPool[ti.idx].curBlock)
			got = append(got, bm)
		}

		if !errors.Is(ti.Error(), tt.wantErr) {
			t.Errorf("Unexpected error: got %v, want %v", ti.err, tt.wantErr)
		}

		if diff := cmp.Diff(got, tt.want,
			cmpopts.IgnoreFields(blockMetadata{}, "timestamps"),
			cmpopts.IgnoreFields(blockMetadata{}, "spans"),
			cmpopts.IgnoreFields(blockMetadata{}, "tags"),
			cmpopts.IgnoreFields(blockMetadata{}, "tagType"),
			cmp.AllowUnexported(blockMetadata{}),
		); diff != "" {
			t.Errorf("Unexpected blockMetadata (-got +want):\n%s", diff)
		}
		return s.epoch
	}

	t.Run("memory snapshot", func(t *testing.T) {
		tests := []testCtx{
			{
				name:   "Test with no traces",
				tsList: []*traces{},
			},
			{
				name:         "Test with single part",
				tsList:       []*traces{tsTS1},
				tid:          "trace1",
				minTimestamp: 1,
				maxTimestamp: 2,
				want: []blockMetadata{
					{traceID: "trace1", count: 1, uncompressedSpanSizeBytes: 5},
				},
			},
			{
				name:         "Test with multiple parts",
				tsList:       []*traces{tsTS1, tsTS2},
				tid:          "trace1",
				minTimestamp: 1,
				maxTimestamp: 2,
				want: []blockMetadata{
					{traceID: "trace1", count: 1, uncompressedSpanSizeBytes: 5},
					{traceID: "trace1", count: 1, uncompressedSpanSizeBytes: 5},
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				tmpPath, defFn := test.Space(require.New(t))
				defer defFn()
				tst := &tsTable{
					loopCloser:    run.NewCloser(2),
					introductions: make(chan *introduction),
					fileSystem:    fs.NewLocalFileSystem(),
					root:          tmpPath,
				}
				tst.gc.init(tst)
				flushCh := make(chan *flusherIntroduction)
				mergeCh := make(chan *mergerIntroduction)
				introducerWatcher := make(watcher.Channel, 1)
				go tst.introducerLoop(flushCh, mergeCh, introducerWatcher, 1)
				for _, ts := range tt.tsList {
					tst.mustAddTraces(ts, nil)
					time.Sleep(100 * time.Millisecond)
				}
				verify(t, tt, tst)
			})
		}
	})
}

var allTagProjections = &model.TagProjection{
	Names: []string{"strArrTag", "strTag", "intTag"},
}

var tsTS1 = &traces{
	traceIDs:   []string{"trace1", "trace2", "trace3"},
	timestamps: []int64{1, 1, 1},
	tags: [][]*tagValue{
		{
			{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value1"), []byte("value2")}},
			{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value1"), valueArr: nil},
			{tag: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(10), valueArr: nil},
		},
		{
			{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value3"), []byte("value4")}},
			{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value2"), valueArr: nil},
			{tag: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(20), valueArr: nil},
		},
		{
			{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value5"), []byte("value6")}},
			{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value3"), valueArr: nil},
			{tag: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(30), valueArr: nil},
		},
	},
	spans:   [][]byte{[]byte("span1"), []byte("span2"), []byte("span3")},
	spanIDs: []string{"span1", "span2", "span3"},
}

var tsTS2 = &traces{
	traceIDs:   []string{"trace1", "trace2", "trace3"},
	timestamps: []int64{2, 2, 2},
	tags: [][]*tagValue{
		{
			{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value5"), []byte("value6")}},
			{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value4"), valueArr: nil},
			{tag: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(40), valueArr: nil},
		},
		{
			{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value7"), []byte("value8")}},
			{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value5"), valueArr: nil},
			{tag: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(50), valueArr: nil},
		},
		{
			{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value9"), []byte("value10")}},
			{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value6"), valueArr: nil},
			{tag: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(60), valueArr: nil},
		},
	},
	spans:   [][]byte{[]byte("span4"), []byte("span5"), []byte("span6")},
	spanIDs: []string{"span4", "span5", "span6"},
}

func generateHugeTraces(num int) *traces {
	traces := &traces{
		traceIDs:   []string{},
		timestamps: []int64{},
		tags:       [][]*tagValue{},
		spans:      [][]byte{},
		spanIDs:    []string{},
	}
	for i := 1; i <= num; i++ {
		traces.traceIDs = append(traces.traceIDs, "trace1")
		traces.timestamps = append(traces.timestamps, int64(i))
		traces.tags = append(traces.tags, []*tagValue{
			{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value5"), []byte("value6")}},
			{tag: "intArrTag", valueType: pbv1.ValueTypeInt64Arr, value: nil, valueArr: [][]byte{convert.Int64ToBytes(35), convert.Int64ToBytes(40)}},
			{tag: "binaryTag", valueType: pbv1.ValueTypeBinaryData, value: longText, valueArr: nil},
			{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value3"), valueArr: nil},
			{tag: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(30), valueArr: nil},
		})
		traces.spans = append(traces.spans, []byte("span1"))
		traces.spanIDs = append(traces.spanIDs, "span1")
	}
	traces.traceIDs = append(traces.traceIDs, []string{"trace2", "trace3"}...)
	traces.timestamps = append(traces.timestamps, []int64{int64(num + 1), int64(num + 2)}...)
	traces.tags = append(traces.tags, [][]*tagValue{
		{
			{tag: "strTag1", valueType: pbv1.ValueTypeStr, value: []byte("tag3"), valueArr: nil},
			{tag: "strTag2", valueType: pbv1.ValueTypeStr, value: []byte("tag4"), valueArr: nil},
		},
		{}, // empty tags
	}...)
	traces.spans = append(traces.spans, [][]byte{[]byte("span2"), []byte("span3")}...)
	traces.spanIDs = append(traces.spanIDs, []string{"span2", "span3"}...)
	return traces
}

// generateRealisticTraces creates a more realistic dataset for benchmarking:
// - Each trace has 3-5 spans
// - Each span is more than 100KB
// - Creates the specified number of unique traces.
func generateRealisticTraces(numTraces int) *traces {
	traces := &traces{
		traceIDs:   []string{},
		timestamps: []int64{},
		tags:       [][]*tagValue{},
		spans:      [][]byte{},
		spanIDs:    []string{},
	}

	// Create a large span payload (>100 KiB)
	// Using a mix of realistic data: stack traces, error messages, metadata
	const spanPayloadSizeKiB = 110 // 110 KiB (1 KiB = 1024 bytes)
	spanPayloadTemplate := make([]byte, spanPayloadSizeKiB*1024)
	for i := range spanPayloadTemplate {
		// Fill with semi-realistic data
		spanPayloadTemplate[i] = byte('A' + (i % 26))
	}

	timestamp := int64(1000000)

	for traceIdx := 0; traceIdx < numTraces; traceIdx++ {
		traceID := fmt.Sprintf("trace_%d", traceIdx)

		// Each trace has 3-5 spans
		numSpans := 3 + (traceIdx % 3) // Will give 3, 4, or 5 spans

		for spanIdx := 0; spanIdx < numSpans; spanIdx++ {
			spanID := fmt.Sprintf("%s_span_%d", traceID, spanIdx)

			// Create a unique span payload by appending span-specific data
			spanPayload := make([]byte, len(spanPayloadTemplate))
			copy(spanPayload, spanPayloadTemplate)
			// Add span-specific suffix to ensure uniqueness
			suffix := []byte(fmt.Sprintf("_trace_%d_span_%d", traceIdx, spanIdx))
			copy(spanPayload[len(spanPayload)-len(suffix):], suffix)

			traces.traceIDs = append(traces.traceIDs, traceID)
			traces.timestamps = append(traces.timestamps, timestamp)
			timestamp++

			// Add realistic tags
			traces.tags = append(traces.tags, []*tagValue{
				{tag: "http.method", valueType: pbv1.ValueTypeStr, value: []byte("POST"), valueArr: nil},
				{tag: "http.status", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(200), valueArr: nil},
				{tag: "http.url", valueType: pbv1.ValueTypeStr, value: []byte(fmt.Sprintf("/api/v1/trace/%d", traceIdx)), valueArr: nil},
				{tag: "service.name", valueType: pbv1.ValueTypeStr, value: []byte("test-service"), valueArr: nil},
				{tag: "span.kind", valueType: pbv1.ValueTypeStr, value: []byte("server"), valueArr: nil},
				{tag: "error", valueType: pbv1.ValueTypeBinaryData, value: spanPayload[:1024], valueArr: nil}, // Use part of payload for error
			})

			traces.spans = append(traces.spans, spanPayload)
			traces.spanIDs = append(traces.spanIDs, spanID)
		}
	}

	return traces
}

// Test_tstIter_reset_does_not_corrupt_parts verifies the fix for the bug where
// tstIter.reset() was corrupting the input parts array through slice aliasing.
// This test ensures that parts array elements remain intact after init/release cycles.
func Test_tstIter_reset_does_not_corrupt_parts(t *testing.T) {
	createMockPart := func(id uint64) *part {
		return &part{
			partMetadata: partMetadata{
				ID:           id,
				MinTimestamp: 0,
				MaxTimestamp: 100,
			},
		}
	}

	bma := generateBlockMetadataArray()
	defer releaseBlockMetadataArray(bma)

	t.Run("single_cycle_preserves_parts", func(t *testing.T) {
		// Create parts array with 3 elements
		parts := []*part{
			createMockPart(1),
			createMockPart(2),
			createMockPart(3),
		}

		// Store original pointers
		originalPtrs := make([]*part, len(parts))
		copy(originalPtrs, parts)

		// Initialize and release tstIter
		ti := generateTstIter()
		ti.init(bma, parts, []string{"trace1"})
		releaseTstIter(ti)

		// Verify parts array is intact
		require.NotNil(t, parts[0], "parts[0] should not be nil after release")
		require.NotNil(t, parts[1], "parts[1] should not be nil after release")
		require.NotNil(t, parts[2], "parts[2] should not be nil after release")

		// Verify elements are the same objects
		require.Equal(t, originalPtrs[0], parts[0], "parts[0] should be the same object")
		require.Equal(t, originalPtrs[1], parts[1], "parts[1] should be the same object")
		require.Equal(t, originalPtrs[2], parts[2], "parts[2] should be the same object")

		t.Log("✓ Single cycle: parts array preserved correctly")
	})

	t.Run("multiple_cycles_no_corruption", func(t *testing.T) {
		// This test simulates scanTraceIDsInline being called multiple times
		// which is the real-world scenario where the bug occurred

		parts := []*part{
			createMockPart(10),
			createMockPart(20),
			createMockPart(30),
		}

		originalPtrs := make([]*part, len(parts))
		copy(originalPtrs, parts)

		traceIDs := []string{"trace1", "trace2", "trace3"}

		// Simulate 5 cycles of getting, using, and releasing tstIter from pool
		for cycle := 0; cycle < 5; cycle++ {
			// Verify parts are intact at start of cycle
			for i, p := range parts {
				require.NotNil(t, p, "Cycle %d: parts[%d] should not be nil at cycle start", cycle, i)
				require.Equal(t, originalPtrs[i], p, "Cycle %d: parts[%d] should be same object", cycle, i)
			}

			// Simulate scanTraceIDsInline: get tstIter, use it, release it
			ti := generateTstIter()
			ti.init(bma, parts, traceIDs)

			// Verify tstIter was initialized correctly
			require.Equal(t, 3, len(ti.parts), "Cycle %d: tstIter should have 3 parts", cycle)

			releaseTstIter(ti)

			// Verify parts are still intact after release
			for i, p := range parts {
				require.NotNil(t, p, "Cycle %d: parts[%d] should not be nil after release", cycle, i)
				require.Equal(t, originalPtrs[i], p, "Cycle %d: parts[%d] should be same object after release", cycle, i)
			}

			t.Logf("Cycle %d: ✓ Parts intact", cycle)
		}

		t.Log("✓ Multiple cycles: no corruption detected")
	})

	t.Run("parts_array_length_preserved", func(t *testing.T) {
		// Verify that the parts array length is preserved across cycles
		parts := []*part{
			createMockPart(100),
			createMockPart(101),
		}

		originalLen := len(parts)
		originalCap := cap(parts)

		ti := generateTstIter()
		ti.init(bma, parts, []string{"trace1"})
		releaseTstIter(ti)

		// Verify length and capacity are preserved
		require.Equal(t, originalLen, len(parts), "parts length should be preserved")
		require.Equal(t, originalCap, cap(parts), "parts capacity should be preserved")

		t.Log("✓ Parts array length and capacity preserved")
	})

	t.Run("reused_tstiter_works_correctly", func(t *testing.T) {
		// Verify that reusing the same tstIter from pool works correctly

		parts1 := []*part{
			createMockPart(1),
			createMockPart(2),
		}

		parts2 := []*part{
			createMockPart(3),
			createMockPart(4),
			createMockPart(5),
		}

		// First use: with parts1
		ti := generateTstIter()
		ti.init(bma, parts1, []string{"a", "b"})
		require.Equal(t, 2, len(ti.parts), "First init: tstIter should have 2 parts")
		releaseTstIter(ti)

		// Verify parts1 is intact
		require.NotNil(t, parts1[0], "parts1[0] should not be nil after first release")
		require.NotNil(t, parts1[1], "parts1[1] should not be nil after first release")

		// Second use: reuse tstIter with parts2 (different size)
		ti = generateTstIter()
		ti.init(bma, parts2, []string{"c", "d", "e"})
		require.Equal(t, 3, len(ti.parts), "Second init: tstIter should have 3 parts")
		releaseTstIter(ti)

		// Verify parts2 is intact
		require.NotNil(t, parts2[0], "parts2[0] should not be nil after second release")
		require.NotNil(t, parts2[1], "parts2[1] should not be nil after second release")
		require.NotNil(t, parts2[2], "parts2[2] should not be nil after second release")

		// Verify parts1 is still intact (wasn't corrupted by reuse)
		require.NotNil(t, parts1[0], "parts1[0] should still be non-nil")
		require.NotNil(t, parts1[1], "parts1[1] should still be non-nil")

		t.Log("✓ Reused tstIter works correctly with different parts arrays")
	})
}

// Test_tstIter_reset_clears_but_preserves_input verifies that reset() properly
// clears the tstIter's internal state without modifying the input.
func Test_tstIter_reset_clears_but_preserves_input(t *testing.T) {
	createMockPart := func(id uint64) *part {
		return &part{
			partMetadata: partMetadata{
				ID:           id,
				MinTimestamp: 0,
				MaxTimestamp: 100,
			},
		}
	}

	bma := generateBlockMetadataArray()
	defer releaseBlockMetadataArray(bma)

	parts := []*part{
		createMockPart(1),
		createMockPart(2),
		createMockPart(3),
	}

	originalPtrs := make([]*part, len(parts))
	copy(originalPtrs, parts)

	// Initialize tstIter
	ti := generateTstIter()
	ti.init(bma, parts, []string{"trace1"})

	require.Equal(t, 3, len(ti.parts), "After init: tstIter should have 3 parts")
	require.NotNil(t, ti.parts[0], "After init: ti.parts[0] should not be nil")

	// Call reset directly (normally called by releaseTstIter)
	ti.reset()

	// Verify tstIter internal state is cleared
	require.Equal(t, 0, len(ti.parts), "After reset: tstIter.parts length should be 0")
	require.Nil(t, ti.err, "After reset: tstIter.err should be nil")
	require.Equal(t, 0, ti.idx, "After reset: tstIter.idx should be 0")

	// Verify original parts array is NOT modified
	require.NotNil(t, parts[0], "Original parts[0] should not be nil after reset")
	require.NotNil(t, parts[1], "Original parts[1] should not be nil after reset")
	require.NotNil(t, parts[2], "Original parts[2] should not be nil after reset")

	// Verify elements are still the same objects
	require.Equal(t, originalPtrs[0], parts[0], "Original parts[0] should be same object")
	require.Equal(t, originalPtrs[1], parts[1], "Original parts[1] should be same object")
	require.Equal(t, originalPtrs[2], parts[2], "Original parts[2] should be same object")

	t.Log("✓ Reset clears tstIter state without modifying input")
}
