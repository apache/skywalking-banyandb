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
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

func Test_mergeTwoBlocks(t *testing.T) {
	tests := []struct {
		left  *blockPointer
		right *blockPointer
		want  *blockPointer
		name  string
	}{
		{
			name:  "Merge two empty blocks",
			left:  &blockPointer{},
			right: &blockPointer{},
			want:  &blockPointer{},
		},
		{
			name:  "Merge left is non-empty right is empty",
			left:  &blockPointer{block: conventionalBlock},
			right: &blockPointer{},
			want:  &blockPointer{block: conventionalBlock, bm: blockMetadata{}},
		},
		{
			name:  "Merge left is empty right is non-empty",
			left:  &blockPointer{},
			right: &blockPointer{block: conventionalBlock},
			want:  &blockPointer{block: conventionalBlock, bm: blockMetadata{}},
		},
		{
			name: "Merge two non-empty blocks without overlap",
			left: &blockPointer{
				block: block{
					spans:   [][]byte{[]byte("span1"), []byte("span2")},
					spanIDs: []string{"span1", "span2"},
					tags: []tag{
						{
							name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
							values: [][]byte{marshalStrArr([][]byte{[]byte("value1"), []byte("value2")}), marshalStrArr([][]byte{[]byte("value3"), []byte("value4")})},
						},
					},
				},
			},
			right: &blockPointer{
				block: block{
					spans:   [][]byte{[]byte("span3"), []byte("span4")},
					spanIDs: []string{"span3", "span4"},
					tags: []tag{
						{
							name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
							values: [][]byte{marshalStrArr([][]byte{[]byte("value5"), []byte("value6")}), marshalStrArr([][]byte{[]byte("value7"), []byte("value8")})},
						},
					},
				},
			},
			want: &blockPointer{block: mergedBlock, bm: blockMetadata{}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target := &blockPointer{}
			mergeTwoBlocks(target, tt.left, tt.right)
			if !reflect.DeepEqual(target, tt.want) {
				t.Errorf("mergeTwoBlocks() = %v, want %v", target, tt.want)
			}
		})
	}
}

func Test_mergeBlocks_fastPath(t *testing.T) {
	tests := []struct {
		name        string
		description string
		parts       []*traces
		wantBlocks  []blockMetadata
	}{
		{
			name: "Fast path with completely different trace IDs",
			parts: []*traces{
				{
					traceIDs:   []string{"traceA", "traceB"},
					timestamps: []int64{1, 2},
					tags: [][]*tagValue{
						{
							{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val1"), valueArr: nil},
						},
						{
							{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val2"), valueArr: nil},
						},
					},
					spans:   [][]byte{[]byte("spanA"), []byte("spanB")},
					spanIDs: []string{"spanA", "spanB"},
				},
				{
					traceIDs:   []string{"traceC", "traceD"},
					timestamps: []int64{3, 4},
					tags: [][]*tagValue{
						{
							{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val3"), valueArr: nil},
						},
						{
							{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val4"), valueArr: nil},
						},
					},
					spans:   [][]byte{[]byte("spanC"), []byte("spanD")},
					spanIDs: []string{"spanC", "spanD"},
				},
			},
			wantBlocks: []blockMetadata{
				{traceID: "traceA", count: 1},
				{traceID: "traceB", count: 1},
				{traceID: "traceC", count: 1},
				{traceID: "traceD", count: 1},
			},
			description: "Each block should use readRaw/WriteRawBlock as there's only one block per trace ID",
		},
		{
			name: "Fast path with single trace ID per part",
			parts: []*traces{
				{
					traceIDs:   []string{"trace1"},
					timestamps: []int64{1},
					tags: [][]*tagValue{
						{
							{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val1"), valueArr: nil},
						},
					},
					spans:   [][]byte{[]byte("span1")},
					spanIDs: []string{"span1"},
				},
				{
					traceIDs:   []string{"trace2"},
					timestamps: []int64{2},
					tags: [][]*tagValue{
						{
							{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val2"), valueArr: nil},
						},
					},
					spans:   [][]byte{[]byte("span2")},
					spanIDs: []string{"span2"},
				},
				{
					traceIDs:   []string{"trace3"},
					timestamps: []int64{3},
					tags: [][]*tagValue{
						{
							{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val3"), valueArr: nil},
						},
					},
					spans:   [][]byte{[]byte("span3")},
					spanIDs: []string{"span3"},
				},
			},
			wantBlocks: []blockMetadata{
				{traceID: "trace1", count: 1},
				{traceID: "trace2", count: 1},
				{traceID: "trace3", count: 1},
			},
			description: "All blocks should use fast path as each trace ID appears only once",
		},
		{
			name: "Mixed fast and slow path - some trace IDs overlap",
			parts: []*traces{
				{
					traceIDs:   []string{"trace1", "trace2"},
					timestamps: []int64{1, 2},
					tags: [][]*tagValue{
						{
							{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val1"), valueArr: nil},
						},
						{
							{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val2"), valueArr: nil},
						},
					},
					spans:   [][]byte{[]byte("span1"), []byte("span2")},
					spanIDs: []string{"span1", "span2"},
				},
				{
					traceIDs:   []string{"trace2", "trace3"},
					timestamps: []int64{3, 4},
					tags: [][]*tagValue{
						{
							{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val2b"), valueArr: nil},
						},
						{
							{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val3"), valueArr: nil},
						},
					},
					spans:   [][]byte{[]byte("span2b"), []byte("span3")},
					spanIDs: []string{"span2b", "span3"},
				},
			},
			wantBlocks: []blockMetadata{
				{traceID: "trace1", count: 1},
				{traceID: "trace2", count: 2},
				{traceID: "trace3", count: 1},
			},
			description: "trace1 and trace3 use fast path, trace2 uses slow path (needs merging)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpPath, defFn := test.Space(require.New(t))
			defer defFn()

			fileSystem := fs.NewLocalFileSystem()

			// Create parts
			var pmi []*partMergeIter
			for i, traces := range tt.parts {
				mp := generateMemPart()
				mp.mustInitFromTraces(traces)
				mp.mustFlush(fileSystem, partPath(tmpPath, uint64(i)))
				p := mustOpenFilePart(uint64(i), tmpPath, fileSystem)
				iter := generatePartMergeIter()
				iter.mustInitFromPart(p)
				pmi = append(pmi, iter)
				releaseMemPart(mp)
			}

			// Test mergeBlocks
			br := generateBlockReader()
			br.init(pmi)
			bw := generateBlockWriter()
			dstPath := partPath(tmpPath, 9999)
			bw.mustInitForFilePart(fileSystem, dstPath, false)

			closeCh := make(chan struct{})
			defer close(closeCh)

			pm, tf, tagTypes, err := mergeBlocks(closeCh, bw, br)
			require.NoError(t, err)
			require.NotNil(t, pm)
			require.NotNil(t, tf)
			require.NotNil(t, tagTypes)

			releaseBlockWriter(bw)
			releaseBlockReader(br)
			for _, iter := range pmi {
				releasePartMergeIter(iter)
			}

			// Write metadata to disk so we can read it back
			pm.mustWriteMetadata(fileSystem, dstPath)
			tf.mustWriteTraceIDFilter(fileSystem, dstPath)
			tagTypes.mustWriteTagType(fileSystem, dstPath)
			fileSystem.SyncPath(dstPath)

			// Verify merged results by reading back
			mergedPart := mustOpenFilePart(9999, tmpPath, fileSystem)
			mergedIter := generatePartMergeIter()
			mergedIter.mustInitFromPart(mergedPart)

			reader := generateBlockReader()
			reader.init([]*partMergeIter{mergedIter})

			var gotBlocks []blockMetadata
			for reader.nextBlockMetadata() {
				gotBlocks = append(gotBlocks, reader.block.bm)
			}
			require.NoError(t, reader.error())

			releaseBlockReader(reader)
			releasePartMergeIter(mergedIter)

			// Verify block counts and trace IDs
			require.Len(t, gotBlocks, len(tt.wantBlocks), tt.description)
			for i, want := range tt.wantBlocks {
				require.Equal(t, want.traceID, gotBlocks[i].traceID, "Block %d trace ID mismatch", i)
				require.Equal(t, want.count, gotBlocks[i].count, "Block %d count mismatch for trace %s", i, want.traceID)
			}
		})
	}
}

var mergedBlock = block{
	spans:   [][]byte{[]byte("span1"), []byte("span2"), []byte("span3"), []byte("span4")},
	spanIDs: []string{"span1", "span2", "span3", "span4"},
	tags: []tag{
		{
			name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
			values: [][]byte{
				marshalStrArr([][]byte{[]byte("value1"), []byte("value2")}), marshalStrArr([][]byte{[]byte("value3"), []byte("value4")}),
				marshalStrArr([][]byte{[]byte("value5"), []byte("value6")}), marshalStrArr([][]byte{[]byte("value7"), []byte("value8")}),
			},
		},
	},
}

// Helper function to verify that a merged part contains expected traces.
func verifyPartContainsTraces(t *testing.T, partID uint64, tmpPath string, fileSystem fs.FileSystem, expectedTraces map[string]int) {
	t.Helper()

	// Open the part
	p := mustOpenFilePart(partID, tmpPath, fileSystem)
	defer func() {
		for _, r := range p.tags {
			fs.MustClose(r)
		}
		for _, r := range p.tagMetadata {
			fs.MustClose(r)
		}
		fs.MustClose(p.primary)
		fs.MustClose(p.spans)
	}()

	// Create iterator to read blocks
	pmi := generatePartMergeIter()
	pmi.mustInitFromPart(p)
	defer releasePartMergeIter(pmi)

	reader := generateBlockReader()
	reader.init([]*partMergeIter{pmi})
	defer releaseBlockReader(reader)

	// Read all blocks and verify detailed content
	foundTraces := make(map[string]int)
	decoder := &encoding.BytesBlockDecoder{}
	defer decoder.Reset()

	for reader.nextBlockMetadata() {
		bm := reader.block.bm
		traceID := bm.traceID
		spanCount := int(bm.count)

		foundTraces[traceID] += spanCount

		// Verify the block was read successfully
		require.NotNil(t, reader.block, "Block should not be nil for trace %s", traceID)
		require.NotEmpty(t, traceID, "Trace ID should not be empty")
		require.Greater(t, spanCount, 0, "Trace %s: span count should be greater than 0", traceID)

		// Load the actual block data to verify spans and tags
		reader.loadBlockData(decoder)

		// Verify span count matches
		require.Equal(t, spanCount, len(reader.block.spanIDs),
			"Trace %s: span count mismatch between metadata (%d) and actual span IDs (%d)",
			traceID, spanCount, len(reader.block.spanIDs))
		require.Equal(t, spanCount, len(reader.block.spans),
			"Trace %s: span count mismatch between span IDs (%d) and span data (%d)",
			traceID, len(reader.block.spanIDs), len(reader.block.spans))

		// Verify each span has valid data
		for i, span := range reader.block.spans {
			require.NotEmpty(t, span, "Trace %s: span %d should not be empty", traceID, i)
			require.NotEmpty(t, reader.block.spanIDs[i], "Trace %s: span ID %d should not be empty", traceID, i)
		}

		// Verify tags
		require.NotNil(t, reader.block.tags, "Trace %s: tags should not be nil", traceID)
		for _, tag := range reader.block.tags {
			require.NotEmpty(t, tag.name, "Trace %s: tag name should not be empty", traceID)
			require.Equal(t, spanCount, len(tag.values),
				"Trace %s: tag %s should have %d values, got %d",
				traceID, tag.name, spanCount, len(tag.values))

			// Verify each tag value is valid
			for i, value := range tag.values {
				require.NotNil(t, value, "Trace %s: tag %s value %d should not be nil", traceID, tag.name, i)
			}
		}

		// Verify timestamps metadata is valid
		require.True(t, bm.timestamps.min <= bm.timestamps.max,
			"Trace %s: timestamp min (%d) should be <= max (%d)",
			traceID, bm.timestamps.min, bm.timestamps.max)

		// Verify tag metadata consistency
		require.NotNil(t, bm.tags, "Trace %s: tag metadata should not be nil", traceID)
		require.Equal(t, len(bm.tags), len(reader.block.tags),
			"Trace %s: tag metadata count (%d) should match block tags count (%d)",
			traceID, len(bm.tags), len(reader.block.tags))
	}
	require.NoError(t, reader.error())

	// Verify all expected traces are present with correct counts
	for traceID, expectedCount := range expectedTraces {
		actualCount, found := foundTraces[traceID]
		require.True(t, found, "Trace ID %s not found in merged part %d", traceID, partID)
		require.Equal(t, expectedCount, actualCount, "Trace ID %s has incorrect span count in part %d", traceID, partID)
	}

	// Verify no unexpected traces exist
	require.Equal(t, len(expectedTraces), len(foundTraces), "Part %d has unexpected number of trace IDs", partID)
}

func Test_multipleRoundMerges(t *testing.T) {
	tests := []struct {
		name   string
		rounds []struct {
			expectedTraces map[string]int
			tsList         []*traces
		}
		want []blockMetadata
	}{
		{
			name: "Two rounds with overlapping trace IDs in both rounds",
			rounds: []struct {
				expectedTraces map[string]int
				tsList         []*traces
			}{
				{
					tsList: []*traces{
						{
							traceIDs:   []string{"trace1", "trace2"},
							timestamps: []int64{1, 2},
							tags: [][]*tagValue{
								{
									{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val1"), valueArr: nil},
								},
								{
									{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val2"), valueArr: nil},
								},
							},
							spans:   [][]byte{[]byte("span1"), []byte("span2")},
							spanIDs: []string{"span1", "span2"},
						},
						{
							traceIDs:   []string{"trace1", "trace3"},
							timestamps: []int64{3, 4},
							tags: [][]*tagValue{
								{
									{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val1b"), valueArr: nil},
								},
								{
									{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val3"), valueArr: nil},
								},
							},
							spans:   [][]byte{[]byte("span1b"), []byte("span3")},
							spanIDs: []string{"span1b", "span3"},
						},
					},
					expectedTraces: map[string]int{
						"trace1": 2, // 1 from first part + 1 from second part
						"trace2": 1, // 1 from first part
						"trace3": 1, // 1 from second part
					},
				},
				{
					tsList: []*traces{
						{
							traceIDs:   []string{"trace1", "trace2"},
							timestamps: []int64{5, 6},
							tags: [][]*tagValue{
								{
									{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val1c"), valueArr: nil},
								},
								{
									{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val2b"), valueArr: nil},
								},
							},
							spans:   [][]byte{[]byte("span1c"), []byte("span2b")},
							spanIDs: []string{"span1c", "span2b"},
						},
					},
					expectedTraces: map[string]int{
						"trace1": 3, // 2 from previous round + 1 new
						"trace2": 2, // 1 from previous round + 1 new
						"trace3": 1, // 1 from previous round (unchanged)
					},
				},
			},
			want: []blockMetadata{
				{traceID: "trace1", count: 3, uncompressedSpanSizeBytes: 17},
				{traceID: "trace2", count: 2, uncompressedSpanSizeBytes: 11},
				{traceID: "trace3", count: 1, uncompressedSpanSizeBytes: 5},
			},
		},
		{
			name: "Two rounds with non-overlapping trace IDs",
			rounds: []struct {
				expectedTraces map[string]int
				tsList         []*traces
			}{
				{
					tsList: []*traces{
						{
							traceIDs:   []string{"traceA", "traceB"},
							timestamps: []int64{1, 2},
							tags: [][]*tagValue{
								{
									{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("valA"), valueArr: nil},
								},
								{
									{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("valB"), valueArr: nil},
								},
							},
							spans:   [][]byte{[]byte("spanA"), []byte("spanB")},
							spanIDs: []string{"spanA", "spanB"},
						},
						{
							traceIDs:   []string{"traceC"},
							timestamps: []int64{3},
							tags: [][]*tagValue{
								{
									{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("valC"), valueArr: nil},
								},
							},
							spans:   [][]byte{[]byte("spanC")},
							spanIDs: []string{"spanC"},
						},
					},
					expectedTraces: map[string]int{
						"traceA": 1,
						"traceB": 1,
						"traceC": 1,
					},
				},
				{
					tsList: []*traces{
						{
							traceIDs:   []string{"traceD", "traceE"},
							timestamps: []int64{4, 5},
							tags: [][]*tagValue{
								{
									{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("valD"), valueArr: nil},
								},
								{
									{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("valE"), valueArr: nil},
								},
							},
							spans:   [][]byte{[]byte("spanD"), []byte("spanE")},
							spanIDs: []string{"spanD", "spanE"},
						},
					},
					expectedTraces: map[string]int{
						"traceA": 1, // from previous round
						"traceB": 1, // from previous round
						"traceC": 1, // from previous round
						"traceD": 1, // new
						"traceE": 1, // new
					},
				},
			},
			want: []blockMetadata{
				{traceID: "traceA", count: 1, uncompressedSpanSizeBytes: 5},
				{traceID: "traceB", count: 1, uncompressedSpanSizeBytes: 5},
				{traceID: "traceC", count: 1, uncompressedSpanSizeBytes: 5},
				{traceID: "traceD", count: 1, uncompressedSpanSizeBytes: 5},
				{traceID: "traceE", count: 1, uncompressedSpanSizeBytes: 5},
			},
		},
		{
			name: "Three rounds - first round overlapping, subsequent rounds non-overlapping",
			rounds: []struct {
				expectedTraces map[string]int
				tsList         []*traces
			}{
				{
					tsList: []*traces{
						{
							traceIDs:   []string{"trace1", "trace2"},
							timestamps: []int64{1, 2},
							tags: [][]*tagValue{
								{
									{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val1"), valueArr: nil},
								},
								{
									{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val2"), valueArr: nil},
								},
							},
							spans:   [][]byte{[]byte("span1"), []byte("span2")},
							spanIDs: []string{"span1", "span2"},
						},
						{
							traceIDs:   []string{"trace1", "trace2"},
							timestamps: []int64{3, 4},
							tags: [][]*tagValue{
								{
									{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val1b"), valueArr: nil},
								},
								{
									{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val2b"), valueArr: nil},
								},
							},
							spans:   [][]byte{[]byte("span1b"), []byte("span2b")},
							spanIDs: []string{"span1b", "span2b"},
						},
					},
					expectedTraces: map[string]int{
						"trace1": 2, // overlapping from 2 parts
						"trace2": 2, // overlapping from 2 parts
					},
				},
				{
					tsList: []*traces{
						{
							traceIDs:   []string{"trace3"},
							timestamps: []int64{5},
							tags: [][]*tagValue{
								{
									{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val3"), valueArr: nil},
								},
							},
							spans:   [][]byte{[]byte("span3")},
							spanIDs: []string{"span3"},
						},
					},
					expectedTraces: map[string]int{
						"trace1": 2, // from previous round
						"trace2": 2, // from previous round
						"trace3": 1, // new, non-overlapping
					},
				},
				{
					tsList: []*traces{
						{
							traceIDs:   []string{"trace4"},
							timestamps: []int64{6},
							tags: [][]*tagValue{
								{
									{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val4"), valueArr: nil},
								},
							},
							spans:   [][]byte{[]byte("span4")},
							spanIDs: []string{"span4"},
						},
					},
					expectedTraces: map[string]int{
						"trace1": 2, // from previous rounds
						"trace2": 2, // from previous rounds
						"trace3": 1, // from previous round
						"trace4": 1, // new, non-overlapping
					},
				},
			},
			want: []blockMetadata{
				{traceID: "trace1", count: 2, uncompressedSpanSizeBytes: 11},
				{traceID: "trace2", count: 2, uncompressedSpanSizeBytes: 11},
				{traceID: "trace3", count: 1, uncompressedSpanSizeBytes: 5},
				{traceID: "trace4", count: 1, uncompressedSpanSizeBytes: 5},
			},
		},
		{
			name: "Three rounds - overlapping in first, overlapping in second, non-overlapping in third",
			rounds: []struct {
				expectedTraces map[string]int
				tsList         []*traces
			}{
				{
					tsList: []*traces{
						{
							traceIDs:   []string{"trace1"},
							timestamps: []int64{1},
							tags: [][]*tagValue{
								{
									{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val1"), valueArr: nil},
								},
							},
							spans:   [][]byte{[]byte("span1")},
							spanIDs: []string{"span1"},
						},
						{
							traceIDs:   []string{"trace1", "trace2"},
							timestamps: []int64{2, 3},
							tags: [][]*tagValue{
								{
									{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val1b"), valueArr: nil},
								},
								{
									{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val2"), valueArr: nil},
								},
							},
							spans:   [][]byte{[]byte("span1b"), []byte("span2")},
							spanIDs: []string{"span1b", "span2"},
						},
					},
					expectedTraces: map[string]int{
						"trace1": 2, // overlapping from 2 parts
						"trace2": 1, // from second part
					},
				},
				{
					tsList: []*traces{
						{
							traceIDs:   []string{"trace1", "trace3"},
							timestamps: []int64{4, 5},
							tags: [][]*tagValue{
								{
									{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val1c"), valueArr: nil},
								},
								{
									{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val3"), valueArr: nil},
								},
							},
							spans:   [][]byte{[]byte("span1c"), []byte("span3")},
							spanIDs: []string{"span1c", "span3"},
						},
					},
					expectedTraces: map[string]int{
						"trace1": 3, // 2 from previous + 1 new (overlapping)
						"trace2": 1, // from previous round
						"trace3": 1, // new
					},
				},
				{
					tsList: []*traces{
						{
							traceIDs:   []string{"trace4"},
							timestamps: []int64{6},
							tags: [][]*tagValue{
								{
									{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val4"), valueArr: nil},
								},
							},
							spans:   [][]byte{[]byte("span4")},
							spanIDs: []string{"span4"},
						},
					},
					expectedTraces: map[string]int{
						"trace1": 3, // from previous rounds
						"trace2": 1, // from previous rounds
						"trace3": 1, // from previous round
						"trace4": 1, // new, non-overlapping
					},
				},
			},
			want: []blockMetadata{
				{traceID: "trace1", count: 3, uncompressedSpanSizeBytes: 17},
				{traceID: "trace2", count: 1, uncompressedSpanSizeBytes: 5},
				{traceID: "trace3", count: 1, uncompressedSpanSizeBytes: 5},
				{traceID: "trace4", count: 1, uncompressedSpanSizeBytes: 5},
			},
		},
		{
			name: "Four rounds - complex overlapping pattern",
			rounds: []struct {
				expectedTraces map[string]int
				tsList         []*traces
			}{
				{
					// Round 1: merge traceA and traceB with overlaps
					tsList: []*traces{
						{
							traceIDs:   []string{"traceA"},
							timestamps: []int64{1},
							tags: [][]*tagValue{
								{
									{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("valA1"), valueArr: nil},
								},
							},
							spans:   [][]byte{[]byte("spanA1")},
							spanIDs: []string{"spanA1"},
						},
						{
							traceIDs:   []string{"traceA", "traceB"},
							timestamps: []int64{2, 3},
							tags: [][]*tagValue{
								{
									{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("valA2"), valueArr: nil},
								},
								{
									{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("valB1"), valueArr: nil},
								},
							},
							spans:   [][]byte{[]byte("spanA2"), []byte("spanB1")},
							spanIDs: []string{"spanA2", "spanB1"},
						},
					},
					expectedTraces: map[string]int{
						"traceA": 2, // overlapping from 2 parts
						"traceB": 1, // from second part
					},
				},
				{
					// Round 2: merge with traceB (overlapping) and traceC (new)
					tsList: []*traces{
						{
							traceIDs:   []string{"traceB", "traceC"},
							timestamps: []int64{4, 5},
							tags: [][]*tagValue{
								{
									{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("valB2"), valueArr: nil},
								},
								{
									{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("valC1"), valueArr: nil},
								},
							},
							spans:   [][]byte{[]byte("spanB2"), []byte("spanC1")},
							spanIDs: []string{"spanB2", "spanC1"},
						},
					},
					expectedTraces: map[string]int{
						"traceA": 2, // from previous round
						"traceB": 2, // 1 from previous + 1 new (overlapping)
						"traceC": 1, // new
					},
				},
				{
					// Round 3: non-overlapping traces
					tsList: []*traces{
						{
							traceIDs:   []string{"traceD"},
							timestamps: []int64{6},
							tags: [][]*tagValue{
								{
									{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("valD1"), valueArr: nil},
								},
							},
							spans:   [][]byte{[]byte("spanD1")},
							spanIDs: []string{"spanD1"},
						},
					},
					expectedTraces: map[string]int{
						"traceA": 2, // from previous rounds
						"traceB": 2, // from previous rounds
						"traceC": 1, // from previous round
						"traceD": 1, // new, non-overlapping
					},
				},
				{
					// Round 4: non-overlapping traces
					tsList: []*traces{
						{
							traceIDs:   []string{"traceE"},
							timestamps: []int64{7},
							tags: [][]*tagValue{
								{
									{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("valE1"), valueArr: nil},
								},
							},
							spans:   [][]byte{[]byte("spanE1")},
							spanIDs: []string{"spanE1"},
						},
					},
					expectedTraces: map[string]int{
						"traceA": 2, // from previous rounds
						"traceB": 2, // from previous rounds
						"traceC": 1, // from previous rounds
						"traceD": 1, // from previous round
						"traceE": 1, // new, non-overlapping
					},
				},
			},
			want: []blockMetadata{
				{traceID: "traceA", count: 2, uncompressedSpanSizeBytes: 12},
				{traceID: "traceB", count: 2, uncompressedSpanSizeBytes: 12},
				{traceID: "traceC", count: 1, uncompressedSpanSizeBytes: 6},
				{traceID: "traceD", count: 1, uncompressedSpanSizeBytes: 6},
				{traceID: "traceE", count: 1, uncompressedSpanSizeBytes: 6},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpPath, defFn := test.Space(require.New(t))
			defer defFn()
			fileSystem := fs.NewLocalFileSystem()
			tst := &tsTable{pm: protector.Nop{}, fileSystem: fileSystem, root: tmpPath}

			var mergedPartID uint64
			partID := uint64(1)

			// Execute multiple merge rounds
			for roundIdx, round := range tt.rounds {
				t.Logf("Round %d: merging %d parts", roundIdx+1, len(round.tsList))

				var partsToMerge []*partWrapper

				// If we have a merged part from previous round, include it
				if roundIdx > 0 && mergedPartID != 0 {
					// Reopen the merged part from the previous round
					prevMergedPart := mustOpenFilePart(mergedPartID, tmpPath, fileSystem)
					prevMergedPart.partMetadata.ID = mergedPartID
					partsToMerge = append(partsToMerge, newPartWrapper(nil, prevMergedPart))
					t.Logf("  Including previous merged part ID %d", mergedPartID)
				}

				// Create new parts for this round
				for i, traces := range round.tsList {
					mp := generateMemPart()
					mp.mustInitFromTraces(traces)
					mp.mustFlush(fileSystem, partPath(tmpPath, partID))
					p := mustOpenFilePart(partID, tmpPath, fileSystem)
					p.partMetadata.ID = partID
					partsToMerge = append(partsToMerge, newPartWrapper(nil, p))
					releaseMemPart(mp)
					partID++
					t.Logf("  Created part %d with %d trace(s)", i+1, len(traces.traceIDs))
				}

				// Merge all parts for this round
				closeCh := make(chan struct{})
				mergedPart, err := tst.mergeParts(fileSystem, closeCh, partsToMerge, partID, tmpPath)
				close(closeCh)
				require.NoError(t, err, "Round %d merge failed", roundIdx+1)
				require.NotNil(t, mergedPart, "Round %d produced nil merged part", roundIdx+1)

				// Release all parts that were merged
				for _, pw := range partsToMerge {
					pw.decRef()
				}

				// Store the merged part ID and release the wrapper
				mergedPartID = mergedPart.ID()
				mergedPart.decRef()
				partID++
				t.Logf("  Merged into part ID %d", mergedPartID)

				// Verify the merged part contains expected traces
				if round.expectedTraces != nil {
					t.Logf("  Verifying merged part %d contains expected traces", mergedPartID)
					verifyPartContainsTraces(t, mergedPartID, tmpPath, fileSystem, round.expectedTraces)
					t.Logf("  ✓ Verification passed for round %d", roundIdx+1)
				}
			}

			// Verify final merged results by reopening the final merged part
			require.NotZero(t, mergedPartID, "No merged part produced")
			finalMergedPart := mustOpenFilePart(mergedPartID, tmpPath, fileSystem)
			finalPW := newPartWrapper(nil, finalMergedPart)
			defer finalPW.decRef()

			pmi := generatePartMergeIter()
			pmi.mustInitFromPart(finalPW.p)
			reader := generateBlockReader()
			reader.init([]*partMergeIter{pmi})

			var got []blockMetadata
			for reader.nextBlockMetadata() {
				got = append(got, reader.block.bm)
			}
			require.NoError(t, reader.error())
			releaseBlockReader(reader)
			releasePartMergeIter(pmi)

			// Verify block counts and trace IDs
			require.Len(t, got, len(tt.want), "Final result should have %d blocks", len(tt.want))
			for i, want := range tt.want {
				require.Equal(t, want.traceID, got[i].traceID, "Block %d trace ID mismatch", i)
				require.Equal(t, want.count, got[i].count, "Block %d count mismatch for trace %s", i, want.traceID)
				require.Equal(t, want.uncompressedSpanSizeBytes, got[i].uncompressedSpanSizeBytes,
					"Block %d uncompressed size mismatch for trace %s", i, want.traceID)
			}
		})
	}
}

func Test_mergeParts(t *testing.T) {
	tests := []struct {
		wantErr error
		name    string
		tsList  []*traces
		want    []blockMetadata
	}{
		{
			name:    "Test with no trace",
			tsList:  []*traces{},
			wantErr: errNoPartToMerge,
		},
		{
			name:   "Test with single part",
			tsList: []*traces{tsTS1},
			want: []blockMetadata{
				{traceID: "trace1", count: 1, uncompressedSpanSizeBytes: 5},
				{traceID: "trace2", count: 1, uncompressedSpanSizeBytes: 5},
				{traceID: "trace3", count: 1, uncompressedSpanSizeBytes: 5},
			},
		},
		{
			name:   "Test with multiple parts",
			tsList: []*traces{tsTS1, tsTS2, tsTS2},
			want: []blockMetadata{
				{traceID: "trace1", count: 3, uncompressedSpanSizeBytes: 15},
				{traceID: "trace2", count: 3, uncompressedSpanSizeBytes: 15},
				{traceID: "trace3", count: 3, uncompressedSpanSizeBytes: 15},
			},
		},
		{
			name:   "Test with multiple parts with a large quantity of spans",
			tsList: []*traces{generateHugeTraces(5000), generateHugeTraces(5000)},
			want: []blockMetadata{
				{traceID: "trace1", count: 10000, uncompressedSpanSizeBytes: 50000},
				{traceID: "trace2", count: 2, uncompressedSpanSizeBytes: 10},
				{traceID: "trace3", count: 2, uncompressedSpanSizeBytes: 10},
			},
		},
		{
			name: "Test with different trace IDs - should use fast path (readRaw/WriteRawBlock)",
			tsList: []*traces{
				{
					traceIDs:   []string{"trace1", "trace2"},
					timestamps: []int64{1, 1},
					tags: [][]*tagValue{
						{
							{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value1"), valueArr: nil},
						},
						{
							{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value2"), valueArr: nil},
						},
					},
					spans:   [][]byte{[]byte("span1"), []byte("span2")},
					spanIDs: []string{"span1", "span2"},
				},
				{
					traceIDs:   []string{"trace3", "trace4"},
					timestamps: []int64{2, 2},
					tags: [][]*tagValue{
						{
							{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value3"), valueArr: nil},
						},
						{
							{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value4"), valueArr: nil},
						},
					},
					spans:   [][]byte{[]byte("span3"), []byte("span4")},
					spanIDs: []string{"span3", "span4"},
				},
			},
			want: []blockMetadata{
				{traceID: "trace1", count: 1, uncompressedSpanSizeBytes: 5},
				{traceID: "trace2", count: 1, uncompressedSpanSizeBytes: 5},
				{traceID: "trace3", count: 1, uncompressedSpanSizeBytes: 5},
				{traceID: "trace4", count: 1, uncompressedSpanSizeBytes: 5},
			},
		},
		{
			name: "Test with completely non-overlapping trace IDs across 3 parts - fast path",
			tsList: []*traces{
				{
					traceIDs:   []string{"traceA"},
					timestamps: []int64{1},
					tags: [][]*tagValue{
						{
							{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("valueA"), valueArr: nil},
						},
					},
					spans:   [][]byte{[]byte("spanA")},
					spanIDs: []string{"spanA"},
				},
				{
					traceIDs:   []string{"traceB"},
					timestamps: []int64{2},
					tags: [][]*tagValue{
						{
							{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("valueB"), valueArr: nil},
						},
					},
					spans:   [][]byte{[]byte("spanB")},
					spanIDs: []string{"spanB"},
				},
				{
					traceIDs:   []string{"traceC"},
					timestamps: []int64{3},
					tags: [][]*tagValue{
						{
							{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("valueC"), valueArr: nil},
						},
					},
					spans:   [][]byte{[]byte("spanC")},
					spanIDs: []string{"spanC"},
				},
			},
			want: []blockMetadata{
				{traceID: "traceA", count: 1, uncompressedSpanSizeBytes: 5},
				{traceID: "traceB", count: 1, uncompressedSpanSizeBytes: 5},
				{traceID: "traceC", count: 1, uncompressedSpanSizeBytes: 5},
			},
		},
		{
			name: "Test with interleaved non-overlapping trace IDs - fast path",
			tsList: []*traces{
				{
					traceIDs:   []string{"trace1", "trace3", "trace5"},
					timestamps: []int64{1, 1, 1},
					tags: [][]*tagValue{
						{
							{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value1"), valueArr: nil},
						},
						{
							{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value3"), valueArr: nil},
						},
						{
							{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value5"), valueArr: nil},
						},
					},
					spans:   [][]byte{[]byte("span1"), []byte("span3"), []byte("span5")},
					spanIDs: []string{"span1", "span3", "span5"},
				},
				{
					traceIDs:   []string{"trace2", "trace4", "trace6"},
					timestamps: []int64{2, 2, 2},
					tags: [][]*tagValue{
						{
							{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value2"), valueArr: nil},
						},
						{
							{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value4"), valueArr: nil},
						},
						{
							{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value6"), valueArr: nil},
						},
					},
					spans:   [][]byte{[]byte("span2"), []byte("span4"), []byte("span6")},
					spanIDs: []string{"span2", "span4", "span6"},
				},
			},
			want: []blockMetadata{
				{traceID: "trace1", count: 1, uncompressedSpanSizeBytes: 5},
				{traceID: "trace2", count: 1, uncompressedSpanSizeBytes: 5},
				{traceID: "trace3", count: 1, uncompressedSpanSizeBytes: 5},
				{traceID: "trace4", count: 1, uncompressedSpanSizeBytes: 5},
				{traceID: "trace5", count: 1, uncompressedSpanSizeBytes: 5},
				{traceID: "trace6", count: 1, uncompressedSpanSizeBytes: 5},
			},
		},
		{
			name: "Test with mixed same and different trace IDs - both fast and slow paths",
			tsList: []*traces{
				{
					traceIDs:   []string{"trace1", "trace2", "trace3"},
					timestamps: []int64{1, 1, 1},
					tags: [][]*tagValue{
						{
							{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value1"), valueArr: nil},
						},
						{
							{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value2"), valueArr: nil},
						},
						{
							{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value3"), valueArr: nil},
						},
					},
					spans:   [][]byte{[]byte("span1"), []byte("span2"), []byte("span3")},
					spanIDs: []string{"span1", "span2", "span3"},
				},
				{
					traceIDs:   []string{"trace1", "trace4"},
					timestamps: []int64{2, 2},
					tags: [][]*tagValue{
						{
							{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value1b"), valueArr: nil},
						},
						{
							{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value4"), valueArr: nil},
						},
					},
					spans:   [][]byte{[]byte("span1b"), []byte("span4")},
					spanIDs: []string{"span1b", "span4"},
				},
			},
			want: []blockMetadata{
				{traceID: "trace1", count: 2, uncompressedSpanSizeBytes: 11},
				{traceID: "trace2", count: 1, uncompressedSpanSizeBytes: 5},
				{traceID: "trace3", count: 1, uncompressedSpanSizeBytes: 5},
				{traceID: "trace4", count: 1, uncompressedSpanSizeBytes: 5},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			verify := func(t *testing.T, pp []*partWrapper, fileSystem fs.FileSystem, root string, partID uint64) {
				closeCh := make(chan struct{})
				defer close(closeCh)
				tst := &tsTable{pm: protector.Nop{}}
				p, err := tst.mergeParts(fileSystem, closeCh, pp, partID, root)
				if tt.wantErr != nil {
					if !errors.Is(err, tt.wantErr) {
						t.Fatalf("Unexpected error: got %v, want %v", err, tt.wantErr)
					}
					return
				}
				defer p.decRef()
				pmi := &partMergeIter{}
				pmi.mustInitFromPart(p.p)
				reader := &blockReader{}
				reader.init([]*partMergeIter{pmi})
				var got []blockMetadata
				for reader.nextBlockMetadata() {
					got = append(got, reader.block.bm)
				}
				require.NoError(t, reader.error())

				if diff := cmp.Diff(got, tt.want,
					cmpopts.IgnoreFields(blockMetadata{}, "tags"),
					cmpopts.IgnoreFields(blockMetadata{}, "tagType"),
					cmpopts.IgnoreFields(blockMetadata{}, "spans"),
					cmpopts.IgnoreFields(blockMetadata{}, "timestamps"),
					cmpopts.IgnoreFields(blockMetadata{}, "tagProjection"),
					cmpopts.IgnoreFields(blockMetadata{}, "uncompressedSpanSizeBytes"),
					cmp.AllowUnexported(blockMetadata{}),
				); diff != "" {
					t.Errorf("Unexpected blockMetadata (-got +want):\n%s", diff)
				}
			}

			t.Run("memory parts", func(t *testing.T) {
				var pp []*partWrapper
				tmpPath, defFn := test.Space(require.New(t))
				defer func() {
					for _, pw := range pp {
						pw.decRef()
					}
					defFn()
				}()
				for _, ts := range tt.tsList {
					mp := generateMemPart()
					mp.mustInitFromTraces(ts)
					pp = append(pp, newPartWrapper(mp, openMemPart(mp)))
				}
				verify(t, pp, fs.NewLocalFileSystem(), tmpPath, 1)
			})

			t.Run("file parts", func(t *testing.T) {
				var fpp []*partWrapper
				tmpPath, defFn := test.Space(require.New(t))
				defer func() {
					for _, pw := range fpp {
						pw.decRef()
					}
					defFn()
				}()
				fileSystem := fs.NewLocalFileSystem()
				for i, ts := range tt.tsList {
					mp := generateMemPart()
					mp.mustInitFromTraces(ts)
					mp.mustFlush(fileSystem, partPath(tmpPath, uint64(i)))
					filePW := newPartWrapper(nil, mustOpenFilePart(uint64(i), tmpPath, fileSystem))
					filePW.p.partMetadata.ID = uint64(i)
					fpp = append(fpp, filePW)
					releaseMemPart(mp)
				}
				verify(t, fpp, fileSystem, tmpPath, uint64(len(tt.tsList)))
			})
		})
	}
}
